"""Single-ticker, multi-metric retrieval over the metric registry.

Companion to ``arrow.retrieval.screener`` (cross-company ranking). Both
read the same `MetricSpec` registry, so every metric becomes queryable
through both ``screen()`` and ``get_metric_values()`` the moment it is
registered. The /ask agent's ``get_metrics`` tool wraps this — it is the
"give me these metric values for this ticker at this period" primitive.

Policy:
- Verticals do not mix in a single call; financials uses company_id while
  estimates/valuation use security_id and their period axes differ.
- Within a vertical, metrics from different views compose: each view
  contributes its own SELECT and rows merge by the period anchor.
- Annual period (FYNNNN) on a quarter-grain TTM/ROIC metric resolves to
  the latest period_end at-or-before fy_end — the year-end snapshot.
- Quarterly period (YYYY-QN) on an annual metric is rejected.
- Window periods return one row per period (no aggregation by default —
  /ask is exploring, not ranking).
- 'latest' resolves per view independently when views disagree on a most
  recent row, surfacing one row per view.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date as _date
from decimal import Decimal
from typing import Any

import psycopg

from arrow.retrieval._query import run_query
from arrow.retrieval.period_spec import ParsedPeriod, parse_period
from arrow.retrieval.registry import METRICS, MetricSpec, get_metric


@dataclass(frozen=True)
class MetricRow:
    """One period's worth of values for the requested metrics.

    `period_label` is the human-friendly fiscal label (e.g. "FY2024 Q3")
    when the source view exposes one; otherwise it falls back to the ISO
    period_end. `values` carries each requested metric_name → Decimal | None.
    `citations` carries metric_name → ``M:view:entity_id:period_key``.
    """

    period_end: str
    period_label: str
    values: dict[str, Decimal | None]
    citations: dict[str, str]


def get_metric_values(
    conn: psycopg.Connection,
    *,
    ticker: str,
    metric_names: list[str],
    period: str = "latest",
    period_kind: str | None = None,
) -> list[MetricRow]:
    """Return one or more ``MetricRow`` rows.

    Raises ``ValueError`` for unknown metric names, mixed verticals, or
    incompatible (grain × period) combinations. Returns an empty list when
    the ticker resolves but no row matches the requested period.
    """
    if not metric_names:
        raise ValueError("metric_names must be non-empty")

    specs = [get_metric(name) for name in metric_names]
    verticals = {s.vertical for s in specs}
    if len(verticals) > 1:
        raise ValueError(
            f"metrics span multiple verticals {sorted(verticals)} — split into "
            "separate calls (financials uses company_id; estimates/valuation use "
            "security_id and live on different period axes)."
        )
    parsed = parse_period(period)
    vertical = next(iter(verticals))

    if vertical == "financials":
        return _financials(conn, ticker=ticker, specs=specs, parsed=parsed)
    if vertical == "valuation":
        return _valuation(conn, ticker=ticker, specs=specs, parsed=parsed)
    if vertical == "estimates":
        return _estimates(
            conn,
            ticker=ticker,
            specs=specs,
            parsed=parsed,
            period_kind=(period_kind or "quarter"),
        )
    raise ValueError(f"unknown vertical {vertical!r}")


# --------------------------------------------------------------------------- #
# Financials
# --------------------------------------------------------------------------- #


_FINANCIALS_RECENCY_COL = {
    "v_metrics_fy": "fy_end",
    "v_metrics_q": "period_end",
    "v_metrics_roic": "period_end",
    "v_metrics_ttm": "period_end",
    "v_metrics_ttm_yoy": "period_end",
    "v_metrics_cy": "period_end",
}

_FINANCIALS_HAS_FISCAL_LABEL = {"v_metrics_fy", "v_metrics_q", "v_metrics_cy"}


def _financials(
    conn: psycopg.Connection,
    *,
    ticker: str,
    specs: list[MetricSpec],
    parsed: ParsedPeriod,
) -> list[MetricRow]:
    co_rows = run_query(
        conn,
        sql="SELECT id FROM companies WHERE ticker = %s LIMIT 1;",
        params=(ticker.upper(),),
    )
    if not co_rows:
        return []
    company_id = int(co_rows[0]["id"])

    grain_set = {s.period_grain for s in specs}

    if parsed.kind in ("asof", "forward_single", "forward_window"):
        raise ValueError(
            f"period {parsed.raw!r} is not supported for financials. Use "
            "'latest', 'FYNNNN', 'YYYY-QN', or 'last_N[q|y]'."
        )
    if parsed.kind == "single_fq" and "annual" in grain_set:
        bad = sorted(s.name for s in specs if s.period_grain == "annual")
        raise ValueError(
            f"metrics {bad} are annual-grain — pass 'FYNNNN' rather than "
            f"{parsed.raw!r} for them, or split the call."
        )
    if parsed.kind == "window":
        if parsed.grain == "q" and "annual" in grain_set:
            bad = sorted(s.name for s in specs if s.period_grain == "annual")
            raise ValueError(
                f"metrics {bad} are annual-grain — use 'last_Ny' (not "
                f"'last_Nq') or split the call."
            )
        if parsed.grain == "y" and "quarter" in grain_set:
            bad = sorted(s.name for s in specs if s.period_grain == "quarter")
            raise ValueError(
                f"metrics {bad} are quarter-grain — use 'last_Nq' (not "
                f"'last_Ny') or split the call."
            )

    # Resolve the period-end anchor that quarter-keyed views need when the
    # caller asked for a single fiscal year/quarter. v_metrics_fy answers
    # by fiscal_year directly; v_metrics_q answers by (fiscal_year,
    # fiscal_quarter); the period-end-only views (roic/ttm/ttm_yoy) need
    # this anchor to filter.
    anchor_date: _date | None = None
    if parsed.kind == "single_fy":
        rows = run_query(
            conn,
            sql="SELECT fy_end FROM v_metrics_fy WHERE company_id = %s AND fiscal_year = %s LIMIT 1;",
            params=(company_id, parsed.year),
        )
        if rows:
            anchor_date = rows[0]["fy_end"]
    elif parsed.kind == "single_fq":
        rows = run_query(
            conn,
            sql=(
                "SELECT period_end FROM v_metrics_q "
                "WHERE company_id = %s AND fiscal_year = %s AND fiscal_quarter = %s LIMIT 1;"
            ),
            params=(company_id, parsed.year, parsed.quarter),
        )
        if rows:
            anchor_date = rows[0]["period_end"]

    by_view: dict[str, list[MetricSpec]] = {}
    for s in specs:
        by_view.setdefault(s.view, []).append(s)

    accumulator: dict[str, dict[str, Any]] = {}
    for view, view_specs in by_view.items():
        view_rows = _financials_view_rows(
            conn,
            view=view,
            view_specs=view_specs,
            parsed=parsed,
            anchor_date=anchor_date,
            company_id=company_id,
        )
        for vr in view_rows:
            slot = accumulator.setdefault(
                vr["period_end_iso"],
                {
                    "period_end": vr["period_end_iso"],
                    "period_label": vr["period_label"] or vr["period_end_iso"],
                    "values": {},
                    "citations": {},
                },
            )
            if vr["period_label"] and slot["period_label"] == slot["period_end"]:
                slot["period_label"] = vr["period_label"]
            citation = f"M:{view}:{company_id}:{vr['cite_period_key']}"
            for spec in view_specs:
                slot["values"][spec.name] = vr["values"].get(spec.name)
                slot["citations"][spec.name] = citation

    rows = sorted(accumulator.values(), key=lambda r: r["period_end"], reverse=True)
    return [MetricRow(**r) for r in rows]


def _financials_view_rows(
    conn: psycopg.Connection,
    *,
    view: str,
    view_specs: list[MetricSpec],
    parsed: ParsedPeriod,
    anchor_date: _date | None,
    company_id: int,
) -> list[dict[str, Any]]:
    period_col = _FINANCIALS_RECENCY_COL.get(view, "period_end")
    has_fiscal_label = view in _FINANCIALS_HAS_FISCAL_LABEL
    cite_col_sql = "fiscal_period_label" if has_fiscal_label else f"{period_col}::text"
    label_col_sql = "fiscal_period_label" if has_fiscal_label else "NULL::text"
    select_metrics_sql = ", ".join(
        f"({s.value_expr})::numeric AS {s.name}" for s in view_specs
    )

    if parsed.kind == "latest":
        any_nonnull = " OR ".join(f"({s.value_expr}) IS NOT NULL" for s in view_specs)
        sql = f"""
            SELECT
                {period_col}::text AS period_end_iso,
                {cite_col_sql}     AS cite_period_key,
                {label_col_sql}    AS period_label,
                {select_metrics_sql}
            FROM {view}
            WHERE company_id = %s AND ({any_nonnull})
            ORDER BY {period_col} DESC
            LIMIT 1;
        """
        params: tuple[Any, ...] = (company_id,)
    elif parsed.kind == "single_fy":
        if view == "v_metrics_fy":
            sql = f"""
                SELECT
                    {period_col}::text AS period_end_iso,
                    {cite_col_sql}     AS cite_period_key,
                    {label_col_sql}    AS period_label,
                    {select_metrics_sql}
                FROM {view}
                WHERE company_id = %s AND fiscal_year = %s
                LIMIT 1;
            """
            params = (company_id, parsed.year)
        else:
            if anchor_date is None:
                return []
            sql = f"""
                SELECT
                    {period_col}::text AS period_end_iso,
                    {cite_col_sql}     AS cite_period_key,
                    {label_col_sql}    AS period_label,
                    {select_metrics_sql}
                FROM {view}
                WHERE company_id = %s AND {period_col} <= %s
                ORDER BY {period_col} DESC
                LIMIT 1;
            """
            params = (company_id, anchor_date)
    elif parsed.kind == "single_fq":
        if view == "v_metrics_q":
            sql = f"""
                SELECT
                    {period_col}::text AS period_end_iso,
                    {cite_col_sql}     AS cite_period_key,
                    {label_col_sql}    AS period_label,
                    {select_metrics_sql}
                FROM {view}
                WHERE company_id = %s AND fiscal_year = %s AND fiscal_quarter = %s
                LIMIT 1;
            """
            params = (company_id, parsed.year, parsed.quarter)
        else:
            if anchor_date is None:
                return []
            sql = f"""
                SELECT
                    {period_col}::text AS period_end_iso,
                    {cite_col_sql}     AS cite_period_key,
                    {label_col_sql}    AS period_label,
                    {select_metrics_sql}
                FROM {view}
                WHERE company_id = %s AND {period_col} = %s
                LIMIT 1;
            """
            params = (company_id, anchor_date)
    elif parsed.kind == "window":
        n = int(parsed.n_periods or 0)
        if n <= 0:
            return []
        sql = f"""
            SELECT
                {period_col}::text AS period_end_iso,
                {cite_col_sql}     AS cite_period_key,
                {label_col_sql}    AS period_label,
                {select_metrics_sql}
            FROM {view}
            WHERE company_id = %s
            ORDER BY {period_col} DESC
            LIMIT %s;
        """
        params = (company_id, n)
    else:
        return []

    rows = run_query(conn, sql=sql, params=params)
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "period_end_iso": r["period_end_iso"],
                "period_label": r["period_label"],
                "cite_period_key": str(r["cite_period_key"]),
                "values": {s.name: r[s.name] for s in view_specs},
            }
        )
    return out


# --------------------------------------------------------------------------- #
# Valuation
# --------------------------------------------------------------------------- #


def _valuation(
    conn: psycopg.Connection,
    *,
    ticker: str,
    specs: list[MetricSpec],
    parsed: ParsedPeriod,
) -> list[MetricRow]:
    if parsed.kind not in ("latest", "asof"):
        raise ValueError(
            f"period {parsed.raw!r} not supported for valuation. Use 'latest' or 'asof:YYYY-MM-DD'."
        )
    sec_rows = run_query(
        conn,
        sql=(
            "SELECT id FROM securities WHERE ticker = %s AND status = 'active' LIMIT 1;"
        ),
        params=(ticker.upper(),),
    )
    if not sec_rows:
        return []
    sid = int(sec_rows[0]["id"])

    select_metrics_sql = ", ".join(
        f"({s.value_expr})::numeric AS {s.name}" for s in specs
    )
    any_nonnull = " OR ".join(f"({s.value_expr}) IS NOT NULL" for s in specs)

    if parsed.kind == "latest":
        sql = f"""
            SELECT date::text AS period_end_iso,
                   date::text AS cite_period_key,
                   fiscal_period_label_at_asof AS period_label,
                   {select_metrics_sql}
            FROM v_valuation_ratios_ttm
            WHERE security_id = %s AND ({any_nonnull})
            ORDER BY date DESC
            LIMIT 1;
        """
        rows = run_query(conn, sql=sql, params=(sid,))
    else:
        sql = f"""
            SELECT date::text AS period_end_iso,
                   date::text AS cite_period_key,
                   fiscal_period_label_at_asof AS period_label,
                   {select_metrics_sql}
            FROM v_valuation_ratios_ttm
            WHERE security_id = %s AND date <= %s AND ({any_nonnull})
            ORDER BY date DESC
            LIMIT 1;
        """
        rows = run_query(conn, sql=sql, params=(sid, parsed.as_of))

    out: list[MetricRow] = []
    for r in rows:
        cite = f"M:v_valuation_ratios_ttm:{sid}:{r['cite_period_key']}"
        out.append(
            MetricRow(
                period_end=r["period_end_iso"],
                period_label=str(r["period_label"] or r["period_end_iso"]),
                values={s.name: r[s.name] for s in specs},
                citations={s.name: cite for s in specs},
            )
        )
    return out


# --------------------------------------------------------------------------- #
# Estimates
# --------------------------------------------------------------------------- #


def _estimates(
    conn: psycopg.Connection,
    *,
    ticker: str,
    specs: list[MetricSpec],
    parsed: ParsedPeriod,
    period_kind: str,
) -> list[MetricRow]:
    pk = period_kind.lower()
    if pk not in ("annual", "quarter"):
        raise ValueError(f"period_kind must be 'annual' | 'quarter', got {period_kind!r}")
    growth = sorted(s.name for s in specs if s.yoy_pair is not None)
    if growth:
        raise ValueError(
            f"forward-growth metrics {growth} not supported in get_metrics — call "
            "screen_estimates for cross-company growth ranking, or read_consensus to see "
            "forward and prior-year consensus side-by-side for one ticker."
        )
    if parsed.kind not in ("latest", "forward_single", "forward_window"):
        raise ValueError(
            f"period {parsed.raw!r} not supported for estimates. Use 'next', 'next_fy', "
            "'forward_4q_avg', 'forward_2y_avg', or 'latest'."
        )
    sec_rows = run_query(
        conn,
        sql=(
            "SELECT id FROM securities WHERE ticker = %s AND status = 'active' LIMIT 1;"
        ),
        params=(ticker.upper(),),
    )
    if not sec_rows:
        return []
    sid = int(sec_rows[0]["id"])

    select_metrics_sql = ", ".join(
        f"({s.value_expr})::numeric AS {s.name}" for s in specs
    )

    if parsed.kind == "latest":
        n = 1
    elif parsed.kind == "forward_single":
        n = 1
        if (parsed.grain or "q") == "y":
            pk = "annual"
        else:
            pk = "quarter"
    else:
        n = int(parsed.n_periods or 4)
        if (parsed.grain or "q") == "y":
            pk = "annual"
        else:
            pk = "quarter"

    sql = f"""
        SELECT period_end::text AS period_end_iso,
               period_end::text AS cite_period_key,
               NULL::text       AS period_label,
               {select_metrics_sql}
        FROM analyst_estimates
        WHERE security_id = %s AND period_kind = %s AND period_end >= CURRENT_DATE
        ORDER BY period_end ASC
        LIMIT %s;
    """
    rows = run_query(conn, sql=sql, params=(sid, pk, n))
    out: list[MetricRow] = []
    for r in rows:
        cite = f"M:analyst_estimates:{sid}:{r['cite_period_key']}"
        out.append(
            MetricRow(
                period_end=r["period_end_iso"],
                period_label=r["period_end_iso"],
                values={s.name: r[s.name] for s in specs},
                citations={s.name: cite for s in specs},
            )
        )
    return out
