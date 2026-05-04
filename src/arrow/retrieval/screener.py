"""Generic universe screener.

One entry point — `screen()` — that ranks every company in the universe
by a registered metric, for a parsed period spec, with an aggregation
('level' | 'delta' | 'relative_change'). Replaces the per-question
screen_X tools by parameterizing along the natural axes.

Vertical-specific SQL builders live in this module. Each builder
produces a base CTE shape:

    SELECT ticker, company_id, recency_key, value
    FROM <view>
    WHERE <value_expr> IS NOT NULL [+ vertical-specific filters]

The shared rank/agg layer composes from there.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

import psycopg

from arrow.retrieval._query import run_query
from arrow.retrieval.period_spec import ParsedPeriod, is_window, parse_period, supports_agg
from arrow.retrieval.registry import METRICS, MetricSpec, get_metric


# --------------------------------------------------------------------------- #
# Result types
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ScreenRow:
    """One ranked company row.

    `value` is the primary scalar produced by the agg ('level' returns
    the period value or window aggregate; 'delta' returns latest_avg −
    earliest_avg; 'relative_change' returns delta / |earliest_avg|).
    `earliest_value` and `latest_value` are populated for delta /
    relative_change aggs, and `n_periods` reports observed coverage.
    """
    rank: int
    ticker: str
    company_id: int
    value: Decimal | None
    earliest_value: Decimal | None
    latest_value: Decimal | None
    n_periods: int
    period_start: str
    period_end: str
    metric: str
    metric_kind: str
    view_name: str
    citation: str


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #


def screen(
    conn: psycopg.Connection,
    *,
    metric: str,
    period: str = "latest",
    agg: str = "level",
    sort: str = "desc",
    limit: int = 10,
    period_kind: str | None = None,
) -> list[ScreenRow]:
    """Rank the universe by `metric` for `period`, aggregated by `agg`.

    Args:
        metric: name registered in `arrow.retrieval.registry.METRICS`.
        period: spec string; see `arrow.retrieval.period_spec.parse_period`.
        agg: 'level' (default), 'delta', or 'relative_change'.
        sort: 'desc' (highest first; default) or 'asc'.
        limit: max rows. Capped to 100 to keep payloads bounded.
        period_kind: estimates-only — 'annual' or 'quarter' (default 'quarter').

    Raises:
        ValueError on unknown metric, malformed period, or incompatible
        (period × agg) combos.
    """
    spec = get_metric(metric)
    parsed = parse_period(period)

    if agg not in ("level", "delta", "relative_change"):
        raise ValueError(f"agg must be 'level'|'delta'|'relative_change', got {agg!r}")
    if not supports_agg(parsed, agg):
        raise ValueError(
            f"period {period!r} is not a window — agg {agg!r} requires a "
            "windowed period spec like 'last_12q' or 'forward_8q'."
        )
    if sort not in ("asc", "desc"):
        raise ValueError(f"sort must be 'asc'|'desc', got {sort!r}")
    limit = max(1, min(int(limit), 100))

    if spec.vertical == "financials":
        return _screen_financials(conn, spec=spec, parsed=parsed, agg=agg, sort=sort, limit=limit)
    if spec.vertical == "estimates":
        pk = (period_kind or "quarter").lower()
        if pk not in ("annual", "quarter"):
            raise ValueError(f"period_kind must be 'annual'|'quarter', got {period_kind!r}")
        return _screen_estimates(
            conn, spec=spec, parsed=parsed, agg=agg, sort=sort, limit=limit, period_kind=pk
        )
    if spec.vertical == "valuation":
        return _screen_valuation(conn, spec=spec, parsed=parsed, agg=agg, sort=sort, limit=limit)
    raise ValueError(f"unknown vertical {spec.vertical!r} for metric {metric!r}")


# --------------------------------------------------------------------------- #
# Financials
# --------------------------------------------------------------------------- #


def _financials_recency_col(view: str) -> str:
    if view == "v_metrics_fy":
        return "fy_end"
    return "period_end"


def _financials_citation_col(view: str) -> str:
    """The column used in citation strings for popup lookup.

    Matches the keys in `arrow.web.ask._METRIC_VIEW_KEY_FIELDS`.
    """
    if view in ("v_metrics_fy", "v_metrics_q"):
        return "fiscal_period_label"
    if view == "v_metrics_cy":
        return "calendar_period_label"
    # period_end-keyed views: v_metrics_roic, v_metrics_ttm, v_metrics_ttm_yoy
    return "period_end::text"


def _financials_universe_count(conn: psycopg.Connection, spec: MetricSpec) -> int:
    sql = (
        f"SELECT COUNT(DISTINCT company_id)::int AS n FROM {spec.view} "
        f"WHERE ({spec.value_expr}) IS NOT NULL;"
    )
    rows = run_query(conn, sql=sql, params=())
    return rows[0]["n"] if rows else 0


def _screen_financials(
    conn: psycopg.Connection,
    *,
    spec: MetricSpec,
    parsed: ParsedPeriod,
    agg: str,
    sort: str,
    limit: int,
) -> list[ScreenRow]:
    recency_col = _financials_recency_col(spec.view)
    cite_col = _financials_citation_col(spec.view)
    order = "DESC" if sort == "desc" else "ASC"

    # Validate (grain × period kind) compatibility.
    if parsed.kind == "single_fy" and spec.period_grain == "quarter":
        raise ValueError(
            f"metric '{spec.name}' is quarter-grain — pass 'YYYY-QN' or "
            "'last_Nq[_avg|_sum]', not 'FYNNNN'."
        )
    if parsed.kind == "single_fq" and spec.period_grain != "quarter":
        raise ValueError(
            f"metric '{spec.name}' is annual-grain — use 'FYNNNN' or "
            "'last_Ny[_avg|_sum]', not 'YYYY-QN'."
        )
    if parsed.kind in ("forward_single", "forward_window", "asof"):
        raise ValueError(
            f"period {parsed.raw!r} is not supported for financials; use "
            "'latest', 'FYNNNN', 'YYYY-QN', or 'last_N[q|y][_avg|_sum]'."
        )

    if parsed.kind == "single_fy":
        sql = f"""
            WITH base AS (
                SELECT ticker, company_id,
                       {recency_col}::text AS recency_key,
                       {cite_col} AS cite_period,
                       ({spec.value_expr})::numeric AS value
                FROM {spec.view}
                WHERE ({spec.value_expr}) IS NOT NULL
                  AND fiscal_year = %s
            )
            SELECT ticker, company_id, value,
                   value AS earliest_value, value AS latest_value,
                   1 AS n_periods,
                   cite_period AS period_start, cite_period AS period_end
            FROM base
            ORDER BY value {order} NULLS LAST
            LIMIT %s;
        """
        return _exec(conn, sql, (parsed.year, limit), spec)

    if parsed.kind == "single_fq":
        sql = f"""
            WITH base AS (
                SELECT ticker, company_id,
                       {recency_col}::text AS recency_key,
                       {cite_col} AS cite_period,
                       ({spec.value_expr})::numeric AS value
                FROM {spec.view}
                WHERE ({spec.value_expr}) IS NOT NULL
                  AND fiscal_year = %s AND fiscal_quarter = %s
            )
            SELECT ticker, company_id, value,
                   value AS earliest_value, value AS latest_value,
                   1 AS n_periods,
                   cite_period AS period_start, cite_period AS period_end
            FROM base
            ORDER BY value {order} NULLS LAST
            LIMIT %s;
        """
        return _exec(conn, sql, (parsed.year, parsed.quarter, limit), spec)

    if parsed.kind == "latest":
        sql = f"""
            WITH ranked AS (
                SELECT ticker, company_id,
                       {recency_col}::text AS recency_key,
                       {cite_col} AS cite_period,
                       ({spec.value_expr})::numeric AS value,
                       ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY {recency_col} DESC) AS rn
                FROM {spec.view}
                WHERE ({spec.value_expr}) IS NOT NULL
            )
            SELECT ticker, company_id, value,
                   value AS earliest_value, value AS latest_value,
                   1 AS n_periods,
                   cite_period AS period_start, cite_period AS period_end
            FROM ranked
            WHERE rn = 1
            ORDER BY value {order} NULLS LAST
            LIMIT %s;
        """
        return _exec(conn, sql, (limit,), spec)

    # window — last_Nq or last_Ny
    if parsed.kind == "window":
        n = parsed.n_periods or 0
        # For quarter-grain views, 'y' grain expands to 4y → 4*N quarters.
        if parsed.grain == "y" and spec.period_grain == "quarter":
            n_periods = n * 4
        else:
            n_periods = n
        return _financials_window(
            conn,
            spec=spec,
            recency_col=recency_col,
            cite_col=cite_col,
            n_periods=n_periods,
            agg=agg,
            agg_within=parsed.agg,
            sort=sort,
            limit=limit,
        )

    raise ValueError(f"unhandled period kind {parsed.kind!r} for financials")


def _financials_window(
    conn: psycopg.Connection,
    *,
    spec: MetricSpec,
    recency_col: str,
    cite_col: str,
    n_periods: int,
    agg: str,
    agg_within: str | None,
    sort: str,
    limit: int,
) -> list[ScreenRow]:
    """Trailing-window screen.

    `agg='level'` reduces the window to a single value per company via
    AVG (default) or SUM when the period spec ended in `_sum`.

    `agg='delta'` and `'relative_change'` split the window into early
    (first 1/3) vs late (last 1/3) buckets — the middle is excluded as
    transition. Same semantics as the prior trajectory tool.
    """
    order = "DESC" if sort == "desc" else "ASC"
    min_coverage = max(3, n_periods - 1) if agg != "level" else max(1, n_periods // 2)

    if agg == "level":
        within = (agg_within or "avg").upper()
        if within not in ("AVG", "SUM"):
            raise ValueError(f"unsupported window agg {agg_within!r}")
        sql = f"""
            WITH base AS (
                SELECT ticker, company_id,
                       {recency_col} AS recency_key,
                       {cite_col} AS cite_period,
                       ({spec.value_expr})::numeric AS value
                FROM {spec.view}
                WHERE ({spec.value_expr}) IS NOT NULL
            ),
            windowed AS (
                SELECT ticker, company_id, recency_key, cite_period, value,
                       ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY recency_key DESC) AS rn
                FROM base
            )
            SELECT
                ticker, company_id,
                {within}(value)::numeric AS value,
                {within}(value)::numeric AS earliest_value,
                {within}(value)::numeric AS latest_value,
                COUNT(*)::int AS n_periods,
                MIN(cite_period) AS period_start,
                MAX(cite_period) AS period_end
            FROM windowed
            WHERE rn <= %s
            GROUP BY ticker, company_id
            HAVING COUNT(*) >= %s
            ORDER BY value {order} NULLS LAST
            LIMIT %s;
        """
        return _exec(conn, sql, (n_periods, min_coverage, limit), spec)

    # delta / relative_change — bucketed comparison
    bucket_size = max(1, n_periods // 3)
    rank_expr = (
        "(latest_value - earliest_value)"
        if agg == "delta"
        else "CASE WHEN earliest_value IS NULL OR earliest_value = 0 THEN NULL "
             "ELSE (latest_value - earliest_value) / ABS(earliest_value) END"
    )
    sql = f"""
        WITH base AS (
            SELECT ticker, company_id,
                   {recency_col} AS recency_key,
                   {cite_col} AS cite_period,
                   ({spec.value_expr})::numeric AS value
            FROM {spec.view}
            WHERE ({spec.value_expr}) IS NOT NULL
        ),
        windowed AS (
            SELECT ticker, company_id, recency_key, cite_period, value,
                   ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY recency_key DESC) AS rn_desc
            FROM base
        ),
        bucketed AS (
            SELECT ticker, company_id, recency_key, cite_period, value,
                   CASE
                       WHEN rn_desc <= %s                            THEN 'recent'
                       WHEN rn_desc > %s - %s AND rn_desc <= %s     THEN 'early'
                       ELSE 'middle'
                   END AS bucket
            FROM windowed
            WHERE rn_desc <= %s
        ),
        agg AS (
            SELECT ticker, company_id,
                   AVG(value) FILTER (WHERE bucket = 'recent') AS latest_value,
                   AVG(value) FILTER (WHERE bucket = 'early')  AS earliest_value,
                   COUNT(*) AS n_periods,
                   MIN(cite_period) AS period_start,
                   MAX(cite_period) AS period_end
            FROM bucketed
            GROUP BY ticker, company_id
            HAVING COUNT(*) >= %s
        )
        SELECT ticker, company_id,
               ({rank_expr})::numeric AS value,
               earliest_value, latest_value,
               n_periods, period_start, period_end
        FROM agg
        WHERE latest_value IS NOT NULL AND earliest_value IS NOT NULL
        ORDER BY value {order} NULLS LAST
        LIMIT %s;
    """
    return _exec(
        conn,
        sql,
        (
            bucket_size,                            # rn_desc <= bucket_size (recent)
            n_periods, bucket_size,                 # rn_desc > N - bucket_size
            n_periods,                              # rn_desc <= N (early)
            n_periods,                              # rn_desc <= N (window cap)
            min_coverage,
            limit,
        ),
        spec,
    )


# --------------------------------------------------------------------------- #
# Estimates (forward consensus)
# --------------------------------------------------------------------------- #


def _screen_estimates(
    conn: psycopg.Connection,
    *,
    spec: MetricSpec,
    parsed: ParsedPeriod,
    agg: str,
    sort: str,
    limit: int,
    period_kind: str,
) -> list[ScreenRow]:
    """Universe screen on forward consensus rows.

    Period spec interprets:
      - 'next' / 'next_q' / 'next_fy' → first forward period
      - 'forward_Nq[_avg|_sum]'       → forward window of N periods
      - 'forward_Ny[_avg|_sum]'       → forward window of N annual periods
      - 'latest'                       → most recent row (typically the next forward)

    For YoY-growth metrics (`spec.yoy_pair` set), the screener self-joins
    each forward period with its same-period-prior-year row from
    analyst_estimates and computes (forward − priorY) / |priorY|.

    Period_kind ('annual'|'quarter') is the analyst_estimates partition.
    """
    order = "DESC" if sort == "desc" else "ASC"

    # Day-one estimate windows: only forward shapes + 'latest'/'next*' supported.
    if parsed.kind in ("single_fy", "single_fq", "window", "asof"):
        raise ValueError(
            f"period {parsed.raw!r} not supported for estimates. Use 'next', "
            "'next_fy', 'latest', or 'forward_Nq[_avg|_sum]' / 'forward_Ny[_avg|_sum]'."
        )

    n_forward, agg_within, grain = _resolve_forward_window(parsed)
    # If parsed gave an annual grain (next_fy / forward_Ny), period_kind must align.
    if grain == "y" and period_kind != "annual":
        period_kind = "annual"
    elif grain == "q" and period_kind != "quarter":
        # 'next' / 'forward_Nq' override caller's period_kind to quarter for clarity.
        period_kind = "quarter"

    is_growth = spec.yoy_pair is not None
    val_col = spec.value_expr  # this is the forward column, e.g. 'eps_avg'

    if not is_growth and agg == "level":
        # Simple forward window — average or sum of forward values.
        within = (agg_within or "avg").upper()
        if within not in ("AVG", "SUM"):
            raise ValueError(f"unsupported window agg {agg_within!r}")
        sql = f"""
            WITH base AS (
                SELECT s.ticker, s.id AS company_id,
                       ae.period_end::text AS recency_key,
                       ({val_col})::numeric AS value,
                       ROW_NUMBER() OVER (PARTITION BY s.id ORDER BY ae.period_end ASC) AS rn_asc
                FROM analyst_estimates ae
                JOIN securities s ON s.id = ae.security_id
                WHERE s.status = 'active'
                  AND ae.period_kind = %s
                  AND ae.period_end >= CURRENT_DATE
                  AND ({val_col}) IS NOT NULL
            )
            SELECT
                ticker, company_id,
                {within}(value)::numeric AS value,
                {within}(value)::numeric AS earliest_value,
                {within}(value)::numeric AS latest_value,
                COUNT(*)::int AS n_periods,
                MIN(recency_key) AS period_start,
                MAX(recency_key) AS period_end
            FROM base
            WHERE rn_asc <= %s
            GROUP BY ticker, company_id
            HAVING COUNT(*) >= %s
            ORDER BY value {order} NULLS LAST
            LIMIT %s;
        """
        min_coverage = max(1, n_forward // 2) if n_forward >= 2 else 1
        return _exec(
            conn,
            sql,
            (period_kind, n_forward, min_coverage, limit),
            spec,
            view_name="analyst_estimates",
        )

    if is_growth and agg == "level":
        # YoY growth — pair each forward period with the same period 1y prior.
        # "1y prior" uses period_end - INTERVAL '1 year' against analyst_estimates
        # rows of the same period_kind (the table holds both forward and back-history).
        sql = f"""
            WITH forward AS (
                SELECT s.ticker, s.id AS company_id,
                       ae.security_id,
                       ae.period_end,
                       ({val_col})::numeric AS forward_val,
                       ROW_NUMBER() OVER (PARTITION BY s.id ORDER BY ae.period_end ASC) AS rn_asc
                FROM analyst_estimates ae
                JOIN securities s ON s.id = ae.security_id
                WHERE s.status = 'active'
                  AND ae.period_kind = %s
                  AND ae.period_end >= CURRENT_DATE
                  AND ({val_col}) IS NOT NULL
            ),
            paired AS (
                SELECT f.ticker, f.company_id, f.security_id, f.period_end, f.forward_val,
                       (
                         SELECT ({val_col})::numeric
                         FROM analyst_estimates ae2
                         WHERE ae2.security_id = f.security_id
                           AND ae2.period_kind = %s
                           AND ae2.period_end = (f.period_end - INTERVAL '1 year')::date
                           AND ({val_col}) IS NOT NULL
                         LIMIT 1
                       ) AS prior_val
                FROM forward f
                WHERE f.rn_asc <= %s
            ),
            agg AS (
                SELECT ticker, company_id,
                       AVG(forward_val)::numeric AS forward_avg,
                       AVG(prior_val)::numeric   AS prior_avg,
                       COUNT(*) FILTER (WHERE prior_val IS NOT NULL)::int AS n_periods,
                       MIN(period_end::text) AS period_start,
                       MAX(period_end::text) AS period_end
                FROM paired
                GROUP BY ticker, company_id
            )
            SELECT ticker, company_id,
                   CASE WHEN prior_avg IS NULL OR prior_avg = 0 THEN NULL
                        ELSE (forward_avg - prior_avg) / ABS(prior_avg)
                   END::numeric AS value,
                   prior_avg AS earliest_value,
                   forward_avg AS latest_value,
                   n_periods, period_start, period_end
            FROM agg
            WHERE n_periods >= %s
            ORDER BY value {order} NULLS LAST
            LIMIT %s;
        """
        min_coverage = max(1, n_forward // 2) if n_forward >= 2 else 1
        return _exec(
            conn,
            sql,
            (period_kind, period_kind, n_forward, min_coverage, limit),
            spec,
            view_name="analyst_estimates",
        )

    raise ValueError(
        f"agg={agg!r} on estimates not yet supported. Use 'level' with a "
        "forward window or growth metric."
    )


def _resolve_forward_window(parsed: ParsedPeriod) -> tuple[int, str | None, str]:
    """Translate a parsed estimates period into (n_forward, agg_within, grain).

    grain is 'q' or 'y'. Default is quarter / 4 forward periods / avg.
    """
    if parsed.kind == "latest":
        return 1, None, "q"
    if parsed.kind == "forward_single":
        return 1, None, parsed.grain or "q"
    if parsed.kind == "forward_window":
        n = parsed.n_periods or 4
        return n, parsed.agg or "avg", parsed.grain or "q"
    raise ValueError(f"period {parsed.raw!r} not supported for estimates")


# --------------------------------------------------------------------------- #
# Valuation
# --------------------------------------------------------------------------- #


def _screen_valuation(
    conn: psycopg.Connection,
    *,
    spec: MetricSpec,
    parsed: ParsedPeriod,
    agg: str,
    sort: str,
    limit: int,
) -> list[ScreenRow]:
    order = "DESC" if sort == "desc" else "ASC"

    if agg != "level":
        raise ValueError(
            "agg='delta'/'relative_change' over valuation history not yet supported "
            "(would require trailing daily windows; defer)."
        )

    # The valuation view recomputes a multi-table TTM join over every daily row,
    # so cross-universe screens need to scope to ONE row per security via a
    # LATERAL probe — letting Postgres use the (security_id, date) PKs on
    # prices_daily and historical_market_cap rather than scanning the full
    # daily history. Empirically: <1s vs ~130s for the WINDOW form.
    if parsed.kind == "latest":
        sql = f"""
            SELECT
                latest.ticker,
                latest.security_id AS company_id,
                latest.value::numeric AS value,
                latest.value AS earliest_value,
                latest.value AS latest_value,
                1 AS n_periods,
                latest.date::text AS period_start,
                latest.date::text AS period_end
            FROM securities s,
            LATERAL (
                SELECT v.ticker, v.security_id, v.date,
                       ({spec.value_expr})::numeric AS value
                FROM {spec.view} v
                WHERE v.security_id = s.id
                  AND ({spec.value_expr}) IS NOT NULL
                ORDER BY v.date DESC
                LIMIT 1
            ) latest
            WHERE s.status = 'active'
            ORDER BY value {order} NULLS LAST
            LIMIT %s;
        """
        return _exec(conn, sql, (limit,), spec)

    if parsed.kind == "asof":
        sql = f"""
            SELECT
                latest.ticker,
                latest.security_id AS company_id,
                latest.value::numeric AS value,
                latest.value AS earliest_value,
                latest.value AS latest_value,
                1 AS n_periods,
                latest.date::text AS period_start,
                latest.date::text AS period_end
            FROM securities s,
            LATERAL (
                SELECT v.ticker, v.security_id, v.date,
                       ({spec.value_expr})::numeric AS value
                FROM {spec.view} v
                WHERE v.security_id = s.id
                  AND ({spec.value_expr}) IS NOT NULL
                  AND v.date <= %s
                ORDER BY v.date DESC
                LIMIT 1
            ) latest
            WHERE s.status = 'active'
            ORDER BY value {order} NULLS LAST
            LIMIT %s;
        """
        return _exec(conn, sql, (parsed.as_of, limit), spec)

    raise ValueError(
        f"period {parsed.raw!r} not supported for valuation. Use 'latest' or 'asof:YYYY-MM-DD'."
    )


# --------------------------------------------------------------------------- #
# Common
# --------------------------------------------------------------------------- #


def _exec(
    conn: psycopg.Connection,
    sql: str,
    params: tuple[Any, ...],
    spec: MetricSpec,
    *,
    view_name: str | None = None,
) -> list[ScreenRow]:
    rows = run_query(conn, sql=sql, params=params)
    out: list[ScreenRow] = []
    citation_view = view_name or spec.view
    for i, row in enumerate(rows, start=1):
        period_start = str(row["period_start"]) if row["period_start"] is not None else ""
        period_end = str(row["period_end"]) if row["period_end"] is not None else ""
        citation = _format_citation(citation_view, row["company_id"], period_start, period_end)
        out.append(
            ScreenRow(
                rank=i,
                ticker=row["ticker"],
                company_id=row["company_id"],
                value=row["value"],
                earliest_value=row.get("earliest_value"),
                latest_value=row.get("latest_value"),
                n_periods=int(row.get("n_periods") or 0),
                period_start=period_start,
                period_end=period_end,
                metric=spec.name,
                metric_kind=spec.kind,
                view_name=citation_view,
                citation=citation,
            )
        )
    return out


def _format_citation(view_name: str, company_id: int, period_start: str, period_end: str) -> str:
    """Citation IDs match the existing M:view:co:period[:_to_period] convention."""
    if not period_start and not period_end:
        return f"M:{view_name}:{company_id}"
    if period_start == period_end or not period_start:
        return f"M:{view_name}:{company_id}:{period_end}"
    if not period_end:
        return f"M:{view_name}:{company_id}:{period_start}"
    return f"M:{view_name}:{company_id}:{period_start}_to_{period_end}"


def universe_size(conn: psycopg.Connection, *, metric: str) -> int:
    """How many distinct companies have at least one non-null value for this metric?

    Used by tools to phrase rankings without unnecessary hedging.
    """
    if metric not in METRICS:
        return 0
    spec = METRICS[metric]
    if spec.vertical == "financials":
        return _financials_universe_count(conn, spec)
    if spec.vertical == "estimates":
        # Count securities (active) with at least one non-null forward row.
        col = spec.value_expr
        rows = run_query(
            conn,
            sql=f"""
                SELECT COUNT(DISTINCT s.id)::int AS n
                FROM analyst_estimates ae
                JOIN securities s ON s.id = ae.security_id
                WHERE s.status = 'active'
                  AND ae.period_end >= CURRENT_DATE
                  AND ({col}) IS NOT NULL;
            """,
            params=(),
        )
        return rows[0]["n"] if rows else 0
    if spec.vertical == "valuation":
        rows = run_query(
            conn,
            sql=f"""
                SELECT COUNT(DISTINCT v.security_id)::int AS n
                FROM {spec.view} v
                JOIN securities s ON s.id = v.security_id
                WHERE s.status = 'active' AND ({spec.value_expr}) IS NOT NULL;
            """,
            params=(),
        )
        return rows[0]["n"] if rows else 0
    return 0
