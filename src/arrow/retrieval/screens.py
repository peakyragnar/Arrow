"""Cross-company screening primitives.

These read the existing metric views and rank companies by a chosen metric
over a recent-N-period window. Used by the analyst agent to answer
"highest/lowest X" questions across the universe.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import psycopg

from arrow.retrieval._query import run_query
from arrow.retrieval.types import Company


@dataclass(frozen=True)
class ScreenRow:
    ticker: str
    company_id: int
    value: Decimal | None
    n_periods: int
    period_start: str
    period_end: str
    metric: str
    view_name: str


# metric_name -> (view, value_expr, recency_column, periods_per_year, value_kind)
# value_kind is metadata for the caller's renderer: 'money', 'ratio', 'count'.
_METRIC_DEFS: dict[str, tuple[str, str, str, int, str]] = {
    "revenue": ("v_metrics_fy", "revenue_fy", "fiscal_year", 1, "money"),
    "gross_margin": ("v_metrics_fy", "gross_margin_fy", "fiscal_year", 1, "ratio"),
    "operating_margin": ("v_metrics_fy", "operating_margin_fy", "fiscal_year", 1, "ratio"),
    "net_margin": ("v_metrics_fy", "net_margin_fy", "fiscal_year", 1, "ratio"),
    "fcf": ("v_metrics_fy", "cfo_fy + capital_expenditures_fy", "fiscal_year", 1, "money"),
    "roic": ("v_metrics_roic", "roic", "period_end", 4, "ratio"),
}


def supported_metrics() -> list[str]:
    return sorted(_METRIC_DEFS.keys())


def metric_value_kind(metric: str) -> str:
    return _METRIC_DEFS[metric][4]


def screen_companies_by_metric(
    conn: psycopg.Connection,
    *,
    metric: str,
    n_years: int = 1,
    limit: int = 10,
    sort_desc: bool = True,
    min_coverage: int | None = None,
) -> list[ScreenRow]:
    """Rank companies by ``metric`` averaged over the most recent ``n_years`` years.

    For ROIC (quarterly view), the window is ``n_years * 4`` most recent
    rows per company. For annual-view metrics, it's the ``n_years`` most
    recent annual rows.

    ``min_coverage`` requires at least this many recent periods per
    ticker to be included; defaults to ``n_years`` (annual) or
    ``n_years * 4 - 1`` (quarterly, allowing one missing quarter).
    """
    if metric not in _METRIC_DEFS:
        raise ValueError(
            f"unsupported metric '{metric}'. Supported: {', '.join(sorted(_METRIC_DEFS))}"
        )
    view, value_expr, recency_col, periods_per_year, _kind = _METRIC_DEFS[metric]
    period_count = max(1, n_years) * periods_per_year
    if min_coverage is None:
        min_coverage = period_count if periods_per_year == 1 else max(1, period_count - 1)
    limit = max(1, min(limit, 50))
    order = "DESC" if sort_desc else "ASC"

    sql = f"""
        WITH ranked AS (
            SELECT
                ticker,
                company_id,
                {recency_col} AS recency_key,
                ({value_expr})::numeric AS value
            FROM {view}
            WHERE ({value_expr}) IS NOT NULL
        ),
        windowed AS (
            SELECT
                ticker,
                company_id,
                recency_key,
                value,
                ROW_NUMBER() OVER (
                    PARTITION BY company_id
                    ORDER BY recency_key DESC
                ) AS rn
            FROM ranked
        )
        SELECT
            ticker,
            company_id,
            AVG(value)::numeric AS value,
            COUNT(*)::int AS n_periods,
            MIN(recency_key)::text AS period_start,
            MAX(recency_key)::text AS period_end
        FROM windowed
        WHERE rn <= %s
        GROUP BY ticker, company_id
        HAVING COUNT(*) >= %s
        ORDER BY AVG(value) {order} NULLS LAST
        LIMIT %s;
    """
    rows = run_query(
        conn,
        sql=sql,
        params=(period_count, min_coverage, limit),
    )
    return [
        ScreenRow(
            ticker=row["ticker"],
            company_id=row["company_id"],
            value=row["value"],
            n_periods=row["n_periods"],
            period_start=str(row["period_start"]),
            period_end=str(row["period_end"]),
            metric=metric,
            view_name=view,
        )
        for row in rows
    ]


@dataclass(frozen=True)
class TrajectoryRow:
    """One row of the trajectory screen — a company's recent-vs-prior delta.

    Computed as: avg(metric over recent third of window)
              − avg(metric over earliest third of window).
    Middle third is treated as transition and excluded so the comparison
    is between two distinct cohorts of periods, not adjacent periods.
    """

    ticker: str
    company_id: int
    metric: str
    view_name: str

    earliest_value: Decimal | None    # avg of first 1/3 of window
    latest_value: Decimal | None      # avg of last 1/3 of window
    delta: Decimal | None             # latest - earliest
    relative_change: Decimal | None   # delta / abs(earliest); NULL if earliest near 0

    earliest_period: str
    latest_period: str
    n_periods: int


def screen_companies_by_trajectory(
    conn: psycopg.Connection,
    *,
    metric: str,
    window_periods: int = 12,
    sort_desc: bool = True,
    limit: int = 10,
    min_coverage: int | None = None,
    basis: str = "auto",
) -> list[TrajectoryRow]:
    """Rank companies by the *change* in ``metric`` from earliest-third
    to latest-third of the window. Answers "fastest improving / declining
    X" — distinct from ``screen_companies_by_metric`` which ranks by
    average level.

    Window: ``window_periods`` most recent periods per ticker. Default
    12 — for quarterly metrics (ROIC) that's 3 years; for annual
    metrics it's a longer window.

    Trajectory math:
        earliest_value = AVG(metric) over the first 1/3 of the window
        latest_value   = AVG(metric) over the last 1/3
        delta          = latest_value - earliest_value
        relative_change = delta / |earliest_value|   (NULL if denom near 0)

    ``basis`` controls the rank order:
      - 'auto'      (default) — ratio metrics rank by absolute delta (pp);
                     money metrics rank by relative_change (%)
      - 'absolute'  — always rank by delta
      - 'relative'  — always rank by relative_change

    Companies with fewer than ``min_coverage`` periods (default
    ``window_periods - 1``) are excluded — partial histories produce
    misleading deltas.
    """
    if metric not in _METRIC_DEFS:
        raise ValueError(
            f"unsupported metric '{metric}'. Supported: {', '.join(sorted(_METRIC_DEFS))}"
        )
    if basis not in {"auto", "absolute", "relative"}:
        raise ValueError(f"basis must be 'auto'|'absolute'|'relative', got {basis!r}")

    view, value_expr, recency_col, _periods_per_year, kind = _METRIC_DEFS[metric]

    if basis == "auto":
        rank_by = "absolute" if kind == "ratio" else "relative"
    else:
        rank_by = basis

    if min_coverage is None:
        # Need full window to compute a reliable trajectory; allow one missing.
        min_coverage = max(3, window_periods - 1)
    limit = max(1, min(limit, 50))
    order = "DESC" if sort_desc else "ASC"

    # Bucket boundaries: first third vs last third of the window. Integer
    # arithmetic — for window_periods=12: bucket size = 4 (rn_desc 1..4 = recent,
    # 5..8 = middle, 9..12 = earliest).
    bucket_size = max(1, window_periods // 3)

    rank_expr = (
        "(latest_value - earliest_value)"
        if rank_by == "absolute"
        else "CASE WHEN earliest_value IS NULL OR earliest_value = 0 THEN NULL "
             "ELSE (latest_value - earliest_value) / ABS(earliest_value) END"
    )

    sql = f"""
        WITH base AS (
            SELECT
                ticker,
                company_id,
                {recency_col} AS recency_key,
                ({value_expr})::numeric AS value
            FROM {view}
            WHERE ({value_expr}) IS NOT NULL
        ),
        windowed AS (
            SELECT
                ticker, company_id, recency_key, value,
                ROW_NUMBER() OVER (
                    PARTITION BY company_id
                    ORDER BY recency_key DESC
                ) AS rn_desc
            FROM base
        ),
        bucketed AS (
            SELECT
                ticker, company_id, recency_key, value,
                CASE
                    WHEN rn_desc <= %s                         THEN 'recent'
                    WHEN rn_desc > %s - %s AND rn_desc <= %s   THEN 'early'
                    ELSE 'middle'
                END AS bucket
            FROM windowed
            WHERE rn_desc <= %s
        ),
        agg AS (
            SELECT
                ticker, company_id,
                AVG(value) FILTER (WHERE bucket = 'recent') AS latest_value,
                AVG(value) FILTER (WHERE bucket = 'early')  AS earliest_value,
                COUNT(*)                                    AS n_periods,
                MIN(recency_key)                            AS earliest_period,
                MAX(recency_key)                            AS latest_period
            FROM bucketed
            GROUP BY ticker, company_id
            HAVING COUNT(*) >= %s
        )
        SELECT
            ticker, company_id,
            earliest_value, latest_value,
            (latest_value - earliest_value) AS delta,
            CASE WHEN earliest_value IS NULL OR earliest_value = 0 THEN NULL
                 ELSE (latest_value - earliest_value) / ABS(earliest_value)
            END AS relative_change,
            earliest_period::text, latest_period::text, n_periods
        FROM agg
        WHERE latest_value IS NOT NULL AND earliest_value IS NOT NULL
        ORDER BY {rank_expr} {order} NULLS LAST
        LIMIT %s;
    """
    rows = run_query(
        conn,
        sql=sql,
        params=(
            bucket_size,                         # rn_desc <= bucket_size  (recent)
            window_periods, bucket_size,         # > window-bucket
            window_periods,                      # <= window
            window_periods,                      # rn_desc <= window
            min_coverage,
            limit,
        ),
    )
    return [
        TrajectoryRow(
            ticker=row["ticker"],
            company_id=row["company_id"],
            metric=metric,
            view_name=view,
            earliest_value=row["earliest_value"],
            latest_value=row["latest_value"],
            delta=row["delta"],
            relative_change=row["relative_change"],
            earliest_period=row["earliest_period"],
            latest_period=row["latest_period"],
            n_periods=row["n_periods"],
        )
        for row in rows
    ]


def count_universe_for_metric(conn: psycopg.Connection, *, metric: str) -> int:
    """How many distinct tickers have at least one non-null value for this metric?

    This is the size of the universe that ``screen_companies_by_metric``
    actually ranks across (modulo coverage thresholds). The synthesis model
    uses this to phrase rankings without unnecessary hedging.
    """
    if metric not in _METRIC_DEFS:
        return 0
    view, value_expr, _recency, _ppy, _kind = _METRIC_DEFS[metric]
    rows = run_query(
        conn,
        sql=f"SELECT COUNT(DISTINCT company_id)::int AS n FROM {view} WHERE ({value_expr}) IS NOT NULL;",
        params=(),
    )
    return rows[0]["n"] if rows else 0


def list_companies(conn: psycopg.Connection) -> list[Company]:
    """Return every company in the universe ordered by ticker."""
    rows = run_query(
        conn,
        sql="""
            SELECT id, ticker, name, cik, fiscal_year_end_md
            FROM companies
            ORDER BY ticker;
        """,
        params=(),
    )
    return [Company(**row) for row in rows]


def get_latest_roic(
    conn: psycopg.Connection,
    *,
    company_id: int,
    on_or_before: Any | None = None,
) -> tuple[Decimal | None, str | None]:
    """Most recent ROIC value for a company at-or-before a date.

    Used to attach ROIC onto an annual ``get_metrics`` result keyed by the
    metric's ``fy_end``. Returns ``(roic, period_end_iso)`` or ``(None, None)``.
    """
    if on_or_before is None:
        rows = run_query(
            conn,
            sql="""
                SELECT roic, period_end
                FROM v_metrics_roic
                WHERE company_id = %s AND roic IS NOT NULL
                ORDER BY period_end DESC
                LIMIT 1;
            """,
            params=(company_id,),
        )
    else:
        rows = run_query(
            conn,
            sql="""
                SELECT roic, period_end
                FROM v_metrics_roic
                WHERE company_id = %s
                  AND roic IS NOT NULL
                  AND period_end <= %s
                ORDER BY period_end DESC
                LIMIT 1;
            """,
            params=(company_id, on_or_before),
        )
    if not rows:
        return None, None
    return rows[0]["roic"], str(rows[0]["period_end"])
