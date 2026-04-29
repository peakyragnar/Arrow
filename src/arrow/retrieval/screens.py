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
