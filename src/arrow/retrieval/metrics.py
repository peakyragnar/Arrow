"""Period-metrics retrieval primitive over the metric views.

Wraps ``v_metrics_fy`` and ``v_metrics_q`` so the analyst surface never
constructs metric SQL inline. Formula conventions live in those views; this
primitive is just the typed read.
"""

from __future__ import annotations

import psycopg

from arrow.retrieval._query import run_query
from arrow.retrieval.types import FiscalMetric


_QUARTERLY_SQL = """
    SELECT
        ticker,
        company_id,
        fiscal_year,
        fiscal_period_label,
        period_end AS fy_end,
        revenue AS revenue_fy,
        gross_margin AS gross_margin_fy,
        operating_margin AS operating_margin_fy,
        cfo AS cfo_fy,
        capital_expenditures AS capital_expenditures_fy,
        cfo + capital_expenditures AS fcf_fy
    FROM v_metrics_q
    WHERE company_id = %s
      AND fiscal_year = %s
      AND fiscal_quarter = %s
    ORDER BY period_end DESC
    LIMIT 1;
"""

_ANNUAL_SQL = """
    SELECT
        ticker,
        company_id,
        fiscal_year,
        fiscal_period_label,
        fy_end,
        revenue_fy,
        gross_margin_fy,
        operating_margin_fy,
        cfo_fy,
        capital_expenditures_fy,
        cfo_fy + capital_expenditures_fy AS fcf_fy
    FROM v_metrics_fy
    WHERE company_id = %s
      AND fiscal_year = %s
    ORDER BY fy_end DESC
    LIMIT 1;
"""


def metrics_view_name(period_type: str) -> str:
    return "v_metrics_q" if period_type == "quarter" else "v_metrics_fy"


def get_metrics(
    conn: psycopg.Connection,
    *,
    company_id: int,
    fiscal_year: int,
    fiscal_quarter: int | None,
    period_type: str,
) -> FiscalMetric | None:
    """Return the metric row for one (company, period). None if missing."""
    if period_type == "quarter":
        if fiscal_quarter is None:
            return None
        rows = run_query(
            conn,
            sql=_QUARTERLY_SQL,
            params=(company_id, fiscal_year, fiscal_quarter),
        )
    else:
        rows = run_query(
            conn,
            sql=_ANNUAL_SQL,
            params=(company_id, fiscal_year),
        )
    if not rows:
        return None
    return FiscalMetric(**rows[0])


_QUARTERLY_SERIES_SQL = """
    SELECT
        m.ticker,
        m.company_id,
        m.fiscal_year,
        m.fiscal_period_label,
        m.period_end,
        m.revenue,
        m.gross_margin,
        m.operating_margin,
        m.net_margin,
        m.cfo,
        m.capital_expenditures,
        m.cfo + m.capital_expenditures AS fcf,
        da.value AS dna_cf,
        -- Capex/D&A ratio. Capex is stored negative; take ABS so the ratio
        -- reads as "spend / replacement-rate." > 1.5x sustained = structural
        -- cash compression; ~1.0x = replacement-only spending.
        CASE
            WHEN da.value IS NULL OR da.value = 0 THEN NULL
            WHEN m.capital_expenditures IS NULL THEN NULL
            ELSE ABS(m.capital_expenditures) / da.value
        END AS capex_to_dna_ratio
    FROM v_metrics_q m
    LEFT JOIN v_ff_current da
      ON da.company_id = m.company_id
     AND da.period_end = m.period_end
     AND da.statement = 'cash_flow'
     AND da.concept = 'dna_cf'
     AND da.period_type = 'quarter'
    WHERE m.company_id = %s
    ORDER BY m.period_end DESC
    LIMIT %s;
"""


def get_quarterly_metrics_series(
    conn: psycopg.Connection,
    *,
    company_id: int,
    n: int = 8,
) -> list[dict]:
    """Last N quarters of metrics for one company. Returns row dicts in
    period_end DESC order. Includes dna_cf (D&A from cash flow statement)
    and capex_to_dna_ratio so capex-cycle vs. structural-compression
    questions are answerable in one tool call."""
    return run_query(
        conn,
        sql=_QUARTERLY_SERIES_SQL,
        params=(company_id, int(n)),
    )
