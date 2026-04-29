"""Financial-fact retrieval primitives.

These return raw period-scoped facts. Period-over-period comparisons (prior
values, YoY growth) are recipe concerns — the primitives only read one period
at a time so each call has a single, traceable cost.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

import psycopg

from arrow.retrieval._query import run_query
from arrow.retrieval.types import FinancialFact, SegmentFact


def get_financial_facts(
    conn: psycopg.Connection,
    *,
    company_id: int,
    fiscal_year: int,
    fiscal_quarter: int | None,
    period_type: str,
    concepts: Sequence[str] | None = None,
) -> list[FinancialFact]:
    """Return non-dimensional financial facts for a (company, period).

    ``concepts`` filters by concept name; ``None`` returns every concept.
    Segment / dimensioned facts are intentionally excluded — use
    ``get_segment_facts`` for those.
    """
    if concepts is not None and not concepts:
        return []
    sql = """
        SELECT
            id AS fact_id,
            statement,
            concept,
            value,
            unit,
            fiscal_period_label,
            period_end
        FROM financial_facts
        WHERE company_id = %s
          AND fiscal_year = %s
          AND fiscal_quarter IS NOT DISTINCT FROM %s
          AND period_type = %s
          AND dimension_type IS NULL
          AND superseded_at IS NULL
    """
    params: list = [company_id, fiscal_year, fiscal_quarter, period_type]
    if concepts is not None:
        sql += "  AND concept = ANY(%s)\n"
        params.append(list(concepts))
    sql += "ORDER BY concept;"
    rows = run_query(conn, sql=sql, params=tuple(params))
    return [FinancialFact(**row) for row in rows]


def get_segment_facts(
    conn: psycopg.Connection,
    *,
    company_id: int,
    fiscal_year: int,
    fiscal_quarter: int | None,
    period_type: str,
) -> list[SegmentFact]:
    """Return segment-revenue facts for one period.

    ``prior_value`` and ``yoy_growth`` are always ``None`` here; period
    comparisons are composed by the caller (call this primitive twice and
    merge by ``(dimension_type, dimension_key)``).
    """
    rows = run_query(
        conn,
        sql="""
            SELECT
                id AS fact_id,
                dimension_type,
                dimension_key,
                dimension_label,
                value,
                fiscal_period_label,
                period_end
            FROM financial_facts
            WHERE company_id = %s
              AND fiscal_year = %s
              AND fiscal_quarter IS NOT DISTINCT FROM %s
              AND period_type = %s
              AND statement = 'segment'
              AND concept = 'revenue'
              AND superseded_at IS NULL
            ORDER BY dimension_type, value DESC;
        """,
        params=(company_id, fiscal_year, fiscal_quarter, period_type),
    )
    return [
        SegmentFact(
            fact_id=row["fact_id"],
            dimension_type=row["dimension_type"],
            dimension_key=row["dimension_key"],
            dimension_label=row["dimension_label"],
            value=row["value"],
            prior_value=None,
            yoy_growth=None,
            fiscal_period_label=row["fiscal_period_label"],
            period_end=row["period_end"],
        )
        for row in rows
    ]


def get_segment_value_index(
    conn: psycopg.Connection,
    *,
    company_id: int,
    fiscal_year: int,
    fiscal_quarter: int | None,
    period_type: str,
) -> dict[tuple[str, str], Decimal]:
    """Slim segment lookup: ``(dimension_type, dimension_key) -> value``.

    Used by the recipe to attach prior-year values onto current-period
    SegmentFacts without paying for the full row schema twice.
    """
    rows = run_query(
        conn,
        sql="""
            SELECT
                dimension_type,
                dimension_key,
                value
            FROM financial_facts
            WHERE company_id = %s
              AND fiscal_year = %s
              AND fiscal_quarter IS NOT DISTINCT FROM %s
              AND period_type = %s
              AND statement = 'segment'
              AND concept = 'revenue'
              AND superseded_at IS NULL;
        """,
        params=(company_id, fiscal_year, fiscal_quarter, period_type),
    )
    return {
        (row["dimension_type"], row["dimension_key"]): row["value"]
        for row in rows
    }
