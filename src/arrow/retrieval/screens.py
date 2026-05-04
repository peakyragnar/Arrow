"""Universe-level retrieval helpers (non-screen).

The cross-company ranking primitives now live in
``arrow.retrieval.screener`` (generic over a registry of metrics) and
``arrow.retrieval.registry`` (the metric registry). This module retains
helpers that don't fit the registry shape:

- ``list_companies``  — list every company in the universe
- ``get_latest_roic`` — most recent ROIC value at-or-before a date
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import psycopg

from arrow.retrieval._query import run_query
from arrow.retrieval.types import Company


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
