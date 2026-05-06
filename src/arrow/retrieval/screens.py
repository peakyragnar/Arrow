"""Universe-level retrieval helpers (non-screen).

The cross-company ranking primitives live in ``arrow.retrieval.screener``
(generic over the metric registry). Single-ticker, multi-metric retrieval
lives in ``arrow.retrieval.multi_metric``. This module retains helpers
that don't fit either shape — currently just ``list_companies``.
"""

from __future__ import annotations

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
