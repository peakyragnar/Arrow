"""Company-resolution retrieval primitive."""

from __future__ import annotations

import psycopg

from arrow.retrieval._query import run_query
from arrow.retrieval.types import Company


def get_company(
    conn: psycopg.Connection,
    *,
    company_id: int,
) -> Company | None:
    """Look up a single company by id. Returns None if not found."""
    rows = run_query(
        conn,
        sql="""
            SELECT id, ticker, name, cik, fiscal_year_end_md
            FROM companies
            WHERE id = %s;
        """,
        params=(company_id,),
    )
    if not rows:
        return None
    return Company(**rows[0])


def resolve_company_by_ticker(
    conn: psycopg.Connection,
    *,
    ticker_candidates: list[str],
) -> list[Company]:
    """Return companies matching any of the given uppercased tickers.

    Caller is responsible for raising on zero or ambiguous matches; this
    primitive only retrieves.
    """
    if not ticker_candidates:
        return []
    rows = run_query(
        conn,
        sql="""
            SELECT id, ticker, name, cik, fiscal_year_end_md
            FROM companies
            WHERE upper(ticker) = ANY(%s)
            ORDER BY id;
        """,
        params=([t.upper() for t in ticker_candidates],),
    )
    return [Company(**row) for row in rows]


def suggest_tickers_near(
    conn: psycopg.Connection,
    *,
    ticker: str,
    limit: int = 5,
) -> list[Company]:
    """Suggest near-miss companies when a ticker doesn't resolve exactly.

    Bidirectional prefix match — finds rows where the DB ticker starts with
    the typed string (e.g. AXT → AXTI) OR the typed string starts with a DB
    ticker (e.g. AXTII → AXTI). Common case: user drops or adds one letter.
    Caller decides whether to surface the suggestion or just use it.
    """
    if not ticker:
        return []
    t = ticker.upper()
    rows = run_query(
        conn,
        sql="""
            SELECT id, ticker, name, cik, fiscal_year_end_md
            FROM companies
            WHERE upper(ticker) <> %s
              AND (
                    upper(ticker) LIKE %s || '%%'
                 OR %s LIKE upper(ticker) || '%%'
              )
            ORDER BY length(ticker), ticker
            LIMIT %s;
        """,
        params=(t, t, t, limit),
    )
    return [Company(**row) for row in rows]
