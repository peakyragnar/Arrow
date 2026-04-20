"""Fetch + persist FMP income-statement data for a ticker.

Calls one /income-statement endpoint with period=quarter or period=annual,
writes a raw_responses row + filesystem cache, and returns the parsed
payload alongside the raw_responses id so the caller can feed them into
the normalize layer without a DB round-trip.

Must be called inside an open transaction on `conn` (write_raw_response
assumes the caller owns the transaction).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import psycopg

from arrow.ingest.common.raw_responses import write_raw_response
from arrow.ingest.fmp.client import FMPClient
from arrow.ingest.fmp.paths import fmp_statement_path

INCOME_STATEMENT_ENDPOINT = "income-statement"

# FMP stable's default pagination returns only the last ~5 periods. Without
# an explicit limit, a "backfill" returns just the most recent years.
# 1000 comfortably exceeds any filer's available history (NVDA has ~80
# quarterly and ~28 annual filings as of 2026); FMP silently caps at its
# tier maximum, so oversize is safe.
DEFAULT_LIMIT = 1000


@dataclass(frozen=True)
class IncomeStatementFetch:
    raw_response_id: int
    rows: list[dict[str, Any]]


def fetch_income_statement(
    conn: psycopg.Connection,
    *,
    ticker: str,
    period: str,
    ingest_run_id: int,
    client: FMPClient,
    limit: int = DEFAULT_LIMIT,
) -> IncomeStatementFetch:
    """Fetch one (ticker, period). Persists raw_responses; returns id + rows.

    period: 'annual' or 'quarter'
    limit:  max rows FMP should return (default 1000 = full history)
    """
    if period not in ("annual", "quarter"):
        raise ValueError(f"period must be 'annual' or 'quarter', got {period!r}")

    params = {"symbol": ticker.upper(), "period": period, "limit": limit}
    resp = client.get(INCOME_STATEMENT_ENDPOINT, **params)
    rows = json.loads(resp.body)

    raw_id = write_raw_response(
        conn,
        ingest_run_id=ingest_run_id,
        vendor="fmp",
        endpoint=INCOME_STATEMENT_ENDPOINT,
        params=params,
        request_url=resp.url,
        http_status=resp.status,
        content_type=resp.content_type,
        response_headers=resp.headers,
        body=resp.body,
        cache_path=fmp_statement_path(INCOME_STATEMENT_ENDPOINT, ticker, period),
    )
    return IncomeStatementFetch(raw_response_id=raw_id, rows=rows)
