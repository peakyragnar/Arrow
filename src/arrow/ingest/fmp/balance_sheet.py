"""Fetch + persist FMP balance-sheet data for a ticker.

Mirror of income_statement.py for the BS endpoint. Calls
    /balance-sheet-statement?symbol={TICKER}&period={annual|quarter}&limit=1000
and writes one raw_responses row + filesystem cache. Returns the parsed
payload alongside the raw_responses id so the normalize layer can work
directly from memory.

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

BALANCE_SHEET_ENDPOINT = "balance-sheet-statement"
DEFAULT_LIMIT = 1000


@dataclass(frozen=True)
class BalanceSheetFetch:
    raw_response_id: int
    rows: list[dict[str, Any]]


def fetch_balance_sheet(
    conn: psycopg.Connection,
    *,
    ticker: str,
    period: str,
    ingest_run_id: int,
    client: FMPClient,
    limit: int = DEFAULT_LIMIT,
) -> BalanceSheetFetch:
    """Fetch one (ticker, period). Persists raw_responses; returns id + rows.

    period: 'annual' or 'quarter'
    limit:  max rows FMP should return (default 1000 = full history)
    """
    if period not in ("annual", "quarter"):
        raise ValueError(f"period must be 'annual' or 'quarter', got {period!r}")

    params = {"symbol": ticker.upper(), "period": period, "limit": limit}
    resp = client.get(BALANCE_SHEET_ENDPOINT, **params)
    rows = json.loads(resp.body)

    raw_id = write_raw_response(
        conn,
        ingest_run_id=ingest_run_id,
        vendor="fmp",
        endpoint=BALANCE_SHEET_ENDPOINT,
        params=params,
        request_url=resp.url,
        http_status=resp.status,
        content_type=resp.content_type,
        response_headers=resp.headers,
        body=resp.body,
        cache_path=fmp_statement_path(BALANCE_SHEET_ENDPOINT, ticker, period),
    )
    return BalanceSheetFetch(raw_response_id=raw_id, rows=rows)
