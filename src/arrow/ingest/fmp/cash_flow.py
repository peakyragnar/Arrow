"""Fetch + persist FMP cash-flow data for a ticker.

Mirror of income_statement.py / balance_sheet.py for the CF endpoint.
Calls
    /cash-flow-statement?symbol={TICKER}&period={annual|quarter}&limit=1000
and writes one raw_responses row + filesystem cache. Returns parsed rows.

FMP returns DISCRETE quarterly CF values (not YTD), empirically confirmed
against NVDA — so no YTD→discrete subtraction is needed at load time.
See periods.md § 7 for the YTD/discrete contract we rely on.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import psycopg

from arrow.ingest.common.raw_responses import write_raw_response
from arrow.ingest.fmp.client import FMPClient
from arrow.ingest.fmp.paths import fmp_statement_path

CASH_FLOW_ENDPOINT = "cash-flow-statement"
DEFAULT_LIMIT = 1000


@dataclass(frozen=True)
class CashFlowFetch:
    raw_response_id: int
    rows: list[dict[str, Any]]


def fetch_cash_flow(
    conn: psycopg.Connection,
    *,
    ticker: str,
    period: str,
    ingest_run_id: int,
    client: FMPClient,
    limit: int = DEFAULT_LIMIT,
) -> CashFlowFetch:
    """Fetch one (ticker, period). Persists raw_responses; returns id + rows.

    period: 'annual' or 'quarter'
    limit:  max rows FMP should return (default 1000 = full history)
    """
    if period not in ("annual", "quarter"):
        raise ValueError(f"period must be 'annual' or 'quarter', got {period!r}")

    params = {"symbol": ticker.upper(), "period": period, "limit": limit}
    resp = client.get(CASH_FLOW_ENDPOINT, **params)
    rows = json.loads(resp.body)

    raw_id = write_raw_response(
        conn,
        ingest_run_id=ingest_run_id,
        vendor="fmp",
        endpoint=CASH_FLOW_ENDPOINT,
        params=params,
        request_url=resp.url,
        http_status=resp.status,
        content_type=resp.content_type,
        response_headers=resp.headers,
        body=resp.body,
        cache_path=fmp_statement_path(CASH_FLOW_ENDPOINT, ticker, period),
    )
    return CashFlowFetch(raw_response_id=raw_id, rows=rows)
