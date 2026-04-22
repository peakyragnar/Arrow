"""Fetch + persist FMP historical employee count for a ticker.

FMP endpoint: `historical-employee-count?symbol={TICKER}`

Returns one row per 10-K filing the filer has on EDGAR, each carrying
the filer-disclosed employee count at that fiscal year-end (10-K Item 1
`Business` / `Human Capital` disclosure). Quarterly 10-Qs do not carry
employee counts, so the series is annual-grain by construction.

Row shape (representative, FMP as of 2026-04-22):

    {
      "symbol": "NVDA",
      "cik": "0001045810",
      "acceptanceTime": "2026-02-25 16:42:19",
      "periodOfReport": "2026-01-25",
      "companyName": "NVIDIA Corporation",
      "formType": "10-K",
      "filingDate": "2026-02-25",
      "employeeCount": 42000,
      "source": "https://www.sec.gov/Archives/..."
    }

Used by metric 18 (Revenue per Employee). The loader writes one
`financial_facts` row per payload row with:
    statement  = 'metrics'
    concept    = 'total_employees'
    period_end = periodOfReport
    period_type = 'annual'
    unit       = 'employees'
    published_at = filingDate
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import psycopg

from arrow.ingest.common.raw_responses import write_raw_response
from arrow.ingest.fmp.client import FMPClient
from arrow.ingest.fmp.paths import fmp_per_ticker_path

EMPLOYEE_COUNT_ENDPOINT = "historical-employee-count"
DEFAULT_LIMIT = 1000


@dataclass(frozen=True)
class EmployeeCountFetch:
    raw_response_id: int
    rows: list[dict[str, Any]]


def fetch_employee_count(
    conn: psycopg.Connection,
    *,
    ticker: str,
    ingest_run_id: int,
    client: FMPClient,
    limit: int = DEFAULT_LIMIT,
) -> EmployeeCountFetch:
    """Fetch one ticker's full 10-K employee-count history.

    Persists the raw payload to raw_responses + filesystem cache.
    Returns the raw_response id plus parsed rows.
    """
    params = {"symbol": ticker.upper(), "limit": limit}
    resp = client.get(EMPLOYEE_COUNT_ENDPOINT, **params)
    rows = json.loads(resp.body)

    raw_id = write_raw_response(
        conn,
        ingest_run_id=ingest_run_id,
        vendor="fmp",
        endpoint=EMPLOYEE_COUNT_ENDPOINT,
        params=params,
        request_url=resp.url,
        http_status=resp.status,
        content_type=resp.content_type,
        response_headers=resp.headers,
        body=resp.body,
        cache_path=fmp_per_ticker_path(EMPLOYEE_COUNT_ENDPOINT, ticker),
    )
    return EmployeeCountFetch(raw_response_id=raw_id, rows=rows)
