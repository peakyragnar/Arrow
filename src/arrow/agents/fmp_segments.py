"""FMP revenue segmentation ingest orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import psycopg

from arrow.ingest.common.runs import close_failed, close_succeeded, open_run
from arrow.ingest.fmp.client import FMPClient
from arrow.ingest.fmp.segments import (
    GEOGRAPHIC_SEGMENT_ENDPOINT,
    PRODUCT_SEGMENT_ENDPOINT,
    fetch_revenue_segments,
)
from arrow.normalize.financials.segments_load import load_fmp_segment_rows
from arrow.normalize.periods.derive import (
    max_fiscal_year_for_until_date,
    min_fiscal_year_for_since_date,
)

DEFAULT_SINCE_DATE = date(2016, 1, 1)
SEGMENT_ENDPOINTS = (PRODUCT_SEGMENT_ENDPOINT, GEOGRAPHIC_SEGMENT_ENDPOINT)


@dataclass(frozen=True)
class CompanyRow:
    id: int
    cik: int
    ticker: str
    fiscal_year_end_md: str


class CompanyNotSeeded(RuntimeError):
    pass


def _get_company(conn: psycopg.Connection, ticker: str) -> CompanyRow:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, cik, ticker, fiscal_year_end_md FROM companies WHERE ticker = %s;",
            (ticker.upper(),),
        )
        row = cur.fetchone()
    if row is None:
        raise CompanyNotSeeded(
            f"{ticker} not in companies — run seed_companies.py {ticker} first"
        )
    return CompanyRow(id=row[0], cik=row[1], ticker=row[2], fiscal_year_end_md=row[3])


def backfill_fmp_segments(
    conn: psycopg.Connection,
    tickers: list[str],
    *,
    since_date: date = DEFAULT_SINCE_DATE,
    until_date: date | None = None,
) -> dict[str, Any]:
    """Backfill FMP product and geographic revenue segmentation."""
    run_id = open_run(
        conn,
        run_kind="manual",
        vendor="fmp",
        ticker_scope=[t.upper() for t in tickers],
    )
    client = FMPClient()

    counts: dict[str, Any] = {
        "since_date": since_date.isoformat(),
        "until_date": until_date.isoformat() if until_date else None,
        "min_fiscal_year_by_ticker": {},
        "max_fiscal_year_by_ticker": {},
        "raw_responses": 0,
        "rows_processed": 0,
        "segments_processed": 0,
        "facts_written": 0,
        "facts_superseded": 0,
    }

    try:
        for ticker in tickers:
            company = _get_company(conn, ticker)
            ticker_min_fy = min_fiscal_year_for_since_date(
                since_date, company.fiscal_year_end_md
            )
            ticker_max_fy = (
                max_fiscal_year_for_until_date(until_date, company.fiscal_year_end_md)
                if until_date
                else None
            )
            counts["min_fiscal_year_by_ticker"][ticker.upper()] = ticker_min_fy
            counts["max_fiscal_year_by_ticker"][ticker.upper()] = ticker_max_fy

            with conn.transaction():
                for endpoint in SEGMENT_ENDPOINTS:
                    for period in ("quarter", "annual"):
                        fetched = fetch_revenue_segments(
                            conn,
                            ticker=company.ticker,
                            endpoint=endpoint,
                            period=period,
                            ingest_run_id=run_id,
                            client=client,
                        )
                        result = load_fmp_segment_rows(
                            conn,
                            company_id=company.id,
                            company_fiscal_year_end_md=company.fiscal_year_end_md,
                            endpoint=fetched.endpoint,
                            rows=fetched.rows,
                            source_raw_response_id=fetched.raw_response_id,
                            ingest_run_id=run_id,
                            min_fiscal_year=ticker_min_fy,
                            max_fiscal_year=ticker_max_fy,
                        )
                        counts["raw_responses"] += 1
                        counts["rows_processed"] += result.rows_processed
                        counts["segments_processed"] += result.segments_processed
                        counts["facts_written"] += result.facts_written
                        counts["facts_superseded"] += result.facts_superseded

    except Exception as e:
        close_failed(
            conn,
            run_id,
            error_message=str(e),
            error_details={"kind": type(e).__name__},
        )
        raise

    close_succeeded(conn, run_id, counts=counts)
    counts["ingest_run_id"] = run_id
    return counts
