"""FMP employee-count ingest orchestration.

Per `docs/architecture/repository_flow.md`, the agents/ layer is the one
allowed to orchestrate across ingest + normalize + db. This module owns:

    backfill_fmp_employees(conn, tickers)

For each ticker:
    1. Look up the companies row (must be seeded).
    2. Fetch FMP `historical-employee-count` payload (full history, one
       row per 10-K filing).
    3. Load each 10-K row into `financial_facts` as a
       `statement='metrics', concept='total_employees'` annual fact.

No Layer-1 ties apply (employee count is a single-value disclosure).
Supersession uses the same partial-unique-index contract as the
statement loaders.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import psycopg

from arrow.ingest.common.runs import close_failed, close_succeeded, open_run
from arrow.ingest.fmp.client import FMPClient
from arrow.ingest.fmp.employees import fetch_employee_count
from arrow.normalize.metrics.employees_load import load_fmp_employee_rows


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


def backfill_fmp_employees(
    conn: psycopg.Connection,
    tickers: list[str],
) -> dict[str, Any]:
    """Fetch + load FMP historical-employee-count for each ticker."""
    run_id = open_run(
        conn,
        run_kind="manual",
        vendor="fmp",
        ticker_scope=[t.upper() for t in tickers],
    )
    client = FMPClient()

    counts: dict[str, Any] = {
        "raw_responses": 0,
        "rows_processed": 0,
        "facts_written": 0,
        "facts_superseded": 0,
    }

    try:
        for ticker in tickers:
            company = _get_company(conn, ticker)

            with conn.transaction():
                fetched = fetch_employee_count(
                    conn,
                    ticker=company.ticker,
                    ingest_run_id=run_id,
                    client=client,
                )
                result = load_fmp_employee_rows(
                    conn,
                    company_id=company.id,
                    company_fiscal_year_end_md=company.fiscal_year_end_md,
                    rows=fetched.rows,
                    source_raw_response_id=fetched.raw_response_id,
                    ingest_run_id=run_id,
                )
                counts["raw_responses"] += 1
                counts["rows_processed"] += result.rows_processed
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
