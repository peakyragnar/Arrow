"""FMP ingest orchestration — crosses ingest + normalize layers.

Per docs/architecture/repository_flow.md, agents/ is the layer allowed to
orchestrate across ingest + normalize + db. This module owns:

  backfill_fmp_is(conn, tickers)
    For each ticker:
      - Look up the companies row (must be seeded).
      - Fetch /income-statement?period=quarter, write raw_responses + facts.
      - Fetch /income-statement?period=annual, write raw_responses + facts.
    One ingest_run per invocation; one transaction per (ticker, period_type).

Scripts (`scripts/backfill_fmp_is.py`) wrap this function with argv parsing.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import psycopg

from arrow.ingest.common.runs import close_failed, close_succeeded, open_run
from arrow.ingest.fmp.client import FMPClient
from arrow.ingest.fmp.income_statement import fetch_income_statement
from arrow.normalize.financials.load import (
    FiscalYearMismatch,
    LoadResult,
    VerificationFailed,
    load_fmp_is_rows,
)


@dataclass(frozen=True)
class CompanyRow:
    id: int
    ticker: str
    fiscal_year_end_md: str


class CompanyNotSeeded(RuntimeError):
    pass


def _get_company(conn: psycopg.Connection, ticker: str) -> CompanyRow:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, ticker, fiscal_year_end_md FROM companies WHERE ticker = %s;",
            (ticker.upper(),),
        )
        row = cur.fetchone()
    if row is None:
        raise CompanyNotSeeded(
            f"{ticker} not in companies — run seed_companies.py {ticker} first"
        )
    return CompanyRow(id=row[0], ticker=row[1], fiscal_year_end_md=row[2])


def _backfill_one_period(
    conn: psycopg.Connection,
    *,
    company: CompanyRow,
    period: str,
    ingest_run_id: int,
    client: FMPClient,
) -> LoadResult:
    with conn.transaction():
        fetched = fetch_income_statement(
            conn,
            ticker=company.ticker,
            period=period,
            ingest_run_id=ingest_run_id,
            client=client,
        )
        return load_fmp_is_rows(
            conn,
            company_id=company.id,
            company_fiscal_year_end_md=company.fiscal_year_end_md,
            rows=fetched.rows,
            source_raw_response_id=fetched.raw_response_id,
            ingest_run_id=ingest_run_id,
        )


def backfill_fmp_is(
    conn: psycopg.Connection,
    tickers: list[str],
) -> dict[str, Any]:
    """Backfill FMP income-statement data for one or more tickers.

    Companies must be seeded first (scripts/seed_companies.py). Opens an
    ingest_run, processes every (ticker, period) pair, closes the run with
    a status reflecting outcomes. Raises on verification or integrity
    failures after marking the run failed.
    """
    run_id = open_run(
        conn,
        run_kind="manual",
        vendor="fmp",
        ticker_scope=[t.upper() for t in tickers],
    )
    client = FMPClient()

    counts: dict[str, Any] = {
        "raw_responses": 0,
        "financial_facts_written": 0,
        "financial_facts_superseded": 0,
        "rows_processed": 0,
    }

    try:
        for ticker in tickers:
            company = _get_company(conn, ticker)
            for period in ("quarter", "annual"):
                result = _backfill_one_period(
                    conn,
                    company=company,
                    period=period,
                    ingest_run_id=run_id,
                    client=client,
                )
                counts["raw_responses"] += 1
                counts["financial_facts_written"] += result.facts_written
                counts["financial_facts_superseded"] += result.facts_superseded
                counts["rows_processed"] += result.rows_processed
    except VerificationFailed as e:
        close_failed(
            conn,
            run_id,
            error_message=str(e),
            error_details={
                "kind": "verification_failed",
                "period_label": e.period_label,
                "failed_ties": [
                    {
                        "tie": f.tie,
                        "filer": str(f.filer),
                        "computed": str(f.computed),
                        "delta": str(f.delta),
                        "tolerance": str(f.tolerance),
                    }
                    for f in e.failures
                ],
            },
        )
        raise
    except FiscalYearMismatch as e:
        close_failed(
            conn,
            run_id,
            error_message=str(e),
            error_details={
                "kind": "fiscal_year_mismatch",
                "period_end": e.period_end.isoformat(),
                "fmp_fiscal_year": e.fmp_fiscal_year,
                "derived_fiscal_year": e.derived_fiscal_year,
            },
        )
        raise
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
