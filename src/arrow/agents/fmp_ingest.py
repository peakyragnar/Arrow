"""FMP baseline ingest orchestration.

Per docs/architecture/repository_flow.md, agents/ is the layer allowed to
orchestrate across ingest + normalize + db. This module owns:

  backfill_fmp_statements(conn, tickers, since_date=...)

Per ticker:
  1. Look up the companies row (must be seeded).
  2. Round `since_date` forward to the first fiscal year whose end falls
     on/after it, so complete fiscal years are ingested (not partials).
  3. Load IS quarter + annual. Layer-1 subtotal ties enforced per row.
  4. Load BS quarter + annual. Layer-1 BS balance identities enforced
     per row; subtotal-component drift soft-flags and still loads.
  5. Load CF quarter + annual. Layer-1 CF ties + cash roll-forward
     enforced per row.

Default historical ingest stops there. SEC/XBRL comparison, amendment
work, and other audit logic run outside this path.

Any Layer-1 HARD failure aborts the ticker's work, rolls back that
transaction, and marks the ingest_run failed with structured error details.

`since_date` default 2021-01-01 limits the validated window. Older
history is out-of-scope until the baseline FMP window expands.

backfill_fmp_is is retained as an alias for the old IS-only caller path;
new callers should use backfill_fmp_statements.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import psycopg

from arrow.ingest.common.runs import close_failed, close_succeeded, open_run
from arrow.ingest.fmp.balance_sheet import fetch_balance_sheet
from arrow.ingest.fmp.cash_flow import fetch_cash_flow
from arrow.ingest.fmp.client import FMPClient
from arrow.ingest.fmp.income_statement import fetch_income_statement
from arrow.normalize.financials.load import (
    BSVerificationFailed,
    CFVerificationFailed,
    FiscalYearMismatch,
    LoadResult,
    VerificationFailed,
    load_fmp_bs_rows,
    load_fmp_cf_rows,
    load_fmp_is_rows,
)
from arrow.normalize.periods.derive import (
    max_fiscal_year_for_until_date,
    min_fiscal_year_for_since_date,
)

DEFAULT_SINCE_DATE = date(2021, 1, 1)


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


def _load_is_period(
    conn: psycopg.Connection,
    *,
    company: CompanyRow,
    period: str,
    min_fiscal_year: int,
    max_fiscal_year: int | None,
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
            min_fiscal_year=min_fiscal_year,
            max_fiscal_year=max_fiscal_year,
        )


def _load_bs_period(
    conn: psycopg.Connection,
    *,
    company: CompanyRow,
    period: str,
    min_fiscal_year: int,
    max_fiscal_year: int | None,
    ingest_run_id: int,
    client: FMPClient,
) -> LoadResult:
    with conn.transaction():
        fetched = fetch_balance_sheet(
            conn,
            ticker=company.ticker,
            period=period,
            ingest_run_id=ingest_run_id,
            client=client,
        )
        return load_fmp_bs_rows(
            conn,
            company_id=company.id,
            company_fiscal_year_end_md=company.fiscal_year_end_md,
            rows=fetched.rows,
            source_raw_response_id=fetched.raw_response_id,
            ingest_run_id=ingest_run_id,
            min_fiscal_year=min_fiscal_year,
            max_fiscal_year=max_fiscal_year,
        )


def _load_cf_period(
    conn: psycopg.Connection,
    *,
    company: CompanyRow,
    period: str,
    min_fiscal_year: int,
    max_fiscal_year: int | None,
    ingest_run_id: int,
    client: FMPClient,
) -> LoadResult:
    with conn.transaction():
        fetched = fetch_cash_flow(
            conn,
            ticker=company.ticker,
            period=period,
            ingest_run_id=ingest_run_id,
            client=client,
        )
        return load_fmp_cf_rows(
            conn,
            company_id=company.id,
            company_fiscal_year_end_md=company.fiscal_year_end_md,
            rows=fetched.rows,
            source_raw_response_id=fetched.raw_response_id,
            ingest_run_id=ingest_run_id,
            min_fiscal_year=min_fiscal_year,
            max_fiscal_year=max_fiscal_year,
        )


def backfill_fmp_statements(
    conn: psycopg.Connection,
    tickers: list[str],
    *,
    since_date: date = DEFAULT_SINCE_DATE,
    until_date: date | None = None,
) -> dict[str, Any]:
    """Backfill baseline FMP income-statement, balance-sheet, and cash-flow data.

    Layer 1 IS   — per-row subtotal ties (inline during IS load).
    Layer 1 BS   — per-row balance identity hard gate; subtotal-component drift
                   soft-flags inline during BS load.
    Layer 1 CF   — per-row subtotal ties + cash roll-forward (inline during CF load).

    `until_date`: if set, only ingest fiscal years whose nominal FY-end
    falls on or before this date. Use to exclude filings FMP has
    known-bad data for (see `fmp_mapping.md` § 10) — set to the last
    safe FY-end, e.g. 2025-06-01 to stop at FY2025 for filers with
    FY-end in early calendar year.
    """
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
        # IS
        "is_facts_written": 0,
        "is_facts_superseded": 0,
        # BS
        "bs_facts_written": 0,
        "bs_facts_superseded": 0,
        # CF
        "cf_facts_written": 0,
        "cf_facts_superseded": 0,
        # Soft-tie data_quality_flags.
        # Non-blocking; row is still loaded. Analyst reviews with
        # scripts/review_flags.py.
        "bs_flags_written": 0,
        "cf_flags_written": 0,
    }

    try:
        for ticker in tickers:
            company = _get_company(conn, ticker)
            ticker_min_fy = min_fiscal_year_for_since_date(
                since_date, company.fiscal_year_end_md
            )
            counts["min_fiscal_year_by_ticker"][ticker.upper()] = ticker_min_fy
            ticker_max_fy = (
                max_fiscal_year_for_until_date(until_date, company.fiscal_year_end_md)
                if until_date
                else None
            )
            counts["max_fiscal_year_by_ticker"][ticker.upper()] = ticker_max_fy

            # --- IS ingest (Layer 1 IS inline) ---
            for period in ("quarter", "annual"):
                result = _load_is_period(
                    conn,
                    company=company,
                    period=period,
                    min_fiscal_year=ticker_min_fy,
                    max_fiscal_year=ticker_max_fy,
                    ingest_run_id=run_id,
                    client=client,
                )
                counts["raw_responses"] += 1
                counts["rows_processed"] += result.rows_processed
                counts["is_facts_written"] += result.facts_written
                counts["is_facts_superseded"] += result.facts_superseded

            # --- BS ingest (Layer 1 BS inline) ---
            for period in ("quarter", "annual"):
                result = _load_bs_period(
                    conn,
                    company=company,
                    period=period,
                    min_fiscal_year=ticker_min_fy,
                    max_fiscal_year=ticker_max_fy,
                    ingest_run_id=run_id,
                    client=client,
                )
                counts["raw_responses"] += 1
                counts["rows_processed"] += result.rows_processed
                counts["bs_facts_written"] += result.facts_written
                counts["bs_facts_superseded"] += result.facts_superseded
                counts["bs_flags_written"] += result.flags_written

            # --- CF ingest (Layer 1 CF inline) ---
            for period in ("quarter", "annual"):
                result = _load_cf_period(
                    conn,
                    company=company,
                    period=period,
                    min_fiscal_year=ticker_min_fy,
                    max_fiscal_year=ticker_max_fy,
                    ingest_run_id=run_id,
                    client=client,
                )
                counts["raw_responses"] += 1
                counts["rows_processed"] += result.rows_processed
                counts["cf_facts_written"] += result.facts_written
                counts["cf_facts_superseded"] += result.facts_superseded
                counts["cf_flags_written"] += result.flags_written

    except VerificationFailed as e:
        close_failed(
            conn, run_id, error_message=str(e),
            error_details={
                "kind": "is_verification_failed",
                "period_label": e.period_label,
                "failed_ties": [
                    {"tie": f.tie, "filer": str(f.filer), "computed": str(f.computed),
                     "delta": str(f.delta), "tolerance": str(f.tolerance)}
                    for f in e.failures
                ],
            },
        )
        raise
    except BSVerificationFailed as e:
        close_failed(
            conn, run_id, error_message=str(e),
            error_details={
                "kind": "bs_verification_failed",
                "period_label": e.period_label,
                "failed_ties": [
                    {"tie": f.tie, "filer": str(f.filer), "computed": str(f.computed),
                     "delta": str(f.delta), "tolerance": str(f.tolerance)}
                    for f in e.failures
                ],
            },
        )
        raise
    except CFVerificationFailed as e:
        close_failed(
            conn, run_id, error_message=str(e),
            error_details={
                "kind": "cf_verification_failed",
                "period_label": e.period_label,
                "failed_ties": [
                    {"tie": f.tie, "filer": str(f.filer), "computed": str(f.computed),
                     "delta": str(f.delta), "tolerance": str(f.tolerance)}
                    for f in e.failures
                ],
            },
        )
        raise
    except FiscalYearMismatch as e:
        close_failed(
            conn, run_id, error_message=str(e),
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
            conn, run_id, error_message=str(e),
            error_details={"kind": type(e).__name__},
        )
        raise

    close_succeeded(conn, run_id, counts=counts)
    counts["ingest_run_id"] = run_id
    return counts


# Backward-compatible alias for callers still using the IS-only name.
# Does the full IS + BS flow; the old behavior where only IS was loaded
# is not preserved — all callers should migrate to backfill_fmp_statements.
backfill_fmp_is = backfill_fmp_statements
