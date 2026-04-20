"""FMP ingest orchestration — crosses ingest + normalize + reconcile layers.

Per docs/architecture/repository_flow.md, agents/ is the layer allowed to
orchestrate across ingest + normalize + reconcile + db. This module owns:

  backfill_fmp_is(conn, tickers, since_date)

For each ticker:
  1. Look up the companies row (must be seeded).
  2. Fetch FMP /income-statement for period=quarter and period=annual.
     Write raw_responses + financial_facts per period, with per-row
     Layer-1 subtotal-tie enforcement (HARD BLOCK).
  3. After all FMP rows are loaded, run Layer-3 period arithmetic
     (Q1+Q2+Q3+Q4 ≈ FY) across the validated window. HARD BLOCK on
     mismatch.
  4. Fetch SEC XBRL companyfacts, write raw_responses.
  5. Run cross-source reconciliation (FMP vs XBRL) on every current
     IS fact. HARD BLOCK on divergence.

Any failure at any layer rolls the whole ticker's work back and marks
the ingest_run failed with structured error details. Rows only land in
the DB after all three layers pass.

`since_date` (default 2021-01-01) limits the validated window. Periods
older than this are skipped at load time. Historical data beyond this
window should be treated as out-of-scope until Build Order step 9.5
scales the reconciliation formally.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import psycopg

from arrow.ingest.common.http import HttpClient
from arrow.ingest.common.runs import close_failed, close_succeeded, open_run
from arrow.ingest.fmp.client import FMPClient
from arrow.ingest.fmp.income_statement import fetch_income_statement
from arrow.ingest.sec.bootstrap import SEC_RATE_LIMIT, SEC_USER_AGENT
from arrow.ingest.sec.company_facts import fetch_company_facts
from arrow.normalize.financials.load import (
    EXTRACTION_VERSION,
    FiscalYearMismatch,
    LoadResult,
    VerificationFailed,
    load_fmp_is_rows,
)
from arrow.normalize.financials.verify_period_arithmetic import (
    PeriodArithmeticFailure,
    verify_period_arithmetic,
)
from arrow.normalize.periods.derive import min_fiscal_year_for_since_date
from arrow.reconcile.fmp_vs_xbrl import AnchorCheckResult, reconcile_anchors

# Default scope: 5-year validated window. Change per ticker by passing
# `since_date` to backfill_fmp_is(...).
DEFAULT_SINCE_DATE = date(2021, 1, 1)


@dataclass(frozen=True)
class CompanyRow:
    id: int
    cik: int
    ticker: str
    fiscal_year_end_md: str


class CompanyNotSeeded(RuntimeError):
    pass


class PeriodArithmeticViolation(RuntimeError):
    def __init__(self, failures: list[PeriodArithmeticFailure]) -> None:
        self.failures = failures
        summary = "; ".join(
            f"{f.concept} FY{f.fiscal_year}: Q-sum={f.quarters_sum}, FY={f.annual}, delta={f.delta}"
            for f in failures[:5]
        )
        super().__init__(
            f"Layer-3 period arithmetic failed on {len(failures)} (concept, FY) pair(s): {summary}"
        )


class XBRLDivergenceFailed(RuntimeError):
    def __init__(self, result: AnchorCheckResult) -> None:
        self.result = result
        summary = "; ".join(
            f"{d.concept} {d.period_end} ({d.period_type}): "
            f"fmp={d.fmp_value}, xbrl={d.xbrl_value} "
            f"({d.derivation}, tag={d.xbrl_tag})"
            for d in result.divergences[:5]
        )
        super().__init__(
            f"FMP vs SEC XBRL diverged on {len(result.divergences)} anchor(s): {summary}"
        )


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


def _load_one_period(
    conn: psycopg.Connection,
    *,
    company: CompanyRow,
    period: str,
    min_fiscal_year: int,
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
        )


def _run_layer3(
    conn: psycopg.Connection, company: CompanyRow
) -> list[PeriodArithmeticFailure]:
    """Layer 3: Q1+Q2+Q3+Q4 ≈ FY per flow bucket, per fiscal year."""
    return verify_period_arithmetic(
        conn, company_id=company.id, extraction_version=EXTRACTION_VERSION
    )


def _run_xbrl_anchors(
    conn: psycopg.Connection,
    *,
    company: CompanyRow,
    ingest_run_id: int,
) -> AnchorCheckResult:
    """Fetch SEC XBRL companyfacts and run anchor check against stored IS anchors."""
    http = HttpClient(user_agent=SEC_USER_AGENT, rate_limit=SEC_RATE_LIMIT)
    with conn.transaction():
        fetched = fetch_company_facts(
            conn, cik=company.cik, ingest_run_id=ingest_run_id, http=http
        )
        return reconcile_anchors(
            conn,
            company_id=company.id,
            extraction_version=EXTRACTION_VERSION,
            companyfacts=fetched.payload,
        )


def backfill_fmp_is(
    conn: psycopg.Connection,
    tickers: list[str],
    *,
    since_date: date = DEFAULT_SINCE_DATE,
) -> dict[str, Any]:
    """Backfill FMP income-statement data with full validation stack.

    Layer 1 (per-row subtotal ties): enforced inline during load.
    Layer 3 (period arithmetic):     enforced after all FMP rows loaded.
    Layer 5 (cross-source XBRL):     enforced after Layer 3 passes.

    Any failure aborts the run, rolls back that layer's transaction, and
    records structured error_details. Rows only become "current" when all
    three layers pass.
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
        "min_fiscal_year_by_ticker": {},
        "raw_responses": 0,
        "financial_facts_written": 0,
        "financial_facts_superseded": 0,
        "rows_processed": 0,
        "layer3_identities_checked": 0,
        "anchors_stored": 0,
        "anchors_checked": 0,
        "anchors_matched": 0,
        "anchors_not_in_xbrl": [],
    }

    try:
        for ticker in tickers:
            company = _get_company(conn, ticker)
            # Round the calendar since_date forward to the first fiscal
            # year whose end falls on/after it — so we get complete
            # fiscal years, never partials at the boundary.
            ticker_min_fy = min_fiscal_year_for_since_date(
                since_date, company.fiscal_year_end_md
            )
            counts["min_fiscal_year_by_ticker"][ticker.upper()] = ticker_min_fy

            # Layer 1 (per-row): enforced inline.
            for period in ("quarter", "annual"):
                result = _load_one_period(
                    conn,
                    company=company,
                    period=period,
                    min_fiscal_year=ticker_min_fy,
                    ingest_run_id=run_id,
                    client=client,
                )
                counts["raw_responses"] += 1
                counts["financial_facts_written"] += result.facts_written
                counts["financial_facts_superseded"] += result.facts_superseded
                counts["rows_processed"] += result.rows_processed

            # Layer 3: period arithmetic across the validated window.
            l3_failures = _run_layer3(conn, company)
            if l3_failures:
                raise PeriodArithmeticViolation(l3_failures)
            # Count identities we could have checked; a crude proxy.
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT count(*) FROM (
                        SELECT concept, fiscal_year,
                               count(*) FILTER (WHERE period_type='quarter') AS qn,
                               count(*) FILTER (WHERE period_type='annual') AS an
                        FROM financial_facts
                        WHERE company_id = %s AND superseded_at IS NULL
                          AND statement = 'income_statement'
                          AND extraction_version = %s
                        GROUP BY concept, fiscal_year
                        HAVING count(*) FILTER (WHERE period_type='quarter') = 4
                           AND count(*) FILTER (WHERE period_type='annual') = 1
                    ) t;
                    """,
                    (company.id, EXTRACTION_VERSION),
                )
                counts["layer3_identities_checked"] += cur.fetchone()[0]

            # Layer 5: anchor check against SEC XBRL.
            xbrl_result = _run_xbrl_anchors(
                conn, company=company, ingest_run_id=run_id
            )
            counts["raw_responses"] += 1  # the companyfacts raw_response
            counts["anchors_stored"] += xbrl_result.anchors_with_fmp_stored
            counts["anchors_checked"] += xbrl_result.anchors_checked
            counts["anchors_matched"] += xbrl_result.anchors_matched
            for concept, pe, pt in xbrl_result.anchors_not_in_xbrl:
                counts["anchors_not_in_xbrl"].append({
                    "concept": concept,
                    "period_end": pe.isoformat(),
                    "period_type": pt,
                })
            if xbrl_result.divergences:
                raise XBRLDivergenceFailed(xbrl_result)

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
    except PeriodArithmeticViolation as e:
        close_failed(
            conn,
            run_id,
            error_message=str(e),
            error_details={
                "kind": "period_arithmetic_violation",
                "failures": [
                    {
                        "concept": f.concept,
                        "fiscal_year": f.fiscal_year,
                        "quarters_sum": str(f.quarters_sum),
                        "annual": str(f.annual),
                        "delta": str(f.delta),
                        "tolerance": str(f.tolerance),
                    }
                    for f in e.failures
                ],
            },
        )
        raise
    except XBRLDivergenceFailed as e:
        close_failed(
            conn,
            run_id,
            error_message=str(e),
            error_details={
                "kind": "xbrl_divergence",
                "anchors_checked": e.result.anchors_checked,
                "anchors_matched": e.result.anchors_matched,
                "divergences": [
                    {
                        "concept": d.concept,
                        "period_end": d.period_end.isoformat(),
                        "period_type": d.period_type,
                        "fiscal_year": d.fiscal_year,
                        "fiscal_quarter": d.fiscal_quarter,
                        "fmp_value": str(d.fmp_value),
                        "xbrl_value": str(d.xbrl_value),
                        "xbrl_tag": d.xbrl_tag,
                        "xbrl_filed": d.xbrl_filed,
                        "xbrl_accn": d.xbrl_accn,
                        "derivation": d.derivation,
                        "delta": str(d.delta),
                        "tolerance": str(d.tolerance),
                    }
                    for d in e.result.divergences
                ],
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
