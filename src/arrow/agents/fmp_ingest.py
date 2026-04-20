"""FMP ingest orchestration — crosses ingest + normalize + reconcile layers.

Per docs/architecture/repository_flow.md, agents/ is the layer allowed to
orchestrate across ingest + normalize + reconcile + db. This module owns:

  backfill_fmp_statements(conn, tickers, since_date=...)

Per ticker:
  1. Look up the companies row (must be seeded).
  2. Round `since_date` forward to the first fiscal year whose end falls
     on/after it, so complete fiscal years are ingested (not partials).
  3. Load IS quarter + annual. Layer-1 subtotal ties enforced per row.
  4. Load BS quarter + annual. Layer-1 BS ties + balance identity
     (total_assets == total_liab + total_equity) enforced per row.
  5. Layer 3 period arithmetic (Q1+Q2+Q3+Q4 ≈ FY) on IS flows.
     (BS stocks exempt — snapshots, not flows.)
  6. Fetch SEC XBRL companyfacts (one payload per company).
  7. Layer 5 anchor cross-check: IS anchors (direct + Q4-derived) and
     BS anchors (instant facts, matched by end date).

Any failure at any layer aborts the ticker's work, rolls back that
transaction, and marks the ingest_run failed with structured error
details. Rows only become "current" when all enabled layers pass.

`since_date` default 2021-01-01 limits the validated window. Older
history is out-of-scope until Build Order step 9.5 scales the
reconciliation formally.

backfill_fmp_is is retained as an alias for the old IS-only caller path;
new callers should use backfill_fmp_statements.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import psycopg

from arrow.ingest.common.http import HttpClient
from arrow.ingest.common.runs import close_failed, close_succeeded, open_run
from arrow.ingest.fmp.balance_sheet import fetch_balance_sheet
from arrow.ingest.fmp.client import FMPClient
from arrow.ingest.fmp.income_statement import fetch_income_statement
from arrow.ingest.sec.bootstrap import SEC_RATE_LIMIT, SEC_USER_AGENT
from arrow.ingest.sec.company_facts import fetch_company_facts
from arrow.normalize.financials.load import (
    BS_EXTRACTION_VERSION,
    BSVerificationFailed,
    FiscalYearMismatch,
    IS_EXTRACTION_VERSION,
    LoadResult,
    VerificationFailed,
    load_fmp_bs_rows,
    load_fmp_is_rows,
)
from arrow.normalize.financials.verify_period_arithmetic import (
    PeriodArithmeticFailure,
    verify_period_arithmetic,
)
from arrow.normalize.periods.derive import min_fiscal_year_for_since_date
from arrow.reconcile.fmp_vs_xbrl import (
    AnchorCheckResult,
    reconcile_bs_anchors,
    reconcile_is_anchors,
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
    def __init__(self, result: AnchorCheckResult, statement: str) -> None:
        self.result = result
        self.statement = statement
        summary = "; ".join(
            f"{d.concept} {d.period_end} ({d.period_type}): "
            f"fmp={d.fmp_value}, xbrl={d.xbrl_value} "
            f"({d.derivation}, tag={d.xbrl_tag})"
            for d in result.divergences[:5]
        )
        super().__init__(
            f"FMP vs SEC XBRL diverged on {len(result.divergences)} {statement} anchor(s): {summary}"
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


def _load_is_period(
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


def _load_bs_period(
    conn: psycopg.Connection,
    *,
    company: CompanyRow,
    period: str,
    min_fiscal_year: int,
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
        )


def _run_layer3_is(conn: psycopg.Connection, company: CompanyRow) -> list[PeriodArithmeticFailure]:
    return verify_period_arithmetic(
        conn, company_id=company.id, extraction_version=IS_EXTRACTION_VERSION
    )


def _fetch_xbrl_payload(
    conn: psycopg.Connection, *, company: CompanyRow, ingest_run_id: int
) -> dict[str, Any]:
    """Fetch SEC XBRL companyfacts inside its own transaction. Returns parsed payload."""
    http = HttpClient(user_agent=SEC_USER_AGENT, rate_limit=SEC_RATE_LIMIT)
    with conn.transaction():
        fetched = fetch_company_facts(
            conn, cik=company.cik, ingest_run_id=ingest_run_id, http=http
        )
        return fetched.payload


def _count_layer3_identities(conn: psycopg.Connection, *, company_id: int) -> int:
    """Count (concept, fiscal_year) pairs with all 5 values (Q1-Q4 + FY)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FROM (
                SELECT concept, fiscal_year
                FROM financial_facts
                WHERE company_id = %s AND superseded_at IS NULL
                  AND statement = 'income_statement'
                  AND extraction_version = %s
                GROUP BY concept, fiscal_year
                HAVING count(*) FILTER (WHERE period_type='quarter') = 4
                   AND count(*) FILTER (WHERE period_type='annual') = 1
            ) t;
            """,
            (company_id, IS_EXTRACTION_VERSION),
        )
        return cur.fetchone()[0]


def _xbrl_failure_details(
    result: AnchorCheckResult, statement: str
) -> dict[str, Any]:
    return {
        "kind": "xbrl_divergence",
        "statement": statement,
        "anchors_checked": result.anchors_checked,
        "anchors_matched": result.anchors_matched,
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
            for d in result.divergences
        ],
    }


def backfill_fmp_statements(
    conn: psycopg.Connection,
    tickers: list[str],
    *,
    since_date: date = DEFAULT_SINCE_DATE,
) -> dict[str, Any]:
    """Backfill FMP income-statement + balance-sheet data with full validation.

    Layer 1 IS   — per-row subtotal ties (inline during IS load).
    Layer 1 BS   — per-row subtotal ties + balance identity (inline during BS load).
    Layer 3      — Q1+Q2+Q3+Q4 ≈ FY for IS flows.
    Layer 5 IS   — top-line IS anchors vs SEC XBRL (direct + Q4 derivation).
    Layer 5 BS   — top-line BS anchors vs SEC XBRL (instant facts).
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
        "rows_processed": 0,
        # IS
        "is_facts_written": 0,
        "is_facts_superseded": 0,
        "layer3_identities_checked": 0,
        "is_anchors_stored": 0,
        "is_anchors_checked": 0,
        "is_anchors_matched": 0,
        "is_anchors_not_in_xbrl": [],
        # BS
        "bs_facts_written": 0,
        "bs_facts_superseded": 0,
        "bs_anchors_stored": 0,
        "bs_anchors_checked": 0,
        "bs_anchors_matched": 0,
        "bs_anchors_not_in_xbrl": [],
    }

    try:
        for ticker in tickers:
            company = _get_company(conn, ticker)
            ticker_min_fy = min_fiscal_year_for_since_date(
                since_date, company.fiscal_year_end_md
            )
            counts["min_fiscal_year_by_ticker"][ticker.upper()] = ticker_min_fy

            # --- IS ingest (Layer 1 IS inline) ---
            for period in ("quarter", "annual"):
                result = _load_is_period(
                    conn,
                    company=company,
                    period=period,
                    min_fiscal_year=ticker_min_fy,
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
                    ingest_run_id=run_id,
                    client=client,
                )
                counts["raw_responses"] += 1
                counts["rows_processed"] += result.rows_processed
                counts["bs_facts_written"] += result.facts_written
                counts["bs_facts_superseded"] += result.facts_superseded

            # --- Layer 3: IS period arithmetic ---
            l3_failures = _run_layer3_is(conn, company)
            if l3_failures:
                raise PeriodArithmeticViolation(l3_failures)
            counts["layer3_identities_checked"] += _count_layer3_identities(
                conn, company_id=company.id
            )

            # --- Layer 5: fetch XBRL once, reconcile IS + BS ---
            xbrl_payload = _fetch_xbrl_payload(
                conn, company=company, ingest_run_id=run_id
            )
            counts["raw_responses"] += 1

            is_result = reconcile_is_anchors(
                conn,
                company_id=company.id,
                extraction_version=IS_EXTRACTION_VERSION,
                companyfacts=xbrl_payload,
            )
            counts["is_anchors_stored"] += is_result.anchors_with_fmp_stored
            counts["is_anchors_checked"] += is_result.anchors_checked
            counts["is_anchors_matched"] += is_result.anchors_matched
            for concept, pe, pt in is_result.anchors_not_in_xbrl:
                counts["is_anchors_not_in_xbrl"].append({
                    "concept": concept, "period_end": pe.isoformat(), "period_type": pt,
                })
            if is_result.divergences:
                raise XBRLDivergenceFailed(is_result, statement="income_statement")

            bs_result = reconcile_bs_anchors(
                conn,
                company_id=company.id,
                extraction_version=BS_EXTRACTION_VERSION,
                companyfacts=xbrl_payload,
            )
            counts["bs_anchors_stored"] += bs_result.anchors_with_fmp_stored
            counts["bs_anchors_checked"] += bs_result.anchors_checked
            counts["bs_anchors_matched"] += bs_result.anchors_matched
            for concept, pe, pt in bs_result.anchors_not_in_xbrl:
                counts["bs_anchors_not_in_xbrl"].append({
                    "concept": concept, "period_end": pe.isoformat(), "period_type": pt,
                })
            if bs_result.divergences:
                raise XBRLDivergenceFailed(bs_result, statement="balance_sheet")

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
    except PeriodArithmeticViolation as e:
        close_failed(
            conn, run_id, error_message=str(e),
            error_details={
                "kind": "period_arithmetic_violation",
                "failures": [
                    {"concept": f.concept, "fiscal_year": f.fiscal_year,
                     "quarters_sum": str(f.quarters_sum), "annual": str(f.annual),
                     "delta": str(f.delta), "tolerance": str(f.tolerance)}
                    for f in e.failures
                ],
            },
        )
        raise
    except XBRLDivergenceFailed as e:
        close_failed(
            conn, run_id, error_message=str(e),
            error_details=_xbrl_failure_details(e.result, e.statement),
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
