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
from arrow.ingest.fmp.cash_flow import fetch_cash_flow
from arrow.ingest.fmp.client import FMPClient
from arrow.ingest.fmp.income_statement import fetch_income_statement
from arrow.ingest.sec.bootstrap import SEC_RATE_LIMIT, SEC_USER_AGENT
from arrow.ingest.sec.company_facts import fetch_company_facts
from arrow.normalize.financials.load import (
    BS_EXTRACTION_VERSION,
    BSVerificationFailed,
    CF_EXTRACTION_VERSION,
    CFVerificationFailed,
    FiscalYearMismatch,
    IS_EXTRACTION_VERSION,
    LoadResult,
    VerificationFailed,
    load_fmp_bs_rows,
    load_fmp_cf_rows,
    load_fmp_is_rows,
)
from arrow.normalize.financials.verify_cross_statement import (
    CrossStatementFailure,
    verify_cross_statement_ties,
)
from arrow.normalize.financials.verify_period_arithmetic import (
    PeriodArithmeticFailure,
    verify_period_arithmetic,
)
from arrow.normalize.periods.derive import (
    max_fiscal_year_for_until_date,
    min_fiscal_year_for_since_date,
)
from arrow.reconcile.fmp_vs_xbrl import (
    AnchorCheckResult,
    reconcile_bs_anchors,
    reconcile_cf_anchors,
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
    def __init__(
        self, failures: list[PeriodArithmeticFailure], statement: str
    ) -> None:
        self.failures = failures
        self.statement = statement
        summary = "; ".join(
            f"{f.concept} FY{f.fiscal_year}: Q-sum={f.quarters_sum}, FY={f.annual}, delta={f.delta}"
            for f in failures[:5]
        )
        super().__init__(
            f"Layer-3 period arithmetic failed on {len(failures)} "
            f"({statement}, concept, FY) pair(s): {summary}"
        )


class CrossStatementViolation(RuntimeError):
    """Layer 2: IS, BS, CF don't cohere."""
    def __init__(self, failures: list[CrossStatementFailure]) -> None:
        self.failures = failures
        summary = "; ".join(
            f"{f.tie} @ {f.period_end} ({f.period_type}): "
            f"lhs={f.lhs_value}, rhs={f.rhs_value}, delta={f.delta}"
            for f in failures[:5]
        )
        super().__init__(
            f"Layer-2 cross-statement tie failed on {len(failures)} row(s): {summary}"
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


def _run_layer3(
    conn: psycopg.Connection,
    company: CompanyRow,
    *,
    statement: str,
    extraction_version: str,
) -> list[PeriodArithmeticFailure]:
    return verify_period_arithmetic(
        conn, company_id=company.id,
        extraction_version=extraction_version,
        statement=statement,
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


def _write_layer2_flags(
    conn: psycopg.Connection,
    *,
    company_id: int,
    failures: list[CrossStatementFailure],
    ingest_run_id: int,
) -> None:
    """Write each Layer 2 cross-statement failure as a data_quality_flag.

    These catch vendor-internal inconsistency (FMP IS vs FMP CF disagreement
    on the same net_income), cash/restricted-cash classification drift, etc.
    The data still loads; analyst reviews flags and resolves via manual
    supersession if they care for their specific analysis.
    """
    from decimal import Decimal
    for f in failures:
        ref = max(abs(f.lhs_value), abs(f.rhs_value))
        if ref == 0:
            severity = "investigate"
        else:
            pct = abs(f.delta) / ref
            if pct < Decimal("0.01"):
                severity = "informational"
            elif pct < Decimal("0.10"):
                severity = "warning"
            else:
                severity = "investigate"
        reason = (
            f"Layer 2 cross-statement tie failed: {f.tie}. "
            f"LHS={f.lhs_value:,.0f}, RHS={f.rhs_value:,.0f}, "
            f"delta={f.delta:,.0f} (tolerance {f.tolerance:,.0f}). "
            f"Typically caused by vendor-internal inconsistency (FMP's IS "
            f"and CF endpoints reporting different pre-NCI net income) or "
            f"cash/restricted-cash classification drift. FMP values retained; "
            f"analyst review needed if this concept matters for your analysis."
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO data_quality_flags (
                    company_id, statement, concept, fiscal_year, fiscal_quarter,
                    period_end, period_type,
                    flag_type, severity,
                    expected_value, computed_value, delta, tolerance,
                    reason, source_run_id
                ) VALUES (
                    %s, 'cross_statement', %s, %s, %s,
                    %s, %s,
                    'layer2_cross_statement', %s,
                    %s, %s, %s, %s,
                    %s, %s
                );
                """,
                (
                    company_id, f.tie, f.fiscal_year, f.fiscal_quarter,
                    f.period_end, f.period_type,
                    severity,
                    f.lhs_value, f.rhs_value, f.delta, f.tolerance,
                    reason, ingest_run_id,
                ),
            )


def _write_layer5_flags(
    conn: psycopg.Connection,
    *,
    company_id: int,
    statement: str,
    divergences: list,
    ingest_run_id: int,
) -> None:
    """Write each Layer 5 XBRL anchor divergence as a data_quality_flag.

    Divergences mean FMP's stored value disagrees with SEC XBRL's latest-filed
    value for the same (concept, period). Typically FMP hasn't picked up a
    comparative-period restatement. XBRL value is often more authoritative
    (since it's SEC's own filing record), but analyst confirms.
    """
    from decimal import Decimal
    for d in divergences:
        ref = max(abs(d.fmp_value), abs(d.xbrl_value))
        if ref == 0:
            severity = "investigate"
        else:
            pct = abs(d.delta) / ref
            if pct < Decimal("0.01"):
                severity = "informational"
            elif pct < Decimal("0.10"):
                severity = "warning"
            else:
                severity = "investigate"
        reason = (
            f"Layer 5 anchor divergence: FMP stores {d.fmp_value:,.0f} but "
            f"SEC XBRL ({d.xbrl_tag}, accn {d.xbrl_accn}, filed {d.xbrl_filed}) "
            f"reports {d.xbrl_value:,.0f}. Derivation: {d.derivation}. "
            f"Delta = {d.delta:,.0f} (tolerance {d.tolerance:,.0f}). "
            f"Typically FMP missed a comparative-period restatement; "
            f"XBRL value is usually authoritative. Analyst verifies."
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO data_quality_flags (
                    company_id, statement, concept, fiscal_year, fiscal_quarter,
                    period_end, period_type,
                    flag_type, severity,
                    expected_value, computed_value, delta, tolerance, suggested_value,
                    reason, source_run_id
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s,
                    'layer5_xbrl_anchor', %s,
                    %s, %s, %s, %s, %s,
                    %s, %s
                );
                """,
                (
                    company_id, statement, d.concept, d.fiscal_year, d.fiscal_quarter,
                    d.period_end, d.period_type,
                    severity,
                    d.fmp_value, d.xbrl_value, d.delta, d.tolerance, d.xbrl_value,
                    reason, ingest_run_id,
                ),
            )


def _count_cf_periods(conn: psycopg.Connection, *, company_id: int) -> int:
    """Count distinct (period_end, period_type) pairs in stored CF data
    — upper bound on Layer 2 cross-statement ties we can check."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(DISTINCT (period_end, period_type))
            FROM financial_facts
            WHERE company_id = %s AND statement = 'cash_flow'
              AND extraction_version = %s AND superseded_at IS NULL;
            """,
            (company_id, CF_EXTRACTION_VERSION),
        )
        return cur.fetchone()[0]


def _count_layer3_identities(
    conn: psycopg.Connection,
    *,
    company_id: int,
    statement: str,
    extraction_version: str,
) -> int:
    """Count (concept, fiscal_year) pairs with all 5 values (Q1-Q4 + FY)
    for a given statement / extraction_version."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FROM (
                SELECT concept, fiscal_year
                FROM financial_facts
                WHERE company_id = %s AND superseded_at IS NULL
                  AND statement = %s
                  AND extraction_version = %s
                GROUP BY concept, fiscal_year
                HAVING count(*) FILTER (WHERE period_type='quarter') = 4
                   AND count(*) FILTER (WHERE period_type='annual') = 1
            ) t;
            """,
            (company_id, statement, extraction_version),
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
    until_date: date | None = None,
) -> dict[str, Any]:
    """Backfill FMP income-statement + balance-sheet data with full validation.

    Layer 1 IS   — per-row subtotal ties (inline during IS load).
    Layer 1 BS   — per-row subtotal ties + balance identity (inline during BS load).
    Layer 3      — Q1+Q2+Q3+Q4 ≈ FY for IS flows.
    Layer 5 IS   — top-line IS anchors vs SEC XBRL (direct + Q4 derivation).
    Layer 5 BS   — top-line BS anchors vs SEC XBRL (instant facts).

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
        "is_layer3_identities_checked": 0,
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
        # CF
        "cf_facts_written": 0,
        "cf_facts_superseded": 0,
        "cf_layer3_identities_checked": 0,
        "cf_anchors_stored": 0,
        "cf_anchors_checked": 0,
        "cf_anchors_matched": 0,
        "cf_anchors_not_in_xbrl": [],
        # Layer 2 (cross-statement)
        "cross_statement_ties_checked": 0,
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

            # --- Layer 3: period arithmetic on IS + CF flow buckets ---
            # If Layer 3 fails, invoke the Phase-1.5 amendment-detect agent
            # before giving up. It detects the "amendment-within-regular-filing"
            # pattern (e.g., DELL FY25 10-K restating FY24 Q1-Q4) and applies
            # XBRL-sourced supersessions if and only if all rules hold — see
            # docs/research/amendment_phase_1_5_design.md.
            is_l3 = _run_layer3(
                conn, company,
                statement="income_statement",
                extraction_version=IS_EXTRACTION_VERSION,
            )
            cf_l3 = _run_layer3(
                conn, company,
                statement="cash_flow",
                extraction_version=CF_EXTRACTION_VERSION,
            )

            if is_l3 or cf_l3:
                # Layer 3 is a soft gate. The amendment agent attempts XBRL
                # supersession for clean cases; remaining anomalies are written
                # as rows in data_quality_flags for analyst review. The agent
                # NEVER raises — ingest always proceeds past Layer 3.
                from arrow.agents.amendment_detect import detect_and_apply_amendments
                amend_result = detect_and_apply_amendments(
                    conn, company_id=company.id, company_cik=company.cik,
                    ingest_run_id=run_id,
                )
                counts.setdefault("amendment_supersessions", 0)
                counts["amendment_supersessions"] += len(amend_result.supersessions_applied)
                counts.setdefault("amendment_flags_written", 0)
                counts["amendment_flags_written"] += amend_result.flags_written
                counts.setdefault("amendment_status_by_ticker", {})
                counts["amendment_status_by_ticker"][ticker.upper()] = amend_result.status

            counts["is_layer3_identities_checked"] += _count_layer3_identities(
                conn, company_id=company.id,
                statement="income_statement",
                extraction_version=IS_EXTRACTION_VERSION,
            )
            counts["cf_layer3_identities_checked"] += _count_layer3_identities(
                conn, company_id=company.id,
                statement="cash_flow",
                extraction_version=CF_EXTRACTION_VERSION,
            )

            # --- Fetch XBRL once — used by both Layer 2 (restricted cash
            # lookup for cash roll-forward ties) and Layer 5 (anchor match).
            xbrl_payload = _fetch_xbrl_payload(
                conn, company=company, ingest_run_id=run_id
            )
            counts["raw_responses"] += 1

            # --- Layer 2: cross-statement ties (IS ↔ BS ↔ CF) ---
            # SOFT GATE: Layer 2 failures are written as flags in
            # data_quality_flags rather than raising. Most Layer 2 failures
            # catch vendor-internal inconsistency (FMP's IS endpoint and CF
            # endpoint disagree on the same "net income" for the same
            # period) or timing mismatches (BS cash snapshot vs CF
            # cash_end_of_period with restricted-cash classification drift).
            # These are real data issues worth surfacing, but they shouldn't
            # block ingest — the data itself is still loadable and usable
            # with appropriate annotation. Analyst reviews flags and
            # resolves via manual supersession if they care for their
            # specific analysis.
            cross_failures = verify_cross_statement_ties(
                conn,
                company_id=company.id,
                is_extraction_version=IS_EXTRACTION_VERSION,
                bs_extraction_version=BS_EXTRACTION_VERSION,
                cf_extraction_version=CF_EXTRACTION_VERSION,
                companyfacts=xbrl_payload,
            )
            if cross_failures:
                _write_layer2_flags(
                    conn, company_id=company.id,
                    failures=cross_failures, ingest_run_id=run_id,
                )
                counts.setdefault("layer2_flags_written", 0)
                counts["layer2_flags_written"] += len(cross_failures)
            # 4 ties per CF period: NI, cash_end, cash_begin, net_change.
            # Ties 4/5 skip on the first window period (no prior BS),
            # so "evaluated" is an upper bound.
            counts["cross_statement_ties_checked"] += _count_cf_periods(
                conn, company_id=company.id,
            ) * 4

            # --- Layer 5: XBRL anchor match (SOFT GATE) ---
            # Divergences between FMP and SEC XBRL are written as flags rather
            # than raising. These catch cases where FMP's normalization differs
            # from SEC's (often because FMP didn't pick up in-10-K or in-later-10-Q
            # comparative restatements that XBRL does have). The data still
            # loads; analyst sees which concepts disagree with SEC and reviews.
            is_result = reconcile_is_anchors(
                conn, company_id=company.id,
                extraction_version=IS_EXTRACTION_VERSION, companyfacts=xbrl_payload,
            )
            counts["is_anchors_stored"] += is_result.anchors_with_fmp_stored
            counts["is_anchors_checked"] += is_result.anchors_checked
            counts["is_anchors_matched"] += is_result.anchors_matched
            for concept, pe, pt in is_result.anchors_not_in_xbrl:
                counts["is_anchors_not_in_xbrl"].append({
                    "concept": concept, "period_end": pe.isoformat(), "period_type": pt,
                })
            if is_result.divergences:
                _write_layer5_flags(
                    conn, company_id=company.id, statement="income_statement",
                    divergences=is_result.divergences, ingest_run_id=run_id,
                )
                counts.setdefault("layer5_flags_written", 0)
                counts["layer5_flags_written"] += len(is_result.divergences)

            bs_result = reconcile_bs_anchors(
                conn, company_id=company.id,
                extraction_version=BS_EXTRACTION_VERSION, companyfacts=xbrl_payload,
            )
            counts["bs_anchors_stored"] += bs_result.anchors_with_fmp_stored
            counts["bs_anchors_checked"] += bs_result.anchors_checked
            counts["bs_anchors_matched"] += bs_result.anchors_matched
            for concept, pe, pt in bs_result.anchors_not_in_xbrl:
                counts["bs_anchors_not_in_xbrl"].append({
                    "concept": concept, "period_end": pe.isoformat(), "period_type": pt,
                })
            if bs_result.divergences:
                _write_layer5_flags(
                    conn, company_id=company.id, statement="balance_sheet",
                    divergences=bs_result.divergences, ingest_run_id=run_id,
                )
                counts.setdefault("layer5_flags_written", 0)
                counts["layer5_flags_written"] += len(bs_result.divergences)

            cf_result = reconcile_cf_anchors(
                conn, company_id=company.id,
                extraction_version=CF_EXTRACTION_VERSION, companyfacts=xbrl_payload,
            )
            counts["cf_anchors_stored"] += cf_result.anchors_with_fmp_stored
            counts["cf_anchors_checked"] += cf_result.anchors_checked
            counts["cf_anchors_matched"] += cf_result.anchors_matched
            for concept, pe, pt in cf_result.anchors_not_in_xbrl:
                counts["cf_anchors_not_in_xbrl"].append({
                    "concept": concept, "period_end": pe.isoformat(), "period_type": pt,
                })
            if cf_result.divergences:
                _write_layer5_flags(
                    conn, company_id=company.id, statement="cash_flow",
                    divergences=cf_result.divergences, ingest_run_id=run_id,
                )
                counts.setdefault("layer5_flags_written", 0)
                counts["layer5_flags_written"] += len(cf_result.divergences)

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
    except PeriodArithmeticViolation as e:
        close_failed(
            conn, run_id, error_message=str(e),
            error_details={
                "kind": "period_arithmetic_violation",
                "statement": e.statement,
                "failures": [
                    {"concept": f.concept, "fiscal_year": f.fiscal_year,
                     "quarters_sum": str(f.quarters_sum), "annual": str(f.annual),
                     "delta": str(f.delta), "tolerance": str(f.tolerance)}
                    for f in e.failures
                ],
            },
        )
        raise
    except CrossStatementViolation as e:
        close_failed(
            conn, run_id, error_message=str(e),
            error_details={
                "kind": "cross_statement_violation",
                "failures": [
                    {"tie": f.tie,
                     "period_end": f.period_end.isoformat(),
                     "period_type": f.period_type,
                     "fiscal_year": f.fiscal_year,
                     "fiscal_quarter": f.fiscal_quarter,
                     "lhs_value": str(f.lhs_value),
                     "rhs_value": str(f.rhs_value),
                     "delta": str(f.delta),
                     "tolerance": str(f.tolerance)}
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
