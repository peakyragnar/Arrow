"""Orchestrate FMP IS payload -> financial_facts rows.

For one (ticker, period_type) raw_responses payload:
  1. For each period row in the payload:
     - Parse period_end + period_type from FMP fields
     - Derive fiscal + calendar (two-clocks) columns
     - Cross-check FMP's declared fiscalYear vs the algorithmic value
     - Map FMP fields -> canonical IS buckets
     - Verify Layer 1 subtotal ties (HARD BLOCK on failure)
     - Supersede any existing current rows for the same business identity
     - INSERT one financial_facts row per mapped bucket

Supersession rule: the partial-unique index
`financial_facts_one_current_idx` enforces "at most one current row per
(company, concept, period_end, period_type, extraction_version)". To
re-ingest a fresh payload we first stamp the old current row with
`superseded_at = new published_at`, then INSERT the new one — both
inside the same transaction.

ON CONFLICT DO NOTHING on the full business-identity UNIQUE constraint
makes same-payload re-extraction idempotent (same raw_responses id ->
no-op).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import psycopg

from arrow.normalize.financials.fmp_bs_mapper import map_balance_sheet_row
from arrow.normalize.financials.fmp_cf_mapper import map_cash_flow_row
from arrow.normalize.financials.fmp_is_mapper import map_income_statement_row
from arrow.normalize.financials.verify_bs import (
    TieFailure as BSTieFailure,
    verify_bs_ties,
)
from arrow.normalize.financials.verify_cf import (
    TieFailure as CFTieFailure,
    verify_cf_hard_ties,
    verify_cf_soft_ties,
)
from arrow.normalize.financials.verify_is import TieFailure, verify_is_ties
from arrow.normalize.periods.derive import derive_calendar_period, derive_fiscal_period

# Separate extraction-version tags per statement so IS, BS, and CF can be
# re-derived independently. Supersession still happens in-place within
# each version via the partial unique index on financial_facts.
EXTRACTION_VERSION = "fmp-is-v1"  # retained for BC with callers; IS default
IS_EXTRACTION_VERSION = "fmp-is-v1"
BS_EXTRACTION_VERSION = "fmp-bs-v1"
CF_EXTRACTION_VERSION = "fmp-cf-v1"


@dataclass
class LoadResult:
    facts_written: int = 0
    facts_superseded: int = 0
    rows_processed: int = 0
    period_labels: list[str] = field(default_factory=list)
    # Soft-tie flags written during this load (CF vendor-bucketing drifts).
    # Non-blocking; analyst reviews via scripts/review_flags.py.
    flags_written: int = 0


class VerificationFailed(RuntimeError):
    def __init__(self, period_label: str, failures: list[TieFailure]) -> None:
        self.period_label = period_label
        self.failures = failures
        summary = "; ".join(
            f"{f.tie}: filer={f.filer}, computed={f.computed}, delta={f.delta}"
            for f in failures
        )
        super().__init__(f"IS verification failed for {period_label}: {summary}")


class BSVerificationFailed(RuntimeError):
    def __init__(self, period_label: str, failures: list[BSTieFailure]) -> None:
        self.period_label = period_label
        self.failures = failures
        summary = "; ".join(
            f"{f.tie}: filer={f.filer}, computed={f.computed}, delta={f.delta}"
            for f in failures
        )
        super().__init__(f"BS verification failed for {period_label}: {summary}")


class CFVerificationFailed(RuntimeError):
    def __init__(self, period_label: str, failures: list[CFTieFailure]) -> None:
        self.period_label = period_label
        self.failures = failures
        summary = "; ".join(
            f"{f.tie}: filer={f.filer}, computed={f.computed}, delta={f.delta}"
            for f in failures
        )
        super().__init__(f"CF verification failed for {period_label}: {summary}")


class FiscalYearMismatch(RuntimeError):
    def __init__(
        self,
        period_end: date,
        fmp_fiscal_year: int,
        derived_fiscal_year: int,
    ) -> None:
        self.period_end = period_end
        self.fmp_fiscal_year = fmp_fiscal_year
        self.derived_fiscal_year = derived_fiscal_year
        super().__init__(
            f"FMP fiscalYear={fmp_fiscal_year} disagrees with algorithmic "
            f"derivation={derived_fiscal_year} for period_end={period_end}"
        )


def _parse_fmp_period(period_str: str) -> str:
    if period_str == "FY":
        return "annual"
    if period_str in ("Q1", "Q2", "Q3", "Q4"):
        return "quarter"
    raise ValueError(f"unexpected FMP 'period': {period_str!r}")


# Flag type for CF vendor-bucketing drift (cfo / cfi / cff subtotal !=
# sum of FMP's own component fields inside the shipped row).
CF_SUBTOTAL_DRIFT_FLAG_TYPE = "cf_subtotal_component_drift"


def _severity_for_drift(delta: Decimal, filer: Decimal, computed: Decimal) -> str:
    """Scale severity by |delta| / max(|filer|, |computed|).

    <1%  → 'informational' (vendor rounding / minor misbucket)
    1-10% → 'warning'
    ≥10% → 'investigate'
    """
    scale = max(abs(filer), abs(computed))
    if scale == 0:
        return "investigate"
    pct = abs(delta) / scale
    if pct < Decimal("0.01"):
        return "informational"
    if pct < Decimal("0.10"):
        return "warning"
    return "investigate"


def _write_cf_soft_tie_flag(
    cur: psycopg.Cursor,
    *,
    company_id: int,
    fiscal_year: int,
    fiscal_quarter: int | None,
    period_end: date,
    period_type: str,
    failure: CFTieFailure,
    ingest_run_id: int,
) -> None:
    """Write one `data_quality_flags` row for a soft-tie CF failure.

    The fact rows for this period are loaded verbatim from FMP; this flag
    records that FMP's subtotal and FMP's component fields disagree inside
    the single row FMP shipped. The flag's `concept` is the subtotal that
    disagreed (cfo / cfi / cff), so analysts who query that concept can
    join to this flag row and see the caveat.
    """
    subtotal_concept = failure.tie.split(" ")[0]  # 'cfo' / 'cfi' / 'cff'
    severity = _severity_for_drift(failure.delta, failure.filer, failure.computed)
    reason = (
        f"FMP's reported {subtotal_concept} ({failure.filer}) disagrees with "
        f"the sum of FMP's own component fields ({failure.computed}) by "
        f"{failure.delta} (tolerance {failure.tolerance}). The row was "
        f"loaded verbatim; this flag records that FMP shipped a "
        f"self-inconsistent row. Typical cause: FMP's normalization "
        f"bucketed or dropped an item that SEC XBRL carries separately."
    )
    cur.execute(
        """
        INSERT INTO data_quality_flags (
            company_id, statement, concept,
            fiscal_year, fiscal_quarter, period_end, period_type,
            flag_type, severity,
            expected_value, computed_value, delta, tolerance,
            reason, context, source_run_id
        ) VALUES (
            %s, 'cash_flow', %s,
            %s, %s, %s, %s,
            %s, %s,
            %s, %s, %s, %s,
            %s, %s::jsonb, %s
        );
        """,
        (
            company_id, subtotal_concept,
            fiscal_year, fiscal_quarter, period_end, period_type,
            CF_SUBTOTAL_DRIFT_FLAG_TYPE, severity,
            failure.filer, failure.computed, failure.delta, failure.tolerance,
            reason,
            f'{{"tie": "{failure.tie}"}}',
            ingest_run_id,
        ),
    )


def _parse_published_at(row: dict[str, Any]) -> datetime:
    """FMP's acceptedDate ('YYYY-MM-DD HH:MM:SS') is preferred; filingDate as fallback.

    Timezone: FMP doesn't document a TZ. We label both as UTC — the filingDate
    granularity is days, so TZ misalignment is at most a 24-hour smear on PIT
    queries, which is inside the resolution of fiscal periods (months).
    """
    accepted = row.get("acceptedDate")
    if accepted:
        return datetime.strptime(accepted, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    filing = row.get("filingDate")
    if filing:
        return datetime.strptime(filing, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return datetime.strptime(row["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)


def load_fmp_is_rows(
    conn: psycopg.Connection,
    *,
    company_id: int,
    company_fiscal_year_end_md: str,
    rows: list[dict[str, Any]],
    source_raw_response_id: int,
    ingest_run_id: int,
    min_fiscal_year: int | None = None,
    max_fiscal_year: int | None = None,
) -> LoadResult:
    """Load every row in one FMP IS payload. Caller owns transaction.

    min_fiscal_year: skip rows whose derived fiscal_year < this value.
    Rounds the validated window to complete fiscal years so Layer 3
    period arithmetic and Q4 XBRL derivation have all the periods they
    need. Rows outside the window are counted in rows_processed but not
    written to financial_facts.

    Raises VerificationFailed or FiscalYearMismatch on data integrity issues;
    the caller's transaction should roll back and the ingest run should be
    marked failed.
    """
    result = LoadResult()

    with conn.cursor() as cur:
        for row in rows:
            result.rows_processed += 1
            period_type = _parse_fmp_period(row["period"])
            period_end = datetime.strptime(row["date"], "%Y-%m-%d").date()
            fiscal = derive_fiscal_period(
                period_end,
                company_fiscal_year_end_md,
                period_type=period_type,
            )

            # Skip rows outside the validated fiscal-year window. We filter
            # on fiscal_year (not period_end / calendar date) so partial
            # fiscal years never land — a filer's FY2021 Q1 period_end may
            # be in calendar 2020, but it belongs to FY2021 and should come
            # in with the rest of FY2021.
            if min_fiscal_year is not None and fiscal.fiscal_year < min_fiscal_year:
                continue
            if max_fiscal_year is not None and fiscal.fiscal_year > max_fiscal_year:
                continue

            calendar = derive_calendar_period(period_end)

            fmp_fiscal_year = int(row["fiscalYear"])
            if fmp_fiscal_year != fiscal.fiscal_year:
                raise FiscalYearMismatch(
                    period_end=period_end,
                    fmp_fiscal_year=fmp_fiscal_year,
                    derived_fiscal_year=fiscal.fiscal_year,
                )

            mapped = map_income_statement_row(row)
            values_by_concept = {m.concept: m.value for m in mapped}

            failures = verify_is_ties(values_by_concept)
            if failures:
                raise VerificationFailed(fiscal.fiscal_period_label, failures)

            published_at = _parse_published_at(row)
            result.period_labels.append(fiscal.fiscal_period_label)

            for fact in mapped:
                # Supersede any existing current row for this business identity
                # (different source_raw_response_id -> old payload; this is a
                # fresh re-ingest).
                cur.execute(
                    """
                    UPDATE financial_facts
                    SET superseded_at = %s
                    WHERE company_id = %s
                      AND concept = %s
                      AND period_end = %s
                      AND period_type = %s
                      AND extraction_version = %s
                      AND superseded_at IS NULL
                      AND source_raw_response_id <> %s;
                    """,
                    (
                        published_at,
                        company_id,
                        fact.concept,
                        period_end,
                        fiscal.period_type,
                        EXTRACTION_VERSION,
                        source_raw_response_id,
                    ),
                )
                result.facts_superseded += cur.rowcount

                # Insert the new row. Same-raw-response re-extraction is a
                # no-op via the UNIQUE constraint on business identity.
                cur.execute(
                    """
                    INSERT INTO financial_facts (
                        company_id, statement, concept, value, unit,
                        fiscal_year, fiscal_quarter, fiscal_period_label,
                        period_end, period_type,
                        calendar_year, calendar_quarter, calendar_period_label,
                        published_at, source_raw_response_id, extraction_version,
                        ingest_run_id
                    ) VALUES (
                        %s, 'income_statement', %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s
                    )
                    ON CONFLICT ON CONSTRAINT financial_facts_unique_extraction
                    DO NOTHING
                    RETURNING id;
                    """,
                    (
                        company_id, fact.concept, fact.value, fact.unit,
                        fiscal.fiscal_year, fiscal.fiscal_quarter,
                        fiscal.fiscal_period_label,
                        period_end, fiscal.period_type,
                        calendar.calendar_year, calendar.calendar_quarter,
                        calendar.calendar_period_label,
                        published_at,
                        source_raw_response_id,
                        EXTRACTION_VERSION,
                        ingest_run_id,
                    ),
                )
                if cur.fetchone() is not None:
                    result.facts_written += 1

    return result


def load_fmp_bs_rows(
    conn: psycopg.Connection,
    *,
    company_id: int,
    company_fiscal_year_end_md: str,
    rows: list[dict[str, Any]],
    source_raw_response_id: int,
    ingest_run_id: int,
    min_fiscal_year: int | None = None,
    max_fiscal_year: int | None = None,
) -> LoadResult:
    """Mirror of load_fmp_is_rows for the balance sheet.

    BS rows are point-in-time snapshots, not flows. Each row emits one
    fact per mapped canonical bucket at `period_end` (same date semantics
    as IS). Layer 3 period arithmetic doesn't apply (stocks don't sum
    across quarters).

    Raises BSVerificationFailed (Layer 1 tie miss) or FiscalYearMismatch;
    caller rolls back and marks ingest_run failed.
    """
    result = LoadResult()

    with conn.cursor() as cur:
        for row in rows:
            result.rows_processed += 1
            period_type = _parse_fmp_period(row["period"])
            period_end = datetime.strptime(row["date"], "%Y-%m-%d").date()
            fiscal = derive_fiscal_period(
                period_end,
                company_fiscal_year_end_md,
                period_type=period_type,
            )

            if min_fiscal_year is not None and fiscal.fiscal_year < min_fiscal_year:
                continue
            if max_fiscal_year is not None and fiscal.fiscal_year > max_fiscal_year:
                continue

            calendar = derive_calendar_period(period_end)

            fmp_fiscal_year = int(row["fiscalYear"])
            if fmp_fiscal_year != fiscal.fiscal_year:
                raise FiscalYearMismatch(
                    period_end=period_end,
                    fmp_fiscal_year=fmp_fiscal_year,
                    derived_fiscal_year=fiscal.fiscal_year,
                )

            mapped = map_balance_sheet_row(row)
            values_by_concept = {m.concept: m.value for m in mapped}

            failures = verify_bs_ties(values_by_concept)
            if failures:
                raise BSVerificationFailed(fiscal.fiscal_period_label, failures)

            published_at = _parse_published_at(row)
            result.period_labels.append(fiscal.fiscal_period_label)

            for fact in mapped:
                cur.execute(
                    """
                    UPDATE financial_facts
                    SET superseded_at = %s
                    WHERE company_id = %s
                      AND concept = %s
                      AND period_end = %s
                      AND period_type = %s
                      AND extraction_version = %s
                      AND superseded_at IS NULL
                      AND source_raw_response_id <> %s;
                    """,
                    (
                        published_at,
                        company_id,
                        fact.concept,
                        period_end,
                        fiscal.period_type,
                        BS_EXTRACTION_VERSION,
                        source_raw_response_id,
                    ),
                )
                result.facts_superseded += cur.rowcount

                cur.execute(
                    """
                    INSERT INTO financial_facts (
                        company_id, statement, concept, value, unit,
                        fiscal_year, fiscal_quarter, fiscal_period_label,
                        period_end, period_type,
                        calendar_year, calendar_quarter, calendar_period_label,
                        published_at, source_raw_response_id, extraction_version,
                        ingest_run_id
                    ) VALUES (
                        %s, 'balance_sheet', %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s
                    )
                    ON CONFLICT ON CONSTRAINT financial_facts_unique_extraction
                    DO NOTHING
                    RETURNING id;
                    """,
                    (
                        company_id, fact.concept, fact.value, fact.unit,
                        fiscal.fiscal_year, fiscal.fiscal_quarter,
                        fiscal.fiscal_period_label,
                        period_end, fiscal.period_type,
                        calendar.calendar_year, calendar.calendar_quarter,
                        calendar.calendar_period_label,
                        published_at,
                        source_raw_response_id,
                        BS_EXTRACTION_VERSION,
                        ingest_run_id,
                    ),
                )
                if cur.fetchone() is not None:
                    result.facts_written += 1

    return result


def load_fmp_cf_rows(
    conn: psycopg.Connection,
    *,
    company_id: int,
    company_fiscal_year_end_md: str,
    rows: list[dict[str, Any]],
    source_raw_response_id: int,
    ingest_run_id: int,
    min_fiscal_year: int | None = None,
    max_fiscal_year: int | None = None,
) -> LoadResult:
    """Mirror of load_fmp_is_rows / load_fmp_bs_rows for cash flow.

    FMP returns DISCRETE quarterly CF (not YTD) — empirically verified.
    No YTD→discrete conversion needed. Signs are cash-impact per concepts.md
    § 2.2; FMP's convention matches, no transform.

    Layer 1 CF ties are split two ways:

      HARD (filer integrity) — net_change_in_cash == cfo+cfi+cff+fx, and
      cash roll-forward. Raises CFVerificationFailed on failure; caller
      rolls back the transaction. A real CF statement can't violate these.

      SOFT (vendor bucketing) — cfo/cfi/cff subtotal == sum of FMP's own
      component fields. Failure here means FMP shipped a self-inconsistent
      row (their bucketing dropped/misbucketed an item). The row is
      loaded verbatim and one `data_quality_flags` row is written per
      failing subtotal. The analyst reviews via `scripts/review_flags.py`.

    Raises CFVerificationFailed (HARD CF tie miss) or FiscalYearMismatch.
    """
    result = LoadResult()

    with conn.cursor() as cur:
        for row in rows:
            result.rows_processed += 1
            period_type = _parse_fmp_period(row["period"])
            period_end = datetime.strptime(row["date"], "%Y-%m-%d").date()
            fiscal = derive_fiscal_period(
                period_end,
                company_fiscal_year_end_md,
                period_type=period_type,
            )

            if min_fiscal_year is not None and fiscal.fiscal_year < min_fiscal_year:
                continue
            if max_fiscal_year is not None and fiscal.fiscal_year > max_fiscal_year:
                continue

            calendar = derive_calendar_period(period_end)

            fmp_fiscal_year = int(row["fiscalYear"])
            if fmp_fiscal_year != fiscal.fiscal_year:
                raise FiscalYearMismatch(
                    period_end=period_end,
                    fmp_fiscal_year=fmp_fiscal_year,
                    derived_fiscal_year=fiscal.fiscal_year,
                )

            mapped = map_cash_flow_row(row)
            values_by_concept = {m.concept: m.value for m in mapped}

            # HARD ties first: any failure aborts the whole transaction.
            hard_failures = verify_cf_hard_ties(values_by_concept)
            if hard_failures:
                raise CFVerificationFailed(fiscal.fiscal_period_label, hard_failures)

            # SOFT ties: collect, write flags after fact insert.
            soft_failures = verify_cf_soft_ties(values_by_concept)

            published_at = _parse_published_at(row)
            result.period_labels.append(fiscal.fiscal_period_label)

            for fact in mapped:
                cur.execute(
                    """
                    UPDATE financial_facts
                    SET superseded_at = %s
                    WHERE company_id = %s
                      AND concept = %s
                      AND period_end = %s
                      AND period_type = %s
                      AND extraction_version = %s
                      AND superseded_at IS NULL
                      AND source_raw_response_id <> %s;
                    """,
                    (
                        published_at,
                        company_id,
                        fact.concept,
                        period_end,
                        fiscal.period_type,
                        CF_EXTRACTION_VERSION,
                        source_raw_response_id,
                    ),
                )
                result.facts_superseded += cur.rowcount

                cur.execute(
                    """
                    INSERT INTO financial_facts (
                        company_id, statement, concept, value, unit,
                        fiscal_year, fiscal_quarter, fiscal_period_label,
                        period_end, period_type,
                        calendar_year, calendar_quarter, calendar_period_label,
                        published_at, source_raw_response_id, extraction_version,
                        ingest_run_id
                    ) VALUES (
                        %s, 'cash_flow', %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s
                    )
                    ON CONFLICT ON CONSTRAINT financial_facts_unique_extraction
                    DO NOTHING
                    RETURNING id;
                    """,
                    (
                        company_id, fact.concept, fact.value, fact.unit,
                        fiscal.fiscal_year, fiscal.fiscal_quarter,
                        fiscal.fiscal_period_label,
                        period_end, fiscal.period_type,
                        calendar.calendar_year, calendar.calendar_quarter,
                        calendar.calendar_period_label,
                        published_at,
                        source_raw_response_id,
                        CF_EXTRACTION_VERSION,
                        ingest_run_id,
                    ),
                )
                if cur.fetchone() is not None:
                    result.facts_written += 1

            # Soft-tie failures: one `data_quality_flags` row per failing
            # subtotal, tied to this period. Written after the facts so
            # the flag lives in the same transaction as the facts it
            # annotates (re-ingest auto-closes dependent flags via
            # migration 012's `superseded_by_reingest` resolution).
            for sf in soft_failures:
                _write_cf_soft_tie_flag(
                    cur,
                    company_id=company_id,
                    fiscal_year=fiscal.fiscal_year,
                    fiscal_quarter=fiscal.fiscal_quarter,
                    period_end=period_end,
                    period_type=fiscal.period_type,
                    failure=sf,
                    ingest_run_id=ingest_run_id,
                )
                result.flags_written += 1

    return result
