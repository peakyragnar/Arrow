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

from arrow.normalize.financials.fmp_is_mapper import map_income_statement_row
from arrow.normalize.financials.verify_is import TieFailure, verify_is_ties
from arrow.normalize.periods.derive import derive_calendar_period, derive_fiscal_period

EXTRACTION_VERSION = "fmp-is-v1"


@dataclass
class LoadResult:
    facts_written: int = 0
    facts_superseded: int = 0
    rows_processed: int = 0
    period_labels: list[str] = field(default_factory=list)


class VerificationFailed(RuntimeError):
    def __init__(self, period_label: str, failures: list[TieFailure]) -> None:
        self.period_label = period_label
        self.failures = failures
        summary = "; ".join(
            f"{f.tie}: filer={f.filer}, computed={f.computed}, delta={f.delta}"
            for f in failures
        )
        super().__init__(f"IS verification failed for {period_label}: {summary}")


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
