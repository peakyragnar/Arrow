"""Load FMP historical-employee-count payload -> financial_facts rows.

Writes one fact per 10-K per ticker with:
    statement          = 'metrics'
    concept            = 'total_employees'
    period_end         = payload row's `periodOfReport` (fiscal year-end)
    period_type        = 'annual'
    fiscal_year/fiscal_period_label derived from period_end
    calendar fields    derived from period_end
    unit               = 'employees'
    published_at       = payload row's `filingDate`
    extraction_version = 'fmp-employees-v1'

No tie checks — employee count is a single-value disclosure, not part of
any tie identity. Layer 1 does not apply.

Supersession: same partial-unique-index contract as statement loaders.
Re-ingest of a later payload (e.g., FMP adds a newer 10-K) supersedes
the prior current row for that (company, concept, period_end) and
inserts the new one. Same-payload re-extraction is idempotent via the
full business-identity UNIQUE constraint.

Metric 18 (Revenue per Employee) joins `v_metrics_ttm` to the most
recent `total_employees` row where `employee_period_end <= quarter_end`
— the carry-forward rule from formulas.md § 18.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import psycopg

from arrow.normalize.periods.derive import (
    derive_calendar_period,
    derive_fiscal_period,
)

EXTRACTION_VERSION = "fmp-employees-v1"


@dataclass
class EmployeesLoadResult:
    facts_written: int = 0
    facts_superseded: int = 0
    rows_processed: int = 0
    period_labels: list[str] = field(default_factory=list)


def _parse_published_at(row: dict[str, Any]) -> datetime:
    filing = row.get("filingDate")
    if filing:
        return datetime.strptime(filing, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    # Fallback: use periodOfReport itself if filingDate is absent
    return datetime.strptime(row["periodOfReport"], "%Y-%m-%d").replace(tzinfo=timezone.utc)


def load_fmp_employee_rows(
    conn: psycopg.Connection,
    *,
    company_id: int,
    company_fiscal_year_end_md: str,
    rows: list[dict[str, Any]],
    source_raw_response_id: int,
    ingest_run_id: int,
    min_fiscal_year: int | None = None,
    max_fiscal_year: int | None = None,
) -> EmployeesLoadResult:
    """Load every 10-K employee-count row into financial_facts.

    Filters out non-10-K rows (formType != '10-K') and rows outside
    the optional [min_fiscal_year, max_fiscal_year] window.
    """
    result = EmployeesLoadResult()

    with conn.cursor() as cur:
        for row in rows:
            result.rows_processed += 1

            # Only 10-K rows carry employee counts in a standardized way.
            # FMP occasionally includes 10-K/A (amendments) — treat the
            # same as 10-K (amendments restate the count, not erase it).
            form_type = row.get("formType", "")
            if not form_type.startswith("10-K"):
                continue

            count = row.get("employeeCount")
            if count is None:
                continue

            period_end = datetime.strptime(row["periodOfReport"], "%Y-%m-%d").date()
            fiscal = derive_fiscal_period(
                period_end,
                company_fiscal_year_end_md,
                period_type="annual",
            )

            if min_fiscal_year is not None and fiscal.fiscal_year < min_fiscal_year:
                continue
            if max_fiscal_year is not None and fiscal.fiscal_year > max_fiscal_year:
                continue

            calendar = derive_calendar_period(period_end)
            published_at = _parse_published_at(row)
            result.period_labels.append(fiscal.fiscal_period_label)

            cur.execute(
                """
                UPDATE financial_facts
                SET superseded_at = %s
                WHERE company_id = %s
                  AND concept = 'total_employees'
                  AND period_end = %s
                  AND period_type = 'annual'
                  AND extraction_version = %s
                  AND superseded_at IS NULL
                  AND source_raw_response_id <> %s;
                """,
                (
                    published_at,
                    company_id,
                    period_end,
                    EXTRACTION_VERSION,
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
                    %s, 'metrics', 'total_employees', %s, 'employees',
                    %s, NULL, %s,
                    %s, 'annual',
                    %s, %s, %s,
                    %s, %s, %s,
                    %s
                )
                ON CONFLICT ON CONSTRAINT financial_facts_unique_extraction
                DO NOTHING
                RETURNING id;
                """,
                (
                    company_id, Decimal(str(count)),
                    fiscal.fiscal_year, fiscal.fiscal_period_label,
                    period_end,
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
