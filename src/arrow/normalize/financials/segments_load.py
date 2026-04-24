"""Load FMP revenue segmentation payloads into financial_facts."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Literal

import psycopg

from arrow.ingest.fmp.segments import (
    GEOGRAPHIC_SEGMENT_ENDPOINT,
    PRODUCT_SEGMENT_ENDPOINT,
)
from arrow.normalize.periods.derive import (
    derive_calendar_period,
    derive_fiscal_period,
)

EXTRACTION_VERSION = "fmp-segments-v1"

DimensionType = Literal["product", "geography", "operating_segment"]


@dataclass
class SegmentLoadResult:
    facts_written: int = 0
    facts_superseded: int = 0
    rows_processed: int = 0
    segments_processed: int = 0
    period_labels: list[str] = field(default_factory=list)


def _dimension_type_for_endpoint(endpoint: str) -> DimensionType:
    if endpoint == PRODUCT_SEGMENT_ENDPOINT:
        return "product"
    if endpoint == GEOGRAPHIC_SEGMENT_ENDPOINT:
        return "geography"
    raise ValueError(f"unsupported segment endpoint: {endpoint!r}")


def _dimension_source_for_endpoint(endpoint: str) -> str:
    return f"fmp:{endpoint}"


def normalize_dimension_key(label: str) -> str:
    """Normalize a vendor segment label into a stable company-local key."""
    ascii_label = (
        unicodedata.normalize("NFKD", label)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    key = re.sub(r"[^a-z0-9]+", "_", ascii_label).strip("_")
    key = re.sub(r"_+", "_", key)
    if not key:
        raise ValueError(f"segment label normalizes to empty key: {label!r}")
    return key


def _parse_period_type(value: str) -> str:
    upper = value.upper()
    if upper in {"FY", "ANNUAL"}:
        return "annual"
    if upper in {"Q1", "Q2", "Q3", "Q4", "QUARTER"}:
        return "quarter"
    raise ValueError(f"unsupported FMP segment period: {value!r}")


def _parse_row_published_at(row: dict[str, Any]) -> datetime | None:
    accepted = row.get("acceptedDate")
    if accepted:
        return datetime.strptime(accepted, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    filing = row.get("filingDate")
    if filing:
        return datetime.strptime(filing, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return None


def _fallback_published_at(
    cur: psycopg.Cursor[Any],
    *,
    company_id: int,
    period_end: date,
    period_type: str,
    row: dict[str, Any],
) -> datetime:
    cur.execute(
        """
        SELECT published_at
        FROM financial_facts
        WHERE company_id = %s
          AND statement = 'income_statement'
          AND concept = 'revenue'
          AND period_end = %s
          AND period_type = %s
          AND superseded_at IS NULL
        ORDER BY published_at DESC, id DESC
        LIMIT 1;
        """,
        (company_id, period_end, period_type),
    )
    existing = cur.fetchone()
    if existing is not None:
        return existing[0]
    return datetime.strptime(row["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)


def load_fmp_segment_rows(
    conn: psycopg.Connection,
    *,
    company_id: int,
    company_fiscal_year_end_md: str,
    endpoint: str,
    rows: list[dict[str, Any]],
    source_raw_response_id: int,
    ingest_run_id: int,
    min_fiscal_year: int | None = None,
    max_fiscal_year: int | None = None,
) -> SegmentLoadResult:
    """Load one FMP revenue segmentation payload into financial_facts."""
    result = SegmentLoadResult()
    dimension_type = _dimension_type_for_endpoint(endpoint)
    dimension_source = _dimension_source_for_endpoint(endpoint)

    with conn.cursor() as cur:
        for row in rows:
            result.rows_processed += 1
            data = row.get("data")
            if not isinstance(data, dict) or not data:
                continue

            period_type = _parse_period_type(str(row["period"]))
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

            fmp_fiscal_year = int(row["fiscalYear"])
            if fmp_fiscal_year != fiscal.fiscal_year:
                raise ValueError(
                    "FMP segment fiscalYear mismatch: "
                    f"period_end={period_end}, fmp={fmp_fiscal_year}, "
                    f"derived={fiscal.fiscal_year}"
                )

            calendar = derive_calendar_period(period_end)
            published_at = _parse_row_published_at(row) or _fallback_published_at(
                cur,
                company_id=company_id,
                period_end=period_end,
                period_type=fiscal.period_type,
                row=row,
            )
            unit = row.get("reportedCurrency") or "USD"
            result.period_labels.append(fiscal.fiscal_period_label)

            for label, raw_value in data.items():
                if raw_value is None:
                    continue
                dimension_label = str(label).strip()
                if not dimension_label:
                    continue
                dimension_key = normalize_dimension_key(dimension_label)
                value = Decimal(str(raw_value))
                result.segments_processed += 1

                cur.execute(
                    """
                    UPDATE financial_facts
                    SET superseded_at = %s
                    WHERE company_id = %s
                      AND statement = 'segment'
                      AND concept = 'revenue'
                      AND period_end = %s
                      AND period_type = %s
                      AND extraction_version = %s
                      AND dimension_type = %s
                      AND dimension_key = %s
                      AND dimension_source = %s
                      AND superseded_at IS NULL
                      AND source_raw_response_id <> %s;
                    """,
                    (
                        published_at,
                        company_id,
                        period_end,
                        fiscal.period_type,
                        EXTRACTION_VERSION,
                        dimension_type,
                        dimension_key,
                        dimension_source,
                        source_raw_response_id,
                    ),
                )
                result.facts_superseded += cur.rowcount

                cur.execute(
                    """
                    INSERT INTO financial_facts (
                        company_id, statement, concept, value, unit,
                        dimension_type, dimension_key, dimension_label, dimension_source,
                        fiscal_year, fiscal_quarter, fiscal_period_label,
                        period_end, period_type,
                        calendar_year, calendar_quarter, calendar_period_label,
                        published_at, source_raw_response_id, extraction_version,
                        ingest_run_id
                    ) VALUES (
                        %s, 'segment', 'revenue', %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING id;
                    """,
                    (
                        company_id,
                        value,
                        unit,
                        dimension_type,
                        dimension_key,
                        dimension_label,
                        dimension_source,
                        fiscal.fiscal_year,
                        fiscal.fiscal_quarter,
                        fiscal.fiscal_period_label,
                        period_end,
                        fiscal.period_type,
                        calendar.calendar_year,
                        calendar.calendar_quarter,
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
