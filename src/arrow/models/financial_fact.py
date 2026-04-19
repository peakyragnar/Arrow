from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Literal

Statement = Literal[
    "income_statement",
    "balance_sheet",
    "cash_flow",
    "metrics",
    "ratios",
    "segment",
]

PeriodType = Literal["quarter", "annual", "stub"]


@dataclass
class FinancialFact:
    id: int | None
    ingest_run_id: int | None

    company_id: int

    statement: Statement
    concept: str
    value: Decimal
    unit: str

    fiscal_year: int
    fiscal_quarter: int | None
    fiscal_period_label: str
    period_end: date
    period_type: PeriodType

    calendar_year: int
    calendar_quarter: int
    calendar_period_label: str

    published_at: datetime
    effective_at: datetime | None
    superseded_at: datetime | None
    ingested_at: datetime

    source_raw_response_id: int
    source_artifact_id: int | None
    extraction_version: str
