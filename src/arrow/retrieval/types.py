"""Shared dataclasses for retrieval primitives and analyst recipes.

Every retrieval primitive returns one or more of the typed rows defined here,
so recipes, the future agent loop, and downstream synthesizers can consume the
same shape regardless of which primitive produced it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4


@dataclass(frozen=True)
class Intent:
    ticker: str
    company_id: int
    fiscal_year: int
    fiscal_quarter: int | None
    fiscal_period_key: str
    period_type: str
    topic: str
    mode: str
    source_question: str
    asof: str | None = None


@dataclass(frozen=True)
class TraceAction:
    label: str
    params_hash: str
    row_count: int
    duration_ms: float
    selected_ids: list[str] = field(default_factory=list)


@dataclass
class RuntimeTrace:
    trace_id: str
    surface: str
    source_question: str
    started_at: str
    intent: dict[str, Any] | None = None
    readiness: dict[str, Any] | None = None
    recipe_name: str | None = None
    actions: list[TraceAction] = field(default_factory=list)
    synthesizer: str | None = None
    verifier_status: str | None = None
    gaps: list[str] = field(default_factory=list)
    final_citations: list[str] = field(default_factory=list)

    @classmethod
    def start(cls, *, source_question: str, surface: str = "cli") -> "RuntimeTrace":
        return cls(
            trace_id=str(uuid4()),
            surface=surface,
            source_question=source_question,
            started_at=datetime.now(UTC).isoformat(),
        )


@dataclass(frozen=True)
class Company:
    id: int
    ticker: str
    name: str
    cik: int
    fiscal_year_end_md: str


@dataclass(frozen=True)
class FiscalMetric:
    ticker: str
    company_id: int
    fiscal_year: int
    fiscal_period_label: str
    fy_end: Any
    revenue_fy: Decimal | None
    gross_margin_fy: Decimal | None
    operating_margin_fy: Decimal | None
    cfo_fy: Decimal | None
    capital_expenditures_fy: Decimal | None
    fcf_fy: Decimal | None


@dataclass(frozen=True)
class FinancialFact:
    fact_id: int
    statement: str
    concept: str
    value: Decimal
    unit: str
    fiscal_period_label: str
    period_end: Any


@dataclass(frozen=True)
class SegmentFact:
    fact_id: int
    dimension_type: str
    dimension_key: str
    dimension_label: str
    value: Decimal
    prior_value: Decimal | None
    yoy_growth: Decimal | None
    fiscal_period_label: str
    period_end: Any


@dataclass(frozen=True)
class ArtifactPeriod:
    artifact_id: int
    artifact_type: str
    fiscal_period_key: str | None
    fiscal_period_label: str | None
    period_type: str | None
    period_end: Any
    published_at: Any
    source_document_id: str | None
    accession_number: str | None


@dataclass(frozen=True)
class EvidenceChunk:
    source_kind: str
    artifact_id: int
    chunk_id: int
    chunk_ordinal: int
    fiscal_period_key: str | None
    published_at: Any
    source_document_id: str | None
    accession_number: str | None
    unit_key: str
    unit_title: str
    heading_path: list[str]
    text: str
