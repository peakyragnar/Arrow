from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Literal

ArtifactType = Literal[
    "10k",
    "10q",
    "8k",
    "transcript",
    "press_release",
    "news_article",
    "presentation",
    "video_transcript",
    "research_note",
    "industry_primer",
    "product_primer",
    "macro_primer",
    "macro_release",
]

PeriodType = Literal["quarter", "annual", "stub"]


@dataclass
class Artifact:
    id: int | None
    ingest_run_id: int | None
    artifact_type: ArtifactType
    source: str
    source_document_id: str | None

    raw_hash: bytes
    canonical_hash: bytes

    ticker: str | None

    fiscal_year: int | None
    fiscal_quarter: int | None
    fiscal_period_label: str | None
    period_end: date | None
    period_type: PeriodType | None

    calendar_year: int | None
    calendar_quarter: int | None
    calendar_period_label: str | None

    title: str | None
    url: str | None
    content_type: str | None
    language: str | None

    published_at: datetime | None
    effective_at: datetime | None
    ingested_at: datetime

    supersedes: int | None
    superseded_at: datetime | None

    authored_by: str | None
    last_reviewed_at: datetime | None
    asserted_valid_through: date | None

    artifact_metadata: dict[str, Any] = field(default_factory=dict)
