from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

ChunkType = Literal[
    "section",
    "speaker_turn",
    "timestamp_span",
    "table",
    "paragraph",
]


@dataclass
class ArtifactChunk:
    id: int | None
    artifact_id: int
    chunk_type: ChunkType
    ordinal: int

    section: str | None
    speaker: str | None
    starts_at: timedelta | None
    ends_at: timedelta | None

    text: str
    search_text: str | None
    # tsv is a GENERATED column computed by Postgres; not part of the model.

    fiscal_year: int | None
    fiscal_quarter: int | None
    calendar_year: int | None
    calendar_quarter: int | None

    chunker_version: str | None
    created_at: datetime
