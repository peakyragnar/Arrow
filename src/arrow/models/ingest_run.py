from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

RunKind = Literal["backfill", "incremental", "reconciliation", "manual"]
RunStatus = Literal["started", "succeeded", "failed", "partial"]


@dataclass
class IngestRun:
    id: int | None
    run_kind: RunKind
    vendor: str
    ticker_scope: list[str] | None
    status: RunStatus
    started_at: datetime
    finished_at: datetime | None
    counts: dict[str, Any]
    error_message: str | None
    error_details: dict[str, Any] | None
    code_version: str | None
