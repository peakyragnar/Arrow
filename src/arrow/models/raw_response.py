from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class RawResponse:
    id: int | None
    ingest_run_id: int
    vendor: str
    endpoint: str
    params: dict[str, Any]
    params_hash: bytes
    request_url: str | None
    http_status: int
    content_type: str
    response_headers: dict[str, Any] | None
    body_jsonb: dict[str, Any] | None
    body_raw: bytes | None
    raw_hash: bytes
    canonical_hash: bytes
    fetched_at: datetime
