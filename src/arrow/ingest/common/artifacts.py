"""Artifact writer — immutable documents with append-only raw fetch log."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from typing import Any

import psycopg
from psycopg.types.json import Jsonb


def _sha256(b: bytes) -> bytes:
    return hashlib.sha256(b).digest()


def _canonical_bytes(body: bytes, content_type: str | None) -> bytes:
    lower = (content_type or "").lower()
    if "application/json" in lower:
        parsed = json.loads(body)
        return json.dumps(parsed, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if lower.startswith("text/") or "html" in lower or "xml" in lower:
        return body.decode("utf-8", errors="replace").replace("\r\n", "\n").replace(
            "\r", "\n"
        ).encode("utf-8")
    return body


def write_artifact(
    conn: psycopg.Connection,
    *,
    ingest_run_id: int | None,
    artifact_type: str,
    source: str,
    source_document_id: str | None,
    body: bytes,
    ticker: str | None = None,
    fiscal_year: int | None = None,
    fiscal_quarter: int | None = None,
    fiscal_period_label: str | None = None,
    period_end: date | None = None,
    period_type: str | None = None,
    calendar_year: int | None = None,
    calendar_quarter: int | None = None,
    calendar_period_label: str | None = None,
    title: str | None = None,
    url: str | None = None,
    content_type: str | None = None,
    language: str | None = None,
    published_at: datetime | None = None,
    effective_at: datetime | None = None,
    artifact_metadata: dict[str, Any] | None = None,
) -> tuple[int, bool]:
    """Insert an artifact row, deduping identical current source docs.

    Returns `(artifact_id, created)` where `created=False` means an identical
    current artifact already existed for `(source, source_document_id)`.
    """

    raw_hash = _sha256(body)
    canonical_hash = _sha256(_canonical_bytes(body, content_type))
    supersedes: int | None = None
    superseded_at = published_at or datetime.now(timezone.utc)

    if source_document_id is not None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, raw_hash, canonical_hash
                FROM artifacts
                WHERE source = %s
                  AND source_document_id = %s
                  AND superseded_at IS NULL
                ORDER BY id DESC
                LIMIT 1;
                """,
                (source, source_document_id),
            )
            existing = cur.fetchone()
        if existing is not None:
            existing_id, existing_raw_hash, existing_canonical_hash = existing
            if existing_raw_hash == raw_hash and existing_canonical_hash == canonical_hash:
                return existing_id, False
            supersedes = existing_id

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO artifacts (
                ingest_run_id, artifact_type, source, source_document_id,
                raw_hash, canonical_hash,
                ticker,
                fiscal_year, fiscal_quarter, fiscal_period_label,
                period_end, period_type,
                calendar_year, calendar_quarter, calendar_period_label,
                title, url, content_type, language,
                published_at, effective_at,
                supersedes,
                artifact_metadata
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s,
                %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s,
                %s
            )
            RETURNING id;
            """,
            (
                ingest_run_id,
                artifact_type,
                source,
                source_document_id,
                raw_hash,
                canonical_hash,
                ticker,
                fiscal_year,
                fiscal_quarter,
                fiscal_period_label,
                period_end,
                period_type,
                calendar_year,
                calendar_quarter,
                calendar_period_label,
                title,
                url,
                content_type,
                language,
                published_at,
                effective_at,
                supersedes,
                Jsonb(artifact_metadata or {}),
            ),
        )
        artifact_id = cur.fetchone()[0]

    if supersedes is not None:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE artifacts
                SET superseded_at = %s
                WHERE id = %s AND superseded_at IS NULL;
                """,
                (superseded_at, supersedes),
            )

    return artifact_id, True
