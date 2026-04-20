"""raw_responses writer — hash twice, write to DB, mirror to filesystem.

Per docs/decisions/0005-raw-responses-storage-split.md:
    JSON     -> body_jsonb
    non-JSON -> body_raw

Per docs/architecture/system.md:
    raw_hash       = SHA-256 of bytes-as-received
    canonical_hash = SHA-256 of canonicalized representation
                     (JSON: sorted keys, compact separators)
    params_hash    = SHA-256 of canonicalized params JSON

Append-only: every fetch produces a new row. Re-fetching identical bytes
produces a new row whose raw_hash matches the prior one — that's the
polling-log-as-evidence principle at work.

Ordering inside the enclosing transaction: INSERT first, filesystem write
second. If the file write fails, the transaction rolls back the row —
preferable to a DB row pointing at a file that doesn't exist.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import psycopg
from psycopg.types.json import Jsonb


def _sha256(b: bytes) -> bytes:
    return hashlib.sha256(b).digest()


def canonical_json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")


def hash_params(params: dict[str, Any]) -> bytes:
    return _sha256(canonical_json_bytes(params))


def write_raw_response(
    conn: psycopg.Connection,
    *,
    ingest_run_id: int,
    vendor: str,
    endpoint: str,
    params: dict[str, Any],
    request_url: str,
    http_status: int,
    content_type: str,
    response_headers: dict[str, str] | None,
    body: bytes,
    cache_path: Path | None = None,
) -> int:
    """Insert a raw_responses row; optionally mirror bytes to the filesystem.

    Returns the new row id. Assumes the caller has an open transaction on
    `conn` (e.g. via `with conn.transaction():`). On filesystem failure,
    the caller's transaction rolls back the inserted row.
    """
    is_json = "application/json" in content_type.lower()

    raw_hash = _sha256(body)
    if is_json:
        parsed = json.loads(body)
        canonical_hash = _sha256(canonical_json_bytes(parsed))
        body_jsonb: Any = parsed
        body_raw: bytes | None = None
    else:
        canonical_hash = raw_hash  # conservative default for non-JSON
        body_jsonb = None
        body_raw = body

    params_hash = hash_params(params)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw_responses (
                ingest_run_id, vendor, endpoint, params, params_hash,
                request_url, http_status, content_type, response_headers,
                body_jsonb, body_raw, raw_hash, canonical_hash
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s
            ) RETURNING id;
            """,
            (
                ingest_run_id,
                vendor,
                endpoint,
                Jsonb(params),
                params_hash,
                request_url,
                http_status,
                content_type,
                Jsonb(response_headers) if response_headers is not None else None,
                Jsonb(body_jsonb) if body_jsonb is not None else None,
                body_raw,
                raw_hash,
                canonical_hash,
            ),
        )
        row_id = cur.fetchone()[0]

    if cache_path is not None:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_bytes(body)

    return row_id
