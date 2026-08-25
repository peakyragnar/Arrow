"""Read-only export of current FMP transcript artifacts.

Contract: ``arrow-transcript-export-v1``

This is a migration boundary. It copies already-cached FMP JSON after
verifying each file SHA-256 against ``artifacts.raw_hash``, recomputing
the canonical hash, and binding payload symbol/year/period/date to the
artifact row. It does not fetch from FMP, and it never writes to
Postgres or ``data/raw/``.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Callable

import psycopg

from arrow.ingest.fmp.paths import fmp_transcript_path

CONTRACT = "arrow-transcript-export-v1"
RECORD_KEYS = (
    "call_date",
    "canonical_sha256",
    "cik",
    "fiscal_quarter",
    "fiscal_year",
    "period_end",
    "provider",
    "published_at_claim",
    "raw_path",
    "raw_sha256",
    "source",
    "source_document_id",
    "ticker",
)
MANIFEST_KEYS = (
    "content_sha256",
    "contract",
    "cutoff",
    "exported_at",
    "record_count",
    "records",
    "source",
)


RawPathResolver = Callable[[str, int, int], Path]


class TranscriptExportError(RuntimeError):
    """Export refused; no complete bundle was published."""


@dataclass(frozen=True)
class TranscriptExportResult:
    output_dir: Path
    contract: str
    ticker: str
    since: date
    exported_at: datetime
    record_count: int
    content_sha256: str
    records: list[dict[str, Any]]


def export_transcripts(
    conn: psycopg.Connection,
    *,
    ticker: str,
    since: date | datetime,
    output_dir: Path | str,
    exported_at: datetime | None = None,
    resolve_raw_path: RawPathResolver | None = None,
    expected_count: int | None = None,
) -> TranscriptExportResult:
    """Write a sealed ``arrow-transcript-export-v1`` directory bundle.

    Selects current (``superseded_at IS NULL``) FMP transcript artifacts
    for ``ticker`` with ``published_at >= since``. Refuses missing cache
    files, raw or canonical hash mismatches, payload metadata that does
    not bind to the artifact, multi-row JSON, missing content, malformed
    metadata, duplicate source document ids, duplicate fiscal slots,
    ``expected_count`` mismatch, and overwriting a nonempty destination.
    """
    ticker_key = ticker.strip().upper()
    if not ticker_key:
        raise TranscriptExportError("ticker is required")

    dest = Path(output_dir)
    _refuse_nonempty_destination(dest)

    since_dt = _as_utc_datetime(since)
    since_cutoff = _cutoff_since(since, since_dt)
    exported_at_utc = _as_utc_datetime(exported_at or datetime.now(timezone.utc))
    resolve = resolve_raw_path or fmp_transcript_path

    _require_company(conn, ticker_key)
    rows = _load_current_fmp_transcripts(conn, ticker=ticker_key, since=since_dt)
    prepared = _prepare_records(rows, ticker=ticker_key, resolve=resolve)
    if expected_count is not None:
        if expected_count < 1:
            raise TranscriptExportError("expected_count must be >= 1")
        if len(prepared) != expected_count:
            raise TranscriptExportError(
                f"expected {expected_count} records, got {len(prepared)}"
            )

    records = [item.record for item in prepared]
    cutoff = {"ticker": ticker_key, "since": since_cutoff}
    source = {
        "artifact_type": "transcript",
        "provider": "fmp",
        "selection": "current",
        "system": "arrow",
    }
    digest_body = {
        "contract": CONTRACT,
        "cutoff": cutoff,
        "record_count": len(records),
        "records": records,
        "source": source,
    }
    content_sha256 = _sha256_hex(_canonical_json_bytes(digest_body))
    manifest = {
        "content_sha256": content_sha256,
        "contract": CONTRACT,
        "cutoff": cutoff,
        "exported_at": _iso_utc(exported_at_utc),
        "record_count": len(records),
        "records": records,
        "source": source,
    }

    _write_bundle_atomically(dest, prepared=prepared, manifest=manifest)

    since_date = since if isinstance(since, date) and not isinstance(since, datetime) else since_dt.date()
    return TranscriptExportResult(
        output_dir=dest.resolve(),
        contract=CONTRACT,
        ticker=ticker_key,
        since=since_date,
        exported_at=exported_at_utc,
        record_count=len(records),
        content_sha256=content_sha256,
        records=records,
    )


def _require_company(conn: psycopg.Connection, ticker: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT cik FROM companies WHERE ticker = %s;", (ticker,))
        row = cur.fetchone()
    if row is None:
        raise TranscriptExportError(f"{ticker} not in companies — cannot export transcripts")
    cik = row[0]
    if cik is None:
        raise TranscriptExportError(f"{ticker} has no CIK")
    return int(cik)


def _load_current_fmp_transcripts(
    conn: psycopg.Connection,
    *,
    ticker: str,
    since: datetime,
) -> list[tuple[Any, ...]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                a.ticker,
                c.ticker,
                c.cik,
                a.fiscal_year,
                a.fiscal_quarter,
                a.period_end,
                a.published_at,
                a.source,
                a.source_document_id,
                a.raw_hash,
                a.canonical_hash
            FROM artifacts a
            LEFT JOIN companies c ON c.id = a.company_id
            WHERE a.artifact_type = 'transcript'
              AND a.source = 'fmp'
              AND a.superseded_at IS NULL
              AND a.ticker = %s
              AND a.published_at >= %s
            ORDER BY a.fiscal_year ASC, a.fiscal_quarter ASC, a.source_document_id ASC;
            """,
            (ticker, since),
        )
        return list(cur.fetchall())


@dataclass(frozen=True)
class _Prepared:
    record: dict[str, Any]
    body: bytes


def _prepare_records(
    rows: list[tuple[Any, ...]],
    *,
    ticker: str,
    resolve: RawPathResolver,
) -> list[_Prepared]:
    prepared: list[_Prepared] = []
    seen_ids: dict[str, str] = {}
    seen_slots: dict[tuple[int, int], str] = {}

    for row in rows:
        record, body = _record_from_row(row, ticker=ticker, resolve=resolve)
        source_document_id = record["source_document_id"]
        slot = (record["fiscal_year"], record["fiscal_quarter"])
        if source_document_id in seen_ids:
            raise TranscriptExportError(
                f"duplicate source_document_id: {source_document_id}"
            )
        if slot in seen_slots:
            raise TranscriptExportError(
                f"duplicate fiscal slot {record['fiscal_year']} {record['fiscal_quarter']}"
            )
        seen_ids[source_document_id] = source_document_id
        seen_slots[slot] = source_document_id
        prepared.append(_Prepared(record=record, body=body))

    prepared.sort(
        key=lambda item: (
            item.record["fiscal_year"],
            item.record["fiscal_quarter"],
            item.record["source_document_id"],
        )
    )
    return prepared


def _record_from_row(
    row: tuple[Any, ...],
    *,
    ticker: str,
    resolve: RawPathResolver,
) -> tuple[dict[str, Any], bytes]:
    (
        artifact_ticker,
        company_ticker,
        cik,
        fiscal_year,
        fiscal_quarter,
        period_end,
        published_at,
        source,
        source_document_id,
        raw_hash,
        canonical_hash,
    ) = row

    problems: list[str] = []
    art_ticker = (artifact_ticker or "").strip().upper()
    if art_ticker != ticker:
        problems.append(f"ticker {artifact_ticker!r} does not match {ticker}")
    if company_ticker is not None and str(company_ticker).strip().upper() != ticker:
        problems.append(f"company ticker {company_ticker!r} does not match {ticker}")
    if cik is None:
        problems.append("missing CIK")
    if fiscal_year is None:
        problems.append("missing fiscal_year")
    try:
        year = int(fiscal_year)
    except (TypeError, ValueError):
        year = -1
        problems.append(f"malformed fiscal_year {fiscal_year!r}")
    try:
        quarter = int(fiscal_quarter)
    except (TypeError, ValueError):
        quarter = -1
        problems.append(f"malformed fiscal_quarter {fiscal_quarter!r}")
    if quarter not in (1, 2, 3, 4):
        problems.append(f"fiscal_quarter must be 1..4, got {fiscal_quarter!r}")
    if period_end is None:
        problems.append("missing period_end")
    if published_at is None:
        problems.append("missing published_at")
    if not source_document_id or not str(source_document_id).strip():
        problems.append("missing source_document_id")
    if source != "fmp":
        problems.append(f"source must be 'fmp', got {source!r}")

    raw_hex = _digest_hex(raw_hash, field="raw_hash", problems=problems)
    canonical_hex = _digest_hex(
        canonical_hash, field="canonical_hash", problems=problems
    )
    if problems:
        identity = str(source_document_id or f"{ticker} FY{fiscal_year} Q{fiscal_quarter}")
        raise TranscriptExportError(
            f"malformed transcript metadata for {identity}: {'; '.join(problems)}"
        )

    published = _as_utc_datetime(published_at)
    period = period_end if isinstance(period_end, date) else date.fromisoformat(str(period_end))
    quarter_label = f"Q{quarter}"
    source_id = str(source_document_id).strip()
    cache_path = resolve(ticker, year, quarter)
    if not cache_path.is_file():
        raise TranscriptExportError(f"missing cache file: {cache_path}")
    body = cache_path.read_bytes()
    file_digest = hashlib.sha256(body).digest()
    expected = bytes.fromhex(raw_hex)
    if file_digest != expected:
        raise TranscriptExportError(
            f"raw hash mismatch for {cache_path}: "
            f"file={file_digest.hex()} artifact={raw_hex}"
        )

    parsed = _parse_singleton_payload(body, identity=source_id)
    _bind_payload_to_artifact(
        parsed[0],
        ticker=ticker,
        year=year,
        quarter_label=quarter_label,
        call_date=published.date(),
        identity=source_id,
    )
    canonical_bytes = _canonical_artifact_bytes(parsed)
    canonical_digest = hashlib.sha256(canonical_bytes).digest()
    expected_canonical = bytes.fromhex(canonical_hex)
    if canonical_digest != expected_canonical:
        raise TranscriptExportError(
            f"canonical hash mismatch for {cache_path}: "
            f"recomputed={canonical_digest.hex()} artifact={canonical_hex}"
        )

    record = {
        "call_date": published.date().isoformat(),
        "canonical_sha256": canonical_digest.hex(),
        "cik": int(cik),
        "fiscal_quarter": quarter_label,
        "fiscal_year": year,
        "period_end": period.isoformat(),
        "provider": "fmp",
        "published_at_claim": _iso_utc(published),
        "raw_path": f"raw/FY{year}-Q{quarter}.json",
        "raw_sha256": raw_hex,
        "source": "fmp",
        "source_document_id": source_id,
        "ticker": ticker,
    }
    return record, body


def _parse_singleton_payload(body: bytes, *, identity: str) -> list[Any]:
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise TranscriptExportError(
            f"raw JSON is not valid for {identity}: {exc}"
        ) from exc
    if not isinstance(parsed, list):
        raise TranscriptExportError(
            f"expected singleton JSON array for {identity}, got {type(parsed).__name__}"
        )
    if len(parsed) != 1:
        raise TranscriptExportError(
            f"expected singleton JSON array for {identity}, got {len(parsed)} rows"
        )
    if not isinstance(parsed[0], dict):
        raise TranscriptExportError(
            f"expected one JSON object in array for {identity}, "
            f"got {type(parsed[0]).__name__}"
        )
    return parsed


def _bind_payload_to_artifact(
    payload: dict[str, Any],
    *,
    ticker: str,
    year: int,
    quarter_label: str,
    call_date: date,
    identity: str,
) -> None:
    problems: list[str] = []
    symbol_raw = payload.get("symbol")
    if symbol_raw is None or str(symbol_raw).strip() == "":
        problems.append("missing symbol")
    elif str(symbol_raw).strip().upper() != ticker:
        problems.append(f"symbol {symbol_raw!r} does not match ticker {ticker}")

    try:
        payload_year = int(payload["year"])
    except (KeyError, TypeError, ValueError):
        problems.append(f"missing or malformed year {payload.get('year')!r}")
    else:
        if payload_year != year:
            problems.append(f"year {payload_year} does not match fiscal_year {year}")

    period_raw = payload.get("period")
    if period_raw is None or str(period_raw).strip() == "":
        problems.append("missing period")
    else:
        period = str(period_raw).strip().upper()
        if period != quarter_label:
            problems.append(
                f"period {period_raw!r} does not match fiscal_quarter {quarter_label}"
            )

    date_raw = payload.get("date")
    if date_raw is None or str(date_raw).strip() == "":
        problems.append("missing date")
    else:
        date_text = str(date_raw).strip()
        try:
            payload_date = date.fromisoformat(date_text[:10])
        except ValueError:
            problems.append(f"malformed date {date_raw!r}")
        else:
            if payload_date != call_date:
                problems.append(
                    f"date {payload_date.isoformat()} does not match call_date "
                    f"{call_date.isoformat()}"
                )

    content = payload.get("content")
    if not isinstance(content, str) or not content.strip():
        problems.append("missing content")

    if problems:
        raise TranscriptExportError(
            f"payload metadata mismatch for {identity}: {'; '.join(problems)}"
        )


def _canonical_artifact_bytes(parsed: Any) -> bytes:
    return json.dumps(parsed, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _write_bundle_atomically(
    output_dir: Path,
    *,
    prepared: list[_Prepared],
    manifest: dict[str, Any],
) -> None:
    _refuse_nonempty_destination(output_dir)
    parent = output_dir.parent if output_dir.parent.as_posix() != "" else Path(".")
    parent.mkdir(parents=True, exist_ok=True)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{output_dir.name}.export-tmp-", dir=str(parent))
    )
    published = False
    try:
        raw_dir = staging / "raw"
        raw_dir.mkdir()
        for item in prepared:
            raw_path = item.record["raw_path"]
            if not raw_path.startswith("raw/") or "/" in raw_path[4:]:
                raise TranscriptExportError(f"refusing unsafe raw_path {raw_path!r}")
            dest_file = staging / raw_path
            dest_file.write_bytes(item.body)
        manifest_bytes = (
            json.dumps(manifest, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
        ).encode("utf-8")
        (staging / "manifest.json").write_bytes(manifest_bytes)

        _refuse_nonempty_destination(output_dir)
        if output_dir.exists():
            output_dir.rmdir()
        os.replace(staging, output_dir)
        published = True
    finally:
        if not published and staging.exists():
            shutil.rmtree(staging, ignore_errors=True)


def _refuse_nonempty_destination(output_dir: Path) -> None:
    if not output_dir.exists():
        return
    if not output_dir.is_dir():
        raise TranscriptExportError(
            f"refusing to overwrite existing path that is not a directory: {output_dir}"
        )
    if any(output_dir.iterdir()):
        raise TranscriptExportError(
            f"refusing to overwrite nonempty directory: {output_dir}"
        )


def _as_utc_datetime(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return datetime.combine(value, time.min, tzinfo=timezone.utc)


def _cutoff_since(original: date | datetime, since_dt: datetime) -> str:
    if isinstance(original, datetime):
        return _iso_utc(since_dt)
    return original.isoformat()


def _iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _canonical_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")


def _sha256_hex(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _digest_hex(value: Any, *, field: str, problems: list[str]) -> str:
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, bytearray):
        value = bytes(value)
    if not isinstance(value, bytes) or len(value) != 32:
        problems.append(f"{field} must be a 32-byte SHA-256 digest")
        return ""
    return value.hex()
