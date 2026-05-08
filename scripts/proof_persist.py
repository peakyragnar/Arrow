"""Proof: persist the corrected speakered transcript as an ASR-source artifact.

Mirrors src/arrow/agents/fmp_transcripts.py shape:
- artifacts row (artifact_type='transcript', source='asr')
- artifact_text_units rows (one per speaker turn, unit_type='transcript')
- artifact_text_chunks rows (via existing build_text_unit_chunks helper)
- ingest_runs row with vendor='asr'

After this run, querying CRWV transcripts should show this row alongside
the 4 existing FMP rows.
"""

from __future__ import annotations

import hashlib
import json
import re
import sys
from datetime import UTC, date, datetime, time
from pathlib import Path

from dotenv import load_dotenv

REPO = Path(__file__).resolve().parent.parent
load_dotenv(REPO / ".env", override=True)

sys.path.insert(0, str(REPO / "src"))

from arrow.db.connection import get_conn
from arrow.ingest.common.artifacts import write_artifact
from arrow.ingest.common.runs import close_failed, close_succeeded, open_run
from arrow.ingest.sec.qualitative import (
    TEXT_CHUNKER_VERSION,
    TextUnit,
    build_text_unit_chunks,
)
from arrow.normalize.periods.derive import derive_calendar_period

# --- Inputs ---
TICKER = "CRWV"
FISCAL_YEAR = 2026
FISCAL_QUARTER = 1
CALL_DATE = date(2026, 5, 7)
EVENT_ID = "658779279"
AUDIO_URL = (
    "https://static.events.q4inc.com/edited-recordings/658779279/"
    "9149fbc1-b0db-476f-8e08-ef3f4eff300e.mp4"
)

# Files we already produced
WHISPER_JSON = REPO / "data/scratch/transcripts/whisper-turbo/CRWV/FY2026-Q1.json"
DIARIZE_JSON = REPO / "data/scratch/diarize/pyannote-3.1/CRWV/FY2026-Q1.json"
CORRECTED_TXT = REPO / "data/scratch/transcripts/whisper-turbo/CRWV/FY2026-Q1.corrected.txt"
CORRECTED_META = REPO / "data/scratch/transcripts/whisper-turbo/CRWV/FY2026-Q1.corrected.json"
AUDIO_FILE = REPO / "data/scratch/audio/q4inc/CRWV/FY2026-Q1.mp4"

# Speaker resolution (manually identified for this proof — production
# pipeline would resolve via voiceprint match against speaker_voiceprints)
SPEAKER_MAP = {
    "SPEAKER_01": ("operator", "Operator"),
    "SPEAKER_00": ("ir", "Investor Relations"),
    "SPEAKER_04": ("ceo", "Mike Intrator"),
    "SPEAKER_02": ("cfo", "Nitin Agrawal"),
    "SPEAKER_07": ("analyst", "Brent Thill (Jefferies)"),
    "SPEAKER_05": ("analyst", "Mark Murphy (JP Morgan)"),
    "SPEAKER_06": ("analyst", "Tal Liani (Bank of America)"),
    "SPEAKER_03": ("analyst", "Amit Daryanani (Evercore)"),
    "SPEAKER_08": ("analyst", "Keith Weiss / Nihal Chokshi"),  # merged label
}

ASR_BACKEND = "whisper_local"
ASR_MODEL = "whisper-large-v3-turbo"
ASR_MODEL_VERSION = "mlx-community/whisper-large-v3-turbo"
DIAR_MODEL = "pyannote/speaker-diarization-3.1"
LLM_CORRECTOR = "claude-sonnet-4-6"
EXTRACTOR_VERSION = "asr_whisper_turbo_pyannote_3.1_v1"
SOURCE_DOCUMENT_ID = f"asr:q4inc-edited-recording:{TICKER}:FY{FISCAL_YEAR}-Q{FISCAL_QUARTER}"


def _derive_period_end(fiscal_year: int, fiscal_quarter: int, fye_md: str) -> date:
    """Mirror of arrow.agents.fmp_transcripts._derive_period_end_from_fiscal_calendar."""
    import calendar as _cal
    fye_month, fye_day = (int(p) for p in fye_md.split("-"))
    months_back = (4 - fiscal_quarter) * 3
    target_month = fye_month - months_back
    target_year = fiscal_year
    while target_month <= 0:
        target_month += 12
        target_year -= 1
    last_day = _cal.monthrange(target_year, target_month)[1]
    day = min(fye_day, last_day)
    return date(target_year, target_month, day)


# Parser for the corrected transcript output (block format)
TURN_HEADER_RE = re.compile(
    r"^\[(\d{2}):(\d{2})[–-](\d{2}):(\d{2})\]\s+(SPEAKER_\d{2})\s*$"
)


def parse_corrected_blocks(text: str) -> list[dict]:
    """Parse the corrected transcript back into speaker turns."""
    blocks = []
    cur_header = None
    cur_lines: list[str] = []
    for line in text.splitlines():
        m = TURN_HEADER_RE.match(line)
        if m:
            if cur_header is not None:
                blocks.append({
                    "start_sec": cur_header[0] * 60 + cur_header[1],
                    "end_sec": cur_header[2] * 60 + cur_header[3],
                    "speaker_label": cur_header[4],
                    "text": " ".join(s.strip() for s in cur_lines if s.strip()),
                })
            cur_header = (
                int(m.group(1)), int(m.group(2)),
                int(m.group(3)), int(m.group(4)),
                m.group(5),
            )
            cur_lines = []
        else:
            if cur_header is not None:
                cur_lines.append(line)
    if cur_header is not None:
        blocks.append({
            "start_sec": cur_header[0] * 60 + cur_header[1],
            "end_sec": cur_header[2] * 60 + cur_header[3],
            "speaker_label": cur_header[4],
            "text": " ".join(s.strip() for s in cur_lines if s.strip()),
        })
    return blocks


def build_canonical_content(blocks: list[dict]) -> tuple[str, list[tuple[int, int]]]:
    """Assemble canonical content + per-block (start_offset, end_offset).

    Format: each block is `Speaker Name: text\n\n` (FMP-style).
    """
    parts = []
    offsets = []
    cursor = 0
    for b in blocks:
        role, name = SPEAKER_MAP.get(b["speaker_label"], ("unknown", b["speaker_label"]))
        line = f"{name}: {b['text']}\n\n"
        start = cursor
        end = cursor + len(line) - 2  # don't include trailing \n\n in offset bound
        parts.append(line)
        offsets.append((start, end))
        cursor += len(line)
    return "".join(parts), offsets


def main() -> int:
    # Verify all inputs exist
    for p in (WHISPER_JSON, DIARIZE_JSON, CORRECTED_TXT, CORRECTED_META, AUDIO_FILE):
        if not p.exists():
            print(f"ERROR: missing {p}", file=sys.stderr)
            return 1

    whisper = json.loads(WHISPER_JSON.read_text())
    diar = json.loads(DIARIZE_JSON.read_text())
    corrected_text = CORRECTED_TXT.read_text()
    corrected_meta = json.loads(CORRECTED_META.read_text())

    # Audio hash (sha256 of MP4 bytes — provenance)
    h = hashlib.sha256()
    with AUDIO_FILE.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    audio_sha256 = h.hexdigest()
    audio_size = AUDIO_FILE.stat().st_size

    # Parse corrected output back into blocks
    blocks = parse_corrected_blocks(corrected_text)
    print(f"Parsed {len(blocks)} speaker blocks from corrected transcript")
    if not blocks:
        print("ERROR: no blocks parsed — check parser", file=sys.stderr)
        return 1

    canonical_content, offsets = build_canonical_content(blocks)
    print(f"Canonical content: {len(canonical_content):,} chars")

    # Body for write_artifact: a JSON envelope describing the full pipeline output
    body_obj = {
        "schema": "asr_transcript_v1",
        "ticker": TICKER,
        "fiscal_year": FISCAL_YEAR,
        "fiscal_quarter": FISCAL_QUARTER,
        "call_date": CALL_DATE.isoformat(),
        "audio": {
            "source_vendor": "q4inc",
            "source_url": AUDIO_URL,
            "source_event_id": EVENT_ID,
            "sha256": audio_sha256,
            "size_bytes": audio_size,
        },
        "asr": {
            "backend": ASR_BACKEND,
            "model": ASR_MODEL,
            "model_version": ASR_MODEL_VERSION,
            "language": whisper.get("language"),
            "segment_count": len(whisper["segments"]),
        },
        "diarization": {
            "model": DIAR_MODEL,
            "speaker_count": diar["speaker_count"],
            "segment_count": diar["segment_count"],
        },
        "post_correction": {
            "model": LLM_CORRECTOR,
            "input_tokens": corrected_meta["input_tokens"],
            "output_tokens": corrected_meta["output_tokens"],
        },
        "speaker_map": {label: {"role": role, "name": name}
                        for label, (role, name) in SPEAKER_MAP.items()},
        "blocks": blocks,
    }
    body_bytes = json.dumps(body_obj, sort_keys=True, separators=(",", ":")).encode("utf-8")

    # Look up company
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, ticker, fiscal_year_end_md FROM companies WHERE ticker = %s;",
                (TICKER,),
            )
            row = cur.fetchone()
            if not row:
                print(f"ERROR: {TICKER} not in companies", file=sys.stderr)
                return 1
            company_id, ticker, fye_md = row

        period_end = _derive_period_end(FISCAL_YEAR, FISCAL_QUARTER, fye_md)
        calendar = derive_calendar_period(period_end)
        fiscal_period_key = f"FY{FISCAL_YEAR} Q{FISCAL_QUARTER}"
        published_at = datetime.combine(CALL_DATE, time(21, 0), tzinfo=UTC)  # 5pm ET = 21:00 UTC

        run_id = open_run(
            conn,
            run_kind="manual",
            vendor="asr",
            ticker_scope=[TICKER],
        )
        try:
            with conn.transaction():
                artifact_id, created = write_artifact(
                    conn,
                    ingest_run_id=run_id,
                    artifact_type="transcript",
                    source="asr",
                    source_document_id=SOURCE_DOCUMENT_ID,
                    body=body_bytes,
                    canonical_body=canonical_content.encode("utf-8"),
                    company_id=company_id,
                    ticker=TICKER,
                    fiscal_period_key=fiscal_period_key,
                    fiscal_year=FISCAL_YEAR,
                    fiscal_quarter=FISCAL_QUARTER,
                    fiscal_period_label=fiscal_period_key,
                    period_end=period_end,
                    period_type="quarter",
                    calendar_year=calendar.calendar_year,
                    calendar_quarter=calendar.calendar_quarter,
                    calendar_period_label=calendar.calendar_period_label,
                    title=f"{TICKER} earnings call {fiscal_period_key} (ASR)",
                    content_type="application/json",
                    language=whisper.get("language", "en"),
                    published_at=published_at,
                    artifact_metadata={
                        "asr_backend": ASR_BACKEND,
                        "asr_model": ASR_MODEL,
                        "asr_model_version": ASR_MODEL_VERSION,
                        "diarization_model": DIAR_MODEL,
                        "post_correction_model": LLM_CORRECTOR,
                        "audio_source_vendor": "q4inc",
                        "audio_source_url": AUDIO_URL,
                        "audio_source_event_id": EVENT_ID,
                        "audio_sha256": audio_sha256,
                        "speaker_count_detected": diar["speaker_count"],
                        "block_count": len(blocks),
                    },
                )
                print(f"  artifact_id={artifact_id} created={created}")

                if created:
                    # Insert speaker turn TextUnits + chunks
                    text_units_inserted = 0
                    text_chunks_inserted = 0
                    with conn.cursor() as cur:
                        # Clean any prior units for this artifact (no-op for fresh insert)
                        cur.execute(
                            "DELETE FROM artifact_text_chunks WHERE text_unit_id IN ("
                            "  SELECT id FROM artifact_text_units WHERE artifact_id = %s);",
                            (artifact_id,),
                        )
                        cur.execute(
                            "DELETE FROM artifact_text_units WHERE artifact_id = %s;",
                            (artifact_id,),
                        )

                        for ordinal, (block, (start_off, end_off)) in enumerate(
                            zip(blocks, offsets), start=1
                        ):
                            role, name = SPEAKER_MAP.get(
                                block["speaker_label"], ("unknown", block["speaker_label"])
                            )
                            unit = TextUnit(
                                unit_ordinal=ordinal,
                                unit_type="transcript",
                                unit_key=f"turn:{ordinal:03d}",
                                unit_title=name,
                                text=canonical_content[start_off:end_off],
                                start_offset=start_off,
                                end_offset=end_off,
                                confidence=0.9,
                                extraction_method="deterministic",
                            )
                            cur.execute(
                                """
                                INSERT INTO artifact_text_units (
                                    artifact_id, company_id, fiscal_period_key,
                                    unit_ordinal, unit_type, unit_key, unit_title,
                                    text, start_offset, end_offset,
                                    extractor_version, confidence, extraction_method
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s
                                ) RETURNING id;
                                """,
                                (
                                    artifact_id, company_id, fiscal_period_key,
                                    unit.unit_ordinal, unit.unit_type, unit.unit_key, unit.unit_title,
                                    unit.text, unit.start_offset, unit.end_offset,
                                    EXTRACTOR_VERSION, unit.confidence, unit.extraction_method,
                                ),
                            )
                            unit_id = cur.fetchone()[0]
                            text_units_inserted += 1

                            for chunk in build_text_unit_chunks(unit):
                                cur.execute(
                                    """
                                    INSERT INTO artifact_text_chunks (
                                        text_unit_id, chunk_ordinal, text, search_text,
                                        heading_path, start_offset, end_offset, chunker_version
                                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                                    """,
                                    (
                                        unit_id, chunk.chunk_ordinal, chunk.text,
                                        chunk.search_text, chunk.heading_path,
                                        chunk.start_offset, chunk.end_offset,
                                        TEXT_CHUNKER_VERSION,
                                    ),
                                )
                                text_chunks_inserted += 1

                    print(f"  text_units_inserted={text_units_inserted}")
                    print(f"  text_chunks_inserted={text_chunks_inserted}")
                else:
                    text_units_inserted = 0
                    text_chunks_inserted = 0
                    print("  (already-current artifact — no rewrite)")

            close_succeeded(conn, run_id, counts={
                "actor": "operator",
                "artifact_id": artifact_id,
                "created": created,
                "text_units_inserted": text_units_inserted,
                "text_chunks_inserted": text_chunks_inserted,
                "audio_sha256": audio_sha256,
                "audio_source_url": AUDIO_URL,
            })

            # Summary
            print()
            print("Resulting CRWV transcripts:")
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT a.id, a.fiscal_period_label, a.published_at::date, a.source,
                           (SELECT COUNT(*) FROM artifact_text_units WHERE artifact_id = a.id)
                    FROM artifacts a JOIN companies c ON c.id = a.company_id
                    WHERE c.ticker = %s AND a.artifact_type = 'transcript'
                      AND a.superseded_at IS NULL
                    ORDER BY a.published_at DESC NULLS LAST
                    LIMIT 8;
                """, (TICKER,))
                for r in cur.fetchall():
                    print(f"  id={r[0]:5d}  {r[1]:12s}  {r[2]}  source={r[3]}  text_units={r[4]}")

        except Exception as e:
            close_failed(
                conn, run_id,
                error_message=str(e),
                error_details={"kind": type(e).__name__},
            )
            raise

    return 0


if __name__ == "__main__":
    sys.exit(main())
