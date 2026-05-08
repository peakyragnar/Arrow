"""ASR earnings-call transcript ingest orchestration.

End-to-end pipeline for one (ticker, fiscal period) call:
    1. Acquire audio  (q4inc adapter → Playwright UUID discovery,
                       falling through to operator-pasted URL)
    2. Transcribe     (mlx-whisper large-v3-turbo via subprocess)
    3. Diarize        (pyannote 3.1 + speaker embeddings)
    4. Identify       (auto-resolve operator/IR/CEO/CFO from structural
                       cues; analysts from operator's "next question
                       comes from" pattern; voiceprint match if enrolled)
    5. Correct        (conservative Claude pass — proper-noun mishears)
    6. Persist        (audio_artifacts + asr_transcripts + speaker_segments
                       + speaker_voiceprints, plus artifact + text_units +
                       text_chunks mirror of the FMP path)
    7. Cleanup        (delete audio binary, stamp audio_artifacts.deleted_at)

This is the v1 production-shaped path. The earlier proof_*.py scripts in
scripts/ were the design-validation pass for one specific call.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timezone
from pathlib import Path
from typing import Any

import psycopg

from arrow.ingest.audio.contracts import AudioFetch, AudioRef
from arrow.ingest.audio.download import download_audio
from arrow.ingest.audio.generic import accept_pasted_url as accept_generic_pasted_url
from arrow.ingest.audio.q4inc import accept_pasted_url, discover_audio_url
from arrow.ingest.audio.youtube import (
    accept_video as accept_youtube_video,
    download_youtube_audio,
)
from arrow.ingest.common.artifacts import write_artifact
from arrow.ingest.common.runs import close_failed, close_succeeded, open_run
from arrow.ingest.sec.qualitative import (
    TEXT_CHUNKER_VERSION,
    TextUnit,
    build_text_unit_chunks,
)
from arrow.normalize.periods.derive import derive_calendar_period

ASR_BACKEND = "whisper_local"
ASR_MODEL = "whisper-large-v3-turbo"
ASR_MODEL_VERSION = "mlx-community/whisper-large-v3-turbo"
DIAR_MODEL = "pyannote/speaker-diarization-3.1"
DIAR_EMBEDDING_DIM = 192
LLM_CORRECTOR = "claude-sonnet-4-6"
EXTRACTOR_VERSION = "asr_whisper_turbo_pyannote_3.1_v1"


@dataclass(frozen=True)
class ASRIngestResult:
    audio_artifact_id: int
    asr_transcript_id: int
    artifact_id: int
    artifact_created: bool
    text_units_inserted: int
    text_chunks_inserted: int
    speaker_segments_inserted: int
    voiceprints_enrolled: int
    audio_deleted: bool


# ---- Audio acquisition --------------------------------------------------

def acquire_audio(
    *,
    ticker: str,
    fiscal_period_key: str,
    q4_event_id: str | None,
    pasted_url: str | None,
    youtube_id: str | None,
    scratch_dir: Path,
    headless: bool = False,
) -> AudioFetch:
    """Resolve audio source and download. Vendor-aware:

    1. youtube_id → YouTube via yt-dlp (encrypted-HLS fallback)
    2. q4_event_id → Q4 player via Playwright (auto-discover .mp4 URL)
    3. pasted_url → operator-grabbed URL (Q4 strict, else generic)
    """
    if youtube_id:
        audio_ref = accept_youtube_video(youtube_id)
        dest = (scratch_dir / "audio" / "youtube" / ticker
                / f"{fiscal_period_key.replace(' ', '-')}")
        print(f"  [audio] downloading YouTube video {audio_ref.event_id} via yt-dlp...")
        fetch = download_youtube_audio(audio_ref, dest_path=dest)
        print(f"  [audio] {fetch.audio_size_bytes/1e6:.1f} MB, sha256={fetch.audio_hash_sha256[:16]}...")
        return fetch

    audio_ref: AudioRef | None = None
    if pasted_url:
        try:
            audio_ref = accept_pasted_url(pasted_url, expected_event_id=q4_event_id)
            print(f"  [audio] using Q4-pasted URL (event_id={audio_ref.event_id})")
        except ValueError:
            audio_ref = accept_generic_pasted_url(pasted_url, vendor="manual")
            print(f"  [audio] non-Q4 URL accepted as vendor=manual")
    elif q4_event_id:
        print(f"  [audio] discovering URL via Playwright (event_id={q4_event_id})...")
        audio_ref = discover_audio_url(q4_event_id, headless=headless)
        if audio_ref is None:
            raise RuntimeError(
                f"Playwright failed to discover audio URL for Q4 event {q4_event_id}. "
                f"Re-run with --audio-url <pasted-url> as fallback."
            )
        print(f"  [audio] discovered {audio_ref.source_url}")
    else:
        raise ValueError("Provide --q4-event-id, --audio-url, or --youtube-id")

    url_lower = audio_ref.source_url.split("?", 1)[0].lower()
    ext = "ts" if url_lower.endswith(".ts") else "mp4"
    dest = (scratch_dir / "audio" / audio_ref.vendor / ticker
            / f"{fiscal_period_key.replace(' ', '-')}.{ext}")
    print(f"  [audio] downloading to {dest}")
    fetch = download_audio(audio_ref, dest_path=dest)
    print(f"  [audio] {fetch.audio_size_bytes/1e6:.1f} MB, sha256={fetch.audio_hash_sha256[:16]}...")
    return fetch


# Back-compat alias (callers used acquire_q4_audio in earlier commits)
acquire_q4_audio = acquire_audio


# ---- ASR ----------------------------------------------------------------

def run_whisper_local(
    *,
    audio_path: Path,
    out_dir: Path,
    initial_prompt: str,
    language: str = "en",
) -> tuple[Path, dict]:
    """Invoke mlx_whisper as a subprocess; return (json_path, parsed_data)."""
    out_dir.mkdir(parents=True, exist_ok=True)

    # mlx_whisper ships a console-script in .venv/bin/, not a __main__ module
    mlx_bin = Path(sys.executable).parent / "mlx_whisper"
    if not mlx_bin.exists():
        raise RuntimeError(
            f"mlx_whisper binary not found at {mlx_bin} — pip install mlx-whisper"
        )
    cmd = [
        str(mlx_bin),
        str(audio_path),
        "--model", ASR_MODEL_VERSION,
        "--output-format", "json",
        "--output-dir", str(out_dir),
        "--word-timestamps", "True",
        "--temperature", "0",
        "--condition-on-previous-text", "False",
        "--initial-prompt", initial_prompt,
        "--language", language,
    ]
    print(f"  [whisper] running {ASR_MODEL_VERSION}...")
    subprocess.run(cmd, check=True)

    out_path = out_dir / f"{audio_path.stem}.json"
    if not out_path.exists():
        raise RuntimeError(f"Expected {out_path} after mlx_whisper run")
    data = json.loads(out_path.read_text())
    print(f"  [whisper] {len(data['segments'])} segments, {sum(len(s.get('words', [])) for s in data['segments'])} words")
    return out_path, data


# ---- Diarization --------------------------------------------------------

def extract_wav_16khz_mono(audio_path: Path, wav_path: Path) -> None:
    """Extract 16kHz mono WAV via ffmpeg (pyannote's preferred input format)."""
    wav_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "ffmpeg", "-y", "-i", str(audio_path),
            "-ar", "16000", "-ac", "1",
            str(wav_path),
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def run_pyannote_diarization(wav_path: Path, hf_token: str) -> dict:
    """Diarize a WAV file. Returns {segments, overlap_segments, embeddings_by_speaker, ...}."""
    print(f"  [diarize] loading pyannote/speaker-diarization-3.1...")
    from pyannote.audio import Pipeline
    pipeline = Pipeline.from_pretrained(
        "pyannote/speaker-diarization-3.1",
        token=hf_token,
    )
    try:
        import torch
        if torch.backends.mps.is_available():
            pipeline.to(torch.device("mps"))
    except Exception:
        pass

    print(f"  [diarize] running on {wav_path.name}...")
    out = pipeline(str(wav_path))
    annotation = out.exclusive_speaker_diarization
    overlap = out.speaker_diarization
    embeddings = out.speaker_embeddings

    segments = []
    speakers = set()
    for turn, _, speaker in annotation.itertracks(yield_label=True):
        segments.append({
            "start": float(turn.start),
            "end": float(turn.end),
            "speaker": speaker,
        })
        speakers.add(speaker)

    overlap_segments = [
        {"start": float(t.start), "end": float(t.end), "speaker": sp}
        for t, _, sp in overlap.itertracks(yield_label=True)
    ]

    embeddings_by_speaker: dict[str, list[float]] = {}
    if embeddings is not None:
        for i, label in enumerate(annotation.labels()):
            if i < len(embeddings):
                embeddings_by_speaker[label] = embeddings[i].tolist()

    print(f"  [diarize] {len(speakers)} speakers, {len(segments)} segments")
    return {
        "segments": segments,
        "overlap_segments": overlap_segments,
        "embeddings_by_speaker": embeddings_by_speaker,
        "speakers": sorted(speakers),
    }


# ---- Whisper × diarization fusion --------------------------------------

def fuse_whisper_diarize(whisper: dict, diar: dict) -> list[dict]:
    """For each Whisper segment, assign dominant overlapping speaker.

    Returns list of {start, end, speaker, speaker_confidence, text}.
    """
    diar_segs = sorted(diar["segments"], key=lambda d: d["start"])
    fused = []
    for seg in whisper["segments"]:
        s_start, s_end = seg["start"], seg["end"]
        by_speaker: dict[str, float] = {}
        for d in diar_segs:
            if d["start"] > s_end:
                break
            if d["end"] < s_start:
                continue
            ov = max(0.0, min(s_end, d["end"]) - max(s_start, d["start"]))
            if ov > 0:
                by_speaker[d["speaker"]] = by_speaker.get(d["speaker"], 0.0) + ov
        if by_speaker:
            top = max(by_speaker.items(), key=lambda kv: kv[1])
            seg_len = max(0.001, s_end - s_start)
            speaker, conf = top[0], top[1] / seg_len
        else:
            speaker, conf = "UNKNOWN", 0.0
        fused.append({
            "start": s_start,
            "end": s_end,
            "speaker": speaker,
            "speaker_confidence": round(conf, 3),
            "text": seg["text"].strip(),
        })
    return fused


# ---- Speaker identification --------------------------------------------

_HANDOFF_RE = re.compile(
    r"\b(?:turn|hand|give|pass)\s+(?:it|the\s+call|things|the\s+conference|over)?"
    r"\s*(?:over)?\s+to\s+([A-Z][a-zA-Z]+)",
    re.IGNORECASE,
)

# IR introductions take many forms across issuers:
#   AMD:  "Participants on today's conference call are Dr. Lisa Su, our
#          Chair and CEO, and Jean Hu, Executive Vice President, CFO,
#          and Treasurer."
#   CRWV: "Joining the call today to discuss our results are Mike Intrator,
#          CEO and Nitin Agrawal, CFO."
# Strategy: locate the "CEO"/"CFO" title literal, then take the closest
# preceding 1-3-token capitalized name (stripping a "Dr." prefix). The
# bounded interstitial allows for "our Chair and", "Executive Vice
# President,", etc. without bleeding into the previous person's name.

_EXEC_NAME_TOKEN = r"(?:[A-Z][a-z]+)"
_EXEC_NAME = rf"(?:Dr\.\s+)?({_EXEC_NAME_TOKEN}(?:\s+{_EXEC_NAME_TOKEN}){{1,3}})"
_EXEC_INTERSTITIAL = r"(?:[a-zA-Z][\w,\s\-]{0,80}?)?"

_CEO_PAT = re.compile(
    rf"{_EXEC_NAME}\s*,\s*"                                         # name + comma
    rf"(?:our\s+)?(?:Chair\s+(?:and|&)\s+)?(?:Co-?)?"                # optional qualifier
    rf"(?:CEO|Chief\s+Executive\s+Officer)\b",
    # Deliberately not IGNORECASE: in IGNORECASE mode [A-Z][a-z] also
    # matches lowercase words, which makes the name capture run away.
    # Whisper reliably emits "CEO"/"CFO" in caps.
)
_CFO_PAT = re.compile(
    rf"{_EXEC_NAME}\s*,\s*"                                         # name + comma
    rf"{_EXEC_INTERSTITIAL}\s*"                                     # title qualifiers
    rf"(?:CFO|Chief\s+Financial\s+Officer)\b",
)


def _extract_exec_names(ir_text: str) -> dict[str, str]:
    """Pull (CEO, name) and (CFO, name) out of IR's intro text.

    CFO search starts after the CEO match end position, so the CFO regex
    can't trample the CEO's name through a long interstitial.
    """
    out: dict[str, str] = {}
    m_ceo = _CEO_PAT.search(ir_text)
    cfo_start = 0
    if m_ceo:
        out["ceo"] = m_ceo.group(1).strip()
        cfo_start = m_ceo.end()
    m_cfo = _CFO_PAT.search(ir_text, cfo_start)
    if m_cfo:
        out["cfo"] = m_cfo.group(1).strip()
    return out


def identify_speakers(
    fused_segments: list[dict],
    *,
    company_id: int,
    conn: psycopg.Connection,
    voiceprint_embeddings: dict[str, list[float]],
) -> dict[str, dict]:
    """Auto-resolve pyannote speaker labels into named roles.

    State-machine traversal of the conversation:
      1. First speaker = Operator
      2. After operator's "now I will hand the conference over to..." → IR
      3. After IR's handoff phrase ("hand/turn the call over to <Name>") → CEO
      4. After CEO's handoff phrase ("now I'll turn it over to <Name>") → CFO
      5. Operator's "next question comes from <Name>" → analyst label

    Names are extracted from IR's intro text using regex on AMD-style
    ("Participants on today's call are Dr. Lisa Su, CEO, and Jean Hu, CFO")
    and CRWV-style ("Joining the call today are Mike Intrator, CEO and
    Nitin Agrawal, CFO") patterns.

    Voiceprint match (cosine vs. enrolled embeddings) overrides structural
    if confidence ≥ 0.55.

    Returns {SPEAKER_NN: {role, name, source: 'structural'|'voiceprint'|'unknown'}}
    """
    speaker_label_to_info: dict[str, dict] = {}

    # 1. Operator = first speaker
    if not fused_segments:
        return speaker_label_to_info
    op_label = fused_segments[0]["speaker"]
    speaker_label_to_info[op_label] = {
        "role": "operator", "name": "Operator", "source": "structural",
    }

    # Build per-label first-block-of-text snapshots so we can detect the
    # IR's intro by looking at each label's first long block.
    text_by_label_in_order: list[tuple[str, str]] = []  # (label, full_text_until_speaker_change)
    cur_label, cur_text = None, []
    for s in fused_segments:
        if s["speaker"] == cur_label:
            cur_text.append(s["text"])
        else:
            if cur_label is not None:
                text_by_label_in_order.append((cur_label, " ".join(cur_text)))
            cur_label = s["speaker"]
            cur_text = [s["text"]]
    if cur_label is not None:
        text_by_label_in_order.append((cur_label, " ".join(cur_text)))

    # 2-4. State machine: operator → IR → CEO → CFO
    state = "after_operator"  # next non-operator label is IR
    extracted_names: dict[str, str] = {}
    for label, text in text_by_label_in_order:
        if label == op_label:
            continue
        text_lower = text.lower()

        if state == "after_operator":
            # This label is IR. Try to extract CEO/CFO names from their text.
            speaker_label_to_info.setdefault(label, {
                "role": "ir", "name": "Investor Relations", "source": "structural",
            })
            extracted_names = _extract_exec_names(text)
            # Move state if IR did a handoff
            if _HANDOFF_RE.search(text_lower):
                state = "after_ir"

        elif state == "after_ir":
            # First non-operator/non-IR after IR's handoff = CEO
            ceo_name = extracted_names.get("ceo", "<unknown CEO>")
            speaker_label_to_info.setdefault(label, {
                "role": "ceo", "name": ceo_name, "source": "structural",
            })
            if _HANDOFF_RE.search(text_lower):
                state = "after_ceo"

        elif state == "after_ceo":
            # CEO handed off → next non-operator/non-prior-label = CFO
            already = speaker_label_to_info.get(label)
            if already is None or already.get("role") in (None, "unknown"):
                cfo_name = extracted_names.get("cfo", "<unknown CFO>")
                speaker_label_to_info[label] = {
                    "role": "cfo", "name": cfo_name, "source": "structural",
                }
            state = "in_qa"
            # Any further text from this point is Q&A; analyst name pass below
            break  # done with linear pass; remaining labels handled by Q&A pass

    # 5. Analysts from "next question comes from"
    analyst_intros: list[tuple[int, str]] = []  # (seg_idx, name)
    for i, s in enumerate(fused_segments):
        m = re.search(
            r"(?:next|first) question comes from (?:the line of )?([A-Z][a-zA-Z]+ [A-Z][a-zA-Z]+(?:\s+(?:from|of)\s+[A-Z][\w\s&\.]+)?)",
            s["text"],
        )
        if m:
            analyst_intros.append((i, m.group(1).strip().rstrip(".,")))
    for intro_i, analyst_name in analyst_intros:
        for j in range(intro_i + 1, min(intro_i + 8, len(fused_segments))):
            cand_label = fused_segments[j]["speaker"]
            existing = speaker_label_to_info.get(cand_label)
            if existing is None:
                speaker_label_to_info[cand_label] = {
                    "role": "analyst", "name": analyst_name, "source": "structural",
                }
                break
            elif existing.get("role") == "analyst" and existing.get("name", "").startswith("<"):
                speaker_label_to_info[cand_label] = {
                    "role": "analyst", "name": analyst_name, "source": "structural",
                }
                break
            elif existing.get("role") in ("operator", "ir", "ceo", "cfo"):
                # Skip — known exec/operator label, even if pyannote confused
                # this with an analyst's brief turn.
                continue
            else:
                break

    # 6. Voiceprint match for each speaker — overrides structural where confident
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, person_name, role, embedding
            FROM speaker_voiceprints
            WHERE company_id = %s AND superseded_at IS NULL;
            """,
            (company_id,),
        )
        enrolled = cur.fetchall()

    if enrolled and voiceprint_embeddings:
        import math
        def cosine(a, b):
            if not a or not b or len(a) != len(b):
                return 0.0
            dot = sum(x * y for x, y in zip(a, b))
            na = math.sqrt(sum(x * x for x in a))
            nb = math.sqrt(sum(y * y for y in b))
            return dot / (na * nb) if na and nb else 0.0

        for label, embedding in voiceprint_embeddings.items():
            best = (None, 0.0)
            for vid, name, role, vembed in enrolled:
                sim = cosine(embedding, list(vembed))
                if sim > best[1]:
                    best = ((vid, name, role), sim)
            if best[0] and best[1] >= 0.55:
                vid, name, role = best[0]
                speaker_label_to_info[label] = {
                    "role": role, "name": name, "source": "voiceprint",
                    "voiceprint_id": vid, "voiceprint_confidence": round(best[1], 3),
                }

    # Fill in unknowns
    for s in fused_segments:
        if s["speaker"] not in speaker_label_to_info:
            speaker_label_to_info[s["speaker"]] = {
                "role": "other", "name": s["speaker"], "source": "unknown",
            }

    return speaker_label_to_info


# ---- LLM post-correction ------------------------------------------------

def post_correct_with_llm(
    *,
    fused_segments: list[dict],
    speaker_map: dict[str, dict],
    glossary: dict[str, Any],
    api_key: str,
) -> str:
    """Run Claude correction on the speakered transcript. Returns corrected text."""
    # Compress fused into speaker blocks, with named labels
    blocks = []
    cur = None
    for s in fused_segments:
        info = speaker_map.get(s["speaker"], {})
        name = info.get("name", s["speaker"])
        if cur and cur["speaker_label"] == s["speaker"]:
            cur["end"] = s["end"]
            cur["texts"].append(s["text"].strip())
        else:
            if cur:
                blocks.append(cur)
            cur = {"speaker_label": s["speaker"], "name": name,
                   "start": s["start"], "end": s["end"], "texts": [s["text"].strip()]}
    if cur:
        blocks.append(cur)

    def fmt(t: float) -> str:
        m = int(t // 60); ss = t - m * 60
        return f"{m:02d}:{int(ss):02d}"

    block_strs = []
    for b in blocks:
        block_strs.append(f"[{fmt(b['start'])}–{fmt(b['end'])}]  {b['name']}")
        block_strs.append("  " + " ".join(b["texts"]))
        block_strs.append("")
    transcript = "\n".join(block_strs)

    prompt = (
        "You are correcting an automatic-speech-recognition transcript of an earnings "
        "call. Your job is narrow:\n\n"
        "**Fix only:**\n"
        "- Misspelled proper nouns (people's names, company names, product names, ticker symbols)\n"
        "- Obviously misheard technical terms (e.g., 'in video' -> 'NVIDIA')\n\n"
        "**Do NOT:**\n"
        "- Rewrite for clarity, fluency, or grammar\n"
        "- Remove disfluencies, restarts, or filler words\n"
        "- Change punctuation unless it makes a sentence meaningless\n"
        "- Add or remove sentences\n"
        "- Combine or split speaker turns\n\n"
        f"**Glossary for this call:**\n```json\n{json.dumps(glossary, indent=2)}\n```\n\n"
        "**Format:** Return the corrected transcript verbatim with all speaker labels and "
        "timestamps preserved. No commentary, no headers — only the corrected transcript.\n\n"
        f"---\n\n{transcript}\n"
    )

    print(f"  [correct] calling Claude (input {len(transcript):,} chars)...")
    from anthropic import Anthropic
    client = Anthropic(api_key=api_key)
    msg = client.messages.create(
        model=LLM_CORRECTOR,
        max_tokens=16000,
        messages=[{"role": "user", "content": prompt}],
    )
    corrected = msg.content[0].text
    print(f"  [correct] {msg.usage.input_tokens} in / {msg.usage.output_tokens} out tokens")
    return corrected


# ---- Persistence --------------------------------------------------------

def derive_period_end(fiscal_year: int, fiscal_quarter: int, fye_md: str) -> date:
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


_TURN_HEADER_RE = re.compile(r"^\[(\d{2}):(\d{2})[–\-](\d{2}):(\d{2})\]\s+(.+?)\s*$")


def parse_corrected_blocks(text: str) -> list[dict]:
    blocks = []
    cur = None
    for line in text.splitlines():
        m = _TURN_HEADER_RE.match(line)
        if m:
            if cur is not None:
                blocks.append(cur)
            cur = {
                "start_sec": int(m.group(1)) * 60 + int(m.group(2)),
                "end_sec": int(m.group(3)) * 60 + int(m.group(4)),
                "name": m.group(5).strip(),
                "lines": [],
            }
        elif cur is not None:
            stripped = line.strip()
            if stripped:
                cur["lines"].append(stripped)
    if cur is not None:
        blocks.append(cur)
    for b in blocks:
        b["text"] = " ".join(b["lines"])
    return blocks


def persist_asr_transcript(
    conn: psycopg.Connection,
    *,
    company_id: int,
    ticker: str,
    fiscal_year: int,
    fiscal_quarter: int,
    fiscal_period_key: str,
    period_end: date,
    call_date: date,
    audio_fetch: AudioFetch,
    whisper_data: dict,
    diar: dict,
    fused_segments: list[dict],
    speaker_map: dict[str, dict],
    corrected_text: str,
    ingest_run_id: int,
) -> ASRIngestResult:
    calendar = derive_calendar_period(period_end)
    published_at = datetime.combine(call_date, time(21, 0), tzinfo=UTC)

    # Parse corrected text → speaker turn blocks (with named speakers)
    corrected_blocks = parse_corrected_blocks(corrected_text)
    if not corrected_blocks:
        raise RuntimeError("LLM correction produced no parseable blocks")

    # Build canonical content (Speaker: text\n\n)
    canonical_parts: list[str] = []
    block_offsets: list[tuple[int, int]] = []
    cursor = 0
    for b in corrected_blocks:
        line = f"{b['name']}: {b['text']}\n\n"
        canonical_parts.append(line)
        block_offsets.append((cursor, cursor + len(line) - 2))
        cursor += len(line)
    canonical_body = "".join(canonical_parts)

    # Body envelope (Whisper + diar + correction summary, JSON)
    body_obj = {
        "schema": "asr_transcript_v1",
        "ticker": ticker,
        "fiscal_year": fiscal_year,
        "fiscal_quarter": fiscal_quarter,
        "call_date": call_date.isoformat(),
        "audio": {
            "source_vendor": audio_fetch.audio_ref.vendor,
            "source_url": audio_fetch.audio_ref.source_url,
            "source_event_id": audio_fetch.audio_ref.event_id,
            "source_uuid": audio_fetch.audio_ref.source_uuid,
            "discovered_via": audio_fetch.audio_ref.discovered_via,
            "sha256": audio_fetch.audio_hash_sha256,
            "size_bytes": audio_fetch.audio_size_bytes,
            "duration_sec": audio_fetch.duration_sec,
        },
        "asr": {"backend": ASR_BACKEND, "model": ASR_MODEL, "model_version": ASR_MODEL_VERSION,
                "language": whisper_data.get("language", "en"),
                "segment_count": len(whisper_data["segments"])},
        "diarization": {"model": DIAR_MODEL, "speaker_count": len(diar["speakers"]),
                        "segment_count": len(diar["segments"])},
        "post_correction": {"model": LLM_CORRECTOR},
        "speaker_map": speaker_map,
    }
    body_bytes = json.dumps(body_obj, sort_keys=True, separators=(",", ":")).encode("utf-8")

    source_document_id = f"asr:{audio_fetch.audio_ref.vendor}:{ticker}:FY{fiscal_year}-Q{fiscal_quarter}"

    # 1. audio_artifacts row
    audio_hash_bytes = bytes.fromhex(audio_fetch.audio_hash_sha256)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO audio_artifacts (
                company_id, fiscal_year, fiscal_quarter, fiscal_period_key,
                source_vendor, source_url, source_event_id, source_uuid,
                audio_hash, audio_format, audio_size_bytes, duration_sec
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (company_id, fiscal_period_key, source_url) DO UPDATE
                SET captured_at = excluded.captured_at,
                    audio_size_bytes = excluded.audio_size_bytes
            RETURNING id;
            """,
            (
                company_id, fiscal_year, fiscal_quarter, fiscal_period_key,
                audio_fetch.audio_ref.vendor, audio_fetch.audio_ref.source_url,
                audio_fetch.audio_ref.event_id, audio_fetch.audio_ref.source_uuid,
                audio_hash_bytes, audio_fetch.audio_format,
                audio_fetch.audio_size_bytes, audio_fetch.duration_sec,
            ),
        )
        audio_artifact_id = cur.fetchone()[0]

    # 2. write_artifact (transcript artifact for the canonical content)
    artifact_id, created = write_artifact(
        conn,
        ingest_run_id=ingest_run_id,
        artifact_type="transcript",
        source="asr",
        source_document_id=source_document_id,
        body=body_bytes,
        canonical_body=canonical_body.encode("utf-8"),
        company_id=company_id,
        ticker=ticker,
        fiscal_period_key=fiscal_period_key,
        fiscal_year=fiscal_year,
        fiscal_quarter=fiscal_quarter,
        fiscal_period_label=fiscal_period_key,
        period_end=period_end,
        period_type="quarter",
        calendar_year=calendar.calendar_year,
        calendar_quarter=calendar.calendar_quarter,
        calendar_period_label=calendar.calendar_period_label,
        title=f"{ticker} earnings call {fiscal_period_key} (ASR)",
        content_type="application/json",
        language=whisper_data.get("language", "en"),
        published_at=published_at,
        artifact_metadata={
            "asr_backend": ASR_BACKEND,
            "asr_model": ASR_MODEL,
            "asr_model_version": ASR_MODEL_VERSION,
            "diarization_model": DIAR_MODEL,
            "post_correction_model": LLM_CORRECTOR,
            "audio_source_vendor": audio_fetch.audio_ref.vendor,
            "audio_source_url": audio_fetch.audio_ref.source_url,
            "audio_source_event_id": audio_fetch.audio_ref.event_id,
            "audio_sha256": audio_fetch.audio_hash_sha256,
            "speaker_count_detected": len(diar["speakers"]),
            "block_count": len(corrected_blocks),
        },
    )

    # 3. asr_transcripts row (link audio + artifact)
    raw_payload_hash = hashlib.sha256(
        json.dumps(whisper_data, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).digest()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO asr_transcripts (
                audio_artifact_id, artifact_id, backend, model, model_version,
                language, word_timestamps, raw_payload_hash
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (audio_artifact_id, model, model_version) DO UPDATE
                SET artifact_id = excluded.artifact_id,
                    raw_payload_hash = excluded.raw_payload_hash,
                    transcribed_at = now()
            RETURNING id;
            """,
            (
                audio_artifact_id, artifact_id, ASR_BACKEND, ASR_MODEL,
                ASR_MODEL_VERSION, whisper_data.get("language", "en"),
                True, raw_payload_hash,
            ),
        )
        asr_transcript_id = cur.fetchone()[0]

    # 4 + 5. text_units + chunks (only if artifact was newly created)
    text_units_inserted = 0
    text_chunks_inserted = 0
    if created:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM artifact_text_chunks WHERE text_unit_id IN ("
                " SELECT id FROM artifact_text_units WHERE artifact_id = %s);",
                (artifact_id,),
            )
            cur.execute("DELETE FROM artifact_text_units WHERE artifact_id = %s;", (artifact_id,))

            for ordinal, (b, (start_off, end_off)) in enumerate(
                zip(corrected_blocks, block_offsets), start=1
            ):
                unit_text = canonical_body[start_off:end_off]
                unit = TextUnit(
                    unit_ordinal=ordinal,
                    unit_type="transcript",
                    unit_key=f"turn:{ordinal:03d}",
                    unit_title=b["name"],
                    text=unit_text,
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
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id;
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

    # 6. speaker_segments (one per fused Whisper segment)
    segments_inserted = 0
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM speaker_segments WHERE asr_transcript_id = %s;",
            (asr_transcript_id,),
        )
        # Build per-block start_offset for canonical lookup (approx;
        # mapping is not exact at the segment level, so we use the
        # containing block's offsets for each segment).
        for ordinal, fseg in enumerate(fused_segments, start=1):
            info = speaker_map.get(fseg["speaker"], {})
            vp_id = info.get("voiceprint_id")
            vp_conf = info.get("voiceprint_confidence")
            cur.execute(
                """
                INSERT INTO speaker_segments (
                    asr_transcript_id, ordinal,
                    start_ms, end_ms, raw_speaker_label,
                    voiceprint_match_id, voiceprint_confidence,
                    text_offset_start, text_offset_end
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                """,
                (
                    asr_transcript_id, ordinal,
                    int(fseg["start"] * 1000), int(fseg["end"] * 1000),
                    fseg["speaker"],
                    vp_id, vp_conf,
                    0, len(canonical_body),  # approx — segment-level offsets
                                              # are lossy; canonical text lives
                                              # on text_units. Refine later if
                                              # we need precise lookups.
                ),
            )
            segments_inserted += 1

    # 7. speaker_voiceprints — enroll any newly named exec
    voiceprints_enrolled = 0
    for label, info in speaker_map.items():
        if info.get("source") != "structural":
            continue
        if info["role"] not in ("ceo", "cfo", "coo", "president"):
            continue
        name = info.get("name", "")
        if name.startswith("<"):  # placeholder name like "<unknown CEO>"
            continue
        embedding = diar["embeddings_by_speaker"].get(label)
        if not embedding:
            continue
        with conn.cursor() as cur:
            # Skip if already enrolled
            cur.execute(
                "SELECT 1 FROM speaker_voiceprints WHERE company_id = %s AND person_name = %s "
                "AND superseded_at IS NULL LIMIT 1;",
                (company_id, name),
            )
            if cur.fetchone():
                continue
            cur.execute(
                """
                INSERT INTO speaker_voiceprints (
                    company_id, person_name, role,
                    embedding, embedding_dim, embedding_model,
                    source_audio_artifact_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
                """,
                (
                    company_id, name, info["role"],
                    embedding, len(embedding),
                    "pyannote/embedding-3.1",
                    audio_artifact_id,
                ),
            )
            voiceprints_enrolled += 1
            print(f"  [voiceprint] enrolled {name} ({info['role']}) for company_id={company_id}")

    return ASRIngestResult(
        audio_artifact_id=audio_artifact_id,
        asr_transcript_id=asr_transcript_id,
        artifact_id=artifact_id,
        artifact_created=created,
        text_units_inserted=text_units_inserted,
        text_chunks_inserted=text_chunks_inserted,
        speaker_segments_inserted=segments_inserted,
        voiceprints_enrolled=voiceprints_enrolled,
        audio_deleted=False,
    )


def cleanup_audio(audio_artifact_id: int, audio_path: Path, conn: psycopg.Connection) -> bool:
    """Delete the audio binary and stamp audio_artifacts.deleted_at."""
    if audio_path.exists():
        audio_path.unlink()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE audio_artifacts SET deleted_at = now() WHERE id = %s AND deleted_at IS NULL;",
            (audio_artifact_id,),
        )
    return True


# ---- Top-level orchestrator --------------------------------------------

def ingest_asr_transcript(
    conn: psycopg.Connection,
    *,
    ticker: str,
    fiscal_year: int,
    fiscal_quarter: int,
    call_date: date,
    q4_event_id: str | None = None,
    audio_url: str | None = None,
    youtube_id: str | None = None,
    headless: bool = False,
    keep_audio: bool = False,
    initial_prompt: str | None = None,
    glossary: dict[str, Any] | None = None,
    actor: str = "operator",
) -> ASRIngestResult:
    """End-to-end ASR ingest for a single (ticker, fiscal period) call."""
    fiscal_period_key = f"FY{fiscal_year} Q{fiscal_quarter}"

    # Resolve company
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, ticker, fiscal_year_end_md FROM companies WHERE ticker = %s;",
            (ticker.upper(),),
        )
        row = cur.fetchone()
    if not row:
        raise RuntimeError(f"{ticker} not in companies — seed it first")
    company_id, ticker, fye_md = row
    period_end = derive_period_end(fiscal_year, fiscal_quarter, fye_md)

    repo = Path(__file__).resolve().parents[3]
    scratch = repo / "data" / "scratch"

    initial_prompt = initial_prompt or (
        f"{ticker} earnings call {fiscal_period_key}. Ticker {ticker}. "
        f"AI infrastructure, financial metrics, GAAP and non-GAAP."
    )
    glossary = glossary or {"ticker": ticker, "company_aliases": []}

    run_id = open_run(
        conn, run_kind="manual", vendor="asr", ticker_scope=[ticker],
    )
    audio_artifact_id_for_cleanup: int | None = None
    audio_path_for_cleanup: Path | None = None

    try:
        # 1. Audio
        audio_fetch = acquire_audio(
            ticker=ticker, fiscal_period_key=fiscal_period_key,
            q4_event_id=q4_event_id, pasted_url=audio_url,
            youtube_id=youtube_id,
            scratch_dir=scratch, headless=headless,
        )
        audio_path_for_cleanup = audio_fetch.local_path

        # 2. Whisper
        whisper_dir = scratch / "transcripts" / "whisper-turbo" / ticker
        whisper_json_path, whisper_data = run_whisper_local(
            audio_path=audio_fetch.local_path, out_dir=whisper_dir,
            initial_prompt=initial_prompt,
        )

        # 3. Diarize (extract WAV first)
        wav_path = scratch / "wav" / ticker / f"{fiscal_period_key.replace(' ', '-')}.wav"
        extract_wav_16khz_mono(audio_fetch.local_path, wav_path)
        hf_token = os.environ["HF_TOKEN"]
        diar = run_pyannote_diarization(wav_path, hf_token)

        # 4. Fuse + identify speakers
        fused = fuse_whisper_diarize(whisper_data, diar)
        speaker_map = identify_speakers(
            fused, company_id=company_id, conn=conn,
            voiceprint_embeddings=diar["embeddings_by_speaker"],
        )

        # 5. LLM correction
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("ANTHROPIC_API_KEY required for post-correction")
        corrected_text = post_correct_with_llm(
            fused_segments=fused, speaker_map=speaker_map,
            glossary=glossary, api_key=api_key,
        )

        # 6. Persist (one transaction)
        with conn.transaction():
            result = persist_asr_transcript(
                conn,
                company_id=company_id, ticker=ticker,
                fiscal_year=fiscal_year, fiscal_quarter=fiscal_quarter,
                fiscal_period_key=fiscal_period_key,
                period_end=period_end, call_date=call_date,
                audio_fetch=audio_fetch, whisper_data=whisper_data,
                diar=diar, fused_segments=fused,
                speaker_map=speaker_map, corrected_text=corrected_text,
                ingest_run_id=run_id,
            )
            audio_artifact_id_for_cleanup = result.audio_artifact_id

        # 7. Cleanup audio
        audio_deleted = False
        if not keep_audio and audio_artifact_id_for_cleanup is not None:
            audio_deleted = cleanup_audio(
                audio_artifact_id_for_cleanup, audio_fetch.local_path, conn,
            )
            # Also delete the WAV (regeneratable)
            if wav_path.exists():
                wav_path.unlink()

        result = ASRIngestResult(
            audio_artifact_id=result.audio_artifact_id,
            asr_transcript_id=result.asr_transcript_id,
            artifact_id=result.artifact_id,
            artifact_created=result.artifact_created,
            text_units_inserted=result.text_units_inserted,
            text_chunks_inserted=result.text_chunks_inserted,
            speaker_segments_inserted=result.speaker_segments_inserted,
            voiceprints_enrolled=result.voiceprints_enrolled,
            audio_deleted=audio_deleted,
        )

        close_succeeded(conn, run_id, counts={
            "actor": actor,
            "audio_artifact_id": result.audio_artifact_id,
            "asr_transcript_id": result.asr_transcript_id,
            "artifact_id": result.artifact_id,
            "artifact_created": result.artifact_created,
            "text_units_inserted": result.text_units_inserted,
            "text_chunks_inserted": result.text_chunks_inserted,
            "speaker_segments_inserted": result.speaker_segments_inserted,
            "voiceprints_enrolled": result.voiceprints_enrolled,
            "audio_deleted": result.audio_deleted,
        })
        return result

    except Exception as e:
        close_failed(
            conn, run_id, error_message=str(e),
            error_details={"kind": type(e).__name__, "ticker": ticker},
        )
        raise
