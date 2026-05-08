"""End-to-end ASR transcript ingest CLI.

Usage:
    # Auto-discover the audio URL via Playwright (requires Chromium)
    uv run scripts/ingest_asr_transcript.py CRWV \
        --fiscal FY2026Q1 --call-date 2026-05-07 \
        --q4-event-id 658779279

    # Manual paste fallback (when Playwright fails or for one-off runs)
    uv run scripts/ingest_asr_transcript.py CRWV \
        --fiscal FY2026Q1 --call-date 2026-05-07 \
        --audio-url https://static.events.q4inc.com/edited-recordings/.../...mp4

    # Headed browser mode (helpful for debugging Cloudflare flags)
    ... --no-headless
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

REPO = Path(__file__).resolve().parent.parent
load_dotenv(REPO / ".env", override=True)

sys.path.insert(0, str(REPO / "src"))

from arrow.agents.asr_transcripts import ingest_asr_transcript
from arrow.db.connection import get_conn


_FISCAL_RE = re.compile(r"^FY(\d{4})\s*Q([1-4])$", re.IGNORECASE)


def _parse_fiscal(s: str) -> tuple[int, int]:
    m = _FISCAL_RE.match(s.replace(" ", ""))
    if not m:
        raise argparse.ArgumentTypeError(f"--fiscal must be like FY2026Q1, got {s!r}")
    return int(m.group(1)), int(m.group(2))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("ticker")
    parser.add_argument("--fiscal", type=_parse_fiscal, required=True,
                        help="Fiscal period like FY2026Q1")
    parser.add_argument("--call-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--q4-event-id", help="Q4 event_id for Playwright discovery")
    parser.add_argument("--audio-url", help="Manual paste fallback")
    parser.add_argument("--youtube-id", help="YouTube video_id (for encrypted-HLS fallback)")
    parser.add_argument("--no-headless", action="store_true",
                        help="Run Playwright in headed mode (visible browser)")
    parser.add_argument("--keep-audio", action="store_true",
                        help="Don't delete audio binary after persist (debug)")
    parser.add_argument("--initial-prompt",
                        help="Override the Whisper initial-prompt (vocabulary hint)")
    args = parser.parse_args()

    if not (args.q4_event_id or args.audio_url or args.youtube_id):
        parser.error("Provide one of: --q4-event-id, --audio-url, --youtube-id")

    ticker = args.ticker.upper()
    fiscal_year, fiscal_quarter = args.fiscal
    call_date = date.fromisoformat(args.call_date)

    print(f"=== ASR ingest: {ticker} FY{fiscal_year}Q{fiscal_quarter} ({call_date}) ===")
    with get_conn() as conn:
        result = ingest_asr_transcript(
            conn,
            ticker=ticker,
            fiscal_year=fiscal_year,
            fiscal_quarter=fiscal_quarter,
            call_date=call_date,
            q4_event_id=args.q4_event_id,
            audio_url=args.audio_url,
            youtube_id=args.youtube_id,
            headless=not args.no_headless,
            keep_audio=args.keep_audio,
            initial_prompt=args.initial_prompt,
        )

    print()
    print("=== Done ===")
    print(f"  audio_artifact_id   = {result.audio_artifact_id}")
    print(f"  asr_transcript_id   = {result.asr_transcript_id}")
    print(f"  artifact_id         = {result.artifact_id}  (created={result.artifact_created})")
    print(f"  text_units          = {result.text_units_inserted}")
    print(f"  text_chunks         = {result.text_chunks_inserted}")
    print(f"  speaker_segments    = {result.speaker_segments_inserted}")
    print(f"  voiceprints_enrolled= {result.voiceprints_enrolled}")
    print(f"  audio_deleted       = {result.audio_deleted}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
