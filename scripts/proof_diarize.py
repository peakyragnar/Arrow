"""Proof: run pyannote 3.1 diarization on the CRWV Q1 2026 WAV.

One-off proof script (Commit 0 of asr_transcripts_ingest_plan.md). To be
replaced by src/arrow/ingest/asr/diarize.py once the design is validated.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv

REPO = Path(__file__).resolve().parent.parent
load_dotenv(REPO / ".env")

WAV = REPO / "data/scratch/wav/CRWV/FY2026-Q1.wav"
OUT = REPO / "data/scratch/diarize/pyannote-3.1/CRWV/FY2026-Q1.json"


def main() -> int:
    token = os.environ.get("HF_TOKEN")
    if not token:
        print("ERROR: HF_TOKEN not set in environment / .env", file=sys.stderr)
        return 1

    if not WAV.exists():
        print(f"ERROR: WAV not found at {WAV}", file=sys.stderr)
        return 1

    OUT.parent.mkdir(parents=True, exist_ok=True)

    print(f"Loading pyannote/speaker-diarization-3.1 (first run downloads weights)...")
    t0 = time.monotonic()
    from pyannote.audio import Pipeline
    pipeline = Pipeline.from_pretrained(
        "pyannote/speaker-diarization-3.1",
        token=token,
    )
    print(f"  pipeline loaded in {time.monotonic()-t0:.1f}s")

    # Try MPS (Apple Silicon) first; fall back to CPU on any error.
    try:
        import torch
        if torch.backends.mps.is_available():
            pipeline.to(torch.device("mps"))
            print("  using MPS (Apple Silicon GPU)")
        else:
            print("  using CPU (no MPS)")
    except Exception as e:
        print(f"  device init: {e!r} — falling back to CPU")

    print(f"Diarizing {WAV.name} ({WAV.stat().st_size / 1e6:.1f} MB)...")
    t1 = time.monotonic()
    diarize_output = pipeline(str(WAV))
    elapsed = time.monotonic() - t1

    # New pyannote API returns DiarizeOutput with:
    #   .speaker_diarization           — Annotation (with overlaps)
    #   .exclusive_speaker_diarization — Annotation (one speaker per moment, ASR-friendly)
    #   .speaker_embeddings            — (num_speakers, 192) array; ordered by .labels()
    annotation = diarize_output.exclusive_speaker_diarization
    overlap_annotation = diarize_output.speaker_diarization
    embeddings = diarize_output.speaker_embeddings

    segments = []
    speakers = set()
    for turn, _, speaker in annotation.itertracks(yield_label=True):
        segments.append({
            "start": float(turn.start),
            "end": float(turn.end),
            "speaker": speaker,
        })
        speakers.add(speaker)

    # Also capture overlap-aware segments separately
    overlap_segments = []
    for turn, _, speaker in overlap_annotation.itertracks(yield_label=True):
        overlap_segments.append({
            "start": float(turn.start),
            "end": float(turn.end),
            "speaker": speaker,
        })

    # Embeddings — one per detected speaker, in label order
    speaker_label_order = list(annotation.labels())
    embeddings_by_speaker: dict = {}
    if embeddings is not None:
        for i, label in enumerate(speaker_label_order):
            if i < len(embeddings):
                embeddings_by_speaker[label] = embeddings[i].tolist()

    # Compute audio hash (sha256 of the WAV bytes) for provenance
    h = hashlib.sha256()
    with WAV.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    audio_hash = h.hexdigest()

    out = {
        "model": "pyannote/speaker-diarization-3.1",
        "audio_path": str(WAV.relative_to(REPO)),
        "audio_hash_sha256": audio_hash,
        "audio_duration_sec": segments[-1]["end"] if segments else 0.0,
        "diarized_at": datetime.now(UTC).isoformat(),
        "diarize_elapsed_sec": round(elapsed, 1),
        "speaker_count": len(speakers),
        "speakers": sorted(speakers),
        "segment_count": len(segments),
        "segments": segments,
        "overlap_segment_count": len(overlap_segments),
        "overlap_segments": overlap_segments,
        "embedding_dim": len(next(iter(embeddings_by_speaker.values()))) if embeddings_by_speaker else 0,
        "embeddings_by_speaker": embeddings_by_speaker,
    }
    OUT.write_text(json.dumps(out, indent=2))

    print(f"  diarized in {elapsed:.1f}s")
    print(f"  speakers: {sorted(speakers)} (n={len(speakers)})")
    print(f"  segments: {len(segments)}")
    print(f"  output:   {OUT}")

    # Sample first 10 segments
    print("\nFirst 10 segments:")
    for s in segments[:10]:
        print(f"  [{s['start']:7.1f}s -> {s['end']:7.1f}s]  {s['speaker']}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
