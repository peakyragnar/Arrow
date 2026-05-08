"""Proof: fuse Whisper segments + pyannote diarization → speakered transcript.

For each Whisper segment, find the diarization segment(s) that overlap most,
assign the dominant speaker. Output a readable speakered transcript.
"""

from __future__ import annotations

import json
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
WHISPER_JSON = REPO / "data/scratch/transcripts/whisper-turbo/CRWV/FY2026-Q1.json"
DIARIZE_JSON = REPO / "data/scratch/diarize/pyannote-3.1/CRWV/FY2026-Q1.json"
OUT_JSON = REPO / "data/scratch/transcripts/whisper-turbo/CRWV/FY2026-Q1.speakered.json"
OUT_TXT = REPO / "data/scratch/transcripts/whisper-turbo/CRWV/FY2026-Q1.speakered.txt"


def overlap_seconds(a_start: float, a_end: float, b_start: float, b_end: float) -> float:
    return max(0.0, min(a_end, b_end) - max(a_start, b_start))


def assign_speaker(seg: dict, diar_segments: list[dict]) -> tuple[str, float]:
    """Return (speaker_label, fraction_of_segment_covered_by_that_speaker)."""
    by_speaker: dict[str, float] = {}
    for d in diar_segments:
        if d["start"] > seg["end"]:
            break
        if d["end"] < seg["start"]:
            continue
        ov = overlap_seconds(seg["start"], seg["end"], d["start"], d["end"])
        if ov > 0:
            by_speaker[d["speaker"]] = by_speaker.get(d["speaker"], 0.0) + ov
    if not by_speaker:
        return ("UNKNOWN", 0.0)
    top = max(by_speaker.items(), key=lambda kv: kv[1])
    seg_len = max(0.001, seg["end"] - seg["start"])
    return (top[0], top[1] / seg_len)


def fmt(t: float) -> str:
    m = int(t // 60)
    s = t - m * 60
    return f"{m:02d}:{int(s):02d}"


def main() -> int:
    whisper = json.loads(WHISPER_JSON.read_text())
    diar = json.loads(DIARIZE_JSON.read_text())

    diar_segments = sorted(diar["segments"], key=lambda d: d["start"])
    fused = []
    for seg in whisper["segments"]:
        speaker, conf = assign_speaker(seg, diar_segments)
        fused.append({
            "start": seg["start"],
            "end": seg["end"],
            "speaker": speaker,
            "speaker_confidence": round(conf, 3),
            "text": seg["text"].strip(),
        })

    OUT_JSON.write_text(json.dumps({
        "speakers": diar["speakers"],
        "speaker_count": diar["speaker_count"],
        "segments": fused,
    }, indent=2))

    # Compress consecutive segments from the same speaker into a single block
    blocks = []
    cur = None
    for s in fused:
        if cur and s["speaker"] == cur["speaker"]:
            cur["end"] = s["end"]
            cur["texts"].append(s["text"])
        else:
            if cur:
                blocks.append(cur)
            cur = {"speaker": s["speaker"], "start": s["start"], "end": s["end"], "texts": [s["text"]]}
    if cur:
        blocks.append(cur)

    with OUT_TXT.open("w") as f:
        for b in blocks:
            f.write(f"\n[{fmt(b['start'])}–{fmt(b['end'])}]  {b['speaker']}\n")
            f.write("  " + " ".join(b["texts"]) + "\n")

    print(f"Wrote {OUT_JSON} ({len(fused)} segments)")
    print(f"Wrote {OUT_TXT} ({len(blocks)} speaker blocks)")
    print()
    # Distribution
    by_speaker_total: dict[str, float] = {}
    for s in fused:
        by_speaker_total[s["speaker"]] = by_speaker_total.get(s["speaker"], 0.0) + (s["end"] - s["start"])
    total = sum(by_speaker_total.values())
    print("Speaking time by label:")
    for sp, t in sorted(by_speaker_total.items(), key=lambda kv: -kv[1]):
        pct = 100 * t / total if total else 0
        print(f"  {sp:14s}  {t:6.1f}s  ({t/60:5.1f} min, {pct:5.1f}%)")

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
