"""Proof: LLM post-correction pass on the speakered transcript.

Conservative fix-only prompt — proper nouns, ticker symbols, mis-typed names.
Preserves verbatim text, disfluencies, and Whisper's segmentation.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv

REPO = Path(__file__).resolve().parent.parent
load_dotenv(REPO / ".env", override=True)

SPEAKERED_JSON = REPO / "data/scratch/transcripts/whisper-turbo/CRWV/FY2026-Q1.speakered.json"
OUT_JSON = REPO / "data/scratch/transcripts/whisper-turbo/CRWV/FY2026-Q1.corrected.json"
OUT_TXT = REPO / "data/scratch/transcripts/whisper-turbo/CRWV/FY2026-Q1.corrected.txt"
DIFF_TXT = REPO / "data/scratch/transcripts/whisper-turbo/CRWV/FY2026-Q1.corrections_diff.txt"

# Per-ticker glossary — eventually this gets mined automatically per ticker
# from existing FMP transcripts. For the proof, hand-curated for CRWV.
CRWV_GLOSSARY = {
    "execs": {
        "Mike Intrator": "CEO",
        "Michael Intrator": "CEO (formal)",
        "Nitin Agrawal": "CFO",
        "Brian Venturo": "President / COO",
    },
    "ticker": "CRWV",
    "company_aliases": ["CoreWeave"],
    "customers_partners": ["Microsoft", "OpenAI", "Meta", "NVIDIA"],
    "products_chips": ["H100", "H200", "GB200", "GB300", "Blackwell", "Hopper"],
    "analysts_on_call": [
        "Keith Weiss (Morgan Stanley)",
        "Brent Thill (Jefferies)",
        "Mark Murphy (JP Morgan)",
        "Tal Liani (Bank of America)",
        "Amit Daryanani (Evercore)",
        "Nihal Chokshi (Northland Capital Markets)",
    ],
    "industry_terms": [
        "hyperscale", "AI diffusion", "contracted power", "active power",
        "delayed draw facility", "investment grade", "EBITDA", "GAAP",
        "construction in progress / CIP", "data center capacity",
    ],
}


CORRECTION_PROMPT = """You are correcting an automatic-speech-recognition transcript of an earnings call. Your job is narrow:

**Fix only:**
- Misspelled proper nouns (people's names, company names, product names, ticker symbols)
- Obviously misheard technical terms (e.g., "in video" -> "NVIDIA")
- Numerical values that are clearly wrong from context (rare)

**Do NOT:**
- Rewrite for clarity, fluency, or grammar
- Remove disfluencies, restarts, or filler words
- Change punctuation unless it makes a sentence meaningless
- Add or remove sentences
- Combine or split speaker turns

**Glossary for this call** (use as authoritative spelling):

```json
{glossary}
```

**Important specific corrections to apply if you see them:**
- "Intrader", "Intrator", "Intrater" -> always "Intrator" (CEO surname)
- "Agarwal", "Agarwall", "Agrawall" -> always "Agrawal" (CFO surname; one R, ends -wal)
- "Dharianani", "Daryanani" (analyst from Evercore) — the correct spelling is "Daryanani" (no h)
- "in video", "Nvideo" -> "NVIDIA"
- "Core Wave", "CoreWave", "Cor Wave" -> "CoreWeave"

**Format:** Return the entire corrected transcript verbatim, preserving all speaker labels (`SPEAKER_NN`) and timestamps. Do not add commentary, headers, or summaries. Output only the corrected transcript text.

The transcript is below. Begin output immediately:

---

{transcript}
"""


def fmt(t: float) -> str:
    m = int(t // 60); s = t - m * 60
    return f"{m:02d}:{int(s):02d}"


def build_input_transcript(data: dict) -> str:
    """Compress to speaker blocks for input to the LLM."""
    blocks = []
    cur = None
    for s in data["segments"]:
        if cur and s["speaker"] == cur["speaker"]:
            cur["end"] = s["end"]
            cur["texts"].append(s["text"].strip())
        else:
            if cur:
                blocks.append(cur)
            cur = {"speaker": s["speaker"], "start": s["start"], "end": s["end"], "texts": [s["text"].strip()]}
    if cur:
        blocks.append(cur)
    out = []
    for b in blocks:
        out.append(f"[{fmt(b['start'])}–{fmt(b['end'])}]  {b['speaker']}")
        out.append("  " + " ".join(b["texts"]))
        out.append("")
    return "\n".join(out)


def main() -> int:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not set", file=sys.stderr)
        return 1

    speakered = json.loads(SPEAKERED_JSON.read_text())
    input_transcript = build_input_transcript(speakered)
    print(f"Input transcript: {len(input_transcript):,} chars")

    from anthropic import Anthropic
    client = Anthropic(api_key=api_key)

    glossary_str = json.dumps(CRWV_GLOSSARY, indent=2)
    prompt = CORRECTION_PROMPT.format(glossary=glossary_str, transcript=input_transcript)

    print("Calling Claude (this should take 30-90 seconds for ~50KB input)...")
    t0 = datetime.now(UTC)
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=16000,
        messages=[{"role": "user", "content": prompt}],
    )
    elapsed = (datetime.now(UTC) - t0).total_seconds()

    corrected = msg.content[0].text
    usage = msg.usage
    print(f"  done in {elapsed:.1f}s")
    print(f"  input tokens:  {usage.input_tokens:,}")
    print(f"  output tokens: {usage.output_tokens:,}")
    print(f"  output chars:  {len(corrected):,}")

    OUT_TXT.write_text(corrected)

    # Compute a simple line-by-line diff
    import difflib
    diff = list(difflib.unified_diff(
        input_transcript.splitlines(),
        corrected.splitlines(),
        fromfile="whisper",
        tofile="corrected",
        lineterm="",
    ))
    DIFF_TXT.write_text("\n".join(diff))

    out = {
        "model": "claude-sonnet-4-6",
        "model_version": msg.model,
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "elapsed_sec": round(elapsed, 1),
        "corrected_at": datetime.now(UTC).isoformat(),
        "input_chars": len(input_transcript),
        "output_chars": len(corrected),
        "glossary": CRWV_GLOSSARY,
    }
    OUT_JSON.write_text(json.dumps(out, indent=2))

    print()
    print(f"Wrote: {OUT_TXT}")
    print(f"Diff:  {DIFF_TXT}")
    print(f"Meta:  {OUT_JSON}")
    print()
    print(f"Diff has {len([l for l in diff if l.startswith(('+', '-')) and not l.startswith(('+++', '---'))])} change lines")
    return 0


if __name__ == "__main__":
    sys.exit(main())
