"""Re-apply speaker identification to an already-persisted ASR transcript.

Reads artifact_text_units rows, extracts CEO/CFO names from the IR intro
turn, walks the conversation linearly to remap mis-identified labels,
and UPDATEs unit_title in place. Does NOT re-download audio or re-run
Whisper / pyannote.

Usage:
    uv run scripts/relabel_asr_transcript.py 78809
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from dotenv import load_dotenv

REPO = Path(__file__).resolve().parent.parent
load_dotenv(REPO / ".env", override=True)

sys.path.insert(0, str(REPO / "src"))

from arrow.agents.asr_transcripts import _HANDOFF_RE, _extract_exec_names
from arrow.db.connection import get_conn

_OPERATOR_HINT = re.compile(
    r"\b(?:greetings|good\s+(?:morning|afternoon)).*?\b(?:welcome|conference call)\b",
    re.IGNORECASE,
)
_IR_HINT = re.compile(
    r"\b(?:participants\s+on\s+today|joining\s+the\s+call|joining\s+today|joining\s+us)\b",
    re.IGNORECASE,
)
_NEXT_QUESTION_RE = re.compile(
    r"(?:next|first)\s+question\s+(?:will\s+)?(?:come|comes)\s+from\s+(?:the\s+line\s+of\s+)?"
    r"([A-Z][a-zA-Z'\-]+\s+[A-Z][a-zA-Z'\-]+(?:\s+(?:from|of|with)\s+[A-Z][\w\s&\.]+?)?)"
    r"(?:[\.,]|\s+please|\s+your)",
    re.IGNORECASE,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("artifact_id", type=int)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, unit_ordinal, unit_title, text
                FROM artifact_text_units
                WHERE artifact_id = %s
                ORDER BY unit_ordinal;
                """,
                (args.artifact_id,),
            )
            rows = cur.fetchall()

    if not rows:
        print(f"No artifact_text_units for artifact_id={args.artifact_id}", file=sys.stderr)
        return 1

    blocks = [
        {"id": r[0], "ordinal": r[1], "title": r[2], "text": r[3]} for r in rows
    ]
    print(f"Loaded {len(blocks)} blocks for artifact {args.artifact_id}")

    # Strip "Speaker: " prefix from text since unit_title is already the speaker
    def text_body(b: dict) -> str:
        t = b["text"]
        prefix = f"{b['title']}: "
        return t[len(prefix):] if t.startswith(prefix) else t

    # Step 1: find IR block — the block containing the participant introduction
    ir_block_idx: int | None = None
    extracted_names: dict[str, str] = {}
    for i, b in enumerate(blocks[:8]):  # IR turn is always near the top
        body = text_body(b)
        if _IR_HINT.search(body):
            names = _extract_exec_names(body)
            if names:
                ir_block_idx = i
                extracted_names = names
                break

    if ir_block_idx is None:
        print("WARN: could not locate IR introduction block. No remapping possible.")
        return 0

    print(f"IR block: ordinal={blocks[ir_block_idx]['ordinal']}  current_title={blocks[ir_block_idx]['title']!r}")
    print(f"Extracted exec names: {extracted_names}")

    # Step 2: state machine over UNIQUE titles. Each title corresponds to a
    # logical pyannote-resolved speaker; we want to remap titles, not
    # individual blocks (so each unique title remaps consistently).
    remap: dict[str, str] = {}

    operator_title = blocks[0]["title"]
    ir_title = blocks[ir_block_idx]["title"]

    if "ceo" in extracted_names:
        # First block AFTER ir_block whose title isn't operator/ir → CEO
        for j in range(ir_block_idx + 1, len(blocks)):
            cand_title = blocks[j]["title"]
            if cand_title in (operator_title, ir_title):
                continue
            remap[cand_title] = f"{extracted_names['ceo']} (CEO)"
            ceo_title_resolved = cand_title

            # Within CEO's blocks, find handoff to CFO
            if "cfo" in extracted_names:
                for k in range(j, len(blocks)):
                    if blocks[k]["title"] != cand_title:
                        continue
                    body = text_body(blocks[k])
                    if _HANDOFF_RE.search(body):
                        # CFO is next non-(operator/ir/ceo) title
                        for m in range(k + 1, len(blocks)):
                            cfo_cand = blocks[m]["title"]
                            if cfo_cand in (operator_title, ir_title, ceo_title_resolved):
                                continue
                            remap[cfo_cand] = f"{extracted_names['cfo']} (CFO)"
                            break
                        break
            break

    # If IR title is still raw (looks like SPEAKER_NN), give it a friendlier name
    if re.match(r"^SPEAKER_\d+$", ir_title) or ir_title == "Investor Relations":
        remap.setdefault(ir_title, "Investor Relations")

    # Step 3: walk operator's "next question comes from" intros to fix analyst names
    # If a block's title is currently a misspelled analyst name, look at the
    # operator block JUST BEFORE the next-speaker change and extract the
    # correct name from the operator's intro.
    operator_blocks = [(i, b) for i, b in enumerate(blocks) if b["title"] == operator_title]
    for op_i, op_b in operator_blocks:
        body = text_body(op_b)
        m = _NEXT_QUESTION_RE.search(body)
        if not m:
            continue
        intro_name = m.group(1).strip().rstrip(".,").strip()
        # Next non-operator block
        for j in range(op_i + 1, len(blocks)):
            cand_title = blocks[j]["title"]
            if cand_title == operator_title:
                continue
            # If we already mapped this title to an exec, skip
            if cand_title in remap and ("CEO" in remap[cand_title] or "CFO" in remap[cand_title]):
                break
            # Replace if current title is raw SPEAKER_NN, or if it's an analyst-sounding
            # name that doesn't match the operator's intro (likely typo)
            current = blocks[j]["title"]
            if re.match(r"^SPEAKER_\d+$", current):
                remap[current] = intro_name
            elif current != intro_name and not any(
                kw in current.lower() for kw in ("operator", "investor relations", "ceo", "cfo")
            ):
                # Fuzzy match by surname — only override if surname agrees
                cur_last = current.split()[-1].lower() if current.split() else ""
                intro_last = intro_name.split(",")[0].split()[-1].lower() if intro_name.split() else ""
                # Edit-distance check: same surname or off-by-1
                if cur_last and intro_last and (
                    cur_last == intro_last or _edit_distance(cur_last, intro_last) <= 2
                ):
                    remap[current] = intro_name
            break

    print()
    print(f"Computed remap ({len(remap)} entries):")
    for old, new in remap.items():
        print(f"  {old!r:40s}  ->  {new!r}")

    if args.dry_run:
        print("\n--dry-run: no DB changes")
        return 0

    if not remap:
        print("Nothing to remap.")
        return 0

    # Step 4: apply UPDATEs. Also rewrite the text body so the "Speaker: ..."
    # prefix matches the new unit_title.
    with get_conn() as conn:
        n_updated = 0
        with conn.cursor() as cur:
            for old, new in remap.items():
                cur.execute(
                    """
                    UPDATE artifact_text_units
                    SET unit_title = %s,
                        text = REPLACE(text, %s, %s)
                    WHERE artifact_id = %s AND unit_title = %s;
                    """,
                    (new, f"{old}: ", f"{new}: ", args.artifact_id, old),
                )
                n_updated += cur.rowcount
        conn.commit()
    print(f"\nUpdated {n_updated} artifact_text_units rows.")
    return 0


def _edit_distance(a: str, b: str) -> int:
    """Levenshtein distance, small inputs only."""
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a):
        cur = [i + 1]
        for j, cb in enumerate(b):
            cur.append(min(prev[j + 1] + 1, cur[j] + 1, prev[j] + (0 if ca == cb else 1)))
        prev = cur
    return prev[-1]


if __name__ == "__main__":
    sys.exit(main())
