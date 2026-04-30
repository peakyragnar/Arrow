"""Read pending_triage records left by the post-ingest steward step.

Harness-agnostic. The AI in any chat session (Claude Code, Codex, ...)
calls this at session start to discover whether the operator has
ingested anything that needs triage since the last conversation.

Records live under ``data/pending_triage/<timestamp>_<tickers>.json``.
This script lists pending ones (default), or marks them resolved
(``--resolve <path>`` after triage finishes).

Usage:
    uv run scripts/check_pending_triage.py                # list pending
    uv run scripts/check_pending_triage.py --json         # machine-readable
    uv run scripts/check_pending_triage.py --resolve <path>
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PENDING_TRIAGE_DIR = REPO_ROOT / "data" / "pending_triage"
RESOLVED_DIR = PENDING_TRIAGE_DIR / "resolved"


def list_pending() -> list[dict]:
    """Return pending records (newest first), excluding the resolved subdir."""
    if not PENDING_TRIAGE_DIR.exists():
        return []
    out = []
    for p in sorted(PENDING_TRIAGE_DIR.glob("*.json"), reverse=True):
        if p.parent != PENDING_TRIAGE_DIR:
            continue
        try:
            data = json.loads(p.read_text())
            data["_path"] = str(p.relative_to(REPO_ROOT))
            data["_path_abs"] = str(p)
            out.append(data)
        except (json.JSONDecodeError, OSError):
            continue
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true",
                        help="Emit JSON instead of human-readable text.")
    parser.add_argument("--resolve",
                        help="Path to a pending record to mark resolved "
                             "(moves it under data/pending_triage/resolved/).")
    args = parser.parse_args()

    if args.resolve:
        src = Path(args.resolve)
        if not src.is_absolute():
            src = REPO_ROOT / src
        if not src.exists():
            print(f"not found: {src}")
            return 2
        RESOLVED_DIR.mkdir(parents=True, exist_ok=True)
        dst = RESOLVED_DIR / src.name
        src.rename(dst)
        print(f"resolved: {dst.relative_to(REPO_ROOT)}")
        return 0

    pending = list_pending()
    if args.json:
        print(json.dumps(pending, indent=2, default=str))
        return 0

    if not pending:
        print("No pending triage records.")
        return 0

    print(f"{len(pending)} pending triage record(s):\n")
    for rec in pending:
        created = rec.get("created_at", "?")
        try:
            created_short = datetime.fromisoformat(
                created.replace("Z", "+00:00")
            ).strftime("%Y-%m-%d %H:%M UTC")
        except (ValueError, AttributeError):
            created_short = created
        tickers = rec.get("tickers", [])
        totals = rec.get("steward_totals", {})
        new_ids = rec.get("new_finding_ids", [])
        print(f"  [{created_short}] {tickers}")
        print(f"    new={totals.get('new', 0)}  resolved={totals.get('resolved', 0)}  "
              f"unchanged={totals.get('unchanged', 0)}")
        if new_ids:
            preview = new_ids[:6]
            more = "" if len(new_ids) <= 6 else f" (+{len(new_ids) - 6} more)"
            print(f"    finding ids: {preview}{more}")
        per_check = rec.get("per_check_summary", [])
        for c in per_check:
            print(f"      {c['name']}: new={c['new']} resolved={c['resolved']}")
        print(f"    record: {rec['_path']}")
        print(f"    after triage: uv run scripts/check_pending_triage.py "
              f"--resolve {rec['_path']}")
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
