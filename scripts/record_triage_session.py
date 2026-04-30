"""Record a triage session from a JSON payload.

Harness-agnostic — works the same way under Claude Code, Codex, or
anything that can run a shell command. The AI in chat dumps a JSON
file with the session structure, then runs this script.

Payload schema (all fields optional except intent + created_by):
{
  "intent": "...",                  // required, one sentence
  "created_by": "...",              // required, see sessions.py for conventions
  "harness": "claude_code",         // claude_code | codex | human_only | other
  "finding_ids": [1, 2, 3],
  "operator_quotes": ["..."],
  "investigations": [{"action": "query_facts", "target": "...", "result_summary": "..."}],
  "actions_taken": [{"kind": "script", "target": "scripts/foo.py", "identifier": "run_id=230", "summary": "..."}],
  "outcomes": {"findings_closed": [1,2], "data_changed": {...}},
  "captured_pattern": "...",        // one sentence, the rule the agent should learn
  "started_at": "2026-04-30T10:00:00Z",
  "finished_at": "2026-04-30T10:30:00Z",
  "session_ref": "..."
}

Usage:
    cat session.json | uv run scripts/record_triage_session.py
    uv run scripts/record_triage_session.py --input session.json
"""

from __future__ import annotations

import argparse
import json
import sys

from arrow.db.connection import get_conn
from arrow.steward.sessions import record_triage_session


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        help="Path to JSON payload (default: stdin).",
    )
    args = parser.parse_args()

    if args.input:
        with open(args.input) as f:
            payload = json.load(f)
    else:
        payload = json.load(sys.stdin)

    with get_conn() as conn:
        sid = record_triage_session(conn, **payload)
        conn.commit()
    print(sid)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
