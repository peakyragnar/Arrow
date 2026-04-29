"""Retroactively enrich generic resolution notes on closed findings.

The pre-provenance steward stamped every auto-resolved finding with
``cleared by {check_name} (no longer surfacing)``. That hides the
operator action that actually fixed the underlying data.

This script walks closed findings whose note still has that exact
generic shape, correlates each one with succeeded ``ingest_runs`` that
ran shortly before the finding's ``closed_at``, and rewrites the note
to include the matched runs (and their inferred action labels).

The original closure record stays in ``history`` jsonb — we append a
new ``annotated`` entry so the provenance enrichment is auditable too.

Idempotent. Skips findings whose note is no longer the generic shape
(already annotated, or manually authored).

Usage:
    uv run scripts/annotate_resolution_history.py             # dry run
    uv run scripts/annotate_resolution_history.py --apply
    uv run scripts/annotate_resolution_history.py --ticker LITE --apply
    uv run scripts/annotate_resolution_history.py --window-minutes 90 --apply
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from psycopg.types.json import Jsonb

from arrow.db.connection import get_conn
from arrow.steward.provenance import (
    find_resolving_runs,
    format_resolution_note,
)


GENERIC_NOTE_PREFIX = "cleared by "
GENERIC_NOTE_SUFFIX = "(no longer surfacing)"


def _is_generic_note(note: str | None) -> bool:
    if not note:
        return False
    return (
        note.startswith(GENERIC_NOTE_PREFIX)
        and note.endswith(GENERIC_NOTE_SUFFIX)
        and "|" not in note  # already-annotated notes contain ' | recent operator actions: '
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ticker", help="Limit to one ticker.")
    parser.add_argument("--window-minutes", type=int, default=60,
                        help="Look-back window for matching ingest_runs (default 60).")
    parser.add_argument("--apply", action="store_true",
                        help="Write the updated notes; default is a dry-run preview.")
    args = parser.parse_args()

    sql = [
        "SELECT id, ticker, source_check, fiscal_period_key, closed_at, closed_note",
        "FROM data_quality_findings",
        "WHERE status = 'closed'",
        "  AND closed_reason = 'resolved'",
        "  AND closed_note IS NOT NULL",
    ]
    params: list = []
    if args.ticker:
        sql.append("  AND ticker = %s")
        params.append(args.ticker.upper())
    sql.append("ORDER BY closed_at DESC")

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("\n".join(sql), params)
        rows = cur.fetchall()

        candidates = [
            (fid, ticker, check, fpk, closed_at, note)
            for (fid, ticker, check, fpk, closed_at, note) in rows
            if _is_generic_note(note)
        ]

        if not candidates:
            print(f"Scanned {len(rows)} closed-resolved findings. None have generic notes.")
            return

        print(f"Scanned {len(rows)} closed-resolved findings. "
              f"{len(candidates)} have generic notes — checking for matching runs.\n")

        plan: list[tuple] = []
        for fid, ticker, check, fpk, closed_at, note in candidates:
            runs = find_resolving_runs(
                conn,
                ticker=ticker,
                fiscal_period_key=fpk,
                at=closed_at,
                window_minutes=args.window_minutes,
            )
            if not runs:
                continue
            new_note = format_resolution_note(note, runs)
            if new_note == note:
                continue
            plan.append((fid, ticker, check, fpk, note, new_note, runs))

        if not plan:
            print("No findings could be annotated (no matching runs in the window).")
            return

        print(f"Will annotate {len(plan)} findings.\n")
        # Show a couple of examples
        for fid, ticker, check, fpk, old_note, new_note, runs in plan[:3]:
            print(f"  finding {fid} | {ticker} {fpk} | {check}")
            print(f"    old: {old_note}")
            print(f"    new: {new_note}")
            print()

        if not args.apply:
            print("Dry run. Pass --apply to write.")
            return

        annotated_at = datetime.now(timezone.utc).isoformat()
        with conn.transaction(), conn.cursor() as cur2:
            for fid, ticker, check, fpk, old_note, new_note, runs in plan:
                history_entry = {
                    "at": annotated_at,
                    "actor": "system:annotate_resolution_history",
                    "action": "annotated",
                    "before": {"closed_note": old_note},
                    "after": {"closed_note": new_note},
                    "note": (
                        f"Retroactively linked to {len(runs)} ingest_run(s) "
                        f"within {args.window_minutes}-minute window of closure."
                    ),
                    "linked_run_ids": [r["run_id"] for r in runs],
                }
                cur2.execute(
                    """
                    UPDATE data_quality_findings
                    SET closed_note = %s,
                        history     = history || %s::jsonb
                    WHERE id = %s
                    """,
                    (new_note, Jsonb([history_entry]), fid),
                )
        print(f"Annotated {len(plan)} findings.")


if __name__ == "__main__":
    main()
