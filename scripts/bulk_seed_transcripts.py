"""Bulk-seed companies and pull all available FMP transcripts for them.

Drives a wide transcript-only ingest across a universe defined by
``scripts/build_transcript_universe.py``. Designed to lock in transcripts
while on FMP Ultimate before downgrading.

For each ticker:
1. Seed the `companies` row via SEC company_tickers (idempotent — skip if
   already present). Foreign filers fail this step naturally and get
   marked skipped; that's the desired filter.
2. Run `ingest_transcripts` with `allow_derived_anchor=True` so transcripts
   without financial_facts anchors still land (period_end derived from the
   company's fiscal_year_end_md).

The script is fully resumable: re-running picks up where it left off.
Per-ticker progress is logged to stdout and a JSONL trace.

Usage:
    uv run scripts/bulk_seed_transcripts.py --manifest data/universes/transcript_bulk_20260507.json
    uv run scripts/bulk_seed_transcripts.py --manifest <path> --limit 10  # dry-run first 10
    uv run scripts/bulk_seed_transcripts.py --manifest <path> --resume    # default: yes
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from datetime import UTC, datetime
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env")

from arrow.agents.fmp_transcripts import ingest_transcripts
from arrow.db.connection import get_conn
from arrow.ingest.sec.bootstrap import seed_companies


def _load_manifest(path: Path) -> list[str]:
    data = json.loads(path.read_text())
    return [t["ticker"] for t in data["tickers"]]


def _already_seeded(conn, ticker: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM companies WHERE ticker = %s LIMIT 1;", (ticker.upper(),))
        return cur.fetchone() is not None


def _transcript_count(conn, ticker: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM artifacts a JOIN companies c ON c.id = a.company_id
            WHERE c.ticker = %s AND a.artifact_type = 'transcript';
            """,
            (ticker.upper(),),
        )
        return cur.fetchone()[0]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only the first N tickers in the manifest (for dry-run).",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        default=True,
        help="Skip tickers that already have transcripts (default: True).",
    )
    parser.add_argument(
        "--trace-out",
        type=Path,
        default=REPO_ROOT / "data" / "universes" / "bulk_seed_trace.jsonl",
    )
    args = parser.parse_args()

    if not args.manifest.exists():
        print(f"manifest not found: {args.manifest}", file=sys.stderr)
        return 1
    args.trace_out.parent.mkdir(parents=True, exist_ok=True)

    tickers = _load_manifest(args.manifest)
    if args.limit:
        tickers = tickers[: args.limit]
    print(f"Manifest: {args.manifest}", flush=True)
    print(f"Universe: {len(tickers)} tickers", flush=True)
    print(f"Trace:    {args.trace_out}", flush=True)
    print("=" * 70, flush=True)

    started_total = time.time()
    summary = {
        "seed_skipped_already_present": 0,
        "seed_succeeded": 0,
        "seed_failed_foreign_or_unknown": 0,
        "transcripts_skipped_already_present": 0,
        "transcripts_ingested": 0,
        "transcripts_total_inserted": 0,
        "transcripts_total_existing": 0,
        "transcript_dates_total_seen": 0,
        "errors": 0,
    }

    with args.trace_out.open("a", encoding="utf-8") as trace_f, get_conn() as conn:
        for i, ticker in enumerate(tickers, 1):
            t0 = time.time()
            ticker = ticker.upper()
            entry: dict = {
                "ticker": ticker,
                "i": i,
                "started_at": datetime.now(UTC).isoformat(),
            }

            try:
                # --- Seed step ---
                if _already_seeded(conn, ticker):
                    entry["seed"] = "already_seeded"
                    summary["seed_skipped_already_present"] += 1
                else:
                    try:
                        seed_companies(conn, [ticker])
                        entry["seed"] = "seeded"
                        summary["seed_succeeded"] += 1
                    except LookupError as e:
                        # ticker not in SEC company_tickers — likely foreign
                        # filer or non-SEC-listed. skip.
                        entry["seed"] = f"skip_foreign_or_unknown: {e}"
                        summary["seed_failed_foreign_or_unknown"] += 1
                        trace_f.write(json.dumps(entry) + "\n")
                        trace_f.flush()
                        if i % 25 == 0 or i == len(tickers):
                            print(f"[{i:4}/{len(tickers)}] {ticker:6}  SKIP foreign/unknown", flush=True)
                        continue

                # --- Transcript ingest ---
                if args.skip_existing and _transcript_count(conn, ticker) > 0:
                    entry["transcripts"] = "already_present"
                    summary["transcripts_skipped_already_present"] += 1
                    trace_f.write(json.dumps(entry) + "\n")
                    trace_f.flush()
                    if i % 25 == 0 or i == len(tickers):
                        print(f"[{i:4}/{len(tickers)}] {ticker:6}  already has transcripts", flush=True)
                    continue

                counts = ingest_transcripts(
                    conn,
                    [ticker],
                    actor="bulk_seed_transcripts",
                    allow_derived_anchor=True,
                )
                entry["transcripts"] = {
                    "ingest_run_id": counts.get("ingest_run_id"),
                    "dates_seen": counts.get("transcript_dates_seen", 0),
                    "fetched": counts.get("transcripts_fetched", 0),
                    "inserted": counts.get("artifacts_inserted", 0),
                    "existing": counts.get("artifacts_existing", 0),
                    "skipped_no_anchor": counts.get("transcripts_skipped_no_anchor", 0),
                    "duration_s": round(time.time() - t0, 1),
                }
                summary["transcripts_ingested"] += 1
                summary["transcripts_total_inserted"] += counts.get("artifacts_inserted", 0)
                summary["transcripts_total_existing"] += counts.get("artifacts_existing", 0)
                summary["transcript_dates_total_seen"] += counts.get("transcript_dates_seen", 0)

                d = entry["transcripts"]
                print(
                    f"[{i:4}/{len(tickers)}] {ticker:6}  +{d['inserted']:3} new "
                    f"({d['existing']} existing, {d['skipped_no_anchor']} skipped)  "
                    f"{d['duration_s']}s",
                    flush=True,
                )
            except Exception as e:
                entry["error"] = f"{type(e).__name__}: {e}"
                entry["traceback"] = traceback.format_exc()[:2000]
                summary["errors"] += 1
                print(
                    f"[{i:4}/{len(tickers)}] {ticker:6}  ERROR  {type(e).__name__}: {str(e)[:120]}",
                    flush=True,
                )

            entry["finished_at"] = datetime.now(UTC).isoformat()
            trace_f.write(json.dumps(entry) + "\n")
            trace_f.flush()

    elapsed = time.time() - started_total
    print("\n" + "=" * 70)
    print("Bulk transcript ingest complete")
    print(f"  Elapsed:                                    {elapsed/60:.1f} min")
    for k, v in summary.items():
        print(f"  {k:42}  {v}")
    print(f"  Trace: {args.trace_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
