"""Ingest one or more companies through the normal flow.

Usage:
    uv run scripts/ingest_company.py NVDA [MSFT ...]
    uv run scripts/ingest_company.py --since 2018-01-01 --scoped NVDA
    uv run scripts/ingest_company.py --sec-limit 3 NVDA
    uv run scripts/ingest_company.py --no-xbrl-audit NVDA   # skip Layer 5 audit
    uv run scripts/ingest_company.py --no-steward NVDA      # skip post-ingest steward

Normal flow:
  1. seed company from SEC
  2. backfill baseline FMP financials
  3. ingest FMP revenue segments
  4. ingest FMP employee counts
  5. ingest FMP earnings-call transcripts
  6. audit FMP vs SEC XBRL anchors; auto-promote safe corruption-bucket
     divergences. Unsafe divergences surface via the
     ``xbrl_audit_unresolved`` steward check for analyst review.
  7. backfill SEC `10-K` / `10-Q` qualitative filings
     (5 fiscal years, complete first FY, primary docs only)
  8. POST-INGEST STEWARD PASS: run all deterministic checks scoped to
     the just-ingested tickers; auto-resolve cleared findings; surface
     anything that needs human triage. Drops a pending_triage record
     under data/pending_triage/ if any new findings appeared, so the
     next AI session (Claude Code, Codex, anything) sees pending work
     without the operator having to remember.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from arrow.agents.fmp_employees import backfill_fmp_employees
from arrow.agents.fmp_ingest import DEFAULT_SINCE_DATE, backfill_fmp_statements
from arrow.agents.fmp_segments import backfill_fmp_segments
from arrow.agents.fmp_transcripts import ingest_transcripts
from arrow.agents.xbrl_audit import audit_and_promote_xbrl
from arrow.db.connection import get_conn
from arrow.ingest.sec.bootstrap import seed_companies
from arrow.ingest.sec.filings import DEFAULT_QUAL_SINCE_DATE, ingest_sec_filings
from arrow.normalize.financials.load import (
    BSVerificationFailed,
    CFVerificationFailed,
    VerificationFailed,
)
from arrow.steward.registry import Scope
from arrow.steward.runner import run_steward
import arrow.steward.checks  # noqa: F401  — register checks into REGISTRY


REPO_ROOT = Path(__file__).resolve().parents[1]
PENDING_TRIAGE_DIR = REPO_ROOT / "data" / "pending_triage"


def _usage() -> str:
    return (
        "Usage: ingest_company.py "
        "[--since YYYY-MM-DD] [--until YYYY-MM-DD] [--scoped] [--sec-limit N] "
        "TICKER [TICKER ...]"
    )


def _print_section(title: str, counts: dict[str, Any]) -> None:
    print(title)
    for key, value in counts.items():
        print(f"  {key}: {value}")
    print()


def main() -> int:
    from datetime import date as _d

    args = sys.argv[1:]
    since_date = None
    until_date = None
    scoped = False
    sec_limit: int | None = None

    def _pop_date_flag(flag: str):
        nonlocal args
        if flag not in args:
            return None
        i = args.index(flag)
        if i + 1 >= len(args):
            print(_usage(), file=sys.stderr)
            sys.exit(2)
        try:
            y, m, d = args[i + 1].split("-")
            val = _d(int(y), int(m), int(d))
        except Exception as e:
            print(f"Invalid {flag} date: {e}", file=sys.stderr)
            sys.exit(2)
        args = args[:i] + args[i + 2 :]
        return val

    since_date = _pop_date_flag("--since")
    until_date = _pop_date_flag("--until")
    if "--scoped" in args:
        scoped = True
        args = [a for a in args if a != "--scoped"]
    if "--sec-limit" in args:
        i = args.index("--sec-limit")
        if i + 1 >= len(args):
            print(_usage(), file=sys.stderr)
            return 2
        sec_limit = int(args[i + 1])
        args = args[:i] + args[i + 2 :]

    skip_xbrl_audit = False
    if "--no-xbrl-audit" in args:
        skip_xbrl_audit = True
        args = [a for a in args if a != "--no-xbrl-audit"]

    skip_steward = False
    if "--no-steward" in args:
        skip_steward = True
        args = [a for a in args if a != "--no-steward"]

    if not args:
        print(_usage(), file=sys.stderr)
        return 2

    is_custom_window = (
        (since_date is not None and since_date > DEFAULT_SINCE_DATE)
        or (until_date is not None)
    )
    if is_custom_window and not scoped:
        print(
            "ERROR: --since / --until request a narrower-than-default window "
            f"(default since={DEFAULT_SINCE_DATE.isoformat()}, until=None).\n"
            "       Partial backfills are the wrong default; they silently "
            "skip fiscal years.\n"
            "       If this narrower window is intentional (dev/test/bisect), "
            "re-run with --scoped.",
            file=sys.stderr,
        )
        return 2

    tickers = [t.upper() for t in args]
    fmp_kwargs: dict[str, Any] = {}
    sec_kwargs: dict[str, Any] = {}
    if since_date is not None:
        fmp_kwargs["since_date"] = since_date
        sec_kwargs["since_date"] = since_date
    else:
        sec_kwargs["since_date"] = DEFAULT_QUAL_SINCE_DATE
    if until_date is not None:
        fmp_kwargs["until_date"] = until_date
        sec_kwargs["until_date"] = until_date
    if sec_limit is not None:
        sec_kwargs["limit_per_ticker"] = sec_limit

    try:
        with get_conn() as conn:
            seeded = seed_companies(conn, tickers)
            fmp_counts = backfill_fmp_statements(conn, tickers, **fmp_kwargs)
            segment_counts = backfill_fmp_segments(conn, tickers, **fmp_kwargs)
            employee_counts = backfill_fmp_employees(conn, tickers)
            transcript_counts = ingest_transcripts(conn, tickers, actor="operator:ingest_company")
            xbrl_audit_counts = (
                audit_and_promote_xbrl(conn, tickers, actor="operator:ingest_company")
                if not skip_xbrl_audit
                else None
            )
            sec_counts = ingest_sec_filings(conn, tickers, **sec_kwargs)
    except VerificationFailed as e:
        print(f"FAILED: Layer 1 IS validation — {e}", file=sys.stderr)
        return 1
    except BSVerificationFailed as e:
        print(f"FAILED: Layer 1 BS hard validation — {e}", file=sys.stderr)
        return 1
    except CFVerificationFailed as e:
        print(f"FAILED: Layer 1 CF validation — {e}", file=sys.stderr)
        return 1

    print(f"Completed normal flow for {', '.join(tickers)}")
    print()
    print("Seeded")
    for company in seeded:
        print(
            f"  {company.ticker}: cik={company.cik}, id={company.id}, fye={company.fiscal_year_end_md}"
        )
    print()
    _print_section(
        "FMP financials",
        {
            "ingest_run_id": fmp_counts["ingest_run_id"],
            "since_date": fmp_counts["since_date"],
            "rows_processed": fmp_counts["rows_processed"],
            "raw_responses": fmp_counts["raw_responses"],
            "facts_written": (
                fmp_counts["is_facts_written"]
                + fmp_counts["bs_facts_written"]
                + fmp_counts["cf_facts_written"]
            ),
            "soft_flags_written": fmp_counts.get("bs_flags_written", 0)
            + fmp_counts.get("cf_flags_written", 0),
        },
    )
    _print_section(
        "FMP segments",
        {
            "ingest_run_id": segment_counts["ingest_run_id"],
            "since_date": segment_counts["since_date"],
            "rows_processed": segment_counts["rows_processed"],
            "raw_responses": segment_counts["raw_responses"],
            "segments_processed": segment_counts["segments_processed"],
            "facts_written": segment_counts["facts_written"],
        },
    )
    _print_section(
        "Employees",
        {
            "ingest_run_id": employee_counts["ingest_run_id"],
            "rows_processed": employee_counts["rows_processed"],
            "raw_responses": employee_counts["raw_responses"],
            "facts_written": employee_counts["facts_written"],
        },
    )
    if xbrl_audit_counts is not None:
        _print_section(
            "FMP-vs-XBRL audit",
            {
                "audit_run_id": xbrl_audit_counts["ingest_run_id"],
                "anchors_matched": (
                    xbrl_audit_counts["is_anchors_matched"]
                    + xbrl_audit_counts["bs_anchors_matched"]
                    + xbrl_audit_counts["cf_anchors_matched"]
                ),
                "anchors_checked": (
                    xbrl_audit_counts["is_anchors_checked"]
                    + xbrl_audit_counts["bs_anchors_checked"]
                    + xbrl_audit_counts["cf_anchors_checked"]
                ),
                "divergences_total": xbrl_audit_counts["divergences_total"],
                "divergences_promoted": xbrl_audit_counts["divergences_promoted"],
                "divergences_left_for_review": xbrl_audit_counts["divergences_left_for_review"],
                "promotion_run_ids": xbrl_audit_counts["promotion_run_ids"],
            },
        )

    _print_section(
        "FMP transcripts",
        {
            "ingest_run_id": transcript_counts["ingest_run_id"],
            "transcript_dates_seen": transcript_counts["transcript_dates_seen"],
            "transcripts_requested": transcript_counts["transcripts_requested"],
            "transcripts_fetched": transcript_counts["transcripts_fetched"],
            "transcripts_missing": transcript_counts["transcripts_missing"],
            "transcripts_skipped_no_anchor": transcript_counts["transcripts_skipped_no_anchor"],
            "raw_responses": transcript_counts["raw_responses"],
            "artifacts_inserted": transcript_counts["artifacts_inserted"],
            "artifacts_existing": transcript_counts["artifacts_existing"],
            "artifacts_superseded": transcript_counts["artifacts_superseded"],
            "text_units_inserted": transcript_counts["text_units_inserted"],
            "text_chunks_inserted": transcript_counts["text_chunks_inserted"],
        },
    )
    _print_section(
        "SEC filings",
        {
            "ingest_run_id": sec_counts["ingest_run_id"],
            "since_date": sec_counts["since_date"],
            "min_fiscal_year_by_ticker": sec_counts.get("min_fiscal_year_by_ticker", {}),
            "max_fiscal_year_by_ticker": sec_counts.get("max_fiscal_year_by_ticker", {}),
            "filings_seen": sec_counts["filings_seen"],
            "filing_docs_fetched": sec_counts["documents_fetched"],
            "raw_responses": sec_counts["raw_responses"],
            "artifacts_written": sec_counts["artifacts_written"],
            "artifacts_existing": sec_counts["artifacts_existing"],
            "sections_written": sec_counts["sections_written"],
            "text_units_written": sec_counts.get("text_units_written", 0),
        },
    )
    print("Status: PASS — baseline facts + transcripts + SEC qualitative filings stored.")

    if not skip_steward:
        _run_post_ingest_steward(tickers, ingest_run_ids={
            "fmp_financials": fmp_counts.get("ingest_run_id"),
            "fmp_segments": segment_counts.get("ingest_run_id"),
            "fmp_employees": employee_counts.get("ingest_run_id"),
            "fmp_transcripts": transcript_counts.get("ingest_run_id"),
            "xbrl_audit": (xbrl_audit_counts or {}).get("ingest_run_id"),
            "sec_filings": sec_counts.get("ingest_run_id"),
        })

    return 0


def _run_post_ingest_steward(
    tickers: list[str],
    *,
    ingest_run_ids: dict[str, int | None],
) -> None:
    """Run the steward against the just-ingested tickers and surface
    pending triage. Operator never has to remember to run the steward
    after an ingest — it happens here. New findings are dropped as a
    pending_triage JSON record under ``data/pending_triage/`` so the
    next AI session picks them up regardless of harness.
    """
    print()
    print("=" * 60)
    print("Post-ingest steward")
    print("=" * 60)

    with get_conn() as conn:
        scope = Scope(tickers=tickers, verticals=None, check_names=None)
        summary = run_steward(conn, scope=scope, actor="system:post_ingest")

    totals = summary.to_dict()["totals"]
    print(
        f"  scope: {tickers}  "
        f"new={totals['new']}  unchanged={totals['unchanged']}  "
        f"resolved={totals['resolved']}  suppressed={totals['suppressed']}  "
        f"({summary.duration_ms:.0f}ms)"
    )

    new_finding_ids: list[int] = []
    for r in summary.results:
        new_finding_ids.extend(r.new_finding_ids)

    if not new_finding_ids:
        print("  inbox: clean ✓")
        return

    # Drop a pending_triage record for the next AI session.
    PENDING_TRIAGE_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    record_path = PENDING_TRIAGE_DIR / f"{ts}_{'-'.join(tickers)}.json"
    record = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "tickers": tickers,
        "ingest_run_ids": {k: v for k, v in ingest_run_ids.items() if v is not None},
        "steward_totals": totals,
        "new_finding_ids": sorted(new_finding_ids),
        "per_check_summary": [
            {"name": r.name, "new": r.findings_new, "resolved": r.findings_resolved}
            for r in summary.results
            if r.findings_new or r.findings_resolved
        ],
        "next_step_hint": (
            f"Open the next chat session and ask: "
            f"'walk me through pending triage for {tickers}'. "
            f"The AI will read this record via "
            f"scripts/check_pending_triage.py and surface each finding."
        ),
    }
    record_path.write_text(json.dumps(record, indent=2))

    print(f"  ⚠ {len(new_finding_ids)} new finding(s) need triage")
    print(f"    pending_triage: {record_path.relative_to(REPO_ROOT)}")
    print(f"    next: open a chat session and say 'walk me through pending triage'")


if __name__ == "__main__":
    raise SystemExit(main())
