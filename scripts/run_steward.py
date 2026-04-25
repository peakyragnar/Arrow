"""Run the steward (data-trust) check sweep.

The steward surfaces data-quality findings the operator should act on.
Findings appear in the dashboard `/findings` pane; this CLI is the
operator's "run it now and see what came up" entrypoint.

Usage:

    # Universe sweep — run every registered check, no scope filter.
    uv run scripts/run_steward.py

    # Scope to one or more tickers.
    uv run scripts/run_steward.py --ticker PLTR
    uv run scripts/run_steward.py --ticker PLTR --ticker MSFT

    # Run only one named check (across the same scope filters).
    uv run scripts/run_steward.py --check zero_row_runs

    # Verbose: stream per-finding lines to stderr in addition to the
    # JSON summary on stdout.
    uv run scripts/run_steward.py --verbose

Output:
    stdout: one JSON object — the run summary (see RunSummary.to_dict()).
    stderr: human-readable per-finding lines when --verbose.
    exit:   0 if every check ran without error,
            1 if any check raised (run continued; see summary.error).

The steward never mutates source data. Findings open with structured
suggested_action; the operator decides next via the dashboard or CLI
lifecycle wrappers (V1 manual; V2 agent suggests; V3 promoted checks
auto-execute). See docs/architecture/steward.md.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from arrow.db.connection import get_conn
from arrow.steward.registry import Scope
from arrow.steward.runner import RunSummary, run_steward

# Import the checks package so registration side-effects fire.
import arrow.steward.checks  # noqa: F401


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the steward check sweep and emit a JSON summary.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--ticker",
        action="append",
        default=None,
        metavar="TICKER",
        help=(
            "Scope to a ticker (repeat for multiple). Cross-cutting findings "
            "(no ticker) are not auto-resolved by ticker-scoped runs — they "
            "wait for a universe sweep."
        ),
    )
    parser.add_argument(
        "--vertical",
        action="append",
        default=None,
        metavar="VERTICAL",
        help=(
            "Scope to a vertical (financials | segments | employees | "
            "sec_qual | press_release). Cross-cutting checks always run "
            "regardless of this filter."
        ),
    )
    parser.add_argument(
        "--check",
        action="append",
        default=None,
        metavar="NAME",
        help="Run only the named check(s). Repeat for multiple.",
    )
    parser.add_argument(
        "--actor",
        default="human:michael",
        help=(
            "Actor recorded on every state change. Default: human:michael. "
            "Use system:cron for scheduled sweeps; future agent runs use "
            "agent:steward_v1, etc."
        ),
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Stream per-finding summaries to stderr.",
    )
    return parser.parse_args(argv)


def _build_scope(args: argparse.Namespace) -> Scope:
    return Scope(
        tickers=[t.upper() for t in args.ticker] if args.ticker else None,
        verticals=args.vertical,
        check_names=args.check,
    )


def _emit_verbose(summary: RunSummary, *, conn) -> None:
    """Stream a one-line description of each open or newly-resolved finding
    that this run touched. For human consumption from the terminal."""
    print(file=sys.stderr)
    for r in summary.results:
        if r.error:
            print(f"  [ERROR] {r.name}: {r.error}", file=sys.stderr)
            continue
        outcomes = []
        if r.findings_new:
            outcomes.append(f"{r.findings_new} new")
        if r.findings_unchanged:
            outcomes.append(f"{r.findings_unchanged} re-observed")
        if r.findings_suppressed:
            outcomes.append(f"{r.findings_suppressed} suppressed")
        if r.findings_resolved:
            outcomes.append(f"{r.findings_resolved} resolved")
        if not outcomes:
            outcomes.append("no findings")
        print(
            f"  {r.name}  ({r.duration_ms:.1f}ms)  — {', '.join(outcomes)}",
            file=sys.stderr,
        )

    if summary.findings_new:
        print("\n  New findings this run:", file=sys.stderr)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, severity, ticker, summary
                FROM data_quality_findings
                WHERE status = 'open' AND created_by = %s
                ORDER BY id DESC
                LIMIT %s;
                """,
                (summary.actor, summary.findings_new),
            )
            for fid, sev, ticker, summ in cur.fetchall():
                ticker_label = f"[{ticker}] " if ticker else ""
                print(f"    #{fid}  {sev:<13} {ticker_label}{summ}", file=sys.stderr)


def _emit_json(summary: RunSummary) -> None:
    print(json.dumps(summary.to_dict(), default=str, indent=2))


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    scope = _build_scope(args)

    with get_conn() as conn:
        summary = run_steward(conn, scope=scope, actor=args.actor)
        if args.verbose:
            _emit_verbose(summary, conn=conn)
        _emit_json(summary)

    any_error = any(r.error for r in summary.results)
    return 1 if any_error else 0


if __name__ == "__main__":
    raise SystemExit(main())
