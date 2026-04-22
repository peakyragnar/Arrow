"""Backfill baseline FMP financial facts.

Usage:
    uv run scripts/backfill_fmp.py NVDA [MSFT ...]

Per ticker, in order:
  - Layer 1 IS: per-row subtotal ties (inline).
  - Layer 1 BS: per-row subtotal ties + balance identity (inline).
  - Layer 1 CF: per-row subtotal ties + cash roll-forward (inline).

Companies must be seeded first (scripts/seed_companies.py). Any
Layer-1 validation failure aborts with a structured error in stdout + the
ingest_runs.error_details JSONB.
"""

from __future__ import annotations

import sys
from typing import Any

from arrow.agents.fmp_ingest import (
    backfill_fmp_statements,
)
from arrow.db.connection import get_conn
from arrow.normalize.financials.load import (
    BSVerificationFailed,
    CFVerificationFailed,
    VerificationFailed,
)


def _print_statement_block(
    name: str, counts: dict, fw_key: str, fs_key: str, extra_l1: str = ""
) -> None:
    print(f"{name}:")
    print(f"  facts written:          {counts[fw_key]}")
    print(f"  facts superseded:       {counts[fs_key]}")
    print(f"  Layer 1{extra_l1}:          enforced inline; all rows passed")


def _print_success(tickers: list[str], counts: dict[str, Any]) -> None:
    print(f"Backfilled IS + BS + CF for {', '.join(t.upper() for t in tickers)}:")
    print(f"  since_date:             {counts['since_date']} (calendar input)")
    fy_map = counts.get("min_fiscal_year_by_ticker", {})
    if fy_map:
        per_ticker = ", ".join(f"{t}=FY{fy}" for t, fy in sorted(fy_map.items()))
        print(f"  window start (FY):      rounded forward → {per_ticker}")
    print(f"  ingest_run_id:          {counts['ingest_run_id']}")
    print(f"  raw_responses written:  {counts['raw_responses']}")
    print(f"  rows processed:         {counts['rows_processed']}")
    print()
    _print_statement_block(
        "Income statement", counts,
        fw_key="is_facts_written", fs_key="is_facts_superseded",
        extra_l1=" (subtotal ties)",
    )
    print()
    _print_statement_block(
        "Balance sheet", counts,
        fw_key="bs_facts_written", fs_key="bs_facts_superseded",
        extra_l1=" (subtotal ties + balance identity)",
    )
    print()
    _print_statement_block(
        "Cash flow", counts,
        fw_key="cf_facts_written", fs_key="cf_facts_superseded",
        extra_l1=" (hard ties: cash roll-forward + top-level aggregation)",
    )
    cf_flags = counts.get("cf_flags_written", 0)
    if cf_flags:
        print(f"  soft-tie flags written: {cf_flags}")
        print(
            f"    CF subtotal-component drifts (cfo/cfi/cff vs sum of components)"
        )
        print(
            f"    loaded verbatim; review with: "
            f"uv run scripts/review_flags.py {' '.join(counts.get('min_fiscal_year_by_ticker', {}).keys())}"
        )
    print()
    print("Status: PASS — baseline FMP facts stored; SEC/audit runs separately.")


def _print_failure_header(kind: str, msg: str) -> None:
    print()
    print("=" * 70)
    print(f"VALIDATION FAILED — {kind}")
    print("=" * 70)
    print(msg)
    print()


def _print_is_verification_failed(e: VerificationFailed) -> None:
    _print_failure_header("Layer 1 IS (subtotal tie)", str(e))
    print(f"Period: {e.period_label}")
    for f in e.failures:
        print(f"  - {f.tie}")
        print(f"    filer (FMP)  : {f.filer}")
        print(f"    computed     : {f.computed}")
        print(f"    delta        : {f.delta}  (tolerance {f.tolerance})")


def _print_bs_verification_failed(e: BSVerificationFailed) -> None:
    _print_failure_header("Layer 1 BS (subtotal tie / balance identity)", str(e))
    print(f"Period: {e.period_label}")
    for f in e.failures:
        print(f"  - {f.tie}")
        print(f"    filer (FMP)  : {f.filer}")
        print(f"    computed     : {f.computed}")
        print(f"    delta        : {f.delta}  (tolerance {f.tolerance})")


def _print_cf_verification_failed(e: CFVerificationFailed) -> None:
    _print_failure_header("Layer 1 CF (subtotal tie)", str(e))
    print(f"Period: {e.period_label}")
    for f in e.failures:
        print(f"  - {f.tie}")
        print(f"    filer (FMP)  : {f.filer}")
        print(f"    computed     : {f.computed}")
        print(f"    delta        : {f.delta}  (tolerance {f.tolerance})")


def main() -> int:
    from datetime import date as _d

    from arrow.agents.fmp_ingest import DEFAULT_SINCE_DATE

    usage = (
        "Usage: backfill_fmp.py "
        "[--since YYYY-MM-DD] [--until YYYY-MM-DD] [--scoped] TICKER [TICKER ...]"
    )

    args = sys.argv[1:]
    since_date = None
    until_date = None
    scoped = False

    def _pop_date_flag(flag: str):
        nonlocal args
        if flag not in args:
            return None
        i = args.index(flag)
        if i + 1 >= len(args):
            print(usage, file=sys.stderr)
            sys.exit(2)
        try:
            y, m, d = args[i + 1].split("-")
            val = _d(int(y), int(m), int(d))
        except Exception as e:
            print(f"Invalid {flag} date: {e}", file=sys.stderr)
            sys.exit(2)
        args = args[:i] + args[i + 2:]
        return val

    since_date = _pop_date_flag("--since")
    until_date = _pop_date_flag("--until")
    if "--scoped" in args:
        scoped = True
        args = [a for a in args if a != "--scoped"]

    if not args:
        print(usage, file=sys.stderr)
        return 2

    # Guard: a non-default window must be explicitly opted into. The default
    # window (since_date=DEFAULT_SINCE_DATE, until_date=None) is what loads
    # the complete ~5-year validated horizon every formula and dashboard
    # downstream assumes. Silent partial backfills caused real data gaps
    # once (DELL loaded as FY2023–FY2025 only) — forcing --scoped for
    # anything narrower makes that accident impossible to repeat.
    is_custom_window = (
        (since_date is not None and since_date != DEFAULT_SINCE_DATE)
        or (until_date is not None)
    )
    if is_custom_window and not scoped:
        print(
            "ERROR: --since / --until differ from defaults "
            f"(default since={DEFAULT_SINCE_DATE.isoformat()}, until=None).\n"
            "       Partial backfills are the wrong default; they silently "
            "skip fiscal years.\n"
            "       If this narrow window is intentional (dev/test/bisect), "
            "re-run with --scoped.",
            file=sys.stderr,
        )
        return 2

    tickers = args
    kwargs: dict = {}
    if since_date is not None:
        kwargs["since_date"] = since_date
    if until_date is not None:
        kwargs["until_date"] = until_date
    try:
        with get_conn() as conn:
            counts = backfill_fmp_statements(conn, tickers, **kwargs)
    except VerificationFailed as e:
        _print_is_verification_failed(e)
        return 1
    except BSVerificationFailed as e:
        _print_bs_verification_failed(e)
        return 1
    except CFVerificationFailed as e:
        _print_cf_verification_failed(e)
        return 1

    _print_success(tickers, counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
