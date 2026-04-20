"""Standalone FMP ↔ SEC XBRL anchor reconciliation (Build Order step 9.5).

Usage:
    uv run scripts/reconcile_fmp_vs_xbrl.py NVDA [MSFT ...]

Does NOT re-ingest FMP data. Fetches fresh SEC XBRL companyfacts and
compares against currently-stored financial_facts. Surfaces any
divergence — FMP restatement, vendor drift, or a late-filed SEC
amendment that disagrees with what we previously ingested.

Exit codes:
  0  — all anchors matched
  1  — divergences found (CLI printed; full detail in ingest_runs.error_details)
  2  — usage error
"""

from __future__ import annotations

import sys
from typing import Any

from arrow.agents.fmp_reconcile import CompanyNotSeeded, reconcile_fmp_vs_xbrl
from arrow.db.connection import get_conn


def _print_success(tickers: list[str], counts: dict[str, Any]) -> None:
    print(f"Reconciled {', '.join(tickers)} against SEC XBRL:")
    print(f"  ingest_run_id:          {counts['ingest_run_id']}")
    print(f"  XBRL payloads fetched:  {counts['raw_responses']}")
    print()
    print(f"  IS anchors:   {counts['is_anchors_matched']}/{counts['is_anchors_checked']} "
          f"matched ({counts['is_anchors_stored']} stored)")
    print(f"  BS anchors:   {counts['bs_anchors_matched']}/{counts['bs_anchors_checked']} "
          f"matched ({counts['bs_anchors_stored']} stored)")
    print(f"  CF anchors:   {counts['cf_anchors_matched']}/{counts['cf_anchors_checked']} "
          f"matched ({counts['cf_anchors_stored']} stored)")
    print()
    print("Status: CLEAN — FMP-stored values match SEC XBRL on all checkable anchors.")


def _print_divergences(tickers: list[str], counts: dict[str, Any]) -> None:
    divs = counts["divergences"]
    print(f"Reconciled {', '.join(tickers)} against SEC XBRL:")
    print(f"  ingest_run_id:          {counts['ingest_run_id']}")
    print(f"  XBRL payloads fetched:  {counts['raw_responses']}")
    print()
    print(f"  IS anchors:   {counts['is_anchors_matched']}/{counts['is_anchors_checked']} matched")
    print(f"  BS anchors:   {counts['bs_anchors_matched']}/{counts['bs_anchors_checked']} matched")
    print(f"  CF anchors:   {counts['cf_anchors_matched']}/{counts['cf_anchors_checked']} matched")
    print()
    print("=" * 70)
    print(f"DIVERGENCES ({len(divs)})")
    print("=" * 70)
    for d in divs:
        label = f"FY{d['fiscal_year']}"
        if d["fiscal_quarter"] is not None:
            label += f" Q{d['fiscal_quarter']}"
        print(f"  - [{d['statement']}] {d['concept']}  {label}  ({d['period_type']}, period_end={d['period_end']})")
        print(f"    FMP value    : {d['fmp_value']}")
        print(f"    XBRL value   : {d['xbrl_value']}  (tag: {d['xbrl_tag']}, derivation: {d['derivation']})")
        print(f"    XBRL source  : accn {d['xbrl_accn']}, filed {d['xbrl_filed']}")
        print(f"    delta        : {d['delta']}  (tolerance {d['tolerance']})")
    print()
    print("Status: DIVERGENCES FOUND — investigate via ingest_runs.error_details.")


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: reconcile_fmp_vs_xbrl.py TICKER [TICKER ...]", file=sys.stderr)
        return 2

    tickers = [t.upper() for t in sys.argv[1:]]
    try:
        with get_conn() as conn:
            counts = reconcile_fmp_vs_xbrl(conn, tickers)
    except CompanyNotSeeded as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if counts["status"] == "succeeded":
        _print_success(tickers, counts)
        return 0
    else:
        _print_divergences(tickers, counts)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
