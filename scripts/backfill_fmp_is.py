"""Backfill FMP income-statement history for one or more tickers, with
full deterministic validation (Layer 1 subtotal ties + Layer 3 period
arithmetic + Layer 5 SEC XBRL anchor cross-check).

Usage:
    uv run scripts/backfill_fmp_is.py NVDA [MSFT ...]

Companies must be seeded first (scripts/seed_companies.py). The ingest
aborts on any verification failure and records structured error_details
in ingest_runs.error_details. Output is designed to be debuggable: on
failure, the specific divergences (concept, period, FMP value, XBRL
value, delta, tolerance, source filing accn) are surfaced so you can
investigate a specific number without a separate query.
"""

from __future__ import annotations

import sys
from typing import Any

from arrow.agents.fmp_ingest import (
    PeriodArithmeticViolation,
    XBRLDivergenceFailed,
    backfill_fmp_is,
)
from arrow.db.connection import get_conn
from arrow.normalize.financials.load import VerificationFailed


def _print_success(tickers: list[str], counts: dict[str, Any]) -> None:
    print(f"Backfilled IS for {', '.join(t.upper() for t in tickers)}:")
    print(f"  since_date:          {counts['since_date']}")
    print(f"  ingest_run_id:       {counts['ingest_run_id']}")
    print(f"  raw_responses:       {counts['raw_responses']}")
    print(f"  rows_processed:      {counts['rows_processed']}")
    print(f"  facts written:       {counts['financial_facts_written']}")
    print(f"  facts superseded:    {counts['financial_facts_superseded']}")
    print()
    print("Validation:")
    print(f"  Layer 1 (per-row subtotal ties):     enforced inline; all rows passed")
    print(f"  Layer 3 (Q1+Q2+Q3+Q4 = FY):          {counts['layer3_identities_checked']} identities passed")
    checked = counts["anchors_checked"]
    matched = counts["anchors_matched"]
    stored = counts["anchors_stored"]
    print(f"  Layer 5 (SEC XBRL anchor match):     {matched}/{checked} matched "
          f"({stored} anchor facts stored; Q4 derived as FY − 9M YTD)")
    gaps = counts.get("anchors_not_in_xbrl", [])
    if gaps:
        print(f"\n  Anchors without an XBRL counterpart ({len(gaps)}, informational):")
        for g in gaps[:10]:
            print(f"    - {g['concept']} @ {g['period_end']} ({g['period_type']})")
        if len(gaps) > 10:
            print(f"    ... and {len(gaps) - 10} more (see ingest_runs.counts)")
    print()
    print("Status: PASS — every stored fact validated by internal arithmetic + anchor XBRL match.")


def _print_failure_header(kind: str, msg: str) -> None:
    print()
    print("=" * 70)
    print(f"VALIDATION FAILED — {kind}")
    print("=" * 70)
    print(msg)
    print()


def _print_verification_failed(e: VerificationFailed) -> None:
    _print_failure_header("Layer 1 (subtotal tie)", str(e))
    print(f"Period: {e.period_label}")
    for f in e.failures:
        print(f"  - {f.tie}")
        print(f"    filer (FMP)  : {f.filer}")
        print(f"    computed     : {f.computed}")
        print(f"    delta        : {f.delta}  (tolerance {f.tolerance})")


def _print_period_arithmetic(e: PeriodArithmeticViolation) -> None:
    _print_failure_header("Layer 3 (Q1+Q2+Q3+Q4 = FY)", str(e))
    for f in e.failures:
        print(f"  - {f.concept} / FY{f.fiscal_year}")
        print(f"    Q1+Q2+Q3+Q4  : {f.quarters_sum}")
        print(f"    FY           : {f.annual}")
        print(f"    delta        : {f.delta}  (tolerance {f.tolerance})")


def _print_xbrl_divergence(e: XBRLDivergenceFailed) -> None:
    _print_failure_header(
        "Layer 5 (SEC XBRL anchor mismatch)",
        f"{len(e.result.divergences)} anchor(s) diverged; "
        f"{e.result.anchors_matched}/{e.result.anchors_checked} matched before abort.",
    )
    for d in e.result.divergences:
        label = f"FY{d.fiscal_year}"
        if d.fiscal_quarter is not None:
            label += f" Q{d.fiscal_quarter}"
        print(f"  - {d.concept}  {label}  ({d.period_type}, period_end={d.period_end})")
        print(f"    FMP value    : {d.fmp_value}")
        print(f"    XBRL value   : {d.xbrl_value}  (tag: {d.xbrl_tag}, derivation: {d.derivation})")
        print(f"    XBRL source  : accn {d.xbrl_accn}, filed {d.xbrl_filed}")
        print(f"    delta        : {d.delta}  (tolerance {d.tolerance})")


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: backfill_fmp_is.py TICKER [TICKER ...]", file=sys.stderr)
        return 2

    tickers = sys.argv[1:]
    try:
        with get_conn() as conn:
            counts = backfill_fmp_is(conn, tickers)
    except VerificationFailed as e:
        _print_verification_failed(e)
        return 1
    except PeriodArithmeticViolation as e:
        _print_period_arithmetic(e)
        return 1
    except XBRLDivergenceFailed as e:
        _print_xbrl_divergence(e)
        return 1

    _print_success(tickers, counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
