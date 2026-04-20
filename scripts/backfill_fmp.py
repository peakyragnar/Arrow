"""Backfill FMP income statement + balance sheet with full validation.

Usage:
    uv run scripts/backfill_fmp.py NVDA [MSFT ...]

Per ticker, in order:
  - Layer 1 IS: per-row subtotal ties (inline).
  - Layer 1 BS: per-row subtotal ties + balance identity (inline).
  - Layer 3:    Q1+Q2+Q3+Q4 = FY on every IS flow bucket, per fiscal year.
  - Layer 5 IS: top-line IS anchors vs SEC XBRL (revenue, gross_profit,
                operating_income, pre-tax, net_income). Q4 derived from
                XBRL as FY − 9M YTD.
  - Layer 5 BS: top-line BS anchors vs SEC XBRL (total_assets,
                total_liabilities, total_equity, total_liab_and_equity,
                cash_and_equivalents). Instant-type facts, matched by
                end date.

Companies must be seeded first (scripts/seed_companies.py). Any
validation failure aborts with a structured error in stdout + the
ingest_runs.error_details JSONB.
"""

from __future__ import annotations

import sys
from typing import Any

from arrow.agents.fmp_ingest import (
    PeriodArithmeticViolation,
    XBRLDivergenceFailed,
    backfill_fmp_statements,
)
from arrow.db.connection import get_conn
from arrow.normalize.financials.load import (
    BSVerificationFailed,
    VerificationFailed,
)


def _print_success(tickers: list[str], counts: dict[str, Any]) -> None:
    print(f"Backfilled IS + BS for {', '.join(t.upper() for t in tickers)}:")
    print(f"  since_date:             {counts['since_date']} (calendar input)")
    fy_map = counts.get("min_fiscal_year_by_ticker", {})
    if fy_map:
        per_ticker = ", ".join(f"{t}=FY{fy}" for t, fy in sorted(fy_map.items()))
        print(f"  window start (FY):      rounded forward → {per_ticker}")
    print(f"  ingest_run_id:          {counts['ingest_run_id']}")
    print(f"  raw_responses written:  {counts['raw_responses']}")
    print(f"  rows processed:         {counts['rows_processed']}")
    print()
    print("Income statement:")
    print(f"  facts written:          {counts['is_facts_written']}")
    print(f"  facts superseded:       {counts['is_facts_superseded']}")
    print(f"  Layer 1 (subtotal ties):   enforced inline; all rows passed")
    print(f"  Layer 3 (Q1+Q2+Q3+Q4=FY):  {counts['layer3_identities_checked']} identities passed")
    is_checked = counts["is_anchors_checked"]
    is_matched = counts["is_anchors_matched"]
    print(f"  Layer 5 (SEC XBRL anchors): {is_matched}/{is_checked} matched")
    is_gaps = counts.get("is_anchors_not_in_xbrl", [])
    if is_gaps:
        print(f"    IS anchors without XBRL counterpart ({len(is_gaps)}, informational):")
        for g in is_gaps[:5]:
            print(f"      - {g['concept']} @ {g['period_end']} ({g['period_type']})")
        if len(is_gaps) > 5:
            print(f"      ... +{len(is_gaps) - 5} more")
    print()
    print("Balance sheet:")
    print(f"  facts written:          {counts['bs_facts_written']}")
    print(f"  facts superseded:       {counts['bs_facts_superseded']}")
    print(f"  Layer 1 (subtotal ties + balance identity): enforced inline; all rows passed")
    bs_checked = counts["bs_anchors_checked"]
    bs_matched = counts["bs_anchors_matched"]
    print(f"  Layer 5 (SEC XBRL anchors): {bs_matched}/{bs_checked} matched")
    bs_gaps = counts.get("bs_anchors_not_in_xbrl", [])
    if bs_gaps:
        print(f"    BS anchors without XBRL counterpart ({len(bs_gaps)}, informational):")
        for g in bs_gaps[:5]:
            print(f"      - {g['concept']} @ {g['period_end']} ({g['period_type']})")
        if len(bs_gaps) > 5:
            print(f"      ... +{len(bs_gaps) - 5} more")
    print()
    print("Status: PASS — every stored fact validated by internal arithmetic + anchor XBRL match.")


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


def _print_period_arithmetic(e: PeriodArithmeticViolation) -> None:
    _print_failure_header("Layer 3 (Q1+Q2+Q3+Q4 = FY)", str(e))
    for f in e.failures:
        print(f"  - {f.concept} / FY{f.fiscal_year}")
        print(f"    Q1+Q2+Q3+Q4  : {f.quarters_sum}")
        print(f"    FY           : {f.annual}")
        print(f"    delta        : {f.delta}  (tolerance {f.tolerance})")


def _print_xbrl_divergence(e: XBRLDivergenceFailed) -> None:
    _print_failure_header(
        f"Layer 5 {e.statement.upper()} (SEC XBRL anchor mismatch)",
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
        print("Usage: backfill_fmp.py TICKER [TICKER ...]", file=sys.stderr)
        return 2

    tickers = sys.argv[1:]
    try:
        with get_conn() as conn:
            counts = backfill_fmp_statements(conn, tickers)
    except VerificationFailed as e:
        _print_is_verification_failed(e)
        return 1
    except BSVerificationFailed as e:
        _print_bs_verification_failed(e)
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
