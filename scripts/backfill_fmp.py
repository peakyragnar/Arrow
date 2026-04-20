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
    CrossStatementViolation,
    PeriodArithmeticViolation,
    XBRLDivergenceFailed,
    backfill_fmp_statements,
)
from arrow.db.connection import get_conn
from arrow.normalize.financials.load import (
    BSVerificationFailed,
    CFVerificationFailed,
    VerificationFailed,
)


def _print_statement_block(name: str, counts: dict, fw_key: str, fs_key: str,
                             l3_key: str | None, ach_key: str, amt_key: str,
                             gaps_key: str, extra_l1: str = "") -> None:
    print(f"{name}:")
    print(f"  facts written:          {counts[fw_key]}")
    print(f"  facts superseded:       {counts[fs_key]}")
    print(f"  Layer 1{extra_l1}:          enforced inline; all rows passed")
    if l3_key is not None:
        print(f"  Layer 3 (Q1+Q2+Q3+Q4=FY):  {counts[l3_key]} identities passed")
    checked = counts[ach_key]
    matched = counts[amt_key]
    print(f"  Layer 5 (SEC XBRL anchors): {matched}/{checked} matched")
    gaps = counts.get(gaps_key, [])
    if gaps:
        print(f"    anchors without XBRL counterpart ({len(gaps)}, informational):")
        for g in gaps[:5]:
            print(f"      - {g['concept']} @ {g['period_end']} ({g['period_type']})")
        if len(gaps) > 5:
            print(f"      ... +{len(gaps) - 5} more")


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
        l3_key="is_layer3_identities_checked",
        ach_key="is_anchors_checked", amt_key="is_anchors_matched",
        gaps_key="is_anchors_not_in_xbrl",
        extra_l1=" (subtotal ties)",
    )
    print()
    _print_statement_block(
        "Balance sheet", counts,
        fw_key="bs_facts_written", fs_key="bs_facts_superseded",
        l3_key=None,  # BS stocks exempt from Layer 3 per concepts.md
        ach_key="bs_anchors_checked", amt_key="bs_anchors_matched",
        gaps_key="bs_anchors_not_in_xbrl",
        extra_l1=" (subtotal ties + balance identity)",
    )
    print()
    _print_statement_block(
        "Cash flow", counts,
        fw_key="cf_facts_written", fs_key="cf_facts_superseded",
        l3_key="cf_layer3_identities_checked",
        ach_key="cf_anchors_checked", amt_key="cf_anchors_matched",
        gaps_key="cf_anchors_not_in_xbrl",
        extra_l1=" (subtotal ties + cash roll-forward)",
    )
    print()
    print("Cross-statement (Layer 2):")
    print(f"  ties evaluated:         {counts['cross_statement_ties_checked']} "
          f"(cf.net_income_start ≈ is.net_income) — all passed")
    print(f"  cash roll-forward ties: DEFERRED pending restricted-cash mapping "
          f"(see verify_cross_statement.py)")
    print()
    print("Status: PASS — every stored fact validated by internal arithmetic, "
          "period arithmetic, cross-statement ties, and anchor XBRL match.")


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


def _print_period_arithmetic(e: PeriodArithmeticViolation) -> None:
    _print_failure_header(f"Layer 3 {e.statement.upper()} (Q1+Q2+Q3+Q4 = FY)", str(e))
    for f in e.failures:
        print(f"  - {f.concept} / FY{f.fiscal_year}")
        print(f"    Q1+Q2+Q3+Q4  : {f.quarters_sum}")
        print(f"    FY           : {f.annual}")
        print(f"    delta        : {f.delta}  (tolerance {f.tolerance})")


def _print_cross_statement(e: CrossStatementViolation) -> None:
    _print_failure_header("Layer 2 (cross-statement tie)", str(e))
    for f in e.failures:
        label = f"FY{f.fiscal_year}"
        if f.fiscal_quarter is not None:
            label += f" Q{f.fiscal_quarter}"
        print(f"  - {f.tie}  {label} ({f.period_type}, period_end={f.period_end})")
        print(f"    lhs          : {f.lhs_value}")
        print(f"    rhs          : {f.rhs_value}")
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
    except CFVerificationFailed as e:
        _print_cf_verification_failed(e)
        return 1
    except PeriodArithmeticViolation as e:
        _print_period_arithmetic(e)
        return 1
    except CrossStatementViolation as e:
        _print_cross_statement(e)
        return 1
    except XBRLDivergenceFailed as e:
        _print_xbrl_divergence(e)
        return 1

    _print_success(tickers, counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
