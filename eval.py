"""
Evaluation script: compares extracted data against golden eval.

Usage:
    python3 eval.py --ticker NVDA
    python3 eval.py --ticker NVDA --verbose
"""

import argparse
import json
import os
import sys

# Components to evaluate
EVAL_COMPONENTS = [
    "revenue_q", "cogs_q", "operating_income_q", "rd_expense_q",
    "income_tax_expense_q", "pretax_income_q", "net_income_q",
    "interest_expense_q",
    "equity_q", "short_term_debt_q", "long_term_debt_q",
    "operating_lease_liabilities_q", "cash_q", "short_term_investments_q",
    "accounts_receivable_q", "inventory_q", "accounts_payable_q",
    "total_assets_q",
    "cfo_q", "capex_q", "dna_q", "acquisitions_q", "sbc_q",
    "diluted_shares_q",
]

# R&D and employee fields (mapped from golden names to extracted names)
EVAL_COMPUTED = {
    "rd_amortization_q_manual": "rd_amortization_q",
    "rd_asset_q_manual": "rd_asset_q",
    "rd_OI_Adjustment": "rd_OI_adjustment_q",
    "employee_count_annual_manual": "employee_count",
}


def load_golden(ticker: str) -> list:
    path = os.path.join("golden", f"{ticker.lower()}.json")
    with open(path) as f:
        return json.load(f)


def load_extracted(ticker: str) -> list:
    path = os.path.join("output", f"{ticker.lower()}.json")
    with open(path) as f:
        return json.load(f)


def match_quarters(golden: list, extracted: list) -> tuple:
    """Match golden records to extracted records by fiscal year and period.
    Returns (pairs, missing_from_extracted, extra_in_extracted)."""
    golden_map = {}
    for r in golden:
        key = (r["fiscal_year"], r["fiscal_period"])
        golden_map[key] = r

    ext_map = {}
    for r in extracted:
        key = (r["fiscal_year"], r["fiscal_period"])
        ext_map[key] = r

    pairs = []
    missing_quarters = []  # in golden but not extracted
    for key, g in golden_map.items():
        e = ext_map.get(key)
        if e:
            pairs.append((g, e))
        else:
            missing_quarters.append(key)

    extra_quarters = [k for k in ext_map if k not in golden_map]

    return pairs, sorted(missing_quarters), sorted(extra_quarters)


def evaluate(golden: list, extracted: list, verbose: bool = False) -> bool:
    """Evaluate and print results. Returns True if all checks pass."""
    pairs, missing_quarters, extra_quarters = match_quarters(golden, extracted)

    if not pairs and not missing_quarters:
        print("No matching quarters found!")
        return False

    total_fields = 0
    exact_matches = 0
    close_matches = 0  # within 1% tolerance
    mismatches = []
    missing = 0

    for g, e in pairs:
        fy = g["fiscal_year"]
        fp = g["fiscal_period"]
        quarter_label = f"FY{fy} {fp}"

        # Build field list: direct components + computed (golden_name -> extracted_name)
        fields = [(comp, comp) for comp in EVAL_COMPONENTS]
        fields += [(gname, ename) for gname, ename in EVAL_COMPUTED.items()]

        for golden_key, extract_key in fields:
            golden_val = g.get(golden_key)
            extract_val = e.get(extract_key)

            if golden_val is None:
                continue

            comp = golden_key  # for display
            total_fields += 1

            if extract_val is None:
                missing += 1
                if verbose:
                    print(f"  MISSING  {quarter_label:12s} {comp:30s}  golden={golden_val:>20,}")
                continue

            if golden_val == extract_val:
                exact_matches += 1
            elif golden_val != 0 and abs(extract_val - golden_val) / abs(golden_val) < 0.01:
                close_matches += 1
                if verbose:
                    diff = extract_val - golden_val
                    pct = diff / golden_val * 100
                    print(f"  CLOSE    {quarter_label:12s} {comp:30s}  golden={golden_val:>20,}  got={extract_val:>20,}  diff={diff:>15,} ({pct:+.2f}%)")
            else:
                mismatches.append((quarter_label, comp, golden_val, extract_val))

    print(f"\n{'='*80}")
    print(f"EVALUATION RESULTS: {len(pairs)} quarters matched")
    print(f"{'='*80}")

    if missing_quarters:
        labels = [f"FY{fy} {fp}" for fy, fp in missing_quarters]
        print(f"DROPPED QUARTERS ({len(missing_quarters)}): {', '.join(labels)}")
    if extra_quarters:
        labels = [f"FY{fy} {fp}" for fy, fp in extra_quarters]
        print(f"EXTRA QUARTERS ({len(extra_quarters)}):   {', '.join(labels)}")

    if total_fields > 0:
        print(f"Total fields:    {total_fields}")
        print(f"Exact matches:   {exact_matches:>5} ({exact_matches/total_fields*100:.1f}%)")
        print(f"Close (<1%):     {close_matches:>5} ({close_matches/total_fields*100:.1f}%)")
        print(f"Missing:         {missing:>5} ({missing/total_fields*100:.1f}%)")
        print(f"Mismatches:      {len(mismatches):>5} ({len(mismatches)/total_fields*100:.1f}%)")
    print(f"{'='*80}")

    if mismatches:
        print(f"\nMISMATCHES ({len(mismatches)}):")
        for quarter, comp, golden_val, extract_val in mismatches:
            diff = extract_val - golden_val
            if golden_val != 0:
                pct = diff / golden_val * 100
                print(f"  {quarter:12s} {comp:30s}  golden={golden_val:>20,}  got={extract_val:>20,}  diff={diff:>15,} ({pct:+.2f}%)")
            else:
                print(f"  {quarter:12s} {comp:30s}  golden={golden_val:>20,}  got={extract_val:>20,}  diff={diff:>15,}")

    if missing and verbose:
        print(f"\n(Missing fields shown above with --verbose)")

    passed = len(mismatches) == 0 and missing == 0 and len(missing_quarters) == 0
    return passed


def main():
    parser = argparse.ArgumentParser(description="Evaluate extraction against golden data")
    parser.add_argument("--ticker", required=True, help="Stock ticker")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show all discrepancies")
    args = parser.parse_args()

    golden = load_golden(args.ticker)
    extracted = load_extracted(args.ticker)
    passed = evaluate(golden, extracted, args.verbose)
    if not passed:
        sys.exit(1)


if __name__ == "__main__":
    main()
