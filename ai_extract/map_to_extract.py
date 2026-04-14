"""
Map AI extraction JSON to quarterly records.

Reads AI extraction output (all statements) and produces comprehensive quarterly
records containing ALL extracted data — every line item from IS, BS, CF plus
calculation components. The original 24 deterministic pipeline fields are included
for backward compatibility and accuracy testing.

Handles:
- Selecting quarterly IS periods (not YTD) from 10-Q filings
- Deriving quarterly CF from YTD CF by subtraction
- Operating lease liabilities from calculation_components or not-on-statement items
- Short-term debt from calculation_components or multiple XBRL concepts
- Sign conventions matching deterministic pipeline

Everything stays in ai_extract/ — does not modify any files outside this folder.

Usage:
    python3 ai_extract/map_to_extract.py --ticker NVDA --filings ai_extract/nvda_q1fy26_all_statements.json ai_extract/nvda_q2fy26_all.json ...
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta


# === FIELD DEFINITIONS ===
# Each maps XBRL concept -> (field_name, category, unit_type)
# Categories: 'is' (income statement, duration), 'bs' (balance sheet, instant),
#             'cf' (cash flow, duration/YTD)
# Unit types: 'usd' (millions->raw), 'shares' (millions->raw), 'per_share' (keep as-is)

IS_CONCEPTS = {
    # Core 24-field items
    'us-gaap:Revenues': 'revenue_q',
    'us-gaap:CostOfRevenue': 'cogs_q',
    'us-gaap:GrossProfit': 'gross_profit_q',
    'us-gaap:ResearchAndDevelopmentExpense': 'rd_expense_q',
    'us-gaap:SellingGeneralAndAdministrativeExpense': 'sga_q',
    'us-gaap:OperatingExpenses': 'total_opex_q',
    'us-gaap:OperatingIncomeLoss': 'operating_income_q',
    'us-gaap:InvestmentIncomeInterest': 'interest_income_q',
    'us-gaap:InterestExpenseNonoperating': 'interest_expense_q',
    'us-gaap:OtherNonoperatingIncomeExpense': 'other_nonop_income_q',
    'us-gaap:NonoperatingIncomeExpense': 'total_nonop_income_q',
    'us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest': 'pretax_income_q',
    'us-gaap:IncomeTaxExpenseBenefit': 'income_tax_expense_q',
    'us-gaap:NetIncomeLoss': 'net_income_q',
}

IS_SHARES_CONCEPTS = {
    'us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding': 'diluted_shares_q',
    'us-gaap:WeightedAverageNumberOfSharesOutstandingBasic': 'basic_shares_q',
}

IS_PER_SHARE_CONCEPTS = {
    'us-gaap:EarningsPerShareBasic': 'eps_basic_q',
    'us-gaap:EarningsPerShareDiluted': 'eps_diluted_q',
}

BS_CONCEPTS = {
    # Current assets
    'us-gaap:CashAndCashEquivalentsAtCarryingValue': 'cash_q',
    'us-gaap:MarketableSecuritiesCurrent': 'short_term_investments_q',
    'us-gaap:AccountsReceivableNetCurrent': 'accounts_receivable_q',
    'us-gaap:InventoryNet': 'inventory_q',
    'us-gaap:PrepaidExpenseAndOtherAssetsCurrent': 'prepaid_q',
    'us-gaap:AssetsCurrent': 'total_current_assets_q',
    # Non-current assets
    'us-gaap:PropertyPlantAndEquipmentNet': 'ppe_q',
    'us-gaap:OperatingLeaseRightOfUseAsset': 'operating_lease_assets_q',
    'us-gaap:Goodwill': 'goodwill_q',
    'us-gaap:IntangibleAssetsNetExcludingGoodwill': 'intangibles_q',
    'us-gaap:DeferredIncomeTaxAssetsNet': 'deferred_tax_assets_q',
    'us-gaap:OtherAssetsNoncurrent': 'other_noncurrent_assets_q',
    'us-gaap:Assets': 'total_assets_q',
    # Current liabilities
    'us-gaap:AccountsPayableCurrent': 'accounts_payable_q',
    'us-gaap:AccruedLiabilitiesCurrent': 'accrued_liabilities_q',
    'us-gaap:LiabilitiesCurrent': 'total_current_liabilities_q',
    # Non-current liabilities
    'us-gaap:LongTermDebtNoncurrent': 'long_term_debt_q',
    'us-gaap:OperatingLeaseLiabilityNoncurrent': 'long_term_lease_liabilities_q',
    'us-gaap:OtherLiabilitiesNoncurrent': 'other_noncurrent_liabilities_q',
    'us-gaap:Liabilities': 'total_liabilities_q',
    # Equity
    'us-gaap:CommonStockValue': 'common_stock_q',
    'us-gaap:AdditionalPaidInCapital': 'apic_q',
    'us-gaap:AccumulatedOtherComprehensiveIncomeLossNetOfTax': 'aoci_q',
    'us-gaap:RetainedEarningsAccumulatedDeficit': 'retained_earnings_q',
    'us-gaap:StockholdersEquity': 'equity_q',
    'us-gaap:LiabilitiesAndStockholdersEquity': 'total_liabilities_and_equity_q',
}

CF_CONCEPTS = {
    # Operating activities
    'us-gaap:ShareBasedCompensation': 'sbc_q',
    'us-gaap:DepreciationDepletionAndAmortization': 'dna_q',
    'us-gaap:DeferredIncomeTaxExpenseBenefit': 'deferred_taxes_q',
    'us-gaap:GainLossOnInvestments': 'gain_loss_investments_q',
    'us-gaap:OtherNoncashIncomeExpense': 'other_noncash_q',
    'us-gaap:IncreaseDecreaseInAccountsReceivable': 'change_ar_q',
    'us-gaap:IncreaseDecreaseInInventories': 'change_inventory_q',
    'us-gaap:IncreaseDecreaseInPrepaidDeferredExpenseAndOtherAssets': 'change_prepaid_q',
    'us-gaap:IncreaseDecreaseInAccountsPayable': 'change_ap_q',
    'us-gaap:IncreaseDecreaseInAccruedLiabilitiesAndOtherOperatingLiabilities': 'change_accrued_q',
    'us-gaap:IncreaseDecreaseInOtherNoncurrentLiabilities': 'change_other_lt_liabilities_q',
    'us-gaap:NetCashProvidedByUsedInOperatingActivities': 'cfo_q',
    # Investing activities
    'us-gaap:PaymentsToAcquireProductiveAssets': 'capex_q',
    'us-gaap:PaymentsToAcquirePropertyPlantAndEquipment': 'capex_q',
    'us-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired': 'acquisitions_q',
    'us-gaap:ProceedsFromMaturitiesPrepaymentsAndCallsOfAvailableForSaleSecurities': 'proceeds_maturities_q',
    'us-gaap:ProceedsFromSaleOfAvailableForSaleSecuritiesDebt': 'proceeds_sales_securities_q',
    'us-gaap:PaymentsToAcquireAvailableForSaleSecuritiesDebt': 'purchases_securities_q',
    'us-gaap:NetCashProvidedByUsedInInvestingActivities': 'cfi_q',
    # Financing activities
    'us-gaap:PaymentsForRepurchaseOfCommonStock': 'share_repurchases_q',
    'us-gaap:PaymentsOfDividends': 'dividends_q',
    'us-gaap:RepaymentsOfDebt': 'debt_repayment_q',
    'us-gaap:ProceedsFromStockPlans': 'proceeds_stock_plans_q',
    'us-gaap:PaymentsRelatedToTaxWithholdingForShareBasedCompensation': 'tax_withholding_sbc_q',
    'us-gaap:NetCashProvidedByUsedInFinancingActivities': 'cff_q',
    # Net change and supplemental
    'us-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsPeriodIncreaseDecreaseIncludingExchangeRateEffect': 'net_change_cash_q',
    'us-gaap:IncomeTaxesPaidNet': 'taxes_paid_q',
}

# Fields stored as negative in deterministic output
NEGATE_FIELDS = {'capex_q', 'acquisitions_q', 'interest_expense_q'}

# All CF fields that need YTD-to-quarterly derivation
CF_FIELD_NAMES = set(CF_CONCEPTS.values())


def period_days(period_key):
    """Calculate the number of days in a duration period."""
    if '_' not in period_key:
        return 0
    start, end = period_key.split('_')
    d1 = datetime.strptime(start, '%Y-%m-%d')
    d2 = datetime.strptime(end, '%Y-%m-%d')
    return (d2 - d1).days


def find_value(line_items, xbrl_concept, period_key):
    """Find a value in line_items by XBRL concept and period."""
    for item in line_items:
        if item.get('xbrl_concept') == xbrl_concept:
            vals = item.get('values') or {}
            if period_key in vals:
                return vals[period_key]
    return None


def find_not_on_stmt_value(not_on_stmt, xbrl_concept, period_key):
    """Find a value in the not-on-statement items."""
    for item in not_on_stmt:
        if item.get('concept') == xbrl_concept:
            val = item.get('value')
            if isinstance(val, dict):
                return val.get(period_key)
            period = item.get('period', '')
            if period_key in period:
                return val
    return None


def get_all_duration_periods(line_items):
    """Get all duration period keys from line items, sorted by length (shortest first)."""
    periods = set()
    for item in line_items:
        for pk in (item.get('values') or {}):
            if '_' in pk:
                periods.add(pk)
    return sorted(periods, key=period_days)


def get_all_instant_periods(line_items):
    """Get all instant period keys from line items, sorted by date."""
    periods = set()
    for item in line_items:
        for pk in (item.get('values') or {}):
            if '_' not in pk:
                periods.add(pk)
    return sorted(periods)


def to_raw(val, field):
    """Convert AI extraction value to raw value based on field type."""
    if val is None:
        return None
    if field in ('eps_basic_q', 'eps_diluted_q'):
        return float(val)
    if field in ('diluted_shares_q', 'basic_shares_q'):
        return int(val * 1_000_000)
    return int(val) * 1_000_000


def extract_filing(filing_json):
    """
    Extract all available data from one filing.
    Returns a dict of {period_end: {field: raw_value}} for each period found.
    IS fields use quarterly periods, BS fields use instant, CF fields use YTD.
    """
    ai = filing_json['ai_extraction']
    is_items = ai.get('income_statement', {}).get('line_items', [])
    bs_items = ai.get('balance_sheet', {}).get('line_items', [])
    cf_items = ai.get('cash_flow', {}).get('line_items', [])
    bs_not_on = ai.get('balance_sheet', {}).get('xbrl_not_on_statement', [])

    # Get periods
    is_periods = get_all_duration_periods(is_items)
    bs_periods = get_all_instant_periods(bs_items)
    cf_periods = get_all_duration_periods(cf_items)

    # For IS: find the quarterly periods (shortest duration, ~90 days)
    quarterly_is_periods = [p for p in is_periods if period_days(p) < 120]

    results = {}

    # === INCOME STATEMENT (quarterly duration periods) ===
    for qp in quarterly_is_periods:
        period_end = qp.split('_')[1]
        period_start = qp.split('_')[0]
        if period_end not in results:
            results[period_end] = {'period_end': period_end, 'period_start': period_start}

        # USD fields
        for concept, field in IS_CONCEPTS.items():
            val = find_value(is_items, concept, qp)
            if val is not None:
                raw = to_raw(val, field)
                if field in NEGATE_FIELDS:
                    raw = -abs(raw)
                results[period_end][field] = raw

        # Shares fields
        for concept, field in IS_SHARES_CONCEPTS.items():
            val = find_value(is_items, concept, qp)
            if val is not None:
                results[period_end][field] = to_raw(val, field)

        # Per-share fields
        for concept, field in IS_PER_SHARE_CONCEPTS.items():
            val = find_value(is_items, concept, qp)
            if val is not None:
                results[period_end][field] = to_raw(val, field)

    # === BALANCE SHEET (instant periods) ===
    for ip in bs_periods:
        if ip not in results:
            results[ip] = {'period_end': ip}

        for concept, field in BS_CONCEPTS.items():
            val = find_value(bs_items, concept, ip)
            if val is not None:
                results[ip][field] = to_raw(val, field)

        # Short-term debt — try multiple concepts from line items
        std = find_value(bs_items, 'us-gaap:ShortTermBorrowings', ip)
        if std is None:
            std = find_value(bs_items, 'us-gaap:CommercialPaper', ip)
        if std is None:
            std = find_value(bs_items, 'us-gaap:DebtCurrent', ip)
        results[ip]['short_term_debt_q'] = to_raw(std, 'short_term_debt_q') or 0

        # Operating lease liabilities (total) — try not-on-statement first
        total_lease = find_not_on_stmt_value(bs_not_on, 'us-gaap:OperatingLeaseLiability', ip)
        if total_lease is not None:
            results[ip]['operating_lease_liabilities_q'] = int(total_lease) * 1_000_000
        else:
            current = find_not_on_stmt_value(bs_not_on, 'us-gaap:OperatingLeaseLiabilityCurrent', ip)
            noncurrent = find_value(bs_items, 'us-gaap:OperatingLeaseLiabilityNoncurrent', ip)
            c_val = int(current) * 1_000_000 if current else 0
            n_val = int(noncurrent) * 1_000_000 if noncurrent else 0
            if c_val or n_val:
                results[ip]['operating_lease_liabilities_q'] = c_val + n_val

    # === CASH FLOW (YTD duration periods → derive quarterly later) ===
    for cp in cf_periods:
        period_start = cp.split('_')[0]
        period_end = cp.split('_')[1]
        if period_end not in results:
            results[period_end] = {'period_end': period_end}

        for concept, field in CF_CONCEPTS.items():
            val = find_value(cf_items, concept, cp)
            if val is not None:
                raw = to_raw(val, field)
                if field in NEGATE_FIELDS:
                    raw = -abs(raw)
                # Store as YTD with markers for quarterly derivation
                results[period_end][f'{field}_ytd'] = raw
                results[period_end][f'{field}_ytd_days'] = period_days(cp)
                results[period_end][f'{field}_fy_start'] = period_start

    # === CALCULATION COMPONENTS overlay (current period only) ===
    calc = ai.get('calculation_components', {})
    if calc and bs_periods:
        current_period = bs_periods[-1]  # Most recent instant date
        if current_period in results:
            rec = results[current_period]

            # Short-term debt — authoritative source
            std_comp = calc.get('short_term_debt', {})
            if std_comp and std_comp.get('value') is not None:
                rec['short_term_debt_q'] = int(std_comp['value']) * 1_000_000
            elif std_comp and std_comp.get('confirmed_zero'):
                rec['short_term_debt_q'] = 0

            # Operating lease liabilities — authoritative source
            lease_comp = calc.get('operating_leases', {})
            if lease_comp and lease_comp.get('total') is not None:
                rec['operating_lease_liabilities_q'] = int(lease_comp['total']) * 1_000_000

            # Store full calculation_components for downstream use
            rec['_calculation_components'] = calc

    return results


def derive_quarterly_cf(records):
    """
    Derive quarterly CF values from YTD.
    Q1: YTD is already quarterly (~90 days).
    Q2: quarterly = Q2 YTD - Q1 YTD
    Q3: quarterly = Q3 YTD - Q2 YTD
    Q4 (from 10-K): quarterly = annual - Q3 YTD

    Groups by fiscal year start so we never subtract across fiscal years.
    If a fiscal year has only one non-Q1 period (single filing), the quarterly
    value cannot be derived and is left as None.
    """
    sorted_ends = sorted(records.keys())

    for field in CF_FIELD_NAMES:
        ytd_key = f'{field}_ytd'
        days_key = f'{field}_ytd_days'
        fy_key = f'{field}_fy_start'

        # Group periods by fiscal year start
        fy_groups = {}
        for pe in sorted_ends:
            rec = records[pe]
            if ytd_key not in rec:
                continue
            fy_start = rec.get(fy_key, '')
            if fy_start not in fy_groups:
                fy_groups[fy_start] = []
            fy_groups[fy_start].append(pe)

        # Derive quarterly within each fiscal year
        for fy_start, period_ends in fy_groups.items():
            prev_ytd = 0
            for pe in sorted(period_ends):
                rec = records[pe]
                ytd_val = rec[ytd_key]
                ytd_days = rec.get(days_key, 0)

                if ytd_days < 120:
                    # Q1 — YTD is quarterly
                    rec[field] = ytd_val
                    prev_ytd = ytd_val
                elif prev_ytd != 0:
                    # Q2, Q3, Q4 — subtract previous YTD from same fiscal year
                    rec[field] = ytd_val - prev_ytd
                    prev_ytd = ytd_val
                else:
                    # Single non-Q1 filing with no prior quarter — can't derive
                    rec[field] = None
                    prev_ytd = ytd_val

        # Clean up temp keys
        for pe in sorted_ends:
            rec = records[pe]
            for key in [ytd_key, days_key, fy_key]:
                rec.pop(key, None)

    return records


def main():
    parser = argparse.ArgumentParser(description='Map AI extraction to quarterly records')
    parser.add_argument('--ticker', required=True)
    parser.add_argument('--filings', nargs='+', required=True, help='AI extraction JSON files in chronological order')
    parser.add_argument('--output', help='Output file path')
    args = parser.parse_args()

    # Extract data from all filings
    all_periods = {}
    for filepath in args.filings:
        print(f"Reading {filepath}...")
        with open(filepath) as f:
            filing = json.load(f)
        periods = extract_filing(filing)
        # Merge — later filings overwrite earlier for same period (restatements)
        for pe, data in periods.items():
            if pe in all_periods:
                all_periods[pe].update(data)
            else:
                all_periods[pe] = data

    # Derive quarterly CF from YTD
    all_periods = derive_quarterly_cf(all_periods)

    # Filter to reporting periods (those with IS data) and clean up None values
    records = []
    for pe in sorted(all_periods.keys()):
        rec = all_periods[pe]
        if 'revenue_q' in rec:  # Has IS data = is a reporting period
            rec['ticker'] = args.ticker
            # Remove fields with None values (underivable quarterly CF from single filing)
            rec = {k: v for k, v in rec.items() if v is not None}
            records.append(rec)

    # Display
    print(f"\nMapped {len(records)} quarters for {args.ticker}")

    # Count fields per quarter
    field_counts = [len([k for k in r if k not in ('period_end', 'period_start', 'ticker', '_calculation_components')]) for r in records]
    print(f"Fields per quarter: {', '.join(str(c) for c in field_counts)}")

    print(f"\n{'Period':>12} {'Revenue':>12} {'Net Inc':>12} {'CFO':>12} {'Cash':>12} {'Equity':>12}")
    print("-" * 72)
    for r in records:
        rev = r.get('revenue_q', 0) / 1e9
        ni = r.get('net_income_q', 0) / 1e9
        cfo_val = r.get('cfo_q')
        cfo = f"{cfo_val / 1e9:>11.1f}B" if cfo_val is not None else "          n/a"
        cash = (r.get('cash_q') or 0) / 1e9
        eq = (r.get('equity_q') or 0) / 1e9
        print(f"{r['period_end']:>12} {rev:>11.1f}B {ni:>11.1f}B {cfo} {cash:>11.1f}B {eq:>11.1f}B")

    # Save
    out_path = args.output or f'ai_extract/{args.ticker.lower()}_mapped.json'
    with open(out_path, 'w') as f:
        json.dump(records, f, indent=2)
    print(f"\nSaved to {out_path}")


if __name__ == '__main__':
    main()
