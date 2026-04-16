"""
Compare deterministic XBRL extraction against golden eval data for all companies.

The golden eval has 24 fields per quarter. We extract the same fields deterministically
from parsed_xbrl.json and compare.

Usage:
    python3 ai_extract/test_golden_eval.py
"""

import json
import os
import sys
import glob

sys.path.insert(0, os.path.dirname(__file__))


def load_parsed_xbrl(ticker, accession):
    base_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'filings', ticker, accession)
    path = os.path.join(base_dir, 'parsed_xbrl.json')
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def get_fact_value(facts, concept, period_key, dimensioned=False):
    """Get a single value for a concept at a specific period."""
    for f in facts:
        if f['concept'] != concept:
            continue
        if f['dimensioned'] != dimensioned:
            continue
        if f['value_numeric'] is None:
            continue
        p = f.get('period')
        if not p:
            continue
        if p['type'] == 'duration':
            key = f"{p['startDate']}_{p['endDate']}"
        else:
            key = p['date']
        if key == period_key:
            return f['value_numeric']
    return None


def find_fact_by_concepts(facts, concept_list, period_key, dimensioned=False):
    """Try multiple concept names, return first match."""
    for concept in concept_list:
        val = get_fact_value(facts, concept, period_key, dimensioned)
        if val is not None:
            return val
    return None


def extract_golden_fields(parsed, period_start, period_end):
    """Extract the 24 golden eval fields deterministically from parsed XBRL."""
    facts = parsed['facts']

    # Period keys
    duration_key = f"{period_start}_{period_end}"
    instant_key = period_end  # BS items are as-of period end

    result = {}

    # IS items (duration)
    result['revenue'] = find_fact_by_concepts(facts, [
        'us-gaap:Revenues', 'us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax',
        'us-gaap:SalesRevenueNet', 'us-gaap:RevenueFromContractWithCustomerIncludingAssessedTax',
    ], duration_key)

    result['cogs'] = find_fact_by_concepts(facts, [
        'us-gaap:CostOfRevenue', 'us-gaap:CostOfGoodsAndServicesSold',
        'us-gaap:CostOfGoodsSold', 'us-gaap:CostOfGoodsAndServiceExcludingDepreciationDepletionAndAmortization',
    ], duration_key)

    result['operating_income'] = find_fact_by_concepts(facts, [
        'us-gaap:OperatingIncomeLoss',
    ], duration_key)

    result['rd_expense'] = find_fact_by_concepts(facts, [
        'us-gaap:ResearchAndDevelopmentExpense',
        'us-gaap:ResearchAndDevelopmentExpenseExcludingAcquiredInProcessCost',
    ], duration_key)

    result['income_tax_expense'] = find_fact_by_concepts(facts, [
        'us-gaap:IncomeTaxExpenseBenefit',
    ], duration_key)

    result['pretax_income'] = find_fact_by_concepts(facts, [
        'us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
        'us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments',
    ], duration_key)

    result['net_income'] = find_fact_by_concepts(facts, [
        'us-gaap:NetIncomeLoss',
        'us-gaap:ProfitLoss',
    ], duration_key)

    result['interest_expense'] = find_fact_by_concepts(facts, [
        'us-gaap:InterestExpense',
        'us-gaap:InterestExpenseNonoperating',
        'us-gaap:InterestExpenseDebt',
    ], duration_key)

    result['sbc'] = find_fact_by_concepts(facts, [
        'us-gaap:ShareBasedCompensation',
        'us-gaap:AllocatedShareBasedCompensationExpense',
        'us-gaap:ShareBasedCompensation',
    ], duration_key)

    result['diluted_shares'] = find_fact_by_concepts(facts, [
        'us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding',
    ], duration_key)

    result['dna'] = find_fact_by_concepts(facts, [
        'us-gaap:DepreciationDepletionAndAmortization',
        'us-gaap:DepreciationAndAmortization',
        'us-gaap:Depreciation',
    ], duration_key)

    # BS items (instant)
    result['equity'] = find_fact_by_concepts(facts, [
        'us-gaap:StockholdersEquity',
        'us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest',
    ], instant_key)

    result['short_term_debt'] = find_fact_by_concepts(facts, [
        'us-gaap:ShortTermBorrowings',
        'us-gaap:LongTermDebtCurrent',
        'us-gaap:DebtCurrent',
        'us-gaap:ShortTermDebtAndCurrentPortionOfLongTermDebt',  # custom aggregation
    ], instant_key)

    result['long_term_debt'] = find_fact_by_concepts(facts, [
        'us-gaap:LongTermDebtNoncurrent',
        'us-gaap:LongTermDebt',
    ], instant_key)

    result['operating_lease_liabilities'] = find_fact_by_concepts(facts, [
        'us-gaap:OperatingLeaseLiability',
    ], instant_key)

    result['cash'] = find_fact_by_concepts(facts, [
        'us-gaap:CashAndCashEquivalentsAtCarryingValue',
        'us-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents',
    ], instant_key)

    result['short_term_investments'] = find_fact_by_concepts(facts, [
        'us-gaap:MarketableSecuritiesCurrent',
        'us-gaap:ShortTermInvestments',
        'us-gaap:AvailableForSaleSecuritiesDebtSecuritiesCurrent',
    ], instant_key)

    result['accounts_receivable'] = find_fact_by_concepts(facts, [
        'us-gaap:AccountsReceivableNetCurrent',
        'us-gaap:AccountsReceivableNet',
    ], instant_key)

    result['inventory'] = find_fact_by_concepts(facts, [
        'us-gaap:InventoryNet',
        'us-gaap:InventoryFinishedGoodsAndWorkInProcess',
    ], instant_key)

    result['accounts_payable'] = find_fact_by_concepts(facts, [
        'us-gaap:AccountsPayableCurrent',
        'us-gaap:AccountsPayableAndAccruedLiabilitiesCurrent',
    ], instant_key)

    result['total_assets'] = find_fact_by_concepts(facts, [
        'us-gaap:Assets',
    ], instant_key)

    # CF items (duration)
    result['cfo'] = find_fact_by_concepts(facts, [
        'us-gaap:NetCashProvidedByUsedInOperatingActivities',
    ], duration_key)

    result['capex'] = find_fact_by_concepts(facts, [
        'us-gaap:PaymentsToAcquirePropertyPlantAndEquipment',
        'us-gaap:PaymentsToAcquireProductiveAssets',
        'us-gaap:PaymentsForCapitalImprovements',
    ], duration_key)

    result['acquisitions'] = find_fact_by_concepts(facts, [
        'us-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired',
        'us-gaap:PaymentsToAcquireBusinessesAndInterestsInAffiliates',
    ], duration_key)

    return result


def map_golden_to_accession(ticker, golden_periods):
    """Map golden eval periods to filing accessions."""
    filings_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'filings', ticker)
    if not os.path.isdir(filings_dir):
        return {}

    # Build report_date -> accession map
    date_to_acc = {}
    for acc in os.listdir(filings_dir):
        meta_path = os.path.join(filings_dir, acc, 'filing_meta.json')
        if os.path.exists(meta_path):
            with open(meta_path) as f:
                meta = json.load(f)
            date_to_acc[meta['report_date']] = acc

    mapping = {}
    for gp in golden_periods:
        period_end = gp['period_end']
        if period_end in date_to_acc:
            mapping[period_end] = date_to_acc[period_end]

    return mapping


GOLDEN_TO_DET = {
    'revenue_q': 'revenue',
    'cogs_q': 'cogs',
    'operating_income_q': 'operating_income',
    'rd_expense_q': 'rd_expense',
    'income_tax_expense_q': 'income_tax_expense',
    'pretax_income_q': 'pretax_income',
    'net_income_q': 'net_income',
    'interest_expense_q': 'interest_expense',
    'sbc_q': 'sbc',
    'diluted_shares_q': 'diluted_shares',
    'dna_q': 'dna',
    'equity_q': 'equity',
    'short_term_debt_q': 'short_term_debt',
    'long_term_debt_q': 'long_term_debt',
    'operating_lease_liabilities_q': 'operating_lease_liabilities',
    'cash_q': 'cash',
    'short_term_investments_q': 'short_term_investments',
    'accounts_receivable_q': 'accounts_receivable',
    'inventory_q': 'inventory',
    'accounts_payable_q': 'accounts_payable',
    'total_assets_q': 'total_assets',
    'cfo_q': 'cfo',
    'capex_q': 'capex',
    'acquisitions_q': 'acquisitions',
}


def main():
    golden_dir = os.path.join(os.path.dirname(__file__), '..', 'deterministic-flow', 'golden')
    tickers = ['DELL', 'FCX', 'LYB', 'MSFT', 'NVDA', 'PANW', 'PLTR', 'SYM', 'UNP']

    grand_total_match = 0
    grand_total_mismatch = 0
    grand_total_missing_det = 0
    grand_total_missing_golden = 0
    grand_total_fields = 0
    all_mismatches = []

    for ticker in tickers:
        golden_path = os.path.join(golden_dir, f'{ticker.lower()}.json')
        if not os.path.exists(golden_path):
            print(f"\n{ticker}: no golden eval file")
            continue

        with open(golden_path) as f:
            golden = json.load(f)

        mapping = map_golden_to_accession(ticker, golden)

        ticker_match = 0
        ticker_mismatch = 0
        ticker_missing_det = 0
        ticker_missing_golden = 0
        ticker_fields = 0

        print(f"\n{'='*80}")
        print(f"  {ticker}: {len(golden)} golden periods, {len(mapping)} matched to filings")
        print(f"{'='*80}")

        for gp in golden:
            period_end = gp['period_end']
            period_start = gp['period_start']
            fy = gp['fiscal_year']
            fp = gp['fiscal_period']

            if period_end not in mapping:
                continue

            acc = mapping[period_end]
            parsed = load_parsed_xbrl(ticker, acc)
            if parsed is None:
                print(f"  {fp} FY{fy}: no parsed XBRL")
                continue

            det = extract_golden_fields(parsed, period_start, period_end)

            period_match = 0
            period_mismatch = 0
            period_missing_det = 0
            period_issues = []

            for golden_key, det_key in GOLDEN_TO_DET.items():
                golden_val = gp.get(golden_key)
                det_val = det.get(det_key)
                ticker_fields += 1
                grand_total_fields += 1

                if golden_val is None or golden_val == 0:
                    if det_val is None or det_val == 0:
                        period_match += 1
                        ticker_match += 1
                        grand_total_match += 1
                    elif det_val is not None:
                        # Golden is 0/null, det found something
                        period_issues.append({
                            'field': golden_key,
                            'golden': golden_val,
                            'det': det_val,
                            'type': 'EXTRA'
                        })
                        period_mismatch += 1
                        ticker_mismatch += 1
                        grand_total_mismatch += 1
                    continue

                if det_val is None:
                    period_missing_det += 1
                    ticker_missing_det += 1
                    grand_total_missing_det += 1
                    period_issues.append({
                        'field': golden_key,
                        'golden': golden_val,
                        'det': None,
                        'type': 'MISSING'
                    })
                    continue

                # Compare values - golden is in raw dollars, det_val is also raw
                # Allow 1% tolerance for rounding
                golden_abs = abs(golden_val)
                det_abs = abs(det_val)

                if golden_abs == 0:
                    match = det_abs < 1e6
                else:
                    pct_diff = abs(golden_abs - det_abs) / golden_abs
                    match = pct_diff < 0.01  # 1% tolerance

                if match:
                    # Check sign
                    if (golden_val > 0) != (det_val > 0) and golden_val != 0 and det_val != 0:
                        period_issues.append({
                            'field': golden_key,
                            'golden': golden_val,
                            'det': det_val,
                            'type': 'SIGN'
                        })
                    period_match += 1
                    ticker_match += 1
                    grand_total_match += 1
                else:
                    period_mismatch += 1
                    ticker_mismatch += 1
                    grand_total_mismatch += 1
                    period_issues.append({
                        'field': golden_key,
                        'golden': golden_val,
                        'det': det_val,
                        'type': 'VALUE'
                    })

            status = "OK" if not [i for i in period_issues if i['type'] in ('VALUE', 'MISSING')] else "ISSUES"
            sign_issues = [i for i in period_issues if i['type'] == 'SIGN']
            value_issues = [i for i in period_issues if i['type'] == 'VALUE']
            missing_issues = [i for i in period_issues if i['type'] == 'MISSING']
            extra_issues = [i for i in period_issues if i['type'] == 'EXTRA']

            print(f"\n  {fp} FY{fy} ({period_end}): {period_match}/{period_match + period_mismatch + period_missing_det} match"
                  f"  {len(sign_issues)} sign  {len(value_issues)} value  {len(missing_issues)} missing  {len(extra_issues)} extra")

            for issue in value_issues + missing_issues:
                g = issue['golden']
                d = issue['det']
                if g is not None:
                    g_m = f"{g/1e6:,.0f}M" if abs(g) > 1e6 else f"{g:,.0f}"
                else:
                    g_m = "null"
                if d is not None:
                    d_m = f"{d/1e6:,.0f}M" if abs(d) > 1e6 else f"{d:,.0f}"
                else:
                    d_m = "null"
                print(f"    {issue['type']:7s} {issue['field']:35s} golden={g_m:>12s}  det={d_m:>12s}")

            all_mismatches.extend([{**i, 'ticker': ticker, 'period': f'{fp} FY{fy}'} for i in period_issues if i['type'] in ('VALUE', 'MISSING')])

        print(f"\n  {ticker} TOTAL: {ticker_match}/{ticker_fields} match, {ticker_mismatch} mismatch, {ticker_missing_det} missing")

    print(f"\n{'='*80}")
    print(f"  GRAND TOTAL ACROSS ALL COMPANIES")
    print(f"{'='*80}")
    print(f"  Fields compared: {grand_total_fields}")
    print(f"  Exact match:     {grand_total_match} ({100*grand_total_match/max(grand_total_fields,1):.1f}%)")
    print(f"  Value mismatch:  {grand_total_mismatch} ({100*grand_total_mismatch/max(grand_total_fields,1):.1f}%)")
    print(f"  Missing in det:  {grand_total_missing_det} ({100*grand_total_missing_det/max(grand_total_fields,1):.1f}%)")

    if all_mismatches:
        # Group by field
        by_field = {}
        for m in all_mismatches:
            by_field.setdefault(m['field'], []).append(m)
        print(f"\n  Failures by field:")
        for field, items in sorted(by_field.items(), key=lambda x: -len(x[1])):
            types = [i['type'] for i in items]
            val_count = types.count('VALUE')
            miss_count = types.count('MISSING')
            tickers_affected = set(i['ticker'] for i in items)
            print(f"    {field:35s} {len(items):3d} failures ({val_count} value, {miss_count} missing)  tickers: {','.join(sorted(tickers_affected))}")


if __name__ == '__main__':
    main()
