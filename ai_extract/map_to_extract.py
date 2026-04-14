"""
Map AI extraction JSON to quarterly records for comparison against deterministic pipeline.

Reads AI extraction output (all statements) and produces quarterly records.
Handles:
- Selecting quarterly IS periods (not YTD) from 10-Q filings
- Deriving quarterly CF from YTD CF by subtraction
- Deriving Q4 from 10-K annual minus Q1+Q2+Q3
- Operating lease liabilities from not-on-statement items
- Sign conventions

Everything stays in ai_extract/ — does not modify any files outside this folder.

Usage:
    python3 ai_extract/map_to_extract.py --ticker NVDA --filings ai_extract/nvda_q1fy26_all_statements.json ai_extract/nvda_q2fy26_all.json ...
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta


# XBRL concept -> field name mapping
CONCEPT_MAP = {
    # Income Statement
    'us-gaap:Revenues': 'revenue_q',
    'us-gaap:CostOfRevenue': 'cogs_q',
    'us-gaap:OperatingIncomeLoss': 'operating_income_q',
    'us-gaap:ResearchAndDevelopmentExpense': 'rd_expense_q',
    'us-gaap:IncomeTaxExpenseBenefit': 'income_tax_expense_q',
    'us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest': 'pretax_income_q',
    'us-gaap:NetIncomeLoss': 'net_income_q',
    'us-gaap:InterestExpenseNonoperating': 'interest_expense_q',
    # Balance Sheet
    'us-gaap:StockholdersEquity': 'equity_q',
    'us-gaap:LongTermDebtNoncurrent': 'long_term_debt_q',
    'us-gaap:CashAndCashEquivalentsAtCarryingValue': 'cash_q',
    'us-gaap:MarketableSecuritiesCurrent': 'short_term_investments_q',
    'us-gaap:AccountsReceivableNetCurrent': 'accounts_receivable_q',
    'us-gaap:InventoryNet': 'inventory_q',
    'us-gaap:AccountsPayableCurrent': 'accounts_payable_q',
    'us-gaap:Assets': 'total_assets_q',
    # Cash Flow
    'us-gaap:NetCashProvidedByUsedInOperatingActivities': 'cfo_q',
    'us-gaap:PaymentsToAcquireProductiveAssets': 'capex_q',
    'us-gaap:DepreciationDepletionAndAmortization': 'dna_q',
    'us-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired': 'acquisitions_q',
    'us-gaap:ShareBasedCompensation': 'sbc_q',
    'us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding': 'diluted_shares_q',
}

# Fields stored as negative in deterministic output
NEGATE_FIELDS = {'capex_q', 'acquisitions_q', 'interest_expense_q'}

# Balance sheet fields (use instant periods)
BS_FIELDS = {'equity_q', 'long_term_debt_q', 'cash_q', 'short_term_investments_q',
             'accounts_receivable_q', 'inventory_q', 'accounts_payable_q', 'total_assets_q'}

# Cash flow fields (use YTD periods, need quarterly derivation)
CF_FIELDS = {'cfo_q', 'capex_q', 'dna_q', 'acquisitions_q', 'sbc_q'}


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
    """Convert AI extraction value (millions) to raw value."""
    if val is None:
        return None
    if field == 'diluted_shares_q':
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
    # For IS: find annual/YTD periods (longer duration)
    ytd_is_periods = [p for p in is_periods if period_days(p) >= 120]

    results = {}

    # Extract IS quarterly data
    for qp in quarterly_is_periods:
        period_end = qp.split('_')[1]
        period_start = qp.split('_')[0]
        if period_end not in results:
            results[period_end] = {'period_end': period_end, 'period_start': period_start}

        for concept, field in CONCEPT_MAP.items():
            if field in BS_FIELDS or field in CF_FIELDS:
                continue
            val = find_value(is_items, concept, qp)
            if val is not None:
                raw = to_raw(val, field)
                if field in NEGATE_FIELDS:
                    raw = -abs(raw)
                results[period_end][field] = raw

    # Extract BS instant data
    for ip in bs_periods:
        if ip not in results:
            results[ip] = {'period_end': ip}

        for concept, field in CONCEPT_MAP.items():
            if field not in BS_FIELDS:
                continue
            val = find_value(bs_items, concept, ip)
            if val is not None:
                results[ip][field] = to_raw(val, field)

        # Short-term debt
        std = find_value(bs_items, 'us-gaap:ShortTermBorrowings', ip)
        if std is None:
            std = find_value(bs_items, 'us-gaap:CommercialPaper', ip)
        results[ip]['short_term_debt_q'] = to_raw(std, 'short_term_debt_q') or 0

        # Operating lease liabilities
        total_lease = find_not_on_stmt_value(bs_not_on, 'us-gaap:OperatingLeaseLiability', ip)
        if total_lease is not None:
            results[ip]['operating_lease_liabilities_q'] = int(total_lease) * 1_000_000
        else:
            current = find_not_on_stmt_value(bs_not_on, 'us-gaap:OperatingLeaseLiabilityCurrent', ip)
            noncurrent = find_value(bs_items, 'us-gaap:OperatingLeaseLiabilityNoncurrent', ip)
            c_val = int(current) * 1_000_000 if current else 0
            n_val = int(noncurrent) * 1_000_000 if noncurrent else 0
            results[ip]['operating_lease_liabilities_q'] = c_val + n_val

    # Extract CF YTD data (will be derived to quarterly later)
    for cp in cf_periods:
        period_end = cp.split('_')[1]
        if period_end not in results:
            results[period_end] = {'period_end': period_end}

        for concept, field in CONCEPT_MAP.items():
            if field not in CF_FIELDS:
                continue
            val = find_value(cf_items, concept, cp)
            if val is not None:
                raw = to_raw(val, field)
                if field in NEGATE_FIELDS:
                    raw = -abs(raw)
                # Store as YTD with a marker
                results[period_end][f'{field}_ytd'] = raw
                results[period_end][f'{field}_ytd_days'] = period_days(cp)

    return results


def derive_quarterly_cf(records):
    """
    Derive quarterly CF values from YTD.
    Q1: YTD is already quarterly (~90 days).
    Q2: quarterly = Q2 YTD - Q1 YTD
    Q3: quarterly = Q3 YTD - Q2 YTD
    Q4 (from 10-K): quarterly = annual - Q3 YTD
    """
    sorted_ends = sorted(records.keys())

    for field in CF_FIELDS:
        ytd_key = f'{field}_ytd'
        days_key = f'{field}_ytd_days'
        prev_ytd = 0

        for pe in sorted_ends:
            rec = records[pe]
            if ytd_key not in rec:
                continue

            ytd_val = rec[ytd_key]
            ytd_days = rec.get(days_key, 0)

            if ytd_days < 120:
                # Q1 — YTD is quarterly
                rec[field] = ytd_val
                prev_ytd = ytd_val
            else:
                # Q2, Q3, Q4 — subtract previous YTD
                rec[field] = ytd_val - prev_ytd
                prev_ytd = ytd_val

            # Clean up temp keys
            del rec[ytd_key]
            if days_key in rec:
                del rec[days_key]

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

    # Filter to just the current fiscal year periods (those with IS data)
    records = []
    for pe in sorted(all_periods.keys()):
        rec = all_periods[pe]
        if 'revenue_q' in rec:  # Has IS data = is a reporting period
            rec['ticker'] = args.ticker
            records.append(rec)

    # Display
    print(f"\nMapped {len(records)} quarters for {args.ticker}")
    print(f"{'Period':>12} {'Revenue':>12} {'Net Inc':>12} {'CFO':>12} {'Cash':>12} {'Equity':>12}")
    print("-" * 72)
    for r in records:
        rev = r.get('revenue_q', 0) / 1e9
        ni = r.get('net_income_q', 0) / 1e9
        cfo = r.get('cfo_q', 0) / 1e9
        cash = r.get('cash_q', 0) / 1e9
        eq = r.get('equity_q', 0) / 1e9
        print(f"{r['period_end']:>12} {rev:>11.1f}B {ni:>11.1f}B {cfo:>11.1f}B {cash:>11.1f}B {eq:>11.1f}B")

    # Save
    out_path = args.output or f'ai_extract/{args.ticker.lower()}_mapped.json'
    with open(out_path, 'w') as f:
        json.dump(records, f, indent=2)
    print(f"\nSaved to {out_path}")


if __name__ == '__main__':
    main()
