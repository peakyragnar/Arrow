"""
Map AI extraction JSON to quarterly records using label-based matching.

Instead of XBRL concept resolution, this mapper uses the AI's own labels
to identify line items. The AI already knows what each item is — "Revenue",
"Cost of revenue", "Net income" — regardless of the underlying XBRL concept.

Everything stays in ai_extract/ — does not modify any files outside this folder.

Usage:
    python3 ai_extract/map_by_label.py --ticker NVDA --filings ai_extract/NVDA/q1_fy26_10q.json ...
"""

import argparse
import json
import re
from datetime import datetime


# === LABEL PATTERNS ===
# Each entry: (pattern, field_name)
# Patterns are matched case-insensitive against the line item label.
# First match wins — order matters (more specific patterns first).

IS_LABELS = [
    # Revenue / COGS / Gross profit
    ('revenue', 'revenue_q'),
    ('cost of revenue', 'cogs_q'),
    ('cost of goods', 'cogs_q'),
    ('cost of sales', 'cogs_q'),
    ('production and delivery', 'cogs_q'),
    ('gross profit', 'gross_profit_q'),
    # Operating expenses
    ('research and development', 'rd_expense_q'),
    ('selling, general and admin', 'sga_q'),
    ('sales, general and admin', 'sga_q'),
    ('total operating expenses', 'total_opex_q'),
    ('total costs and expenses', 'total_opex_q'),
    # Operating income
    ('operating income', 'operating_income_q'),
    # Other income/expense
    ('interest income', 'interest_income_q'),
    ('interest expense', 'interest_expense_q'),
    ('total other income', 'total_nonop_income_q'),
    ('other income (expense)', 'total_nonop_income_q'),
    ('other income, net', 'other_nonop_income_q'),
    # Pretax / tax / net income
    ('income before income tax', 'pretax_income_q'),
    ('income tax expense', 'income_tax_expense_q'),
    ('provision for income tax', 'income_tax_expense_q'),
    ('equity in affiliated', 'equity_method_earnings_q'),
    # Net income — match most specific first
    ('net income attributable to noncontrolling', 'net_income_nci_q'),
    ('net income attributable to common', 'net_income_to_common_q'),
    ('net income', 'net_income_q'),
    # EPS
    ('diluted weighted average', 'diluted_shares_q'),
    ('diluted shares', 'diluted_shares_q'),
    ('basic weighted average', 'basic_shares_q'),
    ('basic shares', 'basic_shares_q'),
]

IS_PER_SHARE_LABELS = [
    ('diluted', 'eps_diluted_q'),
    ('basic', 'eps_basic_q'),
]

BS_LABELS = [
    # Current assets
    ('cash and cash equiv', 'cash_q'),
    ('marketable securities', 'short_term_investments_q'),
    ('short-term investments', 'short_term_investments_q'),
    ('accounts receivable', 'accounts_receivable_q'),
    ('inventories', 'inventory_q'),
    ('inventory', 'inventory_q'),
    ('prepaid expense', 'prepaid_q'),
    ('total current assets', 'total_current_assets_q'),
    # Non-current assets
    ('property and equipment', 'ppe_q'),
    ('property, plant', 'ppe_q'),
    ('operating lease asset', 'operating_lease_assets_q'),
    ('operating lease right', 'operating_lease_assets_q'),
    ('goodwill', 'goodwill_q'),
    ('intangible assets', 'intangibles_q'),
    ('deferred income tax asset', 'deferred_tax_assets_q'),
    ('deferred tax asset', 'deferred_tax_assets_q'),
    ('total assets', 'total_assets_q'),
    # Current liabilities
    ('accounts payable', 'accounts_payable_q'),
    ('accrued', 'accrued_liabilities_q'),
    ('short-term debt', 'short_term_debt_q'),
    ('current portion of long-term debt', 'short_term_debt_q'),
    ('total current liabilities', 'total_current_liabilities_q'),
    # Non-current liabilities
    ('long-term debt', 'long_term_debt_q'),
    ('long-term operating lease', 'long_term_lease_liabilities_q'),
    ('total liabilities and', 'total_liabilities_and_equity_q'),
    ('total liabilities', 'total_liabilities_q'),
    # Equity
    ('noncontrolling interest', 'noncontrolling_interests_q'),
    ('additional paid-in', 'apic_q'),
    ('accumulated other comprehensive', 'aoci_q'),
    ('retained earnings', 'retained_earnings_q'),
    ('common stock', 'common_stock_q'),
    ('total stockholders equity', 'equity_q'),
    ('total shareholders equity', 'equity_q'),
    ('total equity', 'equity_incl_nci_q'),
]

CF_LABELS = [
    # Operating activities
    ('net income', 'cf_net_income_q'),
    ('stock-based compensation', 'sbc_q'),
    ('share-based compensation', 'sbc_q'),
    ('depreciation and amortization', 'dna_q'),
    ('depreciation, depletion', 'dna_q'),
    ('deferred income tax', 'deferred_taxes_q'),
    ('deferred tax', 'deferred_taxes_q'),
    ('accounts receivable', 'change_ar_q'),
    ('inventories', 'change_inventory_q'),
    ('inventory', 'change_inventory_q'),
    ('accounts payable', 'change_ap_q'),
    ('accrued', 'change_accrued_q'),
    ('net cash provided by operating', 'cfo_q'),
    ('net cash from operating', 'cfo_q'),
    # Investing activities
    ('purchases related to property', 'capex_q'),
    ('capital expenditure', 'capex_q'),
    ('payments to acquire property', 'capex_q'),
    ('acquisitions, net', 'acquisitions_q'),
    ('net cash used in investing', 'cfi_q'),
    ('net cash provided by.*investing', 'cfi_q'),
    ('net cash from investing', 'cfi_q'),
    # Financing activities
    ('repurchases of common stock', 'share_repurchases_q'),
    ('dividends paid', 'dividends_q'),
    ('repayment of debt', 'debt_repayment_q'),
    ('net cash used in financing', 'cff_q'),
    ('net cash provided by.*financing', 'cff_q'),
    ('net cash from financing', 'cff_q'),
    # Net change
    ('change in cash', 'net_change_cash_q'),
    ('increase.*decrease.*in cash', 'net_change_cash_q'),
    ('cash.*at beginning', 'beginning_cash_q'),
    ('cash.*at end', 'ending_cash_q'),
    ('income taxes paid', 'taxes_paid_q'),
    ('cash paid for income tax', 'taxes_paid_q'),
]

# Fields stored as negative in deterministic pipeline convention
NEGATE_FIELDS = {'capex_q', 'acquisitions_q', 'interest_expense_q'}

# Shares fields — weighted averages, not cumulative
SHARES_FIELDS = {'diluted_shares_q', 'basic_shares_q'}

# EPS fields — per-share, keep as float
EPS_FIELDS = {'eps_basic_q', 'eps_diluted_q'}

# CF fields — need YTD-to-quarterly derivation
CF_FIELD_NAMES = set(f for _, f in CF_LABELS)

# IS flow fields — need YTD-to-quarterly derivation (not shares/EPS)
IS_FLOW_FIELDS = set(f for _, f in IS_LABELS) - SHARES_FIELDS


def period_days(period_key):
    if '_' not in period_key:
        return 0
    start, end = period_key.split('_')
    d1 = datetime.strptime(start, '%Y-%m-%d')
    d2 = datetime.strptime(end, '%Y-%m-%d')
    return (d2 - d1).days


def match_label(label, patterns):
    """Match a label against a list of (pattern, field_name) tuples. First match wins."""
    label_lower = label.lower()
    for pattern, field_name in patterns:
        if re.search(pattern, label_lower):
            return field_name
    return None


def to_raw(val, field):
    if val is None:
        return None
    if field in EPS_FIELDS:
        return float(val)
    if field in SHARES_FIELDS:
        return int(val * 1_000_000)
    return int(val) * 1_000_000


def get_periods(line_items, period_type):
    """Get all periods of a given type from line items."""
    periods = set()
    for item in line_items:
        for pk in (item.get('values') or {}):
            if period_type == 'instant' and '_' not in pk:
                periods.add(pk)
            elif period_type == 'duration' and '_' in pk:
                periods.add(pk)
    return sorted(periods, key=lambda p: period_days(p) if '_' in p else 0)


def extract_statement(line_items, label_map, periods, is_per_share=False):
    """Extract fields from a statement's line items using label matching."""
    results = {}
    for period in periods:
        results[period] = {}
        for item in line_items:
            label = item.get('label', '')
            vals = item.get('values') or {}
            if period not in vals:
                continue
            val = vals[period]

            if is_per_share:
                # EPS items: only match if unit is per_share
                unit = item.get('unit', '')
                if 'per_share' not in unit and 'USD_per_share' not in unit:
                    continue
                field = match_label(label, label_map)
            else:
                field = match_label(label, label_map)

            if field and field not in results[period]:
                raw = to_raw(val, field)
                if field in NEGATE_FIELDS:
                    raw = -abs(raw)
                results[period][field] = raw
    return results


def extract_filing(filing_json):
    """Extract all data from one filing using label-based matching."""
    ai = filing_json['ai_extraction']
    is_items = ai.get('income_statement', {}).get('line_items', [])
    bs_items = ai.get('balance_sheet', {}).get('line_items', [])
    cf_items = ai.get('cash_flow', {}).get('line_items', [])

    # Get periods
    is_durations = get_periods(is_items, 'duration')
    bs_instants = get_periods(bs_items, 'instant')
    cf_durations = get_periods(cf_items, 'duration')

    quarterly_is = [p for p in is_durations if period_days(p) < 120]
    ytd_is = [p for p in is_durations if period_days(p) >= 120]

    results = {}

    # === INCOME STATEMENT — quarterly periods ===
    is_data = extract_statement(is_items, IS_LABELS, quarterly_is)
    eps_data = extract_statement(is_items, IS_PER_SHARE_LABELS, quarterly_is, is_per_share=True)
    for period, fields in is_data.items():
        period_end = period.split('_')[1]
        period_start = period.split('_')[0]
        if period_end not in results:
            results[period_end] = {'period_end': period_end, 'period_start': period_start}
        for field, val in fields.items():
            if field not in results[period_end]:
                results[period_end][field] = val
        # Merge EPS
        for field, val in eps_data.get(period, {}).items():
            if field not in results[period_end]:
                results[period_end][field] = val

    # === INCOME STATEMENT — YTD/annual periods ===
    is_ytd_data = extract_statement(is_items, IS_LABELS, ytd_is)
    eps_ytd_data = extract_statement(is_items, IS_PER_SHARE_LABELS, ytd_is, is_per_share=True)
    for period, fields in is_ytd_data.items():
        period_start = period.split('_')[0]
        period_end = period.split('_')[1]
        days = period_days(period)
        if period_end not in results:
            results[period_end] = {'period_end': period_end}
        rec = results[period_end]

        for field, val in fields.items():
            if field in SHARES_FIELDS:
                # Shares: for annual periods, store directly as Q4
                if days >= 350 and field not in rec:
                    rec[field] = val
            else:
                # Flow fields: store as YTD for derivation
                ytd_key = f'{field}_ytd'
                if ytd_key not in rec:
                    rec[ytd_key] = val
                    rec[f'{field}_ytd_days'] = days
                    rec[f'{field}_fy_start'] = period_start

        # EPS: for annual periods, store directly as Q4
        if days >= 350:
            for field, val in eps_ytd_data.get(period, {}).items():
                if field not in rec:
                    rec[field] = val

    # === BALANCE SHEET ===
    bs_data = extract_statement(bs_items, BS_LABELS, bs_instants)
    for period, fields in bs_data.items():
        if period not in results:
            results[period] = {'period_end': period}
        for field, val in fields.items():
            if field not in results[period]:
                results[period][field] = val

    # === CASH FLOW ===
    cf_data = extract_statement(cf_items, CF_LABELS, cf_durations)
    for period, fields in cf_data.items():
        period_start = period.split('_')[0]
        period_end = period.split('_')[1]
        if period_end not in results:
            results[period_end] = {'period_end': period_end}
        rec = results[period_end]
        for field, val in fields.items():
            ytd_key = f'{field}_ytd'
            if ytd_key not in rec:
                rec[ytd_key] = val
                rec[f'{field}_ytd_days'] = period_days(period)
                rec[f'{field}_fy_start'] = period_start

    # === CALCULATION COMPONENTS overlay ===
    calc = ai.get('calculation_components', {})
    if calc and bs_instants:
        current_period = bs_instants[-1]
        if current_period in results:
            rec = results[current_period]

            std_comp = calc.get('short_term_debt', {})
            if std_comp and std_comp.get('value') is not None:
                rec['short_term_debt_q'] = int(std_comp['value']) * 1_000_000
            elif std_comp and std_comp.get('confirmed_zero'):
                rec['short_term_debt_q'] = 0

            lease_comp = calc.get('operating_leases', {})
            if lease_comp and lease_comp.get('total') is not None:
                rec['operating_lease_liabilities_q'] = int(lease_comp['total']) * 1_000_000

            capex_comp = calc.get('capex', {})
            if capex_comp and capex_comp.get('cf_value') is not None:
                capex_total = int(capex_comp['cf_value']) * 1_000_000
                ytd_key = 'capex_q_ytd'
                if ytd_key in rec:
                    rec[ytd_key] = -abs(capex_total)

            acq_comp = calc.get('acquisitions', {})
            if acq_comp and acq_comp.get('total') is not None:
                acq_total = int(acq_comp['total']) * 1_000_000
                ytd_key = 'acquisitions_q_ytd'
                if ytd_key in rec:
                    rec[ytd_key] = -abs(acq_total)

            rec['_calculation_components'] = calc

    return results


def derive_quarterly(records, field_names):
    """Derive quarterly values from YTD/annual. Same logic as map_to_extract.py."""
    sorted_ends = sorted(records.keys())

    for field in field_names:
        ytd_key = f'{field}_ytd'
        days_key = f'{field}_ytd_days'
        fy_key = f'{field}_fy_start'

        fy_groups = {}
        for pe in sorted_ends:
            rec = records[pe]
            if ytd_key not in rec:
                continue
            fy_start = rec.get(fy_key, '')
            if fy_start not in fy_groups:
                fy_groups[fy_start] = []
            fy_groups[fy_start].append(pe)

        for fy_start, period_ends in fy_groups.items():
            prev_ytd = 0
            for pe in sorted(period_ends):
                rec = records[pe]
                ytd_val = rec[ytd_key]
                ytd_days = rec.get(days_key, 0)

                if ytd_days < 120:
                    if field not in rec:
                        rec[field] = ytd_val
                    prev_ytd = ytd_val
                elif prev_ytd != 0:
                    if field not in rec:
                        rec[field] = ytd_val - prev_ytd
                    prev_ytd = ytd_val
                else:
                    if field not in rec:
                        rec[field] = None
                    prev_ytd = ytd_val

        for pe in sorted_ends:
            rec = records[pe]
            for key in [ytd_key, days_key, fy_key]:
                rec.pop(key, None)

    return records


def main():
    parser = argparse.ArgumentParser(description='Map AI extraction to quarterly records (label-based)')
    parser.add_argument('--ticker', required=True)
    parser.add_argument('--filings', nargs='+', required=True)
    parser.add_argument('--output', help='Output file path')
    args = parser.parse_args()

    all_periods = {}
    for filepath in args.filings:
        print(f"Reading {filepath}...")
        with open(filepath) as f:
            filing = json.load(f)
        periods = extract_filing(filing)
        # Smart merge: only overwrite if value is different
        for pe, data in periods.items():
            if pe not in all_periods:
                all_periods[pe] = data
            else:
                existing = all_periods[pe]
                for key, new_val in data.items():
                    if new_val is None:
                        continue
                    old_val = existing.get(key)
                    if old_val is None:
                        existing[key] = new_val
                    elif new_val != old_val:
                        if 'restatements' not in existing:
                            existing['restatements'] = []
                        existing['restatements'].append({
                            'field': key,
                            'old_value': old_val,
                            'new_value': new_val,
                            'source_filing': filepath,
                        })
                        existing[key] = new_val

    # Derive quarterly values
    all_periods = derive_quarterly(all_periods, IS_FLOW_FIELDS)
    all_periods = derive_quarterly(all_periods, CF_FIELD_NAMES)

    # Filter to reporting periods
    records = []
    for pe in sorted(all_periods.keys()):
        rec = all_periods[pe]
        if 'revenue_q' in rec:
            rec['ticker'] = args.ticker
            rec = {k: v for k, v in rec.items() if v is not None}
            records.append(rec)

    # Display
    print(f"\nMapped {len(records)} quarters for {args.ticker}")
    field_counts = [len([k for k in r if k not in ('period_end', 'period_start', 'ticker', '_calculation_components', 'restatements')]) for r in records]
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
    out_path = args.output or f'ai_extract/{args.ticker.lower()}_label_mapped.json'
    with open(out_path, 'w') as f:
        json.dump(records, f, indent=2)
    print(f"\nSaved to {out_path}")


if __name__ == '__main__':
    main()
