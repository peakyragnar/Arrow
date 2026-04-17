"""Stage 2 analyst-presentation CSV.

Reads `ai_extract/{TICKER}/test/formula_mapped_v3.json` and renders a clean
P&L / BS / CF in the Capital IQ-style format defined in canonical_buckets.md.
Purpose: let you visually check Stage 2's numbers against a filing.

Columns: quarters grouped by fiscal year with an FY-total column for IS and
CF (flows sum Q1..Q4). Balance sheet shows per-quarter snapshots only.

Values: Q4 slots show Q4-standalone (derived from 10-K annual − Q1+Q2+Q3 for
flows; year-end snapshot for BS). Full-year totals for IS/CF are in the FY
column. No debug/audit rows — those live in formula_mapped_v3.json.

Usage:
    python3 ai_extract/export_full_check_csv.py --ticker NVDA [--test]
"""
import argparse
import csv
import json
import os
import sys
from datetime import datetime


# Layout: ordered list of (kind, bucket_or_label, display_label) per statement.
# Kinds: 'section' (header row), 'detail', 'subtotal', 'blank'.
LAYOUT = {
    'income_statement': [
        ('section', 'REVENUES'),
        ('detail', 'revenue', 'Revenue'),
        ('detail', 'finance_div_revenue', 'Finance Div. Revenues'),
        ('detail', 'insurance_div_revenue', 'Insurance Division Revenues'),
        ('detail', 'other_revenue', 'Other Revenues'),
        ('subtotal', 'total_revenue', 'Total Revenues'),
        ('blank',),
        ('section', 'GROSS PROFIT'),
        ('detail', 'cogs', 'Cost of Revenues'),
        ('subtotal', 'gross_profit', 'Gross Profit (Loss)'),
        ('blank',),
        ('section', 'OPERATING INCOME & EXPENSES'),
        ('detail', 'sga', 'Selling General & Admin Expenses'),
        ('detail', 'rd', 'R&D Expenses'),
        ('detail', 'dna', 'Depreciation & Amortization'),
        ('detail', 'other_opex', 'Other Operating Expenses'),
        ('subtotal', 'total_opex', 'Total Operating Expenses'),
        ('subtotal', 'operating_income', 'Operating Income'),
        ('blank',),
        ('section', 'NET INTEREST EXPENSE'),
        ('detail', 'interest_expense', 'Interest Expense'),
        ('detail', 'interest_income', 'Interest And Investment Income'),
        ('subtotal', 'net_interest_expense', 'Net Interest Expenses'),
        ('blank',),
        ('section', 'EBT'),
        ('detail', 'equity_affiliates', 'Income (Loss) On Equity Affiliates'),
        ('detail', 'other_nonop', 'Other Non Operating Income (Expenses)'),
        ('subtotal', 'ebt_excl_unusual', 'EBT, Excl. Unusual Items'),
        ('detail', 'restructuring', 'Restructuring Charges'),
        ('detail', 'goodwill_impairment', 'Impairment of Goodwill'),
        ('detail', 'gain_sale_assets', 'Gain (Loss) On Sale Of Assets'),
        ('detail', 'gain_sale_investments', 'Gain (Loss) On Sale Of Investments'),
        ('detail', 'other_unusual', 'Other Unusual Items, Total'),
        ('subtotal', 'ebt_incl_unusual', 'EBT, Incl. Unusual Items'),
        ('blank',),
        ('section', 'NET INCOME'),
        ('detail', 'tax', 'Income Tax Expense'),
        ('subtotal', 'continuing_ops', 'Earnings From Continuing Operations'),
        ('detail', 'minority_interest', 'Minority Interest'),
        ('subtotal', 'net_income', 'Net Income'),
        ('detail', 'preferred_dividend', 'Preferred Dividend and Other Adjustments'),
        ('subtotal', 'ni_common_incl_extra', 'Net Income to Common Incl Extra Items'),
        ('subtotal', 'ni_common_excl_extra', 'Net Income to Common Excl. Extra Items'),
    ],
    'balance_sheet': [
        ('section', 'ASSETS'),
        ('detail', 'cash', 'Cash And Equivalents'),
        ('detail', 'sti', 'Short Term Investments'),
        ('detail', 'trading_securities', 'Trading Asset Securities'),
        ('subtotal', 'total_cash_sti', 'Total Cash And Short Term Investments'),
        ('detail', 'accounts_receivable', 'Accounts Receivable'),
        ('detail', 'other_receivables', 'Other Receivables'),
        ('subtotal', 'total_receivables', 'Total Receivables'),
        ('detail', 'inventory', 'Inventory'),
        ('detail', 'restricted_cash', 'Restricted Cash'),
        ('detail', 'prepaid_expenses', 'Prepaid Expenses'),
        ('detail', 'other_current_assets', 'Other Current Assets'),
        ('subtotal', 'total_current_assets', 'Total Current Assets'),
        ('blank',),
        ('detail', 'net_ppe', 'Net Property Plant And Equipment'),
        ('detail', 'long_term_investments', 'Long-term Investments'),
        ('detail', 'goodwill', 'Goodwill'),
        ('detail', 'other_intangibles', 'Other Intangibles'),
        ('detail', 'loans_receivable_lt', 'Loans Receivable Long-Term'),
        ('detail', 'deferred_tax_assets_lt', 'Deferred Tax Assets Long-Term'),
        ('detail', 'deferred_charges_lt', 'Deferred Charges Long-Term'),
        ('detail', 'other_lt_assets', 'Other Long-Term Assets'),
        ('subtotal', 'total_assets', 'Total Assets'),
        ('blank',),
        ('section', 'LIABILITIES'),
        ('detail', 'accounts_payable', 'Accounts Payable'),
        ('detail', 'accrued_expenses', 'Accrued Expenses'),
        ('detail', 'current_portion_lt_debt', 'Current Portion of Long-Term Debt'),
        ('detail', 'current_portion_leases', 'Current Portion of Leases'),
        ('detail', 'current_income_taxes_payable', 'Current Income Taxes Payable'),
        ('detail', 'unearned_revenue_current', 'Unearned Revenue Current, Total'),
        ('detail', 'other_current_liabilities', 'Other Current Liabilities'),
        ('subtotal', 'total_current_liabilities', 'Total Current Liabilities'),
        ('detail', 'long_term_debt', 'Long-Term Debt'),
        ('detail', 'long_term_leases', 'Long-Term Leases'),
        ('detail', 'unearned_revenue_nc', 'Unearned Revenue Non Current'),
        ('detail', 'deferred_tax_liability_nc', 'Deferred Tax Liability Non Current'),
        ('detail', 'other_nc_liabilities', 'Other Non Current Liabilities'),
        ('subtotal', 'total_liabilities', 'Total Liabilities'),
        ('blank',),
        ('section', 'EQUITY'),
        ('detail', 'common_stock', 'Common Stock'),
        ('detail', 'apic', 'Additional Paid In Capital'),
        ('detail', 'retained_earnings', 'Retained Earnings'),
        ('detail', 'treasury_stock', 'Treasury Stock'),
        ('detail', 'comprehensive_income_other', 'Comprehensive Income and Other'),
        ('subtotal', 'common_equity', 'Common Equity'),
        ('detail', 'noncontrolling_interest', 'Minority Interest'),
        ('subtotal', 'total_equity', 'Total Equity'),
        ('subtotal', 'total_liabilities_and_equity', 'Total Liabilities And Equity'),
    ],
    'cash_flow': [
        ('section', 'CASH FROM OPERATIONS'),
        ('detail', 'net_income_start', 'Net Income'),
        ('detail', 'dna', 'Depreciation & Amortization, Total'),
        ('detail', 'gain_sale_asset', '(Gain) Loss From Sale Of Asset'),
        ('detail', 'gain_sale_investments', '(Gain) Loss on Sale of Investments'),
        ('detail', 'amort_deferred_charges', 'Amortization of Deferred Charges, Total'),
        ('detail', 'asset_writedown_restructuring', 'Asset Writedown & Restructuring Costs'),
        ('detail', 'sbc', 'Stock-Based Compensation'),
        ('detail', 'other_operating', 'Other Operating Activities, Total'),
        ('detail', 'change_ar', 'Change In Accounts Receivable'),
        ('detail', 'change_inventory', 'Change In Inventories'),
        ('detail', 'change_ap', 'Change In Accounts Payable'),
        ('detail', 'change_unearned_revenue', 'Change in Unearned Revenues'),
        ('detail', 'change_income_taxes', 'Change In Income Taxes'),
        ('detail', 'change_other_operating_assets', 'Change in Other Operating Assets (prepaid-type)'),
        ('detail', 'change_other_operating_liabs', 'Change in Other Operating Liabs (accrued-type)'),
        ('subtotal', 'cfo', 'Cash from Operations'),
        ('blank',),
        ('section', 'CASH FROM INVESTING'),
        ('detail', 'capex', 'Capital Expenditure'),
        ('detail', 'sale_ppe', 'Sale of Property, Plant, and Equipment'),
        ('detail', 'acquisitions', 'Cash Acquisitions'),
        ('detail', 'divestitures', 'Divestitures'),
        ('detail', 'investment_securities', 'Investment in Mkt and Equity Securities, Total'),
        ('detail', 'loans_orig_sold', 'Net (Increase) Decrease in Loans Orig / Sold'),
        ('detail', 'other_investing', 'Other Investing Activities, Total'),
        ('subtotal', 'cfi', 'Cash from Investing'),
        ('blank',),
        ('section', 'CASH FROM FINANCING'),
        ('detail', 'short_term_debt_issued', 'Short Term Debt Issued, Total'),
        ('detail', 'long_term_debt_issued', 'Long-Term Debt Issued, Total'),
        ('subtotal', 'total_debt_issued', 'Total Debt Issued'),
        ('detail', 'short_term_debt_repaid', 'Short Term Debt Repaid, Total'),
        ('detail', 'long_term_debt_repaid', 'Long-Term Debt Repaid, Total'),
        ('subtotal', 'total_debt_repaid', 'Total Debt Repaid'),
        ('detail', 'stock_issuance', 'Issuance of Common Stock'),
        ('detail', 'stock_repurchase', 'Repurchase of Common Stock'),
        ('detail', 'common_dividends', 'Common Dividends Paid'),
        ('detail', 'preferred_dividends', 'Preferred Dividends Paid'),
        ('subtotal', 'total_common_pref_dividends', 'Common & Preferred Stock Dividends Paid'),
        ('detail', 'special_dividends', 'Special Dividends Paid'),
        ('detail', 'other_financing', 'Other Financing Activities'),
        ('subtotal', 'cff', 'Cash from Financing'),
        ('blank',),
        ('section', 'NET CHANGE IN CASH'),
        ('detail', 'fx_adjustments', 'Foreign Exchange Rate Adjustments'),
        ('detail', 'misc_cf_adjustments', 'Miscellaneous Cash Flow Adjustments'),
        ('subtotal', 'net_change_in_cash', 'Net Change in Cash'),
    ],
}

STATEMENT_TITLES = [
    ('income_statement', 'INCOME STATEMENT'),
    ('balance_sheet', 'BALANCE SHEET'),
    ('cash_flow', 'CASH FLOW'),
]

# IS and CF are flows (quarters sum to FY); BS is snapshot (no FY total).
SHOW_FY_TOTAL = {'income_statement': True, 'balance_sheet': False, 'cash_flow': True}


def fmt(v):
    if v is None or v == '':
        return ''
    if isinstance(v, bool):
        return str(v)
    if not isinstance(v, (int, float)):
        return str(v)
    if abs(v) >= 1:
        return f'{round(v):,}'
    return f'{v:.4f}'


def sort_quarters(quarters):
    def key(q):
        if len(q) >= 6 and 'Q' in q:
            prefix = q[:2]
            try:
                yy = int(q[2:4])
                qn = int(q[-1])
                return (prefix, yy, qn)
            except ValueError:
                pass
        return ('ZZ', 99, 99)
    return sorted(quarters, key=key)


def fiscal_year_groups(quarters):
    """Group quarter labels by fiscal year label. Returns ordered list of
    (fy_label, [q1,q2,q3,q4]) with missing quarters preserved as None.
    """
    groups = {}
    for q in quarters:
        if len(q) >= 6 and 'Q' in q:
            fy = q[:4]  # FY24
            try:
                n = int(q[-1])
            except ValueError:
                continue
            groups.setdefault(fy, {})[n] = q
    out = []
    for fy in sorted(groups.keys(), key=lambda s: (s[:2], int(s[2:]))):
        slots = [groups[fy].get(n) for n in (1, 2, 3, 4)]
        out.append((fy, slots))
    return out


def _bucket_value(stmt_data, bucket, quarter, kind):
    """Pull a bucket value at a quarter. kind='detail' uses total (face+note);
    kind='subtotal' uses the scalar face-authoritative subtotal.
    """
    normalized = stmt_data.get('normalized', {}) or {}
    source = normalized.get('detail' if kind == 'detail' else 'subtotals', []) or []
    for entry in source:
        if entry.get('bucket') == bucket:
            vals = entry.get('values_by_quarter', {}) or {}
            v = vals.get(quarter)
            if kind == 'detail':
                if isinstance(v, dict):
                    if v.get('total') is not None:
                        return v['total']
                    if v.get('face') is not None:
                        return v['face']
                    return None
                return v
            else:
                return v
            break
    return None


def build_column_spec(fy_groups, show_fy_total):
    """Returns list of column dicts: {kind: 'quarter'|'fy_total'|'spacer', label, quarter?, fy?}"""
    cols = []
    for i, (fy, slots) in enumerate(fy_groups):
        for n, q in enumerate(slots, start=1):
            cols.append({'kind': 'quarter', 'label': f'{fy} Q{n}', 'quarter': q, 'fy': fy})
        if show_fy_total:
            cols.append({'kind': 'fy_total', 'label': f'{fy} Total', 'fy': fy,
                         'quarters': slots})
        if i < len(fy_groups) - 1:
            cols.append({'kind': 'spacer', 'label': ''})
    return cols


def cell_value(col, stmt_data, bucket, kind):
    if col['kind'] == 'spacer':
        return ''
    if col['kind'] == 'quarter':
        return fmt(_bucket_value(stmt_data, bucket, col['quarter'], kind))
    if col['kind'] == 'fy_total':
        vs = [_bucket_value(stmt_data, bucket, q, kind) for q in col['quarters'] if q]
        vs = [v for v in vs if v is not None]
        return fmt(sum(vs)) if vs else ''
    return ''


def render_statement(writer, stmt_key, title, stmt_data, cols):
    n_cols = 1 + len(cols)
    writer.writerow([f'── {title} (USD millions) ──'] + [''] * len(cols))
    writer.writerow(['']  + [c['label'] for c in cols])

    for entry in LAYOUT.get(stmt_key, []):
        kind = entry[0]
        if kind == 'blank':
            writer.writerow([''] * n_cols)
            continue
        if kind == 'section':
            writer.writerow([entry[1]] + [''] * len(cols))
            continue
        # detail or subtotal
        _, bucket, label = entry
        display_label = f'  {label}' if kind == 'detail' else label.upper()
        row_out = [display_label]
        for col in cols:
            row_out.append(cell_value(col, stmt_data, bucket, kind))
        writer.writerow(row_out)
    writer.writerow([''] * n_cols)


def render_segments(writer, segments, segment_classifications, quarters, cols):
    n_cols = 1 + len(cols)
    writer.writerow(['── SEGMENTS (USD millions) ──'] + [''] * len(cols))
    axes = segments.get('axes', []) or []
    class_by_dim = {c.get('dimension'): c for c in (segment_classifications or [])}

    for axis in axes:
        dim = axis.get('dimension', '?')
        cls = class_by_dim.get(dim) or {}
        axis_type = cls.get('axis_type', '(unclassified)')
        leaf_members = set(cls.get('leaf_members') or [])

        writer.writerow([f'  Axis: {dim}  [type: {axis_type}]'] + [''] * len(cols))

        # Infer metrics present in this axis
        metrics = set()
        for r in axis.get('rows', []):
            for k in (r.get('values_by_quarter_and_metric') or {}):
                if '|' in k:
                    metrics.add(k.split('|', 1)[1])

        for metric in sorted(metrics):
            writer.writerow([f'    Metric: {metric}'] + [c['label'] for c in cols])
            for r in axis.get('rows', []):
                member = r.get('member', '')
                mtype = 'leaf' if (not leaf_members or member in leaf_members) else 'rollup'
                vals = r.get('values_by_quarter_and_metric', {}) or {}
                row_out = [f'      {member}{" [rollup]" if mtype == "rollup" else ""}']
                for col in cols:
                    if col['kind'] == 'quarter':
                        row_out.append(fmt(vals.get(f'{col["quarter"]}|{metric}')))
                    elif col['kind'] == 'fy_total':
                        qs = [q for q in col['quarters'] if q]
                        vs = [vals.get(f'{q}|{metric}') for q in qs]
                        vs = [v for v in vs if isinstance(v, (int, float))]
                        row_out.append(fmt(sum(vs)) if vs else '')
                    else:
                        row_out.append('')
                writer.writerow(row_out)
            # Consolidated row
            cons_by_key = axis.get('consolidated_by_quarter_and_metric', {}) or {}
            row_out = [f'      (Consolidated)']
            for col in cols:
                if col['kind'] == 'quarter':
                    row_out.append(fmt(cons_by_key.get(f'{col["quarter"]}|{metric}')))
                elif col['kind'] == 'fy_total':
                    qs = [q for q in col['quarters'] if q]
                    vs = [cons_by_key.get(f'{q}|{metric}') for q in qs]
                    vs = [v for v in vs if isinstance(v, (int, float))]
                    row_out.append(fmt(sum(vs)) if vs else '')
                else:
                    row_out.append('')
            writer.writerow(row_out)
            writer.writerow([''] * n_cols)


def collect_quarter_labels(result):
    qs = set()
    for stmt_data in result.get('statements', {}).values():
        for entry in stmt_data.get('normalized', {}).get('detail', []):
            qs.update((entry.get('values_by_quarter') or {}).keys())
        for entry in stmt_data.get('normalized', {}).get('subtotals', []):
            qs.update((entry.get('values_by_quarter') or {}).keys())
    return sort_quarters(qs)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ticker', required=True)
    parser.add_argument('--test', action='store_true', default=True)
    parser.add_argument('--no-test', dest='test', action='store_false')
    parser.add_argument('--input', help='Override input path to formula_mapped_v3.json')
    parser.add_argument('--output', help='Override output CSV path')
    args = parser.parse_args()

    extract_dir = os.path.join(os.path.dirname(__file__), args.ticker)
    work_dir = os.path.join(extract_dir, 'test') if args.test else extract_dir
    in_path = args.input or os.path.join(work_dir, 'formula_mapped_v3.json')
    out_path = args.output or os.path.join(work_dir, f'{args.ticker.lower()}_full_check.csv')

    if not os.path.isfile(in_path):
        print(f'ERROR: input not found: {in_path}', file=sys.stderr)
        sys.exit(1)

    with open(in_path) as f:
        result = json.load(f)

    quarters = collect_quarter_labels(result)
    if not quarters:
        print('ERROR: no quarter labels found in input', file=sys.stderr)
        sys.exit(1)

    fy_groups = fiscal_year_groups(quarters)

    with open(out_path, 'w', newline='') as f:
        writer = csv.writer(f)
        # Header / metadata
        ticker = result.get('ticker', args.ticker)
        v = result.get('verification', {}) or {}
        cols_is = build_column_spec(fy_groups, SHOW_FY_TOTAL['income_statement'])
        n_cols_is = 1 + len(cols_is)
        writer.writerow([f'{ticker} — Stage 2 Statement View'] + [''] * (n_cols_is - 1))
        writer.writerow([f'generated: {datetime.now().isoformat(timespec="seconds")}'] + [''] * (n_cols_is - 1))
        writer.writerow([f'verification: {"PASSED" if v.get("passed") else "FAILED"} '
                         f'({len(v.get("failures") or [])} failures)']
                        + [''] * (n_cols_is - 1))
        writer.writerow([''] * n_cols_is)

        statements = result.get('statements', {}) or {}
        for stmt_key, title in STATEMENT_TITLES:
            cols = build_column_spec(fy_groups, SHOW_FY_TOTAL[stmt_key])
            stmt_data = statements.get(stmt_key, {}) or {}
            render_statement(writer, stmt_key, title, stmt_data, cols)

        # Segments
        cols = build_column_spec(fy_groups, True)
        render_segments(writer, result.get('segments', {}) or {},
                        result.get('segment_classifications', []) or [],
                        quarters, cols)

    print(f'Written to {out_path}')
    line_count = sum(1 for _ in open(out_path))
    print(f'  {len(quarters)} quarters across {len(fy_groups)} fiscal years, {line_count} lines')


if __name__ == '__main__':
    main()
