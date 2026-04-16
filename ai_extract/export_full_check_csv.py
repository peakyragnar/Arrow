"""
Export quarterly.json to the full check CSV format with Excel formulas.
Matches the format of nvda_fy24_fy26_full_check.csv.

Usage:
    python3 ai_extract/export_full_check_csv.py --ticker NVDA --input ai_extract/NVDA/test/quarterly.json
"""

import argparse
import json
import os


def get_field(records, period_idx, field, default=0):
    """Get a field value from a quarterly record, converting from raw dollars to millions."""
    if period_idx >= len(records):
        return default
    val = records[period_idx].get(field, default)
    if val is None:
        return default
    if isinstance(val, (int, float)) and abs(val) > 1000:
        return round(val / 1e6)
    return val


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ticker', required=True)
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', help='Output CSV path')
    args = parser.parse_args()

    with open(args.input) as f:
        records = json.load(f)

    # Sort by period_end
    records.sort(key=lambda r: r.get('period_end', ''))

    # Group into fiscal years (4 quarters each)
    # Determine fiscal years from the data
    n = len(records)

    # Build column headers
    # Figure out fiscal year labels from period dates
    fy_groups = []
    for i in range(0, n, 4):
        chunk = records[i:i+4]
        if len(chunk) == 4:
            fy_groups.append(chunk)
        else:
            fy_groups.append(chunk)

    # Build header row
    headers = ['']
    for gi, group in enumerate(fy_groups):
        for qi, r in enumerate(group):
            pe = r.get('period_end', '')
            headers.append(f"Q{qi+1} ({pe})")
        headers.append(f"FY Total")
        if gi < len(fy_groups) - 1:
            headers.append('')  # spacer

    lines = []
    lines.append(','.join(headers))

    def val(gi, qi, field):
        """Get value for fiscal year group gi, quarter qi."""
        if gi >= len(fy_groups) or qi >= len(fy_groups[gi]):
            return ''
        return str(get_field(fy_groups[gi], qi, field, 0))

    def row(label, field, negate=False):
        """Build a data row across all fiscal year groups."""
        cols = [label]
        for gi, group in enumerate(fy_groups):
            for qi in range(4):
                if qi < len(group):
                    v = get_field(group, qi, field, 0)
                    if negate and v:
                        v = -v
                    cols.append(str(v))
                else:
                    cols.append('')
            # FY total = sum formula placeholder
            q_vals = [get_field(group, qi, field, 0) for qi in range(min(4, len(group)))]
            if negate:
                q_vals = [-v if v else 0 for v in q_vals]
            cols.append(str(sum(q_vals)))
            if gi < len(fy_groups) - 1:
                cols.append('')
        lines.append(','.join(cols))

    def blank():
        lines.append(','.join([''] * len(headers)))

    def section(title):
        lines.append(','.join([title] + [''] * (len(headers) - 1)))

    # INCOME STATEMENT — use standard analytical fields
    section('INCOME STATEMENT ($ millions)')
    lines.append(','.join(headers))
    row('Revenue', 'revenue')
    row('Cost of revenue', 'cogs')
    row('Gross profit', 'gross_profit')
    blank()
    row('Research and development', 'rd_expense')
    row('Operating income', 'operating_income')
    blank()
    row('Interest income', 'interest_income')
    row('Interest expense', 'interest_expense')
    row('Pretax income', 'pretax_income')
    row('Income tax expense', 'income_tax_expense')
    row('Net income', 'net_income')
    blank()
    row('Shares - Diluted (M)', 'diluted_shares')
    blank()

    # BALANCE SHEET
    section('BALANCE SHEET ($ millions)')
    lines.append(','.join(headers))
    row('Cash', 'cash')
    row('Short-term investments', 'short_term_investments')
    row('Accounts receivable', 'accounts_receivable')
    row('Inventory', 'inventory')
    row('Total assets', 'total_assets')
    blank()
    row('Accounts payable', 'accounts_payable')
    row('Short-term debt', 'short_term_debt')
    row('Long-term debt', 'long_term_debt')
    row('Equity', 'equity')
    blank()
    row('Operating lease liabilities (total)', 'operating_lease_liabilities')
    blank()

    # CASH FLOW
    section('CASH FLOW ($ millions)')
    lines.append(','.join(headers))
    row('SBC', 'sbc')
    row('D&A', 'dna')
    row('CFO', 'cfo')
    row('Capex', 'capex')
    row('Acquisitions', 'acquisitions')

    # Write
    out_path = args.output or os.path.join(os.path.dirname(args.input),
                                            f'{args.ticker.lower()}_full_check.csv')
    with open(out_path, 'w') as f:
        f.write('\n'.join(lines))
    print(f"Written to {out_path}")
    print(f"  {len(records)} quarters, {len(lines)} rows")


if __name__ == '__main__':
    main()
