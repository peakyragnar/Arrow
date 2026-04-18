"""
Cross-period validation of quarterly.json output.
Flags anomalies for manual review.

Usage:
    python3 ai_extract/validate_quarterly.py --input ai_extract/NVDA/test/quarterly.json
"""

import argparse
import json
import os
import sys


def validate(records):
    """Run all cross-period checks. Returns list of findings."""
    findings = []
    n = len(records)
    if n < 2:
        return findings

    periods = [r.get('period_end', '?') for r in records]

    # Collect all numeric fields across all records
    all_fields = set()
    for r in records:
        for k, v in r.items():
            if isinstance(v, (int, float)) and k not in ('period_end', 'period_start', 'ticker'):
                all_fields.add(k)

    # 1. FIELD PRESENCE — flag fields present in some but not all quarters
    for field in sorted(all_fields):
        present = [(i, r.get(field)) for i, r in enumerate(records) if r.get(field) is not None]
        absent = [i for i, r in enumerate(records) if r.get(field) is None]

        if 0 < len(absent) < n and len(present) >= n * 0.5:
            missing_periods = [periods[i] for i in absent]
            findings.append({
                'type': 'MISSING_FIELD',
                'severity': 'HIGH' if field in STANDARD_FIELDS else 'LOW',
                'field': field,
                'message': f'{field} present in {len(present)}/{n} quarters, missing in: {", ".join(missing_periods)}'
            })

    # 2. DISCONTINUITIES — sudden jumps in value
    for field in sorted(all_fields):
        vals = [(i, r.get(field)) for i, r in enumerate(records)]
        vals = [(i, v) for i, v in vals if v is not None and v != 0]

        for j in range(1, len(vals)):
            prev_i, prev_v = vals[j-1]
            curr_i, curr_v = vals[j]

            if prev_v == 0:
                continue

            ratio = abs(curr_v / prev_v)
            if ratio > 5 or ratio < 0.2:
                findings.append({
                    'type': 'DISCONTINUITY',
                    'severity': 'HIGH',
                    'field': field,
                    'message': f'{field}: {periods[prev_i]}={fmt(prev_v)} -> {periods[curr_i]}={fmt(curr_v)} ({ratio:.1f}x)'
                })

    # 3. SIGN CONSISTENCY — flag if sign flips in a field that should be consistent
    for field in sorted(all_fields):
        vals = [r.get(field) for r in records if r.get(field) is not None and r.get(field) != 0]
        if len(vals) < 3:
            continue

        pos = sum(1 for v in vals if v > 0)
        neg = sum(1 for v in vals if v < 0)

        # If mostly one sign but a few of the other
        if pos > 0 and neg > 0:
            minority = min(pos, neg)
            if minority <= 2 and minority < len(vals) * 0.3:
                # Find the minority quarters
                expected_sign = 'positive' if pos > neg else 'negative'
                anomalies = []
                for i, r in enumerate(records):
                    v = r.get(field)
                    if v is not None and v != 0:
                        if (expected_sign == 'positive' and v < 0) or (expected_sign == 'negative' and v > 0):
                            anomalies.append(f'{periods[i]}={fmt(v)}')
                if anomalies:
                    findings.append({
                        'type': 'SIGN_FLIP',
                        'severity': 'MEDIUM',
                        'field': field,
                        'message': f'{field}: mostly {expected_sign}, but {", ".join(anomalies)}'
                    })

    # 4. BALANCE SHEET CONTINUITY — ending values should be close to next quarter's starting
    bs_fields = ['cash', 'total_assets', 'equity', 'long_term_debt', 'inventory',
                 'accounts_receivable', 'accounts_payable']
    for field in bs_fields:
        for i in range(n - 1):
            curr = records[i].get(field)
            next_val = records[i + 1].get(field)
            if curr is not None and next_val is not None and curr != 0:
                change = abs(next_val - curr) / abs(curr)
                if change > 1.0:  # more than 100% change quarter over quarter
                    findings.append({
                        'type': 'BS_JUMP',
                        'severity': 'MEDIUM',
                        'field': field,
                        'message': f'{field}: {periods[i]}={fmt(curr)} -> {periods[i+1]}={fmt(next_val)} ({change:.0%} change)'
                    })

    # 5. CROSS-CHECK: net income consistency
    for i, r in enumerate(records):
        ni = r.get('net_income')
        cf_ni = r.get('cf_net_income')
        if ni is not None and cf_ni is not None and ni != cf_ni:
            findings.append({
                'type': 'CROSS_CHECK',
                'severity': 'HIGH',
                'field': 'net_income vs cf_net_income',
                'message': f'{periods[i]}: IS net_income={fmt(ni)} != CF net_income={fmt(cf_ni)}'
            })

    # 6. STOCK SPLIT DETECTION
    shares_field = 'diluted_shares'
    shares_vals = [(i, r.get(shares_field)) for i, r in enumerate(records)
                   if r.get(shares_field) is not None]
    for j in range(1, len(shares_vals)):
        prev_i, prev_v = shares_vals[j-1]
        curr_i, curr_v = shares_vals[j]
        if prev_v > 0:
            ratio = curr_v / prev_v
            if ratio > 1.5:
                split_ratio = round(ratio)
                findings.append({
                    'type': 'STOCK_SPLIT',
                    'severity': 'HIGH',
                    'field': shares_field,
                    'message': f'Possible {split_ratio}:1 stock split between {periods[prev_i]} ({fmt(prev_v)}) and {periods[curr_i]} ({fmt(curr_v)})'
                })
            elif ratio < 0.67:
                findings.append({
                    'type': 'STOCK_SPLIT',
                    'severity': 'HIGH',
                    'field': shares_field,
                    'message': f'Possible reverse split between {periods[prev_i]} ({fmt(prev_v)}) and {periods[curr_i]} ({fmt(curr_v)})'
                })

    # 7. FORMULA CHECKS on quarterly data
    for i, r in enumerate(records):
        # Gross profit = revenue - cogs
        rev = r.get('revenue')
        cogs = r.get('cogs')
        gp = r.get('gross_profit')
        if all(v is not None for v in [rev, cogs, gp]):
            expected = rev - cogs
            if abs(expected - gp) > 1e6:
                findings.append({
                    'type': 'FORMULA',
                    'severity': 'HIGH',
                    'field': 'gross_profit',
                    'message': f'{periods[i]}: revenue({fmt(rev)}) - cogs({fmt(cogs)}) = {fmt(expected)} != gross_profit({fmt(gp)})'
                })

        # Pretax - tax = net income
        pretax = r.get('pretax_income')
        tax = r.get('income_tax_expense')
        ni = r.get('net_income')
        if all(v is not None for v in [pretax, tax, ni]):
            expected = pretax - tax
            if abs(expected - ni) > 1e6:
                findings.append({
                    'type': 'FORMULA',
                    'severity': 'HIGH',
                    'field': 'net_income',
                    'message': f'{periods[i]}: pretax({fmt(pretax)}) - tax({fmt(tax)}) = {fmt(expected)} != net_income({fmt(ni)})'
                })

    return findings


STANDARD_FIELDS = {
    'revenue', 'cogs', 'gross_profit', 'operating_income', 'pretax_income',
    'income_tax_expense', 'net_income', 'interest_expense', 'interest_income',
    'sbc', 'dna', 'diluted_shares', 'cash', 'short_term_investments',
    'accounts_receivable', 'inventory', 'accounts_payable', 'total_assets',
    'equity', 'short_term_debt', 'long_term_debt', 'operating_lease_liabilities',
    'cfo', 'capex', 'acquisitions', 'rd_expense',
}


def fmt(val):
    """Format a value for display."""
    if val is None:
        return 'null'
    if isinstance(val, float) and abs(val) < 100:
        return f'{val:.2f}'
    if abs(val) >= 1e9:
        return f'{val/1e9:.1f}B'
    if abs(val) >= 1e6:
        return f'{val/1e6:.0f}M'
    return f'{val:,.0f}'


def main():
    parser = argparse.ArgumentParser(description='Cross-period validation of quarterly data')
    parser.add_argument('--input', required=True, help='Path to quarterly.json')
    args = parser.parse_args()

    with open(args.input) as f:
        records = json.load(f)

    records.sort(key=lambda r: r.get('period_end', ''))
    print(f"Validating {len(records)} quarters from {args.input}")
    print()

    findings = validate(records)

    if not findings:
        print("NO ISSUES FOUND")
        return

    # Group by severity
    high = [f for f in findings if f['severity'] == 'HIGH']
    medium = [f for f in findings if f['severity'] == 'MEDIUM']
    low = [f for f in findings if f['severity'] == 'LOW']

    if high:
        print(f"{'='*80}")
        print(f"  HIGH SEVERITY ({len(high)} issues)")
        print(f"{'='*80}")
        for f in high:
            print(f"  [{f['type']}] {f['message']}")
        print()

    if medium:
        print(f"{'='*80}")
        print(f"  MEDIUM SEVERITY ({len(medium)} issues)")
        print(f"{'='*80}")
        for f in medium:
            print(f"  [{f['type']}] {f['message']}")
        print()

    if low:
        print(f"{'='*80}")
        print(f"  LOW SEVERITY ({len(low)} issues)")
        print(f"{'='*80}")
        for f in low:
            print(f"  [{f['type']}] {f['message']}")
        print()

    print(f"TOTAL: {len(high)} high, {len(medium)} medium, {len(low)} low")


if __name__ == '__main__':
    main()
