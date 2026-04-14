"""
Reads an extraction JSON and prints a clean, readable view for review.

Usage:
    python3 ai_extract/view_extraction.py ai_extract/nvda_q1fy26_income_sonnet.json
"""

import json
import sys


def view(path):
    with open(path) as f:
        data = json.load(f)

    ai = data['ai_extraction']
    ver = data['formula_verification']

    # Get periods from first line item with values
    periods = []
    for item in ai['line_items']:
        if item.get('values'):
            periods = list(item['values'].keys())
            break

    # Header
    period_headers = [p.replace('_', ' to ') if '_' in p else p for p in periods]
    col_w = 16
    label_w = 55
    print()
    print(f"{'':>{label_w}}  {''.join(h.rjust(col_w) for h in period_headers)}")
    print("─" * (label_w + 2 + col_w * len(periods)))

    # Line items
    for item in ai['line_items']:
        indent = "  " * item.get('indent_level', 0)
        label = f"{indent}{item['label']}"
        vals = item.get('values', {})
        unit = item.get('unit', '')

        if not vals:
            print(f"{label:<{label_w}}")
            continue

        formatted = []
        for p in periods:
            v = vals.get(p)
            if v is None:
                formatted.append("—".rjust(col_w))
            elif unit == 'USD_per_share':
                formatted.append(f"${v:.2f}".rjust(col_w))
            elif unit == 'shares_millions':
                formatted.append(f"{v:,}".rjust(col_w))
            else:
                formatted.append(f"{v:,}".rjust(col_w))

        xbrl = "✓" if item.get('xbrl_match') else "✗"
        print(f"{label:<{label_w}}  {''.join(formatted)}  {xbrl}")

    # Formulas
    print()
    print("─" * (label_w + 2 + col_w * len(periods)))
    print("FORMULA VERIFICATION")
    print()
    for check in ver['formula_checks']:
        status = "✓" if check['pass'] else "✗"
        print(f"  {status} {check['formula']}")
        for period, detail in check['periods'].items():
            print(f"      {detail['computation']} = {detail['computed']}  (stated: {detail['stated']})")

    # XBRL not on statement
    not_on = ai.get('xbrl_not_on_statement', [])
    if not_on:
        print()
        print("─" * (label_w + 2 + col_w * len(periods)))
        print("XBRL FACTS NOT ON STATEMENT")
        print()
        for item in not_on:
            print(f"  {item['concept']}: {item['value']:,}  [{item.get('period', '')}]")
            print(f"      {item.get('reason', '')}")

    print()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 ai_extract/view_extraction.py <extraction.json>")
        sys.exit(1)
    view(sys.argv[1])
