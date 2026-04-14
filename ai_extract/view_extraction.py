"""
Reads an extraction JSON and prints a clean, readable view for review.
Handles both single-statement and "all" statement formats.

Usage:
    python3 ai_extract/view_extraction.py ai_extract/nvda_q1fy26_all_statements.json
"""

import json
import sys


def view_statement(name, stmt_data, ver_data):
    """View one statement section."""
    line_items = stmt_data.get('line_items', [])
    not_on = stmt_data.get('xbrl_not_on_statement', [])

    # Get periods from first line item with values
    periods = []
    for item in line_items:
        vals = item.get('values') or {}
        if vals:
            periods = list(vals.keys())
            break

    col_w = 16
    label_w = 55

    print(f"\n{'=' * 40}")
    print(f"  {name}")
    print(f"{'=' * 40}")

    period_headers = [p.replace('_', ' to ') if '_' in p else p for p in periods]
    print(f"\n{'':>{label_w}}  {''.join(h.rjust(col_w) for h in period_headers)}")
    print("─" * (label_w + 2 + col_w * len(periods)))

    for item in line_items:
        indent = "  " * item.get('indent_level', 0)
        label = f"{indent}{item['label']}"
        vals = item.get('values') or {}
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

        xbrl = "✓" if item.get('xbrl_match') is True else ("—" if item.get('xbrl_match') is None else "✗")
        print(f"{label:<{label_w}}  {''.join(formatted)}  {xbrl}")

    # Formulas
    if ver_data:
        print()
        print("─" * (label_w + 2 + col_w * len(periods)))
        print("FORMULA VERIFICATION")
        print()
        for check in ver_data.get('formula_checks', []):
            status = "✓" if check['pass'] else "✗"
            print(f"  {status} {check['formula']}")
            for period, detail in check['periods'].items():
                print(f"      {detail['computation']} = {detail['computed']}  (stated: {detail['stated']})")

    # Not on statement
    if not_on:
        print()
        print("─" * (label_w + 2 + col_w * len(periods)))
        print("XBRL FACTS NOT ON STATEMENT")
        print()
        for item in not_on:
            val = item.get('value', '')
            if isinstance(val, dict):
                val_str = " | ".join(f"{v}" for v in val.values())
            elif isinstance(val, (int, float)):
                val_str = f"{val:,}"
            else:
                val_str = str(val)
            print(f"  {item.get('concept', '?')}: {val_str}  [{item.get('period', '')}]")
            print(f"      {item.get('reason', '')}")


def view(path):
    with open(path) as f:
        data = json.load(f)

    ai = data['ai_extraction']
    ver = data.get('formula_verification', {})

    # Detect format: "all" has income_statement/balance_sheet/cash_flow keys
    if 'income_statement' in ai:
        for stmt_key, title in [('income_statement', 'INCOME STATEMENT'),
                                 ('balance_sheet', 'BALANCE SHEET'),
                                 ('cash_flow', 'CASH FLOW')]:
            stmt_data = ai.get(stmt_key, {})
            ver_data = ver.get(stmt_key, ver) if isinstance(ver, dict) else {}
            view_statement(title, stmt_data, ver_data)

        # Cross-statement checks
        cross = ai.get('cross_statement_checks', [])
        if cross:
            print(f"\n{'=' * 40}")
            print(f"  CROSS-STATEMENT CHECKS")
            print(f"{'=' * 40}\n")
            for check in cross:
                status = "✓" if check.get('match') else "✗"
                print(f"  {status} {check.get('check', '')}")
                for k, v in check.items():
                    if k not in ('check', 'match'):
                        print(f"      {k}: {v}")
    else:
        # Single statement format
        ver_data = ver if 'formula_checks' in ver else {}
        view_statement('EXTRACTION', ai, ver_data)

    print()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 ai_extract/view_extraction.py <extraction.json>")
        sys.exit(1)
    view(sys.argv[1])
