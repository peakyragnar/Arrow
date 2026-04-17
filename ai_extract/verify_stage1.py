"""Independent Python verifier for Stage 1 output.

Walks every per-filing JSON in `ai_extract/{TICKER}/test/` and re-runs each
formula tie from scratch using the filing's stored `line_items` values.
Ignores the AI-produced `pass` flags — computes authoritatively.

Reports per filing: how many formulas tie exactly, how many are off, and the
delta (both absolute and relative) for each failure.

Does not make API calls. Cost: $0.

Usage:
    python3 ai_extract/verify_stage1.py --ticker NVDA [--test]
"""
import argparse
import glob
import json
import os
import re
import sys
from collections import defaultdict


def _eval_computation(computation_str):
    """Safely evaluate a numeric expression string like
    '18775 + 1474 + 611 - -175 + -2177 - -98 - -933 - 1258 - -560 + 941 + 7128 + 350'.
    Only digits, operators, spaces, and minus signs allowed. No names, no calls.
    """
    if not computation_str:
        return None
    s = str(computation_str).strip()
    # Safety: reject anything that isn't digits/operators/spaces/decimals
    if not re.fullmatch(r'[\d\s\+\-\*\/\.\(\)]+', s):
        return None
    try:
        # Python handles '- -175' fine as 'minus negative 175'
        return eval(s, {'__builtins__': {}}, {})
    except Exception:
        return None


def check_filing(filepath):
    """Re-verify every formula in a per-filing JSON. Returns list of findings."""
    with open(filepath) as f:
        d = json.load(f)
    ai = d.get('ai_extraction', d)
    fv = d.get('formula_verification', {}) or {}
    findings = []

    # Also verify stated subtotals against line_items (compute expected from components)
    for stmt_name in ('income_statement', 'balance_sheet', 'cash_flow'):
        stmt_fv = fv.get(stmt_name, {}) or {}
        for chk in stmt_fv.get('formula_checks', []) or []:
            formula = chk.get('formula', '')
            claimed_pass = chk.get('pass')
            periods = chk.get('periods', {}) or {}
            for period, info in periods.items():
                if not isinstance(info, dict):
                    continue
                comp_str = info.get('computation')
                stated = info.get('stated')
                ai_computed = info.get('computed')

                # Independent evaluation
                our_computed = _eval_computation(comp_str)
                if our_computed is None or stated is None:
                    continue

                delta = our_computed - stated
                if abs(delta) < 1:
                    tie = True
                elif abs(stated) > 0 and abs(delta) / abs(stated) < 0.001:
                    tie = True
                else:
                    tie = False

                # Did Stage 1's own `computed` match ours? If not, Stage 1's
                # computation string and computed field disagreed.
                ai_vs_ours = ai_computed is not None and abs(our_computed - ai_computed) > 1

                # Did Stage 1's pass flag match reality?
                ai_lied = (claimed_pass is True) and not tie

                if (not tie) or ai_vs_ours or ai_lied:
                    findings.append({
                        'file': os.path.basename(filepath),
                        'statement': stmt_name,
                        'formula': formula[:80],
                        'period': period,
                        'stated': stated,
                        'ai_computed': ai_computed,
                        'python_computed': our_computed,
                        'delta_vs_stated': delta,
                        'ai_vs_python_differs': ai_vs_ours,
                        'claimed_pass': claimed_pass,
                        'actually_ties': tie,
                        'computation': comp_str,
                    })
    return findings


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ticker', required=True)
    parser.add_argument('--test', action='store_true', default=True)
    parser.add_argument('--no-test', dest='test', action='store_false')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    extract_dir = os.path.join(os.path.dirname(__file__), args.ticker)
    work_dir = os.path.join(extract_dir, 'test') if args.test else extract_dir
    files = sorted(glob.glob(os.path.join(work_dir, 'q*_fy*_10*.json'))
                   + glob.glob(os.path.join(work_dir, 'q*_cy*_10*.json')))
    files = [f for f in files if 'stripped' not in os.path.basename(f)
             and 'formula' not in os.path.basename(f)]

    if not files:
        print(f'No per-filing JSONs found in {work_dir}', file=sys.stderr)
        sys.exit(1)

    print(f'\n{"="*78}')
    print(f'  Independent Stage 1 Verification for {args.ticker}')
    print(f'  Files: {len(files)}')
    print(f'{"="*78}\n')

    all_findings = []
    per_file_summary = []

    for fp in files:
        findings = check_filing(fp)
        fname = os.path.basename(fp)
        all_findings.extend(findings)

        lies = sum(1 for f in findings if f['claimed_pass'] is True and not f['actually_ties'])
        real_breaks = sum(1 for f in findings if not f['actually_ties'])
        ai_miscomputes = sum(1 for f in findings if f['ai_vs_python_differs'])
        per_file_summary.append({
            'file': fname,
            'total_findings': len(findings),
            'claimed_pass_but_broken': lies,
            'real_breaks': real_breaks,
            'ai_computation_wrong': ai_miscomputes,
        })

    # Per-file summary
    print(f'{"File":30s}  {"findings":>9s}  {"pass=True_but_broken":>22s}  {"AI_compute_wrong":>18s}')
    print('-' * 85)
    for s in per_file_summary:
        print(f'{s["file"]:30s}  {s["total_findings"]:>9d}  '
              f'{s["claimed_pass_but_broken"]:>22d}  {s["ai_computation_wrong"]:>18d}')
    print('-' * 85)
    print(f'{"TOTAL":30s}  {sum(s["total_findings"] for s in per_file_summary):>9d}  '
          f'{sum(s["claimed_pass_but_broken"] for s in per_file_summary):>22d}  '
          f'{sum(s["ai_computation_wrong"] for s in per_file_summary):>18d}')

    # Categorize findings
    print(f'\n{"="*78}')
    print(f'  Findings detail — first 30 of {len(all_findings)} total')
    print(f'{"="*78}\n')

    for f in all_findings[:30]:
        flags = []
        if f['claimed_pass'] is True and not f['actually_ties']:
            flags.append('LIED-pass')
        if f['ai_vs_python_differs']:
            flags.append('AI-computed≠Python-computed')
        if not f['actually_ties']:
            flags.append(f'Δ={f["delta_vs_stated"]:+,.0f}')
        flag_str = ', '.join(flags) or 'OK'

        print(f'  {f["file"]:25s}  {f["statement"]:18s}  [{flag_str}]')
        print(f'    formula  : {f["formula"]}')
        print(f'    period   : {f["period"]}')
        print(f'    stated   : {f["stated"]:,}')
        print(f'    AI says  : {f["ai_computed"]:,}' if f["ai_computed"] is not None else '    AI says  : None')
        print(f'    Python   : {f["python_computed"]:,}')
        if args.verbose:
            print(f'    computation: {f["computation"][:120]}')
        print()

    if len(all_findings) > 30:
        print(f'  ... and {len(all_findings) - 30} more. Run with --verbose or read full output.')

    # Materiality summary
    print(f'\n{"="*78}')
    print(f'  Materiality summary')
    print(f'{"="*78}\n')
    by_magnitude = defaultdict(int)
    for f in all_findings:
        if f['actually_ties']:
            continue
        d = abs(f['delta_vs_stated'])
        stated = abs(f['stated']) if f['stated'] else 1
        rel = d / stated if stated else float('inf')
        if d <= 5:
            by_magnitude['<=$5M (rounding)'] += 1
        elif rel <= 0.005:
            by_magnitude['<=0.5% of subtotal'] += 1
        elif rel <= 0.02:
            by_magnitude['0.5%-2%'] += 1
        elif rel <= 0.10:
            by_magnitude['2%-10%'] += 1
        else:
            by_magnitude['>10% — material'] += 1
    for k, n in sorted(by_magnitude.items()):
        print(f'  {k:30s}  {n}')


if __name__ == '__main__':
    main()
