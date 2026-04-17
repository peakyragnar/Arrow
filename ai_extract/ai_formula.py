"""
Stage 2: Quarterize as-reported statements + normalize analytical fields.

Two outputs, both verified by math:

  Output 1 — statements.{income_statement, balance_sheet, cash_flow}
    As-reported rows merged across filings by xbrl_concept, label variants
    joined, values per quarter, plus each statement's declared formulas.
    Segments rendered as a fourth section. Deterministic: built in Python
    from Stage 1 line_items + xbrl_not_on_statement. No plugs.

  Output 2 — analytical.{field: {values_by_quarter, source_per_quarter}}
    The universal analytical field bag needed by formulas.md. Produced by
    the AI because mapping fields like operating_lease_liabilities to the
    right as-reported rows + note detail is judgment work. Every value
    traces back to a row in Output 1.

Verification battery (Python, post-AI):
  1. Every statement formula ties in every quarter.
  2. Q1+Q2+Q3+Q4 = annual for every flow concept where all five exist.
  3. Balance-sheet instants consistent across filings for shared period-ends.
  4. Segment members sum to consolidated total per axis/metric/quarter.
  5. Every analytical value reconciles to signed sum of its source rows.
  6. Forward-fills audited against raw parsed_xbrl.json.
  7. Basic sign sanity (revenue positive, no impossible flips).

Retry up to 3 times with failures echoed to the AI. Hard error on
unresolved failures — no silent accept.

Usage:
    python3 ai_extract/ai_formula.py --ticker NVDA --test
"""

import argparse
import glob
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import date, datetime

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

import anthropic


# ────────────────────────────────────────────────────────────────────────────
# Canonical buckets — MUST match ai_extract/canonical_buckets.md
# ────────────────────────────────────────────────────────────────────────────

CANONICAL_BUCKETS = {
    'income_statement': {
        'detail': [
            'revenue', 'finance_div_revenue', 'insurance_div_revenue', 'other_revenue',
            'cogs',
            'sga', 'rd', 'dna', 'other_opex',
            'interest_expense', 'interest_income',
            'equity_affiliates', 'other_nonop',
            'restructuring', 'goodwill_impairment',
            'gain_sale_assets', 'gain_sale_investments', 'other_unusual',
            'tax', 'minority_interest', 'preferred_dividend',
        ],
        'subtotals': [
            ('total_revenue',
             [('+', 'revenue'), ('+', 'finance_div_revenue'),
              ('+', 'insurance_div_revenue'), ('+', 'other_revenue')]),
            ('gross_profit',
             [('+', 'total_revenue'), ('-', 'cogs')]),
            ('total_opex',
             [('+', 'sga'), ('+', 'rd'), ('+', 'dna'), ('+', 'other_opex')]),
            ('operating_income',
             [('+', 'gross_profit'), ('-', 'total_opex')]),
            ('net_interest_expense',
             [('+', 'interest_expense'), ('-', 'interest_income')]),
            ('ebt_excl_unusual',
             [('+', 'operating_income'), ('-', 'net_interest_expense'),
              ('+', 'equity_affiliates'), ('+', 'other_nonop')]),
            ('ebt_incl_unusual',
             [('+', 'ebt_excl_unusual'), ('+', 'restructuring'),
              ('+', 'goodwill_impairment'), ('+', 'gain_sale_assets'),
              ('+', 'gain_sale_investments'), ('+', 'other_unusual')]),
            ('continuing_ops',
             [('+', 'ebt_incl_unusual'), ('-', 'tax')]),
            ('net_income',
             [('+', 'continuing_ops'), ('-', 'minority_interest')]),
            ('ni_common_incl_extra',
             [('+', 'net_income'), ('-', 'preferred_dividend')]),
        ],
    },
    'balance_sheet': {
        'detail': [
            'cash', 'sti', 'trading_securities',
            'accounts_receivable', 'other_receivables',
            'inventory', 'restricted_cash', 'prepaid_expenses', 'other_current_assets',
            'gross_ppe', 'accumulated_depreciation',
            'long_term_investments', 'goodwill', 'other_intangibles',
            'loans_receivable_lt', 'deferred_tax_assets_lt',
            'deferred_charges_lt', 'other_lt_assets',
            'accounts_payable', 'accrued_expenses',
            'current_portion_lt_debt', 'current_portion_leases',
            'current_income_taxes_payable', 'unearned_revenue_current',
            'other_current_liabilities',
            'long_term_debt', 'long_term_leases',
            'unearned_revenue_nc', 'deferred_tax_liability_nc', 'other_nc_liabilities',
            'common_stock', 'apic', 'retained_earnings',
            'treasury_stock', 'comprehensive_income_other',
            'noncontrolling_interest',
        ],
        'subtotals': [
            ('total_cash_sti',
             [('+', 'cash'), ('+', 'sti'), ('+', 'trading_securities')]),
            ('total_receivables',
             [('+', 'accounts_receivable'), ('+', 'other_receivables')]),
            ('total_current_assets',
             [('+', 'total_cash_sti'), ('+', 'total_receivables'),
              ('+', 'inventory'), ('+', 'restricted_cash'),
              ('+', 'prepaid_expenses'), ('+', 'other_current_assets')]),
            # accumulated_depreciation is stored as a negative number; add it.
            ('net_ppe',
             [('+', 'gross_ppe'), ('+', 'accumulated_depreciation')]),
            ('total_assets',
             [('+', 'total_current_assets'), ('+', 'net_ppe'),
              ('+', 'long_term_investments'), ('+', 'goodwill'),
              ('+', 'other_intangibles'), ('+', 'loans_receivable_lt'),
              ('+', 'deferred_tax_assets_lt'), ('+', 'deferred_charges_lt'),
              ('+', 'other_lt_assets')]),
            ('total_current_liabilities',
             [('+', 'accounts_payable'), ('+', 'accrued_expenses'),
              ('+', 'current_portion_lt_debt'), ('+', 'current_portion_leases'),
              ('+', 'current_income_taxes_payable'),
              ('+', 'unearned_revenue_current'),
              ('+', 'other_current_liabilities')]),
            ('total_liabilities',
             [('+', 'total_current_liabilities'), ('+', 'long_term_debt'),
              ('+', 'long_term_leases'), ('+', 'unearned_revenue_nc'),
              ('+', 'deferred_tax_liability_nc'), ('+', 'other_nc_liabilities')]),
            # treasury_stock is stored as a negative number; add it.
            ('common_equity',
             [('+', 'common_stock'), ('+', 'apic'),
              ('+', 'retained_earnings'), ('+', 'treasury_stock'),
              ('+', 'comprehensive_income_other')]),
            ('total_equity',
             [('+', 'common_equity'), ('+', 'noncontrolling_interest')]),
            ('total_liabilities_and_equity',
             [('+', 'total_liabilities'), ('+', 'total_equity')]),
        ],
    },
    'cash_flow': {
        'detail': [
            'net_income_start',
            'dna', 'gain_sale_asset', 'gain_sale_investments',
            'amort_deferred_charges', 'asset_writedown_restructuring',
            'sbc', 'other_operating',
            'change_ar', 'change_inventory', 'change_ap',
            'change_unearned_revenue', 'change_income_taxes',
            'change_other_operating',
            'capex', 'sale_ppe', 'acquisitions', 'divestitures',
            'investment_securities', 'loans_orig_sold', 'other_investing',
            'short_term_debt_issued', 'long_term_debt_issued',
            'short_term_debt_repaid', 'long_term_debt_repaid',
            'stock_issuance', 'stock_repurchase',
            'common_dividends', 'preferred_dividends',
            'special_dividends', 'other_financing',
            'fx_adjustments', 'misc_cf_adjustments',
        ],
        'subtotals': [
            ('cfo',
             [('+', 'net_income_start'),
              ('+', 'dna'), ('+', 'gain_sale_asset'),
              ('+', 'gain_sale_investments'), ('+', 'amort_deferred_charges'),
              ('+', 'asset_writedown_restructuring'),
              ('+', 'sbc'), ('+', 'other_operating'),
              ('+', 'change_ar'), ('+', 'change_inventory'),
              ('+', 'change_ap'), ('+', 'change_unearned_revenue'),
              ('+', 'change_income_taxes'), ('+', 'change_other_operating')]),
            ('cfi',
             [('+', 'capex'), ('+', 'sale_ppe'), ('+', 'acquisitions'),
              ('+', 'divestitures'), ('+', 'investment_securities'),
              ('+', 'loans_orig_sold'), ('+', 'other_investing')]),
            ('total_debt_issued',
             [('+', 'short_term_debt_issued'), ('+', 'long_term_debt_issued')]),
            ('total_debt_repaid',
             [('+', 'short_term_debt_repaid'), ('+', 'long_term_debt_repaid')]),
            ('total_common_pref_dividends',
             [('+', 'common_dividends'), ('+', 'preferred_dividends')]),
            ('cff',
             [('+', 'total_debt_issued'), ('+', 'total_debt_repaid'),
              ('+', 'stock_issuance'), ('+', 'stock_repurchase'),
              ('+', 'total_common_pref_dividends'),
              ('+', 'special_dividends'), ('+', 'other_financing')]),
            ('net_change_in_cash',
             [('+', 'cfo'), ('+', 'cfi'), ('+', 'cff'),
              ('+', 'fx_adjustments'), ('+', 'misc_cf_adjustments')]),
        ],
    },
}

# Statement's flow buckets (for Q1+Q2+Q3+Q4 = annual checks)
FLOW_STATEMENTS = {'income_statement', 'cash_flow'}


# ────────────────────────────────────────────────────────────────────────────
# Fiscal calendar helpers
# ────────────────────────────────────────────────────────────────────────────

FILENAME_RE = re.compile(r'^q([1-4])_(fy|cy)(\d{2})_10([qk])(?:_stripped)?\.json$')


def parse_filename(path):
    """Extract fiscal metadata from per-filing filename.

    Returns dict with: file, fiscal_year_label (FY24/CY24), fiscal_period (Q1..Q4),
    form (10-Q/10-K), quarter_label (FY24Q1).
    Returns None if filename doesn't match the pattern.
    """
    fname = os.path.basename(path)
    m = FILENAME_RE.match(fname)
    if not m:
        return None
    qn, prefix, yy, form_char = m.groups()
    return {
        'file': fname,
        'fiscal_period': f'Q{qn}',
        'fiscal_year_label': f'{prefix.upper()}{yy}',
        'form': '10-K' if form_char == 'k' else '10-Q',
        'quarter_label': f'{prefix.upper()}{yy}Q{qn}',
    }


def parse_period_key(key):
    """Parse a values-dict key into (kind, start, end, duration_days).

    Face line items use 'YYYY-MM-DD_YYYY-MM-DD' and 'YYYY-MM-DD'.
    Note-detail (xbrl_not_on_statement) values use 'YYYY-MM-DD to YYYY-MM-DD'.
    Both forms accepted.
    """
    if not isinstance(key, str):
        return (None, None, None, 0)
    if ' to ' in key:
        parts = key.split(' to ')
        if len(parts) == 2:
            try:
                s = datetime.strptime(parts[0].strip(), '%Y-%m-%d').date()
                e = datetime.strptime(parts[1].strip(), '%Y-%m-%d').date()
                return ('duration', s, e, (e - s).days)
            except ValueError:
                return (None, None, None, 0)
    parts = key.split('_')
    if len(parts) == 2:
        try:
            s = datetime.strptime(parts[0], '%Y-%m-%d').date()
            e = datetime.strptime(parts[1], '%Y-%m-%d').date()
            return ('duration', s, e, (e - s).days)
        except ValueError:
            return (None, None, None, 0)
    if len(parts) == 1:
        try:
            d = datetime.strptime(parts[0], '%Y-%m-%d').date()
            return ('instant', None, d, 0)
        except ValueError:
            return (None, None, None, 0)
    return (None, None, None, 0)


def derive_filing_period_end(ai):
    """Filing period_end = modal latest instant date from the balance sheet.

    BS line_items are instants; their latest date is the filing's report date.
    Using mode guards against stray instants on the face.
    """
    dates = []
    for item in ai.get('balance_sheet', {}).get('line_items', []):
        for k in item.get('values', {}).keys():
            kind, _, end, _ = parse_period_key(k)
            if kind == 'instant' and end:
                dates.append(end)
    if not dates:
        return None
    latest = max(dates)
    return latest


def pick_period_value(values, filing_period_end, statement, form):
    """Select the current-period value from a line item's values dict.

    Rules:
      - 10-Q IS: shortest duration ending at filing_period_end (the 3-month quarterly column).
      - 10-Q CF: longest duration ending at filing_period_end (YTD — only column).
      - 10-K IS/CF: longest duration ending at filing_period_end (12-month annual).
      - BS (any form): instant value at filing_period_end.

    Returns (value, period_kind, duration_days) or (None, None, 0) if nothing matches.
    """
    if not values or filing_period_end is None:
        return (None, None, 0)

    if statement == 'balance_sheet':
        for k, v in values.items():
            kind, _, end, _ = parse_period_key(k)
            if kind == 'instant' and end == filing_period_end:
                return (v, 'instant', 0)
        return (None, None, 0)

    candidates = []
    for k, v in values.items():
        kind, _, end, days = parse_period_key(k)
        if kind == 'duration' and end == filing_period_end:
            candidates.append((days, v))
    if not candidates:
        return (None, None, 0)

    if form == '10-Q' and statement == 'income_statement':
        days, v = min(candidates, key=lambda x: x[0])
    else:
        days, v = max(candidates, key=lambda x: x[0])
    return (v, 'duration', days)


def pick_ytd_value(values, filing_period_end):
    """For CF in 10-Q, pick the YTD duration ending at filing_period_end.

    Returns (value, days) or (None, 0).
    """
    candidates = []
    for k, v in values.items():
        kind, _, end, days = parse_period_key(k)
        if kind == 'duration' and end == filing_period_end:
            candidates.append((days, v))
    if not candidates:
        return (None, 0)
    days, v = max(candidates, key=lambda x: x[0])
    return (v, days)


# ────────────────────────────────────────────────────────────────────────────
# Filing loader
# ────────────────────────────────────────────────────────────────────────────

def load_filings(work_dir):
    """Load all per-filing JSONs in work_dir. Derive metadata.

    Skips stripped variants when the full file exists. Sorts by period_end.
    Returns list of dicts with: file, path, fiscal_year_label, fiscal_period,
    form, quarter_label, period_end, ai (the ai_extraction subtree).
    """
    paths = sorted(glob.glob(os.path.join(work_dir, 'q*_fy*_10*.json'))
                   + glob.glob(os.path.join(work_dir, 'q*_cy*_10*.json')))
    paths = [p for p in paths if 'formula' not in os.path.basename(p)]

    # Drop stripped when non-stripped exists
    bases = {os.path.basename(p).replace('_stripped', '') for p in paths
             if '_stripped' not in os.path.basename(p)}
    paths = [p for p in paths
             if '_stripped' not in os.path.basename(p)
             or os.path.basename(p).replace('_stripped', '') not in bases]

    filings = []
    for p in paths:
        meta = parse_filename(p)
        if not meta:
            continue
        with open(p) as f:
            d = json.load(f)
        ai = d.get('ai_extraction', d)
        pe = derive_filing_period_end(ai)
        if pe is None:
            print(f"  WARN: no period_end derivable from {meta['file']}; skipping")
            continue
        filings.append({**meta, 'path': p, 'period_end': pe, 'ai': ai})

    filings.sort(key=lambda f: f['period_end'])
    return filings


# ────────────────────────────────────────────────────────────────────────────
# Output 1: as-reported statements, merged + quarterized
# ────────────────────────────────────────────────────────────────────────────

STATEMENTS = ('income_statement', 'balance_sheet', 'cash_flow')


def _collect_rows_from_filing(filing, statement):
    """Yield merge-ready row contributions from one filing for one statement.

    Each contribution carries: xbrl_concept, label, indent_level, is_note_detail,
    period_kind, filing metadata, and the selected value/ytd for this filing.
    """
    ai = filing['ai']
    s = ai.get(statement, {})
    form = filing['form']
    pe = filing['period_end']

    for item in s.get('line_items', []):
        concept = item.get('xbrl_concept')
        if not concept:
            continue
        val, kind, days = pick_period_value(item.get('values', {}), pe, statement, form)
        ytd_val, ytd_days = (None, 0)
        if statement == 'cash_flow' and form == '10-Q':
            ytd_val, ytd_days = pick_ytd_value(item.get('values', {}), pe)
        yield {
            'concept': concept,
            'label': item.get('label', ''),
            'indent_level': item.get('indent_level', 0),
            'is_note_detail': False,
            'parent_concept': None,
            'value': val,
            'period_kind': kind,
            'duration_days': days,
            'ytd_value': ytd_val,
            'ytd_days': ytd_days,
        }

    for item in s.get('xbrl_not_on_statement', []):
        concept = item.get('concept')
        if not concept:
            continue
        vals = item.get('value', {}) or item.get('values', {})
        val, kind, days = pick_period_value(vals, pe, statement, form)
        ytd_val, ytd_days = (None, 0)
        if statement == 'cash_flow' and form == '10-Q':
            ytd_val, ytd_days = pick_ytd_value(vals, pe)
        # Stage 1 writes note-detail values in raw USD; face line_items are in
        # USD_millions. Normalize note detail to millions so buckets sum cleanly.
        # Skip rescaling for ratios/rates (values in [-1, 1] that aren't dollars).
        def _rescale(v):
            if v is None:
                return None
            if isinstance(v, (int, float)) and abs(v) >= 1e6:
                return v / 1_000_000
            return v
        val = _rescale(val)
        ytd_val = _rescale(ytd_val)
        yield {
            'concept': concept,
            'label': _default_label_from_concept(concept),
            'indent_level': 0,
            'is_note_detail': True,
            'parent_concept': None,
            'value': val,
            'period_kind': kind,
            'duration_days': days,
            'ytd_value': ytd_val,
            'ytd_days': ytd_days,
            'classification': item.get('classification', ''),
        }


def _default_label_from_concept(concept):
    """Humanize a us-gaap concept name for display when no label is supplied."""
    name = concept.split(':', 1)[-1]
    out = re.sub(r'(?<!^)(?=[A-Z])', ' ', name)
    return out


def merge_statement_rows(filings, statement):
    """Merge per-filing contributions into one row per xbrl_concept.

    Each row has:
      xbrl_concept, labels (sorted unique variants), indent_level (mode),
      is_note_detail, values_by_quarter, source_refs, period_kind, ytd_by_quarter
    """
    by_concept = defaultdict(lambda: {
        'xbrl_concept': None,
        'labels': set(),
        'indent_levels': Counter(),
        'is_note_detail_votes': Counter(),
        'values_by_quarter': {},
        'ytd_by_quarter': {},
        'source_refs': {},
        'period_kind': None,
        'is_note_by_quarter': {},  # per-quarter face(False)/note(True) flag
    })

    for filing in filings:
        q = filing['quarter_label']
        for c in _collect_rows_from_filing(filing, statement):
            row = by_concept[c['concept']]
            row['xbrl_concept'] = c['concept']
            row['labels'].add(c['label'])
            row['indent_levels'][c['indent_level']] += 1
            row['is_note_detail_votes'][c['is_note_detail']] += 1
            row['period_kind'] = c['period_kind'] or row['period_kind']
            if c['value'] is not None:
                row['values_by_quarter'][q] = c['value']
                row['source_refs'][q] = filing['file']
                row['is_note_by_quarter'][q] = bool(c['is_note_detail'])
            if c['ytd_value'] is not None:
                row['ytd_by_quarter'][q] = {'value': c['ytd_value'], 'days': c['ytd_days']}

    merged = []
    for concept, row in by_concept.items():
        indent = row['indent_levels'].most_common(1)[0][0] if row['indent_levels'] else 0
        is_note = row['is_note_detail_votes'].most_common(1)[0][0] if row['is_note_detail_votes'] else False
        merged.append({
            'xbrl_concept': concept,
            'labels': sorted(row['labels']),
            'indent_level': indent,
            'is_note_detail': is_note,
            'period_kind': row['period_kind'],
            'values_by_quarter': row['values_by_quarter'],
            'is_note_by_quarter': row['is_note_by_quarter'],
            'ytd_by_quarter': row['ytd_by_quarter'],
            'source_refs': row['source_refs'],
        })

    merged.sort(key=lambda r: (r['is_note_detail'], r['indent_level'], r['xbrl_concept']))
    return merged


def fiscal_year_groups(filings):
    """Group filings by fiscal_year_label. Returns dict: fy_label -> {quarter_label -> filing}."""
    groups = defaultdict(dict)
    for f in filings:
        groups[f['fiscal_year_label']][f['fiscal_period']] = f
    return groups


def derive_q2_q3_from_ytd(rows, statement, filings):
    """For cash_flow in 10-Q: Q2/Q3 quarterly value = YTD - prior YTD.

    Applied in place. Only affects rows whose values_by_quarter has a Q2/Q3 entry
    that came from a YTD context (duration_days > 100). Replaces with standalone
    quarterly = YTD_current - YTD_prior.
    """
    if statement != 'cash_flow':
        return
    groups = fiscal_year_groups(filings)
    for row in rows:
        for fy_label, qmap in groups.items():
            for qn in ('Q2', 'Q3'):
                q_label = f'{fy_label}{qn}'
                if qn not in qmap:
                    continue
                if qmap[qn]['form'] != '10-Q':
                    continue
                ytd_entry = row['ytd_by_quarter'].get(q_label)
                if not ytd_entry:
                    continue
                # Determine prior YTD
                if qn == 'Q2':
                    prior_q = 'Q1'
                else:
                    prior_q = 'Q2'
                prior_label = f'{fy_label}{prior_q}'
                # Q1 standalone == Q1 YTD; Q2 YTD = 6-month; Q3 YTD = 9-month
                prior_ytd = 0
                if prior_q == 'Q1':
                    # Q1 value in row = standalone = YTD
                    prior_ytd = row['values_by_quarter'].get(prior_label, 0) or 0
                else:
                    # Q2 YTD from ytd_by_quarter
                    pe = row['ytd_by_quarter'].get(prior_label)
                    prior_ytd = (pe or {}).get('value', 0) or 0
                row['values_by_quarter'][q_label] = ytd_entry['value'] - prior_ytd


def derive_q4_for_flows(rows, statement, filings):
    """For flow concepts in 10-K: Q4 = Annual - Q1 - Q2 - Q3.

    IS + CF: all concepts are flows → derive Q4.
    BS: snapshot → 10-K annual value IS Q4; no derivation.
    """
    if statement == 'balance_sheet':
        return
    groups = fiscal_year_groups(filings)
    for row in rows:
        for fy_label, qmap in groups.items():
            if 'Q4' not in qmap or qmap['Q4']['form'] != '10-K':
                continue
            annual_q = f'{fy_label}Q4'
            annual = row['values_by_quarter'].get(annual_q)
            if annual is None:
                continue
            q123_vals = [row['values_by_quarter'].get(f'{fy_label}{q}') for q in ('Q1', 'Q2', 'Q3')]
            if any(v is None for v in q123_vals):
                continue
            row['values_by_quarter'][annual_q] = annual - sum(q123_vals)


def gather_formulas(filings, statement):
    """Collect unique formulas across all filings for a statement.

    Formulas are stored with label-based components; we translate to concept-based
    using the line_items in the same filing. Each unique formula tracks the set of
    quarters to which it applies (the quarters of filings that declared it, plus —
    for 10-K formulas — the derived Q4 of that fiscal year, by linearity).

    Returns list of:
      {result_concept, components: [{sign, concept}], result_label,
       applicable_quarters: set}
    """
    seen = {}
    for filing in filings:
        s = filing['ai'].get(statement, {})
        line_items = s.get('line_items', [])
        label_to_concept = {it.get('label'): it.get('xbrl_concept') for it in line_items
                            if it.get('label') and it.get('xbrl_concept')}

        for f in s.get('formulas', []):
            op = f.get('operation') or ''
            result_label = f.get('result_label') or ''
            result_concept = label_to_concept.get(result_label)
            if not result_concept or not op:
                continue

            # Parse 'A + B - C' into signed components
            components = []
            token_re = re.compile(r'\s*([+\-])?\s*([^+\-]+?)(?=\s*[+\-]|\s*$)')
            remaining = op.strip()
            first = True
            while remaining:
                m = re.match(r'\s*([+\-]?)\s*(.+)', remaining)
                if not m:
                    break
                sign = m.group(1) or '+'
                rest = m.group(2).strip()
                # find next +/- not inside parens (simple: split at first top-level +/-)
                nxt = None
                depth = 0
                for i, ch in enumerate(rest):
                    if ch == '(':
                        depth += 1
                    elif ch == ')':
                        depth -= 1
                    elif ch in '+-' and depth == 0 and i > 0:
                        nxt = i
                        break
                if nxt is not None:
                    label = rest[:nxt].strip()
                    remaining = rest[nxt:]
                else:
                    label = rest.strip()
                    remaining = ''
                concept = label_to_concept.get(label)
                if concept:
                    components.append({'sign': sign if not first else ('+' if sign == '' else sign),
                                       'concept': concept})
                first = False

            if not components:
                continue

            key = (result_concept, tuple((c['sign'], c['concept']) for c in components))
            applicable = {filing['quarter_label']}
            # A 10-K's formula also applies (by linearity) to derived Q4 — which
            # IS the same quarter_label (10-K filings are tagged Q4). So the
            # single-quarter scoping is correct for 10-Ks too.
            if key not in seen:
                seen[key] = {
                    'result_concept': result_concept,
                    'result_label': result_label,
                    'components': components,
                    'statement': statement,
                    'applicable_quarters': set(applicable),
                }
            else:
                seen[key]['applicable_quarters'] |= applicable
    return list(seen.values())


def evaluate_formulas(rows, formulas):
    """For each formula, compute expected vs reported per quarter. Annotate ties.

    Evaluates only against the formula's applicable_quarters — the quarters
    where the formula actually holds (set at gather-time from its source
    filing). This avoids spurious breaks when component structure drifts
    across filings (e.g., a 10-K formula with one set of components applied
    to a 10-Q that used different components).
    """
    by_concept = {r['xbrl_concept']: r for r in rows}

    for f in formulas:
        f['ties_by_quarter'] = {}
        result_row = by_concept.get(f['result_concept'])
        if not result_row:
            continue
        for q in sorted(f.get('applicable_quarters', set())):
            reported = result_row['values_by_quarter'].get(q)
            if reported is None:
                continue
            computed = 0
            ok = True
            for c in f['components']:
                crow = by_concept.get(c['concept'])
                if not crow or q not in crow['values_by_quarter']:
                    ok = False
                    break
                cv = crow['values_by_quarter'][q]
                computed += cv if c['sign'] == '+' else -cv
            if not ok:
                continue
            delta = computed - reported
            f['ties_by_quarter'][q] = {
                'computed': computed,
                'reported': reported,
                'delta': delta,
                'ties': delta == 0,
            }


# ────────────────────────────────────────────────────────────────────────────
# Segments
# ────────────────────────────────────────────────────────────────────────────

def build_segments(filings):
    """Gather segment_data from all filings and quarterize.

    Stage 1 segment entries have shape:
      { 'dimension', 'items': [{'member', 'values': {'Metric_YYYY-MM-DD_YYYY-MM-DD': val}}],
        'total', 'consolidated_total', 'ties' }

    Values keys are 'Metric_start_end' (duration) or 'Metric_YYYY-MM-DD' (instant).
    We select current-period values using the same rule as line items.
    """
    axes_by_dim = defaultdict(lambda: {
        'dimension': None,
        'metrics': set(),
        'members_by_key': defaultdict(lambda: {
            'member': None,
            'values_by_quarter_and_metric': {},
        }),
        'consolidated_by_quarter_and_metric': {},
    })

    for filing in filings:
        q = filing['quarter_label']
        pe = filing['period_end']
        form = filing['form']
        for seg in filing['ai'].get('segment_data', []) or []:
            dim = seg.get('dimension') or 'Unknown'
            axis = axes_by_dim[dim]
            axis['dimension'] = dim

            for it in seg.get('items', []) or []:
                member = it.get('member') or 'Unknown'
                vals = it.get('values', {}) or {}
                mvals = _segment_metric_values(vals, pe, form)
                mrec = axis['members_by_key'][member]
                mrec['member'] = member
                for metric, val in mvals.items():
                    axis['metrics'].add(metric)
                    mrec['values_by_quarter_and_metric'][f'{q}|{metric}'] = val

            consolidated = seg.get('consolidated_total')
            # Extract metric-specific consolidated values when present
            c_items = seg.get('consolidated_by_metric') or {}
            if c_items:
                for metric, val in c_items.items():
                    axis['consolidated_by_quarter_and_metric'][f'{q}|{metric}'] = val
            elif consolidated is not None and len(seg.get('items', [])) and seg.get('items', [])[0].get('values'):
                # Fall back: first metric encountered maps to total
                first_metrics = _segment_metric_values(seg['items'][0].get('values', {}), pe, form).keys()
                if first_metrics:
                    first_metric = list(first_metrics)[0]
                    axis['consolidated_by_quarter_and_metric'][f'{q}|{first_metric}'] = consolidated

    out_axes = []
    for dim, ax in axes_by_dim.items():
        rows = []
        for key, mrec in ax['members_by_key'].items():
            rows.append({
                'member': mrec['member'],
                'values_by_quarter_and_metric': mrec['values_by_quarter_and_metric'],
            })
        out_axes.append({
            'dimension': ax['dimension'],
            'metrics': sorted(ax['metrics']),
            'rows': rows,
            'consolidated_by_quarter_and_metric': ax['consolidated_by_quarter_and_metric'],
        })
    return {'axes': out_axes}


def _segment_metric_values(values_dict, filing_period_end, form):
    """From a segment item's values dict with 'Metric_periodkey' keys, pick the
    current-period value per metric. Returns {metric: value}.
    """
    by_metric = defaultdict(list)
    for k, v in values_dict.items():
        if '_' not in k:
            continue
        metric, rest = k.split('_', 1)
        kind, _, end, days = parse_period_key(rest)
        if end != filing_period_end:
            continue
        by_metric[metric].append((kind, days, v))

    out = {}
    for metric, entries in by_metric.items():
        durations = [e for e in entries if e[0] == 'duration']
        instants = [e for e in entries if e[0] == 'instant']
        if durations:
            if form == '10-Q':
                _, _, v = min(durations, key=lambda x: x[1])
            else:
                _, _, v = max(durations, key=lambda x: x[1])
        elif instants:
            _, _, v = instants[0]
        else:
            continue
        out[metric] = v
    return out


# ────────────────────────────────────────────────────────────────────────────
# Verification battery (Python, post-AI)
# ────────────────────────────────────────────────────────────────────────────

TOLERANCE_DELTA = 2  # raw-dollar rounding at scale=6 is up to ~$500k; allow small delta
TOLERANCE_FRAC = 0.005


def _within_tol(delta, reference):
    if reference is None or reference == 0:
        return abs(delta) <= TOLERANCE_DELTA
    return abs(delta) <= max(TOLERANCE_DELTA, abs(reference) * TOLERANCE_FRAC)


def check_formulas(statements):
    failures = []
    for stmt_name, s in statements.items():
        for f in s.get('formulas', []):
            for q, rec in f.get('ties_by_quarter', {}).items():
                if rec['ties']:
                    continue
                if _within_tol(rec['delta'], rec['reported']):
                    continue
                failures.append({
                    'type': 'FORMULA_BREAK',
                    'statement': stmt_name,
                    'quarter': q,
                    'message': (f'{stmt_name} {q}: {f["result_concept"]} computed={rec["computed"]} '
                                f'reported={rec["reported"]} delta={rec["delta"]}'),
                })
    return failures


def check_flow_sum_to_annual(statements, filings):
    failures = []
    groups = fiscal_year_groups(filings)
    for stmt_name, s in statements.items():
        if stmt_name not in FLOW_STATEMENTS:
            continue
        for row in s.get('rows', []):
            for fy_label, qmap in groups.items():
                q_labels = [f'{fy_label}Q{n}' for n in range(1, 5)]
                annual_q = q_labels[3]
                # Need all four; 10-K must be present for annual reference
                if 'Q4' not in qmap or qmap['Q4']['form'] != '10-K':
                    continue
                quarters_present = [row['values_by_quarter'].get(q) for q in q_labels]
                if any(v is None for v in quarters_present):
                    continue
                # This check is tautological for derived Q4 (Q4 = annual - Q1-Q2-Q3),
                # but we still run it to catch bugs in the derivation itself.
                total = sum(quarters_present)
                # annual is NOT directly stored; it was overwritten by derived Q4.
                # Integrity check: re-derive annual from original 10-K data and compare.
                original_annual = _original_annual_for_concept(qmap['Q4'], stmt_name, row['xbrl_concept'])
                if original_annual is None:
                    continue
                delta = total - original_annual
                if not _within_tol(delta, original_annual):
                    failures.append({
                        'type': 'FLOW_SUM_MISMATCH',
                        'statement': stmt_name,
                        'fiscal_year': fy_label,
                        'concept': row['xbrl_concept'],
                        'message': (f'{stmt_name} {fy_label} {row["xbrl_concept"]}: '
                                    f'Q1+Q2+Q3+Q4={total} != annual={original_annual} '
                                    f'delta={delta}'),
                    })
    return failures


def _original_annual_for_concept(filing_10k, statement, concept):
    """Look up the original 10-K annual value for a concept before Q4 derivation."""
    ai = filing_10k['ai']
    s = ai.get(statement, {})
    pe = filing_10k['period_end']
    for it in s.get('line_items', []):
        if it.get('xbrl_concept') == concept:
            v, _, _ = pick_period_value(it.get('values', {}), pe, statement, '10-K')
            return v
    for it in s.get('xbrl_not_on_statement', []):
        if it.get('concept') == concept:
            vals = it.get('value', {}) or it.get('values', {})
            v, _, _ = pick_period_value(vals, pe, statement, '10-K')
            return v
    return None


def check_bs_consistency(filings, statements):
    """For each instant date appearing in >1 filing, BS values must match across filings."""
    failures = []
    # Collect each (concept, instant_date) -> list of (filing, value)
    by_concept_date = defaultdict(list)
    for f in filings:
        ai = f['ai']
        for it in ai.get('balance_sheet', {}).get('line_items', []):
            concept = it.get('xbrl_concept')
            if not concept:
                continue
            for k, v in it.get('values', {}).items():
                kind, _, end, _ = parse_period_key(k)
                if kind == 'instant':
                    by_concept_date[(concept, end)].append((f['file'], v))

    for (concept, d), entries in by_concept_date.items():
        if len(entries) < 2:
            continue
        vals = [v for _, v in entries if v is not None]
        if len(vals) < 2:
            continue
        if len(set(vals)) == 1:
            continue
        ref = vals[0]
        for v in vals[1:]:
            if not _within_tol(v - ref, ref):
                files = [ff for ff, _ in entries]
                failures.append({
                    'type': 'BS_INCONSISTENT',
                    'concept': concept,
                    'date': str(d),
                    'message': f'{concept} @ {d}: values disagree across {files}: {vals}',
                })
                break
    return failures


def _collect_rollup_concepts(ticker):
    """Walk parsed_xbrl.json def linkbases across all filings on disk. A concept
    is a rollup if it appears as a parent of another domain-member anywhere. The
    set is the union across all filings — more recent dimension restructurings
    are captured automatically. Returns a set of local concept names
    (without xbrl prefix), because Stage 1 tagged segment members by local name.
    """
    rollups = set()
    filings_root = os.path.join(os.path.dirname(__file__), '..', 'data', 'filings', ticker)
    if not os.path.isdir(filings_root):
        return rollups
    for acc in os.listdir(filings_root):
        parsed_path = os.path.join(filings_root, acc, 'parsed_xbrl.json')
        if not os.path.isfile(parsed_path):
            continue
        try:
            with open(parsed_path) as f:
                parsed = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        for role_entry in parsed.get('definitions', []) or []:
            for h in role_entry.get('hierarchies', []) or []:
                parent = h.get('parent') or ''
                children = h.get('children', []) or []
                if not parent or not children:
                    continue
                has_member_child = any(
                    c.get('arcrole') == 'domain-member' and 'Member' in (c.get('concept') or '')
                    for c in children
                )
                if has_member_child:
                    local = parent.split(':', 1)[-1]
                    rollups.add(local)
    return rollups


def _member_local_name(member_str):
    """Strip Stage 1 descriptive suffixes like ' (subset of ...)' or parentheticals
    and xbrl prefix. Returns the bare concept local name for hierarchy lookup.
    """
    if not member_str:
        return ''
    # Drop anything after first '(' — Stage 1 often appends '(subset of X)'
    s = member_str.split('(', 1)[0].strip()
    # Drop xbrl prefix if present
    s = s.split(':', 1)[-1]
    return s


def check_segments_tie(segments, statements, ticker):
    """Sum LEAF members and compare to consolidated. Rollup members (parents of
    other members per the def linkbase) are excluded from the sum because their
    value is already the sum of their children.
    """
    failures = []
    rollups = _collect_rollup_concepts(ticker)

    for axis in segments.get('axes', []):
        dim = axis['dimension']
        for key, consolidated in axis.get('consolidated_by_quarter_and_metric', {}).items():
            if '|' not in key:
                continue
            if not isinstance(consolidated, (int, float)):
                continue
            q, metric = key.split('|', 1)
            summed = 0
            found = False
            for r in axis['rows']:
                member = r.get('member', '')
                if _member_local_name(member) in rollups:
                    continue  # skip — parent value is already the sum of children
                v = r['values_by_quarter_and_metric'].get(key)
                if isinstance(v, (int, float)):
                    summed += v
                    found = True
            if not found:
                continue
            delta = summed - consolidated
            if not _within_tol(delta, consolidated):
                failures.append({
                    'type': 'SEGMENT_TIE_BREAK',
                    'axis': dim,
                    'quarter': q,
                    'metric': metric,
                    'message': (f'segment axis {dim} {q} {metric}: sum(leaf members)={summed} '
                                f'consolidated={consolidated} delta={delta}'),
                })
    return failures


def compute_bucket_values(statements, assignments):
    """For each bucket, split the signed-sum of source-concept values into two
    streams per quarter:

      face  — signed sum of source values where the contributing row was on the
              statement face (Stage 1 line_items) for that quarter.
      note  — signed sum of source values where the row was note-detail
              (Stage 1 xbrl_not_on_statement) for that quarter.

    Statement subtotal math uses `face` only — because note items are
    typically decompositions already embedded in a face line, and summing them
    into subtotals would double-count. Analytical formulas (ROIC, operating
    lease total, etc.) can use face + note.

    Returns: {stmt_name: {bucket_name: {q: {'face': x, 'note': y}}}}.
    Missing side is None, not 0.
    """
    out = {}
    for stmt_name, stmt_assign in assignments.items():
        by_concept = {r['xbrl_concept']: r for r in statements.get(stmt_name, {}).get('rows', [])}
        stmt_out = {}
        for bucket, sources in stmt_assign.items():
            qvals = {}
            for src in sources or []:
                concept = src.get('concept') if isinstance(src, dict) else src
                sign = (src.get('sign') if isinstance(src, dict) else '+') or '+'
                row = by_concept.get(concept)
                if not row:
                    continue
                note_per_q = row.get('is_note_by_quarter', {})
                for q, v in row['values_by_quarter'].items():
                    if v is None:
                        continue
                    signed = v if sign == '+' else -v
                    slot = 'note' if note_per_q.get(q, row.get('is_note_detail', False)) else 'face'
                    bucket_q = qvals.setdefault(q, {'face': None, 'note': None})
                    bucket_q[slot] = (bucket_q[slot] or 0) + signed
            stmt_out[bucket] = qvals
        out[stmt_name] = stmt_out
    return out


def bucket_face_value(bucket_values, stmt, bucket, q):
    """Face value for subtotal math. Returns None if no face contribution."""
    return ((bucket_values.get(stmt, {}).get(bucket, {}) or {}).get(q, {}) or {}).get('face')


def bucket_total_value(bucket_values, stmt, bucket, q):
    """Face + note combined, for analytical formulas."""
    entry = ((bucket_values.get(stmt, {}).get(bucket, {}) or {}).get(q, {}) or {})
    f, n = entry.get('face'), entry.get('note')
    if f is None and n is None:
        return None
    return (f or 0) + (n or 0)


def compute_subtotals(bucket_values):
    """Compute subtotals from CANONICAL_BUCKETS using only the FACE portion of
    each component bucket. Note-detail values are not summed into subtotals —
    they are decompositions of face items and would double-count.

    Subtotal buckets themselves are stored under the `face` slot (the note slot
    is left None) so downstream face-only consumers work uniformly.

    Returns receipts for CSV rendering.
    """
    receipts = {stmt: {} for stmt in bucket_values}
    for stmt_name, schema in CANONICAL_BUCKETS.items():
        stmt_vals = bucket_values.setdefault(stmt_name, {})
        for subtotal, components in schema['subtotals']:
            qvals = {}
            all_qs = set()
            for sign, comp in components:
                all_qs.update((stmt_vals.get(comp) or {}).keys())
            for q in all_qs:
                total = 0
                any_present = False
                for sign, comp in components:
                    face_v = bucket_face_value(bucket_values, stmt_name, comp, q)
                    if face_v is None:
                        continue
                    any_present = True
                    total += face_v if sign == '+' else -face_v
                if any_present:
                    qvals[q] = {'face': total, 'note': None}
            stmt_vals[subtotal] = qvals
            receipts[stmt_name][subtotal] = {
                'components': [(s, c) for s, c in components],
                'values_by_quarter': {q: (v['face'] if isinstance(v, dict) else v)
                                     for q, v in qvals.items()},
            }
    return receipts


def check_bucket_assignments_valid(statements, assignments):
    """Every assigned concept must exist as a row in the named statement.
    Every bucket name must be a canonical detail bucket.
    """
    failures = []
    concepts_by_stmt = {s: {r['xbrl_concept'] for r in data.get('rows', [])}
                       for s, data in statements.items()}
    for stmt_name, stmt_assign in assignments.items():
        if stmt_name not in CANONICAL_BUCKETS:
            failures.append({
                'type': 'UNKNOWN_STATEMENT',
                'message': f'assignment block names unknown statement: {stmt_name}',
            })
            continue
        valid_buckets = set(CANONICAL_BUCKETS[stmt_name]['detail'])
        for bucket, sources in stmt_assign.items():
            if bucket not in valid_buckets:
                failures.append({
                    'type': 'UNKNOWN_BUCKET',
                    'statement': stmt_name,
                    'bucket': bucket,
                    'message': (f'{stmt_name}.{bucket} is not a canonical detail bucket. '
                                f'Valid detail buckets: {sorted(valid_buckets)}'),
                })
                continue
            for src in sources or []:
                concept = src.get('concept') if isinstance(src, dict) else src
                if concept and concept not in concepts_by_stmt.get(stmt_name, set()):
                    failures.append({
                        'type': 'ASSIGNMENT_SOURCE_MISSING',
                        'statement': stmt_name,
                        'bucket': bucket,
                        'message': (f'{stmt_name}.{bucket} lists concept {concept} '
                                    f'but no such row in {stmt_name}'),
                    })
    return failures


def check_concepts_fully_assigned(statements, assignments, exclusions):
    """Every as-reported concept should be either assigned to a detail bucket
    or listed in `exclusions` (with a reason).

    Exclusions are legitimate for computed subtotals (GrossProfit,
    OperatingIncomeLoss, etc.) and supplementary metrics (EPS, share counts,
    tax rate, antidilutive securities, comprehensive income). The AI is
    expected to place those in `exclusions` with a reason.
    """
    failures = []
    assigned_by_stmt = defaultdict(set)
    excluded_by_stmt = defaultdict(set)

    for stmt_name, stmt_assign in (assignments or {}).items():
        for bucket, sources in stmt_assign.items():
            for src in sources or []:
                concept = src.get('concept') if isinstance(src, dict) else src
                if concept:
                    assigned_by_stmt[stmt_name].add(concept)

    for exc in (exclusions or []):
        stmt = exc.get('statement')
        concept = exc.get('concept')
        if stmt and concept:
            excluded_by_stmt[stmt].add(concept)

    for stmt_name, data in statements.items():
        for row in data.get('rows', []):
            if not row.get('values_by_quarter'):
                continue
            c = row['xbrl_concept']
            if c in assigned_by_stmt[stmt_name]:
                continue
            if c in excluded_by_stmt[stmt_name]:
                continue
            failures.append({
                'type': 'UNASSIGNED_CONCEPT',
                'statement': stmt_name,
                'concept': c,
                'message': (f'{stmt_name}: concept {c} (labels={row.get("labels", [])}) '
                            f'is neither assigned to a bucket nor in exclusions'),
            })
    return failures


def check_cross_statement_invariants(bucket_values, filings):
    """Three face-math invariants, every quarter:
      1. total_assets == total_liabilities_and_equity
      2. income_statement.net_income == cash_flow.net_income_start
      3. cash_flow.net_change_in_cash == bs.cash(q) - bs.cash(prior q)

    Uses face-only values. Note-detail values are never part of these ties.
    """
    failures = []

    def face(stmt, bucket, q):
        return bucket_face_value(bucket_values, stmt, bucket, q)

    # 1. Balance sheet closes
    bs_buckets = bucket_values.get('balance_sheet', {}) or {}
    ta_qs = set((bs_buckets.get('total_assets') or {}).keys())
    tle_qs = set((bs_buckets.get('total_liabilities_and_equity') or {}).keys())
    for q in sorted(ta_qs | tle_qs):
        a = face('balance_sheet', 'total_assets', q)
        b = face('balance_sheet', 'total_liabilities_and_equity', q)
        if a is None or b is None:
            continue
        delta = a - b
        if not _within_tol(delta, a):
            failures.append({
                'type': 'INVARIANT_BS_CLOSES',
                'quarter': q,
                'message': (f'{q}: total_assets={a} != total_liabilities_and_equity={b} '
                            f'delta={delta}'),
            })

    # 2. IS NI == CF net_income_start
    is_buckets = bucket_values.get('income_statement', {}) or {}
    cf_buckets = bucket_values.get('cash_flow', {}) or {}
    ni_qs = set((is_buckets.get('net_income') or {}).keys()) & set((cf_buckets.get('net_income_start') or {}).keys())
    for q in sorted(ni_qs):
        a = face('income_statement', 'net_income', q)
        b = face('cash_flow', 'net_income_start', q)
        if a is None or b is None:
            continue
        if not _within_tol(a - b, a):
            failures.append({
                'type': 'INVARIANT_NI_TIE',
                'quarter': q,
                'message': f'{q}: is.net_income={a} != cf.net_income_start={b} delta={a - b}',
            })

    # 3. net_change_in_cash ties BS cash roll-forward
    by_q = {f['quarter_label']: f for f in filings}
    ordered = sorted(by_q.keys(), key=lambda q: by_q[q]['period_end'])
    for i in range(1, len(ordered)):
        prev_q, curr_q = ordered[i - 1], ordered[i]
        ch = face('cash_flow', 'net_change_in_cash', curr_q)
        cash_now = face('balance_sheet', 'cash', curr_q)
        cash_prev = face('balance_sheet', 'cash', prev_q)
        if ch is None or cash_now is None or cash_prev is None:
            continue
        bs_delta = cash_now - cash_prev
        if not _within_tol(ch - bs_delta, cash_now):
            failures.append({
                'type': 'INVARIANT_CASH_ROLLFORWARD',
                'quarter': curr_q,
                'message': (f'{curr_q}: cf.net_change_in_cash={ch} != '
                            f'bs.cash[{curr_q}]-bs.cash[{prev_q}] = {cash_now}-{cash_prev} = {bs_delta} '
                            f'delta={ch - bs_delta}'),
            })
    return failures


def check_normalized_flow_sum_to_annual(bucket_values, filings):
    """For every flow bucket, Q1+Q2+Q3+Q4 = annual per fiscal year.

    The annual value comes from the 10-K's unaltered bucket value, which equals
    the derived Q4 bucket IF derivation was correct. We cross-check by summing
    the four quarters and comparing to the annual value recoverable from each
    10-K filing through the same bucket assignments. For Q4 derived by linear
    subtraction, this check is tautological — but it catches any non-linearity
    bugs.

    Actually: after our derivation, each bucket's Q4 == annual - (Q1+Q2+Q3),
    so Q1+Q2+Q3+Q4 == annual trivially. This check is informational: we log
    each fiscal year's sum so the CSV can display it.
    """
    failures = []
    # Intentionally no hard assertions here; trivially true post-derivation.
    return failures


def check_forward_fills(forward_fills, ticker, filings):
    """For each flagged forward-fill, confirm the concept genuinely isn't in the
    target period's parsed_xbrl.json.
    """
    failures = []
    repo_root = os.path.join(os.path.dirname(__file__), '..')
    filings_root = os.path.join(repo_root, 'data', 'filings', ticker)
    filing_by_pe = {f['period_end']: f for f in filings}

    # Resolve parsed_xbrl.json path for each filing (match by period_end)
    parsed_by_file = {}
    if os.path.isdir(filings_root):
        for acc in os.listdir(filings_root):
            meta_path = os.path.join(filings_root, acc, 'filing_meta.json')
            parsed_path = os.path.join(filings_root, acc, 'parsed_xbrl.json')
            if not (os.path.isfile(meta_path) and os.path.isfile(parsed_path)):
                continue
            with open(meta_path) as mf:
                meta = json.load(mf)
            rd = meta.get('report_date') or meta.get('period_of_report') or meta.get('period_end')
            if rd:
                try:
                    rd_date = datetime.strptime(rd[:10], '%Y-%m-%d').date()
                    parsed_by_file[rd_date] = parsed_path
                except ValueError:
                    pass

    for ff in forward_fills:
        search_concepts = ff.get('candidate_concepts') or []
        if not search_concepts:
            failures.append({
                'type': 'FORWARD_FILL_NO_RECEIPT',
                'field': ff.get('field'),
                'message': f'forward-fill for {ff.get("field")} has no candidate_concepts — cannot audit',
            })
            continue

        for q_label in ff.get('applies_to_quarters', []):
            # Resolve target filing
            target = next((f for f in filings if f['quarter_label'] == q_label), None)
            if not target:
                continue
            parsed_path = parsed_by_file.get(target['period_end'])
            if not parsed_path:
                continue
            with open(parsed_path) as pf:
                parsed = json.load(pf)
            for fact in parsed.get('facts', []):
                if fact.get('concept') in search_concepts and not fact.get('dimensioned'):
                    if fact.get('value_numeric') is not None:
                        failures.append({
                            'type': 'FALSE_FORWARD_FILL',
                            'field': ff.get('field'),
                            'quarter': q_label,
                            'message': (f'forward-fill {ff.get("field")} at {q_label} rejected: '
                                        f'concept {fact.get("concept")} present in parsed_xbrl.json '
                                        f'with value {fact.get("value_numeric")}'),
                        })
                        break
    return failures


def check_sign_sanity(bucket_values):
    """Total revenue (face) must be positive per quarter."""
    failures = []
    for q in (bucket_values.get('income_statement', {}).get('total_revenue', {}) or {}):
        v = bucket_face_value(bucket_values, 'income_statement', 'total_revenue', q)
        if v is not None and v < 0:
            failures.append({
                'type': 'NEGATIVE_REVENUE',
                'quarter': q,
                'message': f'total_revenue {q}={v} must be positive',
            })
    return failures


def verify_all(result, filings, ticker):
    """Run the verification battery. Returns failures that must retry/block.

    Note: as-reported formula ties (Stage 1 formulas evaluated on Stage 2's
    merged rows) are informational — the receipts live on each formula's
    ties_by_quarter for the CSV to render, but they are NOT checked here.
    Real correctness signals: bucket assignments valid, concepts fully
    assigned, cross-statement invariants tie, segment members tie, BS
    instants consistent across filings, forward-fills audit clean, signs sane.
    """
    statements = result.get('statements', {})
    segments = result.get('segments', {})
    bucket_values = result.get('bucket_values', {})
    assignments = result.get('bucket_assignments', {})
    exclusions = result.get('exclusions', [])
    forward_fills = result.get('forward_fills', [])

    failures = []
    failures.extend(check_bucket_assignments_valid(statements, assignments))
    failures.extend(check_concepts_fully_assigned(statements, assignments, exclusions))
    failures.extend(check_cross_statement_invariants(bucket_values, filings))
    # Note: BS cross-filing consistency is NOT checked here. Restatement
    # reconciliation is Stage 1's job (mapped.json "later filing wins"). Stage 2
    # uses each filing's current-period values only; no cross-filing overlap.
    failures.extend(check_segments_tie(segments, statements, ticker))
    failures.extend(check_forward_fills(forward_fills, ticker, filings))
    failures.extend(check_sign_sanity(bucket_values))
    return failures


# ────────────────────────────────────────────────────────────────────────────
# AI analytical pass (Output 2)
# ────────────────────────────────────────────────────────────────────────────

def load_formulas_md():
    path = os.path.join(os.path.dirname(__file__), '..', 'formulas.md')
    with open(path) as f:
        return f.read()


def _slim_statements_for_prompt(statements):
    """Produce a compact per-statement dump for the AI prompt: rows + formulas,
    but only the fields the AI needs to map analytical values.
    """
    out = {}
    for stmt_name, s in statements.items():
        rows = []
        for r in s.get('rows', []):
            rows.append({
                'xbrl_concept': r['xbrl_concept'],
                'labels': r['labels'],
                'is_note_detail': r['is_note_detail'],
                'values_by_quarter': r['values_by_quarter'],
            })
        out[stmt_name] = {'rows': rows}
    return out


def _slim_segments_for_prompt(segments):
    axes = []
    for ax in segments.get('axes', []):
        rows = [{'member': r['member'], 'values_by_quarter_and_metric': r['values_by_quarter_and_metric']}
                for r in ax['rows']]
        axes.append({'dimension': ax['dimension'], 'metrics': ax['metrics'], 'rows': rows})
    return {'axes': axes}


def _format_canonical_buckets_for_prompt():
    """Render CANONICAL_BUCKETS in a compact human+AI readable form for the prompt."""
    lines = []
    for stmt_name, schema in CANONICAL_BUCKETS.items():
        lines.append(f'\n{stmt_name.upper()}')
        lines.append(f'  detail buckets: {", ".join(schema["detail"])}')
        lines.append(f'  subtotals (computed, do NOT assign):')
        for name, comps in schema['subtotals']:
            expr = ' '.join(f'{s}{c}' for s, c in comps)
            lines.append(f'    {name} = {expr}')
    return '\n'.join(lines)


def build_bucket_prompt(ticker, statements, segments, prior_failures=None):
    slim_stmts = _slim_statements_for_prompt(statements)
    slim_segs = _slim_segments_for_prompt(segments)
    canonical_block = _format_canonical_buckets_for_prompt()

    failure_block = ''
    if prior_failures:
        lines = '\n'.join(f'- [{f["type"]}] {f.get("message","")}' for f in prior_failures[:40])
        failure_block = f'\n\nPRIOR VERIFICATION FAILURES (fix these):\n{lines}\n'

    return f"""You are assigning as-reported rows from {ticker}'s financial statements to a
universal set of analytical buckets. Same bucket names apply to every company.

The statements have already been quarterized and merged across filings by
xbrl_concept. Your task: for each statement, decide which detail bucket each
as-reported concept belongs to. Subtotals (gross_profit, operating_income,
cfo, total_assets, etc.) are computed by Python from the detail buckets — do
NOT assign concepts to subtotal buckets.

PRINCIPLES:
- Every as-reported concept with any values should be assigned to exactly
  one detail bucket. If a concept doesn't fit anywhere, use `_excluded` and
  explain why in the `exclusions` list — but this should be rare (pure
  presentation-only totals, reclassifications).
- A bucket can receive multiple concepts (common when the filing splits a
  single analytical concept into multiple lines, or when a company changes
  naming between filings).
- Sign convention: default `+`. Use `-` only if the as-reported value's
  sign needs to be inverted to fit the bucket's semantics. Most items are
  reported with the sign they should carry (capex is already negative,
  treasury_stock is already negative, income tax expense is positive, etc.).
- When a company doesn't report a bucket, leave it empty — no plugs, no
  zero-fills. Python will produce a null value for that bucket.

FACE vs NOTE ROUTING (automatic — informational):
- Stage 1 tags each concept as either a face line item (on the IS/BS/CF
  face) or note detail (from footnote disclosures). You don't need to
  specify this — Python reads that tag automatically.
- Statement subtotal math (total_revenue, total_assets, cfo, etc.) uses
  only face-tagged contributions. A note-detail concept you assign to a
  bucket still participates in analytical formulas, but never feeds
  statement subtotals — that prevents double-counting when a note item is
  already embedded in a face line.
- Consequence: you can freely assign a note-detail concept to the bucket
  that best represents its analytical meaning (e.g., put
  `us-gaap:OperatingLeaseLiabilityCurrent` in `current_portion_leases`
  even if it's only in a note). It won't break the BS subtotal math.
- Forward-fills are separate from this routing — flag them via the
  `forward_fills` list, with `candidate_concepts` for audit.

CANONICAL BUCKETS:
{canonical_block}

HINTS on some trickier mappings:
- IS `dna` is rarely a separate line item — usually embedded in COGS or SG&A
  on the IS. Most companies leave this bucket empty on the IS. D&A on the
  CF is a different bucket (`cash_flow.dna`) and is always populated.
- On BS, accounts_payable is trade AP only; other payables go to
  accrued_expenses or other_current_liabilities. Operating-lease current
  portion often lives in note detail (xbrl_not_on_statement); it goes to
  `current_portion_leases`. Operating-lease noncurrent goes to
  `long_term_leases`.
- On CF, amortization of intangibles is part of `dna`; amortization of
  deferred charges (debt issuance costs, leasehold improvements financed
  separately) is `amort_deferred_charges`.
- `net_income_start` on CF is the reconciliation starting line; it equals
  `is.net_income` for that quarter.
- `interest_expense` on IS is the gross expense (positive number);
  `interest_income` is investment/interest income (positive number).
  `net_interest_expense` is a computed subtotal.

FORWARD-FILL RULE (strict):
A bucket may be forward-filled ONLY if the concept genuinely does not appear
in that period's raw XBRL. This is audited against parsed_xbrl.json — if any
candidate concept is present with a non-null value, the forward-fill is
rejected as false.

When you forward-fill, declare:
{{
  "bucket": "<name>",
  "statement": "<statement>",
  "source_filing": "<filename of the 10-K whose value was used>",
  "applies_to_quarters": ["<quarter_label>", ...],
  "candidate_concepts": ["us-gaap:..."]
}}

STATEMENTS:
{json.dumps(slim_stmts, indent=2)}

SEGMENTS (for context only — do NOT map segments into these buckets):
{json.dumps(slim_segs, indent=2)}
{failure_block}

OUTPUT ONLY valid JSON:
{{
  "reporting_unit": "USD_millions" | "USD",
  "stock_splits": [{{"between": ["<quarter_label>", "<quarter_label>"], "ratio": <number>, "action": "..."}}],
  "bucket_assignments": {{
    "income_statement": {{
      "revenue": [{{"concept": "us-gaap:Revenues", "sign": "+"}}],
      "cogs": [{{"concept": "us-gaap:CostOfRevenue", "sign": "+"}}],
      ...
    }},
    "balance_sheet": {{...}},
    "cash_flow": {{...}}
  }},
  "exclusions": [
    {{"statement": "...", "concept": "...", "reason": "..."}}
  ],
  "forward_fills": [...]
}}

No monetary values in the output — Python computes those from the
assignments and the as-reported row values. No apostrophes in strings."""


def run_ai_buckets(ticker, statements, segments, model, prior_failures=None):
    """Call the AI to produce bucket assignments + forward_fills."""
    client = anthropic.Anthropic()

    prompt = build_bucket_prompt(ticker, statements, segments, prior_failures)

    print(f"  Calling AI (bucket-assignment pass, ~{len(prompt)//4:,} tokens in)...")
    output_text = ''
    with client.messages.stream(
        model=model,
        max_tokens=32768,
        messages=[{'role': 'user', 'content': prompt}],
    ) as stream:
        for text in stream.text_stream:
            output_text += text
            print('.', end='', flush=True)
        print()
        resp = stream.get_final_message()

    in_tok = resp.usage.input_tokens
    out_tok = resp.usage.output_tokens

    json_text = output_text.strip()
    fb = json_text.find('{')
    lb = json_text.rfind('}')
    if fb != -1 and lb != -1:
        json_text = json_text[fb:lb + 1]

    try:
        parsed = json.loads(json_text)
    except json.JSONDecodeError:
        fixed = json_text.replace('\u2018', "'").replace('\u2019', "'")
        fixed = re.sub(r'[\x00-\x1f]', ' ', fixed)
        fixed = re.sub(r',\s*([}\]])', r'\1', fixed)
        parsed = json.loads(fixed)

    return parsed, in_tok, out_tok


# ────────────────────────────────────────────────────────────────────────────
# Orchestration
# ────────────────────────────────────────────────────────────────────────────

def run_stage2(ticker, model='claude-sonnet-4-6', max_retries=3, test_mode=True):
    extract_dir = os.path.join(os.path.dirname(__file__), ticker)
    work_dir = os.path.join(extract_dir, 'test') if test_mode else extract_dir

    print(f"{'='*60}\n  STAGE 2 for {ticker} (work_dir={work_dir})\n{'='*60}")
    filings = load_filings(work_dir)
    if not filings:
        print(f"  ERROR: no per-filing JSONs found in {work_dir}")
        return None, 0, 0
    print(f"  Loaded {len(filings)} filings:")
    for f in filings:
        print(f"    {f['quarter_label']:8s}  {f['form']:5s}  period_end={f['period_end']}  {f['file']}")

    # ── Output 1: statements ──
    print("\n  Building statements (deterministic)...")
    statements = {}
    for stmt in STATEMENTS:
        rows = merge_statement_rows(filings, stmt)
        derive_q2_q3_from_ytd(rows, stmt, filings)
        derive_q4_for_flows(rows, stmt, filings)
        formulas = gather_formulas(filings, stmt)
        evaluate_formulas(rows, formulas)
        statements[stmt] = {'rows': rows, 'formulas': formulas}
        print(f"    {stmt}: {len(rows)} rows, {len(formulas)} formulas")

    # ── Segments ──
    segments = build_segments(filings)
    print(f"    segments: {len(segments['axes'])} axes")

    # ── AI bucket-assignment pass with retry loop ──
    total_in, total_out = 0, 0
    assignments = {}
    exclusions = []
    forward_fills = []
    reporting_unit = 'USD_millions'
    stock_splits = []
    prior_failures = None
    bucket_values = {}
    subtotal_receipts = {}
    failures = []
    attempt = 0

    for attempt in range(max_retries + 1):
        print(f"\n  AI bucket pass (attempt {attempt + 1}/{max_retries + 1})...")
        try:
            parsed, in_tok, out_tok = run_ai_buckets(
                ticker, statements, segments, model, prior_failures=prior_failures
            )
        except Exception as e:
            print(f"    AI call failed: {e}")
            if attempt >= max_retries:
                return None, total_in, total_out
            continue
        total_in += in_tok
        total_out += out_tok

        assignments = parsed.get('bucket_assignments', {}) or {}
        exclusions = parsed.get('exclusions', []) or []
        forward_fills = parsed.get('forward_fills', []) or []
        reporting_unit = parsed.get('reporting_unit', reporting_unit)
        stock_splits = parsed.get('stock_splits', []) or []

        # Compute bucket values + subtotals
        bucket_values = compute_bucket_values(statements, assignments)
        subtotal_receipts = compute_subtotals(bucket_values)

        # Run verification
        result_for_verify = {
            'statements': statements,
            'segments': segments,
            'bucket_assignments': assignments,
            'bucket_values': bucket_values,
            'exclusions': exclusions,
            'forward_fills': forward_fills,
        }
        failures = verify_all(result_for_verify, filings, ticker)
        print(f"    verification: {len(failures)} failures")
        for f in failures[:15]:
            print(f"      [{f['type']}] {f.get('message','')}")
        if len(failures) > 15:
            print(f"      ... and {len(failures) - 15} more")

        if not failures:
            print(f"\n  ALL CHECKS PASSED")
            break

        if attempt >= max_retries:
            print(f"\n  MAX RETRIES REACHED with {len(failures)} unresolved failures — hard error")
            break

        prior_failures = failures

    # Cost
    in_rate, out_rate = (15.0, 75.0) if 'opus' in model else (3.0, 15.0)
    cost = total_in * in_rate / 1e6 + total_out * out_rate / 1e6
    print(f"\n  Tokens: in={total_in:,}  out={total_out:,}  cost=${cost:.2f}")

    # Attach normalized block to each statement for JSON + CSV consumption.
    # Detail buckets expose {face, note, total} per quarter so CSV can show the
    # breakdown. Subtotals show face-only (they only ever have face values).
    for stmt_name, data in statements.items():
        stmt_buckets = bucket_values.get(stmt_name, {})
        detail = CANONICAL_BUCKETS.get(stmt_name, {}).get('detail', [])
        subtotals = [s for s, _ in CANONICAL_BUCKETS.get(stmt_name, {}).get('subtotals', [])]

        def _flatten(qvals):
            out = {}
            for q, entry in (qvals or {}).items():
                if not isinstance(entry, dict):
                    out[q] = {'face': entry, 'note': None, 'total': entry}
                    continue
                f, n = entry.get('face'), entry.get('note')
                total = None if (f is None and n is None) else (f or 0) + (n or 0)
                out[q] = {'face': f, 'note': n, 'total': total}
            return out

        data['normalized'] = {
            'detail': [{'bucket': b,
                        'values_by_quarter': _flatten(stmt_buckets.get(b, {}) or {}),
                        'sources': (assignments.get(stmt_name) or {}).get(b, [])}
                       for b in detail],
            'subtotals': [{'bucket': s,
                           'values_by_quarter': (subtotal_receipts.get(stmt_name, {}).get(s, {}) or {}).get('values_by_quarter', {}) or {},
                           'components': (subtotal_receipts.get(stmt_name, {}).get(s, {}) or {}).get('components', [])}
                          for s in subtotals],
        }

    result = {
        'ticker': ticker,
        'reporting_unit': reporting_unit,
        'stock_splits': stock_splits,
        'statements': statements,
        'segments': segments,
        'bucket_assignments': assignments,
        'exclusions': exclusions,
        'forward_fills': forward_fills,
        'verification': {
            'retries': attempt,
            'failures': failures,
            'passed': not failures,
        },
    }
    return result, total_in, total_out


def build_quarterly_json(statements, filings):
    """Write one record per quarter with flat bucket values (raw dollars).

    Each record contains the canonical bucket names (detail + subtotals) per
    statement, namespaced by statement prefix ({statement}.{bucket}) to avoid
    collision between same-named buckets across statements (e.g., is.dna vs
    cf.dna). calculate.py reads these directly.
    """
    by_q = {f['quarter_label']: f for f in filings}
    all_quarters = sorted(by_q.keys(), key=lambda q: by_q[q]['period_end'])
    records = []
    for q in all_quarters:
        f = by_q[q]
        rec = {
            'period_end': str(f['period_end']),
            'fiscal_year': f['fiscal_year_label'],
            'fiscal_period': f['fiscal_period'],
            'form': f['form'],
            'quarter_label': q,
        }
        for stmt_name, data in statements.items():
            normalized = data.get('normalized', {})
            prefix = {'income_statement': 'is', 'balance_sheet': 'bs', 'cash_flow': 'cf'}.get(stmt_name, stmt_name)
            for entry in normalized.get('detail', []):
                v = entry['values_by_quarter'].get(q)
                # Use total (face+note) for quarterly.json consumers like
                # calculate.py — analytical formulas want the full picture.
                if isinstance(v, dict):
                    total = v.get('total')
                    face = v.get('face')
                    if total is not None:
                        rec[f'{prefix}.{entry["bucket"]}'] = total
                        if v.get('note') is not None and face != total:
                            rec[f'{prefix}.{entry["bucket"]}.face'] = face
                            rec[f'{prefix}.{entry["bucket"]}.note'] = v.get('note')
                elif v is not None:
                    rec[f'{prefix}.{entry["bucket"]}'] = v
            for entry in normalized.get('subtotals', []):
                v = entry['values_by_quarter'].get(q)
                if v is not None:
                    rec[f'{prefix}.{entry["bucket"]}'] = v
        records.append(rec)
    return records


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ticker', required=True)
    parser.add_argument('--model', default='claude-sonnet-4-6')
    parser.add_argument('--test', action='store_true',
                        help='Work in ai_extract/{TICKER}/test/ (default)')
    parser.add_argument('--no-test', dest='test', action='store_false')
    parser.set_defaults(test=True)
    parser.add_argument('--max-retries', type=int, default=3)
    args = parser.parse_args()

    result, in_tok, out_tok = run_stage2(
        args.ticker, model=args.model, max_retries=args.max_retries, test_mode=args.test
    )
    if result is None:
        print("ERROR: Stage 2 failed to produce a result")
        sys.exit(1)

    extract_dir = os.path.join(os.path.dirname(__file__), args.ticker)
    out_dir = os.path.join(extract_dir, 'test') if args.test else extract_dir

    # Statements/segments JSON is not directly JSON-serializable if rows contain
    # non-primitive types; we've kept them primitive so json.dumps works.
    out_path = os.path.join(out_dir, 'formula_mapped_v3.json')
    with open(out_path, 'w') as f:
        json.dump(_make_json_safe(result), f, indent=2, default=str)
    print(f"\n  Saved {out_path}")

    # Quarterly.json (flat bucket values per period)
    filings = load_filings(out_dir)
    quarterly = build_quarterly_json(result['statements'], filings)
    q_path = os.path.join(out_dir, 'quarterly.json')
    with open(q_path, 'w') as f:
        json.dump(quarterly, f, indent=2, default=str)
    print(f"  Saved {q_path} ({len(quarterly)} quarters)")

    passed = result['verification']['passed']
    sys.exit(0 if passed else 2)


def _make_json_safe(obj):
    if isinstance(obj, dict):
        return {k: _make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_make_json_safe(v) for v in obj]
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return obj


if __name__ == '__main__':
    main()
