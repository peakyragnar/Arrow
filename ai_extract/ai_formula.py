"""
Stage 2: All-periods normalization + quarterly derivation.

Reads all per-filing extractions (from analyze_statement.py), sends them
to the AI in one call for cross-period normalization. The AI produces
standardized analytical fields, handles stock splits, forward-fills annual
values where needed. Quarterly derivation runs as verification — if the
derived quarterly values don't make sense, the AI retries.

Usage:
    python3 ai_extract/ai_formula.py --ticker NVDA --v3 --test
"""

import argparse
import json
import os
import sys
from datetime import datetime

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

import anthropic




def load_formulas_md():
    """Load formulas.md content."""
    formulas_path = os.path.join(os.path.dirname(__file__), '..', 'formulas.md')
    with open(formulas_path) as f:
        return f.read()


def normalize_all_periods(ticker, model='claude-sonnet-4-6', max_retries=3):
    """Stage 2 v3: normalize all periods at once, verify with quarterly derivation.

    Reads mapped.json (all periods), sends to AI in one call. AI produces
    analytical fields with consistent naming, split adjustments, forward-fills.
    Then runs quarterly derivation as verification — if results are impossible,
    retries with specific error feedback.
    """
    import glob as _glob

    extract_dir = os.path.join(os.path.dirname(__file__), ticker)
    mapped_path = os.path.join(extract_dir, 'mapped.json')
    formulas_md = load_formulas_md()

    with open(mapped_path) as f:
        mapped_data = json.load(f)

    # Build slimmed per-filing extractions (drop mapping_reason etc. to save tokens)
    test_dir = os.path.join(extract_dir, 'test')
    filing_dir = extract_dir if not os.path.isdir(test_dir) else test_dir
    filing_paths = sorted(_glob.glob(os.path.join(filing_dir, 'q*_fy*_10*.json')))
    filing_paths = [p for p in filing_paths if 'formula' not in os.path.basename(p)
                    and 'stripped' not in os.path.basename(p)]

    slim_filings = {}
    for fpath in filing_paths:
        fname = os.path.basename(fpath)
        with open(fpath) as f:
            data = json.load(f)
        ai = data.get('ai_extraction', data)

        slim = {'source': fname}
        for stmt in ['income_statement', 'balance_sheet', 'cash_flow']:
            s = ai.get(stmt, {})
            slim[stmt] = {
                'line_items': [{'label': i['label'], 'xbrl_concept': i.get('xbrl_concept'),
                               'values': i.get('values', {})}
                              for i in s.get('line_items', [])],
                'xbrl_not_on_statement': [{'concept': i.get('concept'), 'value': i.get('value'),
                                          'reason': i.get('reason', '')}
                                         for i in s.get('xbrl_not_on_statement', [])]
            }
        slim['segment_data'] = ai.get('segment_data', [])
        slim_filings[fname] = slim

    extraction_str = json.dumps(slim_filings, indent=2)
    print(f"  {len(slim_filings)} filings, ~{len(extraction_str)//4:,} tokens (slimmed)")

    prompt = f"""You are a financial analyst normalizing extracted data across all filings for {ticker}.

You have two inputs:
1. Verified extractions from ALL {len(slim_filings)} filings for {ticker}. Each filing contains line_items, xbrl_not_on_statement for IS/BS/CF — all verified with math checks. Filings are keyed by filename (e.g., q1_fy24_10q.json = Q1 fiscal year 2024, 10-Q filing).
2. The metric formulas that will be computed from this data.

Your job: produce one normalized analytical dataset with one entry per filing.

READ ALL FILINGS FIRST. Before producing any output, review every filing's extraction to understand this company's reporting structure — what it reports, how it names things, what changes between filings, where items are hidden.

ANALYTICAL DERIVATIONS:
Read the metric formulas. For each analytical input the formulas need, determine where it comes from in this company's data. Apply the same mapping consistently across ALL periods.

For each analytical input and each period:
1. SEARCH the period's line_items across all three statements
2. SEARCH the period's xbrl_not_on_statement for all three statements
3. CHECK for the item under different labels (the same item may have different names across filings — you can see this because you have all periods)
4. ONLY if the item genuinely does not exist anywhere in that period's extraction, carry forward the most recent annual (10-K) value and flag it

DO NOT forward-fill if the data exists in the filing under any name or in any section. Exhaust every possibility first. When you do forward-fill, state exactly which 10-K period's value you used and confirm you searched the current period's extraction completely.

RECONCILIATION REQUIREMENT:
Every analytical value MUST trace back to a specific line item or xbrl_not_on_statement entry in the source extraction for that period. For each value, verify it matches the source. If you cannot find the source, flag it — do not guess.

CRITICAL — PERIOD VALUE SELECTION:
Your output will be run through quarterly derivation arithmetic. You MUST select the correct period value:

For 10-Q filings:
- IS fields (revenue, cogs, operating_income, etc.): use the QUARTERLY value (3-month column). For Q2/Q3, the filing shows both quarterly and YTD — use quarterly.
- CF fields (cfo, capex, acquisitions, sbc, dna, etc.):
  - Q1: use as-is (already quarterly)
  - Q2/Q3: use the YTD value and set cf_is_ytd: true. The derivation script will subtract the prior period to get standalone quarterly.
- BS fields (cash, total_assets, equity, etc.): use quarter-end snapshot values.

For 10-K filings:
- IS fields: use ANNUAL values (full year).
- CF fields: use ANNUAL values.
- BS fields: use year-end snapshot values.
- The derivation script computes Q4 = annual minus Q1+Q2+Q3.
- diluted_shares: use the ANNUAL reported value directly. This is NOT a flow — the derivation script passes it through as-is for Q4. Do not subtract.
- diluted_eps: do NOT include for 10-K filings. Q4 EPS is computed downstream as Q4 net_income / diluted_shares. Including annual EPS would be wrong.

NON-FLOW FIELDS (never derived by subtraction):
- diluted_shares, basic_shares: weighted averages, passed through directly
- diluted_eps, basic_eps: do NOT output for 10-K filings (annual EPS != Q4 EPS). For 10-Q filings, use the quarterly reported EPS.
- effective_tax_rate: use the reported rate for that period

If you pick the wrong value (e.g., YTD revenue instead of quarterly, or quarterly CF instead of YTD), the derivation will produce impossible results (negative revenue, negative CFO for a profitable company). The math will catch it.

Handle these cross-period issues:
- STOCK SPLITS: if diluted shares jumps by a large multiple between periods, normalize all pre-split periods to post-split basis (divide shares by the split ratio). Do NOT adjust EPS — EPS is already reported on a per-share basis and the filing reports it in the correct basis for that period. Only adjust shares.
- REPORTING CHANGES: if the company changed its structure between filings (new segments, renamed items), map both sides consistently
- SIGN CONVENTIONS: ensure signs are consistent across all periods for each field

Determine the reporting unit from the extraction data and convert all values to RAW dollars. Do not assume any unit — check the values to determine the scale.

METRIC FORMULAS:
{formulas_md}

EXTRACTION DATA (ALL FILINGS):
{extraction_str}

STANDARD ANALYTICAL FIELD NAMES (use these exact keys):
- revenue: total revenue / net revenue
- cogs: cost of revenue (null if not reported)
- gross_profit: gross profit (null if no COGS)
- operating_income: operating income
- pretax_income: income before income taxes
- income_tax_expense: income tax provision
- net_income: net income (consolidated, same as CF starting line)
- interest_expense: GROSS interest expense as NEGATIVE
- interest_income: interest or investment income
- sbc: stock-based compensation (CF addback, positive)
- dna: depreciation and amortization (CF addback, positive)
- diluted_shares: diluted weighted average shares (raw count, split-adjusted)
- cash: cash and cash equivalents
- short_term_investments: marketable securities (0 if none)
- accounts_receivable: accounts receivable net
- inventory: inventories (0 if not applicable)
- accounts_payable: accounts payable (pure trade AP)
- total_assets: total assets
- equity: stockholders equity (parent only)
- short_term_debt: current debt (0 if none)
- long_term_debt: long-term debt non-current
- operating_lease_liabilities: TOTAL operating lease liabilities (current + non-current)
- cfo: net cash from operating activities
- capex: capital expenditures as NEGATIVE
- acquisitions: acquisitions net of cash as NEGATIVE (0 if none)
- rd_expense: research and development (null if not reported)

Include additional analytical items the metric formulas require beyond this list. Use descriptive field names.

OUTPUT FORMAT:

Output ONLY valid JSON:
{{{{
  "ticker": "{ticker}",
  "reporting_unit": "...",
  "company_mapping": {{{{
    "field_name": "which XBRL concept(s) or line items it maps to across this company's filings, and why. Include the source location (line_items or xbrl_not_on_statement) for traceability."
  }}}},
  "stock_splits": [
    {{{{"between": ["period_1", "period_2"], "ratio": 10, "action": "description of adjustment"}}}}
  ],
  "periods": [
    {{{{
      "period_end": "YYYY-MM-DD",
      "period_start": "YYYY-MM-DD",
      "form": "10-Q or 10-K",
      "cf_is_ytd": false,
      "analytical": {{{{
        "revenue": value,
        "operating_income": value
      }}}},
      "flags": {{{{
        "field_name": "annual_only: 10-K FY24 value (2024-01-28) used. Searched this period extraction: not in IS/BS/CF line_items, not in xbrl_not_on_statement for any statement."
      }}}}
    }}}}
  ]
}}}}

All monetary values in RAW dollars after conversion.
Shares in raw count, split-adjusted.
EPS as reported but split-adjusted.

CRITICAL: Output must be valid JSON. No apostrophes in strings."""

    client = anthropic.Anthropic()
    total_in = 0
    total_out = 0

    for attempt in range(max_retries + 1):
        if attempt == 0:
            current_prompt = prompt
            print(f"  Stage 2: sending {len(mapped_data)} periods + formulas.md...")
        else:
            print(f"  Stage 2 retry {attempt}: sending failures...")

        output_text = ""
        with client.messages.stream(
            model=model,
            max_tokens=65536,
            messages=[{"role": "user", "content": current_prompt}],
        ) as stream:
            for text in stream.text_stream:
                output_text += text
                print(".", end="", flush=True)
            print()
            resp = stream.get_final_message()
            total_in += resp.usage.input_tokens
            total_out += resp.usage.output_tokens

        # Parse JSON
        json_text = output_text.strip()
        fb = json_text.find('{')
        lb = json_text.rfind('}')
        if fb != -1 and lb != -1:
            json_text = json_text[fb:lb + 1]

        try:
            result = json.loads(json_text)
        except json.JSONDecodeError:
            import re
            fixed = json_text.replace('\u2018', "'").replace('\u2019', "'")
            fixed = re.sub(r'[\x00-\x1f]', ' ', fixed)
            fixed = re.sub(r',\s*([}\]])', r'\1', fixed)
            try:
                result = json.loads(fixed)
            except json.JSONDecodeError:
                print("  ERROR: could not parse AI response as JSON")
                if attempt < max_retries:
                    current_prompt = "Your previous response was not valid JSON. Please output ONLY valid JSON with the exact structure specified."
                    continue
                else:
                    return None, total_in, total_out

        # Run quarterly derivation as verification
        periods = result.get('periods', [])
        if not periods:
            print("  ERROR: no periods in output")
            if attempt < max_retries:
                current_prompt = "Your output contained no periods. Please output the complete JSON with all periods."
                continue
            else:
                return result, total_in, total_out

        print(f"  Got {len(periods)} periods, running verification...")

        # Derive quarterly values
        quarterly_records = derive_quarterly_v2(periods)
        quarterly_list = merge_quarters(quarterly_records, ticker)

        # Run verification checks
        failures = verify_stage2_output(result, quarterly_list, mapped_data, ticker)

        if not failures:
            print(f"  ALL CHECKS PASSED ({len(quarterly_list)} quarters)")
            result['quarterly'] = quarterly_list
            break
        else:
            print(f"  {len(failures)} failures found")
            for f in failures[:10]:
                print(f"    [{f['type']}] {f['message']}")
            if len(failures) > 10:
                print(f"    ... and {len(failures) - 10} more")

            if attempt < max_retries:
                failure_text = '\n'.join(f"- [{f['type']}] {f['message']}" for f in failures)
                current_prompt = f"""Your output was run through quarterly derivation and verification. These issues were found:

{failure_text}

Fix these and output the complete corrected JSON. Remember:
- Q2/Q3 CF fields must be YTD values (the script subtracts prior period)
- IS fields for Q2/Q3 must be quarterly (3-month column)
- 10-K fields must be annual (the script derives Q4 = annual - Q1 - Q2 - Q3)
- Every standard analytical field must be present in every period
- Diluted shares must be split-adjusted across all periods

Output the COMPLETE JSON with all periods — not just the corrected ones."""
            else:
                print(f"  Max retries reached. Returning best result with {len(failures)} unresolved issues.")
                result['quarterly'] = quarterly_list
                result['unresolved_failures'] = failures

    # Cost
    in_rate, out_rate = 3.0, 15.0
    if 'opus' in model:
        in_rate, out_rate = 15.0, 75.0
    input_cost = total_in * in_rate / 1_000_000
    output_cost = total_out * out_rate / 1_000_000
    print(f"  Tokens: {total_in:,} in, {total_out:,} out")
    print(f"  Cost: ${input_cost:.2f} + ${output_cost:.2f} = ${input_cost + output_cost:.2f}")

    return result, total_in, total_out


def verify_stage2_output(result, quarterly_list, mapped_data, ticker):
    """Verify Stage 2 output using quarterly derivation results and source data."""
    failures = []
    periods = result.get('periods', [])

    # 1. FIELD PRESENCE — every standard field in every period
    standard_fields = [
        'revenue', 'cogs', 'gross_profit', 'operating_income', 'pretax_income',
        'income_tax_expense', 'net_income', 'interest_expense', 'sbc', 'dna',
        'diluted_shares', 'cash', 'short_term_investments', 'accounts_receivable',
        'inventory', 'accounts_payable', 'total_assets', 'equity', 'short_term_debt',
        'long_term_debt', 'operating_lease_liabilities', 'cfo', 'capex',
        'acquisitions', 'rd_expense',
    ]
    nullable_fields = {'cogs', 'gross_profit', 'rd_expense'}  # some companies don't have these

    for period in periods:
        pe = period.get('period_end', '?')
        analytical = period.get('analytical', {})
        flags = period.get('flags', {})
        for field in standard_fields:
            if field in nullable_fields:
                continue
            val = analytical.get(field)
            if val is None and field not in flags:
                failures.append({
                    'type': 'MISSING_FIELD',
                    'message': f'{field} missing in {pe} (not in analytical, not flagged)'
                })

    # 2. QUARTERLY SANITY CHECKS — after derivation
    for r in quarterly_list:
        pe = r.get('period_end', '?')

        # Revenue must be positive
        rev = r.get('revenue')
        if rev is not None and rev < 0:
            failures.append({
                'type': 'NEGATIVE_REVENUE',
                'message': f'{pe}: revenue = {rev/1e6:,.0f}M (must be positive)'
            })

        # Gross profit shouldn't be negative if revenue is large
        gp = r.get('gross_profit')
        if gp is not None and rev is not None and rev > 0 and gp < 0:
            failures.append({
                'type': 'NEGATIVE_GROSS_PROFIT',
                'message': f'{pe}: gross_profit = {gp/1e6:,.0f}M (negative with positive revenue)'
            })

        # Net income sanity — shouldn't be wildly different from operating income
        oi = r.get('operating_income')
        ni = r.get('net_income')
        if oi is not None and ni is not None and oi > 0 and ni < 0:
            failures.append({
                'type': 'SIGN_MISMATCH',
                'message': f'{pe}: operating_income={oi/1e6:,.0f}M positive but net_income={ni/1e6:,.0f}M negative'
            })

    # 3. FORMULA CHECKS on quarterly results
    for r in quarterly_list:
        pe = r.get('period_end', '?')

        rev = r.get('revenue')
        cogs = r.get('cogs')
        gp = r.get('gross_profit')
        if all(v is not None for v in [rev, cogs, gp]):
            expected = rev - cogs
            if abs(expected - gp) > max(abs(rev) * 0.01, 1e6):
                failures.append({
                    'type': 'FORMULA',
                    'message': f'{pe}: revenue({rev/1e6:,.0f}M) - cogs({cogs/1e6:,.0f}M) = {expected/1e6:,.0f}M != gross_profit({gp/1e6:,.0f}M)'
                })

        pretax = r.get('pretax_income')
        tax = r.get('income_tax_expense')
        ni = r.get('net_income')
        if all(v is not None for v in [pretax, tax, ni]):
            expected = pretax - tax
            if abs(expected - ni) > max(abs(pretax) * 0.01, 1e6):
                failures.append({
                    'type': 'FORMULA',
                    'message': f'{pe}: pretax({pretax/1e6:,.0f}M) - tax({tax/1e6:,.0f}M) = {expected/1e6:,.0f}M != net_income({ni/1e6:,.0f}M)'
                })

    # 4. CONTINUITY — no 5x jumps between consecutive quarters
    skip_continuity = {'acquisitions', 'short_term_debt'}  # these can legitimately jump
    for i in range(1, len(quarterly_list)):
        prev = quarterly_list[i-1]
        curr = quarterly_list[i]
        prev_pe = prev.get('period_end', '?')
        curr_pe = curr.get('period_end', '?')

        for field in standard_fields:
            if field in skip_continuity or field in nullable_fields:
                continue
            pv = prev.get(field)
            cv = curr.get(field)
            if pv is not None and cv is not None and pv != 0:
                ratio = abs(cv / pv)
                if ratio > 5 or ratio < 0.2:
                    failures.append({
                        'type': 'DISCONTINUITY',
                        'message': f'{field}: {prev_pe}={pv/1e6:,.0f}M -> {curr_pe}={cv/1e6:,.0f}M ({ratio:.1f}x)'
                    })

    # 5. SPLIT CHECK — diluted_shares within 2x across all quarters
    shares = [(r.get('period_end', '?'), r.get('diluted_shares'))
              for r in quarterly_list if r.get('diluted_shares') is not None]
    if len(shares) >= 2:
        min_s = min(s for _, s in shares)
        max_s = max(s for _, s in shares)
        if min_s > 0 and max_s / min_s > 2:
            failures.append({
                'type': 'SPLIT_NOT_NORMALIZED',
                'message': f'diluted_shares range: {min_s/1e6:,.0f}M to {max_s/1e6:,.0f}M ({max_s/min_s:.1f}x — split not normalized?)'
            })

    # 6. FORWARD-FILL AUDIT — check flagged items against parsed XBRL
    filings_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'filings', ticker)
    for period in periods:
        pe = period.get('period_end', '?')
        flags = period.get('flags', {})
        for field, flag_text in flags.items():
            if 'annual_only' not in str(flag_text).lower():
                continue
            # Find the accession for this period
            for acc in os.listdir(filings_dir):
                meta_path = os.path.join(filings_dir, acc, 'filing_meta.json')
                if not os.path.exists(meta_path):
                    continue
                with open(meta_path) as f:
                    meta = json.load(f)
                if meta.get('report_date') != pe:
                    continue
                parsed_path = os.path.join(filings_dir, acc, 'parsed_xbrl.json')
                if not os.path.exists(parsed_path):
                    break
                with open(parsed_path) as f:
                    parsed = json.load(f)
                # Check if the concept exists in this filing's XBRL
                mapping = result.get('company_mapping', {})
                concept_info = mapping.get(field, '')
                # Look for common operating lease concepts
                if 'operating_lease' in field.lower():
                    check_concepts = ['us-gaap:OperatingLeaseLiability',
                                     'us-gaap:OperatingLeaseLiabilityCurrent',
                                     'us-gaap:OperatingLeaseLiabilityNoncurrent']
                    for concept in check_concepts:
                        for fact in parsed.get('facts', []):
                            if fact['concept'] == concept and not fact['dimensioned']:
                                if fact['value_numeric'] is not None:
                                    failures.append({
                                        'type': 'FALSE_FORWARD_FILL',
                                        'message': f'{pe}: {field} flagged annual_only but {concept} exists in XBRL with value {fact["value_numeric"]/1e6:,.0f}M'
                                    })
                                    break
                break

    return failures


# Legacy functions map_filing_v2, map_filing, derive_quarterly removed.
# Use normalize_all_periods() + derive_quarterly_v2() instead.


def _legacy_removed():
    raise NotImplementedError("Legacy per-filing mapping removed. Use --v3 (normalize_all_periods).")


map_filing_v2 = _legacy_removed
map_filing = _legacy_removed
derive_quarterly = _legacy_removed
# --- legacy function bodies removed (332 lines) ---


def derive_quarterly_v2(filing_results):
    """
    Pure arithmetic: derive quarterly values from v2 per-filing data.

    Takes a list of v2 AI mapping results and produces one quarterly record
    per period using the standardized analytical field names.

    - Q1 10-Q: already quarterly, use as-is
    - Q2/Q3 10-Q: IS is quarterly, CF is YTD (subtract prior YTD)
    - 10-K: IS and CF are annual (subtract Q1+Q2+Q3 to get Q4)
    """
    # BS fields — instant values, no derivation needed
    bs_fields = {
        'cash', 'short_term_investments', 'accounts_receivable', 'inventory',
        'accounts_payable', 'total_assets', 'equity', 'short_term_debt',
        'long_term_debt', 'operating_lease_liabilities', 'operating_lease_current',
        'operating_lease_noncurrent', 'goodwill',
    }

    # Non-cumulative — never derived by subtraction
    non_cumulative = {
        'diluted_shares', 'basic_shares', 'diluted_eps', 'basic_eps',
        'effective_tax_rate',
    }

    # CF flow fields — these are YTD in Q2/Q3 10-Qs
    cf_flow_fields = {
        'cfo', 'capex', 'acquisitions', 'sbc', 'dna',
    }

    quarterly_filings = []
    annual_filings = []

    for result in filing_results:
        form = result.get('form', '')
        if form == '10-K':
            annual_filings.append(result)
        else:
            quarterly_filings.append(result)

    # Build quarterly records from 10-Q filings
    records = {}
    for result in quarterly_filings:
        analytical = result.get('analytical', {})
        pe = result.get('period_end', '')
        ps = result.get('period_start', '')
        cf_is_ytd = result.get('cf_is_ytd', False)

        rec = {'period_end': pe, 'period_start': ps}

        for field, val in analytical.items():
            if not isinstance(val, (int, float)):
                continue  # skip notes/strings

            # Skip _ytd, _prior, _prior_period suffixed fields from analytical
            # These are informational — the core field has the right value
            if field.endswith('_ytd') or field.endswith('_prior_quarter') or field.endswith('_prior_period'):
                continue

            if field in bs_fields or field in non_cumulative:
                rec[field] = val
            elif not cf_is_ytd:
                # Q1: everything is quarterly
                rec[field] = val
            elif field in cf_flow_fields:
                # CF fields with YTD — store as YTD, derive later
                rec[f'{field}_ytd'] = val
            else:
                # IS field from quarterly column — use directly
                rec[field] = val

        records[pe] = rec

    # Derive quarterly CF from YTD
    sorted_pes = sorted(records.keys())

    all_ytd_fields = set()
    for pe in sorted_pes:
        for k in records[pe]:
            if k.endswith('_ytd'):
                all_ytd_fields.add(k[:-4])

    for field in all_ytd_fields:
        ytd_key = f'{field}_ytd'
        prev_ytd = 0

        for pe in sorted_pes:
            rec = records[pe]
            if ytd_key in rec:
                ytd_val = rec[ytd_key]
                rec[field] = ytd_val - prev_ytd
                prev_ytd = ytd_val
                del rec[ytd_key]
            elif field in rec:
                prev_ytd = rec[field]

    # Derive Q4 from 10-K annual minus Q1+Q2+Q3
    for annual in annual_filings:
        analytical = annual.get('analytical', {})
        pe = annual.get('period_end', '')
        ps = annual.get('period_start', '')

        rec = {'period_end': pe, 'period_start': ps}

        prior_pes = [p for p in sorted_pes if p < pe]
        prior_3 = prior_pes[-3:] if len(prior_pes) >= 3 else prior_pes

        for field, annual_val in analytical.items():
            if not isinstance(annual_val, (int, float)):
                continue

            if field in bs_fields:
                rec[field] = annual_val
            elif field in non_cumulative:
                rec[field] = annual_val
            else:
                prior_sum = sum(records[p].get(field, 0) for p in prior_3 if p in records)
                if len(prior_3) == 3 and all(field in records.get(p, {}) for p in prior_3):
                    rec[field] = annual_val - prior_sum
                else:
                    rec[field] = annual_val

        records[pe] = rec

    return records


def merge_quarters(records, ticker):
    """
    Smart merge: combine quarterly records, only overwrite if value is different.
    Same or absent values are preserved. Changes logged as restatements.
    """
    merged = {}
    for pe in sorted(records.keys()):
        rec = records[pe]
        if pe not in merged:
            merged[pe] = rec
        else:
            existing = merged[pe]
            for key, new_val in rec.items():
                if new_val is None:
                    continue
                old_val = existing.get(key)
                if old_val is None:
                    existing[key] = new_val
                elif new_val != old_val:
                    if 'restatements' not in existing:
                        existing['restatements'] = []
                    existing['restatements'].append({
                        'field': key, 'old_value': old_val, 'new_value': new_val,
                    })
                    existing[key] = new_val

    # Add ticker, filter None values
    result = []
    for pe in sorted(merged.keys()):
        rec = merged[pe]
        if 'revenue_q' in rec or 'revenue' in rec:
            rec['ticker'] = ticker
            rec = {k: v for k, v in rec.items() if v is not None}
            result.append(rec)
    return result


def main():
    parser = argparse.ArgumentParser(description='AI-powered analytical component extraction')
    parser.add_argument('--ticker', required=True)
    parser.add_argument('--model', default='claude-sonnet-4-6')
    parser.add_argument('--v3', action='store_true',
                        help='All-periods normalization with quarterly derivation verification')
    parser.add_argument('--test', action='store_true',
                        help='Write output to test/ subdirectory')
    args = parser.parse_args()

    extract_dir = f'ai_extract/{args.ticker}'

    if args.v3:
        # V3: all-periods normalization with quarterly derivation verification
        print(f"{'='*60}")
        print(f"  STAGE 2 V3: All-periods normalization for {args.ticker}")
        print(f"{'='*60}")

        result, in_tok, out_tok = normalize_all_periods(args.ticker, args.model)

        if result is None:
            print("ERROR: Stage 2 failed")
            sys.exit(1)

        # Save output
        test_dir = os.path.join(extract_dir, 'test') if args.test else extract_dir
        os.makedirs(test_dir, exist_ok=True)

        # Save the full result (analytical mapping + quarterly)
        formula_path = os.path.join(test_dir, 'formula_mapped_v3.json')
        with open(formula_path, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"\nSaved to {formula_path}")

        # Save quarterly.json separately
        quarterly = result.get('quarterly', [])
        if quarterly:
            quarterly_path = os.path.join(test_dir, 'quarterly.json')
            with open(quarterly_path, 'w') as f:
                json.dump(quarterly, f, indent=2)
            print(f"Saved {len(quarterly)} quarters to {quarterly_path}")

            # Display summary
            print(f"\n{'Period':>12} {'Revenue':>12} {'OI':>12} {'NI':>12} {'CFO':>12} {'Capex':>12}")
            print("-" * 72)
            for r in quarterly:
                pe = r.get('period_end', '')
                rev = r.get('revenue', 0) / 1e9
                oi = r.get('operating_income', 0) / 1e9
                ni = r.get('net_income', 0) / 1e9
                cfo = r.get('cfo', 0) / 1e9
                capex = r.get('capex', 0) / 1e9
                print(f"{pe:>12} {rev:>11.1f}B {oi:>11.1f}B {ni:>11.1f}B {cfo:>11.1f}B {capex:>11.1f}B")

        # Run validation report
        if quarterly:
            print(f"\n{'='*60}")
            print(f"  VALIDATION REPORT")
            print(f"{'='*60}")
            from validate_quarterly import validate, fmt
            findings = validate(quarterly)
            if not findings:
                print("  NO ISSUES FOUND")
            else:
                for f in findings:
                    print(f"  [{f['severity']}] [{f['type']}] {f['message']}")
                print(f"\n  Total: {len(findings)} findings")

        return

    else:
        print("Usage: python3 ai_extract/ai_formula.py --ticker NVDA --v3 [--test]")
        print("  --v3 is required (all-periods normalization)")
        sys.exit(1)


if __name__ == '__main__':
    main()
