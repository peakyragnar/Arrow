"""
AI-powered formula field mapping.

Takes an AI extraction JSON (from analyze_statement.py) and asks the model
to map the extracted line items to our standardized formula fields. The model
does the semantic understanding — no lookup tables, no XBRL concept lists.

This is the second pass. The first pass (analyze_statement.py) extracts all
data from the filing. This pass maps it to the fields our formulas need.

Usage:
    python3 ai_extract/ai_formula.py --ticker NVDA --filing ai_extract/NVDA/q1_fy26_10q.json
    python3 ai_extract/ai_formula.py --ticker NVDA --filings ai_extract/NVDA/q1_fy26_10q.json ai_extract/NVDA/q2_fy26_10q.json ...
"""

import argparse
import json
import os
import sys
from datetime import datetime

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

import anthropic


FIELD_DEFINITIONS = """
You are reviewing a verified financial extraction and mapping 23 analytical components for metric calculations (ROIC, ROIIC, reinvestment rate, margins, CCC, etc.).

The extraction has already been verified — all subtotals match reported totals, formulas balance. Your job is NOT to re-extract data. Your job is:

1. Map the 23 fields below from the extraction, resolving ambiguities
2. Validate consistency — no double counting, signs are correct, tax logic flows
3. Flag anything suspicious

## THE 23 FIELDS

### Income Statement (7 fields)

- revenue_q: Total revenue / net revenue. Top line. Straightforward.
- cogs_q: Cost of revenue / cost of goods sold. Straightforward.
- operating_income_q: Operating income / income from operations. Straightforward.
- interest_expense_q: GROSS interest expense, not net. Store as NEGATIVE.
  AMBIGUITY: If the IS only shows "Interest expense, net", find gross interest expense in the notes or calculation_components.interest_expense.gross. If gross is not disclosed, use net as fallback and flag it.
- pretax_income_q: Income before income taxes. Straightforward.
- income_tax_expense_q: Income tax expense / provision for income taxes.
  AMBIGUITY: Can be a benefit (negative). Compute effective_tax_rate = tax / pretax. If pretax is positive but tax is negative (benefit), or if effective rate is > 50% or < 0%, flag it. The downstream formula uses a 21% fallback for nonsensical rates, but output the actual reported value here.
- net_income_q: Net income.
  AMBIGUITY: Must be CONSOLIDATED net income — the same number the cash flow statement uses as its starting line. If the company has noncontrolling interests (NCI), do NOT use "net income attributable to common stockholders." Use the line before the NCI attribution. Verify: net_income_q here should match the "Net income" line at the top of the cash flow statement.

### Balance Sheet (10 fields)

- cash_q: Cash and cash equivalents. Straightforward.
- short_term_investments_q: Marketable securities / short-term investments (current). Straightforward.
- accounts_receivable_q: Accounts receivable, net. Straightforward.
- inventory_q: Inventories. Straightforward.
- total_assets_q: Total assets. Straightforward.
- accounts_payable_q: Accounts payable.
  AMBIGUITY: Must be PURE trade AP, not combined with accrued liabilities. If the BS shows a single "Accounts payable and accrued liabilities" line, find the pure AP breakout in notes or calculation_components.accounts_payable. If no breakout exists, use the combined line and flag it.
- short_term_debt_q: Short-term debt / current portion of long-term debt / commercial paper.
  AMBIGUITY: May not appear as a separate BS line item. Check calculation_components.short_term_debt. Check if there is a current portion of long-term debt in the notes. If genuinely none exists, set to 0 and confirm with "confirmed_zero": true in notes.
- long_term_debt_q: Long-term debt (non-current portion). Straightforward.
- operating_lease_liabilities_q: TOTAL operating lease liabilities (current + non-current combined).
  AMBIGUITY: The current portion is often NOT a separate BS line — it may be buried inside "Accrued and other current liabilities." Find it in calculation_components.operating_leases or search the notes. Add current + non-current to get the total. Do NOT double count — if the BS line "Accrued liabilities" already includes the current operating lease liability, you are extracting it FROM accrued, not in addition to.
- equity_q: Total stockholders equity.
  AMBIGUITY: Must be parent-only stockholders equity, NOT total equity including NCI. If the BS shows both lines, use the stockholders equity line (before NCI).

### Cash Flow / Other (6 fields)

- diluted_shares_q: Diluted weighted average shares outstanding. From IS/EPS section. Straightforward.
- sbc_q: Stock-based compensation expense. From CF operating adjustments. Straightforward.
- dna_q: Depreciation and amortization. From CF operating adjustments. Straightforward.
- cfo_q: Net cash provided by operating activities. Straightforward.
- capex_q: Capital expenditures. Store as NEGATIVE.
  AMBIGUITY: May be labeled "Purchases of property and equipment" or may include intangible asset purchases on the same line. May be split across multiple lines. Use calculation_components.capex if available. Sum all capex-related lines.
- acquisitions_q: Acquisitions net of cash acquired. Store as NEGATIVE.
  AMBIGUITY: May be multiple acquisition lines. Use calculation_components.acquisitions.total if available. Sum all acquisition-related lines.

## VALIDATION CHECKS (perform these, report results)

1. Tax rate: effective_tax_rate = income_tax_expense_q / pretax_income_q. Flag if < 0%, > 50%, or pretax is negative.
2. Net income consistency: net_income_q should equal pretax_income_q - income_tax_expense_q (within rounding). Flag if not.
3. Balance sheet: equity_q + total_liabilities (from extraction) should equal total_assets_q. Flag if not.
4. Operating lease total: current + non-current should match calculation_components.operating_leases.total if available. Flag if not.
5. No double counting: If you extracted operating lease current from inside accrued liabilities, confirm it is not also counted in a separate BS line.

## SIGN CONVENTIONS

- Revenue, income, assets, equity: POSITIVE
- Expenses (COGS, tax): POSITIVE (they are costs)
- Interest expense: NEGATIVE
- Capex: NEGATIVE (cash outflow)
- Acquisitions: NEGATIVE (cash outflow)
- Short-term debt: 0 if none exists (explicitly set, do not omit)

## UNITS

ALL monetary values in RAW dollars (not millions). Multiply the extraction values (in millions) by 1,000,000.
Shares in RAW count. Multiply millions by 1,000,000.

## PERIOD SELECTION

- For 10-Q: use the CURRENT QUARTER values for IS fields and BS fields. For CF fields (sbc_q, dna_q, cfo_q, capex_q, acquisitions_q), use YTD values if this is Q2/Q3 (the caller handles YTD-to-quarterly derivation). For Q1, CF is already quarterly.
- For 10-K: use ANNUAL totals for IS and CF fields. BS is year-end. The caller derives Q4 = annual - Q1 - Q2 - Q3.
- Set cf_is_ytd: true if CF values are YTD (Q2/Q3 10-Q), false otherwise.

## OUTPUT FORMAT

Output ONLY valid JSON:
{
  "period_end": "2025-04-27",
  "period_start": "2025-01-27",
  "form": "10-Q",
  "fields": {
    "revenue_q": 44062000000,
    "cogs_q": 17394000000,
    ...all 23 fields...
  },
  "cf_is_ytd": false,
  "effective_tax_rate": 0.143,
  "validation": {
    "tax_rate_ok": true,
    "net_income_consistent": true,
    "balance_sheet_balances": true,
    "operating_lease_confirmed": true,
    "no_double_counting": true
  },
  "notes": ["any ambiguity resolutions or flags"]
}

CRITICAL: Output exactly 23 fields. No more, no less. If a field genuinely does not apply (e.g., no acquisitions), set to 0. Output must be valid JSON.
"""


def map_period(period_data, ticker, model='claude-sonnet-4-6'):
    """Send one period's verified financial data to AI for analytical component extraction."""

    # Build a compact version for the prompt
    prompt = f"""Extract the 23 analytical components from this {ticker} financial data for period {period_data.get('period', '?')}.

{FIELD_DEFINITIONS}

---
FINANCIAL DATA FOR THIS PERIOD:
{json.dumps(period_data, indent=2)}
"""

    client = anthropic.Anthropic()
    output_text = ""

    with client.messages.stream(
        model=model,
        max_tokens=8192,
        messages=[{"role": "user", "content": prompt}],
    ) as stream:
        for text in stream.text_stream:
            output_text += text
            print(".", end="", flush=True)
        print()
        resp = stream.get_final_message()
        input_tokens = resp.usage.input_tokens
        output_tokens = resp.usage.output_tokens

    # Parse JSON
    json_text = output_text.strip()
    first_brace = json_text.find('{')
    last_brace = json_text.rfind('}')
    if first_brace != -1 and last_brace != -1:
        json_text = json_text[first_brace:last_brace + 1]

    try:
        result = json.loads(json_text)
    except json.JSONDecodeError:
        import re
        fixed = json_text.replace('\u2018', "'").replace('\u2019', "'")
        fixed = re.sub(r'[\x00-\x1f]', ' ', fixed)
        fixed = re.sub(r',\s*([}\]])', r'\1', fixed)
        result = json.loads(fixed)

    return result, input_tokens, output_tokens


def derive_quarterly(filing_results):
    """
    Pure arithmetic: derive quarterly values from YTD/annual per-filing data.

    Takes a list of per-filing AI mapping results (chronological) and produces
    one quarterly record per period. No pattern matching — just subtraction.

    - Q1 10-Q: already quarterly, use as-is
    - Q2 10-Q: IS is quarterly (use as-is), CF is YTD (subtract Q1)
    - Q3 10-Q: IS is quarterly (use as-is), CF is YTD (subtract Q2 YTD)
    - 10-K: IS and CF are annual (subtract Q1+Q2+Q3 to get Q4)
    """
    # Separate filings by type
    quarterly_filings = []  # 10-Q results with quarterly IS
    ytd_cf = {}             # period_end -> YTD CF fields (for subtraction)
    annual_filings = []     # 10-K results with annual IS+CF

    # Fields that are averages/ratios — never derived by subtraction
    non_cumulative = {'diluted_shares_q', 'basic_shares_q', 'eps_diluted_q', 'eps_basic_q'}

    # BS fields — instant values, no derivation needed
    bs_fields = {
        'cash_q', 'short_term_investments_q', 'accounts_receivable_q', 'inventory_q',
        'prepaid_q', 'total_current_assets_q', 'ppe_q', 'operating_lease_assets_q',
        'goodwill_q', 'intangibles_q', 'deferred_tax_assets_q', 'other_noncurrent_assets_q',
        'total_assets_q', 'accounts_payable_q', 'accrued_liabilities_q', 'short_term_debt_q',
        'total_current_liabilities_q', 'long_term_debt_q', 'long_term_lease_liabilities_q',
        'operating_lease_liabilities_q', 'other_noncurrent_liabilities_q', 'total_liabilities_q',
        'common_stock_q', 'apic_q', 'aoci_q', 'retained_earnings_q', 'equity_q',
        'noncontrolling_interests_q', 'equity_incl_nci_q', 'total_liabilities_and_equity_q',
    }

    for result in filing_results:
        form = result.get('form', '')
        fields = result.get('fields', {})
        pe = result.get('period_end', '')
        ps = result.get('period_start', '')
        cf_is_ytd = result.get('cf_is_ytd', False)

        if form == '10-K':
            annual_filings.append(result)
        else:
            quarterly_filings.append(result)

            # Store YTD CF values for later subtraction
            if cf_is_ytd:
                ytd_cf[pe] = {k: v for k, v in fields.items()
                              if k not in bs_fields and k not in non_cumulative}

    # Build quarterly records from 10-Q filings
    records = {}
    for result in quarterly_filings:
        fields = result.get('fields', {})
        pe = result.get('period_end', '')
        ps = result.get('period_start', '')
        cf_is_ytd = result.get('cf_is_ytd', False)

        rec = {'period_end': pe, 'period_start': ps}

        for field, val in fields.items():
            if field in bs_fields or field in non_cumulative:
                # BS and shares: use directly
                rec[field] = val
            elif not cf_is_ytd:
                # Q1: everything is quarterly
                rec[field] = val
            elif field in ('cfo_q', 'capex_q', 'acquisitions_q', 'sbc_q', 'dna_q'):
                # CF fields with YTD value — store as YTD, derive later
                rec[f'{field}_ytd'] = val
            else:
                # IS field from quarterly column — use directly
                rec[field] = val

        records[pe] = rec

    # Derive quarterly CF from YTD by tracking running YTD per field
    sorted_pes = sorted(records.keys())

    # Collect all YTD field names
    all_ytd_fields = set()
    for pe in sorted_pes:
        for k in records[pe]:
            if k.endswith('_ytd'):
                all_ytd_fields.add(k[:-4])

    # For each CF field, iterate forward tracking running YTD
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
                # Q1 direct value — this IS the first period's value
                prev_ytd = rec[field]

    # Derive Q4 from 10-K annual minus Q1+Q2+Q3
    for annual in annual_filings:
        fields = annual.get('fields', {})
        pe = annual.get('period_end', '')
        ps = annual.get('period_start', '')

        rec = {'period_end': pe, 'period_start': ps}

        # Find the 3 prior quarters (same fiscal year)
        prior_pes = [p for p in sorted_pes if p < pe]
        prior_3 = prior_pes[-3:] if len(prior_pes) >= 3 else prior_pes

        for field, annual_val in fields.items():
            if field in bs_fields:
                rec[field] = annual_val
            elif field in non_cumulative:
                # Shares/EPS: use annual value directly as Q4
                rec[field] = annual_val
            else:
                # Flow field: Q4 = annual - sum(Q1+Q2+Q3)
                prior_sum = sum(records[p].get(field, 0) for p in prior_3 if p in records)
                if len(prior_3) == 3 and all(field in records.get(p, {}) for p in prior_3):
                    rec[field] = annual_val - prior_sum
                else:
                    # Can't derive Q4 without all 3 prior quarters
                    rec[field] = annual_val  # Store annual as fallback

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
        if 'revenue_q' in rec:
            rec['ticker'] = ticker
            rec = {k: v for k, v in rec.items() if v is not None}
            result.append(rec)
    return result


def main():
    parser = argparse.ArgumentParser(description='AI-powered analytical component extraction')
    parser.add_argument('--ticker', required=True)
    parser.add_argument('--model', default='claude-sonnet-4-6')
    parser.add_argument('--output', help='Output file path')
    parser.add_argument('--from-mapped', action='store_true',
                        help='Skip AI calls, use existing formula_mapped.json for quarterly derivation only')
    args = parser.parse_args()

    extract_dir = f'ai_extract/{args.ticker}'
    mapped_path = os.path.join(extract_dir, 'mapped.json')
    formula_mapped_path = os.path.join(extract_dir, 'formula_mapped.json')

    if args.from_mapped:
        # Skip AI, just derive quarterly from existing formula_mapped.json
        print(f"Reading {formula_mapped_path}...")
        with open(formula_mapped_path) as f:
            all_results = json.load(f)
        total_in = total_out = 0
    else:
        # Read mapped.json (verified financial statements by period)
        if not os.path.exists(mapped_path):
            print(f"Error: {mapped_path} not found. Run Stage 1 (analyze_statement.py) first.")
            sys.exit(1)

        with open(mapped_path) as f:
            mapped_data = json.load(f)

        # Filter to periods that have IS data (actual reporting periods, not just BS snapshots)
        periods_to_map = [p for p in mapped_data
                          if p.get('income_statement', {}).get('line_items')]

        print(f"Found {len(periods_to_map)} periods with IS data in mapped.json")

        all_results = []
        total_in = 0
        total_out = 0

        for period_data in periods_to_map:
            period = period_data.get('period', '?')
            print(f"\nMapping period {period}...")

            result, in_tok, out_tok = map_period(period_data, args.ticker, args.model)
            total_in += in_tok
            total_out += out_tok

            fields = result.get('fields', {})
            n_fields = len(fields)

            rev = fields.get('revenue_q', 0) / 1e9 if fields.get('revenue_q') else 0
            ni = fields.get('net_income_q', 0) / 1e9 if fields.get('net_income_q') else 0

            print(f"  {period}: {n_fields} fields, revenue={rev:.1f}B, net_income={ni:.1f}B")
            validation = result.get('validation', {})
            if validation:
                failures = [k for k, v in validation.items() if not v]
                if failures:
                    print(f"  VALIDATION FAILURES: {failures}")
                else:
                    print(f"  All validations passed")
            if result.get('notes'):
                for note in result['notes']:
                    print(f"  Note: {note}")

            all_results.append(result)

        # Save formula_mapped.json
        with open(formula_mapped_path, 'w') as f:
            json.dump(all_results, f, indent=2)
        print(f"\nFormula mappings saved to {formula_mapped_path}")

    # Derive quarterly records
    print("\nDeriving quarterly values...")
    quarterly_records = derive_quarterly(all_results)
    final_records = merge_quarters(quarterly_records, args.ticker)

    # Display
    print(f"\n{len(final_records)} quarterly records for {args.ticker}")
    print(f"\n{'Period':>12} {'Revenue':>12} {'Net Inc':>12} {'CFO':>12} {'Cash':>12} {'Equity':>12}")
    print("-" * 72)
    for r in final_records:
        rev = r.get('revenue_q', 0) / 1e9
        ni = r.get('net_income_q', 0) / 1e9
        cfo_val = r.get('cfo_q')
        cfo = f"{cfo_val / 1e9:>11.1f}B" if cfo_val is not None else "          n/a"
        cash = (r.get('cash_q') or 0) / 1e9
        eq = (r.get('equity_q') or 0) / 1e9
        print(f"{r['period_end']:>12} {rev:>11.1f}B {ni:>11.1f}B {cfo} {cash:>11.1f}B {eq:>11.1f}B")

    # Cost
    if total_in > 0:
        in_rate, out_rate = 3.0, 15.0
        if 'opus' in (args.model or ''):
            in_rate, out_rate = 15.0, 75.0
        input_cost = total_in * in_rate / 1_000_000
        output_cost = total_out * out_rate / 1_000_000
        print(f"\nTokens: {total_in:,} in, {total_out:,} out")
        print(f"Cost: ${input_cost:.2f} input + ${output_cost:.2f} output = ${input_cost + output_cost:.2f}")
        print(f"Per filing: ${(input_cost + output_cost) / len(all_results):.3f}")

    # Save quarterly records
    out_path = args.output or f'ai_extract/{args.ticker}/quarterly.json'
    with open(out_path, 'w') as f:
        json.dump(final_records, f, indent=2)
    print(f"\nQuarterly records saved to {out_path}")


if __name__ == '__main__':
    main()
