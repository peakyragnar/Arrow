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
You are mapping extracted financial statement data to standardized fields for formula calculations.

Given the AI extraction JSON (containing income_statement, balance_sheet, cash_flow line items and calculation_components), map each value to the correct field name for the CURRENT PERIOD only.

For each field, find the correct value from the extraction. Use the line item labels, hierarchy, and calculation_components to identify items. If an item is not present in the extraction, omit it.

## INCOME STATEMENT FIELDS (quarterly values from IS)

- revenue_q: Total revenue / net revenue. Top line.
- cogs_q: Cost of revenue / cost of goods sold / cost of sales. The direct cost line under revenue.
- gross_profit_q: Revenue minus COGS.
- rd_expense_q: Research and development expense.
- sga_q: Selling, general and administrative expense.
- total_opex_q: Total operating expenses (R&D + SGA + any other operating expenses).
- operating_income_q: Operating income / income from operations.
- interest_income_q: Interest income / investment income.
- interest_expense_q: Interest expense (GROSS, not net). Store as NEGATIVE. If the IS shows net interest, use calculation_components.interest_expense.gross instead.
- other_nonop_income_q: Other non-operating income/expense.
- total_nonop_income_q: Total other income/expense, net.
- pretax_income_q: Income before income taxes.
- income_tax_expense_q: Income tax expense / provision for income taxes.
- equity_method_earnings_q: Equity in earnings of affiliates (if present).
- net_income_q: Net income. For companies with noncontrolling interests, use the CONSOLIDATED net income (the line the cash flow statement starts with), not net income attributable to common.
- net_income_nci_q: Net income attributable to noncontrolling interests (if present).
- net_income_to_common_q: Net income attributable to common stockholders (if present).
- diluted_shares_q: Diluted weighted average shares outstanding. Store in RAW share count (multiply millions by 1,000,000).
- basic_shares_q: Basic weighted average shares outstanding. Store in RAW share count.
- eps_diluted_q: Diluted earnings per share. Keep as reported (do not multiply).
- eps_basic_q: Basic earnings per share. Keep as reported (do not multiply).

## BALANCE SHEET FIELDS (instant values, most recent period)

- cash_q: Cash and cash equivalents.
- short_term_investments_q: Marketable securities / short-term investments (current).
- accounts_receivable_q: Accounts receivable, net.
- inventory_q: Inventories / inventory.
- prepaid_q: Prepaid expenses and other current assets.
- total_current_assets_q: Total current assets.
- ppe_q: Property and equipment / plant and equipment, net.
- operating_lease_assets_q: Operating lease right-of-use assets.
- goodwill_q: Goodwill.
- intangibles_q: Intangible assets, net (excluding goodwill).
- deferred_tax_assets_q: Deferred income tax assets.
- other_noncurrent_assets_q: Other non-current / long-term assets.
- total_assets_q: Total assets.
- accounts_payable_q: Accounts payable (PURE trade AP, not combined with accrued). Use calculation_components.accounts_payable if BS combines AP with accrued liabilities.
- accrued_liabilities_q: Accrued liabilities / accrued and other current liabilities.
- short_term_debt_q: Short-term debt / current portion of long-term debt / commercial paper. Use calculation_components.short_term_debt.value. If none exists, 0.
- total_current_liabilities_q: Total current liabilities.
- long_term_debt_q: Long-term debt (non-current portion).
- long_term_lease_liabilities_q: Long-term operating lease liabilities (non-current).
- operating_lease_liabilities_q: TOTAL operating lease liabilities (current + non-current). Use calculation_components.operating_leases.total.
- other_noncurrent_liabilities_q: Other non-current / long-term liabilities.
- total_liabilities_q: Total liabilities.
- common_stock_q: Common stock par value.
- apic_q: Additional paid-in capital.
- aoci_q: Accumulated other comprehensive income/loss.
- retained_earnings_q: Retained earnings / accumulated deficit.
- equity_q: Total STOCKHOLDERS equity (parent only, excluding noncontrolling interests). If the BS shows both "Total stockholders equity" and "Total equity" (including NCI), use the stockholders equity line.
- noncontrolling_interests_q: Noncontrolling interests (if present).
- equity_incl_nci_q: Total equity including NCI (if present and different from equity_q).
- total_liabilities_and_equity_q: Total liabilities and stockholders equity.

## CASH FLOW FIELDS (quarterly values — if this is a 10-Q, use the YTD values; if 10-K, use the annual values. The caller will handle YTD-to-quarterly derivation.)

- sbc_q: Stock-based compensation expense (CF operating addback).
- dna_q: Depreciation and amortization / depreciation, depletion and amortization.
- deferred_taxes_q: Deferred income taxes.
- change_ar_q: Change in accounts receivable.
- change_inventory_q: Change in inventories.
- change_prepaid_q: Change in prepaid expenses and other assets.
- change_ap_q: Change in accounts payable.
- change_accrued_q: Change in accrued liabilities.
- change_other_lt_liabilities_q: Change in other long-term liabilities.
- cfo_q: Net cash provided by operating activities.
- capex_q: Capital expenditures (purchases of property/equipment/intangibles). Store as NEGATIVE. Use calculation_components.capex.cf_value if available (handles segmented capex).
- acquisitions_q: Acquisitions net of cash acquired. Store as NEGATIVE. Use calculation_components.acquisitions.total if available (handles multiple acquisition lines).
- cfi_q: Net cash from investing activities.
- share_repurchases_q: Payments for repurchases of common stock.
- dividends_q: Dividends paid.
- debt_repayment_q: Repayment of debt.
- cff_q: Net cash from financing activities.
- net_change_cash_q: Net change in cash and cash equivalents.
- taxes_paid_q: Cash paid for income taxes.

## SIGN CONVENTIONS

- Revenue, income, assets, equity: POSITIVE
- Expenses (COGS, R&D, SGA, tax): POSITIVE (they are costs, stored as positive numbers)
- Interest expense: NEGATIVE
- Capex: NEGATIVE (cash outflow)
- Acquisitions: NEGATIVE (cash outflow)
- Share repurchases: value as reported on CF (typically negative)
- Dividends: value as reported on CF (typically negative)
- Working capital changes: as reported on CF (positive = source of cash, negative = use)
- Short-term debt: 0 if none exists (do not omit, explicitly set to 0)

## UNITS

ALL monetary values in RAW dollars (not millions). Multiply the extraction values (which are in millions) by 1,000,000.
Shares in RAW count. Multiply millions by 1,000,000.
EPS: keep as reported (dollars per share, do not multiply).

## OUTPUT FORMAT

Output ONLY valid JSON with this structure:
{
  "period_end": "2025-04-27",
  "period_start": "2025-01-27",
  "form": "10-Q",
  "fields": {
    "revenue_q": 44062000000,
    "cogs_q": 17394000000,
    ...
  },
  "cf_is_ytd": true,
  "notes": ["any notes about mapping decisions"]
}

- period_end: the most recent IS period end date (for 10-Q: current quarter end; for 10-K: fiscal year end)
- period_start: the most recent IS period start date
- form: "10-Q" or "10-K"
- fields: all mapped fields with values in raw units
- cf_is_ytd: true if CF values are YTD (10-Q with >1 quarter), false if quarterly (10-Q Q1) or annual (10-K)
- For 10-K: IS and CF fields should be the ANNUAL totals (full fiscal year). The caller derives Q4.
- For 10-Q Q1: IS is quarterly, CF is quarterly (same thing for Q1).
- For 10-Q Q2/Q3: IS quarterly column is available (use it). CF is YTD (flag cf_is_ytd: true).

CRITICAL: Output must be valid JSON. No apostrophes in strings.
"""


def map_filing(extraction_json, ticker, model='claude-sonnet-4-6'):
    """Send extraction to AI for field mapping."""
    ai = extraction_json['ai_extraction']

    # Build a compact version of the extraction for the prompt
    prompt = f"""Map this {ticker} financial extraction to standardized formula fields.

{FIELD_DEFINITIONS}

---
EXTRACTION DATA:
{json.dumps(ai, indent=2)}
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
            elif field.startswith('change_') or field in ('cfo_q', 'cfi_q', 'cff_q',
                    'capex_q', 'acquisitions_q', 'sbc_q', 'dna_q', 'deferred_taxes_q',
                    'share_repurchases_q', 'dividends_q', 'debt_repayment_q',
                    'net_change_cash_q', 'taxes_paid_q', 'cf_net_income_q'):
                # CF field with YTD value — store as YTD, derive later
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
    parser = argparse.ArgumentParser(description='AI-powered formula field mapping')
    parser.add_argument('--ticker', required=True)
    parser.add_argument('--filing', help='Single filing to map')
    parser.add_argument('--filings', nargs='+', help='Multiple filings to map')
    parser.add_argument('--model', default='claude-sonnet-4-6')
    parser.add_argument('--output', help='Output file path')
    parser.add_argument('--from-mapped', help='Skip AI calls, use existing formula_mapped.json')
    args = parser.parse_args()

    if args.from_mapped:
        # Skip AI, just derive quarterly from existing per-filing results
        print(f"Reading {args.from_mapped}...")
        with open(args.from_mapped) as f:
            all_results = json.load(f)
        total_in = total_out = 0
    else:
        filing_paths = args.filings or ([args.filing] if args.filing else [])
        if not filing_paths:
            print("Error: provide --filing, --filings, or --from-mapped")
            sys.exit(1)

        all_results = []
        total_in = 0
        total_out = 0

        for filepath in filing_paths:
            print(f"\nMapping {filepath}...")
            with open(filepath) as f:
                extraction = json.load(f)

            result, in_tok, out_tok = map_filing(extraction, args.ticker, args.model)
            total_in += in_tok
            total_out += out_tok

            fields = result.get('fields', {})
            pe = result.get('period_end', '?')
            form = result.get('form', '?')
            n_fields = len(fields)

            rev = fields.get('revenue_q', 0) / 1e9 if fields.get('revenue_q') else 0
            ni = fields.get('net_income_q', 0) / 1e9 if fields.get('net_income_q') else 0

            print(f"  {pe} ({form}): {n_fields} fields, revenue={rev:.1f}B, net_income={ni:.1f}B")
            if result.get('notes'):
                for note in result['notes']:
                    print(f"  Note: {note}")

            all_results.append(result)

        # Save per-filing results
        per_filing_path = f'ai_extract/{args.ticker}/formula_mapped.json'
        with open(per_filing_path, 'w') as f:
            json.dump(all_results, f, indent=2)
        print(f"\nPer-filing results saved to {per_filing_path}")

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
