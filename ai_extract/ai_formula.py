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
You are reviewing a verified financial extraction and mapping ALL financial statement items to standardized field names. The extraction has already been verified — all subtotals match reported totals, formulas balance. Your job is:

1. Map every line item to its standardized field name
2. Resolve ambiguities (9 fields need judgment — marked AMBIGUITY below)
3. Validate consistency — no double counting, signs correct, tax logic flows
4. Use CF PRESENTATION signs (as shown on the statement), NOT XBRL raw signs. If the CF shows (252) for AR, that means -252. If it shows 566 for inventories, that means +566. The sign that makes all components sum to the section total using + only.

## INCOME STATEMENT FIELDS

- revenue_q: Total revenue / net revenue
- cogs_q: Cost of revenue / cost of goods sold
- gross_profit_q: Gross profit
- rd_expense_q: Research and development expense
- sga_q: Selling, general and administrative expense
- acquisition_termination_q: Acquisition termination cost (0 if not present)
- total_opex_q: Total operating expenses
- operating_income_q: Operating income / income from operations
- interest_income_q: Interest income / investment income
- interest_expense_q: GROSS interest expense. Store as NEGATIVE.
  AMBIGUITY: If IS shows "Interest expense, net", find gross in notes or calculation_components.interest_expense.gross.
- other_nonop_income_q: Other non-operating income/expense, net
- total_nonop_income_q: Total other income/expense, net
- pretax_income_q: Income before income taxes
- income_tax_expense_q: Income tax expense / provision for income taxes
  AMBIGUITY: Can be benefit (negative). Flag if effective rate < 0% or > 50%.
- net_income_q: Net income
  AMBIGUITY: Must be CONSOLIDATED (same as CF starting line). Not net income to common if NCI exists.
- eps_basic_q: Basic EPS. Keep as reported (do not multiply).
- eps_diluted_q: Diluted EPS. Keep as reported.
- basic_shares_q: Basic weighted average shares. RAW count (multiply millions by 1,000,000).
- diluted_shares_q: Diluted weighted average shares. RAW count.

## BALANCE SHEET FIELDS

- cash_q: Cash and cash equivalents
- short_term_investments_q: Marketable securities / short-term investments (current)
- accounts_receivable_q: Accounts receivable, net
- inventory_q: Inventories
- prepaid_q: Prepaid expenses and other current assets
- total_current_assets_q: Total current assets
- ppe_q: Property and equipment, net
- operating_lease_assets_q: Operating lease right-of-use assets
- goodwill_q: Goodwill
- intangibles_q: Intangible assets, net (excluding goodwill)
- deferred_tax_assets_q: Deferred income tax assets
- other_noncurrent_assets_q: Other non-current assets
- total_assets_q: Total assets
- accounts_payable_q: Accounts payable
  AMBIGUITY: Must be PURE trade AP. If BS combines AP with accrued, find breakout in notes.
- accrued_liabilities_q: Accrued and other current liabilities
- short_term_debt_q: Short-term debt / current portion of LT debt. 0 if none (confirm in notes).
  AMBIGUITY: May not be a separate BS line. Check notes.
- total_current_liabilities_q: Total current liabilities
- long_term_debt_q: Long-term debt (non-current)
- long_term_lease_liabilities_q: Long-term operating lease liabilities (non-current BS line)
- other_noncurrent_liabilities_q: Other non-current liabilities
- total_liabilities_q: Total liabilities
- operating_lease_liabilities_q: TOTAL operating lease liabilities (current + non-current)
  AMBIGUITY: Current portion often hidden in accrued liabilities. Find in notes. No double counting.
- equity_q: Total stockholders equity (parent only, not incl NCI)
  AMBIGUITY: If BS shows both stockholders equity and total equity incl NCI, use stockholders.
- total_liabilities_and_equity_q: Total liabilities and stockholders equity

## CASH FLOW - OPERATING FIELDS
Use CF PRESENTATION signs. All items should sum to cfo_q using + only.

- cf_net_income_q: Net income (CF starting line — must equal net_income_q)
- sbc_q: Stock-based compensation (positive add-back)
- dna_q: Depreciation and amortization (positive add-back)
- gains_losses_investments_q: Gains/losses on investments, net. Gains are NEGATIVE (removed from income). Losses are POSITIVE (added back).
- deferred_taxes_q: Deferred income taxes (as shown on CF)
- acquisition_termination_cf_q: Acquisition termination cost CF add-back (0 if not present)
- other_noncash_q: Other non-cash adjustments (as shown on CF)
- change_ar_q: Change in accounts receivable (negative = AR increased = use of cash)
- change_inventory_q: Change in inventories (positive = inventory decreased = source of cash)
- change_prepaid_q: Change in prepaid/other assets
- change_ap_q: Change in accounts payable
- change_accrued_q: Change in accrued liabilities
- change_other_lt_liabilities_q: Change in other long-term liabilities
- cfo_q: Net cash provided by operating activities

## CASH FLOW - INVESTING FIELDS
Use CF PRESENTATION signs. All items should sum to cfi_q using + only.

- proceeds_maturities_q: Proceeds from maturities of marketable securities (positive)
- proceeds_sales_securities_q: Proceeds from sales of marketable securities (positive)
- proceeds_sales_equity_q: Proceeds from sales of non-marketable equity securities (positive, 0 if none)
- purchases_securities_q: Purchases of marketable securities (NEGATIVE)
- capex_q: Purchases of PP&E and intangible assets (NEGATIVE)
  AMBIGUITY: May include intangibles. May be split. Use calculation_components.capex if available.
- purchases_equity_investments_q: Purchases of non-marketable equity securities (NEGATIVE, 0 if none)
- acquisitions_q: Acquisitions net of cash acquired (NEGATIVE). Sum all acquisition lines.
  AMBIGUITY: May be multiple lines. Use calculation_components.acquisitions.total if available.
- other_investing_q: Other investing activities, net (0 if none)
- cfi_q: Net cash used in investing activities

## CASH FLOW - FINANCING FIELDS
Use CF PRESENTATION signs. All items should sum to cff_q using + only.

- proceeds_stock_plans_q: Proceeds from employee stock plans (positive)
- share_repurchases_q: Payments for repurchases of common stock (NEGATIVE)
- tax_withholding_q: Payments for employee stock plan taxes (NEGATIVE)
- dividends_q: Dividends paid (NEGATIVE)
- principal_payments_ppe_q: Principal payments on financed PP&E (NEGATIVE, 0 if none)
- debt_repayment_q: Repayment of debt (NEGATIVE, 0 if none)
- debt_issuance_q: Issuance of debt (positive, 0 if none)
- cff_q: Net cash used in financing activities

## CASH FLOW - SUMMARY

- net_change_cash_q: Change in cash and cash equivalents
- taxes_paid_q: Cash paid for income taxes (supplemental, 0 if not disclosed)

## VALIDATION CHECKS (perform these, report results)

1. IS: revenue_q - cogs_q = gross_profit_q. Flag if not.
2. IS: gross_profit_q - total_opex_q = operating_income_q. Flag if not.
3. IS: pretax_income_q - income_tax_expense_q = net_income_q. Flag if not.
4. BS: total_assets_q = total_liabilities_and_equity_q. Flag if not.
5. CF: sum of all CFO items = cfo_q. Flag if not.
6. CF: sum of all CFI items = cfi_q. Flag if not.
7. CF: sum of all CFF items = cff_q. Flag if not.
8. CF: cfo_q + cfi_q + cff_q = net_change_cash_q. Flag if not.
9. Cross: net_income_q = cf_net_income_q. Flag if not.
10. Tax rate: effective_tax_rate = income_tax_expense_q / pretax_income_q. Flag if < 0% or > 50%.
11. Operating lease: current + non-current = operating_lease_liabilities_q. No double counting.

## SIGN CONVENTIONS

- Revenue, income, assets, equity: POSITIVE
- Expenses (COGS, R&D, SGA, tax): POSITIVE
- Interest expense: NEGATIVE
- ALL CF items: use PRESENTATION sign (as shown on the cash flow statement)
  - Positive = source of cash, Negative = use of cash
  - Capex, acquisitions, repurchases, dividends: NEGATIVE
  - Proceeds, add-backs: POSITIVE
- Short-term debt: 0 if none exists

## UNITS

ALL monetary values in RAW dollars (not millions). Multiply extraction values (in millions) by 1,000,000.
Shares in RAW count. Multiply millions by 1,000,000.
EPS: keep as reported.

## PERIOD SELECTION

- 10-Q Q1: IS quarterly, CF quarterly, BS quarter-end. cf_is_ytd = false.
- 10-Q Q2/Q3: IS quarterly (3-month column), CF YTD. BS quarter-end. cf_is_ytd = true.
- 10-K: IS annual, CF annual, BS year-end. cf_is_ytd = false.
- The caller handles YTD-to-quarterly derivation and Q4 derivation.

## OUTPUT FORMAT

Output ONLY valid JSON:
{
  "period_end": "2025-04-27",
  "period_start": "2025-01-27",
  "form": "10-Q",
  "fields": {
    "revenue_q": 44062000000,
    "cogs_q": 17394000000,
    ...
  },
  "cf_is_ytd": false,
  "effective_tax_rate": 0.143,
  "validation": {
    "is_gross_profit_ok": true,
    "is_operating_income_ok": true,
    "is_net_income_ok": true,
    "bs_balances": true,
    "cfo_sums": true,
    "cfi_sums": true,
    "cff_sums": true,
    "net_cash_ok": true,
    "cross_ni_ok": true,
    "tax_rate_ok": true,
    "operating_lease_ok": true
  },
  "notes": ["any ambiguity resolutions or flags"]
}

If a field does not apply (e.g., no acquisitions, no debt issuance), set to 0. Output must be valid JSON.
"""


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
    extract_dir = os.path.join(os.path.dirname(__file__), ticker)
    mapped_path = os.path.join(extract_dir, 'mapped.json')
    formulas_md = load_formulas_md()

    with open(mapped_path) as f:
        mapped_data = json.load(f)

    mapped_json_str = json.dumps(mapped_data, indent=2)
    print(f"  mapped.json: {len(mapped_data)} periods, ~{len(mapped_json_str)//4:,} tokens")

    prompt = f"""You are a financial analyst normalizing extracted data across all filings for {ticker}.

You have two inputs:
1. Verified extractions from ALL filings, organized by period (mapped.json). Each period contains line_items, formulas, and xbrl_not_on_statement for IS/BS/CF — all verified with math checks.
2. The metric formulas that will be computed from this data.

Your job: produce one normalized analytical dataset across all periods.

READ ALL PERIODS FIRST. Before producing any output, review every period's extraction to understand this company's reporting structure — what it reports, how it names things, what changes between filings, where items are hidden.

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

If you pick the wrong value (e.g., YTD revenue instead of quarterly, or quarterly CF instead of YTD), the derivation will produce impossible results (negative revenue, negative CFO for a profitable company). The math will catch it.

Handle these cross-period issues:
- STOCK SPLITS: if diluted shares jumps by a large multiple between periods, normalize all pre-split periods to post-split basis (divide shares, multiply EPS)
- REPORTING CHANGES: if the company changed its structure between filings (new segments, renamed items), map both sides consistently
- SIGN CONVENTIONS: ensure signs are consistent across all periods for each field

Determine the reporting unit from the extraction data and convert all values to RAW dollars. Do not assume any unit — check the values to determine the scale.

METRIC FORMULAS:
{formulas_md}

EXTRACTION DATA (ALL PERIODS):
{mapped_json_str}

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


def map_filing_v2(extraction_json, ticker, model='claude-sonnet-4-6'):
    """Send a per-filing extraction to AI for normalization + analytical derivation. (LEGACY v2)"""
    ai = extraction_json.get('ai_extraction', extraction_json)
    formulas_md = load_formulas_md()

    prompt = f"""You are a financial analyst preparing extracted data for metric calculations.

You have two inputs:
1. The verified extraction from a {ticker} filing
2. The metric formulas that will be computed from this data (below)

Your job has two parts:

PART 1 — AS-REPORTED NORMALIZATION:
Normalize the three financial statements (IS, BS, CF) so every item has a consistent name. Use the data exactly as reported — do not adjust values. If accrued liabilities includes operating lease current, leave it as-is. The quarterly derivation (Stage 3) needs the as-reported numbers so YTD subtraction works correctly.

PART 2 — ANALYTICAL DERIVATIONS:
Read the metric formulas below. Determine where each standard analytical input comes from in this company's data. These are SEPARATE derived values, not replacements for the as-reported data.

Some inputs map directly to a statement line item. Others require combining items (e.g., total operating lease liabilities = current from notes + non-current from BS). Others may not exist for this company (e.g., no COGS for a software company — set to null).

Find hidden items in the extraction's xbrl_not_on_statement data. For each input, document the mapping in company_mapping.

Determine the reporting unit from the extraction data and convert all values to RAW dollars. Do not assume any unit — check the extraction values and the XBRL data to determine the scale.

STANDARD ANALYTICAL FIELD NAMES (use these exact keys in the analytical section):
- revenue: total revenue / net revenue
- cogs: cost of revenue / cost of goods sold (null if not reported)
- gross_profit: gross profit (null if no COGS)
- operating_income: operating income / income from operations
- pretax_income: income before income taxes
- income_tax_expense: income tax provision
- net_income: net income (consolidated, same as CF starting line)
- interest_expense: GROSS interest expense as a NEGATIVE number (find gross in notes if IS reports net)
- sbc: stock-based compensation (CF addback, positive)
- dna: depreciation and amortization (CF addback, positive, sum all D&A components)
- diluted_shares: diluted weighted average shares (raw count, not millions)
- cash: cash and cash equivalents
- short_term_investments: marketable securities / short-term investments (0 if none)
- accounts_receivable: accounts receivable, net (pure trade AR)
- inventory: inventories (0 if not applicable)
- accounts_payable: accounts payable (pure trade AP — find breakout in notes if combined with accrued)
- total_assets: total assets
- equity: total stockholders equity (parent only, not including NCI)
- short_term_debt: current debt + commercial paper + short-term borrowings (0 if none)
- long_term_debt: long-term debt (non-current)
- operating_lease_liabilities: TOTAL operating lease liabilities (current + non-current, no double counting)
- cfo: net cash from operating activities
- capex: capital expenditures as a NEGATIVE number
- acquisitions: acquisitions net of cash as a NEGATIVE number (0 if none)
- rd_expense: research and development expense (null if not reported)

Include any additional analytical items the metric formulas require beyond this list (e.g., inventory breakdown, SBC by function, segment data). Use descriptive field names for those.

METRIC FORMULAS:
{formulas_md}

EXTRACTION DATA:
{json.dumps(ai, indent=2)}

OUTPUT FORMAT:

Output ONLY valid JSON:
{{
  "ticker": "{ticker}",
  "reporting_unit": "description of what unit the filing reports in and how you converted",
  "company_mapping": {{
    "analytical_field_name": "which XBRL concept(s) or line items it maps to, and why"
  }},
  "period_end": "YYYY-MM-DD",
  "period_start": "YYYY-MM-DD",
  "form": "10-Q or 10-K",
  "cf_is_ytd": false,
  "as_reported": {{
    "income_statement": {{
      "label_name": value,
      ...
    }},
    "balance_sheet": {{
      "label_name": value,
      ...
    }},
    "cash_flow": {{
      "label_name": value,
      ...
    }}
  }},
  "analytical": {{
    "revenue": value,
    "operating_income": value,
    "cfo": value,
    ...
  }},
  "segments": {{ ... }}
}}

All monetary values in RAW dollars after conversion.
Shares in raw count.
EPS as reported.

PERIOD SELECTION:
- 10-Q Q1: IS quarterly, CF quarterly, BS quarter-end
- 10-Q Q2/Q3: IS quarterly (3-month column), CF is YTD (set cf_is_ytd: true)
- 10-K: IS annual, CF annual, BS year-end
- The caller handles YTD-to-quarterly derivation and Q4 derivation

CRITICAL: Output must be valid JSON. No apostrophes in strings."""

    client = anthropic.Anthropic()
    output_text = ""

    with client.messages.stream(
        model=model,
        max_tokens=16384,
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


def map_filing(extraction_json, ticker, model='claude-sonnet-4-6'):
    """Send a per-filing extraction to AI for analytical component extraction. (LEGACY)"""
    ai = extraction_json.get('ai_extraction', extraction_json)

    prompt = f"""Extract the 23 analytical components from this {ticker} filing extraction.

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
            elif field in ('cf_net_income_q', 'sbc_q', 'dna_q', 'gains_losses_investments_q',
                    'deferred_taxes_q', 'acquisition_termination_cf_q', 'other_noncash_q',
                    'change_ar_q', 'change_inventory_q', 'change_prepaid_q',
                    'change_ap_q', 'change_accrued_q', 'change_other_lt_liabilities_q', 'cfo_q',
                    'proceeds_maturities_q', 'proceeds_sales_securities_q', 'proceeds_sales_equity_q',
                    'purchases_securities_q', 'capex_q', 'purchases_equity_investments_q',
                    'acquisitions_q', 'other_investing_q', 'cfi_q',
                    'proceeds_stock_plans_q', 'share_repurchases_q', 'tax_withholding_q',
                    'dividends_q', 'principal_payments_ppe_q', 'debt_repayment_q',
                    'debt_issuance_q', 'cff_q', 'net_change_cash_q', 'taxes_paid_q'):
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
    parser.add_argument('--output', help='Output file path')
    parser.add_argument('--from-mapped', action='store_true',
                        help='Skip AI calls, use existing formula_mapped.json for quarterly derivation only')
    parser.add_argument('--v2', action='store_true',
                        help='Use per-filing linkbase-based prompt (LEGACY)')
    parser.add_argument('--v3', action='store_true',
                        help='Use all-periods normalization (reads mapped.json, verifies with quarterly derivation)')
    parser.add_argument('--filing', help='Process a single filing JSON (for --v2 testing)')
    parser.add_argument('--test', action='store_true',
                        help='Write output to test/ subdirectory')
    args = parser.parse_args()

    extract_dir = f'ai_extract/{args.ticker}'
    mapped_path = os.path.join(extract_dir, 'mapped.json')
    formula_mapped_path = os.path.join(extract_dir, 'formula_mapped.json')

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

    elif args.v2 and args.filing:
        # V2: process a single filing with new prompt
        print(f"V2: Processing {args.filing}...")
        with open(args.filing) as f:
            extraction = json.load(f)

        result, in_tok, out_tok = map_filing_v2(extraction, args.ticker, args.model)

        pe = result.get('period_end', '?')
        form = result.get('form', '?')
        unit = result.get('reporting_unit', '?')

        print(f"\n  {pe} ({form}), reporting unit: {unit}")
        print(f"  Company mapping: {len(result.get('company_mapping', {}))} items")

        as_rep = result.get('as_reported', {})
        for stmt in ['income_statement', 'balance_sheet', 'cash_flow']:
            n = len(as_rep.get(stmt, {}))
            print(f"  as_reported.{stmt}: {n} items")

        analytical = result.get('analytical', {})
        print(f"  analytical: {len(analytical)} items")
        for k, v in analytical.items():
            if isinstance(v, (int, float)):
                print(f"    {k}: {v/1e6:,.0f}M" if abs(v) > 1e6 else f"    {k}: {v}")

        segments = result.get('segments', {})
        if segments:
            print(f"  segments: {len(segments)} dimensions")

        # Save output
        test_dir = os.path.join(extract_dir, 'test') if args.test else extract_dir
        os.makedirs(test_dir, exist_ok=True)
        out_path = args.output or os.path.join(test_dir, 'formula_v2_' + os.path.basename(args.filing))
        with open(out_path, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"\nSaved to {out_path}")

        # Cost
        in_rate, out_rate = 3.0, 15.0
        if 'opus' in (args.model or ''):
            in_rate, out_rate = 15.0, 75.0
        input_cost = in_tok * in_rate / 1_000_000
        output_cost = out_tok * out_rate / 1_000_000
        print(f"Tokens: {in_tok:,} in, {out_tok:,} out")
        print(f"Cost: ${input_cost:.2f} input + ${output_cost:.2f} output = ${input_cost + output_cost:.2f}")
        return

    if args.from_mapped:
        # Skip AI, just derive quarterly from existing formula_mapped.json
        print(f"Reading {formula_mapped_path}...")
        with open(formula_mapped_path) as f:
            all_results = json.load(f)
        total_in = total_out = 0
    else:
        import glob
        # Find all per-filing JSONs
        filing_paths = sorted(glob.glob(os.path.join(extract_dir, 'q*_fy*_10*.json')))
        if not filing_paths:
            print(f"Error: no per-filing JSONs found in {extract_dir}. Run Stage 1 first.")
            sys.exit(1)

        print(f"Found {len(filing_paths)} per-filing JSONs")

        all_results = []
        total_in = 0
        total_out = 0

        for filepath in filing_paths:
            fname = os.path.basename(filepath)
            print(f"\nMapping {fname}...")

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
