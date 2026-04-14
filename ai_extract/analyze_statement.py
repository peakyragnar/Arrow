"""
AI-powered financial statement analysis.

Sends the full filing HTML + XBRL facts to Claude. AI does the extraction,
mapping, and cross-referencing. The only deterministic code is arithmetic
verification of the formulas the AI reports.

Usage:
    python3 ai_extract/analyze_statement.py --ticker NVDA --accession 0001045810-25-000116 --statement income
"""

import argparse
import json
import os
import sys

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

import anthropic

from parse_xbrl import parse_filing


def clean_html(html):
    """Strip CSS styling and layout noise from iXBRL HTML. Keeps all tags, text, and ix: elements."""
    import re
    # Strip <style> blocks
    cleaned = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL)
    # Strip inline style attributes
    cleaned = re.sub(r'\s+style="[^"]*"', '', cleaned)
    # Collapse whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned)
    return cleaned


def build_prompt(html_content, xbrl_facts, statement_type, meta):
    """Build the prompt for Claude."""

    # Format XBRL facts as a readable list
    facts_text = "XBRL TAGGED FACTS (all undimensioned facts from this filing):\n"
    seen = set()
    for f in xbrl_facts:
        key = (f['concept'], f['context_ref'])
        if key in seen:
            continue
        seen.add(key)

        period_str = ''
        if f['period']:
            if f['period']['type'] == 'duration':
                period_str = f"{f['period']['startDate']} to {f['period']['endDate']}"
            else:
                period_str = f"as of {f['period']['date']}"

        val = f['value_raw']
        unit = f['unit'] or ''
        if f['value_numeric'] is not None:
            if 'USD' in unit and 'shares' not in unit:
                val = f"${f['value_numeric']/1e6:,.0f}M"
            elif 'shares' in unit and 'USD' not in unit:
                val = f"{f['value_numeric']/1e6:,.0f}M shares"
            elif 'USD' in unit and 'shares' in unit:
                val = f"${f['value_numeric']}/share"

        facts_text += f"  {f['concept']} [{period_str}] ({unit}) = {val}\n"

    if statement_type == 'income':
        task = """You are a data extraction tool. You have two inputs:
1. The full HTML filing (find the income statement within it)
2. The XBRL-tagged facts (the raw structured data)

Output ONLY valid JSON with this structure:

{
  "line_items": [
    {
      "label": "Revenue",
      "indent_level": 0,
      "xbrl_concept": "us-gaap:Revenues",
      "values": {"2025-01-27_2025-04-27": 44062, "2024-01-29_2024-04-28": 26044},
      "unit": "USD_millions",
      "xbrl_match": true,
      "mapping_reason": "HTML table row text is 'Revenue'. The ix:nonFraction tag on this cell has name='us-gaap:Revenues' with contextRef='c-1' and value 44062 (scale=6). XBRL fact list confirms us-gaap:Revenues = $44,062M for this period."
    }
  ],
  "formulas": [
    {
      "formula": "Revenue - Cost of Revenue = Gross Profit",
      "components": ["Revenue", "Cost of Revenue", "Gross Profit"],
      "operation": "Revenue - Cost of Revenue",
      "result_label": "Gross Profit"
    }
  ],
  "xbrl_not_on_statement": [
    {
      "concept": "us-gaap:DepreciationDepletionAndAmortization",
      "value": 611,
      "period": "2025-01-27_2025-04-27",
      "reason": "D&A is tagged in XBRL but does not appear as a line item on the income statement. Likely disclosed in notes or cash flow statement."
    }
  ]
}

Rules:
- line_items: every line item on the income statement, in presentation order.
- indent_level: hierarchy depth (0=top level like Revenue, Gross Profit; 1=sub-item like R&D under Operating Expenses).
- xbrl_concept: the XBRL concept name. Get this from the ix:nonFraction tag in the HTML, and cross-reference against the XBRL facts list.
- values: XBRL values are the source of truth for sign and magnitude. Use the XBRL fact value, not the HTML display value. HTML may show parentheses or negative signs for presentation, but the XBRL taxonomy defines the canonical sign convention (e.g., expenses are positive debits). When you find a matching XBRL fact, use its value directly. Key is period start_end date range.
- unit: one of "USD_millions", "USD_per_share", or "shares_millions".
- xbrl_match: did the value from the ix:nonFraction tag in the HTML match the corresponding XBRL fact? true/false. If false, explain in mapping_reason.
- mapping_reason: explain HOW you matched this line item to this XBRL concept. What did you see in the HTML? What tag confirmed it? Did the XBRL fact value match? This is for audit — be specific.
- formulas: every subtotal relationship on the statement. The operation must use the correct arithmetic operator (+ or -) to make the formula work with the XBRL sign convention. For example, if Interest Expense is 63 (positive in XBRL) and it reduces income, the formula is "Operating Income - Interest Expense", not "Operating Income + Interest Expense". Do NOT compute the math — the verification script will do that independently.
- xbrl_not_on_statement: review the XBRL facts list and identify concepts that are income-statement-related (revenue, expenses, income, gains, losses, SBC, D&A, etc.) but do NOT appear as line items on the rendered income statement. For each, give the concept, value, period, and a brief reason explaining where this item likely lives (notes, cash flow, etc.). This helps identify items that are aggregated or hidden.
- Do NOT include analysis, observations, or commentary.
- Do NOT verify the math yourself — just report what you found."""

    elif statement_type == 'balance_sheet':
        task = """You are a data extraction tool. You have two inputs:
1. The full HTML filing (find the balance sheet within it)
2. The XBRL-tagged facts (the raw structured data)

Output ONLY valid JSON with this structure:

{
  "line_items": [
    {
      "label": "Cash and cash equivalents",
      "indent_level": 1,
      "xbrl_concept": "us-gaap:CashAndCashEquivalentsAtCarryingValue",
      "values": {"2025-04-27": 15176, "2025-01-26": 8495},
      "unit": "USD_millions",
      "xbrl_match": true,
      "mapping_reason": "HTML row text is 'Cash and cash equivalents'. The ix:nonFraction tag has name='us-gaap:CashAndCashEquivalentsAtCarryingValue' with contextRef='c-4' (instant 2025-04-27) and value 15176. XBRL fact confirms."
    }
  ],
  "formulas": [
    {
      "formula": "Total current assets + Total non-current assets = Total assets",
      "components": ["Total current assets", "Total non-current assets", "Total assets"],
      "operation": "Total current assets + Total non-current assets",
      "result_label": "Total assets"
    }
  ],
  "xbrl_not_on_statement": [
    {
      "concept": "us-gaap:OperatingLeaseLiabilityCurrent",
      "value": 200,
      "period": "2025-04-27",
      "reason": "Current operating lease liabilities are tagged in XBRL but do not appear as a separate balance sheet line item. Likely included in 'Accrued and other current liabilities' and disclosed in notes."
    }
  ]
}

Rules:
- line_items: every line item on the balance sheet, in presentation order.
- indent_level: hierarchy depth (0=section headers like Total assets; 1=sub-section like Current assets items; 2=sub-sub-items).
- xbrl_concept: the XBRL concept name. Get this from the ix:nonFraction tag in the HTML, and cross-reference against the XBRL facts list.
- values: XBRL values are the source of truth. Balance sheet items are instant (point-in-time), so use the date as key (e.g., "2025-04-27"), not a date range. Report in millions as integers.
- unit: "USD_millions" for all balance sheet items.
- xbrl_match: did the value from the ix:nonFraction tag in the HTML match the corresponding XBRL fact? true/false.
- mapping_reason: explain HOW you matched this line item to this XBRL concept. Be specific about what you saw in the HTML and what tag confirmed it.
- formulas: every subtotal relationship on the balance sheet. Include at minimum:
  - Current asset components summing to Total current assets
  - Non-current asset components summing to Total non-current assets (if shown)
  - Total current assets + non-current assets = Total assets
  - Current liability components summing to Total current liabilities
  - Non-current liability components summing to Total non-current liabilities (if shown)
  - Total liabilities + Total stockholders' equity = Total liabilities and stockholders' equity
  - The fundamental equation: Total assets = Total liabilities and stockholders' equity
  Do NOT compute the math — the verification script will do that independently.
- xbrl_not_on_statement: review the XBRL facts list and identify concepts that are balance-sheet-related but do NOT appear as line items on the rendered balance sheet. These are items aggregated into "other" buckets — for example, operating lease liabilities inside "accrued liabilities", or specific receivable types inside a broader line. For each, give the concept, value, period, and a brief reason explaining where it likely lives. This is critical for identifying what's hidden in aggregated line items.
- Do NOT include analysis, observations, or commentary.
- Do NOT verify the math yourself — just report what you found."""

    elif statement_type == 'cash_flow':
        task = """You are a data extraction tool. You have two inputs:
1. The full HTML filing (find the cash flow statement within it)
2. The XBRL-tagged facts (the raw structured data)

Output ONLY valid JSON with this structure:

{
  "line_items": [
    {
      "label": "Net income",
      "indent_level": 0,
      "xbrl_concept": "us-gaap:NetIncomeLoss",
      "values": {"2025-01-27_2025-04-27": 18775, "2024-01-29_2024-04-28": 14881},
      "unit": "USD_millions",
      "xbrl_match": true,
      "mapping_reason": "HTML row text is 'Net income'. ix:nonFraction tag has name='us-gaap:NetIncomeLoss'. XBRL fact confirms."
    }
  ],
  "formulas": [
    {
      "formula": "Sum of CFO components = Net cash provided by operating activities",
      "components": ["Net income", "Stock-based compensation", "...other adjustments...", "Net cash provided by operating activities"],
      "operation": "Net income + Stock-based compensation + ...other adjustments...",
      "result_label": "Net cash provided by operating activities"
    }
  ],
  "xbrl_not_on_statement": [
    {
      "concept": "us-gaap:SomeConceptNotOnStatement",
      "value": 100,
      "period": "2025-01-27_2025-04-27",
      "reason": "Explanation of where this item lives."
    }
  ]
}

Rules:
- line_items: every line item on the cash flow statement, in presentation order. This includes:
  - Operating activities: Net income, all adjustments (D&A, SBC, deferred taxes, etc.), all working capital changes, and the CFO total.
  - Investing activities: all items and the CFI total.
  - Financing activities: all items and the CFF total.
  - Effect of exchange rate changes (if shown).
  - Net change in cash.
  - Beginning and ending cash balances.
- indent_level: hierarchy depth (0=section totals like Net cash from operating activities; 1=items within a section like D&A, SBC, working capital changes).
- xbrl_concept: the XBRL concept name from the ix:nonFraction tag, cross-referenced against the XBRL facts list.
- values: Use the values as presented on the cash flow statement — positive means source of cash, negative means use of cash. This applies to ALL items including working capital changes, investing items, and financing items. The sign as shown on the statement is what matters. Cash flow items use duration periods, so key is date range (e.g., "2025-01-27_2025-04-27"). Report in millions as integers.
- unit: "USD_millions" for all cash flow items.
- xbrl_match: did the ix:nonFraction tag value match the XBRL fact? true/false.
- mapping_reason: explain HOW you matched this line item. Be specific about what HTML text and XBRL tag confirmed it.
- formulas: every subtotal relationship. The "operation" field MUST use exact label names from line_items connected by + ONLY. NEVER use minus signs in the operation.
  WHY: The values already contain the correct sign. For example, Accounts receivable = -933 means AR increased and used cash. The negative is IN the value. Purchases of marketable securities = -6546 means cash was spent. The negative is IN the value. Dividends paid = -244. The negative is IN the value. So the formula just adds all signed values: "Net income + SBC + D&A + ... + Accounts receivable + Inventories + ..." and the math works because -933 + 1258 + ... naturally nets out. If you write "- Accounts receivable" you are negating -933 to get +933, which is wrong.
  Include at minimum:
  - All CFO components summed = CFO total (all +, no -)
  - All CFI components summed = CFI total (all +, no -)
  - All CFF components summed = CFF total (all +, no -)
  - CFO + CFI + CFF = Net change in cash
  - Beginning cash + Net change = Ending cash (use the EXACT label names from line_items)
  Do NOT compute the math — the verification script will do that independently.
- xbrl_not_on_statement: XBRL facts that are cash-flow-related but do NOT appear as line items on the rendered statement. These may be sub-components disclosed in notes.
- Do NOT include analysis, observations, or commentary.
- Do NOT verify the math yourself — just report what you found."""

    elif statement_type == 'all':
        task = """You are a data extraction tool. You have two inputs:
1. The full HTML filing (find all three financial statements: income statement, balance sheet, cash flow statement)
2. The XBRL-tagged facts (the raw structured data)

Output ONLY valid JSON with this structure:

{
  "income_statement": {
    "line_items": [...],
    "formulas": [...],
    "xbrl_not_on_statement": [...]
  },
  "balance_sheet": {
    "line_items": [...],
    "formulas": [...],
    "xbrl_not_on_statement": [...]
  },
  "cash_flow": {
    "line_items": [...],
    "formulas": [...],
    "xbrl_not_on_statement": [...]
  },
  "cross_statement_checks": [
    {
      "check": "Net income on IS matches Net income on CF",
      "is_value": 18775,
      "cf_value": 18775,
      "match": true
    }
  ]
}

Each statement section follows the same format:

line_items: every line item on the statement, in presentation order.
- label: the line item text as shown
- indent_level: hierarchy depth (0=top level/totals, 1=sub-items, 2=sub-sub-items)
- xbrl_concept: the XBRL concept name from the ix:nonFraction tag, cross-referenced against XBRL facts
- values: XBRL values are the source of truth for sign and magnitude. Use XBRL fact values, not HTML display values. HTML may show parentheses for presentation but XBRL defines canonical sign convention (expenses are positive debits). Key format: for duration items use "startDate_endDate", for instant items use just the date.
- unit: one of "USD_millions", "USD_per_share", or "shares_millions"
- xbrl_match: did the ix:nonFraction tag value match the XBRL fact? true/false. For section headers with no numeric values, set to null (not false).
- mapping_reason: explain HOW you matched this line item to the XBRL concept. Be specific about HTML text and tags.

formulas: every subtotal relationship. CRITICAL — each formula MUST use this exact structure:
  {"formula": "human readable description", "components": ["Label A", "Label B", "Label C"], "operation": "Label A + Label B", "result_label": "Label C"}
  The "operation" field MUST be a math expression using exact label names from line_items connected by + and - operators. Example: "Revenue - Cost of revenue" NOT {"operation": "subtract", "operands": [...]}. The verification script evaluates the operation string by substituting label names with values.
- Income statement: Revenue - COGS = Gross Profit, opex sums, operating income, other income sums, pretax, net income
- Balance sheet: current asset sums, total assets, current liability sums, total liabilities, equity sums, assets = liabilities + equity
- Cash flow: CFO components sum, CFI components sum, CFF components sum, CFO+CFI+CFF = change in cash, beginning + change = ending cash

xbrl_not_on_statement: XBRL facts related to that statement but NOT appearing as line items. Include concept, value, period, and reason explaining where it likely lives.

cross_statement_checks: verify these ties between statements:
- Net income on IS = Net income starting CF
- Ending cash on CF = Cash on BS (current period)
- Beginning cash on CF = Cash on BS (prior period)
- Retained earnings change on BS = Net income - Dividends - Share repurchases + any other items charged to retained earnings. Account for ALL items that affect retained earnings, not just net income and dividends.

## LAYER 2 — CALCULATION COMPONENT VERIFICATION

After extracting the three statements, search the ENTIRE filing (statement face, notes, supplemental disclosures, dimensioned XBRL facts) to ensure all components needed for downstream calculations are captured. Do not assume a component is absent — actively look for it.

Add a "calculation_components" section to your output with these items:

1. OPERATING LEASES: Find BOTH current and non-current operating lease liabilities.
   - Current portion is often HIDDEN inside "Accrued and other current liabilities." Check the notes.
   - Check for XBRL tag OperatingLeaseLiabilityCurrentStatementOfFinancialPositionExtensibleList — it tells you where current portion is classified.
   - If a total OperatingLeaseLiability exists, use it. If only the split exists, sum current + non-current.
   - If this is a 10-Q and you cannot find them, flag it.
   Output: {"current": X, "noncurrent": X, "total": X, "current_location": "where found"}

2. DEPRECIATION AND AMORTIZATION: Check CF statement for ALL D&A-related lines.
   - May be one line or SPLIT into: depreciation (PP&E), amortization of intangibles, amortization of debt costs, capitalized contract cost amortization, depletion (mining companies).
   - Search for every CF line containing "depreci", "amortiz", or "deplet".
   - Report each component and the total. Also check notes for breakdowns.
   Output: {"total": X, "components": [{"label": "...", "value": X}], "is_single_line": true/false}

3. ACCOUNTS PAYABLE: Must be PURE trade AP, not combined with accrued liabilities.
   - If BS shows "Accounts payable and accrued liabilities" combined — find pure AP in the notes.
   - Check for dimensioned contexts (related vs non-related party splits).
   Output: {"value": X, "is_pure": true/false, "combined_with": null or "description", "note_breakout": X or null}

4. ACCOUNTS RECEIVABLE: Must be pure trade AR.
   - If combined with other receivables, find pure AR in notes.
   - Check for dimensioned contexts.
   Output: {"value": X, "is_pure": true/false, "combined_with": null or "description", "note_breakout": X or null}

5. CAPEX: Capital expenditures on PP&E and intangible assets.
   - Concept names vary between companies and years (PaymentsToAcquirePropertyPlantAndEquipment vs PaymentsToAcquireProductiveAssets etc).
   - Check supplemental disclosures for "Capital expenditures incurred but not yet paid."
   Output: {"cf_value": X, "supplemental_not_yet_paid": X or null, "includes_intangibles": true/false}

6. ACQUISITIONS: Cash paid for acquisitions net of cash acquired.
   - May have MULTIPLE acquisition lines in same period. Sum all.
   - Search both us-gaap and company extension namespaces for concepts containing "acquire" or "acquisition."
   Output: {"total": X, "items": [{"concept": "...", "value": X}]}

7. SHORT-TERM DEBT: Current portion of long-term debt, commercial paper, notes payable, short-term borrowings.
   - If none found, confirm truly zero.
   Output: {"value": X, "components": [...], "confirmed_zero": true/false}

8. SBC: Stock-based compensation from CF statement addback.
   - Also report the note breakdown by function if disclosed.
   Output: {"cf_value": X, "note_by_function": {...} or null}

9. INTEREST EXPENSE: Must be GROSS interest expense, not net of interest income.
   - If IS shows "Interest expense, net" — find gross in notes.
   Output: {"gross": X, "income": X, "net": X, "source": "description"}

10. TAX RATE: Income tax expense and pretax income.
    - FLAG if pretax income is negative (use 21% fallback).
    - FLAG if tax expense is negative (refund).
    Output: {"tax_expense": X, "pretax_income": X, "effective_rate": X, "flags": [...]}

11. INVENTORY: Total and breakdown if disclosed.
    - If company has no inventory (software/services), confirm truly zero.
    Output: {"total": X, "raw_materials": X or null, "wip": X or null, "finished_goods": X or null}

Use values from the CURRENT period (most recent quarter-end for BS items, current quarter/YTD for flow items).

Do NOT include analysis, observations, or commentary.
Do NOT verify the math yourself — just report what you found.
CRITICAL: Output must be valid JSON. In ALL string values, never use apostrophes or single quotes. Use "shareholders equity" not "shareholders' equity". Use "does not" not "doesn't". This applies to labels, mapping_reason, and all other string fields."""

    prompt = f"""You are analyzing a {meta['form']} filing for {meta['ticker']}.
Report date: {meta['report_date']}. Filing date: {meta['filing_date']}.

{task}

---
{facts_text}
---
FULL FILING HTML:
{html_content}
"""
    return prompt


def verify_formulas(ai_result):
    """
    Pure arithmetic verification of the formulas the AI reported.
    No XBRL lookups, no concept matching — just math on the AI's own numbers.
    """
    line_items = ai_result.get('line_items', [])
    formulas = ai_result.get('formulas', [])

    # Build lookup: label -> values
    label_values = {}
    for item in line_items:
        label_values[item['label']] = item.get('values', {})

    formula_checks = []
    for formula in formulas:
        operation = formula.get('operation', '')
        result_label = formula.get('result_label', '')

        result_values = label_values.get(result_label, {})
        periods_checked = {}
        all_pass = True

        for period in result_values:
            stated = result_values[period]
            computed = _evaluate_formula(operation, label_values, period)
            match = computed is not None and computed == stated
            if not match:
                all_pass = False
            periods_checked[period] = {
                'computed': computed,
                'stated': stated,
                'computation': _format_computation(operation, label_values, period),
            }

        formula_checks.append({
            'formula': formula.get('formula', ''),
            'pass': all_pass,
            'periods': periods_checked,
        })

    formulas_pass = sum(1 for f in formula_checks if f['pass'])
    return {
        'formula_checks': formula_checks,
        'formulas_pass': formulas_pass,
        'formulas_total': len(formula_checks),
    }


def _evaluate_formula(operation, label_values, period):
    """Evaluate a formula string like 'Revenue - Cost of Revenue' using extracted values."""
    try:
        expr = operation
        labels_by_length = sorted(label_values.keys(), key=len, reverse=True)
        for label in labels_by_length:
            if label in expr:
                val = label_values.get(label, {}).get(period)
                if val is None:
                    return None
                expr = expr.replace(label, str(val))
        result = eval(expr)
        return int(result) if isinstance(result, float) and result == int(result) else result
    except Exception:
        return None


def _format_computation(operation, label_values, period):
    """Format a computation showing the actual numbers."""
    expr = operation
    labels_by_length = sorted(label_values.keys(), key=len, reverse=True)
    for label in labels_by_length:
        if label in expr:
            val = label_values.get(label, {}).get(period, '?')
            expr = expr.replace(label, f"{val}")
    return expr


def main():
    parser = argparse.ArgumentParser(description='AI-powered financial statement analysis')
    parser.add_argument('--ticker', required=True)
    parser.add_argument('--accession', required=True)
    parser.add_argument('--statement', default='income', choices=['income', 'balance_sheet', 'cash_flow', 'all'])
    parser.add_argument('--output', help='Save output to file')
    parser.add_argument('--model', default='claude-sonnet-4-6', help='Model to use (claude-sonnet-4-6, claude-opus-4-6, gemini-3-flash-preview, gpt-5, etc.)')
    args = parser.parse_args()

    # Step 1: Parse XBRL facts
    print(f"Parsing XBRL facts for {args.ticker} / {args.accession}...")
    parsed = parse_filing(args.ticker, args.accession)
    print(f"  {parsed['total_facts']} total facts, {parsed['total_contexts']} contexts")

    # Step 2: Load full HTML
    base_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'filings', args.ticker, args.accession)
    meta_path = os.path.join(base_dir, 'filing_meta.json')
    with open(meta_path) as f:
        meta = json.load(f)

    html_path = os.path.join(base_dir, meta['html_filename'])
    with open(html_path) as f:
        html_content = f.read()
    html_cleaned = clean_html(html_content)
    print(f"  Full HTML: {len(html_content):,} chars -> cleaned: {len(html_cleaned):,} chars (~{len(html_cleaned)//4:,} tokens)")

    # Step 3: Filter XBRL facts (undimensioned only)
    xbrl_facts = [f for f in parsed['facts'] if not f['dimensioned']]
    print(f"  {len(xbrl_facts)} undimensioned XBRL facts")

    # Step 4: Build prompt and call model
    prompt = build_prompt(html_cleaned, xbrl_facts, args.statement, meta)
    print(f"\nTotal prompt size: ~{len(prompt)//4} tokens")
    print(f"Sending to {args.model}...\n")

    output_text = ""
    input_tokens = 0
    output_tokens = 0

    if args.model.startswith('gemini'):
        from google import genai
        gemini_client = genai.Client(api_key=os.environ.get('GEMINI_API_KEY'))
        response = gemini_client.models.generate_content(
            model=args.model,
            contents=prompt,
            config=genai.types.GenerateContentConfig(max_output_tokens=32768),
        )
        output_text = response.text
        input_tokens = response.usage_metadata.prompt_token_count
        output_tokens = response.usage_metadata.candidates_token_count
    elif args.model.startswith('gpt'):
        from openai import OpenAI
        oai_client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
        response = oai_client.chat.completions.create(
            model=args.model,
            max_completion_tokens=32768,
            messages=[{"role": "user", "content": prompt}],
        )
        output_text = response.choices[0].message.content
        input_tokens = response.usage.prompt_tokens
        output_tokens = response.usage.completion_tokens
    else:
        client = anthropic.Anthropic()
        with client.messages.stream(
            model=args.model,
            max_tokens=32768,
            messages=[{"role": "user", "content": prompt}],
        ) as stream:
            for text in stream.text_stream:
                output_text += text
                print(".", end="", flush=True)
            print()
            resp = stream.get_final_message()
            input_tokens = resp.usage.input_tokens
            output_tokens = resp.usage.output_tokens

    # Parse the JSON from the AI response
    json_text = output_text.strip()
    first_brace = json_text.find('{')
    last_brace = json_text.rfind('}')
    if first_brace != -1 and last_brace != -1:
        json_text = json_text[first_brace:last_brace + 1]

    try:
        ai_result = json.loads(json_text)
    except json.JSONDecodeError:
        # Try fixing common issues: smart quotes, unescaped control chars
        import re
        fixed = json_text
        fixed = fixed.replace('\u2018', "'").replace('\u2019', "'")  # smart single quotes
        fixed = fixed.replace('\u201c', '"').replace('\u201d', '"')  # smart double quotes
        fixed = re.sub(r'[\x00-\x1f]', ' ', fixed)  # control characters
        try:
            ai_result = json.loads(fixed)
        except json.JSONDecodeError:
            ai_result = None

    if ai_result is None:
        # Last resort: try to repair by finding valid JSON subset
        print("WARNING: Attempting JSON repair...")
        import re
        repaired = re.sub(r',\s*([}\]])', r'\1', json_text)
        try:
            ai_result = json.loads(repaired)
        except json.JSONDecodeError as e:
            print(f"ERROR: AI returned invalid JSON: {e}")
            print("Raw output:")
            print(output_text)
            sys.exit(1)

    # === DETERMINISTIC VERIFICATION ===
    if args.statement == 'all':
        # Verify each statement separately
        all_verification = {}
        total_formulas_pass = 0
        total_formulas = 0
        total_xbrl_match = 0
        total_xbrl = 0

        for stmt_name in ['income_statement', 'balance_sheet', 'cash_flow']:
            stmt_data = ai_result.get(stmt_name, {})
            v = verify_formulas(stmt_data)
            all_verification[stmt_name] = v
            total_formulas_pass += v['formulas_pass']
            total_formulas += v['formulas_total']
            total_xbrl_match += sum(1 for i in stmt_data.get('line_items', []) if i.get('xbrl_match') is True)
            total_xbrl += sum(1 for i in stmt_data.get('line_items', []) if i.get('xbrl_match') is not None)

        # === CF RETRY: if CFO formula fails, ask AI to find missing items ===
        cf_v = all_verification.get('cash_flow', {})
        cf_needs_retry = False
        for check in cf_v.get('formula_checks', []):
            # Find the CFO sum formula (contains "operating activities" in result)
            if not check['pass']:
                for period, detail in check['periods'].items():
                    if detail['computed'] is not None and detail['stated'] is not None and detail['computed'] != detail['stated']:
                        cf_needs_retry = True
                        break
            if cf_needs_retry:
                break

        if cf_needs_retry:
            print("CFO formula mismatch detected — retrying to find missing components...")
            cf_items = ai_result.get('cash_flow', {}).get('line_items', [])

            # Build the component list for the retry prompt
            component_summary = []
            for item in cf_items:
                vals = item.get('values') or {}
                if not vals:
                    continue
                first_val = list(vals.values())[0]
                component_summary.append(f"  {item['label']}: {first_val}")

            # Find the CFO total
            cfo_vals = {}
            for item in cf_items:
                concept = item.get('xbrl_concept') or ''
                if 'NetCashProvidedByUsedInOperatingActivities' in concept:
                    cfo_vals = item.get('values') or {}
                    break

            component_text = "\n".join(component_summary)
            retry_prompt = f"""You extracted cash flow operating activity components for {args.ticker} but they do not sum to the stated CFO total.

Your extracted components:
{component_text}

Stated CFO total: {list(cfo_vals.values())[0] if cfo_vals else '?'}

The components must sum EXACTLY to CFO. Rules:
- Positive values = add to cash (D&A addback, decrease in working capital asset, increase in working capital liability)
- Negative values = reduce cash (increase in working capital asset, decrease in working capital liability, payments)
- ALL components added together (using +) must equal CFO, because signs are already embedded in the values.

Go back to the cash flow statement in the filing and:
1. List EVERY line item between Net income and Net cash provided by operating activities
2. Include the value with correct sign (positive = source, negative = use)
3. Verify the sum equals CFO exactly

Output ONLY valid JSON:
{{"corrected_cfo_components": [{{"label": "...", "value": X, "xbrl_concept": "..."}}], "cfo_total": X, "sum_check": X, "items_added": ["list of items that were missing from original extraction"], "items_corrected": ["list of items with sign changes"]}}

CRITICAL: Output must be valid JSON. No apostrophes in strings."""

            # Call the API again
            retry_text = ""
            retry_in = 0
            retry_out = 0
            if args.model.startswith('gemini'):
                from google import genai
                gemini_client = genai.Client(api_key=os.environ.get('GEMINI_API_KEY'))
                retry_resp = gemini_client.models.generate_content(
                    model=args.model,
                    contents=retry_prompt + "\n\nFILING HTML:\n" + html_cleaned,
                    config=genai.types.GenerateContentConfig(max_output_tokens=8192),
                )
                retry_text = retry_resp.text
                retry_in = retry_resp.usage_metadata.prompt_token_count
                retry_out = retry_resp.usage_metadata.candidates_token_count
            elif args.model.startswith('gpt'):
                from openai import OpenAI
                oai_client = OpenAI(api_key=os.environ.get('OPENAI_API_KEY'))
                retry_resp = oai_client.chat.completions.create(
                    model=args.model,
                    max_completion_tokens=8192,
                    messages=[{"role": "user", "content": retry_prompt + "\n\nFILING HTML:\n" + html_cleaned}],
                )
                retry_text = retry_resp.choices[0].message.content
                retry_in = retry_resp.usage.prompt_tokens
                retry_out = retry_resp.usage.completion_tokens
            else:
                client = anthropic.Anthropic()
                with client.messages.stream(
                    model=args.model,
                    max_tokens=8192,
                    messages=[{"role": "user", "content": retry_prompt + "\n\nFILING HTML:\n" + html_cleaned}],
                ) as stream:
                    for text in stream.text_stream:
                        retry_text += text
                        print(".", end="", flush=True)
                    print()
                    resp = stream.get_final_message()
                    retry_in = resp.usage.input_tokens
                    retry_out = resp.usage.output_tokens

            input_tokens += retry_in
            output_tokens += retry_out

            # Parse retry result
            retry_json = retry_text.strip()
            fb = retry_json.find('{')
            lb = retry_json.rfind('}')
            if fb != -1 and lb != -1:
                retry_json = retry_json[fb:lb + 1]
            try:
                retry_result = json.loads(retry_json)
            except json.JSONDecodeError:
                import re
                retry_json = retry_json.replace('\u2018', "'").replace('\u2019', "'")
                retry_json = re.sub(r'[\x00-\x1f]', ' ', retry_json)
                retry_json = re.sub(r',\s*([}\]])', r'\1', retry_json)
                try:
                    retry_result = json.loads(retry_json)
                except json.JSONDecodeError:
                    retry_result = None

            if retry_result:
                ai_result['cfo_retry'] = retry_result
                print(f"CFO retry: sum_check={retry_result.get('sum_check')}, cfo_total={retry_result.get('cfo_total')}")
                if retry_result.get('items_added'):
                    print(f"  Items added: {retry_result['items_added']}")
                if retry_result.get('items_corrected'):
                    print(f"  Items corrected: {retry_result['items_corrected']}")

                # Re-verify with corrected components
                corrected = retry_result.get('corrected_cfo_components', [])
                if corrected:
                    corrected_sum = sum(c.get('value', 0) for c in corrected)
                    cfo_total = retry_result.get('cfo_total', 0)
                    if corrected_sum == cfo_total:
                        print(f"  VERIFIED: corrected components sum to CFO ({corrected_sum} = {cfo_total})")
                    else:
                        print(f"  STILL MISMATCHED: {corrected_sum} vs {cfo_total}")

        # Display each statement
        print("=" * 80)
        print(f"AI EXTRACTION: ALL STATEMENTS")
        print(f"{args.ticker} | {meta['form']} | {meta['report_date']}")
        print("=" * 80)

        for stmt_name, stmt_title in [('income_statement', 'INCOME STATEMENT'), ('balance_sheet', 'BALANCE SHEET'), ('cash_flow', 'CASH FLOW')]:
            stmt_data = ai_result.get(stmt_name, {})
            v = all_verification[stmt_name]

            print(f"\n{'=' * 40}")
            print(f"  {stmt_title}")
            print(f"{'=' * 40}")

            print("\n--- LINE ITEMS ---\n")
            for item in stmt_data.get('line_items', []):
                indent = "  " * item.get('indent_level', 0)
                vals = item.get('values') or {}
                val_str = " | ".join(f"{v}" for v in vals.values())
                xbrl_ok = "✓" if item.get('xbrl_match') else "✗"
                print(f"{indent}{item['label']}: {val_str}  {xbrl_ok}")

            print("\n--- FORMULAS ---\n")
            for check in v['formula_checks']:
                status = "PASS" if check['pass'] else "FAIL"
                print(f"[{status}] {check['formula']}")
                for period, detail in check['periods'].items():
                    print(f"  {detail['computation']} = {detail['computed']} vs {detail['stated']}")

            not_on = stmt_data.get('xbrl_not_on_statement', [])
            if not_on:
                print("\n--- NOT ON STATEMENT ---\n")
                for item in not_on:
                    print(f"  {item.get('concept')}: {item.get('value')} [{item.get('period')}]")
                    print(f"    {item.get('reason', '')}")

        # Cross-statement checks
        cross = ai_result.get('cross_statement_checks', [])
        if cross:
            print(f"\n{'=' * 40}")
            print(f"  CROSS-STATEMENT CHECKS")
            print(f"{'=' * 40}\n")
            for check in cross:
                status = "✓" if check.get('match') else "✗"
                print(f"  {status} {check.get('check')}")
                for k, v in check.items():
                    if k not in ('check', 'match'):
                        print(f"      {k}: {v}")

        # Calculation components
        calc = ai_result.get('calculation_components', {})
        if calc:
            print(f"\n{'=' * 40}")
            print(f"  CALCULATION COMPONENTS")
            print(f"{'=' * 40}\n")
            for comp_name, comp_data in calc.items():
                print(f"  {comp_name}:")
                if isinstance(comp_data, dict):
                    for k, v in comp_data.items():
                        print(f"    {k}: {v}")
                else:
                    print(f"    {comp_data}")
                print()

        print(f"\n{'=' * 80}")
        print(f"Tokens: {input_tokens} in, {output_tokens} out")
        if args.model.startswith('gpt-5.4'):
            in_rate, out_rate = 2.5, 15.0
        elif args.model.startswith('gpt-5'):
            in_rate, out_rate = 0.63, 5.0
        elif args.model.startswith('gemini-3') and 'flash' in args.model:
            in_rate, out_rate = 0.5, 3.0
        elif args.model.startswith('gemini'):
            in_rate, out_rate = 0.3, 2.5
        elif 'opus' in args.model:
            in_rate, out_rate = 15.0, 75.0
        else:
            in_rate, out_rate = 3.0, 15.0  # Sonnet default
        input_cost = input_tokens * in_rate / 1_000_000
        output_cost = output_tokens * out_rate / 1_000_000
        print(f"Cost: ${input_cost:.2f} input + ${output_cost:.2f} output = ${input_cost + output_cost:.2f}")
        print(f"Formulas: {total_formulas_pass}/{total_formulas} pass")
        print(f"XBRL matches (AI-reported): {total_xbrl_match}/{total_xbrl}")
        print("=" * 80)

        full_output = {
            'ai_extraction': ai_result,
            'formula_verification': all_verification,
        }
    else:
        verification = verify_formulas(ai_result)

        # Display
        print("=" * 80)
        print(f"AI EXTRACTION: {args.statement.upper()} STATEMENT")
        print(f"{args.ticker} | {meta['form']} | {meta['report_date']}")
        print("=" * 80)

        print("\n--- LINE ITEMS (from AI) ---\n")
        for item in ai_result.get('line_items', []):
            indent = "  " * item.get('indent_level', 0)
            vals = item.get('values', {})
            val_str = " | ".join(f"{v}" for v in vals.values())
            unit = item.get('unit', '')
            xbrl_ok = item.get('xbrl_match', '?')
            print(f"{indent}{item['label']}: {val_str}  [{unit}] xbrl_match={xbrl_ok}")
            print(f"{indent}  concept: {item.get('xbrl_concept', '?')}")
            print(f"{indent}  reason: {item.get('mapping_reason', 'none given')}")
            print()

        print("--- FORMULA VERIFICATION (deterministic arithmetic) ---\n")
        for check in verification['formula_checks']:
            status = "PASS" if check['pass'] else "FAIL"
            print(f"[{status}] {check['formula']}")
            for period, detail in check['periods'].items():
                print(f"  {period}: {detail['computation']} = {detail['computed']} vs stated {detail['stated']}")
            print()

        not_on_stmt = ai_result.get('xbrl_not_on_statement', [])
        if not_on_stmt:
            print("--- XBRL FACTS NOT ON STATEMENT (from AI) ---\n")
            for item in not_on_stmt:
                print(f"  {item.get('concept')}: {item.get('value')} [{item.get('period')}]")
                print(f"    {item.get('reason', '')}")
                print()

        print("=" * 80)
        print(f"Tokens: {input_tokens} in, {output_tokens} out")
        if args.model.startswith('gpt-5.4'):
            in_rate, out_rate = 2.5, 15.0
        elif args.model.startswith('gpt-5'):
            in_rate, out_rate = 0.63, 5.0
        elif args.model.startswith('gemini-3') and 'flash' in args.model:
            in_rate, out_rate = 0.5, 3.0
        elif args.model.startswith('gemini'):
            in_rate, out_rate = 0.3, 2.5
        elif 'opus' in args.model:
            in_rate, out_rate = 15.0, 75.0
        else:
            in_rate, out_rate = 3.0, 15.0  # Sonnet default
        input_cost = input_tokens * in_rate / 1_000_000
        output_cost = output_tokens * out_rate / 1_000_000
        print(f"Cost: ${input_cost:.2f} input + ${output_cost:.2f} output = ${input_cost + output_cost:.2f}")
        v = verification
        print(f"Formulas: {v['formulas_pass']}/{v['formulas_total']} pass")
        xbrl_matches = sum(1 for i in ai_result.get('line_items', []) if i.get('xbrl_match') is True)
        xbrl_total = sum(1 for i in ai_result.get('line_items', []) if i.get('xbrl_match') is not None)
        print(f"XBRL matches (AI-reported): {xbrl_matches}/{xbrl_total}")
        print("=" * 80)

        full_output = {
            'ai_extraction': ai_result,
            'formula_verification': verification,
        }

    if args.output:
        with open(args.output, 'w') as f:
            json.dump(full_output, f, indent=2)
        print(f"\nSaved to {args.output}")


if __name__ == '__main__':
    main()
