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
    parser.add_argument('--statement', default='income', choices=['income', 'balance_sheet', 'cash_flow'])
    parser.add_argument('--output', help='Save output to file')
    parser.add_argument('--model', default='claude-opus-4-6', help='Claude model to use')
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
    print(f"  Full HTML: {len(html_content)} chars (~{len(html_content)//4} tokens)")

    # Step 3: Filter XBRL facts (undimensioned only)
    xbrl_facts = [f for f in parsed['facts'] if not f['dimensioned']]
    print(f"  {len(xbrl_facts)} undimensioned XBRL facts")

    # Step 4: Build prompt and call Claude
    prompt = build_prompt(html_content, xbrl_facts, args.statement, meta)
    print(f"\nTotal prompt size: ~{len(prompt)//4} tokens")
    print(f"Sending to {args.model}...\n")

    client = anthropic.Anthropic()
    response = client.messages.create(
        model=args.model,
        max_tokens=16384,
        messages=[{"role": "user", "content": prompt}],
    )

    output_text = response.content[0].text

    # Parse the JSON from the AI response
    json_text = output_text.strip()
    first_brace = json_text.find('{')
    last_brace = json_text.rfind('}')
    if first_brace != -1 and last_brace != -1:
        json_text = json_text[first_brace:last_brace + 1]

    try:
        ai_result = json.loads(json_text)
    except json.JSONDecodeError as e:
        print(f"ERROR: AI returned invalid JSON: {e}")
        print("Raw output:")
        print(output_text)
        sys.exit(1)

    # === DETERMINISTIC VERIFICATION: arithmetic only ===
    verification = verify_formulas(ai_result)

    # Display
    print("=" * 80)
    print(f"AI EXTRACTION: {args.statement.upper()} STATEMENT")
    print(f"{args.ticker} | {meta['form']} | {meta['report_date']}")
    print("=" * 80)

    # Section 1: Line items with mapping reasons
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

    # Section 2: Formula verification (deterministic — just arithmetic)
    print("--- FORMULA VERIFICATION (deterministic arithmetic) ---\n")
    for check in verification['formula_checks']:
        status = "PASS" if check['pass'] else "FAIL"
        print(f"[{status}] {check['formula']}")
        for period, detail in check['periods'].items():
            print(f"  {period}: {detail['computation']} = {detail['computed']} vs stated {detail['stated']}")
        print()

    # Section 3: XBRL facts not on the statement (from AI)
    not_on_stmt = ai_result.get('xbrl_not_on_statement', [])
    if not_on_stmt:
        print("--- XBRL FACTS NOT ON STATEMENT (from AI) ---\n")
        for item in not_on_stmt:
            print(f"  {item.get('concept')}: {item.get('value')} [{item.get('period')}]")
            print(f"    {item.get('reason', '')}")
            print()

    # Summary
    print("=" * 80)
    print(f"Tokens: {response.usage.input_tokens} in, {response.usage.output_tokens} out")
    v = verification
    print(f"Formulas: {v['formulas_pass']}/{v['formulas_total']} pass")
    xbrl_matches = sum(1 for i in ai_result.get('line_items', []) if i.get('xbrl_match') is True)
    xbrl_total = sum(1 for i in ai_result.get('line_items', []) if i.get('xbrl_match') is not None)
    print(f"XBRL matches (AI-reported): {xbrl_matches}/{xbrl_total}")
    print("=" * 80)

    # Save
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
