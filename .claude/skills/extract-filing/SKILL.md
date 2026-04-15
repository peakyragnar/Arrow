---
name: extract-filing
description: Extract financial statements from an SEC filing with formula verification. Use when processing XBRL filings for the AI extraction pipeline.
disable-model-invocation: true
argument-hint: "TICKER ACCESSION"
---

# Extract Financial Statements from SEC Filing

Extract all three financial statements (IS, BS, CF) from an SEC filing.
YOU verify the math. If any formula doesn't balance, YOU fix it before outputting.

## Arguments

- `$0` — Ticker (e.g., NVDA)
- `$1` — Accession number (e.g., 0001045810-25-000116)

## Step 1: Prepare the Filing

Run the preparation script to clean HTML and parse XBRL:

```bash
python3 ${CLAUDE_SKILL_DIR}/scripts/prepare_filing.py $0 $1
```

This writes a JSON file to `ai_extract/$0/prep_$1.json` containing:
- `meta`: filing metadata (form type, dates, ticker)
- `xbrl_facts_text`: formatted XBRL facts
- `html_cleaned`: CSS-stripped filing HTML
- `stats`: token estimates

## Step 2: Read the Filing Data

Read the prep file. It contains the full filing HTML and XBRL facts.
Both are your inputs for extraction.

## Step 3: Extract All Three Statements

Read the extraction schema from `${CLAUDE_SKILL_DIR}/extraction_schema.md` for the detailed output format.

For each statement (income statement, balance sheet, cash flow):
1. Find the statement in the HTML
2. Extract every line item in presentation order
3. For each line item, find the XBRL concept from the ix:nonFraction tag
4. Cross-reference values against the XBRL facts list
5. Record all formulas (subtotal relationships)

Extract ALL periods reported in the filing (current + comparatives).

### CF Sign Convention — Read This Carefully

For cash flow items, use the sign AS DISPLAYED on the statement:
- If the CF shows a working capital change as negative, store it negative
- If the CF shows it as positive, store it positive
- ALL CF formula operations use `+` only — signs are in the values
- The sum of all CFO components using `+` MUST equal the CFO total

The XBRL value may have a different sign than the CF presentation. If the
XBRL tag is `IncreaseDecreaseInAccountsReceivable = 2366` but the CF statement
shows `(2,366)` or `-2,366` for accounts receivable, use **-2366**. The
presentation sign is what makes the formulas work.

## Step 4: Verify — YOU Do This

After extracting, YOU must verify every formula. Do not skip this.

### Income Statement Checks
For each period, compute:
1. Revenue - COGS → must equal Gross Profit
2. Sum of opex items → must equal Total Opex
3. Gross Profit - Total Opex → must equal Operating Income
4. Sum of other income items → must equal Total Other Income
5. Operating Income + Total Other Income → must equal Pretax Income
6. Pretax Income - Tax → must equal Net Income

### Balance Sheet Checks
For each period:
1. Sum current asset items → must equal Total Current Assets
2. Total Current Assets + non-current items → must equal Total Assets
3. Sum current liability items → must equal Total Current Liabilities
4. Current + non-current liabilities → must equal Total Liabilities
5. Sum equity components → must equal Total Equity
6. Total Liabilities + Total Equity → must equal Total L&E
7. Total Assets → must equal Total L&E

### Cash Flow Checks — Most Important
For each period:
1. Sum ALL CFO items using `+` → must EXACTLY equal CFO total
2. Sum ALL CFI items using `+` → must EXACTLY equal CFI total
3. Sum ALL CFF items using `+` → must EXACTLY equal CFF total
4. CFO + CFI + CFF → must equal Net Change in Cash
5. Beginning Cash + Net Change → must equal Ending Cash

### Cross-Statement Checks
1. Net Income on IS = Net Income on CF (same value)
2. Ending Cash on CF = Cash on BS (current period)
3. Beginning Cash on CF = Cash on BS (prior period)

### If a Check Fails

Do NOT output the result. Instead:
1. Identify which values caused the mismatch
2. Go back to the filing HTML and re-examine those specific items
3. Check if you used the wrong sign (common for CF working capital items)
4. Check if you missed a line item (common for "Other" items)
5. Fix the values and re-verify
6. Repeat until ALL checks pass

Report each verification result in the `verification` section of your output.

## Step 5: Extract Calculation Components

After the three statements are verified, search the full filing for the
calculation components listed in the extraction schema (operating leases,
D&A breakdown, pure AP/AR, capex, acquisitions, short-term debt, SBC,
gross interest expense, tax rate, inventory breakdown).

## Step 6: Write the Output

Write the final JSON to:
```
ai_extract/$0/{quarter_label}.json
```

Where `{quarter_label}` is derived from the filing:
- 10-Q Q1: `q1_fy{YY}_10q.json`
- 10-Q Q2: `q2_fy{YY}_10q.json`
- 10-Q Q3: `q3_fy{YY}_10q.json`
- 10-K: `q4_fy{YY}_10k.json`

The FY year is determined by the fiscal year end date (e.g., NVDA FY ends in January, so a filing with report_date 2025-04-27 is Q1 FY26).

Wrap the extraction in:
```json
{
  "ai_extraction": { ... your extraction ... },
  "meta": { "ticker": "$0", "accession": "$1", "form": "...", "report_date": "..." },
  "extracted_by": "claude-code-skill"
}
```

## Step 7: Clean Up

Delete the prep file:
```bash
rm ai_extract/$0/prep_$1.json
```

## Summary

Report:
- Filing processed (ticker, form, date)
- Number of line items extracted per statement
- All verification results (pass/fail for each check)
- Output file path
