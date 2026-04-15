# AI Extraction Flow

Three stages. Each stage has one job. No stage does another stage's work.

## Stage 1 — Extract and Verify (`analyze_statement.py`)

**Input**: Raw filing (HTML + XBRL) downloaded by `fetch.py`

**What the AI does**:
1. Reads the full filing (HTML + undimensioned XBRL facts + segment/geography dimensioned XBRL facts)
2. Extracts every line item from all three financial statements (IS, BS, CF)
3. Extracts segment data: revenue by business segment, geography, product/service, and customer concentration. Segment revenue totals must match total revenue from the IS.
4. Verifies using the financial statements' own math:
   - Component items sum to reported subtotals (e.g., sum of current assets = reported Total Current Assets)
   - IS flows: Revenue → Gross Profit → Operating Income → Pretax → Net Income
   - BS balances: Total Assets = Total Liabilities + Total Equity
   - CF reconciles: CFO + CFI + CFF = Net Change in Cash, Beginning + Change = Ending
   - Cross-statement: Net Income on IS = Net Income starting CF, Ending Cash on CF = Cash on BS
4. If any check fails, the AI loops — re-examines the filing, finds what it missed, fixes it
5. Does NOT output until all checks pass, or times out with a failure log

**Outputs**:
- **Per-filing JSON** (`ai_extract/{TICKER}/q1_fy26_10q.json`): Exact extraction output, immutable, used as training data for post-trained model
- **`mapped.json`** (`ai_extract/{TICKER}/mapped.json`): Same data organized by period. If the filing is an amendment (10-Q/A, 10-K/A) or a 10-K with restated comparatives, the amended periods get overwritten. This is the running record of the best-known values for each period.

**What this stage does NOT do**: No field renaming, no analytical mapping, no metric calculations. Just the verified financial statements + segment data stored two ways.

## Stage 2 — Analytical Component Extraction (`ai_formula.py`)

**Input**: `mapped.json` (verified financial statements by period)

**What the AI does**:
1. Reads the verified financial statements
2. Extracts exactly 23 analytical components needed for metric calculations (ROIC, ROIIC, reinvestment rate, margins, CCC, etc.)
3. Resolves ambiguities that exist in 9 of the 23 fields:
   - **interest_expense_q**: Must be GROSS, not net. Find gross in notes if IS only shows net.
   - **income_tax_expense_q**: Can be a benefit (negative). Compute effective rate, flag if nonsensical.
   - **net_income_q**: Must be CONSOLIDATED (same as CF starting line), not net income to common if NCI exists.
   - **accounts_payable_q**: Must be PURE trade AP. If BS combines AP with accrued, find breakout in notes.
   - **short_term_debt_q**: May not be a separate BS line. Check notes for current portion of LT debt. Confirm 0 if none.
   - **operating_lease_liabilities_q**: TOTAL (current + non-current). Current portion often hidden in accrued liabilities.
   - **equity_q**: Parent-only stockholders equity, not total equity including NCI.
   - **capex_q**: May include intangibles, may be split across lines. Sum all capex lines.
   - **acquisitions_q**: May be multiple acquisition lines. Sum all.
4. Validates consistency:
   - Tax rate = tax / pretax. Flag if < 0% or > 50%.
   - Net income = pretax - tax (within rounding).
   - Equity + total liabilities = total assets.
   - Operating lease current + non-current = total. No double counting.
5. Outputs the 23 fields with validation results

**The 23 fields**:

| # | Field | Source | Ambiguity |
|---|-------|--------|-----------|
| 1 | revenue_q | IS | None |
| 2 | cogs_q | IS | None |
| 3 | operating_income_q | IS | None |
| 4 | interest_expense_q | IS/Notes | Gross vs net |
| 5 | pretax_income_q | IS | None |
| 6 | income_tax_expense_q | IS | Benefit handling |
| 7 | net_income_q | IS/CF | Consolidated vs to-common |
| 8 | cash_q | BS | None |
| 9 | short_term_investments_q | BS | None |
| 10 | accounts_receivable_q | BS | None |
| 11 | inventory_q | BS | None |
| 12 | total_assets_q | BS | None |
| 13 | accounts_payable_q | BS/Notes | Pure vs combined |
| 14 | short_term_debt_q | BS/Notes | Hidden or zero |
| 15 | long_term_debt_q | BS | None |
| 16 | operating_lease_liabilities_q | BS/Notes | Current hidden in accrued |
| 17 | equity_q | BS | Parent-only vs incl NCI |
| 18 | diluted_shares_q | IS | None |
| 19 | sbc_q | CF | None |
| 20 | dna_q | CF | None |
| 21 | cfo_q | CF | None |
| 22 | capex_q | CF/Notes | Split lines, intangibles |
| 23 | acquisitions_q | CF/Notes | Multiple lines |

**Output**: `formula_mapped.json` (`ai_extract/{TICKER}/formula_mapped.json`) — one record per filing with the 23 fields in raw dollars, effective tax rate, validation results, and notes on ambiguity resolutions.

**What this stage does NOT do**: No financial statement reproduction. No carrying forward of complete IS/BS/CF line items. That data already lives in `mapped.json`.

## Stage 3 — Quarterly Derivation (`ai_formula.py --from-mapped`)

**Input**: `formula_mapped.json` (23 analytical fields per filing)

**What this stage does** (pure arithmetic, no AI):
- **Q1 10-Q**: All fields already quarterly. Pass through.
- **Q2/Q3 10-Q**: IS fields are already quarterly (use as-is). CF fields (sbc_q, dna_q, cfo_q, capex_q, acquisitions_q) are YTD — subtract prior period's YTD to get standalone quarter.
- **10-K**: IS and CF fields are annual — subtract Q1+Q2+Q3 to get Q4. Shares/EPS use annual value directly (weighted averages can't be derived by subtraction).
- **BS fields**: Point-in-time snapshots. No derivation needed.

**Output**: `quarterly.json` (`ai_extract/{TICKER}/quarterly.json`) — one record per quarter with standalone quarterly values for the 23 fields.

## Downstream — Metric Calculations (`calculate.py`)

**Input**: `quarterly.json`

Computes all metrics from `formulas.md`: ROIC, ROIIC, reinvestment rate, gross profit growth, revenue growth, incremental margins, NOPAT margin, CFO/NOPAT, FCF/NOPAT, accruals ratio, CCC, SBC % revenue, diluted share count growth, net debt, interest coverage, working capital intensity, DSO/DIO/DPO, unlevered FCF.

**Output**: `dashboard/data/{ticker}.json`

## File Summary

```
ai_extract/{TICKER}/
  q1_fy26_10q.json      — Stage 1: per-filing extraction (immutable, training data)
  q2_fy26_10q.json
  q3_fy26_10q.json
  q4_fy26_10k.json
  mapped.json            — Stage 1: all periods, updated on amendments
  formula_mapped.json    — Stage 2: 23 analytical fields per filing
  quarterly.json         — Stage 3: 23 fields per quarter (standalone)
```
