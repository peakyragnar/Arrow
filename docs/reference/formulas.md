# FinEpic Metric Formulas

This document is the canonical metric dictionary for v1. It combines the full formula list with the design clarifications that affect implementation.

## Canonical Analytical Fields

Metric formulas reference values by canonical short names. Those names are the **bucket names** defined in [`concepts.md`](concepts.md) — one universal set per statement (IS, BS, CF), used for every company.

- [`concepts.md`](concepts.md) is the source of truth for bucket names, stored signs, and null semantics. Metric formulas below reference those names directly (e.g., `cfo`, `capital_expenditures`, `operating_income`, `total_assets`). Period is a qualifier: `cfo_q` = quarterly, `cfo_ttm` = trailing twelve months, `cfo_fy` = full fiscal year.
- When a name could be ambiguous across statements (`dna` exists on IS and CF), use the qualified names from `concepts.md`: `dna_is` and `dna_cf`.
- Do not invent parallel names or aliases. Adding a new metric means using existing bucket names; adding a new bucket means editing `concepts.md` first, then referencing it here.
- Legacy archive reference: `archive/ai_extract/canonical_buckets.md` was the pre-v1 draft this doc descended from; don't edit it further.

### Name migration from the archive draft

A small number of bucket names were clarified when lifting to `concepts.md`:

| Legacy archive name | Current canonical (`concepts.md`) |
|---|---|
| `capex` | `capital_expenditures` |
| `dna` (CF) | `dna_cf` |
| `dna` (IS) | `dna_is` |
| `sbc` | `sbc` (unchanged) |
| `stock_repurchase` | `stock_repurchase` (unchanged, now cash-impact sign) |
| `change_ar`, `change_inventory`, `change_ap` | same names, now cash-impact sign throughout |
| `extraordinary_items` / `ni_common_excl_extra` | **dropped** (GAAP ASU 2015-01) |
| `finance_div_revenue`, `insurance_div_revenue` | **dropped** (segment data → future segments table) |

See [`concepts.md`](concepts.md) § 12 for the full change log.

## Formula Correctness — Component Guards

Every formula declares:
- **requires**: the list of bucket names it consumes
- **on_missing**: the behavior when any required bucket is NULL
- **on_out_of_range**: (optional) behavior when an intermediate is economically impossible

### Universal rule: suppress on missing, never plug

```
for each formula F with output bucket O:
    for each required component C in F.requires:
        if C is NULL in the required period:
            O.value = NULL
            O.provenance.reason = "missing component: C at period P"
            STOP; do not compute F
```

**NEVER**:
- substitute 0 for a NULL required component
- interpolate from adjacent periods
- use a prior period's value in place of the missing one
- emit a partial value with a flag

Rationale: a partially-computed metric that looks valid is worse than a missing metric. Suppression surfaces the data gap; plugs hide it.

### Universal rule: handle denominator-near-zero

```
if denominator has |value| < 0.1% of numerator's scale (or absolute $1M, whichever larger):
    O.value = NULL
    O.provenance.reason = "denominator near zero"
```

Applies wherever a ratio is involved (tax rate, ROIC, margins, etc.). Precision loss near zero produces values that are mathematically valid but economically meaningless.

### Enforcement

These guards are layer 4 of the five-layer correctness stack. See [`verification.md`](verification.md) § 5 for the full stack and failure modes. Implementation lives in `src/arrow/normalize/financials/formulas.py` (not yet written).

## Global Calculation Rules

### Canonical period rules

- Use actual filing fact dates, not SEC frames/calendar buckets, as the quarter truth source.
- Flows are stored as discrete quarters.
- Stocks are stored as quarter-end values and are never summed across quarters.
- Prefer explicit 10Q quarter values.
- Derive Q4 only when needed: `FY - Q1 - Q2 - Q3`.
- TTM always means the sum of the most recent 4 discrete quarters.
- Support both `point_in_time` and `latest_restated` views.

### Source rules

- Formulas are computed from canonical components mapped to 10K / 10Q line items.
- Raw filing values beat derivative SEC datasets.
- `companyfacts`, FSDS, and FSNDS are verification inputs, not silent formula substitutes.
- No metric publishes without lineage.
- No high-confidence metric publishes if a required upstream component is `blocked`.
- `net_income_q` uses the same net-income basis that the cash flow statement uses as the
  starting line for CFO reconciliation. If a filing discloses noncontrolling-interest
  attribution separately and the CFO bridge is on a consolidated basis, include that
  amount so the deterministic cash-quality metrics stay internally coherent.

### Common implementation rules

- Use reported signs from filings, then normalize per component policy.
- CapEx is reported as a cash outflow in investing; use its absolute value in formulas.
- Acquisitions use cash paid for acquisitions net of cash acquired; treat as a positive reinvestment outflow.
- Operating lease liabilities are included in invested capital and net debt only if not already included in debt.
- If a denominator is zero, negative where economically non-meaningful, or missing, suppress the display result and retain the raw calculation state for auditability.

### R&D capitalization rules

- Annual shorthand:
  `R&D Amortization = sum(prior 5 years of R&D expense / 5)`
- Quarterly implementation:
  maintain a quarterly vintage table and amortize each quarter of R&D over 20 quarters.
- Explicit quarterly amortization formula:
  `R&D Amortization(t) = sum(R&D(t-j) / 20) for j = 0 to 19`
  which is equivalent to:
  `R&D Amortization(t) = (R&D(t) + R&D(t-1) + ... + R&D(t-19)) / 20`
- Annual shorthand:
  `R&D Asset = sum(R&D expense by vintage x remaining life / 5)`
- Quarterly implementation:
  compute the unamortized balance from the 20-quarter vintage schedule.
- Explicit quarterly asset formula:
  `R&D Asset(t) = sum(R&D(t-j) x (20-j) / 20) for j = 0 to 19`
  which is equivalent to:
  `R&D(t) x 20/20 + R&D(t-1) x 19/20 + ... + R&D(t-19) x 1/20`
- Operating-income adjustment:
  `OI Adjustment(t) = R&D(t) - R&D Amortization(t)`
- Adjusted operating income:
  `Adjusted OI(t) = Reported OI(t) + R&D(t) - R&D Amortization(t)`

### Tax rate rule

- `Tax Rate = Income Tax Expense / Income Before Income Taxes`
- Fallback: `21%` if pretax income is zero or negative.

### Metric 17

- Metric 17 is intentionally retired and unused.

## Metric Dictionary

### 1. ROIC (Adjusted)

- Grain: TTM numerator / average invested capital base
- Formula:
  `Adjusted ROIC = Adjusted NOPAT TTM / Average Adjusted Invested Capital`
- `Adjusted NOPAT = (Operating Income + R&D Expense - R&D Amortization) x (1 - Tax Rate)`
- `Average Adjusted Invested Capital = (Beginning Adjusted Invested Capital + Ending Adjusted Invested Capital) / 2`
- `Adjusted Invested Capital = Total Stockholders' Equity + Short-Term Debt + Long-Term Debt + Capitalized Operating Leases - Cash and Cash Equivalents - Short-Term Investments + R&D Asset`
- Filing sources:
  - Operating Income: income statement, `Income from operations` / `Operating income`
  - R&D Expense: income statement, `Research and development`
  - Income Tax Expense: income statement, `Provision for income taxes`
  - Income Before Income Taxes: income statement, `Income before provision for income taxes`
  - Total Stockholders' Equity: balance sheet, `Total stockholders' equity`
  - Short-Term Debt: balance sheet, `Current portion of long-term debt` and/or `Notes payable`
  - Long-Term Debt: balance sheet, `Long-term debt, net`
  - Cash and Cash Equivalents: balance sheet
  - Short-Term Investments: balance sheet, `Short-term investments` / `Marketable securities`
  - Operating Lease Liabilities: balance sheet, current + non-current operating lease liabilities
- Implementation clarifications:
  - Use TTM adjusted NOPAT built from 4 discrete quarters.
  - Use beginning and ending invested capital as stock values at quarter ends.
  - Use the quarterly 20-quarter R&D capitalization schedule in production; the 5-year annual version is the shorthand.
  - The quarterly implementation is:
    `Adjusted OI(t) = Reported OI(t) + R&D(t) - R&D Amortization(t)`
    and
    `Adjusted Invested Capital(t) = Reported Invested Capital(t) + R&D Asset(t)`

### 2. ROIIC (Incremental ROIC)

- Grain: TTM numerator / YoY stock delta
- Formula:
  `ROIIC = Delta Adjusted NOPAT TTM / Delta Adjusted Invested Capital`
- `Delta Adjusted NOPAT TTM = Adjusted NOPAT TTM current - Adjusted NOPAT TTM prior-year`
- `Delta Adjusted Invested Capital = Adjusted Invested Capital current quarter-end - Adjusted Invested Capital prior-year same quarter-end`
- Implementation clarification:
  - Use TTM for NOPAT to reduce quarterly noise.
  - Do not TTM a stock measure; invested capital remains a quarter-end balance-sheet value.

### 3. Reinvestment Rate

- Grain: TTM
- Formula:
  `Reinvestment Rate = Reinvestment / Adjusted NOPAT TTM`
- `Reinvestment = CapEx + Delta Operating Working Capital + Acquisitions - D&A + Delta R&D Asset`
- `Delta Operating Working Capital = Delta AR + Delta Inventory - Delta AP`
- Filing sources:
  - CapEx: cash flow statement, `Purchases of property and equipment`
  - D&A: cash flow statement, `Depreciation and amortization`
  - Acquisitions: cash flow statement, `Acquisitions, net of cash acquired` / `Business combinations`
  - AR: balance sheet, `Accounts receivable, net`
  - Inventory: balance sheet, `Inventories`
  - AP: balance sheet, `Accounts payable`
- Implementation clarifications:
  - Use absolute value for CapEx.
  - Working-capital components are stock deltas between quarter-end balances.
  - `Delta R&D Asset` comes from the quarterly capitalization schedule.

### 4. Gross Profit TTM Growth

- Grain: TTM YoY
- Formula:
  `Gross Profit TTM Growth = (Gross Profit TTM current - Gross Profit TTM prior-year) / Gross Profit TTM prior-year`
- `Gross Profit = Revenue - Cost of Revenue`
- Filing sources:
  - Revenue: income statement, `Net revenue` / `Total revenue` / `Revenue`
  - Cost of Revenue: income statement, `Cost of revenue` / `Cost of goods sold` / `Cost of sales`
- Implementation clarifications:
  - If gross profit is explicitly reported, that line can be used directly.
  - Otherwise compute `Revenue - Cost of Revenue`.
  - TTM uses the last 4 discrete quarters.

### 5a. Revenue Growth YoY

- Grain: TTM YoY
- Formula:
  `Revenue Growth YoY = (Revenue TTM current - Revenue TTM prior-year) / Revenue TTM prior-year`

### 5b. Revenue Growth QoQ Annualized

- Grain: quarterly
- Formula:
  `Revenue Growth QoQ Annualized = ((Revenue current quarter / Revenue prior quarter) ^ 4) - 1`
- Implementation clarification:
  - Use discrete quarters only.
  - Suppress display when prior-quarter revenue is zero or negative.

### 6. Incremental Gross Margin

- Grain: TTM YoY
- Formula:
  `Incremental Gross Margin = Delta Gross Profit TTM / Delta Revenue TTM`

### 7. Incremental Operating Margin

- Grain: TTM YoY
- Formula:
  `Incremental Operating Margin = Delta Operating Income TTM / Delta Revenue TTM`
- Implementation clarification:
  - Use GAAP operating income.
  - Do not apply the R&D capitalization adjustment here.

### 8. NOPAT Margin

- Grain: TTM
- Formula:
  `NOPAT Margin = Adjusted NOPAT TTM / Revenue TTM`

### 9. CFO / NOPAT

- Grain: TTM
- Formula:
  `CFO / NOPAT = Cash From Operations TTM / Adjusted NOPAT TTM`
- Filing source:
  - CFO: cash flow statement, `Net cash provided by (used in) operating activities`

### 10. FCF / NOPAT

- Grain: TTM
- Formula:
  `FCF / NOPAT = (CFO TTM - CapEx TTM) / Adjusted NOPAT TTM`

### 11. Accruals Ratio

- Grain: TTM numerator / average balance-sheet denominator
- Formula:
  `Accruals Ratio = (Net Income TTM - CFO TTM) / Average Total Assets`
- `Average Total Assets = (Beginning Total Assets + Ending Total Assets) / 2`
- Filing sources:
  - Net Income: income statement net income on the same basis used to start the CFO
    reconciliation. Include noncontrolling interest when the filing's CFO bridge starts
    from consolidated net income before attribution.
  - CFO: cash flow statement
  - Total Assets: balance sheet, `Total assets`

### 12. Cash Conversion Cycle

- Grain: quarter-end balance sheet / TTM income statement
- Formula:
  `CCC = DSO + DIO - DPO`
- `DSO = (AR at quarter end / Revenue TTM) x 365`
- `DIO = (Inventory at quarter end / COGS TTM) x 365`
- `DPO = (AP at quarter end / COGS TTM) x 365`
- Implementation clarification:
  - This TTM-denominator version is the canonical method because it smooths seasonality.
  - Quarter-end balance-sheet values are never annualized or summed.

### 13. SBC as % of Revenue

- Grain: TTM
- Formula:
  `SBC as % Revenue = SBC Expense TTM / Revenue TTM`
- Filing source:
  - SBC: cash flow statement addback, `Stock-based compensation`
- Implementation clarification:
  - Use the cash flow statement addback as the total-company SBC source of truth.

### 14. Diluted Share Count Growth

- Grain: YoY same quarter
- Formula:
  `Diluted Share Count Growth = (Diluted Shares current period - Diluted Shares prior-year same period) / Diluted Shares prior-year same period`
- Filing source:
  - Diluted Shares: income statement / EPS footnote, `Weighted average shares outstanding - diluted`
- Implementation clarification:
  - Use diluted weighted-average shares, not basic shares.

### 15. Net Debt / Net Cash Position

- Grain: most recent quarter-end
- Formula:
  `Net Debt = Short-Term Debt + Long-Term Debt + Current Operating Lease Liabilities + Non-Current Operating Lease Liabilities - Cash and Cash Equivalents - Short-Term Investments`
- If `Net Debt < 0`, classify as net cash.
- Ratio:
  `Net Debt / EBITDA TTM`
- `EBITDA TTM = Operating Income TTM + D&A TTM`
- Implementation clarifications:
  - Include operating lease liabilities only if not already embedded in debt.
  - D&A comes from the cash flow statement addback.
  - Report both the absolute dollar amount and the leverage ratio.

### 16. Interest Coverage

- Grain: most recent quarter or TTM view, depending on API mode
- Formula:
  `Interest Coverage = Operating Income / Interest Expense`
- Filing source:
  - Interest Expense: income statement, `Interest expense` / `Interest expense, net`
- Implementation clarifications:
  - Use interest expense from the income statement, not cash paid for interest.
  - If gross interest expense and interest income are disclosed separately, use gross interest expense for the conservative denominator.
  - If only `Interest expense, net` is disclosed, use that.

### 18. Revenue per Employee

- Grain: TTM revenue / latest annual employee count
- Formula:
  `Revenue per Employee = Revenue TTM / Total Employees`
- Source:
  - Total Employees: 10K Item 1 `Business` / `Human Capital`
- Implementation clarification:
  - Carry the most recent 10K employee count forward until the next 10K.

### 19. Working Capital Intensity

- Grain: quarter-end balance sheet / TTM revenue
- Formula:
  `Working Capital Intensity = NWC / Revenue TTM`
- `NWC = Accounts Receivable + Inventory - Accounts Payable`

### 20. DSO, DIO, DPO

- Grain: quarter-end balance sheet / TTM income statement
- Formulas:
  - `DSO = (AR at quarter end / Revenue TTM) x 365`
  - `DIO = (Inventory at quarter end / COGS TTM) x 365`
  - `DPO = (AP at quarter end / COGS TTM) x 365`
- Implementation clarification:
  - These are the component metrics underlying metric 12.

### 21. Unlevered FCF

- Grain: TTM
- Formula:
  `Unlevered FCF = CFO + Cash Paid for Interest x (1 - Tax Rate) - CapEx`
- Source:
  - Cash Paid for Interest: supplemental cash flow disclosure
- Implementation clarifications:
  - If cash paid for interest is missing in a 10Q, approximate with income-statement interest expense.
  - Use the same tax-rate rule as metric 1.

### 22. Organic vs. Acquired Growth

- Grain: TTM YoY
- Formula:
  `Organic Revenue Growth = ((Revenue current - Revenue from acquisitions made in last 12 months) / Revenue prior-year) - 1`
- Implementation status:
  - This metric is not fully automatable in v1.
  - Revenue contribution from acquisitions is footnote-driven and not standardized.
  - For v1, flag companies with material acquisitions and handle the acquisition contribution via manual research note or override.

## Grain Summary

| Metric | Canonical Grain |
| --- | --- |
| 1. ROIC | TTM NOPAT / average quarter-end invested capital |
| 2. ROIIC | TTM NOPAT delta / YoY quarter-end invested capital delta |
| 3. Reinvestment Rate | TTM |
| 4. Gross Profit Growth | TTM YoY |
| 5a. Revenue Growth YoY | TTM YoY |
| 5b. Revenue Growth QoQ Annualized | Quarterly |
| 6. Incremental Gross Margin | TTM YoY |
| 7. Incremental Operating Margin | TTM YoY |
| 8. NOPAT Margin | TTM |
| 9. CFO / NOPAT | TTM |
| 10. FCF / NOPAT | TTM |
| 11. Accruals Ratio | TTM numerator / average assets |
| 12. CCC | Quarter-end balance sheet / TTM income statement |
| 13. SBC % Revenue | TTM |
| 14. Diluted Share Count Growth | YoY same quarter |
| 15. Net Debt / Net Cash | Most recent quarter-end |
| 16. Interest Coverage | Most recent quarter or TTM API view |
| 18. Revenue per Employee | TTM revenue / latest 10K employee count |
| 19. Working Capital Intensity | Quarter-end NWC / Revenue TTM |
| 20. DSO / DIO / DPO | Quarter-end balance sheet / TTM income statement |
| 21. Unlevered FCF | TTM |
| 22. Organic vs. Acquired Growth | TTM YoY, manual-adjusted where needed |
