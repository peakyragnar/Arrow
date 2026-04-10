# Extraction Logic

How the pipeline determines what to fetch, how to extract quarterly values, and how to source R&D lookback data.

## Fiscal Year Detection

Each company has a fiscal year-end month provided by SEC EDGAR (e.g., `fiscalYearEnd: "0129"` = January). A filing's fiscal year is determined from its report date:

- If the report date's month is **after** the FY-end month, the filing belongs to the **next** calendar year's fiscal year.
- Otherwise, it belongs to the **current** calendar year's fiscal year.

Example (Dell, FY ends January):

| Report date  | Month | > 1? | Fiscal year |
|-------------|-------|------|-------------|
| 2021-04-30  | 4     | yes  | FY2022      |
| 2021-07-30  | 7     | yes  | FY2022      |
| 2021-10-29  | 10    | yes  | FY2022      |
| 2022-01-28  | 1     | no   | FY2022      |

## Fetch Window

Default: **6 fiscal years** ending at the current fiscal year (determined from today's date and the company's FY-end month). No manual `--fy-start`/`--fy-end` required.

6 years provides:
- **3 fiscal years** of output (12 quarters of extracted data)
- **3 fiscal years** of support (R&D lookback data embedded in the earlier 10-Ks, plus derivation dependencies for cash flow)

Expected filings per company: 6 10-Ks + 18 10-Qs = 24. The current (incomplete) fiscal year will have fewer — the pipeline reports expected vs actual counts and flags shortfalls.

`--fy-start` and `--fy-end` remain as optional overrides.

## Quarterly Derivation by Statement Type

XBRL filings contain different period structures depending on the financial statement. The extraction logic handles each differently.

### Income Statement (IS) — flow items

Q1-Q3 10-Q filings contain **discrete quarterly values**. Use them directly.

Q4 has no standalone filing — it must be derived:
```
Q4 = FY (from 10-K) - 9M YTD (from Q3 10-Q)
```

This introduces ~$1M rounding from integer truncation in XBRL.

### Cash Flow Statement (CF) — flow items

10-Q filings contain **only YTD cumulative values**, never discrete quarterly. Every quarter must be derived:

```
Q1 = Q1 YTD (which IS the quarterly value)
Q2 = H1 YTD - Q1 YTD
Q3 = 9M YTD - H1 YTD
Q4 = FY (from 10-K) - 9M YTD (from Q3 10-Q)
```

This means **Q2 derivation requires Q1's filing**, even if Q1 is not in the output window. The 6-year fetch window ensures these dependencies are always satisfied for the 3-year output window.

### Balance Sheet (BS) — stock items

Instant (point-in-time) values. No derivation needed — use the value at the quarter-end date directly.

### Per-Period Items (e.g., diluted shares)

Use the discrete quarterly context. For Q4, fall back to the FY context from the 10-K.

## R&D Capitalization Lookback

The 20-quarter amortization schedule requires R&D history beyond the 12-quarter output window. The pipeline sources this from **annual R&D figures embedded in downloaded 10-Ks**, not from a separate prior 10-K outside the fetch window.

### How it works

1. Scan **all** downloaded 10-Ks (within the 6-year fetch window).
2. Extract annual R&D entries from each (each 10-K typically reports 3 years: current + 2 prior).
3. **Prefer the most recent 10-K's values** for any given year (most accurate due to restatements).
4. Filter to years ending **before** the first extraction quarter.
5. Keep the **most recent 3 years** of annual R&D.

### Why this works

A company's earliest in-window 10-K (e.g., FY2022) contains R&D for FY2022, FY2021, and FY2020. If the output window starts at FY2024, the lookback years are FY2021-FY2023 — all available from 10-Ks already in the window. No extra fetching needed.

Each annual R&D figure is divided by 4 to estimate quarterly values for the lookback period. Combined with 12 actual quarters, this produces a 24-quarter series (12 estimated + 12 actual). The 20-quarter amortization window is fully covered.

### Why cap at 3 years

The golden evaluation was built with a 3-year lookback (12 estimated quarters + 12 actual = 24 total). Adding a 4th year changes the amortization calculation and produces mismatches. The 20-quarter window needs at most 8 estimated quarters (20 - 12 = 8, i.e., 2 years), so 3 years provides comfortable coverage with one year of buffer.

## Extract Output Filtering

`extract.py` parses **all** downloaded filings regardless of the `--fy-start`/`--fy-end` arguments. The FY filter is applied **after** quarterly derivation, not before. This ensures derivation dependencies (e.g., Q1 needed for Q2 cash flow subtraction) are always available, even when Q1 falls outside the output window.

## Evaluation Independence

`eval.py` is company-agnostic. It takes any ticker, loads `golden/{ticker}.json` and `output/{ticker}.json`, matches quarters by `(fiscal_year, fiscal_period)`, and scores. It has no knowledge of XBRL, fiscal calendars, or derivation logic. It reports:

- **Dropped quarters**: in golden but missing from extracted output
- **Extra quarters**: in extracted output but not in golden
- **Field-level comparison**: exact match, close (<1%), missing, or mismatch
- **Exit status**: non-zero if any mismatches, missing fields, or dropped quarters
