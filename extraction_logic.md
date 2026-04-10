# Extraction Logic

How the pipeline determines what to fetch, how to extract quarterly values, and how to source R&D lookback data.

## Extraction Flow

```
For each downloaded filing:
  1. Parse XBRL → contexts + facts
  2. Read DEI elements → fiscal year, fiscal period
  3. Classify contexts by period type (discrete, YTD, FY, instant)
  4. Extract raw values for each component

Then:
  5. Derive quarterly values (YTD subtraction for CF, Q4 derivation, etc.)
  6. Apply restatement overrides from later filings
  7. Apply company-specific post-processing (if companies/{ticker}.py exists)
  8. Output filtered to requested fiscal year range
```

## Fiscal Year and Period Detection

Each XBRL filing contains DEI (Document and Entity Information) elements that explicitly identify the fiscal year, fiscal period, and fiscal year-end date:

- **`dei:DocumentFiscalYearFocus`** — e.g., `2025`
- **`dei:DocumentFiscalPeriodFocus`** — e.g., `Q1`, `Q2`, `Q3`, or `FY` (mapped to `Q4` for 10-Ks)
- **`dei:CurrentFiscalYearEndDate`** — e.g., `--12-31` or `--01-26`
- **`dei:DocumentPeriodEndDate`** — the period end date

The extraction uses these directly — no heuristic inference from report dates or context durations. If DEI elements are missing, extraction fails with an error (every valid SEC filing must have them).

Company overrides can implement `fix_dei(dei, meta)` to correct known DEI tagging errors before the values are used. Example: Dell FY2024 Q1-Q2 10-Qs incorrectly tag `DocumentFiscalPeriodFocus` as `FY` instead of `Q1`/`Q2`.

## Calendar Year and Quarter

Each output record includes `calendar_year` and `calendar_quarter` derived from the period end date, for cross-company normalization:

```
calendar_quarter = (period_end_month - 1) // 3 + 1
```

Example (NVIDIA, FY ends late January):

| Fiscal    | Period end  | Calendar    |
|-----------|-------------|-------------|
| FY2025 Q1 | 2024-04-28  | CY2024 Q2   |
| FY2025 Q2 | 2024-07-28  | CY2024 Q3   |
| FY2025 Q3 | 2024-10-27  | CY2024 Q4   |
| FY2025 Q4 | 2025-01-26  | CY2025 Q1   |

Calendar-year companies (e.g., Palantir) have fiscal and calendar quarters aligned.

## Fetch Window and Output Window

The pipeline has two distinct windows:

**Fetch window (6 fiscal years):** `fetch.py` downloads 6 fiscal years of filings ending at the current fiscal year. This provides the full data needed for derivation dependencies and R&D lookback. Expected: 6 10-Ks + 18 10-Qs = 24 filings. The current (incomplete) fiscal year will have fewer.

**Output window (3 fiscal years):** `extract.py` outputs the **3 most recent complete fiscal years** (12 quarters). A fiscal year is complete when all 4 quarters (Q1-Q4) have been derived. The remaining 3 fiscal years from the fetch window are used only as support data — R&D lookback history, cash flow derivation dependencies, and employee count baselines. They do not appear in the output.

This separation is critical. The R&D capitalization lookback in `compute.py` anchors to the first quarter in the output, not the first fetched quarter. If the output window is wrong, the lookback grabs the wrong years and all R&D calculations are off.

**Do not use `--fy-start`/`--fy-end` overrides.** The defaults handle the alignment correctly for any company regardless of fiscal year-end date. Manual overrides risk misaligning the output window with the lookback data.

## Component Types

Each component has a type that determines how its quarterly value is obtained:

| Type | Source | Derivation |
|------|--------|------------|
| `stock` | Balance sheet | Instant value at quarter-end. No derivation. |
| `flow` + `is` | Income statement | Q1-Q3: discrete quarterly value from 10-Q. Q4: FY minus 9M YTD. |
| `flow` + `cf` | Cash flow | Always derived from YTD subtraction. Q1 = YTD. Q2 = H1 - Q1. Q3 = 9M - H1. Q4 = FY - 9M. |
| `per_period` | Per-period metric | Discrete quarterly value. Q4 falls back to FY context. |

### Sign convention

Some components (capex, acquisitions, interest expense) are reported as positive in XBRL but expected as negative in the output (or vice versa). The `negate: true` flag on a component definition flips the sign after extraction.

### Default values

Components with `default: 0` (e.g., short-term debt) return 0 instead of null when no XBRL value is found. Used for line items that are legitimately zero for many companies.

### Summed concepts

Some balance sheet items are reported as current + noncurrent splits rather than a single total. Operating lease liabilities use `sum_concepts: True` to sum `OperatingLeaseLiabilityCurrent` + `OperatingLeaseLiabilityNoncurrent`. This applies to both the initial extraction and restatement overrides. Companies like NVIDIA and Dell tag the total `OperatingLeaseLiability` in addition to the split, but others (e.g., Palantir) only tag the split.

## Context Classification

Each filing's XBRL contexts are classified by their relationship to the filing's report date:

| Context key | Period type | Duration |
|-------------|-----------|----------|
| `current_instant` | instant | — (within 3 days of report date) |
| `current_discrete` | duration | 60-120 days, ending at report date |
| `current_ytd_h1` | duration | 150-210 days, ending at report date |
| `current_ytd_9m` | duration | 240-300 days, ending at report date |
| `current_fy` | duration | >340 days, ending at report date |
| `prior_instant` | instant | — (day before FY start) |

Only non-dimensioned contexts are used (consolidated totals, not segment breakdowns).

## Concept Resolution

Each component defines a priority-ordered list of XBRL concept names. The extractor tries each in order and uses the first one that has data. This handles companies that use different concept names for the same line item.

Example: CapEx might be `PaymentsToAcquirePropertyPlantAndEquipment` for one company and `PaymentsToAcquireProductiveAssets` for another.

Company-specific overrides (`companies/{ticker}.py`) can replace or extend concept lists via `get_components()`.

## Quarterly Derivation Details

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

## Restatement Overrides

After deriving quarterly values, the pipeline scans filings for prior-period values that should override the original extractions. Overrides only apply from filings that are explicitly flagged:

- **`DocumentFinStmtErrorCorrectionFlag = true`** — a standard DEI concept in the XBRL indicating the filing contains error corrections to prior periods.
- **Amended form types** — `10-Q/A` or `10-K/A`.

Regular 10-Q and 10-K filings include prior-period comparative data as standard disclosure. These are **not** treated as restatements.

Overrides apply to all component types (flow, stock, per_period) and respect `sum_concepts` for components that require it. For each matching prior-period context, if the flagged filing was filed after the original filing for that quarter, the new value replaces the old.

## Stock Split Handling

Stock splits are detected from the XBRL concept `StockholdersEquityNoteStockSplitConversionRatio1`. When a split is detected, overrides to `diluted_shares_q` are checked against the split ratio — if the override value differs by exactly the split ratio, it's a pre-split comparative and is skipped.

## Extract Output Filtering

`extract.py` parses **all** downloaded filings regardless of the output window. Derivation and restatement overrides run on the full filing set. The output filter is applied **last** — only the 3 most recent complete fiscal years (12 quarters) are written to the output JSON. This ensures derivation dependencies (e.g., Q1 needed for Q2 cash flow subtraction) are always available, even when Q1 falls outside the output window.

The default output window is determined automatically: find all fiscal years with 4 derived quarters, take the 3 most recent. This works identically for offset fiscal year companies (NVIDIA, Dell) and calendar fiscal year companies (Palantir).

## Evaluation Independence

`eval.py` is company-agnostic. It takes any ticker, loads `golden/{ticker}.json` and `output/{ticker}.json`, matches quarters by `(fiscal_year, fiscal_period)`, and scores. It has no knowledge of XBRL, fiscal calendars, or derivation logic. It reports:

- **Dropped quarters**: in golden but missing from extracted output
- **Extra quarters**: in extracted output but not in golden
- **Field-level comparison**: exact match, close (<1%), missing, or mismatch
- **Exit status**: non-zero if any mismatches, missing fields, or dropped quarters
