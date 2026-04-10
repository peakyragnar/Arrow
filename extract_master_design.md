# Master Extraction Design

Design choices and rules for `extract.py` — the company-agnostic extraction script.

## Extraction Flow

```
For each downloaded filing:
  1. Parse XBRL → contexts + facts
  2. Classify contexts by period type (discrete, YTD, FY, instant)
  3. Extract raw values for each component

Then:
  4. Derive quarterly values (YTD subtraction for CF, Q4 derivation, etc.)
  5. Apply restatement overrides from later filings
  6. Apply company-specific post-processing (if companies/{ticker}.py exists)
  7. Output filtered to requested fiscal year range
```

## Component Types

Each component has a type that determines how its quarterly value is obtained:

| Type | Source | Derivation |
|------|--------|------------|
| `stock` | Balance sheet | Instant value at quarter-end. No derivation. |
| `flow` + `is` | Income statement | Q1-Q3: discrete quarterly value from 10-Q. Q4: FY minus 9M YTD. |
| `flow` + `cf` | Cash flow | Always derived from YTD subtraction. Q1 = YTD. Q2 = H1 - Q1. Q3 = 9M - H1. Q4 = FY - 9M. |
| `per_period` | Per-period metric | Discrete quarterly value. Q4 falls back to FY context. |

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

Company-specific overrides (`companies/{ticker}.py`) can replace or extend concept lists.

## Restatement Rule

**Problem:** Companies restate prior-period figures. This can appear as:

1. **Amended filings (10-Q/A, 10-K/A):** Explicitly replaces an earlier filing.
2. **Comparative disclosures:** Any filing may include restated values for prior periods — both duration contexts (IS, CF, per-period) and instant contexts (balance sheet). There is no amendment indicator in the XBRL.

**Rule: When any filing contains a value for a period already in our output, the most recently filed document's value wins.**

This applies to **all component types** — flow, stock, and per-period. No type-specific scoping.

Implementation (post-derivation override step):

1. After deriving quarterly values, scan downloaded filings for prior-period contexts that match output quarters.
2. **Only apply overrides from filings that are explicitly flagged or amended:**
   - `DocumentFinStmtErrorCorrectionFlag = true` in the XBRL (standard DEI concept indicating the filing contains error corrections to prior periods)
   - Amended form types (`10-Q/A`, `10-K/A`)
3. For duration-based components (flow, per_period): match ~90-day duration contexts whose end date matches an output quarter's period_end.
4. For instant-based components (stock): match instant contexts whose date matches an output quarter's period_end.
5. If the filing date is more recent than the original filing for that quarter, override the value.

Regular filings (10-Q, 10-K) routinely include prior-period comparative data that is **not** a restatement. Without the error correction flag check, these comparatives would incorrectly override original values (e.g., NVIDIA 10-Q comparative share counts reflecting pre-split figures).

**What this catches:**
- Restated values in filings with `DocumentFinStmtErrorCorrectionFlag = true` (e.g., Dell FY2025 10-K restating FY2024 quarterly IS and BS values).
- Amended filings (10-Q/A, 10-K/A) that contain updated XBRL for prior periods.

**What this does NOT catch:**
- Restatements that only appear in prose (MD&A text) without updated XBRL facts.
- Filings that contain corrections but fail to set the `DocumentFinStmtErrorCorrectionFlag`.

## Stock Split Handling

Stock splits cause later filings to contain pre-split share counts as comparative data. Without special handling, the restatement rule would incorrectly override post-split values with pre-split ones.

**Detection:** Scan all filings for the XBRL concept `StockholdersEquityNoteStockSplitConversionRatio1`. This is a standard US GAAP concept that explicitly records the split ratio (e.g., 10 for a 10:1 split). A company may have multiple splits in its history.

**Rule:** When overriding `diluted_shares_q`, if the ratio between the new value and the existing value matches a detected split ratio, the new value is pre-split — skip the override and keep the post-split original.

This only applies to `diluted_shares_q`. We do not calculate EPS; it would be derived from net income and diluted shares downstream.

## Output Filtering

The `--fy-start` / `--fy-end` filter is applied **after** all derivation and restatement overrides. This ensures:

- Derivation dependencies are always available (e.g., Q1 needed for Q2 CF subtraction)
- Restatement data from later filings can reach earlier quarters
- The filter only controls what appears in the output, not what participates in computation

## Negate Convention

Some components (capex, acquisitions, interest expense) are reported as positive in XBRL but expected as negative in the output (or vice versa). The `negate: true` flag on a component definition flips the sign after extraction.

## Default Values

Components with `default: 0` (e.g., short-term debt) return 0 instead of null when no XBRL value is found. Used for line items that are legitimately zero for many companies.
