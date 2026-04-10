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

**Problem:** Companies sometimes restate prior-period figures. This happens in two ways:

1. **Amended filings (10-Q/A, 10-K/A):** Explicitly replaces an earlier filing. Easy to detect from the form type.

2. **Comparative disclosures in 10-K:** A 10-K may include restated quarterly figures for the prior fiscal year as comparative data. These appear as regular ~90-day duration contexts with prior-period dates. There is no amendment indicator in the XBRL — the values simply differ from the original 10-Q.

**Rule: When the same component for the same quarter-period appears in multiple filings, the value from the most recently filed document wins.**

This is implemented as a post-derivation override step:

1. After deriving quarterly values from each filing's own data, scan all 10-K filings for prior-period quarterly contexts (discrete ~90-day durations that don't match the 10-K's own quarter).

2. For each prior-period value found, compare the 10-K's filing date to the original filing's date for that quarter.

3. If the 10-K is more recent (it always will be), override the derived value.

**What this catches:**
- Dell FY2025 10-K restated all of FY2024 Q1-Q4 for COGS, operating income, tax, pretax income, and net income.
- Any company that presents restated comparative quarterly data in a 10-K.

**What this does NOT catch:**
- Restatements that only appear in prose (MD&A text) without updated XBRL facts.
- Restatements filed as separate 10-Q/A or 10-K/A filings (these would need to be fetched and would replace the original filing entirely).

## Output Filtering

The `--fy-start` / `--fy-end` filter is applied **after** all derivation and restatement overrides. This ensures:

- Derivation dependencies are always available (e.g., Q1 needed for Q2 CF subtraction)
- Restatement data from later filings can reach earlier quarters
- The filter only controls what appears in the output, not what participates in computation

## Negate Convention

Some components (capex, acquisitions, interest expense) are reported as positive in XBRL but expected as negative in the output (or vice versa). The `negate: true` flag on a component definition flips the sign after extraction.

## Default Values

Components with `default: 0` (e.g., short-term debt) return 0 instead of null when no XBRL value is found. Used for line items that are legitimately zero for many companies.
