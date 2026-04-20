# Periods: Fiscal Truth & Calendar Normalization

Canonical rules for how Arrow represents time on every row that describes a company financial period. Pairs with `docs/architecture/system.md` § Foundational Schema Rule: Two Clocks Always.

This doc is the authoritative spec for period fields. If anything here conflicts with `system.md`, `system.md` wins — open a PR updating both together.

---

## 1. Purpose

Every row describing a company financial fact, filing, or transcript must preserve **both**:

- **Fiscal truth** — how the company reported it
- **Calendar normalization** — the real-world quarter it belongs to

These are two parallel representations, not a choice. Both are stored. Both are queryable. Neither is derived from the other at query time — they are derived at ingest time from `period_end` + company fiscal metadata, and written as first-class columns.

Reason: filings, transcripts, guidance, and management commentary are expressed in **fiscal** terms. Cross-company screens, macro alignment, and event timelines need **calendar** terms. Neither dominates; we need both.

---

## 2. Canonical Fields

All fiscal-period-bearing tables (`financial_facts`, `artifacts` where period-relevant, `company_events`, and any chunking table when reintroduced) use these column names with these types.

### 2.1 Fiscal truth

| Column | Type | Nullable | Format / constraint | Example |
|---|---|---|---|---|
| `fiscal_year` | `smallint` | No | Integer year FY ends in | `2025` |
| `fiscal_quarter` | `smallint` | Yes | 1–4; NULL means full fiscal year | `4` |
| `fiscal_period_label` | `text` | No | See § 9 | `FY2025 Q4` |
| `period_end` | `date` | No | Last day of the fiscal period | `2025-01-26` |
| `period_type` | `text` | No | `quarter` or `annual` | `quarter` |

**Invariant:** `period_type = 'quarter' ↔ fiscal_quarter IS NOT NULL`.
**Invariant:** `fiscal_period_label` is derived deterministically from `fiscal_year`, `fiscal_quarter`, `period_type` — see § 9.

### 2.2 Calendar normalization

| Column | Type | Nullable | Format / constraint | Example |
|---|---|---|---|---|
| `calendar_year` | `smallint` | No | Year `period_end` falls in | `2025` |
| `calendar_quarter` | `smallint` | No | 1–4 | `1` |
| `calendar_period_label` | `text` | No | See § 9 | `CY2025 Q1` |

**Invariant:** All three are pure functions of `period_end`. See § 4 for the algorithm.

### 2.3 Fiscal-year-end anchor

Every company carries its fiscal-year-end in `companies.fiscal_year_end_md` (format `MM-DD`, e.g. `01-31` for NVDA, `06-30` for MSFT, `12-31` for calendar-year filers). This anchors both derivations.

For 52/53-week filers like NVDA and AAPL, `fiscal_year_end_md` is the **nominal anchor** (SEC's `fiscalYearEnd` field — for NVDA, the end of January) — not the specific `period_end` date of any one fiscal year. The nominal anchor is the upper bound of where an actual period_end can fall; the algorithm in § 3.2 relies on this property. Storing a specific historical period_end (e.g. `01-26` from NVDA's FY2025 Q4) would misclassify years where the actual end date drifts past it.

---

## 3. Fiscal Truth — Derivation

Primary source: SEC DEI elements on the filing. Fallback: algorithmic derivation from `period_end` + `companies.fiscal_year_end_md`.

### 3.1 From DEI (preferred)

| DEI element | Maps to |
|---|---|
| `dei:DocumentFiscalYearFocus` | `fiscal_year` |
| `dei:DocumentFiscalPeriodFocus` | `fiscal_quarter` — values `Q1`, `Q2`, `Q3` are literal; `FY` on a 10-K maps to `Q4` for quarterly-statement rows *and also* produces an `annual` row |
| `dei:CurrentFiscalYearEndDate` | Confirms `companies.fiscal_year_end_md` |
| `dei:DocumentPeriodEndDate` | `period_end` |

### 3.2 Fallback: compute from `period_end`

When DEI is absent, wrong (see Dell FY2024 Q1/Q2 mislabeling, § 11), or being cross-checked:

```text
FY_end_month, FY_end_day = parse(companies.fiscal_year_end_md)

# 52/53-week drift absorption:
# A 52/53-week filer's quarter-end day can land up to ~7 days past the
# nominal calendar month-end. NVDA FY2000 Q2 actually ended 1999-08-01
# because "Sunday nearest Jul 31" fell on Aug 1 that year — the CONTENT
# month of that period is July, not August, for fiscal-month arithmetic.
# Shift back one week before the month/day arithmetic; this is a no-op on
# canonical late-month period_ends and a correction on early-next-month
# drifted period_ends.
effective = period_end - timedelta(days=7)

# Fiscal year naming: FY is named after the calendar year it ends in.
# A period whose EFFECTIVE date is past the fiscal-year-end anchor belongs
# to the NEXT fiscal year.
if (effective.month, effective.day) > (FY_end_month, FY_end_day):
    expected_fiscal_year = effective.year + 1
else:
    expected_fiscal_year = effective.year

# Quarter within fiscal year, computed on the effective date.
FY_start_month = (FY_end_month % 12) + 1
months_elapsed = ((effective.month - FY_start_month) % 12) + 1   # 1..12
expected_fiscal_quarter = ceil(months_elapsed / 3)                # 1..4
```

**Use `ceil`, not `round`.** Once `effective` is in the content month, `months_elapsed` is exact (no drift) and we need `ceil(n/3)` to map 1..12 into quarters 1..4. `round(n/3)` would misclassify month 1 (→ round(1/3)=0) and is wrong.

**Why subtract a week, not a day.** A single day handles 1-day drift past a month boundary (Aug 1 → Jul 31) but fails on 2-day drift (Aug 2 → Jul 31 OK, but May 2 → May 1, still the wrong month). Seven days covers a full week's drift with no ambiguity, and never wraps across a quarter boundary (each quarter spans ~13 weeks). Empirically fits every NVDA period_end from FY2000 to FY2026.

### 3.3 Rule: when DEI and algorithm disagree

Algorithm wins. Record both values and flag the row as `dei_override` in provenance. Known DEI failures (Dell FY2024 Q1/Q2, NUE FY2023 Q2) are corrected this way; the raw DEI stays in `raw_responses` for audit.

---

## 4. Calendar Normalization — Derivation

Pure functions of `period_end`:

```text
calendar_year    = period_end.year
calendar_quarter = (period_end.month - 1) // 3 + 1
```

No company metadata involved. Calendar normalization is about when the period actually ended in real-world time.

### 4.1 Worked mapping

| Company | Fiscal | Period end | Calendar |
|---|---|---|---|
| NVDA | FY2025 Q1 | 2024-04-28 | CY2024 Q2 |
| NVDA | FY2025 Q2 | 2024-07-28 | CY2024 Q3 |
| NVDA | FY2025 Q3 | 2024-10-27 | CY2024 Q4 |
| NVDA | FY2025 Q4 | 2025-01-26 | CY2025 Q1 |
| MSFT | FY2024 Q4 | 2024-06-30 | CY2024 Q2 |
| AAPL | FY2024 Q4 | 2024-09-28 | CY2024 Q3 |
| PLTR | FY2024 Q4 | 2024-12-31 | CY2024 Q4 |

---

## 5. The 52/53-Week Case

Many filers (NVDA, AAPL, retailers broadly) end each fiscal period on a specific weekday (e.g. "last Saturday of the month") rather than the calendar month-end. This produces:

- 52-week fiscal years most years, 53-week years periodically
- `period_end` dates that drift 1–6 days from the month-end
- Occasional "stub" weeks in restated or transitional filings

**Rule:** we never reconstruct the 52/53-week calendar. We take `period_end` at face value from the filing and use it directly in all calculations.

Implications:
- `calendar_quarter` derived from `period_end` month is **always correct** by the face-value rule — even if NVDA's Q1 ends April 28 vs April 30.
- `fiscal_quarter` derived from months elapsed uses **`ceil`** to absorb 1-day spillovers (see § 3.2).
- For period-length math (e.g. "was this a 13-week or 14-week quarter?"), compute `period_end - prior_period_end` at query time; do not store it.

---

## 6. Q4 Derivation

Only income-statement and cash-flow **flows** are Q4-derived. Balance-sheet **stocks** use the year-end snapshot directly and are never derived.

### 6.1 Default rule

```text
Q4_flow = FY_flow  −  (Q1 + Q2 + Q3)_flow
```

For filers that report each quarter as a discrete 10-Q quantity, this is the direct subtraction. For filers that report cash-flow YTD-only (most of them), `(Q1 + Q2 + Q3)` is taken from the **9-month YTD value in the Q3 10-Q** (which is already a cumulative three-quarters figure):

```text
Q4_flow = FY_flow (from 10-K)  −  9M_YTD_flow (from Q3 10-Q)
```

### 6.2 Restatement handling

When a 10-K restates one or more of Q1–Q3, naively subtracting the pre-restatement 9M YTD produces an inconsistent Q4. Rule:

```text
if the 10-K carries DocumentFinStmtErrorCorrectionFlag = true
   OR any Q1..Q3 row has been superseded:
    Q4_flow = restated_FY  −  restated_Q1  −  restated_Q2  −  restated_Q3
else:
    Q4_flow = FY  −  9M_YTD
```

The restated Q1/Q2/Q3 values come from the restating filing itself (10-K amendment or subsequent 10-K comparatives). Earlier superseded rows remain in `financial_facts` with `superseded_at` set — they are not deleted, just deprioritized by the PIT query.

### 6.3 Balance-sheet Q4

Not derived. `period_end` on the 10-K *is* the Q4 balance-sheet snapshot. Treat as `period_type = 'quarter', fiscal_quarter = 4` and simultaneously emit an `annual` row (same `period_end`, `period_type = 'annual'`) for year-end balance references.

---

## 7. YTD → Discrete Quarter Conversion

Cash-flow statements report **cumulative YTD only**. Discrete quarters are subtracted at ingest time, not stored as YTD.

```text
Q1_discrete = Q1_YTD              (from Q1 10-Q, identical)
Q2_discrete = H1_YTD  − Q1_YTD    (H1_YTD from Q2 10-Q, Q1_YTD from Q1 10-Q)
Q3_discrete = 9M_YTD  − H1_YTD    (9M_YTD from Q3 10-Q, H1_YTD from Q2 10-Q)
Q4_discrete = FY      − 9M_YTD    (see § 6)
```

### 7.1 Reclassification detection

If a later filing silently recasts prior-quarter CF presentation (moving items between lines without changing totals), naive subtraction produces wrong discrete values. Rule:

1. Compute both: the original Q1 (from Q1 10-Q) and the implied Q1 (from Q2's H1 YTD − Q2 discrete).
2. If they differ by more than **0.5%** of the larger absolute value, treat as a reclassification candidate.
3. Confirm by comparing prior-year comparatives in the later filing — the comparatives also get recast.
4. If confirmed, override: use the later filing's values. Supersede the older row with `superseded_at` set; preserve the original in `financial_facts` for audit.
5. If not confirmed (only the current period changed, prior-year stayed), treat as a data error in one of the filings, emit a `reconcile` flag, and leave both rows for manual review.

### 7.2 Required upstream rows

Q2/Q3 derivation requires Q1 (and H1) filings. Ingest order: Q1 → Q2 → Q3 → 10-K. If Q1 is missing (e.g. newly-public filer, PLTR), company-specific overrides supply the Q1 value or defer the series.

---

## 8. Period Types in `financial_facts`

Each row is one (concept, period) fact. Period identification uses:

| `period_type` | `fiscal_quarter` | Example row |
|---|---|---|
| `quarter` | `1`, `2`, `3`, or `4` | NVDA FY2025 Q2 revenue |
| `annual` | `NULL` | NVDA FY2025 revenue |

**Rule:** flows are stored as **discrete quarters** (`period_type = 'quarter'`). TTM is never stored — it is derived at query time by summing the most recent 4 discrete quarters. Annual rows exist in addition to the quarterly rows, not instead of them; they are the filer's own annual disclosure (10-K totals) and may differ from the sum of quarters due to rounding or reclassification.

YTD values are **not stored** as rows. YTD is an extraction-time shape. Discrete is the storage shape. (Extractors may use YTD columns from raw payloads temporarily; they do not survive into `financial_facts`.)

Balance-sheet stocks are stored once per reporting event: both as `period_type = 'quarter'` (with `fiscal_quarter`) and as `period_type = 'annual'` on the Q4/10-K filing.

---

## 9. Label Formats

Deterministic strings derived from the fiscal/calendar integers:

### 9.1 Fiscal

```
period_type = 'quarter': fiscal_period_label = "FY{fiscal_year} Q{fiscal_quarter}"
period_type = 'annual':  fiscal_period_label = "FY{fiscal_year}"
```

Regex: `^FY\d{4}( Q[1-4])?$`
Examples: `FY2025 Q4`, `FY2025`.

### 9.2 Calendar

```
calendar_period_label = "CY{calendar_year} Q{calendar_quarter}"
```

Regex: `^CY\d{4} Q[1-4]$`
Example: `CY2025 Q1`.

Calendar labels are always quarterly — annual fiscal rows still have a `period_end` that falls in one calendar quarter, and `calendar_period_label` names that quarter. If a calendar-year-level label is ever needed, derive from `calendar_year` at query time; do not store.

### 9.3 Why fixed formats

Ingesters and agents parse these strings (in tool calls, CLI, agent queries). A deterministic grammar means no ambiguity — `"FY2024Q4"`, `"Q4 2024"`, `"FY 24 Q4"` are all **wrong** and should fail a CHECK constraint.

---

## 10. Fiscal Year Changes

Companies occasionally change fiscal-year-end (M&A, restructuring). This creates a stub period that is neither a full year nor a clean quarter.

Rule:

1. Treat as a **new fiscal series**. `companies.fiscal_year_end_md` is updated; the old value is preserved in `companies_fiscal_history` (future table) with effective-dated rows.
2. The stub period is stored with `period_type = 'stub'` (new enum value, added to the CHECK constraint when this comes up), `fiscal_quarter = NULL`, `period_end` = actual end of the stub.
3. Calendar fields derive normally from `period_end`.
4. Historical rows prior to the change keep their original fiscal_year numbering; do not renumber.

This is rare enough to punt implementation until the first filer triggers it. The rule is documented so future-us doesn't improvise.

---

## 11. Invariants

These must hold for every period-bearing row. CHECK constraints enforce them in schema.

1. `period_end IS NOT NULL`
2. `fiscal_year BETWEEN 1900 AND 2100`
3. `period_type IN ('quarter', 'annual')` (add `'stub'` when § 10 triggers)
4. `(period_type = 'quarter') = (fiscal_quarter IS NOT NULL)`
5. `fiscal_quarter IN (1, 2, 3, 4)` when not NULL
6. `calendar_year = EXTRACT(year FROM period_end)`
7. `calendar_quarter = FLOOR((EXTRACT(month FROM period_end) - 1) / 3) + 1`
8. `fiscal_period_label` matches regex `^FY\d{4}( Q[1-4])?$`
9. `calendar_period_label` matches regex `^CY\d{4} Q[1-4]$`
10. `fiscal_period_label` and `calendar_period_label` are deterministic from their integer fields (enforced by trigger or generated column)

---

## 12. Worked Examples

### 12.1 MSFT FY2024 Q4 (June 30 year-end)

```
period_end              = 2024-06-30
companies.fiscal_year_end_md = "06-30"
DEI fiscal_year_focus   = 2024
DEI fiscal_period_focus = "FY" → maps to Q4

Derived:
  fiscal_year         = 2024
  fiscal_quarter      = 4
  period_type         = "quarter" (plus a separate "annual" row for the 10-K full-year)
  fiscal_period_label = "FY2024 Q4"
  calendar_year       = 2024
  calendar_quarter    = 2          (June → Q2)
  calendar_period_label = "CY2024 Q2"
```

### 12.2 NVDA FY2025 Q1 (52/53-week, Jan year-end)

```
period_end              = 2024-04-28   (last Sunday of April, not April 30)
companies.fiscal_year_end_md = "01-31"  (SEC nominal anchor; actual FY-end period_end drifts
                                          across late Jan under NVDA's 52/53-week calendar)
DEI fiscal_year_focus   = 2025
DEI fiscal_period_focus = "Q1"

Derived (algorithmic cross-check):
  FY_end_month     = 1
  FY_start_month   = 2
  months_elapsed   = ((4 - 2) % 12) + 1 = 3
  fiscal_quarter   = ceil(3 / 3) = 1   ✓ matches DEI
  year-before-check: (4,28) > (1,31) = true → fiscal_year = period_end.year + 1 = 2025 ✓

Stored:
  fiscal_year           = 2025
  fiscal_quarter        = 1
  fiscal_period_label   = "FY2025 Q1"
  period_end            = 2024-04-28
  period_type           = "quarter"
  calendar_year         = 2024
  calendar_quarter      = 2           (April → Q2)
  calendar_period_label = "CY2024 Q2"
```

### 12.3 PLTR FY2024 Q4 (Dec year-end = calendar)

```
period_end              = 2024-12-31
companies.fiscal_year_end_md = "12-31"

fiscal_year            = 2024
fiscal_quarter         = 4
fiscal_period_label    = "FY2024 Q4"
calendar_year          = 2024
calendar_quarter       = 4
calendar_period_label  = "CY2024 Q4"
```

Fiscal ≡ calendar for this filer. Correct, expected, stored in both forms anyway.

### 12.4 DELL FY2025 Q4 (restatement-affected Q4)

```
period_end              = 2025-01-31
DocumentFinStmtErrorCorrectionFlag = true
Q1, Q2, Q3 restated in the 10-K

Q4 derivation:
  NOT: Q4 = FY − 9M_YTD_pre_restatement     # wrong
  YES: Q4 = restated_FY − restated_Q1 − restated_Q2 − restated_Q3

Superseded rows:
  Original Q1/Q2/Q3 rows remain in financial_facts with superseded_at = 10-K
  published_at.
```

---

## 13. Known Failure Modes (Pointers)

| Failure | Example | Mitigation |
|---|---|---|
| Incorrect DEI `DocumentFiscalPeriodFocus` | Dell FY2024 Q1/Q2 tagged `FY` instead of `Q1`/`Q2` | Algorithmic cross-check (§ 3.3); company override |
| Incorrect DEI `DocumentFiscalYearFocus` | NUE FY2023 Q2 tagged as FY2022 Q3 | Same |
| Multiple instant BS contexts within 3 days | SYM FY2023 Q2 on 2023-03-24 AND 2023-03-25 | Take context closest to report date (§ 11 not covered here — see ingest layer) |
| Q4 derivation with restatements | Dell FY2025 Q4 | Use restated subtractions (§ 6.2) |
| CF line reclassifications across filings | MSFT FY2026 Q1 D&A recast | 0.5% tolerance check + prior-year comparative confirmation (§ 7.1) |
| Spin-offs affecting derivation | Dell FY2022 VMware spin | Company override (`fix_rd_series`) |
| IPO mid-year, missing Q1 | PLTR FY2021 | Company override or accept series gap |

Each of these is handled in the **normalize/** layer per company, not in the universal rules. The universal rules must tolerate company overrides via a priority pattern: DEI → algorithmic derivation → company-specific override. The override is the last word but must be auditable (source + reason in provenance).

---

## 14. Open Questions (Deferred)

Issues we've surfaced but not resolved; we document them so future-us knows they're known.

1. **Stub periods from fiscal-year changes.** § 10 sketches the approach but no implementation until triggered.
2. **Weekly calendars** (e.g. 4-4-5 retailers publishing weekly revenue). Store at the month level for now; revisit when a filer we care about publishes weekly.
3. **Multiple period_end dates within one filing** (e.g. 10-K with both an October fiscal year-end and a November stub). Treated case-by-case in ingest; no universal rule yet.
4. **Fiscal period labels for non-US filers** (JP, UK conventions). Schema supports any year/quarter integers; labels are anglo-centric. Revisit when we ingest a non-US filer.
5. **Intra-quarter period_end drift** (company moves fiscal-quarter-end by a few days). Store face-value; flag as `period_end_shift` event in `company_events` (future table).

---

## 15. What this doc unlocks

- `financial_facts` schema (Build Order step 9) — column list and invariants are ready
- `artifacts` schema where periods are relevant (Build Order step 7) — same columns reused
- `company_events` (step 13) — reuses these fields for event-period alignment
- Derived views:
  - Fiscal summary by (`fiscal_year`, `fiscal_quarter`)
  - Calendar-normalized summary by (`calendar_year`, `calendar_quarter`)
  - PIT summary with `asof_date` filtering on `published_at`/`superseded_at`

Period math will never live inside an analyst tool call. Calendar normalization is stored at ingest, not computed at retrieval.
