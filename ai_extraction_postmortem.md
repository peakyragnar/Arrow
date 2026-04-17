# AI Extraction Postmortem

A candid review of what we tried, what worked, what didn't, and what the
investment actually produced.

## Hypothesis

The AI can read SEC filings and extract structured financial data correctly
across any company, without per-company handlers. Replace the analyst's
"reading and tabulating" work with a general-purpose pipeline.

## What we built

A multi-stage pipeline against NVDA (12 filings, FY24Q1 through FY26Q4):

- **fetch.py** — download 10-Q/10-K HTML + four XBRL files from EDGAR.
- **ai_extract/parse_xbrl.py** — deterministic parse of XBRL instance +
  calculation/presentation/definition linkbases. No AI.
- **ai_extract/analyze_statement.py** (Stage 1) — AI extracts IS/BS/CF +
  segments per filing. Produces `q*_fy*_10*.json` + `mapped.json`.
- **ai_extract/ai_formula.py** (Stage 2) — AI assigns concepts to canonical
  buckets; Python quarterizes + runs verification. Produces
  `formula_mapped_v3.json` + `quarterly.json`.
- **ai_extract/export_full_check_csv.py** — renders Stage 2 output as an
  analyst-format audit CSV.
- **ai_extract/verify_stage1.py** — independent Python verifier that
  re-computes formula ties from the saved per-filing JSONs. Built after
  the AI-self-verification trust failure.
- **ai_extract/canonical_buckets.md** — universal bucket schema aligned to
  the Capital IQ statement presentation.

Plus `ai_extract/ai_extraction_flow_full.md`, `formulas.md`,
`rd_capitalization_reference.md`, `CLAUDE.md` / `AGENTS.md` as supporting
documentation.

## What worked

### Deterministic XBRL infrastructure
- `parse_xbrl.py` reliably produced complete, structured output from every
  filing. Never a problem.
- Cal/pre/def linkbase parsing gave us formula relationships, statement
  structure, and dimension hierarchies for free.
- XBRL face concepts (`us-gaap:Revenues`, `us-gaap:NetCashProvidedByUsed
  InOperatingActivities`, `us-gaap:Assets`, etc.) are reliable ground truth.
  When Stage 2 reads these directly, numbers tie to the filing exactly.

### Architectural pieces
- Canonical bucket schema (IS/BS/CF mapped to Capital IQ presentation) is
  clean and universal.
- Face/note split (distinguishing face line items from note-detail items)
  works and prevents double-counting when an analyst bucket pulls from a
  concept that's also embedded in a face total.
- Bucket-level Q2/Q3/Q4 derivation (Q2 = YTD − Q1 YTD; Q4 = annual −
  Q1−Q2−Q3) is correct and handles concept drift across filings.
- `mapped.json` amendment reconciliation (later filing wins) is the right
  design pattern for handling restatements.
- Cross-statement invariants (`total_assets = total_liabilities_and_
  equity`, `is.net_income = cf.net_income_start`, cash rollforward) all
  pass once we used face-authoritative subtotal values.

### Analyst-format CSV
The final CSV (quarters grouped by fiscal year, canonical sections, FY
totals for flows, snapshots for BS) is a usable audit tool. It reads like
a Capital IQ statement view and matches the filing's own arithmetic at the
subtotal level.

## What didn't work

### Cash flow sign conventions
The AI could not reliably infer sign conventions for CF line items across
filings. The same XBRL concept might be stored positive in one filing and
negative in another depending on how the filing's CF display rendered it
(parentheses vs plain number). This affected 8 of 12 NVDA filings.

Specific examples:
- `IncreaseDecreaseInAccountsReceivable`: stored positive in FY26
  filings (raw XBRL convention) vs negative in FY24 filings (cash-impact
  convention). Summing gives inconsistent CFO reconstructions.
- `PaymentsForRepurchaseOfCommonStock`: same concept, sometimes stored
  positive (amount), sometimes negative (cash impact).
- Gains vs losses on investments: AI unreliable in selecting sign.

Result: when Stage 2 summed CF detail buckets, the sum did not tie to the
reported CFO in 8 filings. Deltas ranged from $279M to $22B.

### AI self-verification trust failure
Stage 1 was designed to "verify its own math" in a `formula_verification`
block. The verification logic itself (`verify_formulas()` in
`analyze_statement.py`) is actually a Python routine — it computes from
the stored values and compares to the stated subtotals. It correctly
identifies when math doesn't tie and marks `pass=False`.

The failure was architectural: Stage 1 saved output **regardless of
pass/fail**. The `pass` flag was written to the JSON but never gated
downstream consumption. Stage 2 loaded the data without checking the flag.

This wasn't Stage 1 "lying" — it was honest about its failures — but
nothing in the pipeline refused to propagate broken data.

### Non-determinism
- Same filing + same prompt + same model produces different extraction
  results across runs. Sign choices vary. Bucket assignments vary.
- This makes reproducibility impossible without caching every output.
- Harder to diagnose bugs: a pipeline that worked yesterday might fail
  today for reasons unrelated to code changes.

### Cost creep
- Budgeted ~$3/filing for Stage 1 based on early estimates.
- Actual ~$7-8/filing once retries and completeness checks included.
- Failed extractions cost as much as successful ones; we pay for every
  attempt.
- Estimated cost to fix all 8 NVDA broken filings: $300-600 depending on
  retry count and whether we escalate to Opus.

### Brittleness, different flavor
The move from the deterministic pipeline to AI was motivated by "the
deterministic pipeline is brittle — every company needs its own handlers."
True.

But the AI pipeline is brittle in a different way: non-deterministic
errors that are harder to reproduce and diagnose. A failure in
`deterministic-flow/companies/nvda.py` was a known-location edit; a
failure in the AI pipeline could be anywhere, cost money to even
reproduce, and might vanish on the next run.

We traded known-error-modes for random-error-modes. Random is worse.

### The validation problem doesn't go away
The fundamental realization: both deterministic and AI extraction produce
output that requires human audit before it can be trusted for a new
company. Neither eliminates the validation burden.

For companies we've manually audited (NVDA's golden_eval.xlsx), either
pipeline is fine — we'll catch discrepancies. For new companies, both
pipelines produce unvetted output. "Does the AI version eliminate the
need to audit?" — no. So the core scalability problem remains.

## Key lessons

### 1. XBRL face concepts are the ground truth
For standard subtotals (Revenue, CFO, Total Assets, Net Income, etc.)
don't involve AI. Map canonical names → XBRL concept names and read
directly. Use the `SUBTOTAL_FACE_CONCEPTS` pattern from `ai_formula.py`.

### 2. Python owns verification
The AI should never grade its own work. Every correctness check must be
recomputed by Python from raw inputs. Any AI-produced `pass` flag is
metadata, not a gate.

### 3. Save-gate or it didn't happen
If the pipeline saves output when verification fails, verification
doesn't exist. Fail loud, delete stale files, force re-invocation. This
was implemented late and revealed the scale of silent breakage.

### 4. AI is unreliable for sign conventions
Sign conventions depend on subtle display conventions in filings. AI
inference is not reliable across runs for these. If you need
sign-correct CF components, either:
- Use XBRL raw values + hardcoded sign formulas (deterministic), or
- Accept that the reported subtotal is authoritative and treat components
  as informational.

### 5. Deterministic > AI for known concepts
If a value has a stable XBRL concept name, extract it deterministically.
Zero AI errors. Zero variability. Zero cost.

### 6. Commercial vendors sell labor, not just code
Data vendors employ analyst staff to handle edge cases. $1,788/yr buys
access to their pipeline **plus** their ongoing review/validation labor.
Solo engineer + AI cannot replicate that labor component, no matter how
clever the code.

### 7. AI belongs in synthesis, not extraction
AI's genuine edge is judgment work over unstructured data:
- MD&A interpretation
- Earnings call transcript synthesis
- News contextualization
- Multi-source reasoning (agent-style)

Not: "read a number from a filing." That's XBRL's job.

### 8. Brittleness is irreducible
Every financial-data pipeline is whack-a-mole at some level. Every company
has quirks. Every year brings new edge cases. The question is whether the
moles pop up in predictable places (deterministic) or random places (AI).

## Recommended path forward

### Archive this extraction pipeline
Move `ai_formula.py`, `analyze_statement.py`-as-primary-extraction, and
`export_full_check_csv.py` to an archived tier similar to
`deterministic-flow/`. Keep the code readable for reference but stop
investing in it as the main data path.

### Subscribe to a commercial feed
FMP Ultimate (~$1,788/yr) or Finnhub equivalent. Get clean financial
statements for thousands of tickers in minutes. Treat vendor data as the
source of truth.

### Repurpose existing infrastructure
- **parse_xbrl.py + verify_stage1.py**: keep as an **audit tool** to
  sanity-check vendor data when a number looks suspicious.
- **canonical_buckets.md**: use as a reference for normalizing vendor
  data into your analyst presentation.
- **Filing fetch pipeline**: repurpose for real-time earnings-day
  monitoring — the one place where being faster than the vendor's update
  cycle matters (~30 min to ~24 hr gap where freshly-filed data isn't
  yet in vendor feeds).

### Invest AI budget in Layer 2–4
- **Layer 2 (qualitative)**: build extraction / curation of MD&A,
  transcripts, news. Vendors do this poorly.
- **Layer 3 (market data)**: buy from Polygon or similar.
- **Layer 4 (synthesis)**: an agent that reads the full corpus
  (structured from vendor + qualitative from your pipeline + market
  from vendor) and produces thesis memos / alerts. **This is where no
  off-the-shelf product competes.** It's the actual differentiation.

## Cost summary

| Category | Approx. cost |
|---|---|
| Stage 1 AI extraction (NVDA, 12 filings, multiple attempts) | ~$100 |
| Stage 2 AI runs (four full runs) | ~$6 |
| Total API spend for this experiment | ~$106 |
| Engineer-time invested | ~6-8 weeks of focused work |

## The refined thesis

**Original:** Build a universal AI data extraction pipeline to eliminate
per-company handler maintenance.

**Refined:** Buy commodity financial data. Build synthesis — the part no
vendor packages well. AI is the analyst on top of structured data, not
the data-entry clerk.

The experiment wasn't wasted. We learned:
- Where the edges of AI automation actually are (not where we hoped).
- How XBRL really works (deeply — makes us better vendor-data consumers).
- What the architecture of a synthesis layer should look like (canonical
  buckets, face-authoritative subtotals, agent-driven orchestration).

The lesson cost ~$100 and a couple months. That's a reasonable price for
a clear strategic pivot.
