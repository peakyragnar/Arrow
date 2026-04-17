# CLAUDE.md

Arrow is a financial data extraction and synthesis system. It extracts structured financial data from SEC filings using AI, computes analytical metrics, and will ultimately generate forward estimates with reasoning.

## Principles

- **The AI replaces the analyst.** It reads the filing, verifies every formula relationship, reads precise values from the HTML, and accounts for every tagged fact — the same work a human analyst does manually. It is not given a template of fields to fill in.
- **Linkbase data is the foundation.** The SEC filing includes calculation, presentation, and definition linkbase files that declare every formula, statement structure, and dimension hierarchy. These are parsed deterministically and fed to the AI as structured input. The AI verifies — it does not discover structure.
- **Math proves correctness.** Financial statements are a closed system. Every subtotal must tie to its components, every cross-statement relationship must hold. If the math passes, the extraction is proven correct. No golden eval needed for the three statements — the math IS the eval.
- **Nothing unaccounted for.** Every XBRL-tagged fact must be placed — either on the statement face or identified as hidden/aggregated. The AI cannot skip items.
- **Extract as reported, normalize downstream.** Stage 1 extracts exactly what the filing says. Stage 2 does the analytical work (decomposing aggregated items, normalizing labels across filings). Stage 3 is pure arithmetic.
- **No forced mappings.** The AI is never given a list of fields to look for. It reads the data and figures out what's there. This applies to financial statements, segments, and all disaggregation data.
- **Per-filing extractions are training data.** Every extraction is stored immutably for training a post-trained model to replace API costs.

## Pipeline

```bash
# 1. Download filings + XBRL linkbases from SEC EDGAR (5 years of history)
python3 fetch.py --cik 0001045810 --ticker NVDA

# 2. Deterministic parse: facts + linkbases → structured JSON (per filing)
python3 ai_extract/parse_xbrl.py --ticker NVDA --accession <ACCESSION>

# 3. Stage 1: AI extracts + verifies IS/BS/CF/segments per filing
python3 ai_extract/analyze_statement.py --ticker NVDA --accession <ACCESSION> --statement all

# 4. Stage 2: Quarterize statements + map normalized buckets (all periods)
python3 ai_extract/ai_formula.py --ticker NVDA --test

# 5. (Optional) Extract R&D history for 20-quarter capitalization lookback
python3 ai_extract/extract_rd_history.py --ticker NVDA

# 6. Calculate financial metrics (ROIC, growth, margins, etc.)
python3 calculate.py --ticker NVDA

# 7. Serve dashboard
python3 -m http.server 8080 --directory dashboard
```

## File Map

```
fetch.py                    — Downloads 10-Q/10-K/amendments + XBRL linkbases from SEC EDGAR
calculate.py                — Computes metrics from quarterly.json + rd_history.json
formulas.md                 — Canonical metric dictionary (references canonical_buckets.md)
rd_capitalization_reference.md — R&D amortization: 20-quarter straight-line, real quarters only
golden_eval.xlsx            — Manually verified financial data (source of truth)
dashboard/                  — Single-file HTML app (Chart.js), served locally

ai_extract/
  parse_xbrl.py             — Deterministic parser: XBRL facts + linkbases → parsed_xbrl.json
  analyze_statement.py      — Stage 1: AI extraction + verification (reads parsed_xbrl.json + HTML)
  ai_formula.py             — Stage 2: as-reported quarterization + normalized bucket mapping
  export_full_check_csv.py  — Renders Stage 2 output as a universal audit CSV
  extract_rd_history.py     — Deterministic R&D history (pre-Stage-1-era filings)
  canonical_buckets.md      — Universal IS/BS/CF bucket lists + subtotal formulas + invariants
  ai_extraction_flow_full.md — Pipeline design doc
  {TICKER}/
    q*_fy*_10*.json         — Per-filing Stage 1 extractions (immutable training data)
    mapped.json              — All periods, amendment-aware index
    formula_mapped_v3.json  — Stage 2 output: statements + normalized buckets + segments + analytical
    quarterly.json          — Flat per-quarter analytical values (calculate.py consumes this)
    rd_history.json         — Historical quarterly R&D (deterministic XBRL extraction)
    {ticker}_full_check.csv — Universal audit CSV (Stage 2's rendered view)
    test/                    — Active workspace while iterating; outputs land here

data/filings/{TICKER}/{ACCESSION}/  — Downloaded filings (gitignored)
  *.htm                     — Filing HTML (iXBRL)
  *_htm.xml                 — XBRL instance document (all tagged facts)
  *_cal.xml                 — Calculation linkbase (declared formulas with weights)
  *_pre.xml                 — Presentation linkbase (concept-to-statement mapping)
  *_def.xml                 — Definition linkbase (dimension hierarchies)
  filing_meta.json          — Filing metadata
  parsed_xbrl.json          — Deterministic parse output (Step 2)

deterministic-flow/         — Archived. Do not reference.
```

## Pipeline Detail

See `ai_extract/ai_extraction_flow_full.md` for the complete design and
`ai_extract/canonical_buckets.md` for the universal bucket lists.

**Step 1 — Download** (`fetch.py`): Downloads filing HTML, XBRL instance document, and three XBRL linkbase files (calculation, presentation, definition) from SEC EDGAR. Default coverage: 5 years of filings, enough for 20-quarter R&D lookback.

**Step 2 — Deterministic Parse** (`parse_xbrl.py`): Parses all XBRL files into `parsed_xbrl.json`. Extracts every tagged fact, every declared formula with signed weights, every concept-to-statement mapping, and every dimension hierarchy. No AI — pure parsing.

**Step 3 — AI Extraction** (`analyze_statement.py`): AI receives the parsed linkbase data + filing HTML (auto-sized: full HTML under 150K tokens, else stripped to statement tables). Runs formula verification, CF-section retry, and XBRL fact completeness check. Outputs per-filing JSON (training data) + updates `mapped.json`.

**Step 4 — Stage 2: Statements + Buckets** (`ai_formula.py`): Two outputs from one run, both math-verified.

*Output 1 — as-reported statements, quarterized:* rows from Stage 1 `line_items` and `xbrl_not_on_statement` are merged across filings by `xbrl_concept` (label variants collected into a joined list). Values per quarter are selected deterministically per period-type rule; Q4 flows derived from annual − Q1 − Q2 − Q3; CF Q2/Q3 derived from YTD − prior YTD. Each statement's declared formulas are carried through and evaluated per quarter (scoped to the filings they came from).

*Output 2 — normalized buckets:* the AI assigns each as-reported row to a universal bucket defined in `canonical_buckets.md` (same bucket names for every company). Subtotals are computed, not assigned. Bucket-level formulas must tie in every quarter. Cross-statement invariants enforced: `total_assets == total_liabilities_and_equity`, `net_change_in_cash == cash_eop − cash_bop`, `is.net_income == cf.net_income_start`.

Verification battery: bucket formulas tie, Q1+Q2+Q3+Q4 = annual for every flow bucket, cross-statement invariants, segment-member sums equal consolidated totals, every analytical value reconciles to its source rows, forward-fills audited against raw `parsed_xbrl.json`. Up to 3 retries on failure; hard error if unresolved — no silent accept, no plugs.

Outputs: `formula_mapped_v3.json` (full structure) + `quarterly.json` (flat per-quarter bucket values for `calculate.py`) + `{ticker}_full_check.csv` (universal audit CSV with as-reported rows, normalized buckets, formulas, and segments for every statement).

**Step 5 — R&D History** (`extract_rd_history.py`, standalone): Deterministic extraction of `us-gaap:ResearchAndDevelopmentExpense` from every downloaded filing's XBRL instance doc, with period-type filtering and Q4 = annual − Q1 − Q2 − Q3 derivation. Writes `rd_history.json`. No AI. Only needed when fewer than 20 quarters have been run through Stage 2.

**Step 6 — Metrics** (`calculate.py`): Reads `quarterly.json` as primary source of bucket values. For R&D capitalization, gap-fills from `rd_history.json` when Stage 2 has fewer than 20 quarters. Computes ROIC, margins, growth per `formulas.md`. Numbers benchmarked against `golden_eval.xlsx` — zero drift tolerated.

## What the AI Does vs. Doesn't Do

| Task | Deterministic | AI (judgment) |
|------|---------------|---------------|
| Statement structure | Presentation linkbase provides it | — |
| Formula relationships | Calculation linkbase declares them | — |
| Hidden items (e.g. leases in accrued) | Cal linkbase declares decomposition | — |
| Segment/dimension structure | Def linkbase provides hierarchy | — |
| Precise values where XBRL rounds | — | Reads HTML for exact numbers |
| Stage 1 math verification | — | Confirms filing's own formulas tie |
| Fact completeness (Stage 1) | — | Accounts for every XBRL fact |
| Stage 2 row merge across filings | By `xbrl_concept`; label variants joined | — |
| Stage 2 value selection per quarter | Period-type rule, duration filter | — |
| Stage 2 Q4 + Q2/Q3 derivations | Pure arithmetic | — |
| Stage 2 bucket assignment | Canonical bucket names fixed | Assigns as-reported rows → buckets |
| Stage 2 subtotals + invariants | Computed from bucket values | — |
| R&D history (pre-Stage-1) | Standard XBRL concept, no judgment | — |

## Architecture: 4 Layers

- **Layer 1 — Financial Data**: AI extraction from SEC XBRL filings (built, validated for NVDA)
- **Layer 2 — Qualitative Data**: MD&A, earnings transcripts, news (planned)
- **Layer 3 — Market Data**: Stock prices from price APIs (planned)
- **Layer 4 — Synthesis**: Frontier model generates forward estimates (planned)

## Current Status

- Stage 1 validated for NVDA (12 filings). Note: CF line items in 10-Qs have a
  narrow bug where prior-year comparable column mirrors current-period values.
  Current-period values are correct, which is what Stage 2 actually reads.
- Stage 2 redesigned for universality: two-layer output per statement (as-reported
  rows merged across filings by xbrl_concept + normalized buckets from
  `canonical_buckets.md`). Deterministic quarterization in Python; AI handles
  only bucket assignment. Verification battery with cross-statement invariants.
  No plugs, no silent fills.
- Active workspace: `ai_extract/NVDA/test/`. Parent `ai_extract/NVDA/` holds
  prior golden artifacts from an earlier NVDA-overfit run (read-only reference).
- R&D capitalization reworked: 20 real quarters, no annual-synthesis shorthand.
  Supplemented by deterministic `rd_history.json` when Stage 2 covers fewer
  than 20 quarters.
- Benchmark: `golden_eval.xlsx` (and its JSON mirror
  `deterministic-flow/golden/nvda.json` — data only, not the archived code).
  NVDA must match end-to-end before any other ticker is considered.
