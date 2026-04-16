# CLAUDE.md

Arrow is a financial data extraction and synthesis system. It extracts structured financial data from SEC filings using AI, computes analytical metrics, and will ultimately generate forward estimates with reasoning.

## Principles

- **The AI replaces the analyst.** It reads the filing, extracts the three financial statements, discovers every formula relationship, and verifies the math — the same work a human analyst does manually. It is not given a template of fields to fill in.
- **Math proves correctness.** Financial statements are a closed system. Every subtotal must tie to its components, every cross-statement relationship must hold. If the math passes, the extraction is proven correct. No golden eval needed for the three statements — the math IS the eval.
- **Nothing unaccounted for.** Every XBRL-tagged fact must be placed — either on the statement face or identified as hidden/aggregated. The AI cannot skip items.
- **Extract as reported, normalize downstream.** Stage 1 extracts exactly what the filing says. Stage 2 does the analytical work (decomposing aggregated items, normalizing labels across filings). Stage 3 is pure arithmetic.
- **No forced mappings.** The AI is never given a list of fields to look for. It reads the data and figures out what's there. This applies to financial statements, segments, and all disaggregation data.
- **Per-filing extractions are training data.** Every extraction is stored immutably for training a post-trained model to replace API costs.

## Pipeline

```bash
# 1. Download filings from SEC EDGAR
python3 fetch.py --cik 0001045810 --ticker NVDA

# 2. Stage 1: AI extracts + verifies IS/BS/CF/segments per filing
python3 ai_extract/analyze_statement.py --ticker NVDA --accession <ACCESSION> --statement all

# 3. Stage 2: AI normalizes labels + decomposes aggregated items across filings
python3 ai_extract/ai_formula.py --ticker NVDA

# 4. Stage 3: Pure arithmetic — YTD to quarterly derivation
python3 ai_extract/ai_formula.py --ticker NVDA --from-mapped

# 5. Calculate financial metrics (ROIC, growth, margins, etc.)
python3 calculate.py --ticker NVDA

# 6. Serve dashboard
python3 -m http.server 8080 --directory dashboard
```

## File Map

```
fetch.py                    — Downloads 10-Q/10-K/amendments from SEC EDGAR
calculate.py                — Computes metrics from quarterly.json, writes dashboard data
formulas.md                 — Canonical metric dictionary (ROIC, margins, growth, etc.)
rd_capitalization_reference.md — R&D amortization schedule formula reference
golden_eval.xlsx            — Manually verified financial data (source of truth)
dashboard/                  — Single-file HTML app (Chart.js), served locally

ai_extract/
  analyze_statement.py      — Stage 1: extraction + self-verification loop
  ai_formula.py             — Stage 2: analytical mapping + Stage 3: quarterly derivation
  parse_xbrl.py             — Deterministic XBRL fact parser
  ai_extraction_flow.md     — Detailed pipeline design doc
  {TICKER}/
    q*_fy*_10*.json         — Per-filing extractions (immutable, training data)
    mapped.json             — All periods by period, handles amendments/restatements
    formula_mapped.json     — Normalized fields per filing
    quarterly.json          — Standalone quarterly values (single source of truth)

deterministic-flow/         — Archived deterministic pipeline (extract.py, eval.py, etc.)
data/filings/{TICKER}/      — Downloaded filings (gitignored)
```

## Three-Stage Pipeline

See `ai_extract/ai_extraction_flow.md` for the full design. Summary:

**Stage 1** (`analyze_statement.py`): AI reads the full filing, extracts every line item from IS/BS/CF in presentation order, discovers all formula relationships, verifies all math, extracts all segment/disaggregation data. Loops on failure until everything ties. Outputs per-filing JSON + updates mapped.json.

**Stage 2** (`ai_formula.py`): AI reads extractions across filings. Decomposes aggregated items (e.g., operating leases buried in accrued liabilities — found via XBRL tags not on statement face). Normalizes labels across filings so the same item has one consistent name. Verifies no double counting.

**Stage 3** (`ai_formula.py --from-mapped`): Pure arithmetic. Q1 pass-through, Q2/Q3 CF YTD subtraction, Q4 = annual minus Q1+Q2+Q3. BS snapshots, no derivation. Segments follow same logic as IS fields.

## Architecture: 4 Layers

- **Layer 1 — Financial Data**: AI extraction from SEC XBRL filings (built, validated for NVDA)
- **Layer 2 — Qualitative Data**: MD&A, earnings transcripts, news (planned)
- **Layer 3 — Market Data**: Stock prices from price APIs (planned)
- **Layer 4 — Synthesis**: Frontier model generates forward estimates (planned)

## Current Status

- NVDA: 12 filings extracted, 12 quarters, 264/264 fields match golden eval
- Stage 1 segment extraction working (Q1+Q2 FY26 have segments, 10 older filings pending re-run)
- Deterministic pipeline archived in `deterministic-flow/` (9 companies validated, 0 mismatches)
- `calculate.py` reads from deterministic output — switchover to `quarterly.json` pending
