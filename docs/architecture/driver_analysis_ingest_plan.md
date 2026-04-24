# Driver Analysis Ingest Plan

Status: working plan

This document turns the current ingestion audit into an execution plan. It is
not a new architecture from a blank page. It checks the existing Arrow design
against the analyst questions Michael wants the system to answer, then names
the narrow ingest decisions that matter before more data is loaded.

## Target Questions

Arrow should support these analyst questions for a company over time:

1. What are the key drivers of revenue growth?
2. What are the key drivers of margin improvement or deterioration?
3. What are the key drivers of cash generation?
4. What is changing in those drivers?
5. What has changed in company reporting over time, including
   inconsistencies, emphasis, and sentiment?
6. How does management commentary match later performance? Is management
   over-promising and under-delivering, or the reverse?
7. What data appears to drive future performance on these metrics, based on
   point-in-time backtests?
8. What forward revenue and EPS estimate is supported by the evidence?

These are driver and claim-tracking questions. They require more than storing
filings. They require joined outcomes, drivers, commentary, source timing, and
eventual backtests.

## Current Audit Verdict

The existing design is compatible with the target questions. Current ingest is
mostly on-spec. No currently loaded data needs to be discarded or rewritten.

The main ingest risk is segment data. Segment/product/geography revenue is the
first source that can turn revenue growth from a top-line fact into a driver
analysis. It should not be ingested casually into ad hoc concept names.

### Passing Foundations

| Foundation | Current state |
|---|---|
| Company identity | SEC artifacts and FMP facts key by `company_id`. |
| Fiscal truth and calendar normalization | `financial_facts` stores both clocks directly; SEC sections inherit calendar context through parent `artifacts`. |
| PIT timing for facts | `financial_facts.published_at` is parsed from FMP `acceptedDate` or `filingDate`, not ingest time. |
| Supersession | `financial_facts.superseded_at` and the current-row unique index support PIT queries. |
| SEC section identity | `section_key` gives stable SEC item-level units such as MD&A and Risk Factors. |
| Chunk provenance | Section chunks store text, offsets, heading path, and chunker version. |
| Regeneratable qualitative structure | Section and chunk versions allow re-extraction or re-chunking from stored artifacts. |

### Important Limitations

| Limitation | Why it matters |
|---|---|
| SEC section keys are item-level, not risk-factor-level entities. | Section-level diffs are feasible now; individual risk-factor drift needs later atoms/entities. |
| Current chunks are retrieval units, not atoms. | Later guidance, driver, risk, and claim atoms should point back to chunks plus quote spans. |
| FMP `published_at` timezone is vendor-derived. | Good enough for period-level PIT; not authoritative for intraday event-reaction work. |
| Segment rows are not loaded today. | Q1-Q4 driver analysis lacks the key quantitative substrate. |
| Some companies have SEC artifacts but no extracted sections yet. | Existing pipeline can fill this; it is coverage work, not a schema redesign. |
| Press-release artifacts currently need period-linkage cleanup. | Earnings releases should be pairable with the fiscal period they discuss. |

## Core Modeling Rule

Keep source truth, observations, and derived signals separate.

| Layer | Meaning | Examples |
|---|---|---|
| Source evidence | Text or document evidence that can be cited. | SEC sections/chunks, press releases, transcripts, news. |
| Structured observations | Numeric or tabular facts observed from a source or vendor. | FMP financial facts, segments, prices, options, estimates. |
| Derived signals | Arrow-computed outputs with lineage back to evidence/observations. | Revenue CAGR, margin bridge, risk drift, guidance miss score. |

Derived signals are not source truth. They must carry lineage to the inputs
that produced them.

## Segment Data Decision

Segment ingestion is the one current gap that can create schema regret. The
system should decide the representation before loading FMP segment endpoints.

### Rejected Option A: Encode Segments In `concept`

Example:

```text
concept = 'segment_revenue_data_center'
```

This requires no schema change, but creates concept explosion, string-matching
queries, weak cross-company normalization, and awkward driver analysis.

### Rejected Option C: Separate `segment_facts` Table

A sibling table is cleanly separated, but downstream analysis has to UNION or
special-case segments whenever it wants revenue and segment revenue together.
It also duplicates fiscal/calendar/PIT/provenance semantics already solved in
`financial_facts`.

### Recommended Option B: Dimension Columns On `financial_facts`

Keep the long/skinny fact model and add nullable dimension columns:

```text
dimension_type    -- NULL for normal facts; product, geography, operating_segment
dimension_key     -- normalized company-local key, e.g. data_center
dimension_label   -- vendor/company label, e.g. Data Center
dimension_source  -- fmp:revenue-product-segmentation, fmp:revenue-geographic-segmentation
```

Non-segment rows keep all dimension columns NULL.

Segment rows use:

```text
statement = 'segment'
concept = 'revenue'
dimension_type = 'product' | 'geography' | 'operating_segment'
dimension_key = normalized label
dimension_label = original vendor label
```

This keeps segment revenue queryable beside income-statement revenue while
avoiding concept-name explosion.

### Constraint Impact

This is not just a harmless column add. The existing uniqueness rules must be
updated so multiple segment rows can share:

```text
company_id, concept, period_end, period_type, source_raw_response_id, extraction_version
```

while differing by dimension.

Migration 016 should update:

- the unique constraint used by `ON CONFLICT`
- the current-row unique index
- any loader code that references the old conflict constraint name
- docs that describe `financial_facts`

Use the `ship-schema-change` skill for that migration because it touches
`db/schema/`.

## Future Segment Canonicalization

Dimension keys should first be company-local. Cross-company canonicalization
can come after real labels are observed.

Future mapping table:

```text
segment_dimension_map
company_id
source
dimension_type
dimension_label
dimension_key
canonical_dimension_key
created_at
updated_at
```

Examples:

```text
NVDA / Data Center / data_center -> data_center
AMD / Data Center Segment / data_center -> data_center
INTC / Data Center and AI / data_center_and_ai -> data_center
```

Do not force this mapping before the first segment ingest. Load source labels
faithfully, normalize company-local keys deterministically, and add canonical
mapping once peer comparison needs it.

## Press Release Period Linkage

Earnings-release `press_release` artifacts should carry period context so
future guidance and commentary atoms can be paired with later actuals.

Rule:

- Scope cleanup to earnings-release press releases only.
- Prefer the parent 8-K's period identity when present.
- Do not assign fiscal periods to generic 8-K event releases without evidence.

This is expected to be a small backfill/update script, not re-extraction.

## SEC Coverage Work

Some companies have 10-K/10-Q artifacts but no extracted sections. That is an
operational coverage issue, not a schema problem.

Next action:

- run the existing SEC qualitative pipeline for companies with artifacts but
  zero `artifact_sections`
- validate MD&A and Risk Factors coverage for at least NVDA, AMD, INTC, MSFT,
  AVGO, AMZN, GOOGL, TSLA, PLTR, VRT, GEV, and CRWV

Q5 is feasible today only at section level. Per-risk-factor drift requires a
later atom/entity layer.

## Planned Source Extensions

Do not design full schemas for all future sources now. Do preserve the landing
contract for each source.

| Source | Required landing contract |
|---|---|
| Transcripts | `company_id`, fiscal period, event time, published time, speaker, Q/A boundaries, source artifact, text chunks. |
| Prices | `company_id`, observed time, OHLCV, adjustment policy, vendor/source, ingest time. |
| Options | `company_id`, observed time, expiration, strike, option type, bid/ask, IV/greeks/open interest if available, vendor/source. |
| News | `company_id` or resolved company mentions, publisher, URL/hash, published time, article text, source artifact. |
| Analyst estimates | target period, metric, estimate date/known time, value, analyst/broker/consensus source, revision history. |
| Industry research | source, author/publisher, published time, reviewed time, asserted validity, company/industry entities, citeable text. |

All future sources must preserve company identity, time semantics, provenance,
and enough source detail for PIT backtests.

## Execution Plan

### Phase 1: Protect Segment Ingest

1. Write ADR for segment facts in `financial_facts`.
2. Implement migration 016 with `ship-schema-change`.
3. Add nullable dimension columns to `financial_facts`.
4. Update uniqueness and loader conflict handling for dimensional facts.
5. Implement FMP segment fetchers:
   - `revenue-product-segmentation`
   - `revenue-geographic-segmentation`
6. Load segment rows as `statement = 'segment'`, `concept = 'revenue'`.
7. Add tests proving multiple segments can coexist for the same company and
   period.

### Phase 2: Clean Current Qualitative Coverage

1. Backfill earnings-release `press_release` period linkage.
2. Re-run SEC section/chunk extraction for companies with artifacts but no
   sections.
3. Validate section coverage for MD&A and Risk Factors over the five-year
   qualitative window.

### Phase 3: Build Driver Query Substrate

1. Build derived views for revenue, margin, cash generation, and working
   capital drivers.
2. Keep metrics computed in views, not stored tables, unless a reusable signal
   needs lineage and review.
3. Add simple driver bridge queries before adding LLM analysis.

### Phase 4: Add Claim Sources

1. Add transcripts.
2. Extract first-pass qualitative atoms:
   - stated drivers
   - guidance statements
   - management claims
   - reporting changes
3. Every atom must point back to source artifact/chunk and quote span.

### Phase 5: Add Market And Backtest Sources

1. Add prices.
2. Add analyst estimates.
3. Add options.
4. Add news and industry research.
5. Build PIT feature sets for backtesting leading indicators.
6. Only then attempt "what did the market miss?" and forward revenue/EPS
   estimates.

## Near-Term Priority

The next concrete work should be:

1. ADR for segment representation.
2. Schema migration 016 for dimension-aware `financial_facts`.
3. FMP segment ingest.
4. Earnings-release period-linkage backfill.
5. SEC coverage run for companies missing sections.

Do not build the full analyst packet yet. Do not build full topic entities or
drift detectors yet. The immediate goal is to ensure today's ingestion will not
block tomorrow's driver analysis.
