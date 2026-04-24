# SEC Qualitative Layer

## Goal

Build the SEC qualitative layer so Arrow can:

- preserve full filing narrative as canonical evidence
- retrieve the right passages quickly for frontier-model analysis
- handle amendments correctly without overwriting source truth
- stay consistent with Arrow's search-first, PIT-aware, training-ready architecture

Operational default retention:

- SEC qualitative full layer: last 5 fiscal years of `10-K` / `10-Q`, plus
  any pre-window quarters needed to complete the first included fiscal year.
  The boundary is computed from `companies.fiscal_year_end_md`; it is not a
  naive filing-date cutoff. Example: if the calendar cutoff is January 1 and
  a company's annual 10-K for that fiscal year is filed after the cutoff,
  include that fiscal year's Q1-Q3 10-Qs even if those 10-Qs were filed before
  January 1.
- Earnings `8-K` retention is filing-date based over the same calendar window.
  The `8-K` itself is the filing envelope. EX-99 earnings-release exhibits are
  stored as `press_release` artifacts and extracted into the generic text-unit
  layer, not the 10-K/10-Q section hierarchy.
- stored raw files: `index.json` + primary filing document for 10-K/Q; for
  earnings 8-Ks, also retain earnings-release exhibits such as EX-99.1 or
  filename/description-detected earnings releases
- do not default-retain package sidecars (`EX-101`, `.xsd`, `.zip`, images, css/js, etc.)
- FMP remains the 10-year numeric history source of truth

## Core Structure

The qualitative hierarchy is:

1. `artifacts`
   - One row per SEC filing document.
   - `10-K`, `10-Q`, `10-K/A`, `10-Q/A` are separate artifacts.
   - Artifacts are immutable source truth.

2. `artifact_sections`
   - One row per extracted full section from a filing artifact.
   - This is the canonical narrative unit.

3. `artifact_section_chunks`
   - Every section is chunked in a standardized way.
   - Chunks are derived retrieval units, not source truth.

4. `artifact_text_units`
   - Generic extracted units for non-10-K/Q text artifacts.
   - Current use case: EX-99 earnings releases attached to 8-Ks.

5. `artifact_text_chunks`
   - Retrieval chunks derived from `artifact_text_units`.

Hierarchy:

- filing artifact
- full extracted section
- standardized chunks

For earnings 8-Ks:

- 8-K filing envelope artifact
- EX-99 `press_release` artifact
- deterministic earnings-release text units
- standardized text chunks

## Identity And Join Contract

Narrative identity is not based on ticker.

Every filing artifact must carry:

- `company_id`
- `fiscal_period_key`
- `form_family`

Rules:

- `company_id` is the stable company identity
- `fiscal_period_key` follows [docs/reference/periods.md](../reference/periods.md)
- `form_family` is `10-K` or `10-Q`
- amendment suffixes like `/A` are not part of `form_family`

The retrieval composition key is:

- `company_id`
- `fiscal_period_key`
- `form_family`
- `section_key`

That is the contract used to connect:

- base filing sections
- amendment sections
- composed company-period narrative views

## Artifacts Layer

For each SEC filing, keep raw data under:

- `data/raw/sec/filings/{CIK}/{ACCESSION}/`

Each filing `artifacts` row must carry:

- `company_id`
- `artifact_type`
- `form_family`
- `fiscal_period_key`
- `published_at`
- `effective_at`
- `ingested_at`
- `period_end`
- fiscal/calendar columns per `periods.md`
- `cik`
- `accession_number`
- `raw_primary_doc_path`
- `raw_hash`
- `canonical_hash`

Lineage fields:

- `amends_artifact_id` — set on amendments, null on base filings

Hash semantics:

- `raw_hash` = sha256 of the primary raw document bytes on disk. Integrity hash for the artifact as stored.
- `canonical_hash` = sha256 of the normalized filing body (post HTML entity decode, unicode NFKC, whitespace collapse). Used to detect same-content re-hosts or re-posts without re-running extraction. The two differ whenever the raw file contains any formatting.
- No third hash column is added unless a genuinely different semantic appears.

Stored vs derived:

- `form_family` may be derivable from SEC form metadata, but if stored it must be validated against `artifact_type`.

Time semantics:

- `published_at` = SEC acceptance timestamp for this specific filing.
- `effective_at` = `published_at`. A filing becomes effective when published.
- `ingested_at` = wall-clock time this artifact row was written.
- `period_end` = last day of the fiscal period the filing reports on.
- As-of queries at a timestamp before an amendment's `published_at` correctly see only the base filing. This is the PIT contract for the qualitative layer.

Uniqueness rule:

- `artifacts` unique on `(cik, accession_number)`. Re-ingestion of the same accession is a no-op, not a new row.

## Canonical Section Layer

Each `artifact_sections` row stores:

- `artifact_id`
- `company_id`
- `fiscal_period_key`
- `form_family`
- `section_key`
- `section_title`
- `part_label`
- `item_label`
- `text`
- `start_offset`
- `end_offset`
- `extractor_version`
- `confidence`
- `extraction_method`

Canonical section keys.

For `10-K`:

- `item_1_business`
- `item_1a_risk_factors`
- `item_1c_cybersecurity`
- `item_3_legal_proceedings`
- `item_7_mda`
- `item_7a_market_risk`
- `item_9a_controls`
- `item_9b_other_information`

For `10-Q`:

- `part1_item2_mda`
- `part1_item3_market_risk`
- `part1_item4_controls`
- `part2_item1_legal_proceedings`
- `part2_item1a_risk_factors`
- `part2_item5_other_information`

Fallback key:

- `unparsed_body`

`extraction_method` enum:

- `deterministic` — headings located cleanly in body order
- `repair` — low-confidence path, repair extractor invoked
- `unparsed_fallback` — no valid heading found, full body stored under `unparsed_body`

`confidence` scale:

- Float in `[0.0, 1.0]`.
- `unparsed_fallback` writes `0.0`.
- `deterministic` requires `confidence >= 0.85`.
- `repair` covers the `(0.0, 0.85)` range.

Uniqueness rules:

- one section row per `(artifact_id, section_key)`
- `unparsed_body` is only allowed when no canonical section was extracted for that artifact

## Section Detection

Extraction is deterministic, not AI-first.

Pipeline:

1. Load the primary filing HTML or filing text.
2. Normalize text:
   - decode HTML entities
   - normalize unicode (NFKC)
   - collapse whitespace
   - strip conservative standalone page-number artifacts
   - strip conservative filing-tail page-number artifacts after signature,
     exhibit, Inline XBRL, and financial-statement furniture
   - remove obvious boilerplate
   - suppress table-of-contents duplicates
3. Detect canonical SEC headings in filing-body order.
4. Mark section starts from real body headings.
5. End each section at the next valid heading.
6. Store the full section span as one `artifact_sections` row.

Part-aware 10-Q rule:

- 10-Q extraction must track the `Part I` / `Part II` header context.
- `Item` numbers are only resolved against the current Part.
- A bare `Item 2` or `Item 1` heading is never mapped to a `section_key` without a Part context.

Low-confidence cases:

- only TOC headings found
- headings out of order
- duplicate ambiguous candidates
- malformed structure
- implausibly short extraction

If no valid section heading is found:

- create one `artifact_sections` row with `section_key = 'unparsed_body'`
- set `confidence = 0.0`, `extraction_method = 'unparsed_fallback'`
- store the full normalized body in `text`

This guarantees every filing artifact has at least one section row.

## Chunk Layer

Every section is chunked. This is mandatory.

Chunking rules:

- chunk only within one section
- never cross section boundaries
- split on structure first:
  - subheadings
  - conservative known embedded subheadings promoted from paragraph text
  - paragraph boundaries
  - sentence boundaries if needed
- reject table-of-contents labels and financial table rows as subheadings
- do not carry sentence overlap across a `heading_path` boundary
- size is a guardrail, not the primary split rule

Standard:

- target size: `1,000-1,500` words
- hard max: about `1,800` words
- overlap: `1-2` sentences
- overlap must be sentence-aligned
- target overlap range: about `10-15%`
- mid-sentence overlap is invalid output

## Earnings 8-K / Press Release Layer

Earnings 8-Ks are not forced into the 10-K / 10-Q section model.

The 8-K primary document is stored as an `8k` artifact. It usually says that
results were furnished under Item 2.02 and that exhibits were furnished under
Item 9.01. The actual useful narrative is commonly an EX-99.1 earnings-release
exhibit, stored separately as a `press_release` artifact with
`artifact_metadata.distribution_channel = 'sec_exhibit'`.

The current extraction target for `press_release` artifacts is deterministic
unitization, not AI classification:

- normalize the exhibit body
- skip SEC exhibit wrapper boilerplate such as `EX-99.1`, `Document`,
  filenames, address blocks, and generic `News Release` labels
- extract the first plausible earnings-release headline
- remove repeated press-release page furniture such as `Company/Page N` and the
  following repeated company masthead line
- split the remaining release on recurring standalone labels, including
  `news_summary`, `financial_results`, `business_unit_summary`,
  `business_outlook`, `earnings_webcast`, `forward_looking_statements`,
  `about_company`, `financial_tables`, `non_gaap_measures`, and
  `non_gaap_reconciliations`
- if no clean headline exists, store the whole document as `full_release_body`
- chunk every text unit for FTS retrieval

Tables:

- `artifact_text_units`
- `artifact_text_chunks`

These tables are generic by design. They are the future home for transcripts,
news, and other non-10-K/Q text artifacts that need retrieval chunks without
polluting the canonical SEC section schema.

The unit labels are intentionally few and based on recurring headings, not
company-specific layouts. Add new labels only after they recur across enough
real releases to be deterministic.

Success criteria:

- every retained earnings EX-99 press release has at least one text unit
- every text unit has at least one chunk
- chunks are FTS-searchable
- audit output shows coverage, unit inventory, and retrieval evidence

Each `artifact_section_chunks` row stores:

- `section_id`
- `chunk_ordinal`
- `text`
- `search_text`
- `heading_path`
- `start_offset`
- `end_offset`
- `chunker_version`

`heading_path TEXT[]` preserves sub-structure within a section, e.g.:

- `['Item 1A. Risk Factors', 'Regulatory Risks', 'Data Privacy']`

There is no separate subsection table.

Uniqueness rule:

- one chunk row per `(section_id, chunk_ordinal)`

## `text` vs `search_text`

- `text` is faithful chunk prose.
- `search_text` is normalized FTS input derived from `text`.

`search_text` normalization:

- HTML entity decode
- unicode NFKC normalize
- lowercase
- whitespace collapse

No stemming and no stopword removal in `search_text`. That belongs to the FTS configuration.

## Amendment Model

Amendments are additive, not replacements.

Rules:

- `10-K/A` and `10-Q/A` are separate filing artifacts
- original filing sections remain intact
- amendment sections are extracted separately
- amendment artifacts point back with `amends_artifact_id`
- amendment sections connect to the same narrative identity through:
  - `company_id`
  - `fiscal_period_key`
  - `form_family`
  - `section_key`

So:

- the base `item_7_mda` remains
- an amended `item_7_mda` is an additional linked section
- untouched original sections remain the base evidence

Nothing is overwritten.

## Presentation And Retrieval Semantics

Three supported views:

1. Base filing view
   - original filing sections only

2. Amendment view
   - amendment artifact sections only

3. Composed period view
   - base filing section
   - zero or more amendment-linked sections for the same `(company_id, fiscal_period_key, form_family, section_key)`

Composed view behavior in v1:

- present base section plus amendment-linked sections in `published_at` order
- no automatic merge or diff in v1
- merge/diff is a later enhancement

## FTS Surfaces

Two FTS surfaces:

1. Section-level FTS on `artifact_sections`
   - recall-oriented lookup
   - answers "which filings mention X"

2. Chunk-level FTS on `artifact_section_chunks`
   - ranked passage retrieval
   - feeds model context packets and citation-ready snippets

Both are derived and regeneratable.

## Retrieval Principles

The qualitative layer uses sections, chunks, and evidence packets for different
jobs:

- `artifact_sections` are the canonical narrative truth. A section is the
  durable unit a human analyst recognizes.
- `artifact_section_chunks` are search and citation indexes. They help locate
  candidate evidence under context-window scarcity; they are not source truth.
- Evidence packets are the product assembled for an analyst agent. They combine
  selected filing evidence with financial facts, transcripts, news, and other
  dated context.

Operational rule:

- retrieve small
- read bigger
- cite precisely

In practice, chunk-level FTS finds candidate passages, then the packet builder
may expand context before handing evidence to a model.

Supported expansion modes:

1. Neighbor expansion
   - Include adjacent chunks from the same section.
   - Use when the matched chunk is short and the answer may straddle a chunk
     boundary.
   - Do not cross `heading_path` boundaries by default; bridging unrelated
     Risk Factors is noise.

2. Subsection expansion
   - Include chunks sharing the same `heading_path` prefix.
   - Use when the match is inside a thematic cluster, such as a risk category
     or MD&A subheading.

3. Parent-section expansion
   - Include the full `artifact_sections.text` value.
   - Use when token budget allows and the question is synthesis-oriented, such
     as an analyst memo, year-over-year comparison, or broad business review.
   - Avoid by default for lookup-style questions where a narrower passage is
     sufficient.

Always carry cheap context with each result:

- `heading_path`
- parent artifact metadata
- `section_key`
- `extraction_method`
- `confidence`

The audit report is the quality gate for this behavior. It should explain why a
retrieval result matched before the system adds more complex chunking,
deterministic tags, or LLM-derived tags.

## Large Text Discipline

Section text can be very large.

Rules:

- do not `SELECT *` from `artifact_sections` in listing queries
- provide metadata-only listing queries or a metadata-only view
- full section text is fetched by `section_id`, not bulk scanned

## Regeneration Rules

Derived layers are single-version active only.

Rules:

- bumping `extractor_version` re-extracts affected `artifact_sections` from raw artifacts, then re-chunks them
- bumping `chunker_version` re-chunks existing sections only
- no parallel-version coexistence
- no historical chunk retention across version bumps
- regeneration always reads from raw artifacts on disk, never from prior derived rows

## Scope For This Phase

In scope:

- `10-K`
- `10-Q`
- amendment-linked composition
- canonical sections
- standardized chunks
- section-level and chunk-level FTS

Out of scope:

- transcripts
- exhibits
- `8-K` body extraction
- table parsing inside narrative sections

Tables inside narrative sections are preserved as text only.

## Why This Fits Arrow

This design preserves:

- immutable source truth in `artifacts`
- regeneratable structure in sections and chunks
- search-first retrieval via SQL, metadata filters, and FTS
- PIT-aware company-period composition
- future post-training value once analyst traces are captured in `qa_log`

It supports the larger goal of giving a frontier model filing evidence, amendment overlays, financial facts, news, prices, and options in one aligned evidence packet.

## Implementation Coupling

The migration that introduces this must ship in the same commit as:

- updated v1 Tables status in [docs/architecture/system.md](system.md)
- a reference doc for section keys and extraction rules under `docs/reference/`
- regenerated [arrow_db_schema.html](../../arrow_db_schema.html) via `uv run scripts/gen_schema_viz.py`

No migration merges without those three.
