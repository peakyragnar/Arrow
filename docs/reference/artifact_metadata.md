# `artifact_metadata` key conventions

`artifacts.artifact_metadata` is a `jsonb` column that holds type-specific fields that don't warrant their own column on the table. The hybrid shape (common columns + metadata jsonb) is chosen in [ADR-0007](../decisions/0007-artifact-hybrid-metadata.md); this doc is the guardrail that keeps metadata keys from drifting into inconsistency.

## Why conventions matter

`jsonb` is flexible. Flexibility without discipline produces keys like `accession`, `accessionNumber`, `acc_no`, `sec_accession` — all meaning the same thing, scattered across rows, impossible to query consistently. This doc exists to prevent that.

**Rule:** every key that appears in `artifact_metadata` is either (a) listed here with its type and meaning, or (b) being added in the same commit that introduces it, by editing this doc.

## Key-name rules

1. **snake_case only.** `accession_number`, not `accessionNumber` or `AccessionNo`.
2. **Flat where possible.** Prefer `filer_cik` over `filer.cik`. Nested keys are allowed when the data is genuinely hierarchical (e.g. `speakers` = array of `{role, name, title}`), not for grouping top-level facts.
3. **No type prefixes.** Don't put `10k_filing_date` — the artifact's `artifact_type` is the prefix. Just `filing_date`.
4. **Match column conventions in periods.md** when the value is period-related. If a field would duplicate a column, use the column, not metadata.
5. **No nulls in metadata.** If a key doesn't apply, omit it. Don't store `{"foo": null}`. Absence is the only signal of "doesn't apply."

## What does NOT go in `artifact_metadata`

These have dedicated columns and must not be duplicated in metadata:

- Anything in [periods.md](./periods.md) — `fiscal_year`, `fiscal_quarter`, `period_end`, `calendar_*`, all labels
- SEC filing identity on `artifacts` — `company_id`, `fiscal_period_key`, `form_family`, `cik`, `accession_number`, `raw_primary_doc_path`, `amends_artifact_id`
- Hashes — `raw_hash`, `canonical_hash`
- Provenance — `source`, `source_document_id`, `ticker`, `url`, `content_type`, `language`
- Time-awareness — `published_at`, `effective_at`, `ingested_at`
- Lineage — `supersedes`, `superseded_at`
- Research freshness — `authored_by`, `last_reviewed_at`, `asserted_valid_through`

**Check first:** if a new field would apply to ≥ 3 artifact types, it's probably a column, not metadata.

## Per-type conventions

These are the starting conventions. Add a row when you introduce a new type; extend an existing row when you find you need a new key for an existing type.

### `10k`, `10q`, `8k` (SEC filings)

| Key | Type | Notes |
|---|---|---|
| `form_type` | string | Literal form, e.g. `"10-K/A"` for amendments |
| `amended` | bool | Present and `true` on amendments (`10-K/A`, `10-Q/A`) |
| `filing_date` | string (ISO date) | Form filing date from SEC header; may differ from `published_at` if we received it late |
| `primary_document` | string | SEC primary document filename, e.g. `"nvda-20251026x10q.htm"` |
| `reporting_period_end` | string (ISO date) | Redundant with `period_end` column — do NOT store here |
| `items` | array of strings | 8-K Item codes, e.g. `["2.02", "9.01"]` |
| `xbrl_available` | bool | Whether XBRL instance was present |
| `is_inline_xbrl` | bool | Whether Inline XBRL was present |

### `transcript`

| Key | Type | Notes |
|---|---|---|
| `call_type` | string | `"earnings"` / `"guidance_update"` / `"investor_day"` / `"conference"` |
| `speakers` | array | `[{role: "ceo" | "cfo" | "analyst" | "operator" | "other", name: string, title: string, affiliation?: string}]` |
| `fmp_transcript_id` | string | FMP's own transcript id |
| `runtime_minutes` | integer | Call duration if known |

### `press_release`

| Key | Type | Notes |
|---|---|---|
| `headline` | string | If different from `title` |
| `distribution_channel` | string | `"pr_newswire"` / `"business_wire"` / `"company_ir"` / ... |
| `accession_number` | string | SEC accession when release came via an 8-K exhibit |
| `filer_cik` | string | CIK when release came via an SEC filing |
| `form_type` | string | Usually `"8-K"` / `"8-K/A"` when sourced from SEC |
| `filing_date` | string (ISO date) | Parent filing date when sourced from SEC |
| `document_name` | string | Exhibit filename, e.g. `"ex99-1.htm"` |
| `document_type` | string | SEC exhibit type, e.g. `"EX-99.1"` |
| `tags` | array of strings | Company-supplied topic tags |

### `news_article`

| Key | Type | Notes |
|---|---|---|
| `publisher` | string | `"Reuters"`, `"WSJ"`, etc. |
| `byline` | string | Author(s) |
| `canonical_url` | string | Publisher's canonical URL if different from `url` |

### `presentation`

| Key | Type | Notes |
|---|---|---|
| `slide_count` | integer | |
| `event` | string | `"investor_day_2025"`, `"jpm_healthcare_conference"`, etc. |

### `video_transcript`

| Key | Type | Notes |
|---|---|---|
| `video_source` | string | `"youtube"` / `"company_ir"` / ... |
| `video_id` | string | Source-specific id |
| `runtime_seconds` | integer | |

### `research_note`, `industry_primer`, `product_primer`, `macro_primer`

| Key | Type | Notes |
|---|---|---|
| `topic_tags` | array of strings | Free-form topic labels |
| `primary_entity` | string | Ticker or macro series this primer most directly covers |
| `related_entities` | array of strings | Secondary tickers / series referenced |
| `confidence_level` | string | `"established"` / `"provisional"` / `"speculative"` — primer author's judgment |

Note: `authored_by`, `last_reviewed_at`, `asserted_valid_through` are columns, not metadata.

### `macro_release`

| Key | Type | Notes |
|---|---|---|
| `series_id` | string | e.g. `"FRED:DFF"`, `"BLS:CPI-U"` — the macro series this release covers |
| `release_name` | string | `"FOMC Statement"`, `"CPI Release"`, etc. |
| `release_type` | string | `"scheduled"` / `"emergency"` / `"minutes"` / `"revision"` |
| `vintage` | string | Release vintage per macro model (e.g. `"advance"`, `"second"`) |

## Adding a new artifact type

1. Add the type literal to the `artifacts_type_check` CHECK constraint via a new migration in `db/schema/`.
2. Add the type to `ArtifactType` in `src/arrow/models/artifact.py`.
3. Add a section to this doc with the expected metadata keys for the new type.
4. Commit all three changes together.

## Adding a new key to an existing type

1. Add a row to the type's table in this doc.
2. Commit the doc change alongside the ingest code that starts writing the key.
3. If the key becomes near-universal (≥ 3 types), propose promoting it to a column via a new migration + ADR.

## Drift-detection (future)

A lightweight job that runs over `artifacts` and reports any key present in `artifact_metadata` but not listed in this doc. Not built yet; ticket it when we have enough types to benefit from it.
