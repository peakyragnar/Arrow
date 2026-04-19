# AGENTS.md

Arrow is being rebuilt around an FMP-first, PostgreSQL-backed architecture.

This file is the repo map, not the encyclopedia.
Start here. Read only the next doc you need.

## Current Source Of Truth

- Current architecture: `docs/architecture/system.md`
- Repository flow / folder map: `docs/architecture/repository_flow.md`
- Period spec (fiscal ↔ calendar rules): `docs/reference/periods.md`
- Artifact metadata key conventions: `docs/reference/artifact_metadata.md`
- Architecture decisions (tool + pattern choices): `docs/decisions/`
- Setup runbook: `docs/setup.md`
- Older strategy snapshot: `docs/strategy/plan.md`
- Metric definitions: `docs/reference/formulas.md`
- R&D capitalization reference: `docs/reference/rd_capitalization_reference.md`
- Benchmark workbook: `docs/benchmarks/golden_eval.xlsx`

## Read Order

1. `AGENTS.md`
2. `docs/architecture/system.md`
3. task-specific doc in `docs/`
4. live code in `src/` / `db/` / `scripts/`
5. `archive/` only if you need legacy reference

## Live Layout

```text
docs/        current architecture, strategy, references, benchmarks
db/          schema, queries, seeds
src/arrow/   live implementation
scripts/     thin entrypoints / utilities
data/raw/    raw source caches by vendor/source
tests/       test suites
archive/     old systems; reference only
```

## Archive Fence

These are archived, not active implementation guidance:

- `archive/ai_extract/`
- `archive/deterministic-flow/`
- `archive/legacy-dashboard/`
- `archive/legacy-root/fetch.py`
- `archive/legacy-root/calculate.py`

Important:
- `archive/ai_extract/` is the last full design iteration before the FMP pivot
- it remains valuable for benchmark context and legacy extraction logic
- it is not the current system design

## Working Rules

- System of record: PostgreSQL
- Primary historical source: FMP
- Low-latency fresh filing path: SEC direct
- Retrieval: search-first, SQL + FTS, not naive RAG
- Preserve fiscal truth and calendar normalization
- Preserve point-in-time semantics where revisions/restatements exist
- Raw responses cached
- Artifacts immutable
- Facts derived and regeneratable

## Data Rules

- Keep raw SEC filings under `data/raw/sec/filings/{CIK}/{ACCESSION}/`
- Keep vendor/source caches under `data/raw/{vendor}/`
- Raw filesystem cache layout mirrors the vendor's endpoint path (endpoint-mirrored, not category-mirrored)
- FMP pattern: `data/raw/fmp/{endpoint-path}/{TICKER}/{key}.json` where `{key}` deterministically encodes request params (`annual`, `quarter`, year slice, `YYYY-Qn`, etc.)
- See `docs/architecture/system.md` § Raw Cache Layout for the full rule and examples
- Do not treat generated JSON/CSV artifacts as policy truth
- Use benchmark/reference docs in `docs/reference/` and `docs/benchmarks/`

## Folder Intent

- `docs/architecture/system.md` is the north star
- `docs/strategy/plan.md` is older and superseded for architecture
- `src/arrow/ingest/fmp/` is where live FMP ingest should land
- `src/arrow/ingest/sec/` is where live SEC fast-path ingest should land
- `archive/` is for reading, not for new implementation

## If Unsure

- prefer `docs/architecture/system.md`
- prefer live paths over archived paths
- if a doc in `archive/` conflicts with a doc in `docs/architecture/`, the architecture doc wins
- for "why did we choose X?" questions about tools/patterns, look in `docs/decisions/` — ADRs capture the trade-offs that principles don't settle
