# 0010 — FMP baseline truth, SEC documents, audit side rail

- Status: accepted
- Date: 2026-04-22

## Context

Arrow's schema, IDs, provenance model, and PIT model already fit the
long-term product design. The problem was not database shape. The
problem was that historical financial ingest had started mixing:

- FMP baseline loading
- SEC/XBRL reconciliation
- amendment adjudication
- flag lifecycle management

That made the default backfill path harder to run, harder to reason
about, and too easy to treat as a financial truth engine instead of the
baseline product substrate.

Arrow still needs SEC:

- for fresh filing arrival
- for `8-K` earnings releases before the `10-Q` / `10-K`
- for raw filing text (`10-Q`, `10-K`, material `8-K`)
- for qualitative extraction later
- for optional audit/reconciliation later

Arrow also still needs to preserve the existing audit structure for later
use. But that audit path should not shape the baseline facts contract.

## Decision

Arrow historical baseline facts are FMP-first.

- `financial_facts` stores baseline historical facts from FMP.
- Default FMP backfill performs inline Layer-1 load validation only.
- Default FMP backfill does not perform inline SEC/XBRL reconciliation.
- Default FMP backfill does not perform inline amendment adjudication.
- Default FMP backfill does not mutate or resolve `data_quality_flags`.

SEC remains an active ingest source, but for a different job:

- raw filing artifacts
- freshness / low-latency filing path
- `8-K` earnings releases
- later filing-text extraction
- optional audit and spot checks

Audit remains in-repo, but as a side rail:

- callable separately
- allowed to write `data_quality_flags`
- not allowed to block baseline ingest
- not allowed to rewrite baseline `financial_facts`

## Consequences

### Positive

- baseline FMP ingest becomes simpler and more repeatable
- the core DB stays aligned with the original FMP-first design
- SEC raw remains available for document workflows
- audit work is preserved without owning the mainline

### Negative

- historical financial facts are trusted baseline facts, not fully
  adjudicated truth
- wrong-number handling becomes reactive: flag/review first, not
  auto-reconcile first
- audit code now has carrying cost as an optional path instead of an
  always-exercised path

## Follow-ons

- update `AGENTS.md`
- update `docs/architecture/system.md`
- keep default `backfill_fmp.py` baseline-only
- keep SEC document ingest active
- keep audit code callable separately
