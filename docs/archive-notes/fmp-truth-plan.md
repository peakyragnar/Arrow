# FMP Truth Plan

> **Status: implemented. Archived for historical context.**
>
> This is the plan that drove the FMP-baseline-truth pivot. The decision itself is encoded in [ADR-0010](../decisions/0010-fmp-baseline-truth-sec-documents-audit-side-rail.md); the architectural consequences are in [docs/architecture/system.md](../architecture/system.md) and [docs/architecture/normal_vs_audit.md](../architecture/normal_vs_audit.md). This document is retained as the longer narrative that preceded the ADR.
>
> Do not treat the imperative-present phrasing below as live work. All eight workstreams have landed (migrations 008–012, `backfill_fmp_statements`, `scripts/reconcile_fmp_vs_xbrl.py`, ADR-0010, updated architecture docs).

## Goal

Keep the existing Arrow database structure.

Change only this rule:

- `financial_facts` historical source of truth = **FMP**
- SEC remains an ingest source for **documents, freshness, and later audit**
- audit functionality stays in repo, but **off the main financial ingest path**

## What stays unchanged

- current DB structure
- current IDs / keys / linking model
- current provenance model
- current PIT/time-aware model
- `companies`
- `ingest_runs`
- `raw_responses`
- `artifacts`
- `financial_facts`
- `data_quality_flags`

No redesign there.

## What changes

### 1. Financial facts policy

Historical financial facts:

- ingest from FMP
- normalize into `financial_facts`
- preserve PIT/vendor revision history
- do **not** adjudicate against SEC inline
- do **not** mutate baseline facts through audit/manual review

Meaning:

- if FMP changes a number later, Arrow records a new version
- if SEC disagrees, that becomes an audit finding, not a baseline overwrite

### 2. SEC policy

SEC stays active, but for different jobs:

- `8-K` earnings releases before the `10-Q` / `10-K`
- raw `10-Q` / `10-K` documents
- filing text extraction
- commentary / risk-factor / MD&A extraction
- provenance
- freshness
- later optional audit

Meaning:

- we still download/store SEC raw
- we stop using SEC as the normal historical financial fact arbiter

### 3. Transcript policy

- FMP remains transcript source
- no change

### 4. Audit policy

Audit stays in repo as a separate feature:

- compare FMP vs SEC/XBRL
- write `data_quality_flags`
- preserve benchmark tooling
- preserve amendment detection/reconciliation logic if wanted later

But:

- audit never blocks default ingest
- audit never rewrites baseline `financial_facts`
- audit is side functionality, not architecture driver

### 5. Manual correction policy

- no manual supersession in core baseline facts
- no partial edits in `financial_facts`
- if a wrong number is found:
  - create flag
  - review it separately
  - keep baseline facts intact

## Active ingest model

### Lane A: mainline ingest

This is the product path.

1. company seeded
2. FMP financial endpoints fetched
3. FMP payloads stored in `raw_responses`
4. FMP financials normalized into `financial_facts`
5. SEC `8-K` / `10-Q` / `10-K` raw filings fetched and stored as `artifacts`
6. FMP transcripts fetched and stored as `artifacts`
7. later: news ingested
8. model/retrieval uses these together

### Lane B: audit

Separate, optional.

1. compare FMP baseline vs SEC/XBRL
2. detect mismatches / amendment candidates
3. write flags
4. support benchmarks / review
5. never change Lane A facts directly

## Detailed workstreams

### Workstream 1: write the decision down

Update docs first.

Need:

- `AGENTS.md`
- short ADR in `docs/decisions/`

Decision text:

- Arrow historical financial facts are FMP-first
- SEC is retained for document ingestion, freshness, and optional audit
- audit does not block ingest
- audit does not rewrite baseline facts

### Workstream 2: simplify live financial ingest

Review mainline financial ingest code and remove/demote anything that assumes:

- SEC decides final historical facts
- amendment resolution is part of normal ingest
- audit findings can mutate baseline facts

Goal:

- make FMP backfill from zero boring and repeatable

### Workstream 3: keep SEC document ingest active

Do **not** remove SEC ingest.

Make its active role explicit:

- store `8-K` earnings releases
- store `10-Q` / `10-K` raw filings
- support text extraction and freshness
- no inline adjudication of baseline financial facts

### Workstream 4: keep audit code, but demote it

Audit code should be one of:

- callable optional tool/script
- frozen reference path
- later feature path

Not:

- default ingest machinery

Need to classify audit components:

- keep active as optional
- keep as frozen reference
- mark superseded where needed in docs

### Workstream 5: preserve raw cache discipline

No architecture change, but important operational rule:

- never overwrite raw source payloads
- every fetch preserved
- FMP revisions preserved as new raw payloads / new fact versions
- SEC docs preserved as artifacts

This matters because baseline trust depends on provenance even without adjudication.

### Workstream 6: retarget verification

Verification should change purpose.

Before:

- try to prove or repair truth inline

Now:

- check baseline quality where useful
- produce audit findings
- support benchmarks
- not own the mainline

So:

- keep `data_quality_flags`
- keep audit logic if useful
- do not make financial ingest depend on passing deep audit

### Workstream 7: define wrong-number protocol

When something wrong surfaces:

1. log/create flag
2. inspect whether it is:
   - one-off baseline issue
   - vendor-wide pattern
   - acceptable noise
3. do not patch baseline facts in place
4. decide later if override/audit layer is worth adding

### Workstream 8: keep product priorities intact

After FMP baseline ingest is stable, continue the real product path:

- transcripts
- `8-K` earnings releases
- `10-Q` / `10-K` text
- risk factors
- MD&A
- news
- synthesis prompt

That stays the goal.

## Concrete repo changes implied

### Docs

- update `AGENTS.md`
- add ADR
- update `docs/architecture/system.md`
- mark audit as side rail, not mainline
- mark archives as reference only

### Code

- simplify FMP ingest path
- strip audit/adjudication from critical path
- keep SEC artifact ingest
- keep transcript ingest
- preserve audit modules, but no longer required for successful baseline ingest

### Tests

Need two test categories:

**Baseline ingest tests**

- seed company
- backfill FMP
- facts land
- no manual intervention
- succeeds from zero

**Audit tests**

- optional reconciliation still runs when called
- flags still generate
- benchmark fixtures still usable

Most important:

- baseline ingest must pass without audit being required

## Final operating rule

### Baseline truth

- for historical financial facts, Arrow trusts FMP as baseline

### SEC role

- SEC provides filing artifacts, freshness, and text

### Audit role

- audit is retained, but separate

### Core facts rule

- baseline facts are not hand-edited

## Short version

This is **not** a database redesign.

It is a source-policy change:

- keep the existing Arrow structure
- make FMP the historical fact source of truth
- keep SEC for `8-K`/`10-Q`/`10-K` document ingest
- keep audit as a side feature
- stop letting audit complicate baseline financial ingest
