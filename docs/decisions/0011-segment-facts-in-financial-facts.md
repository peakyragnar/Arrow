# ADR-0011: Store Segment Revenue As Dimensioned Financial Facts

Status: accepted
Date: 2026-04-24

## Context

Arrow needs segment/product/geographic revenue to answer driver questions:

- what drove revenue growth
- how revenue mix changed
- whether management commentary matched segment performance
- how peer segment exposure compares over time

The existing `financial_facts` table already carries the important contracts
for numeric observations: company identity, fiscal truth, calendar
normalization, PIT timing, supersession, raw-response provenance, and extraction
versioning.

Segment revenue can either reuse that table or live separately.

## Decision

Store FMP revenue segmentation as `financial_facts` rows with dimension
identity:

```text
statement = 'segment'
concept = 'revenue'
dimension_type = product | geography | operating_segment
dimension_key = normalized company-local key
dimension_label = source/vendor label
dimension_source = source endpoint
```

Non-segment rows keep all dimension fields NULL.

Migration 016 adds the dimension fields and replaces the original
non-dimensional uniqueness rules with dimension-aware unique indexes.

## Consequences

**Positive**

- Segment facts retain the same PIT and provenance contract as IS/BS/CF facts.
- Driver queries can read company revenue and segment revenue from one fact
  substrate.
- `concept = 'revenue'` remains stable instead of exploding into
  `segment_revenue_*` strings.
- Future cross-company segment mapping can be layered on top without rewriting
  source facts.

**Negative**

- `financial_facts` now has nullable dimension fields that are meaningful only
  for `statement = 'segment'`.
- The idempotency rule can no longer be a simple table constraint because
  dimension NULLs need to compare as equal; the rule moves to expression
  unique indexes.
- Loader code must use generic `ON CONFLICT DO NOTHING` rather than targeting
  the old unique constraint by name.

## Alternatives Considered

**Encode segment identity in `concept`.**

Rejected. It requires no schema change, but creates concept explosion, weak
normalization, and string-matching queries.

**Create a sibling `segment_facts` table.**

Rejected for v1. It separates segment data cleanly, but duplicates
fiscal/calendar/PIT/provenance semantics already solved in `financial_facts`
and forces consumers to special-case segments.

## Future Work

Add a canonical segment mapping table once real peer labels are observed:

```text
segment_dimension_map(
  company_id,
  source,
  dimension_type,
  dimension_label,
  dimension_key,
  canonical_dimension_key
)
```

The first ingest keeps `dimension_key` company-local and source faithful.
