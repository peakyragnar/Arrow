# Normal Flow Vs Audit Flow

This is the shortest statement of how Arrow works now.

## Normal Flow

This is the default path. This is what a new contributor should assume.

### Financial facts

- source of truth: FMP
- destination: `financial_facts`
- default rule: ingest FMP, store FMP, preserve PIT history
- default Layer 1 behavior:
  - hard: IS subtotal ties, BS balance identity, CF cash roll-forward
  - soft: BS/CF subtotal-component drift writes `data_quality_flags`
  - soft flags do not change stored fact values

### SEC

SEC is still active, but for documents:

- `8-K` earnings releases
- `10-Q`
- `10-K`
- filing text
- freshness / low-latency filing arrival

Destination:

- `raw_responses`
- `artifacts`

### Transcripts

- source: FMP
- destination: `artifacts`

## Audit Flow

Audit is separate from normal ingest.

It can:

- compare FMP vs SEC/XBRL
- detect divergences
- write `data_quality_flags`
- support benchmarks and spot checks

It does **not**:

- block default ingest
- decide what lands in baseline `financial_facts`
- rewrite baseline facts

## Core Rule

If you are working on normal historical financial ingest:

- use FMP
- do not pull SEC into the financial fact path

If you are working on filings/documents:

- use SEC artifacts

If you are working on reconciliation:

- treat it as audit, not baseline ingest
