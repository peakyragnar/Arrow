# ADR-0001: Hetzner Cloud as the cloud-later target
Status: accepted
Date: 2026-04-19

## Context

`docs/architecture/system.md` § Local First, Cloud Later commits Arrow to eventual cloud deployment but leaves the specific provider open. Arrow's workload shape is:

- Single operator; budget-constrained ($25–$75/mo target, not $500)
- Batch ingest jobs hitting FMP/SEC that run for minutes, not seconds (disqualifies serverless function hosts)
- Postgres as system of record (tens of GB growth in year 1)
- Raw cache of JSON payloads that will grow to hundreds of GB (the dominant storage cost)
- Low QPS — tens of analyst queries per day, not a scale story
- No customer-facing surface; downtime is inconvenient, not catastrophic
- Data loss is largely recoverable (raw is re-fetchable from FMP; facts are derived)

## Decision

**Hetzner Cloud VM + self-managed Postgres 16 + Hetzner Storage Box (or Cloudflare R2) for the raw payload cache.**

Managed alternatives (Render, Neon) remain viable fallbacks; migration between them is `pg_dump | restore` + a `DATABASE_URL` swap.

## Consequences

**Positive**
- Cost ~10× cheaper than hyperscaler-managed equivalents (~$10–35/mo all-in vs $80–150/mo on AWS/GCP)
- Local Homebrew pg 16 is the same Postgres binary — migration is nearly mechanical
- No vendor lock-in; vanilla Postgres everywhere
- Full control over extensions, tuning, version pinning
- Deployment fits the Claude-authored IaC model: playbooks and scripts Claude writes, operator runs

**Negative**
- We are the DBA. Backups, upgrades, security patches, disk monitoring all on us
- No automated HA (Arrow doesn't need it at this scale)
- No point-in-time recovery out of the box — requires a `pg_dump` + WAL archival cron targeting object storage
- Initial setup has more moving parts than signing up for Render

## Alternatives considered

- **Render** (~$50/mo managed pg + first-class workers + cron) — solid, the likely fallback. 5× the cost with no unique advantage for this workload. Worth it if the DBA burden ever becomes real friction.
- **Neon** (best-in-class pg with branching and scale-to-zero) — excellent *DB* layer, but needs a separate worker host for batch ingest. Two bills, more coordination.
- **Supabase** — bundles auth/realtime/RLS we don't need; Edge Functions' 150s timeout is wrong for our long batch jobs.
- **Fly.io** — viable; historical managed-pg stability was the question mark.
- **GCP Cloud SQL / AWS RDS** — overkill. Cost floor higher, ops surface larger, egress fees bite. Defensible only if we already knew those ecosystems deeply.
- **Vercel** — disqualified. Serverless function timeouts (60–300s) make batch ingest infeasible.

## When to revisit

- The ops burden (backups, upgrades, disk full incidents) eats more than ~1 hour per month of wall time
- Data volume or write load exceeds what a single Hetzner VM handles comfortably (likely years away)
- A team joins — managed-pg value goes up when multiple operators share on-call
- A regulatory requirement mandates PITR / HA / audited backups that a managed service provides out of the box
