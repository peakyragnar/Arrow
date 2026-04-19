-- Postgres extensions required by Arrow.
--
-- Declared here (not as one-shot psql commands) so a fresh rebuild from
-- db/schema/*.sql reproduces the full runtime, including extensions.
--
-- pg_trgm: trigram-based fuzzy text search. Used later for company/ticker
-- lookup. Shipped with Postgres contrib; available on every major managed
-- Postgres (RDS, Supabase, Neon, Cloud SQL) and on self-managed installs.

CREATE EXTENSION IF NOT EXISTS pg_trgm;
