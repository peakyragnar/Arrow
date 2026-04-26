-- Drop coverage_membership.tier column.
--
-- V1 introduced two tiers (`core` / `extended`) on coverage_membership
-- with per-tier rule sets in expectations.py. V1.1 (commit c6025f3)
-- collapsed coverage to a single uniform standard so cross-ticker
-- comparisons stay symmetric — different depths in the same coverage
-- universe broke the comparability the universe is supposed to provide.
--
-- After c6025f3 the application stopped attaching meaning to the tier
-- column; a transitional `tier='core'` literal in
-- arrow.steward.actions.add_to_coverage satisfied the still-present
-- NOT NULL CHECK. This migration retires both the column and the
-- need for the literal.
--
-- Dropping the column also drops (cascade):
--   - the CHECK constraint coverage_membership_tier_enum
--   - the index coverage_membership_tier_idx
-- Postgres handles both implicitly because they reference only this column.
--
-- Legitimate exceptions (recent IPOs that can't reach 5y of history,
-- etc) used to live in PER_TICKER_OVERRIDES — also deleted in c6025f3.
-- They now live in suppression notes on the resulting findings, where
-- they become V2 training data instead of silent code-side filters.

ALTER TABLE coverage_membership DROP COLUMN tier;

COMMENT ON TABLE coverage_membership IS
  'Curated coverage universe; binary membership (a ticker is tracked or not). One uniform standard from src/arrow/steward/expectations.py applies to every member. See docs/architecture/steward.md.';
