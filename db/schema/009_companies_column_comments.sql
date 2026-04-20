-- 009_companies_column_comments.sql
--
-- Corrects a misleading example in the 007 migration comment. Migration
-- 007 documented `NVDA="01-26"` as the fiscal_year_end_md example. That
-- value is a specific historical period_end (NVDA FY2025 Q4 ended
-- 2025-01-26), not SEC's fiscal-year-end anchor. The periods.md § 3.2
-- derivation algorithm relies on `fiscal_year_end_md` being the upper
-- bound of where any actual period_end can fall; storing a specific year's
-- period_end misclassifies fiscal_year for years when period_end drifts
-- later (e.g. NVDA FY2024 ended 2024-01-28).
--
-- Migration 007 is frozen (checksum-guarded by the runner). This migration
-- records the correction as a column COMMENT so psql \d+ and schema viz
-- tools surface the right semantics.

COMMENT ON COLUMN companies.fiscal_year_end_md IS
    'MM-DD format. Stores SEC submissions.fiscalYearEnd as the nominal fiscal-year-end anchor — the upper bound of where any actual period_end can fall. Used by the derivation algorithm in docs/reference/periods.md §§ 2.3, 3.2. Examples: NVDA=01-31 (52/53-week, anchored end of January), AAPL=09-30 (last Saturday of September), MSFT=06-30, calendar-year filers=12-31. Do NOT store a specific year''s period_end — that breaks year-before-check comparisons for years when period_end drifts past the chosen date.';
