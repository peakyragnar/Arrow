-- v_adjusted_ic_q: quarter-end Adjusted Invested Capital per formulas.md § 1.
--
-- Adjusted Invested Capital =
--     Total Stockholders' Equity
--   + Short-Term Debt                (FMP: current_portion_lt_debt)
--   + Long-Term Debt                 (FMP: long_term_debt)
--   + Current Operating Lease Liab   (FMP: current_portion_leases_operating)
--   + Non-Current Op Lease Liab      (FMP: long_term_leases_operating)
--   − Cash and Cash Equivalents
--   − Short-Term Investments
--   + R&D Asset                      (v_rd_derived.rd_asset_q)
--
-- Operating-lease inclusion: formulas.md § Common implementation rules
-- says "include only if not already included in debt." Per user decision
-- to trust FMP's bucketing, we simply add FMP's operating-lease buckets
-- without a detection heuristic. If FMP bundled operating-lease-current
-- into shortTermDebt for a filer, we'd double-count by $X, but the
-- user policy is to accept FMP structure as given.

CREATE OR REPLACE VIEW v_adjusted_ic_q AS
WITH s AS (
    SELECT
        company_id,
        period_end,
        total_equity,
        current_portion_lt_debt,
        long_term_debt,
        current_portion_leases_operating,
        long_term_leases_operating,
        cash_and_equivalents,
        short_term_investments
    FROM v_company_period_wide
    WHERE period_type = 'quarter'
)
SELECT
    s.company_id,
    s.period_end,
    -- Reported (non-R&D-adjusted) invested capital — useful for debugging
    (
        COALESCE(s.total_equity, 0)
      + COALESCE(s.current_portion_lt_debt, 0)
      + COALESCE(s.long_term_debt, 0)
      + COALESCE(s.current_portion_leases_operating, 0)
      + COALESCE(s.long_term_leases_operating, 0)
      - COALESCE(s.cash_and_equivalents, 0)
      - COALESCE(s.short_term_investments, 0)
    ) AS reported_ic_q,
    -- Adjusted invested capital = reported IC + R&D Asset
    (
        COALESCE(s.total_equity, 0)
      + COALESCE(s.current_portion_lt_debt, 0)
      + COALESCE(s.long_term_debt, 0)
      + COALESCE(s.current_portion_leases_operating, 0)
      + COALESCE(s.long_term_leases_operating, 0)
      - COALESCE(s.cash_and_equivalents, 0)
      - COALESCE(s.short_term_investments, 0)
      + COALESCE(r.rd_asset_q, 0)
    ) AS adjusted_ic_q,
    -- Propagate R&D asset and coverage for consumers
    r.rd_asset_q,
    r.rd_coverage_quarters,
    -- Debug fields: the raw components
    s.total_equity,
    s.current_portion_lt_debt,
    s.long_term_debt,
    s.current_portion_leases_operating,
    s.long_term_leases_operating,
    s.cash_and_equivalents,
    s.short_term_investments
FROM s
LEFT JOIN v_rd_derived r
    ON r.company_id = s.company_id AND r.period_end = s.period_end;

COMMENT ON VIEW v_adjusted_ic_q IS
    'Quarter-end Adjusted Invested Capital per formulas.md § 1. Uses FMP operating-lease buckets as-is per user policy (trust FMP structure).';
