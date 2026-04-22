-- v_metrics_roic: ROIC and ROIIC per formulas.md § 1, § 2.
--
--   1. ROIC (Adjusted)
--      = Adjusted NOPAT TTM / Average Adjusted Invested Capital
--      Average = (Beginning IC + Ending IC) / 2, both quarter-end stocks.
--
--   2. ROIIC (Incremental)
--      = ΔAdjusted NOPAT TTM / ΔAdjusted Invested Capital
--      Δ is current vs prior-year same quarter.

CREATE OR REPLACE VIEW v_metrics_roic AS
WITH ic_series AS (
    SELECT
        ic.company_id,
        ic.period_end,
        ic.adjusted_ic_q,
        LAG(ic.adjusted_ic_q, 1) OVER w AS ic_prior_q,
        LAG(ic.adjusted_ic_q, 4) OVER w AS ic_prior_year,
        ic.rd_coverage_quarters
    FROM v_adjusted_ic_q ic
    WINDOW w AS (PARTITION BY ic.company_id ORDER BY ic.period_end)
),
nopat_series AS (
    SELECT
        n.company_id,
        n.period_end,
        n.adjusted_nopat_ttm,
        LAG(n.adjusted_nopat_ttm, 4) OVER w AS adjusted_nopat_ttm_prior_year
    FROM v_adjusted_nopat_ttm n
    WINDOW w AS (PARTITION BY n.company_id ORDER BY n.period_end)
),
joined AS (
    SELECT
        c.ticker,
        ic.company_id,
        ic.period_end,
        ic.adjusted_ic_q,
        ic.ic_prior_q,
        ic.ic_prior_year,
        ic.rd_coverage_quarters,
        n.adjusted_nopat_ttm,
        n.adjusted_nopat_ttm_prior_year
    FROM ic_series ic
    JOIN nopat_series n
        ON n.company_id = ic.company_id AND n.period_end = ic.period_end
    JOIN companies c ON c.id = ic.company_id
)
SELECT
    ticker,
    company_id,
    period_end,

    -- ===== 1 Adjusted ROIC =====
    -- Denominator: average of (IC_t + IC_{t-1}) / 2. Null-safe: if
    -- IC_{t-1} is missing, suppress (first period has no average).
    CASE
        WHEN adjusted_nopat_ttm IS NULL THEN NULL
        WHEN adjusted_ic_q IS NULL OR ic_prior_q IS NULL THEN NULL
        WHEN ((adjusted_ic_q + ic_prior_q) / 2.0) <= 0 THEN NULL
        ELSE adjusted_nopat_ttm / ((adjusted_ic_q + ic_prior_q) / 2.0)
    END AS roic,

    -- ===== 2 ROIIC =====
    -- Δ numerator: YoY TTM NOPAT delta.
    -- Δ denominator: IC_t − IC_{t-4}. Note formulas.md says "do not TTM
    -- a stock measure; invested capital remains a quarter-end value."
    -- Suppress when denominator near zero to avoid mathematically-valid
    -- but economically-meaningless ratios.
    CASE
        WHEN adjusted_nopat_ttm IS NULL OR adjusted_nopat_ttm_prior_year IS NULL THEN NULL
        WHEN adjusted_ic_q IS NULL OR ic_prior_year IS NULL THEN NULL
        WHEN ABS(adjusted_ic_q - ic_prior_year)
             < GREATEST(1000000, ABS(adjusted_nopat_ttm) * 0.001) THEN NULL
        ELSE (adjusted_nopat_ttm - adjusted_nopat_ttm_prior_year)
           / (adjusted_ic_q - ic_prior_year)
    END AS roiic,

    -- Lineage / partial-history signal
    adjusted_nopat_ttm,
    adjusted_nopat_ttm_prior_year,
    adjusted_ic_q,
    ic_prior_q,
    ic_prior_year,
    rd_coverage_quarters
FROM joined;

COMMENT ON VIEW v_metrics_roic IS
    'Adjusted ROIC + ROIIC per formulas.md § 1, § 2. Carries rd_coverage_quarters so consumers can de-weight periods with <20 R&D history.';
