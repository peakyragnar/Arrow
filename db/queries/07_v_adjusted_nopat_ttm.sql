-- v_adjusted_nopat_ttm: Adjusted NOPAT per formulas.md § 1 (ROIC).
--
-- Adjusted NOPAT = (Operating Income + R&D Expense − R&D Amortization) × (1 − Tax Rate)
--
-- All flows TTM. R&D Amortization is TTM of the quarterly amortization
-- from v_rd_derived (itself a 20-quarter rolling construct).
-- R&D partial-history underscoring applies transparently via v_rd_derived.

CREATE OR REPLACE VIEW v_adjusted_nopat_ttm AS
WITH rd_amort_ttm AS (
    SELECT
        company_id,
        period_end,
        SUM(rd_amortization_q) OVER w AS rd_amortization_ttm,
        MIN(rd_coverage_quarters) OVER w AS min_rd_coverage_in_ttm,
        COUNT(*) OVER w AS quarters_in_window
    FROM v_rd_derived
    WINDOW w AS (PARTITION BY company_id ORDER BY period_end
                 ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
SELECT
    f.company_id,
    f.period_end,
    f.operating_income_ttm,
    f.rd_ttm,
    r.rd_amortization_ttm,
    t.tax_rate_ttm,
    -- Adjusted OI TTM = OI TTM + R&D TTM − R&D Amort TTM
    (f.operating_income_ttm + f.rd_ttm - r.rd_amortization_ttm) AS adjusted_oi_ttm,
    -- Adjusted NOPAT TTM = Adjusted OI × (1 − tax rate)
    CASE
        WHEN f.quarters_in_window < 4 THEN NULL
        WHEN t.tax_rate_ttm IS NULL THEN NULL
        WHEN f.operating_income_ttm IS NULL OR f.rd_ttm IS NULL OR r.rd_amortization_ttm IS NULL THEN NULL
        ELSE (f.operating_income_ttm + f.rd_ttm - r.rd_amortization_ttm) * (1 - t.tax_rate_ttm)
    END AS adjusted_nopat_ttm,
    -- Pass through the R&D coverage floor so downstream ROIC can surface it
    r.min_rd_coverage_in_ttm AS rd_coverage_quarters
FROM v_ttm_flows f
LEFT JOIN rd_amort_ttm r
    ON r.company_id = f.company_id AND r.period_end = f.period_end
LEFT JOIN v_tax_rate_ttm t
    ON t.company_id = f.company_id AND t.period_end = f.period_end;

COMMENT ON VIEW v_adjusted_nopat_ttm IS
    'Adjusted NOPAT (TTM) per formulas.md § 1. Carries rd_coverage_quarters from v_rd_derived so consumers can de-weight partial-history periods.';
