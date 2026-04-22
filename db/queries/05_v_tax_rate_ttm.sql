-- v_tax_rate_ttm: TTM effective tax rate with 15% fallback.
--
-- Per formulas.md § Tax rate rule:
--   Tax Rate = Income Tax Expense / Income Before Income Taxes
--   Fallback: 15% if pretax income is zero or negative
--
-- Grain: one row per (company_id, period_end) where period_end is a
-- quarter end; the rate applies to the 4-quarter window ending there.

CREATE OR REPLACE VIEW v_tax_rate_ttm AS
SELECT
    company_id,
    period_end,
    tax_ttm,
    ebt_incl_unusual_ttm AS pretax_ttm,
    CASE
        WHEN quarters_in_window < 4 THEN NULL
        WHEN ebt_incl_unusual_ttm IS NULL THEN NULL
        WHEN ebt_incl_unusual_ttm <= 0 THEN 0.15
        WHEN tax_ttm IS NULL THEN NULL
        ELSE tax_ttm / ebt_incl_unusual_ttm
    END AS tax_rate_ttm
FROM v_ttm_flows;

COMMENT ON VIEW v_tax_rate_ttm IS
    'TTM effective tax rate. Fallback 15% when pretax ≤ 0. Consumed by ROIC, NOPAT Margin, Unlevered FCF.';
