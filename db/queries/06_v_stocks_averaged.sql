-- v_stocks_averaged: two-point average of stock concepts per quarter-end.
--
-- For ratios with a stock denominator (ROIC, accruals), formulas.md uses
-- the average of beginning and ending balance:
--   avg_stock(t) = (stock(t) + stock(t-1)) / 2
--
-- NULL-safe: if either endpoint is NULL, the average is NULL.

CREATE OR REPLACE VIEW v_stocks_averaged AS
WITH q AS (
    SELECT
        company_id,
        period_end,
        total_assets,
        total_equity,
        cash_and_equivalents,
        short_term_investments,
        accounts_receivable,
        inventory,
        accounts_payable,
        current_portion_lt_debt,
        long_term_debt,
        current_portion_leases_operating,
        long_term_leases_operating
    FROM v_company_period_wide
    WHERE period_type = 'quarter'
)
SELECT
    company_id,
    period_end,
    (total_assets + LAG(total_assets) OVER w) / 2.0 AS avg_total_assets,
    (total_equity + LAG(total_equity) OVER w) / 2.0 AS avg_total_equity,
    total_assets AS total_assets_end,
    LAG(total_assets) OVER w AS total_assets_begin,
    -- Prior-year (4-quarter lag) endpoints, for ROIIC's YoY invested-capital delta.
    LAG(total_equity, 4) OVER w AS total_equity_prior_year,
    -- Raw stock endpoints needed by other views (current quarter-end).
    total_equity AS total_equity_end,
    cash_and_equivalents,
    short_term_investments,
    accounts_receivable,
    inventory,
    accounts_payable,
    current_portion_lt_debt,
    long_term_debt,
    current_portion_leases_operating,
    long_term_leases_operating
FROM q
WINDOW w AS (PARTITION BY company_id ORDER BY period_end);

COMMENT ON VIEW v_stocks_averaged IS
    'Two-point averages of balance-sheet stock concepts for use in TTM-numerator/avg-stock ratios.';
