-- v_ttm_flows: trailing-12-months (4 quarters) rolling sums for every
-- flow concept used by formulas.md.
--
-- One row per (company_id, period_end) where period_end is a quarter
-- end. `quarters_in_window` = how many quarters actually summed (< 4
-- means partial history, so metric consumers typically suppress).
--
-- Window: 4 quarters (3 PRECEDING + current). SUM NULL-safe via
-- COALESCE on the source quarterly value.

CREATE OR REPLACE VIEW v_ttm_flows AS
WITH q AS (
    SELECT
        ticker,
        company_id,
        period_end,
        revenue,
        cogs,
        gross_profit,
        rd,
        total_opex,
        operating_income,
        interest_expense,
        ebt_incl_unusual,
        tax,
        net_income,
        cfo,
        capital_expenditures,
        dna_cf,
        sbc,
        cash_paid_for_interest,
        acquisitions,
        change_accounts_receivable,
        change_inventory,
        change_accounts_payable
    FROM v_company_period_wide
    WHERE period_type = 'quarter'
)
SELECT
    ticker,
    company_id,
    period_end,
    -- TTM sums (SUM NULL-safe: NULL contributes nothing)
    SUM(revenue)                OVER w AS revenue_ttm,
    SUM(cogs)                   OVER w AS cogs_ttm,
    SUM(gross_profit)           OVER w AS gross_profit_ttm,
    SUM(rd)                     OVER w AS rd_ttm,
    SUM(total_opex)             OVER w AS total_opex_ttm,
    SUM(operating_income)       OVER w AS operating_income_ttm,
    SUM(interest_expense)       OVER w AS interest_expense_ttm,
    SUM(ebt_incl_unusual)       OVER w AS ebt_incl_unusual_ttm,
    SUM(tax)                    OVER w AS tax_ttm,
    SUM(net_income)             OVER w AS net_income_ttm,
    SUM(cfo)                    OVER w AS cfo_ttm,
    SUM(capital_expenditures)   OVER w AS capital_expenditures_ttm,
    SUM(dna_cf)                 OVER w AS dna_ttm,
    SUM(sbc)                    OVER w AS sbc_ttm,
    SUM(cash_paid_for_interest) OVER w AS cash_paid_for_interest_ttm,
    SUM(acquisitions)           OVER w AS acquisitions_ttm,
    -- EBITDA TTM = Operating Income TTM + D&A TTM (formulas.md § 15)
    SUM(operating_income) OVER w + SUM(dna_cf) OVER w AS ebitda_ttm,
    COUNT(*) OVER w AS quarters_in_window
FROM q
WINDOW w AS (PARTITION BY company_id ORDER BY period_end
             ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);

COMMENT ON VIEW v_ttm_flows IS
    'Rolling 4-quarter sums for every flow concept used by formulas.md. quarters_in_window < 4 means partial TTM; consumers should typically suppress.';
