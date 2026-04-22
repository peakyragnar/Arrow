-- v_metrics_cy: calendar-year aggregated metrics per formulas.md grain rules.
--
-- For the dashboard's calendar-annual columns, we aggregate the 4
-- calendar quarters inside each calendar year:
--   Flows         → sum of 4 quarterly values (revenue, cfo, etc.)
--   Stocks        → value at calendar Q4 period_end (year-end snapshot)
--   Ratios        → computed on aggregated numerator/denominator
--
-- A calendar year is "complete" only when all 4 calendar quarters are
-- present in our data (quarters_in_year = 4). Partial CYs still emit a
-- row but mark quarters_in_year so consumers can filter.

CREATE OR REPLACE VIEW v_metrics_cy AS
WITH q AS (
    -- Quarterly facts indexed by calendar year.
    SELECT
        ticker,
        company_id,
        calendar_year,
        calendar_quarter,
        period_end,
        revenue,
        cogs,
        gross_profit,
        operating_income,
        net_income,
        rd,
        sbc,
        cfo,
        capital_expenditures,
        dna_cf,
        interest_expense,
        cash_paid_for_interest,
        acquisitions,
        change_accounts_receivable,
        change_inventory,
        change_accounts_payable,
        accounts_receivable,
        inventory,
        accounts_payable,
        total_assets,
        total_equity,
        cash_and_equivalents,
        short_term_investments,
        current_portion_lt_debt,
        long_term_debt,
        current_portion_leases_operating,
        long_term_leases_operating,
        tax,
        ebt_incl_unusual
    FROM v_company_period_wide
    WHERE period_type = 'quarter'
),
agg AS (
    SELECT
        ticker,
        company_id,
        calendar_year,
        COUNT(*) AS quarters_in_year,
        -- Flow sums
        SUM(revenue)                 AS revenue_cy,
        SUM(cogs)                    AS cogs_cy,
        SUM(gross_profit)            AS gross_profit_cy,
        SUM(operating_income)        AS operating_income_cy,
        SUM(net_income)              AS net_income_cy,
        SUM(rd)                      AS rd_cy,
        SUM(sbc)                     AS sbc_cy,
        SUM(cfo)                     AS cfo_cy,
        SUM(capital_expenditures)    AS capital_expenditures_cy,
        SUM(dna_cf)                  AS dna_cy,
        SUM(interest_expense)        AS interest_expense_cy,
        SUM(cash_paid_for_interest)  AS cash_paid_for_interest_cy,
        SUM(acquisitions)            AS acquisitions_cy,
        SUM(tax)                     AS tax_cy,
        SUM(ebt_incl_unusual)        AS ebt_incl_unusual_cy,
        -- Stock snapshots at calendar Q4 (latest calendar_quarter in the year)
        (ARRAY_AGG(total_assets ORDER BY period_end DESC))[1]       AS total_assets_cy_end,
        (ARRAY_AGG(total_equity ORDER BY period_end DESC))[1]       AS total_equity_cy_end,
        (ARRAY_AGG(cash_and_equivalents ORDER BY period_end DESC))[1] AS cash_cy_end,
        (ARRAY_AGG(short_term_investments ORDER BY period_end DESC))[1] AS short_term_investments_cy_end,
        (ARRAY_AGG(current_portion_lt_debt ORDER BY period_end DESC))[1] AS current_portion_lt_debt_cy_end,
        (ARRAY_AGG(long_term_debt ORDER BY period_end DESC))[1]     AS long_term_debt_cy_end,
        (ARRAY_AGG(current_portion_leases_operating ORDER BY period_end DESC))[1] AS current_portion_leases_operating_cy_end,
        (ARRAY_AGG(long_term_leases_operating ORDER BY period_end DESC))[1] AS long_term_leases_operating_cy_end,
        (ARRAY_AGG(accounts_receivable ORDER BY period_end DESC))[1] AS accounts_receivable_cy_end,
        (ARRAY_AGG(inventory ORDER BY period_end DESC))[1]          AS inventory_cy_end,
        (ARRAY_AGG(accounts_payable ORDER BY period_end DESC))[1]   AS accounts_payable_cy_end,
        MAX(period_end) AS cy_last_period_end
    FROM q
    GROUP BY ticker, company_id, calendar_year
)
SELECT
    ticker,
    company_id,
    calendar_year,
    quarters_in_year,
    cy_last_period_end,

    -- Levels
    revenue_cy,
    gross_profit_cy,
    operating_income_cy,
    net_income_cy,
    cfo_cy,
    capital_expenditures_cy,
    dna_cy,
    sbc_cy,

    -- Margins (CY aggregation)
    CASE WHEN revenue_cy IS NULL OR revenue_cy = 0 THEN NULL
         ELSE gross_profit_cy / revenue_cy END AS gross_margin_cy,
    CASE WHEN revenue_cy IS NULL OR revenue_cy = 0 THEN NULL
         ELSE operating_income_cy / revenue_cy END AS operating_margin_cy,
    CASE WHEN revenue_cy IS NULL OR revenue_cy = 0 THEN NULL
         ELSE net_income_cy / revenue_cy END AS net_margin_cy,

    -- Simple derived ratios
    CASE WHEN revenue_cy IS NULL OR revenue_cy = 0 THEN NULL
         ELSE sbc_cy / revenue_cy END AS sbc_pct_revenue_cy,

    -- CY end-of-year stocks for downstream joining
    total_assets_cy_end,
    total_equity_cy_end,

    -- Net debt at CY end
    (
        COALESCE(current_portion_lt_debt_cy_end, 0)
      + COALESCE(long_term_debt_cy_end, 0)
      + COALESCE(current_portion_leases_operating_cy_end, 0)
      + COALESCE(long_term_leases_operating_cy_end, 0)
      - COALESCE(cash_cy_end, 0)
      - COALESCE(short_term_investments_cy_end, 0)
    ) AS net_debt_cy_end,

    accounts_receivable_cy_end,
    inventory_cy_end,
    accounts_payable_cy_end
FROM agg;

COMMENT ON VIEW v_metrics_cy IS
    'Calendar-year aggregated metrics. Flows summed over 4 calendar quarters; stocks snapshotted at calendar Q4 period_end. quarters_in_year < 4 indicates partial year.';
