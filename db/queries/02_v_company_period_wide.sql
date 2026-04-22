-- v_company_period_wide: pivot long-format financial_facts into one row
-- per (ticker, period_end, period_type) with one column per canonical
-- bucket. This is the workbench other views query against.
--
-- Columns: ticker, company_id, period_end, period_type, fiscal + calendar
-- fields, then every IS/BS/CF/metrics bucket.
--
-- Missing facts return NULL (no plug). A bucket column being NULL for a
-- given row means that concept wasn't reported for that period.

CREATE OR REPLACE VIEW v_company_period_wide AS
SELECT
    c.ticker,
    c.id AS company_id,
    f.period_end,
    f.period_type,
    f.fiscal_year,
    f.fiscal_quarter,
    f.fiscal_period_label,
    f.calendar_year,
    f.calendar_quarter,
    f.calendar_period_label,

    -- ====== Income statement ======
    MAX(f.value) FILTER (WHERE f.concept = 'revenue')                     AS revenue,
    MAX(f.value) FILTER (WHERE f.concept = 'cogs')                        AS cogs,
    MAX(f.value) FILTER (WHERE f.concept = 'gross_profit')                AS gross_profit,
    MAX(f.value) FILTER (WHERE f.concept = 'rd')                          AS rd,
    MAX(f.value) FILTER (WHERE f.concept = 'general_and_admin_expense')   AS general_and_admin_expense,
    MAX(f.value) FILTER (WHERE f.concept = 'selling_and_marketing_expense') AS selling_and_marketing_expense,
    MAX(f.value) FILTER (WHERE f.concept = 'sga')                         AS sga,
    MAX(f.value) FILTER (WHERE f.concept = 'total_opex')                  AS total_opex,
    MAX(f.value) FILTER (WHERE f.concept = 'operating_income')            AS operating_income,
    MAX(f.value) FILTER (WHERE f.concept = 'interest_expense')            AS interest_expense,
    MAX(f.value) FILTER (WHERE f.concept = 'interest_income')             AS interest_income,
    MAX(f.value) FILTER (WHERE f.concept = 'ebt_incl_unusual')            AS ebt_incl_unusual,
    MAX(f.value) FILTER (WHERE f.concept = 'tax')                         AS tax,
    MAX(f.value) FILTER (WHERE f.concept = 'continuing_ops_after_tax')    AS continuing_ops_after_tax,
    MAX(f.value) FILTER (WHERE f.concept = 'discontinued_ops')            AS discontinued_ops,
    MAX(f.value) FILTER (WHERE f.concept = 'net_income')                  AS net_income,
    MAX(f.value) FILTER (WHERE f.concept = 'net_income_attributable_to_parent') AS net_income_attributable_to_parent,
    MAX(f.value) FILTER (WHERE f.concept = 'minority_interest')           AS minority_interest,
    MAX(f.value) FILTER (WHERE f.concept = 'eps_basic')                   AS eps_basic,
    MAX(f.value) FILTER (WHERE f.concept = 'eps_diluted')                 AS eps_diluted,
    MAX(f.value) FILTER (WHERE f.concept = 'shares_basic_weighted_avg')   AS shares_basic_weighted_avg,
    MAX(f.value) FILTER (WHERE f.concept = 'shares_diluted_weighted_avg') AS shares_diluted_weighted_avg,

    -- ====== Balance sheet ======
    MAX(f.value) FILTER (WHERE f.concept = 'cash_and_equivalents')        AS cash_and_equivalents,
    MAX(f.value) FILTER (WHERE f.concept = 'short_term_investments')      AS short_term_investments,
    MAX(f.value) FILTER (WHERE f.concept = 'accounts_receivable')         AS accounts_receivable,
    MAX(f.value) FILTER (WHERE f.concept = 'other_receivables')           AS other_receivables,
    MAX(f.value) FILTER (WHERE f.concept = 'inventory')                   AS inventory,
    MAX(f.value) FILTER (WHERE f.concept = 'prepaid_expenses')            AS prepaid_expenses,
    MAX(f.value) FILTER (WHERE f.concept = 'other_current_assets')        AS other_current_assets,
    MAX(f.value) FILTER (WHERE f.concept = 'total_current_assets')        AS total_current_assets,
    MAX(f.value) FILTER (WHERE f.concept = 'net_ppe')                     AS net_ppe,
    MAX(f.value) FILTER (WHERE f.concept = 'long_term_investments')       AS long_term_investments,
    MAX(f.value) FILTER (WHERE f.concept = 'goodwill')                    AS goodwill,
    MAX(f.value) FILTER (WHERE f.concept = 'other_intangibles')           AS other_intangibles,
    MAX(f.value) FILTER (WHERE f.concept = 'deferred_tax_assets_noncurrent') AS deferred_tax_assets_noncurrent,
    MAX(f.value) FILTER (WHERE f.concept = 'other_noncurrent_assets')     AS other_noncurrent_assets,
    MAX(f.value) FILTER (WHERE f.concept = 'total_assets')                AS total_assets,
    MAX(f.value) FILTER (WHERE f.concept = 'accounts_payable')            AS accounts_payable,
    MAX(f.value) FILTER (WHERE f.concept = 'accrued_expenses')            AS accrued_expenses,
    MAX(f.value) FILTER (WHERE f.concept = 'current_portion_lt_debt')     AS current_portion_lt_debt,
    MAX(f.value) FILTER (WHERE f.concept = 'current_portion_leases_operating') AS current_portion_leases_operating,
    MAX(f.value) FILTER (WHERE f.concept = 'deferred_revenue_current')    AS deferred_revenue_current,
    MAX(f.value) FILTER (WHERE f.concept = 'income_taxes_payable_current') AS income_taxes_payable_current,
    MAX(f.value) FILTER (WHERE f.concept = 'other_current_liabilities')   AS other_current_liabilities,
    MAX(f.value) FILTER (WHERE f.concept = 'total_current_liabilities')   AS total_current_liabilities,
    MAX(f.value) FILTER (WHERE f.concept = 'long_term_debt')              AS long_term_debt,
    MAX(f.value) FILTER (WHERE f.concept = 'long_term_leases_operating')  AS long_term_leases_operating,
    MAX(f.value) FILTER (WHERE f.concept = 'deferred_revenue_noncurrent') AS deferred_revenue_noncurrent,
    MAX(f.value) FILTER (WHERE f.concept = 'deferred_tax_liability_noncurrent') AS deferred_tax_liability_noncurrent,
    MAX(f.value) FILTER (WHERE f.concept = 'other_noncurrent_liabilities') AS other_noncurrent_liabilities,
    MAX(f.value) FILTER (WHERE f.concept = 'total_liabilities')           AS total_liabilities,
    MAX(f.value) FILTER (WHERE f.concept = 'preferred_stock')             AS preferred_stock,
    MAX(f.value) FILTER (WHERE f.concept = 'common_stock')                AS common_stock,
    MAX(f.value) FILTER (WHERE f.concept = 'additional_paid_in_capital')  AS additional_paid_in_capital,
    MAX(f.value) FILTER (WHERE f.concept = 'retained_earnings')           AS retained_earnings,
    MAX(f.value) FILTER (WHERE f.concept = 'treasury_stock')              AS treasury_stock,
    MAX(f.value) FILTER (WHERE f.concept = 'accumulated_other_comprehensive_income') AS aoci,
    MAX(f.value) FILTER (WHERE f.concept = 'other_equity')                AS other_equity,
    MAX(f.value) FILTER (WHERE f.concept = 'noncontrolling_interest')     AS noncontrolling_interest,
    MAX(f.value) FILTER (WHERE f.concept = 'total_equity')                AS total_equity,
    MAX(f.value) FILTER (WHERE f.concept = 'total_liabilities_and_equity') AS total_liabilities_and_equity,

    -- ====== Cash flow ======
    MAX(f.value) FILTER (WHERE f.concept = 'net_income_start')            AS net_income_start,
    MAX(f.value) FILTER (WHERE f.concept = 'dna_cf')                      AS dna_cf,
    MAX(f.value) FILTER (WHERE f.concept = 'sbc')                         AS sbc,
    MAX(f.value) FILTER (WHERE f.concept = 'deferred_income_tax')         AS deferred_income_tax,
    MAX(f.value) FILTER (WHERE f.concept = 'other_noncash')               AS other_noncash,
    MAX(f.value) FILTER (WHERE f.concept = 'change_accounts_receivable')  AS change_accounts_receivable,
    MAX(f.value) FILTER (WHERE f.concept = 'change_inventory')            AS change_inventory,
    MAX(f.value) FILTER (WHERE f.concept = 'change_accounts_payable')     AS change_accounts_payable,
    MAX(f.value) FILTER (WHERE f.concept = 'change_other_working_capital') AS change_other_working_capital,
    MAX(f.value) FILTER (WHERE f.concept = 'cfo')                         AS cfo,
    MAX(f.value) FILTER (WHERE f.concept = 'capital_expenditures')        AS capital_expenditures,
    MAX(f.value) FILTER (WHERE f.concept = 'acquisitions')                AS acquisitions,
    MAX(f.value) FILTER (WHERE f.concept = 'purchases_of_investments')    AS purchases_of_investments,
    MAX(f.value) FILTER (WHERE f.concept = 'sales_of_investments')        AS sales_of_investments,
    MAX(f.value) FILTER (WHERE f.concept = 'other_investing')             AS other_investing,
    MAX(f.value) FILTER (WHERE f.concept = 'cfi')                         AS cfi,
    MAX(f.value) FILTER (WHERE f.concept = 'short_term_debt_issuance')    AS short_term_debt_issuance,
    MAX(f.value) FILTER (WHERE f.concept = 'long_term_debt_issuance')     AS long_term_debt_issuance,
    MAX(f.value) FILTER (WHERE f.concept = 'stock_issuance')              AS stock_issuance,
    MAX(f.value) FILTER (WHERE f.concept = 'stock_repurchase')            AS stock_repurchase,
    MAX(f.value) FILTER (WHERE f.concept = 'common_dividends_paid')       AS common_dividends_paid,
    MAX(f.value) FILTER (WHERE f.concept = 'preferred_dividends_paid')    AS preferred_dividends_paid,
    MAX(f.value) FILTER (WHERE f.concept = 'other_financing')             AS other_financing,
    MAX(f.value) FILTER (WHERE f.concept = 'cff')                         AS cff,
    MAX(f.value) FILTER (WHERE f.concept = 'fx_effect_on_cash')           AS fx_effect_on_cash,
    MAX(f.value) FILTER (WHERE f.concept = 'net_change_in_cash')          AS net_change_in_cash,
    MAX(f.value) FILTER (WHERE f.concept = 'cash_begin_of_period')        AS cash_begin_of_period,
    MAX(f.value) FILTER (WHERE f.concept = 'cash_end_of_period')          AS cash_end_of_period,
    MAX(f.value) FILTER (WHERE f.concept = 'cash_paid_for_interest')      AS cash_paid_for_interest,

    -- ====== Metrics (non-statement) ======
    MAX(f.value) FILTER (WHERE f.concept = 'total_employees')             AS total_employees

FROM v_ff_current f
JOIN companies c ON c.id = f.company_id
GROUP BY
    c.ticker, c.id,
    f.period_end, f.period_type,
    f.fiscal_year, f.fiscal_quarter, f.fiscal_period_label,
    f.calendar_year, f.calendar_quarter, f.calendar_period_label;

COMMENT ON VIEW v_company_period_wide IS
    'Long-to-wide pivot of v_ff_current. One row per (ticker, period_end, period_type); columns are canonical buckets. NULL means concept not reported for that period.';
