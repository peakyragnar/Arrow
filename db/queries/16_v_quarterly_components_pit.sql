-- v_quarterly_components_pit: quarterly facts pivoted wide with asof_date.
--
-- One row per (company_id, period_end) for quarterly facts. Columns are
-- the concepts needed for valuation. `asof_date` is when this quarter
-- was published (max published_at across the row's concepts) — the date
-- on which downstream PIT consumers can start using these values.
--
-- Used by v_quarterly_ttm_pit (rolling 4-quarter sums) and downstream
-- valuation views.
--
-- v1 PIT note: this view reads v_ff_current (superseded_at IS NULL).
-- That means: for periods that have been restated, only the latest
-- restated value appears. The PIT axis we DO handle is publication date
-- (asof_date). Restatement-aware backtests are a v2 concern; see
-- docs/architecture/prices_ingest_plan.md § Point-in-time methodology.

CREATE OR REPLACE VIEW v_quarterly_components_pit AS
SELECT
    company_id,
    period_end,
    fiscal_period_label,
    MAX(published_at)::date AS asof_date,

    -- Income statement (TTM-summable)
    MAX(value) FILTER (WHERE statement='income_statement' AND concept='net_income')        AS net_income,
    MAX(value) FILTER (WHERE statement='income_statement' AND concept='revenue')           AS revenue,
    MAX(value) FILTER (WHERE statement='income_statement' AND concept='operating_income')  AS operating_income,

    -- Cash flow (TTM-summable)
    MAX(value) FILTER (WHERE statement='cash_flow'        AND concept='dna_cf')            AS dna_cf,
    MAX(value) FILTER (WHERE statement='cash_flow'        AND concept='cfo')               AS cfo,
    MAX(value) FILTER (WHERE statement='cash_flow'        AND concept='capital_expenditures') AS capital_expenditures,

    -- Balance sheet (point-in-time, NOT summed)
    MAX(value) FILTER (WHERE statement='balance_sheet' AND concept='cash_and_equivalents')      AS cash_and_equivalents,
    MAX(value) FILTER (WHERE statement='balance_sheet' AND concept='short_term_investments')    AS short_term_investments,
    MAX(value) FILTER (WHERE statement='balance_sheet' AND concept='long_term_debt')            AS long_term_debt,
    MAX(value) FILTER (WHERE statement='balance_sheet' AND concept='current_portion_lt_debt')   AS current_portion_lt_debt,
    MAX(value) FILTER (WHERE statement='balance_sheet' AND concept='noncontrolling_interest')   AS noncontrolling_interest

FROM v_ff_current
WHERE period_type = 'quarter'
GROUP BY company_id, period_end, fiscal_period_label;

COMMENT ON VIEW v_quarterly_components_pit IS
    'Quarterly facts pivoted wide with asof_date (max published_at). Substrate for v_quarterly_ttm_pit and the valuation views.';
