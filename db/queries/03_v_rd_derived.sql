-- v_rd_derived: 20-quarter R&D capitalization schedule per formulas.md.
--
-- For each quarterly (company, period_end) produce:
--   rd_q                     — R&D expense for that quarter
--   rd_amortization_q        — (sum rd over last 20 quarters) / 20
--   rd_asset_q               — sum rd(t−j) × (20−j)/20 for j=0..19
--   rd_coverage_quarters     — count of quarters actually present in the
--                              20-window (partial-history indicator)
--
-- Partial-history policy (formulas.md § R&D capitalization):
-- when fewer than 20 priors exist, missing priors are treated as 0 and
-- the metric is still computed. The `rd_coverage_quarters` field lets
-- consumers surface under-amortization in early-window periods.
--
-- Implementation: LATERAL subquery pulls the most-recent 20 quarters per
-- anchor row (fewer if not enough history). Aggregates compute amort and
-- asset. LIMIT 20 handles the window; the coverage count is COUNT(*).

CREATE OR REPLACE VIEW v_rd_derived AS
WITH quarterly_rd AS (
    SELECT
        company_id,
        period_end,
        COALESCE(MAX(value) FILTER (WHERE concept = 'rd'), 0) AS rd_q
    FROM v_ff_current
    WHERE period_type = 'quarter'
    GROUP BY company_id, period_end
)
SELECT
    anchor.company_id,
    anchor.period_end,
    anchor.rd_q,
    -- amortization = sum / 20, missing priors implicitly 0 (sum over <20 rows)
    COALESCE(SUM(prior.rd_q), 0) / 20.0 AS rd_amortization_q,
    -- asset = sum rd(t-j) * (20-j)/20; rn=1 is current (j=0, weight 20/20)
    COALESCE(SUM(prior.rd_q * (21 - prior.rn) / 20.0), 0) AS rd_asset_q,
    COUNT(prior.rd_q)::integer AS rd_coverage_quarters
FROM quarterly_rd anchor
LEFT JOIN LATERAL (
    SELECT rd_q, ROW_NUMBER() OVER (ORDER BY period_end DESC) AS rn
    FROM quarterly_rd inner_q
    WHERE inner_q.company_id = anchor.company_id
      AND inner_q.period_end <= anchor.period_end
    ORDER BY period_end DESC
    LIMIT 20
) prior ON true
GROUP BY anchor.company_id, anchor.period_end, anchor.rd_q;

COMMENT ON VIEW v_rd_derived IS
    'Quarterly R&D capitalization schedule per formulas.md § R&D capitalization rules. rd_coverage_quarters < 20 signals partial history; under-amortization is deliberate per the partial-history policy.';
