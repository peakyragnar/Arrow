-- Screen: tickers in a net-cash position at the latest quarter-end.
--
-- Net Debt < 0 per formulas.md § 15: cash + ST investments exceeds total
-- debt + operating lease liabilities. The ratio is reported so screens
-- can rank companies by depth of net cash relative to their earnings.

WITH latest AS (
    SELECT
        ticker,
        period_end,
        net_debt,
        net_debt_to_ebitda,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end DESC) AS rn
    FROM v_metrics_q
)
SELECT
    ticker,
    period_end              AS latest_quarter_end,
    ROUND(net_debt / 1e9, 2) AS net_debt_bn,
    ROUND(net_debt_to_ebitda, 2) AS net_debt_to_ebitda
FROM latest
WHERE rn = 1
  AND net_debt < 0
ORDER BY net_debt ASC;  -- most-negative (deepest net cash) first
