-- Screen: highest Days Payable Outstanding in the last complete
-- calendar year per ticker.
--
-- "Last complete calendar year" = the most recent CY with all 4 calendar
-- quarters present in the database (quarters_in_year = 4). For each
-- ticker, takes the max DPO across the 4 quarters in that year.

WITH last_complete_cy AS (
    SELECT ticker, MAX(calendar_year) AS cy
    FROM v_metrics_cy
    WHERE quarters_in_year = 4
    GROUP BY ticker
),
dpo_in_cy AS (
    SELECT
        q.ticker,
        q.period_end,
        q.calendar_year,
        q.dpo,
        ROW_NUMBER() OVER (PARTITION BY q.ticker, q.calendar_year ORDER BY q.dpo DESC NULLS LAST) AS rn
    FROM v_metrics_q q
    JOIN last_complete_cy lcy
      ON lcy.ticker = q.ticker AND lcy.cy = q.calendar_year
    WHERE q.dpo IS NOT NULL
)
SELECT
    ticker,
    calendar_year AS cy,
    period_end    AS peak_dpo_quarter_end,
    ROUND(dpo, 1) AS peak_dpo_days
FROM dpo_in_cy
WHERE rn = 1
ORDER BY peak_dpo_days DESC;
