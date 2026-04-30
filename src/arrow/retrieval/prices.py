"""Retrieval helpers for prices + valuations.

Powers the `/ask` tools `read_prices` and `read_valuations`. Reads from
`prices_daily`, `historical_market_cap`, and `v_valuation_ratios_ttm`.

Design:
  - `read_prices`     — cheap range read, returns daily bars with both
                        raw (`close`) and adjusted (`adj_close`) prices.
  - `read_valuations` — single-date PIT lookup. Defaults to the latest
                        available trading day if `as_of` is omitted.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

import psycopg


# Hard cap so a runaway window doesn't dump 8 years × 252 days into the
# planner's context. The planner can always page if needed.
DEFAULT_MAX_PRICE_ROWS = 400


@dataclass(frozen=True)
class PriceBar:
    security_id: int
    ticker: str
    date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    adj_close: Decimal
    volume: int


@dataclass(frozen=True)
class Valuation:
    security_id: int
    ticker: str
    company_id: int | None
    date: date
    close: Decimal
    adj_close: Decimal
    market_cap: Decimal | None
    fiscal_period_label_at_asof: str | None
    components_known_since: date | None
    quarters_in_window: int

    pe_ttm: Decimal | None
    ps_ttm: Decimal | None
    ev_ebitda_ttm: Decimal | None
    fcf_yield_ttm: Decimal | None  # fraction; multiply by 100 for percent
    ev: Decimal | None

    ttm_net_income: Decimal | None
    ttm_revenue: Decimal | None
    ttm_operating_income: Decimal | None
    ttm_dna: Decimal | None
    ttm_ebitda: Decimal | None
    ttm_cfo: Decimal | None
    ttm_capex: Decimal | None
    ttm_fcf: Decimal | None

    cash_and_equivalents: Decimal | None
    short_term_investments: Decimal | None
    long_term_debt: Decimal | None
    current_portion_lt_debt: Decimal | None
    noncontrolling_interest: Decimal | None


def read_prices(
    conn: psycopg.Connection,
    *,
    ticker: str,
    from_date: date,
    to_date: date,
    max_rows: int = DEFAULT_MAX_PRICE_ROWS,
) -> list[PriceBar]:
    """Daily bars for one security across [from_date, to_date], asc.

    Caps at ``max_rows`` (default 400) so a runaway window doesn't flood
    the planner. Caller is responsible for narrowing if hit.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT pd.security_id, s.ticker, pd.date,
                   pd.open, pd.high, pd.low, pd.close, pd.adj_close, pd.volume
            FROM prices_daily pd
            JOIN securities s ON s.id = pd.security_id
            WHERE s.ticker = %s
              AND s.status = 'active'
              AND pd.date BETWEEN %s AND %s
            ORDER BY pd.date ASC
            LIMIT %s;
            """,
            (ticker.upper(), from_date, to_date, max_rows),
        )
        rows = cur.fetchall()
    return [
        PriceBar(
            security_id=r[0],
            ticker=r[1],
            date=r[2],
            open=r[3],
            high=r[4],
            low=r[5],
            close=r[6],
            adj_close=r[7],
            volume=r[8],
        )
        for r in rows
    ]


def latest_price_date(conn: psycopg.Connection, ticker: str) -> date | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(pd.date)
            FROM prices_daily pd
            JOIN securities s ON s.id = pd.security_id
            WHERE s.ticker = %s AND s.status = 'active';
            """,
            (ticker.upper(),),
        )
        row = cur.fetchone()
    return row[0] if row else None


@dataclass(frozen=True)
class ValuationSamplePoint:
    security_id: int
    ticker: str
    date: date
    market_cap: Decimal | None
    pe_ttm: Decimal | None
    ps_ttm: Decimal | None
    ev_ebitda_ttm: Decimal | None
    fcf_yield_ttm: Decimal | None  # fraction
    fiscal_period_label_at_asof: str | None


@dataclass(frozen=True)
class ValuationPercentile:
    security_id: int
    ticker: str
    as_of: date
    window_from: date
    window_to: date
    n_samples: int

    pe_ttm: Decimal | None
    pe_min: Decimal | None
    pe_median: Decimal | None
    pe_max: Decimal | None
    pe_percentile: float | None  # fraction 0..1; how many historical days had a *lower* PE

    ps_ttm: Decimal | None
    ps_percentile: float | None

    ev_ebitda_ttm: Decimal | None
    ev_ebitda_percentile: float | None

    fcf_yield_ttm: Decimal | None
    fcf_yield_percentile: float | None  # high yield = "cheap" — interpret as inverse


def read_valuation_series(
    conn: psycopg.Connection,
    *,
    ticker: str,
    from_date: date,
    to_date: date,
    sample: str = "monthly",
) -> list[ValuationSamplePoint]:
    """Sampled valuation series for one ticker.

    ``sample`` controls grain:
      - ``daily``    every trading day in window (caps at 400 rows)
      - ``monthly``  first trading day of each month (default)
      - ``quarterly`` first trading day of each calendar quarter
      - ``yearly``    first trading day of each year

    Sampling is "first row in the period" — picks the earliest available
    trading day within each period bucket. Skips rows where pe_ttm IS NULL
    (partial TTM history at the start of a series).
    """
    bucket_expr = {
        "daily": None,
        "monthly": "date_trunc('month', date)",
        "quarterly": "date_trunc('quarter', date)",
        "yearly": "date_trunc('year', date)",
    }
    if sample not in bucket_expr:
        raise ValueError(f"sample must be one of {list(bucket_expr)}, got {sample!r}")

    if sample == "daily":
        sql = """
            SELECT security_id, ticker, date, market_cap,
                   pe_ttm, ps_ttm, ev_ebitda_ttm, fcf_yield_ttm,
                   fiscal_period_label_at_asof
            FROM v_valuation_ratios_ttm
            WHERE ticker = %s AND date BETWEEN %s AND %s
              AND pe_ttm IS NOT NULL
            ORDER BY date
            LIMIT 400;
        """
        params: tuple = (ticker.upper(), from_date, to_date)
    else:
        bucket = bucket_expr[sample]
        sql = f"""
            SELECT security_id, ticker, date, market_cap,
                   pe_ttm, ps_ttm, ev_ebitda_ttm, fcf_yield_ttm,
                   fiscal_period_label_at_asof
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY ticker, {bucket}
                           ORDER BY date
                       ) AS rn
                FROM v_valuation_ratios_ttm
                WHERE ticker = %s
                  AND date BETWEEN %s AND %s
                  AND pe_ttm IS NOT NULL
            ) ranked
            WHERE rn = 1
            ORDER BY date;
        """
        params = (ticker.upper(), from_date, to_date)

    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return [
        ValuationSamplePoint(
            security_id=r[0], ticker=r[1], date=r[2], market_cap=r[3],
            pe_ttm=r[4], ps_ttm=r[5], ev_ebitda_ttm=r[6], fcf_yield_ttm=r[7],
            fiscal_period_label_at_asof=r[8],
        )
        for r in rows
    ]


def valuation_percentile(
    conn: psycopg.Connection,
    *,
    ticker: str,
    as_of: date | None = None,
    window_years: int = 5,
) -> ValuationPercentile | None:
    """Where today's valuation sits in this ticker's own historical distribution.

    Returns the current ratios + their percentile rank vs the trailing
    ``window_years``. Percentile = fraction of historical observations
    BELOW the current value. So pe_percentile = 0.85 means "today's P/E
    is higher than 85% of the last 5 years' daily P/Es."

    For FCF yield, higher is "cheaper" — interpret the percentile inversely.
    """
    if as_of is None:
        as_of = latest_price_date(conn, ticker)
        if as_of is None:
            return None

    from datetime import timedelta
    window_from = as_of - timedelta(days=365 * window_years)

    with conn.cursor() as cur:
        cur.execute(
            """
            WITH win AS (
                SELECT pe_ttm, ps_ttm, ev_ebitda_ttm, fcf_yield_ttm
                FROM v_valuation_ratios_ttm
                WHERE ticker = %s
                  AND date BETWEEN %s AND %s
                  AND pe_ttm IS NOT NULL
            ),
            cur AS (
                SELECT security_id, ticker, date,
                       pe_ttm, ps_ttm, ev_ebitda_ttm, fcf_yield_ttm
                FROM v_valuation_ratios_ttm
                WHERE ticker = %s AND date = %s
                LIMIT 1
            )
            SELECT
                cur.security_id, cur.ticker, cur.date,
                (SELECT COUNT(*) FROM win) AS n_samples,
                cur.pe_ttm, (SELECT MIN(pe_ttm) FROM win), (SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY pe_ttm) FROM win), (SELECT MAX(pe_ttm) FROM win),
                CASE WHEN cur.pe_ttm IS NULL THEN NULL ELSE
                    (SELECT COUNT(*)::float / NULLIF((SELECT COUNT(*) FROM win), 0)
                     FROM win WHERE pe_ttm < cur.pe_ttm)
                END,
                cur.ps_ttm,
                CASE WHEN cur.ps_ttm IS NULL THEN NULL ELSE
                    (SELECT COUNT(*)::float / NULLIF((SELECT COUNT(*) FROM win), 0)
                     FROM win WHERE ps_ttm < cur.ps_ttm)
                END,
                cur.ev_ebitda_ttm,
                CASE WHEN cur.ev_ebitda_ttm IS NULL THEN NULL ELSE
                    (SELECT COUNT(*)::float / NULLIF((SELECT COUNT(*) FROM win), 0)
                     FROM win WHERE ev_ebitda_ttm < cur.ev_ebitda_ttm)
                END,
                cur.fcf_yield_ttm,
                CASE WHEN cur.fcf_yield_ttm IS NULL THEN NULL ELSE
                    (SELECT COUNT(*)::float / NULLIF((SELECT COUNT(*) FROM win), 0)
                     FROM win WHERE fcf_yield_ttm < cur.fcf_yield_ttm)
                END
            FROM cur;
            """,
            (ticker.upper(), window_from, as_of, ticker.upper(), as_of),
        )
        row = cur.fetchone()
    if row is None or row[0] is None:
        return None
    return ValuationPercentile(
        security_id=row[0], ticker=row[1], as_of=row[2],
        window_from=window_from, window_to=as_of,
        n_samples=int(row[3]),
        pe_ttm=row[4], pe_min=row[5], pe_median=row[6], pe_max=row[7],
        pe_percentile=row[8],
        ps_ttm=row[9], ps_percentile=row[10],
        ev_ebitda_ttm=row[11], ev_ebitda_percentile=row[12],
        fcf_yield_ttm=row[13], fcf_yield_percentile=row[14],
    )


def read_valuation(
    conn: psycopg.Connection,
    *,
    ticker: str,
    as_of: date | None = None,
) -> Valuation | None:
    """Valuation ratios + components for one (security, date).

    If ``as_of`` is omitted, returns the latest available trading day.
    If the requested date is not a trading day (weekend/holiday), returns
    None — caller should normalize to the nearest trading day if needed.
    """
    if as_of is None:
        as_of = latest_price_date(conn, ticker)
        if as_of is None:
            return None

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                security_id, ticker, company_id, date,
                close, adj_close, market_cap,
                fiscal_period_label_at_asof, components_known_since, quarters_in_window,
                pe_ttm, ps_ttm, ev_ebitda_ttm, fcf_yield_ttm, ev,
                ttm_net_income, ttm_revenue, ttm_operating_income, ttm_dna,
                ttm_ebitda, ttm_cfo, ttm_capex, ttm_fcf,
                cash_and_equivalents, short_term_investments,
                long_term_debt, current_portion_lt_debt, noncontrolling_interest
            FROM v_valuation_ratios_ttm
            WHERE ticker = %s AND date = %s
            LIMIT 1;
            """,
            (ticker.upper(), as_of),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return Valuation(
        security_id=row[0], ticker=row[1], company_id=row[2], date=row[3],
        close=row[4], adj_close=row[5], market_cap=row[6],
        fiscal_period_label_at_asof=row[7], components_known_since=row[8],
        quarters_in_window=row[9],
        pe_ttm=row[10], ps_ttm=row[11], ev_ebitda_ttm=row[12],
        fcf_yield_ttm=row[13], ev=row[14],
        ttm_net_income=row[15], ttm_revenue=row[16],
        ttm_operating_income=row[17], ttm_dna=row[18],
        ttm_ebitda=row[19], ttm_cfo=row[20], ttm_capex=row[21], ttm_fcf=row[22],
        cash_and_equivalents=row[23], short_term_investments=row[24],
        long_term_debt=row[25], current_portion_lt_debt=row[26],
        noncontrolling_interest=row[27],
    )
