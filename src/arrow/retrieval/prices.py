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
