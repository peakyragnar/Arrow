"""FMP daily prices + market cap ingest orchestration.

For each security:
  1. Fetch three FMP endpoints (raw prices, adjusted prices, market cap).
  2. Persist raw payloads to ``raw_responses`` (3 rows per security).
  3. Join raw + adjusted by date and upsert into ``prices_daily``.
  4. Upsert ``historical_market_cap``.

Idempotent: re-runs UPSERT on (security_id, date), so the script is safe to
schedule daily — historical rows stay stable, new rows fall in at the end of
the series.

See ``docs/architecture/prices_ingest_plan.md`` for design rationale.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import psycopg

from arrow.ingest.common.runs import close_failed, close_succeeded, open_run
from arrow.ingest.fmp.client import FMPClient
from arrow.ingest.fmp.prices import (
    fetch_market_cap,
    fetch_prices_adjusted,
    fetch_prices_raw,
)


DEFAULT_SINCE_DATE = "2018-01-01"


@dataclass(frozen=True)
class SecurityRow:
    id: int
    ticker: str
    kind: str  # 'common_stock' | 'etf' | 'index'


class SecurityNotSeeded(RuntimeError):
    pass


def _resolve_securities(
    conn: psycopg.Connection, tickers: list[str]
) -> list[SecurityRow]:
    out: list[SecurityRow] = []
    with conn.cursor() as cur:
        for ticker in tickers:
            cur.execute(
                """
                SELECT id, ticker, kind
                FROM securities
                WHERE ticker = %s AND status = 'active'
                """,
                (ticker.upper(),),
            )
            row = cur.fetchone()
            if row is None:
                raise SecurityNotSeeded(
                    f"{ticker} not in securities — run seed_securities.py first"
                )
            out.append(SecurityRow(id=row[0], ticker=row[1], kind=row[2]))
    return out


def _upsert_prices_daily(
    conn: psycopg.Connection,
    *,
    security_id: int,
    raw_rows: list[dict[str, Any]],
    adjusted_rows: list[dict[str, Any]],
    raw_response_id: int,
) -> tuple[int, int]:
    """Join raw + adjusted by date and upsert into prices_daily.

    Returns (inserted, updated). Updates are values that changed on conflict;
    no-op rewrites count as "updated" since we cannot distinguish without an
    extra round trip per row (not worth it at this scale).
    """
    adj_close_by_date: dict[str, Any] = {
        r["date"]: r.get("adjClose") for r in adjusted_rows
    }

    inserted = 0
    updated = 0

    with conn.cursor() as cur:
        for r in raw_rows:
            date_str = r.get("date")
            adj_close = adj_close_by_date.get(date_str)
            if adj_close is None:
                # Mismatched date between raw and adjusted endpoints.
                # Skip rather than fabricate. Surfaces as a row-count gap if
                # this happens often enough to matter.
                continue

            cur.execute(
                """
                INSERT INTO prices_daily (
                    security_id, date, open, high, low, close, adj_close, volume,
                    source_raw_response_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (security_id, date) DO UPDATE SET
                    open      = EXCLUDED.open,
                    high      = EXCLUDED.high,
                    low       = EXCLUDED.low,
                    close     = EXCLUDED.close,
                    adj_close = EXCLUDED.adj_close,
                    volume    = EXCLUDED.volume,
                    source_raw_response_id = EXCLUDED.source_raw_response_id,
                    ingested_at = now()
                RETURNING (xmax = 0) AS inserted
                """,
                (
                    security_id,
                    date_str,
                    Decimal(str(r["adjOpen"])),
                    Decimal(str(r["adjHigh"])),
                    Decimal(str(r["adjLow"])),
                    Decimal(str(r["adjClose"])),  # raw close in this endpoint
                    Decimal(str(adj_close)),
                    int(r.get("volume") or 0),
                    raw_response_id,
                ),
            )
            was_insert = cur.fetchone()[0]
            if was_insert:
                inserted += 1
            else:
                updated += 1
    return inserted, updated


def _upsert_market_cap(
    conn: psycopg.Connection,
    *,
    security_id: int,
    rows: list[dict[str, Any]],
    raw_response_id: int,
) -> tuple[int, int]:
    inserted = 0
    updated = 0
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(
                """
                INSERT INTO historical_market_cap (
                    security_id, date, market_cap, source_raw_response_id
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (security_id, date) DO UPDATE SET
                    market_cap = EXCLUDED.market_cap,
                    source_raw_response_id = EXCLUDED.source_raw_response_id,
                    ingested_at = now()
                RETURNING (xmax = 0) AS inserted
                """,
                (
                    security_id,
                    r["date"],
                    Decimal(str(r["marketCap"])),
                    raw_response_id,
                ),
            )
            was_insert = cur.fetchone()[0]
            if was_insert:
                inserted += 1
            else:
                updated += 1
    return inserted, updated


def backfill_fmp_prices(
    conn: psycopg.Connection,
    tickers: list[str],
    *,
    since_date: str | None = DEFAULT_SINCE_DATE,
    until_date: str | None = None,
) -> dict[str, Any]:
    """Backfill prices_daily + historical_market_cap for the given tickers.

    Each ticker resolves to a `securities` row (ETFs/common stock both work).
    Three FMP endpoints are called per ticker; results upserted on
    (security_id, date).
    """
    securities = _resolve_securities(conn, tickers)

    run_id = open_run(
        conn,
        run_kind="manual",
        vendor="fmp",
        ticker_scope=[s.ticker for s in securities],
    )
    client = FMPClient()

    counts: dict[str, Any] = {
        "raw_responses": 0,
        "prices_rows_inserted": 0,
        "prices_rows_updated": 0,
        "market_cap_rows_inserted": 0,
        "market_cap_rows_updated": 0,
    }

    try:
        for security in securities:
            with conn.transaction():
                raw = fetch_prices_raw(
                    conn,
                    ticker=security.ticker,
                    since_date=since_date,
                    until_date=until_date,
                    ingest_run_id=run_id,
                    client=client,
                )
                adj = fetch_prices_adjusted(
                    conn,
                    ticker=security.ticker,
                    since_date=since_date,
                    until_date=until_date,
                    ingest_run_id=run_id,
                    client=client,
                )
                mc = fetch_market_cap(
                    conn,
                    ticker=security.ticker,
                    since_date=since_date,
                    until_date=until_date,
                    ingest_run_id=run_id,
                    client=client,
                )
                counts["raw_responses"] += 3

                p_ins, p_upd = _upsert_prices_daily(
                    conn,
                    security_id=security.id,
                    raw_rows=raw.rows,
                    adjusted_rows=adj.rows,
                    raw_response_id=raw.raw_response_id,
                )
                counts["prices_rows_inserted"] += p_ins
                counts["prices_rows_updated"] += p_upd

                m_ins, m_upd = _upsert_market_cap(
                    conn,
                    security_id=security.id,
                    rows=mc.rows,
                    raw_response_id=mc.raw_response_id,
                )
                counts["market_cap_rows_inserted"] += m_ins
                counts["market_cap_rows_updated"] += m_upd

    except Exception as e:
        close_failed(
            conn,
            run_id,
            error_message=str(e),
            error_details={"kind": type(e).__name__},
        )
        raise

    close_succeeded(conn, run_id, counts=counts)
    counts["ingest_run_id"] = run_id
    return counts
