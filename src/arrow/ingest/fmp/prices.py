"""Fetch + persist FMP price/market-cap payloads for a security.

Three endpoint variants are involved (the spike on 2026-04-30 enumerated
them; see docs/architecture/prices_ingest_plan.md § FMP Endpoints):

  - ``historical-price-eod/non-split-adjusted``  → raw as-traded prices.
        Fields: ``adjOpen / adjHigh / adjLow / adjClose / volume`` (the
        ``adj`` prefix is FMP's naming; the values are NOT
        adjusted — they are the original quoted prices, e.g. NVDA's
        $1,210 close on 2024-06-06 pre-split).

  - ``historical-price-eod/dividend-adjusted``   → split + dividend
        adjusted close. Use for return math.

  - ``historical-market-capitalization``         → daily market cap series.

Each fetcher persists its raw payload to ``raw_responses`` (JSON body) and
mirrors bytes to the filesystem cache at the standard endpoint-mirrored
path. Returns the parsed rows + the raw_response id for downstream loaders.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import psycopg

from arrow.ingest.common.raw_responses import write_raw_response
from arrow.ingest.fmp.client import FMPClient
from arrow.ingest.fmp.paths import fmp_per_ticker_path

PRICES_RAW_ENDPOINT = "historical-price-eod/non-split-adjusted"
PRICES_ADJUSTED_ENDPOINT = "historical-price-eod/dividend-adjusted"
MARKET_CAP_ENDPOINT = "historical-market-capitalization"


@dataclass(frozen=True)
class PricesFetch:
    raw_response_id: int
    rows: list[dict[str, Any]]


def _fetch_endpoint(
    conn: psycopg.Connection,
    *,
    endpoint: str,
    ticker: str,
    since_date: str | None,
    until_date: str | None,
    ingest_run_id: int,
    client: FMPClient,
) -> PricesFetch:
    params: dict[str, Any] = {"symbol": ticker.upper()}
    if since_date is not None:
        params["from"] = since_date
    if until_date is not None:
        params["to"] = until_date

    resp = client.get(endpoint, **params)
    rows = json.loads(resp.body)
    if not isinstance(rows, list):
        raise RuntimeError(
            f"FMP {endpoint} for {ticker} returned non-list: "
            f"{type(rows).__name__}: {str(rows)[:200]}"
        )

    raw_id = write_raw_response(
        conn,
        ingest_run_id=ingest_run_id,
        vendor="fmp",
        endpoint=endpoint,
        params=params,
        request_url=resp.url,
        http_status=resp.status,
        content_type=resp.content_type,
        response_headers=resp.headers,
        body=resp.body,
        cache_path=fmp_per_ticker_path(endpoint, ticker),
    )
    return PricesFetch(raw_response_id=raw_id, rows=rows)


def fetch_prices_raw(
    conn: psycopg.Connection,
    *,
    ticker: str,
    since_date: str | None,
    until_date: str | None,
    ingest_run_id: int,
    client: FMPClient,
) -> PricesFetch:
    """Raw as-traded prices. Use for `prices_daily.close` / open / high / low / volume."""
    return _fetch_endpoint(
        conn,
        endpoint=PRICES_RAW_ENDPOINT,
        ticker=ticker,
        since_date=since_date,
        until_date=until_date,
        ingest_run_id=ingest_run_id,
        client=client,
    )


def fetch_prices_adjusted(
    conn: psycopg.Connection,
    *,
    ticker: str,
    since_date: str | None,
    until_date: str | None,
    ingest_run_id: int,
    client: FMPClient,
) -> PricesFetch:
    """Split + dividend adjusted close. Use for `prices_daily.adj_close`."""
    return _fetch_endpoint(
        conn,
        endpoint=PRICES_ADJUSTED_ENDPOINT,
        ticker=ticker,
        since_date=since_date,
        until_date=until_date,
        ingest_run_id=ingest_run_id,
        client=client,
    )


def fetch_market_cap(
    conn: psycopg.Connection,
    *,
    ticker: str,
    since_date: str | None,
    until_date: str | None,
    ingest_run_id: int,
    client: FMPClient,
) -> PricesFetch:
    """Historical daily market capitalization."""
    return _fetch_endpoint(
        conn,
        endpoint=MARKET_CAP_ENDPOINT,
        ticker=ticker,
        since_date=since_date,
        until_date=until_date,
        ingest_run_id=ingest_run_id,
        client=client,
    )
