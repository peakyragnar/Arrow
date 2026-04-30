"""Fetch + persist FMP analyst-estimates / price-target / earnings / grades payloads.

Six endpoints (the probe on 2026-04-30 enumerated them; see
``docs/architecture/estimates_ingest_plan.md`` § Endpoint Choice):

  - ``analyst-estimates?period=annual``    forward + historical annual consensus
  - ``analyst-estimates?period=quarter``   forward + historical quarterly consensus
  - ``price-target-consensus``             single-row snapshot per ticker
  - ``earnings``                           historical actuals + upcoming estimates
  - ``grades``                             event log of rating actions (full history, no pagination)
  - ``price-target-news``                  paginated event log of analyst price-target updates

Each fetcher persists its raw payload to ``raw_responses`` (JSON body) and
mirrors bytes to the filesystem cache at the standard endpoint-mirrored
path. Returns the parsed rows + the raw_response id for downstream loaders.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg

from arrow.ingest.common.cache import cache_path
from arrow.ingest.common.raw_responses import write_raw_response
from arrow.ingest.fmp.client import FMPClient
from arrow.ingest.fmp.paths import fmp_per_ticker_path

ANALYST_ESTIMATES_ENDPOINT = "analyst-estimates"
PRICE_TARGET_CONSENSUS_ENDPOINT = "price-target-consensus"
EARNINGS_ENDPOINT = "earnings"
GRADES_ENDPOINT = "grades"
PRICE_TARGET_NEWS_ENDPOINT = "price-target-news"


@dataclass(frozen=True)
class EstimatesFetch:
    raw_response_id: int
    rows: list[dict[str, Any]]


def _persist(
    conn: psycopg.Connection,
    *,
    endpoint: str,
    params: dict[str, Any],
    resp,
    ingest_run_id: int,
    cache_target: Path,
) -> EstimatesFetch:
    body = json.loads(resp.body)
    if not isinstance(body, list):
        raise RuntimeError(
            f"FMP {endpoint} returned non-list: "
            f"{type(body).__name__}: {str(body)[:200]}"
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
        cache_path=cache_target,
    )
    return EstimatesFetch(raw_response_id=raw_id, rows=body)


def fetch_analyst_estimates(
    conn: psycopg.Connection,
    *,
    ticker: str,
    period: str,            # 'annual' | 'quarter'
    ingest_run_id: int,
    client: FMPClient,
    limit: int = 200,
) -> EstimatesFetch:
    """Forward + historical analyst consensus per fiscal period."""
    if period not in ("annual", "quarter"):
        raise ValueError(f"period must be 'annual' or 'quarter', got {period!r}")
    params: dict[str, Any] = {
        "symbol": ticker.upper(),
        "period": period,
        "limit": limit,
    }
    resp = client.get(ANALYST_ESTIMATES_ENDPOINT, **params)
    return _persist(
        conn,
        endpoint=ANALYST_ESTIMATES_ENDPOINT,
        params=params,
        resp=resp,
        ingest_run_id=ingest_run_id,
        cache_target=cache_path(
            "fmp", ANALYST_ESTIMATES_ENDPOINT, ticker.upper(), f"{period}.json"
        ),
    )


def fetch_price_target_consensus(
    conn: psycopg.Connection,
    *,
    ticker: str,
    ingest_run_id: int,
    client: FMPClient,
) -> EstimatesFetch:
    """Single-row consensus snapshot (high / low / median / consensus)."""
    params = {"symbol": ticker.upper()}
    resp = client.get(PRICE_TARGET_CONSENSUS_ENDPOINT, **params)
    return _persist(
        conn,
        endpoint=PRICE_TARGET_CONSENSUS_ENDPOINT,
        params=params,
        resp=resp,
        ingest_run_id=ingest_run_id,
        cache_target=fmp_per_ticker_path(PRICE_TARGET_CONSENSUS_ENDPOINT, ticker),
    )


def fetch_earnings(
    conn: psycopg.Connection,
    *,
    ticker: str,
    ingest_run_id: int,
    client: FMPClient,
    limit: int = 200,
) -> EstimatesFetch:
    """Historical actuals + upcoming estimates per announcement."""
    params = {"symbol": ticker.upper(), "limit": limit}
    resp = client.get(EARNINGS_ENDPOINT, **params)
    return _persist(
        conn,
        endpoint=EARNINGS_ENDPOINT,
        params=params,
        resp=resp,
        ingest_run_id=ingest_run_id,
        cache_target=fmp_per_ticker_path(EARNINGS_ENDPOINT, ticker),
    )


def fetch_grades(
    conn: psycopg.Connection,
    *,
    ticker: str,
    ingest_run_id: int,
    client: FMPClient,
    limit: int = 2000,
) -> EstimatesFetch:
    """Event log of rating actions. FMP returns full history in one call."""
    params = {"symbol": ticker.upper(), "limit": limit}
    resp = client.get(GRADES_ENDPOINT, **params)
    return _persist(
        conn,
        endpoint=GRADES_ENDPOINT,
        params=params,
        resp=resp,
        ingest_run_id=ingest_run_id,
        cache_target=fmp_per_ticker_path(GRADES_ENDPOINT, ticker),
    )


def fetch_price_target_news_page(
    conn: psycopg.Connection,
    *,
    ticker: str,
    page: int,
    ingest_run_id: int,
    client: FMPClient,
    limit: int = 100,
) -> EstimatesFetch:
    """One page of analyst price-target updates. Caller walks pages until empty."""
    params = {"symbol": ticker.upper(), "page": page, "limit": limit}
    resp = client.get(PRICE_TARGET_NEWS_ENDPOINT, **params)
    page_path = cache_path(
        "fmp",
        PRICE_TARGET_NEWS_ENDPOINT,
        ticker.upper(),
        f"page-{page:03d}.json",
    )
    return _persist(
        conn,
        endpoint=PRICE_TARGET_NEWS_ENDPOINT,
        params=params,
        resp=resp,
        ingest_run_id=ingest_run_id,
        cache_target=page_path,
    )
