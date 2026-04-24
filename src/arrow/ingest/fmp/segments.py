"""Fetch + persist FMP revenue segmentation data for a ticker."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

import psycopg

from arrow.ingest.common.raw_responses import write_raw_response
from arrow.ingest.fmp.client import FMPClient
from arrow.ingest.fmp.paths import fmp_statement_path

PRODUCT_SEGMENT_ENDPOINT = "revenue-product-segmentation"
GEOGRAPHIC_SEGMENT_ENDPOINT = "revenue-geographic-segmentation"
DEFAULT_LIMIT = 1000

SegmentEndpoint = Literal[
    "revenue-product-segmentation",
    "revenue-geographic-segmentation",
]


@dataclass(frozen=True)
class SegmentRevenueFetch:
    raw_response_id: int
    rows: list[dict[str, Any]]
    endpoint: SegmentEndpoint


def fetch_revenue_segments(
    conn: psycopg.Connection,
    *,
    ticker: str,
    endpoint: SegmentEndpoint,
    period: str,
    ingest_run_id: int,
    client: FMPClient,
    limit: int = DEFAULT_LIMIT,
) -> SegmentRevenueFetch:
    """Fetch one FMP revenue segmentation endpoint for a ticker/period."""
    if endpoint not in (PRODUCT_SEGMENT_ENDPOINT, GEOGRAPHIC_SEGMENT_ENDPOINT):
        raise ValueError(f"unsupported segment endpoint: {endpoint!r}")
    if period not in ("annual", "quarter"):
        raise ValueError(f"period must be 'annual' or 'quarter', got {period!r}")

    params = {"symbol": ticker.upper(), "period": period, "limit": limit}
    resp = client.get(endpoint, **params)
    rows = json.loads(resp.body)

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
        cache_path=fmp_statement_path(endpoint, ticker, period),
    )
    return SegmentRevenueFetch(raw_response_id=raw_id, rows=rows, endpoint=endpoint)
