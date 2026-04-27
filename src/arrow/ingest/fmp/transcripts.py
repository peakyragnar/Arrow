"""Fetch + persist FMP earnings-call transcript data.

This module mirrors the existing FMP statement fetchers: callers own the
transaction, fetchers write one raw_responses row plus the filesystem cache,
and return parsed payloads for the normalize/orchestration layer.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import psycopg

from arrow.ingest.common.raw_responses import write_raw_response
from arrow.ingest.fmp.client import FMPClient
from arrow.ingest.fmp.paths import fmp_transcript_dates_path, fmp_transcript_path

TRANSCRIPT_DATES_ENDPOINT = "earning-call-transcript-dates"
TRANSCRIPT_ENDPOINT = "earning-call-transcript"


@dataclass(frozen=True)
class TranscriptDate:
    fiscal_year: int
    fiscal_quarter: int
    call_date: date


@dataclass(frozen=True)
class Transcript:
    ticker: str
    fiscal_year: int
    fiscal_quarter: int
    call_date: date
    content: str
    raw_row: dict[str, Any]


@dataclass(frozen=True)
class TranscriptDatesFetch:
    raw_response_id: int
    dates: list[TranscriptDate]
    raw_body: bytes


@dataclass(frozen=True)
class TranscriptFetch:
    raw_response_id: int
    transcript: Transcript | None
    raw_body: bytes


def fetch_transcript_dates(
    conn: psycopg.Connection,
    *,
    ticker: str,
    ingest_run_id: int,
    client: FMPClient,
) -> TranscriptDatesFetch:
    """Fetch available transcript periods for one ticker."""
    ticker = ticker.upper()
    params = {"symbol": ticker}
    resp = client.get(TRANSCRIPT_DATES_ENDPOINT, **params)
    rows = json.loads(resp.body)
    if not isinstance(rows, list):
        raise ValueError(f"expected list from {TRANSCRIPT_DATES_ENDPOINT}, got {type(rows).__name__}")

    raw_id = write_raw_response(
        conn,
        ingest_run_id=ingest_run_id,
        vendor="fmp",
        endpoint=TRANSCRIPT_DATES_ENDPOINT,
        params=params,
        request_url=resp.url,
        http_status=resp.status,
        content_type=resp.content_type,
        response_headers=resp.headers,
        body=resp.body,
        cache_path=fmp_transcript_dates_path(ticker),
    )
    return TranscriptDatesFetch(
        raw_response_id=raw_id,
        dates=[_parse_transcript_date(row) for row in rows],
        raw_body=resp.body,
    )


def fetch_earning_call_transcript(
    conn: psycopg.Connection,
    *,
    ticker: str,
    fiscal_year: int,
    fiscal_quarter: int,
    ingest_run_id: int,
    client: FMPClient,
) -> TranscriptFetch:
    """Fetch one FMP earnings-call transcript. Empty payload returns None."""
    ticker = ticker.upper()
    if fiscal_quarter not in (1, 2, 3, 4):
        raise ValueError(f"fiscal_quarter must be 1..4, got {fiscal_quarter!r}")

    params = {"symbol": ticker, "year": int(fiscal_year), "quarter": int(fiscal_quarter)}
    resp = client.get(TRANSCRIPT_ENDPOINT, **params)
    rows = json.loads(resp.body)
    if not isinstance(rows, list):
        raise ValueError(f"expected list from {TRANSCRIPT_ENDPOINT}, got {type(rows).__name__}")

    raw_id = write_raw_response(
        conn,
        ingest_run_id=ingest_run_id,
        vendor="fmp",
        endpoint=TRANSCRIPT_ENDPOINT,
        params=params,
        request_url=resp.url,
        http_status=resp.status,
        content_type=resp.content_type,
        response_headers=resp.headers,
        body=resp.body,
        cache_path=fmp_transcript_path(ticker, fiscal_year, fiscal_quarter),
    )

    transcript = None
    if rows:
        if len(rows) > 1:
            raise ValueError(
                f"expected at most one transcript for {ticker} FY{fiscal_year} Q{fiscal_quarter}, "
                f"got {len(rows)}"
            )
        transcript = _parse_transcript(rows[0], ticker=ticker)

    return TranscriptFetch(
        raw_response_id=raw_id,
        transcript=transcript,
        raw_body=resp.body,
    )


def _parse_transcript_date(row: dict[str, Any]) -> TranscriptDate:
    try:
        quarter = int(row["quarter"])
        fiscal_year = int(row["fiscalYear"])
        call_date = datetime.strptime(str(row["date"]), "%Y-%m-%d").date()
    except Exception as e:
        raise ValueError(f"invalid transcript-date row: {row!r}") from e
    if quarter not in (1, 2, 3, 4):
        raise ValueError(f"invalid transcript quarter in row: {row!r}")
    return TranscriptDate(
        fiscal_year=fiscal_year,
        fiscal_quarter=quarter,
        call_date=call_date,
    )


def _parse_transcript(row: dict[str, Any], *, ticker: str) -> Transcript:
    try:
        period = str(row["period"]).upper()
        if not period.startswith("Q"):
            raise ValueError(f"period does not start with Q: {period!r}")
        quarter = int(period[1:])
        fiscal_year = int(row["year"])
        call_date = datetime.strptime(str(row["date"]), "%Y-%m-%d").date()
        content = str(row["content"])
        symbol = str(row["symbol"]).upper()
    except Exception as e:
        raise ValueError(f"invalid transcript row: {row!r}") from e

    if quarter not in (1, 2, 3, 4):
        raise ValueError(f"invalid transcript quarter in row: {row!r}")
    if symbol != ticker.upper():
        raise ValueError(f"transcript symbol mismatch: expected {ticker.upper()}, got {symbol}")
    return Transcript(
        ticker=symbol,
        fiscal_year=fiscal_year,
        fiscal_quarter=quarter,
        call_date=call_date,
        content=content,
        raw_row=row,
    )
