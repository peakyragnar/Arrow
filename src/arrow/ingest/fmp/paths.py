"""FMP cache path helpers.

Endpoint-mirrored per docs/architecture/system.md § Raw Cache Layout:
    data/raw/fmp/{endpoint-path}/{TICKER}/{key}.json
"""

from __future__ import annotations

from pathlib import Path

from arrow.ingest.common.cache import cache_path


def fmp_statement_path(endpoint: str, ticker: str, period: str) -> Path:
    """Build cache path for an FMP statement endpoint.

    endpoint: 'income-statement' | 'balance-sheet-statement' | 'cash-flow-statement'
    period:   'annual' | 'quarter'
    """
    return cache_path("fmp", endpoint, ticker.upper(), f"{period}.json")


def fmp_per_ticker_path(endpoint: str, ticker: str) -> Path:
    """Build cache path for a per-ticker FMP endpoint with no period slicing.

    Used for endpoints like `historical-employee-count` that return the
    filer's full history in one pull.

        data/raw/fmp/{endpoint}/{TICKER}.json
    """
    return cache_path("fmp", endpoint, f"{ticker.upper()}.json")


def fmp_transcript_dates_path(ticker: str) -> Path:
    """Build cache path for FMP transcript-date inventory.

        data/raw/fmp/earning-call-transcript-dates/{TICKER}.json
    """
    return cache_path("fmp", "earning-call-transcript-dates", f"{ticker.upper()}.json")


def fmp_transcript_path(ticker: str, fiscal_year: int, fiscal_quarter: int) -> Path:
    """Build cache path for one FMP earnings-call transcript.

        data/raw/fmp/earning-call-transcript/{TICKER}/FY{year}-Q{quarter}.json
    """
    if fiscal_quarter not in (1, 2, 3, 4):
        raise ValueError(f"fiscal_quarter must be 1..4, got {fiscal_quarter!r}")
    return cache_path(
        "fmp",
        "earning-call-transcript",
        ticker.upper(),
        f"FY{int(fiscal_year)}-Q{fiscal_quarter}.json",
    )
