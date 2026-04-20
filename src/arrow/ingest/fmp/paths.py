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
