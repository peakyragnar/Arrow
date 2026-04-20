"""Unit tests for the filesystem cache path builder.

Scope: endpoint-mirrored layout per docs/architecture/system.md §
Raw Cache Layout. Pure path construction; no filesystem writes.
"""

from __future__ import annotations

from arrow.ingest.common.cache import RAW_DIR, cache_path


def test_path_is_rooted_at_data_raw() -> None:
    p = cache_path("sec", "files", "company_tickers.json")
    assert p.is_relative_to(RAW_DIR)


def test_path_preserves_vendor_segment() -> None:
    p = cache_path("sec", "files", "company_tickers.json")
    assert p.parts[-4:] == ("raw", "sec", "files", "company_tickers.json")


def test_path_splits_slashes_in_endpoint() -> None:
    p = cache_path("sec", "submissions/CIK0001045810.json")
    assert p.parts[-3:] == ("sec", "submissions", "CIK0001045810.json")


def test_fmp_endpoint_mirrored_layout() -> None:
    p = cache_path("fmp", "income-statement/NVDA/annual.json")
    assert p.parts[-4:] == ("fmp", "income-statement", "NVDA", "annual.json")


def test_empty_pieces_from_double_slash_are_dropped() -> None:
    # Defensive: endpoint strings with leading slashes shouldn't break path.
    p = cache_path("sec", "/files/company_tickers.json")
    assert p.parts[-3:] == ("sec", "files", "company_tickers.json")


def test_deterministic_for_identical_inputs() -> None:
    a = cache_path("fmp", "balance-sheet-statement/NVDA/quarter.json")
    b = cache_path("fmp", "balance-sheet-statement/NVDA/quarter.json")
    assert a == b
