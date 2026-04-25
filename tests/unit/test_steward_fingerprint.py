"""Unit tests for steward fingerprint determinism."""

from __future__ import annotations

import pytest

from arrow.steward.fingerprint import fingerprint


def test_same_inputs_same_fingerprint():
    a = fingerprint("zero_row_runs", {"ticker": "PLTR"}, {"window_days": 7})
    b = fingerprint("zero_row_runs", {"ticker": "PLTR"}, {"window_days": 7})
    assert a == b


def test_dict_key_order_irrelevant():
    a = fingerprint(
        "expected_coverage",
        {"ticker": "MSFT", "vertical": "segments", "fiscal_period_key": "FY2025-Q4"},
        {"min_periods": 4},
    )
    b = fingerprint(
        "expected_coverage",
        {"fiscal_period_key": "FY2025-Q4", "vertical": "segments", "ticker": "MSFT"},
        {"min_periods": 4},
    )
    assert a == b


def test_none_scope_value_equivalent_to_omitted():
    a = fingerprint("cross_cutting_check", {"ticker": None}, {})
    b = fingerprint("cross_cutting_check", {}, {})
    assert a == b


def test_different_check_name_different_fingerprint():
    a = fingerprint("zero_row_runs", {"ticker": "PLTR"}, {})
    b = fingerprint("broken_provenance", {"ticker": "PLTR"}, {})
    assert a != b


def test_different_scope_value_different_fingerprint():
    a = fingerprint("zero_row_runs", {"ticker": "PLTR"}, {})
    b = fingerprint("zero_row_runs", {"ticker": "MSFT"}, {})
    assert a != b


def test_different_rule_params_different_fingerprint():
    """A tightened threshold is a different rule and should fingerprint
    differently — otherwise the runner would dedup against the looser
    rule's findings."""
    a = fingerprint("unresolved_flags_aging", {"ticker": "PLTR"}, {"threshold_days": 14})
    b = fingerprint("unresolved_flags_aging", {"ticker": "PLTR"}, {"threshold_days": 7})
    assert a != b


def test_unicode_preserved_in_scope():
    a = fingerprint("seg_label", {"label": "Récurrent"}, {})
    b = fingerprint("seg_label", {"label": "Récurrent"}, {})
    assert a == b


def test_empty_check_name_rejected():
    with pytest.raises(ValueError):
        fingerprint("", {"ticker": "PLTR"}, {})


def test_returns_64_char_hex():
    result = fingerprint("any_check", {"x": 1}, {})
    assert len(result) == 64
    assert all(c in "0123456789abcdef" for c in result)


def test_no_args_optional_scope_and_params():
    """Universe-scope checks may have no scope or params at all."""
    a = fingerprint("global_check")
    b = fingerprint("global_check", None, None)
    c = fingerprint("global_check", {}, {})
    assert a == b == c
