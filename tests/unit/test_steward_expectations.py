"""Unit tests for steward expectations resolution + evaluation.

Pure functions, no DB. Verifies:
  - The single STANDARD covers the four canonical verticals
  - expectations_for(ticker) returns the standard verbatim (V1.1+
    has no per-tier or per-ticker variation; legitimate exceptions
    live in suppression notes on findings, not in code)
  - Each rule kind evaluates correctly
"""

from __future__ import annotations

import pytest

from arrow.steward.expectations import (
    STANDARD,
    Expectation,
    evaluate_expectation,
    expectations_for,
)


# ---------------------------------------------------------------------------
# expectations_for + STANDARD
# ---------------------------------------------------------------------------


def test_standard_covers_four_canonical_verticals() -> None:
    verticals = {e.vertical for e in STANDARD}
    assert "financials" in verticals
    assert "segments" in verticals
    assert "employees" in verticals
    assert "sec_qual" in verticals


def test_expectations_for_returns_standard_verbatim() -> None:
    rules = expectations_for("ANY")
    assert rules == STANDARD


def test_expectations_for_uniform_across_tickers() -> None:
    """Two different tickers get identical expectations. Comparability
    is the property we're protecting — every coverage member is held
    to the same standard."""
    a = expectations_for("PLTR")
    b = expectations_for("CRWV")
    assert a == b


def test_financials_standard_is_5_years_quarterly() -> None:
    fin = next(e for e in STANDARD if e.vertical == "financials")
    assert fin.rule == "min_periods"
    assert fin.params["count"] == 20


# ---------------------------------------------------------------------------
# evaluate_expectation
# ---------------------------------------------------------------------------


def test_evaluate_present_with_data() -> None:
    r = evaluate_expectation(
        Expectation("segments", "present", {}),
        has_data=True, period_count=3, latest_age_days=10.0,
    )
    assert r.met is True


def test_evaluate_present_without_data() -> None:
    r = evaluate_expectation(
        Expectation("segments", "present", {}),
        has_data=False, period_count=0, latest_age_days=None,
    )
    assert r.met is False
    assert "no current rows" in r.detail


def test_evaluate_min_periods_met() -> None:
    r = evaluate_expectation(
        Expectation("financials", "min_periods", {"count": 20}),
        has_data=True, period_count=25, latest_age_days=30.0,
    )
    assert r.met is True
    assert r.actual == 25
    assert r.expected == 20


def test_evaluate_min_periods_unmet() -> None:
    r = evaluate_expectation(
        Expectation("financials", "min_periods", {"count": 20}),
        has_data=True, period_count=12, latest_age_days=30.0,
    )
    assert r.met is False
    assert r.actual == 12
    assert r.expected == 20
    assert "12 < 20" in r.detail


def test_evaluate_min_periods_at_threshold_is_met() -> None:
    r = evaluate_expectation(
        Expectation("financials", "min_periods", {"count": 20}),
        has_data=True, period_count=20, latest_age_days=30.0,
    )
    assert r.met is True


def test_evaluate_recency_within_window() -> None:
    r = evaluate_expectation(
        Expectation("employees", "recency", {"max_age_days": 400}),
        has_data=True, period_count=1, latest_age_days=120.0,
    )
    assert r.met is True


def test_evaluate_recency_stale() -> None:
    r = evaluate_expectation(
        Expectation("employees", "recency", {"max_age_days": 400}),
        has_data=True, period_count=1, latest_age_days=500.0,
    )
    assert r.met is False
    assert "500d" in r.detail
    assert "400d" in r.detail


def test_evaluate_recency_no_data() -> None:
    r = evaluate_expectation(
        Expectation("employees", "recency", {"max_age_days": 400}),
        has_data=False, period_count=0, latest_age_days=None,
    )
    assert r.met is False
    assert "no data" in r.detail


def test_evaluate_unknown_rule_raises() -> None:
    with pytest.raises(ValueError):
        evaluate_expectation(
            Expectation("financials", "weirdrule", {}),
            has_data=True, period_count=10, latest_age_days=30.0,
        )
