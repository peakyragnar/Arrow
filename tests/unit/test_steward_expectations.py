"""Unit tests for steward expectations resolution + evaluation.

Pure functions, no DB. Verifies:
  - tier defaults are present
  - per-ticker overrides REPLACE matching params, leave others intact
  - unknown tier raises
  - each rule kind evaluates correctly
"""

from __future__ import annotations

import pytest

from arrow.steward.expectations import (
    PER_TICKER_OVERRIDES,
    UNIVERSE_DEFAULTS,
    Expectation,
    evaluate_expectation,
    expectations_for,
)


# ---------------------------------------------------------------------------
# expectations_for
# ---------------------------------------------------------------------------


def test_expectations_for_core_returns_tier_defaults() -> None:
    rules = expectations_for("UNRELATED", "core")
    verticals = {e.vertical for e in rules}
    # The four core verticals defined in UNIVERSE_DEFAULTS.
    assert "financials" in verticals
    assert "segments" in verticals
    assert "employees" in verticals
    assert "sec_qual" in verticals


def test_expectations_for_extended_lighter_than_core() -> None:
    core = expectations_for("UNRELATED", "core")
    extended = expectations_for("UNRELATED", "extended")
    assert len(extended) <= len(core)
    # Extended should expect fewer financial periods than core.
    core_fin = next(e for e in core if e.vertical == "financials")
    ext_fin = next(e for e in extended if e.vertical == "financials")
    assert ext_fin.params["count"] < core_fin.params["count"]


def test_expectations_for_unknown_tier_raises() -> None:
    with pytest.raises(ValueError):
        expectations_for("X", "premium")


def test_per_ticker_override_replaces_matching_params() -> None:
    """CRWV (recent IPO) overrides 'financials' min_periods count to a
    smaller value. The rule kind stays the same; only the count
    changes."""
    crwv = expectations_for("CRWV", "core")
    crwv_fin = next(e for e in crwv if e.vertical == "financials")
    universe_fin = next(
        e for e in UNIVERSE_DEFAULTS["core"] if e.vertical == "financials"
    )
    assert crwv_fin.rule == universe_fin.rule  # rule unchanged
    assert crwv_fin.params["count"] != universe_fin.params["count"]
    assert crwv_fin.params["count"] == PER_TICKER_OVERRIDES["CRWV"]["financials"]["count"]


def test_per_ticker_override_only_affects_named_verticals() -> None:
    """CRWV overrides financials + sec_qual but NOT segments/employees.
    Those should still get the universe defaults verbatim."""
    crwv = expectations_for("CRWV", "core")
    crwv_seg = next(e for e in crwv if e.vertical == "segments")
    universe_seg = next(
        e for e in UNIVERSE_DEFAULTS["core"] if e.vertical == "segments"
    )
    assert crwv_seg == universe_seg


def test_ticker_with_no_overrides_gets_pure_defaults() -> None:
    pltr = expectations_for("PLTR", "core")
    expected = expectations_for("UNRELATED", "core")
    assert pltr == expected


def test_per_ticker_override_is_case_insensitive() -> None:
    a = expectations_for("crwv", "core")
    b = expectations_for("CRWV", "core")
    assert a == b


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
