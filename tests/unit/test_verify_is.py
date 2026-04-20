"""Unit tests for Layer 1 IS subtotal-tie verification."""

from __future__ import annotations

from decimal import Decimal

from arrow.normalize.financials.verify_is import (
    TOLERANCE_ABSOLUTE,
    TieFailure,
    verify_is_ties,
)


def _nvda_fy26_q4_values() -> dict[str, Decimal]:
    return {
        "revenue":                  Decimal("68127000000"),
        "cogs":                     Decimal("17034000000"),
        "gross_profit":             Decimal("51093000000"),
        "total_opex":               Decimal("6794000000"),
        "operating_income":         Decimal("44299000000"),
        "ebt_incl_unusual":         Decimal("50398000000"),
        "tax":                      Decimal("7438000000"),
        "continuing_ops_after_tax": Decimal("42960000000"),
        "discontinued_ops":         Decimal("0"),
        "net_income":               Decimal("42960000000"),
    }


def test_real_nvda_row_passes_all_four_ties() -> None:
    assert verify_is_ties(_nvda_fy26_q4_values()) == []


def test_gross_profit_mismatch_beyond_tolerance_fails() -> None:
    values = _nvda_fy26_q4_values()
    # Break gross_profit by $500M — well beyond $1M absolute, also beyond 0.1%.
    values["gross_profit"] = Decimal("51593000000")
    failures = verify_is_ties(values)
    assert len(failures) >= 1
    assert any("gross_profit" in f.tie for f in failures)


def test_small_rounding_within_tolerance_passes() -> None:
    """A $500K mismatch is well under the $1M absolute floor — should pass."""
    values = _nvda_fy26_q4_values()
    values["gross_profit"] = values["revenue"] - values["cogs"] + Decimal("500000")
    assert verify_is_ties(values) == []


def test_pct_tolerance_allows_larger_absolute_on_bigger_filers() -> None:
    """0.1% of $100B = $100M; delta of $50M should pass under pct floor."""
    values = {
        "revenue":      Decimal("200000000000"),
        "cogs":          Decimal("100000000000"),
        "gross_profit":  Decimal("100050000000"),  # $50M over perfect
    }
    # Only the first tie has enough data; others are skipped.
    assert verify_is_ties(values) == []


def test_tie_skipped_when_component_missing() -> None:
    """If a required component is absent, that tie is SUPPRESSED (not failed)."""
    values = _nvda_fy26_q4_values()
    del values["cogs"]  # breaks the gross_profit tie availability
    failures = verify_is_ties(values)
    # gross_profit tie skipped; the other three still check and still pass.
    for f in failures:
        assert "gross_profit == revenue - cogs" not in f.tie


def test_multiple_tie_failures_are_all_reported() -> None:
    """Breaking gross_profit cascades into operating_income (same component).
    Additionally breaking net_income hits that tie independently.
    We assert all affected ties surface rather than short-circuiting after the first.
    """
    values = _nvda_fy26_q4_values()
    values["gross_profit"] = values["revenue"] - values["cogs"] + Decimal("2000000000")
    values["net_income"] = values["continuing_ops_after_tax"] + Decimal("3000000000")
    failures = verify_is_ties(values)
    tie_names = {f.tie for f in failures}
    assert any("gross_profit" in t for t in tie_names)
    assert any("operating_income" in t for t in tie_names)  # cascades from broken gross_profit
    assert any("net_income" in t for t in tie_names)
    assert len(failures) == 3


def test_tie_failure_records_filer_computed_and_delta() -> None:
    values = _nvda_fy26_q4_values()
    values["gross_profit"] = Decimal("999999999999")  # absurd
    failures = verify_is_ties(values)
    gp = next(f for f in failures if "gross_profit" in f.tie)
    assert isinstance(gp, TieFailure)
    assert gp.filer == Decimal("999999999999")
    assert gp.computed == values["revenue"] - values["cogs"]
    assert gp.delta == abs(gp.filer - gp.computed)
    assert gp.tolerance >= TOLERANCE_ABSOLUTE


def test_no_ties_checkable_returns_empty() -> None:
    """Empty input yields no failures (all ties skipped)."""
    assert verify_is_ties({}) == []
