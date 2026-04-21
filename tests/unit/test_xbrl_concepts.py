"""Unit tests for the canonical-to-XBRL concept mapping."""

from __future__ import annotations

from arrow.reconcile.xbrl_concepts import (
    all_is_mappings,
    mapping_for,
)


def test_mapping_for_known_bucket_returns_mapping() -> None:
    m = mapping_for("revenue")
    assert m is not None
    assert m.canonical == "revenue"
    assert "Revenues" in m.xbrl_tags
    assert m.unit == "USD"


def test_mapping_for_eps_is_usd_per_share() -> None:
    m = mapping_for("eps_diluted")
    assert m.unit == "USD/shares"
    assert "EarningsPerShareDiluted" in m.xbrl_tags


def test_mapping_for_shares_uses_shares_unit() -> None:
    m = mapping_for("shares_basic_weighted_avg")
    assert m.unit == "shares"


def test_mapping_for_unknown_bucket_returns_none() -> None:
    assert mapping_for("not_a_real_bucket") is None
    assert mapping_for("") is None


def test_all_mappings_cover_the_current_is_buckets() -> None:
    expected = {
        "revenue", "cogs", "gross_profit", "rd", "sga", "total_opex",
        "operating_income", "interest_expense", "interest_income",
        "ebt_incl_unusual", "tax", "continuing_ops_after_tax",
        "discontinued_ops", "net_income",
        "net_income_attributable_to_parent", "minority_interest",
        "eps_basic", "eps_diluted",
        "shares_basic_weighted_avg", "shares_diluted_weighted_avg",
    }
    got = {m.canonical for m in all_is_mappings()}
    assert got == expected, f"mapping set drifted from fmp_mapping.md §5.1: {got ^ expected}"


def test_revenue_mapping_covers_both_pre_and_post_asc606_tags() -> None:
    """NVDA switched from 'Revenues' to
    'RevenueFromContractWithCustomerExcludingAssessedTax' after ASC 606
    adoption. Both tags must resolve."""
    m = mapping_for("revenue")
    assert "Revenues" in m.xbrl_tags
    assert "RevenueFromContractWithCustomerExcludingAssessedTax" in m.xbrl_tags
