"""Unit tests for Layer 1 BS subtotal-tie + balance-identity verification."""

from __future__ import annotations

from decimal import Decimal

from arrow.normalize.financials.verify_bs import (
    TieFailure,
    verify_bs_hard_ties,
    verify_bs_soft_ties,
    verify_bs_ties,
)


def _nvda_fy26_q4_values() -> dict[str, Decimal]:
    """Minimum set of canonical BS buckets that tie for NVDA FY2026 Q4."""
    return {
        "cash_and_equivalents": Decimal("10605000000"),
        "short_term_investments": Decimal("51951000000"),
        "accounts_receivable": Decimal("38466000000"),
        "inventory": Decimal("21403000000"),
        "prepaid_expenses": Decimal("0"),
        "other_current_assets": Decimal("3180000000"),
        "total_current_assets": Decimal("125605000000"),
        "net_ppe": Decimal("13250000000"),
        "long_term_investments": Decimal("22251000000"),
        "goodwill": Decimal("20832000000"),
        "other_intangibles": Decimal("3306000000"),
        "deferred_tax_assets_noncurrent": Decimal("13258000000"),
        "other_noncurrent_assets": Decimal("8301000000"),
        "total_assets": Decimal("206803000000"),
        "accounts_payable": Decimal("12481000000"),  # accountPayables + otherPayables
        "accrued_expenses": Decimal("9239000000"),
        "current_portion_lt_debt": Decimal("999000000"),
        "current_portion_leases_operating": Decimal("372000000"),
        "deferred_revenue_current": Decimal("1379000000"),
        "other_current_liabilities": Decimal("7693000000"),
        "total_current_liabilities": Decimal("32163000000"),
        "long_term_debt": Decimal("7469000000"),
        "long_term_leases_operating": Decimal("2572000000"),
        "deferred_revenue_noncurrent": Decimal("1193000000"),
        "deferred_tax_liability_noncurrent": Decimal("1774000000"),
        "other_noncurrent_liabilities": Decimal("4339000000"),
        "total_liabilities": Decimal("49510000000"),
        "preferred_stock": Decimal("0"),
        "common_stock": Decimal("24000000"),
        "additional_paid_in_capital": Decimal("10118000000"),
        "retained_earnings": Decimal("146973000000"),
        "treasury_stock": Decimal("0"),
        "accumulated_other_comprehensive_income": Decimal("178000000"),
        "noncontrolling_interest": Decimal("0"),
        "total_equity": Decimal("157293000000"),
        "total_liabilities_and_equity": Decimal("206803000000"),
    }


def test_real_nvda_row_passes_all_bs_ties() -> None:
    assert verify_bs_ties(_nvda_fy26_q4_values()) == []


def test_balance_identity_failure_surfaces() -> None:
    """Breaking total_equity component tree so the equity subtotal + the
    liab+equity tie don't match anymore. At least one tie fails."""
    values = _nvda_fy26_q4_values()
    values["total_equity"] = values["total_equity"] + Decimal("2000000000")
    failures = verify_bs_ties(values)
    tie_names = {f.tie for f in failures}
    # The total_equity component-sum tie catches broken equity,
    # AND the total_liab_and_equity tie catches the resulting mismatch.
    assert any(
        "total_liabilities_and_equity" in t or "total_equity" in t
        for t in tie_names
    )
    assert len(failures) >= 1


def test_breaking_total_liabilities_and_equity_fails_balance_identity() -> None:
    """Directly breaking total_liabilities_and_equity so it doesn't
    match total_assets must fail the balance identity."""
    values = _nvda_fy26_q4_values()
    values["total_liabilities_and_equity"] = values["total_assets"] + Decimal("10000000000")
    failures = verify_bs_ties(values)
    tie_names = {f.tie for f in failures}
    assert any("total_assets == total_liabilities_and_equity" in t for t in tie_names)


def test_mapping_gap_surfaces_as_current_assets_failure() -> None:
    """If we forgot to map inventory (20B+), the current-assets tie
    should fail by the missing amount. This protects against silent
    mapping incompleteness."""
    values = _nvda_fy26_q4_values()
    del values["inventory"]  # simulate mapper gap
    failures = verify_bs_ties(values)
    failed_ties = {f.tie for f in failures}
    assert any("total_current_assets" in t for t in failed_ties)


def test_absent_optional_components_treated_as_zero() -> None:
    """BS has many optional components (restricted_cash_current, etc.).
    Their absence shouldn't fail ties — they contribute 0 to the sum."""
    values = _nvda_fy26_q4_values()
    # These are legitimately absent from FMP's NVDA BS; verify ties still pass.
    assert "restricted_cash_current" not in values
    assert "other_receivables" not in values
    assert "income_taxes_receivable_current" not in values
    assert "right_of_use_assets_operating" not in values
    assert "equity_method_investments" not in values
    assert "short_term_borrowings" not in values
    assert "current_portion_leases_finance" not in values
    assert "income_taxes_payable_current" not in values
    assert "long_term_leases_finance" not in values
    assert "common_stock_and_apic" not in values
    assert verify_bs_ties(values) == []


def test_tolerance_accepts_small_rounding() -> None:
    """Within $1M, ties pass (same tolerance as Layer 1 IS)."""
    values = _nvda_fy26_q4_values()
    values["total_assets"] = values["total_assets"] + Decimal("500000")
    assert verify_bs_ties(values) == []


def test_tolerance_fails_beyond_threshold() -> None:
    """$50M off on a $200B total → exceeds max($1M, 0.1% × 206B ≈ $207M).
    Wait, 50M is less than 207M. Use $500M instead."""
    values = _nvda_fy26_q4_values()
    values["total_equity"] = values["total_equity"] + Decimal("500000000")
    failures = verify_bs_ties(values)
    assert len(failures) >= 1


def test_tie_failure_records_filer_computed_and_delta() -> None:
    values = _nvda_fy26_q4_values()
    # Break total_current_liabilities by 10B → clearly beyond tolerance.
    values["total_current_liabilities"] = values["total_current_liabilities"] + Decimal("10000000000")
    failures = verify_bs_ties(values)
    tc = next(f for f in failures if "total_current_liabilities" in f.tie)
    assert isinstance(tc, TieFailure)
    assert tc.delta == Decimal("10000000000")


def test_subtotal_component_drift_is_soft_not_hard() -> None:
    values = _nvda_fy26_q4_values()
    values["total_current_assets"] = values["total_current_assets"] + Decimal("200000000")
    soft = verify_bs_soft_ties(values)
    hard = verify_bs_hard_ties(values)
    assert any("total_current_assets" in f.tie for f in soft)
    assert hard == []


def test_balance_identity_failure_is_hard() -> None:
    values = _nvda_fy26_q4_values()
    values["total_assets"] = values["total_assets"] + Decimal("10000000000")
    hard = verify_bs_hard_ties(values)
    assert any("total_assets == total_liabilities_and_equity" in f.tie for f in hard)


def test_mu_fy2020_q3_vendor_inconsistency_is_soft() -> None:
    """FMP populated totalLiabilitiesAndTotalEquity inconsistently for MU
    FY2020 Q3: it equals totalLiabilities + stockholders' equity (excl. NCI)
    rather than totalLiabilities + totalEquity (incl. NCI). The 98M delta is
    the noncontrolling interest from Micron's IM Flash JV with Intel.

    Real balance identity (TA == TLE) still holds, so ingest must NOT abort.
    The TLE == TL + TE check fires as a soft flag → routes to steward.
    """
    values: dict[str, Decimal] = {
        "total_assets": Decimal("52005000000"),
        "total_liabilities": Decimal("14185000000"),
        "total_equity": Decimal("37918000000"),  # includes NCI of 98M
        "noncontrolling_interest": Decimal("98000000"),
        "total_liabilities_and_equity": Decimal("52005000000"),  # FMP excluded NCI here
    }
    hard = verify_bs_hard_ties(values)
    soft = verify_bs_soft_ties(values)
    assert hard == [], f"balance identity must still hold; got {hard}"
    assert any(
        "total_liabilities_and_equity == total_liabilities + total_equity" in f.tie
        for f in soft
    ), f"vendor-consistency tie should fire as soft; got {soft}"
