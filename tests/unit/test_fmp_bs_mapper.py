"""Unit tests for FMP balance-sheet → canonical bucket mapper.

Fixture: real NVDA FY2026 Q4 BS values. All filer-reported subtotals
tie within tolerance on this row (verified separately by the BS Layer 1
verifier).
"""

from __future__ import annotations

from decimal import Decimal

from arrow.normalize.financials.fmp_bs_mapper import (
    MappedFact,
    map_balance_sheet_row,
)


_NVDA_FY26_Q4_BS: dict = {
    "date": "2026-01-25", "period": "Q4", "fiscalYear": "2026",
    "symbol": "NVDA",
    "cashAndCashEquivalents": 10605000000,
    "shortTermInvestments": 51951000000,
    "accountsReceivables": 38466000000,
    "inventory": 21403000000,
    "prepaids": 0,
    "otherCurrentAssets": 3180000000,
    "totalCurrentAssets": 125605000000,
    "propertyPlantEquipmentNet": 13250000000,
    "longTermInvestments": 22251000000,
    "goodwill": 20832000000,
    "intangibleAssets": 3306000000,
    "taxAssets": 13258000000,
    "otherNonCurrentAssets": 8301000000,
    "totalAssets": 206803000000,
    "accountPayables": 9812000000,
    "otherPayables": 2669000000,
    "accruedExpenses": 9239000000,
    "shortTermDebt": 999000000,
    "capitalLeaseObligationsCurrent": 372000000,
    "deferredRevenue": 1379000000,
    "otherCurrentLiabilities": 7693000000,
    "totalCurrentLiabilities": 32163000000,
    "longTermDebt": 7469000000,
    "capitalLeaseObligationsNonCurrent": 2572000000,
    "deferredRevenueNonCurrent": 1193000000,
    "deferredTaxLiabilitiesNonCurrent": 1774000000,
    "otherNonCurrentLiabilities": 4339000000,
    "totalLiabilities": 49510000000,
    "preferredStock": 0,
    "commonStock": 24000000,
    "additionalPaidInCapital": 10118000000,
    "retainedEarnings": 146973000000,
    "treasuryStock": 0,
    "accumulatedOtherComprehensiveIncomeLoss": 178000000,
    "minorityInterest": 0,
    "totalEquity": 157293000000,
    "totalLiabilitiesAndTotalEquity": 206803000000,
}


def _by_concept(facts: list[MappedFact]) -> dict[str, MappedFact]:
    return {f.concept: f for f in facts}


def test_accounts_payable_bundles_account_plus_other_payables() -> None:
    """FMP splits payables into trade (accountPayables) and non-trade
    (otherPayables). Our canonical accounts_payable bundles both so the
    current-liabilities tie holds."""
    facts = _by_concept(map_balance_sheet_row(_NVDA_FY26_Q4_BS))
    ap = facts["accounts_payable"]
    assert ap.value == Decimal("9812000000") + Decimal("2669000000")  # 12481000000
    assert ap.unit == "USD"


def test_balance_identity_holds_on_mapped_values() -> None:
    """Sanity: total_assets == total_liab + total_equity on the fixture."""
    facts = _by_concept(map_balance_sheet_row(_NVDA_FY26_Q4_BS))
    assert (
        facts["total_assets"].value
        == facts["total_liabilities"].value + facts["total_equity"].value
    )


def test_current_assets_sum_ties_on_mapped_values() -> None:
    facts = _by_concept(map_balance_sheet_row(_NVDA_FY26_Q4_BS))
    computed = (
        facts["cash_and_equivalents"].value
        + facts["short_term_investments"].value
        + facts["accounts_receivable"].value
        + facts["inventory"].value
        + facts["prepaid_expenses"].value
        + facts["other_current_assets"].value
    )
    assert computed == facts["total_current_assets"].value


def test_equity_sum_ties() -> None:
    """preferred + common + APIC + retained + treasury + AOCI + NCI.
    Treasury is ADDED (not subtracted) because FMP stores it with its
    signed value (negative for buybacks; 0 for NVDA FY26 Q4)."""
    facts = _by_concept(map_balance_sheet_row(_NVDA_FY26_Q4_BS))
    computed = (
        facts["preferred_stock"].value
        + facts["common_stock"].value
        + facts["additional_paid_in_capital"].value
        + facts["treasury_stock"].value
        + facts["retained_earnings"].value
        + facts["accumulated_other_comprehensive_income"].value
        + facts["noncontrolling_interest"].value
    )
    assert computed == facts["total_equity"].value


def test_treasury_stock_signed_negative_is_preserved() -> None:
    """FMP returns treasuryStock as SIGNED NEGATIVE for buybacks. Our
    mapper must preserve the sign — NOT convert to positive magnitude.
    The BS equity tie depends on this."""
    row = dict(_NVDA_FY26_Q4_BS)
    row["treasuryStock"] = -12038000000  # NVDA FY2022 Q3's actual value
    # Re-balance equity so the row is self-consistent: totalEquity adjusts.
    # (We're only testing sign preservation in the mapper, not the full tie.)
    facts = _by_concept(map_balance_sheet_row(row))
    assert facts["treasury_stock"].value == Decimal("-12038000000")


def test_bucket_with_zero_value_is_emitted_not_dropped() -> None:
    """A 0-valued preferred_stock is meaningful filer data (NVDA has no
    preferred). We emit it; we don't drop it as 'absent'."""
    facts = _by_concept(map_balance_sheet_row(_NVDA_FY26_Q4_BS))
    assert facts["preferred_stock"].value == Decimal("0")
    assert facts["treasury_stock"].value == Decimal("0")


def test_bucket_with_all_fields_missing_is_skipped() -> None:
    """If every FMP source field for a bucket is absent from the row,
    we emit nothing for that bucket (no 0 from nothing)."""
    row = dict(_NVDA_FY26_Q4_BS)
    del row["cashAndCashEquivalents"]
    facts = _by_concept(map_balance_sheet_row(row))
    assert "cash_and_equivalents" not in facts


def test_all_expected_buckets_emitted_on_complete_nvda_fixture() -> None:
    facts = _by_concept(map_balance_sheet_row(_NVDA_FY26_Q4_BS))
    expected = {
        "cash_and_equivalents", "short_term_investments", "accounts_receivable",
        "inventory", "prepaid_expenses", "other_current_assets",
        "total_current_assets",
        "net_ppe", "long_term_investments", "goodwill", "other_intangibles",
        "deferred_tax_assets_noncurrent", "other_noncurrent_assets",
        "total_assets",
        "accounts_payable", "accrued_expenses", "current_portion_lt_debt",
        "current_portion_leases_operating", "deferred_revenue_current",
        "other_current_liabilities", "total_current_liabilities",
        "long_term_debt", "long_term_leases_operating",
        "deferred_revenue_noncurrent", "deferred_tax_liability_noncurrent",
        "other_noncurrent_liabilities", "total_liabilities",
        "preferred_stock", "common_stock", "additional_paid_in_capital",
        "retained_earnings", "treasury_stock",
        "accumulated_other_comprehensive_income",
        "noncontrolling_interest", "total_equity", "total_liabilities_and_equity",
    }
    assert set(facts.keys()) == expected
