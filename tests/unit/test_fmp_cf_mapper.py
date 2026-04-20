"""Unit tests for FMP cash-flow → canonical bucket mapper.

Fixture: real NVDA FY2026 Q4 CF values (discrete, cash-impact signs).
All three subtotal ties + cash roll-forward hold on the mapped output.
"""

from __future__ import annotations

from decimal import Decimal

from arrow.normalize.financials.fmp_cf_mapper import (
    MappedFact,
    map_cash_flow_row,
)


_NVDA_FY26_Q4_CF: dict = {
    "date": "2026-01-25", "period": "Q4", "fiscalYear": "2026",
    "netIncome": 42960000000,
    "depreciationAndAmortization": 812000000,
    "stockBasedCompensation": 1633000000,
    "deferredIncomeTax": 611000000,
    "otherNonCashItems": 6121000000,
    "accountsReceivables": -5074000000,
    "inventory": -1621000000,
    "accountsPayables": 1064000000,
    "otherWorkingCapital": -10318000000,
    "netCashProvidedByOperatingActivities": 36188000000,
    "investmentsInPropertyPlantAndEquipment": -1284000000,
    "acquisitionsNet": -165000000,
    "purchasesOfInvestments": -33340000000,
    "salesMaturitiesOfInvestments": 16928000000,
    "otherInvestingActivities": -13000000000,
    "netCashProvidedByInvestingActivities": -30861000000,
    "shortTermNetDebtIssuance": 0,
    "longTermNetDebtIssuance": 0,
    "commonStockIssuance": 0,
    "commonStockRepurchased": -3815000000,
    "commonDividendsPaid": -242000000,
    "preferredDividendsPaid": 0,
    "otherFinancingActivities": -2151000000,
    "netCashProvidedByFinancingActivities": -6208000000,
    "effectOfForexChangesOnCash": 0,
    "netChangeInCash": -881000000,
    "cashAtBeginningOfPeriod": 11486000000,
    "cashAtEndOfPeriod": 10605000000,
}


def _by_concept(facts: list[MappedFact]) -> dict[str, MappedFact]:
    return {f.concept: f for f in facts}


def test_all_expected_cf_buckets_emitted() -> None:
    facts = _by_concept(map_cash_flow_row(_NVDA_FY26_Q4_CF))
    expected = {
        "net_income_start",
        "dna_cf", "sbc", "deferred_income_tax", "other_noncash",
        "change_accounts_receivable", "change_inventory",
        "change_accounts_payable", "change_other_working_capital",
        "cfo",
        "capital_expenditures", "acquisitions",
        "purchases_of_investments", "sales_of_investments",
        "other_investing", "cfi",
        "short_term_debt_issuance", "long_term_debt_issuance",
        "stock_issuance", "stock_repurchase",
        "common_dividends_paid", "preferred_dividends_paid",
        "other_financing", "cff",
        "fx_effect_on_cash",
        "net_change_in_cash",
        "cash_begin_of_period", "cash_end_of_period",
    }
    assert set(facts.keys()) == expected


def test_signs_preserved_cash_impact() -> None:
    """FMP returns cash-impact signs; mapper stores as-is."""
    facts = _by_concept(map_cash_flow_row(_NVDA_FY26_Q4_CF))
    assert facts["capital_expenditures"].value == Decimal("-1284000000")  # negative
    assert facts["stock_repurchase"].value == Decimal("-3815000000")      # negative
    assert facts["common_dividends_paid"].value == Decimal("-242000000")  # negative
    assert facts["change_accounts_receivable"].value == Decimal("-5074000000")  # AR up
    assert facts["change_accounts_payable"].value == Decimal("1064000000")      # AP up


def test_cfo_tie_holds_on_mapped_values() -> None:
    facts = _by_concept(map_cash_flow_row(_NVDA_FY26_Q4_CF))
    # net_income_start + adjustments + working-capital changes → cfo
    computed = (
        facts["net_income_start"].value
        + facts["dna_cf"].value
        + facts["sbc"].value
        + facts["deferred_income_tax"].value
        + facts["other_noncash"].value
        + facts["change_accounts_receivable"].value
        + facts["change_inventory"].value
        + facts["change_accounts_payable"].value
        + facts["change_other_working_capital"].value
    )
    assert computed == facts["cfo"].value


def test_cash_roll_forward_ties() -> None:
    facts = _by_concept(map_cash_flow_row(_NVDA_FY26_Q4_CF))
    # cashEnd = cashBegin + netChangeInCash
    assert (
        facts["cash_end_of_period"].value
        == facts["cash_begin_of_period"].value + facts["net_change_in_cash"].value
    )


def test_net_change_equals_subtotal_sum() -> None:
    facts = _by_concept(map_cash_flow_row(_NVDA_FY26_Q4_CF))
    # net_change_in_cash = cfo + cfi + cff + fx
    assert (
        facts["net_change_in_cash"].value
        == facts["cfo"].value
        + facts["cfi"].value
        + facts["cff"].value
        + facts["fx_effect_on_cash"].value
    )
