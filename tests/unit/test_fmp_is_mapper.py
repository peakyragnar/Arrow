"""Unit tests for the FMP income-statement -> canonical buckets mapper.

Tests the current FMP IS contract, including the NCI-aware net-income
chain. Fixture uses
real NVDA FY2026 Q4 values (period ending 2026-01-25) — see
data/raw/fmp/income-statement/NVDA/quarter.json for provenance.
"""

from __future__ import annotations

from decimal import Decimal

from arrow.normalize.financials.fmp_is_mapper import (
    MappedFact,
    map_income_statement_row,
)


# Real NVDA FY2026 Q4 from the cached FMP response. Trimmed to the fields
# the mapper reads — unmapped fields are irrelevant.
_NVDA_FY26_Q4_ROW: dict = {
    "date": "2026-01-25",
    "period": "Q4",
    "fiscalYear": "2026",
    "symbol": "NVDA",
    "reportedCurrency": "USD",
    "filingDate": "2026-02-25",
    "acceptedDate": "2026-02-25 16:42:19",
    "revenue": 68127000000,
    "costOfRevenue": 17034000000,
    "grossProfit": 51093000000,
    "researchAndDevelopmentExpenses": 5512000000,
    "sellingGeneralAndAdministrativeExpenses": 1282000000,
    "operatingExpenses": 6794000000,
    "operatingIncome": 44299000000,
    "interestIncome": 568000000,
    "interestExpense": 73000000,
    "incomeBeforeTax": 50398000000,
    "incomeTaxExpense": 7438000000,
    "netIncomeFromContinuingOperations": 42960000000,
    "netIncomeFromDiscontinuedOperations": 0,
    "netIncome": 42960000000,
    "eps": 1.77,
    "epsDiluted": 1.76,
    "weightedAverageShsOut": 24304000000,
    "weightedAverageShsOutDil": 24432000000,
}


def _by_concept(facts: list[MappedFact]) -> dict[str, MappedFact]:
    return {f.concept: f for f in facts}


def test_maps_all_18_verified_buckets() -> None:
    facts = map_income_statement_row(_NVDA_FY26_Q4_ROW)
    assert len(facts) == 20


def test_usd_magnitudes_carry_usd_unit() -> None:
    facts = _by_concept(map_income_statement_row(_NVDA_FY26_Q4_ROW))
    for concept in [
        "revenue", "cogs", "gross_profit", "rd", "sga", "total_opex",
        "operating_income", "interest_expense", "interest_income",
        "ebt_incl_unusual", "tax", "continuing_ops_after_tax",
        "discontinued_ops", "net_income",
        "net_income_attributable_to_parent", "minority_interest",
    ]:
        assert facts[concept].unit == "USD", f"{concept} should be USD"


def test_eps_uses_usd_per_share_unit() -> None:
    facts = _by_concept(map_income_statement_row(_NVDA_FY26_Q4_ROW))
    assert facts["eps_basic"].unit == "USD/share"
    assert facts["eps_diluted"].unit == "USD/share"


def test_shares_use_shares_unit_and_absolute_value() -> None:
    facts = _by_concept(map_income_statement_row(_NVDA_FY26_Q4_ROW))
    assert facts["shares_basic_weighted_avg"].unit == "shares"
    assert facts["shares_basic_weighted_avg"].value == Decimal("24304000000")
    assert facts["shares_diluted_weighted_avg"].unit == "shares"


def test_values_match_fmp_fields_exactly() -> None:
    facts = _by_concept(map_income_statement_row(_NVDA_FY26_Q4_ROW))
    expected: dict[str, Decimal] = {
        "revenue":                     Decimal("68127000000"),
        "cogs":                        Decimal("17034000000"),
        "gross_profit":                Decimal("51093000000"),
        "rd":                          Decimal("5512000000"),
        "sga":                         Decimal("1282000000"),
        "total_opex":                  Decimal("6794000000"),
        "operating_income":            Decimal("44299000000"),
        "interest_income":             Decimal("568000000"),
        "interest_expense":            Decimal("73000000"),
        "ebt_incl_unusual":            Decimal("50398000000"),
        "tax":                         Decimal("7438000000"),
        "continuing_ops_after_tax":    Decimal("42960000000"),
        "discontinued_ops":            Decimal("0"),
        "net_income":                  Decimal("42960000000"),
        "net_income_attributable_to_parent": Decimal("42960000000"),
        "minority_interest":           Decimal("0"),
        "eps_basic":                   Decimal("1.77"),
        "eps_diluted":                 Decimal("1.76"),
        "shares_basic_weighted_avg":   Decimal("24304000000"),
        "shares_diluted_weighted_avg": Decimal("24432000000"),
    }
    for concept, expected_value in expected.items():
        assert facts[concept].value == expected_value, (
            f"{concept}: got {facts[concept].value}, want {expected_value}"
        )


def test_missing_fmp_fields_are_skipped_not_nulled() -> None:
    """Absent fields don't produce rows (financial_facts.value is NOT NULL)."""
    row = dict(_NVDA_FY26_Q4_ROW)
    del row["researchAndDevelopmentExpenses"]
    del row["eps"]
    facts = _by_concept(map_income_statement_row(row))
    assert "rd" not in facts
    assert "eps_basic" not in facts
    # Other buckets still present.
    assert "revenue" in facts
    assert "net_income" in facts


def test_none_values_are_skipped() -> None:
    row = dict(_NVDA_FY26_Q4_ROW)
    row["sellingGeneralAndAdministrativeExpenses"] = None
    facts = _by_concept(map_income_statement_row(row))
    assert "sga" not in facts


def test_verified_ties_hold_on_real_nvda_row() -> None:
    """Sanity: the canonical output of the mapper should tie on the four
    Layer 1 IS subtotals — otherwise the mapping is broken."""
    facts = _by_concept(map_income_statement_row(_NVDA_FY26_Q4_ROW))
    assert facts["gross_profit"].value == facts["revenue"].value - facts["cogs"].value
    assert (
        facts["operating_income"].value
        == facts["gross_profit"].value - facts["total_opex"].value
    )
    assert (
        facts["continuing_ops_after_tax"].value
        == facts["ebt_incl_unusual"].value - facts["tax"].value
    )
    assert (
        facts["net_income"].value
        == facts["continuing_ops_after_tax"].value + facts["discontinued_ops"].value
    )
