"""Unit tests for CF subtotal-tie verification.

The FMP-source path treats every CF tie as vendor-consistency (soft);
hard ties are empty until SEC XBRL direct ingest provides a single
internally-consistent source.
"""

from __future__ import annotations

from decimal import Decimal

from arrow.normalize.financials.verify_cf import (
    verify_cf_hard_ties,
    verify_cf_soft_ties,
)


def test_mu_fy2022_q3_cash_rollforward_inconsistency_is_soft() -> None:
    """FMP shipped MU FY2022 Q3 with begin/end values that don't chain
    to prior-period close: Q2 ended 9,224M but Q3 begins 9,116M. Within
    the row itself, cashEnd - cashBegin = 41M while netChange = 37M
    (4M delta, exactly the unaccounted gap from FMP's own bucketing).

    Filer's actual 10-Q ties; defect is FMP normalization. Must NOT
    abort ingest — flag and route to steward.
    """
    values: dict[str, Decimal] = {
        "net_change_in_cash": Decimal("37000000"),
        "cash_begin_of_period": Decimal("9116000000"),
        "cash_end_of_period": Decimal("9157000000"),
        "cfo": Decimal("3838000000"),
        "cfi": Decimal("-2585000000"),
        "cff": Decimal("-1161000000"),
        "fx_effect_on_cash": Decimal("-55000000"),
    }
    hard = verify_cf_hard_ties(values)
    soft = verify_cf_soft_ties(values)
    assert hard == [], f"hard ties must not fire on FMP vendor inconsistency; got {hard}"
    assert any(
        "cash_end_of_period - cash_begin_of_period" in f.tie for f in soft
    ), f"cash-roll-forward should fire as soft; got {soft}"


def test_mu_fy2017_q4_q4_derivation_inconsistency_is_soft() -> None:
    """FMP derives Q4 by subtraction (FY − 9M YTD) and gets it wrong for
    MU FY2017 Q4: netChangeInCash = 5,109M but cashEnd - cashBegin =
    5,216M - 4,048M = 1,168M (-3,941M delta). Section sum also tied
    to 5,109M, so both soft ties fire.
    """
    values: dict[str, Decimal] = {
        "net_change_in_cash": Decimal("5109000000"),
        "cash_begin_of_period": Decimal("4048000000"),
        "cash_end_of_period": Decimal("5216000000"),
    }
    hard = verify_cf_hard_ties(values)
    soft = verify_cf_soft_ties(values)
    assert hard == []
    assert any(
        "cash_end_of_period - cash_begin_of_period" in f.tie for f in soft
    )


def test_clean_cf_row_passes_all_ties() -> None:
    """Real NVDA FY26 Q4 — section subtotals + cash roll-forward all tie."""
    values: dict[str, Decimal] = {
        "net_income_start": Decimal("42960000000"),
        "dna_cf": Decimal("812000000"),
        "sbc": Decimal("1633000000"),
        "deferred_income_tax": Decimal("611000000"),
        "other_noncash": Decimal("6121000000"),
        "change_accounts_receivable": Decimal("-5074000000"),
        "change_inventory": Decimal("-1621000000"),
        "change_accounts_payable": Decimal("1064000000"),
        "change_other_working_capital": Decimal("-10318000000"),
        "cfo": Decimal("36188000000"),
        "capital_expenditures": Decimal("-1284000000"),
        "acquisitions": Decimal("-165000000"),
        "purchases_of_investments": Decimal("-9943000000"),
        "sales_of_investments": Decimal("4148000000"),
        "other_investing": Decimal("0"),
        "cfi": Decimal("-7244000000"),
        "short_term_debt_issuance": Decimal("0"),
        "long_term_debt_issuance": Decimal("0"),
        "stock_issuance": Decimal("0"),
        "stock_repurchase": Decimal("-9982000000"),
        "common_dividends_paid": Decimal("-244000000"),
        "preferred_dividends_paid": Decimal("0"),
        "other_financing": Decimal("-2065000000"),
        "cff": Decimal("-12291000000"),
        "fx_effect_on_cash": Decimal("0"),
        "net_change_in_cash": Decimal("16653000000"),
        "cash_begin_of_period": Decimal("-6048000000"),
        "cash_end_of_period": Decimal("10605000000"),
    }
    assert verify_cf_hard_ties(values) == []
    assert verify_cf_soft_ties(values) == []
