"""Unit tests for fiscal + calendar period derivation.

Per docs/reference/periods.md § 3.2 (fiscal) and § 4 (calendar).
Explicit coverage for NVDA's 52/53-week calendar (the case the anchor
fix in migration 009 was about).
"""

from __future__ import annotations

from datetime import date

import pytest

from arrow.normalize.periods.derive import (
    CalendarPeriod,
    FiscalPeriod,
    derive_calendar_period,
    derive_fiscal_period,
    parse_fiscal_year_end_md,
)


# ---------------------------------------------------------------------------
# Anchor parsing
# ---------------------------------------------------------------------------


def test_parse_fiscal_year_end_md_valid() -> None:
    assert parse_fiscal_year_end_md("01-31") == (1, 31)
    assert parse_fiscal_year_end_md("06-30") == (6, 30)
    assert parse_fiscal_year_end_md("12-31") == (12, 31)


@pytest.mark.parametrize("bad", ["1-31", "01-3", "13-01", "00-15", "01/31", "Jan-31", "", "01-32"])
def test_parse_fiscal_year_end_md_rejects_bad_formats(bad: str) -> None:
    with pytest.raises(ValueError):
        parse_fiscal_year_end_md(bad)


# ---------------------------------------------------------------------------
# Fiscal derivation: NVDA (01-31 anchor, 52/53-week calendar)
# ---------------------------------------------------------------------------


def test_nvda_fy2025_q4_ended_jan_26_2025() -> None:
    """FY2025 Q4: last Sunday of January 2025. period_end < anchor -> same year."""
    p = derive_fiscal_period(date(2025, 1, 26), "01-31", period_type="quarter")
    assert p.fiscal_year == 2025
    assert p.fiscal_quarter == 4
    assert p.fiscal_period_label == "FY2025 Q4"
    assert p.period_type == "quarter"


def test_nvda_fy2024_q4_ended_jan_28_2024() -> None:
    """FY2024 Q4 ended Jan 28, 2024. With anchor=01-31, (1,28) < (1,31) -> FY2024.

    This is the test that would FAIL if the anchor were the stale 01-26 value
    the old docs example had — (1,28) > (1,26) would push to FY2025.
    """
    p = derive_fiscal_period(date(2024, 1, 28), "01-31", period_type="quarter")
    assert p.fiscal_year == 2024
    assert p.fiscal_quarter == 4


def test_nvda_fy2025_q1_ended_apr_28_2024() -> None:
    """FY2025 Q1 ended Apr 28, 2024. period_end > anchor -> year+1."""
    p = derive_fiscal_period(date(2024, 4, 28), "01-31", period_type="quarter")
    assert p.fiscal_year == 2025
    assert p.fiscal_quarter == 1


def test_nvda_fy2025_q2_ended_jul_28_2024() -> None:
    p = derive_fiscal_period(date(2024, 7, 28), "01-31", period_type="quarter")
    assert p.fiscal_year == 2025
    assert p.fiscal_quarter == 2


def test_nvda_fy2025_q3_ended_oct_27_2024() -> None:
    p = derive_fiscal_period(date(2024, 10, 27), "01-31", period_type="quarter")
    assert p.fiscal_year == 2025
    assert p.fiscal_quarter == 3


# 52/53-week drift cases — the bug fixed by migrating to the
# subtract-7-days algorithm. These are real NVDA period_ends where the
# quarter-end Sunday drifted past the nominal calendar month-end.


def test_nvda_fy2000_q2_drifted_to_aug_1() -> None:
    """NVDA Q2 FY2000 ended 1999-08-01 (last Sunday of July rolled into August).
    Old algorithm misclassified this as Q3. Subtract-7-days correctly
    anchors it to the July content month.
    """
    p = derive_fiscal_period(date(1999, 8, 1), "01-31", period_type="quarter")
    assert p.fiscal_year == 2000
    assert p.fiscal_quarter == 2
    assert p.fiscal_period_label == "FY2000 Q2"


def test_nvda_fy2006_q1_drifted_to_may_1() -> None:
    p = derive_fiscal_period(date(2005, 5, 1), "01-31", period_type="quarter")
    assert p.fiscal_year == 2006
    assert p.fiscal_quarter == 1


def test_nvda_fy2011_q1_drifted_to_may_2() -> None:
    """Two-day drift: May 2. Subtract-1-day would still misclassify this."""
    p = derive_fiscal_period(date(2010, 5, 2), "01-31", period_type="quarter")
    assert p.fiscal_year == 2011
    assert p.fiscal_quarter == 1


def test_nvda_fy2011_q2_drifted_to_aug_1() -> None:
    p = derive_fiscal_period(date(2010, 8, 1), "01-31", period_type="quarter")
    assert p.fiscal_year == 2011
    assert p.fiscal_quarter == 2


def test_nvda_fy2017_q1_drifted_to_may_1() -> None:
    p = derive_fiscal_period(date(2016, 5, 1), "01-31", period_type="quarter")
    assert p.fiscal_year == 2017
    assert p.fiscal_quarter == 1


def test_nvda_fy2022_q2_drifted_to_aug_1() -> None:
    p = derive_fiscal_period(date(2021, 8, 1), "01-31", period_type="quarter")
    assert p.fiscal_year == 2022
    assert p.fiscal_quarter == 2


def test_nvda_fy2023_q1_drifted_to_may_1() -> None:
    p = derive_fiscal_period(date(2022, 5, 1), "01-31", period_type="quarter")
    assert p.fiscal_year == 2023
    assert p.fiscal_quarter == 1


def test_nvda_drift_does_not_affect_canonical_late_month_cases() -> None:
    """Subtract-7-days must be a no-op on canonical late-month period_ends."""
    # Jul 31 canonical vs Aug 1 drifted — both must produce FY2000 Q2.
    canonical = derive_fiscal_period(date(1999, 7, 31), "01-31")
    drifted = derive_fiscal_period(date(1999, 8, 1), "01-31")
    assert canonical.fiscal_period_label == drifted.fiscal_period_label == "FY2000 Q2"


def test_nvda_annual_returns_null_quarter_and_bare_label() -> None:
    p = derive_fiscal_period(date(2025, 1, 26), "01-31", period_type="annual")
    assert p.fiscal_year == 2025
    assert p.fiscal_quarter is None
    assert p.fiscal_period_label == "FY2025"
    assert p.period_type == "annual"


# ---------------------------------------------------------------------------
# Fiscal derivation: MSFT (06-30 anchor, calendar-exact)
# ---------------------------------------------------------------------------


def test_msft_fy2024_q4_ended_jun_30_2024() -> None:
    """Exact-match period_end case: (6,30) NOT > (6,30) -> same year."""
    p = derive_fiscal_period(date(2024, 6, 30), "06-30", period_type="quarter")
    assert p.fiscal_year == 2024
    assert p.fiscal_quarter == 4


def test_msft_fy2024_q1_ended_sep_30_2023() -> None:
    """FY starts Jul 1. period_end Sep 30 -> 3 months elapsed -> Q1."""
    p = derive_fiscal_period(date(2023, 9, 30), "06-30", period_type="quarter")
    assert p.fiscal_year == 2024
    assert p.fiscal_quarter == 1


def test_msft_fy2024_q3_ended_mar_31_2024() -> None:
    p = derive_fiscal_period(date(2024, 3, 31), "06-30", period_type="quarter")
    assert p.fiscal_year == 2024
    assert p.fiscal_quarter == 3


# ---------------------------------------------------------------------------
# Fiscal derivation: calendar-year filer (12-31 anchor)
# ---------------------------------------------------------------------------


def test_pltr_fy2024_q4_ended_dec_31_2024() -> None:
    p = derive_fiscal_period(date(2024, 12, 31), "12-31", period_type="quarter")
    assert p.fiscal_year == 2024
    assert p.fiscal_quarter == 4


def test_pltr_fy2024_q1_ended_mar_31_2024() -> None:
    p = derive_fiscal_period(date(2024, 3, 31), "12-31", period_type="quarter")
    assert p.fiscal_year == 2024
    assert p.fiscal_quarter == 1


# ---------------------------------------------------------------------------
# period_type validation
# ---------------------------------------------------------------------------


def test_bad_period_type_raises() -> None:
    with pytest.raises(ValueError):
        derive_fiscal_period(date(2024, 6, 30), "06-30", period_type="stub")


# ---------------------------------------------------------------------------
# Calendar derivation (pure function of period_end)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "period_end,expected",
    [
        (date(2024, 1, 28),  CalendarPeriod(2024, 1, "CY2024 Q1")),
        (date(2024, 4, 28),  CalendarPeriod(2024, 2, "CY2024 Q2")),
        (date(2024, 6, 30),  CalendarPeriod(2024, 2, "CY2024 Q2")),
        (date(2024, 7, 28),  CalendarPeriod(2024, 3, "CY2024 Q3")),
        (date(2024, 9, 30),  CalendarPeriod(2024, 3, "CY2024 Q3")),
        (date(2024, 10, 27), CalendarPeriod(2024, 4, "CY2024 Q4")),
        (date(2024, 12, 31), CalendarPeriod(2024, 4, "CY2024 Q4")),
        (date(2025, 1, 26),  CalendarPeriod(2025, 1, "CY2025 Q1")),
    ],
)
def test_calendar_period(period_end: date, expected: CalendarPeriod) -> None:
    assert derive_calendar_period(period_end) == expected
