"""Fiscal + calendar period derivation.

Implements the algorithm in docs/reference/periods.md § 3.2 (fiscal) and
§ 4 (calendar). Used at ingest time to populate the two-clocks columns
on financial_facts (and, later, artifacts / company_events).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from math import ceil

_PERIOD_TYPES = ("quarter", "annual")

# 52/53-week filers (NVDA, AAPL, most retailers) end each fiscal period on a
# specific weekday. The quarter-end day can land up to a week past the
# nominal calendar month-end — e.g. NVDA's FY2000 Q2 period_end was 1999-08-01
# because "Sunday nearest Jul 31" fell on Aug 1 that year. Shifting back one
# week before computing month-based arithmetic maps the period_end to its
# content month (July, not August) without special-casing drift directions.
_DRIFT_WINDOW = timedelta(days=7)


@dataclass(frozen=True)
class FiscalPeriod:
    fiscal_year: int
    fiscal_quarter: int | None  # None for annual
    period_type: str            # "quarter" | "annual"
    fiscal_period_label: str    # "FY2025 Q4" | "FY2025"


@dataclass(frozen=True)
class CalendarPeriod:
    calendar_year: int
    calendar_quarter: int
    calendar_period_label: str  # "CY2025 Q1"


def parse_fiscal_year_end_md(fye: str) -> tuple[int, int]:
    """Parse 'MM-DD' -> (month, day)."""
    if not (len(fye) == 5 and fye[2] == "-" and fye[:2].isdigit() and fye[3:].isdigit()):
        raise ValueError(f"fiscal_year_end_md must be MM-DD, got {fye!r}")
    month, day = int(fye[:2]), int(fye[3:])
    if not (1 <= month <= 12):
        raise ValueError(f"month out of range in {fye!r}")
    if not (1 <= day <= 31):
        raise ValueError(f"day out of range in {fye!r}")
    return month, day


def derive_fiscal_period(
    period_end: date,
    fiscal_year_end_md: str,
    *,
    period_type: str = "quarter",
) -> FiscalPeriod:
    """Compute fiscal_year (+ quarter) from period_end and the FY-end anchor.

    Algorithm per periods.md § 3.2:
      - Compute an "effective date" = period_end shifted back by one week.
        This absorbs 52/53-week drift of up to 7 days past a month-end
        (e.g. NVDA Q2 FY2000 ended 1999-08-01 because "Sunday nearest
        Jul 31" landed on Aug 1 that year; the content month is July).
      - Fiscal year: named after the calendar year it ends in. A period
        whose effective date is past the fiscal-year-end anchor belongs
        to the NEXT fiscal year.
      - Quarter: ceil(months_elapsed_since_FY_start / 3), computed on
        the effective date.

    `fiscal_year_end_md` for 52/53-week filers is the nominal (calendar
    month-end) anchor, not any specific year's actual period_end.
    """
    if period_type not in _PERIOD_TYPES:
        raise ValueError(
            f"period_type must be one of {_PERIOD_TYPES}, got {period_type!r}"
        )

    fy_end_month, fy_end_day = parse_fiscal_year_end_md(fiscal_year_end_md)
    effective = period_end - _DRIFT_WINDOW

    if (effective.month, effective.day) > (fy_end_month, fy_end_day):
        fiscal_year = effective.year + 1
    else:
        fiscal_year = effective.year

    if period_type == "annual":
        return FiscalPeriod(
            fiscal_year=fiscal_year,
            fiscal_quarter=None,
            period_type="annual",
            fiscal_period_label=f"FY{fiscal_year}",
        )

    fy_start_month = (fy_end_month % 12) + 1
    months_elapsed = ((effective.month - fy_start_month) % 12) + 1
    fiscal_quarter = ceil(months_elapsed / 3)
    return FiscalPeriod(
        fiscal_year=fiscal_year,
        fiscal_quarter=fiscal_quarter,
        period_type="quarter",
        fiscal_period_label=f"FY{fiscal_year} Q{fiscal_quarter}",
    )


def derive_calendar_period(period_end: date) -> CalendarPeriod:
    """Pure function of period_end per periods.md § 4."""
    cal_year = period_end.year
    cal_quarter = (period_end.month - 1) // 3 + 1
    return CalendarPeriod(
        calendar_year=cal_year,
        calendar_quarter=cal_quarter,
        calendar_period_label=f"CY{cal_year} Q{cal_quarter}",
    )


def min_fiscal_year_for_since_date(
    since_date: date,
    fiscal_year_end_md: str,
) -> int:
    """Round a calendar `since_date` forward to the first complete fiscal year.

    Returns the earliest fiscal_year whose nominal FY-end date falls on or
    after `since_date`. Use this when ingesting a bounded historical window
    so partial fiscal years aren't included at the boundary — every
    included fiscal year has all four quarters + annual, which keeps
    Layer 3 period arithmetic and the Q4 XBRL derivation coherent.

    Example: NVDA (fiscal_year_end_md='01-31'), since_date=2016-01-01.
    FY2020 nominally ends 2020-01-31 (before since_date) — excluded.
    FY2021 nominally ends 2021-01-31 (≥ since_date) — returned.
    The full FY2021 (including Q1-Q3 period_ends in CY2020) then gets
    ingested.

    The nominal FY-end date is used as the boundary rather than specific
    (possibly drifted) period_ends. For 52/53-week filers this is the
    right upper-bound anchor — see periods.md § 2.3.
    """
    fy_end_month, fy_end_day = parse_fiscal_year_end_md(fiscal_year_end_md)
    # Try a small window of candidate fiscal years around since_date.year.
    for fy in range(since_date.year - 1, since_date.year + 2):
        try:
            fy_end = date(fy, fy_end_month, fy_end_day)
        except ValueError:
            continue  # e.g. 02-29 in a non-leap year
        if fy_end >= since_date:
            return fy
    # Fallback: something very unusual with the inputs; return since_date.year.
    return since_date.year


def max_fiscal_year_for_until_date(
    until_date: date,
    fiscal_year_end_md: str,
) -> int:
    """Round a calendar `until_date` backward to the last complete fiscal year.

    Returns the latest fiscal_year whose nominal FY-end date falls on or
    before `until_date`. Symmetric to `min_fiscal_year_for_since_date` —
    use this when the ingest window should exclude periods after a
    specific calendar date (e.g., to skip a filing FMP has known-bad
    data for, or to bound the ingest to a completed fiscal year).

    Example: DELL (fiscal_year_end_md='01-29'), until_date=2025-06-01.
    FY2025 nominally ends 2025-01-31 (≤ until_date) — returned.
    FY2026 nominally ends 2026-01-30 (> until_date) — excluded.
    """
    fy_end_month, fy_end_day = parse_fiscal_year_end_md(fiscal_year_end_md)
    for fy in range(until_date.year + 1, until_date.year - 2, -1):
        try:
            fy_end = date(fy, fy_end_month, fy_end_day)
        except ValueError:
            continue
        if fy_end <= until_date:
            return fy
    return until_date.year
