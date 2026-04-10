"""
Palantir-specific extraction overrides.

Known quirks:
- R&D: PLTR IPO'd in Sep 2020, so FY2021 Q1 10-Q is not in the fetch window.
  The extraction has 19 quarters (FY2021 Q2 through FY2025 Q4). The R&D
  amortization schedule needs 20 quarters, so fix_rd_series prepends an
  estimated FY2021 Q1 using FY2021 annual/4 ($387,487,000 / 4 = $96,871,750).
"""


def fix_rd_series(quarterly_rd: list, records: list) -> list:
    """Prepend estimated FY2021 Q1 R&D.

    PLTR's first extracted quarter is FY2021 Q2. FY2021 Q1 is missing because
    the 10-Q isn't in the fetch window (IPO was Sep 2020). Use FY2021 annual
    R&D / 4 = $387,487,000 / 4 = $96,871,750 as the estimate.
    """
    if records and records[0]["fiscal_year"] == 2021 and records[0]["fiscal_period"] == "Q1":
        return quarterly_rd
    return [96871750] + list(quarterly_rd)
