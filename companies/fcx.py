"""
Freeport-McMoRan (FCX) specific extraction overrides.

Known quirks:
- FY2024 10-K (accession 0000831259-25-000006, period end 2024-12-31):
  DocumentFiscalYearFocus is incorrectly tagged as "2023" instead of "2024".
  Fixed by fix_dei() correcting the fiscal year based on DocumentPeriodEndDate.
"""


def fix_dei(dei: dict, meta: dict) -> dict:
    """Fix incorrect DEI fiscal year on FCX FY2024 10-K."""
    if meta["accession"] == "0000831259-25-000006":
        dei["DocumentFiscalYearFocus"] = "2024"
    return dei
