"""
LyondellBasell (LYB) specific extraction overrides.

Known quirks:
- MarketableSecuritiesCurrent is a footnote disclosure of the composition of
  CashAndCashEquivalentsAtCarryingValue, not a separate balance sheet line item.
  LYB states: "marketable securities classified as Cash and cash equivalents."
  The master script picks this up as short_term_investments_q, which double-counts
  since the amount is already included in cash_q. Override to zero.
"""


def post_process(record: dict, all_extractions: list) -> dict:
    """Zero out short_term_investments_q — it's a subset of cash, not a separate line."""
    record["short_term_investments_q"] = 0
    return record
