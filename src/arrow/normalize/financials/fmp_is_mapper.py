"""FMP income-statement row -> canonical IS buckets.

Per docs/reference/fmp_mapping.md § 5.1. Only `verified` buckets are
mapped; `seed` and `needs_check` buckets are left unpopulated by
FMP-sourced ingest (filled later by SEC XBRL direct per Build Order
step 19 or derived at query time).

Units (per fmp_mapping.md § 4):
  - USD magnitudes           -> 'USD'
  - EPS                      -> 'USD/share'
  - Share counts             -> 'shares' (absolute, not millions)

Signs: FMP's IS convention matches our canonical (concepts.md § 2.1) —
no transforms required on the 18 verified buckets below (empirically
validated across 12 NVDA filings per fmp_mapping.md § 7).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class MappedFact:
    concept: str
    value: Decimal
    unit: str


# (canonical_concept, fmp_field, unit) triples — verified buckets only.
_IS_BUCKETS: list[tuple[str, str, str]] = [
    # USD magnitudes
    ("revenue",                     "revenue",                                  "USD"),
    ("cogs",                        "costOfRevenue",                            "USD"),
    ("gross_profit",                "grossProfit",                              "USD"),
    ("rd",                          "researchAndDevelopmentExpenses",           "USD"),
    ("sga",                         "sellingGeneralAndAdministrativeExpenses",  "USD"),
    ("total_opex",                  "operatingExpenses",                        "USD"),
    ("operating_income",            "operatingIncome",                          "USD"),
    ("interest_expense",            "interestExpense",                          "USD"),
    ("interest_income",             "interestIncome",                           "USD"),
    ("ebt_incl_unusual",            "incomeBeforeTax",                          "USD"),
    ("tax",                         "incomeTaxExpense",                         "USD"),
    ("continuing_ops_after_tax",    "netIncomeFromContinuingOperations",        "USD"),
    ("discontinued_ops",            "netIncomeFromDiscontinuedOperations",      "USD"),
    ("net_income",                  "netIncome",                                "USD"),
    # Per-share
    ("eps_basic",                   "eps",                                      "USD/share"),
    ("eps_diluted",                 "epsDiluted",                               "USD/share"),
    # Share counts
    ("shares_basic_weighted_avg",   "weightedAverageShsOut",                    "shares"),
    ("shares_diluted_weighted_avg", "weightedAverageShsOutDil",                 "shares"),
]


def map_income_statement_row(row: dict[str, Any]) -> list[MappedFact]:
    """Translate one FMP income-statement JSON row into canonical IS buckets.

    Skips a bucket if its FMP field is absent or None. The schema forbids
    NULL on financial_facts.value, so emitting nothing for a missing field
    is the correct behavior.
    """
    out: list[MappedFact] = []
    for concept, fmp_field, unit in _IS_BUCKETS:
        raw_value = row.get(fmp_field)
        if raw_value is None:
            continue
        out.append(
            MappedFact(
                concept=concept,
                value=Decimal(str(raw_value)),
                unit=unit,
            )
        )
    return out
