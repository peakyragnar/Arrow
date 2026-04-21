"""FMP income-statement row -> canonical IS buckets.

Per docs/reference/fmp_mapping.md § 5.1 and concepts.md § 4.6. Emits
directly-mapped buckets plus derived buckets that implement the
net-income chain and the operating-expense detail split.

Units (per fmp_mapping.md § 4):
  - USD magnitudes           -> 'USD'
  - EPS                      -> 'USD/share'
  - Share counts             -> 'shares' (absolute, not millions)

Signs: FMP's IS convention matches our canonical (concepts.md § 2.1) —
no transforms required on the directly-mapped buckets.

### Net-income chain and NCI (concepts.md § 4.6)

SEC GAAP defines three distinct quantities for filers with
non-controlling interests:

  ProfitLoss (pre-NCI consolidated)          = IS "total net income"
  NetIncomeLossAttributableToNoncontrollingInterest = NCI's slice
  NetIncomeLoss (post-NCI)                   = "net income attributable
                                                 to [parent] shareholders"

FMP exposes these ambiguously:
  IS endpoint  netIncome -> POST-NCI value (= XBRL NetIncomeLoss)
  CF endpoint  netIncome -> PRE-NCI value  (= XBRL ProfitLoss)

(Verified empirically on DELL Q3 FY25: IS endpoint returns 1,132M,
CF endpoint returns 1,127M, delta = -5M NCI loss.)

concepts.md defines `net_income` as the PRE-NCI consolidated value
(= continuing + discontinued), because that is the quantity the CF
statement starts from and that Layer 2 ties against. To honor that
contract, we compute `net_income` from its components rather than
trusting FMP's IS-endpoint `netIncome` field (which is post-NCI).

We then also emit `net_income_attributable_to_parent` (FMP IS
`netIncome` directly) and derive `minority_interest` as the
reconciling difference. Downstream metric code picks whichever is
correct for its purpose: EPS/P/E use parent; CF tie uses pre-NCI.

### G&A / S&M split (concepts.md § 4.x)

FMP reports three potentially-overlapping fields:
  generalAndAdministrativeExpenses
  sellingAndMarketingExpenses
  sellingGeneralAndAdministrativeExpenses (combined)

~11/20 golden-eval filers report the split; others report only
combined. We store all three when FMP provides them: filers who
split emit gna + sme + sga; filers who don't emit only sga. Downstream
queries can ask for sga and get the combined value regardless.

Confirmed empirically: for filers who split, FMP's SG&A already equals
gna + sme (they return the aggregate AND the components). No derivation
needed on our side.
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


# Directly mapped (canonical_concept, fmp_field, unit) triples.
# `net_income` is NOT in this list — it's derived from continuing + disc
# to honor concepts.md § 4.6 (see module docstring).
_IS_BUCKETS: list[tuple[str, str, str]] = [
    # --- Top line / cost / gross ---
    ("revenue",                     "revenue",                                  "USD"),
    ("cogs",                        "costOfRevenue",                            "USD"),
    ("gross_profit",                "grossProfit",                              "USD"),
    # --- Operating expense detail ---
    ("rd",                          "researchAndDevelopmentExpenses",           "USD"),
    # G&A and S&M are reported separately by ~half of filers; we store
    # both detail buckets AND the combined sga, per concepts.md § 4.x.
    ("general_and_admin_expense",   "generalAndAdministrativeExpenses",         "USD"),
    ("selling_and_marketing_expense", "sellingAndMarketingExpenses",            "USD"),
    ("sga",                         "sellingGeneralAndAdministrativeExpenses",  "USD"),
    ("total_opex",                  "operatingExpenses",                        "USD"),
    ("operating_income",            "operatingIncome",                          "USD"),
    # --- Below-the-line ---
    ("interest_expense",            "interestExpense",                          "USD"),
    ("interest_income",             "interestIncome",                           "USD"),
    ("ebt_incl_unusual",            "incomeBeforeTax",                          "USD"),
    ("tax",                         "incomeTaxExpense",                         "USD"),
    ("continuing_ops_after_tax",    "netIncomeFromContinuingOperations",        "USD"),
    ("discontinued_ops",            "netIncomeFromDiscontinuedOperations",      "USD"),
    # Net-income-attributable-to-parent: POST-NCI value from FMP IS endpoint.
    # For filers without NCI, equals net_income; for NCI filers, differs by
    # the NCI share (see minority_interest below).
    ("net_income_attributable_to_parent", "netIncome",                          "USD"),
    # --- Per-share ---
    ("eps_basic",                   "eps",                                      "USD/share"),
    ("eps_diluted",                 "epsDiluted",                               "USD/share"),
    # --- Share counts ---
    ("shares_basic_weighted_avg",   "weightedAverageShsOut",                    "shares"),
    ("shares_diluted_weighted_avg", "weightedAverageShsOutDil",                 "shares"),
]


def map_income_statement_row(row: dict[str, Any]) -> list[MappedFact]:
    """Translate one FMP income-statement JSON row into canonical IS buckets.

    Emits directly-mapped buckets plus two DERIVED buckets per concepts.md § 4.6:
      - net_income = continuing_ops_after_tax + discontinued_ops (pre-NCI)
      - minority_interest = net_income - net_income_attributable_to_parent

    Skips a directly-mapped bucket if its FMP field is absent or None.
    Skips the derived pair if either component is absent (preserves the
    Layer-1 component-guard contract: ties are suppressed rather than
    failed when components are missing).
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

    by_concept = {m.concept: m.value for m in out}
    continuing = by_concept.get("continuing_ops_after_tax")
    disc = by_concept.get("discontinued_ops")
    parent_ni = by_concept.get("net_income_attributable_to_parent")

    if continuing is not None and disc is not None:
        net_income = continuing + disc
        out.append(MappedFact(concept="net_income", value=net_income, unit="USD"))

        if parent_ni is not None:
            mi = net_income - parent_ni
            out.append(MappedFact(concept="minority_interest", value=mi, unit="USD"))

    return out
