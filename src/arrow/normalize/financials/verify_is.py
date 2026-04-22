"""IS subtotal-tie verification (verification.md Layer 1, § 2.1).

SOFT FLAG. The load keeps the FMP row, then writes `data_quality_flags`
if any tie fails. Ties where at least one component is missing from the
mapped data are skipped rather than failed — this matches the Layer 1 +
component-guard interaction in verification.md and lets filer-optional /
source-missing concepts stay null without turning absence into a false
soft flag.

For FMP-sourced IS, four ties can be checked end-to-end (all components
are in FMP's verified field set):

  gross_profit             == revenue - cogs
  operating_income         == gross_profit - total_opex
  continuing_ops_after_tax == ebt_incl_unusual - tax
  net_income               == continuing_ops_after_tax + discontinued_ops

Ebt_excl_unusual and unusual-items ties are not checkable — FMP doesn't
break unusual items out on the IS endpoint (see fmp_mapping.md § 5.1).
Those ties become relevant when SEC XBRL direct ingest lands.

Tolerance per verification.md § 2.4:
  - magnitudes: max(±$1M absolute, ±0.1% of larger abs)
  - EPS ties (future): ±$0.01
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

TOLERANCE_ABSOLUTE = Decimal("1000000")     # $1M in absolute USD
TOLERANCE_PCT = Decimal("0.001")             # 0.1%


@dataclass(frozen=True)
class TieFailure:
    tie: str
    filer: Decimal
    computed: Decimal
    delta: Decimal
    tolerance: Decimal


def _within_tolerance(filer: Decimal, computed: Decimal) -> tuple[bool, Decimal, Decimal]:
    delta = abs(filer - computed)
    threshold = max(
        TOLERANCE_ABSOLUTE,
        max(abs(filer), abs(computed)) * TOLERANCE_PCT,
    )
    return delta <= threshold, delta, threshold


# (name, [required concepts], [(component, sign) to sum into `computed`]).
# The last concept in `required` is the filer-reported subtotal; the
# others are the components used to compute the expected value.
_IS_TIES: list[tuple[str, list[str], list[tuple[str, int]]]] = [
    (
        "gross_profit == revenue - cogs",
        ["revenue", "cogs", "gross_profit"],
        [("revenue", +1), ("cogs", -1)],
    ),
    (
        "operating_income == gross_profit - total_opex",
        ["gross_profit", "total_opex", "operating_income"],
        [("gross_profit", +1), ("total_opex", -1)],
    ),
    (
        "continuing_ops_after_tax == ebt_incl_unusual - tax",
        ["ebt_incl_unusual", "tax", "continuing_ops_after_tax"],
        [("ebt_incl_unusual", +1), ("tax", -1)],
    ),
    (
        "net_income == continuing_ops_after_tax + discontinued_ops",
        ["continuing_ops_after_tax", "discontinued_ops", "net_income"],
        [("continuing_ops_after_tax", +1), ("discontinued_ops", +1)],
    ),
    # Post-NCI vs pre-NCI reconciliation (concepts.md § 4.6). Tautological
    # by the mapper's derivation of minority_interest, but retained as a
    # contract guard — any future mapper change that breaks this relationship
    # fails loudly.
    (
        "net_income_attributable_to_parent == net_income - minority_interest",
        ["net_income", "minority_interest", "net_income_attributable_to_parent"],
        [("net_income", +1), ("minority_interest", -1)],
    ),
]


def verify_is_ties(values_by_concept: dict[str, Decimal]) -> list[TieFailure]:
    """Return the list of ties that failed (empty = all ties passed).

    Component-guard semantics: if a required component for a tie isn't
    present in `values_by_concept`, that tie is suppressed rather than
    failed. This is required for FMP-sourced ingest, where some concepts are
    legitimately filer-optional (e.g. discontinued_ops usually 0/absent) or
    source-absent (e.g. the NCI reconciliation on non-NCI filers).
    """
    failures: list[TieFailure] = []

    for name, required, components in _IS_TIES:
        if any(c not in values_by_concept for c in required):
            continue
        filer = values_by_concept[required[-1]]
        computed = sum(
            (values_by_concept[concept] * sign for concept, sign in components),
            start=Decimal("0"),
        )
        ok, delta, threshold = _within_tolerance(filer, computed)
        if not ok:
            failures.append(TieFailure(
                tie=name,
                filer=filer,
                computed=computed,
                delta=delta,
                tolerance=threshold,
            ))

    return failures
