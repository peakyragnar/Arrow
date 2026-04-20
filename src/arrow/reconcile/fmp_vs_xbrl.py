"""Cross-source verification: FMP-derived financial_facts vs SEC XBRL.

For every USD-magnitude (concept, period_end, period_type) row in our
IS facts, find the corresponding SEC XBRL fact and compare values.
Divergence beyond tolerance is a HARD BLOCK.

Per-share (eps_*) and share-count (shares_*) buckets are SKIPPED from
cross-check until a splits-events table exists. FMP back-adjusts these
values for every historical split (e.g. NVDA's June 2024 10-for-1);
SEC XBRL reports them as-originally-filed. Comparing the two without
a split-aware transform always fails across a split boundary. Per
fmp_mapping.md § 3.2 we've committed to storing FMP's adjusted values;
external verification of the adjustment itself is future work
(Build Order ~step 13 when company_events grows a stock_split event).

Matching algorithm:
  1. Resolve `concept` to a list of candidate XBRL tags
     (reconcile/xbrl_concepts.py).
  2. Scan the companyfacts payload's us-gaap section for each tag.
  3. Filter candidate facts by:
     - `end` == stored period_end (exact match)
     - duration (end - start) in the expected window:
         quarter: 80..100 days (3-month discrete; excludes H1/9M YTD)
         annual:  350..380 days
  4. If multiple facts match (restatements in different filings), pick
     the one with the latest `filed` date — matches our PIT semantics
     of "the most recent authoritative value."
  5. If no fact matches, SUPPRESS (skip) that comparison — we don't
     fail the ingest on SEC-side absence (filers don't always report
     every bucket, and some us-gaap tags are optional).

Tolerance:
  USD: same as Layer 1 — max($1M, 0.1% of larger abs).
  USD/shares (EPS): $0.01 absolute (per-share rounding).
  shares: 500K absolute (share-count rounding).

Coverage caveat:
  Q4 flows aren't in XBRL directly (SEC only reports Q1/Q2/Q3 + FY on
  flow concepts). Q4 divergence is caught by Layer 3 period arithmetic
  (Q1+Q2+Q3+Q4 ≈ FY), not here. This module explicitly skips Q4 for
  flow buckets.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any

import psycopg

from arrow.reconcile.xbrl_concepts import XBRLConceptMapping, mapping_for

# Tolerances (shared names with verify_is for the USD magnitude case).
USD_TOLERANCE_ABSOLUTE = Decimal("1000000")
USD_TOLERANCE_PCT = Decimal("0.001")
EPS_TOLERANCE_ABSOLUTE = Decimal("0.01")
SHARES_TOLERANCE_ABSOLUTE = Decimal("500000")


@dataclass(frozen=True)
class XBRLDivergence:
    concept: str
    period_end: date
    period_type: str
    fmp_value: Decimal
    xbrl_value: Decimal
    xbrl_tag: str
    xbrl_filed: str | None
    delta: Decimal
    tolerance: Decimal


@dataclass
class ReconcileResult:
    checked: int = 0
    matched: int = 0
    skipped_no_xbrl: int = 0
    skipped_q4: int = 0
    skipped_unmapped: int = 0
    skipped_split_sensitive: int = 0
    divergences: list[XBRLDivergence] = field(default_factory=list)


# Per-share and share-count buckets — SEC XBRL stores as-originally-filed,
# FMP stores split-adjusted. Safe direct comparison requires a splits-aware
# transform that we haven't built yet.
_SPLIT_SENSITIVE_CONCEPTS = frozenset({
    "eps_basic", "eps_diluted",
    "shares_basic_weighted_avg", "shares_diluted_weighted_avg",
})


def _tolerance_for(unit: str, a: Decimal, b: Decimal) -> Decimal:
    # Storage writes 'USD/share' (singular, per fmp_mapping.md § 4); XBRL
    # uses 'USD/shares' (plural). Treat both as EPS.
    if unit == "USD":
        return max(
            USD_TOLERANCE_ABSOLUTE,
            max(abs(a), abs(b)) * USD_TOLERANCE_PCT,
        )
    if unit in ("USD/share", "USD/shares"):
        return EPS_TOLERANCE_ABSOLUTE
    if unit == "shares":
        return SHARES_TOLERANCE_ABSOLUTE
    raise ValueError(f"unknown unit: {unit!r}")


def _duration_window(period_type: str) -> tuple[int, int]:
    if period_type == "quarter":
        return (80, 100)  # 3-month discrete
    if period_type == "annual":
        return (350, 380)  # 52- or 53-week FY
    raise ValueError(f"unknown period_type: {period_type!r}")


def _find_xbrl_value(
    companyfacts: dict[str, Any],
    mapping: XBRLConceptMapping,
    period_end: date,
    period_type: str,
) -> tuple[Decimal, str, dict[str, Any]] | None:
    """Return (value, xbrl_tag, fact_entry) or None if no match."""
    us_gaap = companyfacts.get("facts", {}).get("us-gaap", {})
    min_days, max_days = _duration_window(period_type)
    target_end_iso = period_end.isoformat()

    for tag in mapping.xbrl_tags:
        concept_block = us_gaap.get(tag)
        if not concept_block:
            continue
        entries = concept_block.get("units", {}).get(mapping.unit, [])
        candidates = []
        for entry in entries:
            if entry.get("end") != target_end_iso:
                continue
            start_iso = entry.get("start")
            if start_iso is None:
                continue  # instant fact (BS); IS/CF facts must have start
            try:
                start_d = date.fromisoformat(start_iso)
                end_d = date.fromisoformat(entry["end"])
            except ValueError:
                continue
            duration = (end_d - start_d).days
            if not (min_days <= duration <= max_days):
                continue
            candidates.append(entry)

        if candidates:
            # Pick most recently filed (handles restatements).
            candidates.sort(key=lambda e: e.get("filed", ""), reverse=True)
            entry = candidates[0]
            return Decimal(str(entry["val"])), tag, entry
    return None


def reconcile_company(
    conn: psycopg.Connection,
    *,
    company_id: int,
    extraction_version: str,
    companyfacts: dict[str, Any],
) -> ReconcileResult:
    """Compare every current IS fact for this company against XBRL."""
    result = ReconcileResult()

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT concept, period_end, period_type, fiscal_quarter, value, unit
            FROM financial_facts
            WHERE company_id = %s
              AND extraction_version = %s
              AND superseded_at IS NULL
              AND statement = 'income_statement'
            ORDER BY period_end, period_type, concept;
            """,
            (company_id, extraction_version),
        )
        rows = cur.fetchall()

    for concept, period_end, period_type, fiscal_quarter, fmp_value, unit in rows:
        # Q4 flow isn't reported directly by SEC; Layer 3 covers it.
        if period_type == "quarter" and fiscal_quarter == 4:
            result.skipped_q4 += 1
            continue

        # Split-sensitive buckets: FMP back-adjusts, SEC XBRL doesn't.
        # Direct comparison is wrong; skip until splits-aware transform exists.
        if concept in _SPLIT_SENSITIVE_CONCEPTS:
            result.skipped_split_sensitive += 1
            continue

        mapping = mapping_for(concept)
        if mapping is None:
            result.skipped_unmapped += 1
            continue

        match = _find_xbrl_value(companyfacts, mapping, period_end, period_type)
        if match is None:
            result.skipped_no_xbrl += 1
            continue

        xbrl_value, xbrl_tag, entry = match
        result.checked += 1
        tolerance = _tolerance_for(unit, fmp_value, xbrl_value)
        delta = abs(fmp_value - xbrl_value)
        if delta <= tolerance:
            result.matched += 1
        else:
            result.divergences.append(
                XBRLDivergence(
                    concept=concept,
                    period_end=period_end,
                    period_type=period_type,
                    fmp_value=fmp_value,
                    xbrl_value=xbrl_value,
                    xbrl_tag=xbrl_tag,
                    xbrl_filed=entry.get("filed"),
                    delta=delta,
                    tolerance=tolerance,
                )
            )

    return result
