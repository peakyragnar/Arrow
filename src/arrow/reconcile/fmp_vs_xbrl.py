"""Layer 5 — anchor cross-check of FMP-derived facts against SEC XBRL.

FMP is canonical — we trust its normalization. Our job is to verify the
TOP-LINE anchor values against SEC's own XBRL filing. If the anchors
match, FMP's top-line normalization is trustworthy; internal arithmetic
(Layer 1) and period arithmetic (Layer 3) propagate that confidence to
the rest of the chain.

Anchors (IS only in Slice 2a; BS/CF anchors added when those ingest slices
land):
    revenue            — the IS top line
    gross_profit       — margin structure
    operating_income   — operating performance
    ebt_incl_unusual   — pre-tax figure
    net_income         — the IS bottom line

For each anchor × period_end × period_type that we have stored:
  quarterly Q1/Q2/Q3 → match against XBRL's 3-month discrete fact
  annual FY          → match against XBRL's 12-month FY fact
  quarterly Q4       → DERIVED from XBRL as (FY − 9M_YTD), then compared
                       to FMP's stored Q4 value. This gives Q4 a
                       genuine external cross-check, since SEC does not
                       file Q4 discrete directly.

What this catches:
  - FMP top-line values disagree with SEC's own filing (FMP extractor bug,
    vendor drift, or our mapping error).
  - Q4 value is inconsistent with the implied XBRL Q4 (FY − 9M).

What this does NOT catch:
  - FMP returning a wrong value that still ties with an equally-wrong
    companion (compensating error). The top-line anchor check is
    mathematical downstream verification — the anchors are independent of
    each other in SEC's filing, so a consistent set of anchor matches is
    strong confirmation.
  - Concepts outside the anchor set. Those are trusted via Layer 1 ties.

Tolerance: max($1M, 0.1% of larger absolute value). Same as Layer 1.
HARD BLOCK on any divergence.

Per-share and share-count buckets are NOT anchors. FMP back-adjusts for
splits, SEC XBRL does not — apples to oranges. Internal relation
eps ≈ net_income / shares is validated within FMP's own output.

Items the filer doesn't report on IS face (e.g., a company that doesn't
break out gross_profit) simply have no stored FMP value for that bucket,
so the anchor check doesn't fire for that (filer, concept, period). FMP
is our canonical source for what's reported; we don't second-guess it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any

import psycopg

from arrow.reconcile.xbrl_concepts import XBRLConceptMapping, mapping_for

# Tolerance (shared with Layer 1 USD case).
USD_TOLERANCE_ABSOLUTE = Decimal("1000000")
USD_TOLERANCE_PCT = Decimal("0.001")

# IS anchor set — the small number of top-line figures whose XBRL match
# validates FMP's overall IS normalization.
IS_ANCHORS: tuple[str, ...] = (
    "revenue",
    "gross_profit",
    "operating_income",
    "ebt_incl_unusual",
    "net_income",                         # PRE-NCI (concepts.md § 4.6)
    "net_income_attributable_to_parent",  # POST-NCI; for non-NCI filers == net_income
)

# BS anchor set — the top-line balance-sheet figures. `total_assets` and
# `total_liabilities_and_equity` together prove the balance identity
# externally; `total_liabilities`, `total_equity`, `cash_and_equivalents`
# are the other analyst-critical anchors.
BS_ANCHORS: tuple[str, ...] = (
    "cash_and_equivalents",
    "total_assets",
    "total_liabilities",
    "total_equity",
    "total_liabilities_and_equity",
)

# CF anchor set — the three net-flow subtotals. XBRL has these as
# DURATION facts (like IS), but 10-Q filings typically only publish YTD
# (H1 for Q2, 9M for Q3) — not 3-month discrete — so Q2/Q3 direct
# matching fails in XBRL. We still get Q1 direct, Q4 derived
# (FY − 9M YTD), and annual direct; Q2/Q3 discrete CF is covered
# inductively by Layer 3 (Q1+Q2+Q3+Q4 = FY for CF flows).
CF_ANCHORS: tuple[str, ...] = (
    "cfo",
    "cfi",
    "cff",
)

# XBRL fact-matching duration windows.
QUARTER_DURATION = (80, 100)       # 3-month discrete (IS/CF)
ANNUAL_DURATION = (350, 380)        # 52/53-week FY (IS/CF)
NINE_MONTH_DURATION = (260, 285)    # 9-month YTD (Q3 10-Q reports this)


@dataclass(frozen=True)
class XBRLDivergence:
    """One anchor failed to match SEC XBRL. Carries every field needed
    for a human to debug: what tie we computed, the two values, where
    they came from."""
    concept: str
    period_end: date
    period_type: str
    fiscal_year: int
    fiscal_quarter: int | None
    fmp_value: Decimal
    xbrl_value: Decimal
    xbrl_tag: str
    xbrl_filed: str | None
    xbrl_accn: str | None
    delta: Decimal
    tolerance: Decimal
    derivation: str  # "direct" | "q4_derived_fy_minus_9m"


@dataclass
class AnchorCheckResult:
    anchors_with_fmp_stored: int = 0   # total anchor facts we have in DB
    anchors_checked: int = 0           # anchors we could match to XBRL
    anchors_matched: int = 0           # anchors where FMP and XBRL agreed
    anchors_not_in_xbrl: list[tuple[str, date, str]] = field(default_factory=list)
    divergences: list[XBRLDivergence] = field(default_factory=list)


def _within_tolerance(a: Decimal, b: Decimal) -> tuple[bool, Decimal, Decimal]:
    delta = abs(a - b)
    threshold = max(
        USD_TOLERANCE_ABSOLUTE,
        max(abs(a), abs(b)) * USD_TOLERANCE_PCT,
    )
    return delta <= threshold, delta, threshold


def _find_xbrl_fact(
    us_gaap: dict[str, Any],
    mapping: XBRLConceptMapping,
    *,
    end: date,
    duration: tuple[int, int],
) -> dict[str, Any] | None:
    """Find the XBRL fact entry matching (end, duration window) across any
    of the concept's alternate tags. Picks the latest-filed on ties
    (restatement semantics). For IS/CF duration facts."""
    target_end_iso = end.isoformat()
    min_days, max_days = duration
    for tag in mapping.xbrl_tags:
        concept_block = us_gaap.get(tag)
        if not concept_block:
            continue
        entries = concept_block.get("units", {}).get(mapping.unit, [])
        candidates: list[dict[str, Any]] = []
        for entry in entries:
            if entry.get("end") != target_end_iso:
                continue
            start_iso = entry.get("start")
            if not start_iso:
                continue
            try:
                span = (date.fromisoformat(entry["end"]) - date.fromisoformat(start_iso)).days
            except ValueError:
                continue
            if min_days <= span <= max_days:
                candidates.append({**entry, "__tag": tag})
        if candidates:
            candidates.sort(key=lambda e: e.get("filed", ""), reverse=True)
            return candidates[0]
    return None


def _find_xbrl_instant_fact(
    us_gaap: dict[str, Any],
    mapping: XBRLConceptMapping,
    *,
    end: date,
) -> dict[str, Any] | None:
    """Match an INSTANT-type XBRL fact (balance-sheet snapshot).

    Instant facts have only `end` (no `start`, no duration). Matching is
    purely by end date; latest-filed wins on ties (restatements or the
    same snapshot filed in multiple subsequent 10-Qs as comparative)."""
    target_end_iso = end.isoformat()
    for tag in mapping.xbrl_tags:
        concept_block = us_gaap.get(tag)
        if not concept_block:
            continue
        entries = concept_block.get("units", {}).get(mapping.unit, [])
        candidates = [
            {**e, "__tag": tag} for e in entries
            if e.get("end") == target_end_iso and not e.get("start")
        ]
        if candidates:
            candidates.sort(key=lambda e: e.get("filed", ""), reverse=True)
            return candidates[0]
    return None


def _derive_xbrl_q4(
    us_gaap: dict[str, Any],
    mapping: XBRLConceptMapping,
    *,
    fy_end: date,
    q3_period_end: date,
) -> tuple[Decimal, dict[str, Any], dict[str, Any]] | None:
    """Compute Q4 discrete = FY − 9M YTD from two XBRL facts.

    `fy_end` is the period_end of the fiscal year (= Q4 period_end).
    `q3_period_end` is the period_end of that fiscal year's Q3 — the
    end-date of the 9M YTD fact we subtract from FY.

    We match the 9M YTD fact by its end date, NOT by `fy`/`fp` tags. In
    SEC companyfacts, `fy` and `fp` denote the FILING's fiscal period
    (e.g., a 10-Q for FY2026 Q3 will tag both its own 9M YTD AND the
    prior-year comparative 9M YTD as `fy=2026, fp=Q3`). End-date
    matching disambiguates cleanly.

    Returns (q4_value, fy_entry, ytd_entry) or None if either side is absent.
    """
    fy_entry = _find_xbrl_fact(us_gaap, mapping, end=fy_end, duration=ANNUAL_DURATION)
    if not fy_entry:
        return None

    ytd_entry = _find_xbrl_fact(
        us_gaap, mapping, end=q3_period_end, duration=NINE_MONTH_DURATION,
    )
    if not ytd_entry:
        return None

    q4_value = Decimal(str(fy_entry["val"])) - Decimal(str(ytd_entry["val"]))
    return q4_value, fy_entry, ytd_entry


def _lookup_q3_period_end(
    conn: psycopg.Connection,
    *,
    company_id: int,
    fiscal_year: int,
    extraction_version: str,
) -> date | None:
    """Look up the period_end we've stored for (company, fiscal_year, Q3)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT period_end FROM financial_facts
            WHERE company_id = %s
              AND fiscal_year = %s
              AND fiscal_quarter = 3
              AND period_type = 'quarter'
              AND superseded_at IS NULL
              AND extraction_version = %s
            LIMIT 1;
            """,
            (company_id, fiscal_year, extraction_version),
        )
        row = cur.fetchone()
    return row[0] if row else None


def reconcile_anchors(
    conn: psycopg.Connection,
    *,
    company_id: int,
    extraction_version: str,
    companyfacts: dict[str, Any],
) -> AnchorCheckResult:
    """IS anchor check — compare FMP-stored IS anchor values to SEC XBRL.

    Q1/Q2/Q3 and annual: direct XBRL duration lookup.
    Q4: derived as FY − 9M YTD from XBRL, compared to FMP's stored Q4.

    Alias: reconcile_is_anchors (kept for symmetry with reconcile_bs_anchors).
    """
    result = AnchorCheckResult()
    us_gaap = companyfacts.get("facts", {}).get("us-gaap", {})

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT concept, period_end, period_type,
                   fiscal_year, fiscal_quarter, value
            FROM financial_facts
            WHERE company_id = %s
              AND extraction_version = %s
              AND superseded_at IS NULL
              AND statement = 'income_statement'
              AND concept = ANY(%s)
            ORDER BY period_end, period_type, concept;
            """,
            (company_id, extraction_version, list(IS_ANCHORS)),
        )
        rows = cur.fetchall()

    for concept, period_end, period_type, fy, fq, fmp_value in rows:
        result.anchors_with_fmp_stored += 1
        mapping = mapping_for(concept)
        if mapping is None:
            continue  # shouldn't happen; anchors are all mapped

        # Quarter Q4 → derive from XBRL FY − 9M YTD.
        if period_type == "quarter" and fq == 4:
            q3_period_end = _lookup_q3_period_end(
                conn,
                company_id=company_id,
                fiscal_year=fy,
                extraction_version=extraction_version,
            )
            if q3_period_end is None:
                # Q3 isn't in our validated window — can't derive Q4.
                result.anchors_not_in_xbrl.append((concept, period_end, period_type))
                continue
            derived = _derive_xbrl_q4(
                us_gaap, mapping,
                fy_end=period_end,
                q3_period_end=q3_period_end,
            )
            if derived is None:
                result.anchors_not_in_xbrl.append((concept, period_end, period_type))
                continue
            xbrl_value, fy_entry, ytd_entry = derived
            result.anchors_checked += 1
            ok, delta, threshold = _within_tolerance(fmp_value, xbrl_value)
            if ok:
                result.anchors_matched += 1
            else:
                result.divergences.append(XBRLDivergence(
                    concept=concept, period_end=period_end,
                    period_type=period_type,
                    fiscal_year=fy, fiscal_quarter=fq,
                    fmp_value=fmp_value, xbrl_value=xbrl_value,
                    xbrl_tag=fy_entry.get("__tag", ""),
                    xbrl_filed=fy_entry.get("filed"),
                    xbrl_accn=fy_entry.get("accn"),
                    delta=delta, tolerance=threshold,
                    derivation="q4_derived_fy_minus_9m",
                ))
            continue

        # Q1/Q2/Q3/annual → direct XBRL lookup
        duration = (
            ANNUAL_DURATION if period_type == "annual" else QUARTER_DURATION
        )
        entry = _find_xbrl_fact(
            us_gaap, mapping, end=period_end, duration=duration,
        )
        if entry is None:
            result.anchors_not_in_xbrl.append((concept, period_end, period_type))
            continue

        xbrl_value = Decimal(str(entry["val"]))
        result.anchors_checked += 1
        ok, delta, threshold = _within_tolerance(fmp_value, xbrl_value)
        if ok:
            result.anchors_matched += 1
        else:
            result.divergences.append(XBRLDivergence(
                concept=concept, period_end=period_end,
                period_type=period_type,
                fiscal_year=fy, fiscal_quarter=fq,
                fmp_value=fmp_value, xbrl_value=xbrl_value,
                xbrl_tag=entry.get("__tag", ""),
                xbrl_filed=entry.get("filed"),
                xbrl_accn=entry.get("accn"),
                delta=delta, tolerance=threshold,
                derivation="direct",
            ))

    return result


# Alias for clarity when both IS and BS reconciliation are in play.
reconcile_is_anchors = reconcile_anchors


def reconcile_bs_anchors(
    conn: psycopg.Connection,
    *,
    company_id: int,
    extraction_version: str,
    companyfacts: dict[str, Any],
) -> AnchorCheckResult:
    """BS anchor check — compare FMP-stored BS anchor values to SEC XBRL.

    BS facts in XBRL are INSTANT type (only `end`, no `start`/duration).
    Matching is by end date; latest-filed wins on ties. No Q4 derivation
    for BS — the BS snapshot at FY-end IS the Q4 BS, identical to the
    annual BS row we also store (per periods.md § 6.3). Both our Q4 and
    annual rows compare against the same XBRL instant fact.
    """
    result = AnchorCheckResult()
    us_gaap = companyfacts.get("facts", {}).get("us-gaap", {})

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT concept, period_end, period_type,
                   fiscal_year, fiscal_quarter, value
            FROM financial_facts
            WHERE company_id = %s
              AND extraction_version = %s
              AND superseded_at IS NULL
              AND statement = 'balance_sheet'
              AND concept = ANY(%s)
            ORDER BY period_end, period_type, concept;
            """,
            (company_id, extraction_version, list(BS_ANCHORS)),
        )
        rows = cur.fetchall()

    for concept, period_end, period_type, fy, fq, fmp_value in rows:
        result.anchors_with_fmp_stored += 1
        mapping = mapping_for(concept)
        if mapping is None:
            continue

        entry = _find_xbrl_instant_fact(us_gaap, mapping, end=period_end)
        if entry is None:
            result.anchors_not_in_xbrl.append((concept, period_end, period_type))
            continue

        xbrl_value = Decimal(str(entry["val"]))
        result.anchors_checked += 1
        ok, delta, threshold = _within_tolerance(fmp_value, xbrl_value)
        if ok:
            result.anchors_matched += 1
        else:
            result.divergences.append(XBRLDivergence(
                concept=concept, period_end=period_end,
                period_type=period_type,
                fiscal_year=fy, fiscal_quarter=fq,
                fmp_value=fmp_value, xbrl_value=xbrl_value,
                xbrl_tag=entry.get("__tag", ""),
                xbrl_filed=entry.get("filed"),
                xbrl_accn=entry.get("accn"),
                delta=delta, tolerance=threshold,
                derivation="instant",
            ))

    return result


def reconcile_cf_anchors(
    conn: psycopg.Connection,
    *,
    company_id: int,
    extraction_version: str,
    companyfacts: dict[str, Any],
) -> AnchorCheckResult:
    """CF anchor check — compare FMP-stored CFO/CFI/CFF to SEC XBRL.

    Q1 and annual: direct duration XBRL lookup.
    Q2 and Q3: SKIPPED — SEC filings publish YTD for these (H1 and 9M),
               not 3-month discrete. Layer 3 period arithmetic covers
               these inductively (Q1+Q2+Q3+Q4 ≈ FY on CF flows).
    Q4: derived as FY − 9M YTD from XBRL, compared to FMP's stored Q4.
    """
    result = AnchorCheckResult()
    us_gaap = companyfacts.get("facts", {}).get("us-gaap", {})

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT concept, period_end, period_type,
                   fiscal_year, fiscal_quarter, value
            FROM financial_facts
            WHERE company_id = %s
              AND extraction_version = %s
              AND superseded_at IS NULL
              AND statement = 'cash_flow'
              AND concept = ANY(%s)
            ORDER BY period_end, period_type, concept;
            """,
            (company_id, extraction_version, list(CF_ANCHORS)),
        )
        rows = cur.fetchall()

    for concept, period_end, period_type, fy, fq, fmp_value in rows:
        result.anchors_with_fmp_stored += 1
        mapping = mapping_for(concept)
        if mapping is None:
            continue

        # Q2 and Q3 of a quarter aren't directly filed as 3-month
        # discrete CF in XBRL — skip. Layer 3 period arithmetic
        # catches quarterly CF errors inductively.
        if period_type == "quarter" and fq in (2, 3):
            result.anchors_not_in_xbrl.append((concept, period_end, period_type))
            continue

        # Q4 → derive FY − 9M YTD
        if period_type == "quarter" and fq == 4:
            q3_period_end = _lookup_q3_period_end(
                conn,
                company_id=company_id,
                fiscal_year=fy,
                extraction_version=extraction_version,
            )
            if q3_period_end is None:
                result.anchors_not_in_xbrl.append((concept, period_end, period_type))
                continue
            derived = _derive_xbrl_q4(
                us_gaap, mapping,
                fy_end=period_end,
                q3_period_end=q3_period_end,
            )
            if derived is None:
                result.anchors_not_in_xbrl.append((concept, period_end, period_type))
                continue
            xbrl_value, fy_entry, _ytd = derived
            result.anchors_checked += 1
            ok, delta, threshold = _within_tolerance(fmp_value, xbrl_value)
            if ok:
                result.anchors_matched += 1
            else:
                result.divergences.append(XBRLDivergence(
                    concept=concept, period_end=period_end,
                    period_type=period_type,
                    fiscal_year=fy, fiscal_quarter=fq,
                    fmp_value=fmp_value, xbrl_value=xbrl_value,
                    xbrl_tag=fy_entry.get("__tag", ""),
                    xbrl_filed=fy_entry.get("filed"),
                    xbrl_accn=fy_entry.get("accn"),
                    delta=delta, tolerance=threshold,
                    derivation="q4_derived_fy_minus_9m",
                ))
            continue

        # Q1 / annual → direct duration lookup.
        duration = (
            ANNUAL_DURATION if period_type == "annual" else QUARTER_DURATION
        )
        entry = _find_xbrl_fact(
            us_gaap, mapping, end=period_end, duration=duration,
        )
        if entry is None:
            result.anchors_not_in_xbrl.append((concept, period_end, period_type))
            continue

        xbrl_value = Decimal(str(entry["val"]))
        result.anchors_checked += 1
        ok, delta, threshold = _within_tolerance(fmp_value, xbrl_value)
        if ok:
            result.anchors_matched += 1
        else:
            result.divergences.append(XBRLDivergence(
                concept=concept, period_end=period_end,
                period_type=period_type,
                fiscal_year=fy, fiscal_quarter=fq,
                fmp_value=fmp_value, xbrl_value=xbrl_value,
                xbrl_tag=entry.get("__tag", ""),
                xbrl_filed=entry.get("filed"),
                xbrl_accn=entry.get("accn"),
                delta=delta, tolerance=threshold,
                derivation="direct",
            ))

    return result
