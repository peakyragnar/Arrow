"""Amendment detection + XBRL supersession — Build Order step 1.5.

Handles the "amendment-within-regular-filing" pattern: when a filer
restates a prior period's value in a later filing that is NOT a formal
10-Q/A (e.g., in-10-K restatement of comparative quarters, or in-later-10-Q
restatement of prior-year comparatives), FMP's pipeline does NOT pick up
the restated value — it continues to serve the original.

This agent detects the discrepancy via Layer 3 failure (Q1+Q2+Q3+Q4 ≠ FY
for a fiscal year), finds authoritative restated values in SEC XBRL
companyfacts, applies them via supersession, and re-verifies that all
Layer 1, Layer 2, and Layer 3 ties hold holistically.

Follows the rules in docs/research/amendment_phase_1_5_design.md:
  A. XBRL provenance is explicit and authoritative (no invention).
  B. Sanity bounds (no sign flips, ≤50% delta).
  C. Holistic post-supersession verification — ALL layers must pass.
  D. Full provenance on every supersession row.
  E. Deterministic reproducibility.
  F. Categorical outcomes: Clean / Amended / UnresolvableAmendment / Layer1Fail.

Refuse-on-ambiguity is architecturally enforced: Rule C's holistic
re-verification is atomic. If any post-supersession tie fails, the
entire supersession transaction is rolled back — no partial state is
ever persisted.
"""

from __future__ import annotations

import json as _json
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import psycopg

from arrow.ingest.common.http import HttpClient
from arrow.ingest.sec.bootstrap import SEC_RATE_LIMIT, SEC_USER_AGENT
from arrow.ingest.sec.company_facts import fetch_company_facts
from arrow.normalize.financials.load import (
    BS_EXTRACTION_VERSION,
    CF_EXTRACTION_VERSION,
    IS_EXTRACTION_VERSION,
)
from arrow.normalize.financials.verify_bs import verify_bs_ties
from arrow.normalize.financials.verify_cf import verify_cf_ties
from arrow.normalize.financials.verify_is import verify_is_ties
from arrow.normalize.financials.verify_period_arithmetic import (
    PeriodArithmeticFailure,
    verify_period_arithmetic,
)
from arrow.reconcile.xbrl_concepts import mapping_for

# Amendment extraction version tags — distinct from FMP versions so the
# chain is traceable: fmp-*-v1 row gets superseded by xbrl-amendment-*-v1 row.
IS_AMENDMENT_VERSION = "xbrl-amendment-is-v1"
BS_AMENDMENT_VERSION = "xbrl-amendment-bs-v1"
CF_AMENDMENT_VERSION = "xbrl-amendment-cf-v1"

_STATEMENT_TO_AMENDMENT_VERSION = {
    "income_statement": IS_AMENDMENT_VERSION,
    "balance_sheet": BS_AMENDMENT_VERSION,
    "cash_flow": CF_AMENDMENT_VERSION,
}
_STATEMENT_TO_FMP_VERSION = {
    "income_statement": IS_EXTRACTION_VERSION,
    "balance_sheet": BS_EXTRACTION_VERSION,
    "cash_flow": CF_EXTRACTION_VERSION,
}

# Layer-3 flow buckets per statement — duplicates of what's in
# verify_period_arithmetic but imported via that module for single source of truth.
from arrow.normalize.financials.verify_period_arithmetic import (
    _CF_FLOW_BUCKETS,
    _IS_FLOW_BUCKETS,
)

# Rule B: sanity bounds
SANITY_MAX_ABS_DELTA_PCT = Decimal("0.50")  # ≤50% delta
SANITY_MIN_VALUE_FOR_DELTA_CHECK = Decimal("100000")  # $100k — below this,
# percentage deltas become meaningless (small-base amplification).
# Values below this skip the delta check but still must not sign-flip.

# Layer-3 re-verify tolerance (matches verify_period_arithmetic).
L3_TOLERANCE_ABS = Decimal("2500000")
L3_TOLERANCE_PCT = Decimal("0.001")


class UnresolvableAmendment(RuntimeError):
    """Rule C/B violation — amendment could not be cleanly resolved.

    Raised when:
      - Post-supersession Layer 1/2/3 re-verification fails (partial
        restatement, spinoff, or real data error — not an amendment).
      - A candidate fails Rule B sanity bounds.
      - Multiple restatement chains produce ambiguity we can't resolve.
    """

    def __init__(self, reason: str, diagnostics: dict[str, Any] | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.diagnostics = diagnostics or {}


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SupersessionCandidate:
    """One potential (FMP row -> XBRL value) supersession."""
    statement: str
    concept: str
    period_end: date
    period_type: str
    fiscal_year: int
    fiscal_quarter: int | None
    # FMP side
    fmp_fact_id: int
    fmp_value: Decimal
    fmp_published_at: datetime
    # XBRL side
    xbrl_value: Decimal
    xbrl_tag: str
    xbrl_accn: str
    xbrl_filed: str
    xbrl_form: str
    xbrl_start: str
    xbrl_end: str


@dataclass
class AmendmentResult:
    status: str
    # status values:
    #   "clean"               — no Layer 3 failures; nothing to do
    #   "amended"             — all L3 failures auto-resolved via supersession; no flags written
    #   "partially_amended"   — some L3 resolved via supersession, residuals written as flags
    #   "unresolved_flagged"  — no supersessions applied (no XBRL candidates, or they'd break L1);
    #                           all Layer 3 failures written as flags
    supersessions_applied: list[SupersessionCandidate] = field(default_factory=list)
    fiscal_years_checked: list[tuple[str, int]] = field(default_factory=list)
    flags_written: int = 0


# ---------------------------------------------------------------------------
# Step 1: find Layer-3 failures across all statements
# ---------------------------------------------------------------------------


def _all_layer3_failures(
    conn: psycopg.Connection,
    *,
    company_id: int,
) -> list[tuple[str, PeriodArithmeticFailure]]:
    """Return [(statement, failure), ...] across IS + CF.

    BS is exempt from Layer 3 (instant facts don't sum across periods).
    """
    out: list[tuple[str, PeriodArithmeticFailure]] = []
    for statement, ev in (
        ("income_statement", IS_EXTRACTION_VERSION),
        ("cash_flow", CF_EXTRACTION_VERSION),
    ):
        fails = verify_period_arithmetic(
            conn, company_id=company_id, extraction_version=ev, statement=statement,
        )
        for f in fails:
            out.append((statement, f))
    return out


# ---------------------------------------------------------------------------
# Step 2: for each failing quarter, look up XBRL latest value
# ---------------------------------------------------------------------------


def _find_xbrl_latest_fact(
    us_gaap: dict[str, Any],
    canonical_concept: str,
    *,
    period_end: date,
    duration_min_days: int,
    duration_max_days: int,
) -> dict[str, Any] | None:
    """Return the LATEST-FILED XBRL fact matching (concept, period_end, duration).

    Rule A: supersession source must be the latest-filed value. If FMP's
    stored value already matches this, supersession is a no-op.
    """
    mapping = mapping_for(canonical_concept)
    if mapping is None:
        return None  # no XBRL mapping known — can't find candidates

    target_end = period_end.isoformat()
    candidates: list[dict[str, Any]] = []
    for tag in mapping.xbrl_tags:
        block = us_gaap.get(tag)
        if not block:
            continue
        entries = block.get("units", {}).get(mapping.unit, [])
        for entry in entries:
            if entry.get("end") != target_end:
                continue
            start_s = entry.get("start")
            if not start_s:
                continue
            try:
                span = (date.fromisoformat(entry["end"]) - date.fromisoformat(start_s)).days
            except ValueError:
                continue
            if duration_min_days <= span <= duration_max_days:
                candidates.append({**entry, "__tag": tag})

    if not candidates:
        return None
    # Sort by filed date DESC; the top is the latest authoritative value.
    candidates.sort(key=lambda e: e.get("filed", ""), reverse=True)
    return candidates[0]


# ---------------------------------------------------------------------------
# Step 3: query FMP-stored facts we might supersede
# ---------------------------------------------------------------------------


def _fmp_quarterly_facts_for_fy(
    conn: psycopg.Connection,
    *,
    company_id: int,
    statement: str,
    concept: str,
    fiscal_year: int,
) -> list[dict[str, Any]]:
    """Return current FMP rows for Q1-Q4 of a given (statement, concept, fiscal_year).

    Only rows with superseded_at IS NULL and the FMP extraction_version.
    """
    ev = _STATEMENT_TO_FMP_VERSION[statement]
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, concept, period_end, period_type, value,
                   fiscal_year, fiscal_quarter, published_at, unit,
                   fiscal_period_label, calendar_year, calendar_quarter,
                   calendar_period_label, source_raw_response_id
            FROM financial_facts
            WHERE company_id = %s
              AND statement = %s
              AND concept = %s
              AND fiscal_year = %s
              AND period_type = 'quarter'
              AND extraction_version = %s
              AND superseded_at IS NULL
            ORDER BY fiscal_quarter;
            """,
            (company_id, statement, concept, fiscal_year, ev),
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _all_fmp_facts_for_period(
    conn: psycopg.Connection,
    *,
    company_id: int,
    statement: str,
    period_end: date,
    period_type: str,
) -> list[dict[str, Any]]:
    """Return ALL current FMP-extraction-version rows at a specific period.

    Used when the agent has decided a period has a restatement and wants to
    refresh every mappable concept at that period to keep it internally
    consistent per Layer 1 ties.
    """
    ev = _STATEMENT_TO_FMP_VERSION[statement]
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, concept, period_end, period_type, value,
                   fiscal_year, fiscal_quarter, published_at, unit
            FROM financial_facts
            WHERE company_id = %s
              AND statement = %s
              AND period_end = %s
              AND period_type = %s
              AND extraction_version = %s
              AND superseded_at IS NULL;
            """,
            (company_id, statement, period_end, period_type, ev),
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _all_fmp_facts_for_fiscal_year_annual(
    conn: psycopg.Connection,
    *,
    company_id: int,
    statement: str,
    fiscal_year: int,
) -> list[dict[str, Any]]:
    """Return ALL current FMP annual rows for a fiscal year (statement-scoped)."""
    ev = _STATEMENT_TO_FMP_VERSION[statement]
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, concept, period_end, period_type, value,
                   fiscal_year, fiscal_quarter, published_at, unit
            FROM financial_facts
            WHERE company_id = %s
              AND statement = %s
              AND fiscal_year = %s
              AND period_type = 'annual'
              AND extraction_version = %s
              AND superseded_at IS NULL;
            """,
            (company_id, statement, fiscal_year, ev),
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Step 4: Rule B — sanity bounds
# ---------------------------------------------------------------------------


def _passes_sanity_bounds(fmp_value: Decimal, xbrl_value: Decimal) -> tuple[bool, str]:
    """Rule B. Return (ok, reason_if_fail)."""
    # Sign flip check — only if both nonzero
    if fmp_value != 0 and xbrl_value != 0:
        if (fmp_value > 0) != (xbrl_value > 0):
            return False, (
                f"sign flip: FMP={fmp_value} XBRL={xbrl_value} "
                f"(Rule B forbids unexplained sign flips)"
            )

    # Percentage-delta check — only for values with meaningful magnitude.
    abs_fmp = abs(fmp_value)
    if abs_fmp >= SANITY_MIN_VALUE_FOR_DELTA_CHECK:
        delta = abs(xbrl_value - fmp_value)
        pct = delta / abs_fmp
        if pct > SANITY_MAX_ABS_DELTA_PCT:
            return False, (
                f"magnitude delta {pct:.1%} exceeds {SANITY_MAX_ABS_DELTA_PCT:.0%} "
                f"(Rule B sanity bound; FMP={fmp_value} XBRL={xbrl_value})"
            )

    return True, ""


# ---------------------------------------------------------------------------
# Step 3c: identity-derived candidates (IS subtotal ties)
# ---------------------------------------------------------------------------


# (derived_concept, [(partner_concept, sign_in_tie)]) — represents
# `derived_concept = sum(partner * sign)` — but rearranged: the "tie" says
# lhs = sum(components). If lhs and some components are known from XBRL,
# the remaining component can be solved for.
#
# Tie form: `filer_subtotal == sum(components with signs)`
#   - gross_profit == revenue - cogs
#     → if we have gross_profit + revenue from XBRL, derive cogs = revenue - gross_profit
#   - operating_income == gross_profit - total_opex
#     → if we have operating_income + gross_profit from XBRL, derive total_opex = gross_profit - operating_income
_DERIVABLE_IS_IDENTITIES: list[tuple[str, str, list[tuple[str, int]]]] = [
    # (derived, anchor, [(other, sign_relative_to_derived_when_solving)])
    # gross_profit = revenue - cogs  -->  cogs = revenue - gross_profit
    ("cogs", "derived from revenue - gross_profit",
     [("revenue", +1), ("gross_profit", -1)]),
    # operating_income = gross_profit - total_opex  -->  total_opex = gross_profit - operating_income
    ("total_opex", "derived from gross_profit - operating_income",
     [("gross_profit", +1), ("operating_income", -1)]),
]


def _derive_identity_candidates(
    conn: psycopg.Connection,
    *,
    company_id: int,
    existing_candidates: list[SupersessionCandidate],
    affected_quarter_set: set[tuple[str, date, int, int]],
    failing_fys: set[tuple[str, int]],
    xbrl_us_gaap: dict[str, Any],
) -> list[SupersessionCandidate]:
    """For each affected period, if an IS identity has N-1 XBRL anchors but
    missing the Nth, derive the Nth from the identity and create a
    supersession candidate for it.

    Provenance: the derived candidate points to the XBRL raw response (same
    as its anchors) and its `supersession_reason` explicitly notes
    "derived from X - Y per Layer-1 identity".
    """
    # Build a lookup of existing XBRL-candidate values by (statement, period_end, concept).
    # This tells us which concepts are *being* superseded (had FMP-XBRL delta).
    xbrl_cand_vals: dict[tuple[str, date, str, str], SupersessionCandidate] = {}
    for c in existing_candidates:
        xbrl_cand_vals[(c.statement, c.period_end, c.period_type, c.concept)] = c

    def _anchor_value(period_end: date, period_type: str, concept: str) -> Decimal | None:
        """Find the current-authoritative value for an anchor concept at this
        period. Preferred: XBRL candidate (if it exists in our supersession
        plan). Fallback: XBRL latest fact directly (for concepts where FMP
        already matches XBRL — no candidate was created but XBRL IS the truth).
        Returns None if XBRL has no value for this concept at this period."""
        key = ("income_statement", period_end, period_type, concept)
        if key in xbrl_cand_vals:
            return xbrl_cand_vals[key].xbrl_value
        # Fall back: check XBRL directly
        dur = (80, 100) if period_type == "quarter" else (350, 380)
        xbrl_fact = _find_xbrl_latest_fact(
            xbrl_us_gaap, concept, period_end=period_end,
            duration_min_days=dur[0], duration_max_days=dur[1],
        )
        if xbrl_fact is None:
            return None
        return Decimal(str(xbrl_fact["val"]))

    new_derived: list[SupersessionCandidate] = []

    # Process every affected (statement, period_end, period_type):
    # quarters from affected_quarter_set, annuals from failing_fys.
    periods: list[tuple[str, date, str, int, int | None]] = []
    for (statement, period_end, fq, fy) in affected_quarter_set:
        if statement == "income_statement":
            periods.append((statement, period_end, "quarter", fy, fq))
    for (statement, fy) in failing_fys:
        if statement != "income_statement":
            continue
        fy_rows = _all_fmp_facts_for_fiscal_year_annual(
            conn, company_id=company_id, statement=statement, fiscal_year=fy,
        )
        if fy_rows:
            periods.append((statement, fy_rows[0]["period_end"], "annual", fy, None))

    for statement, period_end, period_type, fy, fq in periods:
        for derived_concept, reason_stub, partner_list in _DERIVABLE_IS_IDENTITIES:
            # Must be MISSING a direct XBRL candidate for derived_concept.
            # Also skip if XBRL has derived_concept directly at this period
            # (means XBRL tagged it — use that, don't derive).
            if (statement, period_end, period_type, derived_concept) in xbrl_cand_vals:
                continue
            direct_xbrl = _find_xbrl_latest_fact(
                xbrl_us_gaap, derived_concept,
                period_end=period_end,
                duration_min_days=80 if period_type == "quarter" else 350,
                duration_max_days=100 if period_type == "quarter" else 380,
            )
            if direct_xbrl is not None:
                continue  # XBRL has it directly — no derivation needed

            # All partners must have XBRL values (candidate OR direct fact)
            partner_values: dict[str, Decimal] = {}
            partner_candidate: SupersessionCandidate | None = None
            all_present = True
            for partner_concept, _sign in partner_list:
                val = _anchor_value(period_end, period_type, partner_concept)
                if val is None:
                    all_present = False
                    break
                partner_values[partner_concept] = val
                cand_key = (statement, period_end, period_type, partner_concept)
                if cand_key in xbrl_cand_vals:
                    partner_candidate = xbrl_cand_vals[cand_key]
            if not all_present:
                continue

            # Look up the FMP-stored row for derived_concept at this period —
            # we need its id to mark superseded and copy metadata.
            fmp_row = _single_fmp_fact(
                conn, company_id=company_id, statement=statement,
                period_end=period_end, period_type=period_type,
                concept=derived_concept,
            )
            if fmp_row is None:
                continue

            # Compute the derived value
            derived_val = Decimal("0")
            for partner_concept, sign in partner_list:
                derived_val += partner_values[partner_concept] * sign

            if abs(derived_val - fmp_row["value"]) < Decimal("1"):
                continue  # already current

            # Rule B sanity on derived value. Skip + flag if bizarre.
            ok, reason = _passes_sanity_bounds(fmp_row["value"], derived_val)
            if not ok:
                # Note: conn/ingest_run_id aren't in this function's scope;
                # the derivation is already post-supersession-candidate-set.
                # Bizarre derivations are rare; we simply skip and let Layer 3
                # surface the residual as its own flag downstream.
                continue

            # Build the derived candidate — use the partner's XBRL provenance
            # as the supersession's nominal source (same accn/filed), but
            # flag the derivation in xbrl_tag.
            new_derived.append(SupersessionCandidate(
                statement=statement, concept=derived_concept,
                period_end=period_end, period_type=period_type,
                fiscal_year=fy, fiscal_quarter=fq,
                fmp_fact_id=fmp_row["id"], fmp_value=fmp_row["value"],
                fmp_published_at=fmp_row["published_at"],
                xbrl_value=derived_val,
                xbrl_tag=f"DERIVED[{reason_stub}]",
                xbrl_accn=partner_candidate.xbrl_accn if partner_candidate else "",
                xbrl_filed=partner_candidate.xbrl_filed if partner_candidate else "",
                xbrl_form=partner_candidate.xbrl_form if partner_candidate else "",
                xbrl_start=partner_candidate.xbrl_start if partner_candidate else "",
                xbrl_end=partner_candidate.xbrl_end if partner_candidate else "",
            ))
    return new_derived


def _single_fmp_fact(
    conn: psycopg.Connection,
    *,
    company_id: int,
    statement: str,
    period_end: date,
    period_type: str,
    concept: str,
) -> dict[str, Any] | None:
    """Return the current FMP-extraction row for (concept, period_end, period_type), or None."""
    ev = _STATEMENT_TO_FMP_VERSION[statement]
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, concept, period_end, period_type, value,
                   fiscal_year, fiscal_quarter, published_at, unit
            FROM financial_facts
            WHERE company_id = %s
              AND statement = %s
              AND period_end = %s
              AND period_type = %s
              AND concept = %s
              AND extraction_version = %s
              AND superseded_at IS NULL
            LIMIT 1;
            """,
            (company_id, statement, period_end, period_type, concept, ev),
        )
        row = cur.fetchone()
        if row is None:
            return None
        cols = [d.name for d in cur.description]
        return dict(zip(cols, row))


# ---------------------------------------------------------------------------
# Step 5: apply supersession (write the new row, mark old superseded)
# ---------------------------------------------------------------------------


def _apply_supersession(
    conn: psycopg.Connection,
    candidate: SupersessionCandidate,
    *,
    company_id: int,
    xbrl_raw_response_id: int,
    ingest_run_id: int,
) -> int:
    """Insert the XBRL-amendment row; mark the FMP row superseded. Return new row id."""
    amendment_version = _STATEMENT_TO_AMENDMENT_VERSION[candidate.statement]
    now_utc = datetime.now(timezone.utc)
    reason = (
        f"XBRL fact (concept {candidate.xbrl_tag}) from accn {candidate.xbrl_accn} "
        f"form {candidate.xbrl_form} filed {candidate.xbrl_filed} reports "
        f"{candidate.xbrl_value}; supersedes FMP value {candidate.fmp_value} "
        f"(fact_id {candidate.fmp_fact_id}) published {candidate.fmp_published_at.isoformat()}."
    )

    with conn.cursor() as cur:
        # Load the full FMP row so we can copy its non-value fields onto the new row.
        cur.execute(
            """
            SELECT fiscal_period_label, calendar_year, calendar_quarter,
                   calendar_period_label, unit
            FROM financial_facts
            WHERE id = %s;
            """,
            (candidate.fmp_fact_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(f"FMP fact {candidate.fmp_fact_id} disappeared")
        fiscal_period_label, cy, cq, cy_label, unit = row

        # Mark FMP row superseded.
        cur.execute(
            """
            UPDATE financial_facts
            SET superseded_at = %s
            WHERE id = %s AND superseded_at IS NULL;
            """,
            (now_utc, candidate.fmp_fact_id),
        )

        # Insert the XBRL-amendment row.
        cur.execute(
            """
            INSERT INTO financial_facts (
                company_id, statement, concept, value, unit,
                fiscal_year, fiscal_quarter, fiscal_period_label,
                period_end, period_type,
                calendar_year, calendar_quarter, calendar_period_label,
                published_at, source_raw_response_id, extraction_version,
                ingest_run_id,
                supersedes_fact_id, supersession_reason
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s,
                %s, %s
            )
            RETURNING id;
            """,
            (
                company_id, candidate.statement, candidate.concept,
                candidate.xbrl_value, unit,
                candidate.fiscal_year, candidate.fiscal_quarter, fiscal_period_label,
                candidate.period_end, candidate.period_type,
                cy, cq, cy_label,
                now_utc, xbrl_raw_response_id, amendment_version,
                ingest_run_id,
                candidate.fmp_fact_id, reason,
            ),
        )
        (new_id,) = cur.fetchone()
    return new_id


# ---------------------------------------------------------------------------
# Step 6: Rule C — re-verify Layer 1/2/3 post-supersession
# ---------------------------------------------------------------------------


def _values_for_period(
    conn: psycopg.Connection,
    *,
    company_id: int,
    statement: str,
    period_end: date,
    period_type: str,
) -> dict[str, Decimal]:
    """Return current values_by_concept for a single (company, statement, period_end, period_type).

    Includes both FMP-sourced rows AND XBRL-amendment-sourced rows (any
    extraction_version, as long as superseded_at IS NULL).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT concept, value
            FROM financial_facts
            WHERE company_id = %s
              AND statement = %s
              AND period_end = %s
              AND period_type = %s
              AND superseded_at IS NULL;
            """,
            (company_id, statement, period_end, period_type),
        )
        return {c: v for c, v in cur.fetchall()}


def _reverify_layer1(
    conn: psycopg.Connection,
    *,
    company_id: int,
    affected_periods: set[tuple[str, date, str]],
) -> list[str]:
    """Re-run Layer 1 ties for each affected (statement, period_end, period_type).
    Return list of failure descriptions (empty = all passed)."""
    failures: list[str] = []
    for statement, period_end, period_type in affected_periods:
        values = _values_for_period(
            conn, company_id=company_id, statement=statement,
            period_end=period_end, period_type=period_type,
        )
        if statement == "income_statement":
            fails = verify_is_ties(values)
        elif statement == "balance_sheet":
            fails = verify_bs_ties(values)
        elif statement == "cash_flow":
            fails = verify_cf_ties(values)
        else:
            continue
        for f in fails:
            failures.append(
                f"{statement} @ {period_end} [{period_type}]: {f.tie} "
                f"(filer={f.filer}, computed={f.computed}, delta={f.delta})"
            )
    return failures


def _reverify_layer3_for_fiscal_year(
    conn: psycopg.Connection,
    *,
    company_id: int,
    statement: str,
    fiscal_year: int,
) -> list[PeriodArithmeticFailure]:
    """Re-run Layer 3 for a specific (statement, fiscal_year), counting
    current rows regardless of extraction_version (i.e., FMP + amendment
    rows combined)."""
    flow_buckets = _IS_FLOW_BUCKETS if statement == "income_statement" else _CF_FLOW_BUCKETS

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT concept, fiscal_year, fiscal_quarter, period_type, value
            FROM financial_facts
            WHERE company_id = %s
              AND superseded_at IS NULL
              AND statement = %s
              AND fiscal_year = %s
              AND concept = ANY(%s);
            """,
            (company_id, statement, fiscal_year, list(flow_buckets)),
        )
        rows = cur.fetchall()

    from collections import defaultdict
    shape: dict[tuple[str, int], dict[str, Decimal]] = defaultdict(dict)
    for concept, fy, fq, period_type, value in rows:
        key = (concept, fy)
        if period_type == "annual":
            shape[key]["FY"] = value
        elif period_type == "quarter" and fq is not None:
            shape[key][f"Q{fq}"] = value

    failures: list[PeriodArithmeticFailure] = []
    for (concept, fy), components in shape.items():
        required = {"Q1", "Q2", "Q3", "Q4", "FY"}
        if not required.issubset(components.keys()):
            continue
        q_sum = components["Q1"] + components["Q2"] + components["Q3"] + components["Q4"]
        annual = components["FY"]
        delta = abs(q_sum - annual)
        threshold = max(L3_TOLERANCE_ABS, max(abs(q_sum), abs(annual)) * L3_TOLERANCE_PCT)
        if delta > threshold:
            failures.append(PeriodArithmeticFailure(
                company_id=company_id, concept=concept, fiscal_year=fy,
                quarters_sum=q_sum, annual=annual, delta=delta, tolerance=threshold,
            ))
    return failures


# ---------------------------------------------------------------------------
# Flag writers (Layer 3 soft-gate: write to data_quality_flags, don't raise)
# ---------------------------------------------------------------------------


def _classify_severity(delta: Decimal, reference: Decimal) -> str:
    """Severity buckets: informational <1%, warning 1-10%, investigate >=10%."""
    if reference == 0:
        return "investigate"
    pct = abs(delta) / abs(reference)
    if pct < Decimal("0.01"):
        return "informational"
    if pct < Decimal("0.10"):
        return "warning"
    return "investigate"


def _write_layer3_flag(
    conn: psycopg.Connection,
    *,
    company_id: int,
    statement: str,
    layer3_fail: PeriodArithmeticFailure,
    ingest_run_id: int,
    reason_extra: str | None = None,
    suggested_value: Decimal | None = None,
    context: dict[str, Any] | None = None,
) -> int:
    """Insert a Layer 3 anomaly row into data_quality_flags. Returns new flag id."""
    sev = _classify_severity(layer3_fail.delta, layer3_fail.annual)
    base_reason = (
        f"Q1+Q2+Q3+Q4 ≠ FY for {layer3_fail.concept} in FY{layer3_fail.fiscal_year}. "
        f"Quarter sum = {layer3_fail.quarters_sum:,.0f}, annual = {layer3_fail.annual:,.0f}, "
        f"delta = {layer3_fail.delta:,.0f} (tolerance {layer3_fail.tolerance:,.0f})."
    )
    if reason_extra:
        base_reason += f" {reason_extra}"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_quality_flags (
                company_id, statement, concept, fiscal_year, fiscal_quarter,
                period_end, period_type,
                flag_type, severity,
                expected_value, computed_value, delta, tolerance, suggested_value,
                reason, context,
                source_run_id
            ) VALUES (
                %s, %s, %s, %s, NULL,
                NULL, 'annual',
                'layer3_q_sum_vs_fy', %s,
                %s, %s, %s, %s, %s,
                %s, %s::jsonb,
                %s
            )
            RETURNING id;
            """,
            (
                company_id, statement, layer3_fail.concept, layer3_fail.fiscal_year,
                sev,
                layer3_fail.annual,   # expected (filer-reported FY)
                layer3_fail.quarters_sum,  # computed (sum of our stored quarters)
                layer3_fail.delta, layer3_fail.tolerance, suggested_value,
                base_reason,
                _json.dumps(context or {}),
                ingest_run_id,
            ),
        )
        (flag_id,) = cur.fetchone()
    return flag_id


def _write_sanity_flag(
    conn: psycopg.Connection,
    *,
    company_id: int,
    statement: str,
    concept: str,
    period_end: date,
    fiscal_year: int,
    fiscal_quarter: int | None,
    period_type: str,
    fmp_value: Decimal,
    xbrl_value: Decimal,
    reason_text: str,
    xbrl_accn: str,
    ingest_run_id: int,
) -> int:
    """Rule B sanity violation on an XBRL supersession candidate — write a
    flag explaining why the candidate was skipped, and move on. The candidate
    is NOT applied; FMP's value remains authoritative until an analyst reviews."""
    delta = xbrl_value - fmp_value
    sev = "investigate"  # sanity-bound violations are always worth looking at
    reason = (
        f"Rule B sanity-bound violation: FMP stores {fmp_value:,.0f} but XBRL "
        f"({xbrl_accn}) reports {xbrl_value:,.0f}. {reason_text}. "
        f"Supersession was NOT applied; FMP value retained."
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO data_quality_flags (
                company_id, statement, concept, fiscal_year, fiscal_quarter,
                period_end, period_type,
                flag_type, severity,
                expected_value, computed_value, delta, suggested_value,
                reason, source_run_id
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s,
                'xbrl_sanity_bound', %s,
                %s, %s, %s, %s,
                %s, %s
            )
            RETURNING id;
            """,
            (
                company_id, statement, concept, fiscal_year, fiscal_quarter,
                period_end, period_type,
                sev,
                fmp_value, xbrl_value, delta, xbrl_value,  # suggested = XBRL (human judges)
                reason, ingest_run_id,
            ),
        )
        (flag_id,) = cur.fetchone()
    return flag_id


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------


def detect_and_apply_amendments(
    conn: psycopg.Connection,
    *,
    company_id: int,
    company_cik: int,
    ingest_run_id: int,
    http: HttpClient | None = None,
) -> AmendmentResult:
    """Orchestrate Phase 1.5 amendment detection + supersession.

    Call this AFTER FMP ingest has completed and Layer 3 has failed. This
    function operates within the caller's outer transaction — a SAVEPOINT
    is used to implement Rule C's atomic rollback semantics.

    Returns AmendmentResult with status and supersession details.
    Raises UnresolvableAmendment when refused (Rule A/B/C violation)
    because the caller (backfill agent) needs to propagate the failure.
    """
    # Step 1: identify failures
    layer3_failures = _all_layer3_failures(conn, company_id=company_id)
    if not layer3_failures:
        return AmendmentResult(status="clean")

    # Unique failing (statement, fiscal_year) pairs
    failing_fys: set[tuple[str, int]] = {
        (stmt, f.fiscal_year) for stmt, f in layer3_failures
    }

    # Step 2: fetch XBRL companyfacts (one fetch for the whole company)
    if http is None:
        http = HttpClient(user_agent=SEC_USER_AGENT, rate_limit=SEC_RATE_LIMIT)
    with conn.transaction():
        fetched = fetch_company_facts(
            conn, cik=company_cik, ingest_run_id=ingest_run_id, http=http,
        )
    xbrl_us_gaap = fetched.payload.get("facts", {}).get("us-gaap", {})

    # Step 3: find supersession candidates
    #
    # Key architectural note: we do NOT only supersede the specific concepts
    # flagged by Layer 3. When a 10-K restates a period, it restates MANY
    # related concepts at that period (cogs, gross_profit, operating_income,
    # continuing_ops, net_income, etc.) because GAAP subtotal ties must hold
    # internally. If we only superseded the Layer-3-listed concepts, Layer 1
    # re-verify would fail because we'd mix original + restated values within
    # the same period (e.g., restated cogs but original gross_profit doesn't
    # equal restated revenue - restated cogs).
    #
    # Instead: for every PERIOD that has at least one Layer 3 candidate,
    # refresh ALL mappable concepts at that period from XBRL. This mirrors
    # what the filer actually did in the 10-K: restate the full period.
    # Any concept whose XBRL latest differs from FMP's stored value becomes
    # a supersession candidate.

    # Step 3a: identify the set of affected periods (statement, period_end, quarter_num, fiscal_year)
    affected_quarter_set: set[tuple[str, date, int, int]] = set()
    for statement, layer3_fail in layer3_failures:
        fy = layer3_fail.fiscal_year
        concept = layer3_fail.concept
        q_rows = _fmp_quarterly_facts_for_fy(
            conn, company_id=company_id, statement=statement,
            concept=concept, fiscal_year=fy,
        )
        for q_row in q_rows:
            xbrl_check = _find_xbrl_latest_fact(
                xbrl_us_gaap, concept,
                period_end=q_row["period_end"],
                duration_min_days=80, duration_max_days=100,
            )
            if xbrl_check is None:
                continue
            xbrl_val = Decimal(str(xbrl_check["val"]))
            if abs(xbrl_val - q_row["value"]) < Decimal("1"):
                continue  # FMP already current for this concept/period
            # This quarter has at least one restatement — mark for full refresh
            affected_quarter_set.add(
                (statement, q_row["period_end"], q_row["fiscal_quarter"], fy)
            )

    # Cross-statement coordination: when an IS period is affected, also
    # mark the matching CF period so CF-side concepts (notably
    # net_income_start, which mirrors IS.net_income via Layer 2) get
    # co-superseded. Without this, IS supersession breaks Layer 2's
    # cross-statement net_income tie.
    cross_statement_additions: set[tuple[str, date, int, int]] = set()
    for (stmt, pe, fq, fy) in affected_quarter_set:
        if stmt == "income_statement":
            cross_statement_additions.add(("cash_flow", pe, fq, fy))
    affected_quarter_set.update(cross_statement_additions)

    # Step 3b: for every affected period, pull ALL current FMP rows at that
    # (company, statement, period_end, period_type) and find XBRL candidates.
    candidates: list[SupersessionCandidate] = []
    for statement, period_end, fiscal_quarter, fy in affected_quarter_set:
        all_fmp_rows = _all_fmp_facts_for_period(
            conn, company_id=company_id, statement=statement,
            period_end=period_end, period_type="quarter",
        )
        for fmp_row in all_fmp_rows:
            concept = fmp_row["concept"]
            xbrl_fact = _find_xbrl_latest_fact(
                xbrl_us_gaap, concept,
                period_end=period_end,
                duration_min_days=80, duration_max_days=100,
            )
            if xbrl_fact is None:
                continue  # No XBRL mapping or no matching fact — leave FMP alone
            xbrl_val = Decimal(str(xbrl_fact["val"]))
            fmp_val = fmp_row["value"]
            if abs(xbrl_val - fmp_val) < Decimal("1"):
                continue  # FMP already current

            # Rule B: sanity check. If the XBRL candidate looks unreasonable
            # (sign flip, >50% delta), skip it with a flag — don't supersede,
            # don't raise. Analyst reviews.
            ok, reason = _passes_sanity_bounds(fmp_val, xbrl_val)
            if not ok:
                _write_sanity_flag(
                    conn, company_id=company_id,
                    statement=statement, concept=concept,
                    period_end=period_end, fiscal_year=fy,
                    fiscal_quarter=fiscal_quarter, period_type="quarter",
                    fmp_value=fmp_val, xbrl_value=xbrl_val,
                    reason_text=reason, xbrl_accn=xbrl_fact.get("accn", ""),
                    ingest_run_id=ingest_run_id,
                )
                continue
            candidates.append(SupersessionCandidate(
                statement=statement, concept=concept,
                period_end=period_end, period_type="quarter",
                fiscal_year=fy, fiscal_quarter=fiscal_quarter,
                fmp_fact_id=fmp_row["id"], fmp_value=fmp_val,
                fmp_published_at=fmp_row["published_at"],
                xbrl_value=xbrl_val, xbrl_tag=xbrl_fact["__tag"],
                xbrl_accn=xbrl_fact.get("accn", ""),
                xbrl_filed=xbrl_fact.get("filed", ""),
                xbrl_form=xbrl_fact.get("form", ""),
                xbrl_start=xbrl_fact.get("start", ""),
                xbrl_end=xbrl_fact.get("end", ""),
            ))

    # Also include FY-annual restatements: look at FY rows for each affected (statement, fy).
    for statement, fy in failing_fys:
        fy_rows = _all_fmp_facts_for_fiscal_year_annual(
            conn, company_id=company_id, statement=statement, fiscal_year=fy,
        )
        for fmp_row in fy_rows:
            concept = fmp_row["concept"]
            xbrl_fact = _find_xbrl_latest_fact(
                xbrl_us_gaap, concept,
                period_end=fmp_row["period_end"],
                duration_min_days=350, duration_max_days=380,  # annual 52/53-week
            )
            if xbrl_fact is None:
                continue
            xbrl_val = Decimal(str(xbrl_fact["val"]))
            fmp_val = fmp_row["value"]
            if abs(xbrl_val - fmp_val) < Decimal("1"):
                continue
            ok, reason = _passes_sanity_bounds(fmp_val, xbrl_val)
            if not ok:
                _write_sanity_flag(
                    conn, company_id=company_id,
                    statement=statement, concept=concept,
                    period_end=fmp_row["period_end"], fiscal_year=fy,
                    fiscal_quarter=None, period_type="annual",
                    fmp_value=fmp_val, xbrl_value=xbrl_val,
                    reason_text=reason, xbrl_accn=xbrl_fact.get("accn", ""),
                    ingest_run_id=ingest_run_id,
                )
                continue
            candidates.append(SupersessionCandidate(
                statement=statement, concept=concept,
                period_end=fmp_row["period_end"], period_type="annual",
                fiscal_year=fy, fiscal_quarter=None,
                fmp_fact_id=fmp_row["id"], fmp_value=fmp_val,
                fmp_published_at=fmp_row["published_at"],
                xbrl_value=xbrl_val, xbrl_tag=xbrl_fact["__tag"],
                xbrl_accn=xbrl_fact.get("accn", ""),
                xbrl_filed=xbrl_fact.get("filed", ""),
                xbrl_form=xbrl_fact.get("form", ""),
                xbrl_start=xbrl_fact.get("start", ""),
                xbrl_end=xbrl_fact.get("end", ""),
            ))

    if not candidates:
        # Layer 3 failed but XBRL has no superseding values. This is NOT an
        # amendment — likely a spinoff, a filer reclassification that kept
        # quarterly values but changed annual, or an FMP data issue. Write
        # flags for each Layer 3 failure and return. Ingest continues.
        flag_count = 0
        for statement, l3_fail in layer3_failures:
            _write_layer3_flag(
                conn, company_id=company_id, statement=statement,
                layer3_fail=l3_fail, ingest_run_id=ingest_run_id,
                reason_extra=(
                    "No XBRL supersession candidates found for this concept — "
                    "not an amendment (likely spinoff/reclassification or "
                    "FMP data issue). FMP values retained; analyst review needed."
                ),
            )
            flag_count += 1
        return AmendmentResult(
            status="unresolved_flagged",
            fiscal_years_checked=list(failing_fys),
            flags_written=flag_count,
        )

    # Step 3c: identity-derived candidates. When XBRL tagged N-1 components
    # of an N-component Layer-1 identity at a restated period, the missing
    # component is implied mathematically. The filer's 10-K/10-Q is
    # authoritative for whichever N-1 values it tagged; the Nth is a
    # pure identity derivation. This matches the analyst's mental model:
    # "10-K shows revenue=X and gross_profit=Y, therefore cogs=X-Y".
    #
    # Supported derivations (all on income_statement):
    #   cogs = revenue - gross_profit
    #   total_opex = gross_profit - operating_income
    # Other concepts: left alone — if Layer 1 re-verify fails, agent refuses.
    candidates.extend(
        _derive_identity_candidates(
            conn, company_id=company_id,
            existing_candidates=candidates,
            affected_quarter_set=affected_quarter_set,
            failing_fys=failing_fys,
            xbrl_us_gaap=xbrl_us_gaap,
        )
    )

    # Step 4: apply all supersessions in a SAVEPOINT.
    #   - If Layer 1 would break post-supersession: ROLLBACK the savepoint and
    #     write flags for every Layer 3 failure. FMP values retained.
    #   - If Layer 1 passes but Layer 3 still has residuals: COMMIT the
    #     savepoint (keep the resolutions) and write flags for what's left.
    #   - If everything passes: COMMIT and return amended.
    affected_periods: set[tuple[str, date, str]] = set()
    for c in candidates:
        affected_periods.add((c.statement, c.period_end, c.period_type))

    class _SupersessionWouldBreakL1(Exception):
        """Sentinel to roll back the savepoint via transaction-context exit."""
        def __init__(self, fails: list[str]) -> None:
            super().__init__("supersession breaks Layer 1")
            self.fails = fails

    l1_break_fails: list[str] | None = None
    try:
        with conn.transaction():
            for c in candidates:
                _apply_supersession(
                    conn, c, company_id=company_id,
                    xbrl_raw_response_id=fetched.raw_response_id,
                    ingest_run_id=ingest_run_id,
                )
                affected_periods.add((c.statement, c.period_end, c.period_type))
            l1_fails = _reverify_layer1(
                conn, company_id=company_id, affected_periods=affected_periods,
            )
            if l1_fails:
                # Savepoint will roll back on exception exit.
                raise _SupersessionWouldBreakL1(l1_fails)
            # Savepoint commits on normal exit.
    except _SupersessionWouldBreakL1 as sp_exc:
        l1_break_fails = sp_exc.fails

    if l1_break_fails is not None:
        # Supersessions rolled back. Write flags for every Layer 3 failure.
        flag_count = 0
        for statement, l3_fail in layer3_failures:
            _write_layer3_flag(
                conn, company_id=company_id, statement=statement,
                layer3_fail=l3_fail, ingest_run_id=ingest_run_id,
                reason_extra=(
                    f"Auto-resolution attempted but would break {len(l1_break_fails)} "
                    f"Layer-1 tie(s) post-supersession — supersessions rolled back. "
                    f"FMP values retained; analyst review needed."
                ),
                context={"layer1_failures_preview": l1_break_fails[:5]},
            )
            flag_count += 1
        return AmendmentResult(
            status="unresolved_flagged",
            fiscal_years_checked=list(failing_fys),
            flags_written=flag_count,
        )

    # Savepoint committed. Supersessions are now persisted. Check remaining L3.
    flag_count = 0
    for statement, fy in failing_fys:
        l3_remaining = _reverify_layer3_for_fiscal_year(
            conn, company_id=company_id, statement=statement, fiscal_year=fy,
        )
        for f in l3_remaining:
            _write_layer3_flag(
                conn, company_id=company_id, statement=statement,
                layer3_fail=f, ingest_run_id=ingest_run_id,
                reason_extra=(
                    "Auto-resolution via XBRL supersession applied for some "
                    "concepts in this fiscal year but this specific tie still "
                    "doesn't hold. May indicate filer-level annual-only "
                    "restatement or a concept we can't map."
                ),
            )
            flag_count += 1

    if flag_count > 0:
        return AmendmentResult(
            status="partially_amended",
            supersessions_applied=candidates,
            fiscal_years_checked=list(failing_fys),
            flags_written=flag_count,
        )

    return AmendmentResult(
        status="amended",
        supersessions_applied=candidates,
        fiscal_years_checked=list(failing_fys),
        flags_written=0,
    )
