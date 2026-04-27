"""Audit-and-promote orchestration: FMP-vs-XBRL reconciliation in normal flow.

After FMP backfill writes its facts, this step compares them to SEC XBRL
companyfacts and auto-promotes the cells where:

  - the XBRL value is directly tagged in a filing (not audit-derived),
  - the concept is unambiguous (not definitional-prone like total_equity
    or ebt_incl_unusual),
  - the gap is material but not so wild that basis-mismatch is more
    likely than corruption,
  - the fiscal year is recent enough that older XBRL tagging variability
    isn't muddying the signal.

For each promoted cell: the FMP row is superseded with reason
``xbrl-disagrees`` and a new row at extraction_version
``xbrl-amendment-{is|bs|cf}-v1`` is inserted. The wide view automatically
preferences XBRL-amendment over FMP downstream.

Divergences that don't pass the safety filters are *kept* in
``ingest_runs.error_details.divergences`` for the steward check
``xbrl_audit_unresolved`` to surface as findings for analyst review.

This is the architecturally-designed answer to "is FMP correct" — XBRL is
authoritative; trust-but-verify with continuous comparison.

Usage (library):
    from arrow.agents.xbrl_audit import audit_and_promote_xbrl
    counts = audit_and_promote_xbrl(conn, ["DELL"])
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import psycopg

from arrow.agents.fmp_reconcile import reconcile_fmp_vs_xbrl
from arrow.ingest.common.runs import close_succeeded, open_run
from arrow.normalize.financials.verify_bs import (
    verify_bs_hard_ties,
    verify_bs_soft_ties,
)
from arrow.normalize.financials.verify_cf import (
    verify_cf_hard_ties,
    verify_cf_soft_ties,
)
from arrow.normalize.financials.verify_is import verify_is_ties


STATEMENT_TO_AMENDMENT_VERSION = {
    "income_statement": "xbrl-amendment-is-v1",
    "balance_sheet": "xbrl-amendment-bs-v1",
    "cash_flow": "xbrl-amendment-cf-v1",
}

SOURCE_VERSIONS = ("fmp-is-v1", "fmp-bs-v1", "fmp-cf-v1")

#: Concepts where the XBRL ↔ FMP gap is most often definitional, not
#: corruption (e.g., total_equity differs by minority-interest treatment).
#: Auto-promotion skips these.
DEFINITIONAL_PRONE_CONCEPTS = frozenset({
    "total_equity",
    "total_liabilities",
    "ebt_incl_unusual",
    "cash_and_equivalents",
    "total_liabilities_and_equity",
    "total_assets",
})

#: Concepts where the XBRL tag is unambiguous and a material gap signals
#: real disagreement. Only these are eligible for auto-promotion.
CORRUPTION_PRIMARY_CONCEPTS = frozenset({
    "revenue",
    "gross_profit",
    "operating_income",
    "net_income",
    "net_income_attributable_to_parent",
    "cfo",
    "cfi",
    "cff",
})

#: Default safety filter thresholds. Calibrated from the 2026-04-27 audit
#: across 15 tickers — these values capture small recent-year restatement
#: adjustments without triggering on basis-mismatch artifacts (DELL
#: post-VMWare-spinoff being the canonical hard case).
DEFAULT_REQUIRE_DIRECT = True
DEFAULT_MIN_FISCAL_YEAR = 2022
DEFAULT_MAX_RELATIVE_GAP = 0.25
DEFAULT_MIN_ABSOLUTE_GAP = 10_000_000

#: Flag type written to data_quality_flags when an XBRL promotion leaves
#: a Layer 1 tie broken (cross-statement arithmetic inconsistency between
#: the new XBRL anchor and the still-FMP-sourced component values).
TIE_BREAK_FLAG_TYPE = "xbrl_promotion_tie_break"


def _is_safe_for_auto_promotion(
    d: dict,
    *,
    require_direct: bool,
    min_fiscal_year: int,
    max_relative_gap: float,
    min_absolute_gap: float,
) -> tuple[bool, str]:
    """Return (ok, reason). reason is a short explanation when ok=False."""
    if require_direct and d.get("derivation") != "direct":
        return False, "non-direct XBRL derivation (audit-side calculation)"
    if d["fiscal_year"] < min_fiscal_year:
        return False, f"older than FY{min_fiscal_year}"
    if d["concept"] in DEFINITIONAL_PRONE_CONCEPTS:
        return False, f"{d['concept']} is definitional-prone"
    if d["concept"] not in CORRUPTION_PRIMARY_CONCEPTS:
        return False, f"{d['concept']} not in auto-promote allowlist"
    abs_delta = abs(float(d["delta"]))
    if abs_delta < min_absolute_gap:
        return False, f"gap ${abs_delta:,.0f} below ${min_absolute_gap:,.0f}"
    xbrl = float(d["xbrl_value"])
    rel_gap = abs_delta / abs(xbrl) if xbrl else 1.0
    if rel_gap >= max_relative_gap:
        return False, f"gap {rel_gap:.0%} ≥ {max_relative_gap:.0%} (basis-mismatch likely)"
    return True, ""


def _fetch_company_id_and_cik(conn: psycopg.Connection, ticker: str) -> tuple[int, int] | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, cik FROM companies WHERE ticker = %s",
            (ticker.upper(),),
        )
        row = cur.fetchone()
        return (row[0], row[1]) if row else None


def _fetch_xbrl_raw_response_id(conn: psycopg.Connection, cik: int, audit_run_id: int) -> int | None:
    """Find the XBRL companyfacts raw_response written by this audit run."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id FROM raw_responses
            WHERE vendor='sec'
              AND endpoint LIKE %s
              AND (params->>'cik')::int = %s
              AND ingest_run_id = %s
            ORDER BY fetched_at DESC LIMIT 1
            """,
            ("api/xbrl/companyfacts/%", cik, audit_run_id),
        )
        row = cur.fetchone()
        return row[0] if row else None


def _find_fmp_fact(conn: psycopg.Connection, *, company_id: int, divergence: dict) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, value, unit, fiscal_year, fiscal_quarter, fiscal_period_label,
                   calendar_year, calendar_quarter, calendar_period_label,
                   period_end, extraction_version
            FROM financial_facts
            WHERE company_id = %s
              AND statement = %s
              AND concept = %s
              AND period_end = %s
              AND period_type = %s
              AND superseded_at IS NULL
              AND dimension_type IS NULL
              AND extraction_version = ANY(%s)
            LIMIT 1
            """,
            (
                company_id, divergence["statement"], divergence["concept"],
                divergence["period_end"], divergence["period_type"],
                list(SOURCE_VERSIONS),
            ),
        )
        row = cur.fetchone()
        if row is None:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))


def _promote_one(
    conn: psycopg.Connection,
    *,
    promotion_run_id: int,
    company_id: int,
    xbrl_raw_id: int,
    divergence: dict,
) -> bool:
    """Apply XBRL promotion for one divergence. Return True if applied."""
    fmp_row = _find_fmp_fact(conn, company_id=company_id, divergence=divergence)
    if fmp_row is None:
        return False

    xbrl_filed = divergence.get("xbrl_filed")
    published_at = (
        datetime.strptime(xbrl_filed, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        if xbrl_filed else datetime.now(timezone.utc)
    )
    accn = divergence.get("xbrl_accn", "?")
    reason = (
        f"xbrl-disagrees: accn {accn}, filed {xbrl_filed}; "
        f"FMP={divergence['fmp_value']} XBRL={divergence['xbrl_value']}"
    )
    amendment_version = STATEMENT_TO_AMENDMENT_VERSION[divergence["statement"]]

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE financial_facts
            SET superseded_at = %s,
                supersession_reason = %s
            WHERE id = %s AND superseded_at IS NULL
            """,
            (published_at, reason, fmp_row["id"]),
        )
        cur.execute(
            """
            INSERT INTO financial_facts (
                ingest_run_id, company_id, statement, concept, value, unit,
                fiscal_year, fiscal_quarter, fiscal_period_label,
                period_end, period_type,
                calendar_year, calendar_quarter, calendar_period_label,
                published_at, source_raw_response_id, extraction_version,
                supersedes_fact_id, supersession_reason
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s
            )
            """,
            (
                promotion_run_id, company_id, divergence["statement"], divergence["concept"],
                Decimal(divergence["xbrl_value"]), fmp_row["unit"],
                fmp_row["fiscal_year"], fmp_row["fiscal_quarter"], fmp_row["fiscal_period_label"],
                fmp_row["period_end"], divergence["period_type"],
                fmp_row["calendar_year"], fmp_row["calendar_quarter"], fmp_row["calendar_period_label"],
                published_at, xbrl_raw_id, amendment_version,
                fmp_row["id"], reason,
            ),
        )
    return True


def _fetch_current_values_for_period(
    conn: psycopg.Connection,
    *,
    company_id: int,
    period_end: date,
    period_type: str,
    statement: str,
) -> tuple[dict[str, Decimal], int | None, int | None, str | None]:
    """Pull the current value for each concept in a (statement, period).

    Includes both fmp-* and xbrl-amendment-* current rows; if both
    coexist somehow, the lower extraction_version-rank wins (xbrl-amendment
    over fmp). Returns (values_by_concept, fiscal_year, fiscal_quarter, fiscal_period_label).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT concept, value, fiscal_year, fiscal_quarter, fiscal_period_label,
                   extraction_version
            FROM financial_facts
            WHERE company_id = %s
              AND statement = %s
              AND period_end = %s
              AND period_type = %s
              AND superseded_at IS NULL
              AND dimension_type IS NULL
            """,
            (company_id, statement, period_end, period_type),
        )
        rows = cur.fetchall()

    if not rows:
        return {}, None, None, None

    # Rank lower = preferred (matches v_company_period_wide rule)
    def rank(ev: str) -> int:
        if ev.startswith("human-verified"):
            return 1
        if ev.startswith("xbrl-amendment-"):
            return 2
        if ev.startswith("fmp-"):
            return 3
        return 9

    by_concept: dict[str, tuple[int, Decimal]] = {}
    fiscal_year = fiscal_quarter = fiscal_period_label = None
    for concept, value, fy, fq, fpl, ev in rows:
        r = rank(ev)
        cur_best = by_concept.get(concept)
        if cur_best is None or r < cur_best[0]:
            by_concept[concept] = (r, value)
        if fiscal_year is None:
            fiscal_year, fiscal_quarter, fiscal_period_label = fy, fq, fpl
    return ({c: v for c, (_, v) in by_concept.items()}, fiscal_year, fiscal_quarter, fiscal_period_label)


def _write_tie_break_flag(
    conn: psycopg.Connection,
    *,
    company_id: int,
    statement: str,
    fiscal_year: int,
    fiscal_quarter: int | None,
    period_end: date,
    period_type: str,
    failure: Any,
    promotion_run_id: int,
) -> None:
    """Write a soft data_quality_flag when an XBRL promotion leaves a tie broken.

    Idempotent: same (company, statement, period_end, period_type, flag_type, tie)
    won't double-write because we check first. We don't have a unique
    constraint on data_quality_flags so this is a SELECT-then-INSERT pattern.
    """
    tie = getattr(failure, "tie", "?")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM data_quality_flags
            WHERE company_id = %s
              AND statement = %s
              AND period_end = %s
              AND period_type = %s
              AND flag_type = %s
              AND context->>'tie' = %s
              AND resolved_at IS NULL
            LIMIT 1
            """,
            (company_id, statement, period_end, period_type,
             TIE_BREAK_FLAG_TYPE, tie),
        )
        if cur.fetchone():
            return  # already flagged

        reason = (
            f"XBRL anchor promotion left this tie broken: {tie}. "
            f"Filer-reported (post-promotion) value vs computed-from-components "
            f"differ by {failure.delta} (tolerance {failure.tolerance}). "
            f"Likely cause: an XBRL-amended anchor (e.g. operating_income) no "
            f"longer matches the still-FMP-sourced component values that "
            f"originally tied to the FMP-anchor. Investigate which side to "
            f"trust for cross-statement consistency."
        )
        cur.execute(
            """
            INSERT INTO data_quality_flags (
                company_id, statement, concept,
                fiscal_year, fiscal_quarter, period_end, period_type,
                flag_type, severity,
                expected_value, computed_value, delta, tolerance,
                reason, context, source_run_id
            ) VALUES (
                %s, %s, NULL,
                %s, %s, %s, %s,
                %s, 'warning',
                %s, %s, %s, %s,
                %s, %s::jsonb, %s
            );
            """,
            (
                company_id, statement,
                fiscal_year, fiscal_quarter, period_end, period_type,
                TIE_BREAK_FLAG_TYPE,
                failure.filer, failure.computed, failure.delta, failure.tolerance,
                reason,
                f'{{"tie": "{tie}"}}',
                promotion_run_id,
            ),
        )


def verify_period_after_promotion(
    conn: psycopg.Connection,
    *,
    company_id: int,
    period_end: date,
    period_type: str,
    promotion_run_id: int,
    statements: tuple[str, ...] = ("income_statement", "balance_sheet", "cash_flow"),
) -> dict[str, int]:
    """Re-run Layer 1 ties for the statements actually promoted in this period.

    Writes a soft data_quality_flag for each new tie failure. Doesn't raise
    on hard-tie failures — promotion is already committed and we want the
    analyst to see the inconsistency, not roll back.

    Pass ``statements`` to limit verification to only the statements that
    had XBRL-amendments in this period (avoids attributing pre-existing
    FMP drift to a promotion that didn't touch that statement).

    Returns counts of failures per statement.
    """
    counts = {"is_failures": 0, "bs_failures": 0, "cf_failures": 0}

    verifier_map = {
        "income_statement": (verify_is_ties,),
        "balance_sheet": (verify_bs_soft_ties, verify_bs_hard_ties),
        "cash_flow": (verify_cf_soft_ties, verify_cf_hard_ties),
    }
    for statement in statements:
        verifiers = verifier_map.get(statement)
        if verifiers is None:
            continue
        values, fy, fq, _fpl = _fetch_current_values_for_period(
            conn, company_id=company_id, period_end=period_end,
            period_type=period_type, statement=statement,
        )
        if not values or fy is None:
            continue
        failures: list = []
        for verify_fn in verifiers:
            failures.extend(verify_fn(values))
        for failure in failures:
            _write_tie_break_flag(
                conn,
                company_id=company_id, statement=statement,
                fiscal_year=fy, fiscal_quarter=fq,
                period_end=period_end, period_type=period_type,
                failure=failure, promotion_run_id=promotion_run_id,
            )
        if statement == "income_statement":
            counts["is_failures"] = len(failures)
        elif statement == "balance_sheet":
            counts["bs_failures"] = len(failures)
        elif statement == "cash_flow":
            counts["cf_failures"] = len(failures)
    return counts


def audit_and_promote_xbrl(
    conn: psycopg.Connection,
    tickers: list[str],
    *,
    actor: str = "operator",
    require_direct: bool = DEFAULT_REQUIRE_DIRECT,
    min_fiscal_year: int = DEFAULT_MIN_FISCAL_YEAR,
    max_relative_gap: float = DEFAULT_MAX_RELATIVE_GAP,
    min_absolute_gap: float = DEFAULT_MIN_ABSOLUTE_GAP,
) -> dict[str, Any]:
    """Run FMP↔XBRL reconciliation, auto-promote safe divergences.

    Returns counts including the audit run id, divergence totals, and
    promotion totals. Unsafe divergences are left in
    ``ingest_runs.error_details`` for the steward check
    ``xbrl_audit_unresolved`` to surface.
    """
    audit_counts = reconcile_fmp_vs_xbrl(conn, tickers)
    divergences = audit_counts.get("divergences", []) or []

    # Group divergences by ticker via company_id lookup
    company_by_id: dict[int, str] = {}
    for ticker in tickers:
        info = _fetch_company_id_and_cik(conn, ticker)
        if info:
            company_by_id[info[0]] = ticker.upper()

    audit_run_id = audit_counts["ingest_run_id"]

    safe_by_ticker: dict[str, list[dict]] = defaultdict(list)
    skipped_reasons: dict[str, int] = defaultdict(int)
    for d in divergences:
        ok, why = _is_safe_for_auto_promotion(
            d,
            require_direct=require_direct,
            min_fiscal_year=min_fiscal_year,
            max_relative_gap=max_relative_gap,
            min_absolute_gap=min_absolute_gap,
        )
        if not ok:
            skipped_reasons[why] += 1
            continue
        # Look up which ticker owns this divergence by company_id from financial_facts
        # (the divergence dict doesn't carry ticker; we resolve via FMP fact lookup)
        for company_id, ticker in company_by_id.items():
            fact = _find_fmp_fact(conn, company_id=company_id, divergence=d)
            if fact is not None:
                safe_by_ticker[ticker].append(d)
                break

    promoted = 0
    skipped_no_fmp_row = 0
    promotion_run_ids: list[int] = []

    for ticker, ticker_divs in safe_by_ticker.items():
        info = _fetch_company_id_and_cik(conn, ticker)
        if info is None:
            continue
        company_id, cik = info
        xbrl_raw_id = _fetch_xbrl_raw_response_id(conn, cik, audit_run_id)
        if xbrl_raw_id is None:
            # XBRL companyfacts wasn't cached under this audit run id —
            # fall back to most recent for this CIK.
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id FROM raw_responses
                    WHERE vendor='sec' AND endpoint LIKE %s
                      AND (params->>'cik')::int = %s
                    ORDER BY fetched_at DESC LIMIT 1
                    """,
                    ("api/xbrl/companyfacts/%", cik),
                )
                row = cur.fetchone()
                xbrl_raw_id = row[0] if row else None
        if xbrl_raw_id is None:
            continue

        promotion_run_id = open_run(
            conn, run_kind="manual", vendor="arrow", ticker_scope=[ticker],
        )
        promotion_run_ids.append(promotion_run_id)
        ticker_promoted = 0
        # (period_end, period_type) -> set of statements that were promoted.
        # We only re-verify the statements that actually got an XBRL anchor
        # promotion to avoid attributing pre-existing FMP drift to today's
        # promotion when the broken tie is in a statement we didn't touch.
        affected: dict[tuple[date, str], set[str]] = defaultdict(set)
        with conn.transaction():
            for d in ticker_divs:
                applied = _promote_one(
                    conn,
                    promotion_run_id=promotion_run_id,
                    company_id=company_id,
                    xbrl_raw_id=xbrl_raw_id,
                    divergence=d,
                )
                if applied:
                    ticker_promoted += 1
                    pe = (date.fromisoformat(d["period_end"])
                          if isinstance(d["period_end"], str) else d["period_end"])
                    affected[(pe, d["period_type"])].add(d["statement"])
                else:
                    skipped_no_fmp_row += 1

        # Post-promotion Layer 1 tie verification — runs in its own transaction
        # so any writes commit independently of the promotion writes (which
        # already committed via the with-block above). Scoped to the
        # statements actually promoted in each period.
        tie_break_counts = {"is_failures": 0, "bs_failures": 0, "cf_failures": 0}
        for (period_end, period_type), promoted_statements in affected.items():
            with conn.transaction():
                pc = verify_period_after_promotion(
                    conn,
                    company_id=company_id,
                    period_end=period_end,
                    period_type=period_type,
                    promotion_run_id=promotion_run_id,
                    statements=tuple(promoted_statements),
                )
                for k, v in pc.items():
                    tie_break_counts[k] += v

        close_succeeded(
            conn, promotion_run_id,
            counts={
                "is_facts_written": sum(1 for d in ticker_divs if d["statement"]=="income_statement"),
                "bs_facts_written": sum(1 for d in ticker_divs if d["statement"]=="balance_sheet"),
                "cf_facts_written": sum(1 for d in ticker_divs if d["statement"]=="cash_flow"),
                "is_facts_superseded": ticker_promoted,
                "ticker": ticker,
                "audit_run_id": audit_run_id,
                "tie_break_flags_is": tie_break_counts["is_failures"],
                "tie_break_flags_bs": tie_break_counts["bs_failures"],
                "tie_break_flags_cf": tie_break_counts["cf_failures"],
            },
        )
        promoted += ticker_promoted

    return {
        **audit_counts,
        "divergences_total": len(divergences),
        "divergences_promoted": promoted,
        "divergences_left_for_review": len(divergences) - promoted,
        "promotion_run_ids": promotion_run_ids,
        "skipped_reasons": dict(skipped_reasons),
        "skipped_no_fmp_row": skipped_no_fmp_row,
    }
