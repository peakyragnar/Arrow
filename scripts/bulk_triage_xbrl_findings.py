"""Bulk-triage xbrl_audit_unresolved findings into suppression categories.

The audit-and-promote layer auto-handles the unambiguous corruption
bucket. The residual findings on the steward queue need human triage,
but they fall into a few clear classes that can be bulk-suppressed
without filing-level review:

  1. audit_derivation_artifact — derivation='q4_derived_fy_minus_9m'.
     The audit derives Q4 as XBRL annual − 9-month YTD. For filers
     with a mid-year discontinued-ops split (DELL post-VMWare being
     the canonical hard case), annual was restated to continuing-only
     while the 9-month YTD remained consolidated, so the derivation
     is unreliable. FMP's value is no more wrong than the audit's
     derived XBRL. Don't auto-promote AND don't surface as a
     warning-severity finding.

  2. definitional_difference — concept is one where FMP and XBRL
     systematically encode different things:
       - total_equity vs StockholdersEquity (parent-only for NCI
         filers): gap = NCI value
       - total_liabilities (lease / accrual treatment)
       - ebt_incl_unusual (FMP includes unusual items, XBRL tag
         doesn't always)
       - cash_and_equivalents (equivalents-vs-investments boundary)
     These are documented mapping differences in
     `arrow.reconcile.xbrl_concepts`, not corruption.

  3. vendor_basis_mismatch — anything else NOT in (1) or (2): keep
     open for case-by-case analyst review against the actual filing.

Usage:
    uv run scripts/bulk_triage_xbrl_findings.py            # dry run
    uv run scripts/bulk_triage_xbrl_findings.py --apply
"""

from __future__ import annotations

import argparse
from collections import Counter

from arrow.db.connection import get_conn
from arrow.steward.actions import suppress_finding


DEFINITIONAL_CONCEPTS = frozenset({
    "total_equity",
    "total_liabilities",
    "ebt_incl_unusual",
    "cash_and_equivalents",
    "total_liabilities_and_equity",
    "total_assets",
})


def categorize(evidence: dict) -> tuple[str, str]:
    """Return (category, suppression_reason). category='keep_open' means don't suppress."""
    derivation = evidence.get("derivation", "")
    concept = evidence.get("concept", "")

    if derivation == "q4_derived_fy_minus_9m":
        return (
            "audit_derivation_artifact",
            "Audit derives Q4 as XBRL annual − 9M YTD. For filers with mid-year "
            "discontinued ops or restatement (e.g., DELL post-VMWare), the bases "
            "differ between annual and 9M YTD, making the derived XBRL value "
            "no more reliable than FMP's. Not corruption.",
        )

    if concept in DEFINITIONAL_CONCEPTS:
        return (
            "definitional_difference",
            f"{concept}: FMP canonical and XBRL tag selection differ definitionally "
            "(NCI / lease treatment / unusual items / equivalents-vs-investments "
            "boundary). See arrow.reconcile.xbrl_concepts mapping. Not corruption.",
        )

    return ("keep_open", "")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="Write suppressions; default is dry-run preview.")
    parser.add_argument("--actor", default="operator:bulk_triage",
                        help="Actor field on the suppression history entry.")
    args = parser.parse_args()

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
          SELECT id, ticker, fiscal_period_key, evidence
          FROM data_quality_findings
          WHERE source_check='xbrl_audit_unresolved' AND status='open'
          ORDER BY id
        """)
        findings = cur.fetchall()

    if not findings:
        print("No open xbrl_audit_unresolved findings.")
        return

    by_category: dict[str, list] = {"audit_derivation_artifact": [], "definitional_difference": [], "keep_open": []}
    for fid, ticker, fpk, ev in findings:
        cat, reason = categorize(ev)
        by_category[cat].append((fid, ticker, fpk, ev, reason))

    print(f"Total open findings: {len(findings)}\n")
    for cat in ("audit_derivation_artifact", "definitional_difference", "keep_open"):
        print(f"  {cat}: {len(by_category[cat])}")

    print("\nKeep-open breakdown by concept:")
    keep_concepts = Counter(ev["concept"] for _, _, _, ev, _ in by_category["keep_open"])
    for c, n in keep_concepts.most_common():
        print(f"  {c}: {n}")

    print("\nKeep-open by ticker:")
    keep_tickers = Counter(t for _, t, _, _, _ in by_category["keep_open"])
    for t, n in keep_tickers.most_common():
        print(f"  {t}: {n}")

    if not args.apply:
        print("\nDry run. Pass --apply to suppress the audit_derivation_artifact + definitional_difference buckets.")
        return

    suppressed = 0
    with get_conn() as conn:
        for cat in ("audit_derivation_artifact", "definitional_difference"):
            for fid, _, _, _, reason in by_category[cat]:
                full_reason = f"[{cat}] {reason}"
                with conn.transaction():
                    suppress_finding(conn, fid, actor=args.actor, reason=full_reason)
                suppressed += 1
    print(f"\nSuppressed {suppressed} findings; left {len(by_category['keep_open'])} open for case-by-case review.")


if __name__ == "__main__":
    main()
