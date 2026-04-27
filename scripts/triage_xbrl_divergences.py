"""Triage FMP-vs-XBRL divergences from Phase A audit runs.

Reads ingest_runs.error_details from reconciliation runs and classifies
each divergence into one of three buckets:

  1. DEFINITIONAL — FMP and XBRL encode the concept differently. Don't
     supersede. Examples:
       - total_equity vs StockholdersEquity (parent-only for NCI filers)
       - total_liabilities small gap (lease/accrued treatment)
       - ebt_incl_unusual moderate gap (unusual-items inclusion)
       - cash_and_equivalents small gap (equivalents-vs-investments)

  2. CORRUPTION — XBRL and FMP disagree on a value where the concept
     mapping is unambiguous. Action: supersede FMP with XBRL. Examples:
       - revenue, gross_profit, operating_income, net_income with
         material gap (>$10M absolute AND >1% relative)
       - cfo, cfi, cff material gaps

  3. AMBIGUOUS — small gaps on unambiguous concepts; could be either.
     Default action: leave alone, surface as steward finding for review.

Usage:
    uv run scripts/triage_xbrl_divergences.py
    uv run scripts/triage_xbrl_divergences.py --bucket=corruption
    uv run scripts/triage_xbrl_divergences.py --ticker=DELL
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict

from arrow.db.connection import get_conn


# Concepts where the XBRL-FMP gap is most often definitional, not corruption.
# These need higher gap thresholds (or special handling) before flagging
# as corruption.
DEFINITIONAL_PRONE_CONCEPTS = {
    "total_equity",
    "total_liabilities",
    "ebt_incl_unusual",
    "cash_and_equivalents",
    "total_liabilities_and_equity",
    "total_assets",
}

# Concepts where the XBRL tag is unambiguous and a material gap signals
# real disagreement. These are the prime corruption candidates.
CORRUPTION_PRIMARY_CONCEPTS = {
    "revenue",
    "gross_profit",
    "operating_income",
    "net_income",
    "net_income_attributable_to_parent",
    "cfo",
    "cfi",
    "cff",
}


def classify(d: dict) -> tuple[str, str]:
    """Return (bucket, reason) for a divergence row."""
    concept = d["concept"]
    fmp = float(d["fmp_value"])
    xbrl = float(d["xbrl_value"])
    delta = float(d["delta"])
    abs_delta = abs(delta)
    rel_gap = abs_delta / abs(xbrl) if xbrl else None
    tag = d.get("xbrl_tag", "")

    # Rule 1: total_equity with parent-only StockholdersEquity tag → NCI definitional
    if concept == "total_equity" and tag == "StockholdersEquity":
        return ("DEFINITIONAL", "NCI: filer has minority interest; FMP excludes, XBRL StockholdersEquity excludes parent-only — definitional")

    # Rule 2: total_equity with tiny gap (<0.5%) → rounding
    if concept == "total_equity" and rel_gap is not None and rel_gap < 0.005:
        return ("DEFINITIONAL", "rounding/timing")

    # Rule 3: total_liabilities with small gap (<5%) → lease/accrual definitional
    if concept == "total_liabilities" and rel_gap is not None and rel_gap < 0.05:
        return ("DEFINITIONAL", "small total_liabilities gap; likely lease/accrual treatment")

    # Rule 4: ebt_incl_unusual with gap <25% → unusual-items inclusion
    if concept == "ebt_incl_unusual" and rel_gap is not None and rel_gap < 0.25:
        return ("DEFINITIONAL", "ebt_incl_unusual; FMP and XBRL disagree on unusual-items inclusion")

    # Rule 5: cash_and_equivalents small gap → equivalents-vs-investments
    if concept == "cash_and_equivalents" and rel_gap is not None and rel_gap < 0.05:
        return ("DEFINITIONAL", "cash_and_equivalents small gap; equivalents-vs-investments boundary")

    # Rule 6: total_liabilities_and_equity / total_assets tiny gap → rounding
    if concept in ("total_liabilities_and_equity", "total_assets") and rel_gap is not None and rel_gap < 0.005:
        return ("DEFINITIONAL", "balance-identity rounding")

    # Rule 7a: derived-Q4 XBRL values (`annual − 9M YTD`) are unreliable when
    # the filer has had a restatement / discontinued ops where annual and 9M
    # are on different bases. Don't auto-promote these — XBRL's own derivation
    # may be at fault, not FMP. Empirical case: DELL FY2020/FY2021 Q4 derived
    # XBRL revenue is ~$7B low because annual was restated post-VMWare-spinoff
    # to continuing-ops basis while the 9M YTD remained consolidated.
    derivation = d.get("derivation", "")
    if derivation == "q4_derived_fy_minus_9m":
        return ("AMBIGUOUS", "Q4 derived from XBRL annual − 9M YTD; bases may differ (restatement/discontinued ops)")

    # Rule 7b: high-confidence corruption — primary IS/CF concept, material gap,
    # and XBRL value is directly tagged (not derived).
    if concept in CORRUPTION_PRIMARY_CONCEPTS:
        if abs_delta >= 10_000_000 and (rel_gap is None or rel_gap >= 0.005):
            return ("CORRUPTION", "primary IS/CF concept; XBRL directly tagged; material gap")
        else:
            return ("AMBIGUOUS", "primary concept but small gap (≤$10M or <0.5%)")

    # Rule 8: definitional-prone concept with larger gap — still ambiguous, needs review
    if concept in DEFINITIONAL_PRONE_CONCEPTS:
        return ("AMBIGUOUS", f"{concept} with larger gap; likely definitional but worth review")

    # Default: unclassified
    return ("AMBIGUOUS", "unclassified")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", choices=("definitional", "corruption", "ambiguous"))
    parser.add_argument("--ticker")
    parser.add_argument("--detail", action="store_true", help="Print every divergence in scope.")
    args = parser.parse_args()

    bucket_filter = args.bucket.upper() if args.bucket else None
    ticker_filter = args.ticker.upper() if args.ticker else None

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
          SELECT ticker_scope[1], error_details->'divergences'
          FROM ingest_runs
          WHERE vendor='sec' AND run_kind='reconciliation'
            AND error_details ? 'divergences'
          ORDER BY id
        """)
        runs = cur.fetchall()

    rows: list[dict] = []
    for ticker, divs in runs:
        if ticker_filter and ticker != ticker_filter:
            continue
        for d in (divs or []):
            d2 = dict(d)
            d2["ticker"] = ticker
            bucket, reason = classify(d2)
            d2["bucket"] = bucket
            d2["reason"] = reason
            if bucket_filter and bucket != bucket_filter:
                continue
            rows.append(d2)

    # Aggregate
    bucket_counts = Counter(r["bucket"] for r in rows)
    by_ticker_bucket: dict[str, Counter] = defaultdict(Counter)
    for r in rows:
        by_ticker_bucket[r["ticker"]][r["bucket"]] += 1

    print(f"Total in scope: {len(rows)}\n")
    print("Bucket counts:")
    for b in ("DEFINITIONAL", "CORRUPTION", "AMBIGUOUS"):
        print(f"  {b}: {bucket_counts.get(b, 0)}")
    print()

    if not bucket_filter:
        print("Per-ticker breakdown:")
        print(f"  {'TICKER':<8}{'TOTAL':>7}{'DEFINIT':>10}{'CORRUPT':>10}{'AMBIG':>8}")
        for t in sorted(by_ticker_bucket):
            tc = by_ticker_bucket[t]
            print(f"  {t:<8}{sum(tc.values()):>7}{tc.get('DEFINITIONAL',0):>10}{tc.get('CORRUPTION',0):>10}{tc.get('AMBIGUOUS',0):>8}")
        print()

    if args.detail or bucket_filter == "CORRUPTION":
        print("Detail:")
        # Sort by absolute gap descending
        rows_sorted = sorted(rows, key=lambda r: -abs(float(r["delta"])))
        for r in rows_sorted[:60]:
            period = f"FY{r['fiscal_year']}"
            if r['fiscal_quarter']:
                period += f" Q{r['fiscal_quarter']}"
            fmp = float(r['fmp_value'])
            xbrl = float(r['xbrl_value'])
            delta = float(r['delta'])
            pct = abs(delta) / abs(xbrl) * 100 if xbrl else 0
            print(
                f"  [{r['bucket'][:6]}] {r['ticker']:<6}{period:<10}"
                f"{r['concept']:<35} FMP={fmp:>14,.0f}  XBRL={xbrl:>14,.0f}  "
                f"delta={delta:>+13,.0f} ({pct:5.1f}%)"
            )
        if len(rows_sorted) > 60:
            print(f"  ... and {len(rows_sorted)-60} more")


if __name__ == "__main__":
    main()
