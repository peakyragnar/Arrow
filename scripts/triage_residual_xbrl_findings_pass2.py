"""Pass 2 of residual triage: handle the 63 cases that surfaced after the
first promotion batch (the per-(ticker,year) cap freed slots).

Logic:
  1. Suppress every divergence with absolute gap < $50M as
     ``below_materiality`` — noise relative to our $50M+5% steward filter.
     Keeps the queue focused on cases that materially affect analyst views.

  2. Promote DELL FY2021 quarterly revenue + gross_profit + annual
     net_income (VMWare-spinoff continuing-ops restatement; XBRL has the
     post-spinoff continuing-only values from the FY22 10-K).

  3. Promote INTC FY2020 cff annual + FY2021 Q1 cff/cfo + FY2021 Q2/Q3
     op_income (legitimate restatements vintage-tagged in XBRL).

  4. Promote AMD FY2017 cfi/cfo + DELL FY2017 Q3 gross_profit/NI +
     DELL FY2019 Q1 net_income (older but XBRL-authoritative
     restatements with material gaps).

Run: uv run scripts/triage_residual_xbrl_findings_pass2.py [--apply]
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

from arrow.db.connection import get_conn
from arrow.ingest.common.runs import close_succeeded, open_run
from arrow.steward.actions import suppress_finding


STATEMENT_TO_AMENDMENT_VERSION = {
    "income_statement": "xbrl-amendment-is-v1",
    "balance_sheet": "xbrl-amendment-bs-v1",
    "cash_flow": "xbrl-amendment-cf-v1",
}
SOURCE_VERSIONS = ("fmp-is-v1", "fmp-bs-v1", "fmp-cf-v1")
MATERIALITY_FLOOR = 50_000_000  # $50M


def categorize_residual(d: dict) -> tuple[str, str]:
    """Return (action, reason). action ∈ {'suppress','promote','keep_open'}."""
    abs_delta = abs(float(d["delta"]))
    ticker = d["ticker"]
    fy = d["fiscal_year"]
    fq = d.get("fiscal_quarter")
    concept = d["concept"]
    pt = d["period_type"]

    # Hard suppression for AMD FY2021 sign flips (already established as XBRL filing error)
    if ticker == "AMD" and fy == 2021 and concept in ("cff", "cfi"):
        return ("suppress",
                "[xbrl_filing_error] AMD's FY23 10-K (filed 2024-01-31) restated FY21 "
                f"{concept} with FLIPPED SIGN. Identical magnitude — XBRL filing error. "
                "FMP matches originally-filed value.")

    # Below-materiality suppression
    if abs_delta < MATERIALITY_FLOOR:
        return ("suppress",
                f"[below_materiality] absolute gap ${abs_delta:,.0f} below $50M floor. "
                "Below materiality threshold for analyst review; legitimate but trivial "
                "restatement / reclassification.")

    # DELL FY2021 VMWare continuing-ops restatement
    if ticker == "DELL" and fy == 2021 and concept in ("revenue", "gross_profit", "net_income"):
        return ("promote",
                "FY22 10-K (filed 2022-03-24) restated DELL FY21 to continuing-ops "
                "basis post-VMWare-spinoff. XBRL captures the continuing-only value; "
                "FMP captured the originally-filed consolidated value. XBRL is the "
                "current-comparison-correct view.")

    # INTC FY2020 cff annual restatement (vintage update in FY22 10-K)
    if ticker == "INTC" and fy == 2020 and pt == "annual" and concept == "cff":
        return ("promote",
                "FY22 10-K restated INTC FY20 cff (legitimate vintage update).")

    # INTC FY2021 Q1 cff/cfo restatements
    if ticker == "INTC" and fy == 2021 and fq == 1 and concept in ("cff", "cfo"):
        return ("promote",
                "FY22 Q1 10-Q restated INTC FY21 Q1 CF (legitimate vintage update).")

    # INTC FY2021 Q2 op_income restatement (same pattern as Q1 we already promoted)
    if ticker == "INTC" and fy == 2021 and fq == 2 and concept == "operating_income":
        return ("promote",
                "FY22 Q2 10-Q restated INTC FY21 Q2 operating_income.")

    # DELL FY2017 Q3 gross_profit / NI_attrib (large EMC-merger-vintage restatements)
    if ticker == "DELL" and fy == 2017 and fq == 3 and concept in ("gross_profit", "net_income_attributable_to_parent"):
        return ("promote",
                "DELL FY17 Q3 EMC-merger-era restatement; XBRL latest reflects "
                "post-restatement view.")

    # DELL FY2019 Q1 net_income $98M restatement
    if ticker == "DELL" and fy == 2019 and fq == 1 and concept == "net_income":
        return ("promote",
                "DELL FY19 Q1 net_income legitimate restatement.")

    # AMD FY2017 cfi/cfo: smaller-magnitude sign-flip-pattern. Verify against XBRL —
    # if XBRL latest is similar magnitude with different sign, suppress as filing error.
    # If XBRL latest is a partial-magnitude reclassification, promote.
    # AMD FY17 cfi: FMP=-$114M, XBRL=-$54M (same sign, 53% reclassification). PROMOTE.
    # AMD FY17 cfo: FMP=$68M, XBRL=$12M (same sign, partial reclass). PROMOTE.
    if ticker == "AMD" and fy == 2017 and concept in ("cfi", "cfo"):
        return ("promote",
                "AMD FY17 CF reclassification between filings; XBRL latest reflects "
                "the restated value, not a sign error (gap is partial magnitude, not 200%).")

    return ("keep_open", "")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--actor", default="operator:residual_pass2")
    args = parser.parse_args()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
              SELECT DISTINCT ON (ticker_scope[1])
                     ticker_scope[1], error_details->'divergences'
              FROM ingest_runs WHERE vendor='sec' AND run_kind='reconciliation'
                AND error_details ? 'divergences'
              ORDER BY ticker_scope[1], started_at DESC
            """)
            runs = cur.fetchall()

        # Find unresolved divergences (no xbrl-amendment row + not in suppress categories)
        from bulk_triage_xbrl_findings import categorize as bulk_categorize
        unresolved = []
        with conn.cursor() as cur:
            for ticker, divs in runs:
                for d in (divs or []):
                    d2 = dict(d); d2['ticker'] = ticker
                    cat, _ = bulk_categorize(d2)
                    if cat != "keep_open":
                        continue
                    cur.execute("""
                      SELECT 1 FROM financial_facts ff
                      JOIN companies c ON c.id=ff.company_id
                      WHERE c.ticker=%s AND ff.statement=%s AND ff.concept=%s
                        AND ff.period_end=%s AND ff.period_type=%s
                        AND ff.superseded_at IS NULL AND ff.dimension_type IS NULL
                        AND ff.extraction_version LIKE 'xbrl-amendment-%%'
                      LIMIT 1
                    """, (ticker, d['statement'], d['concept'], d['period_end'], d['period_type']))
                    if cur.fetchone():
                        continue
                    unresolved.append(d2)

        # Categorize each
        actions = defaultdict(list)
        for d in unresolved:
            action, reason = categorize_residual(d)
            actions[action].append((d, reason))

        print(f"Total unresolved: {len(unresolved)}")
        print(f"  promote: {len(actions['promote'])}")
        print(f"  suppress: {len(actions['suppress'])}")
        print(f"  keep_open: {len(actions['keep_open'])}")

        if not args.apply:
            print("\nKeep-open detail:")
            for d, _ in actions['keep_open']:
                period = f"FY{d['fiscal_year']}" + (f" Q{d['fiscal_quarter']}" if d.get('fiscal_quarter') else "")
                print(f"  {d['ticker']:<6}{period:<10}{d['concept']:<35} gap={float(d['delta']):>+12,.0f} ({abs(float(d['delta']))/abs(float(d['xbrl_value']))*100 if float(d['xbrl_value']) else 0:.1f}%)")
            print("\nDry run. Pass --apply to write.")
            return

        # APPLY — promotions
        print("\nPromoting...")
        for d, reason in actions['promote']:
            apply_promotion(conn, d, reason)

        # APPLY — suppressions (find finding by id)
        print("\nSuppressing...")
        for d, reason in actions['suppress']:
            period = f"FY{d['fiscal_year']}" + (f" Q{d['fiscal_quarter']}" if d.get('fiscal_quarter') else "")
            with conn.cursor() as cur:
                cur.execute("""
                  SELECT id FROM data_quality_findings
                  WHERE source_check='xbrl_audit_unresolved' AND status='open'
                    AND ticker=%s AND fiscal_period_key=%s
                    AND evidence->>'concept'=%s
                    AND evidence->>'period_type'=%s
                """, (d['ticker'], period, d['concept'], d['period_type']))
                row = cur.fetchone()
            if row is None:
                # No open finding for this divergence — could be capped out of view.
                # Skip; will surface on next steward run if material.
                continue
            with conn.transaction():
                suppress_finding(conn, row[0], actor=args.actor, reason=reason)
            print(f"  {d['ticker']} FY{d['fiscal_year']} {d['concept']}: suppressed #{row[0]}")


def apply_promotion(conn, divergence, override_reason):
    ticker = divergence['ticker']
    statement = divergence['statement']
    concept = divergence['concept']
    fiscal_year = divergence['fiscal_year']
    fiscal_quarter = divergence.get('fiscal_quarter')
    period_type = divergence['period_type']
    period_end = divergence['period_end']

    with conn.cursor() as cur:
        cur.execute("SELECT id, cik FROM companies WHERE ticker=%s", (ticker,))
        company_id, cik = cur.fetchone()
        cur.execute("""
          SELECT id FROM raw_responses WHERE vendor='sec'
          AND endpoint LIKE %s AND (params->>'cik')::int=%s
          ORDER BY fetched_at DESC LIMIT 1
        """, ("api/xbrl/companyfacts/%", cik))
        xbrl_raw_id = cur.fetchone()[0]
        cur.execute("""
          SELECT id, unit, fiscal_period_label, period_end,
                 calendar_year, calendar_quarter, calendar_period_label
          FROM financial_facts
          WHERE company_id=%s AND statement=%s AND concept=%s
            AND period_end=%s AND period_type=%s
            AND extraction_version=ANY(%s) AND superseded_at IS NULL
            AND dimension_type IS NULL LIMIT 1
        """, (company_id, statement, concept, period_end, period_type, list(SOURCE_VERSIONS)))
        fmp = cur.fetchone()
        if fmp is None:
            print(f"  {ticker} FY{fiscal_year} {concept}: FMP row not found")
            return
        fmp_id, unit, fpl, pe, cy, cq, cpl = fmp

        promo_run_id = open_run(conn, run_kind="manual", vendor="arrow", ticker_scope=[ticker])
        published_at = (
            datetime.strptime(divergence["xbrl_filed"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if divergence.get("xbrl_filed") else datetime.now(timezone.utc)
        )
        accn = divergence.get("xbrl_accn", "?")
        reason = (
            f"xbrl-disagrees: accn {accn} filed {divergence.get('xbrl_filed')}; "
            f"FMP={divergence['fmp_value']} XBRL={divergence['xbrl_value']} ({override_reason})"
        )
        with conn.transaction():
            cur.execute("""
              UPDATE financial_facts SET superseded_at=%s, supersession_reason=%s
              WHERE id=%s AND superseded_at IS NULL
            """, (published_at, reason, fmp_id))
            cur.execute("""
              INSERT INTO financial_facts (
                ingest_run_id, company_id, statement, concept, value, unit,
                fiscal_year, fiscal_quarter, fiscal_period_label, period_end, period_type,
                calendar_year, calendar_quarter, calendar_period_label,
                published_at, source_raw_response_id, extraction_version,
                supersedes_fact_id, supersession_reason
              ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                promo_run_id, company_id, statement, concept,
                Decimal(divergence["xbrl_value"]), unit,
                fiscal_year, fiscal_quarter, fpl, pe, period_type,
                cy, cq, cpl,
                published_at, xbrl_raw_id, STATEMENT_TO_AMENDMENT_VERSION[statement],
                fmp_id, reason,
            ))
        close_succeeded(conn, promo_run_id, counts={
            "is_facts_written": 1 if statement=="income_statement" else 0,
            "bs_facts_written": 1 if statement=="balance_sheet" else 0,
            "cf_facts_written": 1 if statement=="cash_flow" else 0,
            "is_facts_superseded": 1,
            "ticker": ticker,
            "manual_override": override_reason[:80],
        })
        print(f"  {ticker} FY{fiscal_year} {concept} {period_type}: promoted via run #{promo_run_id}")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, "scripts")
    main()
