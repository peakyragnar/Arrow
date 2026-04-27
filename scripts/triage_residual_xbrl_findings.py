"""Triage the residual 13 xbrl_audit_unresolved findings case-by-case.

Each case was inspected against actual XBRL companyfacts history; this
script applies the verified action.

Promotes (XBRL is authoritative — legitimate restatement or correction):
  - INTC FY2021 Q1 op_income: $5.9B → $3.7B (XBRL consistent across filings)
  - INTC FY2020 cfi annual:   -$20.8B → -$21.5B (FY22 10-K restatement)
  - INTC FY2021 Q1 cfi:       -$2.5B → -$2.0B (FY22 Q1 10-Q restatement)
  - INTC FY2020 cfo annual:    $35.4B → $35.9B (FY22 10-K restatement)
  - DELL FY2021 Q2 gross_profit: $7.2B → $4.9B (FY22 10-K continuing-ops restatement post-VMWare)
  - GEV  FY2024 op_income:     $787M → $471M (XBRL consistent across filings)
  - GEV  FY2025 Q1 op_income:  $76M → $43M (XBRL consistent across filings)
  - META FY2017 cfi annual:    -$20.04B → -$20.12B (FY18 10-K restatement)
  - VRT  FY2021 Q3 op_income:  $92.3M → $81.8M (XBRL consistent; reporting-basis difference)
  - VRT  FY2022 Q2 op_income:  $28.1M → $26.2M (same)

Suppresses (FMP is correct or PIT-as-originally-filed):
  - AMD FY2021 cff annual: AMD's FY23 10-K has a sign-flip XBRL filing
    error (+$1.9B vs the original FY21/FY22 10-K's -$1.9B). FMP matches
    the original filing.
  - AMD FY2021 cfi annual: same sign-flip XBRL error in FY23 10-K.
  - AMD FY2018 Q1 cfi: small ($21M) old reclassification between
    FY18 Q1 10-Q ($-46M) and FY19 Q1 10-Q ($-25M). PIT-correct as
    originally filed.

Run: uv run scripts/triage_residual_xbrl_findings.py [--apply]
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal

from arrow.db.connection import get_conn
from arrow.ingest.common.runs import close_succeeded, open_run
from arrow.steward.actions import suppress_finding


PROMOTIONS = [
    # (ticker, fiscal_year, fiscal_quarter, period_type, statement, concept, override_reason)
    ("INTC", 2021, 1, "quarter", "income_statement", "operating_income",
     "XBRL value $3.694B is consistent across original Q1 10-Q and FY22 Q1 10-Q comparative; FMP's $5.903B has no XBRL support."),
    ("INTC", 2020, None, "annual", "cash_flow", "cfi",
     "FY22 10-K restated FY20 cfi from -$20.796B to -$21.524B (legitimate vintage update)."),
    ("INTC", 2021, 1, "quarter", "cash_flow", "cfi",
     "FY22 Q1 10-Q restated FY21 Q1 cfi from -$2.547B to -$2.001B."),
    ("INTC", 2020, None, "annual", "cash_flow", "cfo",
     "FY22 10-K restated FY20 cfo from $35.384B to $35.864B."),
    ("DELL", 2021, 2, "quarter", "income_statement", "gross_profit",
     "FY22 10-K (filed 2022-03-24) restated DELL FY21 Q2 gross_profit to continuing-ops basis post-VMWare-spinoff: $7.156B → $4.877B."),
    ("GEV", 2024, None, "annual", "income_statement", "operating_income",
     "XBRL value $471M consistent across FY24 10-K and FY25 10-K comparative; FMP's $787M has no XBRL support."),
    ("GEV", 2025, 1, "quarter", "income_statement", "operating_income",
     "XBRL value $43M consistent across original FY25 Q1 10-Q and FY26 Q1 10-Q comparative."),
    ("META", 2017, None, "annual", "cash_flow", "cfi",
     "FY18 10-K restated FY17 cfi from -$20.038B to -$20.118B."),
    ("VRT", 2021, 3, "quarter", "income_statement", "operating_income",
     "XBRL value $81.8M consistent across original FY21 Q3 10-Q and FY22 Q3 10-Q comparative; FMP $92.3M reporting-basis difference."),
    ("VRT", 2022, 2, "quarter", "income_statement", "operating_income",
     "XBRL value $26.2M consistent; FMP $28.1M minor reporting-basis difference."),
]


SUPPRESSIONS = [
    # (ticker, fiscal_year, fiscal_quarter, period_type, concept, reason)
    ("AMD", 2021, None, "annual", "cff",
     "[xbrl_filing_error] AMD's FY23 10-K (filed 2024-01-31) restated FY21 cff with FLIPPED SIGN (+$1,895M vs original 2022/2023 10-K -$1,895M). Identical magnitude, sign change is implausible — likely an XBRL tagging error in the 2024 filing. FMP matches the originally-filed value."),
    ("AMD", 2021, None, "annual", "cfi",
     "[xbrl_filing_error] Same pattern as cff: FY23 10-K restated FY21 cfi from -$686M to +$686M. Sign-flip XBRL filing error; FMP matches original."),
    ("AMD", 2018, 1, "quarter", "cfi",
     "[pit_original_filing] FY18 Q1 10-Q reported $-46M; FY19 Q1 10-Q comparative restated to $-25M (small reclassification). FMP value is PIT-correct as originally filed; XBRL latest reflects the restated comparative. Both legitimate; not an error."),
]


STATEMENT_TO_AMENDMENT_VERSION = {
    "income_statement": "xbrl-amendment-is-v1",
    "balance_sheet": "xbrl-amendment-bs-v1",
    "cash_flow": "xbrl-amendment-cf-v1",
}

SOURCE_VERSIONS = ("fmp-is-v1", "fmp-bs-v1", "fmp-cf-v1")


def find_divergence(conn, ticker, fiscal_year, fiscal_quarter, period_type, statement, concept):
    """Find the latest reconciliation run's divergence record matching the case."""
    with conn.cursor() as cur:
        cur.execute("""
          SELECT id, error_details FROM ingest_runs
          WHERE vendor='sec' AND run_kind='reconciliation'
            AND ticker_scope[1]=%s
            AND error_details ? 'divergences'
          ORDER BY started_at DESC LIMIT 1
        """, (ticker,))
        row = cur.fetchone()
    if not row:
        return None, None
    audit_run_id, err = row
    for d in err.get("divergences", []):
        if (d["concept"] == concept
            and d["fiscal_year"] == fiscal_year
            and d.get("fiscal_quarter") == fiscal_quarter
            and d["period_type"] == period_type
            and d["statement"] == statement):
            return audit_run_id, d
    return audit_run_id, None


def find_finding_id(conn, ticker, fiscal_year, fiscal_quarter, period_type, concept):
    period = f"FY{fiscal_year}" + (f" Q{fiscal_quarter}" if fiscal_quarter else "")
    with conn.cursor() as cur:
        cur.execute("""
          SELECT id FROM data_quality_findings
          WHERE source_check='xbrl_audit_unresolved' AND status='open'
            AND ticker=%s AND fiscal_period_key=%s
            AND evidence->>'concept'=%s
            AND evidence->>'period_type'=%s
        """, (ticker, period, concept, period_type))
        row = cur.fetchone()
        return row[0] if row else None


def promote(conn, *, ticker, fiscal_year, fiscal_quarter, period_type, statement, concept, override_reason):
    audit_run_id, div = find_divergence(
        conn, ticker, fiscal_year, fiscal_quarter, period_type, statement, concept,
    )
    if div is None:
        return f"  {ticker} FY{fiscal_year} {concept}: no divergence record found"

    with conn.cursor() as cur:
        cur.execute("SELECT id, cik FROM companies WHERE ticker=%s", (ticker,))
        company_id, cik = cur.fetchone()

        cur.execute("""
          SELECT id FROM raw_responses WHERE vendor='sec'
          AND endpoint LIKE %s AND (params->>'cik')::int=%s
          AND ingest_run_id=%s LIMIT 1
        """, ("api/xbrl/companyfacts/%", cik, audit_run_id))
        r = cur.fetchone()
        if r is None:
            cur.execute("""
              SELECT id FROM raw_responses WHERE vendor='sec'
              AND endpoint LIKE %s AND (params->>'cik')::int=%s
              ORDER BY fetched_at DESC LIMIT 1
            """, ("api/xbrl/companyfacts/%", cik))
            r = cur.fetchone()
        xbrl_raw_id = r[0]

        cur.execute("""
          SELECT id, unit, fiscal_period_label, period_end,
                 calendar_year, calendar_quarter, calendar_period_label
          FROM financial_facts
          WHERE company_id=%s AND statement=%s AND concept=%s
            AND period_end=%s AND period_type=%s
            AND extraction_version = ANY(%s)
            AND superseded_at IS NULL AND dimension_type IS NULL
          LIMIT 1
        """, (company_id, statement, concept, div["period_end"], period_type, list(SOURCE_VERSIONS)))
        fmp = cur.fetchone()
        if fmp is None:
            return f"  {ticker} FY{fiscal_year} {concept}: FMP row not found (already superseded?)"
        fmp_id, unit, fpl, pe, cy, cq, cpl = fmp

        promo_run_id = open_run(conn, run_kind="manual", vendor="arrow", ticker_scope=[ticker])
        published_at = (
            datetime.strptime(div["xbrl_filed"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if div.get("xbrl_filed") else datetime.now(timezone.utc)
        )
        accn = div.get("xbrl_accn", "?")
        reason = (
            f"xbrl-disagrees: accn {accn} filed {div.get('xbrl_filed')}; "
            f"FMP={div['fmp_value']} XBRL={div['xbrl_value']} (manual override: {override_reason})"
        )
        with conn.transaction():
            cur.execute("""
              UPDATE financial_facts SET superseded_at=%s, supersession_reason=%s
              WHERE id=%s AND superseded_at IS NULL
            """, (published_at, reason, fmp_id))
            cur.execute("""
              INSERT INTO financial_facts (
                ingest_run_id, company_id, statement, concept, value, unit,
                fiscal_year, fiscal_quarter, fiscal_period_label,
                period_end, period_type,
                calendar_year, calendar_quarter, calendar_period_label,
                published_at, source_raw_response_id, extraction_version,
                supersedes_fact_id, supersession_reason
              ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                promo_run_id, company_id, statement, concept,
                Decimal(div["xbrl_value"]), unit,
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
            "manual_override": override_reason,
        })
    return f"  {ticker} FY{fiscal_year} {concept}: promoted XBRL via run #{promo_run_id}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--actor", default="operator:residual_triage")
    args = parser.parse_args()

    if not args.apply:
        print("DRY RUN — would apply:")
        print(f"  {len(PROMOTIONS)} XBRL promotions")
        print(f"  {len(SUPPRESSIONS)} suppressions")
        print("\nPromotions:")
        for p in PROMOTIONS:
            print(f"  {p[0]} FY{p[1]} {p[5]} {p[3]} ({p[4]})")
        print("\nSuppressions:")
        for s in SUPPRESSIONS:
            print(f"  {s[0]} FY{s[1]} {s[4]} {s[3]}")
        print("\nPass --apply to write.")
        return

    with get_conn() as conn:
        print("Promotions:")
        for ticker, fy, fq, pt, stmt, concept, ov in PROMOTIONS:
            print(promote(conn, ticker=ticker, fiscal_year=fy, fiscal_quarter=fq,
                          period_type=pt, statement=stmt, concept=concept,
                          override_reason=ov))

        print("\nSuppressions:")
        for ticker, fy, fq, pt, concept, reason in SUPPRESSIONS:
            fid = find_finding_id(conn, ticker, fy, fq, pt, concept)
            if fid is None:
                print(f"  {ticker} FY{fy} {concept}: no open finding (already closed?)")
                continue
            with conn.transaction():
                suppress_finding(conn, fid, actor=args.actor, reason=reason)
            print(f"  {ticker} FY{fy} {concept}: suppressed finding #{fid}")


if __name__ == "__main__":
    main()
