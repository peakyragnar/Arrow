"""Phase C — supersede FMP facts with SEC XBRL values for corruption-bucket divergences.

Reads divergences from prior `reconcile_fmp_vs_xbrl` runs (Phase A),
classifies them via `triage_xbrl_divergences.classify`, and for those
in the CORRUPTION bucket:

  1. Finds the current FMP fact row in financial_facts.
  2. Marks it superseded (superseded_at = XBRL filing's published date,
     supersession_reason cites the XBRL accession).
  3. Inserts a new row carrying the XBRL value at extraction_version
     ``xbrl-amendment-{statement}-v1``, source_raw_response_id pointing
     to the cached SEC companyfacts payload, supersedes_fact_id pointing
     to the old row.

The wide view (v_company_period_wide) preferences ``xbrl-amendment-%``
over ``fmp-%``, so the new XBRL value automatically wins downstream.

Idempotent: rows already superseded by an XBRL amendment are skipped.

Usage:
    uv run scripts/promote_xbrl_for_corruption.py            # dry run
    uv run scripts/promote_xbrl_for_corruption.py --apply
    uv run scripts/promote_xbrl_for_corruption.py --ticker DELL --apply
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import psycopg

from arrow.db.connection import get_conn
from arrow.ingest.common.runs import close_succeeded, open_run

# Reuse the triage classifier
sys.path.insert(0, str(Path(__file__).parent))
from triage_xbrl_divergences import classify  # type: ignore  # noqa: E402


STATEMENT_TO_AMENDMENT_VERSION = {
    "income_statement": "xbrl-amendment-is-v1",
    "balance_sheet": "xbrl-amendment-bs-v1",
    "cash_flow": "xbrl-amendment-cf-v1",
}

SOURCE_VERSIONS = ("fmp-is-v1", "fmp-bs-v1", "fmp-cf-v1")


def fetch_company_id(conn: psycopg.Connection, ticker: str) -> tuple[int, int]:
    """Return (company_id, cik)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, cik FROM companies WHERE ticker = %s",
            (ticker.upper(),),
        )
        row = cur.fetchone()
        if row is None:
            raise SystemExit(f"company not found: {ticker}")
        return row[0], row[1]


def fetch_xbrl_raw_response_id(conn: psycopg.Connection, cik: int) -> int | None:
    """Find the most recent SEC companyfacts raw_response for this CIK."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id
            FROM raw_responses
            WHERE vendor = 'sec'
              AND endpoint LIKE %s
              AND (params->>'cik')::int = %s
            ORDER BY fetched_at DESC LIMIT 1
            """,
            ("api/xbrl/companyfacts/%", cik),
        )
        row = cur.fetchone()
        return row[0] if row else None


def find_fmp_fact_row(
    conn: psycopg.Connection,
    *,
    company_id: int,
    statement: str,
    concept: str,
    period_end: str,
    period_type: str,
) -> dict | None:
    """Find the current FMP fact row matching the divergence."""
    extraction_versions = SOURCE_VERSIONS
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
            (company_id, statement, concept, period_end, period_type, list(extraction_versions)),
        )
        row = cur.fetchone()
        if row is None:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))


#: Only promote XBRL when the value is directly tagged in the filing.
#: Audit-side derived values (annual − 9M YTD) are unreliable for filers
#: with restatements / discontinued ops where annual and 9M YTD are on
#: different bases. Empirical case: DELL FY2020/FY2021 Q4 derived XBRL
#: revenue is implausibly low because annual was restated post-VMWare to
#: continuing-ops while the 9M YTD remained consolidated.
REQUIRE_DIRECT_DERIVATION = True

#: Don't promote if the gap is wildly large — those are usually basis
#: mismatches between FMP and XBRL (continuing-vs-consolidated, sign
#: convention, etc.) that need analyst adjudication, not auto-supersede.
MAX_RELATIVE_GAP_AUTO = 0.25

#: Only promote recent fiscal years where corporate-event noise is lower.
#: Older years often have multiple "right" answers depending on which
#: filing version XBRL serves.
MIN_FISCAL_YEAR_AUTO = 2022


def passes_safety_filters(d: dict) -> tuple[bool, str]:
    if REQUIRE_DIRECT_DERIVATION and d.get("derivation") != "direct":
        return False, "non-direct XBRL derivation"
    if d["fiscal_year"] < MIN_FISCAL_YEAR_AUTO:
        return False, f"older than FY{MIN_FISCAL_YEAR_AUTO}"
    xbrl = float(d["xbrl_value"])
    delta = float(d["delta"])
    rel_gap = abs(delta) / abs(xbrl) if xbrl else 1.0
    if rel_gap >= MAX_RELATIVE_GAP_AUTO:
        return False, f"gap {rel_gap:.0%} ≥ {MAX_RELATIVE_GAP_AUTO:.0%}"
    return True, ""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ticker", help="Limit to one ticker.")
    parser.add_argument("--apply", action="store_true",
                        help="Write the supersessions; default is a dry-run preview.")
    args = parser.parse_args()

    ticker_filter = args.ticker.upper() if args.ticker else None

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
          SELECT ticker_scope[1], error_details->'divergences'
          FROM ingest_runs
          WHERE vendor='sec' AND run_kind='reconciliation'
            AND error_details ? 'divergences'
          ORDER BY id DESC
        """)
        runs = cur.fetchall()

    # Collect corruption-bucket divergences (latest audit per ticker wins)
    seen_tickers: set[str] = set()
    corruption_by_ticker: dict[str, list[dict]] = {}
    for ticker, divs in runs:
        if ticker in seen_tickers:
            continue
        seen_tickers.add(ticker)
        if ticker_filter and ticker != ticker_filter:
            continue
        out: list[dict] = []
        for d in (divs or []):
            d2 = dict(d)
            d2["ticker"] = ticker
            bucket, reason = classify(d2)
            if bucket != "CORRUPTION":
                continue
            ok, why = passes_safety_filters(d2)
            if not ok:
                continue
            d2["bucket"] = bucket
            d2["bucket_reason"] = reason
            out.append(d2)
        if out:
            corruption_by_ticker[ticker] = out

    total = sum(len(v) for v in corruption_by_ticker.values())
    print(f"Corruption divergences in scope: {total} across {len(corruption_by_ticker)} tickers")
    for t in sorted(corruption_by_ticker):
        print(f"  {t}: {len(corruption_by_ticker[t])}")
    print()

    if total == 0:
        return

    if not args.apply:
        # Show first 20 from largest tickers
        print("Preview (top 20 by absolute gap):")
        all_divs = [d for divs in corruption_by_ticker.values() for d in divs]
        all_divs.sort(key=lambda d: -abs(float(d["delta"])))
        for d in all_divs[:20]:
            period = f"FY{d['fiscal_year']}"
            if d['fiscal_quarter']:
                period += f" Q{d['fiscal_quarter']}"
            print(
                f"  {d['ticker']:<6}{period:<10}{d['concept']:<35}"
                f"FMP={float(d['fmp_value']):>14,.0f}  "
                f"XBRL={float(d['xbrl_value']):>14,.0f}  "
                f"delta={float(d['delta']):>+13,.0f}"
            )
        print(f"\nDry run. Pass --apply to write {total} XBRL supersessions.")
        return

    # APPLY
    with get_conn() as conn:
        for ticker, divs in sorted(corruption_by_ticker.items()):
            company_id, cik = fetch_company_id(conn, ticker)
            xbrl_raw_id = fetch_xbrl_raw_response_id(conn, cik)
            if xbrl_raw_id is None:
                print(f"WARN {ticker}: no SEC companyfacts raw_response found; skipping")
                continue

            run_id = open_run(
                conn, run_kind="manual", vendor="arrow",
                ticker_scope=[ticker],
            )
            superseded = 0
            inserted = 0
            skipped = 0
            with conn.transaction(), conn.cursor() as cur:
                for d in divs:
                    fmp_row = find_fmp_fact_row(
                        conn,
                        company_id=company_id,
                        statement=d["statement"],
                        concept=d["concept"],
                        period_end=d["period_end"],
                        period_type=d["period_type"],
                    )
                    if fmp_row is None:
                        skipped += 1
                        continue

                    # XBRL filing date as published_at
                    xbrl_filed = d.get("xbrl_filed")
                    published_at = (
                        datetime.strptime(xbrl_filed, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                        if xbrl_filed else datetime.now(timezone.utc)
                    )
                    accn = d.get("xbrl_accn", "?")
                    reason = f"xbrl-disagrees: accn {accn}, filed {xbrl_filed}; FMP={d['fmp_value']} XBRL={d['xbrl_value']}"

                    # Supersede the FMP row
                    cur.execute(
                        """
                        UPDATE financial_facts
                        SET superseded_at = %s,
                            supersession_reason = %s
                        WHERE id = %s AND superseded_at IS NULL
                        """,
                        (published_at, reason, fmp_row["id"]),
                    )
                    superseded += 1

                    # Insert the XBRL-amendment row
                    amendment_version = STATEMENT_TO_AMENDMENT_VERSION[d["statement"]]
                    cur.execute(
                        """
                        INSERT INTO financial_facts (
                            ingest_run_id, company_id, statement, concept, value, unit,
                            fiscal_year, fiscal_quarter, fiscal_period_label,
                            period_end, period_type,
                            calendar_year, calendar_quarter, calendar_period_label,
                            published_at, source_raw_response_id, extraction_version,
                            supersedes_fact_id, supersession_reason
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s,
                            %s, %s,
                            %s, %s, %s,
                            %s, %s, %s,
                            %s, %s
                        )
                        """,
                        (
                            run_id, company_id, d["statement"], d["concept"],
                            Decimal(d["xbrl_value"]), fmp_row["unit"],
                            fmp_row["fiscal_year"], fmp_row["fiscal_quarter"], fmp_row["fiscal_period_label"],
                            fmp_row["period_end"], d["period_type"],
                            fmp_row["calendar_year"], fmp_row["calendar_quarter"], fmp_row["calendar_period_label"],
                            published_at, xbrl_raw_id, amendment_version,
                            fmp_row["id"], reason,
                        ),
                    )
                    inserted += 1

            close_succeeded(
                conn, run_id,
                counts={
                    "is_facts_written": inserted if any(d["statement"]=="income_statement" for d in divs) else 0,
                    "bs_facts_written": sum(1 for d in divs if d["statement"]=="balance_sheet"),
                    "cf_facts_written": sum(1 for d in divs if d["statement"]=="cash_flow"),
                    "is_facts_superseded": superseded,
                    "ticker": ticker,
                    "skipped_missing": skipped,
                },
            )
            print(f"  {ticker}: superseded {superseded}, inserted {inserted}, skipped {skipped} (run #{run_id})")


if __name__ == "__main__":
    main()
