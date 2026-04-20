"""Standalone FMP ↔ SEC XBRL reconciliation — Build Order step 9.5.

Runs Layer 5 (anchor cross-check against SEC XBRL) independently of the
ingest path. Does NOT re-ingest FMP data. Does NOT modify financial_facts.
Read-only verification against currently-stored facts.

Intended to run on a schedule (cron, Airflow, etc.) to catch drift:
  - FMP restating a prior value in a later refresh
  - FMP mapping changes between vendor releases
  - New SEC filings superseding prior XBRL values

On failure, the ingest_run is marked `status='failed'` with divergences
in `error_details`. The CLI exits non-zero so a cron wrapper can alert.
Successful runs record `status='succeeded'` with per-statement match
counts, forming the audit trail Build Order 9.5 calls for.

Usage (library): agents.fmp_reconcile.reconcile_fmp_vs_xbrl(conn, tickers)
Usage (CLI):     scripts/reconcile_fmp_vs_xbrl.py NVDA [MSFT ...]
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import psycopg

from arrow.ingest.common.http import HttpClient
from arrow.ingest.common.runs import close_failed, close_succeeded, open_run
from arrow.ingest.sec.bootstrap import SEC_RATE_LIMIT, SEC_USER_AGENT
from arrow.ingest.sec.company_facts import fetch_company_facts
from arrow.normalize.financials.load import (
    BS_EXTRACTION_VERSION,
    CF_EXTRACTION_VERSION,
    IS_EXTRACTION_VERSION,
)
from arrow.reconcile.fmp_vs_xbrl import (
    AnchorCheckResult,
    XBRLDivergence,
    reconcile_bs_anchors,
    reconcile_cf_anchors,
    reconcile_is_anchors,
)


@dataclass(frozen=True)
class CompanyRow:
    id: int
    cik: int
    ticker: str


class CompanyNotSeeded(RuntimeError):
    pass


def _get_company(conn: psycopg.Connection, ticker: str) -> CompanyRow:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, cik, ticker FROM companies WHERE ticker = %s;",
            (ticker.upper(),),
        )
        row = cur.fetchone()
    if row is None:
        raise CompanyNotSeeded(f"{ticker} not in companies")
    return CompanyRow(id=row[0], cik=row[1], ticker=row[2])


def _divergence_dict(d: XBRLDivergence, statement: str) -> dict[str, Any]:
    return {
        "statement": statement,
        "concept": d.concept,
        "period_end": d.period_end.isoformat(),
        "period_type": d.period_type,
        "fiscal_year": d.fiscal_year,
        "fiscal_quarter": d.fiscal_quarter,
        "fmp_value": str(d.fmp_value),
        "xbrl_value": str(d.xbrl_value),
        "xbrl_tag": d.xbrl_tag,
        "xbrl_filed": d.xbrl_filed,
        "xbrl_accn": d.xbrl_accn,
        "derivation": d.derivation,
        "delta": str(d.delta),
        "tolerance": str(d.tolerance),
    }


def reconcile_fmp_vs_xbrl(
    conn: psycopg.Connection,
    tickers: list[str],
) -> dict[str, Any]:
    """Re-run Layer 5 anchor reconciliation against already-stored facts.

    Returns a counts dict containing per-statement match totals and a
    `divergences` list. Does NOT raise on divergences — the caller (CLI
    or cron wrapper) decides how to respond. Does raise if a ticker
    isn't seeded or if XBRL fetching fails.
    """
    run_id = open_run(
        conn,
        run_kind="reconciliation",
        vendor="sec",
        ticker_scope=[t.upper() for t in tickers],
    )
    http = HttpClient(user_agent=SEC_USER_AGENT, rate_limit=SEC_RATE_LIMIT)

    counts: dict[str, Any] = {
        "tickers": [t.upper() for t in tickers],
        "raw_responses": 0,
        "is_anchors_stored": 0,
        "is_anchors_checked": 0,
        "is_anchors_matched": 0,
        "bs_anchors_stored": 0,
        "bs_anchors_checked": 0,
        "bs_anchors_matched": 0,
        "cf_anchors_stored": 0,
        "cf_anchors_checked": 0,
        "cf_anchors_matched": 0,
        "divergences": [],
    }

    try:
        for ticker in tickers:
            company = _get_company(conn, ticker)
            with conn.transaction():
                fetched = fetch_company_facts(
                    conn, cik=company.cik, ingest_run_id=run_id, http=http,
                )
            counts["raw_responses"] += 1
            xbrl = fetched.payload

            is_res = reconcile_is_anchors(
                conn, company_id=company.id,
                extraction_version=IS_EXTRACTION_VERSION,
                companyfacts=xbrl,
            )
            _merge_result(counts, is_res, "is_", "income_statement")

            bs_res = reconcile_bs_anchors(
                conn, company_id=company.id,
                extraction_version=BS_EXTRACTION_VERSION,
                companyfacts=xbrl,
            )
            _merge_result(counts, bs_res, "bs_", "balance_sheet")

            cf_res = reconcile_cf_anchors(
                conn, company_id=company.id,
                extraction_version=CF_EXTRACTION_VERSION,
                companyfacts=xbrl,
            )
            _merge_result(counts, cf_res, "cf_", "cash_flow")

    except Exception as e:
        close_failed(
            conn, run_id, error_message=str(e),
            error_details={"kind": type(e).__name__},
        )
        raise

    if counts["divergences"]:
        close_failed(
            conn, run_id,
            error_message=f"{len(counts['divergences'])} anchor divergence(s) — see error_details",
            error_details={
                "kind": "reconciliation_divergences",
                "count": len(counts["divergences"]),
                "divergences": counts["divergences"],
            },
        )
    else:
        close_succeeded(conn, run_id, counts={
            k: v for k, v in counts.items() if k != "divergences"
        })

    counts["ingest_run_id"] = run_id
    counts["status"] = "failed" if counts["divergences"] else "succeeded"
    return counts


def _merge_result(
    counts: dict[str, Any],
    result: AnchorCheckResult,
    prefix: str,
    statement: str,
) -> None:
    counts[f"{prefix}anchors_stored"] += result.anchors_with_fmp_stored
    counts[f"{prefix}anchors_checked"] += result.anchors_checked
    counts[f"{prefix}anchors_matched"] += result.anchors_matched
    for d in result.divergences:
        counts["divergences"].append(_divergence_dict(d, statement))
