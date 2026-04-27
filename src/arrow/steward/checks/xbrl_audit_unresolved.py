"""xbrl_audit_unresolved: surface FMP-vs-XBRL divergences that weren't auto-promoted.

The audit-and-promote step (`arrow.agents.xbrl_audit.audit_and_promote_xbrl`)
runs FMP-vs-SEC-XBRL reconciliation and auto-promotes divergences that pass
strict safety filters: direct-tagged XBRL value, recent fiscal year,
unambiguous concept, moderate gap size. Everything else stays in
``ingest_runs.error_details.divergences`` for human review.

This check reads the most recent reconciliation run per company, finds
divergences that have NOT been resolved by an arrow-amended row, and
emits a steward finding per unresolved divergence so the analyst can
adjudicate against the actual filing.

A divergence is considered RESOLVED when there's a current row at
``extraction_version LIKE 'xbrl-amendment-%'`` for the same
``(company, statement, concept, period_end, period_type)``. Otherwise
it's unresolved.

Vertical: ``"financials"``. Scope: ``scope.tickers`` filters by ticker.
"""

from __future__ import annotations

from typing import Iterable

import psycopg

from arrow.steward.fingerprint import fingerprint
from arrow.steward.registry import Check, FindingDraft, Scope, register


#: Cap findings emitted per (ticker, fiscal_year). Without a cap, a single
#: bad fiscal year (e.g., DELL FY2017 with VMWare-spinoff entanglement)
#: would emit 30+ findings and drown out everything else. The cap surfaces
#: the most material gaps; the rest are still queryable via ingest_runs.
MAX_FINDINGS_PER_TICKER_YEAR = 3

#: Suppress findings below both thresholds — small drifts not worth analyst
#: time on the steward queue. Calibrated 2026-04-27: with the auto-promote
#: layer handling small recent restatements, a residual divergence that's
#: under both $50M and 5% is almost always rounding / minor reclassification.
MIN_ABSOLUTE_GAP_TO_SURFACE = 50_000_000
MIN_RELATIVE_GAP_TO_SURFACE = 0.05


@register
class XbrlAuditUnresolved(Check):
    name = "xbrl_audit_unresolved"
    severity = "warning"
    vertical = "financials"

    def run(self, conn: psycopg.Connection, *, scope: Scope) -> Iterable[FindingDraft]:
        # Find the most recent reconciliation run per ticker
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (ticker_scope[1])
                       ticker_scope[1] AS ticker,
                       id,
                       error_details->'divergences' AS divs
                FROM ingest_runs
                WHERE vendor='sec' AND run_kind='reconciliation'
                  AND error_details ? 'divergences'
                ORDER BY ticker_scope[1], started_at DESC
                """
            )
            latest_runs = cur.fetchall()

        per_ticker_year_count: dict[tuple[str, int], int] = {}

        for ticker, run_id, divs in latest_runs:
            if scope.tickers is not None and ticker.upper() not in scope.tickers:
                continue
            if not divs:
                continue

            company_id = self._fetch_company_id(conn, ticker)
            if company_id is None:
                continue

            # Sort divergences by absolute gap descending so we surface the
            # biggest issues first under the per-(ticker, year) cap.
            sorted_divs = sorted(
                divs, key=lambda d: -abs(float(d.get("delta", 0)))
            )

            for d in sorted_divs:
                if self._is_resolved(conn, company_id=company_id, divergence=d):
                    continue
                # Materiality filter — both thresholds must trip for a gap to
                # count as analyst-worthy. Small absolute + small relative =
                # rounding / restatement noise.
                abs_delta = abs(float(d.get("delta", 0)))
                xbrl = float(d.get("xbrl_value", 0))
                rel_gap = abs_delta / abs(xbrl) if xbrl else 0.0
                if abs_delta < MIN_ABSOLUTE_GAP_TO_SURFACE and rel_gap < MIN_RELATIVE_GAP_TO_SURFACE:
                    continue
                key = (ticker, d["fiscal_year"])
                if per_ticker_year_count.get(key, 0) >= MAX_FINDINGS_PER_TICKER_YEAR:
                    continue
                per_ticker_year_count[key] = per_ticker_year_count.get(key, 0) + 1

                yield self._build_draft(
                    company_id=company_id, ticker=ticker, divergence=d, run_id=run_id,
                )

    def _fetch_company_id(self, conn: psycopg.Connection, ticker: str) -> int | None:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM companies WHERE ticker = %s", (ticker.upper(),))
            row = cur.fetchone()
            return row[0] if row else None

    def _is_resolved(self, conn: psycopg.Connection, *, company_id: int, divergence: dict) -> bool:
        """A divergence is resolved when an xbrl-amendment row exists for the same business identity."""
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM financial_facts
                WHERE company_id = %s
                  AND statement = %s
                  AND concept = %s
                  AND period_end = %s
                  AND period_type = %s
                  AND superseded_at IS NULL
                  AND dimension_type IS NULL
                  AND extraction_version LIKE 'xbrl-amendment-%%'
                LIMIT 1
                """,
                (
                    company_id, divergence["statement"], divergence["concept"],
                    divergence["period_end"], divergence["period_type"],
                ),
            )
            return cur.fetchone() is not None

    def _build_draft(
        self, *, company_id: int, ticker: str, divergence: dict, run_id: int,
    ) -> FindingDraft:
        period = f"FY{divergence['fiscal_year']}"
        if divergence.get("fiscal_quarter"):
            period += f" Q{divergence['fiscal_quarter']}"
        fp = fingerprint(
            self.name,
            scope={
                "company_id": company_id,
                "fiscal_year": divergence["fiscal_year"],
                "fiscal_quarter": divergence.get("fiscal_quarter"),
                "period_type": divergence["period_type"],
                "statement": divergence["statement"],
                "concept": divergence["concept"],
            },
        )
        fmp = float(divergence["fmp_value"])
        xbrl = float(divergence["xbrl_value"])
        delta = float(divergence["delta"])
        rel_gap = abs(delta) / abs(xbrl) * 100 if xbrl else 0.0
        derivation = divergence.get("derivation", "?")
        accn = divergence.get("xbrl_accn", "?")

        summary = (
            f"{ticker} {period} {divergence['concept']} ({divergence['statement']}) — "
            f"FMP={fmp:,.0f} vs XBRL={xbrl:,.0f} (gap {delta:+,.0f}, {rel_gap:.1f}%, "
            f"derivation={derivation}, accn={accn}). Audit auto-promote skipped this "
            f"because it didn't pass safety filters; needs analyst adjudication."
        )
        suggested = {
            "kind": "review_xbrl_divergence",
            "params": {
                "ticker": ticker,
                "fiscal_year": divergence["fiscal_year"],
                "fiscal_quarter": divergence.get("fiscal_quarter"),
                "concept": divergence["concept"],
                "statement": divergence["statement"],
            },
            "command": (
                "# Verify against the actual filing, then either:\n"
                f"#   uv run scripts/promote_xbrl_for_corruption.py --ticker {ticker} --apply  "
                "# (override safety filters with explicit decision)\n"
                "# OR if the gap is definitional / FMP is right, suppress this finding."
            ),
            "prose": (
                f"FMP and XBRL disagree on {ticker} {period} {divergence['concept']}. "
                f"XBRL value comes from accession {accn} via tag '{divergence.get('xbrl_tag', '?')}' "
                f"({derivation}). Auto-promotion was skipped because either: the XBRL value "
                f"is audit-derived (not directly tagged), the fiscal year is older than "
                f"the auto-promote cutoff, the concept is definitional-prone (NCI, lease "
                f"treatment, unusual items), or the gap is wide enough that basis-mismatch "
                f"is more likely than corruption. Review the original filing to decide."
            ),
        }
        return FindingDraft(
            fingerprint=fp,
            finding_type=self.name,
            severity=self.severity,
            company_id=company_id,
            ticker=ticker,
            vertical=self.vertical,
            fiscal_period_key=period,
            evidence={
                "statement": divergence["statement"],
                "concept": divergence["concept"],
                "fiscal_year": divergence["fiscal_year"],
                "fiscal_quarter": divergence.get("fiscal_quarter"),
                "period_type": divergence["period_type"],
                "fmp_value": divergence["fmp_value"],
                "xbrl_value": divergence["xbrl_value"],
                "delta": divergence["delta"],
                "rel_gap_pct": round(rel_gap, 4),
                "derivation": derivation,
                "xbrl_accn": accn,
                "xbrl_tag": divergence.get("xbrl_tag"),
                "audit_run_id": run_id,
            },
            summary=summary,
            suggested_action=suggested,
        )
