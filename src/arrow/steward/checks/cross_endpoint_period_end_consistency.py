"""cross_endpoint_period_end_consistency: catch FMP IS/BS/CF period_end drift
from the trusted-endpoint (employees/segments) date for the same fiscal period.

FMP's stable API sometimes stamps the IS/BS/CF endpoints (both annual
and quarterly) with a calendar-month-end approximation, while the
employees and segments endpoints carry the actual fiscal year-end /
quarter-end. When they disagree, ``v_metrics_fy`` fans out into
duplicate rows per fiscal year (one with revenue/COGS, another with
employees), and the dashboard's annual panel mis-renders.

Phase 1 (`q4_period_end_consistency`) handled Q4-quarter-vs-FY-annual
*within* IS/BS/CF. Phase 2 (this check) handles cross-endpoint splits
where IS/BS/CF disagree with the trusted endpoints.

Trusted endpoints: ``fmp-employees-v1``, ``fmp-segments-v1``.
Targets: ``fmp-is-v1``, ``fmp-bs-v1``, ``fmp-cf-v1``.

Suggested fix: ``uv run scripts/backfill_cross_endpoint_period_end.py``.
"""

from __future__ import annotations

from typing import Iterable

import psycopg

from arrow.steward.fingerprint import fingerprint
from arrow.steward.registry import Check, FindingDraft, Scope, register


TRUSTED_VERSIONS = ("fmp-employees-v1", "fmp-segments-v1")
TARGET_VERSIONS = ("fmp-is-v1", "fmp-bs-v1", "fmp-cf-v1")


@register
class CrossEndpointPeriodEndConsistency(Check):
    name = "cross_endpoint_period_end_consistency"
    severity = "warning"
    vertical = "financials"

    def run(self, conn: psycopg.Connection, *, scope: Scope) -> Iterable[FindingDraft]:
        sql = [
            "WITH trusted_dates AS (",
            "  SELECT company_id, fiscal_year, fiscal_quarter, period_type, period_end",
            "  FROM financial_facts",
            "  WHERE superseded_at IS NULL",
            "    AND extraction_version = ANY(%s)",
            "  GROUP BY company_id, fiscal_year, fiscal_quarter, period_type, period_end",
            "),",
            "canonical AS (",
            "  SELECT company_id, fiscal_year, fiscal_quarter, period_type,",
            "         MIN(period_end) AS canon_pe",
            "  FROM trusted_dates",
            "  GROUP BY company_id, fiscal_year, fiscal_quarter, period_type",
            "  HAVING COUNT(DISTINCT period_end) = 1",
            "),",
            "mismatched AS (",
            "  SELECT ff.company_id, c.ticker, ff.fiscal_year, ff.fiscal_quarter,",
            "         ff.period_type, ff.statement, ff.extraction_version,",
            "         ff.period_end AS target_pe, can.canon_pe,",
            "         COUNT(*) AS row_count",
            "  FROM financial_facts ff",
            "  JOIN canonical can",
            "    ON can.company_id = ff.company_id",
            "   AND can.fiscal_year = ff.fiscal_year",
            "   AND COALESCE(can.fiscal_quarter, -1) = COALESCE(ff.fiscal_quarter, -1)",
            "   AND can.period_type = ff.period_type",
            "  JOIN companies c ON c.id = ff.company_id",
            "  WHERE ff.superseded_at IS NULL",
            "    AND ff.dimension_type IS NULL",
            "    AND ff.extraction_version = ANY(%s)",
            "    AND ff.period_end <> can.canon_pe",
        ]
        params: list = [list(TRUSTED_VERSIONS), list(TARGET_VERSIONS)]
        if scope.tickers is not None:
            sql.append("    AND c.ticker = ANY(%s)")
            params.append([t.upper() for t in scope.tickers])
        sql.extend([
            "  GROUP BY ff.company_id, c.ticker, ff.fiscal_year, ff.fiscal_quarter,",
            "           ff.period_type, ff.statement, ff.extraction_version,",
            "           ff.period_end, can.canon_pe",
            ")",
            "SELECT company_id, ticker, fiscal_year, fiscal_quarter, period_type,",
            "       statement, extraction_version, target_pe, canon_pe, row_count",
            "FROM mismatched",
            "ORDER BY ticker, fiscal_year, period_type, statement;",
        ])

        with conn.cursor() as cur:
            cur.execute("\n".join(sql), params)
            rows = cur.fetchall()

        for (
            company_id, ticker, fiscal_year, fiscal_quarter, period_type,
            statement, extraction_version, target_pe, canon_pe, row_count,
        ) in rows:
            yield self._build_draft(
                company_id=company_id,
                ticker=ticker,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                period_type=period_type,
                statement=statement,
                extraction_version=extraction_version,
                target_pe=target_pe,
                canon_pe=canon_pe,
                row_count=row_count,
            )

    def _build_draft(
        self,
        *,
        company_id: int,
        ticker: str,
        fiscal_year: int,
        fiscal_quarter: int | None,
        period_type: str,
        statement: str,
        extraction_version: str,
        target_pe,
        canon_pe,
        row_count: int,
    ) -> FindingDraft:
        if fiscal_quarter is None:
            period = f"FY{fiscal_year}"
        else:
            period = f"FY{fiscal_year} Q{fiscal_quarter}"
        fp = fingerprint(
            self.name,
            scope={
                "company_id": company_id,
                "fiscal_year": fiscal_year,
                "fiscal_quarter": fiscal_quarter,
                "period_type": period_type,
                "statement": statement,
                "extraction_version": extraction_version,
            },
        )
        summary = (
            f"{ticker} {period} {statement} period_end {target_pe} disagrees "
            f"with the trusted-endpoint canonical {canon_pe} ({row_count} rows). "
            f"FMP IS/BS/CF stamped a calendar-approximation while employees/"
            f"segments carry the real fiscal date."
        )
        suggested = {
            "kind": "backfill_cross_endpoint_period_end",
            "params": {"ticker": ticker, "fiscal_year": fiscal_year},
            "command": "uv run scripts/backfill_cross_endpoint_period_end.py --apply",
            "prose": (
                f"Run `uv run scripts/backfill_cross_endpoint_period_end.py` "
                f"(dry run) to confirm the affected rows, then `--apply` to fix. "
                f"Snaps IS/BS/CF rows to the period_end stamped by the trusted "
                f"endpoints (employees/segments) for the same fiscal period."
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
                "statement": statement,
                "extraction_version": extraction_version,
                "fiscal_year": fiscal_year,
                "fiscal_quarter": fiscal_quarter,
                "period_type": period_type,
                "target_period_end": str(target_pe),
                "canonical_period_end": str(canon_pe),
                "row_count": row_count,
            },
            summary=summary,
            suggested_action=suggested,
        )
