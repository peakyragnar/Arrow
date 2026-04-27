"""q4_period_end_consistency: Q4 quarterly period_end must match the FY annual.

FMP's stable API sometimes stamps the Q4 quarterly endpoint with a
calendar-month-end approximation while the annual endpoint carries the
actual fiscal year-end. Both come from the same 10-K filing — the
filingDate and acceptedDate are identical. When period_end diverges,
downstream views that join Q4 quarterly to FY annual on `period_end`
silently drop the Q4 data (dashboard FY panel, ROIC view, screener).

The ingest layer canonicalizes Q4 quarterly period_end to the FY
annual's date (see `_canonical_q4_period_end` in
`src/arrow/normalize/financials/load.py`). This check guards against
regression by surfacing any (company, fiscal_year) where the Q4
quarterly IS/BS/CF row's period_end no longer matches the FY annual
row's period_end within the same statement.

Scope: ``fmp-is-v1`` / ``fmp-bs-v1`` / ``fmp-cf-v1``. Cross-endpoint
splits (employees / segments at a different date than IS/BS/CF for the
same fiscal year) are a separate concern not flagged here.
"""

from __future__ import annotations

from typing import Iterable

import psycopg

from arrow.steward.fingerprint import fingerprint
from arrow.steward.registry import Check, FindingDraft, Scope, register


TARGET_VERSIONS = ("fmp-is-v1", "fmp-bs-v1", "fmp-cf-v1")


@register
class Q4PeriodEndConsistency(Check):
    name = "q4_period_end_consistency"
    severity = "warning"
    vertical = "financials"

    def run(self, conn: psycopg.Connection, *, scope: Scope) -> Iterable[FindingDraft]:
        sql = [
            "WITH annual_pe AS (",
            "  SELECT company_id, fiscal_year, statement, extraction_version,",
            "         period_end AS fy_pe",
            "  FROM financial_facts",
            "  WHERE period_type = 'annual'",
            "    AND superseded_at IS NULL",
            "    AND dimension_type IS NULL",
            "    AND extraction_version = ANY(%s)",
            "  GROUP BY company_id, fiscal_year, statement, extraction_version, period_end",
            "),",
            "mismatched AS (",
            "  SELECT q.company_id, c.ticker, q.fiscal_year, q.statement,",
            "         q.extraction_version,",
            "         q.period_end AS q4_pe, a.fy_pe,",
            "         COUNT(*) AS row_count",
            "  FROM financial_facts q",
            "  JOIN annual_pe a",
            "    ON a.company_id = q.company_id",
            "   AND a.fiscal_year = q.fiscal_year",
            "   AND a.statement = q.statement",
            "   AND a.extraction_version = q.extraction_version",
            "  JOIN companies c ON c.id = q.company_id",
            "  WHERE q.period_type = 'quarter'",
            "    AND q.fiscal_quarter = 4",
            "    AND q.superseded_at IS NULL",
            "    AND q.dimension_type IS NULL",
            "    AND q.extraction_version = ANY(%s)",
            "    AND q.period_end <> a.fy_pe",
        ]
        params: list = [list(TARGET_VERSIONS), list(TARGET_VERSIONS)]
        if scope.tickers is not None:
            sql.append("    AND c.ticker = ANY(%s)")
            params.append([t.upper() for t in scope.tickers])
        sql.extend([
            "  GROUP BY q.company_id, c.ticker, q.fiscal_year, q.statement,",
            "           q.extraction_version, q.period_end, a.fy_pe",
            ")",
            "SELECT company_id, ticker, fiscal_year, statement, extraction_version,",
            "       q4_pe, fy_pe, row_count",
            "FROM mismatched",
            "ORDER BY ticker, fiscal_year, statement;",
        ])

        with conn.cursor() as cur:
            cur.execute("\n".join(sql), params)
            rows = cur.fetchall()

        for company_id, ticker, fiscal_year, statement, extraction_version, q4_pe, fy_pe, row_count in rows:
            yield self._build_draft(
                company_id=company_id,
                ticker=ticker,
                fiscal_year=fiscal_year,
                statement=statement,
                extraction_version=extraction_version,
                q4_pe=q4_pe,
                fy_pe=fy_pe,
                row_count=row_count,
            )

    def _build_draft(
        self,
        *,
        company_id: int,
        ticker: str,
        fiscal_year: int,
        statement: str,
        extraction_version: str,
        q4_pe,
        fy_pe,
        row_count: int,
    ) -> FindingDraft:
        period = f"FY{fiscal_year} Q4"
        fp = fingerprint(
            self.name,
            scope={
                "company_id": company_id,
                "fiscal_year": fiscal_year,
                "statement": statement,
                "extraction_version": extraction_version,
            },
        )
        summary = (
            f"{ticker} {period} {statement} period_end {q4_pe} disagrees "
            f"with FY annual period_end {fy_pe} ({row_count} rows). "
            f"Both come from the same 10-K filing — Q4 quarterly should "
            f"be snapped to the annual date."
        )
        suggested = {
            "kind": "backfill_q4_period_end",
            "params": {"ticker": ticker, "fiscal_year": fiscal_year},
            "command": "uv run scripts/backfill_q4_period_end.py --apply",
            "prose": (
                f"Run `uv run scripts/backfill_q4_period_end.py` (dry run) "
                f"to confirm the affected rows, then `--apply` to fix. "
                f"Going forward, the FMP ingest canonicalizes Q4 quarterly "
                f"period_end to match the FY annual filing's date."
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
                "q4_period_end": str(q4_pe),
                "fy_annual_period_end": str(fy_pe),
                "row_count": row_count,
            },
            summary=summary,
            suggested_action=suggested,
        )
