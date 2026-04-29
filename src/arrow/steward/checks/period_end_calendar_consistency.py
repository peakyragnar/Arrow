"""period_end_calendar_consistency: catch drift between a row's
``period_end`` and its dependent ``calendar_year`` /
``calendar_quarter`` / ``calendar_period_label`` fields.

The calendar fields are pure functions of ``period_end`` (per
``arrow.normalize.periods.derive.derive_calendar_period``). Any
divergence is a data-integrity bug: ``v_company_period_wide`` groups
by all calendar fields, so a row whose calendar_quarter disagrees with
its period_end will be split into a separate group from its peers,
producing duplicate-looking columns in the dashboard.

Original incident (2026-04-29): ``backfill_cross_endpoint_period_end.py``
snapped ``period_end`` (e.g. ``2023-06-30`` → ``2023-07-01``) but did
not update calendar_*. For non-calendar fiscal-year filers like LITE
(June fiscal year-end), the snap crossed CY-Q2 → CY-Q3, leaving 900
rows with stale calendar_quarter=2 and label='CY2023 Q2'.

Suggested fix: ``uv run scripts/repair_calendar_fields.py``.
"""

from __future__ import annotations

from typing import Iterable

import psycopg

from arrow.steward.fingerprint import fingerprint
from arrow.steward.registry import Check, FindingDraft, Scope, register


@register
class PeriodEndCalendarConsistency(Check):
    name = "period_end_calendar_consistency"
    severity = "warning"
    vertical = "financials"

    def run(self, conn: psycopg.Connection, *, scope: Scope) -> Iterable[FindingDraft]:
        sql = [
            "SELECT ff.company_id, c.ticker, ff.fiscal_year, ff.fiscal_quarter,",
            "       ff.period_type, ff.statement, ff.extraction_version,",
            "       ff.period_end,",
            "       ff.calendar_year, ff.calendar_quarter, ff.calendar_period_label,",
            "       EXTRACT(YEAR FROM ff.period_end)::int AS implied_year,",
            "       EXTRACT(QUARTER FROM ff.period_end)::int AS implied_quarter,",
            "       COUNT(*) AS row_count",
            "FROM financial_facts ff",
            "JOIN companies c ON c.id = ff.company_id",
            "WHERE ff.superseded_at IS NULL",
            "  AND ff.period_end IS NOT NULL",
            "  AND (",
            "    ff.calendar_year <> EXTRACT(YEAR FROM ff.period_end)::int",
            "    OR ff.calendar_quarter <> EXTRACT(QUARTER FROM ff.period_end)::int",
            "    OR ff.calendar_period_label <> ('CY' || EXTRACT(YEAR FROM ff.period_end)::int",
            "                                    || ' Q' || EXTRACT(QUARTER FROM ff.period_end)::int)",
            "  )",
        ]
        params: list = []
        if scope.tickers is not None:
            sql.append("  AND c.ticker = ANY(%s)")
            params.append([t.upper() for t in scope.tickers])
        sql.extend([
            "GROUP BY ff.company_id, c.ticker, ff.fiscal_year, ff.fiscal_quarter,",
            "         ff.period_type, ff.statement, ff.extraction_version,",
            "         ff.period_end, ff.calendar_year, ff.calendar_quarter,",
            "         ff.calendar_period_label",
            "ORDER BY c.ticker, ff.fiscal_year, ff.period_type, ff.statement;",
        ])

        with conn.cursor() as cur:
            cur.execute("\n".join(sql), params)
            rows = cur.fetchall()

        for (
            company_id, ticker, fiscal_year, fiscal_quarter, period_type,
            statement, extraction_version, period_end,
            stored_year, stored_q, stored_label,
            implied_year, implied_q, row_count,
        ) in rows:
            yield self._build_draft(
                company_id=company_id,
                ticker=ticker,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                period_type=period_type,
                statement=statement,
                extraction_version=extraction_version,
                period_end=period_end,
                stored_year=stored_year,
                stored_q=stored_q,
                stored_label=stored_label,
                implied_year=implied_year,
                implied_q=implied_q,
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
        period_end,
        stored_year: int,
        stored_q: int,
        stored_label: str,
        implied_year: int,
        implied_q: int,
        row_count: int,
    ) -> FindingDraft:
        if fiscal_quarter is None:
            period = f"FY{fiscal_year}"
        else:
            period = f"FY{fiscal_year} Q{fiscal_quarter}"
        implied_label = f"CY{implied_year} Q{implied_q}"
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
            f"{ticker} {period} {statement} period_end {period_end} "
            f"implies {implied_label} but stored as ({stored_year}, Q{stored_q}, "
            f"{stored_label!r}) ({row_count} rows). The calendar_* fields are "
            f"pure functions of period_end; mismatches split metric-view rows "
            f"into duplicate columns."
        )
        suggested = {
            "kind": "repair_calendar_fields",
            "params": {"ticker": ticker, "fiscal_year": fiscal_year},
            "command": "uv run scripts/repair_calendar_fields.py --apply",
            "prose": (
                "Run `uv run scripts/repair_calendar_fields.py` (dry run) to "
                "confirm the affected rows, then `--apply` to fix. Recomputes "
                "calendar_year / calendar_quarter / calendar_period_label from "
                "the row's current period_end."
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
                "period_end": str(period_end),
                "stored_calendar_year": stored_year,
                "stored_calendar_quarter": stored_q,
                "stored_calendar_period_label": stored_label,
                "implied_calendar_year": implied_year,
                "implied_calendar_quarter": implied_q,
                "implied_calendar_period_label": implied_label,
                "row_count": row_count,
            },
            summary=summary,
            suggested_action=suggested,
        )
