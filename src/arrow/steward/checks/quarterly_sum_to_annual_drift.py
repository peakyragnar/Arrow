"""quarterly_sum_to_annual_drift: surface IS rows where Q1+Q2+Q3+Q4 ≠ annual.

For income-statement flow concepts (revenue, COGS, R&D, etc.), the four
fiscal quarters should sum to the annual within rounding tolerance. A
material gap means one of the quarter rows is wrong — typically the
fabricated-Q4 pattern where FMP ships a ``Q4 = annual − (Q1+Q2+Q3)``
calculation that fails because one of the inputs (annual or interim)
was reported on a different basis (continuing ops vs total, before/after
a discontinued ops split, etc.).

Empirical case (2026-04-27): DELL FY2022 Q4 IS — every flow concept
failed by hundreds of millions to billions, traced to FMP's mishandling
of the VMWare spinoff that completed in Q3 FY22. Fixed via
``scripts/correct_corrupted_q4_is.py``.

Threshold: gap must exceed both ``MIN_ABSOLUTE_GAP`` (in USD) AND
``MIN_RELATIVE_GAP`` of the annual magnitude. Avoids noise on small
rounding drift while catching real fabrication.

Vertical: ``"financials"``. Scope: ``scope.tickers`` filters by ticker.
"""

from __future__ import annotations

from typing import Iterable

import psycopg

from arrow.steward.fingerprint import fingerprint
from arrow.steward.registry import Check, FindingDraft, Scope, register


SUMMABLE_FLOW_CONCEPTS = (
    "revenue",
    "cogs",
    "gross_profit",
    "rd",
    "sga",
    "total_opex",
    "operating_income",
    "ebt_incl_unusual",
    "net_income",
    "net_income_attributable_to_parent",
    "tax",
    "interest_expense",
    "continuing_ops_after_tax",
)

#: Minimum absolute gap (USD) before flagging. Below this is rounding /
#: reclassification noise. Calibrated against the live corpus 2026-04-27:
#: $50M is comfortably above normal restatement drift but well below the
#: hundreds-of-millions-to-billions gaps seen in real Q4-fabrication cases.
MIN_ABSOLUTE_GAP = 50_000_000

#: Minimum gap as a fraction of annual magnitude. Five percent excludes
#: typical 1–3% reclassification drift while catching the >25% gaps that
#: signal real fabrication (DELL FY22 Q4 was 5.7% on revenue, much higher
#: on the affected expense lines; AMZN FY16 rd was 44%).
MIN_RELATIVE_GAP = 0.05


@register
class QuarterlySumToAnnualDrift(Check):
    name = "quarterly_sum_to_annual_drift"
    severity = "warning"
    vertical = "financials"

    def run(self, conn: psycopg.Connection, *, scope: Scope) -> Iterable[FindingDraft]:
        sql = [
            "WITH quarterly AS (",
            "  SELECT ff.company_id, c.ticker, ff.fiscal_year, ff.concept,",
            "         SUM(ff.value) AS sum_q",
            "  FROM financial_facts ff",
            "  JOIN companies c ON c.id = ff.company_id",
            "  WHERE ff.statement = 'income_statement'",
            "    AND ff.period_type = 'quarter'",
            "    AND ff.fiscal_quarter IN (1, 2, 3, 4)",
            "    AND ff.superseded_at IS NULL",
            "    AND ff.dimension_type IS NULL",
            "    AND ff.concept = ANY(%s)",
        ]
        params: list = [list(SUMMABLE_FLOW_CONCEPTS)]
        if scope.tickers is not None:
            sql.append("    AND c.ticker = ANY(%s)")
            params.append([t.upper() for t in scope.tickers])
        sql.extend([
            "  GROUP BY ff.company_id, c.ticker, ff.fiscal_year, ff.concept",
            "  HAVING COUNT(DISTINCT ff.fiscal_quarter) = 4",
            "),",
            "annual AS (",
            "  SELECT ff.company_id, ff.fiscal_year, ff.concept, ff.value AS annual_value",
            "  FROM financial_facts ff",
            "  WHERE ff.statement = 'income_statement'",
            "    AND ff.period_type = 'annual'",
            "    AND ff.superseded_at IS NULL",
            "    AND ff.dimension_type IS NULL",
            "    AND ff.concept = ANY(%s)",
            ")",
            "SELECT q.company_id, q.ticker, q.fiscal_year, q.concept,",
            "       a.annual_value, q.sum_q,",
            "       (q.sum_q - a.annual_value) AS gap",
            "FROM quarterly q",
            "JOIN annual a USING (company_id, fiscal_year, concept)",
            f"WHERE ABS(q.sum_q - a.annual_value) >= {MIN_ABSOLUTE_GAP}",
            f"  AND ABS(q.sum_q - a.annual_value) >= {MIN_RELATIVE_GAP} * NULLIF(ABS(a.annual_value), 0)",
            "ORDER BY q.ticker, q.fiscal_year, q.concept;",
        ])
        params.append(list(SUMMABLE_FLOW_CONCEPTS))

        with conn.cursor() as cur:
            cur.execute("\n".join(sql), params)
            rows = cur.fetchall()

        for company_id, ticker, fiscal_year, concept, annual_value, sum_q, gap in rows:
            yield self._build_draft(
                company_id=company_id,
                ticker=ticker,
                fiscal_year=fiscal_year,
                concept=concept,
                annual_value=float(annual_value),
                sum_q=float(sum_q),
                gap=float(gap),
            )

    def _build_draft(
        self,
        *,
        company_id: int,
        ticker: str,
        fiscal_year: int,
        concept: str,
        annual_value: float,
        sum_q: float,
        gap: float,
    ) -> FindingDraft:
        period = f"FY{fiscal_year}"
        fp = fingerprint(
            self.name,
            scope={
                "company_id": company_id,
                "fiscal_year": fiscal_year,
                "concept": concept,
            },
            rule_params={
                "min_absolute_gap": MIN_ABSOLUTE_GAP,
                "min_relative_gap": MIN_RELATIVE_GAP,
            },
        )
        gap_pct = abs(gap) / abs(annual_value) if annual_value else 0.0
        summary = (
            f"{ticker} {period} {concept} — Q1+Q2+Q3+Q4 = {sum_q:,.0f} but "
            f"annual = {annual_value:,.0f} (gap {gap:+,.0f}, {gap_pct:.1%}). "
            f"One of the quarter rows is likely fabricated — see "
            f"`scripts/correct_corrupted_q4_is.py`."
        )
        suggested = {
            "kind": "correct_corrupted_q4_is",
            "params": {"ticker": ticker, "fiscal_year": fiscal_year},
            "command": (
                f"uv run scripts/correct_corrupted_q4_is.py "
                f"--ticker {ticker} --fiscal-year {fiscal_year}"
            ),
            "prose": (
                f"Inspect the four quarterly values for {ticker} {period} "
                f"{concept}. If Q4 is the outlier (most common — FMP often "
                f"derives Q4 = annual - prior quarters with one input on a "
                f"wrong basis), supersede the corrupted row and write a "
                f"derived replacement via "
                f"`uv run scripts/correct_corrupted_q4_is.py "
                f"--ticker {ticker} --fiscal-year {fiscal_year}`. "
                f"That script handles all summable flow concepts in one pass."
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
                "concept": concept,
                "fiscal_year": fiscal_year,
                "annual_value": annual_value,
                "sum_quarters": sum_q,
                "gap": gap,
                "gap_share": round(gap_pct, 4),
            },
            summary=summary,
            suggested_action=suggested,
        )
