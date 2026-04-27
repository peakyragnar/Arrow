"""quarterly_value_duplication: surface fabricated quarterly facts where a
vendor split a multi-period interim disclosure into individual quarters.

Pre-IPO companies typically disclose interim periods (H1 / 9-month) in
their S-1 prospectus, NOT individual quarters. When a vendor (FMP) ships
quarterly rows for these periods anyway, they're commonly fabricated by
splitting the interim subtotal evenly across constituent quarters. The
giveaway is exact value equality across consecutive quarters in the
same fiscal year.

Empirical case (2026-04-27): PLTR FY2019 Q1 vs Q2 cash_flow had 14 of
16 non-zero concepts with IDENTICAL values (cfo, cff, cfi, capex, dna,
sbc, change_AR, etc.) — H1 split in half. The 87.5% duplication rate
is mathematically impossible for real reported quarterly data.

Threshold (designed against the live corpus 2026-04-27):

  - per (company, statement, fiscal_year) consecutive-quarter pair
  - need ≥ ``MIN_NONZERO_CONCEPTS`` non-zero concepts compared
    (avoid noise on companies with mostly-zero CF)
  - duplication rate must exceed ``MIN_DUP_SHARE`` of those concepts

These thresholds catch exactly the PLTR FY2019 Q1/Q2 cash_flow case
(87.5% duplication) and zero false positives across the rest of the
13-ticker corpus. Threshold sensitivity tested at 30/50/70/90% — all
isolate the same single pair, so 50% is comfortably in the middle.

Vertical: ``"financials"``. Scope: ``scope.tickers`` filters by the
flagged company's ticker.

Suggested action: investigate the originating disclosure (was this
period only available as an interim subtotal?) and supersede the
fabricated quarterly facts with reason ``fmp_fabricated_split``.
"""

from __future__ import annotations

from typing import Iterable

import psycopg

from arrow.steward.fingerprint import fingerprint
from arrow.steward.registry import Check, FindingDraft, Scope, register

#: Minimum non-zero concepts in the consecutive-quarter pair for the
#: check to apply. Avoids noise on (company, statement, fiscal_year)
#: combos with very sparse reported data.
MIN_NONZERO_CONCEPTS = 5

#: Duplication rate that triggers the finding. ">0.5" means "more than
#: half the non-zero concepts have identical values across the two
#: consecutive quarters" — mathematically impossible for real data.
MIN_DUP_SHARE = 0.5


@register
class QuarterlyValueDuplication(Check):
    name = "quarterly_value_duplication"
    severity = "warning"
    vertical = "financials"

    def run(self, conn: psycopg.Connection, *, scope: Scope) -> Iterable[FindingDraft]:
        sql = [
            "WITH quarterly AS (",
            "  SELECT ff.company_id, c.ticker, ff.statement, ff.fiscal_year,",
            "         ff.fiscal_quarter, ff.concept, ff.value",
            "  FROM financial_facts ff",
            "  JOIN companies c ON c.id = ff.company_id",
            "  WHERE ff.fiscal_quarter IN (1,2,3,4)",
            "    AND ff.superseded_at IS NULL",
            "    AND ff.dimension_type IS NULL",
        ]
        params: list = []
        if scope.tickers is not None:
            sql.append("    AND c.ticker = ANY(%s)")
            params.append([t.upper() for t in scope.tickers])
        sql.extend([
            "),",
            "pairs AS (",
            "  SELECT a.company_id, a.ticker, a.statement, a.fiscal_year,",
            "         a.fiscal_quarter AS q_a, b.fiscal_quarter AS q_b,",
            "         (a.value = b.value AND a.value <> 0) AS dup_nonzero,",
            "         (a.value <> 0 OR b.value <> 0) AS either_nonzero",
            "  FROM quarterly a",
            "  JOIN quarterly b",
            "    ON a.company_id = b.company_id",
            "   AND a.statement = b.statement",
            "   AND a.fiscal_year = b.fiscal_year",
            "   AND a.concept = b.concept",
            "   AND a.fiscal_quarter = b.fiscal_quarter - 1",
            "),",
            "rollup AS (",
            "  SELECT company_id, ticker, statement, fiscal_year, q_a, q_b,",
            "         COUNT(*) FILTER (WHERE either_nonzero) AS nonzero_concepts,",
            "         COUNT(*) FILTER (WHERE dup_nonzero) AS duplicated_nonzero",
            "  FROM pairs",
            "  GROUP BY company_id, ticker, statement, fiscal_year, q_a, q_b",
            ")",
            "SELECT company_id, ticker, statement, fiscal_year, q_a, q_b,",
            "       nonzero_concepts, duplicated_nonzero",
            "FROM rollup",
            f"WHERE nonzero_concepts >= {MIN_NONZERO_CONCEPTS}",
            f"  AND duplicated_nonzero::float / NULLIF(nonzero_concepts, 0) > {MIN_DUP_SHARE}",
            "ORDER BY ticker, fiscal_year, statement, q_a;",
        ])

        with conn.cursor() as cur:
            cur.execute("\n".join(sql), params)
            rows = cur.fetchall()

        for (
            company_id, ticker, statement, fiscal_year, q_a, q_b,
            nonzero_concepts, duplicated_nonzero,
        ) in rows:
            yield self._build_draft(
                company_id=company_id, ticker=ticker, statement=statement,
                fiscal_year=fiscal_year, q_a=q_a, q_b=q_b,
                nonzero_concepts=nonzero_concepts,
                duplicated_nonzero=duplicated_nonzero,
            )

    def _build_draft(
        self, *, company_id, ticker, statement, fiscal_year, q_a, q_b,
        nonzero_concepts, duplicated_nonzero,
    ) -> FindingDraft:
        dup_share = duplicated_nonzero / nonzero_concepts if nonzero_concepts else 0.0
        fp = fingerprint(
            self.name,
            scope={
                "company_id": company_id,
                "statement": statement,
                "fiscal_year": fiscal_year,
                "q_a": q_a,
                "q_b": q_b,
            },
            rule_params={
                "min_nonzero_concepts": MIN_NONZERO_CONCEPTS,
                "min_dup_share": MIN_DUP_SHARE,
            },
        )
        period = f"FY{fiscal_year} Q{q_a}/Q{q_b}"
        summary = (
            f"{ticker} {statement} {period} — {duplicated_nonzero}/"
            f"{nonzero_concepts} non-zero concepts ({dup_share:.0%}) have "
            f"IDENTICAL values across consecutive quarters. Likely vendor "
            f"fabricated quarterly split of an interim disclosure (H1 / 9-month)."
        )
        suggested = {
            "kind": "investigate_quarterly_fabrication",
            "params": {
                "ticker": ticker,
                "statement": statement,
                "fiscal_year": fiscal_year,
                "q_a": q_a,
                "q_b": q_b,
            },
            "command": (
                f"# Inspect the duplicated values:\n"
                f"uv run python -c \"from arrow.db.connection import get_conn; "
                f"with get_conn() as c, c.cursor() as cur: "
                f"cur.execute('''SELECT concept, fiscal_quarter, value FROM financial_facts ff JOIN companies c ON c.id=ff.company_id "
                f"WHERE c.ticker=%s AND ff.fiscal_year=%s AND ff.statement=%s AND ff.fiscal_quarter IN (%s,%s) AND ff.superseded_at IS NULL "
                f"ORDER BY concept, fiscal_quarter''', "
                f"('{ticker}', {fiscal_year}, '{statement}', {q_a}, {q_b})); "
                f"[print(r) for r in cur.fetchall()]\""
            ),
            "prose": (
                f"For {ticker} {period} {statement}, {duplicated_nonzero} of "
                f"{nonzero_concepts} non-zero concepts have IDENTICAL values "
                f"across the two consecutive quarters. Real reported quarterly "
                f"data essentially never produces this pattern — operating "
                f"results, working-capital changes, and cash flows vary "
                f"between quarters.\n\n"
                f"Most common cause: the vendor (FMP) populated quarterly "
                f"rows for a period that the filer disclosed only as an "
                f"interim subtotal (H1 in S-1 prospectus, 9-month in interim "
                f"financials, etc.). The vendor split the subtotal evenly "
                f"across constituent quarters. Pre-IPO periods are the most "
                f"common case — companies don't file 10-Qs before going "
                f"public, so quarterly data has to be derived.\n\n"
                f"Triage:\n"
                f"  - Check whether the company was public in this period "
                f"(IPO date / first 10-Q filing date).\n"
                f"  - If pre-public AND only interim disclosure exists: "
                f"supersede the affected facts with reason "
                f"'fmp_fabricated_split' to remove from current view.\n"
                f"  - If post-public: investigate the original 10-Q — this "
                f"may indicate a vendor data corruption rather than a "
                f"legitimate split."
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
                "fiscal_year": fiscal_year,
                "q_a": q_a,
                "q_b": q_b,
                "nonzero_concepts": nonzero_concepts,
                "duplicated_nonzero": duplicated_nonzero,
                "duplication_share": round(dup_share, 4),
            },
            summary=summary,
            suggested_action=suggested,
        )
