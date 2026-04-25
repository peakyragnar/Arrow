"""unparsed_body_fallback: artifacts whose section extractor fell back
to the catch-all ``unparsed_body`` section.

The SEC qualitative extractor produces canonical section keys
(``item_7_mda``, ``item_1a_risk_factors``, etc.) for filings whose
layout it can parse. When it can't, it stores the entire body as a
single section with ``section_key = 'unparsed_body'`` so the retrieval
contract stays uniform — but that's a degraded result. Each such
artifact is a candidate for extractor improvement.

One finding per affected artifact (an artifact may have multiple
fallback rows; we group). Severity warning. Vertical ``"sec_qual"``.

Scope: ``scope.tickers`` filters by ``artifacts.ticker``.
"""

from __future__ import annotations

from typing import Iterable

import psycopg

from arrow.steward.fingerprint import fingerprint
from arrow.steward.registry import Check, FindingDraft, Scope, register


@register
class UnparsedBodyFallback(Check):
    name = "unparsed_body_fallback"
    severity = "warning"
    vertical = "sec_qual"

    def run(self, conn: psycopg.Connection, *, scope: Scope) -> Iterable[FindingDraft]:
        sql = [
            "SELECT a.id, a.ticker, a.company_id, a.form_family,",
            "       a.fiscal_period_key, a.accession_number, a.published_at,",
            "       COUNT(s.id) AS fallback_section_count",
            "FROM artifacts a",
            "JOIN artifact_sections s ON s.artifact_id = a.id",
            "WHERE s.section_key = 'unparsed_body'",
            "  AND a.superseded_at IS NULL",
        ]
        params: list = []
        if scope.tickers is not None:
            sql.append("  AND a.ticker = ANY(%s)")
            params.append([t.upper() for t in scope.tickers])
        sql.append("GROUP BY a.id, a.ticker, a.company_id, a.form_family,")
        sql.append("         a.fiscal_period_key, a.accession_number, a.published_at")
        sql.append("ORDER BY a.published_at DESC NULLS LAST;")

        with conn.cursor() as cur:
            cur.execute("\n".join(sql), params)
            rows = cur.fetchall()

        for (
            artifact_id, ticker, company_id, form_family,
            fpk, accession, published_at, n_fallback,
        ) in rows:
            yield self._build_draft(
                artifact_id=artifact_id,
                ticker=ticker,
                company_id=company_id,
                form_family=form_family,
                fiscal_period_key=fpk,
                accession=accession,
                published_at=published_at,
                fallback_count=n_fallback,
            )

    def _build_draft(self, *, artifact_id, ticker, company_id, form_family,
                     fiscal_period_key, accession, published_at,
                     fallback_count) -> FindingDraft:
        fp = fingerprint(
            self.name,
            scope={"artifact_id": artifact_id},
            rule_params={},
        )
        summary = (
            f"{ticker} {form_family} ({accession or 'no accession'}) has "
            f"{fallback_count} unparsed_body section(s) — extractor fell back "
            f"to catch-all instead of canonical section keys."
        )
        suggested = {
            "kind": "investigate_extractor",
            "params": {
                "artifact_id": artifact_id,
                "form_family": form_family,
                "ticker": ticker,
            },
            "command": (
                f"uv run python -c "
                f"\"from arrow.db.connection import get_conn; "
                f"with get_conn() as c, c.cursor() as cur: "
                f"cur.execute('SELECT raw_primary_doc_path FROM artifacts WHERE id=%s', "
                f"({artifact_id},)); print(cur.fetchone())\""
            ),
            "prose": (
                f"The SEC qualitative extractor couldn't identify section "
                f"boundaries in this {form_family} and stored the body as a "
                f"single unparsed_body section. Likely causes: the filing uses "
                f"a heading style the regex doesn't match, has a non-standard "
                f"layout (table-of-contents-only, unusual styling), or is a very "
                f"short filing without typical sections. Inspect "
                f"raw_primary_doc_path; if it's a layout the extractor should "
                f"handle, update src/arrow/ingest/sec/qualitative.py and "
                f"re-extract. If the filing genuinely lacks sections, suppress "
                f"with reason."
            ),
        }
        return FindingDraft(
            fingerprint=fp,
            finding_type=self.name,
            severity=self.severity,
            company_id=company_id,
            ticker=ticker,
            vertical=self.vertical,
            fiscal_period_key=fiscal_period_key,
            evidence={
                "artifact_id": artifact_id,
                "form_family": form_family,
                "accession_number": accession,
                "fallback_section_count": fallback_count,
                "published_at": published_at.isoformat() if published_at else None,
            },
            summary=summary,
            suggested_action=suggested,
        )
