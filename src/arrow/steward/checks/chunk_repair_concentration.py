"""chunk_repair_concentration: surface single-filing extraction
degradation that the corpus-level drift check can't see.

The SEC qualitative extractor classifies each section by
``extraction_method``:

  - ``'deterministic'`` — regex matched cleanly (confidence ≥ 0.85)
  - ``'repair'``        — needed remediation (0 < confidence < 0.85)
  - ``'unparsed_fallback'`` — gave up (confidence = 0)

`extraction_method_drift` (the corpus-level check) compares the
share of `deterministic` sections per (form_family, section_key)
across rolling windows. It catches systemic regressions but needs
≥10 sections per pair per window to fire and only sees patterns
that play out over weeks.

This check fills the gap: a single filing where most sections fell
to `repair` is a per-filing degradation worth noticing immediately,
not 30+ days later when drift catches it (if ever — single
filings rarely move corpus averages enough to trigger drift).

Threshold (designed against the live corpus 2026-04-26):

  - artifact must have ≥ ``MIN_SECTIONS`` sections (avoid noise on
    tiny / amendment artifacts that legitimately have few sections)
  - repair_share must be > ``MIN_REPAIR_SHARE`` of those sections

These thresholds catch exactly 1 artifact in the current corpus:
META FY2025 Q1 10-Q (6/6 sections in repair = 100%). Every other
ticker has 100% deterministic extraction. The check is calibrated
to the actual signal distribution, not picked from thin air.

Cross-cutting check (``vertical = 'sec_qual'``, ticker carried on
findings so the dashboard can filter). One finding per affected
artifact.

Scope: ``scope.tickers`` filters by ``artifacts.ticker``.
"""

from __future__ import annotations

from typing import Iterable

import psycopg

from arrow.steward.fingerprint import fingerprint
from arrow.steward.registry import Check, FindingDraft, Scope, register

#: Minimum total sections on the artifact for the check to apply.
#: Avoids noise on tiny / amendment filings that legitimately have
#: few sections.
MIN_SECTIONS = 3

#: Repair share that triggers the finding. ">0.5" means "more than
#: half the sections in this artifact needed repair extraction."
MIN_REPAIR_SHARE = 0.5


@register
class ChunkRepairConcentration(Check):
    name = "chunk_repair_concentration"
    severity = "warning"
    vertical = "sec_qual"

    def run(self, conn: psycopg.Connection, *, scope: Scope) -> Iterable[FindingDraft]:
        sql = [
            "SELECT a.id, a.ticker, a.company_id, a.artifact_type,",
            "       a.fiscal_period_key, a.accession_number, a.published_at,",
            "       COUNT(*) FILTER (WHERE s.extraction_method = 'repair') AS repair_count,",
            "       COUNT(*) AS total_sections",
            "FROM artifacts a",
            "JOIN artifact_sections s ON s.artifact_id = a.id",
            "WHERE a.superseded_at IS NULL",
        ]
        params: list = []
        if scope.tickers is not None:
            sql.append("  AND a.ticker = ANY(%s)")
            params.append([t.upper() for t in scope.tickers])
        sql.extend([
            "GROUP BY a.id, a.ticker, a.company_id, a.artifact_type,",
            "         a.fiscal_period_key, a.accession_number, a.published_at",
            f"HAVING COUNT(*) >= {MIN_SECTIONS}",
            f"   AND COUNT(*) FILTER (WHERE s.extraction_method = 'repair') * 1.0 / COUNT(*) > {MIN_REPAIR_SHARE}",
            "ORDER BY a.published_at DESC NULLS LAST;",
        ])

        with conn.cursor() as cur:
            cur.execute("\n".join(sql), params)
            rows = cur.fetchall()

        for (
            artifact_id, ticker, company_id, artifact_type, fpk, accession,
            published_at, repair_count, total_sections,
        ) in rows:
            yield self._build_draft(
                artifact_id=artifact_id, ticker=ticker, company_id=company_id,
                artifact_type=artifact_type, fiscal_period_key=fpk,
                accession=accession, published_at=published_at,
                repair_count=repair_count, total_sections=total_sections,
            )

    def _build_draft(
        self, *, artifact_id, ticker, company_id, artifact_type,
        fiscal_period_key, accession, published_at, repair_count, total_sections,
    ) -> FindingDraft:
        repair_share = repair_count / total_sections if total_sections else 0.0
        fp = fingerprint(
            self.name,
            scope={"artifact_id": artifact_id},
            rule_params={
                "min_sections": MIN_SECTIONS,
                "min_repair_share": MIN_REPAIR_SHARE,
            },
        )
        type_label = artifact_type.upper() if artifact_type else "?"
        summary = (
            f"{ticker} {type_label} ({accession or 'no accession'}) — "
            f"{repair_count}/{total_sections} sections ({repair_share:.0%}) "
            f"fell to repair extraction. Possible per-filing layout shift."
        )
        suggested = {
            "kind": "investigate_filing_layout",
            "params": {
                "artifact_id": artifact_id, "ticker": ticker,
                "accession": accession,
            },
            "command": (
                f"# Open the raw filing to inspect heading layout:\n"
                f"uv run python -c "
                f"\"from arrow.db.connection import get_conn; "
                f"with get_conn() as c, c.cursor() as cur: "
                f"cur.execute('SELECT raw_primary_doc_path FROM artifacts WHERE id=%s', "
                f"({artifact_id},)); print(cur.fetchone()[0])\""
            ),
            "prose": (
                f"On this {type_label} for {ticker}, {repair_count} of "
                f"{total_sections} sections needed the 'repair' extraction "
                f"path (lower confidence than 'deterministic'). The data is "
                f"present and searchable — the extractor just needed to "
                f"work harder to find section boundaries.\n\n"
                f"Likely causes:\n"
                f"  - Filer changed their HTML template (filing agent change, "
                f"new tooling, etc) and the new layout differs subtly from "
                f"prior filings.\n"
                f"  - One-off styling quirks under deadline pressure.\n\n"
                f"Triage:\n"
                f"  - Open raw_primary_doc_path; compare heading styling to "
                f"a clean prior filing for the same ticker.\n"
                f"  - If recurring across the same filer's subsequent "
                f"filings: fix the extractor regex.\n"
                f"  - If one-off (e.g. filing-agent change): suppress with a "
                f"reason naming the trigger."
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
                "artifact_type": artifact_type,
                "accession_number": accession,
                "repair_count": repair_count,
                "total_sections": total_sections,
                "repair_share": repair_share,
                "published_at": published_at.isoformat() if published_at else None,
            },
            summary=summary,
            suggested_action=suggested,
        )
