"""sec_artifact_orphans: SEC filings with no extracted sections or text units.

A SEC filing artifact (10-K / 10-Q / press_release) should normally
have either ``artifact_sections`` rows (10-K/Q canonical sectioning) or
``artifact_text_units`` rows (press release extraction). An artifact
with neither was either ingested before the extraction layer existed,
hit an extractor failure, or has a structure the extractor couldn't
parse. Either way it isn't searchable from the analyst layer — that's
a problem worth surfacing.

One finding per orphaned artifact. Severity warning. Vertical
``"sec_qual"``.

Scope: ``scope.tickers`` filters by ``artifacts.ticker``.

Note: filters by ``artifact_type`` rather than ``form_family`` because
``form_family`` is currently constrained to NULL/'10-K'/'10-Q' — 8-K
envelopes carry NULL form_family. The artifact_type column has clean
values per filing kind.
"""

from __future__ import annotations

from typing import Iterable

import psycopg

from arrow.steward.fingerprint import fingerprint
from arrow.steward.registry import Check, FindingDraft, Scope, register

#: artifact_type values that should always have either sections or
#: text_units. '8k' (the envelope) is excluded — the press_release
#: exhibits attached to it are separate artifacts and ARE checked.
SEC_ARTIFACT_TYPES = ("10k", "10q", "press_release")


@register
class SecArtifactOrphans(Check):
    name = "sec_artifact_orphans"
    severity = "warning"
    vertical = "sec_qual"

    def run(self, conn: psycopg.Connection, *, scope: Scope) -> Iterable[FindingDraft]:
        sql = [
            "SELECT a.id, a.ticker, a.company_id, a.artifact_type,",
            "       a.fiscal_period_key, a.accession_number, a.published_at",
            "FROM artifacts a",
            "WHERE a.artifact_type = ANY(%s)",
            "  AND a.superseded_at IS NULL",
            "  AND NOT EXISTS (",
            "        SELECT 1 FROM artifact_sections s WHERE s.artifact_id = a.id",
            "  )",
            "  AND NOT EXISTS (",
            "        SELECT 1 FROM artifact_text_units u WHERE u.artifact_id = a.id",
            "  )",
        ]
        params: list = [list(SEC_ARTIFACT_TYPES)]
        if scope.tickers is not None:
            sql.append("  AND a.ticker = ANY(%s)")
            params.append([t.upper() for t in scope.tickers])
        sql.append("ORDER BY a.published_at DESC NULLS LAST;")

        with conn.cursor() as cur:
            cur.execute("\n".join(sql), params)
            rows = cur.fetchall()

        for artifact_id, ticker, company_id, artifact_type, fpk, accession, published_at in rows:
            yield self._build_draft(
                artifact_id=artifact_id,
                ticker=ticker,
                company_id=company_id,
                artifact_type=artifact_type,
                fiscal_period_key=fpk,
                accession=accession,
                published_at=published_at,
            )

    def _build_draft(self, *, artifact_id, ticker, company_id, artifact_type,
                     fiscal_period_key, accession, published_at) -> FindingDraft:
        fp = fingerprint(
            self.name,
            scope={"artifact_id": artifact_id},
            rule_params={},
        )
        type_label = artifact_type.upper() if artifact_type else "?"
        summary = (
            f"{ticker} {type_label} ({accession or 'no accession'}) has no "
            f"extracted sections or text units — not searchable from the analyst layer."
        )
        suggested = {
            "kind": "re_extract_artifact",
            "params": {"artifact_id": artifact_id, "artifact_type": artifact_type},
            "command": (
                f"# Re-run SEC qualitative extraction for this artifact.\n"
                f"uv run python -c "
                f"\"from arrow.db.connection import get_conn; "
                f"from arrow.ingest.sec.qualitative import extract_for_artifact; "
                f"with get_conn() as c: extract_for_artifact(c, {artifact_id})\""
            ),
            "prose": (
                f"This {type_label} filing was ingested as an artifact but no "
                f"sections or text units were extracted from it. Likely causes: "
                f"the extractor's regex didn't match the filing's heading layout, "
                f"the document is empty, or extraction was skipped at ingest time. "
                f"Inspect the raw_primary_doc_path; if extractor logic needs "
                f"updating, fix it and re-extract. If the filing is genuinely "
                f"empty (rare), suppress with reason."
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
                "published_at": published_at.isoformat() if published_at else None,
            },
            summary=summary,
            suggested_action=suggested,
        )
