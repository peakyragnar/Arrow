"""transcript_artifact_orphans: transcript artifacts with no text units."""

from __future__ import annotations

from typing import Iterable

import psycopg

from arrow.steward.fingerprint import fingerprint
from arrow.steward.registry import Check, FindingDraft, Scope, register


@register
class TranscriptArtifactOrphans(Check):
    name = "transcript_artifact_orphans"
    severity = "warning"
    vertical = "transcript"

    def run(self, conn: psycopg.Connection, *, scope: Scope) -> Iterable[FindingDraft]:
        sql = [
            "SELECT a.id, a.ticker, a.company_id, a.fiscal_period_key,",
            "       a.source_document_id, a.published_at",
            "FROM artifacts a",
            "WHERE a.artifact_type = 'transcript'",
            "  AND a.superseded_at IS NULL",
            "  AND NOT EXISTS (",
            "        SELECT 1 FROM artifact_text_units u WHERE u.artifact_id = a.id",
            "  )",
        ]
        params: list = []
        if scope.tickers is not None:
            sql.append("  AND a.ticker = ANY(%s)")
            params.append([t.upper() for t in scope.tickers])
        sql.append("ORDER BY a.published_at DESC NULLS LAST;")

        with conn.cursor() as cur:
            cur.execute("\n".join(sql), params)
            rows = cur.fetchall()

        for artifact_id, ticker, company_id, fiscal_period_key, source_document_id, published_at in rows:
            yield self._build_draft(
                artifact_id=artifact_id,
                ticker=ticker,
                company_id=company_id,
                fiscal_period_key=fiscal_period_key,
                source_document_id=source_document_id,
                published_at=published_at,
            )

    def _build_draft(
        self,
        *,
        artifact_id,
        ticker,
        company_id,
        fiscal_period_key,
        source_document_id,
        published_at,
    ) -> FindingDraft:
        fp = fingerprint(
            self.name,
            scope={"artifact_id": artifact_id},
            rule_params={},
        )
        summary = (
            f"{ticker} transcript {fiscal_period_key or source_document_id or artifact_id} "
            f"has no text units — not searchable from the analyst layer."
        )
        suggested = {
            "kind": "reingest_transcript",
            "params": {"artifact_id": artifact_id, "ticker": ticker},
            "command": f"uv run scripts/ingest_transcripts.py --refresh {ticker}",
            "prose": (
                "This transcript artifact exists but no speaker-turn or fallback "
                "text unit was written. Re-run transcript ingest with --refresh. "
                "If the artifact body is genuinely empty, suppress with a reason."
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
                "source_document_id": source_document_id,
                "published_at": published_at.isoformat() if published_at else None,
            },
            summary=summary,
            suggested_action=suggested,
        )
