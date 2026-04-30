"""section_text_suspicious_length: a section's extracted text is so short
relative to its peers that it almost certainly contains only a heading
and no body.

The classic failure mode this catches (discovered 2026-04-30 against MSFT
10-Q MD&A): the deterministic parser locks onto the table-of-contents
heading, can't find body text past it because the next-item boundary
fires one line later, and emits a "section" containing only the heading
itself (~85 chars). To `unparsed_body_fallback` and the row-level
provenance machinery, the section looks fine — it has a row, has
provenance, has reasonable confidence. The pathology is only visible
relative to the universe norm for the same `section_key`.

Threshold (calibrated against the actual MSFT failure data 2026-04-30):
A section is "suspiciously short" when ALL of:
  - peer median for the section_key is ≥ 10,000 chars (only flag where
    the section type genuinely has substantial content in the universe)
  - this row's text is < 1% of the peer median (catches heading-only
    artifacts at ~85-300 chars when peer median is 30-100k)
  - this row's text is < 500 chars (absolute floor — legitimately short
    "see our 10-K" boilerplate sections sit at 500-3000 chars)

This intentionally does NOT flag every quarterly "see our 10-K Risk
Factors" boilerplate section, which is real content even though it's
short relative to a 10-K's full risk-factor disclosure.

Severity: warning. The data isn't corrupted; it's missing. Re-extract
or investigate the parser against the offending filing.
"""

from __future__ import annotations

from typing import Iterable

import psycopg

from arrow.steward.fingerprint import fingerprint
from arrow.steward.registry import Check, FindingDraft, Scope, register


# Only flag stubs in sections where the universe norm is substantial.
# Legal proceedings, controls, and similar sections are legitimately
# short and shouldn't be flagged.
MIN_PEER_MEDIAN_CHARS = 10_000

# Maximum ratio (this row vs peer median) for a stub. 1% catches the
# 85-char-on-30k-median heading-only failure; legitimate boilerplate
# ("see our 10-K") sits at 1-3% of peer median.
STUB_FRACTION_THRESHOLD = 0.01

# Absolute char floor. Calibrated against the MSFT 10-Q MD&A failure
# (uniformly 85 chars = section title only) and CAT/GEV legitimate
# "see our 10-K" boilerplate (174-336 chars = a real-but-brief disclosure).
# 200 cleanly separates: heading-only artifacts sit under 100 chars;
# legitimate-but-brief boilerplate sections sit at 170+ chars.
ABSOLUTE_STUB_CHARS = 200


@register
class SectionTextSuspiciousLength(Check):
    name = "section_text_suspicious_length"
    severity = "warning"
    vertical = "sec_qual"

    def run(self, conn: psycopg.Connection, *, scope: Scope) -> Iterable[FindingDraft]:
        sql = """
        WITH section_stats AS (
            SELECT section_key,
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY length(text)) AS median_chars
            FROM artifact_sections
            WHERE length(text) > 0
            GROUP BY section_key
        ),
        offenders AS (
            SELECT sec.id AS section_id, sec.artifact_id, sec.section_key,
                   sec.section_title, length(sec.text) AS chars,
                   ss.median_chars,
                   a.fiscal_period_key, a.artifact_type, a.published_at,
                   co.id AS company_id, co.ticker
            FROM artifact_sections sec
            JOIN section_stats ss USING (section_key)
            JOIN artifacts a ON a.id = sec.artifact_id
            JOIN companies co ON co.id = a.company_id
            WHERE a.superseded_at IS NULL
              AND ss.median_chars >= %s
              AND length(sec.text) < ss.median_chars * %s
              AND length(sec.text) < %s
        )
        SELECT section_id, section_key, section_title, chars, median_chars,
               artifact_id, fiscal_period_key, artifact_type, published_at,
               company_id, ticker
        FROM offenders
        ORDER BY ticker, published_at DESC, section_key;
        """
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (MIN_PEER_MEDIAN_CHARS, STUB_FRACTION_THRESHOLD, ABSOLUTE_STUB_CHARS),
            )
            rows = cur.fetchall()

        scope_tickers: set[str] | None = (
            {t.upper() for t in scope.tickers} if scope.tickers is not None else None
        )

        for row in rows:
            (section_id, section_key, section_title, chars, median_chars,
             artifact_id, fiscal_period_key, artifact_type, published_at,
             company_id, ticker) = row
            if scope_tickers is not None and ticker.upper() not in scope_tickers:
                continue

            ratio = chars / median_chars if median_chars else 0
            fp = fingerprint(
                self.name,
                scope={
                    "company_id": company_id,
                    "artifact_id": artifact_id,
                    "section_key": section_key,
                },
                rule_params={
                    "stub_fraction_threshold": STUB_FRACTION_THRESHOLD,
                    "min_peer_median_chars": MIN_PEER_MEDIAN_CHARS,
                    "absolute_stub_chars": ABSOLUTE_STUB_CHARS,
                },
            )
            yield FindingDraft(
                fingerprint=fp,
                finding_type=self.name,
                severity=self.severity,
                company_id=company_id,
                ticker=ticker,
                vertical=self.vertical,
                fiscal_period_key=fiscal_period_key,
                evidence={
                    "section_id": section_id,
                    "artifact_id": artifact_id,
                    "section_key": section_key,
                    "section_title": section_title,
                    "chars": int(chars),
                    "peer_median_chars": int(median_chars),
                    "ratio_to_peer_median": round(ratio, 3),
                    "artifact_type": artifact_type,
                },
                summary=(
                    f"{ticker} {fiscal_period_key} {artifact_type}: "
                    f"{section_key} is {chars:,} chars vs peer median "
                    f"{int(median_chars):,} ({ratio:.1%}). Likely a parser "
                    f"failure that emitted only a heading."
                ),
                suggested_action={
                    "kind": "reextract_artifact",
                    "params": {"artifact_id": artifact_id, "ticker": ticker},
                    "command": (
                        f"uv run scripts/reextract_sec_qualitative.py "
                        f"--ticker {ticker}"
                    ),
                    "prose": (
                        f"This section's text is dramatically shorter than its "
                        f"peer norm — usually means the parser locked onto a "
                        f"heading (e.g. table-of-contents entry) and couldn't "
                        f"find the body. Re-run extraction; if the stub "
                        f"persists, inspect the filing's HTML and see what "
                        f"layout difference is defeating the parser regex."
                    ),
                },
            )
