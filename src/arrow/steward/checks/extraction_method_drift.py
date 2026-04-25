"""extraction_method_drift: detect extractor regressions via method-share shifts.

The SEC qualitative extractor classifies each section by
``extraction_method``:

  - ``'deterministic'`` — regex matched cleanly (confidence ≥ 0.85)
  - ``'repair'``        — needed remediation (0 < confidence < 0.85)
  - ``'unparsed_fallback'`` — gave up entirely (confidence = 0)

A real extractor regression typically shows up as **sections leaving
the deterministic bucket** — confidence drops *across* methods, not
*within* a method. The schema's CHECK contract makes within-method
confidence drift narrow and bounded (deterministic is constrained to
[0.85, 1.0], so there's only ~0.15 of confidence range to drift in
that bucket); but the *share* of sections classified as deterministic
can drop sharply when a regex change misclassifies real headings.

This check measures that share, per ``(form_family, section_key)``,
over a recent window vs a baseline window. Alerts when the
deterministic share drops by ``MIN_SHARE_DROP`` percentage points or
more.

Why this is the right shape (not a within-bucket confidence z-test):

A within-bucket confidence drift check (an earlier draft of this
check) only sees sections that *stayed* deterministic. It misses the
realistic regression mode — sections demoted from deterministic to
repair or fallback — entirely, because those sections are filtered
out of its window. Method-share drift catches that regression
directly.

Cross-cutting check (``vertical = 'sec_qual'``, no ticker scope —
extractor degradation is corpus-wide). One finding per
``(form_family, section_key)`` pair that's degraded.

Scope: ``scope.tickers`` is ignored — this check is about the
extractor, not any one filer.
"""

from __future__ import annotations

from typing import Iterable

import psycopg

from arrow.steward.fingerprint import fingerprint
from arrow.steward.registry import Check, FindingDraft, Scope, register

#: Recent window: method-share over these many trailing days is the
#: signal we're testing.
RECENT_WINDOW_DAYS = 30

#: Baseline window: prior period to compare against. Starts where the
#: recent window ends and extends back this many additional days.
BASELINE_WINDOW_DAYS = 60

#: Minimum row count in EACH window for the test to fire. Below this,
#: the share estimates are too noisy to compare.
MIN_ROWS = 10

#: Threshold: how many *percentage points* the deterministic share must
#: drop in the recent window vs the baseline before we flag. 0.15 means
#: "the deterministic share fell by 15 points or more" — large enough
#: to be a real regression, small enough to catch incremental decay.
MIN_SHARE_DROP = 0.15


@register
class ExtractionMethodDrift(Check):
    name = "extraction_method_drift"
    severity = "warning"
    vertical = "sec_qual"

    def run(self, conn: psycopg.Connection, *, scope: Scope) -> Iterable[FindingDraft]:
        sql = f"""
            WITH recent AS (
              SELECT form_family, section_key,
                     COUNT(*) FILTER (WHERE extraction_method = 'deterministic') AS det_count,
                     COUNT(*) FILTER (WHERE extraction_method = 'repair')        AS repair_count,
                     COUNT(*) FILTER (WHERE extraction_method = 'unparsed_fallback') AS fallback_count,
                     COUNT(*) AS total
              FROM artifact_sections
              WHERE created_at > now() - interval '{RECENT_WINDOW_DAYS} days'
                AND form_family IS NOT NULL
                AND section_key <> 'unparsed_body'
              GROUP BY form_family, section_key
            ),
            baseline AS (
              SELECT form_family, section_key,
                     COUNT(*) FILTER (WHERE extraction_method = 'deterministic') AS det_count,
                     COUNT(*) FILTER (WHERE extraction_method = 'repair')        AS repair_count,
                     COUNT(*) FILTER (WHERE extraction_method = 'unparsed_fallback') AS fallback_count,
                     COUNT(*) AS total
              FROM artifact_sections
              WHERE created_at <= now() - interval '{RECENT_WINDOW_DAYS} days'
                AND created_at >  now() - interval '{RECENT_WINDOW_DAYS + BASELINE_WINDOW_DAYS} days'
                AND form_family IS NOT NULL
                AND section_key <> 'unparsed_body'
              GROUP BY form_family, section_key
            )
            SELECT b.form_family, b.section_key,
                   r.det_count AS recent_det,    r.total AS recent_total,
                   r.repair_count AS recent_repair,
                   r.fallback_count AS recent_fallback,
                   b.det_count AS baseline_det,  b.total AS baseline_total,
                   b.repair_count AS baseline_repair,
                   b.fallback_count AS baseline_fallback,
                   (r.det_count::float / r.total) AS recent_share,
                   (b.det_count::float / b.total) AS baseline_share,
                   (b.det_count::float / b.total) - (r.det_count::float / r.total) AS share_drop
            FROM baseline b
            JOIN recent r USING (form_family, section_key)
            WHERE r.total >= %s
              AND b.total >= %s
              AND (b.det_count::float / b.total) - (r.det_count::float / r.total) >= %s
            ORDER BY share_drop DESC;
        """

        with conn.cursor() as cur:
            cur.execute(sql, (MIN_ROWS, MIN_ROWS, MIN_SHARE_DROP))
            rows = cur.fetchall()

        for (
            form_family, section_key,
            recent_det, recent_total, recent_repair, recent_fallback,
            baseline_det, baseline_total, baseline_repair, baseline_fallback,
            recent_share, baseline_share, share_drop,
        ) in rows:
            yield self._build_draft(
                form_family=form_family,
                section_key=section_key,
                recent_det=recent_det,
                recent_total=recent_total,
                recent_repair=recent_repair,
                recent_fallback=recent_fallback,
                baseline_det=baseline_det,
                baseline_total=baseline_total,
                baseline_repair=baseline_repair,
                baseline_fallback=baseline_fallback,
                recent_share=float(recent_share),
                baseline_share=float(baseline_share),
                share_drop=float(share_drop),
            )

    def _build_draft(self, *, form_family, section_key,
                     recent_det, recent_total, recent_repair, recent_fallback,
                     baseline_det, baseline_total, baseline_repair, baseline_fallback,
                     recent_share, baseline_share, share_drop) -> FindingDraft:
        fp = fingerprint(
            self.name,
            scope={"form_family": form_family, "section_key": section_key},
            rule_params={
                "recent_window_days": RECENT_WINDOW_DAYS,
                "baseline_window_days": BASELINE_WINDOW_DAYS,
                "min_share_drop": MIN_SHARE_DROP,
            },
        )
        summary = (
            f"Extractor degradation on {form_family}/{section_key}: deterministic "
            f"share dropped from {baseline_share:.0%} to {recent_share:.0%} "
            f"({share_drop:+.0%}) over the last {RECENT_WINDOW_DAYS}d."
        )
        suggested = {
            "kind": "investigate_extractor_regression",
            "params": {"form_family": form_family, "section_key": section_key},
            "command": (
                f"# Pull recent sections that landed in repair or fallback for "
                f"this (form_family, section_key) — those are the demoted ones.\n"
                f"uv run python -c "
                f"\"from arrow.db.connection import get_conn; "
                f"with get_conn() as c, c.cursor() as cur: "
                f"cur.execute('SELECT s.artifact_id, s.extraction_method, s.confidence, "
                f"a.ticker, a.accession_number "
                f"FROM artifact_sections s JOIN artifacts a ON a.id = s.artifact_id "
                f"WHERE s.section_key=%s AND s.form_family=%s "
                f"AND s.extraction_method <> ''deterministic'' "
                f"AND s.created_at > now() - interval ''{RECENT_WINDOW_DAYS} days'' "
                f"ORDER BY s.created_at DESC LIMIT 20', "
                f"({section_key!r}, {form_family!r})); "
                f"print(cur.fetchall())\""
            ),
            "prose": (
                f"On {form_family}/{section_key}, the share of sections "
                f"extracted with method='deterministic' (the high-confidence "
                f"path) dropped from {baseline_share:.0%} (over {baseline_total} "
                f"baseline rows) to {recent_share:.0%} (over {recent_total} "
                f"recent rows). The remaining sections fell into 'repair' "
                f"({baseline_repair} → {recent_repair}) and 'unparsed_fallback' "
                f"({baseline_fallback} → {recent_fallback}). "
                f"Likely causes: a regex change that no longer matches the "
                f"section heading, a new filer using a different layout, or a "
                f"vendor template change. Pull the demoted rows with the "
                f"suggested command, identify the common pattern, and fix the "
                f"extractor in src/arrow/ingest/sec/qualitative.py. If the "
                f"degradation is concentrated on a handful of new filings with "
                f"a one-off layout, suppress with reason."
            ),
        }
        return FindingDraft(
            fingerprint=fp,
            finding_type=self.name,
            severity=self.severity,
            company_id=None,
            ticker=None,  # corpus-wide
            vertical=self.vertical,
            fiscal_period_key=None,
            evidence={
                "form_family": form_family,
                "section_key": section_key,
                "recent_window_days": RECENT_WINDOW_DAYS,
                "baseline_window_days": BASELINE_WINDOW_DAYS,
                "recent": {
                    "deterministic": recent_det,
                    "repair": recent_repair,
                    "unparsed_fallback": recent_fallback,
                    "total": recent_total,
                    "deterministic_share": recent_share,
                },
                "baseline": {
                    "deterministic": baseline_det,
                    "repair": baseline_repair,
                    "unparsed_fallback": baseline_fallback,
                    "total": baseline_total,
                    "deterministic_share": baseline_share,
                },
                "share_drop": share_drop,
            },
            summary=summary,
            suggested_action=suggested,
        )
