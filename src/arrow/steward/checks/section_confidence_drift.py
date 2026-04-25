"""section_confidence_drift: rolling-window check on extraction confidence.

For each ``(form_family, section_key)`` pair, compare the mean
extraction confidence over the recent window (last 30 days) against
the baseline window (the prior 60 days, ending 30 days ago). Alert
when the recent mean drops more than ``Z_THRESHOLD`` baseline standard
deviations below the baseline mean — a one-sided z-test on the recent
mean.

This is the steward's eyes for *systemic* extractor degradation: a
regex change, a vendor template change, or a new filing layout that
the extractor handles worse than before. Per-row checks like
``unparsed_body_fallback`` catch individual failures; this catches a
distributional shift even when no single section is obviously broken.

Skipped per (form_family, section_key) if either window has fewer than
``MIN_ROWS`` rows or baseline stdev is zero (the test isn't
statistically meaningful below those thresholds).

Cross-cutting (vertical=``"sec_qual"``, but no ticker scope — the
finding is about the extractor, not any one filer). One finding per
(form_family, section_key) pair that's degraded.

Scope: ``scope.tickers`` is ignored — degradation is corpus-wide.
"""

from __future__ import annotations

from typing import Iterable

import psycopg

from arrow.steward.fingerprint import fingerprint
from arrow.steward.registry import Check, FindingDraft, Scope, register

#: Recent window: confidence over these many trailing days is the
#: signal we're testing.
RECENT_WINDOW_DAYS = 30

#: Baseline window: prior period to compare against.
BASELINE_WINDOW_DAYS = 60

#: Minimum row count in EACH window for the test to fire. Below this,
#: the means are too noisy to compare.
MIN_ROWS = 10

#: How many baseline standard deviations the recent mean must fall
#: below the baseline mean before we flag.
Z_THRESHOLD = 2.0


@register
class SectionConfidenceDrift(Check):
    name = "section_confidence_drift"
    severity = "warning"
    vertical = "sec_qual"

    def run(self, conn: psycopg.Connection, *, scope: Scope) -> Iterable[FindingDraft]:
        sql = f"""
            WITH recent AS (
              SELECT form_family, section_key,
                     AVG(confidence) AS mean,
                     STDDEV_POP(confidence) AS stdev,
                     COUNT(*) AS n
              FROM artifact_sections
              WHERE created_at > now() - interval '{RECENT_WINDOW_DAYS} days'
                AND confidence IS NOT NULL
                AND form_family IS NOT NULL
                AND section_key <> 'unparsed_body'
                AND extraction_method = 'deterministic'
              GROUP BY form_family, section_key
            ),
            baseline AS (
              SELECT form_family, section_key,
                     AVG(confidence) AS mean,
                     STDDEV_POP(confidence) AS stdev,
                     COUNT(*) AS n
              FROM artifact_sections
              WHERE created_at <= now() - interval '{RECENT_WINDOW_DAYS} days'
                AND created_at >  now() - interval '{RECENT_WINDOW_DAYS + BASELINE_WINDOW_DAYS} days'
                AND confidence IS NOT NULL
                AND form_family IS NOT NULL
                AND section_key <> 'unparsed_body'
                AND extraction_method = 'deterministic'
              GROUP BY form_family, section_key
            )
            SELECT b.form_family, b.section_key,
                   r.mean AS recent_mean, r.stdev AS recent_stdev, r.n AS recent_n,
                   b.mean AS baseline_mean, b.stdev AS baseline_stdev, b.n AS baseline_n,
                   (b.mean - r.mean) AS drop_amount,
                   CASE WHEN b.stdev > 0 THEN (b.mean - r.mean) / b.stdev END AS z_score
            FROM baseline b
            JOIN recent r USING (form_family, section_key)
            WHERE r.n >= %s
              AND b.n >= %s
              AND b.stdev > 0
              AND r.mean < b.mean - %s * b.stdev
            ORDER BY z_score DESC NULLS LAST;
        """

        with conn.cursor() as cur:
            cur.execute(sql, (MIN_ROWS, MIN_ROWS, Z_THRESHOLD))
            rows = cur.fetchall()

        for (
            form_family, section_key,
            recent_mean, recent_stdev, recent_n,
            baseline_mean, baseline_stdev, baseline_n,
            drop_amount, z_score,
        ) in rows:
            yield self._build_draft(
                form_family=form_family,
                section_key=section_key,
                recent_mean=float(recent_mean),
                recent_stdev=float(recent_stdev) if recent_stdev else 0.0,
                recent_n=recent_n,
                baseline_mean=float(baseline_mean),
                baseline_stdev=float(baseline_stdev),
                baseline_n=baseline_n,
                drop_amount=float(drop_amount),
                z_score=float(z_score),
            )

    def _build_draft(self, *, form_family, section_key,
                     recent_mean, recent_stdev, recent_n,
                     baseline_mean, baseline_stdev, baseline_n,
                     drop_amount, z_score) -> FindingDraft:
        fp = fingerprint(
            self.name,
            scope={"form_family": form_family, "section_key": section_key},
            rule_params={
                "recent_window_days": RECENT_WINDOW_DAYS,
                "baseline_window_days": BASELINE_WINDOW_DAYS,
                "z_threshold": Z_THRESHOLD,
            },
        )
        summary = (
            f"Extraction confidence drift on {form_family}/{section_key}: "
            f"recent mean {recent_mean:.3f} vs baseline {baseline_mean:.3f} "
            f"(z={z_score:.2f}σ, drop={drop_amount:.3f})."
        )
        suggested = {
            "kind": "investigate_extractor_drift",
            "params": {"form_family": form_family, "section_key": section_key},
            "command": (
                f"uv run python -c "
                f"\"from arrow.db.connection import get_conn; "
                f"with get_conn() as c, c.cursor() as cur: "
                f"cur.execute('SELECT artifact_id, confidence, ticker, accession_number "
                f"FROM artifact_sections s JOIN artifacts a ON a.id=s.artifact_id "
                f"WHERE s.section_key=%s AND s.form_family=%s "
                f"AND s.created_at > now() - interval ''{RECENT_WINDOW_DAYS} days'' "
                f"ORDER BY s.confidence ASC LIMIT 10', "
                f"({section_key!r}, {form_family!r})); "
                f"print(cur.fetchall())\""
            ),
            "prose": (
                f"Extraction confidence on {form_family}/{section_key} dropped "
                f"more than {Z_THRESHOLD} baseline-stdev recently "
                f"(recent mean {recent_mean:.3f} over {recent_n} rows; baseline "
                f"mean {baseline_mean:.3f} over {baseline_n} rows). Likely "
                f"causes: a vendor template change, a new filer layout the "
                f"extractor doesn't handle, or a recent extractor change that "
                f"regressed quality. Pull the lowest-confidence recent rows "
                f"(suggested SQL in `command`) to see whether the same filer "
                f"or layout pattern recurs. If it's a real regression, fix the "
                f"extractor and re-extract; if it's a temporary blip across "
                f"few filings, suppress with reason."
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
                "recent_mean": recent_mean,
                "recent_stdev": recent_stdev,
                "recent_n": recent_n,
                "baseline_mean": baseline_mean,
                "baseline_stdev": baseline_stdev,
                "baseline_n": baseline_n,
                "drop_amount": drop_amount,
                "z_score": z_score,
            },
            summary=summary,
            suggested_action=suggested,
        )
