"""Backfill fiscal-period linkage for earnings-release press releases.

Usage:
    uv run scripts/backfill_press_release_periods.py
    uv run scripts/backfill_press_release_periods.py --dry-run

This is a metadata cleanup for SEC EX-99 earnings releases attached to 8-Ks.
The source text and chunks are not rewritten; only missing period fields are
filled on artifacts and artifact_text_units.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from typing import Any

from arrow.db.connection import get_conn


FILENAME_QUARTER_RE = re.compile(
    r"(?<![a-z0-9])q([1-4])[_-]?(?:fy)?([0-9]{2})(?![0-9])",
    re.IGNORECASE,
)
PREFIX_QUARTER_RE = re.compile(
    r"([1-4])q([0-9]{2,4})(?![0-9])",
    re.IGNORECASE,
)
YEAR_QUARTER_RE = re.compile(
    r"([12][0-9]{3})q([1-4])(?![0-9])",
    re.IGNORECASE,
)
WORD_QUARTER_RE = re.compile(
    r"\b(first|second|third|fourth)\s+quarter\s+fiscal\s+([12][0-9]{3})\b",
    re.IGNORECASE,
)
WORD_QUARTERS = {
    "first": 1,
    "second": 2,
    "third": 3,
    "fourth": 4,
}


@dataclass(frozen=True)
class PeriodFields:
    fiscal_period_key: str
    fiscal_year: int
    fiscal_quarter: int
    fiscal_period_label: str
    period_end: Any | None
    period_type: str
    calendar_year: int | None
    calendar_quarter: int | None
    calendar_period_label: str | None
    method: str


def _parse_period_from_name(value: str | None) -> tuple[int, int] | None:
    if not value:
        return None
    year_quarter = YEAR_QUARTER_RE.search(value)
    if year_quarter is not None:
        return int(year_quarter.group(1)), int(year_quarter.group(2))

    prefix_quarter = PREFIX_QUARTER_RE.search(value)
    if prefix_quarter is not None:
        quarter = int(prefix_quarter.group(1))
        year_raw = prefix_quarter.group(2)
        fiscal_year = int(year_raw) if len(year_raw) == 4 else 2000 + int(year_raw)
        return fiscal_year, quarter

    match = FILENAME_QUARTER_RE.search(value)
    if match is None:
        word_quarter = WORD_QUARTER_RE.search(value)
        if word_quarter is None:
            return None
        return int(word_quarter.group(2)), WORD_QUARTERS[word_quarter.group(1).lower()]
    quarter = int(match.group(1))
    year_2 = int(match.group(2))
    fiscal_year = 2000 + year_2
    return fiscal_year, quarter


def _period_from_financial_fact(cur, artifact: dict[str, Any]) -> PeriodFields | None:
    cur.execute(
        """
        SELECT
            f.fiscal_period_label,
            f.fiscal_year,
            f.fiscal_quarter,
            f.period_end,
            f.period_type,
            f.calendar_year,
            f.calendar_quarter,
            f.calendar_period_label
        FROM financial_facts f
        WHERE f.company_id = %(company_id)s
          AND f.statement = 'income_statement'
          AND f.period_type = 'quarter'
          AND f.superseded_at IS NULL
          AND f.published_at::date = %(published_date)s
        GROUP BY
            f.fiscal_period_label,
            f.fiscal_year,
            f.fiscal_quarter,
            f.period_end,
            f.period_type,
            f.calendar_year,
            f.calendar_quarter,
            f.calendar_period_label
        ORDER BY f.period_end DESC
        LIMIT 1;
        """,
        artifact,
    )
    row = cur.fetchone()
    if row is None:
        return None
    return PeriodFields(
        fiscal_period_key=row[0],
        fiscal_year=row[1],
        fiscal_quarter=row[2],
        fiscal_period_label=row[0],
        period_end=row[3],
        period_type=row[4],
        calendar_year=row[5],
        calendar_quarter=row[6],
        calendar_period_label=row[7],
        method="financial_fact_published_date",
    )


def _period_from_parsed_label(cur, artifact: dict[str, Any]) -> PeriodFields | None:
    candidates = [
        artifact.get("source_document_id"),
        artifact.get("document_name"),
        artifact.get("title"),
        artifact.get("headline_text"),
    ]
    parsed = next((p for p in (_parse_period_from_name(v) for v in candidates) if p), None)
    if parsed is None:
        return None

    fiscal_year, fiscal_quarter = parsed
    label = f"FY{fiscal_year} Q{fiscal_quarter}"
    cur.execute(
        """
        SELECT
            f.period_end,
            f.calendar_year,
            f.calendar_quarter,
            f.calendar_period_label
        FROM financial_facts f
        WHERE f.company_id = %(company_id)s
          AND f.statement = 'income_statement'
          AND f.period_type = 'quarter'
          AND f.fiscal_period_label = %(label)s
          AND f.superseded_at IS NULL
        ORDER BY f.period_end DESC
        LIMIT 1;
        """,
        {"company_id": artifact["company_id"], "label": label},
    )
    row = cur.fetchone()
    return PeriodFields(
        fiscal_period_key=label,
        fiscal_year=fiscal_year,
        fiscal_quarter=fiscal_quarter,
        fiscal_period_label=label,
        period_end=row[0] if row else None,
        period_type="quarter",
        calendar_year=row[1] if row else None,
        calendar_quarter=row[2] if row else None,
        calendar_period_label=row[3] if row else None,
        method="press_release_filename",
    )


def main() -> int:
    dry_run = "--dry-run" in sys.argv[1:]
    unexpected = [arg for arg in sys.argv[1:] if arg != "--dry-run"]
    if unexpected:
        print("Usage: backfill_press_release_periods.py [--dry-run]", file=sys.stderr)
        return 2

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    a.id,
                    a.company_id,
                    c.ticker,
                    a.source_document_id,
                    a.title,
                    a.published_at::date AS published_date,
                    a.artifact_metadata->>'document_name' AS document_name
                    , (
                        SELECT u.unit_title || ' ' || left(u.text, 500)
                        FROM artifact_text_units u
                        WHERE u.artifact_id = a.id
                        ORDER BY u.unit_ordinal
                        LIMIT 1
                    ) AS headline_text
                FROM artifacts a
                JOIN companies c ON c.id = a.company_id
                WHERE a.artifact_type = 'press_release'
                  AND a.fiscal_period_key IS NULL
                  AND a.superseded_at IS NULL
                ORDER BY c.ticker, a.published_at;
                """
            )
            artifacts = [dict(zip([desc.name for desc in cur.description], row)) for row in cur]

            updates: list[tuple[dict[str, Any], PeriodFields]] = []
            unresolved: list[dict[str, Any]] = []
            for artifact in artifacts:
                period = _period_from_financial_fact(cur, artifact)
                if period is None:
                    period = _period_from_parsed_label(cur, artifact)
                if period is None:
                    unresolved.append(artifact)
                else:
                    updates.append((artifact, period))

            if dry_run:
                for artifact, period in updates:
                    print(
                        f"would update {artifact['ticker']} artifact_id={artifact['id']} "
                        f"-> {period.fiscal_period_key} ({period.method})"
                    )
                for artifact in unresolved:
                    print(
                        f"unresolved {artifact['ticker']} artifact_id={artifact['id']} "
                        f"{artifact['source_document_id']}"
                    )
                print(
                    f"Status: DRY RUN — {len(updates)} resolvable, {len(unresolved)} unresolved."
                )
                return 1 if unresolved else 0

            with conn.transaction():
                for artifact, period in updates:
                    cur.execute(
                        """
                        UPDATE artifacts
                        SET fiscal_period_key = %s,
                            fiscal_year = COALESCE(fiscal_year, %s),
                            fiscal_quarter = COALESCE(fiscal_quarter, %s),
                            fiscal_period_label = COALESCE(fiscal_period_label, %s),
                            period_end = COALESCE(period_end, %s),
                            period_type = COALESCE(period_type, %s),
                            calendar_year = COALESCE(calendar_year, %s),
                            calendar_quarter = COALESCE(calendar_quarter, %s),
                            calendar_period_label = COALESCE(calendar_period_label, %s),
                            artifact_metadata = artifact_metadata || %s::jsonb
                        WHERE id = %s
                          AND artifact_type = 'press_release'
                          AND fiscal_period_key IS NULL;
                        """,
                        (
                            period.fiscal_period_key,
                            period.fiscal_year,
                            period.fiscal_quarter,
                            period.fiscal_period_label,
                            period.period_end,
                            period.period_type,
                            period.calendar_year,
                            period.calendar_quarter,
                            period.calendar_period_label,
                            f'{{"period_backfill_method": "{period.method}"}}',
                            artifact["id"],
                        ),
                    )
                    cur.execute(
                        """
                        UPDATE artifact_text_units
                        SET fiscal_period_key = %s
                        WHERE artifact_id = %s
                          AND fiscal_period_key IS NULL;
                        """,
                        (period.fiscal_period_key, artifact["id"]),
                    )

            print(f"Updated press-release periods: {len(updates)}")
            print(f"Unresolved press releases:     {len(unresolved)}")
            if unresolved:
                for artifact in unresolved:
                    print(
                        f"  {artifact['ticker']} artifact_id={artifact['id']} "
                        f"{artifact['source_document_id']}"
                    )
                return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
