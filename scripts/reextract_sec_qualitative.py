"""Re-run section + chunk extraction over every existing 10-K / 10-Q artifact.

Reads each artifact's raw filing from `raw_primary_doc_path`, normalizes the
body, runs the current `extract_sections` + `build_chunks`, and replaces the
artifact_sections + artifact_section_chunks rows. Idempotent — re-runs after
the parser changes converge to the new version.

Usage:
    uv run scripts/reextract_sec_qualitative.py            # all 10-K/10-Q
    uv run scripts/reextract_sec_qualitative.py --ticker MSFT NVDA

Bookkeeping:
    Tracks per-artifact before/after section counts and total characters so
    the operator sees what changed. Errors are reported per-artifact and do
    not abort the run.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import psycopg

from arrow.db.connection import get_conn
from arrow.ingest.sec.qualitative import (
    EXTRACTOR_VERSION,
    extract_sections,
    normalize_filing_body,
    replace_sections_and_chunks,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--ticker", nargs="*", help="Limit to one or more tickers.")
    p.add_argument("--dry-run", action="store_true", help="Compute deltas without writing.")
    return p.parse_args()


def _select_artifacts(conn: psycopg.Connection, tickers: list[str] | None) -> list[dict[str, Any]]:
    sql = """
    SELECT a.id, a.company_id, a.fiscal_period_key, a.artifact_type, a.form_family,
           a.raw_primary_doc_path, a.content_type, co.ticker
    FROM artifacts a
    JOIN companies co ON co.id = a.company_id
    WHERE a.artifact_type IN ('10k','10q')
      AND a.superseded_at IS NULL
      AND a.raw_primary_doc_path IS NOT NULL
    """
    params: list[Any] = []
    if tickers:
        sql += " AND co.ticker = ANY(%s)"
        params.append([t.upper() for t in tickers])
    sql += " ORDER BY co.ticker, a.published_at DESC;"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _section_summary(conn: psycopg.Connection, artifact_id: int) -> tuple[int, int]:
    """Return (section_count, total_chars)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*), COALESCE(SUM(length(text)), 0) "
            "FROM artifact_sections WHERE artifact_id = %s",
            (artifact_id,),
        )
        return tuple(cur.fetchone())  # type: ignore[return-value]


def main() -> int:
    args = _parse_args()

    with get_conn() as conn:
        artifacts = _select_artifacts(conn, args.ticker)
        print(f"Re-extracting {len(artifacts)} artifact(s) "
              f"(extractor_version={EXTRACTOR_VERSION!r}, dry_run={args.dry_run})")
        print()

        rows_changed = 0
        rows_unchanged = 0
        rows_failed = 0
        chars_before_total = 0
        chars_after_total = 0

        for art in artifacts:
            ticker = art["ticker"]
            fpk = art["fiscal_period_key"]
            artifact_type = art["artifact_type"]
            label = f"{ticker:<6} {fpk:<14} {artifact_type}"

            raw_path = REPO_ROOT / art["raw_primary_doc_path"]
            if not raw_path.exists():
                print(f"  {label}  RAW_MISSING {raw_path}")
                rows_failed += 1
                continue

            try:
                body = raw_path.read_bytes()
                content_type = art["content_type"] or "text/html"
                form_family = art["form_family"] or (
                    "10-K" if artifact_type == "10k" else "10-Q"
                )
                normalized = normalize_filing_body(body, content_type)
                sections = extract_sections(form_family, normalized)
            except Exception as e:
                print(f"  {label}  EXTRACT_FAILED {type(e).__name__}: {e}")
                rows_failed += 1
                continue

            sec_count_after = len(sections)
            chars_after = sum(len(s.text) for s in sections)
            sec_count_before, chars_before = _section_summary(conn, art["id"])

            chars_before_total += chars_before
            chars_after_total += chars_after

            delta = chars_after - chars_before
            sign = "+" if delta >= 0 else ""
            verdict = "CHANGED" if delta != 0 or sec_count_after != sec_count_before else "same"
            if verdict == "CHANGED":
                rows_changed += 1
            else:
                rows_unchanged += 1

            print(f"  {label}  before {sec_count_before} sec/{chars_before:>7,} ch  "
                  f"after {sec_count_after} sec/{chars_after:>7,} ch  "
                  f"delta {sign}{delta:>+8,} ch  {verdict}")

            if args.dry_run:
                continue
            try:
                with conn.transaction():
                    replace_sections_and_chunks(
                        conn,
                        artifact_id=art["id"],
                        company_id=art["company_id"],
                        fiscal_period_key=fpk,
                        form_family=form_family,
                        sections=sections,
                    )
            except Exception as e:
                print(f"  {label}  WRITE_FAILED {type(e).__name__}: {e}", file=sys.stderr)
                rows_failed += 1

        print()
        print(f"Changed: {rows_changed}  Unchanged: {rows_unchanged}  Failed: {rows_failed}")
        print(f"Total chars: {chars_before_total:>10,} → {chars_after_total:>10,}  "
              f"delta {chars_after_total - chars_before_total:+,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
