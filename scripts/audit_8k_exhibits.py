"""Audit earnings 8-K exhibit detection without fetching or writing data.

Usage:
    uv run scripts/audit_8k_exhibits.py
    uv run scripts/audit_8k_exhibits.py --details AMD AMZN

The report reads stored 8-K artifacts and cached SEC `index.json` files, then
applies the current press-release classifier to each text exhibit. It is a
dry-run guardrail before widening SEC exhibit ingest.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from arrow.db.connection import get_conn
from arrow.ingest.sec.filings import TEXT_EXHIBIT_SUFFIXES, press_release_doc_reason


def _index_path(raw_primary_doc_path: str | None) -> Path | None:
    if not raw_primary_doc_path:
        return None
    path = Path(raw_primary_doc_path)
    return path.parent / "index.json"


def _load_index(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    return json.loads(path.read_text())


def _is_text_exhibit(name: str | None) -> bool:
    return bool(name) and str(name).lower().endswith(TEXT_EXHIBIT_SUFFIXES)


def _rows(tickers: list[str]) -> list[dict[str, Any]]:
    params: dict[str, Any] = {}
    ticker_filter = ""
    if tickers:
        ticker_filter = "AND c.ticker = ANY(%(tickers)s)"
        params["tickers"] = [ticker.upper() for ticker in tickers]

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                c.ticker,
                a.accession_number,
                a.published_at::date AS published_date,
                a.raw_primary_doc_path,
                a.artifact_metadata->>'primary_document' AS primary_document,
                (
                    SELECT count(*)
                    FROM artifacts pr
                    WHERE pr.company_id = a.company_id
                      AND pr.artifact_type = 'press_release'
                      AND pr.source_document_id LIKE a.accession_number || ':%%'
                ) AS stored_press_releases
            FROM artifacts a
            JOIN companies c ON c.id = a.company_id
            WHERE a.artifact_type = '8k'
              {ticker_filter}
            ORDER BY c.ticker, a.published_at DESC, a.accession_number;
            """,
            params,
        )
        names = [desc.name for desc in cur.description]
        return [dict(zip(names, row)) for row in cur]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tickers", nargs="*", help="Optional ticker filter.")
    parser.add_argument(
        "--details",
        action="store_true",
        help="Print candidate exhibit rows under the summary.",
    )
    parser.add_argument(
        "--show-skipped",
        action="store_true",
        help="With --details, also show text exhibits that were skipped.",
    )
    args = parser.parse_args()

    filings = _rows(args.tickers)
    summary: dict[str, Counter[str]] = defaultdict(Counter)
    reason_counts: Counter[str] = Counter()
    detail_rows: list[dict[str, Any]] = []

    for filing in filings:
        ticker = filing["ticker"]
        summary[ticker]["eight_k"] += 1
        summary[ticker]["stored_press_releases"] += int(filing["stored_press_releases"] or 0)
        payload = _load_index(_index_path(filing["raw_primary_doc_path"]))
        if payload is None:
            summary[ticker]["missing_index"] += 1
            continue

        primary = filing["primary_document"]
        candidates_for_filing = 0
        for item in payload.get("directory", {}).get("item", []):
            name = item.get("name")
            if not name or name == primary:
                continue
            if not _is_text_exhibit(name):
                continue
            reason = press_release_doc_reason(item)
            if reason is None:
                if args.details and args.show_skipped:
                    detail_rows.append(
                        {
                            **filing,
                            "name": name,
                            "reason": "skipped",
                            "description": item.get("description") or "",
                        }
                    )
                continue
            candidates_for_filing += 1
            reason_counts[reason] += 1
            detail_rows.append(
                {
                    **filing,
                    "name": name,
                    "reason": reason,
                    "description": item.get("description") or "",
                }
            )

        if candidates_for_filing:
            summary[ticker]["with_candidate"] += 1
            summary[ticker]["candidate_docs"] += candidates_for_filing
        if candidates_for_filing > 1:
            summary[ticker]["multi_candidate_filings"] += 1

    print("8-K Exhibit Audit")
    print("=================")
    print(
        "ticker  8-Ks  with_candidate  candidate_docs  multi_candidate  stored_press_release  missing_index"
    )
    for ticker in sorted(summary):
        row = summary[ticker]
        print(
            f"{ticker:<6} "
            f"{row['eight_k']:>4} "
            f"{row['with_candidate']:>15} "
            f"{row['candidate_docs']:>15} "
            f"{row['multi_candidate_filings']:>16} "
            f"{row['stored_press_releases']:>20} "
            f"{row['missing_index']:>13}"
        )

    if reason_counts:
        print("\nCandidate Reasons")
        print("-----------------")
        for reason, count in reason_counts.most_common():
            print(f"{reason:<32} {count}")

    if args.details:
        print("\nDetails")
        print("-------")
        for row in detail_rows:
            print(
                f"{row['ticker']} {row['published_date']} {row['accession_number']} "
                f"{row['reason']} {row['name']} {row['description']}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
