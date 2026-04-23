"""Audit SEC qualitative coverage, extraction health, chunks, and retrieval.

Usage:
    uv run scripts/audit_sec_qualitative.py NVDA
    uv run scripts/audit_sec_qualitative.py --db-only NVDA
    uv run scripts/audit_sec_qualitative.py --query "data center revenue" NVDA

Default mode compares stored artifacts against live SEC submissions metadata.
Use --db-only when offline or when you only want to inspect already-stored
rows.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from typing import Any

from arrow.db.connection import get_conn
from arrow.ingest.common.http import HttpClient
from arrow.ingest.sec.bootstrap import SEC_RATE_LIMIT, SEC_USER_AGENT
from arrow.ingest.sec.filings import (
    DEFAULT_FORMS,
    DEFAULT_QUAL_SINCE_DATE,
    _artifact_type_for_form,
    _form_family_for_form,
    _get_company,
    _is_earnings_8k,
    _is_in_qualitative_window,
    _iter_filings,
    _iter_submission_payloads,
    _period_fields,
)
from arrow.normalize.periods.derive import (
    max_fiscal_year_for_until_date,
    min_fiscal_year_for_since_date,
)


TEN_K_KEYS = [
    "item_1_business",
    "item_1a_risk_factors",
    "item_1c_cybersecurity",
    "item_3_legal_proceedings",
    "item_7_mda",
    "item_7a_market_risk",
    "item_9a_controls",
    "item_9b_other_information",
]
TEN_Q_KEYS = [
    "part1_item2_mda",
    "part1_item3_market_risk",
    "part1_item4_controls",
    "part2_item1_legal_proceedings",
    "part2_item1a_risk_factors",
    "part2_item5_other_information",
]
DEFAULT_QUERIES = [
    "data center revenue",
    "gross margin",
    "export controls",
    "supply constraints",
]


@dataclass(frozen=True)
class ExpectedFiling:
    accession_number: str
    artifact_type: str
    form_type: str
    fiscal_period_key: str | None
    fiscal_year: int | None
    filing_date: date


def _parse_date(value: str | None) -> date | None:
    if value is None:
        return None
    return date.fromisoformat(value)


def _expected_filings(
    conn,
    ticker: str,
    *,
    since_date: date | None,
    until_date: date | None,
) -> tuple[list[ExpectedFiling], int | None, int | None]:
    company = _get_company(conn, ticker)
    min_fiscal_year = (
        min_fiscal_year_for_since_date(since_date, company.fiscal_year_end_md)
        if since_date is not None
        else None
    )
    max_fiscal_year = (
        max_fiscal_year_for_until_date(until_date, company.fiscal_year_end_md)
        if until_date is not None
        else None
    )

    expected: list[ExpectedFiling] = []
    wanted_forms = {form.upper() for form in DEFAULT_FORMS}
    http = HttpClient(user_agent=SEC_USER_AGENT, rate_limit=SEC_RATE_LIMIT)
    seen: set[str] = set()
    for _endpoint, _response, payload in _iter_submission_payloads(company, http):
        for filing in _iter_filings(payload):
            if filing.accession_number in seen:
                continue
            seen.add(filing.accession_number)
            if filing.form_type.upper() not in wanted_forms:
                continue
            if filing.form_type.upper().startswith("8-K") and not _is_earnings_8k(filing.items):
                continue
            fields = _period_fields(
                company,
                form_type=filing.form_type,
                report_date=filing.report_date,
            )
            form_family = _form_family_for_form(filing.form_type)
            if not _is_in_qualitative_window(
                filing,
                form_family=form_family,
                period_fields=fields,
                min_fiscal_year=min_fiscal_year,
                max_fiscal_year=max_fiscal_year,
                since_date=since_date,
                until_date=until_date,
            ):
                continue
            expected.append(
                ExpectedFiling(
                    accession_number=filing.accession_number,
                    artifact_type=_artifact_type_for_form(filing.form_type) or "8k",
                    form_type=filing.form_type,
                    fiscal_period_key=fields.get("fiscal_period_label"),
                    fiscal_year=fields.get("fiscal_year"),
                    filing_date=filing.filing_date,
                )
            )
    return expected, min_fiscal_year, max_fiscal_year


def _stored_artifacts(conn, ticker: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, artifact_type, accession_number, source_document_id,
                   fiscal_period_key, fiscal_year, published_at::date
            FROM artifacts
            WHERE ticker = %s
              AND source = 'sec'
              AND artifact_type IN ('10k', '10q', '8k')
            ORDER BY published_at, id;
            """,
            (ticker.upper(),),
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _print_table(headers: list[str], rows: list[tuple[Any, ...]]) -> None:
    if not rows:
        print("  (none)")
        return
    widths = [len(header) for header in headers]
    formatted = []
    for row in rows:
        vals = ["-" if value is None else str(value) for value in row]
        formatted.append(vals)
        for idx, value in enumerate(vals):
            widths[idx] = max(widths[idx], len(value))
    print("  " + "  ".join(header.ljust(widths[idx]) for idx, header in enumerate(headers)))
    print("  " + "  ".join("-" * width for width in widths))
    for row in formatted:
        print("  " + "  ".join(value.ljust(widths[idx]) for idx, value in enumerate(row)))


def _coverage(conn, ticker: str, expected: list[ExpectedFiling] | None) -> tuple[int, int]:
    stored = _stored_artifacts(conn, ticker)
    stored_keys = {(row["artifact_type"], row["accession_number"]) for row in stored}

    print("Filing Coverage")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT artifact_type, count(*), min(fiscal_year), max(fiscal_year),
                   min(published_at)::date, max(published_at)::date
            FROM artifacts
            WHERE ticker = %s AND source = 'sec'
              AND artifact_type IN ('10k', '10q', '8k')
            GROUP BY artifact_type
            ORDER BY artifact_type;
            """,
            (ticker.upper(),),
        )
        _print_table(
            ["type", "stored", "min_fy", "max_fy", "first_file", "last_file"],
            cur.fetchall(),
        )

    if expected is None:
        print("  live SEC comparison: skipped (--db-only)")
        return 0, 0

    expected_keys = {(row.artifact_type, row.accession_number) for row in expected}
    missing = sorted(expected_keys - stored_keys)
    unexpected = sorted(stored_keys - expected_keys)
    print()
    print("Live SEC Comparison")
    expected_rows = []
    for artifact_type in ("10k", "10q", "8k"):
        expected_n = sum(1 for item in expected if item.artifact_type == artifact_type)
        stored_n = sum(1 for row in stored if row["artifact_type"] == artifact_type)
        expected_rows.append((artifact_type, expected_n, stored_n))
    _print_table(["type", "expected", "stored"], expected_rows)

    if missing:
        print()
        print("Missing expected accessions")
        _print_table(["type", "accession"], [(kind, accn) for kind, accn in missing[:25]])
    if unexpected:
        print()
        print("Stored but outside current expected window")
        _print_table(["type", "accession"], [(kind, accn) for kind, accn in unexpected[:25]])
    return len(missing), len(unexpected)


def _section_health(conn, ticker: str) -> int:
    print()
    print("Section Extraction Health")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.artifact_type,
                   count(DISTINCT a.id) AS artifacts,
                   count(DISTINCT s.artifact_id) AS with_sections,
                   count(DISTINCT s.id) AS sections,
                   count(ch.id) AS chunks,
                   min(s.confidence) AS min_confidence,
                   count(*) FILTER (WHERE s.extraction_method = 'repair') AS repairs,
                   count(*) FILTER (WHERE s.extraction_method = 'unparsed_fallback') AS fallbacks
            FROM artifacts a
            LEFT JOIN artifact_sections s ON s.artifact_id = a.id
            LEFT JOIN artifact_section_chunks ch ON ch.section_id = s.id
            WHERE a.ticker = %s AND a.source = 'sec'
              AND a.artifact_type IN ('10k', '10q', '8k')
            GROUP BY a.artifact_type
            ORDER BY a.artifact_type;
            """,
            (ticker.upper(),),
        )
        _print_table(
            ["type", "artifacts", "with_sections", "sections", "chunks", "min_conf", "repairs", "fallbacks"],
            cur.fetchall(),
        )

        cur.execute(
            """
            SELECT a.artifact_type, a.fiscal_period_key, a.accession_number,
                   count(s.id) AS sections,
                   min(s.confidence) AS min_confidence,
                   count(*) FILTER (WHERE s.extraction_method = 'unparsed_fallback') AS fallbacks
            FROM artifacts a
            LEFT JOIN artifact_sections s ON s.artifact_id = a.id
            WHERE a.ticker = %s AND a.source = 'sec'
              AND a.artifact_type IN ('10k', '10q')
            GROUP BY a.id, a.artifact_type, a.fiscal_period_key, a.accession_number, a.published_at
            HAVING count(s.id) = 0
                OR min(s.confidence) < 0.85
                OR count(*) FILTER (WHERE s.extraction_method = 'unparsed_fallback') > 0
            ORDER BY a.published_at;
            """,
            (ticker.upper(),),
        )
        weak = cur.fetchall()
    if weak:
        print()
        print("Weak or missing extraction")
        _print_table(["type", "period", "accession", "sections", "min_conf", "fallbacks"], weak)
    else:
        print("  weak extraction: none")
    return len(weak)


def _section_inventory(conn, ticker: str) -> int:
    print()
    print("Section Inventory")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.form_family, s.section_key, s.extraction_method,
                   count(*) AS filings_with_section,
                   min(s.confidence) AS min_confidence
            FROM artifact_sections s
            JOIN artifacts a ON a.id = s.artifact_id
            WHERE a.ticker = %s
            GROUP BY s.form_family, s.section_key, s.extraction_method
            ORDER BY s.form_family, s.section_key, s.extraction_method;
            """,
            (ticker.upper(),),
        )
        rows = cur.fetchall()
    _print_table(["family", "section_key", "method", "filings", "min_conf"], rows)

    print()
    print("Per-Filing Missing Standard Sections")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.artifact_type, a.fiscal_period_key, a.accession_number,
                   array_agg(s.section_key ORDER BY s.section_key) FILTER (WHERE s.section_key IS NOT NULL)
            FROM artifacts a
            LEFT JOIN artifact_sections s ON s.artifact_id = a.id
            WHERE a.ticker = %s AND a.source = 'sec'
              AND a.artifact_type IN ('10k', '10q')
            GROUP BY a.id, a.artifact_type, a.fiscal_period_key, a.accession_number, a.published_at
            ORDER BY a.published_at;
            """,
            (ticker.upper(),),
        )
        missing_rows = []
        for artifact_type, period, accession, present in cur.fetchall():
            expected = TEN_K_KEYS if artifact_type == "10k" else TEN_Q_KEYS
            present_set = set(present or [])
            missing = [key for key in expected if key not in present_set]
            if missing:
                missing_rows.append((artifact_type, period, accession, ", ".join(missing)))
    _print_table(["type", "period", "accession", "missing_standard_sections"], missing_rows[:30])
    return len(missing_rows)


def _chunk_health(conn, ticker: str) -> int:
    print()
    print("Chunk Shape")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT percentile_disc(0.05) WITHIN GROUP (ORDER BY length(ch.text)) AS p05_chars,
                   percentile_disc(0.50) WITHIN GROUP (ORDER BY length(ch.text)) AS p50_chars,
                   percentile_disc(0.95) WITHIN GROUP (ORDER BY length(ch.text)) AS p95_chars,
                   max(length(ch.text)) AS max_chars,
                   min(length(ch.text)) AS min_chars,
                   count(*) AS chunks
            FROM artifact_section_chunks ch
            JOIN artifact_sections s ON s.id = ch.section_id
            JOIN artifacts a ON a.id = s.artifact_id
            WHERE a.ticker = %s;
            """,
            (ticker.upper(),),
        )
        _print_table(["p05", "p50", "p95", "max", "min", "chunks"], [cur.fetchone()])

        cur.execute(
            """
            SELECT a.fiscal_period_key, s.section_key, ch.chunk_ordinal,
                   length(ch.text) AS chars,
                   left(ch.text, 120) AS starts_with,
                   right(ch.text, 120) AS ends_with
            FROM artifact_section_chunks ch
            JOIN artifact_sections s ON s.id = ch.section_id
            JOIN artifacts a ON a.id = s.artifact_id
            WHERE a.ticker = %s
              AND (length(ch.text) < 500 OR length(ch.text) > 12000)
            ORDER BY length(ch.text) DESC
            LIMIT 20;
            """,
            (ticker.upper(),),
        )
        outliers = cur.fetchall()
    if outliers:
        print()
        print("Chunk size outliers")
        _print_table(["period", "section", "ord", "chars", "starts_with", "ends_with"], outliers)
    else:
        print("  chunk size outliers: none using <500 or >12000 chars")
    return len(outliers)


def _retrieval_smoke(conn, ticker: str, queries: list[str]) -> None:
    print()
    print("Retrieval Smoke Tests")
    for query in queries:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.fiscal_period_key, s.section_key, ch.chunk_ordinal,
                       left(ch.search_text, 180) AS snippet
                FROM artifact_section_chunks ch
                JOIN artifact_sections s ON s.id = ch.section_id
                JOIN artifacts a ON a.id = s.artifact_id
                WHERE a.ticker = %s
                  AND ch.tsv @@ websearch_to_tsquery('english', %s)
                ORDER BY a.published_at DESC, ch.chunk_ordinal
                LIMIT 5;
                """,
                (ticker.upper(), query),
            )
            rows = cur.fetchall()
        print(f"  query: {query!r}")
        _print_table(["period", "section", "ord", "snippet"], rows)


def _period_listing(conn, ticker: str) -> None:
    print()
    print("Kept 10-K / 10-Q Filings")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT artifact_type, fiscal_period_key, accession_number, published_at::date
            FROM artifacts
            WHERE ticker = %s AND source = 'sec'
              AND artifact_type IN ('10k', '10q')
            ORDER BY published_at;
            """,
            (ticker.upper(),),
        )
        _print_table(["type", "period", "accession", "filed"], cur.fetchall())


def _audit_ticker(args: argparse.Namespace, ticker: str) -> int:
    since_date = _parse_date(args.since) if args.since else DEFAULT_QUAL_SINCE_DATE
    until_date = _parse_date(args.until)
    queries = args.query or DEFAULT_QUERIES
    with get_conn() as conn:
        expected = None
        min_fy = None
        max_fy = None
        if not args.db_only:
            expected, min_fy, max_fy = _expected_filings(
                conn,
                ticker,
                since_date=since_date,
                until_date=until_date,
            )
        else:
            company = _get_company(conn, ticker)
            min_fy = min_fiscal_year_for_since_date(since_date, company.fiscal_year_end_md)
            max_fy = max_fiscal_year_for_until_date(until_date, company.fiscal_year_end_md) if until_date else None

        print(f"{ticker.upper()} SEC Qualitative Audit")
        print(f"  since_date:          {since_date.isoformat() if since_date else '-'}")
        print(f"  until_date:          {until_date.isoformat() if until_date else '-'}")
        print(f"  10-K/Q min FY:       {min_fy or '-'}")
        print(f"  10-K/Q max FY:       {max_fy or '-'}")
        print()

        missing, unexpected = _coverage(conn, ticker, expected)
        weak = _section_health(conn, ticker)
        missing_sections = _section_inventory(conn, ticker)
        chunk_outliers = _chunk_health(conn, ticker)
        _retrieval_smoke(conn, ticker, queries)
        if args.list_filings:
            _period_listing(conn, ticker)

    print()
    print("Audit Summary")
    hard_issues = missing + unexpected + weak
    warning_items = missing_sections + chunk_outliers
    if hard_issues:
        status = "FAIL"
    elif warning_items:
        status = "PASS_WITH_WARNINGS"
    else:
        status = "PASS"
    print(f"  status:                       {status}")
    print(f"  missing expected filings:     {missing}")
    print(f"  unexpected stored filings:    {unexpected}")
    print(f"  weak/missing extractions:     {weak}")
    print(f"  filings missing some standard sections: {missing_sections}")
    print(f"  chunk size outliers:          {chunk_outliers}")
    return 0 if hard_issues == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit SEC qualitative extraction quality.")
    parser.add_argument("--db-only", action="store_true", help="skip live SEC coverage comparison")
    parser.add_argument("--since", help="calendar cutoff date, YYYY-MM-DD")
    parser.add_argument("--until", help="optional calendar upper-bound date, YYYY-MM-DD")
    parser.add_argument("--query", action="append", help="retrieval smoke-test query; repeatable")
    parser.add_argument("--list-filings", action="store_true", help="print kept 10-K/10-Q filing inventory")
    parser.add_argument("tickers", nargs="+")
    args = parser.parse_args()

    exit_code = 0
    for idx, ticker in enumerate(args.tickers):
        if idx:
            print()
            print("=" * 80)
            print()
        exit_code = max(exit_code, _audit_ticker(args, ticker))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
