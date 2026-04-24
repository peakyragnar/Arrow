"""Audit SEC qualitative coverage, extraction health, chunks, and retrieval.

Usage:
    uv run scripts/audit_sec_qualitative.py NVDA
    uv run scripts/audit_sec_qualitative.py --db-only NVDA
    uv run scripts/audit_sec_qualitative.py --query "data center revenue" NVDA
    uv run scripts/audit_sec_qualitative.py --html outputs/nvda_qual_audit.html NVDA

Default mode compares stored artifacts against live SEC submissions metadata.
Use --db-only when offline or when you only want to inspect already-stored
rows.
"""

from __future__ import annotations

import argparse
import html
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
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
DEFAULT_EARNINGS_RELEASE_QUERIES = [
    "business outlook",
    "revenue",
    "gross margin outlook",
    "non-GAAP reconciliations",
]
DISPLAY_FORM_ORDER = ["10-K", "10-K/A", "10-Q", "10-Q/A", "8-K", "8-K/A"]
MD_AND_A_KEYS = ["item_7_mda", "part1_item2_mda"]
RISK_KEYS = ["item_1a_risk_factors", "part2_item1a_risk_factors"]
BUSINESS_KEYS = ["item_1_business"]
SHORT_VALID_SECTION_KEYS = {
    "item_3_legal_proceedings",
    "item_9a_controls",
    "item_9b_other_information",
    "part1_item3_market_risk",
    "part1_item4_controls",
    "part2_item1_legal_proceedings",
    "part2_item5_other_information",
}
SHORT_VALID_HEADING_TITLES = {
    "Adoption of New and Recently Issued Accounting Pronouncements",
    "Climate Change",
    "Critical Accounting Policies and Estimates",
    "Critical Accounting Estimates",
    "Off-Balance Sheet Arrangements",
    "Overview and Recent Developments",
    "Recent Accounting Pronouncements",
    "Recently Issued Accounting Pronouncements",
}
BOUNDARY_MARKER_RE = re.compile(
    r"\b("
    r"PART\s+[IVX]+\s*[\.\-:]\s+OTHER\s+INFORMATION|"
    r"ITEM\s+6\s*[\.\-:]\s*EXHIBITS|"
    r"SIGNATURES?|"
    r"TABLE\s+OF\s+CONTENTS"
    r")\b",
    re.IGNORECASE,
)
TRAILING_PAGE_NUMBER_RE = re.compile(r"\.\s+\d{1,4}\s*$")
MARKET_RISK_REFERENCE_RE = re.compile(
    r"(?is)\breference\s+is\s+made\s+to\b"
    r".*\bitem\s+7a\b"
    r".*\bannual\s+report\s+on\s+form\s+10-k\b"
    r".*\b(?:have|has)\s+not\s+been\s+any\s+material\s+changes?\b"
)


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


def _preferred_sections_for_query(query: str) -> list[str]:
    q = query.lower()
    preferred: list[str] = []
    if any(
        token in q
        for token in (
            "revenue",
            "sales",
            "margin",
            "gross",
            "operating",
            "cash",
            "capex",
            "expenses",
        )
    ):
        preferred.extend(MD_AND_A_KEYS)
    if any(
        token in q
        for token in (
            "risk",
            "export",
            "controls",
            "constraint",
            "supply",
            "regulation",
            "china",
        )
    ):
        preferred.extend(RISK_KEYS + MD_AND_A_KEYS + BUSINESS_KEYS)
    return list(dict.fromkeys(preferred))


def _preferred_reason_for_query(query: str, preferred_sections: list[str]) -> str:
    if not preferred_sections:
        return "No section preference; ranked by FTS score."
    q = query.lower()
    reasons = []
    if any(
        token in q
        for token in (
            "revenue",
            "sales",
            "margin",
            "gross",
            "operating",
            "cash",
            "capex",
            "expenses",
        )
    ):
        reasons.append("financial operating topic; prefer MD&A")
    if any(
        token in q
        for token in (
            "risk",
            "export",
            "controls",
            "constraint",
            "supply",
            "regulation",
            "china",
        )
    ):
        reasons.append("risk/regulatory/supply topic; prefer Risk Factors, MD&A, Business")
    return "; ".join(reasons)


def _query_terms(query: str) -> list[str]:
    terms = re.findall(r"[a-z0-9]+", query.lower())
    return [term for term in terms if len(term) > 1]


def _retrieval_explanation(query: str, search_text: str | None) -> dict[str, Any]:
    haystack = (search_text or "").lower()
    terms = _query_terms(query)
    matched = [term for term in terms if term in haystack]
    missing = [term for term in terms if term not in haystack]
    return {
        "matched_terms": matched,
        "missing_terms": missing,
        "exact_phrase": query.lower() in haystack if query else False,
        "term_coverage": f"{len(matched)}/{len(terms)}" if terms else "-",
    }


def _earnings_release_reason_for_query(query: str) -> str:
    q = query.lower()
    if "outlook" in q or "guidance" in q:
        return "earnings-release guidance topic; validate Business Outlook / forward-looking units."
    if "non-gaap" in q or "reconciliation" in q:
        return "earnings-release adjustment topic; validate Non-GAAP units."
    if "revenue" in q or "margin" in q or "eps" in q:
        return "earnings-release financial topic; validate summary/results/outlook units."
    return "Earnings-release-only search; ranked by FTS score."


def _highlight_html(value: Any) -> str:
    escaped = _escape(value)
    return (
        escaped.replace("__HIGHLIGHT_START__", "<mark>")
        .replace("__HIGHLIGHT_END__", "</mark>")
    )


def _highlight_text(value: Any) -> str:
    return (
        str(value or "")
        .replace("__HIGHLIGHT_START__", "[")
        .replace("__HIGHLIGHT_END__", "]")
    )


def _heading_path_label(value: Any) -> str:
    if not value:
        return "-"
    if isinstance(value, list):
        return " > ".join(str(item) for item in value if item)
    return str(value)


def _sentence_count(text: str) -> int:
    return len([part for part in re.split(r"(?<=[.!?])\s+", text.strip()) if part])


def _word_count(text: str) -> int:
    return len(re.findall(r"\b\w+\b", text))


def _chunk_warning_bucket(row: dict[str, Any]) -> tuple[str, str]:
    text = row.get("text") or ""
    section_key = row.get("section_key") or ""
    chars = row.get("chars") or 0
    heading_path = row.get("heading_path") or []
    primary_heading = heading_path[-1] if heading_path else None
    starts_with = row.get("starts_with") or ""
    ends_with = row.get("ends_with") or ""
    words = _word_count(text)
    sentences = _sentence_count(text)

    if chars > 12000:
        return "large_chunk", "Over 12,000 characters; may be too broad for precise retrieval."
    if _is_normal_market_risk_reference(section_key, text):
        return (
            "normal_market_risk_reference",
            "Complete 10-Q market-risk cross-reference; no material changes disclosed.",
        )
    if not heading_path:
        return "possible_boundary_issue", "No heading_path; chunk may be structurally orphaned."
    if TRAILING_PAGE_NUMBER_RE.search(text):
        return "possible_boundary_issue", "Tail ends with a likely page-number bleed."
    if BOUNDARY_MARKER_RE.search(starts_with) or BOUNDARY_MARKER_RE.search(ends_with):
        return "possible_boundary_issue", "Chunk edge contains a filing boundary marker."
    if re.fullmatch(r"[A-Z0-9 ,.;:()&/\\-]{20,}", starts_with.strip()) and words < 200:
        return "possible_boundary_issue", "Short chunk starts with an all-caps orphan line."
    if words < 200 and section_key not in SHORT_VALID_SECTION_KEYS:
        if section_key in RISK_KEYS and len(heading_path) > 1:
            return "short_valid_section", "Short but expected for an individual risk-factor bullet."
        if primary_heading in SHORT_VALID_HEADING_TITLES:
            return "short_valid_section", "Short but expected for this boilerplate subsection."
        return "possible_boundary_issue", "Under 200 words outside a normally short section."
    if sentences < 3 and section_key not in SHORT_VALID_SECTION_KEYS:
        if section_key in RISK_KEYS and len(heading_path) > 1:
            return "short_valid_section", "Short but expected for an individual risk-factor bullet."
        if primary_heading in SHORT_VALID_HEADING_TITLES:
            return "short_valid_section", "Short but expected for this boilerplate subsection."
        return "possible_boundary_issue", "Fewer than three sentences outside a normally short section."
    if chars < 500 and section_key in SHORT_VALID_SECTION_KEYS:
        return "short_valid_section", "Short but expected for this SEC section type."
    return "size_outlier", "Unusual size; review manually."


def _is_normal_market_risk_reference(section_key: str, text: str) -> bool:
    return section_key == "part1_item3_market_risk" and bool(MARKET_RISK_REFERENCE_RE.search(text))


def _classify_chunk_warnings(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    buckets = {
        "possible_boundary_issue": [],
        "large_chunk": [],
        "normal_market_risk_reference": [],
        "short_valid_section": [],
        "size_outlier": [],
    }
    for row in rows:
        bucket, reason = _chunk_warning_bucket(row)
        row["warning_bucket"] = bucket
        row["warning_reason"] = reason
        row["word_count"] = _word_count(row.get("text") or "")
        row["sentence_count"] = _sentence_count(row.get("text") or "")
        buckets.setdefault(bucket, []).append(row)
    return buckets


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
                   COALESCE(artifact_metadata->>'form_type', title) AS form_type,
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
        rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    for row in rows:
        row["display_type"] = _filing_display_type(
            row["artifact_type"],
            row.get("form_type"),
        )
    return rows


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


def _dict_rows(cur) -> list[dict[str, Any]]:
    cols = [d.name for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _escape(value: Any) -> str:
    if value is None:
        return ""
    return html.escape(str(value))


def _expected_section_keys(artifact_type: str) -> list[str]:
    return TEN_K_KEYS if artifact_type == "10k" else TEN_Q_KEYS


def _filing_display_type(artifact_type: str, form_type: str | None) -> str:
    form = (form_type or "").upper()
    artifact = artifact_type.lower()
    if artifact == "10k":
        return "10-K/A" if form.startswith("10-K/A") else "10-K"
    if artifact == "10q":
        return "10-Q/A" if form.startswith("10-Q/A") else "10-Q"
    if artifact == "8k":
        return "8-K/A" if form.startswith("8-K/A") else "8-K"
    return artifact_type


def _display_type_sort_key(display_type: str) -> tuple[int, str]:
    try:
        return (DISPLAY_FORM_ORDER.index(display_type), display_type)
    except ValueError:
        return (len(DISPLAY_FORM_ORDER), display_type)


def _display_types_for_counts(
    stored: list[dict[str, Any]], expected: list[ExpectedFiling] | None
) -> list[str]:
    labels = set(DISPLAY_FORM_ORDER)
    labels.update(row["display_type"] for row in stored)
    if expected is not None:
        labels.update(_filing_display_type(row.artifact_type, row.form_type) for row in expected)
    return sorted(labels, key=_display_type_sort_key)


def _coverage_rows_from_stored(
    stored: list[dict[str, Any]], expected: list[ExpectedFiling] | None
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for display_type in _display_types_for_counts(stored, expected):
        matches = [row for row in stored if row["display_type"] == display_type]
        fiscal_years = [row["fiscal_year"] for row in matches if row.get("fiscal_year") is not None]
        filed_dates = [row["published_at"] for row in matches if row.get("published_at") is not None]
        rows.append(
            {
                "artifact_type": display_type,
                "stored": len(matches),
                "min_fy": min(fiscal_years) if fiscal_years else None,
                "max_fy": max(fiscal_years) if fiscal_years else None,
                "first_file": min(filed_dates) if filed_dates else None,
                "last_file": max(filed_dates) if filed_dates else None,
            }
        )
    return rows


def _expected_count_rows(
    stored: list[dict[str, Any]], expected: list[ExpectedFiling] | None
) -> list[dict[str, Any]]:
    rows = []
    for display_type in _display_types_for_counts(stored, expected):
        rows.append(
            {
                "artifact_type": display_type,
                "expected": (
                    sum(
                        1
                        for item in expected
                        if _filing_display_type(item.artifact_type, item.form_type) == display_type
                    )
                    if expected is not None
                    else None
                ),
                "stored": sum(1 for item in stored if item["display_type"] == display_type),
            }
        )
    return rows


def _is_amendment_filing(row: dict[str, Any]) -> bool:
    form_type = str(row.get("form_type") or row.get("title") or "").upper()
    return bool(row.get("is_amendment") or row.get("amends_artifact_id") or form_type.endswith("/A"))


def _missing_standard_sections_for_filing(
    filing: dict[str, Any], section_map: dict[str, dict[str, Any]]
) -> list[str]:
    if _is_amendment_filing(filing):
        return []
    expected_keys_for_type = _expected_section_keys(filing["artifact_type"])
    return [key for key in expected_keys_for_type if key not in section_map]


def _is_hard_weak_filing(filing: dict[str, Any]) -> bool:
    sections = filing.get("sections") or 0
    fallbacks = filing.get("fallbacks") or 0
    min_confidence = filing.get("min_confidence")
    if _is_amendment_filing(filing):
        return sections == 0
    return sections == 0 or (min_confidence is not None and min_confidence < 0.85) or fallbacks > 0


def _is_amendment_extraction_note(filing: dict[str, Any]) -> bool:
    if not _is_amendment_filing(filing) or _is_hard_weak_filing(filing):
        return False
    min_confidence = filing.get("min_confidence")
    repairs = filing.get("repairs") or 0
    fallbacks = filing.get("fallbacks") or 0
    return fallbacks > 0 or repairs > 0 or (min_confidence is not None and min_confidence < 0.85)


def _section_label(section_key: str) -> str:
    labels = {
        "item_1_business": "Item 1",
        "item_1a_risk_factors": "Item 1A",
        "item_1c_cybersecurity": "Item 1C",
        "item_3_legal_proceedings": "Item 3",
        "item_7_mda": "Item 7",
        "item_7a_market_risk": "Item 7A",
        "item_9a_controls": "Item 9A",
        "item_9b_other_information": "Item 9B",
        "part1_item2_mda": "P1 I2",
        "part1_item3_market_risk": "P1 I3",
        "part1_item4_controls": "P1 I4",
        "part2_item1_legal_proceedings": "P2 I1",
        "part2_item1a_risk_factors": "P2 I1A",
        "part2_item5_other_information": "P2 I5",
    }
    return labels.get(section_key, section_key)


def _collect_report(
    conn,
    ticker: str,
    *,
    expected: list[ExpectedFiling] | None,
    since_date: date | None,
    until_date: date | None,
    min_fy: int | None,
    max_fy: int | None,
    queries: list[str],
) -> dict[str, Any]:
    stored = _stored_artifacts(conn, ticker)
    stored_keys = {(row["artifact_type"], row["accession_number"]) for row in stored}
    expected_keys = (
        {(row.artifact_type, row.accession_number) for row in expected}
        if expected is not None
        else set()
    )
    missing = sorted(expected_keys - stored_keys)
    unexpected = sorted(stored_keys - expected_keys) if expected is not None else []

    coverage = _coverage_rows_from_stored(stored, expected)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT CASE
                       WHEN a.artifact_type = '10k'
                            AND upper(COALESCE(a.artifact_metadata->>'form_type', a.title, '')) LIKE '10-K/A%%' THEN '10-K/A'
                       WHEN a.artifact_type = '10k' THEN '10-K'
                       WHEN a.artifact_type = '10q'
                            AND upper(COALESCE(a.artifact_metadata->>'form_type', a.title, '')) LIKE '10-Q/A%%' THEN '10-Q/A'
                       WHEN a.artifact_type = '10q' THEN '10-Q'
                       WHEN a.artifact_type = '8k'
                            AND upper(COALESCE(a.artifact_metadata->>'form_type', a.title, '')) LIKE '8-K/A%%' THEN '8-K/A'
                       WHEN a.artifact_type = '8k' THEN '8-K'
                       ELSE a.artifact_type
                   END AS artifact_type,
                   count(DISTINCT a.id) AS artifacts,
                   count(DISTINCT s.artifact_id) AS with_sections,
                   count(DISTINCT s.id) AS sections,
                   count(ch.id) AS chunks,
                   min(s.confidence) AS min_confidence,
                   count(DISTINCT s.id) FILTER (WHERE s.extraction_method = 'repair') AS repairs,
                   count(DISTINCT s.id) FILTER (WHERE s.extraction_method = 'unparsed_fallback') AS fallbacks
            FROM artifacts a
            LEFT JOIN artifact_sections s ON s.artifact_id = a.id
            LEFT JOIN artifact_section_chunks ch ON ch.section_id = s.id
            WHERE a.ticker = %s AND a.source = 'sec'
              AND a.artifact_type IN ('10k', '10q', '8k')
            GROUP BY 1
            ORDER BY 1;
            """,
            (ticker.upper(),),
        )
        section_health = _dict_rows(cur)

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
        section_inventory = _dict_rows(cur)

        cur.execute(
            """
            SELECT count(DISTINCT a.id) AS artifacts,
                   count(DISTINCT u.artifact_id) AS with_units,
                   count(DISTINCT u.id) AS units,
                   count(ch.id) AS chunks,
                   min(u.confidence) AS min_confidence,
                   count(DISTINCT u.id) FILTER (WHERE u.extraction_method = 'unparsed_fallback') AS fallbacks
            FROM artifacts a
            LEFT JOIN artifact_text_units u ON u.artifact_id = a.id
            LEFT JOIN artifact_text_chunks ch ON ch.text_unit_id = u.id
            WHERE a.ticker = %s AND a.source = 'sec'
              AND a.artifact_type = 'press_release';
            """,
            (ticker.upper(),),
        )
        press_release_health = _dict_rows(cur)[0]

        cur.execute(
            """
            SELECT u.unit_type, u.unit_key, u.extraction_method,
                   count(*) AS artifacts_with_unit,
                   min(u.confidence) AS min_confidence
            FROM artifact_text_units u
            JOIN artifacts a ON a.id = u.artifact_id
            WHERE a.ticker = %s
            GROUP BY u.unit_type, u.unit_key, u.extraction_method
            ORDER BY u.unit_type, u.unit_key, u.extraction_method;
            """,
            (ticker.upper(),),
        )
        text_unit_inventory = _dict_rows(cur)

        cur.execute(
            """
            SELECT a.id, a.artifact_type, a.fiscal_period_key, a.accession_number,
                   COALESCE(a.artifact_metadata->>'form_type', a.title) AS form_type,
                   a.amends_artifact_id,
                   (a.amends_artifact_id IS NOT NULL
                    OR upper(COALESCE(a.artifact_metadata->>'form_type', a.title, '')) LIKE '%%/A') AS is_amendment,
                   a.published_at::date AS filed,
                   count(DISTINCT s.id) AS sections,
                   count(ch.id) AS chunks,
                   min(s.confidence) AS min_confidence,
                   count(DISTINCT s.id) FILTER (WHERE s.extraction_method = 'repair') AS repairs,
                   count(DISTINCT s.id) FILTER (WHERE s.extraction_method = 'unparsed_fallback') AS fallbacks
            FROM artifacts a
            LEFT JOIN artifact_sections s ON s.artifact_id = a.id
            LEFT JOIN artifact_section_chunks ch ON ch.section_id = s.id
            WHERE a.ticker = %s AND a.source = 'sec'
              AND a.artifact_type IN ('10k', '10q')
            GROUP BY a.id, a.artifact_type, a.fiscal_period_key, a.accession_number,
                     a.title, a.artifact_metadata, a.amends_artifact_id, a.published_at
            ORDER BY a.published_at;
            """,
            (ticker.upper(),),
        )
        filings = _dict_rows(cur)

        cur.execute(
            """
            SELECT a.id AS artifact_id, s.section_key, s.extraction_method,
                   s.confidence, count(ch.id) AS chunks,
                   length(s.text) AS section_chars
            FROM artifacts a
            JOIN artifact_sections s ON s.artifact_id = a.id
            LEFT JOIN artifact_section_chunks ch ON ch.section_id = s.id
            WHERE a.ticker = %s AND a.source = 'sec'
            GROUP BY a.id, s.id, s.section_key, s.extraction_method, s.confidence, s.text
            ORDER BY a.published_at, s.section_key;
            """,
            (ticker.upper(),),
        )
        section_rows = _dict_rows(cur)

        cur.execute(
            """
            WITH chunks AS (
                SELECT ch.text
                FROM artifact_section_chunks ch
                JOIN artifact_sections s ON s.id = ch.section_id
                JOIN artifacts a ON a.id = s.artifact_id
                WHERE a.ticker = %s
                UNION ALL
                SELECT ch.text
                FROM artifact_text_chunks ch
                JOIN artifact_text_units u ON u.id = ch.text_unit_id
                JOIN artifacts a ON a.id = u.artifact_id
                WHERE a.ticker = %s
            )
            SELECT percentile_disc(0.05) WITHIN GROUP (ORDER BY length(text)) AS p05_chars,
                   percentile_disc(0.50) WITHIN GROUP (ORDER BY length(text)) AS p50_chars,
                   percentile_disc(0.95) WITHIN GROUP (ORDER BY length(text)) AS p95_chars,
                   max(length(text)) AS max_chars,
                   min(length(text)) AS min_chars,
                   count(*) AS chunks
            FROM chunks;
            """,
            (ticker.upper(), ticker.upper()),
        )
        chunk_stats = _dict_rows(cur)[0]

        cur.execute(
            """
            SELECT a.fiscal_period_key, s.section_key, ch.chunk_ordinal,
                   length(ch.text) AS chars,
                   ch.heading_path,
                   ch.text,
                   left(ch.text, 220) AS starts_with,
                   right(ch.text, 220) AS ends_with
            FROM artifact_section_chunks ch
            JOIN artifact_sections s ON s.id = ch.section_id
            JOIN artifacts a ON a.id = s.artifact_id
            WHERE a.ticker = %s
              AND (
                  length(ch.text) < 500
                  OR length(ch.text) > 12000
                  OR ch.text ~ '\\.\\s+\\d{1,4}\\s*$'
              )
            ORDER BY length(ch.text) DESC
            LIMIT 30;
            """,
            (ticker.upper(),),
        )
        chunk_outliers = _dict_rows(cur)
        chunk_warning_buckets = _classify_chunk_warnings(chunk_outliers)

    sections_by_artifact: dict[int, dict[str, dict[str, Any]]] = {}
    for row in section_rows:
        sections_by_artifact.setdefault(row["artifact_id"], {})[row["section_key"]] = row

    quality_filings = filings
    if expected is not None:
        expected_keys = {(row.artifact_type, row.accession_number) for row in expected}
        quality_filings = [
            filing
            for filing in filings
            if (filing["artifact_type"], filing["accession_number"]) in expected_keys
        ]

    missing_sections = []
    weak = []
    amendment_notes = []
    for filing in quality_filings:
        section_map = sections_by_artifact.get(filing["id"], {})
        missing_for_filing = _missing_standard_sections_for_filing(filing, section_map)
        if missing_for_filing:
            missing_sections.append({**filing, "missing_sections": missing_for_filing})
        if _is_hard_weak_filing(filing):
            weak.append(filing)
        elif _is_amendment_extraction_note(filing):
            amendment_notes.append(filing)

    retrieval: dict[str, list[dict[str, Any]]] = {}
    retrieval_reasons: dict[str, str] = {}
    for query in queries:
        preferred_sections = _preferred_sections_for_query(query)
        retrieval_reasons[query] = _preferred_reason_for_query(query, preferred_sections)
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH q AS (
                    SELECT websearch_to_tsquery('english', %s) AS tsq
                ),
                matches AS (
                    SELECT a.fiscal_period_key,
                           'filing_section' AS source_kind,
                           s.section_key,
                           ch.chunk_ordinal,
                           CASE WHEN s.section_key = ANY(%s::text[]) THEN true ELSE false END
                               AS preferred_section,
                           ts_rank_cd(ch.tsv, q.tsq) AS rank,
                           s.extraction_method,
                           s.confidence,
                           ch.heading_path,
                           ch.search_text,
                           a.published_at
                    FROM artifact_section_chunks ch
                    JOIN artifact_sections s ON s.id = ch.section_id
                    JOIN artifacts a ON a.id = s.artifact_id
                    CROSS JOIN q
                    WHERE a.ticker = %s
                      AND ch.tsv @@ q.tsq
                    UNION ALL
                    SELECT COALESCE(a.fiscal_period_key, a.published_at::date::text) AS fiscal_period_key,
                           'press_release' AS source_kind,
                           'press_release:' || u.unit_key AS section_key,
                           ch.chunk_ordinal,
                           false AS preferred_section,
                           ts_rank_cd(ch.tsv, q.tsq) AS rank,
                           u.extraction_method,
                           u.confidence,
                           ch.heading_path,
                           ch.search_text,
                           a.published_at
                    FROM artifact_text_chunks ch
                    JOIN artifact_text_units u ON u.id = ch.text_unit_id
                    JOIN artifacts a ON a.id = u.artifact_id
                    CROSS JOIN q
                    WHERE a.ticker = %s
                      AND ch.tsv @@ q.tsq
                )
                SELECT fiscal_period_key, source_kind, section_key, chunk_ordinal,
                       preferred_section,
                       round(rank::numeric, 4) AS rank,
                       extraction_method,
                       confidence,
                       heading_path,
                       ts_headline(
                           'english',
                           search_text,
                           q.tsq,
                           'StartSel=__HIGHLIGHT_START__, StopSel=__HIGHLIGHT_END__, MaxWords=35, MinWords=12'
                       ) AS highlighted_snippet,
                       left(search_text, 320) AS snippet,
                       search_text AS match_source
                FROM matches
                CROSS JOIN q
                ORDER BY preferred_section DESC, rank DESC, published_at DESC,
                         chunk_ordinal
                LIMIT 5;
                """,
                (query, preferred_sections, ticker.upper(), ticker.upper()),
            )
            rows = _dict_rows(cur)
            for row in rows:
                row.update(_retrieval_explanation(query, row.get("match_source")))
                row.pop("match_source", None)
            retrieval[query] = rows

    earnings_release_retrieval: dict[str, list[dict[str, Any]]] = {}
    earnings_release_reasons: dict[str, str] = {}
    for query in DEFAULT_EARNINGS_RELEASE_QUERIES:
        earnings_release_reasons[query] = _earnings_release_reason_for_query(query)
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH q AS (
                    SELECT websearch_to_tsquery('english', %s) AS tsq
                )
                SELECT COALESCE(a.fiscal_period_key, a.published_at::date::text) AS fiscal_period_key,
                       'press_release' AS source_kind,
                       'press_release:' || u.unit_key AS section_key,
                       ch.chunk_ordinal,
                       true AS preferred_section,
                       round(ts_rank_cd(ch.tsv, q.tsq)::numeric, 4) AS rank,
                       u.extraction_method,
                       u.confidence,
                       ch.heading_path,
                       ts_headline(
                           'english',
                           ch.search_text,
                           q.tsq,
                           'StartSel=__HIGHLIGHT_START__, StopSel=__HIGHLIGHT_END__, MaxWords=35, MinWords=12'
                       ) AS highlighted_snippet,
                       left(ch.search_text, 320) AS snippet,
                       ch.search_text AS match_source,
                       a.published_at
                FROM artifact_text_chunks ch
                JOIN artifact_text_units u ON u.id = ch.text_unit_id
                JOIN artifacts a ON a.id = u.artifact_id
                CROSS JOIN q
                WHERE a.ticker = %s
                  AND a.artifact_type = 'press_release'
                  AND ch.tsv @@ q.tsq
                ORDER BY a.published_at DESC, rank DESC, u.unit_ordinal, ch.chunk_ordinal
                LIMIT 5;
                """,
                (query, ticker.upper()),
            )
            rows = _dict_rows(cur)
            for row in rows:
                row.update(_retrieval_explanation(query, row.get("match_source")))
                row.pop("match_source", None)
            earnings_release_retrieval[query] = rows

    expected_counts = _expected_count_rows(stored, expected)

    hard_issues = len(missing) + len(weak)
    reviewable_chunk_warnings = [
        row
        for row in chunk_outliers
        if row.get("warning_bucket") not in {"short_valid_section", "normal_market_risk_reference"}
    ]
    press_release_missing_units = max(
        0,
        (press_release_health.get("artifacts") or 0)
        - (press_release_health.get("with_units") or 0),
    )
    warnings = (
        len(missing_sections)
        + len(reviewable_chunk_warnings)
        + len(amendment_notes)
        + press_release_missing_units
    )
    status = "FAIL" if hard_issues else "PASS_WITH_WARNINGS" if warnings else "PASS"
    return {
        "ticker": ticker.upper(),
        "since_date": since_date,
        "until_date": until_date,
        "min_fy": min_fy,
        "max_fy": max_fy,
        "coverage": coverage,
        "expected_counts": expected_counts,
        "missing": missing,
        "unexpected": unexpected,
        "section_health": section_health,
        "section_inventory": section_inventory,
        "press_release_health": press_release_health,
        "press_release_missing_units": press_release_missing_units,
        "text_unit_inventory": text_unit_inventory,
        "filings": filings,
        "sections_by_artifact": sections_by_artifact,
        "missing_sections": missing_sections,
        "weak": weak,
        "amendment_notes": amendment_notes,
        "chunk_stats": chunk_stats,
        "chunk_outliers": chunk_outliers,
        "chunk_warning_buckets": chunk_warning_buckets,
        "retrieval": retrieval,
        "retrieval_reasons": retrieval_reasons,
        "earnings_release_retrieval": earnings_release_retrieval,
        "earnings_release_reasons": earnings_release_reasons,
        "status": status,
        "hard_issues": hard_issues,
        "warnings": warnings,
    }


def _coverage(conn, ticker: str, expected: list[ExpectedFiling] | None) -> tuple[int, int]:
    stored = _stored_artifacts(conn, ticker)
    stored_keys = {(row["artifact_type"], row["accession_number"]) for row in stored}

    print("Filing Coverage")
    _print_table(
        ["type", "stored", "min_fy", "max_fy", "first_file", "last_file"],
        [
            (
                row["artifact_type"],
                row["stored"],
                row["min_fy"],
                row["max_fy"],
                row["first_file"],
                row["last_file"],
            )
            for row in _coverage_rows_from_stored(stored, expected)
        ],
    )

    if expected is None:
        print("  live SEC comparison: skipped (--db-only)")
        return 0, 0

    expected_keys = {(row.artifact_type, row.accession_number) for row in expected}
    missing = sorted(expected_keys - stored_keys)
    unexpected = sorted(stored_keys - expected_keys)
    print()
    print("Live SEC Comparison")
    _print_table(
        ["type", "expected", "stored"],
        [
            (row["artifact_type"], row["expected"], row["stored"])
            for row in _expected_count_rows(stored, expected)
        ],
    )

    if missing:
        print()
        print("Missing expected accessions")
        _print_table(["type", "accession"], [(kind, accn) for kind, accn in missing[:25]])
    if unexpected:
        print()
        print("Stored outside current expected window (informational)")
        _print_table(["type", "accession"], [(kind, accn) for kind, accn in unexpected[:25]])
    return len(missing), len(unexpected)


def _section_health(
    conn, ticker: str, expected: list[ExpectedFiling] | None = None
) -> int:
    print()
    print("Section Extraction Health")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT CASE
                       WHEN a.artifact_type = '10k'
                            AND upper(COALESCE(a.artifact_metadata->>'form_type', a.title, '')) LIKE '10-K/A%%' THEN '10-K/A'
                       WHEN a.artifact_type = '10k' THEN '10-K'
                       WHEN a.artifact_type = '10q'
                            AND upper(COALESCE(a.artifact_metadata->>'form_type', a.title, '')) LIKE '10-Q/A%%' THEN '10-Q/A'
                       WHEN a.artifact_type = '10q' THEN '10-Q'
                       WHEN a.artifact_type = '8k'
                            AND upper(COALESCE(a.artifact_metadata->>'form_type', a.title, '')) LIKE '8-K/A%%' THEN '8-K/A'
                       WHEN a.artifact_type = '8k' THEN '8-K'
                       ELSE a.artifact_type
                   END AS artifact_type,
                   count(DISTINCT a.id) AS artifacts,
                   count(DISTINCT s.artifact_id) AS with_sections,
                   count(DISTINCT s.id) AS sections,
                   count(ch.id) AS chunks,
                   min(s.confidence) AS min_confidence,
                   count(DISTINCT s.id) FILTER (WHERE s.extraction_method = 'repair') AS repairs,
                   count(DISTINCT s.id) FILTER (WHERE s.extraction_method = 'unparsed_fallback') AS fallbacks
            FROM artifacts a
            LEFT JOIN artifact_sections s ON s.artifact_id = a.id
            LEFT JOIN artifact_section_chunks ch ON ch.section_id = s.id
            WHERE a.ticker = %s AND a.source = 'sec'
              AND a.artifact_type IN ('10k', '10q', '8k')
            GROUP BY 1
            ORDER BY 1;
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
                   count(DISTINCT s.id) AS sections,
                   min(s.confidence) AS min_confidence,
                   count(DISTINCT s.id) FILTER (WHERE s.extraction_method = 'repair') AS repairs,
                   count(DISTINCT s.id) FILTER (WHERE s.extraction_method = 'unparsed_fallback') AS fallbacks,
                   (a.amends_artifact_id IS NOT NULL
                    OR upper(COALESCE(a.artifact_metadata->>'form_type', a.title, '')) LIKE '%%/A') AS is_amendment
            FROM artifacts a
            LEFT JOIN artifact_sections s ON s.artifact_id = a.id
            WHERE a.ticker = %s AND a.source = 'sec'
              AND a.artifact_type IN ('10k', '10q')
            GROUP BY a.id, a.artifact_type, a.fiscal_period_key, a.accession_number,
                     a.title, a.artifact_metadata, a.amends_artifact_id, a.published_at
            HAVING (
                    NOT (a.amends_artifact_id IS NOT NULL
                         OR upper(COALESCE(a.artifact_metadata->>'form_type', a.title, '')) LIKE '%%/A')
                    AND (
                        count(DISTINCT s.id) = 0
                        OR min(s.confidence) < 0.85
                        OR count(DISTINCT s.id) FILTER (WHERE s.extraction_method = 'unparsed_fallback') > 0
                    )
                )
                OR (
                    (a.amends_artifact_id IS NOT NULL
                     OR upper(COALESCE(a.artifact_metadata->>'form_type', a.title, '')) LIKE '%%/A')
                    AND (
                        count(DISTINCT s.id) = 0
                        OR count(DISTINCT s.id) FILTER (WHERE s.extraction_method = 'unparsed_fallback') > 0
                    )
                )
            ORDER BY a.published_at;
            """,
            (ticker.upper(),),
        )
        weak_rows = cur.fetchall()
        if expected is not None:
            expected_keys = {(row.artifact_type, row.accession_number) for row in expected}
            weak_rows = [
                row
                for row in weak_rows
                if (row[0], row[2]) in expected_keys
            ]
        weak = [row for row in weak_rows if not row[7] or row[3] == 0]
        weak_amendment_notes = [row for row in weak_rows if row[7] and row[3] > 0]
        cur.execute(
            """
            SELECT a.artifact_type, a.fiscal_period_key, a.accession_number,
                   count(DISTINCT s.id) AS sections,
                   min(s.confidence) AS min_confidence,
                   count(DISTINCT s.id) FILTER (WHERE s.extraction_method = 'repair') AS repairs
            FROM artifacts a
            LEFT JOIN artifact_sections s ON s.artifact_id = a.id
            WHERE a.ticker = %s AND a.source = 'sec'
              AND a.artifact_type IN ('10k', '10q')
              AND (
                  a.amends_artifact_id IS NOT NULL
                  OR upper(COALESCE(a.artifact_metadata->>'form_type', a.title, '')) LIKE '%%/A'
              )
            GROUP BY a.id, a.artifact_type, a.fiscal_period_key, a.accession_number, a.published_at
            HAVING count(DISTINCT s.id) > 0
               AND (
                   min(s.confidence) < 0.85
                   OR count(DISTINCT s.id) FILTER (WHERE s.extraction_method = 'repair') > 0
               )
            ORDER BY a.published_at;
            """,
            (ticker.upper(),),
        )
        amendment_notes = [*weak_amendment_notes, *cur.fetchall()]
    if weak:
        print()
        print("Weak or missing extraction")
        _print_table(["type", "period", "accession", "sections", "min_conf", "repairs", "fallbacks", "amendment"], weak)
    else:
        print("  weak extraction: none")
    if amendment_notes:
        print()
        print("Amendment extraction notes")
        _print_table(["type", "period", "accession", "sections", "min_conf", "repairs"], [row[:6] for row in amendment_notes])
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
                   (a.amends_artifact_id IS NOT NULL
                    OR upper(COALESCE(a.artifact_metadata->>'form_type', a.title, '')) LIKE '%%/A') AS is_amendment,
                   array_agg(s.section_key ORDER BY s.section_key) FILTER (WHERE s.section_key IS NOT NULL)
            FROM artifacts a
            LEFT JOIN artifact_sections s ON s.artifact_id = a.id
            WHERE a.ticker = %s AND a.source = 'sec'
              AND a.artifact_type IN ('10k', '10q')
            GROUP BY a.id, a.artifact_type, a.fiscal_period_key, a.accession_number,
                     a.title, a.artifact_metadata, a.amends_artifact_id, a.published_at
            ORDER BY a.published_at;
            """,
            (ticker.upper(),),
        )
        missing_rows = []
        for artifact_type, period, accession, is_amendment, present in cur.fetchall():
            if is_amendment:
                continue
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
                   ch.heading_path,
                   ch.text,
                   left(ch.text, 120) AS starts_with,
                   right(ch.text, 120) AS ends_with
            FROM artifact_section_chunks ch
            JOIN artifact_sections s ON s.id = ch.section_id
            JOIN artifacts a ON a.id = s.artifact_id
            WHERE a.ticker = %s
              AND (
                  length(ch.text) < 500
                  OR length(ch.text) > 12000
                  OR ch.text ~ '\\.\\s+\\d{1,4}\\s*$'
              )
            ORDER BY length(ch.text) DESC
            LIMIT 20;
            """,
            (ticker.upper(),),
        )
        outliers = _dict_rows(cur)
        buckets = _classify_chunk_warnings(outliers)
    if outliers:
        print()
        print("Chunk Review By Type")
        for bucket, rows in buckets.items():
            if not rows:
                continue
            print(f"  {bucket}: {len(rows)}")
            table_rows = [
                (
                    row["fiscal_period_key"],
                    row["section_key"],
                    row["chunk_ordinal"],
                    row["chars"],
                    row["word_count"],
                    row["warning_reason"],
                    row["ends_with"],
                )
                for row in rows[:8]
            ]
            _print_table(["period", "section", "ord", "chars", "words", "reason", "ends_with"], table_rows)
    else:
        print("  chunk review items: none using <500 or >12000 chars")
    return sum(
        len(rows)
        for bucket, rows in buckets.items()
        if bucket not in {"short_valid_section", "normal_market_risk_reference"}
    )


def _retrieval_smoke(conn, ticker: str, queries: list[str]) -> None:
    print()
    print("Retrieval Smoke Tests")
    for query in queries:
        preferred_sections = _preferred_sections_for_query(query)
        reason = _preferred_reason_for_query(query, preferred_sections)
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH q AS (
                    SELECT websearch_to_tsquery('english', %s) AS tsq
                ),
                matches AS (
                    SELECT a.fiscal_period_key,
                           'filing_section' AS source_kind,
                           s.section_key,
                           ch.chunk_ordinal,
                           CASE WHEN s.section_key = ANY(%s::text[]) THEN 'yes' ELSE 'no' END
                               AS preferred,
                           ts_rank_cd(ch.tsv, q.tsq) AS rank,
                           ch.heading_path,
                           ch.search_text,
                           a.published_at
                    FROM artifact_section_chunks ch
                    JOIN artifact_sections s ON s.id = ch.section_id
                    JOIN artifacts a ON a.id = s.artifact_id
                    CROSS JOIN q
                    WHERE a.ticker = %s
                      AND ch.tsv @@ q.tsq
                    UNION ALL
                    SELECT COALESCE(a.fiscal_period_key, a.published_at::date::text) AS fiscal_period_key,
                           'press_release' AS source_kind,
                           'press_release:' || u.unit_key AS section_key,
                           ch.chunk_ordinal,
                           'no' AS preferred,
                           ts_rank_cd(ch.tsv, q.tsq) AS rank,
                           ch.heading_path,
                           ch.search_text,
                           a.published_at
                    FROM artifact_text_chunks ch
                    JOIN artifact_text_units u ON u.id = ch.text_unit_id
                    JOIN artifacts a ON a.id = u.artifact_id
                    CROSS JOIN q
                    WHERE a.ticker = %s
                      AND ch.tsv @@ q.tsq
                )
                SELECT fiscal_period_key, source_kind, section_key, chunk_ordinal,
                       preferred,
                       round(rank::numeric, 4) AS rank,
                       heading_path,
                       ts_headline(
                           'english',
                           search_text,
                           q.tsq,
                           'StartSel=__HIGHLIGHT_START__, StopSel=__HIGHLIGHT_END__, MaxWords=22, MinWords=8'
                       ) AS highlighted_snippet,
                       search_text AS match_source
                FROM matches
                CROSS JOIN q
                ORDER BY preferred DESC, rank DESC, published_at DESC,
                         chunk_ordinal
                LIMIT 5;
                """,
                (query, preferred_sections, ticker.upper(), ticker.upper()),
            )
            rows = _dict_rows(cur)
        print(f"  query: {query!r}")
        print(f"  preference: {reason}")
        table_rows = []
        for row in rows:
            explanation = _retrieval_explanation(query, row.get("match_source"))
            table_rows.append(
                (
                    row["fiscal_period_key"],
                    row["source_kind"],
                    row["section_key"],
                    row["chunk_ordinal"],
                    row["preferred"],
                    row["rank"],
                    explanation["term_coverage"],
                    ", ".join(explanation["matched_terms"]),
                    _heading_path_label(row["heading_path"]),
                    _highlight_text(row["highlighted_snippet"]),
                )
            )
        _print_table(
            ["period", "source", "section", "ord", "pref", "rank", "terms", "matched", "heading", "snippet"],
            table_rows,
        )


def _earnings_release_smoke(conn, ticker: str) -> None:
    print()
    print("Earnings Release Smoke Tests")
    for query in DEFAULT_EARNINGS_RELEASE_QUERIES:
        reason = _earnings_release_reason_for_query(query)
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH q AS (
                    SELECT websearch_to_tsquery('english', %s) AS tsq
                )
                SELECT COALESCE(a.fiscal_period_key, a.published_at::date::text) AS fiscal_period_key,
                       'press_release' AS source_kind,
                       'press_release:' || u.unit_key AS section_key,
                       ch.chunk_ordinal,
                       round(ts_rank_cd(ch.tsv, q.tsq)::numeric, 4) AS rank,
                       ch.heading_path,
                       ts_headline(
                           'english',
                           ch.search_text,
                           q.tsq,
                           'StartSel=__HIGHLIGHT_START__, StopSel=__HIGHLIGHT_END__, MaxWords=22, MinWords=8'
                       ) AS highlighted_snippet,
                       ch.search_text AS match_source,
                       a.published_at
                FROM artifact_text_chunks ch
                JOIN artifact_text_units u ON u.id = ch.text_unit_id
                JOIN artifacts a ON a.id = u.artifact_id
                CROSS JOIN q
                WHERE a.ticker = %s
                  AND a.artifact_type = 'press_release'
                  AND ch.tsv @@ q.tsq
                ORDER BY a.published_at DESC, rank DESC, u.unit_ordinal, ch.chunk_ordinal
                LIMIT 5;
                """,
                (query, ticker.upper()),
            )
            rows = _dict_rows(cur)
        print(f"  query: {query!r}")
        print(f"  preference: {reason}")
        table_rows = []
        for row in rows:
            explanation = _retrieval_explanation(query, row.get("match_source"))
            table_rows.append(
                (
                    row["fiscal_period_key"],
                    row["section_key"],
                    row["chunk_ordinal"],
                    row["rank"],
                    explanation["term_coverage"],
                    ", ".join(explanation["matched_terms"]),
                    _heading_path_label(row["heading_path"]),
                    _highlight_text(row["highlighted_snippet"]),
                )
            )
        _print_table(
            ["period", "section", "ord", "rank", "terms", "matched", "heading", "snippet"],
            table_rows,
        )


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


def _metric_card(label: str, value: Any, note: str, tone: str = "neutral") -> str:
    return (
        f'<div class="metric {tone}">'
        f'<div class="metric-label">{_escape(label)}</div>'
        f'<div class="metric-value">{_escape(value)}</div>'
        f'<div class="metric-note">{_escape(note)}</div>'
        "</div>"
    )


def _coverage_html(report: dict[str, Any]) -> str:
    rows = []
    for row in report["coverage"]:
        rows.append(
            "<tr>"
            f"<td>{_escape(row['artifact_type'])}</td>"
            f"<td>{_escape(row['stored'])}</td>"
            f"<td>{_escape(row['min_fy'])}</td>"
            f"<td>{_escape(row['max_fy'])}</td>"
            f"<td>{_escape(row['first_file'])}</td>"
            f"<td>{_escape(row['last_file'])}</td>"
            "</tr>"
        )
    expected_rows = []
    missing_types = {kind for kind, _accession in report["missing"]}
    for row in report["expected_counts"]:
        match = row["expected"] is None or row["artifact_type"] not in missing_types
        expected_rows.append(
            f'<tr class="{"ok-row" if match else "bad-row"}">'
            f"<td>{_escape(row['artifact_type'])}</td>"
            f"<td>{_escape(row['expected']) if row['expected'] is not None else 'skipped'}</td>"
            f"<td>{_escape(row['stored'])}</td>"
            "</tr>"
        )
    return f"""
        <section>
          <h2>Filing Coverage</h2>
          <div class="two-col">
            <div>
              <h3>Stored Corpus</h3>
              <table>
                <thead><tr><th>Type</th><th>Stored</th><th>Min FY</th><th>Max FY</th><th>First Filed</th><th>Last Filed</th></tr></thead>
                <tbody>{''.join(rows)}</tbody>
              </table>
            </div>
            <div>
              <h3>Live SEC Comparison</h3>
              <table>
                <thead><tr><th>Type</th><th>Expected</th><th>Stored</th></tr></thead>
                <tbody>{''.join(expected_rows)}</tbody>
              </table>
            </div>
          </div>
        </section>
    """


def _section_health_html(report: dict[str, Any]) -> str:
    rows = []
    for row in report["section_health"]:
        tone = "ok-row"
        rows.append(
            f'<tr class="{tone}">'
            f"<td>{_escape(row['artifact_type'])}</td>"
            f"<td>{_escape(row['artifacts'])}</td>"
            f"<td>{_escape(row['with_sections'])}</td>"
            f"<td>{_escape(row['sections'])}</td>"
            f"<td>{_escape(row['chunks'])}</td>"
            f"<td>{_escape(row['min_confidence'])}</td>"
            f"<td>{_escape(row['repairs'])}</td>"
            f"<td>{_escape(row['fallbacks'])}</td>"
            "</tr>"
        )
    inv_rows = []
    for row in report["section_inventory"]:
        inv_rows.append(
            "<tr>"
            f"<td>{_escape(row['form_family'])}</td>"
            f"<td>{_escape(row['section_key'])}</td>"
            f"<td>{_escape(row['extraction_method'])}</td>"
            f"<td>{_escape(row['filings_with_section'])}</td>"
            f"<td>{_escape(row['min_confidence'])}</td>"
            "</tr>"
        )
    return f"""
        <section>
          <h2>Extraction Health</h2>
          <table>
            <thead><tr><th>Type</th><th>Artifacts</th><th>With Sections</th><th>Sections</th><th>Chunks</th><th>Min Conf</th><th>Repairs</th><th>Fallbacks</th></tr></thead>
            <tbody>{''.join(rows)}</tbody>
          </table>
          <h3>Section Inventory</h3>
          <table>
            <thead><tr><th>Family</th><th>Section Key</th><th>Method</th><th>Filings</th><th>Min Conf</th></tr></thead>
            <tbody>{''.join(inv_rows)}</tbody>
          </table>
        </section>
    """


def _press_release_html(report: dict[str, Any]) -> str:
    health = report["press_release_health"]
    inv_rows = []
    for row in report["text_unit_inventory"]:
        inv_rows.append(
            "<tr>"
            f"<td>{_escape(row['unit_type'])}</td>"
            f"<td>{_escape(row['unit_key'])}</td>"
            f"<td>{_escape(row['extraction_method'])}</td>"
            f"<td>{_escape(row['artifacts_with_unit'])}</td>"
            f"<td>{_escape(row['min_confidence'])}</td>"
            "</tr>"
        )
    if not inv_rows:
        inv_rows.append('<tr><td colspan="5">No press-release text units yet.</td></tr>')
    tone = "ok-row" if health["artifacts"] == health["with_units"] else "warn-row"
    return f"""
        <section>
          <h2>Earnings Release Extraction</h2>
          <p class="section-note">8-K filing envelopes are stored as artifacts. EX-99 earnings releases are stored as press_release artifacts, then extracted into generic text units and chunks.</p>
          <table>
            <thead><tr><th>Artifacts</th><th>With Units</th><th>Units</th><th>Chunks</th><th>Min Conf</th><th>Fallbacks</th></tr></thead>
            <tbody>
              <tr class="{tone}">
                <td>{_escape(health['artifacts'])}</td>
                <td>{_escape(health['with_units'])}</td>
                <td>{_escape(health['units'])}</td>
                <td>{_escape(health['chunks'])}</td>
                <td>{_escape(health['min_confidence'])}</td>
                <td>{_escape(health['fallbacks'])}</td>
              </tr>
            </tbody>
          </table>
          <h3>Text Unit Inventory</h3>
          <table>
            <thead><tr><th>Type</th><th>Unit Key</th><th>Method</th><th>Artifacts</th><th>Min Conf</th></tr></thead>
            <tbody>{''.join(inv_rows)}</tbody>
          </table>
        </section>
    """


def _amendments_html(report: dict[str, Any]) -> str:
    amendments = [row for row in report["filings"] if _is_amendment_filing(row)]
    if not amendments:
        return ""
    rows = []
    for row in amendments:
        note = "partial amendment"
        if _is_hard_weak_filing(row):
            tone = "bad-row"
            note = "no usable amendment extraction"
        elif _is_amendment_extraction_note(row):
            tone = "warn-row"
            note = "usable partial amendment; not expected to contain every base filing section"
        else:
            tone = "ok-row"
            note = "usable amendment"
        rows.append(
            f'<tr class="{tone}">'
            f"<td>{_escape(row['artifact_type'])}</td>"
            f"<td>{_escape(row['fiscal_period_key'])}</td>"
            f"<td>{_escape(row['filed'])}</td>"
            f"<td class=\"mono\">{_escape(row['accession_number'])}</td>"
            f"<td class=\"mono\">{_escape(row.get('amends_artifact_id'))}</td>"
            f"<td>{_escape(row['sections'])}</td>"
            f"<td>{_escape(row['chunks'])}</td>"
            f"<td>{_escape(row['min_confidence'])}</td>"
            f"<td>{_escape(row['repairs'])}</td>"
            f"<td>{_escape(note)}</td>"
            "</tr>"
        )
    return f"""
        <section>
          <h2>Amendments</h2>
          <p class="section-note">Amended filings are partial by design in v1. The audit does not require a 10-K/A or 10-Q/A to repeat every base filing section.</p>
          <table>
            <thead><tr><th>Type</th><th>Period</th><th>Filed</th><th>Accession</th><th>Amends ID</th><th>Sections</th><th>Chunks</th><th>Min Conf</th><th>Repairs</th><th>Note</th></tr></thead>
            <tbody>{''.join(rows)}</tbody>
          </table>
        </section>
    """


def _section_matrix_html(report: dict[str, Any], artifact_type: str) -> str:
    keys = _expected_section_keys(artifact_type)
    filings = [row for row in report["filings"] if row["artifact_type"] == artifact_type]
    body = []
    for filing in filings:
        sections = report["sections_by_artifact"].get(filing["id"], {})
        cells = []
        for key in keys:
            section = sections.get(key)
            if section is None:
                if _is_amendment_filing(filing):
                    cells.append(f'<td class="section-cell muted" title="{_escape(key)}; amendment partial filing">·</td>')
                else:
                    cells.append(f'<td class="section-cell missing" title="{_escape(key)}">-</td>')
            else:
                method = section["extraction_method"]
                conf = section["confidence"]
                chunks = section["chunks"]
                tone = "ok" if method == "deterministic" and conf >= 0.85 else "warn"
                cells.append(
                    f'<td class="section-cell {tone}" title="{_escape(key)}; method={_escape(method)}; confidence={_escape(conf)}; chunks={_escape(chunks)}">✓</td>'
                )
        body.append(
            "<tr>"
            f"<td>{_escape(_filing_display_type(filing['artifact_type'], filing.get('form_type')))}</td>"
            f"<td>{_escape(filing['fiscal_period_key'])}</td>"
            f"<td>{_escape(filing['filed'])}</td>"
            f"<td class=\"mono\">{_escape(filing['accession_number'])}</td>"
            f"<td>{_escape(filing['sections'])}</td>"
            f"<td>{_escape(filing['chunks'])}</td>"
            + "".join(cells)
            + "</tr>"
        )
    headers = "".join(f"<th>{_escape(_section_label(key))}</th>" for key in keys)
    title = "10-K Section Matrix" if artifact_type == "10k" else "10-Q Section Matrix"
    return f"""
        <section>
          <h2>{title}</h2>
          <div class="matrix-wrap">
            <table class="matrix">
              <thead><tr><th>Type</th><th>Period</th><th>Filed</th><th>Accession</th><th>Sections</th><th>Chunks</th>{headers}</tr></thead>
              <tbody>{''.join(body)}</tbody>
            </table>
          </div>
        </section>
    """


def _warnings_html(report: dict[str, Any]) -> str:
    missing_rows = []
    for row in report["missing_sections"]:
        missing_rows.append(
            "<tr>"
            f"<td>{_escape(row['artifact_type'])}</td>"
            f"<td>{_escape(row['fiscal_period_key'])}</td>"
            f"<td class=\"mono\">{_escape(row['accession_number'])}</td>"
            f"<td>{_escape(', '.join(row['missing_sections']))}</td>"
            "</tr>"
        )
    if not missing_rows:
        missing_rows.append('<tr><td colspan="4">No missing standard section warnings.</td></tr>')

    bucket_titles = {
        "possible_boundary_issue": "Possible Boundary Issues",
        "large_chunk": "Large Chunks",
        "normal_market_risk_reference": "Normal Market-Risk References",
        "short_valid_section": "Short Valid Sections",
        "size_outlier": "Other Size Outliers",
    }
    bucket_notes = {
        "possible_boundary_issue": "Review these first. They may indicate a section tail, orphan heading, or fragment.",
        "large_chunk": "These may be too broad for precise retrieval or citation.",
        "normal_market_risk_reference": "Expected 10-Q Item 3 pattern: the filer points back to annual Item 7A and discloses no material market-risk changes.",
        "short_valid_section": "Usually informational. Legal, controls, and other-information sections are often short by design.",
        "size_outlier": "Unusual size but not classified by a stronger rule.",
    }
    outlier_groups = []
    for bucket, title in bucket_titles.items():
        rows = report["chunk_warning_buckets"].get(bucket, [])
        if not rows:
            continue
        cards = []
        for row in rows:
            cards.append(
                "<article class=\"chunk-card\">"
                f"<div><strong>{_escape(row['fiscal_period_key'])}</strong> · {_escape(row['section_key'])} · chunk {_escape(row['chunk_ordinal'])} · {_escape(row['chars'])} chars · {_escape(row['word_count'])} words</div>"
                f"<p><span>Reason:</span> {_escape(row['warning_reason'])}</p>"
                f"<p><span>Heading:</span> {_escape(_heading_path_label(row['heading_path']))}</p>"
                f"<p><span>Starts:</span> {_escape(row['starts_with'])}</p>"
                f"<p><span>Ends:</span> {_escape(row['ends_with'])}</p>"
                f"<details><summary>Full chunk text</summary><pre class=\"chunk-full\">{_escape(row.get('text'))}</pre></details>"
                "</article>"
            )
        outlier_groups.append(
            f"<h4>{_escape(title)} ({len(rows)})</h4>"
            f"<p class=\"section-note\">{_escape(bucket_notes[bucket])}</p>"
            f"<div class=\"chunk-grid\">{''.join(cards)}</div>"
        )
    if not outlier_groups:
        outlier_groups.append("<p>No chunk review items.</p>")

    return f"""
        <section>
          <h2>Warnings To Review</h2>
          <h3>Missing Standard Sections</h3>
          <table>
            <thead><tr><th>Type</th><th>Period</th><th>Accession</th><th>Missing Sections</th></tr></thead>
            <tbody>{''.join(missing_rows)}</tbody>
          </table>
          <h3>Chunk Review</h3>
          {''.join(outlier_groups)}
        </section>
    """


def _retrieval_html(report: dict[str, Any]) -> str:
    def render_groups(retrieval: dict[str, list[dict[str, Any]]], reasons: dict[str, str]) -> str:
        groups = []
        for query, rows in retrieval.items():
            cards = []
            reason = reasons.get(query, "")
            for row in rows:
                preferred = "preferred" if row.get("preferred_section") else "matched"
                source = row.get("source_kind") or "filing_section"
                exact = "exact phrase" if row.get("exact_phrase") else "term match"
                matched_terms = row.get("matched_terms") or []
                missing_terms = row.get("missing_terms") or []
                chips = "".join(f'<span class="chip ok">{_escape(term)}</span>' for term in matched_terms)
                chips += "".join(f'<span class="chip missing">{_escape(term)}</span>' for term in missing_terms)
                cards.append(
                    "<article class=\"result-card\">"
                    f"<div class=\"result-meta\">{_escape(row['fiscal_period_key'])} · {_escape(source)} · {_escape(row['section_key'])} · chunk {_escape(row['chunk_ordinal'])} · {_escape(preferred)} · rank {_escape(row.get('rank'))} · {_escape(row.get('term_coverage'))} terms · {_escape(exact)}</div>"
                    f"<div class=\"breadcrumb\">{_escape(_heading_path_label(row.get('heading_path')))}</div>"
                    f"<div class=\"chips\">{chips}</div>"
                    f"<p>{_highlight_html(row.get('highlighted_snippet') or row.get('snippet'))}</p>"
                    f"<p class=\"why\"><span>Extraction:</span> {_escape(row.get('extraction_method'))} / conf {_escape(row.get('confidence'))}</p>"
                    "</article>"
                )
            if not cards:
                cards.append("<p>No matches.</p>")
            groups.append(
                f"<div class=\"retrieval-group\"><h3>{_escape(query)}</h3><p class=\"section-note\">{_escape(reason)}</p>{''.join(cards)}</div>"
            )
        return "".join(groups)

    broad_groups = render_groups(report["retrieval"], report["retrieval_reasons"])
    earnings_groups = render_groups(
        report["earnings_release_retrieval"],
        report["earnings_release_reasons"],
    )
    return f"""
        <section>
          <h2>Retrieval Smoke Tests</h2>
          <p class="section-note">These broad checks validate that 10-K/10-Q sections and press releases are searchable together. Preferred sections bias analytical queries toward filing sections.</p>
          <div class="retrieval-grid">{broad_groups}</div>
          <h3>Earnings Release Smoke Tests</h3>
          <p class="section-note">These checks search only EX-99 press-release chunks, so 8-K earnings evidence is visible even when filing sections rank higher in the broad search.</p>
          <div class="retrieval-grid">{earnings_groups}</div>
        </section>
    """

def _render_html_report(reports: list[dict[str, Any]]) -> str:
    sections = []
    for report in reports:
        status_tone = "bad" if report["status"] == "FAIL" else "warn" if report["status"] == "PASS_WITH_WARNINGS" else "ok"
        total_expected = sum(row["expected"] or 0 for row in report["expected_counts"])
        expected_covered = total_expected - len(report["missing"])
        total_stored = sum(row["stored"] for row in report["expected_counts"])
        total_chunks = report["chunk_stats"]["chunks"]
        coverage_value = f"{expected_covered}/{total_expected}" if total_expected else total_stored
        coverage_note = "expected filings present" if total_expected else "stored filings"
        section = f"""
          <div class="report">
            <header class="hero">
              <div>
                <p class="eyebrow">SEC Qualitative Audit</p>
                <h1>{_escape(report['ticker'])}</h1>
                <p>Since {_escape(report['since_date'])}; 10-K/Q window starts FY{_escape(report['min_fy'])}</p>
              </div>
              <span class="status {status_tone}">{_escape(report['status'])}</span>
            </header>
            <div class="metrics">
              {_metric_card('Filing Coverage', coverage_value, coverage_note, 'ok' if not report['missing'] else 'bad')}
              {_metric_card('Weak Extractions', len(report['weak']), 'low confidence, fallback, or missing', 'ok' if not report['weak'] else 'bad')}
              {_metric_card('Chunks', total_chunks, 'retrieval units', 'neutral')}
              {_metric_card('Extra Stored', len(report['unexpected']), 'outside audit window', 'neutral')}
              {_metric_card('Review Items', report['warnings'], 'missing sections + reviewable chunk issues', 'warn' if report['warnings'] else 'ok')}
            </div>
              {_coverage_html(report)}
              {_section_health_html(report)}
              {_press_release_html(report)}
              {_amendments_html(report)}
              {_section_matrix_html(report, '10k')}
            {_section_matrix_html(report, '10q')}
            {_warnings_html(report)}
            {_retrieval_html(report)}
          </div>
        """
        sections.append(section)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>SEC Qualitative Audit</title>
  <style>
    :root {{
      --ink: #172026;
      --muted: #65717a;
      --line: #d8e0e5;
      --bg: #f5f7f8;
      --panel: #ffffff;
      --ok: #147a4b;
      --ok-bg: #e7f5ee;
      --warn: #9a6500;
      --warn-bg: #fff3d8;
      --bad: #b42318;
      --bad-bg: #ffe7e3;
      --blue: #1f5f99;
      font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--ink);
      background: var(--bg);
    }}
    body {{ margin: 0; }}
    .report {{ max-width: 1440px; margin: 0 auto; padding: 28px; }}
    .hero {{ display: flex; justify-content: space-between; align-items: flex-start; gap: 20px; margin-bottom: 20px; }}
    .eyebrow {{ margin: 0 0 4px; color: var(--blue); font-weight: 700; text-transform: uppercase; font-size: 12px; }}
    h1 {{ margin: 0; font-size: 40px; letter-spacing: 0; }}
    h2 {{ margin: 0 0 14px; font-size: 22px; letter-spacing: 0; }}
    h3 {{ margin: 18px 0 10px; font-size: 16px; letter-spacing: 0; }}
    h4 {{ margin: 18px 0 6px; font-size: 14px; letter-spacing: 0; color: #31414a; }}
    p {{ color: var(--muted); }}
    section, .metric {{ background: var(--panel); border: 1px solid var(--line); border-radius: 8px; }}
    section {{ padding: 18px; margin: 16px 0; overflow-x: auto; }}
    .metrics {{ display: grid; grid-template-columns: repeat(4, minmax(180px, 1fr)); gap: 12px; }}
    .metric {{ padding: 14px; }}
    .metric-label {{ color: var(--muted); font-size: 12px; text-transform: uppercase; font-weight: 700; }}
    .metric-value {{ font-size: 30px; font-weight: 760; margin-top: 6px; }}
    .metric-note {{ color: var(--muted); font-size: 13px; margin-top: 4px; }}
    .status {{ padding: 8px 12px; border-radius: 999px; font-weight: 800; border: 1px solid; white-space: nowrap; }}
    .status.ok, .metric.ok {{ background: var(--ok-bg); border-color: #91d4b4; color: var(--ok); }}
    .status.warn, .metric.warn {{ background: var(--warn-bg); border-color: #ecc15a; color: var(--warn); }}
    .status.bad, .metric.bad {{ background: var(--bad-bg); border-color: #f4a096; color: var(--bad); }}
    table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid var(--line); padding: 8px 10px; text-align: left; vertical-align: top; }}
    th {{ color: #31414a; background: #edf2f5; position: sticky; top: 0; z-index: 1; }}
    tr.ok-row td {{ background: #fbfffd; }}
    tr.warn-row td {{ background: #fffaf0; }}
    tr.bad-row td {{ background: #fff1ef; }}
    .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 18px; }}
    .matrix-wrap {{ overflow-x: auto; }}
    .matrix th, .matrix td {{ white-space: nowrap; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12px; }}
    .section-cell {{ text-align: center; font-weight: 800; min-width: 44px; }}
    .section-cell.ok {{ background: var(--ok-bg); color: var(--ok); }}
    .section-cell.warn {{ background: var(--warn-bg); color: var(--warn); }}
    .section-cell.missing {{ background: #eef1f3; color: #7a858c; }}
    .chunk-grid, .retrieval-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 12px; }}
    .chunk-card, .result-card {{ border: 1px solid var(--line); border-radius: 8px; padding: 12px; background: #fbfcfd; }}
    .chunk-card p, .result-card p {{ margin: 8px 0 0; color: var(--ink); line-height: 1.45; }}
    .chunk-card span, .result-meta {{ color: var(--muted); font-weight: 700; font-size: 12px; }}
    details {{ margin-top: 10px; }}
    summary {{ cursor: pointer; color: var(--blue); font-weight: 700; font-size: 12px; }}
    .chunk-full {{ white-space: pre-wrap; overflow-wrap: anywhere; background: #f2f6f8; border: 1px solid var(--line); border-radius: 6px; padding: 10px; max-height: 360px; overflow: auto; font-size: 12px; line-height: 1.45; }}
    .breadcrumb {{ color: var(--blue); font-size: 12px; font-weight: 700; margin-top: 8px; }}
    .chips {{ display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }}
    .chip {{ border: 1px solid var(--line); border-radius: 999px; padding: 2px 8px; font-size: 12px; font-weight: 700; }}
    .chip.ok {{ background: var(--ok-bg); color: var(--ok); border-color: #91d4b4; }}
    .chip.missing {{ background: #eef1f3; color: #7a858c; }}
    mark {{ background: #fff0a8; color: inherit; padding: 0 2px; border-radius: 3px; }}
    .why span {{ color: var(--muted); font-weight: 700; font-size: 12px; }}
    .section-note {{ margin-top: -4px; }}
    @media (max-width: 900px) {{
      .metrics, .two-col {{ grid-template-columns: 1fr; }}
      .report {{ padding: 16px; }}
      h1 {{ font-size: 32px; }}
    }}
  </style>
</head>
<body>
  {''.join(sections)}
</body>
</html>
"""


def _write_html_report(args: argparse.Namespace) -> int:
    since_date = _parse_date(args.since) if args.since else DEFAULT_QUAL_SINCE_DATE
    until_date = _parse_date(args.until)
    queries = args.query or DEFAULT_QUERIES
    reports = []
    exit_code = 0
    with get_conn() as conn:
        for ticker in args.tickers:
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
            report = _collect_report(
                conn,
                ticker,
                expected=expected,
                since_date=since_date,
                until_date=until_date,
                min_fy=min_fy,
                max_fy=max_fy,
                queries=queries,
            )
            reports.append(report)
            if report["hard_issues"]:
                exit_code = 1
            print(
                f"{report['ticker']}: {report['status']} "
                f"(missing={len(report['missing'])}, unexpected={len(report['unexpected'])}, weak={len(report['weak'])})"
            )
    out_path = Path(args.html)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(_render_html_report(reports), encoding="utf-8")
    print(f"Wrote HTML report: {out_path}")
    return exit_code


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
        weak = _section_health(conn, ticker, expected)
        missing_sections = _section_inventory(conn, ticker)
        chunk_outliers = _chunk_health(conn, ticker)
        _retrieval_smoke(conn, ticker, queries)
        _earnings_release_smoke(conn, ticker)
        if args.list_filings:
            _period_listing(conn, ticker)

    print()
    print("Audit Summary")
    hard_issues = missing + weak
    warning_items = missing_sections + chunk_outliers
    if hard_issues:
        status = "FAIL"
    elif warning_items:
        status = "PASS_WITH_WARNINGS"
    else:
        status = "PASS"
    print(f"  status:                       {status}")
    print(f"  missing expected filings:     {missing}")
    print(f"  stored outside audit window:  {unexpected}")
    print(f"  weak/missing extractions:     {weak}")
    print(f"  filings missing some standard sections: {missing_sections}")
    print(f"  reviewable chunk issues:      {chunk_outliers}")
    return 0 if hard_issues == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit SEC qualitative extraction quality.")
    parser.add_argument("--db-only", action="store_true", help="skip live SEC coverage comparison")
    parser.add_argument("--since", help="calendar cutoff date, YYYY-MM-DD")
    parser.add_argument("--until", help="optional calendar upper-bound date, YYYY-MM-DD")
    parser.add_argument("--query", action="append", help="retrieval smoke-test query; repeatable")
    parser.add_argument("--list-filings", action="store_true", help="print kept 10-K/10-Q filing inventory")
    parser.add_argument("--html", help="write a visual HTML audit report to this path")
    parser.add_argument("tickers", nargs="+")
    args = parser.parse_args()

    if args.html:
        return _write_html_report(args)

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
