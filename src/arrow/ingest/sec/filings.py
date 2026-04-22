"""SEC filing/document ingest — recent filings, raw artifacts, no fact loading."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Iterable

import psycopg

from arrow.ingest.common.artifacts import write_artifact
from arrow.ingest.common.cache import RAW_DIR, cache_path
from arrow.ingest.common.http import HttpClient
from arrow.ingest.common.raw_responses import write_raw_response
from arrow.ingest.common.runs import close_failed, close_succeeded, open_run
from arrow.ingest.sec.bootstrap import SEC_RATE_LIMIT, SEC_USER_AGENT, SUBMISSIONS_URL_TEMPLATE
from arrow.normalize.periods.derive import derive_calendar_period, derive_fiscal_period

DEFAULT_FORMS = ("10-K", "10-K/A", "10-Q", "10-Q/A", "8-K", "8-K/A")


@dataclass(frozen=True)
class CompanyRow:
    id: int
    cik: int
    ticker: str
    fiscal_year_end_md: str


@dataclass(frozen=True)
class RecentFiling:
    accession_number: str
    form_type: str
    filing_date: date
    report_date: date | None
    primary_document: str
    primary_doc_description: str | None
    items: list[str]
    is_xbrl: bool
    is_inline_xbrl: bool


@dataclass(frozen=True)
class FilingDocument:
    artifact_type: str
    source_document_id: str
    filename: str
    title: str
    metadata: dict[str, Any]


def _cik10(cik: int) -> str:
    return f"{cik:010d}"


def _cik_compact(cik: int) -> str:
    return str(int(cik))


def _accession_nodash(accession_number: str) -> str:
    return accession_number.replace("-", "")


def _submissions_endpoint(cik: int) -> str:
    return f"submissions/CIK{_cik10(cik)}.json"


def _filing_index_endpoint(cik: int, accession_number: str) -> str:
    return f"filings/{_cik10(cik)}/{accession_number}/index.json"


def _filing_document_endpoint(cik: int, accession_number: str, filename: str) -> str:
    return f"filings/{_cik10(cik)}/{accession_number}/{filename}"


def _filing_cache_path(cik: int, accession_number: str, filename: str) -> Path:
    return RAW_DIR / "sec" / "filings" / _cik10(cik) / accession_number / filename


def _submissions_url(cik: int) -> str:
    return SUBMISSIONS_URL_TEMPLATE.format(cik10=_cik10(cik))


def _filing_index_url(cik: int, accession_number: str) -> str:
    return (
        f"https://www.sec.gov/Archives/edgar/data/{_cik_compact(cik)}/"
        f"{_accession_nodash(accession_number)}/index.json"
    )


def _filing_document_url(cik: int, accession_number: str, filename: str) -> str:
    return (
        f"https://www.sec.gov/Archives/edgar/data/{_cik_compact(cik)}/"
        f"{_accession_nodash(accession_number)}/{filename}"
    )


def _artifact_type_for_form(form_type: str) -> str | None:
    upper = form_type.upper()
    if upper.startswith("10-K"):
        return "10k"
    if upper.startswith("10-Q"):
        return "10q"
    if upper.startswith("8-K"):
        return "8k"
    return None


def _items_list(items: str | None) -> list[str]:
    if not items:
        return []
    return [item.strip() for item in items.split(",") if item.strip()]


def _get_company(conn: psycopg.Connection, ticker: str) -> CompanyRow:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, cik, ticker, fiscal_year_end_md
            FROM companies
            WHERE ticker = %s;
            """,
            (ticker.upper(),),
        )
        row = cur.fetchone()
    if row is None:
        raise LookupError(f"{ticker} not in companies — run seed_companies.py {ticker} first")
    return CompanyRow(id=row[0], cik=row[1], ticker=row[2], fiscal_year_end_md=row[3])


def _iter_recent_filings(payload: dict[str, Any]) -> Iterable[RecentFiling]:
    recent = payload.get("filings", {}).get("recent", {})
    accessions = recent.get("accessionNumber", [])
    n = len(accessions)
    for i in range(n):
        form_type = recent.get("form", [None] * n)[i]
        accession_number = accessions[i]
        filing_date_raw = recent.get("filingDate", [None] * n)[i]
        primary_document = recent.get("primaryDocument", [None] * n)[i]
        if not form_type or not accession_number or not filing_date_raw or not primary_document:
            continue
        report_date_raw = recent.get("reportDate", [None] * n)[i]
        yield RecentFiling(
            accession_number=accession_number,
            form_type=form_type,
            filing_date=date.fromisoformat(filing_date_raw),
            report_date=date.fromisoformat(report_date_raw) if report_date_raw else None,
            primary_document=primary_document,
            primary_doc_description=recent.get("primaryDocDescription", [None] * n)[i],
            items=_items_list(recent.get("items", [None] * n)[i]),
            is_xbrl=bool(recent.get("isXBRL", [0] * n)[i]),
            is_inline_xbrl=bool(recent.get("isInlineXBRL", [0] * n)[i]),
        )


def _period_fields(
    company: CompanyRow,
    *,
    form_type: str,
    report_date: date | None,
) -> dict[str, Any]:
    if report_date is None:
        return {
            "fiscal_year": None,
            "fiscal_quarter": None,
            "fiscal_period_label": None,
            "period_end": None,
            "period_type": None,
            "calendar_year": None,
            "calendar_quarter": None,
            "calendar_period_label": None,
        }
    upper = form_type.upper()
    if upper.startswith("10-K"):
        ptype = "annual"
    elif upper.startswith("10-Q"):
        ptype = "quarter"
    else:
        return {
            "fiscal_year": None,
            "fiscal_quarter": None,
            "fiscal_period_label": None,
            "period_end": None,
            "period_type": None,
            "calendar_year": None,
            "calendar_quarter": None,
            "calendar_period_label": None,
        }
    fiscal = derive_fiscal_period(
        report_date,
        company.fiscal_year_end_md,
        period_type=ptype,
    )
    calendar = derive_calendar_period(report_date)
    return {
        "fiscal_year": fiscal.fiscal_year,
        "fiscal_quarter": fiscal.fiscal_quarter,
        "fiscal_period_label": fiscal.fiscal_period_label,
        "period_end": report_date,
        "period_type": fiscal.period_type,
        "calendar_year": calendar.calendar_year,
        "calendar_quarter": calendar.calendar_quarter,
        "calendar_period_label": calendar.calendar_period_label,
    }


def _press_release_docs(index_payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = index_payload.get("directory", {}).get("item", [])
    out: list[dict[str, Any]] = []
    for item in items:
        name = item.get("name")
        doc_type = str(item.get("type") or "")
        if not name:
            continue
        upper_type = doc_type.upper()
        if upper_type.startswith("EX-99"):
            out.append(item)
            continue
        description = str(item.get("description") or "").lower()
        if "press release" in description or "earnings release" in description:
            out.append(item)
    return out


def _filing_documents(
    filing: RecentFiling,
    *,
    company: CompanyRow,
    index_payload: dict[str, Any],
) -> list[FilingDocument]:
    amended = filing.form_type.upper().endswith("/A")
    base_metadata = {
        "accession_number": filing.accession_number,
        "filer_cik": _cik10(company.cik),
        "form_type": filing.form_type,
        "amended": amended,
        "filing_date": filing.filing_date.isoformat(),
        "items": filing.items,
        "xbrl_available": filing.is_xbrl,
        "is_inline_xbrl": filing.is_inline_xbrl,
        "primary_document": filing.primary_document,
    }
    docs = [
        FilingDocument(
            artifact_type=_artifact_type_for_form(filing.form_type) or "8k",
            source_document_id=filing.accession_number,
            filename=filing.primary_document,
            title=filing.primary_doc_description or f"{company.ticker} {filing.form_type}",
            metadata=base_metadata,
        )
    ]
    if _artifact_type_for_form(filing.form_type) != "8k":
        return docs

    for item in _press_release_docs(index_payload):
        name = str(item.get("name"))
        if name == filing.primary_document:
            continue
        docs.append(
            FilingDocument(
                artifact_type="press_release",
                source_document_id=f"{filing.accession_number}:{name}",
                filename=name,
                title=str(item.get("description") or f"{company.ticker} press release"),
                metadata={
                    **base_metadata,
                    "document_name": name,
                    "document_type": item.get("type"),
                    "distribution_channel": "sec_exhibit",
                },
            )
        )
    return docs


def ingest_recent_sec_filings(
    conn: psycopg.Connection,
    tickers: list[str],
    *,
    forms: tuple[str, ...] = DEFAULT_FORMS,
    limit_per_ticker: int = 5,
) -> dict[str, Any]:
    run_id = open_run(
        conn,
        run_kind="manual",
        vendor="sec",
        ticker_scope=[t.upper() for t in tickers],
    )
    http = HttpClient(user_agent=SEC_USER_AGENT, rate_limit=SEC_RATE_LIMIT)
    wanted_forms = {form.upper() for form in forms}
    counts: dict[str, Any] = {
        "forms": list(forms),
        "limit_per_ticker": limit_per_ticker,
        "raw_responses": 0,
        "filings_seen": 0,
        "artifacts_written": 0,
        "artifacts_existing": 0,
        "artifacts_by_type": {},
    }

    try:
        for ticker in tickers:
            company = _get_company(conn, ticker)
            submissions_url = _submissions_url(company.cik)
            submissions_resp = http.get(submissions_url)
            submissions_payload = json.loads(submissions_resp.body)
            with conn.transaction():
                write_raw_response(
                    conn,
                    ingest_run_id=run_id,
                    vendor="sec",
                    endpoint=_submissions_endpoint(company.cik),
                    params={"cik": company.cik},
                    request_url=submissions_resp.url,
                    http_status=submissions_resp.status,
                    content_type=submissions_resp.content_type,
                    response_headers=submissions_resp.headers,
                    body=submissions_resp.body,
                    cache_path=cache_path("sec", _submissions_endpoint(company.cik)),
                )
            counts["raw_responses"] += 1

            matched = 0
            for filing in _iter_recent_filings(submissions_payload):
                if filing.form_type.upper() not in wanted_forms:
                    continue
                if matched >= limit_per_ticker:
                    break
                matched += 1
                counts["filings_seen"] += 1

                index_url = _filing_index_url(company.cik, filing.accession_number)
                index_resp = http.get(index_url)
                index_payload = json.loads(index_resp.body)
                with conn.transaction():
                    write_raw_response(
                        conn,
                        ingest_run_id=run_id,
                        vendor="sec",
                        endpoint=_filing_index_endpoint(company.cik, filing.accession_number),
                        params={},
                        request_url=index_resp.url,
                        http_status=index_resp.status,
                        content_type=index_resp.content_type,
                        response_headers=index_resp.headers,
                        body=index_resp.body,
                        cache_path=_filing_cache_path(
                            company.cik, filing.accession_number, "index.json"
                        ),
                    )
                counts["raw_responses"] += 1

                period_fields = _period_fields(
                    company, form_type=filing.form_type, report_date=filing.report_date
                )
                published_at = datetime.combine(
                    filing.filing_date,
                    datetime.min.time(),
                    tzinfo=UTC,
                )
                for document in _filing_documents(
                    filing, company=company, index_payload=index_payload
                ):
                    document_url = _filing_document_url(
                        company.cik, filing.accession_number, document.filename
                    )
                    doc_resp = http.get(document_url)
                    with conn.transaction():
                        write_raw_response(
                            conn,
                            ingest_run_id=run_id,
                            vendor="sec",
                            endpoint=_filing_document_endpoint(
                                company.cik, filing.accession_number, document.filename
                            ),
                            params={},
                            request_url=doc_resp.url,
                            http_status=doc_resp.status,
                            content_type=doc_resp.content_type,
                            response_headers=doc_resp.headers,
                            body=doc_resp.body,
                            cache_path=_filing_cache_path(
                                company.cik, filing.accession_number, document.filename
                            ),
                        )
                        _, created = write_artifact(
                            conn,
                            ingest_run_id=run_id,
                            artifact_type=document.artifact_type,
                            source="sec",
                            source_document_id=document.source_document_id,
                            body=doc_resp.body,
                            ticker=company.ticker,
                            title=document.title,
                            url=document_url,
                            content_type=doc_resp.content_type,
                            language="en",
                            published_at=published_at,
                            artifact_metadata=document.metadata,
                            **period_fields,
                        )
                    counts["raw_responses"] += 1
                    key = document.artifact_type
                    counts["artifacts_by_type"][key] = counts["artifacts_by_type"].get(key, 0) + 1
                    if created:
                        counts["artifacts_written"] += 1
                    else:
                        counts["artifacts_existing"] += 1

    except Exception as e:
        close_failed(
            conn,
            run_id,
            error_message=str(e),
            error_details={"type": type(e).__name__},
        )
        raise

    close_succeeded(conn, run_id, counts=counts)
    counts["ingest_run_id"] = run_id
    return counts
