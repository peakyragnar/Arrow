"""Fetch SEC XBRL companyfacts — every us-gaap concept x every period in one payload.

Endpoint: https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json

Returns every numeric fact ever filed by the company, grouped by taxonomy
(us-gaap, dei, invest, srt) and concept. This is the authoritative
secondary source for verifying FMP-derived financial_facts.

Written as one raw_responses row per fetch. Must be called inside an open
transaction; follows the same pattern as ingest/sec/bootstrap.py.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import psycopg

from arrow.ingest.common.cache import cache_path
from arrow.ingest.common.http import HttpClient
from arrow.ingest.common.raw_responses import write_raw_response

COMPANY_FACTS_URL_TEMPLATE = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10}.json"
COMPANY_FACTS_ENDPOINT_TEMPLATE = "api/xbrl/companyfacts/CIK{cik10}.json"


@dataclass(frozen=True)
class CompanyFactsFetch:
    raw_response_id: int
    payload: dict[str, Any]


def _cik10(cik: int) -> str:
    return f"{cik:010d}"


def fetch_company_facts(
    conn: psycopg.Connection,
    *,
    cik: int,
    ingest_run_id: int,
    http: HttpClient,
) -> CompanyFactsFetch:
    """Fetch + persist the SEC companyfacts JSON for one CIK."""
    url = COMPANY_FACTS_URL_TEMPLATE.format(cik10=_cik10(cik))
    endpoint = COMPANY_FACTS_ENDPOINT_TEMPLATE.format(cik10=_cik10(cik))
    resp = http.get(url)
    payload = json.loads(resp.body)

    raw_id = write_raw_response(
        conn,
        ingest_run_id=ingest_run_id,
        vendor="sec",
        endpoint=endpoint,
        params={"cik": cik},
        request_url=resp.url,
        http_status=resp.status,
        content_type=resp.content_type,
        response_headers=resp.headers,
        body=resp.body,
        cache_path=cache_path("sec", endpoint),
    )
    return CompanyFactsFetch(raw_response_id=raw_id, payload=payload)
