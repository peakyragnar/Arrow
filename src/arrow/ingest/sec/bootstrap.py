"""SEC bootstrap — seed companies from SEC filings endpoints.

Two fetches per ticker:
  1. https://www.sec.gov/files/company_tickers.json          (ticker -> CIK)
  2. https://data.sec.gov/submissions/CIK{cik10}.json        (name + fiscalYearEnd)

Idempotent: upsert on (cik). Re-runs update ticker/name/fiscal_year_end_md
and bump updated_at.

SEC policy: requests must carry a descriptive User-Agent with contact info,
and throttle to <= 10 req/sec. We set a generous 0.15s min-interval.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import psycopg

from arrow.ingest.common.cache import cache_path
from arrow.ingest.common.http import HttpClient, RateLimit, Response
from arrow.ingest.common.raw_responses import write_raw_response
from arrow.ingest.common.runs import close_failed, close_succeeded, open_run

SEC_USER_AGENT = "Arrow Research info@exascale.capital"
SEC_RATE_LIMIT = RateLimit(min_interval_s=0.15)

COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
COMPANY_TICKERS_ENDPOINT = "files/company_tickers.json"

SUBMISSIONS_URL_TEMPLATE = "https://data.sec.gov/submissions/CIK{cik10}.json"


@dataclass(frozen=True)
class SeededCompany:
    id: int
    cik: int
    ticker: str
    name: str
    fiscal_year_end_md: str


def _cik10(cik: int) -> str:
    return f"{cik:010d}"


def _fiscal_year_end_md(fye: str) -> str:
    # SEC returns "0126"; schema requires "01-26".
    if not (len(fye) == 4 and fye.isdigit()):
        raise ValueError(f"unexpected fiscalYearEnd: {fye!r}")
    return f"{fye[:2]}-{fye[2:]}"


def _find_cik_for_ticker(directory: dict[str, dict[str, Any]], ticker: str) -> int:
    ticker_up = ticker.upper()
    for entry in directory.values():
        if str(entry.get("ticker", "")).upper() == ticker_up:
            return int(entry["cik_str"])
    raise LookupError(f"ticker {ticker!r} not in SEC company_tickers directory")


def _upsert_company(
    conn: psycopg.Connection,
    *,
    cik: int,
    ticker: str,
    name: str,
    fiscal_year_end_md: str,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO companies (cik, ticker, name, fiscal_year_end_md)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (cik) DO UPDATE SET
                ticker             = EXCLUDED.ticker,
                name               = EXCLUDED.name,
                fiscal_year_end_md = EXCLUDED.fiscal_year_end_md,
                updated_at         = now()
            RETURNING id;
            """,
            (cik, ticker.upper(), name, fiscal_year_end_md),
        )
        return cur.fetchone()[0]


def _seed_one(
    conn: psycopg.Connection,
    *,
    ticker: str,
    http: HttpClient,
    ingest_run_id: int,
) -> SeededCompany:
    tick_resp: Response = http.get(COMPANY_TICKERS_URL)
    directory = json.loads(tick_resp.body)
    cik = _find_cik_for_ticker(directory, ticker)

    subs_url = SUBMISSIONS_URL_TEMPLATE.format(cik10=_cik10(cik))
    subs_endpoint = f"submissions/CIK{_cik10(cik)}.json"
    subs_resp: Response = http.get(subs_url)
    subs = json.loads(subs_resp.body)

    name = subs["name"]
    fye_md = _fiscal_year_end_md(subs["fiscalYearEnd"])

    with conn.transaction():
        write_raw_response(
            conn,
            ingest_run_id=ingest_run_id,
            vendor="sec",
            endpoint=COMPANY_TICKERS_ENDPOINT,
            params={},
            request_url=tick_resp.url,
            http_status=tick_resp.status,
            content_type=tick_resp.content_type,
            response_headers=tick_resp.headers,
            body=tick_resp.body,
            cache_path=cache_path("sec", COMPANY_TICKERS_ENDPOINT),
        )
        write_raw_response(
            conn,
            ingest_run_id=ingest_run_id,
            vendor="sec",
            endpoint=subs_endpoint,
            params={"cik": cik},
            request_url=subs_resp.url,
            http_status=subs_resp.status,
            content_type=subs_resp.content_type,
            response_headers=subs_resp.headers,
            body=subs_resp.body,
            cache_path=cache_path("sec", subs_endpoint),
        )
        company_id = _upsert_company(
            conn,
            cik=cik,
            ticker=ticker,
            name=name,
            fiscal_year_end_md=fye_md,
        )

    return SeededCompany(
        id=company_id,
        cik=cik,
        ticker=ticker.upper(),
        name=name,
        fiscal_year_end_md=fye_md,
    )


def seed_companies(
    conn: psycopg.Connection,
    tickers: list[str],
) -> list[SeededCompany]:
    """Seed one or more companies from SEC. Opens + closes an ingest_run."""
    run_id = open_run(
        conn,
        run_kind="manual",
        vendor="sec",
        ticker_scope=[t.upper() for t in tickers],
    )
    http = HttpClient(user_agent=SEC_USER_AGENT, rate_limit=SEC_RATE_LIMIT)

    seeded: list[SeededCompany] = []
    counts = {"companies": 0, "raw_responses": 0}
    try:
        for t in tickers:
            result = _seed_one(conn, ticker=t, http=http, ingest_run_id=run_id)
            seeded.append(result)
            counts["companies"] += 1
            counts["raw_responses"] += 2
    except Exception as e:
        close_failed(
            conn,
            run_id,
            error_message=str(e),
            error_details={"type": type(e).__name__},
        )
        raise

    close_succeeded(conn, run_id, counts=counts)
    return seeded
