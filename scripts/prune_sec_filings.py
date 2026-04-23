"""Prune SEC qualitative filings outside Arrow's default retention window.

Usage:
    uv run scripts/prune_sec_filings.py NVDA
    uv run scripts/prune_sec_filings.py --execute NVDA

Default is a dry run. The retention rule is:
  - 10-K / 10-Q: keep fiscal years whose FY end is on/after the 5-year
    calendar cutoff, so the first kept fiscal year is complete.
  - earnings 8-K / press_release artifacts: keep by the 5-year filing-date
    cutoff because they do not carry the 10-K/10-Q fiscal section contract.
"""

from __future__ import annotations

import argparse
import shutil
from dataclasses import dataclass
from pathlib import Path

from arrow.db.connection import get_conn
from arrow.ingest.common.cache import RAW_DIR
from arrow.ingest.sec.filings import DEFAULT_QUAL_SINCE_DATE
from arrow.normalize.periods.derive import min_fiscal_year_for_since_date


@dataclass(frozen=True)
class Company:
    id: int
    cik10: str
    ticker: str
    fiscal_year_end_md: str


@dataclass(frozen=True)
class PrunePlan:
    ticker: str
    since_date: str
    min_fiscal_year: int
    artifact_ids: list[int]
    accessions: list[str]
    raw_response_count: int
    cache_dirs: list[Path]


def _company(conn, ticker: str) -> Company:
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
        raise LookupError(f"{ticker} not in companies")
    return Company(
        id=row[0],
        cik10=f"{row[1]:010d}",
        ticker=row[2],
        fiscal_year_end_md=row[3],
    )


def _accession_for_artifact(accession_number: str | None, source_document_id: str | None) -> str | None:
    if accession_number:
        return accession_number
    if source_document_id and ":" in source_document_id:
        return source_document_id.split(":", 1)[0]
    return None


def _plan_for_ticker(conn, ticker: str) -> PrunePlan:
    company = _company(conn, ticker)
    min_fy = min_fiscal_year_for_since_date(
        DEFAULT_QUAL_SINCE_DATE,
        company.fiscal_year_end_md,
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, accession_number, source_document_id
            FROM artifacts
            WHERE company_id = %s
              AND source = 'sec'
              AND (
                  (artifact_type IN ('10k', '10q') AND fiscal_year < %s)
                  OR (artifact_type IN ('8k', 'press_release') AND published_at::date < %s)
              )
            ORDER BY published_at, id;
            """,
            (company.id, min_fy, DEFAULT_QUAL_SINCE_DATE),
        )
        rows = cur.fetchall()

    artifact_ids = [row[0] for row in rows]
    accessions = sorted(
        {
            accession
            for _id, accession_number, source_document_id in rows
            if (accession := _accession_for_artifact(accession_number, source_document_id))
        }
    )

    raw_response_count = 0
    if accessions:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT count(*)
                FROM raw_responses r
                JOIN unnest(%s::text[]) AS old_accession(accession)
                  ON r.endpoint LIKE ('filings/' || %s || '/' || old_accession.accession || '/%%')
                WHERE r.vendor = 'sec';
                """,
                (accessions, company.cik10),
            )
            raw_response_count = cur.fetchone()[0]

    cache_root = RAW_DIR / "sec" / "filings" / company.cik10
    cache_dirs = [cache_root / accession for accession in accessions if (cache_root / accession).exists()]
    return PrunePlan(
        ticker=company.ticker,
        since_date=DEFAULT_QUAL_SINCE_DATE.isoformat(),
        min_fiscal_year=min_fy,
        artifact_ids=artifact_ids,
        accessions=accessions,
        raw_response_count=raw_response_count,
        cache_dirs=cache_dirs,
    )


def _execute_plan(conn, plan: PrunePlan, *, cik10: str) -> None:
    if not plan.artifact_ids and not plan.accessions:
        return
    with conn.transaction():
        if plan.artifact_ids:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE artifacts SET amends_artifact_id = NULL WHERE amends_artifact_id = ANY(%s);",
                    (plan.artifact_ids,),
                )
                cur.execute(
                    "UPDATE artifacts SET supersedes = NULL WHERE supersedes = ANY(%s);",
                    (plan.artifact_ids,),
                )
                cur.execute("DELETE FROM artifacts WHERE id = ANY(%s);", (plan.artifact_ids,))
        if plan.accessions:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM raw_responses r
                    USING unnest(%s::text[]) AS old_accession(accession)
                    WHERE r.vendor = 'sec'
                      AND r.endpoint LIKE ('filings/' || %s || '/' || old_accession.accession || '/%%');
                    """,
                    (plan.accessions, cik10),
                )

    for directory in plan.cache_dirs:
        shutil.rmtree(directory)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true", help="actually delete rows/files")
    parser.add_argument("tickers", nargs="+")
    args = parser.parse_args()

    with get_conn() as conn:
        plans = [_plan_for_ticker(conn, ticker) for ticker in args.tickers]
        companies = {_company(conn, ticker).ticker: _company(conn, ticker) for ticker in args.tickers}
        for plan in plans:
            print(f"{plan.ticker}:")
            print(f"  since_date:             {plan.since_date}")
            print(f"  10-K/Q window start:    FY{plan.min_fiscal_year}")
            print(f"  artifacts to delete:    {len(plan.artifact_ids)}")
            print(f"  accessions to delete:   {len(plan.accessions)}")
            print(f"  raw_responses to delete:{plan.raw_response_count:5d}")
            print(f"  cache dirs to delete:   {len(plan.cache_dirs)}")
            if args.execute:
                _execute_plan(conn, plan, cik10=companies[plan.ticker].cik10)
                print("  status:                 pruned")
            else:
                print("  status:                 dry-run")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
