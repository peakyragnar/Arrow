"""Seed `securities` rows for existing companies + benchmark ETFs.

Idempotent. Safe to re-run.

Usage:
    uv run scripts/seed_securities.py

For each row in `companies`:
  - insert a `securities` row (kind=common_stock, ticker=companies.ticker)
  - set companies.primary_security_id to that row

For benchmarks (SPY, QQQ):
  - insert a `securities` row (kind=etf, company_id=NULL)
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

from arrow.db.connection import get_conn


BENCHMARK_ETFS = [
    ("SPY", "S&P 500 ETF (broad-market baseline)"),
    ("QQQ", "Nasdaq-100 ETF (tech-tilted baseline)"),
]


@dataclass
class SeedResult:
    companies_linked: int
    benchmarks_inserted: int
    already_seeded: int


def seed_securities() -> SeedResult:
    companies_linked = 0
    benchmarks_inserted = 0
    already_seeded = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            # 1. Ensure every company has a primary_security row + linkage.
            cur.execute(
                """
                SELECT id, ticker
                FROM companies
                WHERE status = 'active'
                ORDER BY ticker
                """
            )
            companies = cur.fetchall()

            for company_id, ticker in companies:
                # Check whether this company already has a security row.
                cur.execute(
                    """
                    SELECT id
                    FROM securities
                    WHERE company_id = %s AND kind = 'common_stock' AND status = 'active'
                    """,
                    (company_id,),
                )
                existing = cur.fetchone()

                if existing:
                    security_id = existing[0]
                    already_seeded += 1
                else:
                    cur.execute(
                        """
                        INSERT INTO securities (company_id, ticker, kind, status)
                        VALUES (%s, %s, 'common_stock', 'active')
                        RETURNING id
                        """,
                        (company_id, ticker),
                    )
                    security_id = cur.fetchone()[0]
                    companies_linked += 1

                # Always (re)set primary_security_id — idempotent and self-healing.
                cur.execute(
                    """
                    UPDATE companies
                    SET primary_security_id = %s, updated_at = now()
                    WHERE id = %s AND (primary_security_id IS NULL OR primary_security_id <> %s)
                    """,
                    (security_id, company_id, security_id),
                )

            # 2. Benchmarks: ETFs with NULL company_id.
            for ticker, _description in BENCHMARK_ETFS:
                cur.execute(
                    """
                    SELECT id FROM securities
                    WHERE ticker = %s AND kind = 'etf' AND status = 'active'
                    """,
                    (ticker,),
                )
                if cur.fetchone():
                    already_seeded += 1
                    continue

                cur.execute(
                    """
                    INSERT INTO securities (company_id, ticker, kind, status)
                    VALUES (NULL, %s, 'etf', 'active')
                    """,
                    (ticker,),
                )
                benchmarks_inserted += 1

        conn.commit()

    return SeedResult(
        companies_linked=companies_linked,
        benchmarks_inserted=benchmarks_inserted,
        already_seeded=already_seeded,
    )


def main() -> int:
    result = seed_securities()
    print(f"Companies linked to new security:   {result.companies_linked}")
    print(f"Benchmark ETFs inserted:            {result.benchmarks_inserted}")
    print(f"Already-seeded (no change):         {result.already_seeded}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
