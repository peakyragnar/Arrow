"""FMP estimates ingest orchestration.

For each security:
  1. Fetch + load forward / historical analyst estimates (annual + quarter).
  2. Fetch + load price-target consensus (one row).
  3. Fetch + load earnings surprises (announcement-grain history).
  4. Fetch + load analyst grades event log (full history).
  5. Walk price-target-news pages until empty.

Per-table semantics (see docs/architecture/estimates_ingest_plan.md):

  analyst_estimates       — delete-by-(security, period_kind) then insert
  price_target_consensus  — UPSERT on PK (security_id)
  earnings_surprises      — UPSERT on (security_id, announcement_date)
  analyst_grades          — INSERT ... ON CONFLICT DO NOTHING (natural key)
  analyst_price_targets   — INSERT ... ON CONFLICT DO NOTHING (natural key)

Idempotent end-to-end: re-runs converge on the same state. The grades and
price-target-news event logs grow only when FMP exposes new events.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import psycopg

from arrow.ingest.common.runs import close_failed, close_succeeded, open_run
from arrow.ingest.fmp.client import FMPClient
from arrow.ingest.fmp.estimates import (
    fetch_analyst_estimates,
    fetch_earnings,
    fetch_grades,
    fetch_price_target_consensus,
    fetch_price_target_news_page,
)


@dataclass(frozen=True)
class SecurityRow:
    id: int
    ticker: str
    kind: str


class SecurityNotSeeded(RuntimeError):
    pass


def _resolve_securities(
    conn: psycopg.Connection, tickers: list[str]
) -> list[SecurityRow]:
    out: list[SecurityRow] = []
    with conn.cursor() as cur:
        for ticker in tickers:
            cur.execute(
                """
                SELECT id, ticker, kind
                FROM securities
                WHERE ticker = %s AND status = 'active'
                """,
                (ticker.upper(),),
            )
            row = cur.fetchone()
            if row is None:
                raise SecurityNotSeeded(
                    f"{ticker} not in securities — run seed_securities.py first"
                )
            out.append(SecurityRow(id=row[0], ticker=row[1], kind=row[2]))
    return out


# --------------------------------------------------------------------------- #
# Coercion helpers
# --------------------------------------------------------------------------- #


def _dec(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _date(value: Any) -> str | None:
    """Pass-through for ISO date strings; psycopg parses them."""
    if not value:
        return None
    return str(value)[:10]


# --------------------------------------------------------------------------- #
# Loaders (one per table)
# --------------------------------------------------------------------------- #


def _load_analyst_estimates(
    conn: psycopg.Connection,
    *,
    security_id: int,
    period_kind: str,
    rows: list[dict[str, Any]],
    raw_response_id: int,
    fetched_at: datetime,
) -> int:
    """Delete-by-(security, period_kind) then insert. Returns row count inserted."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM analyst_estimates "
            "WHERE security_id = %s AND period_kind = %s",
            (security_id, period_kind),
        )
        inserted = 0
        for r in rows:
            period_end = _date(r.get("date"))
            if period_end is None:
                continue
            cur.execute(
                """
                INSERT INTO analyst_estimates (
                    security_id, period_kind, period_end,
                    revenue_low, revenue_avg, revenue_high,
                    ebitda_low, ebitda_avg, ebitda_high,
                    ebit_low, ebit_avg, ebit_high,
                    net_income_low, net_income_avg, net_income_high,
                    sga_expense_low, sga_expense_avg, sga_expense_high,
                    eps_low, eps_avg, eps_high,
                    num_analysts_revenue, num_analysts_eps,
                    fetched_at, source_raw_response_id
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s
                )
                ON CONFLICT (security_id, period_kind, period_end) DO UPDATE SET
                    revenue_low=EXCLUDED.revenue_low,
                    revenue_avg=EXCLUDED.revenue_avg,
                    revenue_high=EXCLUDED.revenue_high,
                    ebitda_low=EXCLUDED.ebitda_low,
                    ebitda_avg=EXCLUDED.ebitda_avg,
                    ebitda_high=EXCLUDED.ebitda_high,
                    ebit_low=EXCLUDED.ebit_low,
                    ebit_avg=EXCLUDED.ebit_avg,
                    ebit_high=EXCLUDED.ebit_high,
                    net_income_low=EXCLUDED.net_income_low,
                    net_income_avg=EXCLUDED.net_income_avg,
                    net_income_high=EXCLUDED.net_income_high,
                    sga_expense_low=EXCLUDED.sga_expense_low,
                    sga_expense_avg=EXCLUDED.sga_expense_avg,
                    sga_expense_high=EXCLUDED.sga_expense_high,
                    eps_low=EXCLUDED.eps_low,
                    eps_avg=EXCLUDED.eps_avg,
                    eps_high=EXCLUDED.eps_high,
                    num_analysts_revenue=EXCLUDED.num_analysts_revenue,
                    num_analysts_eps=EXCLUDED.num_analysts_eps,
                    fetched_at=EXCLUDED.fetched_at,
                    source_raw_response_id=EXCLUDED.source_raw_response_id
                """,
                (
                    security_id,
                    period_kind,
                    period_end,
                    _dec(r.get("revenueLow")),
                    _dec(r.get("revenueAvg")),
                    _dec(r.get("revenueHigh")),
                    _dec(r.get("ebitdaLow")),
                    _dec(r.get("ebitdaAvg")),
                    _dec(r.get("ebitdaHigh")),
                    _dec(r.get("ebitLow")),
                    _dec(r.get("ebitAvg")),
                    _dec(r.get("ebitHigh")),
                    _dec(r.get("netIncomeLow")),
                    _dec(r.get("netIncomeAvg")),
                    _dec(r.get("netIncomeHigh")),
                    _dec(r.get("sgaExpenseLow")),
                    _dec(r.get("sgaExpenseAvg")),
                    _dec(r.get("sgaExpenseHigh")),
                    _dec(r.get("epsLow")),
                    _dec(r.get("epsAvg")),
                    _dec(r.get("epsHigh")),
                    _int(r.get("numAnalystsRevenue")),
                    _int(r.get("numAnalystsEps")),
                    fetched_at,
                    raw_response_id,
                ),
            )
            inserted += 1
    return inserted


def _load_price_target_consensus(
    conn: psycopg.Connection,
    *,
    security_id: int,
    rows: list[dict[str, Any]],
    raw_response_id: int,
    fetched_at: datetime,
) -> int:
    if not rows:
        return 0
    r = rows[0]
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO price_target_consensus (
                security_id, target_high, target_low, target_median, target_consensus,
                fetched_at, source_raw_response_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (security_id) DO UPDATE SET
                target_high=EXCLUDED.target_high,
                target_low=EXCLUDED.target_low,
                target_median=EXCLUDED.target_median,
                target_consensus=EXCLUDED.target_consensus,
                fetched_at=EXCLUDED.fetched_at,
                source_raw_response_id=EXCLUDED.source_raw_response_id
            """,
            (
                security_id,
                _dec(r.get("targetHigh")),
                _dec(r.get("targetLow")),
                _dec(r.get("targetMedian")),
                _dec(r.get("targetConsensus")),
                fetched_at,
                raw_response_id,
            ),
        )
    return 1


def _load_earnings(
    conn: psycopg.Connection,
    *,
    security_id: int,
    rows: list[dict[str, Any]],
    raw_response_id: int,
) -> tuple[int, int]:
    """UPSERT on (security_id, announcement_date). Returns (inserted, updated)."""
    inserted = 0
    updated = 0
    with conn.cursor() as cur:
        for r in rows:
            announcement_date = _date(r.get("date"))
            if announcement_date is None:
                continue
            cur.execute(
                """
                INSERT INTO earnings_surprises (
                    security_id, announcement_date,
                    eps_actual, eps_estimated,
                    revenue_actual, revenue_estimated,
                    last_updated, source_raw_response_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (security_id, announcement_date) DO UPDATE SET
                    eps_actual=EXCLUDED.eps_actual,
                    eps_estimated=EXCLUDED.eps_estimated,
                    revenue_actual=EXCLUDED.revenue_actual,
                    revenue_estimated=EXCLUDED.revenue_estimated,
                    last_updated=EXCLUDED.last_updated,
                    source_raw_response_id=EXCLUDED.source_raw_response_id,
                    ingested_at=now()
                RETURNING (xmax = 0) AS inserted
                """,
                (
                    security_id,
                    announcement_date,
                    _dec(r.get("epsActual")),
                    _dec(r.get("epsEstimated")),
                    _dec(r.get("revenueActual")),
                    _dec(r.get("revenueEstimated")),
                    _date(r.get("lastUpdated")),
                    raw_response_id,
                ),
            )
            was_insert = cur.fetchone()[0]
            if was_insert:
                inserted += 1
            else:
                updated += 1
    return inserted, updated


def _load_grades(
    conn: psycopg.Connection,
    *,
    security_id: int,
    rows: list[dict[str, Any]],
    raw_response_id: int,
) -> int:
    """Append-only with natural-key dedup. Returns rows inserted."""
    inserted = 0
    valid_actions = {"upgrade", "downgrade", "maintain"}
    with conn.cursor() as cur:
        for r in rows:
            action_date = _date(r.get("date"))
            grading_company = r.get("gradingCompany")
            action = r.get("action")
            if not action_date or not grading_company or action not in valid_actions:
                continue
            cur.execute(
                """
                INSERT INTO analyst_grades (
                    security_id, action_date, grading_company,
                    previous_grade, new_grade, action,
                    source_raw_response_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (
                    security_id, action_date, grading_company,
                    COALESCE(previous_grade, ''), COALESCE(new_grade, ''), action
                ) DO NOTHING
                RETURNING id
                """,
                (
                    security_id,
                    action_date,
                    grading_company,
                    r.get("previousGrade"),
                    r.get("newGrade"),
                    action,
                    raw_response_id,
                ),
            )
            if cur.fetchone() is not None:
                inserted += 1
    return inserted


def _load_price_targets(
    conn: psycopg.Connection,
    *,
    security_id: int,
    rows: list[dict[str, Any]],
    raw_response_id: int,
) -> int:
    """Append-only with natural-key dedup. Returns rows inserted."""
    inserted = 0
    with conn.cursor() as cur:
        for r in rows:
            published_at = r.get("publishedDate")
            if not published_at:
                continue
            cur.execute(
                """
                INSERT INTO analyst_price_targets (
                    security_id, published_at,
                    analyst_name, analyst_company,
                    price_target, adj_price_target, price_when_posted,
                    news_url, news_title, news_publisher, news_base_url,
                    source_raw_response_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (
                    security_id, published_at,
                    COALESCE(analyst_company, ''),
                    COALESCE(price_target::text, '')
                ) DO NOTHING
                RETURNING id
                """,
                (
                    security_id,
                    published_at,
                    (r.get("analystName") or None) or None,
                    (r.get("analystCompany") or None) or None,
                    _dec(r.get("priceTarget")),
                    _dec(r.get("adjPriceTarget")),
                    _dec(r.get("priceWhenPosted")),
                    r.get("newsURL") or None,
                    r.get("newsTitle") or None,
                    r.get("newsPublisher") or None,
                    r.get("newsBaseURL") or None,
                    raw_response_id,
                ),
            )
            if cur.fetchone() is not None:
                inserted += 1
    return inserted


# --------------------------------------------------------------------------- #
# Per-security pipeline
# --------------------------------------------------------------------------- #


# Hard cap on price-target-news pagination per ticker — guards against an
# unending walk if FMP starts returning duplicate pages.
MAX_PRICE_TARGET_NEWS_PAGES = 50
PRICE_TARGET_NEWS_PAGE_SIZE = 100


def _ingest_one_security(
    conn: psycopg.Connection,
    *,
    security: SecurityRow,
    ingest_run_id: int,
    client: FMPClient,
    counts: dict[str, Any],
) -> None:
    fetched_at = datetime.now(timezone.utc)

    # 1+2. Analyst estimates — annual + quarter.
    for period in ("annual", "quarter"):
        with conn.transaction():
            fetch = fetch_analyst_estimates(
                conn,
                ticker=security.ticker,
                period=period,
                ingest_run_id=ingest_run_id,
                client=client,
            )
            counts["raw_responses"] += 1
            n = _load_analyst_estimates(
                conn,
                security_id=security.id,
                period_kind=period,
                rows=fetch.rows,
                raw_response_id=fetch.raw_response_id,
                fetched_at=fetched_at,
            )
            counts["analyst_estimates_rows"] += n

    # 3. Price target consensus.
    with conn.transaction():
        fetch = fetch_price_target_consensus(
            conn,
            ticker=security.ticker,
            ingest_run_id=ingest_run_id,
            client=client,
        )
        counts["raw_responses"] += 1
        n = _load_price_target_consensus(
            conn,
            security_id=security.id,
            rows=fetch.rows,
            raw_response_id=fetch.raw_response_id,
            fetched_at=fetched_at,
        )
        counts["price_target_consensus_rows"] += n

    # 4. Earnings surprises.
    with conn.transaction():
        fetch = fetch_earnings(
            conn,
            ticker=security.ticker,
            ingest_run_id=ingest_run_id,
            client=client,
        )
        counts["raw_responses"] += 1
        ins, upd = _load_earnings(
            conn,
            security_id=security.id,
            rows=fetch.rows,
            raw_response_id=fetch.raw_response_id,
        )
        counts["earnings_inserted"] += ins
        counts["earnings_updated"] += upd

    # 5. Analyst grades event log.
    with conn.transaction():
        fetch = fetch_grades(
            conn,
            ticker=security.ticker,
            ingest_run_id=ingest_run_id,
            client=client,
        )
        counts["raw_responses"] += 1
        n = _load_grades(
            conn,
            security_id=security.id,
            rows=fetch.rows,
            raw_response_id=fetch.raw_response_id,
        )
        counts["grades_inserted"] += n

    # 6. Price-target-news pagination.
    for page in range(MAX_PRICE_TARGET_NEWS_PAGES):
        with conn.transaction():
            fetch = fetch_price_target_news_page(
                conn,
                ticker=security.ticker,
                page=page,
                ingest_run_id=ingest_run_id,
                client=client,
                limit=PRICE_TARGET_NEWS_PAGE_SIZE,
            )
            counts["raw_responses"] += 1
            if not fetch.rows:
                break
            n = _load_price_targets(
                conn,
                security_id=security.id,
                rows=fetch.rows,
                raw_response_id=fetch.raw_response_id,
            )
            counts["price_target_events_inserted"] += n
            if len(fetch.rows) < PRICE_TARGET_NEWS_PAGE_SIZE:
                # Last page (under-full); no need to call again.
                break


def backfill_fmp_estimates(
    conn: psycopg.Connection,
    tickers: list[str],
) -> dict[str, Any]:
    """Backfill the estimates vertical for the given tickers.

    Common-stock-only — ETFs / indices have no analyst coverage. Caller
    is responsible for filtering; we error loudly on a non-common-stock
    ticker rather than silently skip.
    """
    securities = _resolve_securities(conn, tickers)
    non_stock = [s for s in securities if s.kind != "common_stock"]
    if non_stock:
        raise ValueError(
            "estimates ingest only supports common stock; got "
            f"{[(s.ticker, s.kind) for s in non_stock]}"
        )

    run_id = open_run(
        conn,
        run_kind="manual",
        vendor="fmp",
        ticker_scope=[s.ticker for s in securities],
    )
    client = FMPClient()

    counts: dict[str, Any] = {
        "raw_responses": 0,
        "analyst_estimates_rows": 0,
        "price_target_consensus_rows": 0,
        "earnings_inserted": 0,
        "earnings_updated": 0,
        "grades_inserted": 0,
        "price_target_events_inserted": 0,
    }

    try:
        for security in securities:
            _ingest_one_security(
                conn,
                security=security,
                ingest_run_id=run_id,
                client=client,
                counts=counts,
            )
    except Exception as e:
        close_failed(
            conn,
            run_id,
            error_message=str(e),
            error_details={"kind": type(e).__name__},
        )
        raise

    close_succeeded(conn, run_id, counts=counts)
    counts["ingest_run_id"] = run_id
    return counts
