"""Retrieval helpers for the estimates vertical.

Powers the /ask tools `read_consensus`, `read_target_gap`,
`read_surprise_history`, `recent_analyst_actions`. Reads from
`analyst_estimates`, `price_target_consensus`, `earnings_surprises`,
`analyst_grades`, `analyst_price_targets`.

Design:
  - `read_consensus`           — forward + 1 most-recent past period per kind
  - `read_target_gap`          — current price vs target consensus snapshot
  - `read_surprise_history`    — last N quarterly announcements
  - `recent_analyst_actions`   — combined event log over a window
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import psycopg


# --------------------------------------------------------------------------- #
# Consensus
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class ConsensusRow:
    security_id: int
    ticker: str
    period_kind: str        # 'annual' | 'quarter'
    period_end: date
    revenue_avg: Decimal | None
    revenue_low: Decimal | None
    revenue_high: Decimal | None
    eps_avg: Decimal | None
    eps_low: Decimal | None
    eps_high: Decimal | None
    ebitda_avg: Decimal | None
    ebit_avg: Decimal | None         # forward operating income; often unreliable, see steward
    ebit_low: Decimal | None
    ebit_high: Decimal | None
    net_income_avg: Decimal | None
    num_analysts_revenue: int | None
    num_analysts_eps: int | None
    fetched_at: datetime
    is_forward: bool


def read_consensus(
    conn: psycopg.Connection,
    *,
    ticker: str,
    period_kind: str = "quarter",
    n_forward: int = 4,
    n_past: int = 1,
) -> list[ConsensusRow]:
    """Forward + most-recent past consensus rows for one ticker, period_kind.

    Returns up to ``n_forward`` upcoming periods (period_end >= today)
    and ``n_past`` most recent past periods (period_end < today).
    Sorted ascending by period_end. ``period_kind`` is 'annual' or 'quarter'.
    """
    if period_kind not in ("annual", "quarter"):
        raise ValueError(
            f"period_kind must be 'annual' or 'quarter', got {period_kind!r}"
        )

    today = date.today()
    sql = """
    WITH forward AS (
        SELECT ae.*, s.ticker
        FROM analyst_estimates ae
        JOIN securities s ON s.id = ae.security_id
        WHERE s.ticker = %s AND s.status = 'active'
          AND ae.period_kind = %s
          AND ae.period_end >= %s
        ORDER BY ae.period_end ASC
        LIMIT %s
    ),
    past AS (
        SELECT ae.*, s.ticker
        FROM analyst_estimates ae
        JOIN securities s ON s.id = ae.security_id
        WHERE s.ticker = %s AND s.status = 'active'
          AND ae.period_kind = %s
          AND ae.period_end < %s
        ORDER BY ae.period_end DESC
        LIMIT %s
    )
    SELECT * FROM past
    UNION ALL
    SELECT * FROM forward
    ORDER BY period_end ASC;
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                ticker.upper(), period_kind, today, n_forward,
                ticker.upper(), period_kind, today, n_past,
            ),
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]

    out: list[ConsensusRow] = []
    for r in rows:
        d = dict(zip(cols, r))
        out.append(
            ConsensusRow(
                security_id=d["security_id"],
                ticker=d["ticker"],
                period_kind=d["period_kind"],
                period_end=d["period_end"],
                revenue_avg=d.get("revenue_avg"),
                revenue_low=d.get("revenue_low"),
                revenue_high=d.get("revenue_high"),
                eps_avg=d.get("eps_avg"),
                eps_low=d.get("eps_low"),
                eps_high=d.get("eps_high"),
                ebitda_avg=d.get("ebitda_avg"),
                ebit_avg=d.get("ebit_avg"),
                ebit_low=d.get("ebit_low"),
                ebit_high=d.get("ebit_high"),
                net_income_avg=d.get("net_income_avg"),
                num_analysts_revenue=d.get("num_analysts_revenue"),
                num_analysts_eps=d.get("num_analysts_eps"),
                fetched_at=d["fetched_at"],
                is_forward=d["period_end"] >= today,
            )
        )
    return out


# --------------------------------------------------------------------------- #
# Estimate warnings (steward findings)
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class EstimateWarning:
    finding_id: int
    source_check: str               # 'forward_estimate_consistency' | 'earnings_surprise_sanity'
    period_kind: str | None         # 'annual' | 'quarter' | None for non-period-scoped
    period_end: date | None
    severity: str
    summary: str


def read_estimate_warnings(
    conn: psycopg.Connection,
    *,
    ticker: str,
) -> list[EstimateWarning]:
    """Open steward findings for the estimates vertical.

    Surfaces forward_estimate_consistency and earnings_surprise_sanity
    warnings so the synthesizer can flag unreliable consensus values
    alongside the raw numbers. Without this, /ask reads consensus blind
    to what the dashboard surfaces via the same checks.
    """
    sql = """
    SELECT id, source_check, evidence, severity, summary
    FROM data_quality_findings
    WHERE ticker = %s
      AND vertical = 'estimates'
      AND status = 'open'
      AND source_check IN ('forward_estimate_consistency', 'earnings_surprise_sanity')
    ORDER BY source_check, id;
    """
    out: list[EstimateWarning] = []
    with conn.cursor() as cur:
        cur.execute(sql, (ticker.upper(),))
        for fid, source_check, evidence, severity, summary in cur.fetchall():
            ev = evidence or {}
            pk = ev.get("period_kind")
            pe_raw = ev.get("period_end")
            pe: date | None = None
            if pe_raw:
                try:
                    pe = date.fromisoformat(pe_raw)
                except (TypeError, ValueError):
                    pe = None
            out.append(
                EstimateWarning(
                    finding_id=fid,
                    source_check=source_check,
                    period_kind=pk,
                    period_end=pe,
                    severity=severity,
                    summary=summary,
                )
            )
    return out


# --------------------------------------------------------------------------- #
# Target gap
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class TargetGap:
    security_id: int
    ticker: str
    target_high: Decimal | None
    target_low: Decimal | None
    target_median: Decimal | None
    target_consensus: Decimal | None
    current_close: Decimal | None
    current_close_date: date | None
    upside_to_consensus_pct: float | None    # (target_consensus - close) / close * 100
    fetched_at: datetime


def read_target_gap(
    conn: psycopg.Connection,
    *,
    ticker: str,
    as_of: date | None = None,
) -> TargetGap | None:
    """Current price vs analyst consensus target, with upside %.

    `as_of` defaults to the latest available trading day for this ticker.
    Returns None if no consensus snapshot exists.
    """
    sql = """
    SELECT s.id, s.ticker,
           ptc.target_high, ptc.target_low, ptc.target_median, ptc.target_consensus,
           ptc.fetched_at,
           pd.close, pd.date AS close_date
    FROM securities s
    LEFT JOIN price_target_consensus ptc ON ptc.security_id = s.id
    LEFT JOIN LATERAL (
        SELECT pd.close, pd.date
        FROM prices_daily pd
        WHERE pd.security_id = s.id
          AND (%s::date IS NULL OR pd.date <= %s::date)
        ORDER BY pd.date DESC
        LIMIT 1
    ) pd ON TRUE
    WHERE s.ticker = %s AND s.status = 'active'
    LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (as_of, as_of, ticker.upper()))
        row = cur.fetchone()
    if row is None:
        return None
    (
        security_id, t,
        target_high, target_low, target_median, target_consensus,
        fetched_at,
        close, close_date,
    ) = row
    if target_consensus is None and target_high is None:
        return None

    upside: float | None = None
    if close is not None and target_consensus is not None and close != 0:
        upside = float((target_consensus - close) / close) * 100.0

    return TargetGap(
        security_id=security_id,
        ticker=t,
        target_high=target_high,
        target_low=target_low,
        target_median=target_median,
        target_consensus=target_consensus,
        current_close=close,
        current_close_date=close_date,
        upside_to_consensus_pct=upside,
        fetched_at=fetched_at,
    )


# --------------------------------------------------------------------------- #
# Surprise history
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class SurpriseRow:
    security_id: int
    ticker: str
    announcement_date: date
    eps_actual: Decimal | None
    eps_estimated: Decimal | None
    eps_surprise_pct: float | None       # (actual - estimated) / |estimated| * 100
    revenue_actual: Decimal | None
    revenue_estimated: Decimal | None
    revenue_surprise_pct: float | None


def read_surprise_history(
    conn: psycopg.Connection,
    *,
    ticker: str,
    n: int = 8,
) -> list[SurpriseRow]:
    """Last N announcements for one ticker, newest first.

    Filters to rows with at least one non-null actual (skips upcoming).
    """
    sql = """
    SELECT es.security_id, s.ticker, es.announcement_date,
           es.eps_actual, es.eps_estimated,
           es.revenue_actual, es.revenue_estimated
    FROM earnings_surprises es
    JOIN securities s ON s.id = es.security_id
    WHERE s.ticker = %s AND s.status = 'active'
      AND (es.eps_actual IS NOT NULL OR es.revenue_actual IS NOT NULL)
    ORDER BY es.announcement_date DESC
    LIMIT %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (ticker.upper(), n))
        rows = cur.fetchall()

    out: list[SurpriseRow] = []
    for security_id, t, announcement_date, eps_a, eps_e, rev_a, rev_e in rows:
        eps_pct: float | None = None
        if eps_a is not None and eps_e is not None and eps_e != 0:
            eps_pct = float((eps_a - eps_e) / abs(eps_e)) * 100.0
        rev_pct: float | None = None
        if rev_a is not None and rev_e is not None and rev_e != 0:
            rev_pct = float((rev_a - rev_e) / abs(rev_e)) * 100.0
        out.append(
            SurpriseRow(
                security_id=security_id,
                ticker=t,
                announcement_date=announcement_date,
                eps_actual=eps_a,
                eps_estimated=eps_e,
                eps_surprise_pct=eps_pct,
                revenue_actual=rev_a,
                revenue_estimated=rev_e,
                revenue_surprise_pct=rev_pct,
            )
        )
    return out


# --------------------------------------------------------------------------- #
# Analyst actions (combined event log)
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class AnalystAction:
    kind: str                            # 'grade' | 'price_target'
    ticker: str
    security_id: int
    when: datetime                       # action_date or published_at, UTC
    firm: str | None
    analyst_name: str | None
    # grade fields
    previous_grade: str | None
    new_grade: str | None
    action: str | None                   # 'upgrade' | 'downgrade' | 'maintain'
    # price target fields
    price_target: Decimal | None
    adj_price_target: Decimal | None
    price_when_posted: Decimal | None
    news_url: str | None
    news_title: str | None
    citation: str                        # G:<id> or A:<id>


def recent_analyst_actions(
    conn: psycopg.Connection,
    *,
    ticker: str,
    days: int = 90,
    limit: int = 50,
) -> list[AnalystAction]:
    """Combined event log of grade + price-target events for one ticker, newest first."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    sql_grades = """
    SELECT 'grade'::text AS kind, ag.id, s.ticker, s.id AS security_id,
           ag.action_date, ag.grading_company, NULL::text AS analyst_name,
           ag.previous_grade, ag.new_grade, ag.action,
           NULL::numeric, NULL::numeric, NULL::numeric,
           NULL::text, NULL::text
    FROM analyst_grades ag
    JOIN securities s ON s.id = ag.security_id
    WHERE s.ticker = %s AND s.status = 'active'
      AND ag.action_date >= %s::date;
    """
    sql_targets = """
    SELECT 'price_target'::text AS kind, apt.id, s.ticker, s.id AS security_id,
           apt.published_at::date, apt.analyst_company, apt.analyst_name,
           NULL::text, NULL::text, NULL::text,
           apt.price_target, apt.adj_price_target, apt.price_when_posted,
           apt.news_url, apt.news_title
    FROM analyst_price_targets apt
    JOIN securities s ON s.id = apt.security_id
    WHERE s.ticker = %s AND s.status = 'active'
      AND apt.published_at >= %s;
    """
    with conn.cursor() as cur:
        cur.execute(sql_grades, (ticker.upper(), cutoff.date()))
        grade_rows = cur.fetchall()
        cur.execute(sql_targets, (ticker.upper(), cutoff))
        target_rows = cur.fetchall()

    out: list[AnalystAction] = []
    for kind, row_id, t, sid, when_date, firm, analyst, prev, new, act, pt, apt, pwp, url, title in (
        grade_rows + target_rows
    ):
        # action_date for grades is plain date; published_at::date for targets above.
        # Promote both to UTC datetime for sorting.
        when = datetime(when_date.year, when_date.month, when_date.day, tzinfo=timezone.utc)
        cite_prefix = "G" if kind == "grade" else "A"
        out.append(
            AnalystAction(
                kind=kind,
                ticker=t,
                security_id=sid,
                when=when,
                firm=firm,
                analyst_name=analyst,
                previous_grade=prev,
                new_grade=new,
                action=act,
                price_target=pt,
                adj_price_target=apt,
                price_when_posted=pwp,
                news_url=url,
                news_title=title,
                citation=f"{cite_prefix}:{row_id}",
            )
        )
    out.sort(key=lambda a: a.when, reverse=True)
    return out[:limit]
