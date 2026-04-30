"""Coverage matrix queries.

Pure SQL helpers that summarize what data Arrow has per (ticker,
vertical). Two consumers in V1: the dashboard ``/coverage`` matrix
(one row per ticker in `companies`) and the per-ticker
``/coverage/{ticker}`` detail page.

V1.2 design (commit history): the previous `coverage_membership` table
was dropped. Every ticker in `companies` is automatically tracked —
"in the database" IS the universe. This removes the conceptual
distinction between "data we have" and "data we care about" that
the operator never wanted in the first place.

Verticals (V1):
  - financials       income_statement, balance_sheet, cash_flow
                     (the IS/BS/CF baseline from FMP backfill)
  - segments         financial_facts where statement = 'segment'
  - employees        financial_facts where statement = 'metrics'
                     and concept = 'total_employees'
  - sec_qual         artifacts where artifact_type IN ('10k', '10q')
  - press_release    artifacts where artifact_type = 'press_release'
  - transcript       artifacts where artifact_type = 'transcript'
  - prices           prices_daily joined to securities (common_stock only;
                     ETF/index securities do not roll up under a company)
  - estimates        analyst_estimates joined to securities. `latest` is
                     MAX(fetched_at) — the refresh timestamp — so the
                     `recency` expectation evaluates "are estimates fresh?"
                     not "how far forward does coverage reach?"

Two patterns recur in the queries below:
  - ``superseded_at IS NULL`` everywhere — only current rows count
  - ``GROUP BY company_id`` so the matrix joins cleanly back to
    ``companies``
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import psycopg


#: Stable ordering for the matrix columns. Matches docs/architecture/steward.md.
VERTICALS: tuple[str, ...] = (
    "financials",
    "segments",
    "employees",
    "sec_qual",
    "press_release",
    "transcript",
    "prices",
    "estimates",
)


@dataclass(frozen=True)
class VerticalCoverage:
    """Per-vertical summary for one ticker."""

    vertical: str
    row_count: int
    period_count: int          # distinct periods (for facts: distinct period_end;
                               # for artifacts: distinct fiscal_period_key)
    earliest: Any | None       # earliest period_end / published_at
    latest: Any | None         # latest period_end / published_at

    @property
    def has_data(self) -> bool:
        return self.row_count > 0


@dataclass(frozen=True)
class CoverageRow:
    """One row in the coverage matrix.

    No tier or membership fields — V1.2 collapsed coverage to "every
    ticker in companies is tracked." The `seeded_at` field reflects
    when the company was first added to `companies` (typically when
    `ingest_company.py` first ran for it).
    """

    company_id: int
    ticker: str
    name: str
    seeded_at: Any
    by_vertical: dict[str, VerticalCoverage]


def compute_coverage_matrix(conn: psycopg.Connection) -> list[CoverageRow]:
    """Return one CoverageRow per ticker in `companies`, with per-vertical
    summaries.

    Every ticker we have data for is in scope — no separate membership
    step. To remove a ticker from the steward's attention you'd delete
    it from `companies` (which requires deleting its data first because
    of FK constraints), or suppress its findings.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, ticker, name, created_at
            FROM companies
            ORDER BY ticker;
            """
        )
        members = cur.fetchall()

    if not members:
        return []

    company_ids = [m[0] for m in members]

    # Aggregate all five verticals in five queries (one per vertical) so
    # each can use the index that's right for its base table. Could be
    # combined with UNION ALL but that gives up index access. The five
    # are independent, fast, and total < 100ms even on large datasets.
    by_vertical_per_company = _vertical_aggregates(conn, company_ids)

    rows: list[CoverageRow] = []
    for company_id, ticker, name, seeded_at in members:
        per_vertical: dict[str, VerticalCoverage] = {}
        for vertical in VERTICALS:
            agg = by_vertical_per_company[vertical].get(
                company_id,
                VerticalCoverage(vertical=vertical, row_count=0,
                                 period_count=0, earliest=None, latest=None),
            )
            per_vertical[vertical] = agg
        rows.append(CoverageRow(
            company_id=company_id,
            ticker=ticker,
            name=name,
            seeded_at=seeded_at,
            by_vertical=per_vertical,
        ))
    return rows


def compute_ticker_coverage(
    conn: psycopg.Connection, ticker: str
) -> tuple[CoverageRow, dict[str, list[dict[str, Any]]]] | None:
    """Detailed per-ticker coverage. Returns (row_summary, per_vertical_periods)
    or None if the ticker is not in `companies`.

    ``per_vertical_periods`` maps vertical → list of period summaries
    (one row per period_end / fiscal_period_key with a row count).
    """
    ticker = ticker.upper()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, ticker, name, created_at
            FROM companies
            WHERE ticker = %s;
            """,
            (ticker,),
        )
        m = cur.fetchone()
        if m is None:
            return None
    company_id, _ticker, name, seeded_at = m

    aggs = _vertical_aggregates(conn, [company_id])
    by_vertical: dict[str, VerticalCoverage] = {}
    for vertical in VERTICALS:
        by_vertical[vertical] = aggs[vertical].get(
            company_id,
            VerticalCoverage(vertical=vertical, row_count=0,
                             period_count=0, earliest=None, latest=None),
        )

    summary = CoverageRow(
        company_id=company_id, ticker=ticker, name=name,
        seeded_at=seeded_at, by_vertical=by_vertical,
    )

    per_vertical_periods: dict[str, list[dict[str, Any]]] = {}
    with conn.cursor() as cur:
        # Financials per period_end + period_type
        cur.execute(
            """
            SELECT period_end, period_type,
                   fiscal_year, fiscal_quarter, fiscal_period_label,
                   COUNT(*) AS rows
            FROM financial_facts
            WHERE company_id = %s
              AND statement IN ('income_statement','balance_sheet','cash_flow')
              AND superseded_at IS NULL
            GROUP BY period_end, period_type, fiscal_year, fiscal_quarter,
                     fiscal_period_label
            ORDER BY period_end DESC;
            """,
            (company_id,),
        )
        per_vertical_periods["financials"] = _rows_as_dicts(cur)

        cur.execute(
            """
            SELECT period_end, period_type,
                   fiscal_year, fiscal_quarter, fiscal_period_label,
                   COUNT(*) AS rows
            FROM financial_facts
            WHERE company_id = %s
              AND statement = 'segment'
              AND superseded_at IS NULL
            GROUP BY period_end, period_type, fiscal_year, fiscal_quarter,
                     fiscal_period_label
            ORDER BY period_end DESC;
            """,
            (company_id,),
        )
        per_vertical_periods["segments"] = _rows_as_dicts(cur)

        cur.execute(
            """
            SELECT period_end, period_type,
                   fiscal_year, fiscal_quarter, fiscal_period_label,
                   COUNT(*) AS rows
            FROM financial_facts
            WHERE company_id = %s
              AND statement = 'metrics' AND concept = 'total_employees'
              AND superseded_at IS NULL
            GROUP BY period_end, period_type, fiscal_year, fiscal_quarter,
                     fiscal_period_label
            ORDER BY period_end DESC;
            """,
            (company_id,),
        )
        per_vertical_periods["employees"] = _rows_as_dicts(cur)

        cur.execute(
            """
            SELECT artifact_type, fiscal_period_key, period_end,
                   accession_number, published_at
            FROM artifacts
            WHERE company_id = %s
              AND artifact_type IN ('10k','10q')
              AND superseded_at IS NULL
            ORDER BY published_at DESC NULLS LAST;
            """,
            (company_id,),
        )
        per_vertical_periods["sec_qual"] = _rows_as_dicts(cur)

        cur.execute(
            """
            SELECT artifact_type, fiscal_period_key, period_end,
                   accession_number, published_at
            FROM artifacts
            WHERE company_id = %s
              AND artifact_type = 'press_release'
              AND superseded_at IS NULL
            ORDER BY published_at DESC NULLS LAST;
            """,
            (company_id,),
        )
        per_vertical_periods["press_release"] = _rows_as_dicts(cur)

        cur.execute(
            """
            SELECT artifact_type, fiscal_period_key, period_end,
                   source_document_id, published_at
            FROM artifacts
            WHERE company_id = %s
              AND artifact_type = 'transcript'
              AND superseded_at IS NULL
            ORDER BY published_at DESC NULLS LAST;
            """,
            (company_id,),
        )
        per_vertical_periods["transcript"] = _rows_as_dicts(cur)

        # Prices: per-month rollup so the per-ticker detail page isn't
        # 2000+ rows. The matrix-level summary still has total / first / last.
        cur.execute(
            """
            SELECT date_trunc('month', pd.date)::date AS period,
                   COUNT(*) AS rows,
                   MIN(pd.date) AS earliest,
                   MAX(pd.date) AS latest
            FROM prices_daily pd
            JOIN securities s ON s.id = pd.security_id
            WHERE s.company_id = %s AND s.status = 'active'
            GROUP BY date_trunc('month', pd.date)
            ORDER BY period DESC;
            """,
            (company_id,),
        )
        per_vertical_periods["prices"] = _rows_as_dicts(cur)

        # Estimates per fiscal period (annual + quarter), latest first.
        cur.execute(
            """
            SELECT ae.period_kind, ae.period_end,
                   ae.eps_avg, ae.revenue_avg, ae.num_analysts_eps,
                   ae.fetched_at
            FROM analyst_estimates ae
            JOIN securities s ON s.id = ae.security_id
            WHERE s.company_id = %s AND s.status = 'active'
            ORDER BY ae.period_end DESC, ae.period_kind;
            """,
            (company_id,),
        )
        per_vertical_periods["estimates"] = _rows_as_dicts(cur)

    return summary, per_vertical_periods


# ---------------------------------------------------------------------------
# internal
# ---------------------------------------------------------------------------


def _vertical_aggregates(
    conn: psycopg.Connection, company_ids: list[int]
) -> dict[str, dict[int, VerticalCoverage]]:
    """For each vertical, return {company_id → VerticalCoverage}.

    Five SELECTs (one per vertical), each scoped to the given
    company_ids and grouped by company_id. Idiomatically simple,
    each query uses indexes on its base table.
    """
    out: dict[str, dict[int, VerticalCoverage]] = {v: {} for v in VERTICALS}

    with conn.cursor() as cur:
        # Financials
        cur.execute(
            """
            SELECT company_id,
                   COUNT(*) AS rows,
                   COUNT(DISTINCT period_end) AS periods,
                   MIN(period_end) AS earliest,
                   MAX(period_end) AS latest
            FROM financial_facts
            WHERE company_id = ANY(%s)
              AND statement IN ('income_statement','balance_sheet','cash_flow')
              AND superseded_at IS NULL
            GROUP BY company_id;
            """,
            (company_ids,),
        )
        for cid, rows, periods, earliest, latest in cur.fetchall():
            out["financials"][cid] = VerticalCoverage(
                vertical="financials", row_count=rows, period_count=periods,
                earliest=earliest, latest=latest,
            )

        # Segments
        cur.execute(
            """
            SELECT company_id, COUNT(*), COUNT(DISTINCT period_end),
                   MIN(period_end), MAX(period_end)
            FROM financial_facts
            WHERE company_id = ANY(%s)
              AND statement = 'segment'
              AND superseded_at IS NULL
            GROUP BY company_id;
            """,
            (company_ids,),
        )
        for cid, rows, periods, earliest, latest in cur.fetchall():
            out["segments"][cid] = VerticalCoverage(
                vertical="segments", row_count=rows, period_count=periods,
                earliest=earliest, latest=latest,
            )

        # Employees
        cur.execute(
            """
            SELECT company_id, COUNT(*), COUNT(DISTINCT period_end),
                   MIN(period_end), MAX(period_end)
            FROM financial_facts
            WHERE company_id = ANY(%s)
              AND statement = 'metrics' AND concept = 'total_employees'
              AND superseded_at IS NULL
            GROUP BY company_id;
            """,
            (company_ids,),
        )
        for cid, rows, periods, earliest, latest in cur.fetchall():
            out["employees"][cid] = VerticalCoverage(
                vertical="employees", row_count=rows, period_count=periods,
                earliest=earliest, latest=latest,
            )

        # SEC qualitative (10-K/10-Q artifacts)
        cur.execute(
            """
            SELECT company_id, COUNT(*),
                   COUNT(DISTINCT fiscal_period_key),
                   MIN(published_at), MAX(published_at)
            FROM artifacts
            WHERE company_id = ANY(%s)
              AND artifact_type IN ('10k','10q')
              AND superseded_at IS NULL
            GROUP BY company_id;
            """,
            (company_ids,),
        )
        for cid, rows, periods, earliest, latest in cur.fetchall():
            out["sec_qual"][cid] = VerticalCoverage(
                vertical="sec_qual", row_count=rows, period_count=periods,
                earliest=earliest, latest=latest,
            )

        # Press releases
        cur.execute(
            """
            SELECT company_id, COUNT(*),
                   COUNT(DISTINCT fiscal_period_key),
                   MIN(published_at), MAX(published_at)
            FROM artifacts
            WHERE company_id = ANY(%s)
              AND artifact_type = 'press_release'
              AND superseded_at IS NULL
            GROUP BY company_id;
            """,
            (company_ids,),
        )
        for cid, rows, periods, earliest, latest in cur.fetchall():
            out["press_release"][cid] = VerticalCoverage(
                vertical="press_release", row_count=rows, period_count=periods,
                earliest=earliest, latest=latest,
            )

        # FMP earnings-call transcripts
        cur.execute(
            """
            SELECT company_id, COUNT(*),
                   COUNT(DISTINCT fiscal_period_key),
                   MIN(published_at), MAX(published_at)
            FROM artifacts
            WHERE company_id = ANY(%s)
              AND artifact_type = 'transcript'
              AND superseded_at IS NULL
            GROUP BY company_id;
            """,
            (company_ids,),
        )
        for cid, rows, periods, earliest, latest in cur.fetchall():
            out["transcript"][cid] = VerticalCoverage(
                vertical="transcript", row_count=rows, period_count=periods,
                earliest=earliest, latest=latest,
            )

        # Daily prices (joined through securities to companies). ETFs/indices
        # don't roll up under a company so they're absent from this matrix
        # — they're covered by the dedicated steward checks instead.
        cur.execute(
            """
            SELECT s.company_id,
                   COUNT(*),
                   COUNT(DISTINCT pd.date),
                   MIN(pd.date), MAX(pd.date)
            FROM prices_daily pd
            JOIN securities s ON s.id = pd.security_id
            WHERE s.company_id = ANY(%s) AND s.status = 'active'
            GROUP BY s.company_id;
            """,
            (company_ids,),
        )
        for cid, rows, periods, earliest, latest in cur.fetchall():
            out["prices"][cid] = VerticalCoverage(
                vertical="prices", row_count=rows, period_count=periods,
                earliest=earliest, latest=latest,
            )

        # Analyst estimates (joined through securities to companies).
        # `latest` is MAX(fetched_at) — the refresh timestamp — because
        # period_end is forward-looking (e.g. FY2031) and isn't a freshness
        # signal. ETFs/indices have no analyst coverage and are absent here.
        cur.execute(
            """
            SELECT s.company_id,
                   COUNT(*),
                   COUNT(DISTINCT ae.period_end),
                   MIN(ae.period_end), MAX(ae.fetched_at)
            FROM analyst_estimates ae
            JOIN securities s ON s.id = ae.security_id
            WHERE s.company_id = ANY(%s) AND s.status = 'active'
            GROUP BY s.company_id;
            """,
            (company_ids,),
        )
        for cid, rows, periods, earliest, latest in cur.fetchall():
            out["estimates"][cid] = VerticalCoverage(
                vertical="estimates", row_count=rows, period_count=periods,
                earliest=earliest, latest=latest,
            )

    return out


def _rows_as_dicts(cur: psycopg.Cursor) -> list[dict[str, Any]]:
    cols = [d[0] for d in cur.description] if cur.description else []
    return [dict(zip(cols, r)) for r in cur.fetchall()]
