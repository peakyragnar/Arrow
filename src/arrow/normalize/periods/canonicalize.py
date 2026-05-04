"""Canonicalize ``financial_facts.period_end`` across endpoints and Q4-vs-annual.

Two passes addressing FMP's date-stamping inconsistencies for
52/53-week filers:

* ``canonicalize_cross_endpoint`` (Phase 2) — snap IS/BS/CF rows to
  the trusted (employees/segments) ``period_end`` for the same fiscal
  period. Empirical: FN/Fabrinet (FYE = last Friday of June) had IS/CF
  Q4 stamped at calendar Jun-30 while BS+segments used the real
  Friday-close date; this collision aborts ``ingest_transcripts``
  via ``AmbiguousFiscalAnchor``.

* ``canonicalize_q4_to_annual`` (Phase 1) — snap Q4 quarterly rows to
  the FY annual ``period_end`` within the same statement when they
  disagree.

Both passes are idempotent. They scope to a ticker list when one is
provided (typical orchestrator use) or run globally otherwise.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import psycopg

from arrow.ingest.common.runs import close_succeeded, open_run


CROSS_ENDPOINT_TRUSTED = ("fmp-employees-v1", "fmp-segments-v1")
CROSS_ENDPOINT_TARGETS = ("fmp-is-v1", "fmp-bs-v1", "fmp-cf-v1")
Q4_TARGETS = ("fmp-is-v1", "fmp-bs-v1", "fmp-cf-v1")


@dataclass
class CanonicalizationResult:
    rows_processed: int
    groups: int
    ingest_run_id: int | None


def _ticker_clause(table_alias: str, tickers: Iterable[str] | None) -> tuple[str, list]:
    """Return ('AND <alias>.company_id IN (SELECT id FROM companies WHERE ticker = ANY(%s))', [list])
    or ('', []) when tickers is None/empty.
    """
    if not tickers:
        return "", []
    upper = [t.upper() for t in tickers]
    return (
        f" AND {table_alias}.company_id IN (SELECT id FROM companies WHERE ticker = ANY(%s))",
        [upper],
    )


def _cross_endpoint_sql(
    tickers: Iterable[str] | None,
) -> tuple[str, str, list]:
    """Build (preview_sql, update_sql, params) for cross-endpoint canonicalization."""
    ff_clause, ticker_params = _ticker_clause("ff", tickers)
    # Both queries reference ff in two filter positions: inside the
    # trusted_dates CTE and in the outer SELECT/UPDATE. We append the
    # clause in both places.
    preview_sql = f"""
WITH trusted_dates AS (
  SELECT company_id, fiscal_year, fiscal_quarter, period_type, period_end
  FROM financial_facts ff
  WHERE superseded_at IS NULL
    AND extraction_version = ANY(%s){ff_clause}
  GROUP BY company_id, fiscal_year, fiscal_quarter, period_type, period_end
),
canonical AS (
  SELECT company_id, fiscal_year, fiscal_quarter, period_type,
         MIN(period_end) AS canon_pe
  FROM trusted_dates
  GROUP BY company_id, fiscal_year, fiscal_quarter, period_type
  HAVING COUNT(DISTINCT period_end) = 1
)
SELECT c.ticker, ff.fiscal_year, ff.fiscal_quarter, ff.period_type,
       ff.statement, ff.extraction_version,
       ff.period_end AS old_pe, can.canon_pe AS new_pe,
       COUNT(*) AS rows
FROM financial_facts ff
JOIN canonical can
  ON can.company_id = ff.company_id
 AND can.fiscal_year = ff.fiscal_year
 AND COALESCE(can.fiscal_quarter, -1) = COALESCE(ff.fiscal_quarter, -1)
 AND can.period_type = ff.period_type
JOIN companies c ON c.id = ff.company_id
WHERE ff.superseded_at IS NULL
  AND ff.dimension_type IS NULL
  AND ff.extraction_version = ANY(%s)
  AND ff.period_end <> can.canon_pe{ff_clause}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
ORDER BY 1, 2, 3, 4, 5;
"""
    update_sql = f"""
WITH trusted_dates AS (
  SELECT company_id, fiscal_year, fiscal_quarter, period_type, period_end
  FROM financial_facts ff
  WHERE superseded_at IS NULL
    AND extraction_version = ANY(%s){ff_clause}
  GROUP BY company_id, fiscal_year, fiscal_quarter, period_type, period_end
),
canonical AS (
  SELECT company_id, fiscal_year, fiscal_quarter, period_type,
         MIN(period_end) AS canon_pe
  FROM trusted_dates
  GROUP BY company_id, fiscal_year, fiscal_quarter, period_type
  HAVING COUNT(DISTINCT period_end) = 1
),
mismatched AS (
  SELECT ff.id, can.canon_pe AS new_pe
  FROM financial_facts ff
  JOIN canonical can
    ON can.company_id = ff.company_id
   AND can.fiscal_year = ff.fiscal_year
   AND COALESCE(can.fiscal_quarter, -1) = COALESCE(ff.fiscal_quarter, -1)
   AND can.period_type = ff.period_type
  WHERE ff.superseded_at IS NULL
    AND ff.dimension_type IS NULL
    AND ff.extraction_version = ANY(%s)
    AND ff.period_end <> can.canon_pe{ff_clause}
)
UPDATE financial_facts ff
SET period_end            = m.new_pe,
    calendar_year         = EXTRACT(YEAR FROM m.new_pe)::int,
    calendar_quarter      = EXTRACT(QUARTER FROM m.new_pe)::int,
    calendar_period_label = 'CY' || EXTRACT(YEAR FROM m.new_pe)::int
                            || ' Q' || EXTRACT(QUARTER FROM m.new_pe)::int
FROM mismatched m
WHERE ff.id = m.id
RETURNING ff.id;
"""
    # Params: trusted_versions + ticker (CTE), target_versions + ticker (outer)
    params = [list(CROSS_ENDPOINT_TRUSTED), *ticker_params, list(CROSS_ENDPOINT_TARGETS), *ticker_params]
    return preview_sql, update_sql, params


def _q4_sql(tickers: Iterable[str] | None) -> tuple[str, str, list]:
    """Build (preview_sql, update_sql, params) for Q4-vs-annual canonicalization."""
    ann_clause, tp1 = _ticker_clause("ff", tickers)
    q_clause, tp2 = _ticker_clause("q", tickers)
    preview_sql = f"""
WITH annual_pe AS (
  SELECT company_id, fiscal_year, statement, extraction_version,
         period_end AS fy_pe
  FROM financial_facts ff
  WHERE period_type = 'annual'
    AND superseded_at IS NULL
    AND dimension_type IS NULL
    AND extraction_version = ANY(%s){ann_clause}
  GROUP BY company_id, fiscal_year, statement, extraction_version, period_end
)
SELECT c.ticker, q.fiscal_year, q.statement,
       q.period_end AS old_pe, a.fy_pe AS new_pe,
       COUNT(*) AS rows
FROM financial_facts q
JOIN annual_pe a
  ON a.company_id = q.company_id
 AND a.fiscal_year = q.fiscal_year
 AND a.statement = q.statement
 AND a.extraction_version = q.extraction_version
JOIN companies c ON c.id = q.company_id
WHERE q.period_type = 'quarter'
  AND q.fiscal_quarter = 4
  AND q.superseded_at IS NULL
  AND q.dimension_type IS NULL
  AND q.extraction_version = ANY(%s)
  AND q.period_end <> a.fy_pe{q_clause}
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3;
"""
    update_sql = f"""
WITH annual_pe AS (
  SELECT company_id, fiscal_year, statement, extraction_version,
         period_end AS fy_pe
  FROM financial_facts ff
  WHERE period_type = 'annual'
    AND superseded_at IS NULL
    AND dimension_type IS NULL
    AND extraction_version = ANY(%s){ann_clause}
  GROUP BY company_id, fiscal_year, statement, extraction_version, period_end
),
mismatched AS (
  SELECT q.id, a.fy_pe AS new_pe
  FROM financial_facts q
  JOIN annual_pe a
    ON a.company_id = q.company_id
   AND a.fiscal_year = q.fiscal_year
   AND a.statement = q.statement
   AND a.extraction_version = q.extraction_version
  WHERE q.period_type = 'quarter'
    AND q.fiscal_quarter = 4
    AND q.superseded_at IS NULL
    AND q.dimension_type IS NULL
    AND q.extraction_version = ANY(%s)
    AND q.period_end <> a.fy_pe{q_clause}
)
UPDATE financial_facts ff
SET period_end = m.new_pe
FROM mismatched m
WHERE ff.id = m.id
RETURNING ff.id;
"""
    params = [list(Q4_TARGETS), *tp1, list(Q4_TARGETS), *tp2]
    return preview_sql, update_sql, params


def canonicalize_cross_endpoint(
    conn: psycopg.Connection,
    *,
    tickers: Iterable[str] | None = None,
    apply: bool = True,
    actor: str = "operator",
) -> CanonicalizationResult:
    """Snap IS/BS/CF period_end to trusted (employees/segments) dates."""
    preview_sql, update_sql, params = _cross_endpoint_sql(tickers)

    with conn.cursor() as cur:
        cur.execute(preview_sql, params)
        preview = cur.fetchall()

        groups = len(preview)
        rows_to_update = sum(row[8] for row in preview)
        if not preview:
            return CanonicalizationResult(rows_processed=0, groups=0, ingest_run_id=None)

        if not apply:
            return CanonicalizationResult(
                rows_processed=rows_to_update, groups=groups, ingest_run_id=None
            )

        tickers_in_scope = sorted({row[0] for row in preview})
        run_id = open_run(
            conn,
            run_kind="manual",
            vendor="arrow",
            ticker_scope=tickers_in_scope,
        )
        cur.execute(update_sql, params)
        updated = cur.rowcount
        conn.commit()
        close_succeeded(
            conn,
            run_id,
            counts={
                "action_kind": "backfill_cross_endpoint_period_end",
                "actor": actor,
                "tickers": tickers_in_scope,
                "rows_processed": updated,
                "rows_updated": updated,
                "groups_updated": groups,
            },
        )
        return CanonicalizationResult(
            rows_processed=updated, groups=groups, ingest_run_id=run_id
        )


def canonicalize_q4_to_annual(
    conn: psycopg.Connection,
    *,
    tickers: Iterable[str] | None = None,
    apply: bool = True,
    actor: str = "operator",
) -> CanonicalizationResult:
    """Snap Q4 quarterly period_end to FY annual within the same statement."""
    preview_sql, update_sql, params = _q4_sql(tickers)

    with conn.cursor() as cur:
        cur.execute(preview_sql, params)
        preview = cur.fetchall()

        groups = len(preview)
        rows_to_update = sum(row[5] for row in preview)
        if not preview:
            return CanonicalizationResult(rows_processed=0, groups=0, ingest_run_id=None)

        if not apply:
            return CanonicalizationResult(
                rows_processed=rows_to_update, groups=groups, ingest_run_id=None
            )

        tickers_in_scope = sorted({row[0] for row in preview})
        run_id = open_run(
            conn,
            run_kind="manual",
            vendor="arrow",
            ticker_scope=tickers_in_scope,
        )
        cur.execute(update_sql, params)
        updated = cur.rowcount
        conn.commit()
        close_succeeded(
            conn,
            run_id,
            counts={
                "action_kind": "backfill_q4_period_end",
                "actor": actor,
                "tickers": tickers_in_scope,
                "rows_processed": updated,
                "rows_updated": updated,
                "groups_updated": groups,
            },
        )
        return CanonicalizationResult(
            rows_processed=updated, groups=groups, ingest_run_id=run_id
        )
