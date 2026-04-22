"""Review and act on data_quality_flags rows.

Data-quality flags come from soft-validation layers (Layer 1 IS/BS/CF
subtotal drift today; Layer 3 period arithmetic and Layer 5
cross-source reconciliation when those audit rails run). A flag records
an arithmetic disagreement Arrow noticed while loading a row. The row
itself is stored verbatim; the flag is a caveat attached to it.

Usage:

    # List all unresolved flags across the DB
    uv run scripts/review_flags.py

    # List unresolved flags for specific tickers
    uv run scripts/review_flags.py NVDA AMD

    # Show details of one flag
    uv run scripts/review_flags.py --show 42

    # Accept a flag as-is (keeps the row loaded, records your acceptance)
    uv run scripts/review_flags.py --accept 42 --note "vendor rounding, within noise"

    # Accept all unresolved flags for a ticker with the same note
    uv run scripts/review_flags.py --accept-all AMD --note "FY22 Q1/Q3 FMP normalization drift"

Resolution actions write `resolution='accept_as_is'`, `resolved_at=now()`,
and your `resolution_note`. The flag row stays forever — resolved flags
are never deleted, so the audit trail is queryable as long as the DB
exists.

This script does NOT modify financial_facts. Accepting a flag only
records that you reviewed it and chose to keep the row as FMP shipped
it. The fact values remain exactly what FMP returned.
"""

from __future__ import annotations

import argparse
import sys
from typing import Any

import psycopg

from arrow.db.connection import get_conn


LIST_QUERY = """
SELECT
    f.id,
    c.ticker,
    f.flag_type,
    f.severity,
    f.statement,
    f.concept,
    f.fiscal_period_label_or_null,
    f.period_end,
    f.expected_value,
    f.computed_value,
    f.delta,
    f.tolerance,
    f.flagged_at
FROM (
    SELECT
        id, company_id, flag_type, severity, statement, concept,
        CASE
            WHEN fiscal_quarter IS NOT NULL
                THEN 'FY' || fiscal_year || ' Q' || fiscal_quarter
            WHEN fiscal_year IS NOT NULL
                THEN 'FY' || fiscal_year
            ELSE NULL
        END AS fiscal_period_label_or_null,
        period_end, expected_value, computed_value, delta, tolerance,
        flagged_at
    FROM data_quality_flags
    WHERE resolved_at IS NULL
) f
JOIN companies c ON c.id = f.company_id
{ticker_filter}
ORDER BY c.ticker, f.flag_type, f.period_end, f.concept;
"""


def _list_flags(conn: psycopg.Connection, tickers: list[str]) -> int:
    if tickers:
        placeholders = ",".join(["%s"] * len(tickers))
        ticker_filter = f"WHERE c.ticker = ANY(ARRAY[{placeholders}])"
        params: tuple[Any, ...] = tuple(t.upper() for t in tickers)
    else:
        ticker_filter = ""
        params = ()

    with conn.cursor() as cur:
        cur.execute(LIST_QUERY.format(ticker_filter=ticker_filter), params)
        rows = cur.fetchall()

    if not rows:
        scope = ", ".join(t.upper() for t in tickers) if tickers else "the database"
        print(f"No unresolved flags for {scope}.")
        return 0

    print(f"Unresolved data_quality_flags ({len(rows)} total):")
    print()
    header = (
        f"  {'id':>5} {'ticker':6} {'flag_type':30} {'sev':14} "
        f"{'period':10} {'concept':12} {'delta':>16} {'flagged_at'}"
    )
    print(header)
    print("  " + "-" * (len(header) - 2))
    for (
        fid, ticker, flag_type, severity, statement, concept,
        period_label, period_end, expected, computed, delta, tolerance, flagged_at,
    ) in rows:
        period = period_label or (str(period_end) if period_end else "—")
        delta_fmt = f"{delta:>16}" if delta is not None else " " * 16
        print(
            f"  {fid:>5} {ticker:6} {flag_type:30} {severity:14} "
            f"{period:10} {concept:12} {delta_fmt} {flagged_at}"
        )
    print()
    print("Show details :  uv run scripts/review_flags.py --show <id>")
    print("Accept       :  uv run scripts/review_flags.py --accept <id> --note '...'")
    return 0


SHOW_QUERY = """
SELECT
    f.id, c.ticker, f.flag_type, f.severity,
    f.statement, f.concept, f.fiscal_year, f.fiscal_quarter,
    f.period_end, f.period_type,
    f.expected_value, f.computed_value, f.delta, f.tolerance,
    f.reason, f.context,
    f.source_run_id, f.flagged_at,
    f.resolved_at, f.resolution, f.resolution_note
FROM data_quality_flags f
JOIN companies c ON c.id = f.company_id
WHERE f.id = %s;
"""


def _show_flag(conn: psycopg.Connection, flag_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute(SHOW_QUERY, (flag_id,))
        row = cur.fetchone()
    if row is None:
        print(f"No flag with id={flag_id}.", file=sys.stderr)
        return 1

    (
        fid, ticker, flag_type, severity, statement, concept,
        fiscal_year, fiscal_quarter, period_end, period_type,
        expected, computed, delta, tolerance, reason, context,
        source_run_id, flagged_at, resolved_at, resolution, note,
    ) = row

    period = (
        f"FY{fiscal_year} Q{fiscal_quarter}" if fiscal_quarter else
        (f"FY{fiscal_year}" if fiscal_year else "—")
    )

    print(f"Flag id={fid}")
    print(f"  ticker          : {ticker}")
    print(f"  flag_type       : {flag_type}")
    print(f"  severity        : {severity}")
    print(f"  statement       : {statement}")
    print(f"  concept         : {concept}")
    print(f"  period          : {period}  (period_end={period_end}, type={period_type})")
    print(f"  expected_value  : {expected}")
    print(f"  computed_value  : {computed}")
    print(f"  delta           : {delta}")
    print(f"  tolerance       : {tolerance}")
    print(f"  source_run_id   : {source_run_id}")
    print(f"  flagged_at      : {flagged_at}")
    print(f"  resolved_at     : {resolved_at or '(unresolved)'}")
    if resolved_at:
        print(f"  resolution      : {resolution}")
        print(f"  resolution_note : {note}")
    print(f"  context         : {context}")
    print()
    print("Reason:")
    print(f"  {reason}")
    return 0


def _accept_flag(conn: psycopg.Connection, flag_id: int, note: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE data_quality_flags
            SET resolved_at = now(),
                resolution = 'accept_as_is',
                resolution_note = %s
            WHERE id = %s AND resolved_at IS NULL
            RETURNING id;
            """,
            (note, flag_id),
        )
        updated = cur.fetchone()
    conn.commit()

    if updated is None:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT resolved_at FROM data_quality_flags WHERE id = %s;",
                (flag_id,),
            )
            row = cur.fetchone()
        if row is None:
            print(f"No flag with id={flag_id}.", file=sys.stderr)
        else:
            print(
                f"Flag id={flag_id} is already resolved at {row[0]}.",
                file=sys.stderr,
            )
        return 1

    print(f"Flag id={flag_id} accepted as-is.")
    print(f"  note: {note}")
    return 0


def _accept_all_for_ticker(
    conn: psycopg.Connection, ticker: str, note: str
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE data_quality_flags
            SET resolved_at = now(),
                resolution = 'accept_as_is',
                resolution_note = %s
            WHERE resolved_at IS NULL
              AND company_id = (SELECT id FROM companies WHERE ticker = %s)
            RETURNING id;
            """,
            (note, ticker.upper()),
        )
        ids = [r[0] for r in cur.fetchall()]
    conn.commit()

    if not ids:
        print(f"No unresolved flags for {ticker.upper()}.")
        return 0
    print(f"Accepted {len(ids)} flags for {ticker.upper()}: {ids}")
    print(f"  note: {note}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="List / show / accept data_quality_flags rows."
    )
    parser.add_argument(
        "tickers", nargs="*", help="Filter listing to these tickers."
    )
    parser.add_argument("--show", type=int, help="Show details of flag id.")
    parser.add_argument("--accept", type=int, help="Accept flag id as-is.")
    parser.add_argument(
        "--accept-all",
        metavar="TICKER",
        help="Accept every unresolved flag for this ticker.",
    )
    parser.add_argument(
        "--note",
        help="Resolution note (required with --accept or --accept-all).",
    )
    args = parser.parse_args()

    conn = get_conn()
    try:
        if args.show is not None:
            return _show_flag(conn, args.show)
        if args.accept is not None:
            if not args.note:
                print("--accept requires --note", file=sys.stderr)
                return 2
            return _accept_flag(conn, args.accept, args.note)
        if args.accept_all:
            if not args.note:
                print("--accept-all requires --note", file=sys.stderr)
                return 2
            return _accept_all_for_ticker(conn, args.accept_all, args.note)
        return _list_flags(conn, args.tickers)
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
