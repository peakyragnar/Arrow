"""Repair calendar_year/calendar_quarter/calendar_period_label drift.

The cross-endpoint period_end backfill snaps a row's ``period_end`` to
the trusted-endpoint canonical date, but historically did not update
the dependent calendar fields. When the snap crossed a calendar-quarter
boundary (e.g. ``2023-06-30`` → ``2023-07-01``, CY-Q2 → CY-Q3), the
calendar fields were left stale.

Symptom: dashboard shows two columns for the same fiscal year — one
with IS/BS/CF metrics (under the stale calendar label) and one with
employees/segments metrics (under the correct calendar label) — because
``v_company_period_wide`` groups by all calendar fields.

This script recomputes ``calendar_year``, ``calendar_quarter``, and
``calendar_period_label`` from the row's current ``period_end`` using
``derive_calendar_period``, and updates only rows where the stored
fields disagree.

Idempotent. Operates on current rows (``superseded_at IS NULL``) of any
extraction_version, since the bug spans every statement and dimension.

Usage:
    uv run scripts/repair_calendar_fields.py            # dry run
    uv run scripts/repair_calendar_fields.py --apply
"""

from __future__ import annotations

import argparse

from arrow.db.connection import get_conn
from arrow.ingest.common.runs import close_succeeded, open_run
from arrow.normalize.periods.derive import derive_calendar_period


SCAN_SQL = """
SELECT id, period_end,
       calendar_year, calendar_quarter, calendar_period_label
FROM financial_facts
WHERE superseded_at IS NULL
  AND period_end IS NOT NULL
ORDER BY id
"""


UPDATE_SQL = """
UPDATE financial_facts
SET calendar_year         = %s,
    calendar_quarter      = %s,
    calendar_period_label = %s
WHERE id = %s
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="Write the updates; default is a dry-run preview.")
    args = parser.parse_args()

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(SCAN_SQL)
        rows = cur.fetchall()

        plan: list[tuple] = []
        for row_id, period_end, cy_year, cy_q, cy_label in rows:
            cp = derive_calendar_period(period_end)
            if (
                cy_year == cp.calendar_year
                and cy_q == cp.calendar_quarter
                and cy_label == cp.calendar_period_label
            ):
                continue
            plan.append((cp.calendar_year, cp.calendar_quarter,
                         cp.calendar_period_label, row_id,
                         period_end, cy_year, cy_q, cy_label))

        if not plan:
            print(f"Scanned {len(rows)} current rows. No calendar-field drift. Nothing to fix.")
            return

        # Per-ticker summary
        with conn.cursor() as cur2:
            cur2.execute("""
              SELECT ff.id, c.ticker
              FROM financial_facts ff JOIN companies c ON c.id=ff.company_id
              WHERE ff.id = ANY(%s)
            """, ([p[3] for p in plan],))
            ticker_by_id = dict(cur2.fetchall())

        by_ticker: dict[str, int] = {}
        for p in plan:
            t = ticker_by_id.get(p[3], "?")
            by_ticker[t] = by_ticker.get(t, 0) + 1

        print(f"Scanned {len(rows)} current rows. {len(plan)} need calendar-field repair.\n")
        print("Per-ticker:")
        for t, n in sorted(by_ticker.items(), key=lambda x: -x[1]):
            print(f"  {t}: {n} rows")

        # Show a few examples
        print("\nFirst 5 rows to update:")
        for new_y, new_q, new_label, rid, pe, old_y, old_q, old_label in plan[:5]:
            print(f"  id={rid} period_end={pe} | "
                  f"old: ({old_y}, {old_q}, {old_label!r}) → "
                  f"new: ({new_y}, {new_q}, {new_label!r})")

        if not args.apply:
            print("\nDry run. Pass --apply to write.")
            return

        tickers_in_scope = sorted({t for t in by_ticker.keys() if t and t != "?"})
        run_id = open_run(
            conn,
            run_kind="manual",
            vendor="arrow",
            ticker_scope=tickers_in_scope or None,
        )
        with conn.transaction(), conn.cursor() as cur3:
            for new_y, new_q, new_label, rid, *_ in plan:
                cur3.execute(UPDATE_SQL, (new_y, new_q, new_label, rid))
        close_succeeded(
            conn,
            run_id,
            counts={
                "action_kind": "repair_calendar_fields",
                "tickers": tickers_in_scope,
                "rows_updated": len(plan),
                "rows_per_ticker": by_ticker,
            },
        )
        print(f"\nUpdated {len(plan)} rows. ingest_run_id={run_id}")


if __name__ == "__main__":
    main()
