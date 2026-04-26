"""Steward action callables.

Every operator action lives here as a typed function with an ``actor``
parameter. UI route handlers in scripts/dashboard.py are 3-line wrappers
around these functions; the V2+ steward agent will call the same functions
with a different actor. Same primitives, different consumer.

See docs/architecture/steward.md § Action Surface.

Conventions:
  - Every state change appends to ``history`` jsonb on the affected row:
    ``{at, actor, action, before, after, note}``.
  - Every action takes ``actor: str`` (e.g. ``"human:michael"``,
    ``"agent:steward_v1"``, ``"system:check_runner"``).
  - Functions are idempotent where the operation has a natural
    "already done" state (open_finding, add_to_coverage); transitions
    that don't (close_finding) raise on invalid input.
  - Functions take a ``conn`` first; the caller is responsible for
    transaction scope. Each function uses one or two cursors and does
    not commit — letting the caller batch.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import psycopg
from psycopg.types.json import Jsonb


# ---------------------------------------------------------------------------
# Return types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FindingRef:
    """Reference to a finding row after an action.

    ``outcome`` is set by ``open_finding`` and reports what the call did:
      - ``"created"``    — inserted a new open row
      - ``"re_observed"``— bumped last_seen_at on an existing open row
      - ``"suppressed"`` — returned an active closed-suppressed row instead
                          of opening anything (suppression respected)
      - ``"transitioned"`` — set by close_finding family
    """

    id: int
    fingerprint: str
    status: str
    closed_reason: str | None
    outcome: str = "transitioned"


# V1.2 dropped CoverageRef + add_to_coverage / remove_from_coverage.
# Every ticker in `companies` is automatically tracked by the steward —
# there is no separate membership step. To add a ticker, run
# `scripts/ingest_company.py TICKER`. To remove one, delete from
# `companies` (which requires deleting its data first via FK
# constraints) — but the typical pattern is to suppress findings on
# tickers you no longer want noise about.


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class StewardActionError(RuntimeError):
    """Raised when a steward action is invoked with invalid arguments
    or against invalid current state."""


# ---------------------------------------------------------------------------
# Finding lifecycle
# ---------------------------------------------------------------------------


def open_finding(
    conn: psycopg.Connection,
    *,
    fingerprint: str,
    finding_type: str,
    severity: str,
    company_id: int | None,
    ticker: str | None,
    vertical: str | None,
    fiscal_period_key: str | None,
    source_check: str,
    evidence: dict[str, Any],
    summary: str,
    suggested_action: dict[str, Any] | None,
    actor: str,
) -> FindingRef:
    """Insert an open finding for ``fingerprint``, or bump ``last_seen_at``
    on an existing open one. Idempotent and concurrency-safe.

    Respects active suppressions: if a closed-suppressed row exists for
    this fingerprint with ``suppressed_until`` in the future (or NULL,
    meaning permanently suppressed), no new open row is created and the
    existing closed-suppressed row is returned. This is the rule that
    lets ``suppress_finding(reason='AVGO segment reorg confirmed')``
    actually stick across nightly sweeps.

    Concurrency:
        The atomic insert-or-bump uses ``INSERT ... ON CONFLICT (fingerprint)
        WHERE status='open' DO UPDATE`` against the partial unique index
        ``data_quality_findings_open_fingerprint_uidx``. Two concurrent
        callers for the same fingerprint will not crash on
        UniqueViolation: one inserts, the other takes the update path
        and reports outcome='re_observed'. Tested in
        ``test_open_finding_concurrent_inserts_no_crash``.

        Residual race (documented, not eliminated): the suppression
        check is a separate statement, so a suppression added between
        that check and the upsert can be missed. The next sweep will
        respect the new suppression. Eliminating this window entirely
        would require SERIALIZABLE isolation around both statements,
        which would conflict with the caller-controlled-transaction
        contract this function follows.

    Outcome reported on the returned FindingRef:
        - "created"     — new open row inserted
        - "re_observed" — existing open row had last_seen_at bumped
        - "suppressed"  — active suppression matched; no row touched
    """
    _require(actor, "actor")
    _require(fingerprint, "fingerprint")
    _require(finding_type, "finding_type")
    _require(source_check, "source_check")
    _require(summary, "summary")
    if severity not in ("informational", "warning", "investigate"):
        raise StewardActionError(f"invalid severity: {severity!r}")

    with conn.cursor() as cur:
        # 1. Active suppression? If so, return the suppressed row.
        cur.execute(
            """
            SELECT id, fingerprint, status, closed_reason
            FROM data_quality_findings
            WHERE fingerprint = %s
              AND status = 'closed'
              AND closed_reason = 'suppressed'
              AND (suppressed_until IS NULL OR suppressed_until > now())
            ORDER BY closed_at DESC
            LIMIT 1;
            """,
            (fingerprint,),
        )
        row = cur.fetchone()
        if row is not None:
            return FindingRef(
                id=row[0], fingerprint=row[1], status=row[2], closed_reason=row[3],
                outcome="suppressed",
            )

        # 2. Atomic insert-or-bump on the partial unique
        #    (fingerprint) WHERE status='open'. Eliminates the
        #    concurrent-callers crash. The xmax=0 trick distinguishes
        #    insert (xmax = 0 on a fresh row) vs update (xmax > 0,
        #    set to the current transaction id).
        initial_history = [
            _history_entry(
                actor=actor, action="opened",
                after={"status": "open"}, note=None,
            )
        ]
        bump_history = [
            _history_entry(actor=actor, action="re_observed", note=None)
        ]
        cur.execute(
            """
            INSERT INTO data_quality_findings (
                fingerprint, finding_type, severity,
                company_id, ticker, vertical, fiscal_period_key,
                source_check, evidence, summary, suggested_action,
                status, history, created_by
            )
            VALUES (
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                'open', %s, %s
            )
            ON CONFLICT (fingerprint) WHERE status = 'open'
            DO UPDATE SET
                last_seen_at = now(),
                history = data_quality_findings.history || %s::jsonb
            RETURNING id, fingerprint, status, closed_reason, (xmax = 0) AS was_inserted;
            """,
            (
                fingerprint, finding_type, severity,
                company_id, ticker, vertical, fiscal_period_key,
                source_check, Jsonb(evidence or {}), summary,
                Jsonb(suggested_action) if suggested_action is not None else None,
                Jsonb(initial_history), actor,
                Jsonb(bump_history),
            ),
        )
        r = cur.fetchone()
        outcome = "created" if r[4] else "re_observed"
        return FindingRef(
            id=r[0], fingerprint=r[1], status=r[2], closed_reason=r[3],
            outcome=outcome,
        )


def close_finding(
    conn: psycopg.Connection,
    finding_id: int,
    *,
    closed_reason: str,
    actor: str,
    note: str | None = None,
    suppressed_until: date | datetime | None = None,
) -> FindingRef:
    """Move an open finding → closed with structured ``closed_reason``."""
    _require(actor, "actor")
    if closed_reason not in ("resolved", "suppressed", "dismissed"):
        raise StewardActionError(f"invalid closed_reason: {closed_reason!r}")

    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, fingerprint FROM data_quality_findings WHERE id = %s FOR UPDATE;",
            (finding_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise StewardActionError(f"finding {finding_id} does not exist")
        current_status, fp = row
        if current_status != "open":
            raise StewardActionError(
                f"finding {finding_id} is already {current_status!r}; "
                f"cannot close again. Reopen first if needed."
            )

        history_entry = _history_entry(
            actor=actor,
            action=f"closed:{closed_reason}",
            before={"status": "open"},
            after={"status": "closed", "closed_reason": closed_reason},
            note=note,
        )
        cur.execute(
            """
            UPDATE data_quality_findings
            SET status = 'closed',
                closed_reason = %s,
                closed_at = now(),
                closed_by = %s,
                closed_note = %s,
                suppressed_until = %s,
                history = history || %s::jsonb
            WHERE id = %s
            RETURNING id, fingerprint, status, closed_reason;
            """,
            (
                closed_reason,
                actor,
                note,
                suppressed_until,
                Jsonb([history_entry]),
                finding_id,
            ),
        )
        r = cur.fetchone()
        return FindingRef(id=r[0], fingerprint=r[1], status=r[2], closed_reason=r[3])


def resolve_finding(
    conn: psycopg.Connection,
    finding_id: int,
    *,
    actor: str,
    note: str | None = None,
) -> FindingRef:
    """Convenience: close with reason='resolved' (problem fixed)."""
    return close_finding(conn, finding_id, closed_reason="resolved", actor=actor, note=note)


def suppress_finding(
    conn: psycopg.Connection,
    finding_id: int,
    *,
    actor: str,
    reason: str,
    expires: date | datetime | None = None,
) -> FindingRef:
    """Convenience: close with reason='suppressed' (known, not actionable now).

    ``reason`` is required because suppressions without reasons rot the
    inbox over time. ``expires=None`` means suppress indefinitely; the
    runner respects active suppressions when reopening.
    """
    if not reason or not reason.strip():
        raise StewardActionError("suppress_finding requires a non-empty reason")
    return close_finding(
        conn,
        finding_id,
        closed_reason="suppressed",
        actor=actor,
        note=reason,
        suppressed_until=expires,
    )


def dismiss_finding(
    conn: psycopg.Connection,
    finding_id: int,
    *,
    actor: str,
    note: str | None = None,
) -> FindingRef:
    """Convenience: close with reason='dismissed' (false positive)."""
    return close_finding(conn, finding_id, closed_reason="dismissed", actor=actor, note=note)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _require(value: Any, name: str) -> None:
    if value is None or (isinstance(value, str) and not value.strip()):
        raise StewardActionError(f"{name} is required")


def _history_entry(
    *,
    actor: str,
    action: str,
    before: dict[str, Any] | None = None,
    after: dict[str, Any] | None = None,
    note: str | None = None,
) -> dict[str, Any]:
    """Build a single audit entry. Stored as one element of the history
    jsonb array on a finding row."""
    entry: dict[str, Any] = {
        "at": datetime.now(timezone.utc).isoformat(),
        "actor": actor,
        "action": action,
    }
    if before is not None:
        entry["before"] = before
    if after is not None:
        entry["after"] = after
    if note:
        entry["note"] = note
    return entry
