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
    """Reference to a finding row after an action."""

    id: int
    fingerprint: str
    status: str
    closed_reason: str | None


@dataclass(frozen=True)
class CoverageRef:
    """Reference to a coverage_membership row after an action."""

    id: int
    company_id: int
    ticker: str
    tier: str


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
    on an existing open one. Idempotent.

    Respects active suppressions: if a closed-suppressed row exists for
    this fingerprint with ``suppressed_until`` in the future (or NULL,
    meaning permanently suppressed), no new open row is created and the
    existing closed-suppressed row is returned. This is the rule that
    lets ``suppress_finding(reason='AVGO segment reorg confirmed')``
    actually stick across nightly sweeps.
    """
    _require(actor, "actor")
    _require(fingerprint, "fingerprint")
    _require(finding_type, "finding_type")
    _require(source_check, "source_check")
    _require(summary, "summary")
    if severity not in ("informational", "warning", "investigate"):
        raise StewardActionError(f"invalid severity: {severity!r}")

    with conn.cursor() as cur:
        # 1. Active suppression? If so, return the suppressed row, skip insert.
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
            return FindingRef(id=row[0], fingerprint=row[1], status=row[2], closed_reason=row[3])

        # 2. Existing open row? Bump last_seen_at, append history note.
        cur.execute(
            "SELECT id FROM data_quality_findings "
            "WHERE fingerprint = %s AND status = 'open' LIMIT 1;",
            (fingerprint,),
        )
        row = cur.fetchone()
        if row is not None:
            existing_id = row[0]
            cur.execute(
                """
                UPDATE data_quality_findings
                SET last_seen_at = now(),
                    history = history || %s::jsonb
                WHERE id = %s
                RETURNING id, fingerprint, status, closed_reason;
                """,
                (
                    Jsonb([_history_entry(actor=actor, action="re_observed", note=None)]),
                    existing_id,
                ),
            )
            r = cur.fetchone()
            return FindingRef(id=r[0], fingerprint=r[1], status=r[2], closed_reason=r[3])

        # 3. Insert new open row with initial history entry.
        initial_history = [
            _history_entry(
                actor=actor,
                action="opened",
                after={"status": "open"},
                note=None,
            )
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
            RETURNING id, fingerprint, status, closed_reason;
            """,
            (
                fingerprint, finding_type, severity,
                company_id, ticker, vertical, fiscal_period_key,
                source_check, Jsonb(evidence or {}), summary,
                Jsonb(suggested_action) if suggested_action is not None else None,
                Jsonb(initial_history), actor,
            ),
        )
        r = cur.fetchone()
        return FindingRef(id=r[0], fingerprint=r[1], status=r[2], closed_reason=r[3])


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
# Coverage membership
# ---------------------------------------------------------------------------


def add_to_coverage(
    conn: psycopg.Connection,
    *,
    ticker: str,
    tier: str,
    actor: str,
    notes: str | None = None,
) -> CoverageRef:
    """Add a ticker to coverage_membership at ``tier``. Idempotent.

    If the ticker is already in coverage at the same tier, returns the
    existing row. If the tier differs, raises (tier change is a separate
    explicit action — see ``set_coverage_tier``).
    """
    _require(actor, "actor")
    _require(ticker, "ticker")
    if tier not in ("core", "extended"):
        raise StewardActionError(f"invalid tier: {tier!r}")
    ticker = ticker.upper()

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM companies WHERE ticker = %s;", (ticker,))
        company_row = cur.fetchone()
        if company_row is None:
            raise StewardActionError(
                f"ticker {ticker!r} is not in companies. "
                f"Seed it first via scripts/ingest_company.py or seed_companies()."
            )
        company_id = company_row[0]

        cur.execute(
            """
            SELECT id, tier FROM coverage_membership WHERE company_id = %s;
            """,
            (company_id,),
        )
        existing = cur.fetchone()
        if existing is not None:
            existing_id, existing_tier = existing
            if existing_tier != tier:
                raise StewardActionError(
                    f"ticker {ticker} is already in coverage at tier "
                    f"{existing_tier!r}; will not silently change to {tier!r}. "
                    f"Use set_coverage_tier() if a tier change is intentional."
                )
            return CoverageRef(
                id=existing_id, company_id=company_id, ticker=ticker, tier=existing_tier
            )

        cur.execute(
            """
            INSERT INTO coverage_membership (company_id, tier, added_by, notes)
            VALUES (%s, %s, %s, %s)
            RETURNING id;
            """,
            (company_id, tier, actor, notes),
        )
        new_id = cur.fetchone()[0]
        return CoverageRef(id=new_id, company_id=company_id, ticker=ticker, tier=tier)


def remove_from_coverage(
    conn: psycopg.Connection,
    *,
    ticker: str,
    actor: str,
) -> bool:
    """Remove a ticker from coverage_membership. Returns True if a row
    was removed, False if the ticker was not in coverage. Idempotent.

    Note: does NOT delete the company from ``companies`` (the data stays).
    Open findings against the ticker stay open — the operator decides
    whether to dismiss them.
    """
    _require(actor, "actor")
    _require(ticker, "ticker")
    ticker = ticker.upper()

    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM coverage_membership
            WHERE company_id = (SELECT id FROM companies WHERE ticker = %s)
            RETURNING id;
            """,
            (ticker,),
        )
        return cur.fetchone() is not None


def set_coverage_tier(
    conn: psycopg.Connection,
    *,
    ticker: str,
    tier: str,
    actor: str,
) -> CoverageRef:
    """Change a ticker's coverage tier. Raises if the ticker is not in
    coverage."""
    _require(actor, "actor")
    _require(ticker, "ticker")
    if tier not in ("core", "extended"):
        raise StewardActionError(f"invalid tier: {tier!r}")
    ticker = ticker.upper()

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE coverage_membership
            SET tier = %s
            WHERE company_id = (SELECT id FROM companies WHERE ticker = %s)
            RETURNING id, company_id, tier;
            """,
            (tier, ticker),
        )
        row = cur.fetchone()
        if row is None:
            raise StewardActionError(
                f"ticker {ticker} is not in coverage; use add_to_coverage() first."
            )
        return CoverageRef(id=row[0], company_id=row[1], ticker=ticker, tier=row[2])


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
