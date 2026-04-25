"""Steward check registry.

Each check is a class that subclasses ``Check`` and is wrapped with the
``@register`` decorator so it appears in ``REGISTRY`` once its module is
imported. ``arrow.steward.checks.__init__`` imports each check module so
the side-effect populates the registry on package import.

A check is a pure function over DB state in V1 (deterministic SQL); V2
adds ``LLMCheck`` for prose-judgment failure modes. Both use this same
registry.

Checks never write to the DB themselves. They yield ``FindingDraft``
instances; the runner stamps ``source_check`` with the check's name,
computes fingerprints, calls ``open_finding``, and handles
auto-resolve.

See docs/architecture/steward.md § Core Objects and § Stage Contracts.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Iterable


# ---------------------------------------------------------------------------
# Scope
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Scope:
    """Filter for which work the runner should do.

    ``tickers`` and ``verticals`` and ``check_names`` may each be None
    (meaning "all"). Cross-cutting checks (vertical=None) always run
    even when ``verticals`` is set.
    """

    tickers: list[str] | None = None
    verticals: list[str] | None = None
    check_names: list[str] | None = None

    @classmethod
    def universe(cls) -> "Scope":
        return cls()

    @classmethod
    def for_tickers(cls, *tickers: str) -> "Scope":
        return cls(tickers=[t.upper() for t in tickers])

    def matches_ticker(self, ticker: str | None) -> bool:
        """True if ticker is in scope. Cross-cutting findings (ticker=None)
        are in scope only on universe runs."""
        if self.tickers is None:
            return True
        if ticker is None:
            return False
        return ticker.upper() in self.tickers


# ---------------------------------------------------------------------------
# FindingDraft
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FindingDraft:
    """What a check yields. The runner stamps source_check + actor and
    persists via ``open_finding``."""

    fingerprint: str
    finding_type: str
    severity: str
    company_id: int | None
    ticker: str | None
    vertical: str | None
    fiscal_period_key: str | None
    evidence: dict[str, Any]
    summary: str
    suggested_action: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Check ABC + registry
# ---------------------------------------------------------------------------


class Check(ABC):
    """Base class for steward checks. Subclasses set ``name``,
    ``severity``, ``vertical``, and implement ``run``.
    """

    #: Stable check identifier (e.g. ``"zero_row_runs"``). Used as
    #: ``source_check`` on findings, as the auto-resolve scope key,
    #: and as the CLI ``--check`` argument.
    name: str = ""

    #: Default severity for findings this check produces. Individual
    #: drafts may override.
    severity: str = "warning"

    #: Vertical the check belongs to (``"financials"``, ``"sec_qual"``,
    #: etc.) or None for cross-cutting checks. Cross-cutting checks
    #: always run regardless of ``Scope.verticals``.
    vertical: str | None = None

    @abstractmethod
    def run(self, conn, *, scope: Scope) -> Iterable[FindingDraft]:
        """Yield zero or more FindingDrafts. Must not write to the DB."""


REGISTRY: list[Check] = []


def register(cls: type[Check]) -> type[Check]:
    """Class decorator: instantiate the check (zero-arg constructor) and
    add it to the registry. Refuses duplicate names.

    Usage::

        @register
        class ZeroRowRuns(Check):
            name = "zero_row_runs"
            severity = "warning"
            vertical = None

            def run(self, conn, *, scope):
                ...
    """
    if not cls.name:
        raise ValueError(f"check class {cls.__name__} has no `name`")
    if any(c.name == cls.name for c in REGISTRY):
        raise ValueError(f"duplicate check name: {cls.name!r}")
    REGISTRY.append(cls())
    return cls


def select_checks(scope: Scope) -> list[Check]:
    """Return the checks that should run for ``scope``."""
    out: list[Check] = []
    for check in REGISTRY:
        if scope.check_names is not None and check.name not in scope.check_names:
            continue
        if scope.verticals is not None:
            # Cross-cutting checks (vertical=None) always run.
            if check.vertical is not None and check.vertical not in scope.verticals:
                continue
        out.append(check)
    return out
