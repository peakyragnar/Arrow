"""Deterministic fingerprint for steward findings.

A finding's fingerprint is a stable identifier for "the same problem
recurring." It must produce the same hex string across runs given the
same logical inputs, so the runner can:

  - dedup new drafts against existing open findings
  - auto-resolve open findings whose fingerprint did not surface this run
  - respect active suppressions when a fingerprint would otherwise reopen

Inputs:
  - check_name: the steward check that produced this draft
  - scope: the dimensions that identify *this instance* of the problem
           (e.g. {ticker, period, vertical}). NULL/missing keys are
           normalized to empty so checks can omit irrelevant scopes.
  - rule_params: the parameters of the rule that fired
                 (e.g. {threshold_days: 14}). Including these means a
                 tightened threshold produces a different fingerprint
                 (correct: it is a different rule).

Determinism guarantees:
  - dict ordering does not affect output (sorted keys)
  - JSON encoding is canonical (sort_keys=True, no whitespace)
  - non-ASCII / unicode is preserved verbatim (ensure_ascii=False)
  - non-string values must be JSON-serializable (ints, floats, bools,
    lists of those, nested dicts of those)
"""

from __future__ import annotations

import hashlib
import json
from typing import Any


def fingerprint(
    check_name: str,
    scope: dict[str, Any] | None = None,
    rule_params: dict[str, Any] | None = None,
) -> str:
    """Return the SHA-256 hex of a canonical encoding of the inputs."""
    if not check_name:
        raise ValueError("check_name must be a non-empty string")

    payload = {
        "check": check_name,
        "scope": _canonicalize(scope or {}),
        "params": _canonicalize(rule_params or {}),
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonicalize(d: dict[str, Any]) -> dict[str, Any]:
    """Drop None values so ``{"x": None}`` and ``{}`` fingerprint identically.

    A scope field set to None means "no value for this dimension," which
    is the same as omitting it. Without this rule, a check that started
    passing ``ticker=None`` for cross-cutting findings would produce
    different fingerprints than a check that omitted the key entirely.
    """
    return {k: v for k, v in d.items() if v is not None}
