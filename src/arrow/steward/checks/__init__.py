"""Steward checks. Importing this package self-registers each check.

Add a new check:
  1. Create ``src/arrow/steward/checks/<name>.py`` with a ``Check``
     subclass decorated by ``@register``.
  2. Import it here so the registration side-effect fires on package
     import.

The runner reads ``arrow.steward.registry.REGISTRY`` after this package
has been imported.
"""

from __future__ import annotations

# noqa: F401 — these imports exist for their registration side-effects.
from arrow.steward.checks import zero_row_runs  # noqa: F401
from arrow.steward.checks import unresolved_flags_aging  # noqa: F401
from arrow.steward.checks import sec_artifact_orphans  # noqa: F401
from arrow.steward.checks import unparsed_body_fallback  # noqa: F401
from arrow.steward.checks import extraction_method_drift  # noqa: F401
from arrow.steward.checks import chunk_repair_concentration  # noqa: F401
from arrow.steward.checks import expected_coverage  # noqa: F401
from arrow.steward.checks import transcript_artifact_orphans  # noqa: F401
from arrow.steward.checks import quarterly_value_duplication  # noqa: F401
from arrow.steward.checks import q4_period_end_consistency  # noqa: F401
from arrow.steward.checks import cross_endpoint_period_end_consistency  # noqa: F401
from arrow.steward.checks import period_end_calendar_consistency  # noqa: F401
from arrow.steward.checks import quarterly_sum_to_annual_drift  # noqa: F401
from arrow.steward.checks import xbrl_audit_unresolved  # noqa: F401
from arrow.steward.checks import prices_gap_detection  # noqa: F401
from arrow.steward.checks import price_target_consensus_freshness  # noqa: F401
from arrow.steward.checks import analyst_estimates_orphan  # noqa: F401
from arrow.steward.checks import earnings_surprise_sanity  # noqa: F401
