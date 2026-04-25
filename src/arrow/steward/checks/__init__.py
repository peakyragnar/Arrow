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
