from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

CompanyStatus = Literal["active", "delisted", "merged", "acquired", "private"]


@dataclass
class Company:
    id: int | None
    cik: int
    ticker: str
    name: str
    fiscal_year_end_md: str
    status: CompanyStatus
    created_at: datetime
    updated_at: datetime
