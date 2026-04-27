from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional


@dataclass(frozen=True)
class DateRange:
    start: date
    end: date


def parse_date(value: str) -> date:
    # esperado: YYYY-MM-DD
    return datetime.strptime(value, "%Y-%m-%d").date()


def parse_date_range(start: Optional[str], end: Optional[str]) -> Optional[DateRange]:
    if not start and not end:
        return None
    if not start or not end:
        raise ValueError("start y end son obligatorios juntos (YYYY-MM-DD).")
    s = parse_date(start)
    e = parse_date(end)
    if s > e:
        raise ValueError("start debe ser <= end.")
    return DateRange(start=s, end=e)

