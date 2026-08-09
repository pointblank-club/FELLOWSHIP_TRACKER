"""Date parsing and application-window status helpers."""
from __future__ import annotations
from calendar import monthrange
from datetime import date, datetime
import re

_MONTHS = {
    name.lower(): number
    for number, names in enumerate(
        (
            ("january", "jan"),
            ("february", "feb"),
            ("march", "mar"),
            ("april", "apr"),
            ("may",),
            ("june", "jun"),
            ("july", "jul"),
            ("august", "aug"),
            ("september", "sep", "sept"),
            ("october", "oct"),
            ("november", "nov"),
            ("december", "dec"),
        ),
        start=1,
    )
    for name in names
}
_UNKNOWN = {"", "check website", "not specified", "unknown", "n/a", "none", "null"}
_ROLLING = {"rolling", "ongoing", "open until filled", "open-ended"}

def _valid_date(year: int, month: int, day: int, *, end: bool) -> date | None:
    try:
        if day == 0:
            day = monthrange(year, month)[1] if end else 1
        return date(year, month, day)
    except ValueError:
        return None

def parse_opportunity_date(value, *, end: bool = False) -> date | None:
    """Parse the normalized date formats requested from the extraction model."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if value is None:
        return None

    text = re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", str(value).strip().lower())
    text = re.sub(r"\s+", " ", text)
    if text in _UNKNOWN or text in _ROLLING:
        return None

    iso_dates = re.findall(r"\b(\d{4})[-/](\d{1,2})[-/](\d{1,2})\b", text)
    if iso_dates:
        year, month, day = (int(part) for part in (iso_dates[-1] if end else iso_dates[0]))
        return _valid_date(year, month, day, end=end)

    named_dates = re.findall(
        r"\b([a-z]+)\s+(\d{1,2})(?:,)?\s+(\d{4})\b|"
        r"\b(\d{1,2})\s+([a-z]+)\s+(\d{4})\b",
        text,
    )
    if named_dates:
        match = named_dates[-1] if end else named_dates[0]
        if match[0]:
            month_name, day, year = match[0], match[1], match[2]
        else:
            day, month_name, year = match[3], match[4], match[5]
        month = _MONTHS.get(month_name)
        if month:
            return _valid_date(int(year), month, int(day), end=end)

    month_years = re.findall(r"\b([a-z]+)\s+(\d{4})\b", text)
    if month_years:
        month_name, year = month_years[-1] if end else month_years[0]
        month = _MONTHS.get(month_name)
        if month:
            return _valid_date(int(year), month, 0, end=end)

    return None

def calculate_is_open(
    start_value,
    deadline_value,
    model_value,
    *,
    today: date | None = None,
) -> bool:
    """Calculate whether applications are open from the extracted window."""
    today = today or date.today()
    start = parse_opportunity_date(start_value)
    deadline = parse_opportunity_date(deadline_value, end=True)

    if start is not None and today < start:
        return False
    if deadline is not None and today > deadline:
        return False

    if start is not None and deadline is not None:
        return True

    # If one boundary is missing, use the page's explicit status rather than
    # guessing from a single date.
    if start is not None or deadline is not None:
        if model_value is not None:
            if isinstance(model_value, str):
                return model_value.strip().lower() in {"true", "open", "yes"}
            return bool(model_value)
        return deadline is not None

    if isinstance(model_value, str):
        return model_value.strip().lower() in {"true", "open", "yes"}
    return bool(model_value)
