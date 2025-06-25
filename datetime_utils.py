from datetime import datetime
from zoneinfo import ZoneInfo


TIME_ZONE = "Europe/Lisbon"

def _safe_dt(dt: datetime | None) -> datetime:
    return dt or now()

def now():
    return datetime.now(tz=ZoneInfo(TIME_ZONE))

def midnight(dt: datetime | None = None) -> datetime:
    return _safe_dt(dt).replace(hour=0, minute=0, second=0, microsecond=0)

def to_string(dt: datetime | None = None) -> str:
    return _safe_dt(dt).isoformat(sep=" ")

def to_datetime(dt: str, format: str | None = None) -> datetime:
    if format:
        return datetime.strptime(dt, format).replace(tzinfo=ZoneInfo(TIME_ZONE))
    return datetime.fromisoformat(dt).astimezone(ZoneInfo(TIME_ZONE))
