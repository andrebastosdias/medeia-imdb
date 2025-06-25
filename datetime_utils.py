import re
from datetime import datetime
from zoneinfo import ZoneInfo


TIME_ZONE = "Europe/Lisbon"
HOUR_MINUTE_PATTERN = re.compile(r'^(?:(\d+)\s*(?:h|:|\s))?\s(\*d+)\s*(?:m|min)?$')
SUM_RUNTIME_PATTERN = re.compile(r'^(\d+)\s*\+\s*(\d+)$')

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

def string_to_runtime(runtime: str) -> str:
    match = SUM_RUNTIME_PATTERN.match(runtime)
    if match:
        return int(match.group(1) + int(match.group(2)))

    match = HOUR_MINUTE_PATTERN.match(runtime)
    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    return hours * 60 + minutes

def runtime_to_string(minutes: int) -> str:
    h, m = divmod(minutes, 60)
    return f"{h:02d}:{m:02d}:00"
