import re
from datetime import datetime
from zoneinfo import ZoneInfo



TIME_ZONE = "Europe/Lisbon"
HOUR_MINUTE_PATTERN = re.compile(r'^(?P<h1>\d+):(?P<m1>\d+):00$|^(?:(?P<h2>\d+)\s*(?:h|\s))?\s*(?:(?P<m2>\d+)\s*(?:m|min)?)?$')
SUM_RUNTIME_PATTERN = re.compile(r'^(\d+)\s*\+\s*(\d+)$')


def now():
    return datetime.now(tz=ZoneInfo(TIME_ZONE))

def midnight(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)

def to_string(dt: datetime) -> str:
    return dt.isoformat(sep=" ")

def to_datetime(dt: str, format: str | None = None) -> datetime:
    if format:
        return datetime.strptime(dt, format).replace(tzinfo=ZoneInfo(TIME_ZONE))
    return datetime.fromisoformat(dt).astimezone(ZoneInfo(TIME_ZONE))


def string_to_runtime(runtime: str) -> int:
    match = SUM_RUNTIME_PATTERN.match(runtime)
    if match:
        return int(match.group(1)) + int(match.group(2))

    match = HOUR_MINUTE_PATTERN.match(runtime)
    assert match, f"Invalid runtime format: {runtime}"
    hour = int(match.group('h1') or match.group('h2') or 0)
    minutes = int(match.group('m1') or match.group('m2') or 0)
    return hour * 60 + minutes

def runtime_to_string(minutes: int) -> str:
    h, m = divmod(minutes, 60)
    return f"{h:02d}:{m:02d}:00"
