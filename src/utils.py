import re
from datetime import datetime
from typing import cast
from zoneinfo import ZoneInfo

TIME_ZONE = "Europe/Lisbon"
# Exact prime notation, e.g. "90'".
PRIME_PATTERN = re.compile(r"^(\d+)\'$")
# Exact clock notation, e.g. "01:30:00".
HOUR_MINUTE_CLOCK_PATTERN = re.compile(r"^(?P<h>\d+):(?P<m>\d+):00$")
# Exact text notation, e.g. "2h 15m", "2 15", "45min", or "2h".
HOUR_MINUTE_TEXT_PATTERN = re.compile(
    r"^(?:(?P<h>\d+)\s*(?:h|\s))?\s*(?:(?P<m>\d+)\s*(?:m|min)?)?$"
)

PART_PATTERN = re.compile(r"^Parte \d+: (.+)$")


def now():
    return datetime.now(tz=ZoneInfo(TIME_ZONE))


def midnight(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def to_string(dt: datetime) -> str:
    return dt.isoformat(sep=" ")


def to_datetime(dt: str, fmt: str | None = None) -> datetime:
    if fmt:
        return datetime.strptime(dt, fmt).replace(tzinfo=ZoneInfo(TIME_ZONE))
    return datetime.fromisoformat(dt).astimezone(ZoneInfo(TIME_ZONE))


def _parse_single_runtime_chunk(runtime: str) -> int | None:
    match = PART_PATTERN.match(runtime)
    if match:
        runtime = match.group(1)

    match = PRIME_PATTERN.match(runtime)
    if match:
        return int(match.group(1))

    for pattern in (HOUR_MINUTE_CLOCK_PATTERN, HOUR_MINUTE_TEXT_PATTERN):
        match = pattern.match(runtime)
        if match:
            hour = int(match.group("h") or 0)
            minutes = int(match.group("m") or 0)
            return hour * 60 + minutes

    return None


def string_to_runtime(runtime: str) -> int | None:
    runtime = runtime.strip()

    parts = [part.strip() for part in runtime.split("+")]
    if len(parts) > 1:
        parsed_parts = [_parse_single_runtime_chunk(part) for part in parts]
        if all(part is not None for part in parsed_parts):
            return sum(cast(list[int], parsed_parts))

    return _parse_single_runtime_chunk(runtime)


def runtime_to_string(minutes: int) -> str:
    h, m = divmod(minutes, 60)
    return f"{h:02d}:{m:02d}:00"
