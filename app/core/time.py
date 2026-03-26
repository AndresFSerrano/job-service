from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from zoneinfo import ZoneInfoNotFoundError


try:
    BOGOTA_TZ = ZoneInfo("America/Bogota")
except ZoneInfoNotFoundError:
    BOGOTA_TZ = timezone(timedelta(hours=-5), name="America/Bogota")


def now_bogota_iso() -> str:
    return datetime.now(BOGOTA_TZ).isoformat()


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=BOGOTA_TZ)
    return parsed
