from datetime import datetime, timezone

UTC = timezone.utc


def to_utc(dt: datetime | None) -> datetime | None:
    """Convert any datetime to UTC timezone-aware datetime."""
    if dt is None:
        return None

    if dt.tzinfo is None:
        # Assume naive datetime is already in UTC
        return dt.replace(tzinfo=UTC)
    else:
        # Convert timezone-aware datetime to UTC
        return dt.astimezone(UTC)


def utc_now() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(UTC)
