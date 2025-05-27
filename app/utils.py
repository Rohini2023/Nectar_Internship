from datetime import datetime, timezone, timedelta, time

# Define UAE timezone (UTC+4)
UAE_TZ = timezone(timedelta(hours=4))

def ensure_utc_datetime(dt: datetime) -> datetime:
    """Ensure the datetime is in UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def to_uae_time(dt: datetime) -> datetime:
    """Convert datetime to UAE time (UTC+4)."""
    return ensure_utc_datetime(dt).astimezone(UAE_TZ)

def convert_utc_to_uae(dt_utc: datetime) -> datetime:
    """Convert a UTC datetime to UAE timezone."""
    return ensure_utc_datetime(dt_utc).astimezone(UAE_TZ)

def to_uae_midnight(date_input) -> datetime:
    """
    Convert a date or datetime to UAE midnight (00:00:00+04:00).
    Args:
        date_input: A datetime or date object
    Returns:
        datetime object at midnight in UAE timezone
    """
    if isinstance(date_input, datetime):
        date_input = to_uae_time(date_input).date()
    return datetime.combine(date_input, time.min, tzinfo=UAE_TZ)
