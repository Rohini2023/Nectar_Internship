from datetime import datetime, timezone, timedelta,

def ensure_utc_datetime(dt):
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def to_uae_time(dt):
    return dt.astimezone(timezone(timedelta(hours=4)))
def convert_utc_to_uae(dt_utc: datetime) -> datetime:
    """
    Convert a UTC datetime to UAE timezone.
    
    Args:
        dt_utc (datetime): A timezone-aware or naive UTC datetime.

    Returns:
        datetime: Datetime converted to UAE timezone.
    """
    if dt_utc.tzinfo is None:
        # Assume naive datetime is in UTC
        dt_utc = UTC.localize(dt_utc)
    return dt_utc.astimezone(uae_tz)


def to_uae_midnight(date_input):
    """Convert date/datetime to UAE midnight (00:00:00+04:00)."""
    if isinstance(date_input, datetime):
        if date_input.tzinfo:
            date_input = date_input.astimezone(uae_tz).date()
        else:
            date_input = date_input.date()
    return uae_tz.localize(datetime.combine(date_input, time.min))