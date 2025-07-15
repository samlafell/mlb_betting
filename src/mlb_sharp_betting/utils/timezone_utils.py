"""
Timezone utilities for consistent EST handling throughout the MLB Sharp Betting system.

Per user requirements:
- All times should be displayed in EST (Eastern Time)
- MLB APIs provide UTC times that need conversion to EST
- Database operations should handle timezone-naive vs timezone-aware datetime comparisons properly
"""

from datetime import datetime, timedelta, timezone

import pytz

# Constants
EST = pytz.timezone("US/Eastern")
UTC = pytz.timezone("UTC")


def now_est() -> datetime:
    """Get current time in EST timezone."""
    return datetime.now(EST)


def now_utc() -> datetime:
    """Get current time in UTC timezone."""
    return datetime.now(UTC)


def now_naive() -> datetime:
    """Get current time as timezone-naive datetime (for database comparisons)."""
    return datetime.now()


def ensure_est(dt: datetime | None) -> datetime | None:
    """
    Convert any datetime to EST timezone.

    Args:
        dt: Datetime object (timezone-aware or naive)

    Returns:
        Datetime in EST timezone, or None if input is None
    """
    if dt is None:
        return None

    if dt.tzinfo is None:
        # Assume naive datetime is in UTC and convert to EST
        dt = UTC.localize(dt)

    return dt.astimezone(EST)


def ensure_naive(dt: datetime | None) -> datetime | None:
    """
    Convert any datetime to timezone-naive datetime for database operations.

    Args:
        dt: Datetime object (timezone-aware or naive)

    Returns:
        Timezone-naive datetime, or None if input is None
    """
    if dt is None:
        return None

    if dt.tzinfo is not None:
        # Convert timezone-aware to naive by removing timezone info
        # This assumes the database stores times in the local timezone
        return dt.replace(tzinfo=None)

    return dt


def safe_datetime_diff(dt1: datetime, dt2: datetime) -> timedelta:
    """
    Safely calculate the difference between two datetime objects,
    handling timezone-aware vs timezone-naive mismatches.

    Args:
        dt1: First datetime (typically current time)
        dt2: Second datetime (typically from database)

    Returns:
        Timedelta representing the difference
    """
    # If both are timezone-aware or both are naive, subtract directly
    if (dt1.tzinfo is None) == (dt2.tzinfo is None):
        return dt1 - dt2

    # Handle mixed timezone situations
    if dt1.tzinfo is None and dt2.tzinfo is not None:
        # dt1 is naive, dt2 is aware - make dt1 aware
        dt1 = dt1.replace(tzinfo=timezone.utc)
    elif dt1.tzinfo is not None and dt2.tzinfo is None:
        # dt1 is aware, dt2 is naive - make dt2 aware
        dt2 = dt2.replace(tzinfo=timezone.utc)

    return dt1 - dt2


def format_est_time(
    dt: datetime | None, format_str: str = "%Y-%m-%d %H:%M EST"
) -> str:
    """
    Format a datetime in EST timezone with the specified format.

    Args:
        dt: Datetime object to format
        format_str: Format string for datetime formatting

    Returns:
        Formatted datetime string in EST, or "None" if dt is None
    """
    if dt is None:
        return "None"

    dt_est = ensure_est(dt)
    return dt_est.strftime(format_str)


def hours_ago(hours: float) -> datetime:
    """
    Get a timezone-naive datetime representing the specified number of hours ago.
    Useful for database queries.

    Args:
        hours: Number of hours ago

    Returns:
        Timezone-naive datetime
    """
    return datetime.now() - timedelta(hours=hours)


def hours_since(dt: datetime | None) -> float:
    """
    Calculate hours since the given datetime.
    Handles timezone-aware vs timezone-naive comparisons safely.

    Args:
        dt: Datetime to calculate hours since

    Returns:
        Hours since the datetime, or float('inf') if dt is None
    """
    if dt is None:
        return float("inf")

    diff = safe_datetime_diff(datetime.now(), dt)
    return diff.total_seconds() / 3600


def display_age(dt: datetime | None) -> str:
    """
    Display the age of a datetime in a human-readable format.

    Args:
        dt: Datetime to show age for

    Returns:
        Human-readable age string
    """
    if dt is None:
        return "Never"

    hours = hours_since(dt)

    if hours < 1:
        minutes = int(hours * 60)
        return f"{minutes} minutes ago"
    elif hours < 24:
        return f"{hours:.1f} hours ago"
    else:
        days = int(hours / 24)
        return f"{days} days ago"
