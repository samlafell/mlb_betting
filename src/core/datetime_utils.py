"""
Enhanced DateTime Utilities for PostgreSQL Integration

Provides proper datetime parsing and timezone conversion utilities specifically
designed for PostgreSQL insertion with EST timezone handling.

Key Features:
- Convert ISO strings to proper datetime objects for PostgreSQL
- Handle UTC to EST timezone conversion
- Standardized datetime formatting for database operations
- Safe parsing with error handling
"""

from datetime import datetime, timezone

import pytz

# Project timezone constant
EST = pytz.timezone("US/Eastern")


def parse_iso_to_datetime(
    iso_string: str, target_tz: pytz.BaseTzInfo | None = None
) -> datetime:
    """
    Parse ISO datetime string to proper datetime object for PostgreSQL.

    Args:
        iso_string: ISO format datetime string (e.g., '2025-07-18T02:24:41.714814+00:00')
        target_tz: Target timezone (defaults to EST)

    Returns:
        datetime: Properly parsed datetime object with timezone

    Example:
        >>> iso_str = '2025-07-18T02:24:41.714814+00:00'
        >>> dt = parse_iso_to_datetime(iso_str)
        >>> print(dt)  # 2025-07-17 22:24:41.714814-04:00 (EST)
    """
    if target_tz is None:
        target_tz = EST

    try:
        # Handle various ISO formats
        if iso_string.endswith("Z"):
            iso_string = iso_string[:-1] + "+00:00"

        # Parse the ISO string
        dt = datetime.fromisoformat(iso_string)

        # Convert to target timezone
        if dt.tzinfo is None:
            # Naive datetime - assume UTC and convert to target
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone(target_tz)

    except (ValueError, TypeError) as e:
        raise ValueError(f"Failed to parse ISO datetime string '{iso_string}': {e}")


def utc_to_est(dt: datetime | str) -> datetime:
    """
    Convert UTC datetime (string or object) to EST datetime object.

    Args:
        dt: UTC datetime as string or datetime object

    Returns:
        datetime: EST datetime object suitable for PostgreSQL
    """
    if isinstance(dt, str):
        dt = parse_iso_to_datetime(dt, target_tz=timezone.utc)

    if dt.tzinfo is None:
        # Naive datetime - assume UTC
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(EST)


def now_est() -> datetime:
    """Get current EST time as datetime object."""
    return datetime.now(EST)


def prepare_for_postgres(dt: datetime | str) -> datetime:
    """
    Prepare datetime for PostgreSQL insertion.

    Converts any datetime input to a timezone-aware EST datetime object
    that PostgreSQL can properly handle.

    Args:
        dt: Datetime as string or datetime object

    Returns:
        datetime: PostgreSQL-ready EST datetime object
    """
    if isinstance(dt, str):
        return parse_iso_to_datetime(dt, target_tz=EST)
    elif isinstance(dt, datetime):
        if dt.tzinfo is None:
            # Naive datetime - assume EST per project requirements
            return EST.localize(dt)
        else:
            # Convert to EST
            return dt.astimezone(EST)
    else:
        raise TypeError(f"Expected datetime or string, got {type(dt)}")


def format_for_display(dt: datetime | str) -> str:
    """
    Format datetime for human-readable display in EST.

    Args:
        dt: Datetime as string or datetime object

    Returns:
        str: Formatted datetime string in EST
    """
    if isinstance(dt, str):
        dt = parse_iso_to_datetime(dt)
    elif dt.tzinfo is None:
        dt = EST.localize(dt)
    else:
        dt = dt.astimezone(EST)

    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")


def safe_game_datetime_parse(game_datetime: str | datetime | None) -> datetime | None:
    """
    Safely parse game datetime from various formats.

    Handles common game datetime formats from APIs and converts to EST.

    Args:
        game_datetime: Game datetime in various formats

    Returns:
        datetime: EST datetime object or None if parsing fails
    """
    if game_datetime is None:
        return None

    try:
        if isinstance(game_datetime, str):
            # Handle common API formats
            if "T" in game_datetime:
                # ISO format
                return parse_iso_to_datetime(game_datetime)
            else:
                # Try standard formats
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
                    try:
                        dt = datetime.strptime(game_datetime, fmt)
                        return EST.localize(dt)
                    except ValueError:
                        continue

                # If all formats fail, try fromisoformat as last resort
                return parse_iso_to_datetime(game_datetime)

        elif isinstance(game_datetime, datetime):
            return prepare_for_postgres(game_datetime)

        else:
            return None

    except Exception:
        # Return None for any parsing errors
        return None


# Convenience functions for common operations
def collection_timestamp() -> datetime:
    """Get current EST timestamp for data collection."""
    return now_est()


def ensure_est_datetime(dt: datetime | str | None) -> datetime | None:
    """
    Ensure datetime is in EST timezone, handling None safely.

    Args:
        dt: Datetime in any format or None

    Returns:
        datetime: EST datetime object or None
    """
    if dt is None:
        return None

    try:
        return prepare_for_postgres(dt)
    except Exception:
        return None
