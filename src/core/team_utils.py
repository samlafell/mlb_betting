"""
MLB Team Name Utilities

Provides team name normalization and abbreviation mapping for PostgreSQL integration.
Maps full team names to database-compatible abbreviations.
"""

# Comprehensive MLB team mapping
MLB_TEAM_MAPPINGS = {
    # American League East
    "Yankees": "NYY",
    "New York Yankees": "NYY",
    "NY Yankees": "NYY",
    "Red Sox": "BOS",
    "Boston Red Sox": "BOS",
    "Blue Jays": "TOR",
    "Toronto Blue Jays": "TOR",
    "Rays": "TB",
    "Tampa Bay Rays": "TB",
    "Orioles": "BAL",
    "Baltimore Orioles": "BAL",
    # American League Central
    "White Sox": "CWS",
    "Chicago White Sox": "CWS",
    "Guardians": "CLE",
    "Cleveland Guardians": "CLE",
    "Tigers": "DET",
    "Detroit Tigers": "DET",
    "Royals": "KC",
    "Kansas City Royals": "KC",
    "Twins": "MIN",
    "Minnesota Twins": "MIN",
    # American League West
    "Astros": "HOU",
    "Houston Astros": "HOU",
    "Angels": "LAA",
    "Los Angeles Angels": "LAA",
    "LA Angels": "LAA",
    "Athletics": "OAK",
    "Oakland Athletics": "OAK",
    "Mariners": "SEA",
    "Seattle Mariners": "SEA",
    "Rangers": "TEX",
    "Texas Rangers": "TEX",
    # National League East
    "Braves": "ATL",
    "Atlanta Braves": "ATL",
    "Marlins": "MIA",
    "Miami Marlins": "MIA",
    "Mets": "NYM",
    "New York Mets": "NYM",
    "NY Mets": "NYM",
    "Phillies": "PHI",
    "Philadelphia Phillies": "PHI",
    "Nationals": "WSH",
    "Washington Nationals": "WSH",
    # National League Central
    "Cubs": "CHC",
    "Chicago Cubs": "CHC",
    "Reds": "CIN",
    "Cincinnati Reds": "CIN",
    "Brewers": "MIL",
    "Milwaukee Brewers": "MIL",
    "Pirates": "PIT",
    "Pittsburgh Pirates": "PIT",
    "Cardinals": "STL",
    "St. Louis Cardinals": "STL",
    "Saint Louis Cardinals": "STL",
    # National League West
    "Diamondbacks": "ARI",
    "Arizona Diamondbacks": "ARI",
    "D-backs": "ARI",
    "Rockies": "COL",
    "Colorado Rockies": "COL",
    "Dodgers": "LAD",
    "Los Angeles Dodgers": "LAD",
    "LA Dodgers": "LAD",
    "Padres": "SD",
    "San Diego Padres": "SD",
    "Giants": "SF",
    "San Francisco Giants": "SF",
    # Common abbreviations that should map to themselves
    "NYY": "NYY",
    "BOS": "BOS",
    "TOR": "TOR",
    "TB": "TB",
    "BAL": "BAL",
    "CWS": "CWS",
    "CLE": "CLE",
    "DET": "DET",
    "KC": "KC",
    "MIN": "MIN",
    "HOU": "HOU",
    "LAA": "LAA",
    "OAK": "OAK",
    "SEA": "SEA",
    "TEX": "TEX",
    "ATL": "ATL",
    "MIA": "MIA",
    "NYM": "NYM",
    "PHI": "PHI",
    "WSH": "WSH",
    "CHC": "CHC",
    "CIN": "CIN",
    "MIL": "MIL",
    "PIT": "PIT",
    "STL": "STL",
    "ARI": "ARI",
    "COL": "COL",
    "LAD": "LAD",
    "SD": "SD",
    "SF": "SF",
}


def normalize_team_name(team_name: str) -> str:
    """
    Normalize team name to database-compatible abbreviation.

    Args:
        team_name: Full team name or abbreviation

    Returns:
        str: 3-character team abbreviation suitable for database

    Example:
        >>> normalize_team_name("Boston Red Sox")
        'BOS'
        >>> normalize_team_name("Red Sox")
        'BOS'
        >>> normalize_team_name("BOS")
        'BOS'
    """
    if not team_name or not isinstance(team_name, str):
        return "UNK"  # Unknown team fallback

    # Clean the input
    clean_name = team_name.strip()

    # Try exact match first
    if clean_name in MLB_TEAM_MAPPINGS:
        return MLB_TEAM_MAPPINGS[clean_name]

    # Try case-insensitive match
    for full_name, abbrev in MLB_TEAM_MAPPINGS.items():
        if clean_name.lower() == full_name.lower():
            return abbrev

    # Try partial matching (for cases like "Cubs" in "Chicago Cubs")
    for full_name, abbrev in MLB_TEAM_MAPPINGS.items():
        if (
            clean_name.lower() in full_name.lower()
            or full_name.lower() in clean_name.lower()
        ):
            return abbrev

    # If no match found, create a safe abbreviation
    # Take first 3 characters, uppercase
    safe_abbrev = clean_name[:3].upper()
    return safe_abbrev if len(safe_abbrev) == 3 else "UNK"


def validate_team_abbreviation(abbrev: str) -> bool:
    """
    Validate that team abbreviation is valid for database storage.

    Args:
        abbrev: Team abbreviation to validate

    Returns:
        bool: True if valid for database storage
    """
    if not abbrev or not isinstance(abbrev, str):
        return False

    # Must be 2-5 characters (database constraint)
    if len(abbrev) < 2 or len(abbrev) > 5:
        return False

    # Must be alphanumeric
    if not abbrev.isalnum():
        return False

    return True


def get_team_mapping_summary() -> dict[str, str]:
    """
    Get summary of team mappings for verification.

    Returns:
        dict: Team mappings organized by division
    """
    return {
        "al_east": {
            name: abbrev
            for name, abbrev in MLB_TEAM_MAPPINGS.items()
            if abbrev in ["NYY", "BOS", "TOR", "TB", "BAL"]
        },
        "al_central": {
            name: abbrev
            for name, abbrev in MLB_TEAM_MAPPINGS.items()
            if abbrev in ["CWS", "CLE", "DET", "KC", "MIN"]
        },
        "al_west": {
            name: abbrev
            for name, abbrev in MLB_TEAM_MAPPINGS.items()
            if abbrev in ["HOU", "LAA", "OAK", "SEA", "TEX"]
        },
        "nl_east": {
            name: abbrev
            for name, abbrev in MLB_TEAM_MAPPINGS.items()
            if abbrev in ["ATL", "MIA", "NYM", "PHI", "WSH"]
        },
        "nl_central": {
            name: abbrev
            for name, abbrev in MLB_TEAM_MAPPINGS.items()
            if abbrev in ["CHC", "CIN", "MIL", "PIT", "STL"]
        },
        "nl_west": {
            name: abbrev
            for name, abbrev in MLB_TEAM_MAPPINGS.items()
            if abbrev in ["ARI", "COL", "LAD", "SD", "SF"]
        },
    }


def create_external_source_id(
    source: str, game_id: str, book_id: str, bet_type: str
) -> str:
    """
    Create properly sized external source ID for database storage.

    Args:
        source: Data source (e.g., "ACTION_NETWORK")
        game_id: Game identifier
        book_id: Sportsbook identifier
        bet_type: Bet type (ml, spread, total)

    Returns:
        str: External source ID under 50 characters
    """
    # Abbreviate source name
    source_abbrev = {
        "ACTION_NETWORK": "AN",
        "SPORTSBETTING_REPORT": "SBR",
        "VSIN": "VSIN",
        "SBD": "SBD",
    }.get(source, source[:3])

    # Create compact ID: source_gameid_bookid_type
    external_id = f"{source_abbrev}_{game_id}_{book_id}_{bet_type}"

    # Ensure it fits in database constraint (50 chars)
    if len(external_id) > 50:
        # Truncate game_id if needed
        max_game_id_len = (
            50 - len(source_abbrev) - len(book_id) - len(bet_type) - 3
        )  # 3 underscores
        if max_game_id_len > 0:
            game_id = game_id[:max_game_id_len]
            external_id = f"{source_abbrev}_{game_id}_{book_id}_{bet_type}"
        else:
            # Fallback to very short format
            external_id = f"{source_abbrev}_{book_id}_{bet_type}"[:50]

    return external_id
