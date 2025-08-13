"""
MLB Team Name Utilities

Provides team name normalization, abbreviation mapping, and advanced team resolution
for PostgreSQL integration. Includes both simple normalization and multi-strategy
team name population from various data sources.
"""

import asyncio
from typing import Dict, Any, Optional, TypedDict
from dataclasses import dataclass


class TeamResolutionError(Exception):
    """Raised when team resolution fails."""
    pass


@dataclass
class TeamInfo:
    """Team information result."""
    home_team: str
    away_team: str
    source: str  # Which resolution strategy was used
    confidence: float = 1.0


# Multi-strategy team name resolution functions

async def populate_team_names(
    external_game_id: str,
    raw_data: Optional[Dict[str, Any]] = None,
    mlb_stats_api_game_id: Optional[str] = None
) -> TeamInfo:
    """
    Populate team names using multiple resolution strategies.
    
    Args:
        external_game_id: External game identifier
        raw_data: Raw data from data source
        mlb_stats_api_game_id: MLB Stats API game ID
        
    Returns:
        TeamInfo: Resolved team information
        
    Raises:
        TeamResolutionError: If team resolution fails
    """
    strategies = [
        _extract_from_raw_data_direct,
        _extract_from_raw_data_game_object,
        _extract_from_raw_data_teams_array,
        _extract_from_raw_data_pattern_inference
    ]
    
    for strategy in strategies:
        try:
            result = await strategy(external_game_id, raw_data, mlb_stats_api_game_id)
            if result:
                return result
        except Exception as e:
            continue  # Try next strategy
    
    raise TeamResolutionError(f"Failed to resolve team names for game {external_game_id}")


async def _extract_from_raw_data_direct(
    external_game_id: str,
    raw_data: Optional[Dict[str, Any]],
    mlb_stats_api_game_id: Optional[str]
) -> Optional[TeamInfo]:
    """Extract team names from direct home_team/away_team fields."""
    if not raw_data:
        return None
        
    home_team = raw_data.get('home_team')
    away_team = raw_data.get('away_team')
    
    if home_team and away_team:
        return TeamInfo(
            home_team=normalize_team_name(str(home_team)),
            away_team=normalize_team_name(str(away_team)),
            source='raw_data_direct',
            confidence=0.9
        )
    
    return None


async def _extract_from_raw_data_game_object(
    external_game_id: str,
    raw_data: Optional[Dict[str, Any]],
    mlb_stats_api_game_id: Optional[str]
) -> Optional[TeamInfo]:
    """Extract team names from game object structure."""
    if not raw_data:
        return None
        
    game_obj = raw_data.get('game', {})
    if not isinstance(game_obj, dict):
        return None
        
    home_team = game_obj.get('home_team')
    away_team = game_obj.get('away_team')
    
    if home_team and away_team:
        return TeamInfo(
            home_team=normalize_team_name(str(home_team)),
            away_team=normalize_team_name(str(away_team)),
            source='raw_data_game_object',
            confidence=0.8
        )
    
    return None


async def _extract_from_raw_data_teams_array(
    external_game_id: str,
    raw_data: Optional[Dict[str, Any]],
    mlb_stats_api_game_id: Optional[str]
) -> Optional[TeamInfo]:
    """Extract team names from teams array structure."""
    if not raw_data:
        return None
        
    teams = raw_data.get('teams', [])
    if not isinstance(teams, list) or len(teams) < 2:
        return None
        
    home_team = None
    away_team = None
    
    for team in teams:
        if isinstance(team, dict):
            if team.get('is_home'):
                home_team = team.get('name')
            else:
                away_team = team.get('name')
    
    if home_team and away_team:
        return TeamInfo(
            home_team=normalize_team_name(str(home_team)),
            away_team=normalize_team_name(str(away_team)),
            source='raw_data_teams_array',
            confidence=0.7
        )
    
    return None


async def _extract_from_raw_data_pattern_inference(
    external_game_id: str,
    raw_data: Optional[Dict[str, Any]],
    mlb_stats_api_game_id: Optional[str]
) -> Optional[TeamInfo]:
    """Extract team names using pattern inference."""
    if not raw_data:
        return None
        
    # Look for team names in various field patterns
    team_fields = []
    
    def extract_team_like_fields(obj, prefix=''):
        if isinstance(obj, dict):
            for key, value in obj.items():
                full_key = f"{prefix}.{key}" if prefix else key
                if any(word in key.lower() for word in ['team', 'home', 'away']):
                    if isinstance(value, str) and len(value) > 2:
                        team_fields.append((full_key, value))
                elif isinstance(value, (dict, list)):
                    extract_team_like_fields(value, full_key)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                extract_team_like_fields(item, f"{prefix}[{i}]")
    
    extract_team_like_fields(raw_data)
    
    # Simple heuristic: if we found exactly 2 team-like fields, use them
    if len(team_fields) == 2:
        team1_name = normalize_team_name(team_fields[0][1])
        team2_name = normalize_team_name(team_fields[1][1])
        
        # Determine home/away (simple heuristic)
        if 'home' in team_fields[0][0].lower():
            home_team, away_team = team1_name, team2_name
        elif 'away' in team_fields[0][0].lower():
            home_team, away_team = team2_name, team1_name
        else:
            # Default assignment
            home_team, away_team = team1_name, team2_name
        
        return TeamInfo(
            home_team=home_team,
            away_team=away_team,
            source='pattern_inference',
            confidence=0.5
        )
    
    return None


def validate_team_names(home_team: str, away_team: str) -> bool:
    """
    Validate that team names are reasonable.
    
    Args:
        home_team: Home team name
        away_team: Away team name
        
    Returns:
        bool: True if team names appear valid
    """
    if not home_team or not away_team:
        return False
        
    if home_team == away_team:
        return False
        
    # Check if teams are in known MLB teams
    known_teams = set(MLB_TEAM_MAPPINGS.values())
    home_normalized = normalize_team_name(home_team)
    away_normalized = normalize_team_name(away_team)
    
    # If both teams map to known abbreviations, they're valid
    if home_normalized in known_teams and away_normalized in known_teams:
        return True
        
    # If they don't contain "unknown" they're probably valid
    return 'unknown' not in home_team.lower() and 'unknown' not in away_team.lower()


# Original simple normalization functions below

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
    # Enhanced input validation
    if not team_name or not isinstance(team_name, str):
        return "UNK"  # Unknown team fallback

    # Security: Prevent extremely long input strings that could cause issues
    if len(team_name) > 100:
        return "UNK"  # Reject suspiciously long inputs

    # Clean the input
    clean_name = team_name.strip()

    # Handle empty string after stripping
    if not clean_name:
        return "UNK"

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
