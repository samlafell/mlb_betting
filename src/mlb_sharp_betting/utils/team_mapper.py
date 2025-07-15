"""
Team name normalization and mapping utilities.

This module provides functionality to normalize team names from various
sources and formats to standardized team codes.
"""

import re
from difflib import SequenceMatcher

import structlog

from ..models.game import Team

logger = structlog.get_logger(__name__)


class TeamMapper:
    """
    Utility class for normalizing and mapping team names.

    Handles various team name formats, abbreviations, and aliases
    to map them to standardized Team enum values.
    """

    def __init__(self) -> None:
        """Initialize team mapper with comprehensive mappings."""
        self.logger = logger.bind(component="TeamMapper")

        # Build comprehensive team mappings
        self._build_team_mappings()

        # Cache for performance
        self._normalization_cache: dict[str, str | None] = {}

    def _build_team_mappings(self) -> None:
        """Build comprehensive team name mappings."""

        # Full team names to abbreviations
        self.full_names = {
            # American League East
            "BALTIMORE ORIOLES": Team.BAL.value,
            "BOSTON RED SOX": Team.BOS.value,
            "NEW YORK YANKEES": Team.NYY.value,
            "TAMPA BAY RAYS": Team.TB.value,
            "TORONTO BLUE JAYS": Team.TOR.value,
            # American League Central
            "CHICAGO WHITE SOX": Team.CWS.value,
            "CLEVELAND GUARDIANS": Team.CLE.value,
            "DETROIT TIGERS": Team.DET.value,
            "KANSAS CITY ROYALS": Team.KC.value,
            "MINNESOTA TWINS": Team.MIN.value,
            # American League West
            "HOUSTON ASTROS": Team.HOU.value,
            "LOS ANGELES ANGELS": Team.LAA.value,
            "OAKLAND ATHLETICS": Team.OAK.value,
            "SEATTLE MARINERS": Team.SEA.value,
            "TEXAS RANGERS": Team.TEX.value,
            # National League East
            "ATLANTA BRAVES": Team.ATL.value,
            "MIAMI MARLINS": Team.MIA.value,
            "NEW YORK METS": Team.NYM.value,
            "PHILADELPHIA PHILLIES": Team.PHI.value,
            "WASHINGTON NATIONALS": Team.WSH.value,
            # National League Central
            "CHICAGO CUBS": Team.CHC.value,
            "CINCINNATI REDS": Team.CIN.value,
            "MILWAUKEE BREWERS": Team.MIL.value,
            "PITTSBURGH PIRATES": Team.PIT.value,
            "ST. LOUIS CARDINALS": Team.STL.value,
            # National League West
            "ARIZONA DIAMONDBACKS": Team.ARI.value,
            "COLORADO ROCKIES": Team.COL.value,
            "LOS ANGELES DODGERS": Team.LAD.value,
            "SAN DIEGO PADRES": Team.SD.value,
            "SAN FRANCISCO GIANTS": Team.SF.value,
        }

        # Common abbreviation variations
        self.abbreviation_aliases = {
            # Standard abbreviations
            "BAL": Team.BAL.value,
            "BOS": Team.BOS.value,
            "NYY": Team.NYY.value,
            "TB": Team.TB.value,
            "TOR": Team.TOR.value,
            "CWS": Team.CWS.value,
            "CLE": Team.CLE.value,
            "DET": Team.DET.value,
            "KC": Team.KC.value,
            "MIN": Team.MIN.value,
            "HOU": Team.HOU.value,
            "LAA": Team.LAA.value,
            "OAK": Team.OAK.value,
            "SEA": Team.SEA.value,
            "TEX": Team.TEX.value,
            "ATL": Team.ATL.value,
            "MIA": Team.MIA.value,
            "NYM": Team.NYM.value,
            "PHI": Team.PHI.value,
            "WSH": Team.WSH.value,
            "CHC": Team.CHC.value,
            "CIN": Team.CIN.value,
            "MIL": Team.MIL.value,
            "PIT": Team.PIT.value,
            "STL": Team.STL.value,
            "ARI": Team.ARI.value,
            "COL": Team.COL.value,
            "LAD": Team.LAD.value,
            "SD": Team.SD.value,
            "SF": Team.SF.value,
            # Alternative abbreviations
            "WSN": Team.WSH.value,  # Washington Nationals (alternative)
            "WAS": Team.WSH.value,  # Washington (alternative)
            "CHW": Team.CWS.value,  # Chicago White Sox (alternative)
            "ANA": Team.LAA.value,  # Los Angeles Angels (former Anaheim)
            "LAA": Team.LAA.value,  # Los Angeles Angels
            "TBR": Team.TB.value,  # Tampa Bay Rays (alternative)
            "TBD": Team.TB.value,  # Tampa Bay Devil Rays (former)
            "CLE": Team.CLE.value,  # Cleveland Guardians
            "CLV": Team.CLE.value,  # Cleveland (alternative)
        }

        # City name mappings
        self.city_names = {
            # Primary cities
            "BALTIMORE": Team.BAL.value,
            "BOSTON": Team.BOS.value,
            "NEW YORK": None,  # Ambiguous - need more context
            "TAMPA BAY": Team.TB.value,
            "TORONTO": Team.TOR.value,
            "CHICAGO": None,  # Ambiguous - Cubs or White Sox
            "CLEVELAND": Team.CLE.value,
            "DETROIT": Team.DET.value,
            "KANSAS CITY": Team.KC.value,
            "MINNESOTA": Team.MIN.value,
            "HOUSTON": Team.HOU.value,
            "LOS ANGELES": None,  # Ambiguous - Angels or Dodgers
            "OAKLAND": Team.OAK.value,
            "SEATTLE": Team.SEA.value,
            "TEXAS": Team.TEX.value,
            "ATLANTA": Team.ATL.value,
            "MIAMI": Team.MIA.value,
            "PHILADELPHIA": Team.PHI.value,
            "WASHINGTON": Team.WSH.value,
            "CINCINNATI": Team.CIN.value,
            "MILWAUKEE": Team.MIL.value,
            "PITTSBURGH": Team.PIT.value,
            "ST. LOUIS": Team.STL.value,
            "ARIZONA": Team.ARI.value,
            "COLORADO": Team.COL.value,
            "SAN DIEGO": Team.SD.value,
            "SAN FRANCISCO": Team.SF.value,
        }

        # Nickname/mascot mappings
        self.nicknames = {
            "ORIOLES": Team.BAL.value,
            "RED SOX": Team.BOS.value,
            "YANKEES": Team.NYY.value,
            "RAYS": Team.TB.value,
            "BLUE JAYS": Team.TOR.value,
            "WHITE SOX": Team.CWS.value,
            "GUARDIANS": Team.CLE.value,
            "TIGERS": Team.DET.value,
            "ROYALS": Team.KC.value,
            "TWINS": Team.MIN.value,
            "ASTROS": Team.HOU.value,
            "ANGELS": Team.LAA.value,
            "ATHLETICS": Team.OAK.value,
            "MARINERS": Team.SEA.value,
            "RANGERS": Team.TEX.value,
            "BRAVES": Team.ATL.value,
            "MARLINS": Team.MIA.value,
            "METS": Team.NYM.value,
            "PHILLIES": Team.PHI.value,
            "NATIONALS": Team.WSH.value,
            "CUBS": Team.CHC.value,
            "REDS": Team.CIN.value,
            "BREWERS": Team.MIL.value,
            "PIRATES": Team.PIT.value,
            "CARDINALS": Team.STL.value,
            "DIAMONDBACKS": Team.ARI.value,
            "ROCKIES": Team.COL.value,
            "DODGERS": Team.LAD.value,
            "PADRES": Team.SD.value,
            "GIANTS": Team.SF.value,
            # Shortened nicknames
            "O'S": Team.BAL.value,
            "SOX": None,  # Ambiguous - Red Sox or White Sox
            "YANKS": Team.NYY.value,
            "JAYS": Team.TOR.value,
            "A'S": Team.OAK.value,
            "NATS": Team.WSH.value,
            "CARDS": Team.STL.value,
            "DBACKS": Team.ARI.value,
        }

        # Historical/alternative names
        self.historical_names = {
            "DEVIL RAYS": Team.TB.value,
            "ANAHEIM ANGELS": Team.LAA.value,
            "CALIFORNIA ANGELS": Team.LAA.value,
            "CLEVELAND INDIANS": Team.CLE.value,
            "FLORIDA MARLINS": Team.MIA.value,
            "MONTREAL EXPOS": Team.WSH.value,  # Moved to Washington
        }

        # Create combined mapping for faster lookups
        self.all_mappings = {}
        self.all_mappings.update(self.full_names)
        self.all_mappings.update(self.abbreviation_aliases)
        self.all_mappings.update(
            {k: v for k, v in self.city_names.items() if v is not None}
        )
        self.all_mappings.update(self.nicknames)
        self.all_mappings.update(self.historical_names)

    def normalize_team_name(self, team_name: str) -> str | None:
        """
        Normalize a team name to standard abbreviation.

        Args:
            team_name: Team name in any format

        Returns:
            Standardized team abbreviation or None if not found
        """
        if not team_name or not isinstance(team_name, str):
            return None

        # Check cache first
        cache_key = team_name.upper().strip()
        if cache_key in self._normalization_cache:
            return self._normalization_cache[cache_key]

        # Clean and normalize input
        cleaned_name = self._clean_team_name(team_name)

        # Try exact match first
        result = self._exact_match(cleaned_name)
        if result:
            self._normalization_cache[cache_key] = result
            return result

        # Try fuzzy matching
        result = self._fuzzy_match(cleaned_name)
        if result:
            self._normalization_cache[cache_key] = result
            return result

        # Try parsing compound names
        result = self._parse_compound_name(cleaned_name)
        if result:
            self._normalization_cache[cache_key] = result
            return result

        # Log failed normalization
        self.logger.debug("Failed to normalize team name", team_name=team_name)
        self._normalization_cache[cache_key] = None
        return None

    def _clean_team_name(self, team_name: str) -> str:
        """Clean and standardize team name format."""
        # Convert to uppercase and strip
        cleaned = team_name.upper().strip()

        # Remove common prefixes/suffixes
        prefixes = ["THE ", "TEAM ", "MLB "]
        for prefix in prefixes:
            if cleaned.startswith(prefix):
                cleaned = cleaned[len(prefix) :]

        # Remove punctuation and extra spaces
        cleaned = re.sub(r"[^\w\s]", " ", cleaned)
        cleaned = re.sub(r"\s+", " ", cleaned)
        cleaned = cleaned.strip()

        return cleaned

    def _exact_match(self, cleaned_name: str) -> str | None:
        """Try exact matching against all mappings."""
        return self.all_mappings.get(cleaned_name)

    def _fuzzy_match(self, cleaned_name: str, threshold: float = 0.8) -> str | None:
        """
        Try fuzzy matching against team names.

        Args:
            cleaned_name: Cleaned team name
            threshold: Similarity threshold (0-1)

        Returns:
            Best match or None if no good match found
        """
        best_match = None
        best_score = 0

        for known_name, team_code in self.all_mappings.items():
            if team_code is None:  # Skip ambiguous mappings
                continue

            # Calculate similarity
            similarity = SequenceMatcher(None, cleaned_name, known_name).ratio()

            if similarity > threshold and similarity > best_score:
                best_score = similarity
                best_match = team_code

        return best_match

    def _parse_compound_name(self, cleaned_name: str) -> str | None:
        """
        Try to parse compound team names (e.g., "Yankees Red Sox").

        Args:
            cleaned_name: Cleaned team name

        Returns:
            Team code if single team identified, None otherwise
        """
        words = cleaned_name.split()
        if len(words) < 2:
            return None

        # Try different word combinations
        found_teams = set()

        # Check each word individually
        for word in words:
            if word in self.all_mappings:
                team = self.all_mappings[word]
                if team:
                    found_teams.add(team)

        # Check two-word combinations
        for i in range(len(words) - 1):
            two_word = f"{words[i]} {words[i + 1]}"
            if two_word in self.all_mappings:
                team = self.all_mappings[two_word]
                if team:
                    found_teams.add(team)

        # Check three-word combinations
        for i in range(len(words) - 2):
            three_word = f"{words[i]} {words[i + 1]} {words[i + 2]}"
            if three_word in self.all_mappings:
                team = self.all_mappings[three_word]
                if team:
                    found_teams.add(team)

        # Return team only if exactly one found
        if len(found_teams) == 1:
            return list(found_teams)[0]

        return None

    def parse_matchup(self, matchup_text: str) -> dict[str, str] | None:
        """
        Parse a matchup string to extract home and away teams.

        Args:
            matchup_text: Matchup text (e.g., "Yankees @ Red Sox")

        Returns:
            Dictionary with 'home' and 'away' team codes or None
        """
        if not matchup_text:
            return None

        # Common separators for away @ home format
        separators = ["@", " at ", " AT ", " vs ", " VS ", " v ", " V "]

        for separator in separators:
            if separator in matchup_text:
                parts = matchup_text.split(separator)
                if len(parts) == 2:
                    away_text = parts[0].strip()
                    home_text = parts[1].strip()

                    away_team = self.normalize_team_name(away_text)
                    home_team = self.normalize_team_name(home_text)

                    if away_team and home_team:
                        return {"away": away_team, "home": home_team}

        # Try without separator (space-separated)
        words = matchup_text.split()
        if len(words) >= 2:
            # Try different split points
            for i in range(1, len(words)):
                away_text = " ".join(words[:i])
                home_text = " ".join(words[i:])

                away_team = self.normalize_team_name(away_text)
                home_team = self.normalize_team_name(home_text)

                if away_team and home_team:
                    return {"away": away_team, "home": home_team}

        return None

    def get_team_display_name(self, team_code: str) -> str | None:
        """
        Get display name for a team code.

        Args:
            team_code: Team abbreviation

        Returns:
            Full team name or None if not found
        """
        try:
            team = Team(team_code)
            return Team.get_team_name(team_code)
        except ValueError:
            return None

    def validate_team_code(self, team_code: str) -> bool:
        """
        Validate if a team code is valid.

        Args:
            team_code: Team abbreviation to validate

        Returns:
            True if valid, False otherwise
        """
        try:
            Team(team_code)
            return True
        except ValueError:
            return False

    def get_all_team_codes(self) -> list[str]:
        """Get list of all valid team codes."""
        return [team.value for team in Team]

    def get_team_variations(self, team_code: str) -> list[str]:
        """
        Get all known variations of a team name.

        Args:
            team_code: Team abbreviation

        Returns:
            List of all known variations for this team
        """
        if not self.validate_team_code(team_code):
            return []

        variations = []

        # Find all mappings that point to this team
        for name, code in self.all_mappings.items():
            if code == team_code:
                variations.append(name)

        return variations

    def clear_cache(self) -> None:
        """Clear the normalization cache."""
        self._normalization_cache.clear()
        self.logger.debug("Team mapper cache cleared")

    def get_cache_stats(self) -> dict[str, int]:
        """Get cache statistics."""
        return {
            "cache_size": len(self._normalization_cache),
            "total_mappings": len(self.all_mappings),
        }


# Global instance for convenience
_team_mapper = None


def get_team_mapper() -> TeamMapper:
    """Get global TeamMapper instance."""
    global _team_mapper
    if _team_mapper is None:
        _team_mapper = TeamMapper()
    return _team_mapper


# Convenience functions
def normalize_team_name(team_name: str) -> str | None:
    """
    Convenience function to normalize a team name.

    Args:
        team_name: Team name in any format

    Returns:
        Standardized team abbreviation or None if not found
    """
    return get_team_mapper().normalize_team_name(team_name)


def parse_matchup(matchup_text: str) -> dict[str, str] | None:
    """
    Convenience function to parse a matchup string.

    Args:
        matchup_text: Matchup text (e.g., "Yankees @ Red Sox")

    Returns:
        Dictionary with 'home' and 'away' team codes or None
    """
    return get_team_mapper().parse_matchup(matchup_text)


def validate_team_code(team_code: str) -> bool:
    """
    Convenience function to validate a team code.

    Args:
        team_code: Team abbreviation to validate

    Returns:
        True if valid, False otherwise
    """
    return get_team_mapper().validate_team_code(team_code)


__all__ = [
    "TeamMapper",
    "get_team_mapper",
    "normalize_team_name",
    "parse_matchup",
    "validate_team_code",
]
