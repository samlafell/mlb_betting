"""
MLB Stats API integration service for correlating SportsbookReview data.

This service provides functionality to:
1. Fetch official MLB game data using proper datetime timestamps
2. Correlate SportsbookReview games with MLB Stats API games 
3. Handle double headers by using full datetime precision
4. Enrich betting data with official MLB information (weather, venue, etc.)
"""

import logging
import asyncio
import json
import os
import time
from datetime import datetime, date, timedelta, timezone
from typing import Dict, List, Optional, Any, Tuple
import aiohttp
from dataclasses import dataclass, asdict
from functools import lru_cache

from src.mlb_sharp_betting.models.game import Team

logger = logging.getLogger(__name__)

# MLB team mapping from SportsbookReview abbreviations to MLB team IDs
MLB_TEAM_MAPPING = {
    # American League East
    "BAL": 110,  # Baltimore Orioles
    "BOS": 111,  # Boston Red Sox
    "NYY": 147,  # New York Yankees
    "TB": 139,   # Tampa Bay Rays
    "TOR": 141,  # Toronto Blue Jays
    
    # American League Central
    "CHW": 145,  # Chicago White Sox
    "CLE": 114,  # Cleveland Guardians
    "DET": 116,  # Detroit Tigers
    "KC": 118,   # Kansas City Royals
    "MIN": 142,  # Minnesota Twins
    
    # American League West
    "HOU": 117,  # Houston Astros
    "LAA": 108,  # Los Angeles Angels
    "OAK": 133,  # Oakland Athletics
    "SEA": 136,  # Seattle Mariners
    "TEX": 140,  # Texas Rangers
    
    # National League East
    "ATL": 144,  # Atlanta Braves
    "MIA": 146,  # Miami Marlins
    "NYM": 121,  # New York Mets
    "PHI": 143,  # Philadelphia Phillies
    "WSH": 120,  # Washington Nationals
    
    # National League Central
    "CHC": 112,  # Chicago Cubs
    "CIN": 113,  # Cincinnati Reds
    "MIL": 158,  # Milwaukee Brewers
    "PIT": 134,  # Pittsburgh Pirates
    "STL": 138,  # St. Louis Cardinals
    
    # National League West
    "ARI": 109,  # Arizona Diamondbacks
    "COL": 115,  # Colorado Rockies
    "LAD": 119,  # Los Angeles Dodgers
    "SD": 135,   # San Diego Padres
    "SF": 137,   # San Francisco Giants
}

# Reverse mapping from MLB team ID to abbreviation
MLB_ID_TO_ABBREVIATION = {
    110: "BAL", 111: "BOS", 147: "NYY", 139: "TB", 141: "TOR",  # AL East
    145: "CHW", 114: "CLE", 116: "DET", 118: "KC", 142: "MIN",  # AL Central  
    117: "HOU", 108: "LAA", 133: "OAK", 136: "SEA", 140: "TEX", # AL West
    144: "ATL", 146: "MIA", 121: "NYM", 143: "PHI", 120: "WSH", # NL East
    112: "CHC", 113: "CIN", 158: "MIL", 134: "PIT", 138: "STL", # NL Central
    109: "ARI", 115: "COL", 119: "LAD", 135: "SD", 137: "SF"   # NL West
}

@dataclass
class MLBGameInfo:
    """Official MLB game information"""
    game_pk: str
    game_datetime: datetime
    home_team_id: int
    away_team_id: int
    home_team_name: str
    away_team_name: str
    venue_id: Optional[int] = None
    venue_name: Optional[str] = None
    weather: Optional[Dict[str, Any]] = None
    game_status: Optional[str] = None
    game_type: Optional[str] = None
    double_header: bool = False
    
@dataclass 
class CorrelationResult:
    """Result of correlating SBR game with MLB data"""
    confidence: float
    mlb_game: Optional[MLBGameInfo] = None
    match_reasons: List[str] = None
    
    def __post_init__(self):
        if self.match_reasons is None:
            self.match_reasons = []

@dataclass
class CacheEntry:
    """Cache entry with timestamp for TTL"""
    data: List[MLBGameInfo]
    timestamp: datetime
    
class MLBAPIService:
    """Service for integrating with MLB Stats API"""
    
    BASE_URL = "https://statsapi.mlb.com/api/v1"
    CACHE_TTL_HOURS = 1  # Cache TTL in hours
    CACHE_DIR = "data/mlb_api_cache"
    RATE_LIMIT_DELAY = 0.1  # Minimum seconds between API calls
    
    def __init__(self, enable_persistent_cache: bool = True):
        self.session: Optional[aiohttp.ClientSession] = None
        self._games_cache: Dict[str, CacheEntry] = {}  # In-memory cache
        self.enable_persistent_cache = enable_persistent_cache
        self._last_api_call = 0.0  # For rate limiting
        
        # Create cache directory if it doesn't exist
        if self.enable_persistent_cache:
            os.makedirs(self.CACHE_DIR, exist_ok=True)
        
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
            
    def _is_cache_valid(self, cache_entry: CacheEntry) -> bool:
        """Check if cache entry is still valid based on TTL"""
        age = datetime.now() - cache_entry.timestamp
        return age.total_seconds() / 3600 < self.CACHE_TTL_HOURS
        
    def _get_cache_file_path(self, date_str: str) -> str:
        """Get the file path for cached data"""
        return os.path.join(self.CACHE_DIR, f"games_{date_str}.json")
        
    def _save_to_persistent_cache(self, date_str: str, games: List[MLBGameInfo]):
        """Save games to persistent cache"""
        if not self.enable_persistent_cache:
            return
            
        try:
            cache_file = self._get_cache_file_path(date_str)
            cache_data = {
                "timestamp": datetime.now().isoformat(),
                "games": [asdict(game) for game in games]
            }
            
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, default=str, indent=2)
                
            logger.debug(f"Saved {len(games)} games to persistent cache: {cache_file}")
            
        except Exception as e:
            logger.warning(f"Failed to save persistent cache for {date_str}: {e}")
            
    def _load_from_persistent_cache(self, date_str: str) -> Optional[List[MLBGameInfo]]:
        """Load games from persistent cache if valid"""
        if not self.enable_persistent_cache:
            return None
            
        try:
            cache_file = self._get_cache_file_path(date_str)
            if not os.path.exists(cache_file):
                return None
                
            with open(cache_file, 'r') as f:
                cache_data = json.load(f)
                
            # Check if cache is still valid
            cache_timestamp = datetime.fromisoformat(cache_data["timestamp"])
            if not self._is_cache_valid(CacheEntry([], cache_timestamp)):
                logger.debug(f"Persistent cache expired for {date_str}")
                return None
                
            # Reconstruct MLBGameInfo objects
            games = []
            for game_dict in cache_data["games"]:
                # Convert datetime strings back to datetime objects
                if isinstance(game_dict["game_datetime"], str):
                    game_dict["game_datetime"] = datetime.fromisoformat(game_dict["game_datetime"])
                games.append(MLBGameInfo(**game_dict))
                
            logger.debug(f"Loaded {len(games)} games from persistent cache: {cache_file}")
            return games
            
        except Exception as e:
            logger.warning(f"Failed to load persistent cache for {date_str}: {e}")
            return None
            
    def clear_cache(self, include_persistent: bool = False):
        """Clear the games cache"""
        self._games_cache.clear()
        
        if include_persistent and self.enable_persistent_cache:
            try:
                for file in os.listdir(self.CACHE_DIR):
                    if file.startswith("games_") and file.endswith(".json"):
                        os.remove(os.path.join(self.CACHE_DIR, file))
                logger.debug("Persistent cache cleared")
            except Exception as e:
                logger.warning(f"Failed to clear persistent cache: {e}")
                
        logger.debug("MLB API cache cleared")
        
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        in_memory_games = sum(len(entry.data) for entry in self._games_cache.values())
        
        persistent_files = 0
        persistent_games = 0
        if self.enable_persistent_cache and os.path.exists(self.CACHE_DIR):
            try:
                cache_files = [f for f in os.listdir(self.CACHE_DIR) 
                              if f.startswith("games_") and f.endswith(".json")]
                persistent_files = len(cache_files)
                
                for file in cache_files:
                    try:
                        with open(os.path.join(self.CACHE_DIR, file), 'r') as f:
                            data = json.load(f)
                            persistent_games += len(data.get("games", []))
                    except:
                        continue
            except:
                pass
                
        return {
            "in_memory": {
                "cached_dates": list(self._games_cache.keys()),
                "total_games": in_memory_games,
                "cache_size": len(self._games_cache)
            },
            "persistent": {
                "cache_files": persistent_files,
                "total_games": persistent_games,
                "cache_dir": self.CACHE_DIR
            },
            "settings": {
                "ttl_hours": self.CACHE_TTL_HOURS,
                "persistent_enabled": self.enable_persistent_cache
            }
        }
            
    async def get_games_for_date(self, game_date: date) -> List[MLBGameInfo]:
        """
        Get all MLB games for a specific date.
        
        Args:
            game_date: Date to fetch games for
            
        Returns:
            List of MLB game information
        """
        # Format date for API (YYYY-MM-DD)
        date_str = game_date.strftime("%Y-%m-%d")
        
        # Check in-memory cache first
        if date_str in self._games_cache:
            cache_entry = self._games_cache[date_str]
            if self._is_cache_valid(cache_entry):
                logger.debug(f"Returning in-memory cached games for {date_str}")
                return cache_entry.data
            else:
                # Remove expired cache entry
                del self._games_cache[date_str]
                logger.debug(f"Expired in-memory cache for {date_str}")
        
        # Check persistent cache
        cached_games = self._load_from_persistent_cache(date_str)
        if cached_games is not None:
            # Add to in-memory cache
            self._games_cache[date_str] = CacheEntry(cached_games, datetime.now())
            return cached_games
            
        if not self.session:
            self.session = aiohttp.ClientSession()
            
        url = f"{self.BASE_URL}/schedule"
        params = {
            "sportId": "1",  # MLB
            "date": date_str,
            "hydrate": "game(content(editorial(recap))),weather,venue"
        }
        
        try:
            # Rate limiting - ensure minimum delay between API calls
            current_time = time.time()
            time_since_last_call = current_time - self._last_api_call
            if time_since_last_call < self.RATE_LIMIT_DELAY:
                await asyncio.sleep(self.RATE_LIMIT_DELAY - time_since_last_call)
            
            self._last_api_call = time.time()
            
            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                
                games = []
                for date_entry in data.get("dates", []):
                    for game_data in date_entry.get("games", []):
                        mlb_game = self._parse_mlb_game(game_data)
                        if mlb_game:
                            games.append(mlb_game)
                            
                # Cache the results both in-memory and persistently
                cache_entry = CacheEntry(games, datetime.now())
                self._games_cache[date_str] = cache_entry
                self._save_to_persistent_cache(date_str, games)
                
                logger.debug(f"Found {len(games)} MLB games for {date_str}")
                return games
                
        except Exception as e:
            logger.error(f"Error fetching MLB games for {date_str}: {e}")
            return []
            
    def _parse_mlb_game(self, game_data: Dict[str, Any]) -> Optional[MLBGameInfo]:
        """
        Parse MLB API game data into MLBGameInfo object.
        
        Args:
            game_data: Raw game data from MLB API
            
        Returns:
            Parsed MLB game info or None if parsing fails
        """
        try:
            logger.debug(f"Parsing game data: {type(game_data)}")
            # Parse game datetime (MLB API returns in UTC)
            game_datetime_str = game_data.get("gameDate")
            if not game_datetime_str:
                return None
                
            # Convert from UTC to EST (MLB games are typically shown in EST)
            game_datetime_utc = datetime.fromisoformat(game_datetime_str.replace('Z', '+00:00'))
            # Convert to EST (UTC-5)
            est_timezone = timezone(timedelta(hours=-5))
            game_datetime_est = game_datetime_utc.astimezone(est_timezone).replace(tzinfo=None)
            
            # Extract team information
            teams = game_data.get("teams", {})
            home_team_data = teams.get("home", {})
            away_team_data = teams.get("away", {})
            
            # Get team info from the nested structure
            home_team = home_team_data.get("team", {}) if isinstance(home_team_data, dict) else {}
            away_team = away_team_data.get("team", {}) if isinstance(away_team_data, dict) else {}
            
            # Get team IDs and convert to abbreviations
            home_team_id = home_team.get("id")
            away_team_id = away_team.get("id")
            home_team_abbr = MLB_ID_TO_ABBREVIATION.get(home_team_id, f"TEAM_{home_team_id}")
            away_team_abbr = MLB_ID_TO_ABBREVIATION.get(away_team_id, f"TEAM_{away_team_id}")
            
            # Extract venue information
            venue = game_data.get("venue", {})
            
            # Extract weather information
            weather_data = None
            if "weather" in game_data:
                weather = game_data["weather"]
                if isinstance(weather, dict):
                    weather_data = {
                        "condition": weather.get("condition"),
                        "temperature": weather.get("temp"),
                        "wind_speed": weather.get("wind", {}).get("speed") if isinstance(weather.get("wind"), dict) else None,
                        "wind_direction": weather.get("wind", {}).get("direction") if isinstance(weather.get("wind"), dict) else None,
                        "humidity": weather.get("humidity")
                    }
            
            # Check if this is a double header
            double_header = game_data.get("doubleHeader", "N") != "N"
            
            return MLBGameInfo(
                game_pk=str(game_data.get("gamePk")),
                game_datetime=game_datetime_est,
                home_team_id=home_team_id,
                away_team_id=away_team_id,
                home_team_name=home_team_abbr,
                away_team_name=away_team_abbr,
                venue_id=venue.get("id"),
                venue_name=venue.get("name"),
                weather=weather_data,
                game_status=game_data.get("status", {}).get("abstractGameState"),
                game_type=game_data.get("gameType"),
                double_header=double_header
            )
            
        except Exception as e:
            logger.error(f"Error parsing MLB game data: {e}")
            logger.error(f"Game data type: {type(game_data)}")
            logger.error(f"Game data: {game_data}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None
            
    async def correlate_game(
        self, 
        home_team: str, 
        away_team: str, 
        game_datetime: datetime,
        tolerance_hours: int = 6
    ) -> CorrelationResult:
        """
        Correlate a SportsbookReview game with MLB official data.
        
        Args:
            home_team: Home team abbreviation (e.g., "NYY")
            away_team: Away team abbreviation (e.g., "BOS") 
            game_datetime: Game datetime from SportsbookReview
            tolerance_hours: Hours of tolerance for datetime matching
            
        Returns:
            Correlation result with confidence score
        """
        # Convert team names to standardized format
        home_team_std = self._standardize_team_name(home_team)
        away_team_std = self._standardize_team_name(away_team)
        
        if not home_team_std or not away_team_std:
            logger.warning(f"Could not standardize team names: {home_team}, {away_team}")
            return CorrelationResult(confidence=0.0, match_reasons=["Unknown team names"])
        
        # Get MLB games for the date
        game_date = game_datetime.date()
        mlb_games = await self.get_games_for_date(game_date)
        
        if not mlb_games:
            return CorrelationResult(confidence=0.0, match_reasons=["No MLB games found for date"])
        
        # Find best matching game
        best_match = None
        best_confidence = 0.0
        best_reasons = []
        
        for mlb_game in mlb_games:
            confidence, reasons = self._calculate_match_confidence(
                home_team_std, away_team_std, game_datetime,
                mlb_game, tolerance_hours
            )
            
            if confidence > best_confidence:
                best_confidence = confidence
                best_match = mlb_game
                best_reasons = reasons
                
        return CorrelationResult(
            confidence=best_confidence,
            mlb_game=best_match,
            match_reasons=best_reasons
        )
        
    def _calculate_match_confidence(
        self,
        sbr_home: str,
        sbr_away: str, 
        sbr_datetime: datetime,
        mlb_game: MLBGameInfo,
        tolerance_hours: int
    ) -> Tuple[float, List[str]]:
        """
        Calculate confidence score for game matching.
        
        Args:
            sbr_home: SportsbookReview home team
            sbr_away: SportsbookReview away team
            sbr_datetime: SportsbookReview game datetime
            mlb_game: MLB game information
            tolerance_hours: Datetime tolerance in hours
            
        Returns:
            Tuple of (confidence_score, match_reasons)
        """
        confidence = 0.0
        reasons = []
        
        # Team matching (most important factor)
        if (mlb_game.home_team_name == sbr_home and 
            mlb_game.away_team_name == sbr_away):
            confidence += 0.7
            reasons.append("Exact team match")
        elif (mlb_game.home_team_name == sbr_away and 
              mlb_game.away_team_name == sbr_home):
            # Sometimes home/away can be flipped in different sources
            confidence += 0.5
            reasons.append("Reversed team match")
        else:
            # No team match - very low confidence
            confidence += 0.0
            reasons.append("No team match")
            
        # Datetime matching
        time_diff = abs((mlb_game.game_datetime - sbr_datetime).total_seconds() / 3600)
        if time_diff <= 1:  # Within 1 hour
            confidence += 0.25
            reasons.append(f"Exact time match ({time_diff:.1f}h diff)")
        elif time_diff <= tolerance_hours:
            # Gradually decrease confidence based on time difference
            time_confidence = 0.25 * (1 - (time_diff - 1) / (tolerance_hours - 1))
            confidence += time_confidence
            reasons.append(f"Close time match ({time_diff:.1f}h diff)")
        else:
            reasons.append(f"Time mismatch ({time_diff:.1f}h diff)")
            
        # Double header bonus - if SBR game is close in time to multiple MLB games
        if mlb_game.double_header:
            confidence += 0.05
            reasons.append("Double header detected")
            
        return confidence, reasons
        
    def _standardize_team_name(self, team_name: str) -> Optional[str]:
        """
        Standardize team name to match MLB API format.
        
        Args:
            team_name: Team name from SportsbookReview
            
        Returns:
            Standardized team name or None if not found
        """
        # Handle both string and Team enum inputs
        if isinstance(team_name, Team):
            team_str = team_name.value
        else:
            team_str = str(team_name).upper()
            
        # Direct mapping for most teams
        if team_str in MLB_TEAM_MAPPING:
            # Return the abbreviation that MLB API uses
            team_id = MLB_TEAM_MAPPING[team_str]
            # We need to reverse lookup the abbreviation
            # For now, return the input since most should match
            return team_str
            
        # Handle special cases or alternative abbreviations
        special_mappings = {
            "CWS": "CHW",  # Chicago White Sox
            "WSN": "WSH",  # Washington Nationals
            "SDP": "SD",   # San Diego Padres
            "SFG": "SF",   # San Francisco Giants
            "TBR": "TB",   # Tampa Bay Rays
            "LAD": "LAD",  # Los Angeles Dodgers
            "LAA": "LAA",  # Los Angeles Angels
        }
        
        return special_mappings.get(team_str)
        
    async def enrich_game_data(self, mlb_game_pk: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed game data for enrichment.
        
        Args:
            mlb_game_pk: MLB game primary key
            
        Returns:
            Detailed game data or None if not found
        """
        if not self.session:
            self.session = aiohttp.ClientSession()
            
        url = f"{self.BASE_URL}/game/{mlb_game_pk}/feed/live"
        
        try:
            # Rate limiting
            current_time = time.time()
            time_since_last_call = current_time - self._last_api_call
            if time_since_last_call < self.RATE_LIMIT_DELAY:
                await asyncio.sleep(self.RATE_LIMIT_DELAY - time_since_last_call)
            
            self._last_api_call = time.time()
            
            async with self.session.get(url) as response:
                response.raise_for_status()
                data = await response.json()
                
                # Extract relevant enrichment data
                game_data = data.get("gameData", {})
                
                enrichment = {
                    "venue": game_data.get("venue", {}),
                    "weather": game_data.get("weather", {}),
                    "status": game_data.get("status", {}),
                    "teams": game_data.get("teams", {}),
                    "datetime": game_data.get("datetime", {}),
                    "flags": game_data.get("flags", {})
                }
                
                return enrichment
                
        except Exception as e:
            logger.error(f"Error fetching detailed game data for {mlb_game_pk}: {e}")
            return None


# Convenience functions for common operations
async def correlate_sportsbookreview_game(
    home_team: str,
    away_team: str, 
    game_datetime: datetime
) -> CorrelationResult:
    """
    Convenience function to correlate a single game.
    
    Args:
        home_team: Home team abbreviation
        away_team: Away team abbreviation
        game_datetime: Game datetime
        
    Returns:
        Correlation result
    """
    async with MLBAPIService() as mlb_service:
        return await mlb_service.correlate_game(home_team, away_team, game_datetime)

async def get_mlb_games_for_date_range(
    start_date: date,
    end_date: date
) -> Dict[date, List[MLBGameInfo]]:
    """
    Get MLB games for a date range.
    
    Args:
        start_date: Start date
        end_date: End date
        
    Returns:
        Dictionary mapping dates to lists of games
    """
    games_by_date = {}
    
    async with MLBAPIService() as mlb_service:
        current_date = start_date
        while current_date <= end_date:
            games = await mlb_service.get_games_for_date(current_date)
            games_by_date[current_date] = games
            current_date += timedelta(days=1)
            
            # Add small delay to be respectful to MLB API
            await asyncio.sleep(0.1)
            
    return games_by_date 