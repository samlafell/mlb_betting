"""MLB Stats API service for getting official game information."""

from datetime import datetime, date, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
import structlog
import statsapi
from dataclasses import dataclass

from ..models.game import Team

logger = structlog.get_logger(__name__)


@dataclass
class MLBGameInfo:
    """Information about an MLB game from the official API."""
    game_pk: int
    game_id: str  # Official MLB game ID string format
    home_team: str
    away_team: str
    home_team_abbr: str
    away_team_abbr: str
    game_date: datetime
    status: str
    venue: str
    home_record: Optional[str] = None
    away_record: Optional[str] = None


class MLBStatsAPIService:
    """Service for interacting with MLB Stats API to get official game information."""
    
    def __init__(self):
        """Initialize the MLB Stats API service."""
        self.logger = logger.bind(service="mlb_stats_api")
        
        # Team name mappings to MLB official names for better matching
        self.team_name_mappings = {
            # Common variations used by betting sites
            "Yankees": "New York Yankees",
            "Red Sox": "Boston Red Sox", 
            "Dodgers": "Los Angeles Dodgers",
            "Giants": "San Francisco Giants",
            "Cubs": "Chicago Cubs",
            "Cardinals": "St. Louis Cardinals",
            "Phillies": "Philadelphia Phillies",
            "Braves": "Atlanta Braves",
            "Astros": "Houston Astros",
            "Padres": "San Diego Padres",
            "Rays": "Tampa Bay Rays",
            "Tigers": "Detroit Tigers",
            "Twins": "Minnesota Twins",
            "Orioles": "Baltimore Orioles",
            "Angels": "Los Angeles Angels",
            "Mariners": "Seattle Mariners",
            "Rangers": "Texas Rangers",
            "Athletics": "Oakland Athletics",
            "A's": "Oakland Athletics",
            "Royals": "Kansas City Royals",
            "Guardians": "Cleveland Guardians",
            "Indians": "Cleveland Guardians",  # Legacy name
            "White Sox": "Chicago White Sox",
            "Brewers": "Milwaukee Brewers",
            "Pirates": "Pittsburgh Pirates",
            "Reds": "Cincinnati Reds",
            "Rockies": "Colorado Rockies",
            "Diamondbacks": "Arizona Diamondbacks",
            "D-backs": "Arizona Diamondbacks",
            "Marlins": "Miami Marlins",
            "Mets": "New York Mets",
            "Nationals": "Washington Nationals",
            "Nats": "Washington Nationals",
            "Blue Jays": "Toronto Blue Jays",
        }
        
        # Reverse mapping for abbreviations
        self.abbr_to_full_name = {
            "NYY": "New York Yankees",
            "BOS": "Boston Red Sox",
            "LAD": "Los Angeles Dodgers",
            "SF": "San Francisco Giants",
            "CHC": "Chicago Cubs",
            "STL": "St. Louis Cardinals",
            "PHI": "Philadelphia Phillies",
            "ATL": "Atlanta Braves",
            "HOU": "Houston Astros",
            "SD": "San Diego Padres",
            "TB": "Tampa Bay Rays",
            "DET": "Detroit Tigers",
            "MIN": "Minnesota Twins",
            "BAL": "Baltimore Orioles",
            "LAA": "Los Angeles Angels",  
            "SEA": "Seattle Mariners",
            "TEX": "Texas Rangers",
            "OAK": "Oakland Athletics",
            "KC": "Kansas City Royals",
            "CLE": "Cleveland Guardians",
            "CWS": "Chicago White Sox",
            "MIL": "Milwaukee Brewers",
            "PIT": "Pittsburgh Pirates",
            "CIN": "Cincinnati Reds",
            "COL": "Colorado Rockies",
            "ARI": "Arizona Diamondbacks",
            "MIA": "Miami Marlins",
            "NYM": "New York Mets",
            "WSH": "Washington Nationals",
            "TOR": "Toronto Blue Jays",
        }
    
    def normalize_team_name(self, team_name: str) -> str:
        """
        Normalize team name to official MLB name.
        
        Args:
            team_name: Team name from betting data
            
        Returns:
            Official MLB team name
        """
        # Try direct mapping first
        if team_name in self.team_name_mappings:
            return self.team_name_mappings[team_name]
        
        # Try abbreviation mapping
        if team_name in self.abbr_to_full_name:
            return self.abbr_to_full_name[team_name]
        
        # Try partial matching
        team_lower = team_name.lower()
        for mapping_key, official_name in self.team_name_mappings.items():
            if team_lower in mapping_key.lower() or mapping_key.lower() in team_lower:
                return official_name
        
        # Return as-is if no mapping found
        return team_name
    
    def get_games_for_date(self, target_date: date) -> List[MLBGameInfo]:
        """
        Get all MLB games for a specific date.
        
        Args:
            target_date: Date to get games for
            
        Returns:
            List of MLBGameInfo objects
        """
        try:
            date_str = target_date.strftime('%Y-%m-%d')
            self.logger.info("Fetching MLB games", date=date_str)
            
            # Get schedule from MLB API
            schedule = statsapi.schedule(date=date_str)
            
            games = []
            for game_data in schedule:
                try:
                    # Parse game datetime
                    game_datetime = datetime.fromisoformat(
                        game_data['game_datetime'].replace('Z', '+00:00')
                    ).replace(tzinfo=timezone.utc)
                    
                    game_info = MLBGameInfo(
                        game_pk=game_data['game_id'],
                        game_id=str(game_data['game_id']),  # Use game_pk as string ID
                        home_team=game_data['home_name'],
                        away_team=game_data['away_name'],
                        home_team_abbr=game_data.get('home_short_name', ''),
                        away_team_abbr=game_data.get('away_short_name', ''),
                        game_date=game_datetime,
                        status=game_data['status'],
                        venue=game_data.get('venue_name', ''),
                        home_record=game_data.get('home_record', None),
                        away_record=game_data.get('away_record', None)
                    )
                    
                    games.append(game_info)
                    
                except Exception as e:
                    self.logger.warning("Failed to parse game", 
                                      game_data=game_data, 
                                      error=str(e))
                    continue
            
            self.logger.info("Successfully fetched MLB games", 
                           date=date_str, 
                           games_count=len(games))
            
            return games
            
        except Exception as e:
            self.logger.error("Failed to fetch MLB games", 
                            date=target_date, 
                            error=str(e))
            return []
    
    def find_game_by_teams(self, home_team: str, away_team: str, 
                          target_date: Optional[date] = None) -> Optional[MLBGameInfo]:
        """
        Find an MLB game by team names, prioritizing upcoming games over completed ones.
        
        Args:
            home_team: Home team name from betting data
            away_team: Away team name from betting data
            target_date: Date to search for games (defaults to today)
            
        Returns:
            MLBGameInfo if found, None otherwise
        """
        if target_date is None:
            target_date = date.today()
        
        # Search today first, then yesterday and tomorrow to handle timezone differences
        search_dates = [
            target_date,          # Today first (most likely for upcoming games)
            target_date + timedelta(days=1),  # Tomorrow 
            target_date - timedelta(days=1),  # Yesterday (last resort for completed games)
        ]
        
        # Normalize team names
        home_team_norm = self.normalize_team_name(home_team)
        away_team_norm = self.normalize_team_name(away_team)
        
        self.logger.info("Searching for game by teams", 
                        home_team=home_team,
                        away_team=away_team,
                        home_team_norm=home_team_norm,
                        away_team_norm=away_team_norm,
                        search_dates=[d.strftime('%Y-%m-%d') for d in search_dates])
        
        # Collect all matching games, then prioritize by status
        all_matches = []
        
        for search_date in search_dates:
            games = self.get_games_for_date(search_date)
            
            for game in games:
                # Try multiple matching strategies
                match_found = False
                
                # Strategy 1: Exact match on normalized names
                if (game.home_team == home_team_norm and 
                    game.away_team == away_team_norm):
                    match_found = True
                
                # Strategy 2: Partial match (contains)
                if not match_found:
                    home_match = (home_team_norm.lower() in game.home_team.lower() or
                                 game.home_team.lower() in home_team_norm.lower() or
                                 home_team.lower() in game.home_team.lower() or
                                 game.home_team.lower() in home_team.lower())
                    
                    away_match = (away_team_norm.lower() in game.away_team.lower() or
                                 game.away_team.lower() in away_team_norm.lower() or
                                 away_team.lower() in game.away_team.lower() or
                                 game.away_team.lower() in away_team.lower())
                    
                    if home_match and away_match:
                        match_found = True
                
                # Strategy 3: Abbreviation match
                if not match_found and game.home_team_abbr and game.away_team_abbr:
                    home_match = (home_team.upper() == game.home_team_abbr.upper() or
                                 home_team_norm.upper() == game.home_team_abbr.upper())
                    away_match = (away_team.upper() == game.away_team_abbr.upper() or
                                 away_team_norm.upper() == game.away_team_abbr.upper())
                    
                    if home_match and away_match:
                        match_found = True
                
                if match_found:
                    all_matches.append(game)
        
        # If we found matches, prioritize by status and date
        if all_matches:
            # Define status priority (upcoming games first)
            status_priority = {
                'Pre-Game': 1,
                'Scheduled': 2, 
                'In Progress': 3,
                'Warmup': 4,
                'Delayed': 5,
                'Final': 10,  # Completed games last
                'Game Over': 10,
                'Postponed': 15,
                'Cancelled': 20
            }
            
            # Sort by status priority, then by date (future dates first for upcoming games)
            def sort_key(game):
                status_rank = status_priority.get(game.status, 99)
                # For upcoming games, prefer future dates; for completed games, prefer recent dates
                if status_rank < 10:  # Upcoming game
                    date_rank = (date.today() - game.game_date.date()).days  # Negative for future
                else:  # Completed game
                    date_rank = abs((date.today() - game.game_date.date()).days)  # Positive for past
                return (status_rank, date_rank)
            
            all_matches.sort(key=sort_key)
            best_match = all_matches[0]
            
            self.logger.info("Found matching game", 
                           game_pk=best_match.game_pk,
                           matchup=f"{best_match.away_team} @ {best_match.home_team}",
                           game_date=best_match.game_date.strftime('%Y-%m-%d %H:%M:%S'),
                           status=best_match.status,
                           total_matches=len(all_matches))
            return best_match
        
        self.logger.warning("No matching game found", 
                          home_team=home_team,
                          away_team=away_team,
                          search_dates=[d.strftime('%Y-%m-%d') for d in search_dates])
        return None
    
    def get_official_game_id(self, home_team: str, away_team: str, 
                           game_datetime: Optional[datetime] = None) -> Optional[str]:
        """
        Get the official MLB game ID for a betting split.
        
        Args:
            home_team: Home team name from betting data
            away_team: Away team name from betting data  
            game_datetime: Game datetime from betting data (optional)
            
        Returns:
            Official MLB game PK as string, or None if not found
        """
        try:
            # Determine target date from game_datetime or use today
            if game_datetime:
                target_date = game_datetime.date()
            else:
                target_date = date.today()
            
            # Find the game
            game_info = self.find_game_by_teams(home_team, away_team, target_date)
            
            if game_info:
                return game_info.game_id
            
            return None
            
        except Exception as e:
            self.logger.error("Failed to get official game ID", 
                            home_team=home_team,
                            away_team=away_team,
                            error=str(e))
            return None
    
    def batch_get_game_ids(self, team_pairs: List[Tuple[str, str, Optional[datetime]]]) -> Dict[Tuple[str, str], Optional[str]]:
        """
        Get official game IDs for multiple team pairs efficiently.
        
        Args:
            team_pairs: List of (home_team, away_team, game_datetime) tuples
            
        Returns:
            Dictionary mapping (home_team, away_team) to official game ID
        """
        results = {}
        
        # Group by date to minimize API calls
        date_groups = {}
        for home_team, away_team, game_datetime in team_pairs:
            target_date = game_datetime.date() if game_datetime else date.today()
            if target_date not in date_groups:
                date_groups[target_date] = []
            date_groups[target_date].append((home_team, away_team))
        
        # Process each date group
        for target_date, teams in date_groups.items():
            games = self.get_games_for_date(target_date)
            
            for home_team, away_team in teams:
                game_info = None
                
                # Try to find matching game in fetched games
                for game in games:
                    home_team_norm = self.normalize_team_name(home_team)
                    away_team_norm = self.normalize_team_name(away_team)
                    
                    # Multiple matching strategies (same as find_game_by_teams)
                    match_found = False
                    
                    # Exact match
                    if (game.home_team == home_team_norm and 
                        game.away_team == away_team_norm):
                        match_found = True
                    
                    # Partial match
                    if not match_found:
                        home_match = (home_team_norm.lower() in game.home_team.lower() or
                                     game.home_team.lower() in home_team_norm.lower())
                        away_match = (away_team_norm.lower() in game.away_team.lower() or
                                     game.away_team.lower() in away_team_norm.lower())
                        if home_match and away_match:
                            match_found = True
                    
                    if match_found:
                        game_info = game
                        break
                
                # Store result
                results[(home_team, away_team)] = game_info.game_id if game_info else None
        
        self.logger.info("Batch game ID lookup completed", 
                       requested_count=len(team_pairs),
                       found_count=sum(1 for v in results.values() if v is not None))
        
        return results 
    
    def get_game_data(self, home_team: str, away_team: str, 
                     estimated_datetime: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
        """
        Get comprehensive game data including official ID and correct datetime.
        
        Args:
            home_team: Home team name from betting data
            away_team: Away team name from betting data
            estimated_datetime: Estimated game datetime from raw data (optional)
            
        Returns:
            Dictionary with game_id and game_datetime, or None if not found
        """
        try:
            # Determine target date from estimated_datetime or use today
            if estimated_datetime:
                target_date = estimated_datetime.date()
            else:
                target_date = date.today()
            
            # Find the game
            game_info = self.find_game_by_teams(home_team, away_team, target_date)
            
            if game_info:
                return {
                    'game_id': game_info.game_id,
                    'game_datetime': game_info.game_date,  # This is the official datetime from MLB API
                    'home_team': game_info.home_team,
                    'away_team': game_info.away_team,
                    'home_team_abbr': game_info.home_team_abbr,
                    'away_team_abbr': game_info.away_team_abbr,
                    'status': game_info.status,
                    'venue': game_info.venue
                }
            
            self.logger.warning("No game data found", 
                              home_team=home_team,
                              away_team=away_team,
                              estimated_datetime=estimated_datetime)
            return None
            
        except Exception as e:
            self.logger.error("Failed to get game data", 
                            home_team=home_team,
                            away_team=away_team,
                            estimated_datetime=estimated_datetime,
                            error=str(e))
            return None