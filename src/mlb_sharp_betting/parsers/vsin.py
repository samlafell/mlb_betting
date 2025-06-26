"""
VSIN HTML parser for betting splits data.

This module provides functionality to parse VSIN HTML data into
validated BettingSplit model instances with comprehensive error handling.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Type
import re

import structlog

from .base import BaseParser, ValidationResult, ValidationConfig
from ..models.splits import BettingSplit, SplitType, BookType, DataSource
from ..models.game import Team
from ..core.exceptions import ValidationError

logger = structlog.get_logger(__name__)


class VSINParser(BaseParser):
    """
    Parser for VSIN betting splits HTML data.
    
    Transforms raw HTML-scraped data from VSIN into validated
    BettingSplit model instances with proper data validation.
    """
    
    def __init__(self, validation_config: Optional[ValidationConfig] = None) -> None:
        """
        Initialize VSIN parser.
        
        Args:
            validation_config: Validation configuration
        """
        super().__init__(
            parser_name="VSIN", 
            validation_config=validation_config
        )
        
        # MLB Stats API service will be initialized lazily to avoid circular imports
        self._mlb_api_service = None
        
        # Field mappings for different VSIN table formats
        # Based on ACTUAL VSIN scraper output - updated after testing scraper
        self.field_mappings = {
            # Common field variations
            'game': ['game', 'matchup', 'teams', 'team'],
            'home_team': ['home team', 'home_team', 'home'],
            'away_team': ['away team', 'away_team', 'away', 'visitor'],
            'spread': ['spread', 'line', 'point spread', 'away spread', 'home spread'],  # Updated with new fields
            'total': ['total', 'over/under', 'o/u', 'Total'],  # Updated to include 'Total' from scraper
            'moneyline': ['moneyline', 'money line', 'ml', 'Away Line', 'Home Line', 'away line', 'home line'],  # Updated with capitalized fields
            
            # Bet percentage fields - EXACT match from scraper output
            'home_bets_pct': ['home bets %', 'Home Bets %'],
            'away_bets_pct': ['away bets %', 'Away Bets %'],
            'over_bets_pct': ['over bets %', 'Over Bets %'],
            'under_bets_pct': ['under bets %', 'Under Bets %'],
            
            # Spread bet percentage fields
            'spread_home_bets_pct': ['home spread bets %', 'Home Spread Bets %', 'spread bets %'],
            'spread_away_bets_pct': ['away spread bets %', 'Away Spread Bets %', 'spread bets away %'],
            
            # Stake/Handle percentage fields - EXACT match from scraper output
            'home_stake_pct': ['home handle %', 'Home Handle %'],
            'away_stake_pct': ['away handle %', 'Away Handle %'],
            'over_stake_pct': ['over handle %', 'Over Handle %'],
            'under_stake_pct': ['under handle %', 'Under Handle %'],
            
            # Spread handle fields - EXACT match from scraper output
            'spread_home_stake_pct': ['home spread handle %', 'Home Spread Handle %', 'spread handle %'],
            'spread_away_stake_pct': ['away spread handle %', 'Away Spread Handle %', 'spread handle away %'],
            
            # Count fields (not in current scraper output but keep for future)
            'home_bets_count': ['home bets', 'home count'],
            'away_bets_count': ['away bets', 'away count'],
            'over_bets_count': ['over bets', 'over count'],
            'under_bets_count': ['under bets', 'under count'],
        }
    
    @property
    def target_model_class(self) -> Type[BettingSplit]:
        """Get the target model class for this parser."""
        return BettingSplit
    
    def _get_mlb_api_service(self):
        """Lazy initialization of MLB API service to avoid circular imports."""
        if self._mlb_api_service is None:
            from ..services.mlb_api_service import MLBStatsAPIService
            self._mlb_api_service = MLBStatsAPIService()
        return self._mlb_api_service
    
    async def parse_raw_data(self, raw_data: Dict[str, Any]) -> Optional[BettingSplit]:
        """
        Parse a single raw VSIN data item into a BettingSplit model.
        
        Args:
            raw_data: Raw data dictionary from VSIN scraper
            
        Returns:
            Parsed BettingSplit instance or None if parsing fails
        """
        try:
            # Extract metadata
            source = raw_data.get('source', DataSource.VSIN.value)
            book = raw_data.get('book', BookType.CIRCA.value)
            sport = raw_data.get('sport', 'mlb')
            scraped_at = raw_data.get('scraped_at')
            
            if scraped_at:
                last_updated = datetime.fromisoformat(scraped_at.replace('Z', '+00:00'))
            else:
                last_updated = datetime.now()
            
            # Parse game information
            game_info = self._parse_game_info(raw_data)
            if not game_info:
                self.logger.debug("Failed to parse game info", raw_data=raw_data)
                return None
            
            # Parse different split types
            splits = []
            
            # Parse spread data
            spread_split = self._parse_spread_split(raw_data, game_info, source, book, last_updated)
            if spread_split:
                splits.append(spread_split)
            
            # Parse total data
            total_split = self._parse_total_split(raw_data, game_info, source, book, last_updated)
            if total_split:
                splits.append(total_split)
            
            # Parse moneyline data
            moneyline_split = self._parse_moneyline_split(raw_data, game_info, source, book, last_updated)
            if moneyline_split:
                splits.append(moneyline_split)
            
            # Return the first valid split (can be enhanced to return all splits)
            return splits[0] if splits else None
            
        except Exception as e:
            self.logger.error("Failed to parse VSIN data", error=str(e), raw_data=raw_data)
            return None
    
    async def parse_all_splits(self, raw_data_list: List[Dict[str, Any]]) -> List[BettingSplit]:
        """
        Parse all splits from VSIN raw data into BettingSplit objects.
        Uses batch processing to efficiently get official MLB game IDs and datetimes.
        
        Args:
            raw_data_list: List of raw data from VSIN scraper
            
        Returns:
            List of BettingSplit objects
        """
        all_splits = []
        
        # Step 1: Pre-process to extract unique games and batch get official data
        unique_games = {}
        official_game_data = {}  # Map from game_key to official data
        
        for raw_data in raw_data_list:
            try:
                # Parse basic game info
                game_info = self._parse_game_info(raw_data)
                if not game_info:
                    continue
                
                # Create a unique key for this game
                game_key = f"{game_info['home_team'].value}_{game_info['away_team'].value}_{game_info['game_datetime'].strftime('%Y%m%d')}"
                
                unique_games[game_key] = {
                    'home_team': game_info['home_team'].value,
                    'away_team': game_info['away_team'].value,
                    'game_datetime': game_info['game_datetime']
                }
                
            except Exception as e:
                self.logger.debug("Failed to pre-process game info", error=str(e))
                continue
        
        # Step 2: Batch get official game data (IDs and datetimes)
        if unique_games:
            try:
                # Prepare batch requests for comprehensive game data
                for game_key, game_info in unique_games.items():
                    try:
                        # Get comprehensive game data from MLB API
                        mlb_game_data = self._get_mlb_api_service().get_game_data(
                            home_team=game_info['home_team'],
                            away_team=game_info['away_team'],
                            estimated_datetime=game_info['game_datetime']
                        )
                        
                        if mlb_game_data:
                            # Convert official game datetime to EST timezone-naive for consistency
                            official_datetime = mlb_game_data['game_datetime']
                            if official_datetime.tzinfo is not None:
                                # Convert to EST
                                import pytz
                                est_tz = pytz.timezone('US/Eastern')
                                est_datetime = official_datetime.astimezone(est_tz).replace(tzinfo=None)
                            else:
                                est_datetime = official_datetime
                            
                            # CRITICAL FIX: Don't override VSIN's date if there's a significant discrepancy
                            # VSIN often has more current information about game date changes
                            vsin_date = game_info['game_datetime'].date()
                            mlb_api_date = est_datetime.date()
                            
                            if abs((vsin_date - mlb_api_date).days) > 1:
                                # Use VSIN's date but MLB API's time
                                self.logger.warning(
                                    "Date discrepancy between VSIN and MLB API - using VSIN date",
                                    vsin_date=vsin_date.strftime('%Y-%m-%d'),
                                    mlb_api_date=mlb_api_date.strftime('%Y-%m-%d'),
                                    game_key=game_key
                                )
                                # Keep VSIN's date, use MLB API's time
                                final_datetime = game_info['game_datetime'].replace(
                                    hour=est_datetime.hour,
                                    minute=est_datetime.minute,
                                    second=est_datetime.second
                                )
                            else:
                                # Use MLB API datetime when dates are close
                                final_datetime = est_datetime
                            
                            official_game_data[game_key] = {
                                'game_id': str(mlb_game_data['game_id']),
                                'game_datetime': final_datetime
                            }
                            
                            self.logger.debug(
                                "Got official game data",
                                vsin_game_key=game_key,
                                official_game_id=mlb_game_data['game_id'],
                                final_datetime=final_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                                matchup=f"{game_info['away_team']} @ {game_info['home_team']}"
                            )
                        else:
                            # Fallback to generated ID and estimated datetime
                            fallback_id = self._generate_game_id(
                                Team(game_info['away_team']),
                                Team(game_info['home_team']),
                                game_info['game_datetime']
                            )
                            official_game_data[game_key] = {
                                'game_id': fallback_id,
                                'game_datetime': game_info['game_datetime']
                            }
                            
                    except Exception as e:
                        self.logger.warning("Failed to get game data", 
                                          game_key=game_key, 
                                          error=str(e))
                        # Fallback for this specific game
                        fallback_id = self._generate_game_id(
                            Team(game_info['away_team']),
                            Team(game_info['home_team']),
                            game_info['game_datetime']
                        )
                        official_game_data[game_key] = {
                            'game_id': fallback_id,
                            'game_datetime': game_info['game_datetime']
                        }
            
            except Exception as e:
                self.logger.warning("Batch game data lookup failed", error=str(e))
                # Generate fallback data for all games
                for game_key, game_info in unique_games.items():
                    fallback_id = self._generate_game_id(
                        Team(game_info['away_team']),
                        Team(game_info['home_team']),
                        game_info['game_datetime']
                    )
                    official_game_data[game_key] = {
                        'game_id': fallback_id,
                        'game_datetime': game_info['game_datetime']
                    }
        
        # Step 3: Parse all splits using pre-computed official game data
        for raw_data in raw_data_list:
            try:
                # Parse basic game info again
                game_info = self._parse_game_info(raw_data)
                if not game_info:
                    continue
                
                # Get the official game data
                game_key = f"{game_info['home_team'].value}_{game_info['away_team'].value}_{game_info['game_datetime'].strftime('%Y%m%d')}"
                official_data = official_game_data.get(game_key)
                
                if not official_data:
                    self.logger.warning("No official game data found", game_key=game_key)
                    continue
                
                # Update game_info with official data
                game_info_with_official = {
                    'game_id': official_data['game_id'],
                    'home_team': game_info['home_team'],
                    'away_team': game_info['away_team'],
                    'game_datetime': official_data['game_datetime'],  # Use official datetime
                    'matchup_text': game_info['matchup_text']
                }
                
                # Parse splits for this game
                splits = self._parse_splits_with_id(raw_data, official_data['game_id'], game_info_with_official)
                all_splits.extend(splits)
                
            except Exception as e:
                self.logger.debug("Failed to parse splits for game", error=str(e))
                continue
        
        return all_splits
    
    def _parse_splits_with_id(self, raw_data: Dict[str, Any], official_game_id: str, game_info: Dict[str, Any]) -> List[BettingSplit]:
        """
        Parse splits for a single game with pre-computed official game ID.
        
        Args:
            raw_data: Raw data dictionary
            official_game_id: Pre-computed official game ID
            game_info: Game information dictionary
            
        Returns:
            List of parsed BettingSplit instances
        """
        try:
            # Extract metadata
            source = raw_data.get('source', DataSource.VSIN.value)
            book = raw_data.get('book', BookType.CIRCA.value)
            scraped_at = raw_data.get('scraped_at')
            
            if scraped_at:
                last_updated = datetime.fromisoformat(scraped_at.replace('Z', '+00:00'))
            else:
                last_updated = datetime.now()
            
            # Prepare game info with official ID
            complete_game_info = {
                'game_id': official_game_id,
                'home_team': Team(game_info['home_team']),
                'away_team': Team(game_info['away_team']),
                'game_datetime': game_info['game_datetime']
            }
            
            # Parse different split types
            splits = []
            
            # Parse spread data
            spread_split = self._parse_spread_split(raw_data, complete_game_info, source, book, last_updated)
            if spread_split:
                splits.append(spread_split)
            
            # Parse total data
            total_split = self._parse_total_split(raw_data, complete_game_info, source, book, last_updated)
            if total_split:
                splits.append(total_split)
            
            # Parse moneyline data
            moneyline_split = self._parse_moneyline_split(raw_data, complete_game_info, source, book, last_updated)
            if moneyline_split:
                splits.append(moneyline_split)
            
            return splits
            
        except Exception as e:
            self.logger.debug("Failed to parse splits with ID", error=str(e))
            return []
    
    def _parse_game_info(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse game information from raw data.
        
        Args:
            raw_data: Raw data dictionary
            
        Returns:
            Dictionary with game information or None if parsing fails
        """
        try:
            # Look for game/matchup field
            game_text = None
            for field_key in raw_data:
                if any(game_field in field_key.lower() for game_field in self.field_mappings['game']):
                    game_text = raw_data[field_key]
                    break
            
            if not game_text:
                return None
            
            # Parse teams from game text (e.g., "Yankees @ Red Sox")
            teams = self._parse_teams_from_text(game_text)
            if not teams:
                return None
            
            # Try to parse game datetime (may not be available in all data)
            estimated_datetime = self._parse_game_datetime(raw_data) or datetime.now()
            
            return {
                'home_team': teams['home_team'],
                'away_team': teams['away_team'],
                'game_datetime': estimated_datetime,
                'matchup_text': game_text
            }
            
        except Exception as e:
            self.logger.debug("Failed to parse game info", error=str(e))
            return None
    
    def _parse_teams_from_text(self, game_text: str) -> Optional[Dict[str, Team]]:
        """
        Parse team names from game text.
        
        Args:
            game_text: Game text (e.g., "Yankees @ Red Sox")
            
        Returns:
            Dictionary with home and away teams or None if parsing fails
        """
        try:
            # Common separators
            separators = ['@', 'at', 'vs', 'v']
            
            for separator in separators:
                if separator in game_text.lower():
                    parts = game_text.split(separator)
                    if len(parts) == 2:
                        away_team_text = parts[0].strip()
                        home_team_text = parts[1].strip()
                        
                        # Normalize team names using enhanced mapping
                        away_team = self._normalize_team_name_enhanced(away_team_text)
                        home_team = self._normalize_team_name_enhanced(home_team_text)
                        
                        if away_team and home_team:
                            return {
                                'home_team': Team(home_team),
                                'away_team': Team(away_team)
                            }
            
            return None
            
        except Exception as e:
            self.logger.debug("Failed to parse teams from text", error=str(e), game_text=game_text)
            return None
    
    def _normalize_team_name_enhanced(self, name: str) -> Optional[str]:
        """
        Enhanced team name normalization that handles common nicknames.
        
        Args:
            name: Team name, nickname, or abbreviation
            
        Returns:
            Standardized team abbreviation or None if not found
        """
        name = name.strip().upper()
        
        # First try the standard Team.normalize_team_name method
        normalized = Team.normalize_team_name(name)
        if normalized:
            return normalized
        
        # Enhanced mappings for common nicknames and variations
        nickname_mappings = {
            # Common nicknames
            "YANKEES": "NYY",
            "RED SOX": "BOS",
            "DODGERS": "LAD",
            "GIANTS": "SF",
            "CUBS": "CHC",
            "CARDINALS": "STL",
            "PHILLIES": "PHI",
            "BRAVES": "ATL",
            "ASTROS": "HOU",
            "PADRES": "SD",
            "RAYS": "TB",
            "TIGERS": "DET",
            "TWINS": "MIN",
            "ORIOLES": "BAL",
            "ANGELS": "LAA",
            "MARINERS": "SEA",
            "RANGERS": "TEX",
            "ATHLETICS": "OAK",
            "ROYALS": "KC",
            "INDIANS": "CLE",   # Legacy name
            "GUARDIANS": "CLE",
            "WHITE SOX": "CWS",
            "BREWERS": "MIL",
            "PIRATES": "PIT",
            "REDS": "CIN",
            "ROCKIES": "COL",
            "DIAMONDBACKS": "ARI",
            "MARLINS": "MIA",
            "METS": "NYM",
            "NATIONALS": "WSH",
            
            # City-based nicknames
            "BOSTON": "BOS",
            "NEW YORK": "NYY",  # Default to Yankees for ambiguity
            "LOS ANGELES": "LAD",  # Default to Dodgers for ambiguity
            "SAN FRANCISCO": "SF",
            "CHICAGO": "CHC",  # Default to Cubs for ambiguity
            "ST. LOUIS": "STL",
            "ST LOUIS": "STL",
            "PHILADELPHIA": "PHI",
            "ATLANTA": "ATL",
            "HOUSTON": "HOU",
            "SAN DIEGO": "SD",
            "TAMPA BAY": "TB",
            "TAMPA": "TB",
            "DETROIT": "DET",
            "MINNESOTA": "MIN",
            "BALTIMORE": "BAL",
            "SEATTLE": "SEA",
            "TEXAS": "TEX",
            "OAKLAND": "OAK",
            "KANSAS CITY": "KC",
            "CLEVELAND": "CLE",
            "MILWAUKEE": "MIL",
            "PITTSBURGH": "PIT",
            "CINCINNATI": "CIN",
            "COLORADO": "COL",
            "ARIZONA": "ARI",
            "MIAMI": "MIA",
            "WASHINGTON": "WSH",
            "TORONTO": "TOR",
        }
        
        return nickname_mappings.get(name)
    

    
    def _generate_game_id(self, away_team: Team, home_team: Team, game_datetime: Optional[datetime]) -> str:
        """
        Generate a game ID from team names and datetime.
        
        Args:
            away_team: Away team (first in ID)
            home_team: Home team  
            game_datetime: Game datetime (can be None)
            
        Returns:
            Generated game ID
        """
        if game_datetime is None:
            game_datetime = datetime.now()
        
        date_str = game_datetime.strftime("%Y%m%d")
        return f"VSIN_{away_team.value}_{home_team.value}_{date_str}"
    
    def _parse_game_datetime(self, raw_data: Dict[str, Any]) -> Optional[datetime]:
        """
        Parse game datetime from raw data.
        
        Args:
            raw_data: Raw data dictionary
            
        Returns:
            Parsed datetime or None if not available
        """
        # Look for time/date fields
        time_fields = ['time', 'date', 'datetime', 'start_time']
        
        for field_key in raw_data:
            if any(time_field in field_key.lower() for time_field in time_fields):
                try:
                    time_text = raw_data[field_key]
                    # Attempt to parse various datetime formats
                    return self._parse_datetime_string(time_text)
                except:
                    continue
        
        return None
    
    def _parse_datetime_string(self, datetime_str: str) -> Optional[datetime]:
        """
        Parse datetime string in various formats.
        
        Args:
            datetime_str: Datetime string
            
        Returns:
            Parsed datetime or None if parsing fails
        """
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%m/%d/%Y %H:%M",
            "%m/%d/%Y",
            "%H:%M",
        ]
        
        for fmt in formats:
            try:
                if len(datetime_str.strip()) > 0:
                    return datetime.strptime(datetime_str.strip(), fmt)
            except ValueError:
                continue
        
        return None
    
    def _parse_spread_split(
        self, 
        raw_data: Dict[str, Any], 
        game_info: Dict[str, Any],
        source: str,
        book: str,
        last_updated: datetime
    ) -> Optional[BettingSplit]:
        """Parse spread betting split data."""
        try:
            # Find spread value - prioritize the properly assigned spread values
            away_spread = self._find_field_value(raw_data, ['away spread'])
            home_spread = self._find_field_value(raw_data, ['home spread'])
            
            # If we have properly assigned spreads, use the away team's spread as the split value
            spread_float = None
            if away_spread:
                # Parse the away team's spread (e.g., "+1.5" -> 1.5)
                spread_float = self._parse_numeric_value(away_spread)
            elif home_spread:
                # If only home spread available, flip the sign (e.g., "-1.5" -> 1.5)
                home_spread_float = self._parse_numeric_value(home_spread)
                if home_spread_float is not None:
                    spread_float = -home_spread_float
            else:
                # Fallback to original spread field
                spread_value = self._find_field_value(raw_data, self.field_mappings['spread'])
                spread_float = self._parse_numeric_value(spread_value) if spread_value else None
            
            # Find betting percentages - use spread-specific fields for spread splits
            home_bets_pct = (
                self._find_field_value(raw_data, self.field_mappings['spread_home_bets_pct']) or
                self._find_field_value(raw_data, self.field_mappings['home_bets_pct'])
            )
            away_bets_pct = (
                self._find_field_value(raw_data, self.field_mappings['spread_away_bets_pct']) or
                self._find_field_value(raw_data, self.field_mappings['away_bets_pct'])
            )
            
            # Find stake percentages - use spread-specific fields
            home_stake_pct = self._find_field_value(raw_data, self.field_mappings['spread_home_stake_pct'])
            away_stake_pct = self._find_field_value(raw_data, self.field_mappings['spread_away_stake_pct'])
            
            # Check if we have any spread betting data
            if not any([home_bets_pct, away_bets_pct, home_stake_pct, away_stake_pct]):
                return None
            
            # Find bet counts
            home_bets_count = self._find_field_value(raw_data, self.field_mappings['home_bets_count'])
            away_bets_count = self._find_field_value(raw_data, self.field_mappings['away_bets_count'])
            
            # Calculate sharp action (significant difference between bets % and stake %)
            home_bets_parsed = self._parse_percentage(home_bets_pct) or 0.0
            home_stake_parsed = self._parse_percentage(home_stake_pct) or 0.0
            sharp_action_detected = abs(home_bets_parsed - home_stake_parsed) >= 10.0
            
            # Convert sharp action to string format
            sharp_action = None
            if sharp_action_detected:
                if home_stake_parsed > home_bets_parsed:
                    sharp_action = "home"
                else:
                    sharp_action = "away"
            
            return BettingSplit(
                game_id=game_info['game_id'],
                home_team=game_info['home_team'],
                away_team=game_info['away_team'],
                game_datetime=game_info['game_datetime'],
                split_type=SplitType.SPREAD,
                split_value=spread_float,  # Now properly parsed as float
                source=DataSource(source),
                book=BookType(book),
                last_updated=last_updated,
                home_or_over_bets_percentage=home_bets_parsed,
                home_or_over_stake_percentage=home_stake_parsed,
                home_or_over_bets=self._parse_numeric_value(home_bets_count, int),
                away_or_under_bets_percentage=self._parse_percentage(away_bets_pct),
                away_or_under_stake_percentage=self._parse_percentage(away_stake_pct),
                away_or_under_bets=self._parse_numeric_value(away_bets_count, int),
                sharp_action=sharp_action
            )
            
        except Exception as e:
            self.logger.debug("Failed to parse spread split", error=str(e))
            return None
    
    def _parse_total_split(
        self, 
        raw_data: Dict[str, Any], 
        game_info: Dict[str, Any],
        source: str,
        book: str,
        last_updated: datetime
    ) -> Optional[BettingSplit]:
        """Parse total betting split data."""
        try:
            # Find total value - now properly cleaned and rounded by scraper
            total_value = self._find_field_value(raw_data, self.field_mappings['total'])
            total_float = self._parse_numeric_value(total_value) if total_value else None
            
            # Find betting percentages
            over_bets_pct = self._find_field_value(raw_data, self.field_mappings['over_bets_pct'])
            under_bets_pct = self._find_field_value(raw_data, self.field_mappings['under_bets_pct'])
            
            # Find stake percentages
            over_stake_pct = self._find_field_value(raw_data, self.field_mappings['over_stake_pct'])
            under_stake_pct = self._find_field_value(raw_data, self.field_mappings['under_stake_pct'])
            
            # Check if we have any total betting data
            if not any([over_bets_pct, under_bets_pct, over_stake_pct, under_stake_pct]):
                return None
            
            # Find bet counts
            over_bets_count = self._find_field_value(raw_data, self.field_mappings['over_bets_count'])
            under_bets_count = self._find_field_value(raw_data, self.field_mappings['under_bets_count'])
            
            # Calculate sharp action (significant difference between bets % and stake %)
            over_bets_parsed = self._parse_percentage(over_bets_pct) or 0.0
            over_stake_parsed = self._parse_percentage(over_stake_pct) or 0.0
            sharp_action_detected = abs(over_bets_parsed - over_stake_parsed) >= 10.0
            
            # Convert sharp action to string format
            sharp_action = None
            if sharp_action_detected:
                if over_stake_parsed > over_bets_parsed:
                    sharp_action = "over"
                else:
                    sharp_action = "under"
            
            return BettingSplit(
                game_id=game_info['game_id'],
                home_team=game_info['home_team'],
                away_team=game_info['away_team'],
                game_datetime=game_info['game_datetime'],
                split_type=SplitType.TOTAL,
                split_value=total_float,  # Now properly cleaned and rounded (e.g., 8.58 -> 8.5)
                source=DataSource(source),
                book=BookType(book),
                last_updated=last_updated,
                home_or_over_bets_percentage=over_bets_parsed,
                home_or_over_stake_percentage=over_stake_parsed,
                home_or_over_bets=self._parse_numeric_value(over_bets_count, int),
                away_or_under_bets_percentage=self._parse_percentage(under_bets_pct),
                away_or_under_stake_percentage=self._parse_percentage(under_stake_pct),
                away_or_under_bets=self._parse_numeric_value(under_bets_count, int),
                sharp_action=sharp_action
            )
            
        except Exception as e:
            self.logger.debug("Failed to parse total split", error=str(e))
            return None
    
    def _parse_moneyline_split(
        self, 
        raw_data: Dict[str, Any], 
        game_info: Dict[str, Any],
        source: str,
        book: str,
        last_updated: datetime
    ) -> Optional[BettingSplit]:
        """Parse moneyline betting split data."""
        try:
            # Find betting percentages
            home_bets_pct = self._find_field_value(raw_data, self.field_mappings['home_bets_pct'])
            away_bets_pct = self._find_field_value(raw_data, self.field_mappings['away_bets_pct'])
            
            # For moneyline, we need at least bet percentages
            if home_bets_pct is None and away_bets_pct is None:
                return None
            
            # Find stake percentages - use the updated field mappings
            home_stake_pct = self._find_field_value(raw_data, self.field_mappings['home_stake_pct'])
            away_stake_pct = self._find_field_value(raw_data, self.field_mappings['away_stake_pct'])
            
            # Find bet counts
            home_bets_count = self._find_field_value(raw_data, self.field_mappings['home_bets_count'])
            away_bets_count = self._find_field_value(raw_data, self.field_mappings['away_bets_count'])
            
            # Extract moneyline odds and create JSON map
            home_line = self._find_field_value(raw_data, ['Home Line', 'home line'])
            away_line = self._find_field_value(raw_data, ['Away Line', 'away line'])
            
            moneyline_value = None
            if home_line or away_line:
                try:
                    import json
                    moneyline_map = {}
                    
                    if home_line:
                        # Parse home odds (e.g., "-155" -> -155, "+139" -> 139)
                        home_odds = self._parse_moneyline_odds(home_line)
                        if home_odds is not None:
                            moneyline_map['home'] = home_odds
                    
                    if away_line:
                        # Parse away odds (e.g., "+139" -> 139, "-155" -> -155)
                        away_odds = self._parse_moneyline_odds(away_line)
                        if away_odds is not None:
                            moneyline_map['away'] = away_odds
                    
                    if moneyline_map:
                        moneyline_value = json.dumps(moneyline_map)
                except Exception as e:
                    self.logger.debug("Failed to parse moneyline odds", error=str(e))
            
            # Calculate sharp action (significant difference between bets % and stake %)
            home_bets_parsed = self._parse_percentage(home_bets_pct) or 0.0
            home_stake_parsed = self._parse_percentage(home_stake_pct) or 0.0
            sharp_action_detected = abs(home_bets_parsed - home_stake_parsed) >= 10.0
            
            # Convert sharp action to string format
            sharp_action = None
            if sharp_action_detected:
                if home_stake_parsed > home_bets_parsed:
                    sharp_action = "home"
                else:
                    sharp_action = "away"
            
            return BettingSplit(
                game_id=game_info['game_id'],
                home_team=game_info['home_team'],
                away_team=game_info['away_team'],
                game_datetime=game_info['game_datetime'],
                split_type=SplitType.MONEYLINE,
                split_value=moneyline_value,  # JSON string with moneyline odds map
                source=DataSource(source),
                book=BookType(book),
                last_updated=last_updated,
                home_or_over_bets_percentage=home_bets_parsed,
                home_or_over_stake_percentage=home_stake_parsed,
                home_or_over_bets=self._parse_numeric_value(home_bets_count, int),
                away_or_under_bets_percentage=self._parse_percentage(away_bets_pct),
                away_or_under_stake_percentage=self._parse_percentage(away_stake_pct),
                away_or_under_bets=self._parse_numeric_value(away_bets_count, int),
                sharp_action=sharp_action
            )
            
        except Exception as e:
            self.logger.debug("Failed to parse moneyline split", error=str(e))
            return None
    
    def _find_field_value(self, raw_data: Dict[str, Any], field_aliases: List[str]) -> Optional[str]:
        """
        Find field value using multiple possible field names.
        
        Args:
            raw_data: Raw data dictionary
            field_aliases: List of possible field names
            
        Returns:
            Field value or None if not found
        """
        # First try exact matches (case-insensitive)
        for alias in field_aliases:
            for field_key in raw_data:
                if alias.lower() == field_key.lower():
                    value = raw_data[field_key]
                    if value is not None and str(value).strip():
                        return str(value).strip()
        
        # If no exact match, try substring matches
        for field_key in raw_data:
            for alias in field_aliases:
                if alias.lower() in field_key.lower():
                    value = raw_data[field_key]
                    if value is not None and str(value).strip():
                        return str(value).strip()
        return None
    
    def _parse_percentage(self, value: Optional[str]) -> Optional[float]:
        """
        Parse percentage value from string.
        
        Args:
            value: String value (e.g., "65%", "0.65")
            
        Returns:
            Float percentage (0-100) or None if parsing fails
        """
        if not value:
            return None
        
        try:
            original_value = str(value).strip()
            has_percent_sign = '%' in original_value
            
            # Remove % sign and whitespace
            clean_value = original_value.replace('%', '').strip()
            
            # Parse as float
            float_value = float(clean_value)
            
            # If the original value had a % sign, it's already a percentage
            if has_percent_sign:
                if 0 <= float_value <= 100:
                    return float_value
                else:
                    return None
            else:
                # No % sign - check if it's in 0-1 range (decimal) or 0-100 range
                if 0 <= float_value <= 1:
                    return float_value * 100
                elif 0 <= float_value <= 100:
                    return float_value
                else:
                    return None
                
        except (ValueError, TypeError):
            return None
    
    def _parse_numeric_value(self, value: Optional[str], target_type: type = float) -> Optional[float]:
        """
        Parse numeric value from string.
        
        Args:
            value: String value
            target_type: Target numeric type (int or float)
            
        Returns:
            Parsed numeric value or None if parsing fails
        """
        if not value:
            return None
        
        try:
            # Clean the value
            clean_value = str(value).replace(',', '').replace('$', '').strip()
            
            # Remove common prefixes/suffixes
            clean_value = re.sub(r'^[+-]?([O|U])?', '', clean_value)
            
            # Parse as target type
            if target_type == int:
                return int(float(clean_value))
            else:
                return float(clean_value)
                
        except (ValueError, TypeError):
            return None
    
    def _parse_moneyline_odds(self, odds_str: str) -> Optional[int]:
        """
        Parse moneyline odds string to integer.
        
        Args:
            odds_str: Odds string (e.g., "+139", "-155", "139")
            
        Returns:
            Parsed odds as integer or None if parsing fails
        """
        if not odds_str:
            return None
        
        try:
            # Clean the odds string
            clean_odds = str(odds_str).strip()
            
            # Remove any non-numeric characters except + and -
            import re
            clean_odds = re.sub(r'[^\d+-]', '', clean_odds)
            
            if not clean_odds:
                return None
            
            # Parse as integer
            odds_value = int(clean_odds)
            
            # Ensure reasonable range for moneyline odds
            if -10000 <= odds_value <= 10000:
                return odds_value
            else:
                return None
                
        except (ValueError, TypeError):
            return None
    
    async def _custom_validation(self, item: BettingSplit) -> ValidationResult:
        """
        Perform VSIN-specific validation.
        
        Args:
            item: BettingSplit instance to validate
            
        Returns:
            ValidationResult with validation status
        """
        errors = []
        warnings = []
        
        # Validate percentage ranges
        percentage_fields = [
            'home_or_over_bets_percentage',
            'away_or_under_bets_percentage',
            'home_or_over_stake_percentage',
            'away_or_under_stake_percentage'
        ]
        
        for field in percentage_fields:
            value = getattr(item, field, None)
            if value is not None and not (0 <= value <= 100):
                errors.append(f"{field} out of range (0-100): {value}")
        
        # Validate percentage pairs sum to ~100%
        if (item.home_or_over_bets_percentage is not None and 
            item.away_or_under_bets_percentage is not None):
            total_pct = item.home_or_over_bets_percentage + item.away_or_under_bets_percentage
            if not (95 <= total_pct <= 105):  # Allow 5% tolerance
                warnings.append(f"Bet percentages don't sum to 100%: {total_pct}%")
        
        if (item.home_or_over_stake_percentage is not None and 
            item.away_or_under_stake_percentage is not None):
            total_pct = item.home_or_over_stake_percentage + item.away_or_under_stake_percentage
            if not (95 <= total_pct <= 105):  # Allow 5% tolerance
                warnings.append(f"Stake percentages don't sum to 100%: {total_pct}%")
        
        # Validate split values
        if item.split_type == SplitType.SPREAD and item.split_value is not None:
            if not (-30 <= item.split_value <= 30):  # Reasonable spread range
                warnings.append(f"Unusual spread value: {item.split_value}")
        
        if item.split_type == SplitType.TOTAL and item.split_value is not None:
            if not (2 <= item.split_value <= 20):  # Reasonable total range for MLB
                warnings.append(f"Unusual total value: {item.split_value}")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )


# Convenience function
async def parse_vsin_data(
    raw_data: List[Dict[str, Any]], 
    validation_config: Optional[ValidationConfig] = None
):
    """
    Convenience function to parse VSIN data.
    
    Args:
        raw_data: List of raw VSIN data dictionaries
        validation_config: Optional validation configuration
        
    Returns:
        ParsingResult with parsed BettingSplit instances
    """
    parser = VSINParser(validation_config)
    return await parser.parse(raw_data)