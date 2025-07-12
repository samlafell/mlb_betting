"""
Base parser for SportsbookReview.com data extraction.

This module provides the foundation for parsing SportsbookReview HTML content
and extracting betting data with proper error handling and validation.
"""

import re
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Dict, List, Optional, Union, Any, Tuple
from urllib.parse import urljoin, urlparse
import logging

from bs4 import BeautifulSoup, Tag
from pydantic import ValidationError

from ..models.game import EnhancedGame
from ..models.odds_data import OddsData, OddsSnapshot
from ..models.base import BetType, DataQuality

# Import Team enum for centralized team name handling
from src.mlb_sharp_betting.models.game import Team


logger = logging.getLogger(__name__)


class ParseError(Exception):
    """Custom exception for parsing errors."""
    pass


class SportsbookReviewBaseParser(ABC):
    """
    Base class for SportsbookReview.com parsers.
    
    Provides common functionality for parsing game data and betting information
    from SportsbookReview HTML content.
    """
    
    def __init__(self, base_url: str = "https://www.sportsbookreview.com"):
        self.base_url = base_url
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
    @abstractmethod
    def parse_game_data(self, soup: BeautifulSoup, game_url: str) -> Optional[EnhancedGame]:
        """Parse basic game information from HTML."""
        pass
    
    @abstractmethod
    def parse_betting_data(self, soup: BeautifulSoup, game_id: str) -> List[OddsData]:
        """Parse betting odds and splits data from HTML."""
        pass
    
    def parse_html(self, html_content: str, url: str) -> Dict[str, Any]:
        """
        Parse HTML content and extract game and betting data.
        
        Args:
            html_content: Raw HTML content from SportsbookReview
            url: URL of the page being parsed
            
        Returns:
            Dictionary containing parsed game and betting data
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract game data
            game_data = self.parse_game_data(soup, url)
            if not game_data:
                raise ParseError(f"Failed to parse game data from {url}")
            
            # Extract betting data
            betting_data = self.parse_betting_data(soup, game_data.sbr_game_id)
            
            return {
                "game": game_data,
                "betting_data": betting_data,
                "url": url,
                "parsed_at": datetime.now(timezone.utc)
            }
            
        except Exception as e:
            self.logger.error(f"Error parsing HTML from {url}: {e}")
            raise ParseError(f"Failed to parse HTML: {e}")
    
    def extract_game_id(self, url: str) -> Optional[str]:
        """Extract SportsbookReview game ID from URL."""
        try:
            # SportsbookReview URLs typically have game IDs in the path
            # Example: /betting/baseball/mlb/game/123456
            match = re.search(r'/game/(\d+)', url)
            if match:
                return match.group(1)
            
            # Alternative format: /betting/baseball/mlb/123456
            match = re.search(r'/mlb/(\d+)', url)
            if match:
                return match.group(1)
                
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting game ID from URL {url}: {e}")
            return None
    
    def extract_team_names(self, text: str) -> Tuple[Optional[str], Optional[str]]:
        """
        Extract team names from text like "Yankees @ Red Sox".
        
        Returns:
            Tuple of (away_team, home_team)
        """
        try:
            # Common patterns for team matchups
            patterns = [
                r'(.+?)\s+@\s+(.+)',  # Yankees @ Red Sox
                r'(.+?)\s+at\s+(.+)',  # Yankees at Red Sox
                r'(.+?)\s+vs\s+(.+)',  # Yankees vs Red Sox
            ]
            
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    away_team = match.group(1).strip()
                    home_team = match.group(2).strip()
                    
                    # Normalize team names using the centralized Team enum
                    away_team = Team.normalize_team_name(away_team)
                    home_team = Team.normalize_team_name(home_team)
                    
                    return away_team, home_team
            
            return None, None
            
        except Exception as e:
            self.logger.error(f"Error extracting team names from '{text}': {e}")
            return None, None
    
    def normalize_team_name(self, team_name: str) -> Optional[str]:
        """
        Normalize team name to standard abbreviation using the centralized Team enum.
        
        Args:
            team_name: Raw team name from SportsbookReview
            
        Returns:
            Normalized team abbreviation or None if not found
        """
        return Team.normalize_team_name(team_name)
    
    def parse_american_odds(self, odds_text: str) -> Optional[int]:
        """
        Parse American odds format (+150, -110, etc.).
        
        Args:
            odds_text: Text containing odds (e.g., "+150", "-110")
            
        Returns:
            Integer odds value or None if parsing fails
        """
        try:
            if not odds_text:
                return None
            
            # Clean the text
            cleaned = re.sub(r'[^\d\+\-]', '', odds_text)
            
            # Extract the numeric value with sign
            match = re.search(r'([+-]?\d+)', cleaned)
            if match:
                odds_value = int(match.group(1))
                
                # Validate range (reasonable odds)
                if -10000 <= odds_value <= 10000:
                    return odds_value
            
            return None
            
        except (ValueError, AttributeError) as e:
            self.logger.debug(f"Error parsing odds '{odds_text}': {e}")
            return None
    
    def parse_percentage(self, pct_text: str) -> Optional[float]:
        """
        Parse percentage text (e.g., "65%", "0.65").
        
        Args:
            pct_text: Text containing percentage
            
        Returns:
            Float percentage value (0.0-100.0) or None if parsing fails
        """
        try:
            if not pct_text:
                return None
            
            # Extract numeric value
            match = re.search(r'([\d.]+)', pct_text)
            if match:
                value = float(match.group(1))
                
                # If it's already a percentage (>1), return as-is
                if value > 1:
                    return value
                else:
                    # Convert decimal to percentage
                    return value * 100
            
            return None
            
        except (ValueError, AttributeError) as e:
            self.logger.debug(f"Error parsing percentage '{pct_text}': {e}")
            return None
    
    def parse_game_datetime(self, date_text: str, time_text: str = None) -> Optional[datetime]:
        """
        Parse game date and time from SportsbookReview format.
        
        Args:
            date_text: Date text (e.g., "April 4, 2021")
            time_text: Optional time text (e.g., "7:05 PM ET")
            
        Returns:
            Parsed datetime in EST timezone
        """
        try:
            if not date_text:
                return None
            
            # Common date formats
            date_formats = [
                "%B %d, %Y",  # April 4, 2021
                "%b %d, %Y",  # Apr 4, 2021
                "%m/%d/%Y",   # 04/04/2021
                "%Y-%m-%d",   # 2021-04-04
            ]
            
            parsed_date = None
            for date_format in date_formats:
                try:
                    parsed_date = datetime.strptime(date_text, date_format)
                    break
                except ValueError:
                    continue
            
            if not parsed_date:
                self.logger.warning(f"Could not parse date: {date_text}")
                return None
            
            # Handle time if provided
            if time_text:
                time_match = re.search(r'(\d{1,2}):(\d{2})\s*(AM|PM)', time_text, re.IGNORECASE)
                if time_match:
                    hour = int(time_match.group(1))
                    minute = int(time_match.group(2))
                    am_pm = time_match.group(3).upper()
                    
                    if am_pm == 'PM' and hour != 12:
                        hour += 12
                    elif am_pm == 'AM' and hour == 12:
                        hour = 0
                    
                    parsed_date = parsed_date.replace(hour=hour, minute=minute)
            
            # Convert to EST timezone
            # SportsbookReview times are typically in EST
            from zoneinfo import ZoneInfo
            est_tz = ZoneInfo("America/New_York")
            
            if parsed_date.tzinfo is None:
                parsed_date = parsed_date.replace(tzinfo=est_tz)
            
            return parsed_date
            
        except Exception as e:
            self.logger.error(f"Error parsing datetime '{date_text}' '{time_text}': {e}")
            return None
    
    def extract_sportsbook_name(self, element: Tag) -> Optional[str]:
        """
        Extract sportsbook name from HTML element.
        
        Args:
            element: BeautifulSoup element containing sportsbook info
            
        Returns:
            Normalized sportsbook name
        """
        try:
            # Common sportsbook name patterns
            sportsbook_mapping = {
                "draftkings": "DraftKings",
                "fanduel": "FanDuel",
                "mgm": "BetMGM",
                "caesars": "Caesars",
                "barstool": "Barstool",
                "betrivers": "BetRivers",
                "pointsbet": "PointsBet",
                "unibet": "Unibet",
                "william hill": "WilliamHill",
                "fox bet": "FOX Bet",
                "bet365": "bet365",
                "bovada": "Bovada",
                "mybookie": "MyBookie",
                "betway": "Betway",
                "betfred": "Betfred",
                "betcris": "BetCris",
                "sportsbetting": "SportsBetting.ag",
                "betonline": "BetOnline",
                "jazz": "Jazz",
                "circa": "Circa",
                "westgate": "Westgate",
                "station": "Station",
                "south point": "South Point",
                "golden nugget": "Golden Nugget",
                "wynn": "Wynn",
            }
            
            # Get text content
            text = element.get_text(strip=True).lower()
            
            # Check for matches
            for key, value in sportsbook_mapping.items():
                if key in text:
                    return value
            
            # If no match, return cleaned version
            return text.title()
            
        except Exception as e:
            self.logger.debug(f"Error extracting sportsbook name: {e}")
            return None
    
    def validate_betting_data(self, data: Dict[str, Any]) -> bool:
        """
        Validate extracted betting data for completeness and accuracy.
        
        Args:
            data: Dictionary containing betting data
            
        Returns:
            True if data is valid, False otherwise
        """
        try:
            # Check required fields
            required_fields = ['game_id', 'sportsbook', 'bet_type']
            for field in required_fields:
                if field not in data or not data[field]:
                    return False
            
            # Validate odds if present
            if 'odds' in data:
                odds = data['odds']
                if isinstance(odds, dict):
                    for value in odds.values():
                        if value is not None and not isinstance(value, (int, float)):
                            return False
            
            # Validate percentages if present
            if 'percentages' in data:
                percentages = data['percentages']
                if isinstance(percentages, dict):
                    for value in percentages.values():
                        if value is not None and not (0 <= value <= 100):
                            return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error validating betting data: {e}")
            return False
    
    def get_data_quality_score(self, data: Dict[str, Any]) -> DataQuality:
        """
        Assess data quality based on completeness and accuracy.
        
        Args:
            data: Dictionary containing parsed data
            
        Returns:
            DataQuality enum value
        """
        try:
            score = 0
            max_score = 10
            
            # Check for basic game information
            if data.get('game'):
                score += 2
                game = data['game']
                if hasattr(game, 'home_team') and hasattr(game, 'away_team'):
                    score += 2
                if hasattr(game, 'game_date'):
                    score += 1
            
            # Check for betting data
            if data.get('betting_data'):
                score += 2
                betting_data = data['betting_data']
                if len(betting_data) > 0:
                    score += 1
                    # Check for odds
                    if any(hasattr(bet, 'odds') for bet in betting_data):
                        score += 1
                    # Check for splits
                    if any(hasattr(bet, 'splits') for bet in betting_data):
                        score += 1
            
            # Determine quality level
            quality_ratio = score / max_score
            
            if quality_ratio >= 0.8:
                return DataQuality.HIGH
            elif quality_ratio >= 0.6:
                return DataQuality.MEDIUM
            else:
                return DataQuality.LOW
                
        except Exception as e:
            self.logger.error(f"Error assessing data quality: {e}")
            return DataQuality.LOW 