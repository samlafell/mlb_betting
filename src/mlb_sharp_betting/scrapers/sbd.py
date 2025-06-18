"""SBD (SportsBettingDime) data scraper."""

from datetime import datetime
from typing import Any, Dict, List, Optional

import structlog

from .base import JSONScraper, ScrapingResult
from ..models.splits import DataSource

logger = structlog.get_logger(__name__)


class SBDScraper(JSONScraper):
    """Scraper for SportsBettingDime API betting data."""
    
    def __init__(self, **kwargs: Any) -> None:
        """Initialize SBD scraper with API configuration."""
        super().__init__(
            source_name="SBD",
            **kwargs
        )
        
        self.base_url = "https://srfeeds.sportsbettingdime.com"
        
        # API configuration
        self.api_version = "v2"
        self.default_books = [
            "betmgm", "bet365", "fanatics", 
            "draftkings", "caesars", "fanduel"
        ]
        
    def build_url(self, sport: str = "mlb", books: Optional[List[str]] = None) -> str:
        """
        Build API URL for SportsBettingDime betting splits.
        
        Args:
            sport: Sport to fetch data for
            books: List of sportsbooks to include
            
        Returns:
            Complete API URL
        """
        if books is None:
            books = self.default_books
            
        # URL encode the books parameter
        books_param = "%2C".join(books)  # URL encoded comma
        
        url = f"{self.base_url}/{self.api_version}/matchups/{sport}/betting-splits"
        url += f"?books={books_param}"
        
        logger.debug("Built SBD API URL", url=url, sport=sport, books=books)
        return url
    
    async def _fetch_api_data(self, url: str) -> Dict[str, Any]:
        """
        Fetch data from SBD API using base class method.
        
        Args:
            url: API URL to fetch
            
        Returns:
            Parsed JSON response
        """
        logger.info("Fetching SBD API data", url=url)
        return await self._get_json(url)
    
    def _validate_api_response(self, data: Dict[str, Any]) -> bool:
        """
        Validate the API response structure.
        
        Args:
            data: API response data
            
        Returns:
            True if response is valid
        """
        if not isinstance(data, dict):
            logger.warning("API response is not a dictionary")
            return False
            
        if "games" not in data:
            logger.warning("API response missing 'games' field")
            return False
            
        games = data.get("games", [])
        if not isinstance(games, list):
            logger.warning("Games field is not a list")
            return False
            
        logger.debug("API response validation passed", games_count=len(games))
        return True
    
    def _extract_game_data(self, games: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Extract relevant data from games list.
        
        Args:
            games: List of game dictionaries from API
            
        Returns:
            List of extracted game data
        """
        extracted_games = []
        
        for game in games:
            try:
                # Validate required fields
                if not game.get('_id'):
                    logger.warning("Game missing _id field, skipping")
                    continue
                    
                if not game.get('bettingSplits'):
                    logger.warning("Game missing bettingSplits field", game_id=game.get('_id'))
                    continue
                
                # Extract core game information
                game_data = {
                    "game_id": game.get('_id'),
                    "date": game.get('date'),
                    "home_team": game.get('home', {}).get('team', 'Unknown'),
                    "away_team": game.get('away', {}).get('team', 'Unknown'),
                    "home_code": game.get('home', {}).get('code', ''),
                    "away_code": game.get('away', {}).get('code', ''),
                    "betting_splits": game.get('bettingSplits', {}),
                    "sportsbooks": game.get('sportsbooks', []),
                    "source": DataSource.SBD.value,
                    "scraped_at": datetime.now().isoformat()
                }
                
                extracted_games.append(game_data)
                logger.debug("Extracted game data", 
                           game_id=game_data["game_id"],
                           matchup=f"{game_data['away_team']} @ {game_data['home_team']}")
                
            except Exception as e:
                logger.error("Failed to extract game data", 
                           game_id=game.get('_id', 'unknown'),
                           error=str(e))
                self.metrics["errors"] += 1
                continue
        
        logger.info("Game data extraction completed", 
                   total_games=len(games),
                   extracted_games=len(extracted_games))
        
        return extracted_games
    
    async def scrape(self, sport: str = "mlb", books: Optional[List[str]] = None, **kwargs: Any) -> "ScrapingResult":
        """
        Scrape betting splits data from SBD API.
        
        Args:
            sport: Sport to scrape data for
            books: List of sportsbooks to include
            
        Returns:
            ScrapingResult with extracted game data
        """
        logger.info("Starting SBD API scraping", sport=sport, books=books)
        
        errors = []
        start_time = datetime.now()
        
        try:
            # Build API URL
            url = self.build_url(sport=sport, books=books)
            
            # Fetch data from API
            api_data = await self._fetch_api_data(url)
            
            # Validate response
            if not self._validate_api_response(api_data):
                error_msg = "Invalid API response received"
                logger.error(error_msg)
                errors.append(error_msg)
                return self._create_result(
                    success=False,
                    data=[],
                    errors=errors,
                    metadata={"sport": sport, "books": books}
                )
            
            # Extract game data
            games = api_data.get('games', [])
            extracted_data = self._extract_game_data(games)
            
            end_time = datetime.now()
            response_time = (end_time - start_time).total_seconds() * 1000
            
            logger.info("SBD scraping completed successfully",
                       games_found=len(games),
                       games_extracted=len(extracted_data))
            
            return self._create_result(
                success=True,
                data=extracted_data,
                errors=errors,
                metadata={
                    "sport": sport,
                    "books": books,
                    "games_found": len(games),
                    "url": url
                },
                request_count=1,
                response_time_ms=response_time
            )
            
        except Exception as e:
            error_msg = f"SBD scraping failed: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)
            
            return self._create_result(
                success=False,
                data=[],
                errors=errors,
                metadata={"sport": sport, "books": books}
            )
    
    def validate_data(self, data: Any) -> bool:
        """
        Validate scraped data structure.
        
        Args:
            data: Data to validate
            
        Returns:
            True if data is valid
        """
        if not isinstance(data, list):
            return False
            
        for item in data:
            if not isinstance(item, dict):
                return False
                
            required_fields = ['game_id', 'betting_splits', 'home_team', 'away_team']
            if not all(field in item for field in required_fields):
                return False
        
        return True


__all__ = ["SBDScraper"] 