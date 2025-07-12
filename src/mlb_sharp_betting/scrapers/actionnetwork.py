"""
Action Network scraper for historical line movement data.

This scraper fetches historical line movement data from Action Network
history URLs to track how betting lines change over time.
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
import structlog

from .base import JSONScraper, ScrapingResult, RateLimitConfig
from ..models.actionnetwork import (
    ActionNetworkHistoricalData, ActionNetworkHistoricalEntry,
    ActionNetworkMarketData, ActionNetworkPrice, LineMovementPeriod
)

logger = structlog.get_logger(__name__)


class ActionNetworkScraper(JSONScraper):
    """
    Scraper for Action Network historical line movement data.
    
    Fetches historical betting data from Action Network history URLs
    to track line movement over time for MLB games.
    """
    
    def __init__(self, **kwargs: Any) -> None:
        """Initialize Action Network scraper with rate limiting."""
        super().__init__(
            source_name="ActionNetwork",
            rate_limit_config=RateLimitConfig(
                requests_per_minute=30,  # Conservative rate limit
                burst_size=5
            ),
            **kwargs
        )
        
        # Action Network specific headers
        self.headers.update({
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.actionnetwork.com/",
            "Origin": "https://www.actionnetwork.com",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
        })
    
    async def scrape_history_url(self, history_url: str, game_id: int,
                                home_team: str, away_team: str,
                                game_datetime: datetime) -> ScrapingResult:
        """
        Scrape historical line movement data from Action Network history URL.
        
        Args:
            history_url: The Action Network history API URL
            game_id: Action Network game ID
            home_team: Home team name
            away_team: Away team name
            game_datetime: Scheduled game start time
            
        Returns:
            ScrapingResult with historical line movement data
        """
        start_time = datetime.now()
        errors = []
        
        try:
            self.logger.info("Scraping Action Network history",
                           game_id=game_id, 
                           matchup=f"{away_team} @ {home_team}",
                           url=history_url)
            
            # Fetch historical data from API
            response_data = await self._get_json(history_url)
            
            if not response_data:
                error_msg = f"No data received from history URL: {history_url}"
                self.logger.error(error_msg)
                errors.append(error_msg)
                return self._create_result(
                    success=False,
                    data=[],
                    errors=errors,
                    metadata={
                        "game_id": game_id,
                        "history_url": history_url,
                        "matchup": f"{away_team} @ {home_team}"
                    }
                )
            
            # Process historical data
            historical_data = self._process_historical_response(
                response_data, game_id, home_team, away_team, 
                game_datetime, history_url
            )
            
            if not historical_data:
                error_msg = "Failed to process historical data"
                errors.append(error_msg)
                return self._create_result(
                    success=False,
                    data=[],
                    errors=errors,
                    metadata={
                        "game_id": game_id,
                        "history_url": history_url,
                        "matchup": f"{away_team} @ {home_team}"
                    }
                )
            
            end_time = datetime.now()
            response_time = (end_time - start_time).total_seconds() * 1000
            
            self.logger.info("Action Network history scraping completed",
                           game_id=game_id,
                           total_entries=historical_data.total_entries,
                           pregame_entries=historical_data.pregame_entries,
                           live_entries=historical_data.live_entries,
                           response_time_ms=response_time)
            
            return self._create_result(
                success=True,
                data=[historical_data.dict()],
                errors=errors,
                metadata={
                    "game_id": game_id,
                    "history_url": history_url,
                    "matchup": f"{away_team} @ {home_team}",
                    "total_entries": historical_data.total_entries,
                    "pregame_entries": historical_data.pregame_entries,
                    "live_entries": historical_data.live_entries
                },
                request_count=1,
                response_time_ms=response_time
            )
            
        except Exception as e:
            error_msg = f"Action Network history scraping failed: {str(e)}"
            self.logger.error(error_msg, game_id=game_id, url=history_url)
            errors.append(error_msg)
            
            return self._create_result(
                success=False,
                data=[],
                errors=errors,
                metadata={
                    "game_id": game_id,
                    "history_url": history_url,
                    "matchup": f"{away_team} @ {home_team}"
                }
            )
    
    def _process_historical_response(self, response_data: Dict[str, Any],
                                   game_id: int, home_team: str, away_team: str,
                                   game_datetime: datetime, history_url: str) -> Optional[ActionNetworkHistoricalData]:
        """
        Process the raw response data from Action Network history API.
        
        Args:
            response_data: Raw JSON response from API
            game_id: Action Network game ID
            home_team: Home team name
            away_team: Away team name
            game_datetime: Scheduled game start time
            history_url: History URL used for extraction
            
        Returns:
            Processed ActionNetworkHistoricalData or None if processing fails
        """
        try:
            # Validate response structure
            if not isinstance(response_data, list):
                self.logger.error("Expected list response from history API",
                                response_type=type(response_data))
                return None
            
            if len(response_data) == 0:
                self.logger.warning("Empty response from history API")
                return None
            
            historical_entries = []
            pregame_count = 0
            live_count = 0
            
            # Process each entry in the response
            for i, entry_data in enumerate(response_data):
                if not isinstance(entry_data, dict):
                    self.logger.warning(f"Skipping invalid entry at index {i}",
                                      entry_type=type(entry_data))
                    continue
                
                # Extract event data
                event_data = entry_data.get('event', {})
                if not event_data:
                    self.logger.warning(f"No event data in entry {i}")
                    continue
                
                # Determine if this is pregame or live data
                # Typically, pregame data is in indices 0-1, live data starts from index 2
                period = LineMovementPeriod.PREGAME if i < 2 else LineMovementPeriod.LIVE
                
                # Extract market data
                moneyline_data = self._extract_market_data(event_data, 'moneyline')
                spread_data = self._extract_market_data(event_data, 'spread')
                total_data = self._extract_market_data(event_data, 'total')
                
                # Create historical entry
                historical_entry = ActionNetworkHistoricalEntry(
                    event=event_data,
                    period=period,
                    moneyline=moneyline_data,
                    spread=spread_data,
                    total=total_data
                )
                
                historical_entries.append(historical_entry)
                
                if period == LineMovementPeriod.PREGAME:
                    pregame_count += 1
                else:
                    live_count += 1
            
            # Create the complete historical data object
            historical_data = ActionNetworkHistoricalData(
                game_id=game_id,
                home_team=home_team,
                away_team=away_team,
                game_datetime=game_datetime,
                historical_entries=historical_entries,
                history_url=history_url,
                total_entries=len(historical_entries),
                pregame_entries=pregame_count,
                live_entries=live_count
            )
            
            return historical_data
            
        except Exception as e:
            self.logger.error("Failed to process historical response",
                            error=str(e), game_id=game_id)
            return None
    
    def _extract_market_data(self, event_data: Dict[str, Any], 
                           market_type: str) -> Optional[ActionNetworkMarketData]:
        """
        Extract market data for a specific market type from event data.
        
        Args:
            event_data: Event data from the API response
            market_type: Type of market ('moneyline', 'spread', 'total')
            
        Returns:
            ActionNetworkMarketData or None if not found
        """
        try:
            market_data = event_data.get(market_type)
            if not market_data:
                return None
            
            # Extract pricing data
            home_price = None
            away_price = None
            line_value = None
            
            if market_type == 'moneyline':
                # Moneyline has home and away prices
                home_odds = market_data.get('home', {})
                away_odds = market_data.get('away', {})
                
                if home_odds:
                    home_price = ActionNetworkPrice(
                        decimal=home_odds.get('decimal'),
                        american=home_odds.get('american')
                    )
                
                if away_odds:
                    away_price = ActionNetworkPrice(
                        decimal=away_odds.get('decimal'),
                        american=away_odds.get('american')
                    )
            
            elif market_type in ['spread', 'total']:
                # Spread and total have line values and pricing
                line_value = market_data.get('line') or market_data.get('value')
                
                # Extract pricing (may be in different formats)
                home_odds = market_data.get('home', {}) or market_data.get('over', {})
                away_odds = market_data.get('away', {}) or market_data.get('under', {})
                
                if home_odds:
                    home_price = ActionNetworkPrice(
                        decimal=home_odds.get('decimal'),
                        american=home_odds.get('american')
                    )
                
                if away_odds:
                    away_price = ActionNetworkPrice(
                        decimal=away_odds.get('decimal'),
                        american=away_odds.get('american')
                    )
            
            # Create market data object
            return ActionNetworkMarketData(
                home=home_price,
                away=away_price,
                line=line_value
            )
            
        except Exception as e:
            self.logger.warning(f"Failed to extract {market_type} market data",
                              error=str(e))
            return None
    
    async def scrape_multiple_histories(self, game_data_list: List[Dict[str, Any]]) -> List[ScrapingResult]:
        """
        Scrape historical data for multiple games concurrently.
        
        Args:
            game_data_list: List of game data dictionaries with history URLs
            
        Returns:
            List of ScrapingResult objects
        """
        if not game_data_list:
            return []
        
        self.logger.info("Scraping multiple Action Network histories",
                        game_count=len(game_data_list))
        
        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(3)  # Max 3 concurrent requests
        
        async def scrape_single_game(game_data: Dict[str, Any]) -> ScrapingResult:
            async with semaphore:
                return await self.scrape_history_url(
                    history_url=game_data['history_url'],
                    game_id=game_data['game_id'],
                    home_team=game_data['home_team'],
                    away_team=game_data['away_team'],
                    game_datetime=game_data['game_datetime']
                )
        
        # Execute all scraping tasks
        tasks = [scrape_single_game(game_data) for game_data in game_data_list]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        scraping_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.logger.error("Failed to scrape game history",
                                game_id=game_data_list[i].get('game_id'),
                                error=str(result))
                # Create failed result
                scraping_results.append(
                    self._create_result(
                        success=False,
                        data=[],
                        errors=[f"Exception during scraping: {str(result)}"],
                        metadata=game_data_list[i]
                    )
                )
            else:
                scraping_results.append(result)
        
        successful_scrapes = sum(1 for r in scraping_results if r.success)
        self.logger.info("Completed multiple history scraping",
                        total_games=len(game_data_list),
                        successful=successful_scrapes,
                        failed=len(game_data_list) - successful_scrapes)
        
        return scraping_results
    
    async def scrape(self, **kwargs: Any) -> ScrapingResult:
        """
        Main scrape method for compatibility with base class.
        
        This method is required by the base class but Action Network
        historical scraping is typically done via scrape_history_url.
        """
        return self._create_result(
            success=False,
            data=[],
            errors=["Use scrape_history_url method for Action Network historical data"],
            metadata=kwargs
        ) 