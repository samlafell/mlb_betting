"""Pinnacle data scraper for fetching MLB betting odds from JSON endpoints."""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any
import time

import aiohttp
import structlog
from dataclasses import dataclass

from ..models.game import Team
from ..models.pinnacle import (
    PinnacleMarket, PinnaclePrice, PinnacleLimit, PinnacleOddsSnapshot,
    PinnacleMarketType, PriceDesignation, MarketStatus, LimitType
)

logger = structlog.get_logger(__name__)


@dataclass
class PinnacleScraperConfig:
    """Configuration for Pinnacle data scraping."""
    
    base_url: str = "https://guest.api.arcadia.pinnacle.com/0.1"
    mlb_league_id: int = 246
    max_retries: int = 3
    retry_delay: float = 1.0
    timeout: float = 30.0
    rate_limit_delay: float = 0.1
    
    headers: Dict[str, str] = None
    
    def __post_init__(self):
        """Initialize default headers for web scraping."""
        if self.headers is None:
            self.headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
            }


class PinnacleScraper:
    """Scraper for extracting MLB betting data from Pinnacle's JSON endpoints."""
    
    def __init__(self, config: Optional[PinnacleScraperConfig] = None):
        """Initialize the Pinnacle scraper."""
        self.config = config or PinnacleScraperConfig()
        self.logger = logger.bind(service="pinnacle_scraper")
        self.last_request_time = 0.0
        
        # Essential team name mappings for MLB
        self.team_mappings = {
            # AL Teams
            "Athletics": Team.OAK, "Oakland Athletics": Team.OAK,
            "Houston Astros": Team.HOU, "Astros": Team.HOU,
            "New York Yankees": Team.NYY, "Yankees": Team.NYY,
            "Boston Red Sox": Team.BOS, "Red Sox": Team.BOS,
            "Tampa Bay Rays": Team.TB, "Rays": Team.TB,
            "Toronto Blue Jays": Team.TOR, "Blue Jays": Team.TOR,
            "Chicago White Sox": Team.CWS, "White Sox": Team.CWS,
            "Cleveland Guardians": Team.CLE, "Guardians": Team.CLE,
            "Detroit Tigers": Team.DET, "Tigers": Team.DET,
            "Kansas City Royals": Team.KC, "Royals": Team.KC,
            "Minnesota Twins": Team.MIN, "Twins": Team.MIN,
            "Los Angeles Angels": Team.LAA, "Angels": Team.LAA,
            "Seattle Mariners": Team.SEA, "Mariners": Team.SEA,
            "Texas Rangers": Team.TEX, "Rangers": Team.TEX,
            
            # NL Teams
            "Atlanta Braves": Team.ATL, "Braves": Team.ATL,
            "Miami Marlins": Team.MIA, "Marlins": Team.MIA,
            "New York Mets": Team.NYM, "Mets": Team.NYM,
            "Philadelphia Phillies": Team.PHI, "Phillies": Team.PHI,
            "Washington Nationals": Team.WSH, "Nationals": Team.WSH,
            "Chicago Cubs": Team.CHC, "Cubs": Team.CHC,
            "Cincinnati Reds": Team.CIN, "Reds": Team.CIN,
            "Milwaukee Brewers": Team.MIL, "Brewers": Team.MIL,
            "Pittsburgh Pirates": Team.PIT, "Pirates": Team.PIT,
            "St. Louis Cardinals": Team.STL, "Cardinals": Team.STL,
            "Arizona Diamondbacks": Team.ARI, "Diamondbacks": Team.ARI,
            "Colorado Rockies": Team.COL, "Rockies": Team.COL,
            "Los Angeles Dodgers": Team.LAD, "Dodgers": Team.LAD,
            "San Diego Padres": Team.SD, "Padres": Team.SD,
            "San Francisco Giants": Team.SF, "Giants": Team.SF,
        }
    
    async def _fetch_json(self, url: str, session: aiohttp.ClientSession) -> Optional[Dict[str, Any]]:
        """Fetch JSON data from URL with rate limiting and error handling."""
        # Rate limiting to be respectful
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.config.rate_limit_delay:
            await asyncio.sleep(self.config.rate_limit_delay - time_since_last)
        
        self.last_request_time = time.time()
        
        for attempt in range(self.config.max_retries):
            try:
                self.logger.debug("Scraping JSON data", url=url, attempt=attempt + 1)
                
                async with session.get(
                    url,
                    headers=self.config.headers,
                    timeout=aiohttp.ClientTimeout(total=self.config.timeout)
                ) as response:
                    
                    if response.status == 200:
                        data = await response.json()
                        self.logger.debug("Successfully scraped JSON", url=url, data_size=len(str(data)))
                        return data
                    elif response.status == 429:
                        self.logger.warning("Rate limited, backing off", url=url)
                        await asyncio.sleep(self.config.retry_delay * (2 ** attempt))
                        continue
                    else:
                        self.logger.warning("Failed to scrape data", url=url, status=response.status)
                        if attempt == self.config.max_retries - 1:
                            return None
                        await asyncio.sleep(self.config.retry_delay * (2 ** attempt))
                        
            except Exception as e:
                self.logger.error("Scraping error", url=url, error=str(e), attempt=attempt + 1)
                if attempt == self.config.max_retries - 1:
                    return None
                await asyncio.sleep(self.config.retry_delay * (2 ** attempt))
        
        return None
    
    async def scrape_active_matchups(self) -> List[int]:
        """Scrape active MLB matchup IDs from Pinnacle's JSON endpoint."""
        url = f"{self.config.base_url}/leagues/{self.config.mlb_league_id}/markets/straight"
        
        try:
            self.logger.info("Scraping active MLB matchups from Pinnacle")
            
            async with aiohttp.ClientSession() as session:
                data = await self._fetch_json(url, session)
                
                if not data or not isinstance(data, list):
                    self.logger.warning("No valid data found", data_type=type(data))
                    return []
                
                # Extract only the matchup IDs we need
                matchup_ids = []
                for item in data:
                    if isinstance(item, dict):
                        # Primary matchup ID
                        if "matchupId" in item:
                            matchup_ids.append(item["matchupId"])
                        # Parent matchup ID for special bets
                        elif "parent" in item and isinstance(item["parent"], dict):
                            parent_id = item["parent"].get("id")
                            if parent_id:
                                matchup_ids.append(parent_id)
                
                # Remove duplicates
                matchup_ids = list(set(matchup_ids))
                
                self.logger.info("Successfully scraped matchup IDs", count=len(matchup_ids))
                return matchup_ids
                
        except Exception as e:
            self.logger.error("Failed to scrape active matchups", error=str(e))
            return []
    
    async def scrape_matchup_markets(self, matchup_id: int) -> List[Dict[str, Any]]:
        """Scrape market data for a specific matchup, returning only essential info."""
        url = f"{self.config.base_url}/matchups/{matchup_id}/markets/related/straight"
        
        try:
            async with aiohttp.ClientSession() as session:
                data = await self._fetch_json(url, session)
                
                if not data or not isinstance(data, list):
                    return []
                
                # Extract only essential market information
                essential_markets = []
                for market_data in data:
                    if not isinstance(market_data, dict):
                        continue
                    
                    essential_info = self._extract_essential_market_info(market_data, matchup_id)
                    if essential_info:
                        essential_markets.append(essential_info)
                
                self.logger.debug("Scraped market data", matchup_id=matchup_id, markets_found=len(essential_markets))
                return essential_markets
                
        except Exception as e:
            self.logger.error("Failed to scrape markets", matchup_id=matchup_id, error=str(e))
            return []
    
    def _extract_essential_market_info(self, raw_data: Dict[str, Any], matchup_id: int) -> Optional[Dict[str, Any]]:
        """Extract only the essential information we need from raw market data."""
        try:
            # Extract teams from parent data
            teams = self._extract_team_info(raw_data)
            if not teams:
                return None
            
            # Extract market type
            market_type = raw_data.get("type", "").lower()
            if market_type not in ["moneyline", "spread", "total", "team_total", "special"]:
                return None
            
            # Extract prices (odds)
            prices = self._extract_price_info(raw_data.get("prices", []))
            if not prices:
                return None
            
            # Extract essential fields only
            essential_info = {
                "matchup_id": matchup_id,
                "market_type": market_type,
                "teams": teams,
                "prices": prices,
                "key": raw_data.get("key", ""),
                "period": raw_data.get("period", 0),
                "status": raw_data.get("status", "open"),
                "version": raw_data.get("version", 0),
                "cutoff_at": raw_data.get("cutoffAt"),
                "start_time": self._extract_start_time(raw_data),
            }
            
            # Add line value for spread/total markets
            if market_type in ["spread", "total"]:
                line_value = raw_data.get("line") or raw_data.get("points") or raw_data.get("total")
                if line_value is not None:
                    essential_info["line_value"] = float(line_value)
            
            # Add limits if available
            limits = self._extract_limit_info(raw_data.get("limits", []))
            if limits:
                essential_info["limits"] = limits
            
            return essential_info
            
        except Exception as e:
            self.logger.debug("Failed to extract essential market info", error=str(e))
            return None
    
    def _extract_team_info(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """Extract team information from raw data."""
        # Try parent data first
        parent_data = raw_data.get("parent", {})
        participants = parent_data.get("participants", [])
        
        # If no parent, try direct participants
        if not participants:
            participants = raw_data.get("participants", [])
        
        if len(participants) < 2:
            return None
        
        teams = {}
        for participant in participants:
            if not isinstance(participant, dict):
                continue
                
            alignment = participant.get("alignment", "").lower()
            name = participant.get("name", "")
            
            if alignment in ["home", "away"] and name:
                normalized_team = self._normalize_team_name(name)
                if normalized_team:
                    teams[alignment] = normalized_team.value
        
        # Must have both home and away
        if "home" in teams and "away" in teams:
            return teams
        
        return None
    
    def _extract_price_info(self, prices_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract essential price information."""
        prices = []
        
        for price_data in prices_data:
            if not isinstance(price_data, dict):
                continue
                
            price_value = price_data.get("price")
            designation = price_data.get("designation", "").lower()
            
            if price_value is not None and designation in ["home", "away", "over", "under"]:
                prices.append({
                    "designation": designation,
                    "price": int(price_value)
                })
        
        return prices
    
    def _extract_limit_info(self, limits_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract essential limit information."""
        limits = []
        
        for limit_data in limits_data:
            if not isinstance(limit_data, dict):
                continue
                
            amount = limit_data.get("amount")
            limit_type = limit_data.get("type", "").lower()
            
            if amount is not None and limit_type in ["maxriskstake", "maxwinstake"]:
                limits.append({
                    "amount": float(amount),
                    "type": limit_type
                })
        
        return limits
    
    def _extract_start_time(self, raw_data: Dict[str, Any]) -> Optional[str]:
        """Extract start time from various possible locations."""
        # Try different locations for start time
        start_time = (
            raw_data.get("startTime") or
            raw_data.get("start_time") or
            (raw_data.get("parent", {}).get("startTime"))
        )
        
        return start_time
    
    def _normalize_team_name(self, team_name: str) -> Optional[Team]:
        """Normalize team name to Team enum."""
        if not team_name:
            return None
        
        # Direct lookup
        if team_name in self.team_mappings:
            return self.team_mappings[team_name]
        
        # Case-insensitive lookup
        for key, value in self.team_mappings.items():
            if key.lower() == team_name.lower():
                return value
        
        # Partial matching as fallback
        team_lower = team_name.lower()
        for key, value in self.team_mappings.items():
            if team_lower in key.lower() or key.lower() in team_lower:
                return value
        
        self.logger.debug("Could not normalize team name", team_name=team_name)
        return None
    
    async def scrape_all_mlb_data(self) -> Dict[str, Any]:
        """Scrape all current MLB betting data from Pinnacle."""
        try:
            self.logger.info("Starting full MLB data scrape from Pinnacle")
            
            # Get active matchups
            matchup_ids = await self.scrape_active_matchups()
            
            if not matchup_ids:
                self.logger.warning("No active matchups found")
                return {"matchups": [], "markets": [], "scraped_at": datetime.now().isoformat()}
            
            # Scrape markets for all matchups with concurrency control
            all_markets = []
            semaphore = asyncio.Semaphore(3)  # Limit concurrent requests to be respectful
            
            async def scrape_single_matchup(matchup_id: int) -> List[Dict[str, Any]]:
                async with semaphore:
                    return await self.scrape_matchup_markets(matchup_id)
            
            # Execute scraping tasks
            tasks = [scrape_single_matchup(matchup_id) for matchup_id in matchup_ids]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Collect results
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    self.logger.warning("Failed to scrape matchup markets",
                                      matchup_id=matchup_ids[i],
                                      error=str(result))
                else:
                    all_markets.extend(result)
            
            scraped_data = {
                "matchups": matchup_ids,
                "markets": all_markets,
                "scraped_at": datetime.now().isoformat(),
                "total_matchups": len(matchup_ids),
                "total_markets": len(all_markets)
            }
            
            self.logger.info("Successfully completed MLB data scrape", 
                           matchups=len(matchup_ids),
                           markets=len(all_markets))
            
            return scraped_data
            
        except Exception as e:
            self.logger.error("Failed to scrape MLB data", error=str(e))
            return {"error": str(e), "scraped_at": datetime.now().isoformat()}
    
    async def scrape_team_matchup(self, home_team: Team, away_team: Team) -> List[Dict[str, Any]]:
        """Scrape betting data for a specific team matchup."""
        try:
            # Get all current data
            all_data = await self.scrape_all_mlb_data()
            
            # Filter for the specific teams
            matching_markets = []
            for market in all_data.get("markets", []):
                teams = market.get("teams", {})
                if (teams.get("home") == home_team.value and 
                    teams.get("away") == away_team.value):
                    matching_markets.append(market)
            
            self.logger.info("Found matching markets for teams",
                           home_team=home_team.value,
                           away_team=away_team.value,
                           markets_found=len(matching_markets))
            
            return matching_markets
            
        except Exception as e:
            self.logger.error("Failed to scrape team matchup data", 
                            home_team=home_team, 
                            away_team=away_team, 
                            error=str(e))
            return [] 