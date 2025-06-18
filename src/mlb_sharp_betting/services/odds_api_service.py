"""
The Odds API service for retrieving MLB betting odds.
Implements intelligent usage tracking to stay within monthly limits.
"""

import os
import json
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path

from ..core.config import get_settings
from ..core.logging import get_logger
from ..models.base import BaseModel

logger = get_logger(__name__)


class OddsAPIUsageTracker:
    """Tracks API usage to prevent exceeding monthly limits."""
    
    def __init__(self, max_monthly_usage: int = 480):
        self.max_monthly_usage = max_monthly_usage
        self.usage_file = Path("data/odds_api_usage.json")
        self.usage_file.parent.mkdir(parents=True, exist_ok=True)
        
    def _load_usage_data(self) -> Dict:
        """Load usage data from file."""
        if not self.usage_file.exists():
            return {"month": datetime.now().strftime("%Y-%m"), "used": 0, "calls": []}
        
        try:
            with open(self.usage_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            return {"month": datetime.now().strftime("%Y-%m"), "used": 0, "calls": []}
    
    def _save_usage_data(self, data: Dict) -> None:
        """Save usage data to file."""
        with open(self.usage_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def can_make_call(self, estimated_cost: int) -> bool:
        """Check if we can make a call without exceeding limits."""
        data = self._load_usage_data()
        current_month = datetime.now().strftime("%Y-%m")
        
        # Reset if new month
        if data["month"] != current_month:
            data = {"month": current_month, "used": 0, "calls": []}
            self._save_usage_data(data)
        
        return (data["used"] + estimated_cost) <= self.max_monthly_usage
    
    def record_usage(self, cost: int, endpoint: str, markets: List[str]) -> None:
        """Record API usage."""
        data = self._load_usage_data()
        current_month = datetime.now().strftime("%Y-%m")
        
        # Reset if new month
        if data["month"] != current_month:
            data = {"month": current_month, "used": 0, "calls": []}
        
        # Record the call
        data["used"] += cost
        data["calls"].append({
            "timestamp": datetime.now().isoformat(),
            "cost": cost,
            "endpoint": endpoint,
            "markets": markets
        })
        
        self._save_usage_data(data)
        logger.info(f"Odds API usage: {cost} credits used, {data['used']}/{self.max_monthly_usage} total this month")
    
    def get_usage_status(self) -> Dict:
        """Get current usage status."""
        data = self._load_usage_data()
        current_month = datetime.now().strftime("%Y-%m")
        
        if data["month"] != current_month:
            return {
                "month": current_month,
                "used": 0,
                "remaining": self.max_monthly_usage,
                "percentage_used": 0.0
            }
        
        return {
            "month": data["month"],
            "used": data["used"],
            "remaining": self.max_monthly_usage - data["used"],
            "percentage_used": (data["used"] / self.max_monthly_usage) * 100
        }


class OddsAPIService:
    """Service for retrieving MLB betting odds from The Odds API."""
    
    BASE_URL = "https://api.the-odds-api.com/v4"
    SPORT_KEY = "baseball_mlb"
    
    # Market configurations with their priorities
    MARKET_CONFIGS = {
        "essential": ["h2h"],  # Moneyline only - 10 credits
        "standard": ["h2h", "spreads", "totals"],  # All main markets - 30 credits
        "comprehensive": ["h2h", "spreads", "totals", "h2h_lay"]  # Extended - 40 credits
    }
    
    def __init__(self):
        self.api_key = os.getenv("ODDS_API_KEY")
        if not self.api_key:
            raise ValueError("ODDS_API_KEY environment variable is required")
        
        self.usage_tracker = OddsAPIUsageTracker()
        self.session = requests.Session()
        
    def _calculate_cost(self, markets: List[str], regions: int = 1) -> int:
        """Calculate the cost of an API call."""
        return 10 * len(markets) * regions
    
    def _make_request(self, endpoint: str, params: Dict) -> Optional[Dict]:
        """Make a request to The Odds API."""
        url = f"{self.BASE_URL}/{endpoint}"
        params["apiKey"] = self.api_key
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            # Track usage from response headers
            if "x-requests-last" in response.headers:
                actual_cost = int(response.headers["x-requests-last"])
                markets = params.get("markets", "").split(",") if params.get("markets") else []
                self.usage_tracker.record_usage(actual_cost, endpoint, markets)
            
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Odds API request failed: {e}")
            return None
    
    def get_mlb_odds(self, 
                     market_config: str = "standard",
                     regions: str = "us",
                     odds_format: str = "american") -> Optional[List[Dict]]:
        """
        Get MLB odds with intelligent market selection.
        
        Args:
            market_config: 'essential', 'standard', or 'comprehensive'
            regions: Comma-separated regions (default: 'us')
            odds_format: 'american' or 'decimal'
        
        Returns:
            List of games with odds data
        """
        if market_config not in self.MARKET_CONFIGS:
            raise ValueError(f"Invalid market_config. Choose from: {list(self.MARKET_CONFIGS.keys())}")
        
        markets = self.MARKET_CONFIGS[market_config]
        estimated_cost = self._calculate_cost(markets, len(regions.split(",")))
        
        # Check if we can make the call
        if not self.usage_tracker.can_make_call(estimated_cost):
            usage_status = self.usage_tracker.get_usage_status()
            logger.warning(f"Cannot make Odds API call. Would exceed monthly limit. "
                         f"Current usage: {usage_status['used']}/{usage_status['remaining']}")
            return None
        
        params = {
            "regions": regions,
            "markets": ",".join(markets),
            "oddsFormat": odds_format,
            "dateFormat": "iso"
        }
        
        logger.info(f"Fetching MLB odds with {market_config} markets (estimated cost: {estimated_cost})")
        return self._make_request(f"sports/{self.SPORT_KEY}/odds", params)
    
    def get_specific_game_odds(self, 
                              event_id: str,
                              market_config: str = "standard",
                              regions: str = "us",
                              odds_format: str = "american") -> Optional[Dict]:
        """
        Get odds for a specific game.
        
        Args:
            event_id: The event ID from The Odds API
            market_config: 'essential', 'standard', or 'comprehensive'
            regions: Comma-separated regions
            odds_format: 'american' or 'decimal'
        
        Returns:
            Game odds data
        """
        markets = self.MARKET_CONFIGS[market_config]
        estimated_cost = self._calculate_cost(markets, len(regions.split(",")))
        
        if not self.usage_tracker.can_make_call(estimated_cost):
            usage_status = self.usage_tracker.get_usage_status()
            logger.warning(f"Cannot make Odds API call for game {event_id}. "
                         f"Current usage: {usage_status['used']}/{usage_status['remaining']}")
            return None
        
        params = {
            "regions": regions,
            "markets": ",".join(markets),
            "oddsFormat": odds_format,
            "dateFormat": "iso"
        }
        
        return self._make_request(f"sports/{self.SPORT_KEY}/events/{event_id}/odds", params)
    
    def get_today_games(self) -> Optional[List[Dict]]:
        """Get today's MLB games (free endpoint - no usage cost)."""
        return self._make_request(f"sports/{self.SPORT_KEY}/events", {
            "dateFormat": "iso"
        })
    
    def get_usage_status(self) -> Dict:
        """Get current API usage status."""
        return self.usage_tracker.get_usage_status()
    
    def optimize_for_budget(self, games_needed: int) -> str:
        """
        Recommend optimal market configuration based on remaining budget.
        
        Args:
            games_needed: Number of games you need odds for
        
        Returns:
            Recommended market configuration
        """
        status = self.get_usage_status()
        remaining = status["remaining"]
        
        # Calculate what we can afford
        if remaining >= games_needed * 30:  # Standard config
            return "standard"
        elif remaining >= games_needed * 10:  # Essential config
            return "essential"
        else:
            max_games = remaining // 10
            logger.warning(f"Budget allows for only {max_games} games with essential markets")
            return "essential"


# Model for structured odds data
class OddsData(BaseModel):
    """Structured odds data from The Odds API."""
    
    event_id: str
    sport_key: str
    commence_time: str
    home_team: str
    away_team: str
    bookmakers: List[Dict]
    
    @classmethod
    def from_odds_api(cls, data: Dict) -> "OddsData":
        """Create OddsData from Odds API response."""
        return cls(
            event_id=data["id"],
            sport_key=data["sport_key"],
            commence_time=data["commence_time"],
            home_team=data["home_team"],
            away_team=data["away_team"],
            bookmakers=data.get("bookmakers", [])
        )
    
    def get_moneyline_odds(self, bookmaker_key: str = None) -> Optional[Dict]:
        """Extract moneyline odds from bookmakers."""
        for bookmaker in self.bookmakers:
            if bookmaker_key and bookmaker["key"] != bookmaker_key:
                continue
                
            for market in bookmaker.get("markets", []):
                if market["key"] == "h2h":
                    return {
                        "bookmaker": bookmaker["title"],
                        "outcomes": market["outcomes"]
                    }
        return None
    
    def get_spread_odds(self, bookmaker_key: str = None) -> Optional[Dict]:
        """Extract spread odds from bookmakers."""
        for bookmaker in self.bookmakers:
            if bookmaker_key and bookmaker["key"] != bookmaker_key:
                continue
                
            for market in bookmaker.get("markets", []):
                if market["key"] == "spreads":
                    return {
                        "bookmaker": bookmaker["title"],
                        "outcomes": market["outcomes"]
                    }
        return None
    
    def get_total_odds(self, bookmaker_key: str = None) -> Optional[Dict]:
        """Extract total (over/under) odds from bookmakers."""
        for bookmaker in self.bookmakers:
            if bookmaker_key and bookmaker["key"] != bookmaker_key:
                continue
                
            for market in bookmaker.get("markets", []):
                if market["key"] == "totals":
                    return {
                        "bookmaker": bookmaker["title"],
                        "outcomes": market["outcomes"]
                    }
        return None 