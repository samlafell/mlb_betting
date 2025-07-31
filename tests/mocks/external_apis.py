"""
Mock implementations for external APIs.

Provides realistic mock responses for Action Network, SBD, VSIN, and other external services.
"""

import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from unittest.mock import Mock, AsyncMock
from uuid import uuid4


class MockAPIResponse:
    """Mock HTTP response object."""
    
    def __init__(self, data: Dict[str, Any], status_code: int = 200):
        self.data = data
        self.status_code = status_code
        self.headers = {"Content-Type": "application/json"}
    
    def json(self) -> Dict[str, Any]:
        """Return JSON data."""
        return self.data
    
    async def json_async(self) -> Dict[str, Any]:
        """Return JSON data (async version)."""
        return self.data
    
    @property
    def text(self) -> str:
        """Return text representation."""
        return json.dumps(self.data)


class ActionNetworkMock:
    """Mock for Action Network API."""
    
    def __init__(self):
        self.call_count = 0
        self.last_request = None
        self.rate_limit_remaining = 100
        
    def generate_game_data(self, game_id: Optional[str] = None) -> Dict[str, Any]:
        """Generate realistic game data."""
        if not game_id:
            game_id = f"an_game_{uuid4().hex[:8]}"
            
        base_time = datetime.utcnow()
        
        return {
            "id": game_id,
            "status": "scheduled",
            "start_time": (base_time + timedelta(hours=2)).isoformat(),
            "league": "mlb",
            "season": 2024,
            "teams": [
                {
                    "id": "team_home_123",
                    "display_name": "New York Yankees",
                    "abbreviation": "NYY",
                    "location": "home"
                },
                {
                    "id": "team_away_456", 
                    "display_name": "Boston Red Sox",
                    "abbreviation": "BOS",
                    "location": "away"
                }
            ],
            "odds": [
                {
                    "sportsbook_id": "draftkings",
                    "sportsbook_name": "DraftKings",
                    "market_type": "moneyline",
                    "outcomes": [
                        {"name": "home", "odds": -150, "line": None},
                        {"name": "away", "odds": 130, "line": None}
                    ],
                    "updated_at": base_time.isoformat()
                },
                {
                    "sportsbook_id": "fanduel",
                    "sportsbook_name": "FanDuel", 
                    "market_type": "spread",
                    "outcomes": [
                        {"name": "home", "odds": -110, "line": -1.5},
                        {"name": "away", "odds": -110, "line": 1.5}
                    ],
                    "updated_at": base_time.isoformat()
                },
                {
                    "sportsbook_id": "betmgm",
                    "sportsbook_name": "BetMGM",
                    "market_type": "total",
                    "outcomes": [
                        {"name": "over", "odds": -110, "line": 8.5},
                        {"name": "under", "odds": -110, "line": 8.5}
                    ],
                    "updated_at": base_time.isoformat()
                }
            ]
        }
    
    def generate_odds_history(self, game_id: str, hours_back: int = 24) -> List[Dict[str, Any]]:
        """Generate historical odds data."""
        history = []
        base_time = datetime.utcnow()
        
        # Generate odds changes over time
        for i in range(hours_back):
            timestamp = base_time - timedelta(hours=i)
            
            # Simulate line movement
            home_odds = -150 + (i * 2)  # Line moving toward home team
            away_odds = 130 - (i * 2)
            spread = -1.5 - (i * 0.1)
            total = 8.5 + (i * 0.1)
            
            history.append({
                "timestamp": timestamp.isoformat(),
                "game_id": game_id,
                "sportsbook": "draftkings",
                "market_type": "moneyline",
                "home_odds": home_odds,
                "away_odds": away_odds,
                "spread": spread,
                "total": total,
                "volume_indicator": "high" if i % 3 == 0 else "normal"
            })
        
        return sorted(history, key=lambda x: x["timestamp"])
    
    async def get_games_async(self, date: Optional[str] = None) -> MockAPIResponse:
        """Mock async games endpoint."""
        self.call_count += 1
        self.rate_limit_remaining -= 1
        
        games = [self.generate_game_data() for _ in range(5)]
        
        return MockAPIResponse({
            "games": games,
            "total": len(games),
            "date": date or datetime.utcnow().strftime("%Y-%m-%d")
        })
    
    async def get_game_odds_async(self, game_id: str) -> MockAPIResponse:
        """Mock async game odds endpoint."""
        self.call_count += 1
        self.rate_limit_remaining -= 1
        
        game_data = self.generate_game_data(game_id)
        
        return MockAPIResponse({
            "game": game_data,
            "odds_history": self.generate_odds_history(game_id)
        })
    
    def simulate_rate_limit(self):
        """Simulate rate limiting."""
        self.rate_limit_remaining = 0
        
    def reset_rate_limit(self):
        """Reset rate limit."""
        self.rate_limit_remaining = 100


class SBDMock:
    """Mock for SportsBettingDime API."""
    
    def generate_sbd_response(self) -> Dict[str, Any]:
        """Generate realistic SBD response."""
        return {
            "posts": [
                {
                    "id": 12345,
                    "title": {"rendered": "Yankees vs Red Sox Betting Odds"},
                    "content": {"rendered": "Latest odds and analysis..."},
                    "date": datetime.utcnow().isoformat(),
                    "meta": {
                        "game_id": f"sbd_game_{uuid4().hex[:8]}",
                        "sportsbooks": ["draftkings", "fanduel", "betmgm"],
                        "odds_data": {
                            "moneyline": {"home": -150, "away": 130},
                            "spread": {"line": -1.5, "odds": -110},
                            "total": {"line": 8.5, "over": -110, "under": -110}
                        }
                    }
                }
            ],
            "total": 1,
            "total_pages": 1
        }
    
    async def get_posts_async(self, params: Dict[str, Any]) -> MockAPIResponse:
        """Mock WordPress API posts endpoint."""
        return MockAPIResponse(self.generate_sbd_response())


class VSINMock:
    """Mock for VSIN website scraping."""
    
    def generate_vsin_html(self) -> str:
        """Generate mock VSIN HTML content."""
        return """
        <html>
        <body>
            <div class="sharp-report">
                <div class="game-row" data-game-id="vsin_123">
                    <span class="teams">NYY @ BOS</span>
                    <span class="sharp-action">75% on BOS +1.5</span>
                    <span class="line-movement">Line moved from +2 to +1.5</span>
                    <span class="steam-indicator">STEAM MOVE</span>
                </div>
                <div class="game-row" data-game-id="vsin_456">
                    <span class="teams">LAD @ SF</span>
                    <span class="sharp-action">Sharp money on Under 9</span>
                    <span class="line-movement">Total dropped from 9.5 to 9</span>
                </div>
            </div>
        </body>
        </html>
        """
    
    async def get_page_async(self, url: str) -> MockAPIResponse:
        """Mock HTML page request."""
        return MockAPIResponse({
            "content": self.generate_vsin_html(),
            "url": url,
            "status": "success"
        })


class MLBStatsAPIMock:
    """Mock for MLB Stats API."""
    
    def generate_schedule_data(self) -> Dict[str, Any]:
        """Generate mock MLB schedule data."""
        return {
            "dates": [
                {
                    "date": datetime.utcnow().strftime("%Y-%m-%d"),
                    "games": [
                        {
                            "gamePk": 123456,
                            "gameDate": datetime.utcnow().isoformat(),
                            "status": {"statusCode": "S", "detailedState": "Scheduled"},
                            "teams": {
                                "home": {"team": {"id": 147, "name": "New York Yankees"}},
                                "away": {"team": {"id": 111, "name": "Boston Red Sox"}}
                            },
                            "venue": {"id": 3313, "name": "Yankee Stadium"}
                        }
                    ]
                }
            ]
        }
    
    async def get_schedule_async(self, date: str) -> MockAPIResponse:
        """Mock schedule endpoint."""
        return MockAPIResponse(self.generate_schedule_data())


class APIClientMockFactory:
    """Factory for creating API client mocks."""
    
    @staticmethod
    def create_http_client_mock():
        """Create a mock HTTP client."""
        mock_client = AsyncMock()
        
        # Setup default responses
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json = AsyncMock(return_value={"status": "success"})
        mock_response.text = "Success"
        
        mock_client.get = AsyncMock(return_value=mock_response)
        mock_client.post = AsyncMock(return_value=mock_response)
        mock_client.put = AsyncMock(return_value=mock_response)
        mock_client.delete = AsyncMock(return_value=mock_response)
        
        return mock_client
    
    @staticmethod
    def create_action_network_client():
        """Create Action Network client mock."""
        return ActionNetworkMock()
    
    @staticmethod
    def create_sbd_client():
        """Create SBD client mock."""
        return SBDMock()
    
    @staticmethod
    def create_vsin_client():
        """Create VSIN client mock."""
        return VSINMock()
    
    @staticmethod
    def create_mlb_stats_client():
        """Create MLB Stats API client mock."""
        return MLBStatsAPIMock()


def create_mock_api_environment() -> Dict[str, Any]:
    """
    Create a complete mock API environment.
    
    Returns:
        Dictionary containing all API mocks
    """
    return {
        "action_network": APIClientMockFactory.create_action_network_client(),
        "sbd": APIClientMockFactory.create_sbd_client(),
        "vsin": APIClientMockFactory.create_vsin_client(),
        "mlb_stats": APIClientMockFactory.create_mlb_stats_client(),
        "http_client": APIClientMockFactory.create_http_client_mock()
    }