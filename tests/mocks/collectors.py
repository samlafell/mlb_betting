"""
Mock collector implementations for testing.

Provides mock versions of data collectors that don't make external API calls.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from unittest.mock import Mock
from uuid import uuid4

from src.data.collection.base import BaseCollector, CollectorConfig, CollectionRequest
from tests.mocks.external_apis import ActionNetworkMock, SBDMock, VSINMock


class MockBaseCollector(BaseCollector):
    """Base mock collector with common functionality."""
    
    def __init__(self, config: CollectorConfig):
        super().__init__(config)
        self.collection_history: List[Dict[str, Any]] = []
        self.should_fail = False
        self.failure_message = "Mock failure"
        self.call_count = 0
        
    async def collect_data(self, request: CollectionRequest) -> List[Dict[str, Any]]:
        """Mock data collection."""
        self.call_count += 1
        self.collection_history.append({
            "timestamp": datetime.utcnow(),
            "request": request.model_dump() if hasattr(request, 'model_dump') else str(request),
            "call_count": self.call_count
        })
        
        if self.should_fail:
            raise Exception(self.failure_message)
        
        return await self._generate_mock_data(request)
    
    async def _generate_mock_data(self, request: CollectionRequest) -> List[Dict[str, Any]]:
        """Generate mock data - to be overridden by subclasses."""
        return [{"mock": True, "timestamp": datetime.utcnow().isoformat()}]
    
    def validate_record(self, record: Dict[str, Any]) -> bool:
        """Mock validation - always passes unless configured otherwise."""
        return not self.should_fail
    
    def normalize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Mock normalization - adds mock metadata."""
        normalized = record.copy()
        normalized["_mock_normalized"] = True
        normalized["_normalized_at"] = datetime.utcnow().isoformat()
        return normalized
    
    def set_failure_mode(self, should_fail: bool, message: str = "Mock failure"):
        """Configure the collector to fail."""
        self.should_fail = should_fail
        self.failure_message = message
    
    def get_collection_history(self) -> List[Dict[str, Any]]:
        """Get history of collection calls."""
        return self.collection_history.copy()
    
    def reset_history(self):
        """Reset collection history."""
        self.collection_history.clear()
        self.call_count = 0


class MockActionNetworkCollector(MockBaseCollector):
    """Mock Action Network collector."""
    
    def __init__(self, config: CollectorConfig):
        super().__init__(config)
        self.api_mock = ActionNetworkMock()
        self.source_name = "action_network"
    
    async def _generate_mock_data(self, request: CollectionRequest) -> List[Dict[str, Any]]:
        """Generate mock Action Network data."""
        # Simulate API delay
        await asyncio.sleep(0.1)
        
        games_response = await self.api_mock.get_games_async()
        games_data = games_response.json()
        
        results = []
        for game in games_data["games"]:
            # Simulate collecting odds history for each game
            odds_response = await self.api_mock.get_game_odds_async(game["id"])
            odds_data = odds_response.json()
            
            results.append({
                "external_game_id": game["id"],
                "game_data": game,
                "odds_data": odds_data["game"]["odds"],
                "odds_history": odds_data["odds_history"],
                "collected_at": datetime.utcnow().isoformat(),
                "source": self.source_name,
                "_mock": True
            })
        
        return results
    
    def simulate_rate_limit(self):
        """Simulate API rate limiting."""
        self.api_mock.simulate_rate_limit()
        self.set_failure_mode(True, "Rate limit exceeded")
    
    def reset_rate_limit(self):
        """Reset rate limiting."""
        self.api_mock.reset_rate_limit()
        self.set_failure_mode(False)


class MockSBDCollector(MockBaseCollector):
    """Mock SportsBettingDime collector."""
    
    def __init__(self, config: CollectorConfig):
        super().__init__(config)
        self.api_mock = SBDMock()
        self.source_name = "sbd"
    
    async def _generate_mock_data(self, request: CollectionRequest) -> List[Dict[str, Any]]:
        """Generate mock SBD data."""
        await asyncio.sleep(0.1)
        
        posts_response = await self.api_mock.get_posts_async({})
        posts_data = posts_response.json()
        
        results = []
        for post in posts_data["posts"]:
            results.append({
                "external_post_id": post["id"],
                "title": post["title"]["rendered"],
                "content": post["content"]["rendered"],
                "post_date": post["date"],
                "meta_data": post.get("meta", {}),
                "collected_at": datetime.utcnow().isoformat(),
                "source": self.source_name,
                "_mock": True
            })
        
        return results


class MockVSINCollector(MockBaseCollector):
    """Mock VSIN collector."""
    
    def __init__(self, config: CollectorConfig):
        super().__init__(config)
        self.api_mock = VSINMock()
        self.source_name = "vsin"
    
    async def _generate_mock_data(self, request: CollectionRequest) -> List[Dict[str, Any]]:
        """Generate mock VSIN data."""
        await asyncio.sleep(0.1)
        
        page_response = await self.api_mock.get_page_async("https://vsin.com/sharp-report")
        page_data = page_response.json()
        
        # Parse mock HTML content (simplified)
        html_content = page_data["content"]
        
        results = [{
            "url": page_data["url"],
            "html_content": html_content,
            "parsed_reports": self._parse_mock_html(html_content),
            "collected_at": datetime.utcnow().isoformat(),
            "source": self.source_name,
            "_mock": True
        }]
        
        return results
    
    def _parse_mock_html(self, html: str) -> List[Dict[str, Any]]:
        """Parse mock HTML content."""
        # Simplified parsing for mock data
        return [
            {
                "game_id": "vsin_123",
                "teams": "NYY @ BOS",
                "sharp_action": "75% on BOS +1.5",
                "line_movement": "Line moved from +2 to +1.5",
                "indicators": ["STEAM MOVE"]
            },
            {
                "game_id": "vsin_456", 
                "teams": "LAD @ SF",
                "sharp_action": "Sharp money on Under 9",
                "line_movement": "Total dropped from 9.5 to 9",
                "indicators": []
            }
        ]


class MockMLBStatsCollector(MockBaseCollector):
    """Mock MLB Stats API collector."""
    
    def __init__(self, config: CollectorConfig):
        super().__init__(config)
        self.source_name = "mlb_stats"
    
    async def _generate_mock_data(self, request: CollectionRequest) -> List[Dict[str, Any]]:
        """Generate mock MLB Stats data."""
        await asyncio.sleep(0.1)
        
        # Generate mock schedule data
        games = []
        for i in range(10):  # Mock 10 games
            game_pk = 123456 + i
            games.append({
                "gamePk": game_pk,
                "external_game_id": f"mlb_stats_{game_pk}",
                "gameDate": (datetime.utcnow() + timedelta(days=i)).isoformat(),
                "status": {"statusCode": "S", "detailedState": "Scheduled"},
                "teams": {
                    "home": {"team": {"id": 147 + i, "name": f"Home Team {i}"}},
                    "away": {"team": {"id": 111 + i, "name": f"Away Team {i}"}}
                },
                "venue": {"id": 3313 + i, "name": f"Stadium {i}"},
                "collected_at": datetime.utcnow().isoformat(),
                "source": self.source_name,
                "_mock": True
            })
        
        return games


class CollectorMockFactory:
    """Factory for creating collector mocks."""
    
    @staticmethod
    def create_action_network_mock(config: Optional[CollectorConfig] = None) -> MockActionNetworkCollector:
        """Create Action Network collector mock."""
        if config is None:
            config = CollectorConfig(
                source_name="action_network",
                rate_limit_requests=100,
                rate_limit_period=3600,
                timeout_seconds=30
            )
        return MockActionNetworkCollector(config)
    
    @staticmethod
    def create_sbd_mock(config: Optional[CollectorConfig] = None) -> MockSBDCollector:
        """Create SBD collector mock."""
        if config is None:
            config = CollectorConfig(
                source_name="sbd",
                rate_limit_requests=100,
                rate_limit_period=3600,
                timeout_seconds=30
            )
        return MockSBDCollector(config)
    
    @staticmethod
    def create_vsin_mock(config: Optional[CollectorConfig] = None) -> MockVSINCollector:
        """Create VSIN collector mock."""
        if config is None:
            config = CollectorConfig(
                source_name="vsin",
                rate_limit_requests=100,
                rate_limit_period=3600,
                timeout_seconds=30
            )
        return MockVSINCollector(config)
    
    @staticmethod
    def create_mlb_stats_mock(config: Optional[CollectorConfig] = None) -> MockMLBStatsCollector:
        """Create MLB Stats collector mock."""
        if config is None:
            config = CollectorConfig(
                source_name="mlb_stats",
                rate_limit_requests=100,
                rate_limit_period=3600,
                timeout_seconds=30
            )
        return MockMLBStatsCollector(config)
    
    @staticmethod
    def create_all_mocks() -> Dict[str, MockBaseCollector]:
        """Create all collector mocks."""
        return {
            "action_network": CollectorMockFactory.create_action_network_mock(),
            "sbd": CollectorMockFactory.create_sbd_mock(),
            "vsin": CollectorMockFactory.create_vsin_mock(),
            "mlb_stats": CollectorMockFactory.create_mlb_stats_mock()
        }


def create_mock_collector_environment() -> Dict[str, Any]:
    """
    Create a complete mock collector environment.
    
    Returns:
        Dictionary containing all collector mocks and utilities
    """
    mocks = CollectorMockFactory.create_all_mocks()
    
    return {
        "collectors": mocks,
        "factory": CollectorMockFactory(),
        "utilities": {
            "reset_all": lambda: [mock.reset_history() for mock in mocks.values()],
            "set_all_failure": lambda fail, msg="Mock failure": [
                mock.set_failure_mode(fail, msg) for mock in mocks.values()
            ],
            "get_all_history": lambda: {
                name: mock.get_collection_history() 
                for name, mock in mocks.items()
            }
        }
    }