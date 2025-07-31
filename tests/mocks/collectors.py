"""
Mock collector implementations for testing.

Provides mock versions of data collectors that don't make external API calls.
"""

import asyncio
import random
import statistics
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable
from unittest.mock import Mock
from uuid import uuid4

from src.data.collection.base import BaseCollector, CollectorConfig, CollectionRequest
from tests.mocks.external_apis import ActionNetworkMock, SBDMock, VSINMock
from tests.utils.retry_utils import (
    RetryManager, RetryConfig, TransientFailureSimulator, 
    retry_async, RetryStrategy
)
from tests.utils.performance_benchmarking import PerformanceBenchmarker, PerformanceThresholds


class MockBaseCollector(BaseCollector):
    """Base mock collector with common functionality."""
    
    def __init__(self, config: CollectorConfig):
        super().__init__(config)
        self.collection_history: List[Dict[str, Any]] = []
        self.should_fail = False
        self.failure_message = "Mock failure"
        self.call_count = 0
        
        # Retry configuration
        self.retry_config = RetryConfig.for_api_calls()
        self.retry_manager = RetryManager(self.retry_config)
        self.failure_simulator = TransientFailureSimulator(failure_rate=0.0, failure_count=0)
        
        # Performance simulation
        self.base_latency = 0.1  # 100ms base latency
        self.latency_variation = 0.05  # ±50ms variation
        
        # Advanced mock features
        self.data_correlation_enabled = False
        self.realistic_patterns_enabled = False
        self.load_simulation_enabled = False
        self.network_conditions = "normal"  # normal, slow, unstable, congested
        self.data_quality_degradation = 0.0  # 0.0 = perfect, 1.0 = completely degraded
        
        # Data correlation tracking
        self.correlated_data_cache: Dict[str, Any] = {}
        self.correlation_rules: List[Callable] = []
        
        # Realistic pattern simulation
        self.time_of_day_patterns = True
        self.seasonal_patterns = True
        self.market_volatility_simulation = True
        
        # Load simulation
        self.concurrent_requests = 0
        self.max_concurrent_requests = 10
        self.load_degradation_threshold = 5
        
        # Performance benchmarking
        self.performance_benchmarker = PerformanceBenchmarker()
        self.performance_thresholds = PerformanceThresholds()
        
    async def collect_data(self, request: CollectionRequest) -> List[Dict[str, Any]]:
        """Mock data collection with retry capabilities."""
        return await self.retry_manager.execute_async(self._collect_data_internal, request)
    
    async def _collect_data_internal(self, request: CollectionRequest) -> List[Dict[str, Any]]:
        """Internal data collection method."""
        self.call_count += 1
        self.collection_history.append({
            "timestamp": datetime.utcnow(),
            "request": request.model_dump() if hasattr(request, 'model_dump') else str(request),
            "call_count": self.call_count
        })
        
        # Apply advanced mock simulations
        await self._simulate_load_conditions()
        await self._simulate_network_conditions()
        
        # Check for configured failures
        if self.should_fail or self.failure_simulator.should_fail():
            raise Exception(self.failure_message)
        
        # Generate base mock data
        raw_data = await self._generate_mock_data(request)
        
        # Apply advanced data processing
        processed_data = []
        for data_item in raw_data:
            # Apply data correlation
            correlated_data = self._apply_data_correlation(data_item, request)
            
            # Apply realistic patterns
            patterned_data = self._apply_realistic_patterns(correlated_data)
            
            # Apply data quality degradation
            final_data = self._apply_data_quality_degradation(patterned_data)
            
            processed_data.append(final_data)
        
        return processed_data
    
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
    
    def configure_transient_failures(self, failure_rate: float = 0.3, failure_count: int = 2):
        """Configure transient failure simulation."""
        self.failure_simulator = TransientFailureSimulator(failure_rate, failure_count)
    
    def configure_retry_behavior(self, retry_config: RetryConfig):
        """Configure retry behavior."""
        self.retry_config = retry_config
        self.retry_manager = RetryManager(retry_config)
    
    def set_latency_simulation(self, base_latency: float, variation: float = 0.05):
        """Configure latency simulation."""
        self.base_latency = base_latency
        self.latency_variation = variation
    
    def get_retry_statistics(self) -> Dict[str, Any]:
        """Get retry statistics."""
        return self.retry_manager.statistics.get_summary()
    
    def reset_retry_statistics(self):
        """Reset retry statistics."""
        self.retry_manager.statistics = self.retry_manager.statistics.__class__()
        self.failure_simulator.reset()
    
    # Advanced mock configuration methods
    
    def enable_data_correlation(self, correlation_strength: float = 0.8):
        """Enable data correlation between related requests."""
        self.data_correlation_enabled = True
        self.correlation_strength = correlation_strength
    
    def add_correlation_rule(self, rule_func: Callable[[Dict[str, Any]], Dict[str, Any]]):
        """Add custom data correlation rule."""
        self.correlation_rules.append(rule_func)
    
    def enable_realistic_patterns(self, time_of_day: bool = True, seasonal: bool = True, volatility: bool = True):
        """Enable realistic data patterns simulation."""
        self.realistic_patterns_enabled = True
        self.time_of_day_patterns = time_of_day
        self.seasonal_patterns = seasonal
        self.market_volatility_simulation = volatility
    
    def set_network_conditions(self, condition: str):
        """Set network condition simulation."""
        valid_conditions = ["normal", "slow", "unstable", "congested"]
        if condition not in valid_conditions:
            raise ValueError(f"Invalid condition. Must be one of: {valid_conditions}")
        self.network_conditions = condition
        
        # Adjust latency based on conditions
        condition_multipliers = {
            "normal": 1.0,
            "slow": 3.0,
            "unstable": 2.0,
            "congested": 5.0
        }
        
        self.base_latency = 0.1 * condition_multipliers[condition]
        self.latency_variation = self.base_latency * 0.5
    
    def set_data_quality_degradation(self, degradation_level: float):
        """Set data quality degradation level (0.0 = perfect, 1.0 = completely degraded)."""
        self.data_quality_degradation = max(0.0, min(1.0, degradation_level))
    
    def enable_load_simulation(self, max_concurrent: int = 10, degradation_threshold: int = 5):
        """Enable load simulation with concurrency limits."""
        self.load_simulation_enabled = True
        self.max_concurrent_requests = max_concurrent
        self.load_degradation_threshold = degradation_threshold
    
    async def _simulate_network_conditions(self):
        """Simulate realistic network conditions."""
        base_delay = self.base_latency
        
        if self.network_conditions == "unstable":
            # Random spikes in latency
            if random.random() < 0.1:  # 10% chance of spike
                base_delay *= random.uniform(5, 10)
        
        elif self.network_conditions == "congested":
            # Progressive delay based on concurrent requests
            if self.load_simulation_enabled:
                congestion_factor = max(1.0, self.concurrent_requests / self.load_degradation_threshold)
                base_delay *= congestion_factor
        
        # Add realistic jitter
        jitter = random.uniform(-self.latency_variation, self.latency_variation)
        final_delay = max(0.01, base_delay + jitter)
        
        await asyncio.sleep(final_delay)
    
    def _apply_data_correlation(self, data: Dict[str, Any], request: CollectionRequest) -> Dict[str, Any]:
        """Apply data correlation based on historical patterns."""
        if not self.data_correlation_enabled:
            return data
        
        # Create correlation key based on request
        correlation_key = f"{request.source}_{getattr(request, 'date_range', {}).get('start', 'unknown')}"
        
        # Apply correlation rules
        for rule in self.correlation_rules:
            try:
                data = rule(data)
            except Exception:
                continue  # Skip failing rules
        
        # Store in correlation cache
        self.correlated_data_cache[correlation_key] = data
        
        # Apply correlation with cached data
        if len(self.correlated_data_cache) > 1:
            # Simple correlation: adjust values based on similar previous data
            correlation_factor = self.correlation_strength
            
            for key, value in data.items():
                if isinstance(value, (int, float)) and key in ["odds", "line", "price"]:
                    # Find similar cached values
                    similar_values = []
                    for cached_data in self.correlated_data_cache.values():
                        if key in cached_data and isinstance(cached_data[key], (int, float)):
                            similar_values.append(cached_data[key])
                    
                    if similar_values:
                        avg_similar = statistics.mean(similar_values)
                        # Blend current value with historical average
                        data[key] = value * (1 - correlation_factor) + avg_similar * correlation_factor
        
        return data
    
    def _apply_realistic_patterns(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply realistic market patterns to data."""
        if not self.realistic_patterns_enabled:
            return data
        
        current_time = datetime.utcnow()
        
        # Time of day patterns
        if self.time_of_day_patterns:
            hour = current_time.hour
            # Market activity patterns: higher volume during business hours
            if 9 <= hour <= 17:  # Business hours
                activity_multiplier = 1.5
            elif 18 <= hour <= 22:  # Evening
                activity_multiplier = 1.2
            else:  # Night/early morning
                activity_multiplier = 0.7
            
            # Apply to numeric fields
            for key, value in data.items():
                if key in ["volume", "activity", "betting_volume"] and isinstance(value, (int, float)):
                    data[key] = value * activity_multiplier
        
        # Market volatility simulation
        if self.market_volatility_simulation:
            volatility_factor = random.uniform(0.95, 1.05)  # ±5% volatility
            
            for key, value in data.items():
                if key in ["odds", "line", "price"] and isinstance(value, (int, float)):
                    data[key] = value * volatility_factor
        
        # Seasonal patterns
        if self.seasonal_patterns:
            month = current_time.month
            # MLB season patterns
            if 4 <= month <= 10:  # Baseball season
                seasonal_factor = 1.3
            elif month in [3, 11]:  # Pre/post season
                seasonal_factor = 1.1
            else:  # Off season
                seasonal_factor = 0.5
            
            # Apply to baseball-related fields
            for key, value in data.items():
                if "baseball" in key.lower() or "mlb" in key.lower():
                    if isinstance(value, (int, float)):
                        data[key] = value * seasonal_factor
        
        return data
    
    def _apply_data_quality_degradation(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply data quality degradation."""
        if self.data_quality_degradation == 0.0:
            return data
        
        degraded_data = data.copy()
        
        # Randomly remove or corrupt fields based on degradation level
        for key, value in list(degraded_data.items()):
            if random.random() < self.data_quality_degradation:
                if random.random() < 0.5:
                    # Remove field completely
                    del degraded_data[key]
                else:
                    # Corrupt field value
                    if isinstance(value, str):
                        degraded_data[key] = value[:int(len(value) * (1 - self.data_quality_degradation))]
                    elif isinstance(value, (int, float)):
                        noise_factor = random.uniform(0.8, 1.2)
                        degraded_data[key] = value * noise_factor
                    elif isinstance(value, list):
                        # Remove some items
                        keep_count = max(1, int(len(value) * (1 - self.data_quality_degradation)))
                        degraded_data[key] = value[:keep_count]
        
        return degraded_data
    
    async def _simulate_load_conditions(self):
        """Simulate load-based performance degradation."""
        if not self.load_simulation_enabled:
            return
        
        # Track concurrent requests
        self.concurrent_requests += 1
        
        try:
            # Apply load-based delays
            if self.concurrent_requests > self.load_degradation_threshold:
                # Progressive degradation
                overload_factor = self.concurrent_requests / self.load_degradation_threshold
                additional_delay = self.base_latency * (overload_factor - 1) * 0.5
                await asyncio.sleep(additional_delay)
            
            # Simulate queue delays at high load
            if self.concurrent_requests >= self.max_concurrent_requests:
                queue_delay = random.uniform(0.1, 0.5)
                await asyncio.sleep(queue_delay)
        
        finally:
            self.concurrent_requests = max(0, self.concurrent_requests - 1)
    
    def get_mock_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics for the mock collector."""
        return {
            "total_calls": self.call_count,
            "concurrent_requests": self.concurrent_requests,
            "network_conditions": self.network_conditions,
            "data_quality_degradation": self.data_quality_degradation,
            "base_latency_ms": self.base_latency * 1000,
            "correlation_cache_size": len(self.correlated_data_cache),
            "correlation_rules_count": len(self.correlation_rules),
            "load_simulation_enabled": self.load_simulation_enabled,
            "realistic_patterns_enabled": self.realistic_patterns_enabled
        }
    
    def reset_mock_state(self):
        """Reset all mock state to defaults."""
        self.call_count = 0
        self.concurrent_requests = 0
        self.correlated_data_cache.clear()
        self.correlation_rules.clear()
        self.collection_history.clear()
        self.data_correlation_enabled = False
        self.realistic_patterns_enabled = False
        self.load_simulation_enabled = False
        self.network_conditions = "normal"
        self.data_quality_degradation = 0.0
        self.base_latency = 0.1
        self.latency_variation = 0.05


class MockActionNetworkCollector(MockBaseCollector):
    """Mock Action Network collector."""
    
    def __init__(self, config: CollectorConfig):
        super().__init__(config)
        self.api_mock = ActionNetworkMock()
        self.source_name = "action_network"
    
    async def _generate_mock_data(self, request: CollectionRequest) -> List[Dict[str, Any]]:
        """Generate mock Action Network data."""
        games_response = await self.api_mock.get_games_async()
        games_data = games_response.json()
        
        results = []
        for game in games_data["games"]:
            # Simulate collecting odds history for each game
            odds_response = await self.api_mock.get_game_odds_async(game["id"])
            odds_data = odds_response.json()
            
            # Enhanced mock data with realistic betting metrics
            base_result = {
                "external_game_id": game["id"],
                "game_data": game,
                "odds_data": odds_data["game"]["odds"],
                "odds_history": odds_data["odds_history"],
                "collected_at": datetime.utcnow().isoformat(),
                "source": self.source_name,
                "_mock": True
            }
            
            # Add realistic betting data
            if self.realistic_patterns_enabled:
                base_result.update({
                    "betting_volume": random.randint(1000, 50000),
                    "sharp_action_percentage": random.uniform(15, 85),
                    "public_betting_percentage": random.uniform(20, 80),
                    "line_movement_velocity": random.uniform(-5, 5),
                    "market_efficiency_score": random.uniform(0.7, 0.95),
                    "odds": random.uniform(-200, 200),  # American odds
                    "line": random.uniform(-2.5, 2.5),  # Spread
                    "total": random.uniform(7.5, 12.5)  # Over/under
                })
            
            results.append(base_result)
        
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
            from src.data.collection.base import DataSource
            config = CollectorConfig(
                source=DataSource.ACTION_NETWORK,
                enabled=True,
                rate_limit_per_minute=100,
                timeout_seconds=30
            )
        return MockActionNetworkCollector(config)
    
    @staticmethod
    def create_sbd_mock(config: Optional[CollectorConfig] = None) -> MockSBDCollector:
        """Create SBD collector mock."""
        if config is None:
            from src.data.collection.base import DataSource
            config = CollectorConfig(
                source=DataSource.SBD,
                enabled=True,
                rate_limit_per_minute=100,
                timeout_seconds=30
            )
        return MockSBDCollector(config)
    
    @staticmethod
    def create_vsin_mock(config: Optional[CollectorConfig] = None) -> MockVSINCollector:
        """Create VSIN collector mock."""
        if config is None:
            from src.data.collection.base import DataSource
            config = CollectorConfig(
                source=DataSource.VSIN,
                enabled=True,
                rate_limit_per_minute=100,
                timeout_seconds=30
            )
        return MockVSINCollector(config)
    
    @staticmethod
    def create_mlb_stats_mock(config: Optional[CollectorConfig] = None) -> MockMLBStatsCollector:
        """Create MLB Stats collector mock."""
        if config is None:
            from src.data.collection.base import DataSource
            config = CollectorConfig(
                source=DataSource.MLB_STATS_API,
                enabled=True,
                rate_limit_per_minute=100,
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


def create_advanced_mock_environment(scenario: str = "production") -> Dict[str, Any]:
    """
    Create advanced mock environment with realistic scenarios.
    
    Args:
        scenario: Mock scenario type (development, testing, production, stress)
        
    Returns:
        Dictionary containing configured mock environment
    """
    mocks = CollectorMockFactory.create_all_mocks()
    
    # Configure mocks based on scenario
    if scenario == "development":
        # Fast, reliable mocks for development
        for mock in mocks.values():
            mock.set_network_conditions("normal")
            mock.enable_realistic_patterns(time_of_day=True, seasonal=False, volatility=False)
            mock.set_data_quality_degradation(0.0)
    
    elif scenario == "testing":
        # Moderate realism for testing
        for mock in mocks.values():
            mock.set_network_conditions("normal")
            mock.enable_realistic_patterns(time_of_day=True, seasonal=True, volatility=True)
            mock.enable_data_correlation(correlation_strength=0.6)
            mock.set_data_quality_degradation(0.1)
    
    elif scenario == "production":
        # High realism for production-like testing
        for mock in mocks.values():
            mock.set_network_conditions("slow")
            mock.enable_realistic_patterns(time_of_day=True, seasonal=True, volatility=True)
            mock.enable_data_correlation(correlation_strength=0.8)
            mock.enable_load_simulation(max_concurrent=15, degradation_threshold=8)
            mock.set_data_quality_degradation(0.05)
            
            # Add correlation rules
            def correlation_rule(data):
                # Simulate market correlation: high volume = tighter spreads
                if "betting_volume" in data and "line" in data:
                    volume_factor = min(1.0, data["betting_volume"] / 25000)  # Normalize
                    line_tightening = volume_factor * 0.1
                    data["line"] = data["line"] * (1 - line_tightening)
                return data
            
            mock.add_correlation_rule(correlation_rule)
    
    elif scenario == "stress":
        # High load, degraded conditions for stress testing
        for mock in mocks.values():
            mock.set_network_conditions("congested")
            mock.enable_realistic_patterns(time_of_day=True, seasonal=True, volatility=True)
            mock.enable_data_correlation(correlation_strength=0.9)
            mock.enable_load_simulation(max_concurrent=5, degradation_threshold=3)
            mock.set_data_quality_degradation(0.2)
            mock.configure_transient_failures(failure_rate=0.1, failure_count=0)
    
    else:
        raise ValueError(f"Unknown scenario: {scenario}")
    
    return {
        "collectors": mocks,
        "factory": CollectorMockFactory(),
        "scenario": scenario,
        "utilities": {
            "reset_all": lambda: [mock.reset_mock_state() for mock in mocks.values()],
            "get_performance_metrics": lambda: {
                name: mock.get_mock_performance_metrics() 
                for name, mock in mocks.items()
            },
            "set_scenario": lambda new_scenario: create_advanced_mock_environment(new_scenario),
            "simulate_peak_load": lambda: [
                mock.enable_load_simulation(max_concurrent=20, degradation_threshold=5)
                for mock in mocks.values()
            ],
            "simulate_network_issues": lambda: [
                mock.set_network_conditions("unstable") for mock in mocks.values()
            ]
        }
    }