#!/usr/bin/env python3
"""
Base Data Collection Infrastructure

This module provides the foundational classes and interfaces for the modular
data collection system. Each data source (VSIN/SBD, Action Network, etc.) 
implements the BaseCollector interface for consistent behavior.

Phase 5A Migration: Core Business Logic Implementation
- Supports individual source testing and development
- Provides consistent interface across all collectors
- Enables source-specific configuration and rate limiting
- Supports both sync and async collection patterns
"""

import asyncio
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from pathlib import Path

import aiohttp
import structlog
from pydantic import BaseModel, Field

logger = structlog.get_logger(__name__)


class DataSource(Enum):
    """Enumeration of supported data sources."""
    VSIN = "vsin"
    SBD = "sbd"  # Sports Betting Dime
    ACTION_NETWORK = "action_network"
    SPORTS_BETTING_REPORT = "sports_betting_report"
    MLB_STATS_API = "mlb_stats_api"
    ODDS_API = "odds_api"


class CollectionStatus(Enum):
    """Status of data collection operations."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"
    RATE_LIMITED = "rate_limited"


@dataclass
class CollectionMetrics:
    """Metrics for data collection operations."""
    source: DataSource
    start_time: datetime
    end_time: Optional[datetime] = None
    records_collected: int = 0
    records_valid: int = 0
    records_invalid: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    status: CollectionStatus = CollectionStatus.PENDING
    
    @property
    def duration(self) -> Optional[float]:
        """Calculate collection duration in seconds."""
        if self.end_time and self.start_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.records_collected == 0:
            return 0.0
        return (self.records_valid / self.records_collected) * 100


class CollectorConfig(BaseModel):
    """Configuration for data collectors."""
    source: DataSource
    enabled: bool = True
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    rate_limit_per_minute: int = 60
    timeout_seconds: int = 30
    retry_attempts: int = 3
    retry_delay_seconds: float = 1.0
    headers: Dict[str, str] = Field(default_factory=dict)
    params: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        use_enum_values = True


@dataclass
class CollectionResult:
    """Result of a data collection operation."""
    success: bool
    data: List[Any]
    source: str
    timestamp: datetime
    errors: List[str] = field(default_factory=list)
    metadata: Optional[Dict[str, Any]] = None
    request_count: int = 0
    response_time_ms: float = 0.0
    
    @property
    def has_data(self) -> bool:
        """Check if collection result contains data."""
        return self.success and bool(self.data)
    
    @property
    def error_count(self) -> int:
        """Get number of errors encountered."""
        return len(self.errors)
    
    @property
    def data_count(self) -> int:
        """Get number of data items collected."""
        return len(self.data)


@dataclass
class CollectionRequest:
    """Request parameters for data collection."""
    source: DataSource
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    sport: str = "mlb"
    force: bool = False
    dry_run: bool = False
    additional_params: Dict[str, Any] = field(default_factory=dict)


class BaseCollector(ABC):
    """
    Abstract base class for all data collectors.
    
    Each data source implements this interface to provide consistent
    behavior across different collection systems.
    """
    
    def __init__(self, config: CollectorConfig):
        self.config = config
        self.source = config.source
        self.session: Optional[aiohttp.ClientSession] = None
        self.metrics = CollectionMetrics(
            source=self.source,
            start_time=datetime.now()
        )
        self.logger = logger.bind(source=self.source)  # Remove .value since source is already a string
        
    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.cleanup()
    
    async def initialize(self) -> None:
        """Initialize the collector (setup connections, etc.)."""
        if not self.session:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout_seconds)
            self.session = aiohttp.ClientSession(
                headers=self.config.headers,
                timeout=timeout
            )
        self.logger.info("Collector initialized", source=self.source.value)
    
    async def cleanup(self) -> None:
        """Cleanup resources (close connections, etc.)."""
        if self.session:
            await self.session.close()
            self.session = None
        self.logger.info("Collector cleaned up", source=self.source)  # Remove .value since source is already a string
    
    @abstractmethod
    async def collect_data(self, request: CollectionRequest) -> List[Dict[str, Any]]:
        """
        Collect data from the source.
        
        Args:
            request: Collection request parameters
            
        Returns:
            List of raw data records
        """
        pass
    
    @abstractmethod
    def validate_record(self, record: Dict[str, Any]) -> bool:
        """
        Validate a single data record.
        
        Args:
            record: Raw data record
            
        Returns:
            True if valid, False otherwise
        """
        pass
    
    @abstractmethod
    def normalize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize a data record to standard format.
        
        Args:
            record: Raw data record
            
        Returns:
            Normalized data record
        """
        pass
    
    async def test_connection(self) -> bool:
        """
        Test connection to the data source.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            await self.initialize()
            # Subclasses can override this for source-specific testing
            return True
        except Exception as e:
            self.logger.error("Connection test failed", error=str(e))
            return False
        finally:
            await self.cleanup()
    
    def get_metrics(self) -> CollectionMetrics:
        """Get current collection metrics."""
        return self.metrics
    
    def reset_metrics(self) -> None:
        """Reset collection metrics."""
        self.metrics = CollectionMetrics(
            source=self.source,
            start_time=datetime.now()
        )


class MockCollector(BaseCollector):
    """
    Mock collector for testing and development.
    
    Generates realistic sample data for any source.
    """
    
    def __init__(self, config: CollectorConfig):
        super().__init__(config)
        self.mock_data_templates = {
            DataSource.VSIN: self._get_vsin_mock_data,
            DataSource.SBD: self._get_sbd_mock_data,
            DataSource.ACTION_NETWORK: self._get_action_network_mock_data,
            DataSource.SPORTS_BETTING_REPORT: self._get_sbr_mock_data,
            DataSource.MLB_STATS_API: self._get_mlb_api_mock_data,
            DataSource.ODDS_API: self._get_odds_api_mock_data,
        }
    
    async def collect_data(self, request: CollectionRequest) -> List[Dict[str, Any]]:
        """Generate mock data for the specified source."""
        self.metrics.status = CollectionStatus.IN_PROGRESS
        self.logger.info("Starting mock data collection", 
                        source=self.source.value, dry_run=request.dry_run)
        
        # Simulate collection delay
        await asyncio.sleep(0.5)
        
        # Generate mock data
        mock_generator = self.mock_data_templates.get(self.source)
        if not mock_generator:
            raise ValueError(f"No mock data generator for source: {self.source}")
        
        mock_data = mock_generator()
        
        self.metrics.records_collected = len(mock_data)
        self.metrics.records_valid = len(mock_data)  # Mock data is always valid
        self.metrics.status = CollectionStatus.SUCCESS
        self.metrics.end_time = datetime.now()
        
        self.logger.info("Mock data collection completed",
                        records=len(mock_data), duration=self.metrics.duration)
        
        return mock_data
    
    def validate_record(self, record: Dict[str, Any]) -> bool:
        """Mock data is always valid."""
        return True
    
    def normalize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Mock data is already normalized."""
        record["source"] = self.source.value
        record["collected_at"] = datetime.now().isoformat()
        return record
    
    def _get_vsin_mock_data(self) -> List[Dict[str, Any]]:
        """Generate VSIN-style mock data."""
        return [
            {
                "game": "Yankees @ Red Sox",
                "spread": "-1.5",
                "home_bets_pct": "65%",
                "away_bets_pct": "35%",
                "home_money_pct": "58%",
                "away_money_pct": "42%",
                "book": "circa",
                "sport": "mlb",
                "sharp_action": "moderate"
            },
            {
                "game": "Dodgers vs Giants",
                "total": "8.5",
                "over_bets_pct": "72%",
                "under_bets_pct": "28%",
                "over_money_pct": "68%",
                "under_money_pct": "32%",
                "book": "circa",
                "sport": "mlb",
                "sharp_action": "strong"
            }
        ]
    
    def _get_sbd_mock_data(self) -> List[Dict[str, Any]]:
        """Generate SBD-style mock data."""
        return [
            {
                "matchup": "Cubs @ Cardinals",
                "home_spread_bets": "45%",
                "away_spread_bets": "55%",
                "home_spread_money": "38%",
                "away_spread_money": "62%",
                "sportsbook": "draftkings",
                "league": "mlb"
            }
        ]
    
    def _get_action_network_mock_data(self) -> List[Dict[str, Any]]:
        """Generate Action Network-style mock data."""
        return [
            {
                "game_id": "an_12345",
                "home_team": "Astros",
                "away_team": "Mariners",
                "spread_line": "-2.0",
                "total_line": "9.0",
                "home_ml": "-150",
                "away_ml": "+130",
                "betting_trends": {
                    "spread": {"home_pct": 75, "away_pct": 25},
                    "total": {"over_pct": 60, "under_pct": 40}
                }
            }
        ]
    
    def _get_sbr_mock_data(self) -> List[Dict[str, Any]]:
        """Generate Sports Betting Report-style mock data."""
        return [
            {
                "event": "Phillies vs Braves",
                "date": "2025-01-10",
                "consensus": {
                    "spread": {"line": "-1.5", "home_pct": 58},
                    "total": {"line": "8.5", "over_pct": 65}
                },
                "books": ["draftkings", "fanduel", "betmgm"]
            }
        ]
    
    def _get_mlb_api_mock_data(self) -> List[Dict[str, Any]]:
        """Generate MLB Stats API-style mock data."""
        return [
            {
                "game_pk": 12345,
                "home_team": {"name": "Yankees", "id": 147},
                "away_team": {"name": "Red Sox", "id": 111},
                "game_date": "2025-01-10T19:05:00Z",
                "status": "scheduled",
                "venue": {"name": "Yankee Stadium"}
            }
        ]
    
    def _get_odds_api_mock_data(self) -> List[Dict[str, Any]]:
        """Generate Odds API-style mock data."""
        return [
            {
                "id": "odds_12345",
                "sport_key": "baseball_mlb",
                "home_team": "New York Yankees",
                "away_team": "Boston Red Sox",
                "commence_time": "2025-01-10T19:05:00Z",
                "bookmakers": [
                    {
                        "key": "draftkings",
                        "title": "DraftKings",
                        "markets": [
                            {
                                "key": "h2h",
                                "outcomes": [
                                    {"name": "New York Yankees", "price": -150},
                                    {"name": "Boston Red Sox", "price": 130}
                                ]
                            }
                        ]
                    }
                ]
            }
        ]


class CollectorFactory:
    """Factory for creating data collectors."""
    
    _collectors = {
        DataSource.VSIN: None,  # Will be implemented
        DataSource.SBD: None,  # Will be implemented
        DataSource.ACTION_NETWORK: None,  # Will be implemented
        DataSource.SPORTS_BETTING_REPORT: None,  # Will be implemented
        DataSource.MLB_STATS_API: None,  # Will be implemented
        DataSource.ODDS_API: None,  # Will be implemented
    }
    
    @classmethod
    def create_collector(cls, config: CollectorConfig, mock: bool = False) -> BaseCollector:
        """
        Create a collector for the specified source.
        
        Args:
            config: Collector configuration
            mock: If True, return mock collector
            
        Returns:
            Collector instance
        """
        if mock:
            return MockCollector(config)
        
        collector_class = cls._collectors.get(config.source)
        if not collector_class:
            logger.warning("Real collector not implemented, using mock",
                          source=config.source.value)
            return MockCollector(config)
        
        return collector_class(config)
    
    @classmethod
    def register_collector(cls, source: DataSource, collector_class: type) -> None:
        """Register a collector class for a data source."""
        cls._collectors[source] = collector_class
        logger.info("Collector registered", source=source.value,
                   collector=collector_class.__name__)


# Rate limiting utilities
class RateLimiter:
    """Simple rate limiter for API calls."""
    
    def __init__(self, calls_per_minute: int):
        self.calls_per_minute = calls_per_minute
        self.calls = []
    
    async def acquire(self) -> None:
        """Acquire permission to make an API call."""
        now = time.time()
        
        # Remove calls older than 1 minute
        self.calls = [call_time for call_time in self.calls 
                     if now - call_time < 60]
        
        # Check if we're at the limit
        if len(self.calls) >= self.calls_per_minute:
            sleep_time = 60 - (now - self.calls[0])
            if sleep_time > 0:
                logger.info("Rate limit reached, sleeping", 
                           sleep_time=sleep_time)
                await asyncio.sleep(sleep_time)
        
        self.calls.append(now) 