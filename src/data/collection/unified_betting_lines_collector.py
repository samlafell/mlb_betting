#!/usr/bin/env python3
"""
Unified Betting Lines Collector

Base class for all betting line collectors that provides standardized integration
with the core_betting schema, MLB Stats API normalization, and data quality tracking.
"""

import asyncio
import uuid
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import psycopg2
import structlog
from psycopg2.extras import RealDictCursor

from ...core.config import UnifiedSettings
from .base import CollectionResult, CollectionStatus, DataSource

logger = structlog.get_logger(__name__)


class DataQualityLevel(Enum):
    """Data quality levels with scoring thresholds."""

    EXCEPTIONAL = ("EXCEPTIONAL", 0.95)
    HIGH = ("HIGH", 0.80)
    MEDIUM = ("MEDIUM", 0.60)
    LOW = ("LOW", 0.40)
    POOR = ("POOR", 0.0)

    def __init__(self, level: str, threshold: float):
        self.level = level
        self.threshold = threshold


@dataclass
class UnifiedCollectionResult:
    """Result of unified data collection operation."""

    status: CollectionStatus
    records_processed: int
    records_stored: int
    message: str = ""
    errors: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_successful(self) -> bool:
        """Check if collection was successful."""
        return self.status == CollectionStatus.SUCCESS

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.records_processed == 0:
            return 1.0
        return self.records_stored / self.records_processed


@dataclass
class ValidationResult:
    """Result of data validation."""

    is_valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    quality_score: float = 0.0

    def add_error(self, error: str):
        self.errors.append(error)
        self.is_valid = False

    def add_warning(self, warning: str):
        self.warnings.append(warning)


@dataclass
class PerformanceMetrics:
    """Performance metrics for collection operations."""

    total_records: int = 0
    successful_records: int = 0
    failed_records: int = 0
    validation_errors: int = 0
    processing_time: float = 0.0
    database_time: float = 0.0

    @property
    def success_rate(self) -> float:
        return (
            (self.successful_records / self.total_records)
            if self.total_records > 0
            else 0.0
        )

    @property
    def error_rate(self) -> float:
        return (
            (self.failed_records / self.total_records)
            if self.total_records > 0
            else 0.0
        )


class ConnectionPool:
    """Database connection pool for improved performance."""

    def __init__(self, settings: UnifiedSettings, pool_size: int = 10):
        self.settings = settings
        self.pool_size = pool_size
        self._pool = []
        self._lock = asyncio.Lock()
        self.logger = logger.bind(component="ConnectionPool")

    @contextmanager
    def get_connection(self):
        """Get a database connection from the pool."""
        try:
            conn = psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor,
            )
            yield conn
        except Exception as e:
            self.logger.error("Database connection error", error=str(e))
            raise
        finally:
            if conn:
                conn.close()


class MLBStatsAPIGameResolver:
    """Enhanced game resolver with caching and batch processing."""

    def __init__(self, connection_pool: ConnectionPool = None):
        self.settings = UnifiedSettings()
        self.logger = logger.bind(component="MLBStatsAPIGameResolver")
        self.connection_pool = connection_pool or ConnectionPool(self.settings)
        self._cache = {}  # In-memory cache for resolved games
        self._cache_expiry = timedelta(hours=24)

    def resolve_game_id(self, external_id: str, source: DataSource) -> int | None:
        """
        Resolve external game ID to curated.games_complete.id with caching.

        Args:
            external_id: Source-specific game identifier
            source: Data source type

        Returns:
            Database game ID or None if not found
        """
        # Check cache first
        cache_key = f"{source.value}:{external_id}"
        if cache_key in self._cache:
            cached_result, timestamp = self._cache[cache_key]
            if datetime.now() - timestamp < self._cache_expiry:
                return cached_result

        try:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    # Dynamic query based on source
                    source_column_map = {
                        DataSource.SPORTS_BOOK_REVIEW_DEPRECATED: "sportsbookreview_game_id",
                        DataSource.ACTION_NETWORK: "action_network_game_id",
                        DataSource.VSIN: "vsin_game_id",
                        DataSource.SPORTS_BETTING_DIME: "sbd_game_id",
                    }

                    column = source_column_map.get(source)
                    if column:
                        cur.execute(
                            f"SELECT id FROM curated.games_complete WHERE {column} = %s",
                            (external_id,),
                        )
                    else:
                        # Try generic lookup
                        cur.execute(
                            "SELECT id FROM curated.games_complete WHERE external_source_id = %s",
                            (external_id,),
                        )

                    result = cur.fetchone()
                    game_id = result["id"] if result else None

                    # Cache the result
                    self._cache[cache_key] = (game_id, datetime.now())

                    if not game_id:
                        self.logger.warning(
                            "Game not found for external ID",
                            external_id=external_id,
                            source=source.value,
                        )

                    return game_id

        except Exception as e:
            self.logger.error(
                "Error resolving game ID",
                external_id=external_id,
                source=source.value,
                error=str(e),
            )
            return None

    def batch_resolve_game_ids(
        self, game_requests: list[tuple[str, DataSource]]
    ) -> dict[str, int | None]:
        """Batch resolve multiple game IDs for improved performance."""
        results = {}

        # Check cache first
        uncached_requests = []
        for external_id, source in game_requests:
            cache_key = f"{source.value}:{external_id}"
            if cache_key in self._cache:
                cached_result, timestamp = self._cache[cache_key]
                if datetime.now() - timestamp < self._cache_expiry:
                    results[external_id] = cached_result
                    continue
            uncached_requests.append((external_id, source))

        if not uncached_requests:
            return results

        try:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    # Group requests by source for batch processing
                    source_groups = {}
                    for external_id, source in uncached_requests:
                        if source not in source_groups:
                            source_groups[source] = []
                        source_groups[source].append(external_id)

                    # Process each source group
                    for source, external_ids in source_groups.items():
                        source_column_map = {
                            DataSource.SPORTS_BOOK_REVIEW_DEPRECATED: "sportsbookreview_game_id",
                            DataSource.ACTION_NETWORK: "action_network_game_id",
                            DataSource.VSIN: "vsin_game_id",
                            DataSource.SPORTS_BETTING_DIME: "sbd_game_id",
                        }

                        column = source_column_map.get(source)
                        if column:
                            placeholders = ",".join(["%s"] * len(external_ids))
                            cur.execute(
                                f"SELECT id, {column} FROM curated.games_complete WHERE {column} IN ({placeholders})",
                                external_ids,
                            )

                            batch_results = cur.fetchall()
                            for row in batch_results:
                                external_id = row[column]
                                game_id = row["id"]
                                results[external_id] = game_id

                                # Cache the result
                                cache_key = f"{source.value}:{external_id}"
                                self._cache[cache_key] = (game_id, datetime.now())

                            # Mark missing IDs as None
                            for external_id in external_ids:
                                if external_id not in results:
                                    results[external_id] = None
                                    cache_key = f"{source.value}:{external_id}"
                                    self._cache[cache_key] = (None, datetime.now())

        except Exception as e:
            self.logger.error("Error in batch game ID resolution", error=str(e))
            # Return partial results
            for external_id, _ in uncached_requests:
                if external_id not in results:
                    results[external_id] = None

        return results


class SportsbookMapper:
    """Enhanced sportsbook mapper with caching and fuzzy matching."""

    def __init__(self, connection_pool: ConnectionPool = None):
        self.settings = UnifiedSettings()
        self.logger = logger.bind(component="SportsbookMapper")
        self.connection_pool = connection_pool or ConnectionPool(self.settings)
        self._cache = {}
        self._fuzzy_mappings = self._initialize_fuzzy_mappings()

    def _initialize_fuzzy_mappings(self) -> dict[str, str]:
        """Initialize fuzzy name mappings for common sportsbook variations."""
        return {
            "dk": "DraftKings",
            "draftkings": "DraftKings",
            "fanduel": "FanDuel",
            "fd": "FanDuel",
            "betmgm": "BetMGM",
            "mgm": "BetMGM",
            "caesars": "Caesars",
            "czr": "Caesars",
            "pointsbet": "PointsBet",
            "pb": "PointsBet",
            "barstool": "Barstool",
            "bovada": "Bovada",
            "betonline": "BetOnline",
            "bet365": "Bet365",
            "williamhill": "William Hill",
            "wh": "William Hill",
            "circa": "Circa Sports",
            "westgate": "Westgate SuperBook",
            "superbook": "Westgate SuperBook",
        }

    def resolve_sportsbook_id(self, sportsbook_name: str) -> int | None:
        """
        Resolve sportsbook name to standardized ID with fuzzy matching.

        Args:
            sportsbook_name: Sportsbook name from source

        Returns:
            Sportsbook ID or None if not found
        """
        if not sportsbook_name:
            return None

        clean_name = sportsbook_name.strip().lower()

        # Check cache first
        if clean_name in self._cache:
            return self._cache[clean_name]

        # Try fuzzy matching
        normalized_name = self._fuzzy_mappings.get(clean_name, sportsbook_name)

        try:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    # Try exact match first
                    cur.execute(
                        "SELECT id FROM curated.sportsbooks WHERE LOWER(name) = %s",
                        (clean_name,),
                    )
                    result = cur.fetchone()

                    if not result and normalized_name != sportsbook_name:
                        # Try normalized name
                        cur.execute(
                            "SELECT id FROM curated.sportsbooks WHERE LOWER(name) = %s",
                            (normalized_name.lower(),),
                        )
                        result = cur.fetchone()

                    if not result:
                        # Try partial match
                        cur.execute(
                            "SELECT id, name FROM curated.sportsbooks WHERE LOWER(name) LIKE %s",
                            (f"%{clean_name}%",),
                        )
                        result = cur.fetchone()

                    if result:
                        sportsbook_id = result["id"]
                        self._cache[clean_name] = sportsbook_id
                        return sportsbook_id

                    self.logger.warning(f"Sportsbook not found: {sportsbook_name}")
                    self._cache[clean_name] = None
                    return None

        except Exception as e:
            self.logger.error(
                "Error resolving sportsbook ID",
                sportsbook_name=sportsbook_name,
                error=str(e),
            )
            return None


class DataQualityCalculator:
    """Enhanced data quality calculator with advanced validation."""

    def __init__(self):
        self.logger = logger.bind(component="DataQualityCalculator")
        self._field_weights = self._initialize_field_weights()
        self._validation_rules = self._initialize_validation_rules()

    def _initialize_field_weights(self) -> dict[str, dict[str, float]]:
        """Initialize field weights for different bet types."""
        return {
            "moneyline": {
                "core": {
                    "home_ml": 0.15,
                    "away_ml": 0.15,
                    "odds_timestamp": 0.10,
                    "sportsbook": 0.10,
                },
                "opening_closing": {
                    "opening_home_ml": 0.05,
                    "opening_away_ml": 0.05,
                    "closing_home_ml": 0.05,
                    "closing_away_ml": 0.05,
                },
                "volume": {
                    "home_bets_percentage": 0.05,
                    "away_bets_percentage": 0.05,
                    "home_money_percentage": 0.05,
                    "away_money_percentage": 0.05,
                },
                "analysis": {
                    "sharp_action": 0.05,
                    "reverse_line_movement": 0.02,
                    "steam_move": 0.02,
                },
                "outcome": {"winning_side": 0.05, "profit_loss": 0.05},
            },
            "totals": {
                "core": {
                    "total_line": 0.10,
                    "over_price": 0.10,
                    "under_price": 0.10,
                    "odds_timestamp": 0.10,
                    "sportsbook": 0.10,
                },
                "opening_closing": {
                    "opening_total": 0.05,
                    "opening_over_price": 0.05,
                    "opening_under_price": 0.05,
                    "closing_total": 0.05,
                    "closing_over_price": 0.05,
                    "closing_under_price": 0.05,
                },
                "volume": {
                    "over_bets_percentage": 0.05,
                    "under_bets_percentage": 0.05,
                    "over_money_percentage": 0.03,
                    "under_money_percentage": 0.03,
                },
                "analysis": {
                    "sharp_action": 0.05,
                    "reverse_line_movement": 0.02,
                    "steam_move": 0.02,
                },
                "outcome": {"total_score": 0.10},
            },
            "spread": {
                "core": {
                    "spread_line": 0.10,
                    "home_spread_price": 0.10,
                    "away_spread_price": 0.10,
                    "odds_timestamp": 0.10,
                    "sportsbook": 0.10,
                },
                "opening_closing": {
                    "opening_spread": 0.05,
                    "opening_home_price": 0.05,
                    "opening_away_price": 0.05,
                    "closing_spread": 0.05,
                    "closing_home_price": 0.05,
                    "closing_away_price": 0.05,
                },
                "volume": {
                    "home_bets_percentage": 0.05,
                    "away_bets_percentage": 0.05,
                    "home_money_percentage": 0.03,
                    "away_money_percentage": 0.03,
                },
                "analysis": {
                    "sharp_action": 0.05,
                    "reverse_line_movement": 0.02,
                    "steam_move": 0.02,
                },
                "outcome": {"final_score_difference": 0.10},
            },
        }

    def _initialize_validation_rules(self) -> dict[str, dict[str, Any]]:
        """Initialize validation rules for different bet types."""
        return {
            "moneyline": {
                "home_ml": {"type": int, "range": (-10000, 10000), "required": True},
                "away_ml": {"type": int, "range": (-10000, 10000), "required": True},
                "home_bets_percentage": {
                    "type": float,
                    "range": (0, 100),
                    "required": False,
                },
                "away_bets_percentage": {
                    "type": float,
                    "range": (0, 100),
                    "required": False,
                },
            },
            "totals": {
                "total_line": {"type": float, "range": (0, 50), "required": True},
                "over_price": {"type": int, "range": (-10000, 10000), "required": True},
                "under_price": {
                    "type": int,
                    "range": (-10000, 10000),
                    "required": True,
                },
                "over_bets_percentage": {
                    "type": float,
                    "range": (0, 100),
                    "required": False,
                },
                "under_bets_percentage": {
                    "type": float,
                    "range": (0, 100),
                    "required": False,
                },
            },
            "spread": {
                "spread_line": {"type": float, "range": (-50, 50), "required": True},
                "home_spread_price": {
                    "type": int,
                    "range": (-10000, 10000),
                    "required": True,
                },
                "away_spread_price": {
                    "type": int,
                    "range": (-10000, 10000),
                    "required": True,
                },
                "home_bets_percentage": {
                    "type": float,
                    "range": (0, 100),
                    "required": False,
                },
                "away_bets_percentage": {
                    "type": float,
                    "range": (0, 100),
                    "required": False,
                },
            },
        }

    def validate_record(self, data: dict[str, Any], bet_type: str) -> ValidationResult:
        """Comprehensive record validation."""
        result = ValidationResult(is_valid=True)

        # Check bet type validity
        if bet_type not in self._validation_rules:
            result.add_error(f"Invalid bet type: {bet_type}")
            return result

        rules = self._validation_rules[bet_type]

        # Validate each field
        for field_name, rule in rules.items():
            value = data.get(field_name)

            # Check required fields
            if rule.get("required", False) and value is None:
                result.add_error(f"Required field '{field_name}' is missing")
                continue

            # Skip validation for None values of non-required fields
            if value is None:
                continue

            # Type validation
            expected_type = rule.get("type")
            if expected_type and not isinstance(value, expected_type):
                try:
                    # Try to convert
                    if expected_type == int:
                        value = int(value)
                    elif expected_type == float:
                        value = float(value)
                    data[field_name] = value  # Update the data with converted value
                except (ValueError, TypeError):
                    result.add_error(
                        f"Field '{field_name}' has invalid type. Expected {expected_type.__name__}, got {type(value).__name__}"
                    )
                    continue

            # Range validation
            if "range" in rule:
                min_val, max_val = rule["range"]
                if not (min_val <= value <= max_val):
                    result.add_error(
                        f"Field '{field_name}' value {value} is outside valid range ({min_val}, {max_val})"
                    )

        # Additional business logic validation
        self._validate_business_logic(data, bet_type, result)

        # Calculate quality score
        result.quality_score = self.calculate_completeness_score(data, bet_type)

        return result

    def _validate_business_logic(
        self, data: dict[str, Any], bet_type: str, result: ValidationResult
    ):
        """Validate business logic rules."""
        # Validate percentage fields sum to 100 (with tolerance)
        if bet_type == "moneyline":
            home_pct = data.get("home_bets_percentage")
            away_pct = data.get("away_bets_percentage")
            if home_pct is not None and away_pct is not None:
                total_pct = home_pct + away_pct
                if abs(total_pct - 100) > 5:  # 5% tolerance
                    result.add_warning(
                        f"Betting percentages sum to {total_pct}%, expected ~100%"
                    )

        # Validate timestamp is recent (within last 30 days)
        odds_timestamp = data.get("odds_timestamp")
        if odds_timestamp:
            try:
                if isinstance(odds_timestamp, str):
                    timestamp = datetime.fromisoformat(
                        odds_timestamp.replace("Z", "+00:00")
                    )
                else:
                    timestamp = odds_timestamp

                age = datetime.now() - timestamp.replace(tzinfo=None)
                if age > timedelta(days=30):
                    result.add_warning(f"Odds timestamp is {age.days} days old")
            except (ValueError, TypeError):
                result.add_error("Invalid odds_timestamp format")

        # Validate sportsbook name
        sportsbook = data.get("sportsbook")
        if sportsbook and len(sportsbook.strip()) == 0:
            result.add_error("Sportsbook name cannot be empty")

    def calculate_completeness_score(
        self, data: dict[str, Any], bet_type: str
    ) -> float:
        """
        Calculate completeness score based on available fields.

        Args:
            data: Raw betting line data
            bet_type: Type of bet (moneyline, totals, spread)

        Returns:
            Completeness score (0.0 to 1.0)
        """
        if bet_type not in self._field_weights:
            return 0.0

        score = 0.0
        weights = self._field_weights[bet_type]

        # Calculate weighted score based on field presence
        for category, fields in weights.items():
            for field, weight in fields.items():
                if data.get(field) is not None:
                    score += weight

        return min(score, 1.0)

    def determine_quality_level(
        self,
        completeness_score: float,
        reliability_score: float,
        validation_result: ValidationResult = None,
    ) -> str:
        """
        Determine overall quality level based on multiple factors.

        Args:
            completeness_score: Completeness score (0.0 to 1.0)
            reliability_score: Source reliability score (0.0 to 1.0)
            validation_result: Optional validation result

        Returns:
            Quality level string
        """
        # Calculate composite score
        composite_score = (completeness_score * 0.6) + (reliability_score * 0.4)

        # Penalize for validation errors
        if validation_result and validation_result.errors:
            penalty = min(0.3, len(validation_result.errors) * 0.1)
            composite_score -= penalty

        # Determine quality level
        for quality_level in DataQualityLevel:
            if composite_score >= quality_level.threshold:
                return quality_level.level

        return DataQualityLevel.POOR.level


class UnifiedBettingLinesCollector(ABC):
    """
    Enhanced base class for all betting line collectors with performance optimizations.

    Provides standardized integration with core_betting schema,
    MLB Stats API normalization, data quality tracking, and performance monitoring.
    """

    def __init__(self, source: DataSource):
        self.source = source
        self.settings = UnifiedSettings()
        self.logger = logger.bind(component=f"UnifiedCollector_{source.value}")

        # Initialize shared connection pool
        self.connection_pool = ConnectionPool(self.settings)

        # Initialize helper components with shared pool
        self.game_resolver = MLBStatsAPIGameResolver(self.connection_pool)
        self.sportsbook_mapper = SportsbookMapper(self.connection_pool)
        self.quality_calculator = DataQualityCalculator()

        # Performance tracking
        self.performance_metrics = PerformanceMetrics()

        # Batch processing settings
        self.batch_size = 100
        self.enable_batch_processing = True

        # Set source reliability score with dynamic adjustment
        self.source_reliability_scores = {
            DataSource.MLB_STATS_API: 1.0,
            DataSource.ACTION_NETWORK: 0.95,
            DataSource.VSIN: 0.95,
            DataSource.SPORTS_BOOK_REVIEW_DEPRECATED: 0.90,
            DataSource.SPORTS_BETTING_DIME: 0.85,
            DataSource.ODDS_API: 0.80,
        }

        self.reliability_score = self.source_reliability_scores.get(source, 0.75)

        # Enable/disable features based on source
        self.enable_validation = True
        self.enable_performance_tracking = True
        self.enable_advanced_quality_checks = source in [
            DataSource.MLB_STATS_API,
            DataSource.ACTION_NETWORK,
            DataSource.VSIN,
        ]

    @abstractmethod
    def collect_raw_data(self, **kwargs) -> list[dict[str, Any]]:
        """
        Collect raw betting line data from the source.

        Returns:
            List of raw betting line dictionaries
        """
        pass

    def collect_and_store(self, **kwargs) -> CollectionResult:
        """
        Enhanced collection with batch processing and performance monitoring.

        Returns:
            Collection result with status and metrics
        """
        start_time = datetime.now()

        try:
            # Reset performance metrics
            self.performance_metrics = PerformanceMetrics()

            # Collect raw data
            raw_data = self.collect_raw_data(**kwargs)

            if not raw_data:
                return UnifiedCollectionResult(
                    status=CollectionStatus.SUCCESS,
                    records_processed=0,
                    records_stored=0,
                    message="No data available for collection",
                )

            self.performance_metrics.total_records = len(raw_data)

            # Process data in batches if enabled
            if self.enable_batch_processing and len(raw_data) > self.batch_size:
                total_stored = self._process_in_batches(raw_data)
            else:
                total_stored = self._process_sequentially(raw_data)

            # Calculate final metrics
            self.performance_metrics.successful_records = total_stored
            self.performance_metrics.failed_records = (
                self.performance_metrics.total_records - total_stored
            )
            self.performance_metrics.processing_time = (
                datetime.now() - start_time
            ).total_seconds()

            # Log performance metrics
            self.logger.info(
                "Collection completed",
                source=self.source.value,
                total_records=self.performance_metrics.total_records,
                successful_records=self.performance_metrics.successful_records,
                success_rate=f"{self.performance_metrics.success_rate:.2%}",
                processing_time=f"{self.performance_metrics.processing_time:.2f}s",
            )

            return UnifiedCollectionResult(
                status=CollectionStatus.SUCCESS,
                records_processed=self.performance_metrics.total_records,
                records_stored=self.performance_metrics.successful_records,
                message=f"Successfully stored {self.performance_metrics.successful_records}/{self.performance_metrics.total_records} records",
            )

        except Exception as e:
            self.logger.error("Collection failed", error=str(e))
            return UnifiedCollectionResult(
                status=CollectionStatus.FAILED,
                records_processed=0,
                records_stored=0,
                message=f"Collection failed: {str(e)}",
            )

    def _process_in_batches(self, raw_data: list[dict[str, Any]]) -> int:
        """Process data in batches for improved performance."""
        total_stored = 0
        batch_id = uuid.uuid4()

        # Pre-resolve game IDs in batches
        game_id_requests = [
            (record.get("external_source_id"), self.source)
            for record in raw_data
            if record.get("external_source_id")
        ]
        game_id_map = self.game_resolver.batch_resolve_game_ids(game_id_requests)

        # Process records in batches
        for i in range(0, len(raw_data), self.batch_size):
            batch = raw_data[i : i + self.batch_size]
            batch_stored = 0

            for record in batch:
                try:
                    # Use pre-resolved game ID
                    external_id = record.get("external_source_id")
                    game_id = game_id_map.get(external_id) if external_id else None

                    if game_id:
                        result = self._process_and_store_record_with_game_id(
                            record, batch_id, game_id
                        )
                        if result:
                            batch_stored += 1
                    else:
                        self.logger.warning(
                            "Skipping record due to missing game ID",
                            external_source_id=external_id,
                        )

                except Exception as e:
                    self.logger.error(
                        "Error processing record in batch", record=record, error=str(e)
                    )
                    continue

            total_stored += batch_stored

            # Log batch progress
            self.logger.debug(
                f"Processed batch {i // self.batch_size + 1}",
                batch_size=len(batch),
                stored=batch_stored,
            )

        return total_stored

    def _process_sequentially(self, raw_data: list[dict[str, Any]]) -> int:
        """Process data sequentially (legacy method)."""
        total_stored = 0
        batch_id = uuid.uuid4()

        for record in raw_data:
            try:
                result = self._process_and_store_record(record, batch_id)
                if result:
                    total_stored += 1
            except Exception as e:
                self.logger.error(
                    "Error processing record", record=record, error=str(e)
                )
                continue

        return total_stored

    def _process_and_store_record(
        self, record: dict[str, Any], batch_id: uuid.UUID
    ) -> bool:
        """
        Process and store a single betting line record.

        Args:
            record: Raw betting line record
            batch_id: Collection batch identifier

        Returns:
            True if stored successfully, False otherwise
        """
        try:
            # Resolve game ID
            game_id = self.game_resolver.resolve_game_id(
                record["external_source_id"], self.source
            )

            if not game_id:
                self.logger.warning(
                    "Could not resolve game ID",
                    external_source_id=record.get("external_source_id"),
                    source=self.source.value,
                )
                return False

            return self._process_and_store_record_with_game_id(
                record, batch_id, game_id
            )

        except Exception as e:
            self.logger.error("Error processing record", error=str(e))
            return False

    def _process_and_store_record_with_game_id(
        self, record: dict[str, Any], batch_id: uuid.UUID, game_id: int
    ) -> bool:
        """
        Process and store record with pre-resolved game ID.

        Args:
            record: Raw betting line record
            batch_id: Collection batch identifier
            game_id: Pre-resolved game ID

        Returns:
            True if stored successfully, False otherwise
        """
        try:
            # Validate the record
            bet_type = record.get("bet_type", "moneyline")
            validation_result = self.quality_calculator.validate_record(
                record, bet_type
            )

            if not validation_result.is_valid:
                self.performance_metrics.validation_errors += 1
                self.logger.warning(
                    "Record validation failed",
                    external_source_id=record.get("external_source_id"),
                    errors=validation_result.errors,
                )
                return False

            # Resolve sportsbook ID
            sportsbook_id = self.sportsbook_mapper.resolve_sportsbook_id(
                record["sportsbook"]
            )

            # Calculate quality metrics
            completeness_score = validation_result.quality_score
            quality_level = self.quality_calculator.determine_quality_level(
                completeness_score, self.reliability_score, validation_result
            )

            # Prepare unified record
            unified_record = {
                "game_id": game_id,
                "sportsbook_id": sportsbook_id,
                "sportsbook": record["sportsbook"],
                "odds_timestamp": record["odds_timestamp"],
                "source": self.source.value,
                "data_quality": quality_level,
                "data_completeness_score": completeness_score,
                "source_reliability_score": self.reliability_score,
                "collection_batch_id": batch_id,
                "external_source_id": record.get("external_source_id"),
                "collection_method": record.get("collection_method", "API"),
                "source_api_version": record.get("source_api_version"),
                "source_metadata": record.get("source_metadata", {}),
                "game_datetime": record.get("game_datetime"),
                "home_team": record.get("home_team"),
                "away_team": record.get("away_team"),
                "sharp_action": record.get("sharp_action"),
                "reverse_line_movement": record.get("reverse_line_movement", False),
                "steam_move": record.get("steam_move", False),
                "winning_side": record.get("winning_side"),
                "profit_loss": record.get("profit_loss"),
            }

            # Store based on bet type
            if bet_type == "moneyline":
                return self._store_moneyline(unified_record, record)
            elif bet_type == "totals":
                return self._store_totals(unified_record, record)
            elif bet_type == "spread":
                return self._store_spread(unified_record, record)
            else:
                self.logger.warning(f"Unknown bet type: {bet_type}")
                return False

        except Exception as e:
            self.logger.error("Error processing record with game ID", error=str(e))
            return False

    def _store_moneyline(
        self, unified_record: dict[str, Any], raw_record: dict[str, Any]
    ) -> bool:
        """Store moneyline betting data."""
        try:
            # Add moneyline-specific fields
            unified_record.update(
                {
                    "home_ml": raw_record.get("home_ml"),
                    "away_ml": raw_record.get("away_ml"),
                    "opening_home_ml": raw_record.get("opening_home_ml"),
                    "opening_away_ml": raw_record.get("opening_away_ml"),
                    "closing_home_ml": raw_record.get("closing_home_ml"),
                    "closing_away_ml": raw_record.get("closing_away_ml"),
                    "home_bets_count": raw_record.get("home_bets_count"),
                    "away_bets_count": raw_record.get("away_bets_count"),
                    "home_bets_percentage": raw_record.get("home_bets_percentage"),
                    "away_bets_percentage": raw_record.get("away_bets_percentage"),
                    "home_money_percentage": raw_record.get("home_money_percentage"),
                    "away_money_percentage": raw_record.get("away_money_percentage"),
                }
            )

            return self._execute_insert(
                "curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'", unified_record
            )

        except Exception as e:
            self.logger.error("Error storing moneyline data", error=str(e))
            return False

    def _store_totals(
        self, unified_record: dict[str, Any], raw_record: dict[str, Any]
    ) -> bool:
        """Store totals betting data."""
        try:
            # Add totals-specific fields
            unified_record.update(
                {
                    "total_line": raw_record.get("total_line"),
                    "over_price": raw_record.get("over_price"),
                    "under_price": raw_record.get("under_price"),
                    "opening_total": raw_record.get("opening_total"),
                    "opening_over_price": raw_record.get("opening_over_price"),
                    "opening_under_price": raw_record.get("opening_under_price"),
                    "closing_total": raw_record.get("closing_total"),
                    "closing_over_price": raw_record.get("closing_over_price"),
                    "closing_under_price": raw_record.get("closing_under_price"),
                    "over_bets_count": raw_record.get("over_bets_count"),
                    "under_bets_count": raw_record.get("under_bets_count"),
                    "over_bets_percentage": raw_record.get("over_bets_percentage"),
                    "under_bets_percentage": raw_record.get("under_bets_percentage"),
                    "over_money_percentage": raw_record.get("over_money_percentage"),
                    "under_money_percentage": raw_record.get("under_money_percentage"),
                    "total_score": raw_record.get("total_score"),
                }
            )

            return self._execute_insert(
                "curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'", unified_record
            )

        except Exception as e:
            self.logger.error("Error storing totals data", error=str(e))
            return False

    def _store_spread(
        self, unified_record: dict[str, Any], raw_record: dict[str, Any]
    ) -> bool:
        """Store spread betting data."""
        try:
            # Add spread-specific fields
            unified_record.update(
                {
                    "spread_line": raw_record.get("spread_line"),
                    "home_spread_price": raw_record.get("home_spread_price"),
                    "away_spread_price": raw_record.get("away_spread_price"),
                    "opening_spread": raw_record.get("opening_spread"),
                    "opening_home_price": raw_record.get("opening_home_price"),
                    "opening_away_price": raw_record.get("opening_away_price"),
                    "closing_spread": raw_record.get("closing_spread"),
                    "closing_home_price": raw_record.get("closing_home_price"),
                    "closing_away_price": raw_record.get("closing_away_price"),
                    "home_bets_count": raw_record.get("home_bets_count"),
                    "away_bets_count": raw_record.get("away_bets_count"),
                    "home_bets_percentage": raw_record.get("home_bets_percentage"),
                    "away_bets_percentage": raw_record.get("away_bets_percentage"),
                    "home_money_percentage": raw_record.get("home_money_percentage"),
                    "away_money_percentage": raw_record.get("away_money_percentage"),
                    "final_score_difference": raw_record.get("final_score_difference"),
                }
            )

            return self._execute_insert(
                "curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread'", unified_record
            )

        except Exception as e:
            self.logger.error("Error storing spread data", error=str(e))
            return False

    def _execute_insert(self, table_name: str, record: dict[str, Any]) -> bool:
        """Execute database insert with conflict handling and performance tracking."""
        db_start_time = datetime.now()

        try:
            with self.connection_pool.get_connection() as conn:
                with conn.cursor() as cur:
                    # Build dynamic INSERT statement
                    columns = list(record.keys())
                    values = list(record.values())

                    column_str = ", ".join(columns)
                    value_placeholders = ", ".join(["%s"] * len(values))

                    # Use INSERT ... ON CONFLICT for upsert behavior
                    query = f"""
                        INSERT INTO {table_name} ({column_str})
                        VALUES ({value_placeholders})
                        ON CONFLICT (game_id, sportsbook_id, odds_timestamp) 
                        DO UPDATE SET
                            data_quality = EXCLUDED.data_quality,
                            data_completeness_score = EXCLUDED.data_completeness_score,
                            source_reliability_score = EXCLUDED.source_reliability_score,
                            updated_at = NOW()
                    """

                    cur.execute(query, values)
                    conn.commit()

                    # Track database performance
                    self.performance_metrics.database_time += (
                        datetime.now() - db_start_time
                    ).total_seconds()

                    return True

        except Exception as e:
            self.logger.error("Database insert failed", table=table_name, error=str(e))
            return False

    def get_performance_metrics(self) -> dict[str, Any]:
        """Get current performance metrics."""
        return {
            "total_records": self.performance_metrics.total_records,
            "successful_records": self.performance_metrics.successful_records,
            "failed_records": self.performance_metrics.failed_records,
            "validation_errors": self.performance_metrics.validation_errors,
            "success_rate": self.performance_metrics.success_rate,
            "error_rate": self.performance_metrics.error_rate,
            "processing_time": self.performance_metrics.processing_time,
            "database_time": self.performance_metrics.database_time,
            "avg_processing_time_per_record": self.performance_metrics.processing_time
            / max(self.performance_metrics.total_records, 1),
        }
