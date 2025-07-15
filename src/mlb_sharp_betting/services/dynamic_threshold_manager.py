"""
Dynamic Threshold Manager

Implements progressive threshold optimization that:
1. Starts with VERY loose thresholds to collect signals and data
2. Progressively tightens thresholds as sample sizes grow
3. Optimizes thresholds based on actual ROI performance
4. Provides ROI-based threshold recommendations

This replaces static threshold systems with dynamic, performance-driven thresholds.

🚀 PHASE 2B: Updated to use table registry for dynamic table resolution
"""

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

import numpy as np

from ..core.logging import get_logger
from ..db.connection import DatabaseManager, get_db_manager
from ..db.table_registry import get_table_registry


class ThresholdPhase(Enum):
    """Phases of threshold progression based on sample size"""

    BOOTSTRAP = "bootstrap"  # 0-10 samples: Very loose thresholds
    LEARNING = "learning"  # 11-30 samples: Loose thresholds
    CALIBRATION = "calibration"  # 31-100 samples: Moderate thresholds
    OPTIMIZATION = "optimization"  # 100+ samples: Tight, ROI-optimized thresholds


@dataclass
class ThresholdConfig:
    """Dynamic threshold configuration"""

    strategy_key: str
    phase: ThresholdPhase
    sample_size: int

    # Current thresholds
    minimum_threshold: float
    moderate_threshold: float
    high_threshold: float

    # Performance metrics
    current_roi: float
    current_win_rate: float
    confidence_score: float

    # Optimization history
    threshold_history: list[dict[str, Any]] = field(default_factory=list)
    last_optimization: datetime | None = None
    next_optimization: datetime | None = None

    # Meta information
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class ROIOptimizationResult:
    """Result of ROI-based threshold optimization"""

    optimal_threshold: float
    expected_roi: float
    expected_win_rate: float
    signal_volume: int
    confidence_level: float

    # Supporting data
    tested_thresholds: list[float]
    roi_by_threshold: dict[float, float]
    volume_by_threshold: dict[float, int]
    optimization_method: str


class DynamicThresholdManager:
    """
    Manages dynamic thresholds that start loose and progressively tighten
    based on sample size and ROI optimization.

    🚀 PHASE 2B: Updated to use table registry for dynamic table resolution
    """

    def __init__(self, db_manager: DatabaseManager | None = None):
        self.logger = get_logger(__name__)
        self.db_manager = db_manager or get_db_manager()

        # 🚀 PHASE 2B: Initialize table registry for dynamic table resolution
        self.table_registry = get_table_registry()

        # Threshold configurations cache
        self._threshold_cache: dict[str, ThresholdConfig] = {}
        self._cache_expiry = datetime.now(timezone.utc)
        self._cache_duration_minutes = 15

        # Bootstrap thresholds (VERY loose to collect initial data)
        self.bootstrap_thresholds = {
            "sharp_action": {"min": 2.5, "mod": 4.5, "high": 7.5},
            "book_conflicts": {"min": 2.0, "mod": 3.5, "high": 5.5},
            "public_fade": {"min": 42.0, "mod": 47.0, "high": 52.0},
            "late_flip": {"min": 3.5, "mod": 5.5, "high": 7.5},
            "consensus": {"min": 3.0, "mod": 5.0, "high": 7.0},
            "underdog_value": {"min": 4.5, "mod": 7.5, "high": 11.0},
            "line_movement": {"min": 2.5, "mod": 4.5, "high": 7.5},
            "timing_based": {"min": 3.5, "mod": 5.5, "high": 8.5},
            "default": {"min": 2.5, "mod": 4.5, "high": 7.5},
        }

        # Phase-based multipliers for threshold progression
        self.phase_multipliers = {
            ThresholdPhase.BOOTSTRAP: 1.0,  # Use bootstrap thresholds as-is
            ThresholdPhase.LEARNING: 1.3,  # 30% higher than bootstrap
            ThresholdPhase.CALIBRATION: 1.8,  # 80% higher than bootstrap
            ThresholdPhase.OPTIMIZATION: 2.5,  # ROI-optimized, typically 2.5x bootstrap
        }

        self.logger.info(
            "🎯 Dynamic Threshold Manager initialized with table registry support"
        )

    async def get_dynamic_threshold(
        self, strategy_type: str, source: str = "default", split_type: str = "default"
    ) -> ThresholdConfig:
        """
        Get dynamic threshold configuration for a strategy.

        Args:
            strategy_type: Type of strategy (sharp_action, book_conflicts, etc.)
            source: Data source (VSIN, SBD, etc.)
            split_type: Split type (moneyline, spread, total)

        Returns:
            Dynamic threshold configuration
        """
        strategy_key = f"{strategy_type}_{source}_{split_type}".lower()

        # Check cache first
        if (
            strategy_key in self._threshold_cache
            and datetime.now(timezone.utc) < self._cache_expiry
        ):
            return self._threshold_cache[strategy_key]

        # Load or create threshold configuration
        threshold_config = await self._load_or_create_threshold_config(
            strategy_key, strategy_type
        )

        # Update cache
        self._threshold_cache[strategy_key] = threshold_config
        self._refresh_cache_expiry()

        return threshold_config

    async def _create_bootstrap_config(
        self, strategy_key: str, strategy_type: str
    ) -> ThresholdConfig:
        """Create new threshold configuration with bootstrap values"""

        # Get bootstrap thresholds for this strategy type
        bootstrap = self.bootstrap_thresholds.get(
            strategy_type, self.bootstrap_thresholds["default"]
        )

        config = ThresholdConfig(
            strategy_key=strategy_key,
            phase=ThresholdPhase.BOOTSTRAP,
            sample_size=0,
            minimum_threshold=bootstrap["min"],
            moderate_threshold=bootstrap["mod"],
            high_threshold=bootstrap["high"],
            current_roi=0.0,
            current_win_rate=0.5,
            confidence_score=0.1,
        )

        self.logger.info(
            f"🆕 Created bootstrap threshold config for {strategy_key}: "
            f"min={bootstrap['min']}%, mod={bootstrap['mod']}%, high={bootstrap['high']}%"
        )

        return config

    def _refresh_cache_expiry(self) -> None:
        """Refresh cache expiry time"""
        self._cache_expiry = datetime.now(timezone.utc) + timedelta(
            minutes=self._cache_duration_minutes
        )

    async def _load_or_create_threshold_config(
        self, strategy_key: str, strategy_type: str
    ) -> ThresholdConfig:
        """Load existing threshold config or create new one with bootstrap values"""
        # For now, always create bootstrap config - database integration can be added later
        return await self._create_bootstrap_config(strategy_key, strategy_type)

    async def _load_threshold_from_db(
        self, strategy_key: str
    ) -> ThresholdConfig | None:
        """Load threshold configuration from database

        🚀 PHASE 2B: Updated to use table registry for table resolution
        """
        try:
            # 🚀 PHASE 2B: Get table name from registry
            dynamic_thresholds_table = self.table_registry.get_table(
                "dynamic_thresholds"
            )

            query = f"""
                SELECT 
                    strategy_key, phase, sample_size, minimum_threshold,
                    moderate_threshold, high_threshold, current_roi,
                    current_win_rate, confidence_score, threshold_history,
                    last_optimization, next_optimization, created_at, updated_at
                FROM {dynamic_thresholds_table} 
                WHERE strategy_key = %s
            """

            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (strategy_key,))
                row = cursor.fetchone()

                if not row:
                    return None

                # Parse the row data
                if isinstance(row, dict):
                    data = row
                else:
                    # Convert tuple to dict
                    columns = [
                        "strategy_key",
                        "phase",
                        "sample_size",
                        "minimum_threshold",
                        "moderate_threshold",
                        "high_threshold",
                        "current_roi",
                        "current_win_rate",
                        "confidence_score",
                        "threshold_history",
                        "last_optimization",
                        "next_optimization",
                        "created_at",
                        "updated_at",
                    ]
                    data = dict(zip(columns, row, strict=False))

                return ThresholdConfig(
                    strategy_key=data["strategy_key"],
                    phase=ThresholdPhase(data["phase"]),
                    sample_size=int(data["sample_size"])
                    if data["sample_size"] is not None
                    else 0,
                    minimum_threshold=float(data["minimum_threshold"])
                    if data["minimum_threshold"] is not None
                    else 3.0,
                    moderate_threshold=float(data["moderate_threshold"])
                    if data["moderate_threshold"] is not None
                    else 5.0,
                    high_threshold=float(data["high_threshold"])
                    if data["high_threshold"] is not None
                    else 8.0,
                    current_roi=float(data["current_roi"])
                    if data["current_roi"] is not None
                    else 0.0,
                    current_win_rate=float(data["current_win_rate"])
                    if data["current_win_rate"] is not None
                    else 0.5,
                    confidence_score=float(data["confidence_score"])
                    if data["confidence_score"] is not None
                    else 0.1,
                    threshold_history=json.loads(data["threshold_history"])
                    if data["threshold_history"]
                    else [],
                    last_optimization=data["last_optimization"],
                    next_optimization=data["next_optimization"],
                    created_at=data["created_at"],
                    updated_at=data["updated_at"],
                )

        except Exception as e:
            self.logger.warning(
                f"Failed to load threshold config for {strategy_key}: {e}"
            )
            return None

    async def _should_optimize_thresholds(self, config: ThresholdConfig) -> bool:
        """Determine if threshold optimization is needed"""

        # Always optimize if we've moved to a new phase
        current_phase = self._determine_phase(config.sample_size)
        if current_phase != config.phase:
            return True

        # Optimize if scheduled
        if (
            config.next_optimization
            and datetime.now(timezone.utc) >= config.next_optimization
        ):
            return True

        # Optimize if we have significant new data (20% more samples)
        if config.sample_size > 0:
            new_sample_size = await self._get_current_sample_size(config.strategy_key)
            if new_sample_size >= config.sample_size * 1.2:
                return True

        return False

    async def _optimize_thresholds(self, config: ThresholdConfig) -> ThresholdConfig:
        """Optimize thresholds based on current performance data"""

        # Update sample size and performance metrics
        updated_config = await self._update_performance_metrics(config)

        # Determine current phase
        current_phase = self._determine_phase(updated_config.sample_size)

        if current_phase == ThresholdPhase.BOOTSTRAP:
            # Keep bootstrap thresholds
            return updated_config

        elif current_phase in [ThresholdPhase.LEARNING, ThresholdPhase.CALIBRATION]:
            # Use phase-based progression
            return await self._apply_phase_progression(updated_config, current_phase)

        else:  # OPTIMIZATION phase
            # Use ROI-based optimization
            return await self._apply_roi_optimization(updated_config)

    def _determine_phase(self, sample_size: int) -> ThresholdPhase:
        """Determine threshold phase based on sample size"""
        if sample_size <= 10:
            return ThresholdPhase.BOOTSTRAP
        elif sample_size <= 30:
            return ThresholdPhase.LEARNING
        elif sample_size <= 100:
            return ThresholdPhase.CALIBRATION
        else:
            return ThresholdPhase.OPTIMIZATION

    async def _update_performance_metrics(
        self, config: ThresholdConfig
    ) -> ThresholdConfig:
        """Update performance metrics with current data"""

        try:
            # Get current performance from backtesting results
            performance_data = await self._get_strategy_performance(config.strategy_key)

            if performance_data:
                config.sample_size = performance_data["sample_size"]
                config.current_roi = performance_data["roi"]
                config.current_win_rate = performance_data["win_rate"]
                config.confidence_score = self._calculate_confidence_score(
                    config.sample_size, config.current_roi, config.current_win_rate
                )

            config.updated_at = datetime.now(timezone.utc)

        except Exception as e:
            self.logger.warning(
                f"Failed to update performance metrics for {config.strategy_key}: {e}"
            )

        return config

    async def _apply_phase_progression(
        self, config: ThresholdConfig, phase: ThresholdPhase
    ) -> ThresholdConfig:
        """Apply phase-based threshold progression"""

        # Extract strategy type from strategy key
        strategy_type = config.strategy_key.split("_")[0]
        bootstrap = self.bootstrap_thresholds.get(
            strategy_type, self.bootstrap_thresholds["default"]
        )

        # Apply phase multiplier
        multiplier = self.phase_multipliers[phase]

        config.phase = phase
        config.minimum_threshold = bootstrap["min"] * multiplier
        config.moderate_threshold = bootstrap["mod"] * multiplier
        config.high_threshold = bootstrap["high"] * multiplier

        # Schedule next optimization
        config.next_optimization = datetime.now(timezone.utc) + timedelta(days=7)

        # Record in history
        config.threshold_history.append(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "phase": phase.value,
                "method": "phase_progression",
                "multiplier": multiplier,
                "min_threshold": config.minimum_threshold,
                "sample_size": config.sample_size,
                "roi": config.current_roi,
            }
        )

        await self._save_threshold_config(config)

        self.logger.info(
            f"📈 Applied {phase.value} progression to {config.strategy_key}: "
            f"min={config.minimum_threshold:.1f}%, mod={config.moderate_threshold:.1f}%, "
            f"high={config.high_threshold:.1f}% (multiplier={multiplier})"
        )

        return config

    async def _apply_roi_optimization(self, config: ThresholdConfig) -> ThresholdConfig:
        """Apply ROI-based threshold optimization"""

        try:
            # Get historical data for optimization
            historical_data = await self._get_historical_signal_data(
                config.strategy_key
            )

            if not historical_data or len(historical_data) < 50:
                self.logger.warning(
                    f"Insufficient data for ROI optimization of {config.strategy_key}"
                )
                return config

            # Run ROI optimization
            optimization_result = await self._run_roi_optimization(historical_data)

            if optimization_result:
                # Apply optimized thresholds
                config.phase = ThresholdPhase.OPTIMIZATION
                config.minimum_threshold = optimization_result.optimal_threshold
                config.moderate_threshold = optimization_result.optimal_threshold * 1.3
                config.high_threshold = optimization_result.optimal_threshold * 1.8

                # Update performance expectations
                config.current_roi = optimization_result.expected_roi
                config.current_win_rate = optimization_result.expected_win_rate
                config.confidence_score = optimization_result.confidence_level

                # Schedule next optimization
                config.next_optimization = datetime.now(timezone.utc) + timedelta(
                    days=14
                )
                config.last_optimization = datetime.now(timezone.utc)

                # Record in history
                config.threshold_history.append(
                    {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "phase": "optimization",
                        "method": "roi_optimization",
                        "optimal_threshold": optimization_result.optimal_threshold,
                        "expected_roi": optimization_result.expected_roi,
                        "expected_win_rate": optimization_result.expected_win_rate,
                        "signal_volume": optimization_result.signal_volume,
                        "sample_size": config.sample_size,
                    }
                )

                await self._save_threshold_config(config)

                self.logger.info(
                    f"🎯 Applied ROI optimization to {config.strategy_key}: "
                    f"optimal_threshold={optimization_result.optimal_threshold:.1f}%, "
                    f"expected_roi={optimization_result.expected_roi:.1f}%, "
                    f"signal_volume={optimization_result.signal_volume}"
                )

        except Exception as e:
            self.logger.error(
                f"Failed to apply ROI optimization for {config.strategy_key}: {e}"
            )

        return config

    async def _run_roi_optimization(
        self, historical_data: list[dict[str, Any]]
    ) -> ROIOptimizationResult | None:
        """Run ROI optimization to find optimal threshold"""

        try:
            # Test different threshold values
            test_thresholds = np.arange(
                2.0, 25.0, 0.5
            )  # Test from 2% to 25% in 0.5% increments

            roi_by_threshold = {}
            volume_by_threshold = {}

            for threshold in test_thresholds:
                # Filter signals by threshold
                filtered_signals = [
                    signal
                    for signal in historical_data
                    if abs(signal.get("differential", 0)) >= threshold
                ]

                if len(filtered_signals) < 10:  # Need minimum sample
                    continue

                # Calculate ROI for this threshold
                wins = sum(1 for signal in filtered_signals if signal.get("won", False))
                total_bets = len(filtered_signals)
                win_rate = wins / total_bets if total_bets > 0 else 0

                # Assume standard -110 odds for ROI calculation
                roi = ((wins * (100 / 110)) - (total_bets - wins)) / total_bets * 100

                roi_by_threshold[threshold] = roi
                volume_by_threshold[threshold] = total_bets

            if not roi_by_threshold:
                return None

            # Find threshold that maximizes ROI while maintaining reasonable volume
            best_roi = -float("inf")
            optimal_threshold = None

            for threshold, roi in roi_by_threshold.items():
                volume = volume_by_threshold[threshold]

                # Penalize very low volume (need at least 20 signals)
                if volume < 20:
                    continue

                # Weighted score: ROI with volume consideration
                volume_weight = min(1.0, volume / 50)  # Max weight at 50+ signals
                weighted_score = roi * volume_weight

                if weighted_score > best_roi:
                    best_roi = weighted_score
                    optimal_threshold = threshold

            if optimal_threshold is None:
                return None

            # Calculate expected metrics at optimal threshold
            filtered_signals = [
                signal
                for signal in historical_data
                if abs(signal.get("differential", 0)) >= optimal_threshold
            ]

            wins = sum(1 for signal in filtered_signals if signal.get("won", False))
            total_bets = len(filtered_signals)
            expected_win_rate = wins / total_bets if total_bets > 0 else 0
            expected_roi = roi_by_threshold[optimal_threshold]

            # Confidence based on sample size
            confidence_level = min(0.95, 0.5 + (total_bets / 200))

            return ROIOptimizationResult(
                optimal_threshold=optimal_threshold,
                expected_roi=expected_roi,
                expected_win_rate=expected_win_rate,
                signal_volume=total_bets,
                confidence_level=confidence_level,
                tested_thresholds=list(test_thresholds),
                roi_by_threshold=roi_by_threshold,
                volume_by_threshold=volume_by_threshold,
                optimization_method="roi_maximization_with_volume_weighting",
            )

        except Exception as e:
            self.logger.error(f"Failed to run ROI optimization: {e}")
            return None

    async def _get_strategy_performance(
        self, strategy_key: str
    ) -> dict[str, Any] | None:
        """Get current strategy performance from database"""
        try:
            # Get table name from registry
            strategy_performance_table = self.table_registry.get_table_name(
                "analytics", "strategy_performance"
            )

            query = f"""
                SELECT 
                    COUNT(*) as sample_size,
                    AVG(CASE WHEN roi_per_100 IS NOT NULL THEN roi_per_100 ELSE 0 END) as roi,
                    AVG(CASE WHEN win_rate IS NOT NULL THEN win_rate ELSE 0.5 END) as win_rate
                FROM {strategy_performance_table}
                WHERE strategy_name LIKE %s
                  AND total_bets > 0
                  AND backtest_date >= %s
            """

            # Create pattern from strategy key
            pattern = f"%{strategy_key.split('_')[0]}%"
            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=30)).strftime(
                "%Y-%m-%d"
            )

            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (pattern, cutoff_date))
                row = cursor.fetchone()

                if row:
                    if isinstance(row, dict):
                        return {
                            "sample_size": int(row["sample_size"]),
                            "roi": float(row["roi"]),
                            "win_rate": float(row["win_rate"]),
                        }
                    else:
                        return {
                            "sample_size": int(row[0]),
                            "roi": float(row[1]),
                            "win_rate": float(row[2]),
                        }

        except Exception as e:
            self.logger.warning(f"Failed to get performance for {strategy_key}: {e}")

        return None

    async def _get_current_sample_size(self, strategy_key: str) -> int:
        """Get current sample size for a strategy"""
        performance = await self._get_strategy_performance(strategy_key)
        return performance["sample_size"] if performance else 0

    async def _get_historical_signal_data(
        self, strategy_key: str
    ) -> list[dict[str, Any]]:
        """Get historical signal data for optimization"""
        try:
            # Get table names from registry
            betting_splits_table = self.table_registry.get_table_name(
                "raw_data", "raw_mlb_betting_splits"
            )
            game_outcomes_table = self.table_registry.get_table_name(
                "core_betting", "game_outcomes"
            )

            # Get betting splits data that would have generated signals
            query = f"""
                SELECT 
                    s.differential,
                    s.game_id,
                    s.split_type,
                    s.source,
                    s.book,
                    o.home_win,
                    o.over,
                    o.home_score,
                    o.away_score
                FROM {betting_splits_table} s
                LEFT JOIN {game_outcomes_table} o ON s.game_id = o.game_id
                WHERE s.last_updated >= %s
                  AND o.home_score IS NOT NULL
                  AND o.away_score IS NOT NULL
                  AND ABS(s.differential) >= 2.0
                ORDER BY s.last_updated DESC
                LIMIT 1000
            """

            cutoff_date = (datetime.now(timezone.utc) - timedelta(days=60)).strftime(
                "%Y-%m-%d"
            )

            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (cutoff_date,))
                rows = cursor.fetchall()

                signals = []
                for row in rows:
                    if isinstance(row, dict):
                        data = row
                    else:
                        columns = [
                            "differential",
                            "game_id",
                            "split_type",
                            "source",
                            "book",
                            "home_win",
                            "over",
                            "home_score",
                            "away_score",
                        ]
                        data = dict(zip(columns, row, strict=False))

                    # Determine if signal would have won
                    won = self._determine_signal_outcome(data)

                    signals.append(
                        {
                            "differential": float(data["differential"]),
                            "split_type": data["split_type"],
                            "source": data["source"],
                            "book": data["book"],
                            "won": won,
                        }
                    )

                return signals

        except Exception as e:
            self.logger.error(f"Failed to get historical signal data: {e}")
            return []

    def _determine_signal_outcome(self, data: dict[str, Any]) -> bool:
        """Determine if a signal would have won based on game outcome"""
        try:
            differential = float(data["differential"])
            split_type = data["split_type"]

            if split_type == "moneyline":
                # Sharp money indicates which team to bet on
                if differential > 0:  # Sharp on home team
                    return bool(data["home_win"])
                else:  # Sharp on away team
                    return not bool(data["home_win"])

            elif split_type == "total":
                # Sharp money indicates over/under
                if differential > 0:  # Sharp on over
                    return bool(data["over"])
                else:  # Sharp on under
                    return not bool(data["over"])

            # For spread, we'd need the actual spread line
            # For now, use moneyline as proxy
            if differential > 0:
                return bool(data["home_win"])
            else:
                return not bool(data["home_win"])

        except (ValueError, TypeError, KeyError):
            return False

    def _calculate_confidence_score(
        self, sample_size: int, roi: float, win_rate: float
    ) -> float:
        """Calculate confidence score based on performance metrics"""

        # Base confidence from sample size
        size_confidence = min(0.95, sample_size / 100)

        # Performance confidence
        perf_confidence = 0.5
        if roi > 10 and win_rate > 0.55:
            perf_confidence = 0.9
        elif roi > 5 and win_rate > 0.52:
            perf_confidence = 0.7
        elif roi > 0 and win_rate > 0.50:
            perf_confidence = 0.6

        # Weighted average
        return (size_confidence * 0.6) + (perf_confidence * 0.4)

    async def _save_threshold_config(self, config: ThresholdConfig) -> None:
        """Save threshold configuration to database"""
        try:
            # Ensure table exists
            await self._ensure_threshold_table_exists()

            # Get table name from registry
            dynamic_thresholds_table = self.table_registry.get_table_name(
                "analytics", "dynamic_thresholds"
            )

            # Upsert configuration
            query = f"""
                INSERT INTO {dynamic_thresholds_table} (
                    strategy_key, phase, sample_size, minimum_threshold,
                    moderate_threshold, high_threshold, current_roi,
                    current_win_rate, confidence_score, threshold_history,
                    last_optimization, next_optimization, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (strategy_key) DO UPDATE SET
                    phase = EXCLUDED.phase,
                    sample_size = EXCLUDED.sample_size,
                    minimum_threshold = EXCLUDED.minimum_threshold,
                    moderate_threshold = EXCLUDED.moderate_threshold,
                    high_threshold = EXCLUDED.high_threshold,
                    current_roi = EXCLUDED.current_roi,
                    current_win_rate = EXCLUDED.current_win_rate,
                    confidence_score = EXCLUDED.confidence_score,
                    threshold_history = EXCLUDED.threshold_history,
                    last_optimization = EXCLUDED.last_optimization,
                    next_optimization = EXCLUDED.next_optimization,
                    updated_at = EXCLUDED.updated_at
            """

            with self.db_manager.get_cursor() as cursor:
                cursor.execute(
                    query,
                    (
                        config.strategy_key,
                        config.phase.value,
                        config.sample_size,
                        config.minimum_threshold,
                        config.moderate_threshold,
                        config.high_threshold,
                        config.current_roi,
                        config.current_win_rate,
                        config.confidence_score,
                        json.dumps(config.threshold_history),
                        config.last_optimization,
                        config.next_optimization,
                        config.created_at,
                        config.updated_at,
                    ),
                )

        except Exception as e:
            self.logger.error(
                f"Failed to save threshold config for {config.strategy_key}: {e}"
            )

    async def _ensure_threshold_table_exists(self) -> None:
        """Ensure the dynamic thresholds table exists"""
        try:
            # Get table name from registry
            dynamic_thresholds_table = self.table_registry.get_table_name(
                "analytics", "dynamic_thresholds"
            )

            query = f"""
                CREATE TABLE IF NOT EXISTS {dynamic_thresholds_table} (
                    strategy_key VARCHAR(255) PRIMARY KEY,
                    phase VARCHAR(50) NOT NULL,
                    sample_size INTEGER NOT NULL DEFAULT 0,
                    minimum_threshold DECIMAL(8,2) NOT NULL,
                    moderate_threshold DECIMAL(8,2) NOT NULL,
                    high_threshold DECIMAL(8,2) NOT NULL,
                    current_roi DECIMAL(8,2) NOT NULL DEFAULT 0.0,
                    current_win_rate DECIMAL(5,4) NOT NULL DEFAULT 0.5,
                    confidence_score DECIMAL(5,4) NOT NULL DEFAULT 0.1,
                    threshold_history JSONB,
                    last_optimization TIMESTAMP,
                    next_optimization TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """

            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query)

        except Exception as e:
            self.logger.error(f"Failed to create dynamic thresholds table: {e}")

    async def get_threshold_summary(self) -> dict[str, Any]:
        """Get summary of all threshold configurations"""
        try:
            # Get table name from registry
            dynamic_thresholds_table = self.table_registry.get_table_name(
                "analytics", "dynamic_thresholds"
            )

            query = f"""
                SELECT 
                    phase,
                    COUNT(*) as count,
                    AVG(minimum_threshold) as avg_min_threshold,
                    AVG(current_roi) as avg_roi,
                    AVG(sample_size) as avg_sample_size
                FROM {dynamic_thresholds_table}
                GROUP BY phase
                ORDER BY 
                    CASE phase 
                        WHEN 'bootstrap' THEN 1
                        WHEN 'learning' THEN 2  
                        WHEN 'calibration' THEN 3
                        WHEN 'optimization' THEN 4
                    END
            """

            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

                summary = {
                    "total_strategies": 0,
                    "by_phase": {},
                    "overall_stats": {
                        "avg_roi_by_phase": {},
                        "avg_threshold_by_phase": {},
                        "sample_size_by_phase": {},
                    },
                }

                for row in rows:
                    if isinstance(row, dict):
                        data = row
                    else:
                        columns = [
                            "phase",
                            "count",
                            "avg_min_threshold",
                            "avg_roi",
                            "avg_sample_size",
                        ]
                        data = dict(zip(columns, row, strict=False))

                    phase = data["phase"]
                    count = int(data["count"])

                    summary["by_phase"][phase] = count
                    summary["total_strategies"] += count
                    summary["overall_stats"]["avg_roi_by_phase"][phase] = float(
                        data["avg_roi"]
                    )
                    summary["overall_stats"]["avg_threshold_by_phase"][phase] = float(
                        data["avg_min_threshold"]
                    )
                    summary["overall_stats"]["sample_size_by_phase"][phase] = float(
                        data["avg_sample_size"]
                    )

                return summary

        except Exception as e:
            self.logger.error(f"Failed to get threshold summary: {e}")
            return {"error": str(e)}


# Singleton instance
_dynamic_threshold_manager: DynamicThresholdManager | None = None


def get_dynamic_threshold_manager() -> DynamicThresholdManager:
    """Get singleton instance of Dynamic Threshold Manager"""
    global _dynamic_threshold_manager
    if _dynamic_threshold_manager is None:
        _dynamic_threshold_manager = DynamicThresholdManager()
    return _dynamic_threshold_manager
