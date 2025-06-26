#!/usr/bin/env python3
"""
Strategy Auto-Integration Service

This service automatically identifies profitable strategies from backtesting results
and ensures they get included in the live pre-game recommendation workflow.

Key Features:
1. Monitors backtesting results for high-ROI strategies (>10 bets, >10% ROI)
2. Auto-configures strategy thresholds based on performance
3. Integrates with opposing markets detector for contrarian strategies
4. Updates recommendation tracker for performance tracking
"""

import asyncio
import structlog
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import pytz

from ..db.connection import DatabaseManager, get_db_manager
from ..core.exceptions import DatabaseError, ValidationError
from .backtesting_service import BacktestingService, StrategyMetrics
from .strategy_config_manager import StrategyConfigManager
from .pre_game_recommendation_tracker import PreGameRecommendationTracker

logger = structlog.get_logger(__name__)


@dataclass
class HighROIStrategy:
    """A strategy identified as having high ROI and sufficient sample size."""
    strategy_id: str
    source_book_type: str
    split_type: str
    strategy_variant: str
    total_bets: int
    win_rate: float
    roi_per_100_unit: float
    confidence_level: str
    
    # Strategy specific configuration
    min_threshold: float  # Minimum signal strength to trigger
    high_threshold: float  # High confidence threshold
    avg_odds: Optional[float] = None
    
    # Performance tracking
    last_backtesting_update: datetime = None
    integration_status: str = "PENDING"  # PENDING, ACTIVE, PAUSED
    created_at: datetime = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)


@dataclass 
class IntegrationResult:
    """Result of integrating a strategy into live recommendations."""
    strategy: HighROIStrategy
    integration_successful: bool
    configuration_updated: bool
    error_message: Optional[str] = None
    threshold_adjustments: Dict[str, float] = None
    
    def __post_init__(self):
        if self.threshold_adjustments is None:
            self.threshold_adjustments = {}


class StrategyAutoIntegration:
    """Service for automatically integrating high-ROI strategies into live recommendations."""
    
    def __init__(self, 
                 db_manager: Optional[DatabaseManager] = None,
                 min_roi_threshold: float = 10.0,
                 min_bet_count: int = 10):
        """Initialize the auto-integration service."""
        self.db_manager = db_manager or get_db_manager()
        self.backtesting_service = BacktestingService(db_manager=self.db_manager)
        self.strategy_config = StrategyConfigManager(db_manager=self.db_manager)
        self.recommendation_tracker = PreGameRecommendationTracker(db_manager=self.db_manager)
        
        # Thresholds for auto-integration
        self.min_roi_threshold = min_roi_threshold
        self.min_bet_count = min_bet_count
        
        # Timezone setup
        self.est = pytz.timezone('US/Eastern')
        
        self.logger = logger.bind(service="strategy_auto_integration")
        
        # Performance tracking
        self.metrics = {
            "strategies_evaluated": 0,
            "strategies_integrated": 0,
            "strategies_updated": 0,
            "strategies_paused": 0,
            "contrarian_strategies_found": 0,
            "opposing_markets_strategies_found": 0
        }
    
    async def identify_high_roi_strategies(self, 
                                         lookback_days: int = 30) -> List[HighROIStrategy]:
        """
        Identify strategies with high ROI and sufficient sample size from recent backtesting.
        
        Args:
            lookback_days: Days to look back for strategy performance
            
        Returns:
            List of high-ROI strategies ready for integration
        """
        self.logger.info("Identifying high-ROI strategies for auto-integration",
                        min_roi=self.min_roi_threshold,
                        min_bets=self.min_bet_count,
                        lookback_days=lookback_days)
        
        high_roi_strategies = []
        
        try:
            # Get recent strategy metrics from backtesting
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=lookback_days)
            
            # Query the latest strategy results
            query = """
            SELECT DISTINCT
                source_book_type,
                split_type,
                strategy_variant,
                total_bets,
                wins,
                win_rate,
                roi_per_100_unit,
                last_updated
            FROM tracking.strategy_performance_cache 
            WHERE last_updated >= %s
              AND total_bets >= %s
              AND roi_per_100_unit >= %s
              AND strategy_variant IS NOT NULL
            ORDER BY roi_per_100_unit DESC, total_bets DESC
            """
            
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (start_date, self.min_bet_count, self.min_roi_threshold))
                results = cursor.fetchall()
            
            for row in results:
                # Calculate thresholds based on performance
                confidence_level = self._determine_confidence_level(
                    row['total_bets'], row['roi_per_100_unit']
                )
                
                min_threshold, high_threshold = self._calculate_thresholds(
                    row['strategy_variant'], row['roi_per_100_unit'], row['win_rate']
                )
                
                strategy = HighROIStrategy(
                    strategy_id=f"{row['source_book_type']}-{row['split_type']}-{row['strategy_variant']}",
                    source_book_type=row['source_book_type'],
                    split_type=row['split_type'],
                    strategy_variant=row['strategy_variant'],
                    total_bets=row['total_bets'],
                    win_rate=float(row['win_rate']),
                    roi_per_100_unit=float(row['roi_per_100_unit']),
                    confidence_level=confidence_level,
                    min_threshold=min_threshold,
                    high_threshold=high_threshold,
                    last_backtesting_update=row['last_updated']
                )
                
                high_roi_strategies.append(strategy)
                self.metrics["strategies_evaluated"] += 1
                
                # Track special strategy types
                if 'contrarian' in row['strategy_variant'].lower():
                    self.metrics["contrarian_strategies_found"] += 1
                if 'opposing' in row['strategy_variant'].lower():
                    self.metrics["opposing_markets_strategies_found"] += 1
            
            self.logger.info("High-ROI strategies identified",
                           total_strategies=len(high_roi_strategies),
                           contrarian_count=self.metrics["contrarian_strategies_found"],
                           opposing_markets_count=self.metrics["opposing_markets_strategies_found"])
            
            return high_roi_strategies
            
        except Exception as e:
            self.logger.error("Failed to identify high-ROI strategies", error=str(e))
            raise DatabaseError(f"Failed to identify strategies: {e}")
    
    def _determine_confidence_level(self, total_bets: int, roi: float) -> str:
        """Determine confidence level based on sample size and ROI."""
        if total_bets >= 50 and roi >= 20.0:
            return "VERY_HIGH"
        elif total_bets >= 30 and roi >= 15.0:
            return "HIGH"
        elif total_bets >= 20 and roi >= 10.0:
            return "MODERATE"
        elif total_bets >= 10 and roi >= 5.0:
            return "LOW"
        else:
            return "VERY_LOW"
    
    def _calculate_thresholds(self, strategy_variant: str, roi: float, win_rate: float) -> Tuple[float, float]:
        """Calculate appropriate signal thresholds based on strategy performance."""
        
        # Base thresholds
        if 'contrarian' in strategy_variant.lower():
            # Contrarian strategies often work with lower thresholds
            if roi >= 50.0:  # Very high ROI like the 79.2% contrarian
                return (8.0, 15.0)  # More aggressive thresholds
            elif roi >= 25.0:
                return (10.0, 18.0)
            else:
                return (12.0, 20.0)
                
        elif 'opposing' in strategy_variant.lower():
            # Opposing markets strategies need moderate thresholds
            if roi >= 30.0:
                return (15.0, 25.0)
            elif roi >= 15.0:
                return (18.0, 28.0)
            else:
                return (20.0, 30.0)
                
        elif 'sharp' in strategy_variant.lower():
            # Sharp action strategies
            if win_rate >= 60.0:
                return (12.0, 20.0)
            else:
                return (15.0, 25.0)
        
        # Default conservative thresholds
        return (15.0, 25.0)
    
    async def integrate_strategy_into_live_system(self, strategy: HighROIStrategy) -> IntegrationResult:
        """
        Integrate a high-ROI strategy into the live recommendation system.
        
        Args:
            strategy: The strategy to integrate
            
        Returns:
            Integration result with success status and details
        """
        self.logger.info("Integrating strategy into live system",
                        strategy_id=strategy.strategy_id,
                        roi=strategy.roi_per_100_unit,
                        total_bets=strategy.total_bets)
        
        try:
            # Update strategy configuration
            config_updated = await self._update_strategy_configuration(strategy)
            
            # Add to active strategies list
            await self._register_active_strategy(strategy)
            
            # Log integration for tracking
            await self._log_strategy_integration(strategy)
            
            self.metrics["strategies_integrated"] += 1
            
            result = IntegrationResult(
                strategy=strategy,
                integration_successful=True,
                configuration_updated=config_updated,
                threshold_adjustments={
                    "min_threshold": strategy.min_threshold,
                    "high_threshold": strategy.high_threshold
                }
            )
            
            self.logger.info("Strategy integration successful",
                           strategy_id=strategy.strategy_id,
                           min_threshold=strategy.min_threshold,
                           high_threshold=strategy.high_threshold)
            
            return result
            
        except Exception as e:
            self.logger.error("Strategy integration failed",
                            strategy_id=strategy.strategy_id,
                            error=str(e))
            
            return IntegrationResult(
                strategy=strategy,
                integration_successful=False,
                configuration_updated=False,
                error_message=str(e)
            )
    
    async def _update_strategy_configuration(self, strategy: HighROIStrategy) -> bool:
        """Update strategy configuration with new thresholds."""
        try:
            # Create configuration entry for this strategy
            config_data = {
                "strategy_type": strategy.strategy_variant,
                "source_book": strategy.source_book_type,
                "split_type": strategy.split_type,
                "enabled": True,
                "min_threshold": strategy.min_threshold,
                "high_threshold": strategy.high_threshold,
                "confidence_level": strategy.confidence_level,
                "roi_per_100_unit": strategy.roi_per_100_unit,
                "win_rate": strategy.win_rate,
                "total_bets": strategy.total_bets,
                "last_updated": strategy.last_backtesting_update.isoformat() if strategy.last_backtesting_update else None
            }
            
            # Insert or update configuration
            upsert_query = """
            INSERT INTO tracking.active_strategy_configs 
            (strategy_id, configuration, created_at, updated_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (strategy_id) 
            DO UPDATE SET 
                configuration = EXCLUDED.configuration,
                updated_at = EXCLUDED.updated_at
            """
            
            import json
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(upsert_query, 
                    (strategy.strategy_id, json.dumps(config_data), strategy.created_at, datetime.now(timezone.utc))
                )
            
            return True
            
        except Exception as e:
            self.logger.error("Failed to update strategy configuration",
                            strategy_id=strategy.strategy_id,
                            error=str(e))
            return False
    
    async def _register_active_strategy(self, strategy: HighROIStrategy):
        """Register strategy as active in the system."""
        try:
            insert_query = """
            INSERT INTO tracking.active_high_roi_strategies 
            (strategy_id, source_book_type, split_type, strategy_variant, 
             total_bets, win_rate, roi_per_100_unit, confidence_level,
             min_threshold, high_threshold, integration_status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'ACTIVE', %s)
            ON CONFLICT (strategy_id) 
            DO UPDATE SET 
                total_bets = EXCLUDED.total_bets,
                win_rate = EXCLUDED.win_rate,
                roi_per_100_unit = EXCLUDED.roi_per_100_unit,
                confidence_level = EXCLUDED.confidence_level,
                min_threshold = EXCLUDED.min_threshold,
                high_threshold = EXCLUDED.high_threshold,
                integration_status = 'ACTIVE',
                updated_at = %s
            """
            
            now = datetime.now(timezone.utc)
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(insert_query,
                    (strategy.strategy_id, strategy.source_book_type, strategy.split_type,
                     strategy.strategy_variant, strategy.total_bets, strategy.win_rate,
                     strategy.roi_per_100_unit, strategy.confidence_level,
                     strategy.min_threshold, strategy.high_threshold, now, now)
                )
            
        except Exception as e:
            self.logger.error("Failed to register active strategy",
                            strategy_id=strategy.strategy_id,
                            error=str(e))
            raise
    
    async def _log_strategy_integration(self, strategy: HighROIStrategy):
        """Log strategy integration event for tracking."""
        try:
            log_query = """
            INSERT INTO tracking.strategy_integration_log 
            (strategy_id, action, roi_per_100_unit, total_bets, 
             min_threshold, high_threshold, created_at)
            VALUES (%s, 'INTEGRATED', %s, %s, %s, %s, %s)
            """
            
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(log_query,
                    (strategy.strategy_id, strategy.roi_per_100_unit, strategy.total_bets,
                     strategy.min_threshold, strategy.high_threshold, datetime.now(timezone.utc))
                )
            
        except Exception as e:
            self.logger.error("Failed to log strategy integration",
                            strategy_id=strategy.strategy_id,
                            error=str(e))
            # Don't raise - this is just logging
    
    async def auto_integrate_high_roi_strategies(self, 
                                               lookback_days: int = 30) -> List[IntegrationResult]:
        """
        Automatically identify and integrate all high-ROI strategies.
        
        Args:
            lookback_days: Days to look back for strategy performance
            
        Returns:
            List of integration results
        """
        self.logger.info("Starting auto-integration of high-ROI strategies",
                        lookback_days=lookback_days)
        
        # Identify high-ROI strategies
        strategies = await self.identify_high_roi_strategies(lookback_days)
        
        if not strategies:
            self.logger.info("No high-ROI strategies found for integration")
            return []
        
        # Integrate each strategy
        results = []
        for strategy in strategies:
            result = await self.integrate_strategy_into_live_system(strategy)
            results.append(result)
            
            # Brief pause between integrations
            await asyncio.sleep(0.1)
        
        # Summary
        successful = len([r for r in results if r.integration_successful])
        failed = len(results) - successful
        
        self.logger.info("Auto-integration completed",
                        total_strategies=len(results),
                        successful=successful,
                        failed=failed)
        
        return results
    
    async def get_active_high_roi_strategies(self) -> List[HighROIStrategy]:
        """Get all currently active high-ROI strategies."""
        try:
            query = """
            SELECT * FROM tracking.active_high_roi_strategies 
            WHERE integration_status = 'ACTIVE'
            ORDER BY roi_per_100_unit DESC
            """
            
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query)
                results = cursor.fetchall()
            
            strategies = []
            for row in results:
                strategy = HighROIStrategy(
                    strategy_id=row['strategy_id'],
                    source_book_type=row['source_book_type'],
                    split_type=row['split_type'],
                    strategy_variant=row['strategy_variant'],
                    total_bets=row['total_bets'],
                    win_rate=float(row['win_rate']),
                    roi_per_100_unit=float(row['roi_per_100_unit']),
                    confidence_level=row['confidence_level'],
                    min_threshold=float(row['min_threshold']),
                    high_threshold=float(row['high_threshold']),
                    integration_status=row['integration_status'],
                    created_at=row['created_at']
                )
                strategies.append(strategy)
            
            return strategies
            
        except Exception as e:
            self.logger.error("Failed to get active high-ROI strategies", error=str(e))
            return []
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get auto-integration metrics."""
        return {
            **self.metrics,
            "timestamp": datetime.now(timezone.utc)
        }