#!/usr/bin/env python3
"""
Strategy Configuration Manager

Manages dynamic strategy configurations based on backtesting results.
Provides validated thresholds and strategy performance data to live detectors.
"""

import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from pathlib import Path

import structlog

from ..db.connection import DatabaseManager, get_db_manager
from ..core.exceptions import DatabaseError


logger = structlog.get_logger(__name__)


@dataclass
class StrategyConfig:
    """Configuration for a validated strategy."""
    strategy_name: str
    source_book_type: str
    split_type: str
    
    # Performance metrics
    win_rate: float
    roi_per_100: float
    total_bets: int
    confidence_level: str
    
    # Thresholds
    min_threshold: float
    moderate_threshold: float
    high_threshold: float
    
    # Status
    is_active: bool
    last_updated: datetime
    
    # Risk metrics
    max_drawdown: float
    sharpe_ratio: float
    kelly_criterion: float


@dataclass
class ThresholdConfig:
    """Threshold configuration for a specific source/strategy."""
    source: str
    strategy_type: str
    
    # Signal strength thresholds
    high_confidence_threshold: float
    moderate_confidence_threshold: float
    minimum_threshold: float
    
    # Opposing markets thresholds
    opposing_high_threshold: float
    opposing_moderate_threshold: float
    
    # Steam move thresholds
    steam_threshold: float
    steam_time_window_hours: float
    
    # Performance requirements
    min_sample_size: int
    min_win_rate: float
    
    last_validated: datetime
    confidence_level: str


class StrategyConfigManager:
    """Manages dynamic strategy configurations based on backtesting results."""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """Initialize the configuration manager."""
        self.db_manager = db_manager or get_db_manager()
        self.logger = logger.bind(service="strategy_config")
        
        # Cache configurations for performance
        self._strategy_cache: Optional[Dict[str, StrategyConfig]] = None
        self._threshold_cache: Optional[Dict[str, ThresholdConfig]] = None
        self._cache_timestamp: Optional[datetime] = None
        self._cache_ttl_minutes = 15  # Refresh every 15 minutes
        
        # Default fallback configurations (conservative)
        self._default_thresholds = {
            "VSIN": ThresholdConfig(
                source="VSIN",
                strategy_type="default",
                high_confidence_threshold=25.0,  # Conservative
                moderate_confidence_threshold=20.0,
                minimum_threshold=15.0,
                opposing_high_threshold=40.0,
                opposing_moderate_threshold=30.0,
                steam_threshold=25.0,
                steam_time_window_hours=2.0,
                min_sample_size=10,
                min_win_rate=0.52,
                last_validated=datetime.now(timezone.utc),
                confidence_level="DEFAULT"
            ),
            "SBD": ThresholdConfig(
                source="SBD", 
                strategy_type="default",
                high_confidence_threshold=30.0,
                moderate_confidence_threshold=25.0,
                minimum_threshold=20.0,
                opposing_high_threshold=45.0,
                opposing_moderate_threshold=35.0,
                steam_threshold=30.0,
                steam_time_window_hours=2.0,
                min_sample_size=10,
                min_win_rate=0.52,
                last_validated=datetime.now(timezone.utc),
                confidence_level="DEFAULT"
            )
        }
    
    async def get_active_strategies(self) -> List[StrategyConfig]:
        """Get all currently active and profitable strategies."""
        await self._refresh_cache_if_needed()
        
        if not self._strategy_cache:
            return []
        
        # Return only active strategies with good performance
        active_strategies = [
            config for config in self._strategy_cache.values()
            if config.is_active and 
               config.win_rate > 0.52 and  # Above break-even
               config.total_bets >= 5      # Minimum sample size
        ]
        
        # Sort by performance (ROI then win rate)
        active_strategies.sort(key=lambda x: (x.roi_per_100, x.win_rate), reverse=True)
        
        self.logger.info("Retrieved active strategies", 
                        total_strategies=len(active_strategies),
                        top_performer=active_strategies[0].strategy_name if active_strategies else None)
        
        return active_strategies
    
    async def get_threshold_config(self, source: str) -> ThresholdConfig:
        """Get validated threshold configuration for a source."""
        await self._refresh_cache_if_needed()
        
        if self._threshold_cache and source in self._threshold_cache:
            return self._threshold_cache[source]
        
        # Fall back to defaults if no validated thresholds available
        if source in self._default_thresholds:
            self.logger.warning("Using default thresholds", source=source)
            return self._default_thresholds[source]
        
        # Ultimate fallback
        self.logger.warning("No thresholds found, using conservative defaults", source=source)
        return self._default_thresholds["VSIN"]  # Most conservative
    
    async def get_strategy_performance(self, strategy_name: str) -> Optional[StrategyConfig]:
        """Get performance data for a specific strategy."""
        await self._refresh_cache_if_needed()
        
        if self._strategy_cache and strategy_name in self._strategy_cache:
            return self._strategy_cache[strategy_name]
        
        return None
    
    async def get_best_strategies_by_type(self, split_type: str = None) -> List[StrategyConfig]:
        """Get the best performing strategies, optionally filtered by split type."""
        active_strategies = await self.get_active_strategies()
        
        if split_type:
            active_strategies = [s for s in active_strategies if s.split_type == split_type]
        
        # Return top 3 strategies by ROI
        return active_strategies[:3]
    
    async def is_strategy_enabled(self, strategy_name: str, min_win_rate: float = 0.52) -> bool:
        """Check if a strategy is currently enabled and performing well."""
        strategy = await self.get_strategy_performance(strategy_name)
        
        if not strategy:
            return False
        
        return (strategy.is_active and 
                strategy.win_rate >= min_win_rate and
                strategy.total_bets >= 5)
    
    async def get_opposing_markets_config(self) -> Dict[str, Any]:
        """Get specific configuration for opposing markets strategy."""
        strategies = await self.get_active_strategies()
        
        # Find opposing markets strategies
        opposing_strategies = [s for s in strategies if 'opposing' in s.strategy_name.lower()]
        
        if not opposing_strategies:
            return {
                "enabled": False,
                "reason": "No validated opposing markets strategies found"
            }
        
        # Get the best performing opposing markets strategy
        best_strategy = max(opposing_strategies, key=lambda x: x.roi_per_100)
        
        return {
            "enabled": True,
            "strategy_name": best_strategy.strategy_name,
            "win_rate": best_strategy.win_rate,
            "roi_per_100": best_strategy.roi_per_100,
            "min_combined_strength": best_strategy.moderate_threshold,
            "high_confidence_strength": best_strategy.high_threshold,
            "last_validated": best_strategy.last_updated
        }
    
    async def get_steam_move_config(self) -> Dict[str, Any]:
        """Get specific configuration for steam move detection."""
        strategies = await self.get_active_strategies()
        
        # Find steam/timing strategies
        steam_strategies = [s for s in strategies if any(keyword in s.strategy_name.lower() 
                          for keyword in ['steam', 'timing', 'move'])]
        
        if not steam_strategies:
            return {
                "enabled": False,
                "reason": "No validated steam move strategies found"
            }
        
        # Get the best performing steam strategy
        best_strategy = max(steam_strategies, key=lambda x: x.win_rate)
        
        return {
            "enabled": True,
            "strategy_name": best_strategy.strategy_name,
            "win_rate": best_strategy.win_rate,
            "roi_per_100": best_strategy.roi_per_100,
            "min_threshold": best_strategy.min_threshold,
            "time_window_hours": 2.0,  # Steam moves are within 2 hours
            "last_validated": best_strategy.last_updated
        }
    
    async def _refresh_cache_if_needed(self) -> None:
        """Refresh cache if it's stale or empty."""
        now = datetime.now(timezone.utc)
        
        if (self._cache_timestamp is None or 
            (now - self._cache_timestamp).total_seconds() > self._cache_ttl_minutes * 60):
            
            await self._load_configurations()
            self._cache_timestamp = now
    
    async def _load_configurations(self) -> None:
        """Load strategy configurations and thresholds from database."""
        try:
            # Load strategy configurations
            self._strategy_cache = await self._load_strategy_configs()
            
            # Load threshold configurations
            self._threshold_cache = await self._load_threshold_configs()
            
            self.logger.info("Configurations loaded successfully",
                           strategies=len(self._strategy_cache) if self._strategy_cache else 0,
                           thresholds=len(self._threshold_cache) if self._threshold_cache else 0)
            
        except Exception as e:
            self.logger.error("Failed to load configurations", error=str(e))
            # Keep existing cache or use defaults
    
    async def _load_strategy_configs(self) -> Dict[str, StrategyConfig]:
        """Load strategy configurations from backtesting results."""
        strategy_configs = {}
        
        try:
            with self.db_manager.get_cursor() as cursor:
                # Get recent strategy performance (last 7 days)
                cursor.execute("""
                    SELECT 
                        strategy_name,
                        source_book_type,
                        split_type,
                        AVG(win_rate) as avg_win_rate,
                        AVG(roi_per_100) as avg_roi,
                        SUM(total_bets) as total_bets,
                        AVG(sharpe_ratio) as avg_sharpe,
                        AVG(max_drawdown) as avg_drawdown,
                        AVG(kelly_criterion) as avg_kelly,
                        MAX(updated_at) as last_updated,
                        COUNT(*) as validation_count
                    FROM backtesting.strategy_performance
                    WHERE backtest_date >= CURRENT_DATE - INTERVAL '7 days'
                    GROUP BY strategy_name, source_book_type, split_type
                    HAVING SUM(total_bets) >= 5
                    ORDER BY avg_roi DESC
                """)
                
                results = cursor.fetchall()
                
                for row in results:
                    (strategy_name, source_book_type, split_type, win_rate, roi_per_100,
                     total_bets, sharpe_ratio, max_drawdown, kelly_criterion, 
                     last_updated, validation_count) = row
                    
                    # Determine confidence level based on sample size and validation count
                    if total_bets >= 50 and validation_count >= 3:
                        confidence = "HIGH"
                    elif total_bets >= 20 and validation_count >= 2:
                        confidence = "MODERATE"
                    elif total_bets >= 5:
                        confidence = "LOW"
                    else:
                        continue  # Skip insufficient data
                    
                    # Calculate dynamic thresholds based on performance
                    if win_rate > 0.65:  # Excellent performance
                        min_thresh, mod_thresh, high_thresh = 10.0, 15.0, 20.0
                    elif win_rate > 0.58:  # Good performance
                        min_thresh, mod_thresh, high_thresh = 15.0, 20.0, 25.0
                    elif win_rate > 0.52:  # Profitable
                        min_thresh, mod_thresh, high_thresh = 20.0, 25.0, 30.0
                    else:  # Below break-even
                        continue  # Don't include
                    
                    config = StrategyConfig(
                        strategy_name=strategy_name,
                        source_book_type=source_book_type or "",
                        split_type=split_type or "",
                        win_rate=float(win_rate),
                        roi_per_100=float(roi_per_100),
                        total_bets=int(total_bets),
                        confidence_level=confidence,
                        min_threshold=min_thresh,
                        moderate_threshold=mod_thresh,
                        high_threshold=high_thresh,
                        is_active=win_rate > 0.52 and total_bets >= 5,
                        last_updated=last_updated,
                        max_drawdown=float(max_drawdown or 0.0),
                        sharpe_ratio=float(sharpe_ratio or 0.0),
                        kelly_criterion=float(kelly_criterion or 0.0)
                    )
                    
                    key = f"{strategy_name}_{source_book_type}_{split_type}"
                    strategy_configs[key] = config
                    
        except Exception as e:
            self.logger.error("Failed to load strategy configs", error=str(e))
        
        return strategy_configs
    
    async def _load_threshold_configs(self) -> Dict[str, ThresholdConfig]:
        """Load threshold configurations from validated recommendations."""
        threshold_configs = {}
        
        try:
            with self.db_manager.get_cursor() as cursor:
                # Get recent threshold recommendations
                cursor.execute("""
                    SELECT 
                        strategy_name,
                        recommended_threshold,
                        confidence_level,
                        created_at
                    FROM backtesting.threshold_recommendations
                    WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
                      AND requires_human_approval = false
                    ORDER BY created_at DESC
                """)
                
                results = cursor.fetchall()
                
                # Process VSIN thresholds
                vsin_thresholds = [r for r in results if 'vsin' in r[0].lower()]
                if vsin_thresholds:
                    latest = vsin_thresholds[0]
                    threshold_configs["VSIN"] = ThresholdConfig(
                        source="VSIN",
                        strategy_type="validated",
                        high_confidence_threshold=float(latest[1]),
                        moderate_confidence_threshold=float(latest[1]) * 0.75,
                        minimum_threshold=float(latest[1]) * 0.5,
                        opposing_high_threshold=float(latest[1]) * 1.5,
                        opposing_moderate_threshold=float(latest[1]) * 1.25,
                        steam_threshold=float(latest[1]),
                        steam_time_window_hours=2.0,
                        min_sample_size=5,
                        min_win_rate=0.52,
                        last_validated=latest[3],
                        confidence_level=latest[2]
                    )
                
                # Process SBD thresholds
                sbd_thresholds = [r for r in results if 'sbd' in r[0].lower()]
                if sbd_thresholds:
                    latest = sbd_thresholds[0]
                    threshold_configs["SBD"] = ThresholdConfig(
                        source="SBD",
                        strategy_type="validated", 
                        high_confidence_threshold=float(latest[1]),
                        moderate_confidence_threshold=float(latest[1]) * 0.8,
                        minimum_threshold=float(latest[1]) * 0.6,
                        opposing_high_threshold=float(latest[1]) * 1.5,
                        opposing_moderate_threshold=float(latest[1]) * 1.25,
                        steam_threshold=float(latest[1]),
                        steam_time_window_hours=2.0,
                        min_sample_size=5,
                        min_win_rate=0.52,
                        last_validated=latest[3],
                        confidence_level=latest[2]
                    )
                    
        except Exception as e:
            self.logger.error("Failed to load threshold configs", error=str(e))
        
        return threshold_configs
    
    async def get_strategy_summary(self) -> Dict[str, Any]:
        """Get a summary of all active strategies and their performance."""
        active_strategies = await self.get_active_strategies()
        
        if not active_strategies:
            return {
                "total_strategies": 0,
                "status": "No validated strategies available",
                "recommendation": "Use conservative defaults"
            }
        
        # Calculate summary statistics
        total_bets = sum(s.total_bets for s in active_strategies)
        avg_win_rate = sum(s.win_rate * s.total_bets for s in active_strategies) / total_bets if total_bets > 0 else 0
        avg_roi = sum(s.roi_per_100 * s.total_bets for s in active_strategies) / total_bets if total_bets > 0 else 0
        
        top_strategy = active_strategies[0]
        
        return {
            "total_strategies": len(active_strategies),
            "total_bets_analyzed": total_bets,
            "weighted_avg_win_rate": avg_win_rate,
            "weighted_avg_roi": avg_roi,
            "top_strategy": {
                "name": top_strategy.strategy_name,
                "win_rate": top_strategy.win_rate,
                "roi_per_100": top_strategy.roi_per_100,
                "total_bets": top_strategy.total_bets
            },
            "status": "Active strategies available",
            "last_updated": max(s.last_updated for s in active_strategies)
        } 