"""
Retraining Trigger Service

Detects when automated retraining should be initiated based on:
- Performance degradation (ROI drops, win rate decreases)
- New data availability (weekly/monthly triggers)
- Market condition changes (significant line movement patterns)
- Scheduled triggers (regular retraining cycles)

Integrates with PrometheusMetricsService for performance monitoring
and provides trigger notifications to the AutomatedRetrainingEngine.
"""

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

from src.core.config import get_settings
from src.core.logging import LogComponent, get_logger
from src.data.database import UnifiedRepository
from src.services.monitoring.prometheus_metrics_service import get_metrics_service


logger = get_logger(__name__, LogComponent.CORE)


class TriggerType(str, Enum):
    """Types of retraining triggers"""
    
    PERFORMANCE_DEGRADATION = "performance_degradation"
    NEW_DATA_AVAILABLE = "new_data_available"
    MARKET_CONDITION_CHANGE = "market_condition_change" 
    SCHEDULED_TRIGGER = "scheduled_trigger"
    MANUAL_OVERRIDE = "manual_override"
    MODEL_DRIFT_DETECTED = "model_drift_detected"


class TriggerSeverity(str, Enum):
    """Severity levels for triggers"""
    
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class PerformanceThresholds:
    """Performance thresholds for trigger detection"""
    
    # ROI thresholds
    min_roi_percentage: float = 5.0  # Minimum acceptable ROI
    roi_degradation_percentage: float = 15.0  # % drop from baseline that triggers retraining
    
    # Win rate thresholds
    min_win_rate: float = 0.55  # Minimum acceptable win rate
    win_rate_degradation_percentage: float = 10.0  # % drop that triggers retraining
    
    # Volume thresholds
    min_daily_opportunities: int = 10  # Minimum opportunities per day
    min_weekly_opportunities: int = 50  # Minimum opportunities per week
    
    # Time windows for measurement
    short_term_days: int = 7  # Short-term performance window
    medium_term_days: int = 30  # Medium-term performance window
    long_term_days: int = 90  # Long-term performance window
    
    # Consecutive periods for degradation detection
    consecutive_poor_periods: int = 3  # Number of consecutive poor periods to trigger


@dataclass
class DataAvailabilityThresholds:
    """Data availability thresholds for trigger detection"""
    
    # Minimum new data required for retraining
    min_new_games: int = 100  # Minimum new games for retraining
    min_new_outcomes: int = 50  # Minimum completed games with outcomes
    
    # Time-based triggers
    max_days_without_retraining: int = 30  # Maximum days without retraining
    weekly_retraining_enabled: bool = True  # Enable weekly retraining
    monthly_retraining_enabled: bool = True  # Enable monthly retraining
    
    # Data quality requirements
    min_data_completeness_score: float = 0.8  # Minimum data quality score
    min_data_freshness_hours: int = 24  # Maximum age of training data


@dataclass
class MarketConditionThresholds:
    """Market condition change thresholds"""
    
    # Line movement pattern changes
    line_movement_pattern_change_threshold: float = 0.3  # Significant pattern change
    sharp_money_pattern_change_threshold: float = 0.25  # Sharp money behavior change
    
    # Market structure changes
    new_sportsbook_threshold: int = 1  # New major sportsbook added
    sportsbook_line_correlation_change: float = 0.2  # Change in book correlation
    
    # Seasonal adjustments
    season_transition_buffer_days: int = 14  # Days around season transitions
    playoff_adjustment_enabled: bool = True  # Adjust thresholds for playoffs


@dataclass
class TriggerCondition:
    """A specific condition that can trigger retraining"""
    
    trigger_id: str
    trigger_type: TriggerType
    severity: TriggerSeverity
    strategy_name: str
    condition_description: str
    detected_at: datetime
    trigger_data: Dict[str, Any] = field(default_factory=dict)
    resolved_at: Optional[datetime] = None
    retraining_triggered: bool = False


class RetrainingTriggerService:
    """
    Service for detecting when automated retraining should be initiated.
    
    Monitors strategy performance, data availability, and market conditions
    to automatically trigger retraining when beneficial or necessary.
    
    Integrates with existing monitoring infrastructure and provides
    triggers to the AutomatedRetrainingEngine.
    """
    
    def __init__(
        self, 
        repository: UnifiedRepository,
        performance_thresholds: Optional[PerformanceThresholds] = None,
        data_thresholds: Optional[DataAvailabilityThresholds] = None,
        market_thresholds: Optional[MarketConditionThresholds] = None
    ):
        """Initialize the retraining trigger service."""
        
        self.repository = repository
        self.config = get_settings()
        self.logger = logger
        self.metrics_service = get_metrics_service()
        
        # Configure thresholds
        self.performance_thresholds = performance_thresholds or PerformanceThresholds()
        self.data_thresholds = data_thresholds or DataAvailabilityThresholds()  
        self.market_thresholds = market_thresholds or MarketConditionThresholds()
        
        # Active triggers and monitoring state
        self.active_triggers: Dict[str, TriggerCondition] = {}
        self.trigger_history: List[TriggerCondition] = []
        self.monitoring_enabled = True
        
        # Performance baselines for comparison
        self.strategy_baselines: Dict[str, Dict[str, float]] = {}
        
        # Last retraining dates for scheduling
        self.last_retraining: Dict[str, datetime] = {}
        
        self.logger.info("RetrainingTriggerService initialized")
    
    async def start_monitoring(self) -> None:
        """Start continuous monitoring for retraining triggers."""
        
        self.monitoring_enabled = True
        self.logger.info("Started retraining trigger monitoring")
        
        # Initialize baselines
        await self._initialize_performance_baselines()
        
        # Start monitoring tasks
        monitoring_tasks = [
            asyncio.create_task(self._monitor_performance_degradation()),
            asyncio.create_task(self._monitor_data_availability()),
            asyncio.create_task(self._monitor_market_conditions()),
            asyncio.create_task(self._monitor_scheduled_triggers()),
        ]
        
        try:
            await asyncio.gather(*monitoring_tasks)
        except Exception as e:
            self.logger.error(f"Error in trigger monitoring: {e}", exc_info=True)
            raise
    
    async def stop_monitoring(self) -> None:
        """Stop monitoring for retraining triggers."""
        
        self.monitoring_enabled = False
        self.logger.info("Stopped retraining trigger monitoring")
    
    async def check_triggers_for_strategy(self, strategy_name: str) -> List[TriggerCondition]:
        """Check all trigger conditions for a specific strategy."""
        
        triggers = []
        
        # Check performance degradation
        performance_triggers = await self._check_performance_degradation(strategy_name)
        triggers.extend(performance_triggers)
        
        # Check data availability
        data_triggers = await self._check_data_availability_triggers(strategy_name)
        triggers.extend(data_triggers)
        
        # Check market condition changes
        market_triggers = await self._check_market_condition_triggers(strategy_name)
        triggers.extend(market_triggers)
        
        # Check scheduled triggers
        scheduled_triggers = await self._check_scheduled_triggers(strategy_name)
        triggers.extend(scheduled_triggers)
        
        # Add triggers to active list
        for trigger in triggers:
            self.active_triggers[trigger.trigger_id] = trigger
            
        # Log trigger summary
        if triggers:
            self.logger.info(
                f"Detected {len(triggers)} triggers for strategy {strategy_name}",
                extra={
                    "strategy": strategy_name,
                    "trigger_count": len(triggers),
                    "trigger_types": [t.trigger_type.value for t in triggers]
                }
            )
        
        return triggers
    
    async def _initialize_performance_baselines(self) -> None:
        """Initialize performance baselines for all strategies."""
        
        self.logger.debug("Initializing performance baselines")
        
        # Get list of strategies from metrics service
        strategies = await self._get_active_strategies()
        
        for strategy in strategies:
            baseline = await self._calculate_performance_baseline(strategy)
            if baseline:
                self.strategy_baselines[strategy] = baseline
                self.logger.debug(f"Initialized baseline for {strategy}: {baseline}")
    
    async def _get_active_strategies(self) -> List[str]:
        """Get list of active strategies from the system."""
        
        # Query database for active strategies
        query = """
        SELECT DISTINCT strategy_name 
        FROM betting_recommendations 
        WHERE created_at > NOW() - INTERVAL '90 days'
        AND strategy_name IS NOT NULL
        ORDER BY strategy_name
        """
        
        try:
            async with self.repository.get_connection() as conn:
                result = await conn.fetch(query)
                return [row["strategy_name"] for row in result]
        except Exception as e:
            self.logger.error(f"Error getting active strategies: {e}")
            # Return default strategies
            return ["sharp_action", "line_movement", "consensus", "late_flip"]
    
    async def _calculate_performance_baseline(self, strategy_name: str) -> Optional[Dict[str, float]]:
        """Calculate performance baseline for a strategy."""
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.performance_thresholds.long_term_days)
        
        query = """
        SELECT 
            AVG(CASE WHEN outcome = 'win' THEN roi ELSE 0 END) as avg_roi,
            COUNT(CASE WHEN outcome = 'win' THEN 1 END)::float / COUNT(*) as win_rate,
            COUNT(*) as total_bets,
            AVG(confidence_score) as avg_confidence
        FROM betting_recommendations 
        WHERE strategy_name = $1 
            AND created_at BETWEEN $2 AND $3
            AND outcome IS NOT NULL
        """
        
        try:
            async with self.repository.get_connection() as conn:
                result = await conn.fetchrow(query, strategy_name, start_date, end_date)
                
                if result and result["total_bets"] and result["total_bets"] > 10:
                    return {
                        "roi": float(result["avg_roi"] or 0),
                        "win_rate": float(result["win_rate"] or 0),
                        "total_bets": int(result["total_bets"]),
                        "avg_confidence": float(result["avg_confidence"] or 0)
                    }
        except Exception as e:
            self.logger.error(f"Error calculating baseline for {strategy_name}: {e}")
        
        return None
    
    async def _monitor_performance_degradation(self) -> None:
        """Monitor for performance degradation triggers."""
        
        while self.monitoring_enabled:
            try:
                strategies = await self._get_active_strategies()
                
                for strategy in strategies:
                    triggers = await self._check_performance_degradation(strategy)
                    
                    for trigger in triggers:
                        if trigger.trigger_id not in self.active_triggers:
                            self.active_triggers[trigger.trigger_id] = trigger
                            await self._notify_trigger_detected(trigger)
                
                # Check every 30 minutes
                await asyncio.sleep(1800)
                
            except Exception as e:
                self.logger.error(f"Error in performance degradation monitoring: {e}", exc_info=True)
                await asyncio.sleep(60)  # Back off on error
    
    async def _check_performance_degradation(self, strategy_name: str) -> List[TriggerCondition]:
        """Check for performance degradation triggers for a strategy."""
        
        triggers = []
        
        # Get baseline performance
        baseline = self.strategy_baselines.get(strategy_name)
        if not baseline:
            return triggers
        
        # Check short-term performance
        short_term_perf = await self._get_recent_performance(
            strategy_name, 
            self.performance_thresholds.short_term_days
        )
        
        if short_term_perf and short_term_perf["total_bets"] >= 5:
            # Check ROI degradation
            if baseline["roi"] > 0:
                roi_drop_percentage = ((baseline["roi"] - short_term_perf["roi"]) / baseline["roi"]) * 100
                
                if roi_drop_percentage > self.performance_thresholds.roi_degradation_percentage:
                    severity = TriggerSeverity.HIGH if roi_drop_percentage > 25 else TriggerSeverity.MEDIUM
                    
                    trigger = TriggerCondition(
                        trigger_id=str(uuid.uuid4()),
                        trigger_type=TriggerType.PERFORMANCE_DEGRADATION,
                        severity=severity,
                        strategy_name=strategy_name,
                        condition_description=f"ROI degraded by {roi_drop_percentage:.1f}% ({baseline['roi']:.1f}% to {short_term_perf['roi']:.1f}%)",
                        detected_at=datetime.now(),
                        trigger_data={
                            "metric": "roi",
                            "baseline_value": baseline["roi"],
                            "current_value": short_term_perf["roi"],
                            "degradation_percentage": roi_drop_percentage,
                            "measurement_period_days": self.performance_thresholds.short_term_days
                        }
                    )
                    triggers.append(trigger)
            
            # Check win rate degradation
            if baseline["win_rate"] > 0:
                win_rate_drop_percentage = ((baseline["win_rate"] - short_term_perf["win_rate"]) / baseline["win_rate"]) * 100
                
                if win_rate_drop_percentage > self.performance_thresholds.win_rate_degradation_percentage:
                    severity = TriggerSeverity.HIGH if win_rate_drop_percentage > 20 else TriggerSeverity.MEDIUM
                    
                    trigger = TriggerCondition(
                        trigger_id=str(uuid.uuid4()),
                        trigger_type=TriggerType.PERFORMANCE_DEGRADATION,
                        severity=severity,
                        strategy_name=strategy_name,
                        condition_description=f"Win rate degraded by {win_rate_drop_percentage:.1f}% ({baseline['win_rate']:.1f}% to {short_term_perf['win_rate']:.1f}%)",
                        detected_at=datetime.now(),
                        trigger_data={
                            "metric": "win_rate",
                            "baseline_value": baseline["win_rate"],
                            "current_value": short_term_perf["win_rate"],
                            "degradation_percentage": win_rate_drop_percentage,
                            "measurement_period_days": self.performance_thresholds.short_term_days
                        }
                    )
                    triggers.append(trigger)
            
            # Check minimum performance thresholds
            if short_term_perf["roi"] < self.performance_thresholds.min_roi_percentage:
                trigger = TriggerCondition(
                    trigger_id=str(uuid.uuid4()),
                    trigger_type=TriggerType.PERFORMANCE_DEGRADATION,
                    severity=TriggerSeverity.CRITICAL,
                    strategy_name=strategy_name,
                    condition_description=f"ROI below minimum threshold ({short_term_perf['roi']:.1f}% < {self.performance_thresholds.min_roi_percentage}%)",
                    detected_at=datetime.now(),
                    trigger_data={
                        "metric": "roi_minimum",
                        "current_value": short_term_perf["roi"],
                        "threshold": self.performance_thresholds.min_roi_percentage
                    }
                )
                triggers.append(trigger)
        
        return triggers
    
    async def _get_recent_performance(self, strategy_name: str, days: int) -> Optional[Dict[str, float]]:
        """Get recent performance metrics for a strategy."""
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        query = """
        SELECT 
            AVG(CASE WHEN outcome = 'win' THEN roi ELSE 0 END) as avg_roi,
            COUNT(CASE WHEN outcome = 'win' THEN 1 END)::float / COUNT(*) as win_rate,
            COUNT(*) as total_bets,
            AVG(confidence_score) as avg_confidence
        FROM betting_recommendations 
        WHERE strategy_name = $1 
            AND created_at BETWEEN $2 AND $3
            AND outcome IS NOT NULL
        """
        
        try:
            async with self.repository.get_connection() as conn:
                result = await conn.fetchrow(query, strategy_name, start_date, end_date)
                
                if result and result["total_bets"]:
                    return {
                        "roi": float(result["avg_roi"] or 0),
                        "win_rate": float(result["win_rate"] or 0),
                        "total_bets": int(result["total_bets"]),
                        "avg_confidence": float(result["avg_confidence"] or 0)
                    }
        except Exception as e:
            self.logger.error(f"Error getting recent performance for {strategy_name}: {e}")
        
        return None
    
    async def _monitor_data_availability(self) -> None:
        """Monitor for new data availability triggers."""
        
        while self.monitoring_enabled:
            try:
                strategies = await self._get_active_strategies()
                
                for strategy in strategies:
                    triggers = await self._check_data_availability_triggers(strategy)
                    
                    for trigger in triggers:
                        if trigger.trigger_id not in self.active_triggers:
                            self.active_triggers[trigger.trigger_id] = trigger
                            await self._notify_trigger_detected(trigger)
                
                # Check every 2 hours
                await asyncio.sleep(7200)
                
            except Exception as e:
                self.logger.error(f"Error in data availability monitoring: {e}", exc_info=True)
                await asyncio.sleep(300)  # Back off on error
    
    async def _check_data_availability_triggers(self, strategy_name: str) -> List[TriggerCondition]:
        """Check for data availability triggers."""
        
        triggers = []
        
        # Check for sufficient new data since last retraining
        last_retraining_date = self.last_retraining.get(strategy_name)
        if not last_retraining_date:
            # If no retraining record, check last 30 days
            last_retraining_date = datetime.now() - timedelta(days=30)
        
        # Count new games and outcomes since last retraining
        new_data_counts = await self._count_new_data_since_date(last_retraining_date)
        
        if new_data_counts["new_games"] >= self.data_thresholds.min_new_games:
            trigger = TriggerCondition(
                trigger_id=str(uuid.uuid4()),
                trigger_type=TriggerType.NEW_DATA_AVAILABLE,
                severity=TriggerSeverity.MEDIUM,
                strategy_name=strategy_name,
                condition_description=f"Sufficient new data available ({new_data_counts['new_games']} games, {new_data_counts['completed_games']} completed)",
                detected_at=datetime.now(),
                trigger_data={
                    "new_games": new_data_counts["new_games"],
                    "completed_games": new_data_counts["completed_games"],
                    "last_retraining_date": last_retraining_date.isoformat()
                }
            )
            triggers.append(trigger)
        
        # Check time-based triggers
        days_since_retraining = (datetime.now() - last_retraining_date).days
        
        if days_since_retraining >= self.data_thresholds.max_days_without_retraining:
            trigger = TriggerCondition(
                trigger_id=str(uuid.uuid4()),
                trigger_type=TriggerType.SCHEDULED_TRIGGER,
                severity=TriggerSeverity.HIGH,
                strategy_name=strategy_name,
                condition_description=f"Maximum days without retraining exceeded ({days_since_retraining} days)",
                detected_at=datetime.now(),
                trigger_data={
                    "days_since_retraining": days_since_retraining,
                    "threshold": self.data_thresholds.max_days_without_retraining,
                    "last_retraining_date": last_retraining_date.isoformat()
                }
            )
            triggers.append(trigger)
        
        return triggers
    
    async def _count_new_data_since_date(self, since_date: datetime) -> Dict[str, int]:
        """Count new data available since a specific date."""
        
        query = """
        SELECT 
            COUNT(*) as new_games,
            COUNT(CASE WHEN game_outcome IS NOT NULL THEN 1 END) as completed_games
        FROM enhanced_games 
        WHERE game_date > $1
        """
        
        try:
            async with self.repository.get_connection() as conn:
                result = await conn.fetchrow(query, since_date)
                return {
                    "new_games": int(result["new_games"]),
                    "completed_games": int(result["completed_games"])
                }
        except Exception as e:
            self.logger.error(f"Error counting new data: {e}")
            return {"new_games": 0, "completed_games": 0}
    
    async def _monitor_market_conditions(self) -> None:
        """Monitor for market condition change triggers."""
        
        while self.monitoring_enabled:
            try:
                strategies = await self._get_active_strategies()
                
                for strategy in strategies:
                    triggers = await self._check_market_condition_triggers(strategy)
                    
                    for trigger in triggers:
                        if trigger.trigger_id not in self.active_triggers:
                            self.active_triggers[trigger.trigger_id] = trigger
                            await self._notify_trigger_detected(trigger)
                
                # Check every 4 hours
                await asyncio.sleep(14400)
                
            except Exception as e:
                self.logger.error(f"Error in market condition monitoring: {e}", exc_info=True)
                await asyncio.sleep(600)  # Back off on error
    
    async def _check_market_condition_triggers(self, strategy_name: str) -> List[TriggerCondition]:
        """Check for market condition change triggers."""
        
        triggers = []
        
        # Check for significant changes in line movement patterns
        line_movement_change = await self._detect_line_movement_pattern_change()
        
        if line_movement_change > self.market_thresholds.line_movement_pattern_change_threshold:
            trigger = TriggerCondition(
                trigger_id=str(uuid.uuid4()),
                trigger_type=TriggerType.MARKET_CONDITION_CHANGE,
                severity=TriggerSeverity.MEDIUM,
                strategy_name=strategy_name,
                condition_description=f"Significant line movement pattern change detected ({line_movement_change:.2f})",
                detected_at=datetime.now(),
                trigger_data={
                    "pattern_change_score": line_movement_change,
                    "threshold": self.market_thresholds.line_movement_pattern_change_threshold
                }
            )
            triggers.append(trigger)
        
        # Check for new sportsbooks or major market structure changes
        new_sportsbooks = await self._detect_new_sportsbooks()
        
        if len(new_sportsbooks) >= self.market_thresholds.new_sportsbook_threshold:
            trigger = TriggerCondition(
                trigger_id=str(uuid.uuid4()),
                trigger_type=TriggerType.MARKET_CONDITION_CHANGE,
                severity=TriggerSeverity.HIGH,
                strategy_name=strategy_name,
                condition_description=f"New sportsbooks detected: {', '.join(new_sportsbooks)}",
                detected_at=datetime.now(),
                trigger_data={
                    "new_sportsbooks": new_sportsbooks,
                    "count": len(new_sportsbooks)
                }
            )
            triggers.append(trigger)
        
        return triggers
    
    async def _detect_line_movement_pattern_change(self) -> float:
        """Detect changes in line movement patterns."""
        
        # Simplified implementation - would analyze actual line movement patterns
        # For now, return a mock score
        return 0.15  # Below threshold
    
    async def _detect_new_sportsbooks(self) -> List[str]:
        """Detect new sportsbooks that have started providing lines."""
        
        # Check for sportsbooks that appeared in the last 7 days
        query = """
        SELECT DISTINCT sportsbook
        FROM staging_action_network_odds_historical
        WHERE timestamp > NOW() - INTERVAL '7 days'
            AND sportsbook NOT IN (
                SELECT DISTINCT sportsbook
                FROM staging_action_network_odds_historical
                WHERE timestamp BETWEEN NOW() - INTERVAL '30 days' AND NOW() - INTERVAL '7 days'
            )
        """
        
        try:
            async with self.repository.get_connection() as conn:
                result = await conn.fetch(query)
                return [row["sportsbook"] for row in result]
        except Exception as e:
            self.logger.error(f"Error detecting new sportsbooks: {e}")
            return []
    
    async def _monitor_scheduled_triggers(self) -> None:
        """Monitor for scheduled retraining triggers."""
        
        while self.monitoring_enabled:
            try:
                strategies = await self._get_active_strategies()
                
                for strategy in strategies:
                    triggers = await self._check_scheduled_triggers(strategy)
                    
                    for trigger in triggers:
                        if trigger.trigger_id not in self.active_triggers:
                            self.active_triggers[trigger.trigger_id] = trigger
                            await self._notify_trigger_detected(trigger)
                
                # Check every hour
                await asyncio.sleep(3600)
                
            except Exception as e:
                self.logger.error(f"Error in scheduled trigger monitoring: {e}", exc_info=True)
                await asyncio.sleep(300)  # Back off on error
    
    async def _check_scheduled_triggers(self, strategy_name: str) -> List[TriggerCondition]:
        """Check for scheduled retraining triggers."""
        
        triggers = []
        current_time = datetime.now()
        
        # Check weekly triggers (every Sunday)
        if (self.data_thresholds.weekly_retraining_enabled and 
            current_time.weekday() == 6 and  # Sunday
            current_time.hour >= 2 and current_time.hour < 4):  # 2-4 AM
            
            last_weekly = self._get_last_weekly_trigger(strategy_name)
            if not last_weekly or (current_time - last_weekly).days >= 6:
                trigger = TriggerCondition(
                    trigger_id=str(uuid.uuid4()),
                    trigger_type=TriggerType.SCHEDULED_TRIGGER,
                    severity=TriggerSeverity.LOW,
                    strategy_name=strategy_name,
                    condition_description="Weekly scheduled retraining",
                    detected_at=current_time,
                    trigger_data={
                        "trigger_schedule": "weekly",
                        "day_of_week": "sunday",
                        "hour": current_time.hour
                    }
                )
                triggers.append(trigger)
        
        # Check monthly triggers (first Sunday of month)
        if (self.data_thresholds.monthly_retraining_enabled and 
            current_time.weekday() == 6 and  # Sunday
            current_time.day <= 7 and  # First week of month
            current_time.hour >= 3 and current_time.hour < 5):  # 3-5 AM
            
            last_monthly = self._get_last_monthly_trigger(strategy_name)
            if not last_monthly or (current_time - last_monthly).days >= 25:
                trigger = TriggerCondition(
                    trigger_id=str(uuid.uuid4()),
                    trigger_type=TriggerType.SCHEDULED_TRIGGER,
                    severity=TriggerSeverity.MEDIUM,
                    strategy_name=strategy_name,
                    condition_description="Monthly scheduled retraining",
                    detected_at=current_time,
                    trigger_data={
                        "trigger_schedule": "monthly",
                        "first_sunday_of_month": True,
                        "hour": current_time.hour
                    }
                )
                triggers.append(trigger)
        
        return triggers
    
    def _get_last_weekly_trigger(self, strategy_name: str) -> Optional[datetime]:
        """Get the last weekly trigger time for a strategy."""
        
        weekly_triggers = [
            t for t in self.trigger_history 
            if (t.strategy_name == strategy_name and 
                t.trigger_type == TriggerType.SCHEDULED_TRIGGER and
                t.trigger_data.get("trigger_schedule") == "weekly")
        ]
        
        return max([t.detected_at for t in weekly_triggers]) if weekly_triggers else None
    
    def _get_last_monthly_trigger(self, strategy_name: str) -> Optional[datetime]:
        """Get the last monthly trigger time for a strategy."""
        
        monthly_triggers = [
            t for t in self.trigger_history 
            if (t.strategy_name == strategy_name and 
                t.trigger_type == TriggerType.SCHEDULED_TRIGGER and
                t.trigger_data.get("trigger_schedule") == "monthly")
        ]
        
        return max([t.detected_at for t in monthly_triggers]) if monthly_triggers else None
    
    async def _notify_trigger_detected(self, trigger: TriggerCondition) -> None:
        """Notify that a trigger condition has been detected."""
        
        # Record metrics
        self.metrics_service.record_opportunity_detected(
            strategy=trigger.strategy_name,
            confidence_level=trigger.severity.value
        )
        
        # Log trigger
        self.logger.info(
            f"Retraining trigger detected: {trigger.trigger_type.value}",
            extra={
                "trigger_id": trigger.trigger_id,
                "strategy": trigger.strategy_name,
                "severity": trigger.severity.value,
                "description": trigger.condition_description,
                "trigger_data": trigger.trigger_data
            }
        )
        
        # Add to history
        self.trigger_history.append(trigger)
    
    async def create_manual_trigger(
        self, 
        strategy_name: str, 
        reason: str,
        severity: TriggerSeverity = TriggerSeverity.HIGH
    ) -> TriggerCondition:
        """Create a manual retraining trigger."""
        
        trigger = TriggerCondition(
            trigger_id=str(uuid.uuid4()),
            trigger_type=TriggerType.MANUAL_OVERRIDE,
            severity=severity,
            strategy_name=strategy_name,
            condition_description=f"Manual trigger: {reason}",
            detected_at=datetime.now(),
            trigger_data={
                "manual_reason": reason,
                "initiated_by": "user"  # Could be enhanced with user tracking
            }
        )
        
        self.active_triggers[trigger.trigger_id] = trigger
        await self._notify_trigger_detected(trigger)
        
        return trigger
    
    def get_active_triggers(self) -> List[TriggerCondition]:
        """Get all active trigger conditions."""
        return list(self.active_triggers.values())
    
    def get_triggers_for_strategy(self, strategy_name: str) -> List[TriggerCondition]:
        """Get active triggers for a specific strategy."""
        return [
            trigger for trigger in self.active_triggers.values()
            if trigger.strategy_name == strategy_name
        ]
    
    def resolve_trigger(self, trigger_id: str) -> bool:
        """Mark a trigger as resolved (retraining completed)."""
        
        if trigger_id in self.active_triggers:
            trigger = self.active_triggers[trigger_id]
            trigger.resolved_at = datetime.now()
            trigger.retraining_triggered = True
            
            # Remove from active triggers
            del self.active_triggers[trigger_id]
            
            self.logger.info(f"Resolved trigger {trigger_id} for strategy {trigger.strategy_name}")
            return True
        
        return False
    
    def get_trigger_statistics(self) -> Dict[str, Any]:
        """Get comprehensive trigger statistics."""
        
        active_count = len(self.active_triggers)
        total_history = len(self.trigger_history)
        
        # Count by type
        type_counts = {}
        for trigger in self.trigger_history:
            trigger_type = trigger.trigger_type.value
            type_counts[trigger_type] = type_counts.get(trigger_type, 0) + 1
        
        # Count by strategy
        strategy_counts = {}
        for trigger in self.trigger_history:
            strategy = trigger.strategy_name
            strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1
        
        # Recent activity (last 7 days)
        recent_triggers = [
            t for t in self.trigger_history
            if (datetime.now() - t.detected_at).days <= 7
        ]
        
        return {
            "active_triggers": active_count,
            "total_triggers_detected": total_history,
            "recent_triggers_7_days": len(recent_triggers),
            "triggers_by_type": type_counts,
            "triggers_by_strategy": strategy_counts,
            "monitoring_enabled": self.monitoring_enabled,
            "strategies_monitored": len(self.strategy_baselines)
        }