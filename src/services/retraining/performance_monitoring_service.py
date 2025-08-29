"""
Performance Monitoring Service

Provides real-time strategy performance tracking and trend analysis for
the automated retraining system. Monitors strategy performance over time,
detects performance degradation, and provides early warning systems.

Features:
- Real-time strategy performance tracking
- ROI and win rate monitoring with alerting
- Model drift detection and early warning systems
- Performance trend analysis and forecasting
- Integration with Prometheus metrics service
- Comprehensive performance audit logs
"""

import asyncio
import numpy as np
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
import warnings

from src.core.config import get_settings
from src.core.logging import LogComponent, get_logger
from src.data.database import UnifiedRepository
from src.services.monitoring.prometheus_metrics_service import get_metrics_service


logger = get_logger(__name__, LogComponent.CORE)


class PerformanceTrend(str, Enum):
    """Performance trend directions"""
    
    IMPROVING = "improving"
    STABLE = "stable"
    DECLINING = "declining"
    VOLATILE = "volatile"
    UNKNOWN = "unknown"


class AlertLevel(str, Enum):
    """Alert levels for performance issues"""
    
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class PerformanceWindow:
    """Performance data for a specific time window"""
    
    start_date: datetime
    end_date: datetime
    total_bets: int
    winning_bets: int
    losing_bets: int
    push_bets: int
    
    total_roi: float
    avg_roi_per_bet: float
    win_rate: float
    
    total_volume: float
    avg_confidence: float
    
    # Risk metrics
    max_drawdown: float
    volatility: float
    sharpe_ratio: float
    
    # Additional metrics
    profitable_days: int
    total_days: int
    longest_winning_streak: int
    longest_losing_streak: int


@dataclass
class PerformanceAlert:
    """Performance alert notification"""
    
    alert_id: str
    strategy_name: str
    alert_type: str
    alert_level: AlertLevel
    message: str
    
    triggered_at: datetime
    current_value: float
    threshold_value: float
    
    metadata: Dict[str, Any] = field(default_factory=dict)
    resolved_at: Optional[datetime] = None


@dataclass
class TrendAnalysis:
    """Trend analysis for strategy performance"""
    
    trend_direction: PerformanceTrend
    trend_strength: float  # 0-1, how strong the trend is
    trend_duration_days: int
    
    # Statistical trend metrics
    slope: float
    r_squared: float
    p_value: float
    
    # Forecasting
    predicted_7_day_roi: float
    predicted_30_day_roi: float
    forecast_confidence: float
    
    analysis_period: str
    data_points: int


class PerformanceMonitoringService:
    """
    Service for monitoring strategy performance and detecting issues.
    
    Provides comprehensive monitoring capabilities including:
    - Real-time performance tracking
    - Trend analysis and forecasting
    - Performance degradation detection
    - Alert generation and management
    - Integration with metrics service
    """
    
    def __init__(
        self,
        repository: UnifiedRepository,
        monitoring_interval_minutes: int = 30,
        alert_thresholds: Optional[Dict[str, float]] = None
    ):
        """Initialize the performance monitoring service."""
        
        self.repository = repository
        self.config = get_settings()
        self.logger = logger
        self.metrics_service = get_metrics_service()
        
        # Configuration
        self.monitoring_interval = monitoring_interval_minutes * 60  # Convert to seconds
        self.alert_thresholds = alert_thresholds or self._default_alert_thresholds()
        
        # Performance data storage
        self.performance_history: Dict[str, deque] = {}  # strategy -> performance windows
        self.current_performance: Dict[str, PerformanceWindow] = {}  # strategy -> current window
        
        # Alert management
        self.active_alerts: Dict[str, PerformanceAlert] = {}
        self.alert_history: List[PerformanceAlert] = []
        
        # Trend analysis
        self.trend_analysis: Dict[str, TrendAnalysis] = {}
        
        # Monitoring state
        self.monitoring_enabled = False
        self.last_update_time: Dict[str, datetime] = {}
        
        # Initialize performance history storage (keep last 100 windows per strategy)
        self.max_history_windows = 100
        
        self.logger.info("PerformanceMonitoringService initialized")
    
    def _default_alert_thresholds(self) -> Dict[str, float]:
        """Default alert thresholds for performance monitoring."""
        
        return {
            # ROI thresholds
            "roi_critical_threshold": -5.0,  # Below -5% ROI
            "roi_warning_threshold": 0.0,   # Below 0% ROI
            "roi_degradation_threshold": 15.0,  # 15% drop from baseline
            
            # Win rate thresholds
            "win_rate_critical_threshold": 0.45,  # Below 45% win rate
            "win_rate_warning_threshold": 0.50,   # Below 50% win rate
            "win_rate_degradation_threshold": 0.05,  # 5% drop from baseline
            
            # Volume thresholds
            "min_daily_volume": 5,  # Minimum bets per day
            "min_weekly_volume": 20,  # Minimum bets per week
            
            # Risk thresholds
            "max_drawdown_warning": 10.0,  # 10% drawdown warning
            "max_drawdown_critical": 20.0,  # 20% drawdown critical
            "min_sharpe_ratio": 0.5,  # Minimum Sharpe ratio
            
            # Trend thresholds
            "trend_significance_threshold": 0.05,  # p < 0.05
            "trend_strength_threshold": 0.3,  # Minimum trend strength
        }
    
    async def start_monitoring(self, strategies: Optional[List[str]] = None) -> None:
        """Start performance monitoring for specified strategies."""
        
        self.monitoring_enabled = True
        
        # Get strategies to monitor
        if strategies is None:
            strategies = await self._get_active_strategies()
        
        # Initialize performance history for each strategy
        for strategy in strategies:
            if strategy not in self.performance_history:
                self.performance_history[strategy] = deque(maxlen=self.max_history_windows)
            
            # Load initial performance data
            await self._initialize_strategy_performance(strategy)
        
        self.logger.info(
            f"Started performance monitoring for {len(strategies)} strategies",
            extra={"strategies": strategies}
        )
        
        # Start monitoring loop
        asyncio.create_task(self._monitoring_loop())
    
    async def stop_monitoring(self) -> None:
        """Stop performance monitoring."""
        
        self.monitoring_enabled = False
        self.logger.info("Stopped performance monitoring")
    
    async def _monitoring_loop(self) -> None:
        """Main monitoring loop."""
        
        while self.monitoring_enabled:
            try:
                strategies = list(self.performance_history.keys())
                
                for strategy in strategies:
                    await self._update_strategy_performance(strategy)
                    await self._check_performance_alerts(strategy)
                    await self._update_trend_analysis(strategy)
                
                # Update metrics service
                await self._update_prometheus_metrics()
                
                await asyncio.sleep(self.monitoring_interval)
                
            except Exception as e:
                self.logger.error(f"Error in performance monitoring loop: {e}", exc_info=True)
                await asyncio.sleep(60)  # Back off on error
    
    async def _get_active_strategies(self) -> List[str]:
        """Get list of active strategies from database."""
        
        query = """
        SELECT DISTINCT strategy_name 
        FROM betting_recommendations 
        WHERE created_at > NOW() - INTERVAL '30 days'
        AND strategy_name IS NOT NULL
        ORDER BY strategy_name
        """
        
        try:
            async with self.repository.get_connection() as conn:
                result = await conn.fetch(query)
                return [row["strategy_name"] for row in result]
        except Exception as e:
            self.logger.error(f"Error getting active strategies: {e}")
            return ["sharp_action", "line_movement", "consensus"]
    
    async def _initialize_strategy_performance(self, strategy_name: str) -> None:
        """Initialize performance data for a strategy."""
        
        # Load recent performance windows
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        # Get performance data in daily windows
        daily_windows = await self._get_performance_windows(
            strategy_name, start_date, end_date, window_hours=24
        )
        
        # Add to performance history
        for window in daily_windows:
            self.performance_history[strategy_name].append(window)
        
        self.last_update_time[strategy_name] = datetime.now()
        
        self.logger.debug(f"Initialized performance history for {strategy_name}: {len(daily_windows)} windows")
    
    async def _get_performance_windows(
        self,
        strategy_name: str,
        start_date: datetime,
        end_date: datetime,
        window_hours: int = 24
    ) -> List[PerformanceWindow]:
        """Get performance data in time windows."""
        
        query = """
        WITH daily_performance AS (
            SELECT 
                DATE_TRUNC('day', created_at) as day,
                COUNT(*) as total_bets,
                COUNT(CASE WHEN outcome = 'win' THEN 1 END) as winning_bets,
                COUNT(CASE WHEN outcome = 'loss' THEN 1 END) as losing_bets,
                COUNT(CASE WHEN outcome = 'push' THEN 1 END) as push_bets,
                SUM(CASE WHEN outcome = 'win' THEN roi ELSE 0 END) as total_roi,
                AVG(CASE WHEN outcome = 'win' THEN roi ELSE 0 END) as avg_roi,
                COUNT(CASE WHEN outcome = 'win' THEN 1 END)::float / COUNT(*) as win_rate,
                SUM(bet_amount) as total_volume,
                AVG(confidence_score) as avg_confidence
            FROM betting_recommendations
            WHERE strategy_name = $1
                AND created_at BETWEEN $2 AND $3
                AND outcome IS NOT NULL
            GROUP BY day
            ORDER BY day
        )
        SELECT * FROM daily_performance
        WHERE total_bets > 0
        """
        
        windows = []
        
        try:
            async with self.repository.get_connection() as conn:
                rows = await conn.fetch(query, strategy_name, start_date, end_date)
                
                for row in rows:
                    # Calculate additional metrics
                    max_drawdown = await self._calculate_max_drawdown_for_day(
                        strategy_name, row["day"]
                    )
                    
                    window = PerformanceWindow(
                        start_date=row["day"],
                        end_date=row["day"] + timedelta(days=1),
                        total_bets=int(row["total_bets"]),
                        winning_bets=int(row["winning_bets"]),
                        losing_bets=int(row["losing_bets"]),
                        push_bets=int(row["push_bets"]),
                        total_roi=float(row["total_roi"] or 0),
                        avg_roi_per_bet=float(row["avg_roi"] or 0),
                        win_rate=float(row["win_rate"] or 0),
                        total_volume=float(row["total_volume"] or 0),
                        avg_confidence=float(row["avg_confidence"] or 0),
                        max_drawdown=max_drawdown,
                        volatility=0.0,  # Would calculate from intraday data
                        sharpe_ratio=0.0,  # Would calculate with risk-free rate
                        profitable_days=1 if row["total_roi"] > 0 else 0,
                        total_days=1,
                        longest_winning_streak=0,  # Would track across time
                        longest_losing_streak=0
                    )
                    
                    windows.append(window)
        
        except Exception as e:
            self.logger.error(f"Error getting performance windows for {strategy_name}: {e}")
        
        return windows
    
    async def _calculate_max_drawdown_for_day(self, strategy_name: str, day: datetime) -> float:
        """Calculate maximum drawdown for a specific day."""
        
        # Simplified calculation - would use intraday data in production
        query = """
        SELECT 
            SUM(CASE WHEN outcome = 'win' THEN roi ELSE -1 END) as cumulative_return
        FROM betting_recommendations
        WHERE strategy_name = $1
            AND DATE_TRUNC('day', created_at) = $2
            AND outcome IS NOT NULL
        ORDER BY created_at
        """
        
        try:
            async with self.repository.get_connection() as conn:
                result = await conn.fetchrow(query, strategy_name, day)
                # Simplified - actual calculation would track running drawdown
                return abs(float(result["cumulative_return"] or 0)) * 0.1  # Mock calculation
        except Exception as e:
            self.logger.warning(f"Error calculating drawdown for {strategy_name} on {day}: {e}")
            return 0.0
    
    async def _update_strategy_performance(self, strategy_name: str) -> None:
        """Update performance data for a strategy."""
        
        # Get latest performance window (last 24 hours)
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=24)
        
        windows = await self._get_performance_windows(
            strategy_name, start_time, end_time, window_hours=24
        )
        
        if windows:
            latest_window = windows[0]
            
            # Update current performance
            self.current_performance[strategy_name] = latest_window
            
            # Add to history if it's a new window
            last_window = (
                self.performance_history[strategy_name][-1] 
                if self.performance_history[strategy_name] else None
            )
            
            if (not last_window or 
                latest_window.start_date > last_window.start_date):
                self.performance_history[strategy_name].append(latest_window)
        
        self.last_update_time[strategy_name] = datetime.now()
    
    async def _check_performance_alerts(self, strategy_name: str) -> None:
        """Check for performance alerts for a strategy."""
        
        current_window = self.current_performance.get(strategy_name)
        if not current_window:
            return
        
        # Check ROI alerts
        await self._check_roi_alerts(strategy_name, current_window)
        
        # Check win rate alerts
        await self._check_win_rate_alerts(strategy_name, current_window)
        
        # Check volume alerts
        await self._check_volume_alerts(strategy_name, current_window)
        
        # Check risk alerts
        await self._check_risk_alerts(strategy_name, current_window)
    
    async def _check_roi_alerts(self, strategy_name: str, window: PerformanceWindow) -> None:
        """Check ROI-related alerts."""
        
        current_roi = window.avg_roi_per_bet
        
        # Critical ROI threshold
        if current_roi < self.alert_thresholds["roi_critical_threshold"]:
            await self._create_alert(
                strategy_name,
                "roi_critical",
                AlertLevel.CRITICAL,
                f"ROI critically low: {current_roi:.2f}%",
                current_roi,
                self.alert_thresholds["roi_critical_threshold"]
            )
        
        # Warning ROI threshold
        elif current_roi < self.alert_thresholds["roi_warning_threshold"]:
            await self._create_alert(
                strategy_name,
                "roi_warning",
                AlertLevel.WARNING,
                f"ROI below target: {current_roi:.2f}%",
                current_roi,
                self.alert_thresholds["roi_warning_threshold"]
            )
        
        # Check ROI degradation vs historical baseline
        baseline_roi = await self._get_historical_baseline_roi(strategy_name)
        if baseline_roi and baseline_roi > 0:
            degradation_pct = ((baseline_roi - current_roi) / baseline_roi) * 100
            
            if degradation_pct > self.alert_thresholds["roi_degradation_threshold"]:
                await self._create_alert(
                    strategy_name,
                    "roi_degradation",
                    AlertLevel.WARNING,
                    f"ROI degraded {degradation_pct:.1f}% from baseline ({baseline_roi:.2f}% to {current_roi:.2f}%)",
                    degradation_pct,
                    self.alert_thresholds["roi_degradation_threshold"]
                )
    
    async def _check_win_rate_alerts(self, strategy_name: str, window: PerformanceWindow) -> None:
        """Check win rate related alerts."""
        
        current_win_rate = window.win_rate
        
        # Critical win rate threshold
        if current_win_rate < self.alert_thresholds["win_rate_critical_threshold"]:
            await self._create_alert(
                strategy_name,
                "win_rate_critical",
                AlertLevel.CRITICAL,
                f"Win rate critically low: {current_win_rate:.1%}",
                current_win_rate,
                self.alert_thresholds["win_rate_critical_threshold"]
            )
        
        # Warning win rate threshold
        elif current_win_rate < self.alert_thresholds["win_rate_warning_threshold"]:
            await self._create_alert(
                strategy_name,
                "win_rate_warning",
                AlertLevel.WARNING,
                f"Win rate below target: {current_win_rate:.1%}",
                current_win_rate,
                self.alert_thresholds["win_rate_warning_threshold"]
            )
    
    async def _check_volume_alerts(self, strategy_name: str, window: PerformanceWindow) -> None:
        """Check betting volume alerts."""
        
        daily_bets = window.total_bets
        
        if daily_bets < self.alert_thresholds["min_daily_volume"]:
            await self._create_alert(
                strategy_name,
                "low_volume",
                AlertLevel.WARNING,
                f"Low betting volume: {daily_bets} bets in 24h",
                daily_bets,
                self.alert_thresholds["min_daily_volume"]
            )
    
    async def _check_risk_alerts(self, strategy_name: str, window: PerformanceWindow) -> None:
        """Check risk-related alerts."""
        
        # Max drawdown alerts
        if window.max_drawdown > self.alert_thresholds["max_drawdown_critical"]:
            await self._create_alert(
                strategy_name,
                "max_drawdown_critical",
                AlertLevel.CRITICAL,
                f"Maximum drawdown exceeded: {window.max_drawdown:.1f}%",
                window.max_drawdown,
                self.alert_thresholds["max_drawdown_critical"]
            )
        
        elif window.max_drawdown > self.alert_thresholds["max_drawdown_warning"]:
            await self._create_alert(
                strategy_name,
                "max_drawdown_warning",
                AlertLevel.WARNING,
                f"High drawdown detected: {window.max_drawdown:.1f}%",
                window.max_drawdown,
                self.alert_thresholds["max_drawdown_warning"]
            )
    
    async def _create_alert(
        self,
        strategy_name: str,
        alert_type: str,
        level: AlertLevel,
        message: str,
        current_value: float,
        threshold_value: float
    ) -> None:
        """Create a performance alert."""
        
        alert_key = f"{strategy_name}_{alert_type}"
        
        # Check if alert already exists
        if alert_key in self.active_alerts:
            # Update existing alert
            existing_alert = self.active_alerts[alert_key]
            existing_alert.current_value = current_value
            existing_alert.triggered_at = datetime.now()
        else:
            # Create new alert
            alert = PerformanceAlert(
                alert_id=alert_key,
                strategy_name=strategy_name,
                alert_type=alert_type,
                alert_level=level,
                message=message,
                triggered_at=datetime.now(),
                current_value=current_value,
                threshold_value=threshold_value
            )
            
            self.active_alerts[alert_key] = alert
            
            # Log the alert
            self.logger.warning(
                f"Performance alert: {message}",
                extra={
                    "strategy": strategy_name,
                    "alert_type": alert_type,
                    "alert_level": level.value,
                    "current_value": current_value,
                    "threshold": threshold_value
                }
            )
            
            # Record metrics
            self.metrics_service.record_opportunity_detected(
                strategy=strategy_name,
                confidence_level=level.value
            )
    
    async def _get_historical_baseline_roi(self, strategy_name: str) -> Optional[float]:
        """Get historical baseline ROI for comparison."""
        
        if strategy_name not in self.performance_history:
            return None
        
        windows = list(self.performance_history[strategy_name])
        if len(windows) < 7:  # Need at least a week of data
            return None
        
        # Use last 30 days as baseline
        recent_windows = windows[-30:] if len(windows) >= 30 else windows
        
        total_roi = sum(w.avg_roi_per_bet for w in recent_windows)
        return total_roi / len(recent_windows)
    
    async def _update_trend_analysis(self, strategy_name: str) -> None:
        """Update trend analysis for a strategy."""
        
        if strategy_name not in self.performance_history:
            return
        
        windows = list(self.performance_history[strategy_name])
        if len(windows) < 5:  # Need minimum data for trend analysis
            return
        
        # Extract ROI time series
        roi_values = [w.avg_roi_per_bet for w in windows[-30:]]  # Last 30 windows
        time_points = list(range(len(roi_values)))
        
        # Calculate trend
        trend_analysis = self._calculate_trend_statistics(time_points, roi_values)
        
        # Forecast future performance
        forecast_7_day = self._forecast_performance(roi_values, 7)
        forecast_30_day = self._forecast_performance(roi_values, 30)
        
        # Determine trend direction and strength
        trend_direction = self._determine_trend_direction(trend_analysis["slope"], trend_analysis["r_squared"])
        trend_strength = abs(trend_analysis["r_squared"])
        
        self.trend_analysis[strategy_name] = TrendAnalysis(
            trend_direction=trend_direction,
            trend_strength=trend_strength,
            trend_duration_days=len(roi_values),
            slope=trend_analysis["slope"],
            r_squared=trend_analysis["r_squared"],
            p_value=trend_analysis["p_value"],
            predicted_7_day_roi=forecast_7_day,
            predicted_30_day_roi=forecast_30_day,
            forecast_confidence=min(trend_strength, 0.8),  # Cap at 0.8
            analysis_period=f"last_{len(roi_values)}_windows",
            data_points=len(roi_values)
        )
    
    def _calculate_trend_statistics(self, x_values: List[int], y_values: List[float]) -> Dict[str, float]:
        """Calculate trend statistics using linear regression."""
        
        if len(x_values) < 3:
            return {"slope": 0.0, "r_squared": 0.0, "p_value": 1.0}
        
        try:
            # Convert to numpy arrays
            x = np.array(x_values)
            y = np.array(y_values)
            
            # Calculate linear regression
            slope, intercept = np.polyfit(x, y, 1)
            
            # Calculate R-squared
            y_pred = slope * x + intercept
            ss_res = np.sum((y - y_pred) ** 2)
            ss_tot = np.sum((y - np.mean(y)) ** 2)
            
            r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0.0
            
            # Calculate p-value (simplified)
            n = len(x)
            if n > 2:
                t_stat = abs(slope) * np.sqrt((n - 2) / (1 - r_squared)) if r_squared < 1.0 else 0
                # Simplified p-value calculation
                p_value = max(0.001, 2 * (1 - min(0.999, abs(t_stat) / 10)))
            else:
                p_value = 1.0
            
            return {
                "slope": float(slope),
                "r_squared": float(r_squared),
                "p_value": float(p_value)
            }
        
        except Exception as e:
            self.logger.warning(f"Error calculating trend statistics: {e}")
            return {"slope": 0.0, "r_squared": 0.0, "p_value": 1.0}
    
    def _determine_trend_direction(self, slope: float, r_squared: float) -> PerformanceTrend:
        """Determine trend direction based on slope and R-squared."""
        
        significance_threshold = self.alert_thresholds["trend_significance_threshold"]
        strength_threshold = self.alert_thresholds["trend_strength_threshold"]
        
        # Check if trend is significant
        if r_squared < strength_threshold:
            return PerformanceTrend.VOLATILE if r_squared > 0.1 else PerformanceTrend.STABLE
        
        # Determine direction based on slope
        if slope > 0.1:  # Positive trend
            return PerformanceTrend.IMPROVING
        elif slope < -0.1:  # Negative trend
            return PerformanceTrend.DECLINING
        else:
            return PerformanceTrend.STABLE
    
    def _forecast_performance(self, historical_values: List[float], periods_ahead: int) -> float:
        """Simple forecast of future performance."""
        
        if len(historical_values) < 3:
            return historical_values[-1] if historical_values else 0.0
        
        # Simple moving average with trend adjustment
        recent_values = historical_values[-5:]  # Last 5 periods
        moving_average = sum(recent_values) / len(recent_values)
        
        # Calculate simple trend
        if len(historical_values) >= 5:
            older_avg = sum(historical_values[-10:-5]) / 5 if len(historical_values) >= 10 else moving_average
            trend = (moving_average - older_avg) * 0.5  # Dampen trend
        else:
            trend = 0.0
        
        # Project forward
        forecast = moving_average + (trend * periods_ahead / 7)  # Scale by week
        return forecast
    
    async def _update_prometheus_metrics(self) -> None:
        """Update Prometheus metrics with current performance data."""
        
        for strategy_name, window in self.current_performance.items():
            # Update strategy performance metrics
            self.metrics_service.update_strategy_performance(
                strategy_name=strategy_name,
                score=max(0.0, min(1.0, window.avg_roi_per_bet / 10.0))  # Scale to 0-1
            )
            
            # Record recent activity
            self.metrics_service.record_games_processed(
                count=window.total_bets,
                date=window.start_date.strftime("%Y-%m-%d"),
                source=f"monitoring_{strategy_name}"
            )
        
        # Update active strategies count
        self.metrics_service.set_active_strategies_count(
            len(self.current_performance)
        )
    
    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an active alert."""
        
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.resolved_at = datetime.now()
            
            # Move to history
            self.alert_history.append(alert)
            del self.active_alerts[alert_id]
            
            self.logger.info(f"Resolved performance alert: {alert_id}")
            return True
        
        return False
    
    # Public API methods
    
    def get_current_performance(self, strategy_name: str) -> Optional[PerformanceWindow]:
        """Get current performance window for a strategy."""
        return self.current_performance.get(strategy_name)
    
    def get_performance_history(self, strategy_name: str, limit: int = 30) -> List[PerformanceWindow]:
        """Get performance history for a strategy."""
        
        if strategy_name not in self.performance_history:
            return []
        
        windows = list(self.performance_history[strategy_name])
        return windows[-limit:]
    
    def get_trend_analysis(self, strategy_name: str) -> Optional[TrendAnalysis]:
        """Get trend analysis for a strategy."""
        return self.trend_analysis.get(strategy_name)
    
    def get_active_alerts(self, strategy_name: Optional[str] = None) -> List[PerformanceAlert]:
        """Get active performance alerts."""
        
        alerts = list(self.active_alerts.values())
        
        if strategy_name:
            alerts = [alert for alert in alerts if alert.strategy_name == strategy_name]
        
        return alerts
    
    def get_alert_history(self, strategy_name: Optional[str] = None, limit: int = 50) -> List[PerformanceAlert]:
        """Get alert history."""
        
        history = self.alert_history
        
        if strategy_name:
            history = [alert for alert in history if alert.strategy_name == strategy_name]
        
        return history[-limit:]
    
    def get_monitoring_status(self) -> Dict[str, Any]:
        """Get comprehensive monitoring status."""
        
        # Calculate summary statistics
        total_strategies = len(self.performance_history)
        active_alerts_count = len(self.active_alerts)
        
        # Alert breakdown by level
        alert_breakdown = {}
        for alert in self.active_alerts.values():
            level = alert.alert_level.value
            alert_breakdown[level] = alert_breakdown.get(level, 0) + 1
        
        # Performance summary
        performance_summary = {}
        for strategy, window in self.current_performance.items():
            performance_summary[strategy] = {
                "roi": window.avg_roi_per_bet,
                "win_rate": window.win_rate,
                "total_bets": window.total_bets,
                "last_updated": self.last_update_time.get(strategy, datetime.now()).isoformat()
            }
        
        # Trend summary
        trend_summary = {}
        for strategy, trend in self.trend_analysis.items():
            trend_summary[strategy] = {
                "direction": trend.trend_direction.value,
                "strength": trend.trend_strength,
                "forecast_7_day": trend.predicted_7_day_roi
            }
        
        return {
            "monitoring_enabled": self.monitoring_enabled,
            "strategies_monitored": total_strategies,
            "active_alerts": active_alerts_count,
            "alert_breakdown": alert_breakdown,
            "monitoring_interval_minutes": self.monitoring_interval / 60,
            "performance_summary": performance_summary,
            "trend_summary": trend_summary,
            "last_monitoring_cycle": max(self.last_update_time.values()).isoformat() if self.last_update_time else None
        }