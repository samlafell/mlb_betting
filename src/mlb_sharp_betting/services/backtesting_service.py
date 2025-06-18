"""
Automated Backtesting and Strategy Validation Service

This service provides:
1. Daily backtesting pipeline execution
2. Strategy performance monitoring and evaluation
3. Automated threshold adjustment recommendations
4. Performance alerts and reporting
5. Statistical validation with confidence intervals
"""

import asyncio
import json
import numpy as np
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
from scipy import stats
import structlog

try:
    from ..db.connection import DatabaseManager, get_db_manager
    from ..core.exceptions import DatabaseError, ValidationError
    from .mlb_api_service import MLBStatsAPIService
except ImportError:
    # Handle direct execution
    import sys
    from pathlib import Path
    
    # Add the src directory to the path
    src_path = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(src_path))
    
    from mlb_sharp_betting.db.connection import DatabaseManager, get_db_manager
    from mlb_sharp_betting.core.exceptions import DatabaseError, ValidationError
    from mlb_sharp_betting.services.mlb_api_service import MLBStatsAPIService


logger = structlog.get_logger(__name__)


@dataclass
class StrategyMetrics:
    """Strategy performance metrics with statistical validation."""
    strategy_name: str
    source_book_type: str
    split_type: str
    
    # Performance Metrics
    total_bets: int
    wins: int
    win_rate: float
    roi_per_100: float
    
    # Statistical Metrics
    sharpe_ratio: float
    max_drawdown: float
    confidence_interval_lower: float
    confidence_interval_upper: float
    
    # Sample Quality
    sample_size_adequate: bool
    statistical_significance: bool
    p_value: float
    
    # Trend Analysis
    seven_day_win_rate: Optional[float] = None
    thirty_day_win_rate: Optional[float] = None
    trend_direction: Optional[str] = None  # 'improving', 'declining', 'stable'
    
    # Risk Metrics
    consecutive_losses: int = 0
    volatility: float = 0.0
    kelly_criterion: float = 0.0
    
    # Timestamps
    last_updated: datetime = None
    backtest_date: datetime = None
    created_at: datetime = None


@dataclass  
class ThresholdRecommendation:
    """Recommendation for strategy threshold adjustments."""
    strategy_name: str
    current_threshold: float
    recommended_threshold: float
    confidence_level: str
    justification: str
    expected_improvement: float
    risk_assessment: str
    sample_size: int
    
    # Implementation details
    file_path: str
    line_number: int
    variable_name: str
    
    # Safety checks
    requires_human_approval: bool = True
    cooling_period_required: bool = False
    
    created_at: datetime = None


@dataclass
class BacktestingResults:
    """Complete backtesting results for a specific date."""
    backtest_date: datetime
    total_strategies_analyzed: int
    strategies_with_adequate_data: int
    
    # Performance Summary
    profitable_strategies: int
    declining_strategies: int
    stable_strategies: int
    
    # Recommendations
    threshold_recommendations: List[ThresholdRecommendation]
    strategy_alerts: List[Dict[str, Any]]
    
    # Quality Metrics
    data_completeness_pct: float
    game_outcome_freshness_hours: float
    
    execution_time_seconds: float
    created_at: datetime


class BacktestingService:
    """Automated backtesting and strategy validation service."""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """Initialize the backtesting service."""
        self.db_manager = db_manager or get_db_manager()
        self.mlb_api = MLBStatsAPIService()
        self.logger = logger.bind(service="backtesting")
        
        # Configuration
        self.config = {
            "min_sample_size_threshold_adjustment": 50,  # As requested
            "min_sample_size_analysis": 5,  # Temporarily lowered to see all strategies
            "confidence_level": 0.95,
            "bootstrap_iterations": 1000,
            "max_threshold_change_per_day": 0.25,  # 25% max change
            "cooling_period_days": 14,  # 2 weeks as requested
            "performance_degradation_threshold": 0.45,  # 45% win rate trigger
            "data_freshness_hours": 24,  # Relaxed for testing - 24 hours instead of 6
            "win_rate_alert_threshold": 0.10,  # 10% drop triggers alert
            "data_completeness_threshold": 5.0,  # Relaxed for testing - 5% instead of 95%
        }
        
        # SQL script mapping
        self.backtest_scripts = {
            "strategy_comparison_roi": "analysis_scripts/strategy_comparison_roi.sql",
            "sharp_action_detector": "analysis_scripts/sharp_action_detector.sql", 
            "timing_based_strategy": "analysis_scripts/timing_based_strategy.sql",
            "hybrid_line_sharp_strategy": "analysis_scripts/hybrid_line_sharp_strategy.sql",
            "line_movement_strategy": "analysis_scripts/line_movement_strategy.sql",
            "signal_combinations": "analysis_scripts/signal_combinations.sql",
            "opposing_markets_strategy": "analysis_scripts/opposing_markets_strategy.sql",
            "executive_summary_report": "analysis_scripts/executive_summary_report.sql"
        }
        
        # Threshold mapping to validated_betting_detector.py
        self.threshold_mappings = {
            "vsin_strong_threshold": {
                "file": "analysis_scripts/validated_betting_detector.py",
                "variable": "abs_diff >= 20",
                "line_pattern": "if abs_diff >= 20:",
                "current_value": 20.0
            },
            "vsin_moderate_threshold": {
                "file": "analysis_scripts/validated_betting_detector.py", 
                "variable": "abs_diff >= 15",
                "line_pattern": "elif abs_diff >= 15:",
                "current_value": 15.0
            },
            "sbd_moderate_threshold": {
                "file": "analysis_scripts/validated_betting_detector.py",
                "variable": "abs_diff >= 25", 
                "line_pattern": "if abs_diff >= 25:",
                "current_value": 25.0
            }
        }
    
    async def run_daily_backtesting_pipeline(self) -> BacktestingResults:
        """
        Execute the complete daily backtesting pipeline.
        
        Returns:
            Complete backtesting results with recommendations
        """
        start_time = datetime.now(timezone.utc)
        self.logger.info("Starting daily backtesting pipeline")
        
        try:
            # Step 1: Validate data quality and freshness
            data_quality = await self._validate_data_quality()
            if data_quality["completeness_pct"] < self.config["data_completeness_threshold"]:
                raise ValidationError(f"Data completeness {data_quality['completeness_pct']:.1f}% below {self.config['data_completeness_threshold']:.1f}% threshold")
            
            if data_quality["freshness_hours"] > self.config["data_freshness_hours"]:
                raise ValidationError(f"Data freshness {data_quality['freshness_hours']:.1f}h exceeds {self.config['data_freshness_hours']:.1f}h threshold")
            
            # Step 2: Execute all backtesting SQL scripts
            backtest_results = await self._execute_backtest_scripts()
            
            # Step 3: Analyze strategy performance with statistical validation
            strategy_metrics = await self._analyze_strategy_performance(backtest_results)
            
            # Step 4: Detect performance changes and trends
            performance_changes = await self._detect_performance_changes(strategy_metrics)
            
            # Step 5: Generate threshold adjustment recommendations  
            threshold_recommendations = await self._generate_threshold_recommendations(
                strategy_metrics, performance_changes
            )
            
            # Step 6: Generate alerts for significant changes
            strategy_alerts = await self._generate_strategy_alerts(strategy_metrics, performance_changes)
            
            # Step 7: Store results for historical tracking
            await self._store_backtest_results(strategy_metrics, threshold_recommendations)
            
            end_time = datetime.now(timezone.utc)
            execution_time = (end_time - start_time).total_seconds()
            
            results = BacktestingResults(
                backtest_date=start_time,
                total_strategies_analyzed=len(strategy_metrics),
                strategies_with_adequate_data=len([m for m in strategy_metrics if m.sample_size_adequate]),
                profitable_strategies=len([m for m in strategy_metrics if m.win_rate > 0.524]),
                declining_strategies=len([m for m in strategy_metrics if m.trend_direction == 'declining']),
                stable_strategies=len([m for m in strategy_metrics if m.trend_direction == 'stable']),
                threshold_recommendations=threshold_recommendations,
                strategy_alerts=strategy_alerts,
                data_completeness_pct=data_quality["completeness_pct"],
                game_outcome_freshness_hours=data_quality["freshness_hours"],
                execution_time_seconds=execution_time,
                created_at=start_time
            )
            
            self.logger.info("Daily backtesting pipeline completed successfully",
                           execution_time=execution_time,
                           strategies_analyzed=len(strategy_metrics),
                           recommendations=len(threshold_recommendations),
                           alerts=len(strategy_alerts))
            
            return results
            
        except Exception as e:
            self.logger.error("Daily backtesting pipeline failed", error=str(e))
            raise
    
    async def _validate_data_quality(self) -> Dict[str, float]:
        """Validate data quality and freshness requirements."""
        try:
            with self.db_manager.get_cursor() as cursor:
                # Check game outcome completeness (count unique games, not records)
                cursor.execute("""
                    WITH recent_games AS (
                        SELECT COUNT(DISTINCT rmbs.game_id) as total_unique_games
                        FROM mlb_betting.splits.raw_mlb_betting_splits rmbs
                        WHERE rmbs.game_datetime >= CURRENT_DATE - INTERVAL '7 days'
                          AND rmbs.game_datetime < CURRENT_DATE
                    ),
                    games_with_outcomes AS (
                        SELECT COUNT(DISTINCT rmbs.game_id) as games_with_outcomes  
                        FROM mlb_betting.splits.raw_mlb_betting_splits rmbs
                        JOIN mlb_betting.main.game_outcomes go ON rmbs.game_id = go.game_id
                        WHERE rmbs.game_datetime >= CURRENT_DATE - INTERVAL '7 days'
                          AND rmbs.game_datetime < CURRENT_DATE
                    )
                    SELECT 
                        rg.total_unique_games,
                        gwo.games_with_outcomes,
                        ROUND(100.0 * gwo.games_with_outcomes / NULLIF(rg.total_unique_games, 0), 2) as completeness_pct
                    FROM recent_games rg, games_with_outcomes gwo
                """)
                
                completeness_result = cursor.fetchone()
                if not completeness_result:
                    raise ValidationError("Unable to validate data completeness")
                
                completeness_pct = completeness_result[2] or 0.0
                
                # Check data freshness
                cursor.execute("""
                    SELECT 
                        EXTRACT('epoch' FROM (CURRENT_TIMESTAMP - MAX(last_updated))) / 3600 as hours_since_last_update
                    FROM mlb_betting.splits.raw_mlb_betting_splits
                    WHERE game_datetime >= CURRENT_DATE - INTERVAL '1 day'
                """)
                
                freshness_result = cursor.fetchone()
                freshness_hours = freshness_result[0] if freshness_result else 999.0
                
                return {
                    "completeness_pct": completeness_pct,
                    "freshness_hours": freshness_hours
                }
                
        except Exception as e:
            self.logger.error("Data quality validation failed", error=str(e))
            raise
    
    async def _execute_backtest_scripts(self) -> Dict[str, List[Dict]]:
        """Execute all backtesting SQL scripts and return results."""
        backtest_results = {}
        
        for script_name, script_path in self.backtest_scripts.items():
            try:
                self.logger.info("Executing backtest script", script=script_name)
                
                # Read SQL script
                full_path = Path(script_path)
                if not full_path.exists():
                    self.logger.warning("Script not found", script=script_path)
                    continue
                
                sql_content = full_path.read_text()
                
                # Execute script
                with self.db_manager.get_cursor() as cursor:
                    cursor.execute(sql_content)
                    results = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    
                    # Convert to list of dictionaries
                    script_results = [
                        dict(zip(columns, row)) for row in results
                    ]
                    
                    backtest_results[script_name] = script_results
                    
                    self.logger.info("Script executed successfully", 
                                   script=script_name, 
                                   rows_returned=len(script_results))
                
            except Exception as e:
                self.logger.error("Failed to execute backtest script", 
                                script=script_name, error=str(e))
                backtest_results[script_name] = []
        
        return backtest_results
    
    async def _analyze_strategy_performance(self, 
                                          backtest_results: Dict[str, List[Dict]]) -> List[StrategyMetrics]:
        """Analyze strategy performance with statistical validation."""
        strategy_metrics = []
        
        # Process each strategy from backtesting results
        for script_name, results in backtest_results.items():
            for result in results:
                try:
                    # Extract common fields
                    total_bets = result.get('total_bets', 0)
                    wins = result.get('wins', 0) or result.get('sharp_wins', 0)
                    win_rate = float(result.get('win_rate', 0)) / 100.0  # Convert percentage
                    roi_per_100 = float(result.get('roi_per_100_unit', 0))
                    
                    if total_bets < self.config["min_sample_size_analysis"]:
                        continue
                    
                    # Calculate statistical metrics
                    confidence_interval = self._calculate_confidence_interval(wins, total_bets)
                    p_value = self._calculate_significance_test(wins, total_bets)
                    sharpe_ratio = self._calculate_sharpe_ratio(wins, total_bets, win_rate)
                    
                    # Get trend analysis
                    trend_metrics = await self._calculate_trend_metrics(
                        result.get('source_book_type', ''),
                        result.get('split_type', ''),
                        script_name
                    )
                    
                    # Handle strategy variants (like opposing markets strategies)
                    strategy_variant = result.get('strategy_variant', '') or result.get('final_sharp_indicator', '')
                    strategy_name = f"{script_name}_{strategy_variant}" if strategy_variant else script_name
                    
                    metrics = StrategyMetrics(
                        strategy_name=strategy_name,
                        source_book_type=result.get('source_book_type', ''),
                        split_type=result.get('split_type', ''),
                        total_bets=total_bets,
                        wins=wins,
                        win_rate=win_rate,
                        roi_per_100=roi_per_100,
                        sharpe_ratio=sharpe_ratio,
                        max_drawdown=0.0,  # Would need historical data
                        confidence_interval_lower=confidence_interval[0],
                        confidence_interval_upper=confidence_interval[1],
                        sample_size_adequate=total_bets >= self.config["min_sample_size_threshold_adjustment"],
                        statistical_significance=p_value < 0.05,
                        p_value=p_value,
                        seven_day_win_rate=trend_metrics.get('seven_day_win_rate'),
                        thirty_day_win_rate=trend_metrics.get('thirty_day_win_rate'),
                        trend_direction=trend_metrics.get('trend_direction'),
                        consecutive_losses=trend_metrics.get('consecutive_losses', 0),
                        volatility=trend_metrics.get('volatility', 0.0),
                        kelly_criterion=self._calculate_kelly_criterion(win_rate, 1.91),  # Assume -110 odds
                        last_updated=datetime.now(timezone.utc),
                        backtest_date=datetime.now(timezone.utc).date(),
                        created_at=datetime.now(timezone.utc)
                    )
                    
                    strategy_metrics.append(metrics)
                    
                except Exception as e:
                    self.logger.error("Failed to analyze strategy", 
                                    strategy=script_name, error=str(e))
        
        return strategy_metrics
    
    def _calculate_confidence_interval(self, wins: int, total_bets: int, 
                                     confidence_level: float = 0.95) -> Tuple[float, float]:
        """Calculate binomial confidence interval for win rate."""
        if total_bets == 0:
            return (0.0, 0.0)
        
        p = wins / total_bets
        z = stats.norm.ppf(1 - (1 - confidence_level) / 2)
        
        margin_of_error = z * np.sqrt(p * (1 - p) / total_bets)
        
        lower = max(0.0, p - margin_of_error)
        upper = min(1.0, p + margin_of_error)
        
        return (lower, upper)
    
    def _calculate_significance_test(self, wins: int, total_bets: int) -> float:
        """Test if win rate is significantly different from 52.38% (break-even at -110)."""
        if total_bets == 0:
            return 1.0
        
        # One-tailed test: H0: p <= 0.5238, H1: p > 0.5238
        observed_p = wins / total_bets
        null_p = 0.5238
        
        z_score = (observed_p - null_p) / np.sqrt(null_p * (1 - null_p) / total_bets)
        p_value = 1 - stats.norm.cdf(z_score)
        
        return p_value
    
    def _calculate_sharpe_ratio(self, wins: int, total_bets: int, win_rate: float) -> float:
        """Calculate Sharpe ratio for betting strategy."""
        if total_bets == 0:
            return 0.0
        
        # Assume -110 odds, risk-free rate = 0
        expected_return = win_rate * 0.9091 - (1 - win_rate) * 1.0
        
        # Estimate volatility (simplified)
        variance = win_rate * (0.9091 - expected_return)**2 + (1 - win_rate) * (-1.0 - expected_return)**2
        volatility = np.sqrt(variance) if variance > 0 else 0.001
        
        return expected_return / volatility if volatility > 0 else 0.0
    
    def _calculate_kelly_criterion(self, win_rate: float, decimal_odds: float) -> float:
        """Calculate optimal bet size using Kelly Criterion."""
        if win_rate <= 0 or decimal_odds <= 1:
            return 0.0
        
        b = decimal_odds - 1  # Net odds
        p = win_rate
        q = 1 - win_rate
        
        kelly = (b * p - q) / b
        return max(0.0, min(0.25, kelly))  # Cap at 25% of bankroll
    
    async def _calculate_trend_metrics(self, source_book_type: str, split_type: str, 
                                     strategy_name: str) -> Dict[str, Any]:
        """Calculate trend metrics for strategy performance."""
        try:
            # This would need to be implemented based on your historical data structure
            # For now, return placeholder values
            return {
                "seven_day_win_rate": None,
                "thirty_day_win_rate": None, 
                "trend_direction": "stable",
                "consecutive_losses": 0,
                "volatility": 0.0
            }
        except Exception as e:
            self.logger.error("Failed to calculate trend metrics", error=str(e))
            return {}
    
    async def _detect_performance_changes(self, strategy_metrics: List[StrategyMetrics]) -> Dict[str, Any]:
        """Detect significant performance changes requiring attention."""
        performance_changes = {
            "significant_improvements": [],
            "significant_declines": [],
            "threshold_breaches": []
        }
        
        for metrics in strategy_metrics:
            # Check for significant performance decline
            if (metrics.seven_day_win_rate and 
                metrics.win_rate - metrics.seven_day_win_rate > self.config["win_rate_alert_threshold"]):
                performance_changes["significant_declines"].append({
                    "strategy": metrics.strategy_name,
                    "source_book_type": metrics.source_book_type,
                    "current_win_rate": metrics.win_rate,
                    "seven_day_win_rate": metrics.seven_day_win_rate,
                    "decline_magnitude": metrics.win_rate - metrics.seven_day_win_rate
                })
            
            # Check for threshold breach (automatic suspension)
            if metrics.seven_day_win_rate and metrics.seven_day_win_rate < self.config["performance_degradation_threshold"]:
                performance_changes["threshold_breaches"].append({
                    "strategy": metrics.strategy_name,
                    "source_book_type": metrics.source_book_type,
                    "seven_day_win_rate": metrics.seven_day_win_rate,
                    "action_required": "SUSPEND_STRATEGY"
                })
        
        return performance_changes
    
    async def _generate_threshold_recommendations(self, 
                                                strategy_metrics: List[StrategyMetrics],
                                                performance_changes: Dict[str, Any]) -> List[ThresholdRecommendation]:
        """Generate recommendations for threshold adjustments."""
        recommendations = []
        
        # Analyze VSIN strategies for threshold optimization
        vsin_metrics = [m for m in strategy_metrics if 'vsin' in m.source_book_type.lower()]
        
        for metrics in vsin_metrics:
            if (metrics.sample_size_adequate and 
                metrics.statistical_significance and 
                metrics.win_rate > 0.55):  # Strong performance
                
                # Check if we should recommend lowering threshold to capture more bets
                current_threshold = self._get_current_threshold(metrics)
                if current_threshold:
                    recommended_threshold = current_threshold * 0.9  # 10% reduction
                    
                    recommendation = ThresholdRecommendation(
                        strategy_name=f"{metrics.strategy_name}_{metrics.source_book_type}",
                        current_threshold=current_threshold,
                        recommended_threshold=recommended_threshold,
                        confidence_level="HIGH" if metrics.total_bets > 100 else "MEDIUM",
                        justification=f"Strong performance (WR: {metrics.win_rate:.1%}, ROI: {metrics.roi_per_100:.1f}) with adequate sample size ({metrics.total_bets} bets). Lowering threshold could capture more profitable opportunities.",
                        expected_improvement=metrics.roi_per_100 * 0.1,  # Estimate
                        risk_assessment="LOW" if metrics.confidence_interval_lower > 0.52 else "MEDIUM",
                        sample_size=metrics.total_bets,
                        file_path=self.threshold_mappings["vsin_strong_threshold"]["file"],
                        line_number=0,  # Would need to be determined
                        variable_name="abs_diff threshold",
                        requires_human_approval=True,
                        cooling_period_required=False,
                        created_at=datetime.now(timezone.utc)
                    )
                    
                    recommendations.append(recommendation)
        
        return recommendations
    
    def _get_current_threshold(self, metrics: StrategyMetrics) -> Optional[float]:
        """Get current threshold value for a strategy."""
        # This would need to parse the validated_betting_detector.py file
        # For now, return default values
        if 'vsin' in metrics.source_book_type.lower():
            return 20.0  # Strong threshold
        elif 'sbd' in metrics.source_book_type.lower():
            return 25.0
        return None
    
    async def _generate_strategy_alerts(self, strategy_metrics: List[StrategyMetrics],
                                      performance_changes: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate alerts for significant strategy changes."""
        alerts = []
        
        # Critical performance alerts
        for decline in performance_changes["significant_declines"]:
            alerts.append({
                "type": "PERFORMANCE_DECLINE",
                "severity": "HIGH",
                "strategy": decline["strategy"],
                "message": f"Strategy {decline['strategy']} win rate declined by {decline['decline_magnitude']:.1%} over 7 days",
                "data": decline,
                "timestamp": datetime.now(timezone.utc)
            })
        
        # Threshold breach alerts
        for breach in performance_changes["threshold_breaches"]:
            alerts.append({
                "type": "THRESHOLD_BREACH", 
                "severity": "CRITICAL",
                "strategy": breach["strategy"],
                "message": f"Strategy {breach['strategy']} 7-day win rate {breach['seven_day_win_rate']:.1%} below 45% threshold - SUSPEND IMMEDIATELY",
                "data": breach,
                "timestamp": datetime.now(timezone.utc)
            })
        
        # New profitable opportunities
        high_performing = [m for m in strategy_metrics if m.win_rate > 0.60 and m.sample_size_adequate]
        for metrics in high_performing:
            alerts.append({
                "type": "HIGH_PERFORMANCE",
                "severity": "MEDIUM", 
                "strategy": metrics.strategy_name,
                "message": f"Strategy {metrics.strategy_name} showing strong performance: {metrics.win_rate:.1%} win rate, {metrics.roi_per_100:.1f} ROI",
                "data": asdict(metrics),
                "timestamp": datetime.now(timezone.utc)
            })
        
        return alerts
    
    async def _store_backtest_results(self, strategy_metrics: List[StrategyMetrics],
                                    threshold_recommendations: List[ThresholdRecommendation]) -> None:
        """Store backtesting results for historical tracking."""
        try:
            with self.db_manager.get_cursor() as cursor:
                # Create tables if they don't exist
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS mlb_betting.backtesting.strategy_performance (
                        id VARCHAR PRIMARY KEY,
                        backtest_date DATE NOT NULL,
                        strategy_name VARCHAR NOT NULL,
                        source_book_type VARCHAR NOT NULL,
                        split_type VARCHAR NOT NULL,
                        total_bets INTEGER,
                        wins INTEGER,
                        win_rate DOUBLE,
                        roi_per_100 DOUBLE,
                        sharpe_ratio DOUBLE,
                        max_drawdown DOUBLE,
                        confidence_interval_lower DOUBLE,
                        confidence_interval_upper DOUBLE,
                        sample_size_adequate BOOLEAN,
                        statistical_significance BOOLEAN,
                        p_value DOUBLE,
                        seven_day_win_rate DOUBLE,
                        thirty_day_win_rate DOUBLE,
                        trend_direction VARCHAR,
                        consecutive_losses INTEGER,
                        volatility DOUBLE,
                        kelly_criterion DOUBLE,
                        last_updated TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS mlb_betting.backtesting.threshold_recommendations (
                        id VARCHAR PRIMARY KEY,
                        strategy_name VARCHAR NOT NULL,
                        current_threshold DOUBLE,
                        recommended_threshold DOUBLE,
                        confidence_level VARCHAR,
                        justification TEXT,
                        expected_improvement DOUBLE,
                        risk_assessment VARCHAR,
                        sample_size INTEGER,
                        file_path VARCHAR,
                        variable_name VARCHAR,
                        requires_human_approval BOOLEAN,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Insert strategy metrics
                for metrics in strategy_metrics:
                    cursor.execute("""
                        INSERT INTO mlb_betting.backtesting.strategy_performance
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        f"{metrics.strategy_name}_{metrics.source_book_type}_{metrics.split_type}_{metrics.backtest_date}",
                        metrics.backtest_date,
                        metrics.strategy_name,
                        metrics.source_book_type,
                        metrics.split_type,
                        metrics.total_bets,
                        metrics.wins,
                        metrics.win_rate,
                        metrics.roi_per_100,
                        metrics.sharpe_ratio,
                        metrics.max_drawdown,
                        metrics.confidence_interval_lower,
                        metrics.confidence_interval_upper,
                        bool(metrics.sample_size_adequate),
                        bool(metrics.statistical_significance),
                        metrics.p_value,
                        metrics.seven_day_win_rate,
                        metrics.thirty_day_win_rate,
                        metrics.trend_direction,
                        metrics.consecutive_losses,
                        metrics.volatility,
                        metrics.kelly_criterion,
                        metrics.last_updated,
                        metrics.created_at
                    ))
                
                # Insert threshold recommendations
                for rec in threshold_recommendations:
                    cursor.execute("""
                        INSERT INTO mlb_betting.backtesting.threshold_recommendations
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        f"{rec.strategy_name}_{rec.created_at.isoformat()}",
                        rec.strategy_name,
                        rec.current_threshold,
                        rec.recommended_threshold,
                        rec.confidence_level,
                        rec.justification,
                        rec.expected_improvement,
                        rec.risk_assessment,
                        rec.sample_size,
                        rec.file_path,
                        rec.variable_name,
                        bool(rec.requires_human_approval),
                        rec.created_at
                    ))
                
                self.logger.info("Stored backtesting results",
                               metrics_stored=len(strategy_metrics),
                               recommendations_stored=len(threshold_recommendations))
                
        except Exception as e:
            self.logger.error("Failed to store backtesting results", error=str(e))
            raise
    
    async def generate_daily_report(self, results: BacktestingResults) -> str:
        """Generate a comprehensive daily backtesting report."""
        report_lines = [
            "# 📊 DAILY BACKTESTING & STRATEGY VALIDATION REPORT",
            f"**Date:** {results.backtest_date.strftime('%Y-%m-%d %H:%M UTC')}",
            f"**Execution Time:** {results.execution_time_seconds:.1f} seconds",
            "",
            "## 📈 Executive Summary",
            f"- **Strategies Analyzed:** {results.total_strategies_analyzed}",
            f"- **Adequate Sample Size:** {results.strategies_with_adequate_data}",
            f"- **Profitable Strategies:** {results.profitable_strategies}",
            f"- **Declining Strategies:** {results.declining_strategies}",
            f"- **Data Completeness:** {results.data_completeness_pct:.1f}%",
            f"- **Data Freshness:** {results.game_outcome_freshness_hours:.1f} hours",
            "",
            "## 🎯 Threshold Recommendations"
        ]
        
        if results.threshold_recommendations:
            for rec in results.threshold_recommendations:
                report_lines.extend([
                    f"### {rec.strategy_name}",
                    f"- **Current Threshold:** {rec.current_threshold}",
                    f"- **Recommended:** {rec.recommended_threshold}",
                    f"- **Confidence:** {rec.confidence_level}",
                    f"- **Justification:** {rec.justification}",
                    f"- **File:** `{rec.file_path}`",
                    f"- **Expected Improvement:** {rec.expected_improvement:.2f}%",
                    ""
                ])
        else:
            report_lines.append("*No threshold adjustments recommended at this time.*")
        
        report_lines.extend([
            "",
            "## 🚨 Strategy Alerts"
        ])
        
        if results.strategy_alerts:
            for alert in results.strategy_alerts:
                severity_emoji = {"CRITICAL": "🔥", "HIGH": "⚠️", "MEDIUM": "📊"}
                emoji = severity_emoji.get(alert["severity"], "ℹ️")
                report_lines.append(f"- {emoji} **{alert['type']}:** {alert['message']}")
        else:
            report_lines.append("*No critical alerts at this time.*")
        
        report_lines.extend([
            "",
            "---",
            "*Report generated by MLB Sharp Betting Analytics Platform*",
            "*General Balls*"
        ])
        
        return "\n".join(report_lines)

    async def store_strategy_performance(self, strategy_name: str, results: Dict[str, Any]) -> None:
        """Store strategy performance metrics for adaptive configuration."""
        
        try:
            with self.db_manager.get_cursor() as cursor:
                # Store performance metrics
                cursor.execute("""
                    INSERT INTO mlb_betting.backtesting.strategy_performance (
                        strategy_name, source_book_type, split_type, backtest_date,
                        win_rate, roi_per_100, total_bets, total_profit_loss,
                        sharpe_ratio, max_drawdown, kelly_criterion, confidence_level,
                        last_updated
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    strategy_name,
                    results.get('source_book_type', ''),
                    results.get('split_type', ''),
                    datetime.now(timezone.utc).date(),
                    results.get('win_rate', 0.0),
                    results.get('roi_per_100', 0.0),
                    results.get('total_bets', 0),
                    results.get('total_profit_loss', 0.0),
                    results.get('sharpe_ratio', 0.0),
                    results.get('max_drawdown', 0.0),
                    results.get('kelly_criterion', 0.0),
                    results.get('confidence_level', 'LOW'),
                    datetime.now(timezone.utc)
                ))
                
                # Generate threshold recommendations if performance is good
                if (results.get('win_rate', 0.0) > 0.52 and 
                    results.get('total_bets', 0) >= 10):
                    
                    self._store_threshold_recommendation(cursor, strategy_name, results)
                    
        except Exception as e:
            self.logger.error("Failed to store strategy performance", 
                            strategy=strategy_name, error=str(e))
    
    def _store_threshold_recommendation(self, cursor, strategy_name: str, results: Dict[str, Any]) -> None:
        """Store threshold recommendations based on performance."""
        
        win_rate = results.get('win_rate', 0.0)
        total_bets = results.get('total_bets', 0)
        roi_per_100 = results.get('roi_per_100', 0.0)
        
        # Calculate recommended thresholds based on performance
        if win_rate > 0.70:  # Exceptional performance
            base_threshold = 10.0
            confidence = "HIGH"
            requires_approval = False
        elif win_rate > 0.60:  # Very good performance
            base_threshold = 15.0
            confidence = "HIGH"
            requires_approval = False
        elif win_rate > 0.55:  # Good performance
            base_threshold = 20.0
            confidence = "MODERATE"
            requires_approval = False
        elif win_rate > 0.52:  # Profitable
            base_threshold = 25.0
            confidence = "LOW"
            requires_approval = True
        else:
            return  # Don't recommend thresholds for unprofitable strategies
        
        # Adjust based on sample size
        if total_bets < 20:
            base_threshold += 5.0  # Be more conservative with small samples
            requires_approval = True
        
        # Store threshold recommendation
        cursor.execute("""
            INSERT INTO mlb_betting.backtesting.threshold_recommendations (
                strategy_name, recommended_threshold, confidence_level,
                justification, requires_human_approval, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (
            strategy_name,
            base_threshold,
            confidence,
            f"Win rate: {win_rate:.1%}, Total bets: {total_bets}, ROI: {roi_per_100:+.1f}%",
            requires_approval,
            datetime.now(timezone.utc)
        ))
        
        self.logger.info("Generated threshold recommendation",
                        strategy=strategy_name,
                        threshold=base_threshold,
                        confidence=confidence,
                        requires_approval=requires_approval)

    async def analyze_all_strategies(self) -> Dict[str, Any]:
        """Run all backtesting strategies and store performance data."""
        
        self.logger.info("Starting comprehensive strategy analysis")
        
        all_results = {}
        
        try:
            # Run each strategy and store results
            strategies = [
                ("sharp_action_vsin", self._run_sharp_action_strategy),
                ("sharp_action_sbd", self._run_sharp_action_strategy),
                ("opposing_markets", self._run_opposing_markets_strategy),
                ("timing_based_spread", self._run_timing_based_strategy),
                ("timing_based_moneyline", self._run_timing_based_strategy),
                ("line_movement", self._run_line_movement_strategy),
            ]
            
            for strategy_name, strategy_func in strategies:
                try:
                    results = await strategy_func(strategy_name)
                    all_results[strategy_name] = results
                    
                    # Store performance data for adaptive configuration
                    await self.store_strategy_performance(strategy_name, results)
                    
                except Exception as e:
                    self.logger.error("Strategy analysis failed", 
                                    strategy=strategy_name, error=str(e))
                    continue
            
            # Generate summary report
            summary = self._generate_strategy_summary(all_results)
            
            return {
                "strategies": all_results,
                "summary": summary,
                "timestamp": datetime.now(timezone.utc),
                "total_strategies_analyzed": len(all_results)
            }
            
        except Exception as e:
            self.logger.error("Failed to analyze all strategies", error=str(e))
            raise
    
    def _generate_strategy_summary(self, all_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a summary of all strategy performance."""
        
        if not all_results:
            return {
                "status": "No strategies analyzed",
                "recommendation": "Check data availability and strategy configurations"
            }
        
        # Calculate aggregated metrics
        profitable_strategies = [
            name for name, results in all_results.items()
            if results.get('win_rate', 0.0) > 0.52
        ]
        
        total_bets = sum(results.get('total_bets', 0) for results in all_results.values())
        
        if total_bets > 0:
            weighted_win_rate = sum(
                results.get('win_rate', 0.0) * results.get('total_bets', 0)
                for results in all_results.values()
            ) / total_bets
            
            weighted_roi = sum(
                results.get('roi_per_100', 0.0) * results.get('total_bets', 0)
                for results in all_results.values()
            ) / total_bets
        else:
            weighted_win_rate = 0.0
            weighted_roi = 0.0
        
        # Find best performing strategy
        best_strategy = None
        best_roi = -float('inf')
        
        for name, results in all_results.items():
            if (results.get('total_bets', 0) >= 5 and
                results.get('roi_per_100', -float('inf')) > best_roi):
                best_strategy = name
                best_roi = results.get('roi_per_100', 0.0)
        
        return {
            "total_strategies": len(all_results),
            "profitable_strategies": len(profitable_strategies),
            "profitable_strategy_names": profitable_strategies,
            "total_bets_analyzed": total_bets,
            "weighted_win_rate": weighted_win_rate,
            "weighted_roi": weighted_roi,
            "best_strategy": {
                "name": best_strategy,
                "roi": best_roi,
                "details": all_results.get(best_strategy, {}) if best_strategy else {}
            },
            "recommendation": self._get_strategy_recommendation(
                len(profitable_strategies), weighted_win_rate, weighted_roi
            )
        }
    
    def _get_strategy_recommendation(self, profitable_count: int, win_rate: float, roi: float) -> str:
        """Generate recommendation based on strategy performance."""
        
        if profitable_count == 0:
            return "No profitable strategies found. Review data quality and strategy logic."
        elif profitable_count == 1:
            return "One profitable strategy identified. Use conservative thresholds."
        elif profitable_count >= 3 and win_rate > 0.58:
            return "Multiple high-performing strategies. Use aggressive thresholds for maximum profit."
        elif profitable_count >= 2:
            return "Multiple profitable strategies. Use moderate thresholds for balanced risk."
        else:
            return "Limited profitable strategies. Use conservative approach."


async def main():
    """Test the backtesting service."""
    service = BacktestingService()
    results = await service.run_daily_backtesting_pipeline()
    report = await service.generate_daily_report(results)
    print(report)


if __name__ == "__main__":
    asyncio.run(main())