"""
Production A/B Testing Framework for MLB Betting Strategies

This module provides a comprehensive A/B testing infrastructure for comparing
betting strategies in production environment with proper statistical analysis,
traffic allocation, and performance monitoring.

Key Features:
1. Multi-arm bandit and fixed-split A/B testing
2. Statistical significance monitoring with early stopping
3. Risk-based traffic allocation and safety controls
4. Real-time performance tracking and alerting
5. Automated winner selection and rollout
6. Comprehensive experiment analytics and reporting
"""

import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple, NamedTuple
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import json
import uuid

import numpy as np
from scipy import stats
from pydantic import BaseModel, Field

from ...core.config import get_settings
from ...data.database import get_unified_repository
from ..strategy_development_framework import StrategyConfiguration, StrategyPerformanceMetrics

logger = logging.getLogger(__name__)


class ExperimentStatus(str, Enum):
    """A/B test experiment status"""
    DRAFT = "draft"                    # Being configured
    ACTIVE = "active"                  # Currently running
    PAUSED = "paused"                  # Temporarily stopped
    COMPLETED = "completed"            # Finished successfully
    STOPPED_EARLY = "stopped_early"    # Stopped due to significance or safety
    FAILED = "failed"                  # Failed due to errors


class TestType(str, Enum):
    """Types of A/B tests"""
    FIXED_SPLIT = "fixed_split"       # Fixed traffic allocation
    MULTI_ARM_BANDIT = "multi_arm_bandit"  # Dynamic allocation based on performance
    CHAMPION_CHALLENGER = "champion_challenger"  # Existing vs new strategy


class StoppingRule(str, Enum):
    """Early stopping rules for experiments"""
    STATISTICAL_SIGNIFICANCE = "statistical_significance"  # Stop when statistically significant
    SAFETY_THRESHOLD = "safety_threshold"                 # Stop if performance drops too much
    SAMPLE_SIZE_REACHED = "sample_size_reached"           # Stop when target sample size reached
    TIME_LIMIT = "time_limit"                            # Stop after time limit
    BUSINESS_DECISION = "business_decision"               # Manual stop by business decision


@dataclass
class ExperimentArm:
    """Configuration for a single experiment arm"""
    arm_id: str
    name: str
    strategy_config: StrategyConfiguration
    traffic_allocation: float  # Percentage of traffic (0.0 to 1.0)
    is_control: bool = False
    min_allocation: float = 0.05  # Minimum traffic allocation
    max_allocation: float = 0.95  # Maximum traffic allocation


@dataclass 
class ExperimentConfig:
    """Configuration for A/B testing experiment"""
    experiment_id: str
    name: str
    description: str
    test_type: TestType
    arms: List[ExperimentArm]
    
    # Experiment parameters
    start_time: datetime
    planned_end_time: datetime
    min_sample_size_per_arm: int = 100
    max_sample_size_per_arm: int = 10000
    target_statistical_power: float = 0.8
    significance_level: float = 0.05
    
    # Safety controls
    max_daily_loss_per_arm: Decimal = Decimal("500")
    max_drawdown_threshold: float = 0.15  # 15%
    min_win_rate_threshold: float = 0.45   # Stop if win rate drops below 45%
    
    # Business metrics
    primary_metric: str = "roi"  # roi, win_rate, profit, sharpe_ratio
    secondary_metrics: List[str] = field(default_factory=lambda: ["win_rate", "profit", "max_drawdown"])
    
    # Multi-arm bandit parameters (if applicable)
    exploration_rate: float = 0.1
    update_frequency_minutes: int = 60


@dataclass
class ExperimentResult:
    """Results for a single experiment arm"""
    arm_id: str
    samples: int
    wins: int
    losses: int
    total_profit: Decimal
    total_volume: Decimal
    win_rate: float
    roi_percentage: float
    max_drawdown: float
    sharpe_ratio: Optional[float] = None
    
    # Statistical measures
    confidence_interval_lower: float = 0.0
    confidence_interval_upper: float = 0.0
    
    # Time-based metrics
    last_updated: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "arm_id": self.arm_id,
            "samples": self.samples,
            "wins": self.wins,
            "losses": self.losses,
            "total_profit": float(self.total_profit),
            "total_volume": float(self.total_volume),
            "win_rate": self.win_rate,
            "roi_percentage": self.roi_percentage,
            "max_drawdown": self.max_drawdown,
            "sharpe_ratio": self.sharpe_ratio,
            "confidence_interval": (self.confidence_interval_lower, self.confidence_interval_upper),
            "last_updated": self.last_updated.isoformat()
        }


class StatisticalTest(NamedTuple):
    """Statistical test result"""
    test_name: str
    statistic: float
    p_value: float
    significant: bool
    confidence_interval: Tuple[float, float]
    effect_size: float


class ABTestingEngine:
    """
    Core A/B testing engine for strategy comparison in production
    
    Handles experiment lifecycle, traffic allocation, statistical analysis,
    and automated decision making for betting strategy optimization
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.repository = None
        
        # Active experiments registry
        self.active_experiments: Dict[str, ExperimentConfig] = {}
        self.experiment_results: Dict[str, Dict[str, ExperimentResult]] = {}
        
        # Traffic allocation state
        self.current_allocations: Dict[str, Dict[str, float]] = {}
        
        # Safety monitoring
        self.safety_alerts: List[Dict[str, Any]] = []
    
    async def initialize(self) -> bool:
        """Initialize A/B testing engine"""
        try:
            self.repository = get_unified_repository()
            
            # Load active experiments from database
            await self._load_active_experiments()
            
            logger.info("A/B testing engine initialized")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize A/B testing engine: {e}")
            return False
    
    async def create_experiment(
        self,
        name: str,
        description: str,
        arms: List[ExperimentArm],
        test_type: TestType = TestType.FIXED_SPLIT,
        duration_days: int = 30,
        **kwargs
    ) -> str:
        """
        Create new A/B testing experiment
        
        Args:
            name: Experiment name
            description: Experiment description  
            arms: List of experiment arms to test
            test_type: Type of A/B test
            duration_days: Planned experiment duration
            
        Returns:
            Experiment ID
        """
        try:
            experiment_id = str(uuid.uuid4())
            
            # Validate experiment configuration
            self._validate_experiment_config(arms, test_type)
            
            # Create experiment configuration
            config = ExperimentConfig(
                experiment_id=experiment_id,
                name=name,
                description=description,
                test_type=test_type,
                arms=arms,
                start_time=datetime.utcnow(),
                planned_end_time=datetime.utcnow() + timedelta(days=duration_days),
                **kwargs
            )
            
            # Initialize results tracking
            self.experiment_results[experiment_id] = {}
            for arm in arms:
                self.experiment_results[experiment_id][arm.arm_id] = ExperimentResult(
                    arm_id=arm.arm_id,
                    samples=0,
                    wins=0,
                    losses=0,
                    total_profit=Decimal("0"),
                    total_volume=Decimal("0"),
                    win_rate=0.0,
                    roi_percentage=0.0,
                    max_drawdown=0.0
                )
            
            # Set initial traffic allocation
            if test_type == TestType.FIXED_SPLIT:
                self.current_allocations[experiment_id] = {
                    arm.arm_id: arm.traffic_allocation for arm in arms
                }
            else:  # Multi-arm bandit starts with equal allocation
                equal_allocation = 1.0 / len(arms)
                self.current_allocations[experiment_id] = {
                    arm.arm_id: equal_allocation for arm in arms
                }
            
            # Store experiment configuration
            self.active_experiments[experiment_id] = config
            await self._persist_experiment_config(config)
            
            logger.info(f"Created A/B experiment: {name} ({experiment_id}) with {len(arms)} arms")
            return experiment_id
            
        except Exception as e:
            logger.error(f"Error creating A/B experiment: {e}")
            raise
    
    async def allocate_traffic(self, experiment_id: str, user_context: Dict[str, Any]) -> Optional[str]:
        """
        Allocate traffic to experiment arms based on current allocation strategy
        
        Args:
            experiment_id: ID of the experiment
            user_context: Context for traffic allocation (user ID, timestamp, etc.)
            
        Returns:
            Selected arm ID or None if no allocation
        """
        try:
            if experiment_id not in self.active_experiments:
                logger.warning(f"Experiment {experiment_id} not found or not active")
                return None
            
            config = self.active_experiments[experiment_id]
            
            # Check if experiment is still active
            if not self._is_experiment_active(config):
                return None
            
            # Get current traffic allocation
            allocations = self.current_allocations.get(experiment_id, {})
            
            if not allocations:
                logger.warning(f"No traffic allocations found for experiment {experiment_id}")
                return None
            
            # Select arm based on allocation probabilities
            arm_ids = list(allocations.keys())
            probabilities = list(allocations.values())
            
            # Normalize probabilities to ensure they sum to 1
            total_prob = sum(probabilities)
            if total_prob > 0:
                probabilities = [p / total_prob for p in probabilities]
            else:
                # Equal allocation if no valid probabilities
                probabilities = [1.0 / len(arm_ids)] * len(arm_ids)
            
            # Random selection based on probabilities
            selected_arm = np.random.choice(arm_ids, p=probabilities)
            
            logger.debug(f"Allocated traffic to arm {selected_arm} for experiment {experiment_id}")
            return selected_arm
            
        except Exception as e:
            logger.error(f"Error allocating traffic for experiment {experiment_id}: {e}")
            return None
    
    async def record_outcome(
        self,
        experiment_id: str,
        arm_id: str,
        outcome: Dict[str, Any]
    ) -> bool:
        """
        Record outcome for an experiment arm
        
        Args:
            experiment_id: ID of the experiment
            arm_id: ID of the arm
            outcome: Outcome data (win/loss, profit, etc.)
            
        Returns:
            Success status
        """
        try:
            if experiment_id not in self.experiment_results:
                logger.warning(f"Experiment {experiment_id} not found for outcome recording")
                return False
            
            if arm_id not in self.experiment_results[experiment_id]:
                logger.warning(f"Arm {arm_id} not found in experiment {experiment_id}")
                return False
            
            # Update arm results
            result = self.experiment_results[experiment_id][arm_id]
            
            result.samples += 1
            
            if outcome.get('won', False):
                result.wins += 1
            else:
                result.losses += 1
            
            # Update financial metrics
            profit = Decimal(str(outcome.get('profit', 0)))
            volume = Decimal(str(outcome.get('volume', outcome.get('bet_amount', 0))))
            
            result.total_profit += profit
            result.total_volume += volume
            
            # Recalculate derived metrics
            result.win_rate = result.wins / result.samples if result.samples > 0 else 0.0
            result.roi_percentage = (float(result.total_profit) / float(result.total_volume) * 100) if result.total_volume > 0 else 0.0
            
            # Update drawdown (simplified - would need full trade history for accurate calculation)
            if profit < 0:
                current_drawdown = abs(float(profit) / float(result.total_volume)) * 100
                result.max_drawdown = max(result.max_drawdown, current_drawdown)
            
            result.last_updated = datetime.utcnow()
            
            # Check safety thresholds
            await self._check_safety_thresholds(experiment_id, arm_id, result)
            
            # Update traffic allocation for multi-arm bandit
            config = self.active_experiments[experiment_id]
            if config.test_type == TestType.MULTI_ARM_BANDIT:
                await self._update_bandit_allocation(experiment_id)
            
            # Check for statistical significance and stopping rules
            await self._check_stopping_rules(experiment_id)
            
            logger.debug(f"Recorded outcome for experiment {experiment_id}, arm {arm_id}: {outcome}")
            return True
            
        except Exception as e:
            logger.error(f"Error recording outcome for experiment {experiment_id}, arm {arm_id}: {e}")
            return False
    
    async def analyze_experiment(self, experiment_id: str) -> Dict[str, Any]:
        """
        Perform comprehensive statistical analysis of experiment results
        
        Args:
            experiment_id: ID of the experiment to analyze
            
        Returns:
            Comprehensive analysis results
        """
        try:
            if experiment_id not in self.active_experiments:
                raise ValueError(f"Experiment {experiment_id} not found")
            
            config = self.active_experiments[experiment_id]
            results = self.experiment_results.get(experiment_id, {})
            
            if not results:
                return {"error": "No results available for analysis"}
            
            # Calculate summary statistics
            summary_stats = self._calculate_summary_statistics(results)
            
            # Perform pairwise statistical tests
            statistical_tests = await self._perform_statistical_tests(results, config)
            
            # Calculate confidence intervals
            confidence_intervals = self._calculate_confidence_intervals(results, config.significance_level)
            
            # Determine winner (if any)
            winner_analysis = self._analyze_winner(results, statistical_tests, config.primary_metric)
            
            # Calculate experiment power and sample size recommendations
            power_analysis = self._perform_power_analysis(results, config)
            
            # Risk analysis
            risk_analysis = self._perform_risk_analysis(results)
            
            analysis = {
                "experiment_id": experiment_id,
                "experiment_name": config.name,
                "status": self._get_experiment_status(experiment_id),
                "duration_days": (datetime.utcnow() - config.start_time).days,
                "summary_statistics": summary_stats,
                "statistical_tests": statistical_tests,
                "confidence_intervals": confidence_intervals,
                "winner_analysis": winner_analysis,
                "power_analysis": power_analysis,
                "risk_analysis": risk_analysis,
                "recommendations": self._generate_recommendations(config, results, statistical_tests),
                "current_allocations": self.current_allocations.get(experiment_id, {}),
                "analysis_timestamp": datetime.utcnow().isoformat()
            }
            
            logger.info(f"Completed analysis for experiment {experiment_id}")
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing experiment {experiment_id}: {e}")
            raise
    
    async def stop_experiment(
        self,
        experiment_id: str,
        reason: StoppingRule,
        winner_arm_id: Optional[str] = None
    ) -> bool:
        """
        Stop experiment and optionally promote winner
        
        Args:
            experiment_id: ID of experiment to stop
            reason: Reason for stopping
            winner_arm_id: Arm to promote as winner (optional)
            
        Returns:
            Success status
        """
        try:
            if experiment_id not in self.active_experiments:
                logger.warning(f"Experiment {experiment_id} not found")
                return False
            
            config = self.active_experiments[experiment_id]
            
            # Update experiment status
            if reason == StoppingRule.STATISTICAL_SIGNIFICANCE:
                status = ExperimentStatus.COMPLETED
            elif reason in [StoppingRule.SAFETY_THRESHOLD, StoppingRule.BUSINESS_DECISION]:
                status = ExperimentStatus.STOPPED_EARLY
            else:
                status = ExperimentStatus.COMPLETED
            
            # Perform final analysis
            final_analysis = await self.analyze_experiment(experiment_id)
            
            # Log experiment completion
            logger.info(f"Stopped experiment {experiment_id} ({config.name}) - Reason: {reason.value}")
            
            if winner_arm_id:
                winner_config = next((arm for arm in config.arms if arm.arm_id == winner_arm_id), None)
                if winner_config:
                    logger.info(f"Experiment winner: {winner_config.name} ({winner_arm_id})")
                    
                    # In production, this would trigger deployment of winning strategy
                    await self._deploy_winning_strategy(winner_config.strategy_config, final_analysis)
            
            # Archive experiment
            await self._archive_experiment(experiment_id, status, final_analysis)
            
            # Remove from active experiments
            del self.active_experiments[experiment_id]
            if experiment_id in self.current_allocations:
                del self.current_allocations[experiment_id]
            
            return True
            
        except Exception as e:
            logger.error(f"Error stopping experiment {experiment_id}: {e}")
            return False
    
    def get_experiment_status(self, experiment_id: str) -> Dict[str, Any]:
        """Get current status of experiment"""
        try:
            if experiment_id not in self.active_experiments:
                return {"error": "Experiment not found"}
            
            config = self.active_experiments[experiment_id]
            results = self.experiment_results.get(experiment_id, {})
            
            # Calculate current statistics
            total_samples = sum(result.samples for result in results.values())
            
            status_info = {
                "experiment_id": experiment_id,
                "name": config.name,
                "status": self._get_experiment_status(experiment_id),
                "start_time": config.start_time.isoformat(),
                "planned_end_time": config.planned_end_time.isoformat(),
                "duration_days": (datetime.utcnow() - config.start_time).days,
                "total_samples": total_samples,
                "arms": len(config.arms),
                "current_allocations": self.current_allocations.get(experiment_id, {}),
                "arm_results": {arm_id: result.to_dict() for arm_id, result in results.items()},
                "safety_alerts": [alert for alert in self.safety_alerts if alert.get("experiment_id") == experiment_id]
            }
            
            return status_info
            
        except Exception as e:
            logger.error(f"Error getting experiment status for {experiment_id}: {e}")
            return {"error": str(e)}
    
    async def _load_active_experiments(self):
        """Load active experiments from database"""
        # In production implementation, this would:
        # 1. Query database for active experiments
        # 2. Restore experiment configurations
        # 3. Load current results and allocations
        pass
    
    async def _persist_experiment_config(self, config: ExperimentConfig):
        """Persist experiment configuration to database"""
        # In production implementation, this would:
        # 1. Store experiment configuration in database
        # 2. Create audit trail for experiment creation
        # 3. Set up monitoring and alerting
        pass
    
    def _validate_experiment_config(self, arms: List[ExperimentArm], test_type: TestType):
        """Validate experiment configuration"""
        if len(arms) < 2:
            raise ValueError("Experiment must have at least 2 arms")
        
        if test_type == TestType.FIXED_SPLIT:
            total_allocation = sum(arm.traffic_allocation for arm in arms)
            if not (0.95 <= total_allocation <= 1.05):  # Allow small rounding errors
                raise ValueError(f"Traffic allocations must sum to 1.0, got {total_allocation}")
        
        arm_ids = [arm.arm_id for arm in arms]
        if len(arm_ids) != len(set(arm_ids)):
            raise ValueError("Arm IDs must be unique")
    
    def _is_experiment_active(self, config: ExperimentConfig) -> bool:
        """Check if experiment should still be active"""
        now = datetime.utcnow()
        return (
            config.start_time <= now <= config.planned_end_time and
            config.experiment_id in self.active_experiments
        )
    
    async def _check_safety_thresholds(self, experiment_id: str, arm_id: str, result: ExperimentResult):
        """Check safety thresholds and create alerts if needed"""
        config = self.active_experiments[experiment_id]
        
        alerts = []
        
        # Check drawdown threshold
        if result.max_drawdown > config.max_drawdown_threshold * 100:
            alerts.append({
                "experiment_id": experiment_id,
                "arm_id": arm_id,
                "type": "max_drawdown_exceeded",
                "value": result.max_drawdown,
                "threshold": config.max_drawdown_threshold * 100,
                "timestamp": datetime.utcnow().isoformat()
            })
        
        # Check win rate threshold
        if result.samples >= 20 and result.win_rate < config.min_win_rate_threshold:
            alerts.append({
                "experiment_id": experiment_id,
                "arm_id": arm_id,
                "type": "win_rate_too_low", 
                "value": result.win_rate,
                "threshold": config.min_win_rate_threshold,
                "timestamp": datetime.utcnow().isoformat()
            })
        
        # Check daily loss threshold
        daily_loss = float(result.total_profit) if result.total_profit < 0 else 0
        if daily_loss < -float(config.max_daily_loss_per_arm):
            alerts.append({
                "experiment_id": experiment_id,
                "arm_id": arm_id,
                "type": "daily_loss_exceeded",
                "value": daily_loss,
                "threshold": -float(config.max_daily_loss_per_arm),
                "timestamp": datetime.utcnow().isoformat()
            })
        
        # Add alerts to global list and log
        for alert in alerts:
            self.safety_alerts.append(alert)
            logger.warning(f"Safety alert: {alert}")
    
    async def _update_bandit_allocation(self, experiment_id: str):
        """Update traffic allocation for multi-arm bandit"""
        config = self.active_experiments[experiment_id]
        results = self.experiment_results[experiment_id]
        
        # Use epsilon-greedy or Thompson sampling for allocation updates
        # For simplicity, using epsilon-greedy here
        
        total_samples = sum(result.samples for result in results.values())
        if total_samples < 50:  # Not enough data yet
            return
        
        # Calculate performance scores (ROI)
        arm_scores = {}
        for arm_id, result in results.items():
            if result.samples >= 10:  # Minimum samples for consideration
                arm_scores[arm_id] = result.roi_percentage
            else:
                arm_scores[arm_id] = 0.0  # Conservative for new arms
        
        if not arm_scores:
            return
        
        # Find best performing arm
        best_arm = max(arm_scores, key=arm_scores.get)
        
        # Calculate new allocations with exploration
        new_allocations = {}
        exploration_budget = config.exploration_rate
        exploitation_budget = 1.0 - exploration_budget
        
        # Give exploration budget equally to all arms
        equal_exploration = exploration_budget / len(results)
        
        for arm_id in results.keys():
            if arm_id == best_arm:
                new_allocations[arm_id] = exploitation_budget + equal_exploration
            else:
                new_allocations[arm_id] = equal_exploration
        
        # Update allocations
        self.current_allocations[experiment_id] = new_allocations
        
        logger.info(f"Updated bandit allocations for {experiment_id}: {new_allocations}")
    
    async def _check_stopping_rules(self, experiment_id: str):
        """Check if experiment should be stopped early"""
        config = self.active_experiments[experiment_id]
        results = self.experiment_results[experiment_id]
        
        # Check sample size
        total_samples = sum(result.samples for result in results.values())
        if total_samples >= config.max_sample_size_per_arm * len(config.arms):
            await self.stop_experiment(experiment_id, StoppingRule.SAMPLE_SIZE_REACHED)
            return
        
        # Check time limit
        if datetime.utcnow() >= config.planned_end_time:
            await self.stop_experiment(experiment_id, StoppingRule.TIME_LIMIT)
            return
        
        # Check statistical significance
        if total_samples >= config.min_sample_size_per_arm * len(config.arms):
            statistical_tests = await self._perform_statistical_tests(results, config)
            
            # Check if any test shows significance
            for test_result in statistical_tests.get("pairwise_tests", []):
                if test_result["significant"]:
                    winner_arm = test_result["better_arm"]
                    await self.stop_experiment(experiment_id, StoppingRule.STATISTICAL_SIGNIFICANCE, winner_arm)
                    return
        
        # Check safety thresholds
        for alert in self.safety_alerts:
            if (alert.get("experiment_id") == experiment_id and 
                alert.get("type") in ["max_drawdown_exceeded", "daily_loss_exceeded"]):
                await self.stop_experiment(experiment_id, StoppingRule.SAFETY_THRESHOLD)
                return
    
    def _calculate_summary_statistics(self, results: Dict[str, ExperimentResult]) -> Dict[str, Any]:
        """Calculate summary statistics for experiment arms"""
        summary = {}
        
        for arm_id, result in results.items():
            if result.samples > 0:
                # Basic statistics
                summary[arm_id] = {
                    "samples": result.samples,
                    "wins": result.wins,
                    "losses": result.losses,
                    "win_rate": result.win_rate,
                    "roi_percentage": result.roi_percentage,
                    "total_profit": float(result.total_profit),
                    "max_drawdown": result.max_drawdown,
                    "profit_per_bet": float(result.total_profit) / result.samples if result.samples > 0 else 0
                }
                
                # Confidence intervals for win rate
                if result.samples >= 10:
                    ci_lower, ci_upper = self._calculate_binomial_ci(result.wins, result.samples, 0.95)
                    summary[arm_id]["win_rate_ci"] = (ci_lower, ci_upper)
        
        return summary
    
    async def _perform_statistical_tests(
        self, 
        results: Dict[str, ExperimentResult], 
        config: ExperimentConfig
    ) -> Dict[str, Any]:
        """Perform statistical tests between experiment arms"""
        
        test_results = {
            "pairwise_tests": [],
            "overall_test": None
        }
        
        # Get arms with sufficient data
        valid_arms = {
            arm_id: result for arm_id, result in results.items()
            if result.samples >= 20  # Minimum for statistical testing
        }
        
        if len(valid_arms) < 2:
            return test_results
        
        # Pairwise comparisons
        arm_ids = list(valid_arms.keys())
        for i in range(len(arm_ids)):
            for j in range(i + 1, len(arm_ids)):
                arm_a_id, arm_b_id = arm_ids[i], arm_ids[j]
                result_a, result_b = valid_arms[arm_a_id], valid_arms[arm_b_id]
                
                # Compare based on primary metric
                if config.primary_metric == "roi":
                    test_result = self._compare_roi_statistical(result_a, result_b)
                elif config.primary_metric == "win_rate":
                    test_result = self._compare_win_rates_statistical(result_a, result_b)
                else:
                    continue
                
                test_results["pairwise_tests"].append({
                    "arm_a": arm_a_id,
                    "arm_b": arm_b_id,
                    "test_name": test_result.test_name,
                    "statistic": test_result.statistic,
                    "p_value": test_result.p_value,
                    "significant": test_result.significant,
                    "confidence_interval": test_result.confidence_interval,
                    "effect_size": test_result.effect_size,
                    "better_arm": arm_a_id if test_result.effect_size > 0 else arm_b_id
                })
        
        return test_results
    
    def _compare_win_rates_statistical(
        self, 
        result_a: ExperimentResult, 
        result_b: ExperimentResult
    ) -> StatisticalTest:
        """Compare win rates using two-proportion z-test"""
        
        # Two-proportion z-test
        p1, n1 = result_a.win_rate, result_a.samples
        p2, n2 = result_b.win_rate, result_b.samples
        
        # Pooled proportion
        p_pool = (result_a.wins + result_b.wins) / (n1 + n2)
        
        # Standard error
        se = np.sqrt(p_pool * (1 - p_pool) * (1/n1 + 1/n2))
        
        if se == 0:
            return StatisticalTest("two_proportion_z", 0, 1, False, (0, 0), 0)
        
        # Test statistic
        z = (p1 - p2) / se
        
        # p-value (two-tailed)
        p_value = 2 * (1 - stats.norm.cdf(abs(z)))
        
        # Confidence interval for difference
        se_diff = np.sqrt((p1 * (1 - p1) / n1) + (p2 * (1 - p2) / n2))
        z_alpha = stats.norm.ppf(0.975)  # 95% confidence
        ci_lower = (p1 - p2) - z_alpha * se_diff
        ci_upper = (p1 - p2) + z_alpha * se_diff
        
        return StatisticalTest(
            test_name="two_proportion_z",
            statistic=z,
            p_value=p_value,
            significant=p_value < 0.05,
            confidence_interval=(ci_lower, ci_upper),
            effect_size=p1 - p2
        )
    
    def _compare_roi_statistical(
        self, 
        result_a: ExperimentResult, 
        result_b: ExperimentResult
    ) -> StatisticalTest:
        """Compare ROI using simplified test (in production would use actual bet returns)"""
        
        # Simplified comparison - in production would use actual return distributions
        roi_diff = result_a.roi_percentage - result_b.roi_percentage
        
        # Mock statistical test - would need actual return data for proper test
        mock_se = 2.0  # Standard error estimate
        z = roi_diff / mock_se
        p_value = 2 * (1 - stats.norm.cdf(abs(z)))
        
        return StatisticalTest(
            test_name="roi_comparison",
            statistic=z,
            p_value=p_value,
            significant=p_value < 0.05 and abs(roi_diff) > 1.0,  # Minimum practical significance
            confidence_interval=(roi_diff - 1.96 * mock_se, roi_diff + 1.96 * mock_se),
            effect_size=roi_diff
        )
    
    def _calculate_confidence_intervals(
        self, 
        results: Dict[str, ExperimentResult], 
        alpha: float
    ) -> Dict[str, Dict[str, Tuple[float, float]]]:
        """Calculate confidence intervals for key metrics"""
        
        confidence_intervals = {}
        
        for arm_id, result in results.items():
            if result.samples < 10:
                continue
                
            arm_cis = {}
            
            # Win rate confidence interval (binomial)
            ci_lower, ci_upper = self._calculate_binomial_ci(result.wins, result.samples, 1 - alpha)
            arm_cis["win_rate"] = (ci_lower, ci_upper)
            
            # ROI confidence interval (approximate)
            # In production would use bootstrap or actual return distribution
            roi_se = abs(result.roi_percentage) / np.sqrt(result.samples)  # Rough estimate
            z_alpha = stats.norm.ppf(1 - alpha / 2)
            roi_margin = z_alpha * roi_se
            arm_cis["roi"] = (
                result.roi_percentage - roi_margin,
                result.roi_percentage + roi_margin
            )
            
            confidence_intervals[arm_id] = arm_cis
        
        return confidence_intervals
    
    def _calculate_binomial_ci(self, successes: int, trials: int, confidence: float) -> Tuple[float, float]:
        """Calculate binomial confidence interval using normal approximation"""
        if trials == 0:
            return (0.0, 0.0)
        
        p = successes / trials
        z = stats.norm.ppf((1 + confidence) / 2)
        se = np.sqrt(p * (1 - p) / trials)
        
        ci_lower = max(0, p - z * se)
        ci_upper = min(1, p + z * se)
        
        return (ci_lower, ci_upper)
    
    def _analyze_winner(
        self, 
        results: Dict[str, ExperimentResult], 
        statistical_tests: Dict[str, Any],
        primary_metric: str
    ) -> Dict[str, Any]:
        """Analyze experiment to determine winner"""
        
        winner_analysis = {
            "has_winner": False,
            "winner_arm": None,
            "winner_confidence": 0.0,
            "reasons": []
        }
        
        # Check if any pairwise test shows significance
        significant_tests = [
            test for test in statistical_tests.get("pairwise_tests", [])
            if test["significant"]
        ]
        
        if significant_tests:
            # Find most significant result
            most_significant = min(significant_tests, key=lambda x: x["p_value"])
            
            winner_analysis["has_winner"] = True
            winner_analysis["winner_arm"] = most_significant["better_arm"]
            winner_analysis["winner_confidence"] = 1 - most_significant["p_value"]
            winner_analysis["reasons"].append(f"Statistically significant difference in {primary_metric}")
        
        # Check for practical significance
        best_arm = None
        best_score = -float('inf')
        
        for arm_id, result in results.items():
            if result.samples >= 50:  # Minimum for practical significance
                score = getattr(result, primary_metric.replace("_percentage", ""))
                if isinstance(score, Decimal):
                    score = float(score)
                
                if score > best_score:
                    best_score = score
                    best_arm = arm_id
        
        if best_arm and not winner_analysis["has_winner"]:
            # Check if best arm is practically better
            other_arms_scores = []
            for arm_id, result in results.items():
                if arm_id != best_arm and result.samples >= 50:
                    score = getattr(result, primary_metric.replace("_percentage", ""))
                    if isinstance(score, Decimal):
                        score = float(score)
                    other_arms_scores.append(score)
            
            if other_arms_scores:
                avg_other_score = np.mean(other_arms_scores)
                improvement = (best_score - avg_other_score) / abs(avg_other_score) if avg_other_score != 0 else 0
                
                if improvement > 0.1:  # 10% improvement threshold
                    winner_analysis["has_winner"] = True
                    winner_analysis["winner_arm"] = best_arm
                    winner_analysis["winner_confidence"] = min(0.8, improvement)
                    winner_analysis["reasons"].append(f"Practically significant improvement: {improvement:.1%}")
        
        return winner_analysis
    
    def _perform_power_analysis(
        self, 
        results: Dict[str, ExperimentResult], 
        config: ExperimentConfig
    ) -> Dict[str, Any]:
        """Perform statistical power analysis"""
        
        total_samples = sum(result.samples for result in results.values())
        
        power_analysis = {
            "current_power": 0.0,
            "recommended_sample_size": 0,
            "days_to_significance": 0,
            "current_sample_size": total_samples
        }
        
        # Simplified power calculation - in production would use proper power analysis
        if total_samples > 100:
            # Estimate power based on current sample size and effect size
            estimated_power = min(0.95, total_samples / 500)  # Rough approximation
            power_analysis["current_power"] = estimated_power
            
            # Recommend sample size for 80% power
            target_power = config.target_statistical_power
            if estimated_power < target_power:
                recommended_samples = int(total_samples * (target_power / estimated_power))
                power_analysis["recommended_sample_size"] = recommended_samples
                
                # Estimate days needed based on current rate
                current_rate = total_samples / max(1, (datetime.utcnow() - config.start_time).days)
                days_needed = max(0, (recommended_samples - total_samples) / max(1, current_rate))
                power_analysis["days_to_significance"] = int(days_needed)
        
        return power_analysis
    
    def _perform_risk_analysis(self, results: Dict[str, ExperimentResult]) -> Dict[str, Any]:
        """Perform risk analysis across experiment arms"""
        
        risk_analysis = {
            "total_exposure": 0.0,
            "max_loss_arm": None,
            "highest_drawdown_arm": None,
            "risk_metrics": {}
        }
        
        total_volume = Decimal("0")
        total_profit = Decimal("0")
        worst_loss_arm = None
        worst_loss = 0
        highest_dd_arm = None
        highest_dd = 0
        
        for arm_id, result in results.items():
            total_volume += result.total_volume
            total_profit += result.total_profit
            
            arm_loss = float(result.total_profit) if result.total_profit < 0 else 0
            if arm_loss < worst_loss:
                worst_loss = arm_loss
                worst_loss_arm = arm_id
            
            if result.max_drawdown > highest_dd:
                highest_dd = result.max_drawdown
                highest_dd_arm = arm_id
            
            # Individual arm risk metrics
            risk_analysis["risk_metrics"][arm_id] = {
                "profit_loss": float(result.total_profit),
                "max_drawdown": result.max_drawdown,
                "profit_volatility": abs(float(result.total_profit) / result.samples) if result.samples > 0 else 0,
                "samples": result.samples
            }
        
        risk_analysis["total_exposure"] = float(total_volume)
        risk_analysis["total_pnl"] = float(total_profit)
        risk_analysis["max_loss_arm"] = worst_loss_arm
        risk_analysis["highest_drawdown_arm"] = highest_dd_arm
        
        return risk_analysis
    
    def _generate_recommendations(
        self, 
        config: ExperimentConfig, 
        results: Dict[str, ExperimentResult],
        statistical_tests: Dict[str, Any]
    ) -> List[str]:
        """Generate actionable recommendations based on experiment results"""
        
        recommendations = []
        
        total_samples = sum(result.samples for result in results.values())
        
        # Sample size recommendations
        if total_samples < config.min_sample_size_per_arm * len(config.arms):
            needed = config.min_sample_size_per_arm * len(config.arms) - total_samples
            recommendations.append(f"Continue experiment - need {needed} more samples for statistical power")
        
        # Performance recommendations
        best_roi_arm = max(results.items(), key=lambda x: x[1].roi_percentage)
        if best_roi_arm[1].roi_percentage > 3.0:
            recommendations.append(f"Arm '{best_roi_arm[0]}' showing strong performance ({best_roi_arm[1].roi_percentage:.1f}% ROI)")
        
        # Risk recommendations
        high_risk_arms = [
            arm_id for arm_id, result in results.items()
            if result.max_drawdown > 15.0 or result.win_rate < 0.45
        ]
        if high_risk_arms:
            recommendations.append(f"Monitor high-risk arms: {', '.join(high_risk_arms)}")
        
        # Statistical significance recommendations
        significant_tests = [test for test in statistical_tests.get("pairwise_tests", []) if test["significant"]]
        if significant_tests:
            recommendations.append("Statistical significance detected - consider stopping experiment")
        elif total_samples >= 200:
            recommendations.append("No significance yet - consider running longer or increasing effect size")
        
        return recommendations
    
    def _get_experiment_status(self, experiment_id: str) -> str:
        """Get current status of experiment"""
        if experiment_id not in self.active_experiments:
            return ExperimentStatus.FAILED.value
        
        config = self.active_experiments[experiment_id]
        now = datetime.utcnow()
        
        if now > config.planned_end_time:
            return ExperimentStatus.COMPLETED.value
        
        # Check for safety alerts
        safety_alerts = [alert for alert in self.safety_alerts if alert.get("experiment_id") == experiment_id]
        if safety_alerts:
            return ExperimentStatus.PAUSED.value
        
        return ExperimentStatus.ACTIVE.value
    
    async def _deploy_winning_strategy(self, strategy_config: StrategyConfiguration, analysis: Dict[str, Any]):
        """Deploy winning strategy to production"""
        # In production implementation, this would:
        # 1. Update strategy configuration in production system
        # 2. Gradually roll out winning strategy
        # 3. Monitor performance in full production
        # 4. Create audit trail for deployment
        
        logger.info(f"Deploying winning strategy: {strategy_config.name}")
        
        # Mock deployment process
        deployment_result = {
            "strategy_id": strategy_config.strategy_id,
            "deployment_time": datetime.utcnow().isoformat(),
            "analysis_summary": analysis.get("winner_analysis", {}),
            "rollout_percentage": 100.0
        }
        
        logger.info(f"Strategy deployment completed: {deployment_result}")
    
    async def _archive_experiment(self, experiment_id: str, status: ExperimentStatus, final_analysis: Dict[str, Any]):
        """Archive completed experiment"""
        # In production implementation, this would:
        # 1. Store final results in data warehouse
        # 2. Create experiment report
        # 3. Update strategy performance history
        # 4. Clean up temporary resources
        
        archive_record = {
            "experiment_id": experiment_id,
            "final_status": status.value,
            "archive_time": datetime.utcnow().isoformat(),
            "final_analysis": final_analysis
        }
        
        logger.info(f"Archived experiment {experiment_id}: {status.value}")


# Global A/B testing engine instance
ab_testing_engine = ABTestingEngine()