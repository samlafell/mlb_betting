"""
Enhanced Model Registry for MLB Betting Strategies

This module extends the base MLflow model registry with betting-specific validation,
performance tracking, and deployment workflows that integrate backtesting results
with ML model performance.

Key Features:
1. Betting-specific promotion criteria combining ML metrics and profitability
2. Integration with backtesting engine for comprehensive validation
3. Strategy performance tracking across multiple time periods
4. Risk-adjusted model evaluation and deployment decisions
5. A/B testing integration for production model comparison
6. Automated model retraining based on betting performance degradation
"""

import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

import mlflow
from mlflow.tracking import MlflowClient

from ...core.config import get_settings
from ...data.database import get_unified_repository
from ..registry.model_registry import ModelRegistryService, ModelStage, ModelVersionInfo
from ...analysis.strategy_development_framework import StrategyConfiguration, StrategyPerformanceMetrics
from ...analysis.validation.integrated_validation_engine import IntegratedValidationEngine, ValidationPhase
from ...analysis.testing.ab_testing_framework import ABTestingEngine, ExperimentArm, TestType

logger = logging.getLogger(__name__)


class BettingModelStage(str, Enum):
    """Enhanced model stages for betting strategies"""
    DEVELOPMENT = "Development"           # Under development and validation
    BACKTESTING = "Backtesting"          # Passed ML validation, undergoing backtesting
    PAPER_TRADING = "PaperTrading"       # Simulated live trading validation
    STAGING = "Staging"                  # Limited live deployment
    PRODUCTION = "Production"            # Full production deployment
    CHAMPION = "Champion"                # Best performing production model
    CHALLENGER = "Challenger"            # New model challenging champion
    ARCHIVED = "Archived"                # Retired model
    DEPRECATED = "Deprecated"            # Poor performance, deprecated


@dataclass
class BettingModelMetrics:
    """Comprehensive betting model performance metrics"""
    # ML Performance Metrics
    ml_accuracy: Optional[float] = None
    ml_precision: Optional[float] = None
    ml_recall: Optional[float] = None
    ml_f1_score: Optional[float] = None
    ml_roc_auc: Optional[float] = None
    
    # Betting Performance Metrics
    roi_percentage: float = 0.0
    win_rate: float = 0.0
    total_profit: Decimal = Decimal("0")
    max_drawdown: float = 0.0
    profit_factor: float = 0.0
    sharpe_ratio: Optional[float] = None
    
    # Risk Metrics
    value_at_risk_95: Optional[float] = None
    expected_shortfall: Optional[float] = None
    kelly_fraction: Optional[float] = None
    
    # Volume Metrics
    total_bets: int = 0
    total_volume: Decimal = Decimal("0")
    avg_bet_size: Decimal = Decimal("0")
    
    # Time-based Performance
    last_30_days_roi: Optional[float] = None
    last_7_days_roi: Optional[float] = None
    performance_trend: Optional[str] = None  # "improving", "stable", "declining"
    
    # Statistical Significance
    confidence_interval: Optional[Tuple[float, float]] = None
    p_value: Optional[float] = None
    
    # Validation Scores
    backtesting_score: float = 0.0
    paper_trading_score: float = 0.0
    production_score: float = 0.0


@dataclass
class PromotionCriteria:
    """Criteria for model stage promotion"""
    stage: BettingModelStage
    
    # ML Thresholds
    min_ml_accuracy: Optional[float] = None
    min_ml_roc_auc: Optional[float] = None
    min_ml_f1_score: Optional[float] = None
    
    # Betting Thresholds
    min_roi: float = 0.0
    min_win_rate: float = 0.5
    max_drawdown: float = 100.0  # Maximum allowed drawdown %
    min_sharpe_ratio: Optional[float] = None
    
    # Volume Requirements
    min_total_bets: int = 0
    min_validation_days: int = 0
    
    # Statistical Requirements
    min_confidence_level: float = 0.95
    max_p_value: float = 0.05
    
    # Business Requirements
    min_profit_threshold: Decimal = Decimal("0")
    max_risk_exposure: Decimal = Decimal("10000")


class BettingModelRegistry:
    """
    Enhanced model registry with betting-specific validation and deployment workflows
    
    Integrates ML model performance with actual betting results to make informed
    promotion and deployment decisions based on comprehensive evidence
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.base_registry = ModelRegistryService()
        self.validation_engine = IntegratedValidationEngine()
        self.ab_testing_engine = ABTestingEngine()
        self.repository = None
        
        # Define promotion criteria for each stage
        self.promotion_criteria = {
            BettingModelStage.BACKTESTING: PromotionCriteria(
                stage=BettingModelStage.BACKTESTING,
                min_ml_accuracy=0.53,
                min_ml_roc_auc=0.55,
                min_roi=0.5,
                min_win_rate=0.51,
                max_drawdown=30.0,
                min_total_bets=50
            ),
            BettingModelStage.PAPER_TRADING: PromotionCriteria(
                stage=BettingModelStage.PAPER_TRADING,
                min_ml_accuracy=0.55,
                min_ml_roc_auc=0.58,
                min_roi=1.5,
                min_win_rate=0.52,
                max_drawdown=25.0,
                min_total_bets=100,
                min_validation_days=7
            ),
            BettingModelStage.STAGING: PromotionCriteria(
                stage=BettingModelStage.STAGING,
                min_ml_accuracy=0.57,
                min_ml_roc_auc=0.60,
                min_roi=3.0,
                min_win_rate=0.54,
                max_drawdown=20.0,
                min_sharpe_ratio=0.8,
                min_total_bets=200,
                min_validation_days=14,
                min_confidence_level=0.95
            ),
            BettingModelStage.PRODUCTION: PromotionCriteria(
                stage=BettingModelStage.PRODUCTION,
                min_ml_accuracy=0.58,
                min_ml_roc_auc=0.62,
                min_roi=4.0,
                min_win_rate=0.55,
                max_drawdown=15.0,
                min_sharpe_ratio=1.0,
                min_total_bets=500,
                min_validation_days=30,
                min_confidence_level=0.95,
                max_p_value=0.05,
                min_profit_threshold=Decimal("1000")
            ),
            BettingModelStage.CHAMPION: PromotionCriteria(
                stage=BettingModelStage.CHAMPION,
                min_ml_accuracy=0.60,
                min_ml_roc_auc=0.65,
                min_roi=6.0,
                min_win_rate=0.57,
                max_drawdown=12.0,
                min_sharpe_ratio=1.2,
                min_total_bets=1000,
                min_validation_days=60,
                min_confidence_level=0.99,
                max_p_value=0.01,
                min_profit_threshold=Decimal("5000")
            )
        }
        
        # Performance tracking
        self.model_performance_history: Dict[str, List[BettingModelMetrics]] = {}
        self.active_ab_tests: Dict[str, str] = {}  # model_name -> experiment_id
    
    async def initialize(self) -> bool:
        """Initialize enhanced betting model registry"""
        try:
            await self.base_registry.initialize()
            await self.validation_engine.initialize()
            await self.ab_testing_engine.initialize()
            
            self.repository = get_unified_repository()
            
            # Load existing model performance history
            await self._load_model_performance_history()
            
            logger.info("Betting model registry initialized")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize betting model registry: {e}")
            return False
    
    async def register_betting_model(
        self,
        model_uri: str,
        model_name: str,
        strategy_config: StrategyConfiguration,
        initial_validation_results: Optional[Dict[str, Any]] = None,
        description: Optional[str] = None
    ) -> Optional[str]:
        """
        Register new betting model with comprehensive validation
        
        Args:
            model_uri: MLflow model URI
            model_name: Name for registered model
            strategy_config: Associated strategy configuration
            initial_validation_results: Initial ML validation results
            description: Optional description
            
        Returns:
            Model version or None if failed
        """
        try:
            logger.info(f"Registering betting model: {model_name}")
            
            # Register with base MLflow registry
            model_version = await self.base_registry.register_model(
                model_uri=model_uri,
                model_name=model_name,
                description=description,
                tags={
                    "strategy_type": strategy_config.strategy_type.value,
                    "betting_model": "true",
                    "strategy_id": strategy_config.strategy_id
                }
            )
            
            if not model_version:
                logger.error(f"Failed to register model with base registry: {model_name}")
                return None
            
            # Create initial betting metrics
            initial_metrics = BettingModelMetrics()
            if initial_validation_results:
                initial_metrics = self._extract_betting_metrics_from_validation(initial_validation_results)
            
            # Store initial performance
            if model_name not in self.model_performance_history:
                self.model_performance_history[model_name] = []
            self.model_performance_history[model_name].append(initial_metrics)
            
            # Set initial stage to DEVELOPMENT
            await self._set_model_stage(model_name, model_version.version, BettingModelStage.DEVELOPMENT)
            
            logger.info(f"Registered betting model {model_name} v{model_version.version}")
            return model_version.version
            
        except Exception as e:
            logger.error(f"Error registering betting model {model_name}: {e}")
            return None
    
    async def validate_and_promote_model(
        self,
        model_name: str,
        version: str,
        target_stage: BettingModelStage,
        validation_start: datetime,
        validation_end: datetime,
        force: bool = False
    ) -> bool:
        """
        Validate model and promote to target stage if criteria met
        
        Args:
            model_name: Name of model to validate
            version: Version to validate
            target_stage: Target stage for promotion
            validation_start: Start date for validation period
            validation_end: End date for validation period
            force: Skip validation checks
            
        Returns:
            True if promoted successfully
        """
        try:
            logger.info(f"Validating {model_name} v{version} for promotion to {target_stage.value}")
            
            if not force:
                # Get strategy configuration
                strategy_config = await self._get_strategy_config_for_model(model_name)
                if not strategy_config:
                    logger.error(f"No strategy configuration found for model: {model_name}")
                    return False
                
                # Run comprehensive validation based on target stage
                validation_phase = self._map_stage_to_validation_phase(target_stage)
                validation_result = await self.validation_engine.validate_strategy_comprehensive(
                    strategy_config, validation_phase, validation_start, validation_end
                )
                
                if not validation_result.passed:
                    logger.warning(f"Model {model_name} v{version} failed validation for {target_stage.value}")
                    logger.warning(f"Validation recommendations: {validation_result.recommendations}")
                    return False
                
                # Check betting-specific promotion criteria
                betting_metrics = self._extract_betting_metrics_from_validation_result(validation_result)
                criteria_met = await self._check_promotion_criteria(target_stage, betting_metrics)
                
                if not criteria_met:
                    logger.warning(f"Model {model_name} v{version} does not meet promotion criteria for {target_stage.value}")
                    return False
                
                # Record performance metrics
                self.model_performance_history[model_name].append(betting_metrics)
            
            # Promote model to target stage
            success = await self._promote_model_to_stage(model_name, version, target_stage)
            
            if success:
                # Set up stage-specific monitoring and testing
                await self._setup_stage_monitoring(model_name, version, target_stage)
                
                logger.info(f"Successfully promoted {model_name} v{version} to {target_stage.value}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error validating and promoting model {model_name} v{version}: {e}")
            return False
    
    async def setup_champion_challenger_test(
        self,
        champion_model: str,
        challenger_model: str,
        traffic_split: float = 0.8,
        test_duration_days: int = 14
    ) -> str:
        """
        Setup champion vs challenger A/B test
        
        Args:
            champion_model: Current champion model name
            challenger_model: Challenger model name
            traffic_split: Traffic to champion (challenger gets remainder)
            test_duration_days: Duration of A/B test
            
        Returns:
            Experiment ID
        """
        try:
            # Get strategy configurations for both models
            champion_config = await self._get_strategy_config_for_model(champion_model)
            challenger_config = await self._get_strategy_config_for_model(challenger_model)
            
            if not champion_config or not challenger_config:
                raise ValueError("Could not retrieve strategy configurations for models")
            
            # Create experiment arms
            arms = [
                ExperimentArm(
                    arm_id="champion",
                    name=f"Champion: {champion_model}",
                    strategy_config=champion_config,
                    traffic_allocation=traffic_split,
                    is_control=True
                ),
                ExperimentArm(
                    arm_id="challenger",
                    name=f"Challenger: {challenger_model}",
                    strategy_config=challenger_config,
                    traffic_allocation=1.0 - traffic_split,
                    is_control=False
                )
            ]
            
            # Create A/B test experiment
            experiment_id = await self.ab_testing_engine.create_experiment(
                name=f"Champion vs Challenger: {champion_model} vs {challenger_model}",
                description=f"A/B test comparing champion model {champion_model} against challenger {challenger_model}",
                arms=arms,
                test_type=TestType.CHAMPION_CHALLENGER,
                duration_days=test_duration_days,
                primary_metric="roi",
                secondary_metrics=["win_rate", "profit", "max_drawdown"],
                significance_level=0.05,
                target_statistical_power=0.8
            )
            
            # Track active A/B tests
            self.active_ab_tests[champion_model] = experiment_id
            self.active_ab_tests[challenger_model] = experiment_id
            
            # Set challenger model stage
            challenger_versions = await self.base_registry.get_model_versions(challenger_model)
            if challenger_versions:
                latest_challenger = max(challenger_versions, key=lambda v: v.creation_timestamp)
                await self._set_model_stage(challenger_model, latest_challenger.version, BettingModelStage.CHALLENGER)
            
            logger.info(f"Setup champion vs challenger test: {experiment_id}")
            return experiment_id
            
        except Exception as e:
            logger.error(f"Error setting up champion vs challenger test: {e}")
            raise
    
    async def analyze_champion_challenger_results(self, experiment_id: str) -> Dict[str, Any]:
        """
        Analyze champion vs challenger test results and recommend actions
        
        Args:
            experiment_id: ID of the experiment to analyze
            
        Returns:
            Analysis results with recommendations
        """
        try:
            # Get experiment analysis
            analysis = await self.ab_testing_engine.analyze_experiment(experiment_id)
            
            if analysis.get("error"):
                logger.error(f"Error in experiment analysis: {analysis['error']}")
                return analysis
            
            # Determine recommendation
            winner_analysis = analysis.get("winner_analysis", {})
            
            recommendation = {
                "action": "continue",  # continue, promote_challenger, keep_champion
                "confidence": 0.0,
                "reasoning": [],
                "next_steps": []
            }
            
            if winner_analysis.get("has_winner"):
                winner_arm = winner_analysis.get("winner_arm")
                confidence = winner_analysis.get("winner_confidence", 0)
                
                if winner_arm == "challenger" and confidence > 0.8:
                    recommendation["action"] = "promote_challenger"
                    recommendation["confidence"] = confidence
                    recommendation["reasoning"].append("Challenger shows statistically significant improvement")
                    recommendation["next_steps"].append("Promote challenger to champion status")
                    recommendation["next_steps"].append("Archive current champion model")
                elif winner_arm == "champion" and confidence > 0.8:
                    recommendation["action"] = "keep_champion"
                    recommendation["confidence"] = confidence
                    recommendation["reasoning"].append("Champion maintains superior performance")
                    recommendation["next_steps"].append("Archive challenger model")
                    recommendation["next_steps"].append("Continue with current champion")
            
            # Check for safety concerns
            risk_analysis = analysis.get("risk_analysis", {})
            if risk_analysis.get("highest_drawdown_arm") == "challenger":
                recommendation["reasoning"].append("Challenger shows higher risk profile")
                if recommendation["action"] == "promote_challenger":
                    recommendation["action"] = "continue"
                    recommendation["next_steps"].append("Monitor challenger risk metrics")
            
            # Add business context
            statistical_tests = analysis.get("statistical_tests", {})
            significant_tests = [test for test in statistical_tests.get("pairwise_tests", []) if test["significant"]]
            
            if significant_tests:
                test = significant_tests[0]
                roi_diff = abs(test.get("effect_size", 0))
                if roi_diff > 2.0:  # 2% ROI difference
                    recommendation["reasoning"].append(f"Significant ROI difference: {roi_diff:.1f}%")
            
            analysis["recommendation"] = recommendation
            
            logger.info(f"Champion vs challenger analysis completed: {recommendation['action']}")
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing champion vs challenger results: {e}")
            raise
    
    async def execute_champion_challenger_decision(
        self, 
        experiment_id: str, 
        decision: str,
        champion_model: str,
        challenger_model: str
    ) -> bool:
        """
        Execute decision from champion vs challenger analysis
        
        Args:
            experiment_id: ID of the experiment
            decision: Decision to execute ("promote_challenger", "keep_champion", etc.)
            champion_model: Current champion model
            challenger_model: Challenger model
            
        Returns:
            Success status
        """
        try:
            logger.info(f"Executing champion vs challenger decision: {decision}")
            
            if decision == "promote_challenger":
                # Promote challenger to champion
                challenger_versions = await self.base_registry.get_model_versions(challenger_model)
                if challenger_versions:
                    latest_challenger = max(challenger_versions, key=lambda v: v.creation_timestamp)
                    
                    # Set challenger as new champion
                    await self._set_model_stage(challenger_model, latest_challenger.version, BettingModelStage.CHAMPION)
                    
                    # Archive old champion
                    champion_versions = await self.base_registry.get_model_versions(champion_model)
                    if champion_versions:
                        current_champion = next((v for v in champion_versions if v.stage.value == "Production"), None)
                        if current_champion:
                            await self._set_model_stage(champion_model, current_champion.version, BettingModelStage.ARCHIVED)
                    
                    logger.info(f"Promoted challenger {challenger_model} to champion status")
                
            elif decision == "keep_champion":
                # Archive challenger, keep champion
                challenger_versions = await self.base_registry.get_model_versions(challenger_model)
                if challenger_versions:
                    latest_challenger = max(challenger_versions, key=lambda v: v.creation_timestamp)
                    await self._set_model_stage(challenger_model, latest_challenger.version, BettingModelStage.ARCHIVED)
                
                logger.info(f"Kept champion {champion_model}, archived challenger {challenger_model}")
            
            # Stop the A/B test
            await self.ab_testing_engine.stop_experiment(
                experiment_id, 
                reason="business_decision",
                winner_arm_id="challenger" if decision == "promote_challenger" else "champion"
            )
            
            # Clean up tracking
            if champion_model in self.active_ab_tests:
                del self.active_ab_tests[champion_model]
            if challenger_model in self.active_ab_tests:
                del self.active_ab_tests[challenger_model]
            
            return True
            
        except Exception as e:
            logger.error(f"Error executing champion vs challenger decision: {e}")
            return False
    
    async def monitor_production_models(self) -> Dict[str, Any]:
        """
        Monitor all production models for performance degradation
        
        Returns:
            Monitoring report with alerts and recommendations
        """
        try:
            monitoring_report = {
                "timestamp": datetime.utcnow().isoformat(),
                "production_models": [],
                "alerts": [],
                "recommendations": []
            }
            
            # Get all production stage models
            for model_name in self.model_performance_history.keys():
                model_versions = await self.base_registry.get_model_versions(model_name)
                production_versions = [v for v in model_versions if v.stage.value in ["Production", "Champion"]]
                
                for version in production_versions:
                    model_info = await self._analyze_model_performance(model_name, version.version)
                    monitoring_report["production_models"].append(model_info)
                    
                    # Check for alerts
                    alerts = await self._check_model_alerts(model_name, version.version, model_info)
                    monitoring_report["alerts"].extend(alerts)
            
            # Generate recommendations
            recommendations = self._generate_monitoring_recommendations(monitoring_report["alerts"])
            monitoring_report["recommendations"] = recommendations
            
            logger.info(f"Production monitoring completed: {len(monitoring_report['production_models'])} models monitored")
            
            return monitoring_report
            
        except Exception as e:
            logger.error(f"Error monitoring production models: {e}")
            raise
    
    def get_model_performance_summary(self, model_name: str) -> Dict[str, Any]:
        """Get comprehensive performance summary for model"""
        try:
            if model_name not in self.model_performance_history:
                return {"error": "No performance history found"}
            
            history = self.model_performance_history[model_name]
            if not history:
                return {"error": "Empty performance history"}
            
            latest_metrics = history[-1]
            
            # Calculate trends
            trends = {}
            if len(history) >= 2:
                prev_metrics = history[-2]
                trends["roi_trend"] = latest_metrics.roi_percentage - prev_metrics.roi_percentage
                trends["win_rate_trend"] = latest_metrics.win_rate - prev_metrics.win_rate
                trends["profit_trend"] = float(latest_metrics.total_profit - prev_metrics.total_profit)
            
            # Calculate overall statistics
            all_roi = [m.roi_percentage for m in history if m.total_bets > 0]
            all_win_rates = [m.win_rate for m in history if m.total_bets > 0]
            
            summary = {
                "model_name": model_name,
                "total_performance_periods": len(history),
                "latest_metrics": {
                    "roi_percentage": latest_metrics.roi_percentage,
                    "win_rate": latest_metrics.win_rate,
                    "total_profit": float(latest_metrics.total_profit),
                    "total_bets": latest_metrics.total_bets,
                    "max_drawdown": latest_metrics.max_drawdown,
                    "sharpe_ratio": latest_metrics.sharpe_ratio
                },
                "historical_averages": {
                    "avg_roi": sum(all_roi) / len(all_roi) if all_roi else 0,
                    "avg_win_rate": sum(all_win_rates) / len(all_win_rates) if all_win_rates else 0,
                    "roi_volatility": np.std(all_roi) if len(all_roi) > 1 else 0,
                    "win_rate_consistency": 1 - (np.std(all_win_rates) / np.mean(all_win_rates)) if all_win_rates and np.mean(all_win_rates) > 0 else 0
                },
                "trends": trends,
                "active_ab_test": self.active_ab_tests.get(model_name)
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting performance summary for {model_name}: {e}")
            return {"error": str(e)}
    
    async def _get_strategy_config_for_model(self, model_name: str) -> Optional[StrategyConfiguration]:
        """Get strategy configuration associated with model"""
        try:
            # In production, this would query database for strategy config
            # For now, return a mock configuration
            return StrategyConfiguration(
                strategy_id=f"strategy_{model_name}",
                name=f"Strategy for {model_name}",
                description=f"Auto-generated strategy configuration for model {model_name}",
                strategy_type="ml_predictive",
                validation_method="integrated_validation",
                ml_model_name=model_name,
                ml_prediction_targets=["moneyline_home_win"],
                confidence_threshold=0.6
            )
        except Exception as e:
            logger.error(f"Error getting strategy config for {model_name}: {e}")
            return None
    
    def _map_stage_to_validation_phase(self, stage: BettingModelStage) -> ValidationPhase:
        """Map betting model stage to validation phase"""
        mapping = {
            BettingModelStage.BACKTESTING: ValidationPhase.DEVELOPMENT,
            BettingModelStage.PAPER_TRADING: ValidationPhase.PRE_STAGING,
            BettingModelStage.STAGING: ValidationPhase.STAGING,
            BettingModelStage.PRODUCTION: ValidationPhase.PRE_PRODUCTION,
            BettingModelStage.CHAMPION: ValidationPhase.PRODUCTION
        }
        return mapping.get(stage, ValidationPhase.DEVELOPMENT)
    
    def _extract_betting_metrics_from_validation(self, validation_results: Dict[str, Any]) -> BettingModelMetrics:
        """Extract betting metrics from validation results"""
        metrics = BettingModelMetrics()
        
        # Extract ML metrics
        if "training_results" in validation_results:
            for model_name, results in validation_results["training_results"].items():
                if "test_metrics" in results:
                    test_metrics = results["test_metrics"]
                    metrics.ml_accuracy = test_metrics.get("accuracy")
                    metrics.ml_precision = test_metrics.get("precision")
                    metrics.ml_recall = test_metrics.get("recall")
                    metrics.ml_f1_score = test_metrics.get("f1_score")
                    metrics.ml_roc_auc = test_metrics.get("roc_auc")
                break  # Use first model's metrics
        
        return metrics
    
    def _extract_betting_metrics_from_validation_result(self, validation_result) -> BettingModelMetrics:
        """Extract betting metrics from validation result"""
        perf_metrics = validation_result.metrics
        
        return BettingModelMetrics(
            ml_accuracy=perf_metrics.accuracy,
            ml_precision=perf_metrics.precision,
            ml_recall=perf_metrics.recall,
            ml_f1_score=perf_metrics.f1_score,
            ml_roc_auc=perf_metrics.roc_auc,
            roi_percentage=perf_metrics.roi_percentage,
            win_rate=perf_metrics.win_rate,
            total_profit=perf_metrics.total_profit,
            max_drawdown=perf_metrics.max_drawdown,
            profit_factor=perf_metrics.profit_factor,
            sharpe_ratio=perf_metrics.sharpe_ratio,
            value_at_risk_95=perf_metrics.value_at_risk_95,
            expected_shortfall=perf_metrics.expected_shortfall,
            kelly_fraction=perf_metrics.kelly_fraction,
            total_bets=perf_metrics.sample_size,
            confidence_interval=perf_metrics.confidence_interval,
            p_value=perf_metrics.p_value
        )
    
    async def _check_promotion_criteria(self, target_stage: BettingModelStage, metrics: BettingModelMetrics) -> bool:
        """Check if model meets promotion criteria for target stage"""
        if target_stage not in self.promotion_criteria:
            logger.warning(f"No promotion criteria defined for stage: {target_stage}")
            return False
        
        criteria = self.promotion_criteria[target_stage]
        
        checks = {}
        
        # ML performance checks
        if criteria.min_ml_accuracy and metrics.ml_accuracy:
            checks["ml_accuracy"] = metrics.ml_accuracy >= criteria.min_ml_accuracy
        
        if criteria.min_ml_roc_auc and metrics.ml_roc_auc:
            checks["ml_roc_auc"] = metrics.ml_roc_auc >= criteria.min_ml_roc_auc
        
        if criteria.min_ml_f1_score and metrics.ml_f1_score:
            checks["ml_f1_score"] = metrics.ml_f1_score >= criteria.min_ml_f1_score
        
        # Betting performance checks
        checks["roi"] = metrics.roi_percentage >= criteria.min_roi
        checks["win_rate"] = metrics.win_rate >= criteria.min_win_rate
        checks["max_drawdown"] = metrics.max_drawdown <= criteria.max_drawdown
        
        if criteria.min_sharpe_ratio and metrics.sharpe_ratio:
            checks["sharpe_ratio"] = metrics.sharpe_ratio >= criteria.min_sharpe_ratio
        
        # Volume checks
        checks["total_bets"] = metrics.total_bets >= criteria.min_total_bets
        
        # Profit checks
        if criteria.min_profit_threshold:
            checks["profit_threshold"] = metrics.total_profit >= criteria.min_profit_threshold
        
        # Statistical checks
        if criteria.max_p_value and metrics.p_value:
            checks["statistical_significance"] = metrics.p_value <= criteria.max_p_value
        
        passed = all(checks.values())
        
        logger.info(f"Promotion criteria check for {target_stage.value}: {checks} -> {'PASS' if passed else 'FAIL'}")
        
        return passed
    
    async def _promote_model_to_stage(self, model_name: str, version: str, target_stage: BettingModelStage) -> bool:
        """Promote model to target stage in MLflow registry"""
        try:
            # Map betting stage to MLflow stage
            mlflow_stage_mapping = {
                BettingModelStage.DEVELOPMENT: ModelStage.NONE,
                BettingModelStage.BACKTESTING: ModelStage.NONE,
                BettingModelStage.PAPER_TRADING: ModelStage.STAGING,
                BettingModelStage.STAGING: ModelStage.STAGING,
                BettingModelStage.PRODUCTION: ModelStage.PRODUCTION,
                BettingModelStage.CHAMPION: ModelStage.PRODUCTION,
                BettingModelStage.CHALLENGER: ModelStage.STAGING,
                BettingModelStage.ARCHIVED: ModelStage.ARCHIVED,
                BettingModelStage.DEPRECATED: ModelStage.ARCHIVED
            }
            
            mlflow_stage = mlflow_stage_mapping.get(target_stage, ModelStage.NONE)
            
            # Promote in base registry
            success = await self.base_registry.promote_to_staging(model_name, version, force=True)
            if success and mlflow_stage == ModelStage.PRODUCTION:
                success = await self.base_registry.promote_to_production(model_name, version, force=True)
            
            # Add betting-specific tags
            if success:
                await self._set_model_stage(model_name, version, target_stage)
            
            return success
            
        except Exception as e:
            logger.error(f"Error promoting model to stage: {e}")
            return False
    
    async def _set_model_stage(self, model_name: str, version: str, stage: BettingModelStage):
        """Set betting-specific stage tag on model"""
        try:
            client = MlflowClient()
            client.set_model_version_tag(
                name=model_name,
                version=version,
                key="betting_stage",
                value=stage.value
            )
            
            client.set_model_version_tag(
                name=model_name,
                version=version,
                key="stage_promotion_timestamp",
                value=datetime.utcnow().isoformat()
            )
            
        except Exception as e:
            logger.error(f"Error setting model stage tag: {e}")
    
    async def _setup_stage_monitoring(self, model_name: str, version: str, stage: BettingModelStage):
        """Setup monitoring for model at specific stage"""
        # In production, this would:
        # 1. Configure monitoring dashboards
        # 2. Set up alerting rules
        # 3. Schedule automated validation
        # 4. Configure retraining triggers
        
        logger.info(f"Setup monitoring for {model_name} v{version} at {stage.value} stage")
    
    async def _load_model_performance_history(self):
        """Load model performance history from database"""
        # In production implementation, this would load from database
        logger.debug("Loading model performance history from database")
    
    async def _analyze_model_performance(self, model_name: str, version: str) -> Dict[str, Any]:
        """Analyze current model performance"""
        # Mock analysis - in production would analyze recent performance
        return {
            "model_name": model_name,
            "version": version,
            "current_roi": 5.2,
            "current_win_rate": 0.56,
            "recent_trend": "stable",
            "last_updated": datetime.utcnow().isoformat()
        }
    
    async def _check_model_alerts(self, model_name: str, version: str, model_info: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Check for model performance alerts"""
        alerts = []
        
        # Check for performance degradation
        if model_info.get("current_roi", 0) < 2.0:
            alerts.append({
                "type": "low_performance",
                "model": model_name,
                "version": version,
                "metric": "roi",
                "value": model_info.get("current_roi"),
                "threshold": 2.0,
                "severity": "high"
            })
        
        return alerts
    
    def _generate_monitoring_recommendations(self, alerts: List[Dict[str, Any]]) -> List[str]:
        """Generate recommendations based on monitoring alerts"""
        recommendations = []
        
        high_severity_alerts = [alert for alert in alerts if alert.get("severity") == "high"]
        
        if high_severity_alerts:
            recommendations.append("Immediate attention required for high-severity performance alerts")
            
            for alert in high_severity_alerts:
                if alert["type"] == "low_performance":
                    recommendations.append(f"Consider retraining or replacing model {alert['model']}")
        
        return recommendations


# Global betting model registry instance
betting_model_registry = BettingModelRegistry()