"""
Model Quality Gates
Defines validation thresholds and quality criteria for model deployment.

Issue #42: Automated Model Validation & Testing Pipeline
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional
from enum import Enum


class ValidationStatus(Enum):
    """Validation result status."""
    PASS = "PASS"
    FAIL = "FAIL" 
    WARNING = "WARNING"
    SKIP = "SKIP"


@dataclass
class QualityThreshold:
    """Single quality threshold definition."""
    name: str
    minimum_value: float
    maximum_value: Optional[float] = None
    description: str = ""
    critical: bool = True  # If False, failure is WARNING instead of FAIL


@dataclass 
class ValidationResult:
    """Result of a single validation check."""
    threshold_name: str
    status: ValidationStatus
    actual_value: float
    expected_min: float
    expected_max: Optional[float] = None
    message: str = ""
    critical: bool = True


class ModelQualityGates:
    """
    Model quality gates for automated validation.
    
    Implements validation thresholds from Issue #42:
    - Minimum accuracy: 60% (above MLB 55% baseline)
    - Minimum ROI: 3% for deployment approval
    - Maximum drawdown: 25%
    - Feature importance stability check
    """
    
    def __init__(self):
        self.accuracy_thresholds = {
            "moneyline_accuracy": QualityThreshold(
                name="moneyline_accuracy",
                minimum_value=0.60,  # 60% minimum accuracy
                description="Moneyline prediction accuracy above MLB baseline (55%)",
                critical=True
            ),
            "spread_accuracy": QualityThreshold(
                name="spread_accuracy", 
                minimum_value=0.58,  # Slightly lower for spread betting
                description="Spread prediction accuracy",
                critical=True
            ),
            "total_accuracy": QualityThreshold(
                name="total_accuracy",
                minimum_value=0.56,  # Totals are harder to predict
                description="Over/Under total prediction accuracy", 
                critical=True
            )
        }
        
        self.business_metric_thresholds = {
            "roi_percentage": QualityThreshold(
                name="roi_percentage",
                minimum_value=3.0,  # 3% minimum ROI
                description="Return on Investment percentage for deployment approval",
                critical=True
            ),
            "sharpe_ratio": QualityThreshold(
                name="sharpe_ratio",
                minimum_value=0.5,  # Decent risk-adjusted returns
                description="Risk-adjusted return metric (Sharpe ratio)",
                critical=False  # Warning only
            ),
            "maximum_drawdown": QualityThreshold(
                name="maximum_drawdown",
                minimum_value=0.0,
                maximum_value=25.0,  # 25% max drawdown
                description="Maximum portfolio drawdown percentage",
                critical=True
            ),
            "win_rate": QualityThreshold(
                name="win_rate",
                minimum_value=52.0,  # Need >50% to overcome juice
                description="Percentage of winning bets",
                critical=False
            )
        }
        
        self.stability_thresholds = {
            "feature_importance_drift": QualityThreshold(
                name="feature_importance_drift",
                minimum_value=0.0,
                maximum_value=0.3,  # 30% max drift in feature importance
                description="Feature importance stability check",
                critical=False
            ),
            "prediction_confidence": QualityThreshold(
                name="prediction_confidence",
                minimum_value=0.6,  # 60% minimum confidence
                description="Average model prediction confidence",
                critical=False
            )
        }
        
        self.performance_thresholds = {
            "training_time_minutes": QualityThreshold(
                name="training_time_minutes",
                minimum_value=0.0,
                maximum_value=30.0,  # Max 30 minutes training
                description="Model training time efficiency",
                critical=False
            ),
            "prediction_latency_ms": QualityThreshold(
                name="prediction_latency_ms", 
                minimum_value=0.0,
                maximum_value=500.0,  # Max 500ms prediction latency
                description="Real-time prediction performance",
                critical=False
            )
        }
    
    def get_all_thresholds(self) -> Dict[str, QualityThreshold]:
        """Get all defined quality thresholds."""
        all_thresholds = {}
        all_thresholds.update(self.accuracy_thresholds)
        all_thresholds.update(self.business_metric_thresholds)
        all_thresholds.update(self.stability_thresholds)
        all_thresholds.update(self.performance_thresholds)
        return all_thresholds
    
    def get_critical_thresholds(self) -> Dict[str, QualityThreshold]:
        """Get only critical thresholds (failures block deployment)."""
        all_thresholds = self.get_all_thresholds()
        return {name: threshold for name, threshold in all_thresholds.items() 
                if threshold.critical}
    
    def validate_metric(self, metric_name: str, value: float) -> ValidationResult:
        """
        Validate a single metric against its threshold.
        
        Args:
            metric_name: Name of metric to validate
            value: Actual metric value
            
        Returns:
            ValidationResult with pass/fail status
        """
        all_thresholds = self.get_all_thresholds()
        
        if metric_name not in all_thresholds:
            return ValidationResult(
                threshold_name=metric_name,
                status=ValidationStatus.SKIP,
                actual_value=value,
                expected_min=0.0,
                message=f"No threshold defined for metric: {metric_name}",
                critical=False
            )
        
        threshold = all_thresholds[metric_name]
        status = ValidationStatus.PASS
        message = f"{metric_name}: {value:.3f} meets requirements"
        
        # Check minimum threshold
        if value < threshold.minimum_value:
            status = ValidationStatus.FAIL if threshold.critical else ValidationStatus.WARNING
            message = f"{metric_name}: {value:.3f} below minimum {threshold.minimum_value:.3f}"
        
        # Check maximum threshold if defined
        elif threshold.maximum_value is not None and value > threshold.maximum_value:
            status = ValidationStatus.FAIL if threshold.critical else ValidationStatus.WARNING  
            message = f"{metric_name}: {value:.3f} above maximum {threshold.maximum_value:.3f}"
        
        return ValidationResult(
            threshold_name=metric_name,
            status=status,
            actual_value=value,
            expected_min=threshold.minimum_value,
            expected_max=threshold.maximum_value,
            message=message,
            critical=threshold.critical
        )
    
    def validate_model_metrics(self, metrics: Dict[str, float]) -> Dict[str, ValidationResult]:
        """
        Validate multiple model metrics against quality gates.
        
        Args:
            metrics: Dictionary of metric_name -> value
            
        Returns:
            Dictionary of metric_name -> ValidationResult
        """
        results = {}
        
        for metric_name, value in metrics.items():
            results[metric_name] = self.validate_metric(metric_name, value)
        
        return results
    
    def get_overall_status(self, validation_results: Dict[str, ValidationResult]) -> ValidationStatus:
        """
        Determine overall validation status from individual results.
        
        Args:
            validation_results: Results from validate_model_metrics
            
        Returns:
            Overall validation status (PASS/FAIL/WARNING)
        """
        has_critical_failures = any(
            result.status == ValidationStatus.FAIL and result.critical 
            for result in validation_results.values()
        )
        
        if has_critical_failures:
            return ValidationStatus.FAIL
        
        has_warnings = any(
            result.status in [ValidationStatus.FAIL, ValidationStatus.WARNING]
            for result in validation_results.values()
        )
        
        return ValidationStatus.WARNING if has_warnings else ValidationStatus.PASS
    
    def generate_validation_summary(self, validation_results: Dict[str, ValidationResult]) -> str:
        """
        Generate human-readable validation summary.
        
        Args:
            validation_results: Results from validate_model_metrics
            
        Returns:
            Formatted validation summary string
        """
        overall_status = self.get_overall_status(validation_results)
        
        summary_lines = [
            f"🤖 Model Validation Summary - Status: {overall_status.value}",
            "=" * 60
        ]
        
        # Group results by status
        passed = [r for r in validation_results.values() if r.status == ValidationStatus.PASS]
        failed = [r for r in validation_results.values() if r.status == ValidationStatus.FAIL]
        warnings = [r for r in validation_results.values() if r.status == ValidationStatus.WARNING]
        
        summary_lines.append(f"✅ Passed: {len(passed)}")
        summary_lines.append(f"❌ Failed: {len(failed)}")
        summary_lines.append(f"⚠️  Warnings: {len(warnings)}")
        summary_lines.append("")
        
        # Detail critical failures
        if failed:
            summary_lines.append("❌ Critical Failures:")
            for result in failed:
                summary_lines.append(f"  • {result.message}")
            summary_lines.append("")
        
        # Detail warnings
        if warnings:
            summary_lines.append("⚠️  Warnings:")
            for result in warnings:
                summary_lines.append(f"  • {result.message}")
            summary_lines.append("")
        
        # Deployment recommendation
        if overall_status == ValidationStatus.PASS:
            summary_lines.append("🚀 Recommendation: APPROVED FOR DEPLOYMENT")
        elif overall_status == ValidationStatus.WARNING:
            summary_lines.append("⚠️  Recommendation: DEPLOYMENT WITH CAUTION")
        else:
            summary_lines.append("🚫 Recommendation: DEPLOYMENT BLOCKED")
        
        return "\n".join(summary_lines)