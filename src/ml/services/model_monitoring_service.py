"""
ML Model Performance Monitoring Service
Monitors model performance degradation and triggers alerts
"""

import logging
import asyncio
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import json

from ...core.config import get_settings
from ..database.connection_pool import get_database_connection
from .mlflow_integration import mlflow_service

logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    """Alert severity levels"""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class PerformanceMetrics:
    """Model performance metrics"""

    model_name: str
    model_version: str
    prediction_type: str
    accuracy: float
    precision: float
    recall: float
    f1_score: float
    roc_auc: float
    roi_percentage: float
    sharpe_ratio: float
    max_drawdown_pct: float
    sample_size: int
    evaluation_period_start: datetime
    evaluation_period_end: datetime


@dataclass
class PerformanceAlert:
    """Performance degradation alert"""

    alert_id: str
    model_name: str
    model_version: str
    prediction_type: str
    alert_type: str
    severity: AlertSeverity
    message: str
    current_value: float
    threshold_value: float
    baseline_value: Optional[float]
    created_at: datetime
    metadata: Dict[str, Any]


class ModelMonitoringService:
    """
    Service for monitoring ML model performance and detecting degradation
    """

    def __init__(self):
        self.settings = get_settings()

        # Performance thresholds for alerting
        self.thresholds = {
            "accuracy_drop": 0.05,  # Alert if accuracy drops by 5%
            "roi_drop": 10.0,  # Alert if ROI drops by 10%
            "min_accuracy": 0.52,  # Alert if accuracy drops below 52%
            "min_roi": -5.0,  # Alert if ROI drops below -5%
            "max_drawdown": 15.0,  # Alert if drawdown exceeds 15%
            "min_sample_size": 50,  # Alert if sample size too small
            "sharpe_ratio_drop": 0.5,  # Alert if Sharpe ratio drops significantly
        }

        # Baseline performance cache
        self._baseline_cache = {}
        self._last_cache_update = None
        self.cache_ttl = timedelta(hours=1)

    async def check_model_performance(
        self, model_name: str = None, hours_lookback: int = 24
    ) -> List[PerformanceAlert]:
        """
        Check model performance and generate alerts for degradation

        Args:
            model_name: Specific model to check (None for all models)
            hours_lookback: Hours of data to analyze

        Returns:
            List of performance alerts
        """
        alerts = []

        try:
            # Get recent performance metrics
            recent_metrics = await self._get_recent_performance_metrics(
                model_name, hours_lookback
            )

            # Get baseline performance for comparison
            baseline_metrics = await self._get_baseline_performance_metrics(model_name)

            # Check each model's performance
            for metrics in recent_metrics:
                model_alerts = await self._evaluate_model_performance(
                    metrics,
                    baseline_metrics.get(
                        f"{metrics.model_name}_{metrics.prediction_type}"
                    ),
                )
                alerts.extend(model_alerts)

            # Log monitoring results
            if alerts:
                logger.warning(f"Generated {len(alerts)} performance alerts")
            else:
                logger.info(
                    f"No performance issues detected for {len(recent_metrics)} models"
                )

            return alerts

        except Exception as e:
            logger.error(f"Error checking model performance: {e}")
            return []

    async def _get_recent_performance_metrics(
        self, model_name: str = None, hours_lookback: int = 24
    ) -> List[PerformanceMetrics]:
        """Get recent performance metrics from database"""
        try:
            async with get_database_connection() as conn:
                where_clause = ""
                params = [hours_lookback]

                if model_name:
                    where_clause = "AND mp.model_name = $2"
                    params.append(model_name)

                query = f"""
                    SELECT 
                        mp.model_name,
                        mp.model_version,
                        mp.prediction_type,
                        mp.accuracy,
                        mp.precision_score,
                        mp.recall_score,
                        mp.f1_score,
                        mp.roc_auc_score,
                        mp.roi_percentage,
                        mp.sharpe_ratio,
                        mp.max_drawdown_pct,
                        mp.sample_size,
                        mp.evaluation_period_start,
                        mp.evaluation_period_end
                    FROM curated.ml_model_performance mp
                    WHERE mp.created_at >= NOW() - INTERVAL '%s hours'
                    {where_clause}
                    ORDER BY mp.created_at DESC
                """

                rows = await conn.fetch(query, *params)

                metrics = []
                for row in rows:
                    metrics.append(
                        PerformanceMetrics(
                            model_name=row["model_name"],
                            model_version=row["model_version"],
                            prediction_type=row["prediction_type"],
                            accuracy=float(row["accuracy"] or 0),
                            precision=float(row["precision_score"] or 0),
                            recall=float(row["recall_score"] or 0),
                            f1_score=float(row["f1_score"] or 0),
                            roc_auc=float(row["roc_auc_score"] or 0),
                            roi_percentage=float(row["roi_percentage"] or 0),
                            sharpe_ratio=float(row["sharpe_ratio"] or 0),
                            max_drawdown_pct=float(row["max_drawdown_pct"] or 0),
                            sample_size=int(row["sample_size"] or 0),
                            evaluation_period_start=row["evaluation_period_start"],
                            evaluation_period_end=row["evaluation_period_end"],
                        )
                    )

                return metrics

        except Exception as e:
            logger.error(f"Error fetching recent performance metrics: {e}")
            return []

    async def _get_baseline_performance_metrics(
        self, model_name: str = None
    ) -> Dict[str, PerformanceMetrics]:
        """Get baseline performance metrics for comparison"""
        try:
            # Check cache first
            if (
                self._last_cache_update
                and datetime.utcnow() - self._last_cache_update < self.cache_ttl
            ):
                return self._baseline_cache

            async with get_database_connection() as conn:
                where_clause = ""
                params = []

                if model_name:
                    where_clause = "WHERE mp.model_name = $1"
                    params.append(model_name)

                # Get best historical performance as baseline (last 30 days)
                query = f"""
                    SELECT 
                        mp.model_name,
                        mp.model_version,
                        mp.prediction_type,
                        AVG(mp.accuracy) as avg_accuracy,
                        AVG(mp.precision_score) as avg_precision,
                        AVG(mp.recall_score) as avg_recall,
                        AVG(mp.f1_score) as avg_f1_score,
                        AVG(mp.roc_auc_score) as avg_roc_auc,
                        AVG(mp.roi_percentage) as avg_roi_percentage,
                        AVG(mp.sharpe_ratio) as avg_sharpe_ratio,
                        AVG(mp.max_drawdown_pct) as avg_max_drawdown,
                        SUM(mp.sample_size) as total_sample_size,
                        MIN(mp.evaluation_period_start) as period_start,
                        MAX(mp.evaluation_period_end) as period_end
                    FROM curated.ml_model_performance mp
                    WHERE mp.created_at >= NOW() - INTERVAL '30 days'
                    {where_clause}
                    GROUP BY mp.model_name, mp.model_version, mp.prediction_type
                """

                rows = await conn.fetch(query, *params)

                baseline_metrics = {}
                for row in rows:
                    key = f"{row['model_name']}_{row['prediction_type']}"
                    baseline_metrics[key] = PerformanceMetrics(
                        model_name=row["model_name"],
                        model_version=row["model_version"],
                        prediction_type=row["prediction_type"],
                        accuracy=float(row["avg_accuracy"] or 0),
                        precision=float(row["avg_precision"] or 0),
                        recall=float(row["avg_recall"] or 0),
                        f1_score=float(row["avg_f1_score"] or 0),
                        roc_auc=float(row["avg_roc_auc"] or 0),
                        roi_percentage=float(row["avg_roi_percentage"] or 0),
                        sharpe_ratio=float(row["avg_sharpe_ratio"] or 0),
                        max_drawdown_pct=float(row["avg_max_drawdown"] or 0),
                        sample_size=int(row["total_sample_size"] or 0),
                        evaluation_period_start=row["period_start"],
                        evaluation_period_end=row["period_end"],
                    )

                # Update cache
                self._baseline_cache = baseline_metrics
                self._last_cache_update = datetime.utcnow()

                return baseline_metrics

        except Exception as e:
            logger.error(f"Error fetching baseline performance metrics: {e}")
            return {}

    async def _evaluate_model_performance(
        self, current: PerformanceMetrics, baseline: Optional[PerformanceMetrics]
    ) -> List[PerformanceAlert]:
        """Evaluate model performance and generate alerts"""
        alerts = []

        try:
            # Check absolute thresholds
            alerts.extend(self._check_absolute_thresholds(current))

            # Check relative thresholds (vs baseline)
            if baseline:
                alerts.extend(self._check_relative_thresholds(current, baseline))

            return alerts

        except Exception as e:
            logger.error(f"Error evaluating model performance: {e}")
            return []

    def _check_absolute_thresholds(
        self, metrics: PerformanceMetrics
    ) -> List[PerformanceAlert]:
        """Check absolute performance thresholds"""
        alerts = []

        # Minimum accuracy threshold
        if metrics.accuracy < self.thresholds["min_accuracy"]:
            alerts.append(
                PerformanceAlert(
                    alert_id=f"{metrics.model_name}_{metrics.prediction_type}_low_accuracy_{int(datetime.utcnow().timestamp())}",
                    model_name=metrics.model_name,
                    model_version=metrics.model_version,
                    prediction_type=metrics.prediction_type,
                    alert_type="low_accuracy",
                    severity=AlertSeverity.HIGH,
                    message=f"Model accuracy ({metrics.accuracy:.3f}) below minimum threshold",
                    current_value=metrics.accuracy,
                    threshold_value=self.thresholds["min_accuracy"],
                    baseline_value=None,
                    created_at=datetime.utcnow(),
                    metadata={
                        "sample_size": metrics.sample_size,
                        "evaluation_period": f"{metrics.evaluation_period_start} to {metrics.evaluation_period_end}",
                    },
                )
            )

        # Minimum ROI threshold
        if metrics.roi_percentage < self.thresholds["min_roi"]:
            alerts.append(
                PerformanceAlert(
                    alert_id=f"{metrics.model_name}_{metrics.prediction_type}_low_roi_{int(datetime.utcnow().timestamp())}",
                    model_name=metrics.model_name,
                    model_version=metrics.model_version,
                    prediction_type=metrics.prediction_type,
                    alert_type="low_roi",
                    severity=AlertSeverity.HIGH,
                    message=f"Model ROI ({metrics.roi_percentage:.2f}%) below minimum threshold",
                    current_value=metrics.roi_percentage,
                    threshold_value=self.thresholds["min_roi"],
                    baseline_value=None,
                    created_at=datetime.utcnow(),
                    metadata={
                        "sample_size": metrics.sample_size,
                        "sharpe_ratio": metrics.sharpe_ratio,
                    },
                )
            )

        # Maximum drawdown threshold
        if abs(metrics.max_drawdown_pct) > self.thresholds["max_drawdown"]:
            alerts.append(
                PerformanceAlert(
                    alert_id=f"{metrics.model_name}_{metrics.prediction_type}_high_drawdown_{int(datetime.utcnow().timestamp())}",
                    model_name=metrics.model_name,
                    model_version=metrics.model_version,
                    prediction_type=metrics.prediction_type,
                    alert_type="high_drawdown",
                    severity=AlertSeverity.MEDIUM,
                    message=f"Model drawdown ({abs(metrics.max_drawdown_pct):.2f}%) exceeds threshold",
                    current_value=abs(metrics.max_drawdown_pct),
                    threshold_value=self.thresholds["max_drawdown"],
                    baseline_value=None,
                    created_at=datetime.utcnow(),
                    metadata={
                        "roi_percentage": metrics.roi_percentage,
                        "sample_size": metrics.sample_size,
                    },
                )
            )

        # Sample size threshold
        if metrics.sample_size < self.thresholds["min_sample_size"]:
            alerts.append(
                PerformanceAlert(
                    alert_id=f"{metrics.model_name}_{metrics.prediction_type}_low_sample_size_{int(datetime.utcnow().timestamp())}",
                    model_name=metrics.model_name,
                    model_version=metrics.model_version,
                    prediction_type=metrics.prediction_type,
                    alert_type="low_sample_size",
                    severity=AlertSeverity.LOW,
                    message=f"Model sample size ({metrics.sample_size}) below minimum threshold",
                    current_value=metrics.sample_size,
                    threshold_value=self.thresholds["min_sample_size"],
                    baseline_value=None,
                    created_at=datetime.utcnow(),
                    metadata={
                        "accuracy": metrics.accuracy,
                        "roi_percentage": metrics.roi_percentage,
                    },
                )
            )

        return alerts

    def _check_relative_thresholds(
        self, current: PerformanceMetrics, baseline: PerformanceMetrics
    ) -> List[PerformanceAlert]:
        """Check relative performance thresholds vs baseline"""
        alerts = []

        # Accuracy drop threshold
        accuracy_drop = baseline.accuracy - current.accuracy
        if accuracy_drop > self.thresholds["accuracy_drop"]:
            alerts.append(
                PerformanceAlert(
                    alert_id=f"{current.model_name}_{current.prediction_type}_accuracy_drop_{int(datetime.utcnow().timestamp())}",
                    model_name=current.model_name,
                    model_version=current.model_version,
                    prediction_type=current.prediction_type,
                    alert_type="accuracy_degradation",
                    severity=AlertSeverity.HIGH,
                    message=f"Model accuracy dropped by {accuracy_drop:.3f} from baseline",
                    current_value=current.accuracy,
                    threshold_value=baseline.accuracy
                    - self.thresholds["accuracy_drop"],
                    baseline_value=baseline.accuracy,
                    created_at=datetime.utcnow(),
                    metadata={
                        "accuracy_drop": accuracy_drop,
                        "baseline_sample_size": baseline.sample_size,
                        "current_sample_size": current.sample_size,
                    },
                )
            )

        # ROI drop threshold
        roi_drop = baseline.roi_percentage - current.roi_percentage
        if roi_drop > self.thresholds["roi_drop"]:
            alerts.append(
                PerformanceAlert(
                    alert_id=f"{current.model_name}_{current.prediction_type}_roi_drop_{int(datetime.utcnow().timestamp())}",
                    model_name=current.model_name,
                    model_version=current.model_version,
                    prediction_type=current.prediction_type,
                    alert_type="roi_degradation",
                    severity=AlertSeverity.CRITICAL,
                    message=f"Model ROI dropped by {roi_drop:.2f}% from baseline",
                    current_value=current.roi_percentage,
                    threshold_value=baseline.roi_percentage
                    - self.thresholds["roi_drop"],
                    baseline_value=baseline.roi_percentage,
                    created_at=datetime.utcnow(),
                    metadata={
                        "roi_drop": roi_drop,
                        "current_sharpe": current.sharpe_ratio,
                        "baseline_sharpe": baseline.sharpe_ratio,
                    },
                )
            )

        # Sharpe ratio drop threshold
        sharpe_drop = baseline.sharpe_ratio - current.sharpe_ratio
        if sharpe_drop > self.thresholds["sharpe_ratio_drop"]:
            alerts.append(
                PerformanceAlert(
                    alert_id=f"{current.model_name}_{current.prediction_type}_sharpe_drop_{int(datetime.utcnow().timestamp())}",
                    model_name=current.model_name,
                    model_version=current.model_version,
                    prediction_type=current.prediction_type,
                    alert_type="sharpe_degradation",
                    severity=AlertSeverity.MEDIUM,
                    message=f"Model Sharpe ratio dropped by {sharpe_drop:.3f} from baseline",
                    current_value=current.sharpe_ratio,
                    threshold_value=baseline.sharpe_ratio
                    - self.thresholds["sharpe_ratio_drop"],
                    baseline_value=baseline.sharpe_ratio,
                    created_at=datetime.utcnow(),
                    metadata={
                        "sharpe_drop": sharpe_drop,
                        "current_roi": current.roi_percentage,
                        "baseline_roi": baseline.roi_percentage,
                    },
                )
            )

        return alerts

    async def store_alerts(self, alerts: List[PerformanceAlert]) -> bool:
        """Store performance alerts in database with MLFlow integration"""
        if not alerts:
            return True

        try:
            async with get_database_connection() as conn:
                query = """
                    INSERT INTO curated.ml_performance_alerts (
                        alert_id, model_name, model_version, prediction_type,
                        alert_type, severity, message, current_value, threshold_value,
                        baseline_value, metadata, created_at, mlflow_experiment_id,
                        mlflow_run_id, model_artifact_uri
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                    ON CONFLICT (alert_id) DO NOTHING
                """

                for alert in alerts:
                    # Get MLFlow context for this model
                    mlflow_context = await self._get_mlflow_context(
                        alert.model_name, alert.prediction_type
                    )

                    await conn.execute(
                        query,
                        alert.alert_id,
                        alert.model_name,
                        alert.model_version,
                        alert.prediction_type,
                        alert.alert_type,
                        alert.severity.value,
                        alert.message,
                        alert.current_value,
                        alert.threshold_value,
                        alert.baseline_value,
                        json.dumps(alert.metadata),
                        alert.created_at,
                        mlflow_context.get("experiment_id"),
                        mlflow_context.get("run_id"),
                        mlflow_context.get("artifact_uri"),
                    )

                logger.info(
                    f"Stored {len(alerts)} performance alerts with MLFlow context"
                )
                return True

        except Exception as e:
            logger.error(f"Error storing performance alerts: {e}")
            return False

    async def _get_mlflow_context(
        self, model_name: str, prediction_type: str
    ) -> Dict[str, Any]:
        """Get MLFlow experiment and run context for a model"""
        try:
            # Map prediction type to experiment name
            experiment_name = f"{model_name}_{prediction_type}"

            # Get latest model from MLFlow
            latest_model = mlflow_service.get_latest_model(experiment_name)

            if latest_model:
                return {
                    "experiment_id": latest_model["experiment_id"],
                    "run_id": latest_model["run_id"],
                    "artifact_uri": latest_model["artifact_uri"],
                }

            # Fallback: try to get experiment by name
            experiment = mlflow_service.get_experiment_by_name(experiment_name)
            if experiment:
                return {
                    "experiment_id": experiment.experiment_id,
                    "run_id": None,
                    "artifact_uri": None,
                }

            return {}

        except Exception as e:
            logger.warning(f"Could not get MLFlow context for {model_name}: {e}")
            return {}

    async def run_monitoring_cycle(self) -> Dict[str, Any]:
        """Run a complete monitoring cycle"""
        try:
            start_time = datetime.utcnow()

            # Check model performance
            alerts = await self.check_model_performance()

            # Store alerts
            if alerts:
                await self.store_alerts(alerts)

            # Log summary
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()

            summary = {
                "monitoring_cycle_completed": True,
                "alerts_generated": len(alerts),
                "duration_seconds": duration,
                "timestamp": end_time,
                "alerts_by_severity": {
                    severity.value: len([a for a in alerts if a.severity == severity])
                    for severity in AlertSeverity
                },
            }

            logger.info(f"Monitoring cycle completed: {summary}")
            return summary

        except Exception as e:
            logger.error(f"Error in monitoring cycle: {e}")
            return {
                "monitoring_cycle_completed": False,
                "error": str(e),
                "timestamp": datetime.utcnow(),
            }


# Global monitoring service instance
_monitoring_service: Optional[ModelMonitoringService] = None


async def get_monitoring_service() -> ModelMonitoringService:
    """Get the global monitoring service instance"""
    global _monitoring_service

    if _monitoring_service is None:
        _monitoring_service = ModelMonitoringService()

    return _monitoring_service
