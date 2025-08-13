"""
ML Performance Monitoring System
Comprehensive monitoring for ML models with real-time alerts and performance tracking
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from decimal import Decimal
import json

import asyncpg
from pydantic import BaseModel

from ...core.config import get_settings

logger = logging.getLogger(__name__)


@dataclass
class ModelPerformanceAlert:
    """Alert for model performance issues"""
    model_name: str
    alert_type: str  # 'performance_degradation', 'low_confidence', 'prediction_failure'
    severity: str  # 'critical', 'warning', 'info'
    message: str
    timestamp: datetime
    metrics: Dict[str, Any]


class ModelMetrics(BaseModel):
    """Model performance metrics"""
    model_name: str
    model_version: str
    accuracy: Optional[float] = None
    precision: Optional[float] = None
    recall: Optional[float] = None
    f1_score: Optional[float] = None
    roi_percentage: Optional[float] = None
    total_predictions: int = 0
    correct_predictions: int = 0
    recent_performance_trend: Optional[str] = None  # 'improving', 'stable', 'declining'
    last_prediction_time: Optional[datetime] = None
    avg_confidence: Optional[float] = None


class MLPerformanceMonitor:
    """
    Comprehensive ML performance monitoring system
    Tracks model performance, generates alerts, and provides insights
    """

    def __init__(self):
        self.settings = get_settings()
        self.alerts: List[ModelPerformanceAlert] = []
        self.monitoring_active = False
        
        # Performance thresholds
        self.thresholds = {
            "min_accuracy": 0.52,  # Below random chance + margin
            "min_roi": -0.05,  # 5% loss threshold
            "max_performance_drop": 0.10,  # 10% performance drop
            "min_confidence": 0.60,  # Minimum average confidence
            "max_prediction_age_hours": 25,  # Models should predict at least daily
            "min_predictions_per_day": 1,  # Minimum prediction frequency
        }
        
        # Alert configuration
        self.alert_config = {
            "enable_email_alerts": False,  # Set to True in production
            "alert_cooldown_minutes": 60,  # Prevent spam alerts
            "performance_check_interval_minutes": 30,
            "detailed_analysis_interval_hours": 6,
        }
        
        # Performance history tracking
        self.performance_history: Dict[str, List[ModelMetrics]] = {}

    async def start_monitoring(self) -> None:
        """Start continuous performance monitoring"""
        if self.monitoring_active:
            logger.warning("ML Performance monitoring already active")
            return
            
        self.monitoring_active = True
        logger.info("🚀 Starting ML Performance Monitor")
        
        # Start background monitoring tasks
        asyncio.create_task(self._continuous_performance_check())
        asyncio.create_task(self._detailed_analysis_task())
        
        logger.info("✅ ML Performance Monitor started successfully")

    async def stop_monitoring(self) -> None:
        """Stop performance monitoring"""
        self.monitoring_active = False
        logger.info("🛑 ML Performance Monitor stopped")

    async def get_model_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary for all models"""
        try:
            conn = await asyncpg.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
            )
            
            # Get current model performance metrics
            performance_query = """
                SELECT 
                    mp.model_name,
                    mp.model_version,
                    mp.prediction_type,
                    mp.accuracy,
                    mp.precision_score,
                    mp.recall_score,
                    mp.f1_score,
                    mp.roi_percentage,
                    mp.total_predictions,
                    mp.winning_bets as correct_predictions,
                    mp.evaluation_period_start,
                    mp.evaluation_period_end,
                    mp.hit_rate,
                    mp.sharpe_ratio,
                    mp.max_drawdown_pct
                FROM curated.ml_model_performance mp
                WHERE mp.evaluation_period_end >= NOW() - INTERVAL '7 days'
                ORDER BY mp.evaluation_period_end DESC, mp.roi_percentage DESC
            """
            
            performance_records = await conn.fetch(performance_query)
            
            # Get recent predictions for confidence analysis
            predictions_query = """
                SELECT 
                    pred.model_name,
                    pred.model_version,
                    pred.prediction_timestamp,
                    GREATEST(
                        COALESCE(pred.total_over_confidence, 0),
                        COALESCE(pred.home_ml_confidence, 0),
                        COALESCE(pred.home_spread_confidence, 0)
                    ) as max_confidence
                FROM curated.ml_predictions pred
                WHERE pred.prediction_timestamp >= NOW() - INTERVAL '24 hours'
                ORDER BY pred.prediction_timestamp DESC
            """
            
            recent_predictions = await conn.fetch(predictions_query)
            await conn.close()
            
            # Process model metrics
            model_metrics = {}
            
            for record in performance_records:
                model_key = f"{record['model_name']}_{record['prediction_type']}"
                
                if model_key not in model_metrics:
                    model_metrics[model_key] = ModelMetrics(
                        model_name=record['model_name'],
                        model_version=record['model_version'],
                        accuracy=float(record['accuracy']) if record['accuracy'] else None,
                        precision=float(record['precision_score']) if record['precision_score'] else None,
                        recall=float(record['recall_score']) if record['recall_score'] else None,
                        f1_score=float(record['f1_score']) if record['f1_score'] else None,
                        roi_percentage=float(record['roi_percentage']) if record['roi_percentage'] else None,
                        total_predictions=record['total_predictions'] or 0,
                        correct_predictions=record['correct_predictions'] or 0,
                    )
            
            # Add confidence metrics from recent predictions
            confidence_by_model = {}
            latest_prediction_by_model = {}
            
            for pred in recent_predictions:
                model_name = pred['model_name']
                confidence = float(pred['max_confidence']) if pred['max_confidence'] else 0
                pred_time = pred['prediction_timestamp']
                
                if model_name not in confidence_by_model:
                    confidence_by_model[model_name] = []
                    latest_prediction_by_model[model_name] = pred_time
                else:
                    if pred_time > latest_prediction_by_model[model_name]:
                        latest_prediction_by_model[model_name] = pred_time
                
                if confidence > 0:
                    confidence_by_model[model_name].append(confidence)
            
            # Update metrics with confidence data
            for model_key, metrics in model_metrics.items():
                model_name = metrics.model_name
                
                if model_name in confidence_by_model:
                    confidences = confidence_by_model[model_name]
                    metrics.avg_confidence = sum(confidences) / len(confidences) if confidences else None
                    metrics.last_prediction_time = latest_prediction_by_model[model_name]
                
                # Determine performance trend (simplified)
                if metrics.roi_percentage is not None:
                    if metrics.roi_percentage > 0.02:
                        metrics.recent_performance_trend = "improving"
                    elif metrics.roi_percentage > -0.02:
                        metrics.recent_performance_trend = "stable"
                    else:
                        metrics.recent_performance_trend = "declining"
            
            # Generate summary statistics
            all_metrics = list(model_metrics.values())
            
            summary = {
                "timestamp": datetime.utcnow(),
                "total_models": len(all_metrics),
                "active_models": len([m for m in all_metrics if m.last_prediction_time and 
                                    m.last_prediction_time > datetime.utcnow() - timedelta(hours=25)]),
                "profitable_models": len([m for m in all_metrics if m.roi_percentage and m.roi_percentage > 0]),
                "models_needing_attention": len([m for m in all_metrics if self._model_needs_attention(m)]),
                "average_accuracy": sum(m.accuracy for m in all_metrics if m.accuracy) / len([m for m in all_metrics if m.accuracy]) if any(m.accuracy for m in all_metrics) else 0,
                "average_roi": sum(m.roi_percentage for m in all_metrics if m.roi_percentage) / len([m for m in all_metrics if m.roi_percentage]) if any(m.roi_percentage for m in all_metrics) else 0,
                "total_predictions_24h": sum(1 for pred in recent_predictions),
                "average_confidence": sum(m.avg_confidence for m in all_metrics if m.avg_confidence) / len([m for m in all_metrics if m.avg_confidence]) if any(m.avg_confidence for m in all_metrics) else 0,
                "model_details": {model_key: metrics.model_dump() for model_key, metrics in model_metrics.items()},
                "recent_alerts": [alert.__dict__ for alert in self.alerts[-10:]],  # Last 10 alerts
                "system_health": self._assess_system_health(all_metrics),
            }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting model performance summary: {e}")
            return {
                "error": str(e),
                "timestamp": datetime.utcnow(),
                "system_health": "error"
            }

    async def check_model_alerts(self) -> List[ModelPerformanceAlert]:
        """Check for model performance issues and generate alerts"""
        try:
            summary = await self.get_model_performance_summary()
            new_alerts = []
            
            if "model_details" not in summary:
                return new_alerts
            
            for model_key, metrics_dict in summary["model_details"].items():
                metrics = ModelMetrics(**metrics_dict)
                
                # Check accuracy threshold
                if metrics.accuracy is not None and metrics.accuracy < self.thresholds["min_accuracy"]:
                    alert = ModelPerformanceAlert(
                        model_name=metrics.model_name,
                        alert_type="performance_degradation",
                        severity="warning",
                        message=f"Model accuracy ({metrics.accuracy:.3f}) below threshold ({self.thresholds['min_accuracy']:.3f})",
                        timestamp=datetime.utcnow(),
                        metrics={"accuracy": metrics.accuracy, "threshold": self.thresholds["min_accuracy"]}
                    )
                    new_alerts.append(alert)
                
                # Check ROI threshold
                if metrics.roi_percentage is not None and metrics.roi_percentage < self.thresholds["min_roi"]:
                    alert = ModelPerformanceAlert(
                        model_name=metrics.model_name,
                        alert_type="performance_degradation",
                        severity="critical" if metrics.roi_percentage < -0.10 else "warning",
                        message=f"Model ROI ({metrics.roi_percentage:.1%}) below threshold ({self.thresholds['min_roi']:.1%})",
                        timestamp=datetime.utcnow(),
                        metrics={"roi": metrics.roi_percentage, "threshold": self.thresholds["min_roi"]}
                    )
                    new_alerts.append(alert)
                
                # Check confidence threshold
                if metrics.avg_confidence is not None and metrics.avg_confidence < self.thresholds["min_confidence"]:
                    alert = ModelPerformanceAlert(
                        model_name=metrics.model_name,
                        alert_type="low_confidence",
                        severity="warning",
                        message=f"Average confidence ({metrics.avg_confidence:.3f}) below threshold ({self.thresholds['min_confidence']:.3f})",
                        timestamp=datetime.utcnow(),
                        metrics={"confidence": metrics.avg_confidence, "threshold": self.thresholds["min_confidence"]}
                    )
                    new_alerts.append(alert)
                
                # Check prediction freshness
                if metrics.last_prediction_time:
                    hours_since_prediction = (datetime.utcnow() - metrics.last_prediction_time).total_seconds() / 3600
                    if hours_since_prediction > self.thresholds["max_prediction_age_hours"]:
                        alert = ModelPerformanceAlert(
                            model_name=metrics.model_name,
                            alert_type="prediction_failure",
                            severity="warning",
                            message=f"No recent predictions ({hours_since_prediction:.1f}h since last prediction)",
                            timestamp=datetime.utcnow(),
                            metrics={"hours_since_prediction": hours_since_prediction, "threshold": self.thresholds["max_prediction_age_hours"]}
                        )
                        new_alerts.append(alert)
            
            # Store alerts
            self.alerts.extend(new_alerts)
            
            # Keep only recent alerts (last 24 hours)
            cutoff_time = datetime.utcnow() - timedelta(hours=24)
            self.alerts = [alert for alert in self.alerts if alert.timestamp > cutoff_time]
            
            if new_alerts:
                logger.warning(f"Generated {len(new_alerts)} new ML performance alerts")
                for alert in new_alerts:
                    logger.warning(f"ALERT [{alert.severity.upper()}] {alert.model_name}: {alert.message}")
            
            return new_alerts
            
        except Exception as e:
            logger.error(f"Error checking model alerts: {e}")
            return []

    async def get_performance_recommendations(self) -> List[Dict[str, str]]:
        """Generate actionable recommendations based on model performance"""
        try:
            summary = await self.get_model_performance_summary()
            recommendations = []
            
            if "model_details" not in summary:
                return recommendations
            
            # System-level recommendations
            if summary["average_accuracy"] < 0.55:
                recommendations.append({
                    "type": "system",
                    "priority": "high",
                    "title": "Improve Feature Engineering", 
                    "description": "Overall model accuracy is low. Review feature pipeline and add more predictive features.",
                    "action": "uv run -m src.interfaces.cli ml training retrain --force"
                })
            
            if summary["profitable_models"] == 0:
                recommendations.append({
                    "type": "system",
                    "priority": "critical",
                    "title": "No Profitable Models",
                    "description": "No models are currently profitable. Review strategy parameters and risk management.",
                    "action": "Review model configurations and retrain with adjusted parameters"
                })
            
            if summary["models_needing_attention"] > summary["total_models"] * 0.5:
                recommendations.append({
                    "type": "system", 
                    "priority": "high",
                    "title": "Multiple Models Need Attention",
                    "description": "More than half of your models need attention. Consider systematic review.",
                    "action": "uv run -m src.interfaces.cli ml training daily-workflow --retrain"
                })
            
            # Model-specific recommendations
            for model_key, metrics_dict in summary["model_details"].items():
                metrics = ModelMetrics(**metrics_dict)
                
                if metrics.roi_percentage is not None and metrics.roi_percentage < -0.05:
                    recommendations.append({
                        "type": "model",
                        "priority": "high",
                        "title": f"Retrain {metrics.model_name}",
                        "description": f"Model is losing money (ROI: {metrics.roi_percentage:.1%}). Consider retraining or disabling.",
                        "action": f"uv run -m src.interfaces.cli ml training retrain --model {metrics.model_name}"
                    })
                
                if metrics.avg_confidence and metrics.avg_confidence < 0.6:
                    recommendations.append({
                        "type": "model", 
                        "priority": "medium",
                        "title": f"Low Confidence - {metrics.model_name}",
                        "description": "Model confidence is low. Review feature quality and model calibration.",
                        "action": "Check feature completeness and model parameters"
                    })
            
            # Sort by priority
            priority_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
            recommendations.sort(key=lambda x: priority_order.get(x["priority"], 3))
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Error generating recommendations: {e}")
            return []

    def _model_needs_attention(self, metrics: ModelMetrics) -> bool:
        """Determine if a model needs attention"""
        needs_attention = False
        
        # Check accuracy
        if metrics.accuracy is not None and metrics.accuracy < self.thresholds["min_accuracy"]:
            needs_attention = True
        
        # Check ROI
        if metrics.roi_percentage is not None and metrics.roi_percentage < self.thresholds["min_roi"]:
            needs_attention = True
        
        # Check confidence
        if metrics.avg_confidence is not None and metrics.avg_confidence < self.thresholds["min_confidence"]:
            needs_attention = True
        
        # Check prediction freshness
        if metrics.last_prediction_time:
            hours_since_prediction = (datetime.utcnow() - metrics.last_prediction_time).total_seconds() / 3600
            if hours_since_prediction > self.thresholds["max_prediction_age_hours"]:
                needs_attention = True
        
        return needs_attention

    def _assess_system_health(self, all_metrics: List[ModelMetrics]) -> str:
        """Assess overall system health"""
        if not all_metrics:
            return "no_data"
        
        profitable_pct = len([m for m in all_metrics if m.roi_percentage and m.roi_percentage > 0]) / len(all_metrics)
        attention_pct = len([m for m in all_metrics if self._model_needs_attention(m)]) / len(all_metrics)
        avg_accuracy = sum(m.accuracy for m in all_metrics if m.accuracy) / len([m for m in all_metrics if m.accuracy]) if any(m.accuracy for m in all_metrics) else 0
        
        if profitable_pct >= 0.7 and attention_pct <= 0.2 and avg_accuracy >= 0.58:
            return "excellent"
        elif profitable_pct >= 0.5 and attention_pct <= 0.4 and avg_accuracy >= 0.55:
            return "good"
        elif profitable_pct >= 0.3 and attention_pct <= 0.6:
            return "fair"
        elif profitable_pct >= 0.1:
            return "poor"
        else:
            return "critical"

    async def _continuous_performance_check(self) -> None:
        """Background task for continuous performance monitoring"""
        while self.monitoring_active:
            try:
                await self.check_model_alerts()
                await asyncio.sleep(self.alert_config["performance_check_interval_minutes"] * 60)
            except Exception as e:
                logger.error(f"Error in continuous performance check: {e}")
                await asyncio.sleep(60)  # Short delay before retrying

    async def _detailed_analysis_task(self) -> None:
        """Background task for detailed performance analysis"""
        while self.monitoring_active:
            try:
                summary = await self.get_model_performance_summary()
                recommendations = await self.get_performance_recommendations()
                
                logger.info(f"📊 ML Performance Analysis: {summary['total_models']} models, "
                          f"{summary['profitable_models']} profitable, "
                          f"health: {summary['system_health']}")
                
                if recommendations:
                    logger.info(f"💡 Generated {len(recommendations)} performance recommendations")
                
                await asyncio.sleep(self.alert_config["detailed_analysis_interval_hours"] * 3600)
            except Exception as e:
                logger.error(f"Error in detailed analysis task: {e}")
                await asyncio.sleep(600)  # 10 minute delay before retrying

    async def export_performance_report(self, output_path: str) -> bool:
        """Export comprehensive performance report to file"""
        try:
            summary = await self.get_model_performance_summary()
            recommendations = await self.get_performance_recommendations()
            
            report = {
                "report_generated": datetime.utcnow().isoformat(),
                "system_summary": summary,
                "recommendations": recommendations,
                "alert_history": [alert.__dict__ for alert in self.alerts],
                "monitoring_config": {
                    "thresholds": self.thresholds,
                    "alert_config": self.alert_config,
                }
            }
            
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            logger.info(f"📄 Performance report exported to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting performance report: {e}")
            return False


# Global monitor instance
_ml_monitor: Optional[MLPerformanceMonitor] = None


async def get_ml_performance_monitor() -> MLPerformanceMonitor:
    """Get the global ML performance monitor instance"""
    global _ml_monitor
    if _ml_monitor is None:
        _ml_monitor = MLPerformanceMonitor()
    return _ml_monitor


async def start_ml_monitoring() -> MLPerformanceMonitor:
    """Start ML performance monitoring"""
    monitor = await get_ml_performance_monitor()
    await monitor.start_monitoring()
    return monitor


async def stop_ml_monitoring() -> None:
    """Stop ML performance monitoring"""
    global _ml_monitor
    if _ml_monitor:
        await _ml_monitor.stop_monitoring()