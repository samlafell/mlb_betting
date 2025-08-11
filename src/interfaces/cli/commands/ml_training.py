"""
ML Training CLI Commands
Command-line interface for LightGBM training pipeline
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, List
import click

# Add src to path for imports
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

from src.ml.training.training_service import MLTrainingService
from src.core.config import get_settings

# Import ML monitoring
try:
    from src.ml.monitoring.ml_performance_monitor import get_ml_performance_monitor, start_ml_monitoring
    ML_MONITORING_AVAILABLE = True
except ImportError:
    ML_MONITORING_AVAILABLE = False

logger = logging.getLogger(__name__)


@click.group(name="ml-training")
def ml_training_cli():
    """ML training pipeline commands"""
    pass


@ml_training_cli.command("train")
@click.option(
    "--targets", multiple=True, help="Prediction targets to train (default: all)"
)
@click.option("--days", default=90, help="Training window in days (default: 90)")
@click.option("--end-date", help="End date for training data (YYYY-MM-DD format)")
@click.option("--cv-folds", default=5, help="Cross-validation folds (default: 5)")
@click.option("--test-size", default=0.2, help="Test set proportion (default: 0.2)")
@click.option("--no-cache", is_flag=True, help="Disable Redis feature caching")
def train_models(
    targets: tuple,
    days: int,
    end_date: Optional[str],
    cv_folds: int,
    test_size: float,
    no_cache: bool,
):
    """Train LightGBM models for MLB betting predictions"""

    async def _train():
        try:
            click.echo("🚀 Starting ML training pipeline...")

            # Initialize training service
            service = MLTrainingService()

            # Parse end date
            parsed_end_date = None
            if end_date:
                try:
                    parsed_end_date = datetime.strptime(end_date, "%Y-%m-%d")
                except ValueError:
                    click.echo(f"❌ Invalid date format: {end_date}. Use YYYY-MM-DD")
                    return

            # Define available targets
            available_targets = [
                "moneyline_home_win",
                "total_over_under",
                "run_total_regression",
            ]
            
            # Validate and set prediction targets
            if targets:
                invalid_targets = [t for t in targets if t not in available_targets]
                if invalid_targets:
                    click.echo(f"❌ Invalid targets: {invalid_targets}")
                    click.echo(f"Available targets: {', '.join(available_targets)}")
                    return
                prediction_targets = list(targets)
            else:
                prediction_targets = available_targets  # Use all targets by default
            
            # Create training configuration
            training_config = {
                "prediction_targets": prediction_targets,
                "training_window_days": days,
                "cross_validation_folds": cv_folds,
                "test_size": test_size,
                "use_cached_features": not no_cache,
            }

            # Start training
            results = await service.train_initial_models(
                end_date=parsed_end_date, training_config=training_config
            )

            # Display results
            click.echo(f"\n✅ Training completed successfully!")
            click.echo(f"📊 Models trained: {len(results['models_trained'])}")
            click.echo(
                f"📈 Data samples: {results['training_results']['data_samples']}"
            )
            click.echo(
                f"⏱️  Training time: {results['training_results']['training_time_seconds']:.1f}s"
            )

            click.echo(f"\n🎯 Model Performance:")
            for model_name in results["models_trained"]:
                model_results = results["training_results"]["training_results"].get(
                    model_name, {}
                )
                test_metrics = model_results.get("test_metrics", {})

                click.echo(f"\n  {model_name}:")
                for metric, value in test_metrics.items():
                    click.echo(f"    {metric}: {value:.4f}")

            if results.get("deployment_ready"):
                click.echo(f"\n🚀 Models ready for deployment!")

        except Exception as e:
            click.echo(f"❌ Error during training: {e}")
            logger.error(f"Training error: {e}")

    asyncio.run(_train())


@ml_training_cli.command("retrain")
@click.option("--model", help="Specific model to retrain (default: check all)")
@click.option("--window-days", default=7, help="Sliding window in days (default: 7)")
@click.option(
    "--force", is_flag=True, help="Force retraining regardless of performance"
)
@click.option(
    "--min-samples", default=100, help="Minimum samples for retraining (default: 100)"
)
def retrain_models(
    model: Optional[str], window_days: int, force: bool, min_samples: int
):
    """Retrain models with recent data"""

    async def _retrain():
        try:
            click.echo("🔄 Checking models for retraining...")

            service = MLTrainingService()

            if model:
                # Retrain specific model
                click.echo(f"🎯 Retraining model: {model}")

                # Update retraining config
                service.retraining_config.update(
                    {
                        "sliding_window_days": window_days,
                        "min_samples_for_retrain": min_samples,
                    }
                )

                from src.ml.training.lightgbm_trainer import LightGBMTrainer

                trainer = LightGBMTrainer()

                results = await trainer.retrain_model(
                    model_name=model,
                    sliding_window_days=window_days,
                    min_samples=min_samples,
                )

                if results.get("status") == "skipped":
                    click.echo(f"⏭️  Retraining skipped: {results.get('reason')}")
                else:
                    click.echo(f"✅ Model {model} retrained successfully!")
                    if results.get("drift_detected"):
                        click.echo(f"⚠️  Feature drift detected!")

                    test_metrics = results.get("test_metrics", {})
                    for metric, value in test_metrics.items():
                        click.echo(f"  {metric}: {value:.4f}")
            else:
                # Check and retrain all models
                results = await service.check_and_retrain_models(force_retrain=force)

                retrained_count = results["models_retrained"]
                total_checked = len(results["retrain_results"])

                click.echo(f"✅ Retraining check completed!")
                click.echo(f"📊 Models checked: {total_checked}")
                click.echo(f"🔄 Models retrained: {retrained_count}")

                for model_name, result in results["retrain_results"].items():
                    if result.get("status") != "skipped":
                        click.echo(f"\n  {model_name}: retrained")
                        if result.get("drift_detected"):
                            click.echo(f"    ⚠️  Feature drift detected")
                    else:
                        click.echo(
                            f"\n  {model_name}: skipped ({result.get('reason')})"
                        )

        except Exception as e:
            click.echo(f"❌ Error during retraining: {e}")
            logger.error(f"Retraining error: {e}")

    asyncio.run(_retrain())


@ml_training_cli.command("evaluate")
@click.argument("model_name")
@click.option("--days", default=7, help="Evaluation period in days (default: 7)")
def evaluate_model(model_name: str, days: int):
    """Evaluate model performance on recent data"""

    async def _evaluate():
        try:
            click.echo(f"📊 Evaluating model: {model_name}")

            service = MLTrainingService()

            results = await service.evaluate_model_performance(
                model_name=model_name, evaluation_days=days
            )

            click.echo(f"✅ Evaluation completed!")
            click.echo(f"📈 Samples evaluated: {results['samples_evaluated']}")
            click.echo(f"🎯 Performance metrics:")

            for metric, value in results["performance_metrics"].items():
                click.echo(f"  {metric}: {value:.4f}")

        except Exception as e:
            click.echo(f"❌ Error during evaluation: {e}")
            logger.error(f"Evaluation error: {e}")

    asyncio.run(_evaluate())


@ml_training_cli.command("status")
@click.option("--model", help="Show status for specific model")
@click.option("--detailed", is_flag=True, help="Show detailed information")
def show_status(model: Optional[str], detailed: bool):
    """Show ML training service status"""

    async def _status():
        try:
            service = MLTrainingService()

            if model:
                # Show specific model info
                info = await service.get_model_info(model_name=model)
                model_info = info["model_info"]

                click.echo(f"🎯 Model: {model}")
                click.echo(f"📅 Trained: {model_info['trained_at']}")
                click.echo(f"📊 Training samples: {model_info['training_samples']}")
                click.echo(f"🎯 Status: {model_info['status']}")

                if "drift_detected" in model_info:
                    drift_status = "Yes" if model_info["drift_detected"] else "No"
                    click.echo(f"⚠️  Drift detected: {drift_status}")

                if detailed:
                    click.echo(f"\n📈 Performance metrics:")
                    for metric, value in model_info["performance_metrics"].items():
                        click.echo(f"  {metric}: {value:.4f}")

                    if "best_scores" in model_info:
                        click.echo(f"\n🏆 Best scores:")
                        for metric, value in model_info["best_scores"].items():
                            click.echo(f"  {metric}: {value:.4f}")
            else:
                # Show all models and service status
                info = await service.get_model_info()
                health = await service.health_check()

                click.echo(f"🚀 ML Training Service Status")
                click.echo(f"📊 Overall health: {health['overall_health']}")
                click.echo(f"🎯 Active models: {health['active_models_count']}")
                click.echo(f"📅 Last training: {health['last_training_run']}")
                click.echo(f"🔄 Last retrain check: {health['last_retrain_check']}")

                if health.get("stale_models"):
                    click.echo(f"⚠️  Stale models: {', '.join(health['stale_models'])}")

                click.echo(f"\n🎯 Active Models:")
                for model_name, model_info in info["active_models"].items():
                    status_icon = "✅" if model_info["status"] == "active" else "❌"
                    click.echo(f"  {status_icon} {model_name}")
                    click.echo(f"    📅 Trained: {model_info['trained_at']}")
                    click.echo(f"    📊 Samples: {model_info['training_samples']}")

                if detailed:
                    service_stats = info["service_stats"]
                    click.echo(f"\n📈 Training Statistics:")
                    click.echo(f"  Models trained: {service_stats['models_trained']}")
                    click.echo(
                        f"  Experiments logged: {service_stats['experiments_logged']}"
                    )
                    click.echo(
                        f"  Avg training time: {service_stats['avg_training_time_seconds']:.1f}s"
                    )

        except Exception as e:
            click.echo(f"❌ Error getting status: {e}")
            logger.error(f"Status error: {e}")

    asyncio.run(_status())


@ml_training_cli.command("schedule")
@click.option(
    "--schedule-type",
    type=click.Choice(["daily", "weekly"]),
    default="daily",
    help="Schedule type",
)
@click.option("--hour", default=2, help="Hour to run (0-23, default: 2)")
@click.option("--days", default=90, help="Training window in days")
def schedule_training(schedule_type: str, hour: int, days: int):
    """Schedule automated training jobs"""

    async def _schedule():
        try:
            service = MLTrainingService()

            training_config = {
                "training_window_days": days,
                "cross_validation_folds": 5,
                "test_size": 0.2,
                "use_cached_features": True,
            }

            results = await service.schedule_training_job(
                schedule_type=schedule_type,
                schedule_hour=hour,
                training_config=training_config,
            )

            click.echo(f"✅ Training job scheduled!")
            click.echo(f"📅 Schedule: {schedule_type} at {hour}:00 UTC")
            click.echo(f"⏭️  Next run: {results['next_run']}")
            click.echo(f"📊 Training window: {days} days")

        except Exception as e:
            click.echo(f"❌ Error scheduling training: {e}")
            logger.error(f"Scheduling error: {e}")

    asyncio.run(_schedule())


@ml_training_cli.command("predict")
@click.option("--date", help="Date for predictions (YYYY-MM-DD format, default: today)")
@click.option("--save-to-db", is_flag=True, help="Save predictions to database")
@click.option("--confidence-threshold", default=0.7, help="Minimum confidence threshold")
def generate_predictions(date: Optional[str], save_to_db: bool, confidence_threshold: float):
    """Generate daily predictions for today's games"""

    async def _predict():
        try:
            service = MLTrainingService()

            # Parse target date
            target_date = None
            if date:
                try:
                    target_date = datetime.strptime(date, "%Y-%m-%d")
                except ValueError:
                    click.echo(f"❌ Invalid date format: {date}. Use YYYY-MM-DD")
                    return

            click.echo(f"🔮 Generating daily predictions...")
            
            results = await service.generate_daily_predictions(target_date)

            if results["status"] == "no_games":
                click.echo(f"ℹ️  No games found for {results['date']}")
                return

            click.echo(f"\n✅ Daily predictions generated!")
            click.echo(f"📅 Date: {results['date']}")
            click.echo(f"🎮 Games processed: {results['games_processed']}")
            click.echo(f"🔮 Predictions generated: {results['predictions_generated']}")

            # Display predictions
            click.echo(f"\n🎯 Game Predictions:")
            for game_pred in results["predictions"]:
                click.echo(f"\n  {game_pred['away_team']} @ {game_pred['home_team']}")
                click.echo(f"    Game time: {game_pred['game_datetime']}")
                
                for model_name, prediction in game_pred["predictions"].items():
                    confidence = prediction["confidence"]
                    if confidence >= confidence_threshold:
                        status_icon = "🟢"
                    elif confidence >= 0.6:
                        status_icon = "🟡"
                    else:
                        status_icon = "🔴"
                    
                    click.echo(f"    {status_icon} {model_name}: {prediction['probability']:.3f} (confidence: {confidence:.3f})")

        except Exception as e:
            click.echo(f"❌ Error generating predictions: {e}")
            logger.error(f"Prediction error: {e}")

    asyncio.run(_predict())


@ml_training_cli.command("performance")
@click.option("--days", default=7, help="Performance analysis period in days")
@click.option("--model", help="Specific model to analyze (default: all)")
@click.option("--detailed", is_flag=True, help="Show detailed performance breakdown")
def analyze_performance(days: int, model: Optional[str], detailed: bool):
    """Analyze model performance and ROI metrics"""

    async def _analyze():
        try:
            import asyncpg
            from src.core.config import get_settings
            
            settings = get_settings()
            
            click.echo(f"📊 Analyzing model performance over {days} days...")
            
            conn = await asyncpg.connect(
                host=settings.database.host,
                port=settings.database.port,
                database=settings.database.database,
                user=settings.database.user,
                password=settings.database.password,
            )
            
            # Query for recent model performance
            performance_query = """
                SELECT 
                    mp.model_name,
                    mp.prediction_type,
                    mp.accuracy,
                    mp.total_predictions,
                    mp.winning_bets,
                    mp.hit_rate,
                    mp.roi_percentage,
                    mp.evaluation_period_start,
                    mp.evaluation_period_end
                FROM curated.ml_model_performance mp
                WHERE mp.evaluation_period_end >= NOW() - INTERVAL '%s days'
            """ % days
            
            if model:
                performance_query += f" AND mp.model_name = '{model}'"
                
            performance_query += " ORDER BY mp.evaluation_period_end DESC"
            
            performance_records = await conn.fetch(performance_query)
            
            if not performance_records:
                click.echo(f"ℹ️  No performance data found for the last {days} days")
                await conn.close()
                return
            
            click.echo(f"\n📈 Performance Summary:")
            
            for record in performance_records:
                model_name = record["model_name"]
                prediction_type = record["prediction_type"]
                accuracy = record["accuracy"] or 0
                hit_rate = record["hit_rate"] or 0
                roi = record["roi_percentage"] or 0
                total_preds = record["total_predictions"] or 0
                
                performance_icon = "🟢" if roi > 5 else "🟡" if roi > 0 else "🔴"
                
                click.echo(f"\n  {performance_icon} {model_name} ({prediction_type})")
                click.echo(f"    Accuracy: {accuracy:.3f} | Hit Rate: {hit_rate:.3f}")
                click.echo(f"    ROI: {roi:.2f}% | Predictions: {total_preds}")
                click.echo(f"    Period: {record['evaluation_period_start']} to {record['evaluation_period_end']}")
                
                if detailed:
                    winning_bets = record["winning_bets"] or 0
                    click.echo(f"    Winning bets: {winning_bets}/{total_preds}")
            
            await conn.close()
            
        except Exception as e:
            click.echo(f"❌ Error analyzing performance: {e}")
            logger.error(f"Performance analysis error: {e}")

    asyncio.run(_analyze())


@ml_training_cli.command("daily-workflow")
@click.option("--retrain", is_flag=True, help="Check and retrain models before predictions")
@click.option("--confidence-threshold", default=0.7, help="Minimum confidence for betting recommendations")
def daily_workflow(retrain: bool, confidence_threshold: float):
    """Execute complete daily ML workflow: retrain models and generate predictions"""

    async def _daily_workflow():
        try:
            service = MLTrainingService()
            
            click.echo(f"🚀 Starting daily ML workflow...")
            
            # Step 1: Health check
            health = await service.health_check()
            click.echo(f"📊 Service health: {health['overall_health']}")
            
            if health["overall_health"] == "error":
                click.echo(f"❌ Service unhealthy, aborting workflow")
                return
            
            # Step 2: Model retraining (if requested or needed)
            if retrain or health.get("models_need_attention", False):
                click.echo(f"\n🔄 Checking models for retraining...")
                
                retrain_results = await service.check_and_retrain_models()
                retrained_count = retrain_results["models_retrained"]
                
                if retrained_count > 0:
                    click.echo(f"✅ Retrained {retrained_count} models")
                else:
                    click.echo(f"ℹ️  No models needed retraining")
            
            # Step 3: Generate daily predictions
            click.echo(f"\n🔮 Generating today's predictions...")
            
            predictions = await service.generate_daily_predictions()
            
            if predictions["status"] == "success":
                click.echo(f"✅ Generated predictions for {predictions['predictions_generated']} games")
                
                # Show high-confidence predictions
                high_confidence_count = 0
                for game_pred in predictions["predictions"]:
                    for model_name, prediction in game_pred["predictions"].items():
                        if prediction["confidence"] >= confidence_threshold:
                            high_confidence_count += 1
                            break
                
                click.echo(f"🎯 High-confidence predictions ({confidence_threshold}+ confidence): {high_confidence_count}")
                
            else:
                click.echo(f"ℹ️  {predictions['status']} - No predictions generated")
            
            # Step 4: Performance summary
            click.echo(f"\n📈 Workflow completed successfully!")
            
            final_health = await service.health_check()
            click.echo(f"📊 Final service health: {final_health['overall_health']}")
            click.echo(f"🕐 Last predictions generated: {final_health['daily_predictions_generated']}")
            
        except Exception as e:
            click.echo(f"❌ Error in daily workflow: {e}")
            logger.error(f"Daily workflow error: {e}")

    asyncio.run(_daily_workflow())


@ml_training_cli.command("monitor")
@click.option("--start", is_flag=True, help="Start continuous monitoring")
@click.option("--status", is_flag=True, help="Show monitoring status")
@click.option("--alerts", is_flag=True, help="Show recent alerts")
@click.option("--export", help="Export performance report to file")
def monitor_performance(start: bool, status: bool, alerts: bool, export: Optional[str]):
    """Monitor ML model performance and generate alerts"""

    async def _monitor():
        try:
            if not ML_MONITORING_AVAILABLE:
                click.echo(f"❌ ML monitoring not available - missing dependencies")
                return

            monitor = await get_ml_performance_monitor()

            if start:
                click.echo(f"🚀 Starting ML performance monitoring...")
                await start_ml_monitoring()
                click.echo(f"✅ ML monitoring started successfully")
                click.echo(f"📊 Monitor will check performance every 30 minutes")
                click.echo(f"⚠️  Alerts will be generated for performance issues")
                
                # Keep monitoring running
                try:
                    while True:
                        await asyncio.sleep(60)
                except KeyboardInterrupt:
                    click.echo(f"\n🛑 Stopping ML monitoring...")
                    await monitor.stop_monitoring()
                    return

            if status or (not start and not alerts and not export):
                click.echo(f"📊 Getting ML performance status...")
                
                summary = await monitor.get_model_performance_summary()
                
                click.echo(f"\n🎯 ML System Performance Summary:")
                click.echo(f"   📈 Total models: {summary.get('total_models', 0)}")
                click.echo(f"   🟢 Active models: {summary.get('active_models', 0)}")
                click.echo(f"   💰 Profitable models: {summary.get('profitable_models', 0)}")
                click.echo(f"   ⚠️  Models needing attention: {summary.get('models_needing_attention', 0)}")
                
                avg_accuracy = summary.get('average_accuracy', 0)
                avg_roi = summary.get('average_roi', 0)
                avg_confidence = summary.get('average_confidence', 0)
                
                click.echo(f"\n📊 Performance Metrics:")
                click.echo(f"   🎯 Average accuracy: {avg_accuracy:.1%}")
                click.echo(f"   💵 Average ROI: {avg_roi:+.1%}")
                click.echo(f"   🔒 Average confidence: {avg_confidence:.1%}")
                click.echo(f"   🔮 Predictions (24h): {summary.get('total_predictions_24h', 0)}")
                
                health = summary.get('system_health', 'unknown')
                health_icons = {
                    'excellent': '🟢',
                    'good': '🟡', 
                    'fair': '🟠',
                    'poor': '🔴',
                    'critical': '🚨',
                    'error': '❌'
                }
                health_icon = health_icons.get(health, '❓')
                
                click.echo(f"\n{health_icon} System Health: {health.upper()}")

            if alerts:
                click.echo(f"\n⚠️  Checking for recent alerts...")
                
                recent_alerts = await monitor.check_model_alerts()
                
                if recent_alerts:
                    click.echo(f"\n🚨 New Performance Alerts ({len(recent_alerts)}):")
                    for alert in recent_alerts:
                        severity_icon = "🚨" if alert.severity == "critical" else "⚠️" if alert.severity == "warning" else "ℹ️"
                        click.echo(f"   {severity_icon} [{alert.severity.upper()}] {alert.model_name}")
                        click.echo(f"      {alert.message}")
                        click.echo(f"      Type: {alert.alert_type} | Time: {alert.timestamp}")
                else:
                    click.echo(f"✅ No new alerts - all models performing within thresholds")
                
                # Show recommendations
                recommendations = await monitor.get_performance_recommendations()
                if recommendations:
                    click.echo(f"\n💡 Performance Recommendations ({len(recommendations)}):")
                    for i, rec in enumerate(recommendations[:5], 1):  # Show top 5
                        priority_icon = "🚨" if rec["priority"] == "critical" else "⚠️" if rec["priority"] == "high" else "💡"
                        click.echo(f"   {i}. {priority_icon} {rec['title']}")
                        click.echo(f"      {rec['description']}")
                        if rec.get('action'):
                            click.echo(f"      Action: {rec['action']}")

            if export:
                click.echo(f"📄 Exporting performance report to {export}...")
                
                success = await monitor.export_performance_report(export)
                if success:
                    click.echo(f"✅ Performance report exported successfully")
                    click.echo(f"📁 Report saved to: {export}")
                else:
                    click.echo(f"❌ Failed to export performance report")

        except Exception as e:
            click.echo(f"❌ Error in monitoring: {e}")
            logger.error(f"Monitoring error: {e}")

    asyncio.run(_monitor())


@ml_training_cli.command("health")
@click.option("--detailed", is_flag=True, help="Show detailed health information")
def health_check(detailed: bool):
    """Comprehensive ML system health check"""

    async def _health_check():
        try:
            service = MLTrainingService()
            
            click.echo(f"🏥 ML System Health Check")
            
            # Basic service health
            health = await service.health_check()
            
            overall_health = health.get('overall_health', 'unknown')
            health_color = "green" if overall_health == "healthy" else "yellow" if overall_health == "degraded" else "red"
            
            click.echo(f"\n📊 Service Status: {overall_health.upper()}")
            click.echo(f"   🤖 Active models: {health.get('active_models_count', 0)}")
            click.echo(f"   🚂 Training in progress: {health.get('training_in_progress', False)}")
            click.echo(f"   📅 Last training: {health.get('last_training_run', 'Never')}")
            click.echo(f"   🔄 Last retrain check: {health.get('last_retrain_check', 'Never')}")
            click.echo(f"   🔮 Daily predictions: {health.get('daily_predictions_generated', 'Never')}")
            click.echo(f"   📦 MLflow available: {health.get('mlflow_available', False)}")
            
            if health.get('stale_models'):
                click.echo(f"   ⚠️  Stale models: {', '.join(health['stale_models'])}")
            
            # Performance monitoring health (if available)
            if ML_MONITORING_AVAILABLE:
                monitor = await get_ml_performance_monitor()
                perf_summary = await monitor.get_model_performance_summary()
                
                click.echo(f"\n📈 Performance Monitoring:")
                click.echo(f"   💰 Profitable models: {perf_summary.get('profitable_models', 0)}/{perf_summary.get('total_models', 0)}")
                click.echo(f"   ⚠️  Models needing attention: {perf_summary.get('models_needing_attention', 0)}")
                click.echo(f"   🎯 Average accuracy: {perf_summary.get('average_accuracy', 0):.1%}")
                click.echo(f"   💵 Average ROI: {perf_summary.get('average_roi', 0):+.1%}")
                
                if detailed:
                    model_details = perf_summary.get('model_details', {})
                    if model_details:
                        click.echo(f"\n🤖 Individual Model Status:")
                        for model_key, metrics in model_details.items():
                            model_name = metrics['model_name']
                            accuracy = metrics.get('accuracy', 0) or 0
                            roi = metrics.get('roi_percentage', 0) or 0
                            confidence = metrics.get('avg_confidence', 0) or 0
                            
                            status_icon = "🟢" if roi > 0 and accuracy > 0.52 else "🟡" if roi > -0.05 else "🔴"
                            click.echo(f"   {status_icon} {model_name}: accuracy={accuracy:.1%}, roi={roi:+.1%}, confidence={confidence:.1%}")
            
            # Recommendations based on health check
            if overall_health in ["degraded", "unhealthy", "no_models"]:
                click.echo(f"\n💡 Recommendations:")
                if health.get('active_models_count', 0) == 0:
                    click.echo(f"   🚀 Train initial models: uv run -m src.interfaces.cli ml training train")
                if health.get('stale_models'):
                    click.echo(f"   🔄 Retrain stale models: uv run -m src.interfaces.cli ml training retrain --force")
                if not health.get('daily_predictions_generated'):
                    click.echo(f"   🔮 Generate predictions: uv run -m src.interfaces.cli ml training predict")
            
        except Exception as e:
            click.echo(f"❌ Error in health check: {e}")
            logger.error(f"Health check error: {e}")

    asyncio.run(_health_check())


# Add the ml-training commands to the main CLI
def register_ml_training_commands(main_cli):
    """Register ML training commands with main CLI"""
    main_cli.add_command(ml_training_cli)
