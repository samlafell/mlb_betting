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

from ml.training import MLTrainingService
from core.config import get_settings

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

            # Create training configuration
            training_config = {
                "training_window_days": days,
                "cross_validation_folds": cv_folds,
                "test_size": test_size,
                "use_cached_features": not no_cache,
            }

            if targets:
                available_targets = [
                    "moneyline_home_win",
                    "total_over_under",
                    "run_total_regression",
                ]
                invalid_targets = [t for t in targets if t not in available_targets]
                if invalid_targets:
                    click.echo(f"❌ Invalid targets: {invalid_targets}")
                    click.echo(f"Available targets: {', '.join(available_targets)}")
                    return
                training_config["prediction_targets"] = list(targets)

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

                from ml.training import LightGBMTrainer

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


# Add the ml-training commands to the main CLI
def register_ml_training_commands(main_cli):
    """Register ML training commands with main CLI"""
    main_cli.add_command(ml_training_cli)
