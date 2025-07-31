"""
MLflow Integration Service
Connects MLflow to existing PostgreSQL database and ML tables
"""

import os
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime

import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient
from mlflow.entities import Experiment, Run

from ...core.config import get_settings

logger = logging.getLogger(__name__)


class MLflowService:
    """MLflow integration service that connects to existing PostgreSQL database"""

    def __init__(self):
        self.client: Optional[MlflowClient] = None
        self._setup_mlflow()

    def _setup_mlflow(self):
        """Configure MLflow to use existing PostgreSQL database"""
        try:
            # Get database configuration from existing config
            settings = get_settings()

            # MLflow backend store URI - uses same PostgreSQL database
            # This tells MLflow to store experiment metadata in PostgreSQL
            backend_store_uri = (
                f"postgresql://{settings.database.user}:{settings.database.password}"
                f"@{settings.database.host}:{settings.database.port}/{settings.database.database}"
            )

            # Set MLflow tracking URI
            mlflow.set_tracking_uri(backend_store_uri)

            # Artifact root - local file storage for models, plots, etc.
            artifact_root = os.getenv("MLFLOW_DEFAULT_ARTIFACT_ROOT", "./mlruns")

            logger.info(f"MLflow configured:")
            logger.info(
                f"  Backend store: {settings.database.host}:{settings.database.port}/{settings.database.database}"
            )
            logger.info(f"  Artifact root: {artifact_root}")

            # Initialize MLflow client
            self.client = MlflowClient()

            # Verify connection
            experiments = self.client.search_experiments()
            logger.info(f"✅ MLflow connected - found {len(experiments)} experiments")

        except Exception as e:
            logger.error(f"Failed to setup MLflow: {e}")
            raise

    def create_experiment(
        self, name: str, description: str = None, tags: Dict[str, str] = None
    ) -> str:
        """Create a new MLflow experiment"""
        try:
            # Check if experiment already exists
            experiment = self.client.get_experiment_by_name(name)
            if experiment:
                logger.info(f"Using existing experiment: {name}")
                return experiment.experiment_id

            # Create new experiment
            experiment_id = self.client.create_experiment(
                name=name,
                artifact_location=None,  # Use default artifact root
                tags=tags or {},
            )

            if description:
                self.client.set_experiment_tag(
                    experiment_id, "description", description
                )

            logger.info(f"Created MLflow experiment: {name} (ID: {experiment_id})")
            return experiment_id

        except Exception as e:
            logger.error(f"Failed to create experiment {name}: {e}")
            raise

    def start_run(
        self, experiment_id: str, run_name: str = None, tags: Dict[str, str] = None
    ):
        """Start a new MLflow run"""
        try:
            # Set the experiment
            mlflow.set_experiment(experiment_id=experiment_id)

            # Start run
            run = mlflow.start_run(run_name=run_name, tags=tags or {})

            logger.info(f"Started MLflow run: {run.info.run_id}")
            return run

        except Exception as e:
            logger.error(f"Failed to start MLflow run: {e}")
            raise

    def log_model_metrics(self, metrics: Dict[str, float], step: int = None):
        """Log model performance metrics to MLflow"""
        try:
            for metric_name, value in metrics.items():
                mlflow.log_metric(metric_name, value, step=step)

            logger.debug(f"Logged {len(metrics)} metrics to MLflow")

        except Exception as e:
            logger.error(f"Failed to log metrics: {e}")
            raise

    def log_model_params(self, params: Dict[str, Any]):
        """Log model parameters to MLflow"""
        try:
            for param_name, value in params.items():
                mlflow.log_param(param_name, str(value))

            logger.debug(f"Logged {len(params)} parameters to MLflow")

        except Exception as e:
            logger.error(f"Failed to log parameters: {e}")
            raise

    def log_sklearn_model(self, model, model_name: str, signature=None):
        """Log scikit-learn model to MLflow"""
        try:
            mlflow.sklearn.log_model(
                sk_model=model, artifact_path=model_name, signature=signature
            )

            logger.info(f"Logged sklearn model: {model_name}")

        except Exception as e:
            logger.error(f"Failed to log sklearn model: {e}")
            raise

    def get_experiment_by_name(self, name: str) -> Optional[Experiment]:
        """Get experiment by name"""
        try:
            return self.client.get_experiment_by_name(name)
        except Exception as e:
            logger.error(f"Failed to get experiment {name}: {e}")
            return None

    def get_latest_model(
        self, experiment_name: str, metric_name: str = "accuracy"
    ) -> Optional[Dict[str, Any]]:
        """Get the best model from an experiment based on a metric"""
        try:
            experiment = self.get_experiment_by_name(experiment_name)
            if not experiment:
                return None

            # Search for runs in the experiment
            runs = self.client.search_runs(
                experiment_ids=[experiment.experiment_id],
                order_by=[f"metrics.{metric_name} DESC"],
                max_results=1,
            )

            if not runs:
                return None

            best_run = runs[0]
            return {
                "run_id": best_run.info.run_id,
                "experiment_id": best_run.info.experiment_id,
                "metrics": best_run.data.metrics,
                "params": best_run.data.params,
                "tags": best_run.data.tags,
                "artifact_uri": best_run.info.artifact_uri,
            }

        except Exception as e:
            logger.error(f"Failed to get latest model: {e}")
            return None

    def end_run(self, status: str = "FINISHED"):
        """End the current MLflow run"""
        try:
            mlflow.end_run(status=status)
            logger.debug("Ended MLflow run")

        except Exception as e:
            logger.error(f"Failed to end MLflow run: {e}")

    def sync_with_ml_experiments_table(self, experiment_id: str) -> Dict[str, Any]:
        """
        Sync MLflow experiment data with our custom curated.ml_experiments table
        This bridges MLflow's internal storage with our ML prediction system
        """
        try:
            # Get experiment details from MLflow
            experiment = self.client.get_experiment(experiment_id)

            # Get runs for this experiment
            runs = self.client.search_runs(
                experiment_ids=[experiment_id],
                order_by=["metrics.accuracy DESC"],
                max_results=100,
            )

            # Find best run
            best_run = runs[0] if runs else None

            # Create data structure that matches our curated.ml_experiments table
            experiment_data = {
                "mlflow_experiment_id": experiment_id,
                "experiment_name": experiment.name,
                "experiment_tags": dict(experiment.tags) if experiment.tags else {},
                "status": "active"
                if experiment.lifecycle_stage == "active"
                else "archived",
                "lifecycle_stage": experiment.lifecycle_stage,
                "total_runs": len(runs),
                "created_at": datetime.fromtimestamp(experiment.creation_time / 1000),
                "last_updated": datetime.fromtimestamp(
                    experiment.last_update_time / 1000
                ),
            }

            # Add best run information if available
            if best_run:
                experiment_data.update(
                    {
                        "best_run_id": best_run.info.run_id,
                        "best_accuracy": best_run.data.metrics.get("accuracy"),
                        "best_roi": best_run.data.metrics.get("roi_percentage"),
                    }
                )

            return experiment_data

        except Exception as e:
            logger.error(f"Failed to sync experiment {experiment_id}: {e}")
            raise


# Global MLflow service instance
mlflow_service = MLflowService()


# Example usage and integration patterns
def example_model_training_workflow():
    """
    Example showing how MLflow integrates with our existing ML database tables
    """

    # 1. Create/get experiment (this goes into MLflow's internal tables)
    experiment_id = mlflow_service.create_experiment(
        name="mlb_total_over_prediction_v1",
        description="Predict total over/under for MLB games",
        tags={"model_type": "lightgbm", "target": "total_over"},
    )

    # 2. Start a training run
    with mlflow_service.start_run(experiment_id, "training_run_001") as run:
        # 3. Log model parameters
        mlflow_service.log_model_params(
            {
                "n_estimators": 100,
                "max_depth": 6,
                "learning_rate": 0.1,
                "feature_version": "v2.1",
            }
        )

        # 4. Train model (dummy example)
        # model = train_lightgbm_model(features, target)

        # 5. Log performance metrics
        mlflow_service.log_model_metrics(
            {
                "accuracy": 0.67,
                "precision": 0.65,
                "recall": 0.69,
                "f1_score": 0.67,
                "roc_auc": 0.73,
                "roi_percentage": 8.5,
            }
        )

        # 6. Log the trained model
        # mlflow_service.log_sklearn_model(model, "lightgbm_total_over")

        # 7. After training, predictions go into curated.ml_predictions table
        # This links MLflow run_id to our prediction records
        prediction_data = {
            "model_name": "lightgbm_total_over_v1",
            "model_version": "1.0",
            "experiment_id": experiment_id,
            "run_id": run.info.run_id,
            "feature_version": "v2.1",
            # ... other prediction fields
        }

        # 8. Performance tracking goes into curated.ml_model_performance
        # This links MLflow experiment to our performance tracking
        performance_data = {
            "model_name": "lightgbm_total_over_v1",
            "model_version": "1.0",
            "mlflow_experiment_id": experiment_id,
            "mlflow_run_ids": [run.info.run_id],
            # ... other performance fields
        }

    return experiment_data
