"""
ML Experiment Manager

Provides high-level experiment management connecting to existing MLflow container.
Bridges MLflow tracking with curated zone data for comprehensive ML workflows.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union
from pathlib import Path

import mlflow
from mlflow.tracking import MlflowClient
from mlflow.entities import Experiment, Run

from ...core.config import get_settings
from ...core.logging import LogComponent, get_logger
from ...data.database.connection import get_connection
from ..services.mlflow_integration import mlflow_service

logger = get_logger(__name__, LogComponent.CORE)


class ExperimentManager:
    """
    High-level experiment manager for MLB ML experiments.
    
    Connects to existing MLflow container at localhost:5001 and integrates
    with curated zone data for comprehensive ML experiment tracking.
    """
    
    def __init__(self):
        self.settings = get_settings()
        self.mlflow_client = mlflow_service.client
        self.experiment_cache = {}
        
        # Set MLflow tracking URI to existing container
        mlflow.set_tracking_uri("http://localhost:5001")
        
    async def create_experiment(
        self,
        name: str,
        description: Optional[str] = None,
        model_type: str = "classification",
        target_variable: str = "sharp_action",
        data_start_date: Optional[str] = None,
        data_end_date: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Create a new ML experiment with business context.
        
        Args:
            name: Experiment name (e.g., "sharp_action_classifier_v2")
            description: Experiment description
            model_type: Type of model (classification, regression, etc.)
            target_variable: Target variable being predicted
            data_start_date: Start date for training data (YYYY-MM-DD)
            data_end_date: End date for training data (YYYY-MM-DD)
            tags: Additional experiment tags
            
        Returns:
            Dictionary with experiment information
        """
        try:
            logger.info(f"Creating experiment: {name}")
            
            # Prepare tags
            experiment_tags = {
                "model_type": model_type,
                "target_variable": target_variable,
                "created_by": "experiment_manager",
                "framework": "mlb_betting_system",
                **(tags or {})
            }
            
            if data_start_date:
                experiment_tags["data_start_date"] = data_start_date
            if data_end_date:
                experiment_tags["data_end_date"] = data_end_date
            
            # Create MLflow experiment
            experiment_id = mlflow_service.create_experiment(
                name=name,
                description=description,
                tags=experiment_tags
            )
            
            # Create entry in curated.ml_experiments table
            experiment_data = await self._create_experiment_record(
                mlflow_experiment_id=int(experiment_id),
                name=name,
                description=description,
                model_type=model_type,
                target_variable=target_variable,
                data_start_date=data_start_date,
                data_end_date=data_end_date
            )
            
            # Cache experiment info
            self.experiment_cache[name] = {
                "experiment_id": experiment_id,
                "experiment_data": experiment_data
            }
            
            logger.info(f"✅ Created experiment {name} (MLflow ID: {experiment_id})")
            
            return {
                "experiment_id": experiment_id,
                "experiment_name": name,
                "mlflow_experiment_id": int(experiment_id),
                "database_record": experiment_data,
                "status": "created"
            }
            
        except Exception as e:
            logger.error(f"Failed to create experiment {name}: {e}")
            raise
    
    async def start_training_run(
        self,
        experiment_name: str,
        run_name: Optional[str] = None,
        model_config: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Start a new training run within an experiment.
        
        Args:
            experiment_name: Name of the experiment
            run_name: Optional name for this run
            model_config: Model configuration parameters
            tags: Additional run tags
            
        Returns:
            Dictionary with run information
        """
        try:
            # Get experiment info
            experiment_info = await self._get_experiment_info(experiment_name)
            if not experiment_info:
                raise ValueError(f"Experiment {experiment_name} not found")
            
            experiment_id = experiment_info["experiment_id"]
            
            # Prepare run tags
            run_tags = {
                "training_stage": "active",
                "model_framework": "lightgbm",  # Default framework
                "data_source": "curated_zone",
                **(tags or {})
            }
            
            # Start MLflow run
            run = mlflow_service.start_run(
                experiment_id=experiment_id,
                run_name=run_name,
                tags=run_tags
            )
            
            # Log model configuration if provided
            if model_config:
                mlflow_service.log_model_params(model_config)
            
            logger.info(f"Started training run: {run.info.run_id} in experiment {experiment_name}")
            
            return {
                "run_id": run.info.run_id,
                "experiment_id": experiment_id,
                "experiment_name": experiment_name,
                "run_name": run_name,
                "status": "active",
                "started_at": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to start training run: {e}")
            raise
    
    async def log_training_metrics(
        self,
        metrics: Dict[str, float],
        step: Optional[int] = None
    ) -> None:
        """
        Log training metrics to current MLflow run.
        
        Args:
            metrics: Dictionary of metric names and values
            step: Optional step number for multi-step logging
        """
        try:
            mlflow_service.log_model_metrics(metrics, step=step)
            logger.debug(f"Logged {len(metrics)} training metrics")
            
        except Exception as e:
            logger.error(f"Failed to log training metrics: {e}")
            raise
    
    async def complete_training_run(
        self,
        final_metrics: Dict[str, float],
        model_artifact_path: Optional[str] = None,
        performance_notes: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Complete a training run and log final results.
        
        Args:
            final_metrics: Final model performance metrics
            model_artifact_path: Path to saved model artifacts
            performance_notes: Optional notes about model performance
            
        Returns:
            Dictionary with run completion information
        """
        try:
            # Log final metrics
            await self.log_training_metrics(final_metrics)
            
            # Add completion metadata
            completion_tags = {
                "training_completed": "true",
                "completion_time": datetime.now(timezone.utc).isoformat()
            }
            
            if performance_notes:
                completion_tags["performance_notes"] = performance_notes
            
            # Log completion tags
            for tag_key, tag_value in completion_tags.items():
                mlflow.set_tag(tag_key, tag_value)
            
            # End the MLflow run
            mlflow_service.end_run(status="FINISHED")
            
            # Get current run info for database record
            current_run = mlflow.active_run()
            if current_run:
                run_id = current_run.info.run_id
                
                # Create model record in curated.ml_models
                await self._create_model_record(
                    run_id=run_id,
                    final_metrics=final_metrics,
                    model_artifact_path=model_artifact_path
                )
            
            logger.info("✅ Training run completed successfully")
            
            return {
                "status": "completed",
                "final_metrics": final_metrics,
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "model_artifact_path": model_artifact_path
            }
            
        except Exception as e:
            logger.error(f"Failed to complete training run: {e}")
            mlflow_service.end_run(status="FAILED")
            raise
    
    async def get_experiment_summary(self, experiment_name: str) -> Dict[str, Any]:
        """
        Get comprehensive experiment summary.
        
        Args:
            experiment_name: Name of the experiment
            
        Returns:
            Dictionary with experiment summary
        """
        try:
            # Get experiment info from MLflow
            experiment_info = await self._get_experiment_info(experiment_name)
            if not experiment_info:
                raise ValueError(f"Experiment {experiment_name} not found")
            
            experiment_id = experiment_info["experiment_id"]
            
            # Get runs from MLflow
            runs = self.mlflow_client.search_runs(
                experiment_ids=[experiment_id],
                order_by=["metrics.accuracy DESC"],
                max_results=50
            )
            
            # Get database record
            async with get_connection() as conn:
                db_record = await conn.fetchrow("""
                    SELECT * FROM curated.ml_experiments 
                    WHERE mlflow_experiment_id = $1
                """, str(experiment_id))
            
            # Analyze runs
            run_analysis = {
                "total_runs": len(runs),
                "completed_runs": len([r for r in runs if r.info.status == "FINISHED"]),
                "failed_runs": len([r for r in runs if r.info.status == "FAILED"]),
                "best_accuracy": max([r.data.metrics.get("accuracy", 0) for r in runs], default=0),
                "best_run_id": runs[0].info.run_id if runs else None,
                "latest_run_date": max([datetime.fromtimestamp(r.info.start_time / 1000) for r in runs], default=None)
            }
            
            return {
                "experiment_id": experiment_id,
                "experiment_name": experiment_name,
                "mlflow_data": experiment_info,
                "database_record": dict(db_record) if db_record else None,
                "run_analysis": run_analysis,
                "summary_generated_at": datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get experiment summary: {e}")
            raise
    
    async def list_experiments(self, include_archived: bool = False) -> List[Dict[str, Any]]:
        """
        List all experiments with summary information.
        
        Args:
            include_archived: Whether to include archived experiments
            
        Returns:
            List of experiment summaries
        """
        try:
            # Get experiments from database
            async with get_connection() as conn:
                query = """
                    SELECT 
                        mlflow_experiment_id,
                        experiment_name,
                        experiment_description,
                        status,
                        created_at,
                        best_run_id,
                        best_accuracy
                    FROM curated.ml_experiments
                """
                
                if not include_archived:
                    query += " WHERE status != 'archived'"
                
                query += " ORDER BY created_at DESC"
                
                experiments = await conn.fetch(query)
            
            experiment_list = []
            for exp in experiments:
                experiment_list.append({
                    "experiment_id": exp["mlflow_experiment_id"],
                    "name": exp["experiment_name"],
                    "description": exp["experiment_description"],
                    "status": exp["status"],
                    "created_at": exp["created_at"].isoformat(),
                    "best_model_run_id": exp["best_run_id"],
                    "best_accuracy": float(exp["best_accuracy"]) if exp["best_accuracy"] else None
                })
            
            logger.info(f"Found {len(experiment_list)} experiments")
            return experiment_list
            
        except Exception as e:
            logger.error(f"Failed to list experiments: {e}")
            raise
    
    async def _get_experiment_info(self, experiment_name: str) -> Optional[Dict[str, Any]]:
        """Get experiment information from cache or MLflow."""
        if experiment_name in self.experiment_cache:
            return self.experiment_cache[experiment_name]
        
        # Get from MLflow
        experiment = mlflow_service.get_experiment_by_name(experiment_name)
        if not experiment:
            return None
        
        experiment_info = {
            "experiment_id": experiment.experiment_id,
            "name": experiment.name,
            "lifecycle_stage": experiment.lifecycle_stage
        }
        
        self.experiment_cache[experiment_name] = experiment_info
        return experiment_info
    
    async def _create_experiment_record(
        self,
        mlflow_experiment_id: int,
        name: str,
        description: Optional[str],
        model_type: str,
        target_variable: str,
        data_start_date: Optional[str],
        data_end_date: Optional[str]
    ) -> Dict[str, Any]:
        """Create record in curated.ml_experiments table."""
        async with get_connection() as conn:
            record = await conn.fetchrow("""
                INSERT INTO curated.ml_experiments (
                    mlflow_experiment_id,
                    experiment_name,
                    experiment_description,
                    prediction_target,
                    model_type,
                    training_period_start,
                    training_period_end,
                    status,
                    created_at,
                    last_updated
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                RETURNING *
            """, 
                str(mlflow_experiment_id),  # Convert to string as per schema
                name,
                description,
                target_variable,
                model_type,
                datetime.fromisoformat(data_start_date).date() if data_start_date else None,
                datetime.fromisoformat(data_end_date).date() if data_end_date else None,
                "active",
                datetime.now(timezone.utc),
                datetime.now(timezone.utc)
            )
        
        return dict(record)
    
    async def _create_model_record(
        self,
        run_id: str,
        final_metrics: Dict[str, float],
        model_artifact_path: Optional[str]
    ) -> None:
        """Create record in curated.ml_models table."""
        try:
            # Get run info from MLflow
            run = self.mlflow_client.get_run(run_id)
            
            async with get_connection() as conn:
                await conn.execute("""
                    INSERT INTO curated.ml_models (
                        mlflow_experiment_id,
                        mlflow_run_id,
                        mlflow_model_uri,
                        model_name,
                        model_version,
                        model_type,
                        performance_metrics,
                        model_status,
                        validation_accuracy,
                        model_artifact_path,
                        created_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,
                    int(run.info.experiment_id),
                    run_id,
                    run.info.artifact_uri,
                    run.data.tags.get("model_name", "mlb_model"),
                    run.data.tags.get("model_version", "1.0"),
                    run.data.tags.get("model_type", "classification"),
                    final_metrics,
                    "training",
                    final_metrics.get("accuracy", 0.0),
                    model_artifact_path,
                    datetime.now(timezone.utc)
                )
            
            logger.info(f"Created model record for run {run_id}")
            
        except Exception as e:
            logger.error(f"Failed to create model record: {e}")
            raise


# Global experiment manager instance
experiment_manager = ExperimentManager()