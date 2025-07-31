"""
Model Registry with MLflow Integration
Automated model staging, deployment, and lifecycle management
"""

import logging
import asyncio
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from enum import Enum

import mlflow
from mlflow.tracking import MlflowClient
from mlflow.entities.model_registry import ModelVersion
from pydantic import BaseModel, Field

from ...core.config import get_settings

logger = logging.getLogger(__name__)


class ModelStage(str, Enum):
    """Model deployment stages"""
    NONE = "None"
    STAGING = "Staging"
    PRODUCTION = "Production"
    ARCHIVED = "Archived"


class ModelVersionInfo(BaseModel):
    """Model version information"""
    name: str
    version: str
    stage: ModelStage
    creation_timestamp: datetime
    last_updated_timestamp: Optional[datetime] = None
    description: Optional[str] = None
    tags: Optional[Dict[str, str]] = None
    metrics: Optional[Dict[str, float]] = None
    source: Optional[str] = None


class ModelRegistryService:
    """
    Service for managing ML model lifecycle with MLflow Model Registry
    Handles model staging, deployment automation, and performance monitoring
    """

    def __init__(self):
        self.settings = get_settings()
        self.client: Optional[MlflowClient] = None
        
        # Performance thresholds for automatic staging (from configuration)
        self.staging_thresholds = {
            "min_accuracy": self.settings.ml.model_thresholds.staging_min_accuracy,
            "min_roc_auc": self.settings.ml.model_thresholds.staging_min_roc_auc,
            "min_precision": self.settings.ml.model_thresholds.staging_min_precision,
            "min_recall": self.settings.ml.model_thresholds.staging_min_recall,
            "max_training_samples": self.settings.ml.model_thresholds.staging_min_training_samples
        }
        
        # Production promotion thresholds (from configuration)  
        self.production_thresholds = {
            "min_accuracy": self.settings.ml.model_thresholds.production_min_accuracy,
            "min_roc_auc": self.settings.ml.model_thresholds.production_min_roc_auc,
            "min_f1_score": self.settings.ml.model_thresholds.production_min_f1_score,
            "min_roi": self.settings.ml.model_thresholds.production_min_roi,
            "evaluation_days": self.settings.ml.model_thresholds.production_evaluation_days
        }

    async def initialize(self) -> bool:
        """Initialize MLflow client connection"""
        try:
            # Set MLflow tracking URI from configuration
            tracking_uri = self.settings.ml.mlflow.tracking_uri
            mlflow.set_tracking_uri(tracking_uri)
            self.client = MlflowClient()
            
            # Test connection
            experiments = self.client.search_experiments()
            logger.info(f"Model registry initialized with {len(experiments)} experiments")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize model registry: {e}")
            return False

    async def register_model(
        self, 
        model_uri: str, 
        model_name: str,
        description: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> Optional[ModelVersion]:
        """
        Register a new model version in MLflow Model Registry
        
        Args:
            model_uri: URI of the model to register
            model_name: Name for the registered model
            description: Optional model description
            tags: Optional model tags
            
        Returns:
            ModelVersion if successful, None otherwise
        """
        try:
            if not self.client:
                await self.initialize()

            # Register model version
            model_version = mlflow.register_model(
                model_uri=model_uri,
                name=model_name,
                tags=tags
            )
            
            # Add description if provided
            if description:
                self.client.update_model_version(
                    name=model_name,
                    version=model_version.version,
                    description=description
                )
            
            logger.info(f"Registered model {model_name} version {model_version.version}")
            return model_version
            
        except Exception as e:
            logger.error(f"Error registering model {model_name}: {e}")
            return None

    async def get_model_versions(
        self, 
        model_name: str,
        stages: Optional[List[ModelStage]] = None
    ) -> List[ModelVersionInfo]:
        """
        Get all versions of a model with optional stage filtering
        
        Args:
            model_name: Name of the model
            stages: Optional list of stages to filter by
            
        Returns:
            List of model version information
        """
        try:
            if not self.client:
                await self.initialize()

            # Get model versions from MLflow
            model_versions = self.client.search_model_versions(f"name='{model_name}'")
            
            result = []
            for mv in model_versions:
                # Filter by stage if specified
                if stages and ModelStage(mv.current_stage) not in stages:
                    continue
                
                # Get model metrics
                metrics = None
                if mv.run_id:
                    try:
                        run = self.client.get_run(mv.run_id)
                        metrics = run.data.metrics
                    except Exception:
                        pass
                
                version_info = ModelVersionInfo(
                    name=mv.name,
                    version=mv.version,
                    stage=ModelStage(mv.current_stage),
                    creation_timestamp=datetime.fromtimestamp(mv.creation_timestamp / 1000),
                    last_updated_timestamp=datetime.fromtimestamp(mv.last_updated_timestamp / 1000) if mv.last_updated_timestamp else None,
                    description=mv.description,
                    tags=mv.tags,
                    metrics=metrics,
                    source=mv.source
                )
                result.append(version_info)
            
            return result
            
        except Exception as e:
            logger.error(f"Error getting model versions for {model_name}: {e}")
            return []

    async def promote_to_staging(
        self, 
        model_name: str, 
        version: str,
        force: bool = False
    ) -> bool:
        """
        Promote model version to staging with automatic validation
        
        Args:
            model_name: Name of the model
            version: Version to promote
            force: Skip validation checks
            
        Returns:
            True if promoted successfully
        """
        try:
            if not self.client:
                await self.initialize()

            # Validate model performance unless forced
            if not force:
                is_valid = await self._validate_for_staging(model_name, version)
                if not is_valid:
                    logger.warning(f"Model {model_name} v{version} does not meet staging criteria")
                    return False

            # Transition to staging
            self.client.transition_model_version_stage(
                name=model_name,
                version=version,
                stage=ModelStage.STAGING.value,
                archive_existing_versions=True
            )
            
            # Add staging timestamp tag
            self.client.set_model_version_tag(
                name=model_name,
                version=version,
                key="staging_timestamp",
                value=datetime.utcnow().isoformat()
            )
            
            logger.info(f"Promoted {model_name} v{version} to staging")
            return True
            
        except Exception as e:
            logger.error(f"Error promoting {model_name} v{version} to staging: {e}")
            return False

    async def promote_to_production(
        self, 
        model_name: str, 
        version: str,
        force: bool = False
    ) -> bool:
        """
        Promote model version to production with validation
        
        Args:
            model_name: Name of the model
            version: Version to promote
            force: Skip validation checks
            
        Returns:
            True if promoted successfully
        """
        try:
            if not self.client:
                await self.initialize()

            # Validate model performance unless forced
            if not force:
                is_valid = await self._validate_for_production(model_name, version)
                if not is_valid:
                    logger.warning(f"Model {model_name} v{version} does not meet production criteria")
                    return False

            # Get current production model for backup
            current_production = await self._get_current_production_model(model_name)
            
            # Transition to production
            self.client.transition_model_version_stage(
                name=model_name,
                version=version,
                stage=ModelStage.PRODUCTION.value,
                archive_existing_versions=False  # Keep current production as backup
            )
            
            # Archive previous production model
            if current_production:
                self.client.transition_model_version_stage(
                    name=model_name,
                    version=current_production.version,
                    stage=ModelStage.ARCHIVED.value
                )
                logger.info(f"Archived previous production model {model_name} v{current_production.version}")
            
            # Add production timestamp tag
            self.client.set_model_version_tag(
                name=model_name,
                version=version,
                key="production_timestamp",
                value=datetime.utcnow().isoformat()
            )
            
            logger.info(f"Promoted {model_name} v{version} to production")
            return True
            
        except Exception as e:
            logger.error(f"Error promoting {model_name} v{version} to production: {e}")
            return False

    async def rollback_production(self, model_name: str) -> bool:
        """
        Rollback production model to previous version
        
        Args:
            model_name: Name of the model to rollback
            
        Returns:
            True if rollback successful
        """
        try:
            if not self.client:
                await self.initialize()

            # Get current production and archived models
            versions = await self.get_model_versions(model_name)
            
            current_production = None
            previous_archived = None
            
            for version in versions:
                if version.stage == ModelStage.PRODUCTION:
                    current_production = version
                elif version.stage == ModelStage.ARCHIVED and not previous_archived:
                    # Get most recently archived (likely previous production)
                    previous_archived = version
            
            if not previous_archived:
                logger.error(f"No archived version found for rollback of {model_name}")
                return False
            
            # Promote archived version back to production
            self.client.transition_model_version_stage(
                name=model_name,
                version=previous_archived.version,
                stage=ModelStage.PRODUCTION.value
            )
            
            # Archive current production
            if current_production:
                self.client.transition_model_version_stage(
                    name=model_name,
                    version=current_production.version,
                    stage=ModelStage.ARCHIVED.value
                )
            
            # Add rollback timestamp tag
            self.client.set_model_version_tag(
                name=model_name,
                version=previous_archived.version,
                key="rollback_timestamp",
                value=datetime.utcnow().isoformat()
            )
            
            logger.info(f"Rolled back {model_name} from v{current_production.version if current_production else 'unknown'} to v{previous_archived.version}")
            return True
            
        except Exception as e:
            logger.error(f"Error rolling back {model_name}: {e}")
            return False

    async def cleanup_old_versions(
        self, 
        model_name: str, 
        keep_versions: int = 5
    ) -> int:
        """
        Clean up old model versions, keeping the most recent ones
        
        Args:
            model_name: Name of the model
            keep_versions: Number of versions to keep
            
        Returns:
            Number of versions deleted
        """
        try:
            if not self.client:
                await self.initialize()

            versions = await self.get_model_versions(model_name)
            
            # Don't delete production or staging models
            deletable_versions = [
                v for v in versions 
                if v.stage not in [ModelStage.PRODUCTION, ModelStage.STAGING]
            ]
            
            # Sort by creation timestamp (oldest first)
            deletable_versions.sort(key=lambda x: x.creation_timestamp)
            
            # Keep the most recent versions
            to_delete = deletable_versions[:-keep_versions] if len(deletable_versions) > keep_versions else []
            
            deleted_count = 0
            for version in to_delete:
                try:
                    self.client.delete_model_version(
                        name=model_name,
                        version=version.version
                    )
                    deleted_count += 1
                    logger.debug(f"Deleted {model_name} v{version.version}")
                except Exception as e:
                    logger.warning(f"Failed to delete {model_name} v{version.version}: {e}")
            
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old versions of {model_name}")
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"Error cleaning up old versions of {model_name}: {e}")
            return 0

    async def get_model_performance_summary(self, model_name: str) -> Dict[str, Any]:
        """
        Get performance summary across all model versions
        
        Args:
            model_name: Name of the model
            
        Returns:
            Performance summary dictionary
        """
        try:
            versions = await self.get_model_versions(model_name)
            
            if not versions:
                return {"error": "No versions found"}
            
            summary = {
                "model_name": model_name,
                "total_versions": len(versions),
                "stages": {},
                "performance_trends": {},
                "latest_metrics": {}
            }
            
            # Aggregate by stage
            for stage in ModelStage:
                stage_versions = [v for v in versions if v.stage == stage]
                summary["stages"][stage.value] = len(stage_versions)
            
            # Get latest version metrics
            latest_version = max(versions, key=lambda x: x.creation_timestamp)
            if latest_version.metrics:
                summary["latest_metrics"] = latest_version.metrics
            
            # Calculate performance trends (last 5 versions)
            recent_versions = sorted(versions, key=lambda x: x.creation_timestamp)[-5:]
            metrics_over_time = {}
            
            for version in recent_versions:
                if version.metrics:
                    for metric, value in version.metrics.items():
                        if metric not in metrics_over_time:
                            metrics_over_time[metric] = []
                        metrics_over_time[metric].append(value)
            
            # Calculate trends
            for metric, values in metrics_over_time.items():
                if len(values) >= 2:
                    trend = "improving" if values[-1] > values[0] else "declining"
                    summary["performance_trends"][metric] = {
                        "trend": trend,
                        "latest_value": values[-1],
                        "change": values[-1] - values[0]
                    }
            
            return summary
            
        except Exception as e:
            logger.error(f"Error getting performance summary for {model_name}: {e}")
            return {"error": str(e)}

    async def _validate_for_staging(self, model_name: str, version: str) -> bool:
        """Validate model meets staging criteria"""
        try:
            # Get model version
            model_version = self.client.get_model_version(model_name, version)
            
            if not model_version.run_id:
                return False
            
            # Get run metrics
            run = self.client.get_run(model_version.run_id)
            metrics = run.data.metrics
            
            if not metrics:
                return False
            
            # Check thresholds
            checks = {
                "accuracy": metrics.get("test_accuracy", 0) >= self.staging_thresholds["min_accuracy"],
                "roc_auc": metrics.get("test_roc_auc", 0) >= self.staging_thresholds["min_roc_auc"],
                "precision": metrics.get("test_precision", 0) >= self.staging_thresholds["min_precision"],
                "recall": metrics.get("test_recall", 0) >= self.staging_thresholds["min_recall"]
            }
            
            passed = all(checks.values())
            logger.info(f"Staging validation for {model_name} v{version}: {checks} -> {'PASS' if passed else 'FAIL'}")
            
            return passed
            
        except Exception as e:
            logger.error(f"Error validating model for staging: {e}")
            return False

    async def _validate_for_production(self, model_name: str, version: str) -> bool:
        """Validate model meets production criteria"""
        try:
            # Get model version
            model_version = self.client.get_model_version(model_name, version)
            
            # Must be in staging stage
            if model_version.current_stage != ModelStage.STAGING.value:
                logger.warning(f"Model {model_name} v{version} not in staging")
                return False
            
            # Check staging duration
            staging_tag = model_version.tags.get("staging_timestamp")
            if staging_tag:
                staging_time = datetime.fromisoformat(staging_tag)
                days_in_staging = (datetime.utcnow() - staging_time).days
                if days_in_staging < self.production_thresholds["evaluation_days"]:
                    logger.warning(f"Model {model_name} v{version} needs {self.production_thresholds['evaluation_days'] - days_in_staging} more days in staging")
                    return False
            
            # Get run metrics
            if not model_version.run_id:
                return False
            
            run = self.client.get_run(model_version.run_id)
            metrics = run.data.metrics
            
            if not metrics:
                return False
            
            # Check production thresholds
            checks = {
                "accuracy": metrics.get("test_accuracy", 0) >= self.production_thresholds["min_accuracy"],
                "roc_auc": metrics.get("test_roc_auc", 0) >= self.production_thresholds["min_roc_auc"],
                "f1_score": metrics.get("test_f1_score", 0) >= self.production_thresholds["min_f1_score"]
            }
            
            passed = all(checks.values())
            logger.info(f"Production validation for {model_name} v{version}: {checks} -> {'PASS' if passed else 'FAIL'}")
            
            return passed
            
        except Exception as e:
            logger.error(f"Error validating model for production: {e}")
            return False

    async def _get_current_production_model(self, model_name: str) -> Optional[ModelVersionInfo]:
        """Get current production model version"""
        try:
            versions = await self.get_model_versions(model_name, stages=[ModelStage.PRODUCTION])
            return versions[0] if versions else None
        except Exception:
            return None

    def get_registry_stats(self) -> Dict[str, Any]:
        """Get model registry statistics"""
        try:
            if not self.client:
                return {"error": "Registry not initialized"}
            
            # Get all registered models
            registered_models = self.client.search_registered_models()
            
            stats = {
                "total_models": len(registered_models),
                "models_by_stage": {stage.value: 0 for stage in ModelStage},
                "total_versions": 0,
                "models": []
            }
            
            for model in registered_models:
                # Get versions for this model
                versions = self.client.search_model_versions(f"name='{model.name}'")
                stats["total_versions"] += len(versions)
                
                model_info = {
                    "name": model.name,
                    "description": model.description,
                    "total_versions": len(versions),
                    "stages": {stage.value: 0 for stage in ModelStage}
                }
                
                for version in versions:
                    stage = version.current_stage
                    stats["models_by_stage"][stage] += 1
                    model_info["stages"][stage] += 1
                
                stats["models"].append(model_info)
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting registry stats: {e}")
            return {"error": str(e)}


# Global model registry instance
model_registry = ModelRegistryService()