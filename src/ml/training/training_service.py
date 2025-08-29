"""
ML Training Service
Orchestrates LightGBM training pipeline with scheduling and monitoring
"""

import logging
import asyncio
import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from pathlib import Path
import asyncpg

from .lightgbm_trainer import LightGBMTrainer

# Fixed import structure - removed sys.path.append()
try:
    from ...core.config import get_settings
except ImportError:
    # Fallback for environments where unified config is not available
    def get_settings():
        raise ImportError("Unified config system not available. Please check src.core.config module.")

# Import MLflow for enhanced experiment tracking
try:
    import mlflow
    import mlflow.tracking
    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False

logger = logging.getLogger(__name__)


class MLTrainingService:
    """
    ML training service with scheduling and model lifecycle management
    """

    def __init__(self):
        self.settings = get_settings()
        self.trainer = LightGBMTrainer()

        # Training configuration
        self.default_training_config = {
            "prediction_targets": [
                "moneyline_home_win",
                "total_over_under",
                "run_total_regression",
            ],
            "training_window_days": 90,  # 3 months of training data
            "cross_validation_folds": 5,
            "test_size": 0.2,
            "use_cached_features": True,
        }

        # Retraining configuration
        self.retraining_config = {
            "sliding_window_days": 7,
            "min_samples_for_retrain": 100,
            "retrain_schedule_hours": 24,  # Retrain every 24 hours
            "performance_degradation_threshold": 0.05,  # 5% performance drop triggers retrain
        }

        # Service state
        self.service_state = {
            "last_training_run": None,
            "last_retrain_check": None,
            "active_models": {},
            "training_in_progress": False,
            "service_health": "healthy",
            "daily_predictions_generated": None,
            "prediction_performance_tracking": {},
        }
        
        # Enhanced MLflow tracking
        self.experiment_tracking = {
            "experiment_name": "mlb_betting_production",
            "model_registry_enabled": True,
            "auto_log_enabled": True,
            "performance_tracking_enabled": True,
        }

    async def train_initial_models(
        self,
        end_date: Optional[datetime] = None,
        training_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Train initial set of models for production deployment

        Args:
            end_date: End date for training data (default: now)
            training_config: Custom training configuration

        Returns:
            Training results and model deployment status
        """
        try:
            if self.service_state["training_in_progress"]:
                raise ValueError("Training already in progress")

            self.service_state["training_in_progress"] = True

            logger.info("Starting initial model training...")

            # Use provided config or defaults
            config = training_config or self.default_training_config
            
            # Validate configuration
            required_keys = ["training_window_days", "cross_validation_folds", "test_size", "use_cached_features"]
            missing_keys = [key for key in required_keys if key not in config]
            if missing_keys:
                raise ValueError(f"Missing required configuration keys: {missing_keys}")
            
            # Validate prediction targets
            prediction_targets = config.get("prediction_targets")
            if prediction_targets is None:
                logger.info("No prediction targets specified, using defaults")
                prediction_targets = list(self.trainer.model_configs.keys())
            elif not isinstance(prediction_targets, list) or not prediction_targets:
                raise ValueError(f"prediction_targets must be a non-empty list, got: {prediction_targets}")

            # Calculate training period
            if end_date is None:
                end_date = datetime.utcnow()

            start_date = end_date - timedelta(days=config["training_window_days"])
            
            logger.info(f"Training configuration: targets={prediction_targets}, "
                       f"period={start_date.date()} to {end_date.date()}, "
                       f"cv_folds={config['cross_validation_folds']}, "
                       f"test_size={config['test_size']}, "
                       f"use_cached_features={config['use_cached_features']}")

            # Train models
            training_results = await self.trainer.train_models(
                start_date=start_date,
                end_date=end_date,
                prediction_targets=prediction_targets,
                use_cached_features=config["use_cached_features"],
                cross_validation_folds=config["cross_validation_folds"],
                test_size=config["test_size"],
            )

            # Update service state
            self.service_state["last_training_run"] = datetime.utcnow()
            self.service_state["active_models"] = {
                target: {
                    "trained_at": datetime.utcnow(),
                    "training_samples": training_results.get("data_samples", 0),
                    "performance_metrics": training_results["training_results"]
                    .get(target, {})
                    .get("test_metrics", {}),
                    "status": "active",
                }
                for target in config["prediction_targets"]
                if target in training_results["training_results"]
            }

            logger.info(
                f"Initial model training completed: {len(self.service_state['active_models'])} models trained"
            )

            return {
                "status": "success",
                "models_trained": list(self.service_state["active_models"].keys()),
                "training_results": training_results,
                "deployment_ready": True,
            }

        except Exception as e:
            logger.error(f"Error in initial model training: {e}")
            if "prediction_targets" in str(e):
                logger.error("Training configuration issue. Please check that prediction_targets are properly specified.")
                logger.error(f"Available targets: {list(self.trainer.model_configs.keys())}")
            self.service_state["service_health"] = "error"
            raise
        finally:
            self.service_state["training_in_progress"] = False

    async def check_and_retrain_models(
        self, force_retrain: bool = False
    ) -> Dict[str, Any]:
        """
        Check model performance and retrain if necessary

        Args:
            force_retrain: Force retraining regardless of performance

        Returns:
            Retraining results and updated model status
        """
        try:
            logger.info("Checking models for retraining needs...")

            current_time = datetime.utcnow()
            retrain_results = {}

            for model_name, model_info in self.service_state["active_models"].items():
                should_retrain = force_retrain
                retrain_reason = "forced" if force_retrain else None

                # Check time-based retraining
                if not should_retrain:
                    hours_since_training = (
                        current_time - model_info["trained_at"]
                    ).total_seconds() / 3600
                    if (
                        hours_since_training
                        >= self.retraining_config["retrain_schedule_hours"]
                    ):
                        should_retrain = True
                        retrain_reason = "scheduled"

                # Check performance-based retraining
                if not should_retrain:
                    performance_degraded = (
                        await self._check_model_performance_degradation(model_name)
                    )
                    if performance_degraded:
                        should_retrain = True
                        retrain_reason = "performance_degradation"

                if should_retrain:
                    logger.info(
                        f"Retraining model {model_name} - reason: {retrain_reason}"
                    )

                    retrain_result = await self.trainer.retrain_model(
                        model_name=model_name,
                        sliding_window_days=self.retraining_config[
                            "sliding_window_days"
                        ],
                        min_samples=self.retraining_config["min_samples_for_retrain"],
                    )

                    if retrain_result.get("status") != "skipped":
                        # Update model info
                        self.service_state["active_models"][model_name].update(
                            {
                                "trained_at": current_time,
                                "performance_metrics": retrain_result.get(
                                    "test_metrics", {}
                                ),
                                "drift_detected": retrain_result.get(
                                    "drift_detected", False
                                ),
                                "retrain_reason": retrain_reason,
                            }
                        )

                    retrain_results[model_name] = retrain_result
                else:
                    logger.debug(f"Model {model_name} does not need retraining")

            self.service_state["last_retrain_check"] = current_time

            logger.info(
                f"Retraining check completed: {len(retrain_results)} models processed"
            )

            return {
                "retrain_results": retrain_results,
                "models_retrained": len(
                    [
                        r
                        for r in retrain_results.values()
                        if r.get("status") != "skipped"
                    ]
                ),
                "active_models": self.service_state["active_models"],
            }

        except Exception as e:
            logger.error(f"Error in model retraining check: {e}")
            raise

    async def evaluate_model_performance(
        self, model_name: str, evaluation_days: int = 7
    ) -> Dict[str, Any]:
        """
        Evaluate model performance on recent data

        Args:
            model_name: Model to evaluate
            evaluation_days: Days of recent data for evaluation

        Returns:
            Performance evaluation results
        """
        try:
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=evaluation_days)

            evaluation_results = await self.trainer.evaluate_model_performance(
                model_name=model_name,
                evaluation_start=start_date,
                evaluation_end=end_date,
            )

            logger.info(f"Model evaluation completed for {model_name}")
            return evaluation_results

        except Exception as e:
            logger.error(f"Error evaluating model {model_name}: {e}")
            raise

    async def get_model_info(self, model_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get information about trained models

        Args:
            model_name: Specific model name (optional)

        Returns:
            Model information and status
        """
        try:
            if model_name:
                if model_name not in self.service_state["active_models"]:
                    raise ValueError(f"Model {model_name} not found")

                model_info = self.service_state["active_models"][model_name].copy()

                # Add trainer statistics
                trainer_stats = self.trainer.get_training_stats()
                model_specific_stats = trainer_stats.get("best_model_scores", {}).get(
                    model_name, {}
                )

                model_info["best_scores"] = model_specific_stats

                return {"model_name": model_name, "model_info": model_info}
            else:
                # Return all models info
                all_models_info = {}
                for name, info in self.service_state["active_models"].items():
                    model_info = info.copy()
                    trainer_stats = self.trainer.get_training_stats()
                    model_specific_stats = trainer_stats.get(
                        "best_model_scores", {}
                    ).get(name, {})
                    model_info["best_scores"] = model_specific_stats
                    all_models_info[name] = model_info

                return {
                    "active_models": all_models_info,
                    "service_stats": self.trainer.get_training_stats(),
                }

        except Exception as e:
            logger.error(f"Error getting model info: {e}")
            raise

    async def schedule_training_job(
        self,
        schedule_type: str = "daily",
        schedule_hour: int = 2,
        training_config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Schedule automated training jobs

        Args:
            schedule_type: Type of schedule ('daily', 'weekly')
            schedule_hour: Hour of day to run (0-23)
            training_config: Custom training configuration

        Returns:
            Scheduling status and configuration
        """
        try:
            # This is a simplified version - in production, you'd use a task scheduler like Celery
            logger.info(
                f"Training job scheduled: {schedule_type} at {schedule_hour}:00"
            )

            # Store schedule configuration
            schedule_config = {
                "schedule_type": schedule_type,
                "schedule_hour": schedule_hour,
                "training_config": training_config or self.default_training_config,
                "created_at": datetime.utcnow(),
                "status": "active",
            }

            return {
                "status": "scheduled",
                "schedule_config": schedule_config,
                "next_run": self._calculate_next_run_time(schedule_type, schedule_hour),
            }

        except Exception as e:
            logger.error(f"Error scheduling training job: {e}")
            raise

    async def generate_daily_predictions(self, target_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Generate predictions for today's games using trained models
        
        Args:
            target_date: Date to generate predictions for (default: today)
            
        Returns:
            Daily predictions with confidence scores and betting recommendations
        """
        try:
            if target_date is None:
                target_date = datetime.utcnow().date()
                
            logger.info(f"Generating daily predictions for {target_date}")
            
            # Load today's games
            games_for_prediction = await self._load_games_for_prediction(target_date)
            
            if not games_for_prediction:
                logger.warning(f"No games found for prediction on {target_date}")
                return {"status": "no_games", "date": target_date, "predictions": []}
            
            # Generate predictions for each game using active models
            daily_predictions = []
            
            for game in games_for_prediction:
                game_id = game["id"]
                game_predictions = {}
                
                # Calculate feature cutoff time (60 minutes before game)
                cutoff_time = game["game_datetime"] - timedelta(minutes=60)
                
                # Extract features for this game
                from ..features.feature_pipeline import FeaturePipeline
                feature_pipeline = FeaturePipeline()
                
                feature_vector = await feature_pipeline.extract_features_for_game(
                    game_id, cutoff_time
                )
                
                if not feature_vector:
                    logger.warning(f"No features available for game {game_id}")
                    continue
                
                # Generate predictions using each active model
                for model_name, model_info in self.service_state["active_models"].items():
                    try:
                        if not MLFLOW_AVAILABLE:
                            logger.warning(f"MLflow not available, skipping model {model_name}")
                            continue
                            
                        # Validate model configuration
                        if model_name not in self.trainer.model_configs:
                            logger.warning(f"Model {model_name} not found in trainer configurations, skipping")
                            continue
                        
                        # Load model from MLflow
                        model_uri = f"models:/{model_name}/latest"
                        logger.debug(f"Loading model from URI: {model_uri}")
                        
                        try:
                            model = mlflow.lightgbm.load_model(model_uri)
                        except Exception as model_load_error:
                            logger.warning(f"Failed to load model {model_name} from MLflow: {model_load_error}")
                            continue
                            
                        # Convert feature vector to array
                        feature_array = self.trainer._feature_vector_to_array(feature_vector)
                        
                        if feature_array is None:
                            logger.warning(f"Failed to convert feature vector to array for game {game_id}")
                            continue
                            
                        # Validate feature array shape
                        if len(feature_array.shape) != 1 or feature_array.shape[0] == 0:
                            logger.warning(f"Invalid feature array shape {feature_array.shape} for game {game_id}")
                            continue
                        
                        # Generate prediction
                        model_config = self.trainer.model_configs[model_name]
                        
                        if model_config["objective"] == "binary":
                            prediction_prob = model.predict(feature_array.reshape(1, -1))[0]
                            prediction_binary = int(prediction_prob > 0.5)
                            confidence = max(prediction_prob, 1 - prediction_prob)
                        else:
                            prediction_value = model.predict(feature_array.reshape(1, -1))[0]
                            prediction_prob = prediction_value
                            prediction_binary = None
                            confidence = 0.8  # Default confidence for regression
                        
                        # Validate prediction values
                        if not (0 <= prediction_prob <= 1) and model_config["objective"] == "binary":
                            logger.warning(f"Invalid prediction probability {prediction_prob} for binary model {model_name}")
                            continue
                        
                        game_predictions[model_name] = {
                            "probability": float(prediction_prob),
                            "binary_prediction": prediction_binary,
                            "confidence": float(confidence),
                            "feature_completeness": float(feature_vector.feature_completeness_score),
                            "model_objective": model_config["objective"],
                        }
                        
                        logger.debug(f"Generated prediction for {model_name}: prob={prediction_prob:.3f}, confidence={confidence:.3f}")
                                
                    except Exception as e:
                        logger.error(f"Error generating prediction for {model_name} on game {game_id}: {e}")
                        logger.debug(f"Prediction error details:", exc_info=True)
                        continue
                
                if game_predictions:
                    daily_predictions.append({
                        "game_id": game_id,
                        "home_team": game["home_team"],
                        "away_team": game["away_team"],
                        "game_datetime": game["game_datetime"],
                        "predictions": game_predictions,
                        "prediction_timestamp": datetime.utcnow(),
                    })
            
            # Save predictions to database
            await self._save_daily_predictions(daily_predictions, target_date)
            
            # Update service state
            self.service_state["daily_predictions_generated"] = datetime.utcnow()
            
            logger.info(f"Generated predictions for {len(daily_predictions)} games on {target_date}")
            
            return {
                "status": "success",
                "date": target_date,
                "predictions": daily_predictions,
                "games_processed": len(games_for_prediction),
                "predictions_generated": len(daily_predictions),
            }
            
        except Exception as e:
            logger.error(f"Error generating daily predictions: {e}")
            raise

    async def health_check(self) -> Dict[str, Any]:
        """
        Perform comprehensive health check of training service

        Returns:
            Health status and service metrics
        """
        try:
            health_status = {
                "service_status": self.service_state["service_health"],
                "training_in_progress": self.service_state["training_in_progress"],
                "active_models_count": len(self.service_state["active_models"]),
                "last_training_run": self.service_state["last_training_run"],
                "last_retrain_check": self.service_state["last_retrain_check"],
                "daily_predictions_generated": self.service_state["daily_predictions_generated"],
                "mlflow_available": MLFLOW_AVAILABLE,
            }

            # Check trainer health
            trainer_stats = self.trainer.get_training_stats()
            health_status["trainer_stats"] = trainer_stats

            # Check if models are too old
            current_time = datetime.utcnow()
            stale_models = []

            for model_name, model_info in self.service_state["active_models"].items():
                hours_since_training = (
                    current_time - model_info["trained_at"]
                ).total_seconds() / 3600
                if hours_since_training > 72:  # 3 days
                    stale_models.append(model_name)

            health_status["stale_models"] = stale_models
            health_status["models_need_attention"] = len(stale_models) > 0

            # Overall health assessment
            if self.service_state["service_health"] == "error":
                health_status["overall_health"] = "unhealthy"
            elif len(stale_models) > 0:
                health_status["overall_health"] = "degraded"
            elif len(self.service_state["active_models"]) == 0:
                health_status["overall_health"] = "no_models"
            else:
                health_status["overall_health"] = "healthy"

            return health_status

        except Exception as e:
            logger.error(f"Error in health check: {e}")
            return {
                "overall_health": "error",
                "error": str(e),
                "timestamp": datetime.utcnow(),
            }

    async def _check_model_performance_degradation(self, model_name: str) -> bool:
        """Check if model performance has degraded significantly"""
        try:
            # Evaluate model on recent 3 days of data
            evaluation_results = await self.evaluate_model_performance(
                model_name, evaluation_days=3
            )

            # Get baseline performance
            baseline_metrics = self.service_state["active_models"][model_name].get(
                "performance_metrics", {}
            )
            current_metrics = evaluation_results.get("performance_metrics", {})

            # Check for performance degradation
            for metric_name, baseline_value in baseline_metrics.items():
                if metric_name in current_metrics:
                    current_value = current_metrics[metric_name]

                    # Calculate relative degradation
                    if baseline_value > 0:
                        degradation = (baseline_value - current_value) / baseline_value

                        if (
                            degradation
                            > self.retraining_config[
                                "performance_degradation_threshold"
                            ]
                        ):
                            logger.warning(
                                f"Performance degradation detected for {model_name} - {metric_name}: {degradation:.3f}"
                            )
                            return True

            return False

        except Exception as e:
            logger.error(
                f"Error checking performance degradation for {model_name}: {e}"
            )
            return False  # Default to no degradation on error

    def _calculate_next_run_time(
        self, schedule_type: str, schedule_hour: int
    ) -> datetime:
        """Calculate next scheduled run time"""
        now = datetime.utcnow()

        if schedule_type == "daily":
            next_run = now.replace(
                hour=schedule_hour, minute=0, second=0, microsecond=0
            )
            if next_run <= now:
                next_run += timedelta(days=1)
        elif schedule_type == "weekly":
            # Run on Sundays
            days_until_sunday = (6 - now.weekday()) % 7
            if days_until_sunday == 0 and now.hour >= schedule_hour:
                days_until_sunday = 7
            next_run = now + timedelta(days=days_until_sunday)
            next_run = next_run.replace(
                hour=schedule_hour, minute=0, second=0, microsecond=0
            )
        else:
            raise ValueError(f"Unknown schedule type: {schedule_type}")

        return next_run

    async def _load_games_for_prediction(self, target_date: datetime.date) -> List[Dict[str, Any]]:
        """Load games scheduled for the target date"""
        try:
            conn = await asyncpg.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
            )
            
            # Query for games on target date that haven't started yet
            query = """
                SELECT 
                    eg.id,
                    eg.home_team,
                    eg.away_team, 
                    eg.game_datetime,
                    eg.game_status
                FROM curated.enhanced_games eg
                WHERE DATE(eg.game_datetime) = $1
                    AND eg.game_status = 'scheduled'
                    AND eg.game_datetime > NOW() + INTERVAL '60 minutes'
                ORDER BY eg.game_datetime
            """
            
            games = await conn.fetch(query, target_date)
            await conn.close()
            
            return [dict(game) for game in games]
            
        except Exception as e:
            logger.error(f"Error loading games for prediction: {e}")
            return []
    
    async def _save_daily_predictions(self, predictions: List[Dict[str, Any]], target_date: datetime.date) -> bool:
        """Save daily predictions to database"""
        try:
            conn = await asyncpg.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
            )
            
            for game_prediction in predictions:
                game_id = game_prediction["game_id"]
                predictions_data = game_prediction["predictions"]
                prediction_timestamp = game_prediction["prediction_timestamp"]
                
                # Save each model's prediction
                for model_name, prediction_data in predictions_data.items():
                    insert_query = """
                        INSERT INTO curated.ml_predictions (
                            game_id, model_name, model_version, prediction_timestamp,
                            feature_version, prediction_explanation,
                            confidence_threshold_met, created_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        ON CONFLICT (game_id, model_name, model_version, prediction_timestamp)
                        DO UPDATE SET
                            confidence_threshold_met = EXCLUDED.confidence_threshold_met,
                            prediction_explanation = EXCLUDED.prediction_explanation
                    """
                    
                    # Set prediction fields based on model type
                    model_type = model_name
                    confidence_met = prediction_data["confidence"] > 0.7
                    
                    prediction_explanation = {
                        "confidence": prediction_data["confidence"],
                        "feature_completeness": prediction_data["feature_completeness"],
                        "model_type": model_type,
                        "prediction_date": target_date.isoformat(),
                    }
                    
                    await conn.execute(
                        insert_query,
                        game_id,
                        model_name,
                        "v2.1",  # Current model version
                        prediction_timestamp,
                        "v2.1",  # Feature version 
                        json.dumps(prediction_explanation),
                        confidence_met,
                        datetime.utcnow(),
                    )
            
            await conn.close()
            logger.info(f"Saved {len(predictions)} game predictions to database")
            return True
            
        except Exception as e:
            logger.error(f"Error saving daily predictions: {e}")
            return False

    def get_service_state(self) -> Dict[str, Any]:
        """Get current service state"""
        return self.service_state.copy()
