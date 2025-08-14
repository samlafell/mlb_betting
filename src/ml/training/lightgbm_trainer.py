"""
LightGBM Training Pipeline with MLflow Integration
High-performance ML training for MLB betting predictions with experiment tracking
"""

import json
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import lightgbm as lgb
import mlflow
import mlflow.lightgbm
import mlflow.sklearn
import numpy as np
import polars as pl
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import TimeSeriesSplit

from ...core.config import get_settings
from ..features.feature_pipeline import FeaturePipeline
from ..features.models import FeatureVector
from ..features.redis_feature_store import RedisFeatureStore

logger = logging.getLogger(__name__)


class LightGBMTrainer:
    """
    LightGBM training pipeline with MLflow experiment tracking
    Supports multiple prediction targets and cross-validation
    """

    def __init__(
        self,
        experiment_name: str = "mlb_betting_predictions",
        model_version: str = "v2.1",
    ):
        self.settings = get_settings()
        self.model_version = model_version
        self.experiment_name = experiment_name

        # Initialize components
        self.feature_pipeline = FeaturePipeline()
        self.redis_store = RedisFeatureStore()

        # Model configurations for different prediction targets
        self.model_configs = {
            "moneyline_home_win": {
                "objective": "binary",
                "metric": "binary_logloss",
                "boosting_type": "gbdt",
                "num_leaves": 31,
                "learning_rate": 0.05,
                "feature_fraction": 0.9,
                "bagging_fraction": 0.8,
                "bagging_freq": 5,
                "verbose": -1,
                "random_state": 42,
            },
            "total_over_under": {
                "objective": "binary",
                "metric": "binary_logloss",
                "boosting_type": "gbdt",
                "num_leaves": 25,
                "learning_rate": 0.05,
                "feature_fraction": 0.85,
                "bagging_fraction": 0.8,
                "bagging_freq": 5,
                "verbose": -1,
                "random_state": 42,
            },
            "run_total_regression": {
                "objective": "regression",
                "metric": "rmse",
                "boosting_type": "gbdt",
                "num_leaves": 35,
                "learning_rate": 0.05,
                "feature_fraction": 0.9,
                "bagging_fraction": 0.8,
                "bagging_freq": 5,
                "verbose": -1,
                "random_state": 42,
            },
        }

        # Feature importance tracking for drift detection
        self.baseline_feature_importance = {}

        # Training statistics
        self.training_stats = {
            "models_trained": 0,
            "experiments_logged": 0,
            "avg_training_time_seconds": 0.0,
            "best_model_scores": {},
        }

    async def train_models(
        self,
        start_date: datetime,
        end_date: datetime,
        prediction_targets: list[str] = None,
        use_cached_features: bool = True,
        cross_validation_folds: int = 5,
        test_size: float = 0.2,
    ) -> dict[str, Any]:
        """
        Train LightGBM models for specified prediction targets

        Args:
            start_date: Training data start date
            end_date: Training data end date
            prediction_targets: List of targets to train (default: all)
            use_cached_features: Whether to use Redis cached features
            cross_validation_folds: Number of CV folds
            test_size: Test set proportion

        Returns:
            Training results and model performance metrics
        """
        training_start = datetime.utcnow()

        try:
            logger.info(
                f"Starting LightGBM training pipeline: {start_date} to {end_date}"
            )

            # Initialize MLflow tracking
            await self._initialize_mlflow_tracking()

            # Set default prediction targets
            if prediction_targets is None:
                prediction_targets = list(self.model_configs.keys())

            # Load and prepare training data
            logger.info("Loading training data and features...")
            training_data = await self._load_training_data(
                start_date, end_date, use_cached_features
            )

            if not training_data or len(training_data) < 50:
                data_count = len(training_data) if training_data else 0
                raise ValueError(
                    f"Insufficient training data: {data_count} samples (minimum: 50). "
                    f"Date range: {start_date.date()} to {end_date.date()}. "
                    f"Try increasing --days parameter or check if games have outcomes in database."
                )

            # Train models for each prediction target
            training_results = {}

            for target in prediction_targets:
                logger.info(f"Training model for target: {target}")

                with mlflow.start_run(
                    run_name=f"{target}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                ):
                    # Log training parameters
                    mlflow.log_params(
                        {
                            "target": target,
                            "start_date": start_date.isoformat(),
                            "end_date": end_date.isoformat(),
                            "cv_folds": cross_validation_folds,
                            "test_size": test_size,
                            "model_version": self.model_version,
                            "use_cached_features": use_cached_features,
                        }
                    )

                    # Prepare target-specific dataset
                    X, y, feature_names = await self._prepare_target_dataset(
                        training_data, target
                    )

                    if len(X) < 50:
                        logger.warning(
                            f"Insufficient data for target {target}: {len(X)} samples"
                        )
                        continue

                    # Train and evaluate model
                    model_results = await self._train_target_model(
                        X, y, feature_names, target, cross_validation_folds, test_size
                    )

                    training_results[target] = model_results

                    # Log results to MLflow
                    await self._log_model_results(model_results, target, feature_names)

            # Update training statistics
            training_time = (datetime.utcnow() - training_start).total_seconds()
            self._update_training_stats(training_results, training_time)

            logger.info(f"Training pipeline completed in {training_time:.1f}s")
            return {
                "training_results": training_results,
                "training_time_seconds": training_time,
                "models_trained": len(training_results),
                "data_samples": len(training_data) if training_data else 0,
            }

        except Exception as e:
            error_context = {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat() if end_date else None, 
                "prediction_targets": prediction_targets,
                "use_cached_features": use_cached_features,
                "training_data_samples": len(training_data) if 'training_data' in locals() else "unknown"
            }
            logger.error(f"Error in training pipeline: {e}")
            logger.error(f"Training context: {error_context}")
            
            # Provide specific guidance for common errors
            if "No games found" in str(e) or "Insufficient training data" in str(e):
                logger.error("Data availability issue. Try:")
                logger.error("  1. Check if games exist in curated.enhanced_games for the date range")
                logger.error("  2. Increase training window with --days parameter")
                logger.error("  3. Verify database has sufficient game outcome data")
            elif "prediction_targets" in str(e):
                logger.error("Configuration issue. Available targets:")
                for target in self.model_configs.keys():
                    logger.error(f"  - {target}")
            
            raise

    async def retrain_model(
        self, model_name: str, sliding_window_days: int = 7, min_samples: int = 100
    ) -> dict[str, Any]:
        """
        Retrain a specific model with sliding window approach

        Args:
            model_name: Model to retrain
            sliding_window_days: Days of recent data to use
            min_samples: Minimum samples required for retraining

        Returns:
            Retraining results and performance comparison
        """
        try:
            logger.info(
                f"Retraining model {model_name} with {sliding_window_days}-day window"
            )

            # Calculate training window
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=sliding_window_days)

            # Load recent data
            recent_data = await self._load_training_data(
                start_date, end_date, use_cached_features=True
            )

            if not recent_data or len(recent_data) < min_samples:
                logger.warning(
                    f"Insufficient recent data for retraining: {len(recent_data) if recent_data else 0}"
                )
                return {"status": "skipped", "reason": "insufficient_data"}

            # Retrain model
            with mlflow.start_run(
                run_name=f"{model_name}_retrain_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            ):
                X, y, feature_names = await self._prepare_target_dataset(
                    recent_data, model_name
                )

                retrain_results = await self._train_target_model(
                    X, y, feature_names, model_name, cv_folds=3, test_size=0.15
                )

                # Compare with baseline feature importance for drift detection
                drift_detected = self._detect_feature_drift(
                    retrain_results["feature_importance"], model_name
                )

                retrain_results["drift_detected"] = drift_detected
                retrain_results["sliding_window_days"] = sliding_window_days

                await self._log_model_results(
                    retrain_results, model_name, feature_names, is_retrain=True
                )

                logger.info(
                    f"Model {model_name} retrained successfully. Drift detected: {drift_detected}"
                )
                return retrain_results

        except Exception as e:
            logger.error(f"Error retraining model {model_name}: {e}")
            raise

    async def evaluate_model_performance(
        self, model_name: str, evaluation_start: datetime, evaluation_end: datetime
    ) -> dict[str, Any]:
        """
        Evaluate trained model performance on out-of-sample data

        Args:
            model_name: Model to evaluate
            evaluation_start: Evaluation period start
            evaluation_end: Evaluation period end

        Returns:
            Comprehensive performance metrics
        """
        try:
            logger.info(
                f"Evaluating model {model_name}: {evaluation_start} to {evaluation_end}"
            )

            # Load evaluation data
            eval_data = await self._load_training_data(
                evaluation_start, evaluation_end, use_cached_features=True
            )

            if not eval_data:
                raise ValueError("No evaluation data available")

            # Load trained model from MLflow
            model_uri = f"models:/{model_name}/latest"
            model = mlflow.lightgbm.load_model(model_uri)

            # Prepare evaluation dataset
            X, y_true, feature_names = await self._prepare_target_dataset(
                eval_data, model_name
            )

            # Generate predictions
            if self.model_configs[model_name]["objective"] == "binary":
                y_pred_proba = model.predict(X)
                y_pred = (y_pred_proba > 0.5).astype(int)

                # Calculate classification metrics
                metrics = {
                    "accuracy": accuracy_score(y_true, y_pred),
                    "precision": precision_score(y_true, y_pred),
                    "recall": recall_score(y_true, y_pred),
                    "f1_score": f1_score(y_true, y_pred),
                    "roc_auc": roc_auc_score(y_true, y_pred_proba),
                }
            else:
                y_pred = model.predict(X)
                # Calculate regression metrics
                metrics = {
                    "rmse": np.sqrt(np.mean((y_true - y_pred) ** 2)),
                    "mae": np.mean(np.abs(y_true - y_pred)),
                    "r2": 1
                    - (
                        np.sum((y_true - y_pred) ** 2)
                        / np.sum((y_true - np.mean(y_true)) ** 2)
                    ),
                }

            evaluation_results = {
                "model_name": model_name,
                "evaluation_period": f"{evaluation_start} to {evaluation_end}",
                "samples_evaluated": len(X),
                "performance_metrics": metrics,
                "feature_count": len(feature_names),
            }

            logger.info(f"Model evaluation completed: {len(X)} samples")
            return evaluation_results

        except Exception as e:
            logger.error(f"Error evaluating model {model_name}: {e}")
            raise

    async def _initialize_mlflow_tracking(self):
        """Initialize MLflow experiment tracking"""
        try:
            # Set MLflow tracking URI from configuration
            settings = get_settings()
            mlflow_uri = settings.mlflow.effective_tracking_uri
            mlflow.set_tracking_uri(mlflow_uri)

            # Create or get experiment
            try:
                experiment = mlflow.get_experiment_by_name(self.experiment_name)
                if experiment is None:
                    experiment_id = mlflow.create_experiment(self.experiment_name)
                    logger.info(f"Created MLflow experiment: {self.experiment_name}")
                else:
                    experiment_id = experiment.experiment_id
                    logger.info(
                        f"Using existing MLflow experiment: {self.experiment_name}"
                    )

                mlflow.set_experiment(self.experiment_name)

            except Exception as e:
                logger.warning(f"MLflow setup issue: {e}. Using default experiment.")

        except Exception as e:
            logger.error(f"Error initializing MLflow: {e}")
            raise

    async def _load_training_data(
        self, start_date: datetime, end_date: datetime, use_cached_features: bool = True
    ) -> list[dict[str, Any]]:
        """Load training data with feature vectors"""
        try:
            import asyncpg

            # Connect to database
            conn = await asyncpg.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
            )

            # Query for games with outcomes in the training period
            query = """
                SELECT DISTINCT
                    eg.id as game_id,
                    eg.game_datetime,
                    eg.home_team,
                    eg.away_team,
                    eg.home_score,
                    eg.away_score,
                    CASE WHEN eg.home_score > eg.away_score THEN 1 ELSE 0 END as home_win,
                    CASE WHEN (eg.home_score + eg.away_score) > 9.0 THEN 1 ELSE 0 END as over_total,
                    (eg.home_score + eg.away_score) as total_runs
                FROM curated.enhanced_games eg
                WHERE eg.game_datetime >= $1 
                    AND eg.game_datetime <= $2
                    AND eg.home_score IS NOT NULL 
                    AND eg.away_score IS NOT NULL
                ORDER BY eg.game_datetime
            """

            games = await conn.fetch(query, start_date, end_date)
            await conn.close()

            if not games:
                logger.warning(
                    f"No games found in training period: {start_date} to {end_date}"
                )
                return []

            logger.info(f"Found {len(games)} games for training")

            # Load feature vectors for each game
            training_data = []

            if use_cached_features:
                await self.redis_store.initialize()

            for game in games:
                game_dict = dict(game)
                game_id = game_dict["game_id"]
                game_datetime = game_dict["game_datetime"]

                # Calculate feature cutoff time (60 minutes before game)
                cutoff_time = game_datetime - timedelta(minutes=60)

                # Try to get features from cache first
                feature_vector = None
                if use_cached_features:
                    feature_vector = await self.redis_store.get_feature_vector(game_id)

                # Extract features if not cached
                if feature_vector is None:
                    feature_vector = (
                        await self.feature_pipeline.extract_features_for_game(
                            game_id, cutoff_time
                        )
                    )

                    # Cache extracted features
                    if feature_vector and use_cached_features:
                        await self.redis_store.cache_feature_vector(
                            game_id, feature_vector
                        )

                if feature_vector:
                    # Combine game outcome with features
                    training_sample = {**game_dict, "feature_vector": feature_vector}
                    training_data.append(training_sample)
                else:
                    logger.debug(f"No features available for game {game_id}")

            if use_cached_features:
                await self.redis_store.close()

            logger.info(f"Loaded {len(training_data)} training samples with features")
            return training_data

        except Exception as e:
            logger.error(f"Error loading training data: {e}")
            raise

    async def _prepare_target_dataset(
        self, training_data: list[dict[str, Any]], target: str
    ) -> tuple[np.ndarray, np.ndarray, list[str]]:
        """Prepare dataset for specific prediction target"""
        try:
            features_list = []
            targets_list = []

            for sample in training_data:
                feature_vector = sample["feature_vector"]

                # Extract target variable
                if target == "moneyline_home_win":
                    target_value = sample["home_win"]
                elif target == "total_over_under":
                    target_value = sample["over_total"]
                elif target == "run_total_regression":
                    target_value = sample["total_runs"]
                else:
                    raise ValueError(f"Unknown target: {target}")

                # Convert feature vector to flat array
                feature_array = self._feature_vector_to_array(feature_vector)

                if feature_array is not None and target_value is not None:
                    features_list.append(feature_array)
                    targets_list.append(target_value)

            if not features_list:
                raise ValueError(f"No valid samples for target {target}")

            X = np.array(features_list)
            y = np.array(targets_list)

            # Get feature names for interpretability
            feature_names = self._get_feature_names()

            logger.info(
                f"Prepared dataset for {target}: {X.shape[0]} samples, {X.shape[1]} features"
            )
            return X, y, feature_names

        except Exception as e:
            logger.error(f"Error preparing dataset for {target}: {e}")
            raise

    def _feature_vector_to_array(
        self, feature_vector: FeatureVector
    ) -> np.ndarray | None:
        """Convert FeatureVector to numpy array for model training"""
        try:
            feature_dict = {}

            # Extract temporal features
            if feature_vector.temporal_features:
                temporal_dict = feature_vector.temporal_features.model_dump()
                for key, value in temporal_dict.items():
                    if key not in ["feature_version", "last_updated"]:
                        if isinstance(value, Decimal):
                            feature_dict[f"temporal_{key}"] = float(value)
                        elif isinstance(value, (int, float)) and not pl.is_nan(value) if isinstance(value, float) else value is None:
                            feature_dict[f"temporal_{key}"] = float(value)
                        else:
                            feature_dict[f"temporal_{key}"] = 0.0

            # Extract market features
            if feature_vector.market_features:
                market_dict = feature_vector.market_features.model_dump()
                for key, value in market_dict.items():
                    if key not in ["feature_version", "calculation_timestamp"]:
                        if isinstance(value, Decimal):
                            feature_dict[f"market_{key}"] = float(value)
                        elif isinstance(value, (int, float)) and not pl.is_nan(value) if isinstance(value, float) else value is None:
                            feature_dict[f"market_{key}"] = float(value)
                        elif isinstance(value, list) and len(value) > 0:
                            feature_dict[f"market_{key}_count"] = len(value)
                        else:
                            feature_dict[f"market_{key}"] = 0.0

            # Extract team features
            if feature_vector.team_features:
                team_dict = feature_vector.team_features.model_dump()
                for key, value in team_dict.items():
                    if key not in ["feature_version", "mlb_api_last_updated"]:
                        if isinstance(value, Decimal):
                            feature_dict[f"team_{key}"] = float(value)
                        elif isinstance(value, (int, float)) and not pl.is_nan(value) if isinstance(value, float) else value is None:
                            feature_dict[f"team_{key}"] = float(value)
                        elif isinstance(value, str) and key.endswith("_record"):
                            # Parse win-loss records like "7-3"
                            try:
                                wins, losses = value.split("-")
                                win_pct = int(wins) / (int(wins) + int(losses))
                                feature_dict[f"team_{key}_pct"] = win_pct
                            except:
                                feature_dict[f"team_{key}_pct"] = 0.5
                        else:
                            feature_dict[f"team_{key}"] = 0.0

            # Extract betting splits features
            if feature_vector.betting_splits_features:
                splits_dict = feature_vector.betting_splits_features.model_dump()
                for key, value in splits_dict.items():
                    if key not in [
                        "feature_version",
                        "last_updated",
                        "data_sources",
                        "sportsbook_coverage",
                    ]:
                        if isinstance(value, Decimal):
                            feature_dict[f"splits_{key}"] = float(value)
                        elif isinstance(value, (int, float)) and not pl.is_nan(value) if isinstance(value, float) else value is None:
                            feature_dict[f"splits_{key}"] = float(value)
                        elif isinstance(value, list):
                            feature_dict[f"splits_{key}_count"] = len(value)
                        else:
                            feature_dict[f"splits_{key}"] = 0.0

            # Add derived and interaction features
            if feature_vector.derived_features:
                for key, value in feature_vector.derived_features.items():
                    if isinstance(value, (int, float, Decimal)) and not pl.is_nan(value) if isinstance(value, float) else value is None:
                        feature_dict[f"derived_{key}"] = float(value)
                    else:
                        feature_dict[f"derived_{key}"] = 0.0

            if feature_vector.interaction_features:
                for key, value in feature_vector.interaction_features.items():
                    if isinstance(value, (int, float, Decimal)) and not pl.is_nan(value) if isinstance(value, float) else value is None:
                        feature_dict[f"interaction_{key}"] = float(value)
                    else:
                        feature_dict[f"interaction_{key}"] = 0.0

            # Add quality metrics as features
            feature_dict["completeness_score"] = float(
                feature_vector.feature_completeness_score
            )
            feature_dict["source_coverage"] = feature_vector.data_source_coverage
            feature_dict["total_features"] = feature_vector.total_feature_count

            if not feature_dict:
                return None

            # Convert to sorted array for consistency
            sorted_keys = sorted(feature_dict.keys())
            feature_array = np.array([feature_dict[key] for key in sorted_keys])

            return feature_array

        except Exception as e:
            logger.error(f"Error converting feature vector to array: {e}")
            return None

    def _get_feature_names(self) -> list[str]:
        """Get consistent feature names for model interpretability"""
        # This should match the order in _feature_vector_to_array
        # For now, return a basic set - this should be expanded based on actual features
        base_features = [
            "temporal_minutes_before_game",
            "temporal_sharp_action_intensity_60min",
            "temporal_opening_to_current_ml",
            "temporal_money_vs_bet_divergence_home",
            "market_line_stability_score",
            "market_steam_move_indicators",
            "market_arbitrage_opportunity",
            "market_consensus_strength",
            "team_home_win_pct",
            "team_away_win_pct",
            "team_home_recent_form",
            "team_away_recent_form",
            "team_h2h_home_advantage",
            "splits_avg_money_home",
            "splits_sharp_action_signals",
            "splits_consensus_ml",
            "splits_weighted_sharp_score",
            "derived_combined_sharp_intensity",
            "derived_market_efficiency",
            "interaction_sharp_public_home",
            "interaction_consensus_team_strength",
            "completeness_score",
            "source_coverage",
            "total_features",
        ]

        return base_features

    async def _train_target_model(
        self,
        X: np.ndarray,
        y: np.ndarray,
        feature_names: list[str],
        target: str,
        cv_folds: int = 5,
        test_size: float = 0.2,
    ) -> dict[str, Any]:
        """Train LightGBM model for specific target"""
        try:
            # Split data chronologically (important for time series)
            train_size = int(len(X) * (1 - test_size))
            X_train, X_test = X[:train_size], X[train_size:]
            y_train, y_test = y[:train_size], y[train_size:]

            # Get model configuration
            model_config = self.model_configs[target].copy()

            # Create LightGBM datasets
            train_data = lgb.Dataset(X_train, label=y_train, feature_name=feature_names)
            valid_data = lgb.Dataset(
                X_test, label=y_test, reference=train_data, feature_name=feature_names
            )

            # Train model with early stopping
            callbacks = [
                lgb.early_stopping(stopping_rounds=50),
                lgb.log_evaluation(period=100),
            ]

            model = lgb.train(
                model_config,
                train_data,
                valid_sets=[valid_data],
                callbacks=callbacks,
                num_boost_round=1000,
            )

            # Generate predictions
            y_pred_train = model.predict(X_train)
            y_pred_test = model.predict(X_test)

            # Calculate metrics
            if model_config["objective"] == "binary":
                y_pred_train_class = (y_pred_train > 0.5).astype(int)
                y_pred_test_class = (y_pred_test > 0.5).astype(int)

                train_metrics = {
                    "accuracy": accuracy_score(y_train, y_pred_train_class),
                    "precision": precision_score(y_train, y_pred_train_class),
                    "recall": recall_score(y_train, y_pred_train_class),
                    "f1_score": f1_score(y_train, y_pred_train_class),
                    "roc_auc": roc_auc_score(y_train, y_pred_train),
                }

                test_metrics = {
                    "accuracy": accuracy_score(y_test, y_pred_test_class),
                    "precision": precision_score(y_test, y_pred_test_class),
                    "recall": recall_score(y_test, y_pred_test_class),
                    "f1_score": f1_score(y_test, y_pred_test_class),
                    "roc_auc": roc_auc_score(y_test, y_pred_test),
                }
            else:
                train_metrics = {
                    "rmse": np.sqrt(np.mean((y_train - y_pred_train) ** 2)),
                    "mae": np.mean(np.abs(y_train - y_pred_train)),
                }

                test_metrics = {
                    "rmse": np.sqrt(np.mean((y_test - y_pred_test) ** 2)),
                    "mae": np.mean(np.abs(y_test - y_pred_test)),
                }

            # Feature importance
            feature_importance = dict(zip(feature_names, model.feature_importance(), strict=False))

            # Cross-validation scores
            cv_scores = await self._perform_cross_validation(
                X_train, y_train, model_config, cv_folds
            )

            return {
                "model": model,
                "train_metrics": train_metrics,
                "test_metrics": test_metrics,
                "cv_scores": cv_scores,
                "feature_importance": feature_importance,
                "training_samples": len(X_train),
                "test_samples": len(X_test),
                "model_config": model_config,
            }

        except Exception as e:
            logger.error(f"Error training model for {target}: {e}")
            raise

    async def _perform_cross_validation(
        self, X: np.ndarray, y: np.ndarray, model_config: dict[str, Any], cv_folds: int
    ) -> dict[str, float]:
        """Perform time series cross-validation"""
        try:
            # Use TimeSeriesSplit for chronological data
            tscv = TimeSeriesSplit(n_splits=cv_folds)

            cv_scores = []

            for train_idx, val_idx in tscv.split(X):
                X_cv_train, X_cv_val = X[train_idx], X[val_idx]
                y_cv_train, y_cv_val = y[train_idx], y[val_idx]

                # Train fold model
                train_data = lgb.Dataset(X_cv_train, label=y_cv_train)
                val_data = lgb.Dataset(X_cv_val, label=y_cv_val, reference=train_data)

                fold_model = lgb.train(
                    model_config,
                    train_data,
                    valid_sets=[val_data],
                    callbacks=[lgb.early_stopping(stopping_rounds=20)],
                    num_boost_round=500,
                    verbose_eval=False,
                )

                # Evaluate fold
                y_pred = fold_model.predict(X_cv_val)

                if model_config["objective"] == "binary":
                    fold_score = roc_auc_score(y_cv_val, y_pred)
                else:
                    fold_score = -np.sqrt(
                        np.mean((y_cv_val - y_pred) ** 2)
                    )  # Negative RMSE

                cv_scores.append(fold_score)

            return {
                "cv_mean": np.mean(cv_scores),
                "cv_std": np.std(cv_scores),
                "cv_scores": cv_scores,
            }

        except Exception as e:
            logger.error(f"Error in cross-validation: {e}")
            return {"cv_mean": 0.0, "cv_std": 0.0, "cv_scores": []}

    async def _log_model_results(
        self,
        results: dict[str, Any],
        target: str,
        feature_names: list[str],
        is_retrain: bool = False,
    ):
        """Log model results to MLflow"""
        try:
            # Log metrics
            for metric_name, metric_value in results["train_metrics"].items():
                mlflow.log_metric(f"train_{metric_name}", metric_value)

            for metric_name, metric_value in results["test_metrics"].items():
                mlflow.log_metric(f"test_{metric_name}", metric_value)

            # Log cross-validation metrics
            mlflow.log_metric("cv_mean_score", results["cv_scores"]["cv_mean"])
            mlflow.log_metric("cv_std_score", results["cv_scores"]["cv_std"])

            # Log model parameters
            mlflow.log_params(results["model_config"])

            # Log feature importance
            importance_dict = {
                f"importance_{k}": v for k, v in results["feature_importance"].items()
            }
            mlflow.log_metrics(importance_dict)

            # Log model artifact
            model_name = f"{target}_retrain" if is_retrain else target
            mlflow.lightgbm.log_model(
                results["model"],
                artifact_path="model",
                registered_model_name=model_name,
            )

            # Log feature names
            with open("feature_names.json", "w") as f:
                json.dump(feature_names, f)
            mlflow.log_artifact("feature_names.json")

            logger.info(f"Logged {target} model results to MLflow")

        except Exception as e:
            logger.error(f"Error logging model results: {e}")

    def _detect_feature_drift(
        self,
        current_importance: dict[str, float],
        model_name: str,
        drift_threshold: float = 0.1,
    ) -> bool:
        """Detect feature importance drift from baseline"""
        try:
            if model_name not in self.baseline_feature_importance:
                # Store as baseline
                self.baseline_feature_importance[model_name] = current_importance.copy()
                return False

            baseline = self.baseline_feature_importance[model_name]

            # Calculate drift score (normalized L2 distance)
            common_features = set(baseline.keys()) & set(current_importance.keys())

            if not common_features:
                return True  # No common features is significant drift

            drift_scores = []
            for feature in common_features:
                baseline_imp = baseline[feature]
                current_imp = current_importance[feature]

                # Normalize by baseline importance
                if baseline_imp > 0:
                    drift_score = abs(current_imp - baseline_imp) / baseline_imp
                    drift_scores.append(drift_score)

            if not drift_scores:
                return False

            avg_drift = np.mean(drift_scores)
            drift_detected = avg_drift > drift_threshold

            logger.info(
                f"Feature drift analysis for {model_name}: avg_drift={avg_drift:.3f}, threshold={drift_threshold}"
            )

            return drift_detected

        except Exception as e:
            logger.error(f"Error detecting feature drift: {e}")
            return False

    def _update_training_stats(
        self, training_results: dict[str, Any], training_time: float
    ):
        """Update training statistics"""
        self.training_stats["models_trained"] += len(training_results)
        self.training_stats["experiments_logged"] += len(training_results)

        # Update average training time
        current_avg = self.training_stats["avg_training_time_seconds"]
        total_experiments = self.training_stats["experiments_logged"]
        new_avg = (
            (current_avg * (total_experiments - len(training_results))) + training_time
        ) / total_experiments
        self.training_stats["avg_training_time_seconds"] = new_avg

        # Update best model scores
        for target, results in training_results.items():
            if target not in self.training_stats["best_model_scores"]:
                self.training_stats["best_model_scores"][target] = {}

            for metric, value in results["test_metrics"].items():
                current_best = self.training_stats["best_model_scores"][target].get(
                    metric, -float("inf")
                )
                if value > current_best:
                    self.training_stats["best_model_scores"][target][metric] = value

    def get_training_stats(self) -> dict[str, Any]:
        """Get training pipeline statistics"""
        return self.training_stats.copy()
