"""
ML Prediction Service
Handles model predictions, caching, and database interactions
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, date, timedelta
import json
import pickle
import asyncio
from decimal import Decimal
import os

# Proper package imports

import asyncpg
import mlflow
import mlflow.lightgbm
from pydantic import ValidationError

from ..features.feature_pipeline import FeaturePipeline
from ..features.redis_feature_store import RedisFeatureStore
from ..training.lightgbm_trainer import LightGBMTrainer
from ..database.connection_pool import get_db_transaction
try:
    from ...core.config import get_unified_config
except ImportError:
    # Fallback for testing environments
    get_unified_config = None

logger = logging.getLogger(__name__)


class PredictionService:
    """Service for handling ML predictions and model management"""
    
    def __init__(self):
        self.redis_client = None
        self.db_pool = None
        self.models = {}
        self.feature_pipeline = None
        self.redis_store = None
        self.trainer = None
        self.config = None
        
    async def initialize(self):
        """Initialize the prediction service"""
        try:
            logger.info("Initializing ML Prediction Service...")
            
            # Load configuration
            if get_unified_config:
                self.config = get_unified_config()
            else:
                # Fallback configuration for testing
                self.config = type('Config', (), {
                    'redis': type('Redis', (), {'url': 'redis://localhost:6379/0'}),
                    'mlflow': type('MLflow', (), {
                        'tracking_uri': 'sqlite:///mlflow.db',
                        'experiment_name': 'mlb_betting_predictions'
                    })
                })()
            
            # Initialize database connection pool
            await self._initialize_database()
            
            # Initialize feature pipeline
            self.feature_pipeline = FeaturePipeline(feature_version="v2.1")
            logger.info("✅ Feature pipeline initialized")
            
            # Initialize Redis feature store
            self.redis_store = RedisFeatureStore(
                redis_url=self.config.redis.url,
                use_msgpack=True,
                default_ttl=900  # 15 minutes
            )
            await self.redis_store.initialize()
            logger.info("✅ Redis feature store initialized")
            
            # Initialize trainer for model loading
            self.trainer = LightGBMTrainer(
                feature_pipeline=self.feature_pipeline,
                redis_feature_store=self.redis_store
            )
            logger.info("✅ LightGBM trainer initialized")
            
            # Configure MLflow
            mlflow.set_tracking_uri(self.config.mlflow.tracking_uri)
            mlflow.set_experiment(self.config.mlflow.experiment_name)
            
            # Load active models
            await self._load_active_models()
            
            logger.info("✅ ML Prediction Service initialized with all components")
            
        except Exception as e:
            logger.error(f"Failed to initialize prediction service: {e}")
            raise
    
    async def _initialize_database(self):
        """Initialize database connection pool"""
        try:
            # Use environment variables for database connection
            db_config = {
                'host': os.getenv('DATABASE_HOST', 'localhost'), 
                'port': int(os.getenv('DATABASE_PORT', '5432')),
                'database': os.getenv('DATABASE_NAME', 'mlb_betting'),
                'user': os.getenv('DATABASE_USERNAME', 'postgres'),
                'password': os.getenv('DATABASE_PASSWORD', '')
            }
            
            dsn = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            
            self.db_pool = await asyncpg.create_pool(
                dsn,
                min_size=2,
                max_size=10,
                command_timeout=30
            )
            
            logger.info("✅ Database connection pool created")
            
        except Exception as e:
            logger.error(f"Database initialization error: {e}")
            raise
    
    async def _load_active_models(self):
        """Load active models from MLflow registry"""
        try:
            # Query database for active models
            async with self.db_pool.acquire() as conn:
                query = """
                    SELECT DISTINCT 
                        experiment_name,
                        run_id, 
                        model_name,
                        model_version,
                        prediction_target,
                        is_active,
                        created_at,
                        metrics
                    FROM curated.ml_experiments 
                    WHERE is_active = true
                    ORDER BY created_at DESC
                """
                
                rows = await conn.fetch(query)
                
                for row in rows:
                    model_key = f"{row['model_name']}_{row['model_version']}"
                    
                    try:
                        # Load model from MLflow
                        model_uri = f"runs:/{row['run_id']}/model"
                        model = mlflow.lightgbm.load_model(model_uri)
                        
                        self.models[model_key] = {
                            'model': model,
                            'model_name': row['model_name'],
                            'model_version': row['model_version'],
                            'prediction_target': row['prediction_target'],
                            'run_id': row['run_id'],
                            'created_at': row['created_at'],
                            'metrics': row['metrics'] or {}
                        }
                        
                        logger.info(f"✅ Loaded model: {model_key}")
                        
                    except Exception as e:
                        logger.error(f"Failed to load model {model_key}: {e}")
                        continue
                
                logger.info(f"✅ Loaded {len(self.models)} active models")
        
        except Exception as e:
            logger.error(f"Error loading active models: {e}")
            # Continue without models for now
    
    async def cleanup(self):
        """Cleanup resources"""
        try:
            if self.db_pool:
                await self.db_pool.close()
            logger.info("✅ Prediction service cleaned up")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")
    
    async def get_prediction(
        self, 
        game_id: str, 
        model_name: Optional[str] = None,
        include_explanation: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Get ML prediction for a single game
        """
        try:
            logger.info(f"Getting prediction for game {game_id}")
            
            # Convert string game_id to int
            try:
                game_id_int = int(game_id)
            except ValueError:
                logger.error(f"Invalid game_id format: {game_id}")
                return None
            
            # 1. Check cache for existing prediction
            cached_prediction = await self._get_cached_prediction_data(game_id_int, model_name)
            if cached_prediction:
                logger.info(f"Returning cached prediction for game {game_id}")
                return cached_prediction
            
            # 2. Get game information for cutoff time
            game_info = await self._get_game_info(game_id_int)
            if not game_info:
                logger.error(f"Game {game_id} not found")
                return None
            
            # Calculate 60-minute cutoff time
            cutoff_time = game_info['game_datetime'] - timedelta(minutes=60)
            if datetime.now() < cutoff_time:
                logger.warning(f"Too early for prediction - cutoff time: {cutoff_time}")
                return None
                
            # 3. Extract features
            feature_vector = await self.feature_pipeline.extract_features_for_game(
                game_id=game_id_int,
                cutoff_time=cutoff_time
            )
            
            if not feature_vector:
                logger.error(f"Failed to extract features for game {game_id}")
                return None
            
            # 4. Generate predictions for all available models
            predictions = {}
            explanations = {}
            
            # Select models to use
            models_to_use = self._select_models(model_name)
            
            for model_key, model_info in models_to_use.items():
                try:
                    # Convert feature vector to model input format
                    X = await self._prepare_features_for_model(feature_vector, model_info)
                    
                    # Make prediction
                    if model_info['prediction_target'] in ['moneyline_home_win', 'total_over_under']:
                        # Binary classification
                        probabilities = model_info['model'].predict_proba(X)[0]
                        prediction_prob = probabilities[1] if len(probabilities) > 1 else probabilities[0]
                        prediction_binary = 1 if prediction_prob > 0.5 else 0
                        confidence = max(prediction_prob, 1 - prediction_prob)
                    else:
                        # Regression
                        prediction_value = model_info['model'].predict(X)[0]
                        prediction_prob = None
                        prediction_binary = None
                        confidence = 0.7  # Default confidence for regression
                    
                    # Store prediction
                    target = model_info['prediction_target']
                    predictions[target] = {
                        'probability': float(prediction_prob) if prediction_prob is not None else None,
                        'binary': prediction_binary,
                        'value': float(prediction_value) if 'prediction_value' in locals() else None,
                        'confidence': float(confidence),
                        'model_name': model_info['model_name'],
                        'model_version': model_info['model_version']
                    }
                    
                    # Generate explanation if requested
                    if include_explanation:
                        explanations[target] = await self._generate_explanation(
                            model_info, feature_vector, X
                        )
                        
                except Exception as e:
                    logger.error(f"Prediction error for model {model_key}: {e}")
                    continue
            
            if not predictions:
                logger.error(f"No successful predictions for game {game_id}")
                return None
            
            # 5. Create response
            prediction_response = self._format_prediction_response(
                game_id=game_id,
                predictions=predictions,
                feature_vector=feature_vector,
                explanations=explanations if include_explanation else None
            )
            
            # 6. Cache prediction
            await self._cache_prediction(game_id_int, prediction_response)
            
            # 7. Store in database
            await self._store_prediction_in_database(game_id_int, prediction_response, feature_vector)
            
            logger.info(f"Generated prediction for game {game_id} with {len(predictions)} models")
            return prediction_response
            
        except Exception as e:
            logger.error(f"Prediction error for game {game_id}: {e}", exc_info=True)
            return None
    
    async def _get_cached_prediction_data(self, game_id: int, model_name: Optional[str]) -> Optional[Dict[str, Any]]:
        """Get cached prediction from Redis"""
        try:
            if not self.redis_store:
                return None
                
            # Try to get cached prediction from Redis
            cache_key = f"ml:predictions:game:{game_id}"
            if model_name:
                cache_key += f":model:{model_name}"
                
            cached_data = await self.redis_store.redis_client.get(cache_key)
            if cached_data:
                if self.redis_store.use_msgpack:
                    import msgpack
                    return msgpack.unpackb(cached_data, raw=False)
                else:
                    return json.loads(cached_data)
                    
            return None
            
        except Exception as e:
            logger.error(f"Cache retrieval error for game {game_id}: {e}")
            return None
    
    async def _get_game_info(self, game_id: int) -> Optional[Dict[str, Any]]:
        """Get game information from database"""
        try:
            async with self.db_pool.acquire() as conn:
                query = """
                    SELECT 
                        game_id,
                        game_datetime,
                        home_team,
                        away_team,
                        season,
                        game_status
                    FROM curated.enhanced_games 
                    WHERE game_id = $1
                """
                
                row = await conn.fetchrow(query, game_id)
                if row:
                    return dict(row)
                return None
                
        except Exception as e:
            logger.error(f"Error getting game info for {game_id}: {e}")
            return None
    
    def _select_models(self, model_name: Optional[str]) -> Dict[str, Dict[str, Any]]:
        """Select models to use for prediction"""
        if not self.models:
            logger.warning("No models loaded")
            return {}
            
        if model_name:
            # Find specific model
            for key, model_info in self.models.items():
                if model_info['model_name'] == model_name:
                    return {key: model_info}
            logger.warning(f"Model {model_name} not found")
            return {}
        else:
            # Return all active models
            return self.models
    
    async def _prepare_features_for_model(self, feature_vector, model_info) -> List[List[float]]:
        """Convert feature vector to model input format"""
        try:
            # Get feature names from model metadata
            feature_names = self.trainer.model_configs.get(
                model_info['prediction_target'], {}
            ).get('feature_names', [])
            
            # Convert feature vector to flat list
            features = []
            
            # Extract features in the correct order
            feature_dict = feature_vector.model_dump()
            
            # Flatten nested feature categories
            flat_features = {}
            for category, category_data in feature_dict.items():
                if isinstance(category_data, dict):
                    for feature_name, value in category_data.items():
                        flat_features[f"{category}.{feature_name}"] = value
                else:
                    flat_features[category] = category_data
            
            # Create feature array in correct order
            if feature_names:
                features = [float(flat_features.get(name, 0.0)) for name in feature_names]
            else:
                # Use all available features
                features = [float(v) if v is not None else 0.0 for v in flat_features.values()]
            
            return [features]  # LightGBM expects 2D array
            
        except Exception as e:
            logger.error(f"Feature preparation error: {e}")
            raise
    
    async def _generate_explanation(self, model_info, feature_vector, X) -> Dict[str, Any]:
        """Generate model explanation"""
        try:
            # Get feature importance from model
            if hasattr(model_info['model'], 'feature_importances_'):
                importances = model_info['model'].feature_importances_
                
                # Get feature names
                feature_names = self.trainer.model_configs.get(
                    model_info['prediction_target'], {}
                ).get('feature_names', [f'feature_{i}' for i in range(len(importances))])
                
                # Sort by importance
                feature_importance = list(zip(feature_names, importances))
                feature_importance.sort(key=lambda x: abs(x[1]), reverse=True)
                
                return {
                    'top_features': [name for name, _ in feature_importance[:10]],
                    'feature_importance': {
                        name: float(importance) 
                        for name, importance in feature_importance[:10]
                    },
                    'model_type': 'lightgbm',
                    'num_features': len(feature_names)
                }
            
            return {
                'explanation': 'Feature importance not available for this model',
                'model_type': 'lightgbm'
            }
            
        except Exception as e:
            logger.error(f"Explanation generation error: {e}")
            return {'error': 'Failed to generate explanation'}
    
    def _format_prediction_response(
        self, 
        game_id: str, 
        predictions: Dict[str, Any], 
        feature_vector,
        explanations: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Format prediction response"""
        
        response = {
            'game_id': game_id,
            'prediction_timestamp': datetime.utcnow(),
            'feature_version': feature_vector.feature_version,
            'feature_cutoff_time': feature_vector.feature_cutoff_time,
            'confidence_threshold_met': True,  # TODO: Implement threshold logic
            'risk_level': 'medium'  # TODO: Implement risk calculation
        }
        
        # Add predictions based on available models
        for target, pred_data in predictions.items():
            if target == 'moneyline_home_win':
                response.update({
                    'home_ml_probability': pred_data['probability'],
                    'home_ml_binary': pred_data['binary'],
                    'home_ml_confidence': pred_data['confidence'],
                    'model_name': pred_data['model_name'],
                    'model_version': pred_data['model_version']
                })
            elif target == 'total_over_under':
                response.update({
                    'total_over_probability': pred_data['probability'],
                    'total_over_binary': pred_data['binary'],
                    'total_over_confidence': pred_data['confidence']
                })
            elif target == 'run_total_regression':
                response.update({
                    'predicted_total_runs': pred_data['value'],
                    'total_runs_confidence': pred_data['confidence']
                })
        
        # Add explanations if provided
        if explanations:
            response['explanation'] = explanations
            
        # Add basic betting recommendations (placeholder)
        if 'total_over_probability' in response and response['total_over_probability']:
            response['betting_recommendations'] = {
                'total_over': {
                    'expected_value': 0.0,  # TODO: Calculate EV
                    'kelly_fraction': 0.0,  # TODO: Implement Kelly Criterion
                    'recommended_bet_size': 0.0,
                    'confidence_required': 0.6
                }
            }
        
        return response
    
    async def _cache_prediction(self, game_id: int, prediction_data: Dict[str, Any]):
        """Cache prediction in Redis"""
        try:
            if not self.redis_store:
                return
                
            cache_key = f"ml:predictions:game:{game_id}"
            
            # Serialize prediction data
            if self.redis_store.use_msgpack:
                import msgpack
                serialized_data = msgpack.packb(prediction_data, use_bin_type=True)
            else:
                serialized_data = json.dumps(prediction_data, default=str)
            
            # Cache for 4 hours (predictions are valid until game starts)
            ttl = 4 * 60 * 60
            await self.redis_store.redis_client.setex(cache_key, ttl, serialized_data)
            
            logger.info(f"Cached prediction for game {game_id}")
            
        except Exception as e:
            logger.error(f"Prediction caching error for game {game_id}: {e}")
    
    async def _store_prediction_in_database(self, game_id: int, prediction_data: Dict[str, Any], feature_vector):
        """Store prediction in database with proper transaction management"""
        try:
            async with get_db_transaction() as conn:
                # Store in ml_predictions table
                query = """
                    INSERT INTO curated.ml_predictions (
                        game_id, feature_vector_id, model_name, model_version,
                        prediction_target, prediction_value, prediction_probability,
                        confidence_score, feature_version, created_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (game_id, model_name, prediction_target) 
                    DO UPDATE SET
                        prediction_value = EXCLUDED.prediction_value,
                        prediction_probability = EXCLUDED.prediction_probability,
                        confidence_score = EXCLUDED.confidence_score,
                        updated_at = NOW()
                """
                
                # Store each prediction target separately
                for target_key, field_mapping in [
                    ('moneyline_home_win', ('home_ml_probability', 'home_ml_binary', 'home_ml_confidence')),
                    ('total_over_under', ('total_over_probability', 'total_over_binary', 'total_over_confidence'))
                ]:
                    prob_field, binary_field, conf_field = field_mapping
                    
                    if prob_field in prediction_data:
                        await conn.execute(
                            query,
                            game_id,                                    # game_id
                            None,                                       # feature_vector_id (TODO: implement)
                            prediction_data.get('model_name', 'unknown'),
                            prediction_data.get('model_version', '1.0'),
                            target_key,                                 # prediction_target
                            prediction_data.get(binary_field),         # prediction_value
                            prediction_data.get(prob_field),           # prediction_probability
                            prediction_data.get(conf_field),           # confidence_score
                            feature_vector.feature_version,
                            datetime.utcnow()
                        )
                
                logger.info(f"Stored prediction in database for game {game_id}")
                
        except Exception as e:
            logger.error(f"Database storage error for game {game_id}: {e}")
    
    async def get_batch_predictions(
        self,
        game_ids: List[str],
        model_name: Optional[str] = None,
        include_explanation: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Get ML predictions for multiple games with parallel processing
        """
        try:
            logger.info(f"Getting batch predictions for {len(game_ids)} games")
            
            # Process predictions concurrently for better performance
            async def get_single_prediction(game_id: str):
                try:
                    return await self.get_prediction(
                        game_id=game_id,
                        model_name=model_name,
                        include_explanation=include_explanation
                    )
                except Exception as e:
                    logger.error(f"Error predicting game {game_id}: {e}")
                    return None
            
            # Create concurrent tasks
            tasks = [get_single_prediction(game_id) for game_id in game_ids]
            
            # Execute with concurrency limit to prevent overwhelming the system
            predictions = []
            batch_size = 10  # Process 10 games at a time
            
            for i in range(0, len(tasks), batch_size):
                batch_tasks = tasks[i:i + batch_size]
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                
                for result in batch_results:
                    if isinstance(result, Exception):
                        logger.error(f"Batch prediction exception: {result}")
                    elif result is not None:
                        predictions.append(result)
            
            logger.info(f"Successfully generated {len(predictions)} predictions from {len(game_ids)} requests")
            return predictions
            
        except Exception as e:
            logger.error(f"Batch prediction error: {e}")
            return []
    
    async def get_cached_prediction(
        self,
        game_id: str,
        model_name: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get cached prediction from Redis or database
        """
        try:
            game_id_int = int(game_id)
            
            # First check Redis cache
            cached_data = await self._get_cached_prediction_data(game_id_int, model_name)
            if cached_data:
                return cached_data
            
            # If not in cache, check database
            async with self.db_pool.acquire() as conn:
                query = """
                    SELECT 
                        p.game_id,
                        p.model_name,
                        p.model_version,
                        p.prediction_target,
                        p.prediction_value,
                        p.prediction_probability,
                        p.confidence_score,
                        p.feature_version,
                        p.created_at
                    FROM curated.ml_predictions p
                    WHERE p.game_id = $1
                    AND ($2 IS NULL OR p.model_name = $2)
                    ORDER BY p.created_at DESC
                """
                
                rows = await conn.fetch(query, game_id_int, model_name)
                
                if rows:
                    # Convert database rows to prediction response format
                    prediction_data = self._convert_db_rows_to_prediction(rows)
                    
                    # Cache the result for future requests
                    await self._cache_prediction(game_id_int, prediction_data)
                    
                    return prediction_data
            
            return None
            
        except Exception as e:
            logger.error(f"Cached prediction error: {e}")
            return None
    
    def _convert_db_rows_to_prediction(self, rows) -> Dict[str, Any]:
        """Convert database prediction rows to API response format"""
        try:
            if not rows:
                return None
            
            # Group by prediction target
            predictions_by_target = {}
            base_info = None
            
            for row in rows:
                target = row['prediction_target']
                predictions_by_target[target] = {
                    'probability': float(row['prediction_probability']) if row['prediction_probability'] else None,
                    'binary': row['prediction_value'],
                    'confidence': float(row['confidence_score']) if row['confidence_score'] else None,
                    'model_name': row['model_name'],
                    'model_version': row['model_version']
                }
                
                if not base_info:
                    base_info = {
                        'game_id': str(row['game_id']),
                        'prediction_timestamp': row['created_at'],
                        'feature_version': row['feature_version']
                    }
            
            # Format response similar to live predictions
            response = base_info.copy()
            response.update({
                'confidence_threshold_met': True,
                'risk_level': 'medium'
            })
            
            # Add prediction fields based on available targets
            for target, pred_data in predictions_by_target.items():
                if target == 'moneyline_home_win':
                    response.update({
                        'home_ml_probability': pred_data['probability'],
                        'home_ml_binary': pred_data['binary'],
                        'home_ml_confidence': pred_data['confidence'],
                        'model_name': pred_data['model_name'],
                        'model_version': pred_data['model_version']
                    })
                elif target == 'total_over_under':
                    response.update({
                        'total_over_probability': pred_data['probability'],
                        'total_over_binary': pred_data['binary'],
                        'total_over_confidence': pred_data['confidence']
                    })
            
            return response
            
        except Exception as e:
            logger.error(f"Error converting DB rows to prediction: {e}")
            return None
    
    async def get_todays_predictions(
        self,
        model_name: Optional[str] = None,
        min_confidence: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all predictions for today's games
        """
        try:
            logger.info("Getting today's predictions")
            
            async with self.db_pool.acquire() as conn:
                # Get today's games
                query = """
                    SELECT DISTINCT eg.game_id
                    FROM curated.enhanced_games eg
                    WHERE DATE(eg.game_datetime) = CURRENT_DATE
                    AND eg.game_status IN ('scheduled', 'pre-game')
                    ORDER BY eg.game_datetime
                """
                
                rows = await conn.fetch(query)
                game_ids = [str(row['game_id']) for row in rows]
                
                if not game_ids:
                    logger.info("No games scheduled for today")
                    return []
                
                # Get predictions for today's games
                predictions = []
                for game_id in game_ids:
                    try:
                        # Try cached first, then generate if needed
                        prediction = await self.get_cached_prediction(game_id, model_name)
                        
                        if not prediction:
                            # Generate new prediction if not cached
                            prediction = await self.get_prediction(game_id, model_name)
                        
                        if prediction:
                            # Apply confidence filter if specified
                            if min_confidence is None or self._meets_confidence_threshold(prediction, min_confidence):
                                predictions.append(prediction)
                                
                    except Exception as e:
                        logger.error(f"Error getting prediction for game {game_id}: {e}")
                        continue
                
                logger.info(f"Retrieved {len(predictions)} predictions for today")
                return predictions
            
        except Exception as e:
            logger.error(f"Today's predictions error: {e}")
            return []
    
    def _meets_confidence_threshold(self, prediction: Dict[str, Any], min_confidence: float) -> bool:
        """Check if prediction meets minimum confidence threshold"""
        try:
            # Check confidence across all available predictions
            confidence_fields = ['home_ml_confidence', 'total_over_confidence', 'total_runs_confidence']
            
            for field in confidence_fields:
                if field in prediction and prediction[field] is not None:
                    if prediction[field] >= min_confidence:
                        return True
                        
            return False
            
        except Exception as e:
            logger.error(f"Error checking confidence threshold: {e}")
            return False
    
    async def get_active_models(self) -> List[Dict[str, Any]]:
        """
        Get list of active models from database
        """
        try:
            async with self.db_pool.acquire() as conn:
                query = """
                    SELECT 
                        me.model_name,
                        me.model_version,
                        me.prediction_target as target_variable,
                        me.is_active,
                        me.created_at,
                        me.metrics,
                        COUNT(mp.prediction_id) as total_predictions,
                        AVG(CASE WHEN mp.actual_outcome IS NOT NULL 
                            THEN (mp.prediction_value::int = mp.actual_outcome::int)::int 
                            ELSE NULL END) as recent_accuracy
                    FROM curated.ml_experiments me
                    LEFT JOIN curated.ml_predictions mp ON me.model_name = mp.model_name
                        AND mp.created_at >= NOW() - INTERVAL '30 days'
                    WHERE me.is_active = true
                    GROUP BY me.model_name, me.model_version, me.prediction_target, 
                             me.is_active, me.created_at, me.metrics
                    ORDER BY me.created_at DESC
                """
                
                rows = await conn.fetch(query)
                
                models = []
                for row in rows:
                    model_info = {
                        'model_name': row['model_name'],
                        'model_version': row['model_version'],
                        'model_type': 'lightgbm',
                        'is_active': row['is_active'],
                        'created_at': row['created_at'],
                        'target_variable': row['target_variable'],
                        'total_predictions': row['total_predictions'] or 0,
                        'recent_accuracy': float(row['recent_accuracy']) if row['recent_accuracy'] else None,
                        'feature_version': 'v2.1'  # Current feature version
                    }
                    
                    # Extract metrics from JSON if available
                    if row['metrics']:
                        metrics = row['metrics']
                        model_info.update({
                            'recent_roi': metrics.get('roi_percentage', 0.0),
                            'precision_score': metrics.get('precision_score'),
                            'recall_score': metrics.get('recall_score'),
                            'f1_score': metrics.get('f1_score'),
                            'roc_auc': metrics.get('roc_auc')
                        })
                    
                    models.append(model_info)
                
                return models
            
        except Exception as e:
            logger.error(f"Active models error: {e}")
            return []
    
    async def get_model_info(
        self,
        model_name: str,
        model_version: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get detailed model information
        """
        try:
            async with self.db_pool.acquire() as conn:
                query = """
                    SELECT 
                        me.model_name,
                        me.model_version,
                        me.experiment_name,
                        me.run_id,
                        me.prediction_target,
                        me.is_active,
                        me.created_at,
                        me.updated_at,
                        me.metrics,
                        me.hyperparameters,
                        COUNT(mp.prediction_id) as total_predictions,
                        AVG(CASE WHEN mp.actual_outcome IS NOT NULL 
                            THEN (mp.prediction_value::int = mp.actual_outcome::int)::int 
                            ELSE NULL END) as recent_accuracy
                    FROM curated.ml_experiments me
                    LEFT JOIN curated.ml_predictions mp ON me.model_name = mp.model_name
                        AND mp.created_at >= NOW() - INTERVAL '30 days'
                    WHERE me.model_name = $1
                    AND ($2 IS NULL OR me.model_version = $2)
                    GROUP BY me.model_name, me.model_version, me.experiment_name, 
                             me.run_id, me.prediction_target, me.is_active, 
                             me.created_at, me.updated_at, me.metrics, me.hyperparameters
                    ORDER BY me.created_at DESC
                    LIMIT 1
                """
                
                row = await conn.fetchrow(query, model_name, model_version)
                
                if row:
                    model_info = {
                        'model_name': row['model_name'],
                        'model_version': row['model_version'],
                        'experiment_name': row['experiment_name'],
                        'run_id': row['run_id'],
                        'prediction_target': row['prediction_target'],
                        'model_type': 'lightgbm',
                        'is_active': row['is_active'],
                        'created_at': row['created_at'],
                        'updated_at': row['updated_at'],
                        'total_predictions': row['total_predictions'] or 0,
                        'recent_accuracy': float(row['recent_accuracy']) if row['recent_accuracy'] else None,
                        'feature_version': 'v2.1'
                    }
                    
                    # Add metrics and hyperparameters
                    if row['metrics']:
                        model_info['metrics'] = row['metrics']
                        
                    if row['hyperparameters']:
                        model_info['hyperparameters'] = row['hyperparameters']
                    
                    return model_info
                
                return None
            
        except Exception as e:
            logger.error(f"Model info error: {e}")
            return None
    
    async def get_model_performance(
        self,
        model_name: str,
        model_version: Optional[str] = None,
        prediction_type: Optional[str] = None,
        days: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Get model performance metrics from database
        """
        try:
            async with self.db_pool.acquire() as conn:
                query = """
                    SELECT 
                        mp.model_name,
                        mp.model_version,
                        mp.prediction_target,
                        COUNT(*) as total_predictions,
                        AVG(CASE WHEN mp.actual_outcome IS NOT NULL 
                            THEN (mp.prediction_value::int = mp.actual_outcome::int)::int 
                            ELSE NULL END) as accuracy,
                        AVG(mp.confidence_score) as avg_confidence,
                        MIN(mp.created_at) as period_start,
                        MAX(mp.created_at) as period_end
                    FROM curated.ml_predictions mp
                    WHERE mp.model_name = $1
                    AND ($2 IS NULL OR mp.model_version = $2)
                    AND ($3 IS NULL OR mp.prediction_target = $3)
                    AND mp.created_at >= NOW() - INTERVAL '%s days'
                    GROUP BY mp.model_name, mp.model_version, mp.prediction_target
                    ORDER BY mp.model_name, mp.prediction_target
                """ % days
                
                rows = await conn.fetch(query, model_name, model_version, prediction_type)
                
                performance_data = []
                for row in rows:
                    perf_data = {
                        'model_name': row['model_name'],
                        'model_version': row['model_version'],
                        'prediction_type': row['prediction_target'],
                        'evaluation_period_start': row['period_start'],
                        'evaluation_period_end': row['period_end'],
                        'total_predictions': row['total_predictions'],
                        'accuracy': float(row['accuracy']) if row['accuracy'] else None,
                        'avg_confidence': float(row['avg_confidence']) if row['avg_confidence'] else None,
                        'hit_rate': float(row['accuracy']) if row['accuracy'] else None
                    }
                    performance_data.append(perf_data)
                
                return performance_data
            
        except Exception as e:
            logger.error(f"Model performance error: {e}")
            return []
    
    async def get_model_leaderboard(
        self,
        metric: str = 'recent_accuracy',
        prediction_type: Optional[str] = None,
        days: int = 30,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get model leaderboard ranked by specified metric
        """
        try:
            models = await self.get_active_models()
            
            # Filter by prediction type if specified
            if prediction_type:
                models = [m for m in models if m.get('target_variable') == prediction_type]
            
            # Sort by specified metric
            metric_key = 'recent_accuracy' if metric == 'accuracy' else metric
            valid_models = [m for m in models if m.get(metric_key) is not None]
            
            sorted_models = sorted(
                valid_models, 
                key=lambda x: x.get(metric_key, 0), 
                reverse=True
            )
            
            return sorted_models[:limit]
            
        except Exception as e:
            logger.error(f"Model leaderboard error: {e}")
            return []
    
    async def get_model_recent_predictions(
        self,
        model_name: str,
        model_version: Optional[str] = None,
        days: int = 7,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get recent predictions for a model
        """
        try:
            async with self.db_pool.acquire() as conn:
                query = """
                    SELECT 
                        mp.game_id,
                        mp.prediction_target,
                        mp.prediction_value,
                        mp.prediction_probability,
                        mp.confidence_score,
                        mp.created_at as prediction_timestamp,
                        mp.actual_outcome,
                        eg.home_team,
                        eg.away_team,
                        eg.game_datetime
                    FROM curated.ml_predictions mp
                    LEFT JOIN curated.enhanced_games eg ON mp.game_id = eg.game_id
                    WHERE mp.model_name = $1
                    AND ($2 IS NULL OR mp.model_version = $2)
                    AND mp.created_at >= NOW() - INTERVAL '%s days'
                    ORDER BY mp.created_at DESC
                    LIMIT $3
                """ % days
                
                rows = await conn.fetch(query, model_name, model_version, limit)
                
                predictions = []
                for row in rows:
                    pred_data = {
                        'game_id': str(row['game_id']),
                        'prediction_target': row['prediction_target'],
                        'prediction_value': row['prediction_value'],
                        'prediction_probability': float(row['prediction_probability']) if row['prediction_probability'] else None,
                        'confidence_score': float(row['confidence_score']) if row['confidence_score'] else None,
                        'prediction_timestamp': row['prediction_timestamp'],
                        'actual_outcome': row['actual_outcome'],
                        'game_info': {
                            'home_team': row['home_team'],
                            'away_team': row['away_team'],
                            'game_datetime': row['game_datetime']
                        } if row['home_team'] else None
                    }
                    predictions.append(pred_data)
                
                return predictions
            
        except Exception as e:
            logger.error(f"Recent predictions error: {e}")
            return []