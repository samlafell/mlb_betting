MLB Game Outcome Prediction System - ML Architecture Design                                                                                    │ │
│ │                                                                                                                                                │ │
│ │ Overview                                                                                                                                       │ │
│ │                                                                                                                                                │ │
│ │ Design a comprehensive machine learning system that integrates with the existing MLB betting infrastructure to provide real-time game outcome  │ │
│ │ predictions with <100ms inference latency.                                                                                                     │ │
│ │                                                                                                                                                │ │
│ │ System Architecture Design                                                                                                                     │ │
│ │                                                                                                                                                │ │
│ │ Core Components Integration                                                                                                                    │ │
│ │                                                                                                                                                │ │
│ │ 1. Data Ingestion & Streaming Layer                                                                                                            │ │
│ │                                                                                                                                                │ │
│ │ Integration with Existing System:                                                                                                              │ │
│ │ - Enhance existing collectors (src/data/collection/) to publish to Kafka topics                                                                │ │
│ │ - Maintain current PostgreSQL storage for backtesting and historical analysis                                                                  │ │
│ │ - Add streaming capability for real-time ML features                                                                                           │ │
│ │                                                                                                                                                │ │
│ │ New Components:                                                                                                                                │ │
│ │ - Kafka Event Hub: Central streaming platform for real-time data                                                                               │ │
│ │ - Stream Processors: Real-time feature computation                                                                                             │ │
│ │ - Data Validation Layer: Pydantic-based real-time validation                                                                                   │ │
│ │                                                                                                                                                │ │
│ │ 2. Feature Engineering & Store                                                                                                                 │ │
│ │                                                                                                                                                │ │
│ │ Feature Store Architecture:                                                                                                                    │ │
│ │ - Feast with PostgreSQL Backend: Leveraging existing database infrastructure                                                                   │ │
│ │ - Online Feature Store: Redis/PostgreSQL hybrid for <100ms serving                                                                             │ │
│ │ - Feature Pipelines: Transform raw betting data into ML features                                                                               │ │
│ │ - Market-Driven Features: Line movement velocity, reverse line movement, steam moves                                                           │ │
│ │ - Contextual Features: Weather, ballpark factors, team streaks                                                                                 │ │
│ │                                                                                                                                                │ │
│ │ 3. ML Model Pipeline                                                                                                                           │ │
│ │                                                                                                                                                │ │
│ │ Model Components:                                                                                                                              │ │
│ │ - Primary Models: LightGBM/XGBoost for tabular betting data                                                                                    │ │
│ │ - Model Registry: MLflow with PostgreSQL backend                                                                                               │ │
│ │ - A/B Testing Framework: Multi-model serving with traffic splitting                                                                            │ │
│ │ - Continuous Training: Automated retraining with new data                                                                                      │ │
│ │                                                                                                                                                │ │
│ │ 4. Prediction Serving Infrastructure                                                                                                           │ │
│ │                                                                                                                                                │ │
│ │ Serving Architecture:                                                                                                                          │ │
│ │ - Ray Serve: Auto-scaling inference with resource management                                                                                   │ │
│ │ - FastAPI Gateway: RESTful API with <100ms response times                                                                                      │ │
│ │ - gRPC Internal Communication: Low-latency inter-service calls                                                                                 │ │
│ │ - Docker Containerization: Scalable deployment                                                                                                 │ │
│ │                                                                                                                                                │ │
│ │ 5. Business Logic & Strategy Integration                                                                                                       │ │
│ │                                                                                                                                                │ │
│ │ Integration with Existing Analysis Layer:                                                                                                      │ │
│ │ - Enhance existing strategy orchestrator to consume ML predictions                                                                             │ │
│ │ - Add ML prediction results to existing UnifiedBettingSignal model                                                                             │ │
│ │ - Integrate with current backtesting engine for validation                                                                                     │ │
│ │ - Leverage existing sharp action detection as model features                                                                                   │ │
│ │                                                                                                                                                │ │
│ │ 6. Monitoring & Observability                                                                                                                  │ │
│ │                                                                                                                                                │ │
│ │ Production Monitoring:                                                                                                                         │ │
│ │ - Evidently AI: Model drift detection and performance monitoring                                                                               │ │
│ │ - Custom Metrics: Integration with existing Prometheus metrics                                                                                 │ │
│ │ - Real-time Alerting: Model performance degradation alerts                                                                                     │ │
│ │ - Data Quality: Continuous validation of prediction inputs                                                                                     │ │
│ │                                                                                                                                                │ │
│ │ Implementation Plan                                                                                                                            │ │
│ │                                                                                                                                                │ │
│ │ Phase 1: Streaming Infrastructure (Weeks 1-2)                                                                                                  │ │
│ │                                                                                                                                                │ │
│ │ 1. Kafka Integration:                                                                                                                          │ │
│ │   - Deploy Kafka cluster with PostgreSQL schema registry                                                                                       │ │
│ │   - Enhance existing collectors to publish real-time events                                                                                    │ │
│ │   - Create Kafka topics for odds, movements, and game events                                                                                   │ │
│ │   - Implement stream processors for feature computation                                                                                        │ │
│ │ 2. Feature Store Foundation:                                                                                                                   │ │
│ │   - Deploy Feast with PostgreSQL backend                                                                                                       │ │
│ │   - Design feature schemas for betting data                                                                                                    │ │
│ │   - Implement offline feature computation from existing data                                                                                   │ │
│ │   - Set up online feature serving infrastructure                                                                                               │ │
│ │                                                                                                                                                │ │
│ │ Phase 2: ML Pipeline Core (Weeks 3-4)                                                                                                          │ │
│ │                                                                                                                                                │ │
│ │ 1. Model Development:                                                                                                                          │ │
│ │   - Implement feature engineering pipeline from existing data                                                                                  │ │
│ │   - Develop LightGBM models for game outcome prediction                                                                                        │ │
│ │   - Set up MLflow for experiment tracking and model registry                                                                                   │ │
│ │   - Implement juice & value filtering business logic                                                                                           │ │
│ │ 2. Serving Infrastructure:                                                                                                                     │ │
│ │   - Deploy Ray Serve for model inference                                                                                                       │ │
│ │   - Create FastAPI prediction API                                                                                                              │ │
│ │   - Implement data cutoff enforcement (20min before game start)                                                                                │ │
│ │   - Add prediction caching and optimization                                                                                                    │ │
│ │                                                                                                                                                │ │
│ │ Phase 3: Integration & Production (Weeks 5-6)                                                                                                  │ │
│ │                                                                                                                                                │ │
│ │ 1. System Integration:                                                                                                                         │ │
│ │   - Integrate ML predictions with existing strategy orchestrator                                                                               │ │
│ │   - Enhance betting signal models with ML predictions                                                                                          │ │
│ │   - Add ML results to existing backtesting framework                                                                                           │ │
│ │   - Implement A/B testing for model validation                                                                                                 │ │
│ │ 2. Monitoring & Operations:                                                                                                                    │ │
│ │   - Deploy Evidently AI for model monitoring                                                                                                   │ │
│ │   - Set up alerts for model drift and performance                                                                                              │ │
│ │   - Implement comprehensive logging and observability                                                                                          │ │
│ │   - Performance optimization for <100ms targets                                                                                                │ │
│ │                                                                                                                                                │ │
│ │ Technical Specifications                                                                                                                       │ │
│ │                                                                                                                                                │ │
│ │ Data Flow Architecture                                                                                                                         │ │
│ │                                                                                                                                                │ │
│ │ [Existing Collectors]                                                                                                                          │ │
│ │     ↓ (Enhanced)                                                                                                                               │ │
│ │ [Kafka Topics] → [Stream Processing] → [Feature Store]                                                                                         │ │
│ │     ↓                                       ↓                                                                                                  │ │
│ │ [PostgreSQL Raw] ← [Historical Features] ← [Online Features]                                                                                   │ │
│ │                                            ↓                                                                                                   │ │
│ │                       [ML Models] → [Prediction API]                                                                                           │ │
│ │                            ↓              ↓                                                                                                    │ │
│ │                    [Model Registry] → [Strategy Orchestrator]                                                                                  │ │
│ │                                           ↓                                                                                                    │ │
│ │                                    [Betting Decisions]                                                                                         │ │
│ │                                                                                                                                                │ │
│ │ API Design                                                                                                                                     │ │
│ │                                                                                                                                                │ │
│ │ # Prediction API Response                                                                                                                      │ │
│ │ {                                                                                                                                              │ │
│ │   "game_id": "2025-07-01-NYY-BOS",                                                                                                             │ │
│ │   "prediction": "HOME_TEAM_SPREAD",                                                                                                            │ │
│ │   "confidence": 0.73,                                                                                                                          │ │
│ │   "predicted_probability": 0.68,                                                                                                               │ │
│ │   "key_features": {                                                                                                                            │ │
│ │     "line_movement_velocity": 0.45,                                                                                                            │ │
│ │     "reverse_line_movement": true,                                                                                                             │ │
│ │     "sharp_money_percentage": 0.67                                                                                                             │ │
│ │   },                                                                                                                                           │ │
│ │   "business_logic": {                                                                                                                          │ │
│ │     "recommendation": "BET",                                                                                                                   │ │
│ │     "bet_type": "SPREAD",                                                                                                                      │ │
│ │     "value_score": 0.82                                                                                                                        │ │
│ │   },                                                                                                                                           │ │
│ │   "metadata": {                                                                                                                                │ │
│ │     "model_version": "v1.2.3",                                                                                                                 │ │
│ │     "feature_timestamp": "2025-07-01T18:50:00Z",                                                                                               │ │
│ │     "inference_latency_ms": 47                                                                                                                 │ │
│ │   }                                                                                                                                            │ │
│ │ }                                                                                                                                              │ │
│ │                                                                                                                                                │ │
│ │ Database Schema Extensions                                                                                                                     │ │
│ │                                                                                                                                                │ │
│ │ -- ML Predictions Table                                                                                                                        │ │
│ │ CREATE TABLE ml_predictions (                                                                                                                  │ │
│ │     id SERIAL PRIMARY KEY,                                                                                                                     │ │
│ │     game_id VARCHAR(100) REFERENCES games(game_id),                                                                                            │ │
│ │     model_version VARCHAR(50),                                                                                                                 │ │
│ │     prediction_type VARCHAR(50),                                                                                                               │ │
│ │     predicted_outcome VARCHAR(100),                                                                                                            │ │
│ │     confidence_score DECIMAL(5,4),                                                                                                             │ │
│ │     predicted_probability DECIMAL(5,4),                                                                                                        │ │
│ │     key_features JSONB,                                                                                                                        │ │
│ │     business_recommendation VARCHAR(50),                                                                                                       │ │
│ │     value_score DECIMAL(5,4),                                                                                                                  │ │
│ │     created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),                                                                                         │ │
│ │     inference_latency_ms INTEGER                                                                                                               │ │
│ │ );                                                                                                                                             │ │
│ │                                                                                                                                                │ │
│ │ -- Feature Store Tables                                                                                                                        │ │
│ │ CREATE TABLE feature_definitions (                                                                                                             │ │
│ │     feature_name VARCHAR(100) PRIMARY KEY,                                                                                                     │ │
│ │     feature_type VARCHAR(50),                                                                                                                  │ │
│ │     data_source VARCHAR(50),                                                                                                                   │ │
│ │     computation_logic TEXT,                                                                                                                    │ │
│ │     created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()                                                                                          │ │
│ │ );                                                                                                                                             │ │
│ │                                                                                                                                                │ │
│ │ CREATE TABLE online_features (                                                                                                                 │ │
│ │     entity_id VARCHAR(100),                                                                                                                    │ │
│ │     feature_name VARCHAR(100),                                                                                                                 │ │
│ │     feature_value JSONB,                                                                                                                       │ │
│ │     event_timestamp TIMESTAMP WITH TIME ZONE,                                                                                                  │ │
│ │     created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),                                                                                         │ │
│ │     PRIMARY KEY (entity_id, feature_name, event_timestamp)                                                                                     │ │
│ │ );                                                                                                                                             │ │
│ │                                                                                                                                                │ │
│ │ Integration Points                                                                                                                             │ │
│ │                                                                                                                                                │ │
│ │ 1. Collector Enhancement                                                                                                                       │ │
│ │                                                                                                                                                │ │
│ │ # Enhanced collector with Kafka publishing                                                                                                     │ │
│ │ class MLEnabledCollector(BaseCollector):                                                                                                       │ │
│ │     def __init__(self, kafka_producer: KafkaProducer):                                                                                         │ │
│ │         super().__init__()                                                                                                                     │ │
│ │         self.kafka_producer = kafka_producer                                                                                                   │ │
│ │                                                                                                                                                │ │
│ │     async def collect(self, **params):                                                                                                         │ │
│ │         result = await super().collect(**params)                                                                                               │ │
│ │                                                                                                                                                │ │
│ │         # Publish to Kafka for real-time ML                                                                                                    │ │
│ │         for data_point in result.data:                                                                                                         │ │
│ │             await self.kafka_producer.send(                                                                                                    │ │
│ │                 topic=f"{self.source_name}_events",                                                                                            │ │
│ │                 value=data_point.model_dump()                                                                                                  │ │
│ │             )                                                                                                                                  │ │
│ │                                                                                                                                                │ │
│ │         return result                                                                                                                          │ │
│ │                                                                                                                                                │ │
│ │ 2. Strategy Integration                                                                                                                        │ │
│ │                                                                                                                                                │ │
│ │ # Enhanced strategy orchestrator with ML predictions                                                                                           │ │
│ │ class MLEnhancedStrategyOrchestrator(StrategyOrchestrator):                                                                                    │ │
│ │     def __init__(self, ml_prediction_client: MLPredictionClient):                                                                              │ │
│ │         super().__init__()                                                                                                                     │ │
│ │         self.ml_client = ml_prediction_client                                                                                                  │ │
│ │                                                                                                                                                │ │
│ │     async def execute_strategies(self, strategy_names, game_data, context=None):                                                               │ │
│ │         # Get ML predictions for games                                                                                                         │ │
│ │         ml_predictions = await self.ml_client.get_predictions(                                                                                 │ │
│ │             [game['game_id'] for game in game_data]                                                                                            │ │
│ │         )                                                                                                                                      │ │
│ │                                                                                                                                                │ │
│ │         # Enhance context with ML predictions                                                                                                  │ │
│ │         enhanced_context = {                                                                                                                   │ │
│ │             **(context or {}),                                                                                                                 │ │
│ │             'ml_predictions': ml_predictions                                                                                                   │ │
│ │         }                                                                                                                                      │ │
│ │                                                                                                                                                │ │
│ │         return await super().execute_strategies(                                                                                               │ │
│ │             strategy_names, game_data, enhanced_context                                                                                        │ │
│ │         )                                                                                                                                      │ │
│ │                                                                                                                                                │ │
│ │ Performance Targets & SLAs                                                                                                                     │ │
│ │                                                                                                                                                │ │
│ │ Latency Requirements                                                                                                                           │ │
│ │                                                                                                                                                │ │
│ │ - Feature Serving: <50ms P95                                                                                                                   │ │
│ │ - Model Inference: <30ms P95                                                                                                                   │ │
│ │ - End-to-End API: <100ms P99                                                                                                                   │ │
│ │ - Data Freshness: <5 minutes for non-critical features, <30 seconds for critical features                                                      │ │
│ │                                                                                                                                                │ │
│ │ Scalability Targets                                                                                                                            │ │
│ │                                                                                                                                                │ │
│ │ - Concurrent Predictions: 1000+ QPS                                                                                                            │ │
│ │ - Feature Updates: 10,000+ features/second                                                                                                     │ │
│ │ - Model Retraining: Daily automated retraining                                                                                                 │ │
│ │ - A/B Testing: 5% traffic to new models                                                                                                        │ │
│ │                                                                                                                                                │ │
│ │ Quality Metrics                                                                                                                                │ │
│ │                                                                                                                                                │ │
│ │ - Prediction Accuracy: >60% on game outcomes                                                                                                   │ │
│ │ - Model Stability: <5% performance degradation between retrainings                                                                             │ │
│ │ - Data Quality: >95% feature completeness                                                                                                      │ │
│ │ - System Availability: 99.9% uptime                                                                                                            │ │
│ │                                                                                                                                                │ │
│ │ Risk Mitigation                                                                                                                                │ │
│ │                                                                                                                                                │ │
│ │ Technical Risks                                                                                                                                │ │
│ │                                                                                                                                                │ │
│ │ 1. Latency Requirements: Implement caching, optimize queries, use connection pooling                                                           │ │
│ │ 2. Data Quality: Comprehensive validation, monitoring, and alerting                                                                            │ │
│ │ 3. Model Drift: Automated monitoring and retraining pipelines                                                                                  │ │
│ │ 4. Scalability: Horizontal scaling with load balancing                                                                                         │ │
│ │                                                                                                                                                │ │
│ │ Operational Risks                                                                                                                              │ │
│ │                                                                                                                                                │ │
│ │ 1. Complexity: Gradual rollout, comprehensive documentation                                                                                    │ │
│ │ 2. Integration: Extensive testing with existing systems                                                                                        │ │
│ │ 3. Performance: Load testing and optimization before production                                                                                │ │
│ │ 4. Monitoring: Comprehensive observability from day one                                                                                        │ │
│ │                                                                                                                                                │ │
│ │ Success Criteria                                                                                                                               │ │
│ │                                                                                                                                                │ │
│ │ Technical Success                                                                                                                              │ │
│ │                                                                                                                                                │ │
│ │ - All API responses <100ms P99 latency                                                                                                         │ │
│ │ - Feature store serving <50ms P95                                                                                                              │ │
│ │ - Model predictions available 20+ minutes before game start                                                                                    │ │
│ │ - Zero prediction service downtime during games                                                                                                │ │
│ │                                                                                                                                                │ │
│ │ Business Success                                                                                                                               │ │
│ │                                                                                                                                                │ │
│ │ - ML predictions improve strategy performance by >10%                                                                                          │ │
│ │ - Successful integration with existing backtesting framework                                                                                   │ │
│ │ - Positive ROI on ML-enhanced betting decisions                                                                                                │ │
│ │ - Operational efficiency gains through automation                                                                                              │ │
│ │                                                                                                                                                │ │
│ │ This design leverages your existing infrastructure while adding cutting-edge ML capabilities, ensuring a smooth integration path and maximum   │ │
│ │ value delivery.                        