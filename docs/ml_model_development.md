 ML Model Development Strategy

  Current Data Assets Analysis

  Strong Foundation Available:
  - 1,394 games in staging with comprehensive external ID mapping
  - 28,658 historical odds records with temporal precision for
  line movement analysis
  - Three-tier pipeline (RAW → STAGING → CURATED) ideal for ML
  feature engineering
  - Enhanced games table with JSONB feature containers ready for
  ML pipeline integration
  - Sharp action signals table for professional betting indicators

  Proposed ML Pipeline Architecture

  1. Feature Engineering Pipeline (src/ml/feature_engineering/)

  Temporal Features (No Data Leakage):
  # Features extracted only from data ≥60 minutes before first 
  pitch
  - line_movement_velocity_60min: Rate of odds changes in final
  hour
  - sharp_action_intensity_60min: Aggregated professional betting
  signals
  - public_sentiment_shift_60min: Change in public betting
  percentages
  - cross_sbook_consensus_60min: Agreement across multiple
  sportsbooks
  - reverse_line_movement_signals: Professional money moving
  against public

  Market Structure Features:
  - opening_to_current_movement: Initial line vs
  60min-before-pitch line
  - sportsbook_variance: Standard deviation across books for same
  market
  - market_liquidity_score: Volume and depth indicators
  - steam_move_indicators: Synchronized cross-book line movements
  - closing_line_efficiency: Historical accuracy of final pre-game
   lines

  Team Performance Features:
  - recent_form_weighted: Last 10 games with recency weighting
  - head_to_head_trends: Historical matchup performance
  - pitcher_matchup_metrics: Starter vs opposing team historical
  performance
  - bullpen_usage_fatigue: Recent usage patterns affecting
  availability
  - home_field_advantage: Venue-specific performance adjustments

  2. ML Model Architecture (src/ml/models/)

  Target Variables (Binary Classification):
  - total_over_binary: 1 if game goes over total, 0 otherwise
  - home_ml_binary: 1 if home team wins, 0 otherwise
  - home_spread_binary: 1 if home team covers spread, 0 otherwise

  Model Types to Implement:

  Non-Black Box Models:
  - LogisticRegression: Interpretable coefficients for feature
  importance
  - RandomForest: Feature importance ranking and decision tree
  insights
  - XGBoost: Feature importance with SHAP values for
  explainability
  - LinearSVM: Clear decision boundaries with interpretable
  weights

  Black Box Models:
  - Neural Networks: Deep learning for complex feature
  interactions
  - CatBoost: Advanced gradient boosting with categorical feature
  handling
  - LightGBM: Fast training with advanced regularization
  - Ensemble Methods: Stacking/blending multiple model predictions

  3. MLFlow Experiment Tracking Integration 
  (src/ml/mlflow_integration/)

  Experiment Structure:
  # MLFlow experiment hierarchy
  experiments/
  ├── total_over_prediction/
  │   ├── logistic_regression_v1/
  │   ├── xgboost_feature_selection_v2/
  │   └── neural_network_deep_v1/
  ├── home_moneyline_prediction/
  └── home_spread_prediction/

  Tracking Metrics:
  mlflow_metrics = {
      'accuracy': accuracy_score,
      'precision': precision_score,
      'recall': recall_score,
      'f1_score': f1_score,
      'roc_auc': roc_auc_score,
      'log_loss': log_loss,
      'profit_metric': expected_value_calculation,
      'kelly_criterion_sizing': optimal_bet_sizing
  }

  4. Data Pipeline Integration (src/ml/data_pipeline/)

  Feature Store Design:
  -- New ML-specific tables to add to curated schema
  CREATE TABLE curated.ml_features (
      game_id INTEGER REFERENCES curated.enhanced_games(id),
      feature_cutoff_time TIMESTAMP WITH TIME ZONE, -- 60min 
  before first pitch
      features JSONB NOT NULL,
      feature_version VARCHAR(20) NOT NULL,
      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
  );

  CREATE TABLE curated.ml_predictions (
      game_id INTEGER REFERENCES curated.enhanced_games(id),
      model_name VARCHAR(100) NOT NULL,
      model_version VARCHAR(20) NOT NULL,
      prediction_type VARCHAR(20) NOT NULL, -- 'total_over', 
  'home_ml', 'home_spread'
      prediction_probability NUMERIC(5,4),
      prediction_binary INTEGER CHECK (prediction_binary IN (0,
  1)),
      confidence_score NUMERIC(3,2),
      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
  );

  CREATE TABLE curated.ml_model_performance (
      model_name VARCHAR(100) NOT NULL,
      model_version VARCHAR(20) NOT NULL,
      prediction_type VARCHAR(20) NOT NULL,
      evaluation_period_start DATE,
      evaluation_period_end DATE,
      total_predictions INTEGER,
      accuracy NUMERIC(5,4),
      precision_score NUMERIC(5,4),
      recall_score NUMERIC(5,4),
      f1_score NUMERIC(5,4),
      roc_auc NUMERIC(5,4),
      profit_loss NUMERIC(10,2),
      roi_percentage NUMERIC(5,2),
      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
  );

  5. Proposed Development Phases

  Phase 1: Foundation (Weeks 1-2)
  # Core ML infrastructure
  src/ml/
  ├── __init__.py
  ├── feature_engineering/
  │   ├── temporal_features.py      # 60min cutoff feature 
  extraction
  │   ├── market_features.py        # Line movement and consensus 
  features  
  │   ├── team_features.py          # Performance and matchup 
  features
  │   └── feature_pipeline.py       # Orchestration and validation
  ├── data_pipeline/
  │   ├── ml_data_loader.py         # Load from curated schema
  │   ├── feature_store.py          # Feature caching and 
  versioning
  │   └── validation.py             # Data quality and leakage 
  prevention
  └── utils/
      ├── mlflow_setup.py           # MLFlow configuration
      └── evaluation_metrics.py     # Custom betting-specific 
  metrics

  Phase 2: Model Development (Weeks 3-4)
  src/ml/models/
  ├── base_model.py                 # Abstract base class
  ├── interpretable/
  │   ├── logistic_regression.py
  │   ├── random_forest.py
  │   └── xgboost_interpretable.py
  ├── blackbox/
  │   ├── neural_network.py
  │   ├── ensemble_methods.py
  │   └── advanced_boosting.py
  └── model_factory.py              # Model instantiation and 
  management

  Phase 3: Backtesting & Validation (Weeks 5-6)
  src/ml/backtesting/
  ├── time_series_cv.py             # Time-aware cross-validation
  ├── walk_forward_validation.py    # Realistic betting simulation
  ├── profit_analysis.py            # Expected value calculations
  └── model_comparison.py           # Statistical significance 
  testing

  Phase 4: Production Pipeline (Weeks 7-8)
  src/ml/production/
  ├── prediction_service.py         # Real-time prediction API
  ├── model_monitoring.py           # Performance drift detection
  ├── retraining_pipeline.py        # Automated model updates
  └── betting_recommendations.py    # Kelly criterion position 
  sizing

  6. Key Implementation Considerations

  Data Leakage Prevention:
  - Strict 60-minute cutoff before first pitch for all features
  - Time-aware train/validation splits (no future information)
  - Feature engineering pipeline validates temporal constraints
  - Automated alerts for any post-game data contamination

  Feature Engineering Strategy:
  - Market Microstructure: Line movement patterns, bid-ask
  spreads, volume indicators
  - Sharp Action Synthesis: Combine multiple professional betting
  signals
  - Cross-Market Correlations: Relationships between ML, spread,
  and total markets
  - Temporal Patterns: Day-of-week, time-of-day, seasonal effects
  - Weather Integration: Temperature, wind, precipitation impact
  on totals

  Model Evaluation Framework:
  - Statistical Metrics: Standard classification metrics
  (accuracy, precision, recall, F1, AUC)
  - Betting-Specific Metrics: ROI, Sharpe ratio, maximum drawdown,
   hit rate
  - Risk-Adjusted Returns: Kelly criterion optimal sizing, Value
  at Risk calculations
  - Market Efficiency Tests: Comparison against closing line and
  market consensus

  This architecture leverages your existing three-tier pipeline,
  integrates seamlessly with MLFlow for experiment tracking, and
  provides both interpretable and black-box modeling approaches
  for the three binary prediction targets you specified.

⏺ Update Todos
  ⎿  ☒ Examine PostgreSQL staging schema tables to understand data
       structure
     ☒ Read system architecture diagram for context
     ☒ Analyze available features for ML model development
     ☒ Design ML pipeline architecture with MLFlow integration
     ☒ Propose feature engineering strategies

⏺ The proposed ML strategy builds upon your existing robust data
  infrastructure with 1,394+ games and 28,658+ historical odds
  records. The key strengths of this approach:

  1. Data Leakage Prevention: Strict 60-minute cutoff ensures no
  future information contamination
  2. Feature Rich Environment: Leverages your existing market
  microstructure, sharp action signals, and team performance data
  3. Production Ready: Integrates with your three-tier pipeline
  and MLFlow for immediate deployment
  4. Interpretable + Black Box: Balanced approach allowing both
  transparency and maximum predictive power
  5. Betting-Specific Evaluation: Profit-focused metrics aligned
  with actual betting outcomes

  Next steps would be implementing Phase 1 (Foundation) by
  creating the ML module structure and establishing the feature
  engineering pipeline with proper temporal constraints.
