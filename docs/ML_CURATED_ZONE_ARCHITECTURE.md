# ML CURATED Zone Architecture Documentation

## Overview

The ML CURATED zone is a comprehensive data architecture designed specifically for machine learning model development in sports betting. It integrates data from four primary sources (Action Network, VSIN, SBD, MLB Stats API) and provides optimized tables for feature engineering, model training, and performance tracking.

## Architecture Principles

### 1. Data Leakage Prevention
- **60-Minute Cutoff**: All features enforce a strict 60-minute pre-game cutoff to prevent data leakage
- **Temporal Constraints**: Database-level constraints ensure no future information contamination
- **Feature Versioning**: Supports A/B testing and feature evolution without introducing leakage

### 2. Multi-Source Integration
- **Action Network**: Historical odds, line movements, sportsbook data (28,658+ records)
- **VSIN**: Betting splits from DraftKings and Circa with money% vs bet% analysis
- **SBD**: WordPress JSON API with 9+ sportsbooks and sharp action indicators
- **MLB Stats API**: Authoritative game outcomes, player data, venue information

### 3. ML-Optimized Design
- **Binary Classification Targets**: total_over, home_ml, home_spread
- **Feature Containers**: JSONB for flexible feature storage and efficient querying
- **Performance Tracking**: Betting-specific metrics (ROI, Sharpe ratio, Kelly Criterion)
- **MLFlow Integration**: Full experiment tracking and model versioning

## Database Schema Structure

### Core Tables

#### 1. `curated.enhanced_games`
**Purpose**: Master games table with cross-system identifiers and ML features container

**Key Features**:
- Cross-system correlation (MLB Stats API, Action Network, SBD, VSIN)
- Weather data and venue information for totals betting
- Starting pitcher information and handedness matchups
- JSONB feature containers for computed ML features
- Data quality scoring and source coverage tracking

**Usage**: Central hub for all game-related data with ML-ready feature containers

#### 2. `curated.unified_betting_splits`
**Purpose**: Multi-source betting splits with ML cutoff enforcement

**Key Features**:
- Combines VSIN (DK/Circa), SBD (9+ books), and Action Network data
- Sharp action detection and reverse line movement indicators
- Money% vs Bet% divergence analysis (key ML feature)
- Database-enforced 60-minute cutoff constraint
- Data completeness scoring per source

**Usage**: Primary source for public vs sharp money sentiment features

#### 3. `curated.ml_temporal_features`
**Purpose**: Time-series features with strict data leakage prevention

**Key Features**:
- Line movement velocity and pattern analysis
- Sharp action intensity synthesis from multiple sources
- Cross-sportsbook consensus and variance metrics
- Public sentiment shift tracking
- Source-specific features (DK vs Circa gaps)

**Usage**: Core temporal features for ML models with guaranteed 60-minute cutoff

#### 4. `curated.ml_market_features`
**Purpose**: Market structure and efficiency analysis

**Key Features**:
- Steam move detection and arbitrage opportunities
- Market liquidity and line stability scoring
- Sportsbook consensus strength analysis
- Sharp vs public divergence indicators
- Market microstructure metrics

**Usage**: Advanced market analysis features for sophisticated ML models

#### 5. `curated.ml_team_features`
**Purpose**: Team performance with MLB Stats API enrichment

**Key Features**:
- Recent form weighting and head-to-head analysis
- Pitcher matchup metrics and bullpen fatigue
- Venue-specific performance factors
- Weather impact calculations
- Rest and travel considerations

**Usage**: Team-specific features enhanced with authoritative MLB data

#### 6. `curated.ml_feature_vectors`
**Purpose**: Consolidated feature vectors for ML model input

**Key Features**:
- JSONB containers for all feature types
- Feature versioning and SHA-256 hashing
- Data quality and completeness scoring
- Source attribution and coverage tracking
- ML pipeline metadata

**Usage**: Final feature vectors ready for model training and prediction

### ML Infrastructure Tables

#### 7. `curated.ml_predictions`
**Purpose**: Model predictions with betting recommendations

**Key Features**:
- Three binary classification targets (total_over, home_ml, home_spread)
- Kelly Criterion optimal bet sizing
- Feature importance and SHAP explanations
- Expected value calculations
- Model confidence and risk assessment

**Usage**: Store and track model predictions with betting recommendations

#### 8. `curated.ml_model_performance`
**Purpose**: Comprehensive model performance tracking

**Key Features**:
- Standard ML metrics (accuracy, precision, recall, F1, AUC)
- Betting-specific metrics (ROI, hit rate, Sharpe ratio)
- Kelly Criterion performance analysis
- Closing Line Value (CLV) tracking
- Market efficiency comparison

**Usage**: Monitor and compare model performance across different strategies

#### 9. `curated.ml_experiments`
**Purpose**: MLFlow experiment tracking integration

**Key Features**:
- Experiment lifecycle management
- Hyperparameter space tracking
- Model architecture classification
- Performance benchmarking
- Run aggregation and comparison

**Usage**: Full ML experiment lifecycle management with MLFlow integration

## Key Views and Integrations

### 1. `curated.unified_line_movements`
Combines line movements from all sources with betting splits integration:
- Action Network historical odds with microsecond precision
- VSIN betting splits with implied line data
- SBD multi-sportsbook consensus movements

### 2. `curated.ml_feature_summary`
Quick assessment view for ML readiness:
- Feature availability indicators across all tables
- Data source coverage scoring
- Prediction readiness flags
- Latest feature versions

### 3. `curated.ml_model_dashboard` 
Performance monitoring dashboard:
- Model rankings by ROI and accuracy
- Recent vs overall performance comparison
- Risk-adjusted return metrics
- Activity and deployment status

### 4. `curated.data_quality_monitoring`
Multi-source data quality tracking:
- Coverage percentages by source
- Feature completeness trends
- ML readiness indicators
- Data quality degradation alerts

## Feature Engineering Pipeline

### Phase 1: Data Collection
1. **Action Network**: Historical odds collection with temporal precision
2. **VSIN**: Betting splits from DraftKings and Circa
3. **SBD**: Multi-sportsbook odds and splits via WordPress JSON API
4. **MLB Stats API**: Game outcomes and enrichment data

### Phase 2: Feature Engineering
1. **Temporal Features**: Line movement patterns with 60-minute cutoff
2. **Market Features**: Arbitrage detection and steam move analysis
3. **Team Features**: Performance metrics with MLB API enrichment
4. **Sentiment Features**: Public vs sharp money divergence

### Phase 3: Feature Vector Assembly
1. **Consolidation**: Combine all feature types into unified vectors
2. **Quality Scoring**: Assess completeness and source coverage
3. **Versioning**: Hash-based feature vector identification
4. **Validation**: Ensure ML pipeline compatibility

### Phase 4: Model Training & Prediction
1. **Training**: Use feature vectors with outcome labels
2. **Validation**: Time-aware cross-validation with walk-forward analysis
3. **Prediction**: Generate predictions with confidence scores
4. **Betting**: Apply Kelly Criterion for optimal position sizing

## Performance Optimization

### Indexing Strategy
- **Composite Indexes**: Multi-column indexes for common query patterns
- **JSONB Indexes**: GIN indexes for efficient JSONB feature querying
- **Temporal Indexes**: Optimized for time-series analysis patterns
- **Foreign Key Indexes**: Fast joins across related tables

### Query Optimization
- **Materialized Views**: Pre-computed aggregations for dashboard queries
- **Partitioning**: Date-based partitioning for historical data
- **Connection Pooling**: Efficient database connection management
- **Query Planning**: Analyze and optimize common query patterns

## Data Quality Framework

### Source Validation
- **Cross-Source Correlation**: Verify data consistency across sources
- **Completeness Scoring**: Track missing data by source and field
- **Timeliness Monitoring**: Ensure data freshness requirements
- **Anomaly Detection**: Identify unusual patterns in incoming data

### Feature Quality
- **Leakage Prevention**: Automated checks for temporal constraints
- **Distribution Monitoring**: Track feature distribution shifts
- **Correlation Analysis**: Monitor feature relationships over time
- **Missing Value Tracking**: Handle and monitor missing feature values

### Model Quality
- **Performance Monitoring**: Track model degradation over time
- **Prediction Calibration**: Ensure confidence scores are well-calibrated
- **Feature Importance Stability**: Monitor changes in feature importance
- **Bias Detection**: Check for systematic biases in predictions

## Deployment Integration

### MLFlow Integration
- **Experiment Tracking**: Full experiment lifecycle management
- **Model Registry**: Versioned model storage and deployment
- **Artifact Storage**: Model artifacts and feature processors
- **Deployment Monitoring**: Production model performance tracking

### Real-Time Pipeline
- **Feature Serving**: Real-time feature computation and serving
- **Model Inference**: Fast prediction generation for live betting
- **Alert System**: Performance degradation and anomaly alerts
- **Rollback Capability**: Quick model rollback for production issues

### Betting Integration
- **Kelly Criterion**: Automated optimal bet sizing
- **Risk Management**: Position sizing limits and risk controls
- **Performance Tracking**: Real-time P&L monitoring
- **Market Integration**: Interface with sportsbook APIs

## Usage Examples

### Model Training
```sql
-- Get feature vectors for model training
SELECT 
    fv.game_id,
    fv.temporal_features,
    fv.market_features,
    fv.team_features,
    fv.betting_splits_features,
    g.total_runs > 8.5 as total_over_target
FROM curated.ml_feature_vectors fv
JOIN curated.enhanced_games g ON fv.game_id = g.id
WHERE g.game_status = 'final'
  AND fv.feature_completeness_score >= 0.8
  AND g.season = 2024;
```

### Performance Monitoring
```sql
-- Monitor model performance by type
SELECT 
    model_name,
    prediction_type,
    accuracy,
    roi_percentage,
    sharpe_ratio,
    max_drawdown_pct
FROM curated.ml_model_performance
WHERE evaluation_period_end >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY roi_percentage DESC;
```

### Data Quality Check
```sql
-- Check data quality and ML readiness
SELECT 
    data_date,
    total_games,
    ml_ready_games,
    ml_ready_pct,
    avg_feature_completeness
FROM curated.data_quality_monitoring
WHERE data_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY data_date DESC;
```

## Future Enhancements

### Advanced Features
- **Player-Level Data**: Individual player performance and injury tracking
- **Weather Derivatives**: Advanced weather impact modeling
- **Market Sentiment**: Social media and news sentiment analysis
- **Lineup Analysis**: Starting lineup impact on team performance

### Model Improvements
- **Ensemble Methods**: Multi-model prediction combination
- **Deep Learning**: Neural network architectures for complex patterns
- **Reinforcement Learning**: Adaptive betting strategies
- **Online Learning**: Real-time model updates based on new data

### Infrastructure Scaling
- **Data Lake Integration**: Scale to handle larger datasets
- **Stream Processing**: Real-time feature computation
- **Distributed Training**: Scale model training across multiple nodes
- **Edge Deployment**: Deploy models closer to betting markets

## Maintenance and Monitoring

### Regular Tasks
- **Data Quality Audits**: Weekly comprehensive data quality reviews
- **Model Performance Reviews**: Monthly model performance analysis
- **Feature Drift Detection**: Continuous monitoring of feature distributions
- **Schema Evolution**: Quarterly review and updates of schema design

### Alert Thresholds
- **Data Quality**: <80% completeness or >24h data lag
- **Model Performance**: >10% accuracy drop or negative ROI trend
- **System Performance**: >5s query response times or >90% disk usage
- **Data Pipeline**: Failed data collection or processing jobs

This architecture provides a robust foundation for ML-driven sports betting with comprehensive data integration, feature engineering, and performance tracking capabilities.