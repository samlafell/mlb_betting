# GitHub Issues for ML Model Training Gaps

## Issue #1: Implement Automated Model Validation & Testing Pipeline

**Priority:** High  
**Type:** Enhancement  
**Labels:** ml, testing, validation, automation

### Description
Currently missing automated model validation pipeline that ensures model quality before deployment. Need comprehensive validation framework that integrates with existing MLflow infrastructure.

### Current State
- ✅ Manual model evaluation exists
- ✅ Basic performance metrics calculated
- ❌ No automated validation gates
- ❌ No standardized model quality checks

### Requirements
1. **Automated Validation Pipeline**
   - Pre-deployment model quality checks
   - Statistical significance testing
   - Performance threshold validation
   - Business metric validation (ROI, Sharpe ratio)

2. **Integration Points**
   - MLflow experiment tracking integration
   - CLI command: `uv run -m src.interfaces.cli ml validate --model {model_name}`
   - CI/CD pipeline integration for automated testing

3. **Quality Gates**
   - Minimum accuracy: 60% (above MLB 55% baseline)
   - Minimum ROI: 3% for deployment approval
   - Maximum drawdown: 25%
   - Feature importance stability check

### Technical Implementation
```python
# New file: src/ml/validation/model_validation_service.py
class ModelValidationService:
    async def validate_model_performance(self, model_name: str, test_data: pd.DataFrame)
    async def validate_business_metrics(self, predictions: List[Prediction])
    async def validate_feature_stability(self, model_name: str)
    async def generate_validation_report(self, validation_results: Dict)
```

### Acceptance Criteria
- [ ] Automated validation runs on every model training completion
- [ ] Validation results stored in MLflow with pass/fail status
- [ ] Failed validations prevent model deployment
- [ ] CLI command provides detailed validation report
- [ ] Integration with existing `lightgbm_trainer.py` workflow

### Files to Modify
- Create: `src/ml/validation/model_validation_service.py`
- Modify: `src/ml/training/lightgbm_trainer.py`
- Modify: `src/interfaces/cli/commands/ml_commands.py`
- Create: `tests/ml/test_model_validation.py`

---

## Issue #2: Implement Hyperparameter Optimization Framework

**Priority:** High  
**Type:** enhancement  
**Labels:** ml, optimization, performance

### Description
Need automated hyperparameter optimization to maximize model performance. Currently using default LightGBM parameters without systematic optimization.

### Current State
- ✅ LightGBM trainer exists with basic config
- ✅ MLflow integration for experiment tracking
- ❌ No hyperparameter optimization framework
- ❌ Manual parameter tuning only

### Requirements
1. **Optimization Framework**
   - Optuna integration for Bayesian optimization
   - Grid search and random search fallbacks
   - Multi-objective optimization (accuracy + ROI)
   - Early stopping for efficiency

2. **Integration**
   - CLI command: `uv run -m src.interfaces.cli ml optimize --target {target} --trials 100`
   - MLflow tracking of all optimization trials
   - Automatic best parameter selection

3. **Optimization Targets**
   - Primary: Model accuracy
   - Secondary: ROI percentage
   - Constraint: Maximum training time
   - Constraint: Model interpretability

### Technical Implementation
```python
# New file: src/ml/optimization/hyperparameter_optimizer.py
class HyperparameterOptimizer:
    def __init__(self, optimization_config: OptimizationConfig)
    async def optimize_lightgbm_params(self, training_data: pd.DataFrame, target: str)
    async def evaluate_parameter_set(self, params: Dict, data: pd.DataFrame)
    async def track_optimization_trial(self, trial_results: Dict)
```

### Search Spaces
```python
LIGHTGBM_SEARCH_SPACE = {
    'num_leaves': (10, 300),
    'learning_rate': (0.01, 0.3),
    'feature_fraction': (0.4, 1.0),
    'bagging_fraction': (0.4, 1.0),
    'bagging_freq': (1, 7),
    'min_child_samples': (5, 100),
    'max_depth': (3, 15),
    'reg_alpha': (0.0, 10.0),
    'reg_lambda': (0.0, 10.0)
}
```

### Acceptance Criteria
- [ ] Optuna optimization framework integrated
- [ ] 50+ trials completed for parameter optimization
- [ ] Best parameters automatically saved to MLflow
- [ ] CLI interface for starting optimization jobs
- [ ] Results show >5% improvement over default parameters

### Files to Create/Modify
- Create: `src/ml/optimization/hyperparameter_optimizer.py`
- Create: `src/ml/optimization/optimization_config.py`
- Modify: `src/ml/training/lightgbm_trainer.py`
- Modify: `src/interfaces/cli/commands/ml_commands.py`
- Create: `tests/ml/test_hyperparameter_optimization.py`

---

## Issue #3: Implement Model Drift Detection & Monitoring

**Priority:** Medium  
**Type:** enhancement  
**Labels:** ml, monitoring, drift-detection

### Description
Need automated model drift detection to identify when models degrade and require retraining. Critical for maintaining performance in production.

### Current State
- ✅ Model performance monitoring exists
- ✅ Redis feature store for real-time features
- ❌ No drift detection algorithms
- ❌ No automated alerting for model degradation

### Requirements
1. **Drift Detection Framework**
   - Feature drift detection (data distribution changes)
   - Concept drift detection (target relationship changes)
   - Performance drift monitoring (accuracy degradation)
   - Statistical tests: KS test, PSI, Jensen-Shannon divergence

2. **Monitoring Integration**
   - Real-time drift scoring
   - Alerting when drift thresholds exceeded
   - Dashboard visualization of drift metrics
   - Integration with existing Prometheus metrics

3. **Automated Response**
   - Trigger retraining when drift detected
   - Model rollback capabilities
   - Notification system for model maintenance

### Technical Implementation
```python
# New file: src/ml/monitoring/drift_detector.py
class ModelDriftDetector:
    async def detect_feature_drift(self, reference_data: pd.DataFrame, current_data: pd.DataFrame)
    async def detect_concept_drift(self, model: MLModel, recent_predictions: List[Prediction])
    async def calculate_drift_score(self, drift_results: Dict) -> float
    async def trigger_drift_alert(self, drift_score: float, threshold: float)
```

### Drift Metrics
- **Feature Drift**: Population Stability Index (PSI) > 0.2
- **Concept Drift**: Performance drop > 10% over 7-day window  
- **Data Quality Drift**: Missing values increase > 15%
- **Prediction Drift**: Output distribution shift > 0.3 KS statistic

### Acceptance Criteria
- [ ] Drift detection runs hourly on production models
- [ ] Alert system triggers when drift score > 0.7
- [ ] Dashboard shows drift metrics for all active models
- [ ] Automated retraining initiated when concept drift detected
- [ ] Historical drift trends stored and visualized

### Files to Create/Modify
- Create: `src/ml/monitoring/drift_detector.py`
- Create: `src/ml/monitoring/drift_config.py`
- Modify: `src/services/monitoring/prometheus_metrics_service.py`
- Modify: `src/interfaces/cli/commands/ml_commands.py`
- Create: `tests/ml/test_drift_detection.py`

---

## Issue #4: Implement Automated Retraining Workflows

**Priority:** High  
**Type:** enhancement  
**Labels:** ml, automation, retraining

### Description
Need automated retraining system that triggers based on model performance degradation, data drift, or scheduled intervals.

### Current State
- ✅ Manual training via CLI exists
- ✅ Training service infrastructure ready
- ❌ No automated retraining triggers
- ❌ No retraining scheduling system

### Requirements
1. **Automated Triggers**
   - Performance-based: ROI drops below 3% for 7 days
   - Drift-based: Drift score > 0.8 for 3 consecutive days
   - Schedule-based: Weekly retraining on new data
   - Data-based: New data volume > 1000 games

2. **Retraining Pipeline**
   - Automated data collection and preprocessing
   - Hyperparameter optimization for new models
   - A/B testing against current production model
   - Automatic deployment if performance improvement > 5%

3. **Integration Points**
   - Celery/Redis for background job processing
   - MLflow for experiment tracking
   - Model registry for staging/production management

### Technical Implementation
```python
# New file: src/ml/workflows/automated_retraining.py
class AutomatedRetrainingService:
    def __init__(self, scheduler_config: RetrainingConfig)
    async def evaluate_retraining_triggers(self) -> List[RetrainingTrigger]
    async def execute_retraining_job(self, trigger: RetrainingTrigger)
    async def validate_new_model(self, new_model: MLModel, baseline_model: MLModel)
    async def deploy_if_improved(self, validation_results: ValidationResults)
```

### Retraining Triggers
```python
RETRAINING_TRIGGERS = {
    'performance_degradation': {
        'metric': 'roi_percentage',
        'threshold': 0.03,
        'window_days': 7
    },
    'drift_detection': {
        'metric': 'drift_score',
        'threshold': 0.8,
        'consecutive_days': 3
    },
    'scheduled': {
        'frequency': 'weekly',
        'day_of_week': 'sunday'
    }
}
```

### Acceptance Criteria
- [ ] Automated retraining triggers based on performance metrics
- [ ] Scheduled weekly retraining with latest data
- [ ] A/B testing framework for model comparison
- [ ] Automatic deployment of improved models
- [ ] Rollback capability if new model underperforms

### Files to Create/Modify
- Enhance: `src/ml/workflows/automated_retraining.py` (exists but needs triggers)
- Create: `src/ml/workflows/retraining_scheduler.py`
- Create: `src/ml/workflows/model_deployment.py`
- Modify: `src/interfaces/cli/commands/ml_commands.py`
- Create: `tests/ml/test_automated_retraining.py`

---

## Issue #5: Implement A/B Testing Framework for Model Deployment

**Priority:** Medium  
**Type:** enhancement  
**Labels:** ml, testing, deployment

### Description
Need A/B testing framework to safely deploy new models by splitting traffic and comparing performance against baseline models.

### Current State
- ✅ Multiple models can be active simultaneously
- ✅ Model registry tracks different versions
- ❌ No traffic splitting mechanism
- ❌ No A/B test statistical analysis

### Requirements
1. **Traffic Splitting**
   - Configurable traffic allocation (e.g., 90% baseline, 10% new model)
   - Game-based splitting for fair comparison
   - Consistent routing (same game always goes to same model variant)

2. **Statistical Analysis**
   - Bayesian A/B testing for early stopping
   - Statistical significance testing
   - Business metric comparison (ROI, accuracy, Sharpe ratio)
   - Confidence intervals and effect size calculation

3. **Deployment Safety**
   - Circuit breaker for underperforming models
   - Automatic rollback if metrics degrade
   - Real-time monitoring of A/B test results

### Technical Implementation
```python
# New file: src/ml/deployment/ab_testing.py
class ABTestingFramework:
    def __init__(self, test_config: ABTestConfig)
    async def create_ab_test(self, baseline_model: str, candidate_model: str)
    async def route_prediction_request(self, game_id: str) -> str
    async def analyze_test_results(self, test_id: str) -> ABTestResults
    async def determine_winner(self, test_results: ABTestResults) -> Optional[str]
```

### A/B Test Configuration
```python
AB_TEST_CONFIG = {
    'traffic_split': {'baseline': 0.8, 'candidate': 0.2},
    'minimum_sample_size': 100,
    'maximum_test_duration_days': 14,
    'significance_threshold': 0.05,
    'minimum_effect_size': 0.05,
    'early_stopping': True
}
```

### Acceptance Criteria
- [ ] A/B testing framework integrated with prediction API
- [ ] Statistical significance testing with early stopping
- [ ] Automated winner selection based on business metrics
- [ ] Dashboard showing A/B test results in real-time
- [ ] Safe deployment with automatic rollback capability

### Files to Create/Modify
- Create: `src/ml/deployment/ab_testing.py`
- Create: `src/ml/deployment/traffic_router.py`
- Modify: `src/ml/api/routers/predictions.py`
- Modify: `src/interfaces/cli/commands/ml_commands.py`
- Create: `tests/ml/test_ab_testing.py`

---

## Issue #6: Enhance Cross-Validation & Backtesting Integration

**Priority:** Medium  
**Type:** enhancement  
**Labels:** ml, validation, backtesting

### Description
Improve integration between ML model training and existing backtesting framework. Need time-series cross-validation and walk-forward analysis.

### Current State
- ✅ Backtesting engine exists for strategy testing
- ✅ Model training with basic train/test split
- ❌ No time-series cross-validation
- ❌ Limited integration between ML and backtesting systems

### Requirements
1. **Time-Series Cross-Validation**
   - Walk-forward validation for time-series data
   - Purging and embargo to prevent data leakage
   - Multiple validation windows for robustness testing
   - Custom CV splitter for MLB seasonal patterns

2. **Backtesting Integration**
   - ML model predictions fed into backtesting engine
   - Strategy performance evaluation with ML predictions
   - Comparison of ML vs rule-based strategies
   - Historical performance simulation

3. **Validation Metrics**
   - Out-of-time validation results
   - Seasonal performance analysis
   - Model consistency across different market conditions
   - Risk-adjusted returns analysis

### Technical Implementation
```python
# New file: src/ml/validation/time_series_cv.py
class TimeSeriesCV:
    def __init__(self, n_splits: int, test_size: int, gap: int = 0)
    def split(self, X: pd.DataFrame, y: pd.Series, groups: Optional[pd.Series] = None)
    def validate_model(self, model: MLModel, X: pd.DataFrame, y: pd.Series)

# Enhanced: src/ml/validation/ml_backtesting_integration.py  
class MLBacktestingIntegration:
    async def run_ml_strategy_backtest(self, model: MLModel, start_date: datetime, end_date: datetime)
    async def compare_ml_vs_rule_strategies(self, backtest_results: Dict)
```

### Validation Framework
```python
CROSS_VALIDATION_CONFIG = {
    'walk_forward': {
        'n_splits': 5,
        'train_size': 90,  # days
        'test_size': 30,   # days
        'gap': 1           # day gap to prevent leakage
    },
    'purging': {
        'embargo_period': 1,  # days
        'purge_overlap': True
    }
}
```

### Acceptance Criteria
- [ ] Time-series cross-validation implemented
- [ ] Walk-forward validation prevents data leakage
- [ ] ML models integrated with backtesting engine
- [ ] Performance comparison: ML vs rule-based strategies
- [ ] Seasonal and out-of-time validation results

### Files to Create/Modify
- Create: `src/ml/validation/time_series_cv.py`
- Enhance: `src/ml/validation/ml_backtesting_integration.py`
- Modify: `src/analysis/backtesting/ml_engine.py`
- Modify: `src/interfaces/cli/commands/ml_commands.py`
- Create: `tests/ml/test_time_series_validation.py`

---

## Issue #7: Implement Feature Store Management & Versioning

**Priority:** Medium  
**Type:** enhancement  
**Labels:** ml, feature-store, data-management

### Description
Enhance existing Redis feature store with proper versioning, lineage tracking, and feature management capabilities.

### Current State
- ✅ Redis feature store exists with basic caching
- ✅ Feature extraction pipeline operational
- ❌ No feature versioning system
- ❌ Limited feature lineage tracking

### Requirements
1. **Feature Versioning**
   - Version control for feature definitions
   - Backward compatibility for model serving
   - Feature schema validation and evolution
   - Rollback capability for feature changes

2. **Feature Management**
   - Feature catalog with metadata
   - Feature usage tracking and monitoring
   - Data quality monitoring for features
   - Feature freshness and staleness detection

3. **Enhanced Storage**
   - Feature lineage tracking
   - Feature serving optimizations
   - Batch and real-time feature computation
   - Feature sharing across models

### Technical Implementation
```python
# Enhanced: src/ml/features/redis_feature_store.py
class EnhancedFeatureStore:
    async def register_feature_definition(self, feature_def: FeatureDefinition)
    async def get_features_by_version(self, feature_names: List[str], version: str)
    async def track_feature_lineage(self, feature_name: str, computation_graph: Dict)
    async def monitor_feature_quality(self, feature_name: str, values: np.ndarray)

# New file: src/ml/features/feature_catalog.py
class FeatureCatalog:
    def __init__(self, catalog_config: CatalogConfig)
    async def register_feature(self, feature_metadata: FeatureMetadata)
    async def discover_features(self, search_criteria: Dict) -> List[FeatureMetadata]
    async def validate_feature_schema(self, feature_data: pd.DataFrame, schema: Dict)
```

### Feature Metadata Schema
```python
FEATURE_METADATA_SCHEMA = {
    'name': str,
    'version': str,
    'description': str,
    'data_type': str,
    'computation_logic': str,
    'dependencies': List[str],
    'freshness_sla': timedelta,
    'owner': str,
    'tags': List[str]
}
```

### Acceptance Criteria
- [ ] Feature versioning system with backward compatibility
- [ ] Feature catalog with searchable metadata
- [ ] Data quality monitoring for all features
- [ ] Feature lineage tracking and visualization
- [ ] Performance optimization for feature serving

### Files to Create/Modify
- Enhance: `src/ml/features/redis_feature_store.py`
- Create: `src/ml/features/feature_catalog.py`
- Create: `src/ml/features/feature_versioning.py`
- Modify: `src/interfaces/cli/commands/ml_commands.py`
- Create: `tests/ml/test_feature_versioning.py`

---

## Issue #8: Implement Production Model Registry Workflows

**Priority:** Medium  
**Type:** enhancement  
**Labels:** ml, model-registry, deployment

### Description
Enhance existing model registry with proper staging workflows, approval processes, and deployment automation.

### Current State
- ✅ Basic model registry exists with MLflow
- ✅ Model versioning and metadata tracking
- ❌ No staging workflow (dev → staging → prod)
- ❌ Manual deployment process only

### Requirements
1. **Model Staging Pipeline**
   - Development → Staging → Production workflow
   - Approval gates between stages
   - Automated testing at each stage
   - Rollback capabilities

2. **Deployment Automation**
   - Blue-green deployments for zero downtime
   - Canary releases with gradual traffic ramping
   - Health checks and monitoring during deployment
   - Automatic rollback on deployment failures

3. **Model Governance**
   - Model approval workflow
   - Audit trail for all model changes
   - Compliance checks and validations
   - Model retirement and archival

### Technical Implementation
```python
# Enhanced: src/ml/registry/model_registry.py
class EnhancedModelRegistry:
    async def promote_model(self, model_name: str, from_stage: str, to_stage: str)
    async def deploy_model_blue_green(self, model_name: str, target_stage: str)
    async def rollback_deployment(self, deployment_id: str)
    async def audit_model_changes(self, model_name: str) -> List[AuditEntry]

# New file: src/ml/deployment/deployment_manager.py
class DeploymentManager:
    def __init__(self, deployment_config: DeploymentConfig)
    async def execute_blue_green_deployment(self, model: MLModel)
    async def execute_canary_deployment(self, model: MLModel, traffic_percent: float)
    async def monitor_deployment_health(self, deployment_id: str)
```

### Staging Workflow
```python
MODEL_STAGES = {
    'development': {'auto_promote': False, 'requires_approval': False},
    'staging': {'auto_promote': False, 'requires_approval': True},
    'production': {'auto_promote': False, 'requires_approval': True},
    'archived': {'auto_promote': False, 'requires_approval': False}
}
```

### Acceptance Criteria
- [ ] Model staging workflow with approval gates
- [ ] Blue-green deployment for zero-downtime updates
- [ ] Canary deployment with gradual traffic ramping
- [ ] Automated rollback on deployment failures
- [ ] Complete audit trail for model lifecycle

### Files to Create/Modify
- Enhance: `src/ml/registry/model_registry.py`
- Create: `src/ml/deployment/deployment_manager.py`
- Create: `src/ml/deployment/blue_green_deployer.py`
- Modify: `src/interfaces/cli/commands/ml_commands.py`
- Create: `tests/ml/test_model_deployment.py`

---

## Implementation Priority Order

### Phase 1 (Immediate - 1-2 weeks)
1. **Issue #1**: Automated Model Validation & Testing Pipeline
2. **Issue #2**: Hyperparameter Optimization Framework

### Phase 2 (Short-term - 2-4 weeks)  
3. **Issue #4**: Automated Retraining Workflows
4. **Issue #6**: Enhanced Cross-Validation & Backtesting Integration

### Phase 3 (Medium-term - 1-2 months)
5. **Issue #3**: Model Drift Detection & Monitoring
6. **Issue #5**: A/B Testing Framework for Model Deployment

### Phase 4 (Long-term - 2-3 months)
7. **Issue #7**: Feature Store Management & Versioning
8. **Issue #8**: Production Model Registry Workflows

## Summary

These 8 issues address the critical gaps needed to transform the existing ML infrastructure into a production-ready, automated ML training system. The current foundation is very strong - we have:
- ✅ 2 trained profitable models (62-67% accuracy, 5-8% ROI)
- ✅ Complete feature engineering pipeline  
- ✅ Production API with <200ms response times
- ✅ MLflow experiment tracking
- ✅ Docker containerization

With these enhancements, you'll have an enterprise-grade ML system capable of:
- 🤖 Automated model training and optimization
- 🔍 Intelligent drift detection and retraining
- 🧪 Safe A/B testing for model deployment  
- 📊 Comprehensive model validation and monitoring
- 🚀 Zero-downtime production deployments

The estimated effort is 2-3 months for full implementation, with immediate benefits available after Phase 1 completion.