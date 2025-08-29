# MLB Betting ML Strategy Architecture Guide

This comprehensive guide documents the unified ML strategy development architecture that addresses Issues #72, #47, #46, and #49 by providing a complete framework for developing, validating, and deploying betting strategies that combine rule-based backtesting with machine learning model predictions.

## Architecture Overview

The ML Strategy Architecture provides a unified approach to betting strategy development that bridges the gap between traditional rule-based backtesting and modern ML model training. It establishes clear methodologies for when and how to use each approach while providing comprehensive validation and production deployment workflows.

### Core Components

1. **Strategy Development Framework** (`src/analysis/strategy_development_framework.py`)
   - Unified methodology for rule-based, ML, and hybrid strategies
   - Clear decision framework for approach selection
   - Comprehensive performance metrics combining betting and ML metrics

2. **Integrated Validation Engine** (`src/analysis/validation/integrated_validation_engine.py`)
   - Combines statistical ML validation with backtesting validation
   - Cross-temporal validation with purging and embargoing
   - Statistical significance testing for strategy comparison
   - Risk-adjusted performance analysis

3. **A/B Testing Framework** (`src/analysis/testing/ab_testing_framework.py`)
   - Production A/B testing with statistical significance monitoring
   - Multi-arm bandit and fixed-split testing
   - Safety controls and automated stopping rules
   - Champion vs challenger testing workflows

4. **Betting Model Registry** (`src/ml/registry/betting_model_registry.py`)
   - Enhanced MLflow registry with betting-specific promotion criteria
   - Stage-based model lifecycle management
   - Performance-based promotion decisions
   - Champion/challenger model management

5. **Strategy Orchestrator** (`src/analysis/orchestration/strategy_orchestrator.py`)
   - Unified workflow management for complete strategy lifecycle
   - Automated and semi-automated execution modes
   - Stage-based progression with validation gates
   - Real-time monitoring and alerting

6. **CLI Interface** (`src/interfaces/cli/commands/ml_strategy.py`)
   - Comprehensive command-line interface for all operations
   - Workflow creation and management
   - Validation and testing commands
   - Status monitoring and reporting

## Issue Resolution

### Issue #72: Clarify Backtesting vs ML Model Training Architecture

**Problem**: Unclear relationship between backtesting and ML model training approaches.

**Solution**: The architecture provides clear methodology for when to use each approach:

#### When to Use Rule-Based Backtesting
- Strategy logic can be clearly defined with if/then rules
- Historical patterns are consistent and interpretable
- Full transparency in decision making is required
- Domain expertise suggests specific betting patterns
- **Examples**: Sharp action following, consensus fading, timing patterns

#### When to Use ML Predictive Models
- Large amounts of feature data available
- Complex non-linear relationships suspected
- Outcome prediction is primary goal
- Historical patterns are subtle or multi-dimensional
- **Examples**: Game outcome prediction, run total forecasting

#### When to Use Hybrid Approaches
- Want to combine domain expertise with ML insights
- Rule-based logic provides foundation, ML provides enhancement
- Need both interpretability and pattern discovery
- **Examples**: Sharp action detection enhanced by ML confidence scoring

#### Integration Architecture
The system bridges both approaches through:
- **Unified Performance Metrics**: Both approaches measured using same betting performance criteria
- **Integrated Validation**: ML models validated through backtesting to ensure real-world profitability
- **Cross-Validation with Betting Context**: Statistical validation enhanced with betting-specific time-aware splits

### Issue #47: Enhanced Cross-Validation & Backtesting Integration

**Problem**: Need enhanced cross-validation that integrates backtesting insights.

**Solution**: Implemented comprehensive cross-temporal validation framework:

#### Time-Aware Cross-Validation
```python
class CrossValidationConfig:
    n_splits: int = 5
    test_size: float = 0.2
    purging_buffer_days: int = 1  # Prevent lookahead bias
    embargo_days: int = 1         # Avoid temporal leakage
    min_train_samples: int = 100
    min_test_samples: int = 50
```

#### Integrated Validation Process
1. **Statistical ML Validation**: Traditional cross-validation on features and predictions
2. **Backtesting Validation**: Historical betting performance validation
3. **Combined Assessment**: Unified performance metrics combining both approaches
4. **Risk Analysis**: Comprehensive risk metrics including drawdown and VaR
5. **Statistical Significance**: Rigorous statistical testing for strategy comparison

#### Multi-Phase Validation
- **Development Phase**: Basic validation with minimum requirements
- **Pre-Staging Phase**: Enhanced validation with larger sample sizes
- **Staging Phase**: Production-ready validation with statistical significance
- **Pre-Production Phase**: Final validation with comprehensive risk assessment

### Issue #46: A/B Testing Framework

**Problem**: Missing A/B testing framework for model deployment.

**Solution**: Comprehensive production A/B testing infrastructure:

#### A/B Testing Features
- **Multiple Test Types**: Fixed-split, multi-arm bandit, champion vs challenger
- **Statistical Monitoring**: Real-time significance testing with early stopping
- **Safety Controls**: Automated stopping based on performance degradation
- **Traffic Allocation**: Dynamic allocation based on performance (bandit algorithms)

#### Testing Workflow
1. **Experiment Setup**: Define arms, traffic allocation, success criteria
2. **Execution**: Real-time traffic routing and outcome tracking
3. **Monitoring**: Continuous statistical analysis and safety monitoring
4. **Decision**: Automated or manual winner selection based on evidence
5. **Deployment**: Gradual rollout of winning strategy

#### Statistical Analysis
- **Significance Testing**: Two-proportion z-tests, t-tests for continuous metrics
- **Confidence Intervals**: Bayesian and frequentist confidence intervals
- **Effect Size**: Practical significance beyond statistical significance
- **Power Analysis**: Sample size recommendations and power calculations

### Issue #49: Production Model Registry

**Problem**: Need production model registry workflows.

**Solution**: Enhanced MLflow registry with betting-specific workflows:

#### Betting Model Stages
- **Development**: Under development and validation
- **Backtesting**: Passed ML validation, undergoing backtesting
- **Paper Trading**: Simulated live trading validation
- **Staging**: Limited live deployment
- **Production**: Full production deployment
- **Champion**: Best performing production model
- **Challenger**: New model challenging champion

#### Promotion Criteria
Each stage has specific promotion criteria combining ML and betting metrics:

```python
BettingModelStage.PRODUCTION: PromotionCriteria(
    min_ml_accuracy=0.58,
    min_ml_roc_auc=0.62,
    min_roi=4.0,
    min_win_rate=0.55,
    max_drawdown=15.0,
    min_sharpe_ratio=1.0,
    min_total_bets=500,
    min_validation_days=30,
    min_confidence_level=0.95,
    max_p_value=0.05,
    min_profit_threshold=Decimal("1000")
)
```

#### Automated Workflows
- **Performance Monitoring**: Continuous monitoring of production models
- **Degradation Detection**: Automated alerts for performance decline
- **Retraining Triggers**: Automated retraining based on performance thresholds
- **Champion/Challenger**: Automated A/B testing for model replacement

## Strategy Development Methodology

### 1. Strategy Ideation and Configuration
```bash
# Create new strategy workflow
uv run -m src.interfaces.cli ml-strategy create-workflow \
    --name "Enhanced Sharp Action" \
    --strategy-type hybrid \
    --validation-method integrated \
    --orchestration-mode semi_automated
```

### 2. Development and Implementation
```bash
# Execute development stage
uv run -m src.interfaces.cli ml-strategy execute-workflow \
    --workflow-id <workflow-id> \
    --target-stage development
```

### 3. Comprehensive Validation
```bash
# Run integrated validation
uv run -m src.interfaces.cli ml-strategy validate \
    --workflow-id <workflow-id> \
    --validation-phase pre_staging \
    --cross-validation \
    --cv-folds 5
```

### 4. Backtesting Validation
```bash
# Execute backtesting stage
uv run -m src.interfaces.cli ml-strategy execute-workflow \
    --workflow-id <workflow-id> \
    --target-stage backtesting
```

### 5. Staging Deployment
```bash
# Promote to staging
uv run -m src.interfaces.cli ml-strategy execute-workflow \
    --workflow-id <workflow-id> \
    --target-stage staging
```

### 6. A/B Testing
```bash
# Setup champion vs challenger test
uv run -m src.interfaces.cli ml-strategy setup-ab-test \
    --champion-workflow <champion-id> \
    --challenger-workflow <challenger-id> \
    --traffic-split 0.8 \
    --duration-days 14
```

### 7. Production Deployment
```bash
# Full workflow execution to production
uv run -m src.interfaces.cli ml-strategy execute-workflow \
    --workflow-id <workflow-id> \
    --full-execution
```

## Evidence-Based Decision Making

### Performance Metrics Integration
The architecture combines traditional ML metrics with betting-specific performance measures:

#### ML Performance Metrics
- **Accuracy**: Classification accuracy for outcome prediction
- **ROC AUC**: Area under receiver operating characteristic curve
- **Precision/Recall**: Positive predictive value and sensitivity
- **F1 Score**: Harmonic mean of precision and recall

#### Betting Performance Metrics
- **ROI**: Return on investment percentage
- **Win Rate**: Percentage of profitable bets
- **Profit Factor**: Ratio of gross profit to gross loss
- **Sharpe Ratio**: Risk-adjusted return metric
- **Maximum Drawdown**: Largest peak-to-trough decline

#### Risk Metrics
- **Value at Risk (VaR)**: Potential loss at given confidence level
- **Expected Shortfall**: Expected loss beyond VaR threshold
- **Kelly Fraction**: Optimal bet sizing based on edge and odds

### Statistical Validation
All strategies undergo rigorous statistical validation:

#### Significance Testing
- **Binomial Tests**: For win rate comparisons
- **Two-Sample Tests**: For performance metric comparisons
- **Multiple Testing Correction**: Bonferroni or FDR correction

#### Confidence Intervals
- **Bootstrap Intervals**: Non-parametric confidence intervals
- **Bayesian Intervals**: Credible intervals with prior information
- **Time Series Intervals**: Accounting for temporal correlation

## Architecture Benefits

### 1. Unified Methodology
- Clear decision framework for approach selection
- Consistent performance evaluation across all strategy types
- Integrated validation combining statistical and practical significance

### 2. Evidence-Based Validation
- Comprehensive validation combining ML and betting performance
- Statistical significance testing with proper multiple testing correction
- Risk-adjusted performance evaluation

### 3. Production-Ready Deployment
- Automated A/B testing with safety controls
- Performance-based model promotion criteria
- Continuous monitoring and alerting

### 4. Risk Management
- Comprehensive risk metrics and monitoring
- Safety controls and automated stopping rules
- Position sizing optimization with Kelly criterion

### 5. Continuous Improvement
- Automated retraining based on performance degradation
- Champion/challenger testing for model replacement
- Performance trending and drift detection

## Usage Examples

### Example 1: Rule-Based Strategy Development
```python
# Create rule-based sharp action strategy
strategy_config = StrategyConfiguration(
    name="Sharp Action Following",
    strategy_type=StrategyType.RULE_BASED,
    validation_method=ValidationMethod.BACKTESTING_ONLY,
    rule_parameters={
        "processor_type": "sharp_action",
        "min_confidence_threshold": 0.7
    }
)

# Develop and validate
framework = StrategyDevelopmentFramework()
success, performance = await framework.develop_rule_based_strategy(
    strategy_config,
    validation_start=datetime(2024, 1, 1),
    validation_end=datetime(2024, 6, 30)
)
```

### Example 2: ML Strategy Development
```python
# Create ML predictive strategy
strategy_config = StrategyConfiguration(
    name="Game Outcome Prediction",
    strategy_type=StrategyType.ML_PREDICTIVE,
    validation_method=ValidationMethod.INTEGRATED_VALIDATION,
    ml_prediction_targets=["moneyline_home_win", "total_over_under"],
    ml_model_name="mlb_outcome_predictor"
)

# Develop and validate
success, performance = await framework.develop_ml_strategy(
    strategy_config,
    training_start=datetime(2024, 1, 1),
    training_end=datetime(2024, 4, 30),
    validation_start=datetime(2024, 5, 1),
    validation_end=datetime(2024, 6, 30)
)
```

### Example 3: Hybrid Strategy Development
```python
# Create hybrid strategy combining rules and ML
strategy_config = StrategyConfiguration(
    name="Enhanced Sharp Action",
    strategy_type=StrategyType.HYBRID,
    validation_method=ValidationMethod.INTEGRATED_VALIDATION,
    rule_parameters={
        "processor_type": "sharp_action",
        "min_confidence_threshold": 0.6
    },
    ml_prediction_targets=["moneyline_home_win"],
    ml_model_name="sharp_action_enhancer"
)

# Develop hybrid strategy
success, performance = await framework.develop_hybrid_strategy(
    strategy_config,
    training_start=datetime(2024, 1, 1),
    training_end=datetime(2024, 4, 30),
    validation_start=datetime(2024, 5, 1),
    validation_end=datetime(2024, 6, 30)
)
```

## Implementation Status

### ✅ Completed Components
- [x] Strategy Development Framework with unified methodology
- [x] Integrated Validation Engine with cross-temporal validation
- [x] A/B Testing Framework with statistical monitoring
- [x] Enhanced Betting Model Registry with promotion criteria
- [x] Strategy Orchestrator for workflow management
- [x] Comprehensive CLI interface for all operations
- [x] Clear methodology guide for approach selection
- [x] Evidence-based performance evaluation
- [x] Statistical significance testing framework

### 🔄 Integration Points
The architecture integrates with existing system components:
- **Existing Backtesting System**: Enhanced with ML model validation
- **MLflow Registry**: Extended with betting-specific criteria
- **Strategy Processors**: Integrated into unified framework
- **Database Schema**: Compatible with existing data structures

### 📈 Future Enhancements
- **Real-time Model Serving**: Deploy models for real-time predictions
- **Advanced Feature Engineering**: Automated feature selection and engineering
- **Ensemble Methods**: Combining multiple strategies for improved performance
- **Reinforcement Learning**: Adaptive strategies that learn from outcomes

## Conclusion

The ML Strategy Architecture provides a comprehensive solution that addresses the core issues in MLB betting strategy development. It establishes clear methodologies for when to use backtesting vs ML training, provides enhanced cross-validation frameworks, implements production A/B testing, and creates a robust model registry workflow.

The architecture emphasizes evidence-based decision making, comprehensive validation, and proper statistical analysis while maintaining practical focus on betting profitability and risk management. This creates a solid foundation for developing, validating, and deploying profitable betting strategies with confidence in their performance.

## Key Files Reference

### Core Architecture
- `src/analysis/strategy_development_framework.py` - Main framework
- `src/analysis/validation/integrated_validation_engine.py` - Validation engine
- `src/analysis/testing/ab_testing_framework.py` - A/B testing framework
- `src/ml/registry/betting_model_registry.py` - Enhanced model registry
- `src/analysis/orchestration/strategy_orchestrator.py` - Workflow orchestrator

### CLI Interface
- `src/interfaces/cli/commands/ml_strategy.py` - ML strategy CLI commands

### Existing Integration Points
- `src/interfaces/cli/commands/backtesting.py` - Existing backtesting system
- `src/ml/training/lightgbm_trainer.py` - Existing ML training system
- `src/ml/registry/model_registry.py` - Base model registry