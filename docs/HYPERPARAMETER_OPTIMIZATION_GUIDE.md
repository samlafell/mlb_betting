# Hyperparameter Optimization Framework Guide

## Overview

The hyperparameter optimization framework provides automated parameter tuning for MLB betting strategies to maximize ROI through systematic exploration of parameter spaces. The framework implements multiple optimization algorithms with robust validation and statistical analysis.

## Architecture

### Core Components

1. **OptimizationEngine**: Central orchestrator managing optimization jobs and coordinating algorithm execution
2. **ParameterSpace**: Defines search spaces with constraints for different parameter types
3. **StrategyParameterRegistry**: Pre-configured parameter spaces for all supported strategies
4. **OptimizationJob**: Job management with progress tracking and result persistence
5. **Validation Framework**: Cross-validation and statistical validation to prevent overfitting
6. **Results Analysis**: Comprehensive analysis and performance comparison tools

### Supported Algorithms

- **Grid Search**: Exhaustive search over parameter grid
- **Random Search**: Random sampling from parameter space
- **Bayesian Optimization**: Gaussian Process-based intelligent search (requires scikit-learn)

### Strategy Support

- **Sharp Action Processor**: 9 optimizable parameters including differential thresholds, timing multipliers
- **Line Movement Processor**: 10 optimizable parameters including movement thresholds and confidence modifiers  
- **Consensus Processor**: 8 optimizable parameters including consensus thresholds and alignment criteria
- **Late Flip Processor**: 3 optimizable parameters for flip detection and timing

## Quick Start

### 1. Basic Optimization

```bash
# Optimize sharp action strategy with default settings
uv run -m src.interfaces.cli optimize run --strategy sharp_action

# Optimize with specific algorithm and evaluation limit
uv run -m src.interfaces.cli optimize run \
    --strategy line_movement \
    --algorithm bayesian_optimization \
    --max-evaluations 50
```

### 2. Custom Date Range

```bash
# Optimize using specific validation period
uv run -m src.interfaces.cli optimize run \
    --strategy consensus \
    --start-date 2024-01-01 \
    --end-date 2024-06-30 \
    --max-evaluations 100
```

### 3. High-Impact Parameters Only

```bash
# Focus on most impactful parameters for faster optimization
uv run -m src.interfaces.cli optimize run \
    --strategy sharp_action \
    --high-impact-only \
    --max-evaluations 25
```

### 4. Monitor Progress

```bash
# Check status of all active jobs
uv run -m src.interfaces.cli optimize status

# Watch specific job progress in real-time
uv run -m src.interfaces.cli optimize status --job-id JOB_ID --watch

# Check job progress every 10 seconds
uv run -m src.interfaces.cli optimize status --watch --refresh-seconds 10
```

### 5. Analyze Results

```bash
# Analyze optimization results
uv run -m src.interfaces.cli optimize analyze JOB_ID \
    --show-params \
    --show-convergence \
    --output-file analysis_report.json

# Compare against baseline
uv run -m src.interfaces.cli optimize analyze JOB_ID \
    --compare-baseline 5.2
```

## Advanced Usage

### Parameter Exploration

```bash
# List available parameters for a strategy
uv run -m src.interfaces.cli optimize list-parameters sharp_action

# Show only high-impact parameters
uv run -m src.interfaces.cli optimize list-parameters sharp_action --high-impact-only

# Export parameter definitions as JSON
uv run -m src.interfaces.cli optimize list-parameters sharp_action --format json > params.json
```

### Cross-Validation

```bash
# Validate specific parameters with cross-validation
uv run -m src.interfaces.cli optimize validate sharp_action \
    --parameters-file best_params.json \
    --cv-folds 5 \
    --days-back 120
```

### Parallel Optimization

```bash
# Run multiple strategies in parallel
uv run -m src.interfaces.cli optimize run \
    --strategy all \
    --parallel-jobs 4 \
    --max-evaluations 200
```

## Parameter Configuration

### Sharp Action Processor Parameters

| Parameter | Type | Range | Impact | Description |
|-----------|------|-------|---------|-------------|
| `min_differential_threshold` | Continuous | 5.0-30.0 | High | Minimum money/bet percentage differential |
| `high_confidence_threshold` | Continuous | 15.0-35.0 | High | High confidence differential threshold |
| `ultra_late_multiplier` | Continuous | 1.2-2.0 | High | Timing multiplier for ultra late action |
| `pinnacle_weight` | Continuous | 1.5-3.0 | High | Weight multiplier for Pinnacle (sharp book) |
| `volume_weight_factor` | Continuous | 1.0-3.0 | Medium | Weight factor for volume in confidence |
| `min_volume_threshold` | Discrete | [50,75,100,150,200,250,300] | Medium | Minimum volume for reliable detection |
| `circa_weight` | Continuous | 1.5-2.5 | Medium | Weight multiplier for Circa |
| `draftkings_weight` | Continuous | 1.0-1.5 | Low | Weight multiplier for DraftKings |
| `closing_hour_multiplier` | Continuous | 1.1-1.6 | Low | Timing multiplier for closing hour |

### Line Movement Processor Parameters

| Parameter | Type | Range | Impact | Description |
|-----------|------|-------|---------|-------------|
| `min_movement_threshold` | Continuous | 0.25-1.5 | High | Minimum line movement significance |
| `steam_move_threshold` | Continuous | 0.75-2.0 | High | Threshold for steam move detection |
| `reverse_line_movement_modifier` | Continuous | 1.1-1.8 | High | Confidence modifier for reverse movement |
| `min_book_consensus` | Discrete | [2,3,4,5] | High | Minimum books for movement consensus |
| `late_movement_hours` | Continuous | 1.0-6.0 | Medium | Hours before game for late movement |
| `steam_move_modifier` | Continuous | 1.1-1.6 | Medium | Confidence modifier for steam moves |
| `late_movement_modifier` | Continuous | 1.1-1.5 | Medium | Confidence modifier for late movements |
| `multi_book_consensus_modifier` | Continuous | 1.1-1.5 | Medium | Modifier for multi-book consensus |
| `moneyline_movement_threshold` | Discrete | [3,4,5,6,7,8,10] | Low | Minimum cents for moneyline significance |
| `spread_movement_threshold` | Continuous | 0.25-1.0 | Low | Minimum point movement for spreads |

### Consensus Processor Parameters

| Parameter | Type | Range | Impact | Description |
|-----------|------|-------|---------|-------------|
| `heavy_consensus_threshold` | Continuous | 80.0-95.0 | High | Threshold for heavy consensus detection |
| `mixed_consensus_money_threshold` | Continuous | 70.0-85.0 | High | Money percentage for mixed consensus |
| `heavy_consensus_modifier` | Continuous | 1.1-1.6 | High | Confidence modifier for heavy consensus |
| `mixed_consensus_bet_threshold` | Continuous | 55.0-70.0 | Medium | Bet percentage for mixed consensus |
| `min_consensus_strength` | Continuous | 60.0-80.0 | Medium | Minimum consensus strength for signals |
| `max_alignment_difference` | Continuous | 20.0-40.0 | Medium | Max difference between money/bet percentages |
| `mixed_consensus_modifier` | Continuous | 1.0-1.3 | Low | Confidence modifier for mixed consensus |
| `perfect_alignment_modifier` | Continuous | 1.1-1.4 | Low | Bonus for perfect alignment (≤5% difference) |

## Optimization Strategies

### 1. Initial Exploration (Recommended)

```bash
# Start with high-impact parameters and random search
uv run -m src.interfaces.cli optimize run \
    --strategy sharp_action \
    --algorithm random_search \
    --high-impact-only \
    --max-evaluations 30
```

**Benefits**: 
- Fast initial results (15-30 minutes)
- Identifies promising parameter regions
- Low computational cost

### 2. Focused Search

```bash
# Use Bayesian optimization on promising regions
uv run -m src.interfaces.cli optimize run \
    --strategy sharp_action \
    --algorithm bayesian_optimization \
    --max-evaluations 100 \
    --parallel-jobs 2
```

**Benefits**:
- Intelligent exploration
- Efficient parameter space coverage
- Good balance of exploration/exploitation

### 3. Comprehensive Grid Search

```bash
# Exhaustive search for final parameter tuning
uv run -m src.interfaces.cli optimize run \
    --strategy sharp_action \
    --algorithm grid_search \
    --high-impact-only \
    --max-evaluations 200
```

**Benefits**:
- Guaranteed coverage of parameter space
- Reproducible results
- Good for final validation

## Performance Metrics

### Optimization Objectives

- **ROI Percentage** (default): Return on investment percentage
- **Win Rate**: Percentage of winning bets
- **Profit Factor**: Ratio of gross profit to gross loss
- **Sharpe Ratio**: Risk-adjusted returns

### Validation Metrics

- **Cross-Validation Mean/Std**: Average and standard deviation across folds
- **Statistical Significance**: Whether improvement is statistically significant
- **Overfitting Risk**: Score indicating potential overfitting (0-1 scale)
- **Consistency Score**: How consistent performance is across folds
- **Robustness Score**: Worst-case to average-case performance ratio

## Best Practices

### 1. Start Small
- Begin with high-impact parameters only
- Use 20-50 evaluations for initial exploration
- Focus on one strategy at a time

### 2. Validate Properly
- Use realistic validation periods (60-90 days minimum)
- Ensure sufficient data (≥20 bets per fold)
- Check for statistical significance

### 3. Prevent Overfitting
- Use cross-validation with time-series splits
- Monitor overfitting risk scores
- Validate on out-of-sample data

### 4. Monitor Resources
- Use appropriate parallelization
- Set reasonable timeout limits
- Monitor job progress regularly

### 5. Analyze Results
- Examine parameter importance
- Check convergence patterns
- Compare against baselines

## Troubleshooting

### Common Issues

**1. Insufficient Data**
```
Error: Insufficient samples: 15 < 20
```
- **Solution**: Increase validation period or reduce minimum sample requirements
- **Command**: Add `--days-back 120` for longer validation period

**2. No Improvement Found**
```
Warning: No improvement found after 50 evaluations
```
- **Solution**: Try different algorithm or expand parameter ranges
- **Command**: Switch to `--algorithm random_search` or check parameter bounds

**3. Job Timeout**
```
Job timed out after 24 hours
```
- **Solution**: Increase timeout or reduce evaluations
- **Command**: Add `--timeout-hours 48` or reduce `--max-evaluations`

**4. Memory Issues**
```
Error: Memory limit exceeded
```
- **Solution**: Reduce parallel jobs or evaluation batch size
- **Command**: Use `--parallel-jobs 1` for memory-constrained environments

### Performance Optimization

**Speed up optimization**:
```bash
# Reduce evaluations and focus on high-impact parameters
uv run -m src.interfaces.cli optimize run \
    --strategy sharp_action \
    --high-impact-only \
    --max-evaluations 25 \
    --parallel-jobs 4
```

**Improve accuracy**:
```bash
# Longer validation period with more folds
uv run -m src.interfaces.cli optimize validate sharp_action \
    --cv-folds 7 \
    --days-back 150
```

**Debug parameter ranges**:
```bash
# List parameters with current ranges
uv run -m src.interfaces.cli optimize list-parameters sharp_action --format json
```

## Integration with Existing System

### 1. Strategy Processors
The optimization framework integrates seamlessly with existing strategy processors:
- Parameters are automatically mapped to processor configurations
- All processors support dynamic parameter updates
- Optimized parameters can be deployed without code changes

### 2. Backtesting Engine
Uses the existing recommendation-based backtesting engine:
- Same validation logic as live trading
- Consistent performance measurement
- Realistic transaction costs and slippage

### 3. CLI Integration
Full integration with the main CLI system:
- Consistent command patterns
- Unified logging and error handling
- Compatible with existing monitoring tools

### 4. Database Persistence
Results are automatically persisted:
- Job progress and results stored in database
- Parameter configurations tracked for reproducibility
- Historical optimization runs maintained for analysis

## API Usage

### Programmatic Optimization

```python
import asyncio
from datetime import datetime, timedelta
from src.analysis.optimization import OptimizationEngine, StrategyParameterRegistry
from src.analysis.optimization.job import OptimizationAlgorithm
from src.data.database import UnifiedRepository

async def optimize_strategy():
    # Initialize components
    repository = UnifiedRepository("postgresql://...")
    engine = OptimizationEngine(repository, {"max_workers": 2})
    registry = StrategyParameterRegistry()
    
    # Get parameter space
    parameter_space = registry.get_parameter_space("sharp_action")
    
    # Create strategy processors
    processors = [...]  # Your strategy processors
    
    # Run optimization
    job = await engine.optimize_strategy(
        strategy_name="sharp_action",
        parameter_space=parameter_space,
        strategy_processors=processors,
        validation_start_date=datetime.now() - timedelta(days=90),
        validation_end_date=datetime.now(),
        algorithm=OptimizationAlgorithm.BAYESIAN_OPTIMIZATION,
        max_evaluations=50
    )
    
    # Monitor progress
    while job.status.value == "running":
        progress = job.get_progress_info()
        print(f"Progress: {progress['progress_percentage']:.1f}%")
        await asyncio.sleep(30)
    
    # Get results
    best_params = job.get_best_parameters()
    return best_params

# Run optimization
best_parameters = asyncio.run(optimize_strategy())
```

### Custom Parameter Spaces

```python
from src.analysis.optimization import ParameterSpace, ParameterConfig, ParameterType

# Define custom parameters
params = [
    ParameterConfig(
        name="custom_threshold",
        parameter_type=ParameterType.CONTINUOUS,
        bounds=(0.1, 5.0),
        default_value=1.0,
        description="Custom threshold parameter"
    ),
    ParameterConfig(
        name="strategy_mode",
        parameter_type=ParameterType.CATEGORICAL,
        choices=["aggressive", "conservative", "balanced"],
        default_value="balanced",
        description="Strategy mode selection"
    )
]

# Create parameter space
custom_space = ParameterSpace("CustomStrategy", params)

# Use in optimization
job = await engine.optimize_strategy(
    strategy_name="custom_strategy",
    parameter_space=custom_space,
    # ... other parameters
)
```

## Contributing

### Adding New Strategy Parameters

1. **Update StrategyParameterRegistry**:
```python
# In src/analysis/optimization/strategies.py
new_strategy_params = [
    ParameterConfig(
        name="new_parameter",
        parameter_type=ParameterType.CONTINUOUS,
        bounds=(1.0, 10.0),
        default_value=5.0,
        description="Description of new parameter"
    )
]

self._parameter_spaces["new_strategy"] = ParameterSpace("NewStrategy", new_strategy_params)
```

2. **Update High-Impact Parameters**:
```python
high_impact_params = {
    "new_strategy": ["new_parameter", "other_high_impact_param"]
}
```

3. **Add CLI Support**:
```python
# Update CLI command choices
@click.option("--strategy", type=click.Choice(["sharp_action", "line_movement", "consensus", "new_strategy"]))
```

### Adding New Optimization Algorithms

1. **Implement Algorithm Class**:
```python
class NewOptimizer(OptimizationAlgorithmBase):
    async def suggest_parameters(self, job: OptimizationJob) -> List[Dict[str, Any]]:
        # Your algorithm implementation
        pass
```

2. **Update Engine**:
```python
def _create_algorithm(self, config: OptimizationConfig) -> OptimizationAlgorithmBase:
    if config.algorithm == OptimizationAlgorithm.NEW_ALGORITHM:
        return NewOptimizer(config.parameter_space, config)
```

3. **Add CLI Support**:
```python
@click.option("--algorithm", type=click.Choice([..., "new_algorithm"]))
```

## Conclusion

The hyperparameter optimization framework provides a comprehensive solution for improving betting strategy performance through systematic parameter tuning. With support for multiple algorithms, robust validation, and detailed analysis tools, it enables data-driven optimization while preventing overfitting and ensuring reproducible results.

For additional support or questions, refer to the test suite in `tests/test_optimization_framework.py` or examine the implementation details in `src/analysis/optimization/`.