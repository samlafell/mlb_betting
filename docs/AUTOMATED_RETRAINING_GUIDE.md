# Automated Retraining Workflows Guide

## Overview

The Automated Retraining System provides comprehensive, production-ready retraining workflows that continuously improve betting strategies as new game outcomes and market data become available. The system ensures betting strategies stay profitable as baseball and betting markets evolve through automated triggers, workflow execution, and deployment management.

## Key Features

- **📊 Performance Degradation Detection**: Automatic triggers when strategy ROI drops or win rates decrease
- **⚡ Multi-Strategy Parallel Retraining**: Simultaneous retraining of multiple strategies with resource management
- **🔬 A/B Testing & Validation**: Statistical validation and gradual rollouts with automatic rollback
- **📅 Flexible Scheduling**: Weekly, monthly, or custom schedules with priority management
- **🚨 Real-Time Monitoring**: Comprehensive performance tracking with alerting
- **🤖 Hyperparameter Optimization Integration**: Leverages existing optimization framework
- **🔒 Production Safety**: Built-in validation gates and rollback capabilities

## Architecture Components

### 1. RetrainingTriggerService
Detects when automated retraining should be initiated based on:
- Performance degradation (ROI drops, win rate decreases)
- New data availability (weekly/monthly triggers)
- Market condition changes (significant line movement patterns)
- Scheduled triggers (regular retraining cycles)
- Manual overrides (emergency retraining)

### 2. AutomatedRetrainingEngine
Manages end-to-end retraining workflows including:
- Integration with existing hyperparameter optimization framework
- Multi-strategy parallel retraining capabilities
- Model versioning and rollback capabilities
- A/B testing coordination
- Gradual deployment with performance monitoring

### 3. ModelValidationService
Provides comprehensive model validation before deployment:
- Statistical significance testing
- Cross-validation with temporal data splitting
- Performance degradation detection
- Risk assessment and quality validation

### 4. PerformanceMonitoringService
Real-time strategy performance tracking:
- ROI and win rate monitoring with alerting
- Model drift detection and early warning systems
- Performance trend analysis and forecasting
- Integration with Prometheus metrics service

### 5. RetrainingScheduler
Manages scheduled and triggered retraining workflows:
- Job queue management and prioritization
- Conflict resolution and resource management
- Cron-based and interval-based scheduling
- Integration with trigger service

## Quick Start

### 1. System Status Check

```bash
# Check overall retraining system status
uv run -m src.interfaces.cli retraining status --detailed

# Check specific component status
uv run -m src.interfaces.cli retraining monitoring status
uv run -m src.interfaces.cli retraining triggers stats
```

### 2. Start Manual Retraining

```bash
# Trigger immediate retraining for a strategy
uv run -m src.interfaces.cli retraining jobs start \
    --strategy sharp_action \
    --reason "Performance degradation detected" \
    --priority high \
    --max-evaluations 50

# Monitor job progress
uv run -m src.interfaces.cli retraining jobs status --watch
```

### 3. Schedule Automated Retraining

```bash
# Create weekly retraining schedule
uv run -m src.interfaces.cli retraining schedules create \
    --name "Weekly Sharp Action Retraining" \
    --strategy sharp_action \
    --type interval \
    --interval-hours 168 \
    --priority normal

# Create monthly retraining using cron
uv run -m src.interfaces.cli retraining schedules create \
    --name "Monthly Line Movement Retraining" \
    --strategy line_movement \
    --type cron \
    --cron "0 2 1 * *" \
    --priority high
```

### 4. Model Validation

```bash
# Validate a candidate model
uv run -m src.interfaces.cli retraining models validate \
    --strategy sharp_action \
    --candidate-version candidate_v2.1 \
    --baseline-version production_v2.0 \
    --validation-level standard \
    --output-file validation_report.json

# List model versions
uv run -m src.interfaces.cli retraining models list \
    --strategy sharp_action \
    --limit 10
```

## Detailed Usage

### Trigger Management

#### Check Active Triggers
```bash
# Check all active triggers
uv run -m src.interfaces.cli retraining triggers check

# Check triggers for specific strategy
uv run -m src.interfaces.cli retraining triggers check --strategy sharp_action

# Watch triggers in real-time
uv run -m src.interfaces.cli retraining triggers check --watch
```

#### Create Manual Triggers
```bash
# Create critical performance trigger
uv run -m src.interfaces.cli retraining triggers create \
    --strategy line_movement \
    --reason "ROI dropped below 2% for 3 consecutive days" \
    --severity critical

# Create maintenance trigger
uv run -m src.interfaces.cli retraining triggers create \
    --strategy consensus \
    --reason "Monthly maintenance retraining" \
    --severity low
```

#### Resolve Triggers
```bash
# Resolve specific trigger
uv run -m src.interfaces.cli retraining triggers resolve TRIGGER_ID

# View trigger statistics
uv run -m src.interfaces.cli retraining triggers stats
```

### Job Management

#### Start Retraining Jobs
```bash
# Full retraining with default settings
uv run -m src.interfaces.cli retraining jobs start \
    --strategy sharp_action \
    --strategy-type full_retraining \
    --priority normal

# Targeted optimization for quick improvements
uv run -m src.interfaces.cli retraining jobs start \
    --strategy line_movement \
    --strategy-type targeted_optimization \
    --max-evaluations 25 \
    --timeout-hours 6 \
    --priority high \
    --reason "Quick parameter adjustment"
```

#### Monitor Job Progress
```bash
# View all active jobs
uv run -m src.interfaces.cli retraining jobs status

# Monitor specific job with real-time updates
uv run -m src.interfaces.cli retraining jobs status \
    --job-id JOB_ID \
    --watch

# View jobs for specific strategy
uv run -m src.interfaces.cli retraining jobs status \
    --strategy sharp_action
```

#### Job Management
```bash
# Cancel running job
uv run -m src.interfaces.cli retraining jobs cancel JOB_ID

# Force cancellation without confirmation
uv run -m src.interfaces.cli retraining jobs cancel JOB_ID --force
```

### Performance Monitoring

#### Monitor Strategy Performance
```bash
# Overall monitoring status
uv run -m src.interfaces.cli retraining monitoring status

# Detailed status for specific strategy
uv run -m src.interfaces.cli retraining monitoring status \
    --strategy sharp_action \
    --detailed

# View performance alerts
uv run -m src.interfaces.cli retraining monitoring alerts

# View critical alerts only
uv run -m src.interfaces.cli retraining monitoring alerts \
    --level critical

# View alert history
uv run -m src.interfaces.cli retraining monitoring alerts \
    --history \
    --strategy line_movement
```

### Schedule Management

#### Create Schedules
```bash
# Weekly interval schedule
uv run -m src.interfaces.cli retraining schedules create \
    --name "Weekly Maintenance Retraining" \
    --strategy sharp_action \
    --type interval \
    --interval-hours 168 \
    --priority normal \
    --description "Weekly automated retraining for model maintenance"

# Monthly cron schedule (first Sunday of month at 3 AM)
uv run -m src.interfaces.cli retraining schedules create \
    --name "Monthly Full Retraining" \
    --strategy line_movement \
    --type cron \
    --cron "0 3 1-7 * 0" \
    --priority high \
    --description "Monthly comprehensive retraining"
```

#### Manage Schedules
```bash
# List all schedules
uv run -m src.interfaces.cli retraining schedules list

# Update schedule (programmatically via API)
# Remove schedule (programmatically via API)
```

### Model Validation

#### Validation Levels
- **Basic**: Performance comparison only
- **Standard**: Statistical tests and temporal validation
- **Rigorous**: Cross-validation and risk analysis
- **Production**: Comprehensive validation for deployment

```bash
# Basic validation for quick checks
uv run -m src.interfaces.cli retraining models validate \
    --strategy sharp_action \
    --candidate-version new_model_v1.5 \
    --validation-level basic

# Rigorous validation for production deployment
uv run -m src.interfaces.cli retraining models validate \
    --strategy line_movement \
    --candidate-version candidate_v3.0 \
    --baseline-version production_v2.8 \
    --validation-level rigorous \
    --output-file line_movement_validation_report.json
```

## Configuration

### Trigger Thresholds

The system uses configurable thresholds for automatic trigger detection:

```python
# Performance thresholds
min_roi_percentage = 5.0  # Minimum acceptable ROI
roi_degradation_percentage = 15.0  # % drop that triggers retraining
min_win_rate = 0.55  # Minimum acceptable win rate
win_rate_degradation_percentage = 10.0  # % drop that triggers retraining

# Time windows
short_term_days = 7  # Short-term performance window
medium_term_days = 30  # Medium-term performance window
long_term_days = 90  # Long-term performance window
```

### Retraining Configuration

```python
# Optimization settings
algorithm = "bayesian_optimization"
max_evaluations = 50
high_impact_only = True
parallel_jobs = 2
timeout_hours = 12

# A/B testing settings
ab_test_duration_hours = 72
ab_test_traffic_percentage = 20.0
statistical_significance_threshold = 0.05
min_improvement_threshold = 2.0  # Minimum 2% improvement required

# Rollout settings
gradual_rollout_enabled = True
rollout_stages = [10.0, 25.0, 50.0, 100.0]  # Traffic percentages
rollout_stage_duration_hours = 24
```

### Validation Criteria

```python
# Performance requirements
min_improvement_percentage = 2.0
min_absolute_roi = 2.0
min_win_rate = 0.52
min_sample_size = 50

# Statistical requirements
significance_level = 0.05  # p < 0.05
confidence_level = 0.95  # 95% confidence intervals

# Risk requirements
max_drawdown_percentage = 15.0
min_sharpe_ratio = 0.8
```

## Integration with Existing Systems

### Hyperparameter Optimization

The retraining system integrates seamlessly with the existing hyperparameter optimization framework:

```bash
# View optimization parameters for a strategy
uv run -m src.interfaces.cli optimize list-parameters sharp_action

# The retraining system uses the same parameter spaces
# and optimization algorithms automatically
```

### Monitoring Integration

Performance monitoring integrates with the Prometheus metrics service:

```bash
# Start monitoring dashboard to view retraining metrics
uv run -m src.interfaces.cli monitoring dashboard

# View system status including retraining components
uv run -m src.interfaces.cli monitoring status
```

### Database Integration

The system uses the unified repository pattern for all database operations and integrates with existing data quality infrastructure.

## Production Deployment

### 1. Infrastructure Requirements

- **Database**: PostgreSQL with existing MLB betting database
- **Resources**: Sufficient CPU/memory for parallel optimization jobs
- **Monitoring**: Prometheus metrics integration (optional)
- **Scheduling**: Cron or systemd for service management

### 2. Service Management

```bash
# In production, run as a service
# Start the retraining system services

# Example systemd service file
# /etc/systemd/system/mlb-retraining.service
[Unit]
Description=MLB Betting Automated Retraining System
After=network.target postgresql.service

[Service]
Type=simple
User=mlb-betting
WorkingDirectory=/opt/mlb-betting
ExecStart=/opt/mlb-betting/.venv/bin/python -m src.services.retraining_daemon
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### 3. Monitoring Setup

```bash
# Configure Prometheus metrics collection
# Add to prometheus.yml:
scrape_configs:
  - job_name: 'mlb-retraining'
    static_configs:
      - targets: ['localhost:8000']
    scrape_interval: 30s
```

### 4. Backup and Recovery

- **Model Versions**: Automatically stored with versioning
- **Configuration**: Store schedule and trigger configurations
- **Performance Data**: Regular backups of monitoring data
- **Rollback Capability**: Built-in model rollback functionality

## Best Practices

### 1. Gradual Rollout Strategy

- Start with 10% traffic for new models
- Monitor performance closely during rollout
- Use statistical significance testing
- Implement automatic rollback for performance degradation

### 2. Resource Management

- Limit concurrent retraining jobs (default: 2)
- Use appropriate timeout settings
- Monitor system resource usage
- Schedule heavy retraining during low-traffic periods

### 3. Validation Standards

- Always use statistical significance testing
- Require minimum improvement thresholds
- Validate on out-of-sample data
- Monitor for overfitting

### 4. Alert Management

- Set appropriate alert thresholds
- Implement escalation procedures
- Monitor alert fatigue
- Regular review of trigger conditions

### 5. Schedule Management

- Avoid overlapping retraining jobs
- Consider market conditions for timing
- Use priority-based job scheduling
- Regular review of schedule effectiveness

## Troubleshooting

### Common Issues

#### 1. Retraining Jobs Fail to Start

```bash
# Check system resources
uv run -m src.interfaces.cli retraining status --detailed

# Verify strategy configuration
uv run -m src.interfaces.cli optimize list-parameters STRATEGY_NAME

# Check database connectivity
uv run -m src.interfaces.cli database setup-action-network --test-connection
```

#### 2. Poor Model Performance

```bash
# Increase hyperparameter search space
uv run -m src.interfaces.cli retraining jobs start \
    --strategy STRATEGY_NAME \
    --max-evaluations 100 \
    --timeout-hours 24

# Use longer training periods
# Review validation criteria
```

#### 3. Trigger System Not Working

```bash
# Check trigger service status
uv run -m src.interfaces.cli retraining triggers stats

# Verify performance data availability
uv run -m src.interfaces.cli retraining monitoring status --detailed

# Review trigger thresholds in configuration
```

#### 4. Schedule Execution Issues

```bash
# Check schedule configuration
uv run -m src.interfaces.cli retraining schedules list

# Verify system time and cron expressions
# Check for resource conflicts
```

### Performance Optimization

#### Speed up Retraining
```bash
# Use high-impact parameters only
uv run -m src.interfaces.cli retraining jobs start \
    --strategy STRATEGY_NAME \
    --max-evaluations 25 \
    --strategy-type targeted_optimization

# Increase parallel jobs (if resources allow)
# Use shorter validation periods for quick tests
```

#### Improve Accuracy
```bash
# Use longer validation periods
# Increase maximum evaluations
# Use rigorous validation levels
```

### Debug Mode

```bash
# Enable detailed logging (implementation specific)
# Monitor job execution logs
# Review validation reports

# Check individual component status
uv run -m src.interfaces.cli retraining triggers check --watch
uv run -m src.interfaces.cli retraining monitoring status --detailed
```

## API Reference

### Programmatic Usage

The retraining system provides Python APIs for custom integration:

```python
import asyncio
from datetime import datetime, timedelta
from src.services.retraining import (
    RetrainingTriggerService,
    AutomatedRetrainingEngine,
    ModelValidationService,
    TriggerSeverity,
    RetrainingStrategy
)
from src.data.database import UnifiedRepository

async def custom_retraining_workflow():
    config = get_settings()
    repository = UnifiedRepository(config.database.connection_string)
    
    # Initialize services
    trigger_service = RetrainingTriggerService(repository)
    retraining_engine = AutomatedRetrainingEngine(repository, strategy_orchestrator)
    
    await retraining_engine.start_engine()
    
    try:
        # Create manual trigger
        trigger = await trigger_service.create_manual_trigger(
            strategy_name="sharp_action",
            reason="Custom automation trigger",
            severity=TriggerSeverity.HIGH
        )
        
        # Start retraining job
        job = await retraining_engine.trigger_retraining(
            strategy_name="sharp_action",
            trigger_conditions=[trigger],
            retraining_strategy=RetrainingStrategy.FULL_RETRAINING
        )
        
        # Monitor job progress
        while job.status.value == "running":
            status = retraining_engine.get_job_status(job.job_id)
            print(f"Progress: {status.progress_percentage:.1f}%")
            await asyncio.sleep(30)
        
        # Get final results
        final_job = retraining_engine.get_job_status(job.job_id)
        print(f"Job completed: {final_job.status.value}")
        
        if final_job.improvement_percentage:
            print(f"Improvement: {final_job.improvement_percentage:.1f}%")
    
    finally:
        await retraining_engine.stop_engine()

# Run custom workflow
asyncio.run(custom_retraining_workflow())
```

## Metrics and KPIs

### Success Metrics

- **95%+ automated retraining success rate**
- **<10% performance degradation trigger threshold**
- **24-hour maximum time from trigger to deployment**
- **Zero manual intervention for standard cycles**
- **15%+ average improvement in strategy performance**

### Monitoring Metrics

- Active retraining jobs
- Trigger detection rate
- Model validation success rate
- A/B test success rate
- Deployment success rate
- Average time to deployment
- Strategy performance improvement

## Support and Maintenance

### Log Files
- Application logs: `logs/retraining_system.log`
- Job execution logs: `logs/retraining_jobs.log`
- Performance monitoring logs: `logs/performance_monitoring.log`

### Health Checks
```bash
# System health check
uv run -m src.interfaces.cli retraining status

# Component health checks
uv run -m src.interfaces.cli retraining monitoring status
uv run -m src.interfaces.cli retraining triggers stats
```

### Regular Maintenance
- Review and adjust trigger thresholds monthly
- Analyze retraining job success rates weekly
- Monitor system resource usage
- Update validation criteria based on market changes
- Regular backup of model versions and configurations

## Conclusion

The Automated Retraining Workflows system provides a comprehensive, production-ready solution for maintaining and improving betting strategy performance. With automatic trigger detection, sophisticated validation, and gradual deployment capabilities, it ensures that betting strategies adapt to changing market conditions while maintaining high reliability and performance standards.

For additional support or advanced configuration, refer to the test suite in `tests/test_retraining_system.py` or examine the implementation details in `src/services/retraining/`.