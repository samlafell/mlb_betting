# Intelligent Resource Management System

## Overview

The Intelligent Resource Management System provides adaptive resource allocation for the MLB Betting ML Pipeline. It dynamically adjusts CPU, memory, disk, and network resources based on real-time monitoring and predictive analytics to optimize performance while preventing resource exhaustion.

## Architecture

### Core Components

1. **Resource Monitor** (`src/ml/monitoring/resource_monitor.py`)
   - Real-time system resource monitoring
   - CPU, Memory, Disk, Network metrics collection
   - Alert generation and thresholds management
   - Historical data collection for trend analysis

2. **Adaptive Resource Manager** (`src/ml/monitoring/adaptive_resource_manager.py`)
   - Intelligent resource allocation decisions
   - Predictive demand forecasting
   - Multiple allocation strategies (Conservative, Balanced, Aggressive, Adaptive)
   - Component priority management

3. **Resource Allocation Controller** (`src/ml/monitoring/resource_allocation_controller.py`)
   - Applies allocation decisions to ML components
   - Component registration and lifecycle management
   - Allocation history tracking
   - Real-time allocation adjustments

### Integration Points

The system integrates with all major ML pipeline components:

- **Feature Pipeline** (`src/ml/features/feature_pipeline.py`)
- **Prediction Service** (`src/ml/services/prediction_service.py`)
- **Redis Feature Store** (`src/ml/features/redis_feature_store.py`)
- **Comprehensive Monitor** (`src/ml/monitoring/comprehensive_monitoring.py`)

## Key Features

### 1. Adaptive Allocation Strategies

**Conservative Strategy**
- Prioritizes stability and reliability
- Larger resource buffers (20-30%)
- Slower adjustment cycles
- Ideal for production environments

**Balanced Strategy**
- Balance between performance and stability
- Moderate resource buffers (30-40%)
- Standard adjustment cycles
- Default recommendation for most use cases

**Aggressive Strategy**
- Maximizes performance and throughput
- Larger allocations (50-60% buffers)
- Faster adjustment cycles
- Suitable for development and testing

**Adaptive Strategy** (Recommended)
- Dynamically adjusts based on current conditions
- Resource pressure-aware allocations
- Priority-based resource distribution
- Intelligent response to system state

### 2. Resource Quotas and Priorities

**Priority Levels**
- **Critical**: Mission-critical operations (predictions, model serving)
- **High**: Important operations (feature extraction, caching)
- **Normal**: Standard operations (batch processing, analytics)
- **Low**: Background operations (cleanup, maintenance)

**Resource Types**
- **CPU**: Percentage allocation (5-80% range)
- **Memory**: Allocation in MB (100MB-4GB range)
- **Disk I/O**: Operations per second limits
- **Network**: Bandwidth allocation in MB/s

### 3. Predictive Analytics

**Demand Forecasting**
- Historical usage pattern analysis
- Trend calculation using linear regression
- Confidence scoring based on data availability
- Time-horizon predictions (15-minute default)

**Resource Pressure Detection**
- Real-time threshold monitoring
- Multi-level alerting (Warning, Critical, Emergency)
- Automatic cleanup triggers
- Graceful degradation patterns

### 4. Component Integration

**Automatic Registration**
Components self-register with the allocation controller during initialization:

```python
# Feature Pipeline Integration
async def _initialize_allocation_integration(self):
    self.allocation_controller = await get_resource_allocation_controller()
    
    allocation_controllers = {
        "cpu_percent": self.set_cpu_allocation,
        "memory_mb": self.set_memory_allocation,
        "batch_size": self.set_batch_size,
        "concurrent_operations": self.set_max_concurrent_operations,
    }
    
    await self.allocation_controller.register_component(
        "feature_pipeline", self, allocation_controllers
    )
```

**Dynamic Resource Adjustment**
Components implement allocation methods that are called automatically:

```python
async def set_cpu_allocation(self, cpu_percent: float) -> bool:
    old_allocation = self.cpu_allocation
    self.cpu_allocation = max(5.0, min(80.0, cpu_percent))
    
    if abs(self.cpu_allocation - old_allocation) > 1.0:
        self.allocation_adjustments += 1
        logger.info(f"CPU allocation adjusted: {old_allocation:.1f}% → {self.cpu_allocation:.1f}%")
    
    return True
```

## Configuration

### Resource Monitoring Thresholds

Configure in `config.toml`:

```toml
[ml_pipeline]
# Resource Monitoring Thresholds
cpu_warning_threshold = 70.0       # CPU usage warning threshold percentage
cpu_critical_threshold = 85.0      # CPU usage critical threshold percentage
cpu_emergency_threshold = 95.0     # CPU usage emergency threshold percentage

memory_warning_threshold = 75.0    # Memory usage warning threshold percentage
memory_critical_threshold = 85.0   # Memory usage critical threshold percentage
memory_emergency_threshold = 95.0  # Memory usage emergency threshold percentage

disk_warning_threshold = 80.0      # Disk usage warning threshold percentage
disk_critical_threshold = 90.0     # Disk usage critical threshold percentage
disk_emergency_threshold = 95.0    # Disk usage emergency threshold percentage

resource_monitoring_interval = 10  # Resource monitoring interval in seconds
resource_alert_cooldown = 300      # Resource alert cooldown period in seconds
```

### Default Component Quotas

The system initializes with these default quotas:

| Component | Priority | CPU (%) | Memory (MB) | Justification |
|----------|----------|---------|-------------|---------------|
| Prediction Service | Critical | 25.0 | 512 | Core ML predictions |
| Feature Pipeline | High | 20.0 | 400 | Feature processing |
| Redis Feature Store | High | 15.0 | 300 | High-speed caching |
| Comprehensive Monitor | Normal | 8.0 | 150 | System monitoring |

## Usage

### CLI Commands

Start the resource management system:
```bash
uv run -m src.interfaces.cli resource-management start --strategy adaptive --duration 60
```

Check current status:
```bash
uv run -m src.interfaces.cli resource-management status
```

View allocation history:
```bash
uv run -m src.interfaces.cli resource-management history --component feature_pipeline --limit 20
```

Set resource quotas:
```bash
uv run -m src.interfaces.cli resource-management set-quota prediction_service --cpu 30 --memory 600 --priority critical
```

Monitor resources in real-time:
```bash
uv run -m src.interfaces.cli resource-management monitor
```

Export configuration and statistics:
```bash
uv run -m src.interfaces.cli resource-management export --output json
```

### Programmatic API

```python
from src.ml.monitoring.adaptive_resource_manager import get_adaptive_resource_manager, AllocationStrategy
from src.ml.monitoring.resource_allocation_controller import get_resource_allocation_controller

# Initialize system
manager = await get_adaptive_resource_manager(AllocationStrategy.ADAPTIVE)
controller = await get_resource_allocation_controller()

# Start resource management
await manager.start_management()
await controller.start_allocation_control()

# Get current status
stats = manager.get_management_stats()
status = controller.get_allocation_status()
summary = manager.get_resource_summary()

# Stop system
await manager.stop_management()
await controller.stop_allocation_control()
```

## Performance Impact

### Benefits
- **Automatic Optimization**: Dynamic adjustment based on actual usage patterns
- **Resource Efficiency**: Prevents both under-utilization and over-allocation
- **System Stability**: Proactive resource pressure management
- **Performance Scaling**: Intelligent scaling based on workload demands

### Overhead
- **CPU**: <2% additional CPU usage for monitoring and management
- **Memory**: ~50-100MB for system components and historical data
- **Network**: Minimal overhead for internal component communication
- **Disk**: <10MB for configuration and history storage

### Response Times
- **Allocation Decisions**: <100ms average decision time
- **Resource Adjustment**: <500ms to apply changes to components
- **Monitoring Cycle**: 10-30 second intervals (configurable)
- **Prediction Updates**: 1-5 minute intervals based on data availability

## Monitoring and Alerting

### Metrics Exposed

The system exposes comprehensive metrics through Prometheus:

- `ml_resource_cpu_percent`: Current CPU usage percentage
- `ml_resource_memory_percent`: Current memory usage percentage  
- `ml_resource_allocation_adjustments_total`: Total allocation adjustments
- `ml_resource_pressure_events_total`: Resource pressure events
- `ml_resource_prediction_accuracy`: Prediction accuracy metrics

### Alert Conditions

**Resource Pressure Alerts**
- Warning: CPU >70%, Memory >75%, Disk >80%
- Critical: CPU >85%, Memory >85%, Disk >90%
- Emergency: CPU >95%, Memory >95%, Disk >95%

**Allocation Alerts**
- Failed allocations >5% of attempts
- Component unresponsive to allocation changes
- Prediction confidence <0.3 for >10 minutes

### Health Checks

```python
# Component health check
health_status = await manager.get_health_summary()

# Resource monitor health
monitor_health = await resource_monitor.health_check()

# Controller status
controller_status = controller.get_allocation_status()
```

## Testing

### Unit Tests
```bash
uv run pytest tests/unit/test_resource_monitor.py
uv run pytest tests/unit/test_adaptive_resource_manager.py
uv run pytest tests/unit/test_resource_allocation_controller.py
```

### Integration Tests
```bash
uv run pytest tests/integration/test_adaptive_resource_management.py
```

### CLI Tests
```bash
# Basic functionality test
uv run -m src.interfaces.cli resource-management test

# Live system test (requires running services)
uv run -m src.interfaces.cli resource-management start --strategy adaptive --duration 10
```

## Troubleshooting

### Common Issues

**1. Resource Management Not Starting**
- Check if components are properly initialized
- Verify configuration file settings
- Ensure required dependencies are installed

**2. Allocation Adjustments Not Applied**
- Verify component registration with controller
- Check component allocation method implementations
- Review error logs for failed allocations

**3. High Resource Usage**
- Review allocation strategy (consider Conservative)
- Check for resource leaks in components
- Verify monitoring thresholds are appropriate

**4. Prediction Accuracy Low**
- Increase historical data collection period
- Check for volatile workload patterns
- Consider adjusting prediction time horizon

### Debug Mode

Enable debug logging for detailed troubleshooting:

```python
import logging
logging.getLogger("src.ml.monitoring").setLevel(logging.DEBUG)
```

### Performance Tuning

**For High-Performance Environments**
- Use Aggressive allocation strategy
- Reduce monitoring intervals (5-10 seconds)
- Increase resource buffers for critical components

**For Resource-Constrained Environments**
- Use Conservative allocation strategy
- Increase monitoring intervals (30-60 seconds)
- Implement strict resource limits

**For Development/Testing**
- Use Balanced strategy
- Enable verbose logging
- Use shorter adjustment cycles for faster feedback

## Future Enhancements

### Planned Features

1. **Machine Learning Predictions**
   - Replace linear trend analysis with ML models
   - Seasonal pattern recognition
   - Anomaly detection for unusual resource patterns

2. **Advanced Scheduling**
   - Time-based resource allocation
   - Workload scheduling optimization
   - Peak usage period management

3. **Multi-Node Coordination**
   - Distributed resource management
   - Cross-node resource sharing
   - Cluster-wide optimization

4. **Cost Optimization**
   - Cloud resource cost awareness
   - Cost-performance optimization
   - Budget-constrained allocation

### Integration Roadmap

1. **Phase 1**: Core system stabilization and testing
2. **Phase 2**: Advanced ML prediction models
3. **Phase 3**: Multi-node and cloud integration
4. **Phase 4**: Cost optimization and advanced scheduling

## Contributing

When extending the resource management system:

1. **Follow Existing Patterns**: Use the same integration patterns as existing components
2. **Implement Required Methods**: Components must implement allocation control methods
3. **Add Comprehensive Tests**: Include unit and integration tests
4. **Update Documentation**: Document new features and configuration options
5. **Monitor Performance**: Ensure additions don't significantly impact system performance

### Adding New Components

1. Import the allocation controller
2. Add allocation parameters to `__init__`
3. Implement allocation methods (`set_cpu_allocation`, etc.)
4. Register with controller during initialization
5. Add component to default quotas in adaptive manager
6. Add tests for the new integration

Example component integration:

```python
class NewMLComponent:
    def __init__(self):
        self.allocation_controller = None
        self.cpu_allocation = 15.0
        self.memory_allocation_mb = 300.0
        
    async def initialize(self):
        if get_resource_allocation_controller:
            await self._initialize_allocation_integration()
    
    async def _initialize_allocation_integration(self):
        self.allocation_controller = await get_resource_allocation_controller()
        
        allocation_controllers = {
            "cpu_percent": self.set_cpu_allocation,
            "memory_mb": self.set_memory_allocation,
        }
        
        await self.allocation_controller.register_component(
            "new_ml_component", self, allocation_controllers
        )
    
    async def set_cpu_allocation(self, cpu_percent: float) -> bool:
        self.cpu_allocation = max(5.0, min(50.0, cpu_percent))
        return True
    
    async def set_memory_allocation(self, memory_mb: float) -> bool:
        self.memory_allocation_mb = max(100.0, min(1024.0, memory_mb))
        return True
```

## Conclusion

The Intelligent Resource Management System provides a comprehensive, adaptive approach to resource allocation for the MLB Betting ML Pipeline. By combining real-time monitoring, predictive analytics, and intelligent allocation strategies, it ensures optimal performance while preventing resource exhaustion and maintaining system stability.

The system is designed to be:
- **Autonomous**: Minimal manual intervention required
- **Adaptive**: Responds to changing conditions automatically
- **Scalable**: Supports additional components and resources
- **Observable**: Comprehensive monitoring and alerting
- **Configurable**: Flexible strategies and thresholds

For questions or support, refer to the troubleshooting section or review the extensive test suite for usage examples.