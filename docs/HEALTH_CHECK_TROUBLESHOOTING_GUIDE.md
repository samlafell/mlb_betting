# Health Check Service Troubleshooting Guide

## Overview

This guide provides troubleshooting procedures for the enhanced Health Check Service with concurrent checks, circuit breakers, and improved error handling.

## Common Issues and Solutions

### 🚨 Critical Issues

#### Database Circuit Breaker Open
**Symptoms**: Health checks return "circuit breaker is OPEN" 
**Cause**: Multiple consecutive database failures (5+ by default)
**Solution**:
1. Check database connectivity: `PGPASSWORD=postgres psql -h localhost -p 5433 -U samlafell -d mlb_betting`
2. Verify database service is running: `docker ps | grep postgres`
3. Wait for circuit breaker timeout (5 minutes default) or restart service
4. Check logs for root cause: `tail -f logs/system.log | grep database`

#### Concurrent Health Check Failures
**Symptoms**: Multiple health checks fail simultaneously
**Cause**: Resource exhaustion or system overload
**Solution**:
1. Check system resources: `top`, `df -h`, `free -m`
2. Reduce concurrent load or increase timeout settings
3. Review health check configuration in `HealthCheckConfig`

### ⚠️ Performance Issues

#### Slow Database Health Checks (>1000ms)
**Symptoms**: Database marked as DEGRADED due to slow response
**Cause**: Database performance issues or network latency
**Investigation**:
```sql
-- Check active connections
SELECT count(*) FROM pg_stat_activity WHERE state = 'active';

-- Check slow queries
SELECT query, calls, mean_time, total_time 
FROM pg_stat_statements 
ORDER BY mean_time DESC LIMIT 10;

-- Check database size
SELECT pg_size_pretty(pg_database_size('mlb_betting'));
```
**Solution**:
1. Optimize slow queries or add indexes
2. Increase `slow_response_threshold_ms` in configuration if acceptable
3. Consider connection pooling optimizations

#### Cache Not Working
**Symptoms**: Health checks execute every time despite cache
**Cause**: Cache TTL too low or cache invalidation issues
**Solution**:
1. Increase `cache_ttl_seconds` in HealthCheckConfig
2. Verify timestamp comparison logic
3. Check for memory pressure causing cache eviction

### 🔧 Configuration Issues

#### Import Errors
**Symptoms**: "Collector registry import failed" or ImportError
**Cause**: Missing dependencies or circular imports
**Solution**:
1. Verify collector registry is properly initialized
2. Check for circular import dependencies
3. Use lazy imports where possible
4. Validate `PYTHONPATH` environment variable

#### Timeout Issues
**Symptoms**: Health checks timeout frequently
**Cause**: Insufficient timeout settings or slow external services
**Solution**:
1. Increase timeout values in HealthCheckConfig:
   ```python
   config = HealthCheckConfig(
       connection_timeout_seconds=10,
       query_timeout_seconds=15
   )
   ```
2. Check network connectivity to external services
3. Review database query performance

## Configuration Tuning

### Performance Tuning
```python
# High-performance configuration
config = HealthCheckConfig(
    cache_ttl_seconds=60,  # Longer cache
    connection_timeout_seconds=3,  # Faster timeout
    query_timeout_seconds=5,
    slow_response_threshold_ms=500,  # Stricter performance
    critical_response_threshold_ms=2000
)
```

### Resilient Configuration
```python
# High-resilience configuration
config = HealthCheckConfig(
    connection_timeout_seconds=15,  # Longer timeouts
    query_timeout_seconds=30,
    circuit_breaker_failure_threshold=10,  # More tolerant
    circuit_breaker_timeout_minutes=10,  # Longer recovery time
    slow_response_threshold_ms=2000,  # More lenient
    critical_response_threshold_ms=10000
)
```

## Health Check Commands

### Manual Health Check
```bash
# Quick health check
uv run -m src.interfaces.cli monitoring health-check

# Detailed health check with all services
uv run -m src.interfaces.cli monitoring health-check --detailed

# Check specific collector
uv run -m src.interfaces.cli monitoring health-check --collector action_network
```

### Debugging Commands
```bash
# Check database connectivity
PGPASSWORD=postgres psql -h localhost -p 5433 -U samlafell -d mlb_betting -c "SELECT 1;"

# Test collector registry
uv run python -c "from src.data.collection.registry import get_collector_instance; print(get_collector_instance('action_network'))"

# Check system resources
docker stats
df -h
free -m
```

## Circuit Breaker Management

### Monitoring Circuit Breaker State
Health check metadata includes circuit breaker state:
```json
{
  "metadata": {
    "circuit_breaker_state": "CLOSED|OPEN|HALF_OPEN",
    "error_type": "TimeoutError",
    "timeout_threshold": 5
  }
}
```

### Force Circuit Breaker Reset
If you need to manually reset a circuit breaker:
```python
# Access health service
health_service = HealthCheckService()

# Reset database circuit breaker
health_service._db_circuit_breaker.record_success()

# Reset collection circuit breaker  
health_service._collection_circuit_breaker.record_success()
```

## Performance Monitoring

### Expected Performance Metrics
- **Database Health Check**: <200ms typical, <1000ms acceptable
- **Collection Health Check**: <100ms typical, <500ms acceptable
- **Concurrent Execution**: 60-80% faster than sequential
- **Cache Hit Rate**: >80% for frequent health checks

### Performance Investigation
```bash
# Monitor health check performance
tail -f logs/system.log | grep "health_check" | grep "response_time_ms"

# Check concurrent vs sequential performance
uv run python tests/manual/benchmark_health_checks.py
```

## Recovery Procedures

### Database Recovery
1. **Verify Database Status**: Check if PostgreSQL is running
2. **Test Direct Connection**: Use psql to test connectivity
3. **Reset Circuit Breaker**: Wait for timeout or restart service
4. **Monitor Performance**: Check for slow queries or connection leaks

### Collection Service Recovery
1. **Verify Registry**: Test collector registry initialization
2. **Check Dependencies**: Ensure all required collectors are available
3. **Test Individual Collectors**: Test each collector separately
4. **Reset Circuit Breaker**: Allow recovery time or restart service

### System Recovery
1. **Resource Check**: Verify system resources (CPU, memory, disk)
2. **Service Restart**: Restart health check service if needed
3. **Configuration Review**: Verify health check configuration is appropriate
4. **Monitoring Setup**: Ensure proper logging and alerting is configured

## Emergency Contacts

- **Database Issues**: Check database logs and system resources
- **Performance Issues**: Review configuration thresholds and system load
- **Import Failures**: Verify Python environment and dependencies
- **Circuit Breaker Issues**: Wait for automatic recovery or manual reset

## Additional Resources

- [Production Security Guide](PRODUCTION_SECURITY_GUIDE.md)
- [Health Check API Documentation](../src/interfaces/api/monitoring_dashboard.py)
- [Circuit Breaker Implementation](../src/services/monitoring/health_check_service.py)