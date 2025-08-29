# Database Performance Optimization Deployment Guide

## Overview

This guide provides step-by-step instructions for deploying database performance optimizations to achieve production-scale performance targets:

- **Query Response Time**: <100ms for common betting analysis queries
- **Data Ingestion**: Handle 1000+ records/second without performance degradation
- **Concurrent Users**: Support 50+ concurrent analytical queries
- **Storage Efficiency**: 20%+ reduction in storage footprint
- **Index Efficiency**: 90%+ index usage rate for optimized queries

## Performance Optimization Phases

### Phase 1: Strategic Indexing ✅ COMPLETED
- **Purpose**: Add strategic indexes for high-traffic query patterns
- **Impact**: 40-70% improvement in query performance
- **Deployment Time**: 15-30 minutes
- **Risk Level**: LOW (uses CONCURRENTLY, no blocking)

### Phase 2: Table Partitioning ✅ COMPLETED
- **Purpose**: Implement time-based and hash partitioning
- **Impact**: 50-80% improvement for time-range queries
- **Deployment Time**: 30-60 minutes
- **Risk Level**: MEDIUM (new table structures)

### Phase 3: Data Type Optimization (Optional)
- **Purpose**: Optimize column data types for storage efficiency
- **Impact**: 15-25% storage reduction
- **Deployment Time**: 45-90 minutes
- **Risk Level**: HIGH (requires data migration)

## Pre-Deployment Checklist

### System Requirements Validation
```bash
# Verify PostgreSQL version
psql -h localhost -p 5433 -U samlafell -d mlb_betting -c "SELECT version();"

# Check available disk space (need ~30% additional for indexes)
df -h

# Verify current database size
psql -h localhost -p 5433 -U samlafell -d mlb_betting -c "SELECT pg_size_pretty(pg_database_size('mlb_betting'));"
```

### Backup Verification
```bash
# Create full database backup before optimization
pg_dump -h localhost -p 5433 -U samlafell -d mlb_betting > mlb_betting_backup_$(date +%Y%m%d).sql

# Verify backup integrity
pg_restore --list mlb_betting_backup_$(date +%Y%m%d).sql | head -20
```

### Performance Baseline Collection
```bash
# Run baseline performance tests
psql -h localhost -p 5433 -U samlafell -d mlb_betting -f sql/performance_testing.sql
```

## Phase 1 Deployment: Strategic Indexing

### Step 1: Deploy Phase 1 Optimizations

```bash
# Deploy strategic indexing (15-30 minutes)
PGPASSWORD=postgres psql -h localhost -p 5433 -U samlafell -d mlb_betting -f sql/migrations/200_performance_optimization_phase1.sql
```

### Step 2: Validate Phase 1 Results

```bash
# Check deployment status
PGPASSWORD=postgres psql -h localhost -p 5433 -U samlafell -d mlb_betting -c "SELECT * FROM performance_monitoring.phase1_summary;"

# Verify indexes were created
PGPASSWORD=postgres psql -h localhost -p 5433 -U samlafell -d mlb_betting -c "
SELECT COUNT(*) as new_indexes_created 
FROM pg_indexes 
WHERE indexname LIKE '%composite_query%' 
   OR indexname LIKE '%unprocessed%' 
   OR indexname LIKE '%analysis%';"
```

### Step 3: Performance Validation

```bash
# Run performance tests to measure improvement
PGPASSWORD=postgres psql -h localhost -p 5433 -U samlafell -d mlb_betting -f sql/performance_testing.sql

# Check performance targets
PGPASSWORD=postgres psql -h localhost -p 5433 -U samlafell -d mlb_betting -c "SELECT * FROM performance_monitoring.performance_test_summary;"
```

## Phase 2 Deployment: Table Partitioning

### Step 1: Deploy Phase 2 Optimizations

```bash
# Deploy table partitioning (30-60 minutes)
PGPASSWORD=postgres psql -h localhost -p 5433 -U samlafell -d mlb_betting -f sql/migrations/201_performance_optimization_phase2.sql
```

### Step 2: Validate Partitioning

```bash
# Check partition creation status
PGPASSWORD=postgres psql -h localhost -p 5433 -U samlafell -d mlb_betting -c "SELECT * FROM performance_monitoring.phase2_summary;"

# Verify partitioned tables
PGPASSWORD=postgres psql -h localhost -p 5433 -U samlafell -d mlb_betting -c "
SELECT schemaname, tablename 
FROM pg_tables 
WHERE tablename LIKE '%_partitioned' OR tablename LIKE '%_2024_%' 
ORDER BY schemaname, tablename;"
```

### Step 3: Test Partition Pruning

```bash
# Test partition pruning effectiveness
PGPASSWORD=postgres psql -h localhost -p 5433 -U samlafell -d mlb_betting -c "
EXPLAIN (ANALYZE, BUFFERS) 
SELECT COUNT(*) 
FROM raw_data.action_network_odds_partitioned 
WHERE collected_at >= '2024-12-01' AND collected_at < '2024-12-31';"
```

## Performance Monitoring Setup

### Enable Real-Time Monitoring

```sql
-- Enable pg_stat_statements for ongoing query monitoring
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Create monitoring dashboard function
CREATE OR REPLACE FUNCTION performance_monitoring.get_slow_queries(
    min_duration_ms NUMERIC DEFAULT 100
) RETURNS TABLE(
    query TEXT,
    calls BIGINT,
    total_time NUMERIC,
    mean_time NUMERIC,
    rows BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        query,
        calls,
        total_time,
        mean_time,
        rows
    FROM pg_stat_statements 
    WHERE mean_time > min_duration_ms
    ORDER BY mean_time DESC
    LIMIT 20;
END;
$$ LANGUAGE plpgsql;
```

### Automated Maintenance Setup

```sql
-- Schedule automated maintenance
CREATE OR REPLACE FUNCTION performance_monitoring.schedule_maintenance()
RETURNS void AS $$
BEGIN
    -- Create new partitions monthly
    PERFORM performance_monitoring.create_monthly_partitions('raw_data', 'action_network_odds', 3);
    
    -- Drop old partitions (retain 12 months)
    PERFORM performance_monitoring.drop_old_partitions('raw_data', 'action_network_odds', 12);
    
    -- Refresh materialized views
    REFRESH MATERIALIZED VIEW CONCURRENTLY performance_monitoring.recent_betting_activity;
    
    -- Update table statistics
    ANALYZE;
    
    -- Log maintenance completion
    INSERT INTO performance_monitoring.optimization_log 
        (phase, operation, object_name, status)
    VALUES ('maintenance', 'automated_maintenance', 'all_tables', 'completed');
END;
$$ LANGUAGE plpgsql;
```

## Production Deployment Strategy

### Blue-Green Deployment Approach

1. **Blue Environment (Current)**
   - Keep existing tables operational
   - Monitor performance during optimization

2. **Green Environment (Optimized)**
   - Deploy optimized tables alongside existing ones
   - Test thoroughly before switching

3. **Gradual Migration**
   ```sql
   -- Example: Gradually migrate queries to partitioned tables
   -- Week 1: Test with 10% of queries
   -- Week 2: Migrate 50% of queries  
   -- Week 3: Migrate 100% of queries
   -- Week 4: Remove original tables
   ```

### Application Integration

```python
# Example: Update application to use optimized queries
# Before optimization
query = "SELECT * FROM raw_data.action_network_odds WHERE collected_at >= %s"

# After optimization (uses indexes effectively)
query = """
SELECT external_game_id, sportsbook_key, raw_odds, collected_at 
FROM raw_data.action_network_odds 
WHERE collected_at >= %s 
AND processed_at IS NOT NULL
ORDER BY collected_at DESC
"""
```

## Rollback Procedures

### Phase 1 Rollback (if performance degrades)

```bash
# Rollback strategic indexing
PGPASSWORD=postgres psql -h localhost -p 5433 -U samlafell -d mlb_betting -f sql/migrations/200_rollback_performance_optimization_phase1.sql

# Verify rollback
PGPASSWORD=postgres psql -h localhost -p 5433 -U samlafell -d mlb_betting -c "SELECT * FROM performance_monitoring.phase1_rollback_summary;"
```

### Phase 2 Rollback (if needed)

```bash
# Rollback table partitioning (more complex - requires data migration)
PGPASSWORD=postgres psql -h localhost -p 5433 -U samlafell -d mlb_betting -f sql/migrations/201_rollback_performance_optimization_phase2.sql
```

## Performance Targets Validation

### Target Metrics

| Operation Type | Target Time | Measurement Method |
|----------------|-------------|-------------------|
| Common betting queries | <100ms | Average of 5 runs |
| Raw data ingestion | <50ms per batch | Single batch insert |
| ML feature extraction | <200ms | Complex analytics query |
| Join operations | <150ms | Multi-table joins |
| Concurrent load (50 users) | <5000ms total | Simulated concurrent queries |

### Validation Commands

```bash
# Run comprehensive performance validation
PGPASSWORD=postgres psql -h localhost -p 5433 -U samlafell -d mlb_betting -f sql/performance_testing.sql

# Check if all targets are met
PGPASSWORD=postgres psql -h localhost -p 5433 -U samlafell -d mlb_betting -c "
SELECT 
    COUNT(*) as total_tests,
    COUNT(*) FILTER (WHERE performance_status = 'PASS') as passing_tests,
    ROUND(COUNT(*) FILTER (WHERE performance_status = 'PASS')::NUMERIC / COUNT(*) * 100, 1) as pass_rate
FROM performance_monitoring.performance_test_summary;
"
```

## Ongoing Maintenance

### Daily Monitoring

```bash
# Check slow queries daily
PGPASSWORD=postgres psql -h localhost -p 5433 -U samlafell -d mlb_betting -c "SELECT * FROM performance_monitoring.get_slow_queries(100);"

# Monitor index usage
PGPASSWORD=postgres psql -h localhost -p 5433 -U samlafell -d mlb_betting -c "
SELECT schemaname, tablename, indexname, idx_scan, idx_tup_read, idx_tup_fetch
FROM pg_stat_user_indexes 
WHERE idx_scan = 0 AND schemaname IN ('raw_data', 'staging', 'curated')
ORDER BY schemaname, tablename;
"
```

### Weekly Maintenance

```bash
# Run automated maintenance weekly
PGPASSWORD=postgres psql -h localhost -p 5433 -U samlafell -d mlb_betting -c "SELECT performance_monitoring.schedule_maintenance();"

# Update table statistics
PGPASSWORD=postgres psql -h localhost -p 5433 -U samlafell -d mlb_betting -c "ANALYZE;"
```

### Monthly Reviews

1. **Performance Trend Analysis**
   - Review query performance trends
   - Identify new slow queries
   - Plan additional optimizations

2. **Storage Management**
   - Monitor partition sizes
   - Plan partition maintenance
   - Review index effectiveness

3. **Capacity Planning**
   - Project storage growth
   - Plan for increased query load
   - Validate performance under load

## Troubleshooting Guide

### Common Issues

#### Issue 1: Index Creation Taking Too Long
```bash
# Check index creation progress
SELECT 
    pid, 
    now() - query_start as duration, 
    query 
FROM pg_stat_activity 
WHERE query LIKE 'CREATE INDEX%';

# Kill long-running index creation if needed
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE query LIKE 'CREATE INDEX%';
```

#### Issue 2: Performance Regression After Optimization
```bash
# Compare performance before/after
SELECT 
    test_name,
    execution_time_ms,
    optimization_type,
    created_at
FROM performance_monitoring.optimization_log 
WHERE test_name = 'specific_test_name'
ORDER BY created_at;

# Consider rollback if consistent regression
# Run rollback script if needed
```

#### Issue 3: Partition Pruning Not Working
```bash
# Check partition constraint exclusion
SHOW constraint_exclusion;

# Verify partition constraints
SELECT 
    schemaname, 
    tablename, 
    attname, 
    consrc 
FROM pg_constraint c
JOIN pg_class t ON c.conrelid = t.oid
JOIN pg_namespace n ON t.relnamespace = n.oid
WHERE n.nspname = 'raw_data' AND t.relname LIKE '%2024%';
```

## Success Criteria

### Technical Metrics
- [ ] 90%+ of performance tests passing
- [ ] <100ms average query response time
- [ ] 50+ concurrent users supported
- [ ] 20%+ storage efficiency improvement
- [ ] Zero production downtime during deployment

### Business Metrics
- [ ] Improved user experience (faster dashboards)
- [ ] Increased system capacity for growth
- [ ] Reduced infrastructure costs
- [ ] Enhanced data pipeline throughput
- [ ] Better ML model training performance

## Contact and Support

For deployment issues or questions:

1. **Review Logs**: Check `performance_monitoring.optimization_log` table
2. **Performance Issues**: Run performance testing suite
3. **Rollback Needed**: Use provided rollback scripts
4. **Emergency**: Contact system administrator immediately

## Conclusion

This performance optimization provides a solid foundation for production-scale MLB betting system operations. Regular monitoring and maintenance will ensure continued optimal performance as the system grows.