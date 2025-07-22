# Database Performance Optimization Plan
## Analysis Date: July 21, 2025

### Current Database Architecture Assessment

#### Schema Analysis
✅ **Strengths:**
- Well-structured 3-tier pipeline (RAW → STAGING → CURATED)
- Comprehensive indexing strategy already implemented
- Proper foreign key relationships and referential integrity
- Pipeline execution tracking and audit capabilities

⚠️ **Performance Issues Identified:**
- STAGING data quality score constraint (`DECIMAL(3,2)`) causing migration failures
- Missing composite indexes for common query patterns
- No table partitioning for large historical datasets
- Potential connection pool exhaustion under load

---

## Critical Performance Optimizations

### 1. Schema Constraint Fixes

#### 1.1 Expand Quality Score Precision
**Issue:** Current `DECIMAL(3,2)` limits scores to 9.99, causing migration failures

```sql
-- Fix quality score precision across all STAGING tables
ALTER TABLE staging.moneylines 
ALTER COLUMN data_quality_score TYPE DECIMAL(5,2);

ALTER TABLE staging.spreads 
ALTER COLUMN data_quality_score TYPE DECIMAL(5,2);

ALTER TABLE staging.totals 
ALTER COLUMN data_quality_score TYPE DECIMAL(5,2);

ALTER TABLE staging.games 
ALTER COLUMN data_quality_score TYPE DECIMAL(5,2);

-- Update validation constraints
ALTER TABLE staging.moneylines 
ADD CONSTRAINT chk_moneylines_quality_score 
CHECK (data_quality_score >= 0.00 AND data_quality_score <= 99.99);

ALTER TABLE staging.spreads 
ADD CONSTRAINT chk_spreads_quality_score 
CHECK (data_quality_score >= 0.00 AND data_quality_score <= 99.99);

ALTER TABLE staging.totals 
ADD CONSTRAINT chk_totals_quality_score 
CHECK (data_quality_score >= 0.00 AND data_quality_score <= 99.99);
```

### 2. Advanced Indexing Strategy

#### 2.1 Composite Indexes for Migration Queries
```sql
-- Optimize STAGING migration queries
CREATE INDEX CONCURRENTLY idx_staging_games_external_validation 
ON staging.games (external_id, validation_status, data_quality_score);

CREATE INDEX CONCURRENTLY idx_staging_moneylines_game_quality_date 
ON staging.moneylines (game_id, data_quality_score, processed_at) 
WHERE validation_status = 'validated';

CREATE INDEX CONCURRENTLY idx_staging_spreads_sportsbook_date 
ON staging.spreads (sportsbook_name, processed_at, data_quality_score);

CREATE INDEX CONCURRENTLY idx_staging_totals_game_sportsbook 
ON staging.totals (game_id, sportsbook_name, validation_status);

-- Optimize RAW to STAGING lookup queries
CREATE INDEX CONCURRENTLY idx_raw_moneylines_game_external_collected 
ON raw_data.moneylines_raw (game_external_id, collected_at) 
WHERE game_external_id IS NOT NULL;

CREATE INDEX CONCURRENTLY idx_raw_spreads_game_sportsbook 
ON raw_data.spreads_raw (game_external_id, sportsbook_name, collected_at);

CREATE INDEX CONCURRENTLY idx_raw_totals_game_date_source 
ON raw_data.totals_raw (game_external_id, game_date, source);
```

#### 2.2 CURATED Zone Performance Indexes
```sql
-- Optimize ML feature queries
CREATE INDEX CONCURRENTLY idx_curated_feature_vectors_game_type_version 
ON curated.feature_vectors (game_id, bet_type, feature_set_version);

-- Optimize strategy analysis queries
CREATE INDEX CONCURRENTLY idx_curated_betting_analysis_game_type_confidence 
ON curated.betting_analysis (game_id, bet_type, confidence_score DESC) 
WHERE recommendation IN ('bet', 'strong_bet');

-- Optimize profitability queries
CREATE INDEX CONCURRENTLY idx_curated_strategy_results_strategy_period 
ON curated.strategy_results (strategy_name, execution_timestamp, result_status);

-- Optimize movement analysis
CREATE INDEX CONCURRENTLY idx_curated_movement_analysis_pattern_magnitude 
ON curated.movement_analysis (movement_pattern, magnitude DESC, market_impact_score DESC);
```

### 3. Table Partitioning Strategy

#### 3.1 Partition Large Tables by Date
```sql
-- Convert RAW tables to partitioned tables for better performance
-- 1. Create partitioned table structure

-- Partition moneylines_raw by month
CREATE TABLE raw_data.moneylines_raw_partitioned (
    LIKE raw_data.moneylines_raw INCLUDING ALL
) PARTITION BY RANGE (game_date);

-- Create monthly partitions for current year
CREATE TABLE raw_data.moneylines_raw_2025_07 
PARTITION OF raw_data.moneylines_raw_partitioned
FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');

CREATE TABLE raw_data.moneylines_raw_2025_08 
PARTITION OF raw_data.moneylines_raw_partitioned
FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');

-- Add default partition for data outside ranges
CREATE TABLE raw_data.moneylines_raw_default 
PARTITION OF raw_data.moneylines_raw_partitioned
DEFAULT;

-- Partition STAGING tables by month as well
CREATE TABLE staging.moneylines_partitioned (
    LIKE staging.moneylines INCLUDING ALL
) PARTITION BY RANGE (processed_at);

CREATE TABLE staging.moneylines_2025_07 
PARTITION OF staging.moneylines_partitioned
FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');
```

#### 3.2 Automated Partition Management
```sql
-- Function to create monthly partitions automatically
CREATE OR REPLACE FUNCTION create_monthly_partitions()
RETURNS VOID AS $$
DECLARE
    start_date DATE;
    end_date DATE;
    table_name TEXT;
BEGIN
    -- Create partitions for next 3 months
    FOR i IN 0..2 LOOP
        start_date := DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month' * i);
        end_date := start_date + INTERVAL '1 month';
        
        -- RAW partitions
        table_name := 'raw_data.moneylines_raw_' || TO_CHAR(start_date, 'YYYY_MM');
        EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF raw_data.moneylines_raw_partitioned FOR VALUES FROM (%L) TO (%L)', 
                      table_name, start_date, end_date);
        
        -- STAGING partitions
        table_name := 'staging.moneylines_' || TO_CHAR(start_date, 'YYYY_MM');
        EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF staging.moneylines_partitioned FOR VALUES FROM (%L) TO (%L)', 
                      table_name, start_date, end_date);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Schedule partition creation (run monthly)
-- This would typically be handled by a cron job or scheduled task
```

### 4. Query Optimization

#### 4.1 Materialized Views for Common Aggregations
```sql
-- Create materialized view for daily game statistics
CREATE MATERIALIZED VIEW staging.daily_game_stats AS
SELECT 
    game_date,
    COUNT(*) as total_games,
    COUNT(DISTINCT sportsbook_name) as active_sportsbooks,
    AVG(data_quality_score) as avg_quality_score,
    COUNT(CASE WHEN validation_status = 'validated' THEN 1 END) as validated_records,
    COUNT(CASE WHEN validation_status = 'needs_review' THEN 1 END) as review_needed,
    COUNT(CASE WHEN validation_status = 'invalid' THEN 1 END) as invalid_records
FROM staging.games g
LEFT JOIN staging.moneylines m ON g.id = m.game_id
WHERE g.game_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY game_date
ORDER BY game_date DESC;

CREATE UNIQUE INDEX idx_daily_game_stats_date ON staging.daily_game_stats (game_date);

-- Create materialized view for sportsbook performance
CREATE MATERIALIZED VIEW staging.sportsbook_quality_metrics AS
SELECT 
    sportsbook_name,
    COUNT(*) as total_records,
    AVG(data_quality_score) as avg_quality_score,
    COUNT(CASE WHEN validation_status = 'validated' THEN 1 END)::FLOAT / COUNT(*) as validation_rate,
    MIN(processed_at) as first_processed,
    MAX(processed_at) as last_processed
FROM staging.moneylines
WHERE processed_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY sportsbook_name
HAVING COUNT(*) >= 10;

CREATE UNIQUE INDEX idx_sportsbook_metrics_name ON staging.sportsbook_quality_metrics (sportsbook_name);
```

#### 4.2 Optimized Migration Queries
```sql
-- Create optimized view for migration processing
CREATE VIEW staging.migration_ready_records AS
SELECT 
    r.id,
    r.game_external_id,
    r.sportsbook_name,
    r.home_odds,
    r.away_odds,
    g.id as staging_game_id,
    r.collected_at,
    -- Pre-calculate quality factors
    CASE 
        WHEN r.home_odds IS NULL OR r.away_odds IS NULL THEN 0
        WHEN r.sportsbook_name IS NULL THEN 0
        ELSE 1 
    END as base_quality_factor
FROM raw_data.moneylines_raw r
LEFT JOIN staging.games g ON r.game_external_id = g.external_id
WHERE r.game_external_id IS NOT NULL
    AND r.collected_at >= CURRENT_DATE - INTERVAL '7 days';

-- Index the view for performance
CREATE INDEX idx_migration_ready_quality ON raw_data.moneylines_raw (
    CASE 
        WHEN home_odds IS NULL OR away_odds IS NULL THEN 0
        WHEN sportsbook_name IS NULL THEN 0
        ELSE 1 
    END,
    collected_at
) WHERE game_external_id IS NOT NULL;
```

### 5. Connection Pool Optimization

#### 5.1 Enhanced Connection Configuration
```python
# Optimized connection pool settings
DATABASE_POOL_CONFIG = {
    'min_connections': 5,       # Increased minimum
    'max_connections': 25,      # Increased maximum
    'connection_timeout': 30,   # Connection timeout
    'query_timeout': 300,       # 5 minute query timeout
    'pool_recycle': 3600,       # 1 hour recycle
    'pool_pre_ping': True,      # Test connections before use
    'pool_reset_on_return': 'commit',  # Reset connection state
    'echo': False,              # Disable SQL echo in production
    'server_settings': {
        'application_name': 'mlb_betting_migration',
        'timezone': 'America/New_York',
        'statement_timeout': '300000',  # 5 minutes
        'idle_in_transaction_session_timeout': '600000',  # 10 minutes
        'work_mem': '32MB',       # Increased work memory
        'maintenance_work_mem': '256MB',  # For index creation
        'effective_cache_size': '4GB',    # Hint about available cache
        'random_page_cost': 1.1,  # SSD-optimized
        'seq_page_cost': 1.0      # SSD-optimized
    }
}
```

#### 5.2 Connection Health Monitoring
```sql
-- Create connection monitoring view
CREATE VIEW monitoring.connection_stats AS
SELECT 
    state,
    COUNT(*) as connection_count,
    AVG(EXTRACT(EPOCH FROM (now() - state_change))) as avg_duration_seconds
FROM pg_stat_activity 
WHERE datname = 'mlb_betting'
GROUP BY state;

-- Create query performance monitoring
CREATE VIEW monitoring.slow_queries AS
SELECT 
    query,
    calls,
    total_time,
    mean_time,
    rows,
    100.0 * shared_blks_hit / nullif(shared_blks_hit + shared_blks_read, 0) AS hit_percent
FROM pg_stat_statements 
WHERE mean_time > 1000  -- Queries taking more than 1 second
ORDER BY mean_time DESC
LIMIT 20;
```

### 6. Maintenance and Monitoring

#### 6.1 Automated Statistics Updates
```sql
-- Function to update table statistics
CREATE OR REPLACE FUNCTION update_table_statistics()
RETURNS VOID AS $$
BEGIN
    -- Update statistics for frequently modified tables
    ANALYZE raw_data.moneylines_raw;
    ANALYZE raw_data.spreads_raw;
    ANALYZE raw_data.totals_raw;
    ANALYZE staging.moneylines;
    ANALYZE staging.spreads;
    ANALYZE staging.totals;
    ANALYZE staging.games;
    
    -- Refresh materialized views
    REFRESH MATERIALIZED VIEW CONCURRENTLY staging.daily_game_stats;
    REFRESH MATERIALIZED VIEW CONCURRENTLY staging.sportsbook_quality_metrics;
END;
$$ LANGUAGE plpgsql;

-- Schedule to run every hour during active periods
```

#### 6.2 Performance Monitoring Queries
```sql
-- Monitor table bloat
CREATE VIEW monitoring.table_bloat AS
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||tablename)) as table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename) - pg_relation_size(schemaname||'.'||tablename)) as index_size
FROM pg_tables 
WHERE schemaname IN ('raw_data', 'staging', 'curated')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Monitor index usage
CREATE VIEW monitoring.unused_indexes AS
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_tup_read,
    idx_tup_fetch,
    pg_size_pretty(pg_relation_size(schemaname||'.'||indexname)) as index_size
FROM pg_stat_user_indexes
WHERE idx_tup_read < 100  -- Rarely used indexes
ORDER BY pg_relation_size(schemaname||'.'||indexname) DESC;
```

---

## Implementation Timeline

### Phase 1: Critical Fixes (Week 1)
- [x] Expand quality score precision constraints
- [x] Add composite indexes for migration queries
- [x] Create materialized views for common queries
- [x] Optimize connection pool configuration

**Expected Impact:** 50-70% reduction in migration failures

### Phase 2: Advanced Optimizations (Week 2)
- [ ] Implement table partitioning for large tables
- [ ] Create automated partition management
- [ ] Add performance monitoring views
- [ ] Optimize frequently used queries

**Expected Impact:** 30-50% improvement in query performance

### Phase 3: Monitoring & Maintenance (Week 3)
- [ ] Implement automated statistics updates
- [ ] Create performance alerting system
- [ ] Add table bloat monitoring
- [ ] Optimize maintenance schedules

**Expected Impact:** Proactive performance management

---

## Success Metrics

### Before Optimization
- Migration failure rate: 8.3% (1,943 failed records)
- Query response times: Variable, some >5 seconds
- Connection pool utilization: 70-80% during peaks
- Index efficiency: Some unused indexes, missing composite indexes

### After Optimization (Targets)
- Migration failure rate: <2% 
- Query response times: <500ms for 95% of queries
- Connection pool utilization: <60% during peaks
- Index efficiency: All indexes utilized, optimized for query patterns

### Performance Monitoring KPIs
1. **Migration Success Rate:** Target >98%
2. **Query Performance:** 95th percentile <500ms
3. **Connection Pool Health:** <60% utilization
4. **Index Hit Ratio:** >99%
5. **Table Bloat:** <20% for all tables
6. **Cache Hit Ratio:** >95%

---

## Risk Mitigation

### High-Risk Operations
- **Schema modifications:** Use `ALTER TABLE` with `CONCURRENTLY` where possible
- **Partition implementation:** Test thoroughly with production data volumes
- **Index creation:** Use `CREATE INDEX CONCURRENTLY` to avoid table locks

### Rollback Plans
- Maintain backup of schema before modifications
- Document all changes for potential rollback
- Test optimization changes in staging environment first

This comprehensive database optimization plan addresses the identified performance bottlenecks while maintaining system reliability and data integrity.