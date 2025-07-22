# Migration Scripts Optimization Plan
## Analysis Date: July 21, 2025

### Current Migration Performance Analysis

#### Phase 3 STAGING Migration Results
- **Success Rate:** 91.7% (21,448/23,391 records)
- **Failure Rate:** 8.3% (1,943 failed records)
- **Primary Failure Point:** Totals table processing
- **Bottleneck:** Database precision constraints and data validation

#### Phase 4 CURATED Migration Results
- **Success Rate:** 100% (32,431/32,431 records)
- **Performance:** Excellent with no failures
- **Processing Time:** ~7 minutes for full dataset
- **Strong Point:** ML feature engineering pipeline

---

## Critical Optimization Opportunities

### 1. Phase 3 STAGING Migration Enhancements

#### 1.1 Database Schema Optimizations
**Issue:** Numeric precision constraint (`numeric(3,2)`) limiting quality scores to 9.99 max

**Solution:**
```sql
-- Expand precision for quality scoring
ALTER TABLE staging.moneylines ALTER COLUMN data_quality_score TYPE numeric(5,2);
ALTER TABLE staging.spreads ALTER COLUMN data_quality_score TYPE numeric(5,2);
ALTER TABLE staging.totals ALTER COLUMN data_quality_score TYPE numeric(5,2);
ALTER TABLE staging.games ALTER COLUMN data_quality_score TYPE numeric(5,2);

-- Add performance indexes
CREATE INDEX CONCURRENTLY idx_staging_totals_game_id_quality 
ON staging.totals (game_id, data_quality_score) 
WHERE validation_status = 'validated';

CREATE INDEX CONCURRENTLY idx_staging_moneylines_sportsbook_date 
ON staging.moneylines (sportsbook_name, processed_at);
```

#### 1.2 Enhanced Error Recovery
**Current Issue:** 1,943 failed records in totals processing (21.6% failure rate for that table)

**Optimization Strategy:**
```python
class EnhancedStagingMigrator:
    def __init__(self, batch_size: int = 500):  # Smaller batches for better error isolation
        self.retry_config = {
            'max_retries': 3,
            'backoff_factor': 2.0,
            'retry_delays': [1, 2, 4]  # Progressive delays
        }
    
    async def _migrate_with_transaction_isolation(self, conn, records, table_name):
        """Migrate with per-record transaction isolation for better error recovery."""
        successful = 0
        failed_records = []
        
        for record in records:
            async with conn.transaction():
                try:
                    await self._insert_single_record(conn, record, table_name)
                    successful += 1
                except Exception as e:
                    failed_records.append({
                        'record_id': record.get('id'),
                        'error': str(e),
                        'retry_count': 0
                    })
                    
        # Retry failed records with exponential backoff
        for failed_record in failed_records:
            successful += await self._retry_failed_record(conn, failed_record, table_name)
        
        return successful, len(failed_records)
    
    async def _prevalidate_batch(self, records, bet_type):
        """Pre-validate records before database insertion."""
        validated_records = []
        validation_errors = []
        
        for record in records:
            try:
                # Enhanced validation
                if self._validate_record_schema(record, bet_type):
                    # Fix common data issues before insertion
                    cleaned_record = self._clean_record_data(record)
                    validated_records.append(cleaned_record)
                else:
                    validation_errors.append(record)
            except Exception as e:
                logger.error(f"Validation error for record {record.get('id')}: {e}")
                validation_errors.append(record)
        
        return validated_records, validation_errors
```

#### 1.3 Parallel Processing Implementation
**Current:** Sequential batch processing
**Target:** Parallel batch processing with connection pooling

```python
class ParallelStagingMigrator:
    async def _migrate_table_parallel(self, table_name, max_workers=4):
        """Process table migration with parallel workers."""
        # Get total record count
        total_records = await self._get_table_count(table_name)
        
        # Calculate batch ranges for parallel processing
        batch_ranges = self._calculate_batch_ranges(total_records, max_workers)
        
        # Create semaphore for connection pool management
        semaphore = asyncio.Semaphore(max_workers)
        
        async def process_batch_range(start_offset, end_offset):
            async with semaphore:
                connection_manager = get_connection()
                async with connection_manager.get_async_connection() as conn:
                    return await self._process_batch_range(conn, table_name, start_offset, end_offset)
        
        # Execute parallel processing
        tasks = [
            process_batch_range(start, end) 
            for start, end in batch_ranges
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return self._aggregate_results(results)
```

### 2. Phase 4 CURATED Migration Enhancements

#### 2.1 ML Feature Engineering Optimization
**Current State:** Comprehensive but could be more efficient

**Optimization:**
```python
class OptimizedMLFeatureEngineer:
    def __init__(self):
        # Cache frequently calculated values
        self._sportsbook_tier_cache = {}
        self._probability_cache = {}
    
    @lru_cache(maxsize=1000)
    def calculate_implied_probability(self, american_odds: int) -> float:
        """Cached probability calculation."""
        if american_odds == 0:
            return 0.0
        
        if american_odds > 0:
            return 100 / (american_odds + 100)
        else:
            return abs(american_odds) / (abs(american_odds) + 100)
    
    def create_vectorized_features(self, records: List[dict]) -> List[dict]:
        """Process multiple records with vectorized operations."""
        # Pre-calculate common values
        odds_values = [r.get('home_odds', 0) for r in records]
        probabilities = [self.calculate_implied_probability(odds) for odds in odds_values]
        
        features_list = []
        for i, record in enumerate(records):
            features = self._create_single_record_features(record, probabilities[i])
            features_list.append(features)
        
        return features_list
```

#### 2.2 Advanced Feature Vector Creation
**Enhancement:** Create more sophisticated ML features

```python
class AdvancedFeatureEngineer:
    def create_time_series_features(self, game_record, betting_records):
        """Create time-series features from betting line history."""
        return {
            'line_volatility': self._calculate_line_volatility(betting_records),
            'momentum_indicators': self._calculate_momentum(betting_records),
            'market_consensus': self._calculate_consensus_deviation(betting_records),
            'late_money_indicators': self._detect_late_money(betting_records)
        }
    
    def create_market_microstructure_features(self, betting_records):
        """Advanced market microstructure analysis."""
        return {
            'bid_ask_spreads': self._calculate_spreads(betting_records),
            'liquidity_measures': self._calculate_liquidity(betting_records),
            'order_flow_imbalance': self._calculate_flow_imbalance(betting_records),
            'price_impact_measures': self._calculate_price_impact(betting_records)
        }
```

### 3. Performance Monitoring & Optimization

#### 3.1 Real-Time Performance Tracking
```python
class MigrationPerformanceMonitor:
    def __init__(self):
        self.metrics = {
            'processing_speed': deque(maxlen=100),
            'error_rates': deque(maxlen=100),
            'memory_usage': deque(maxlen=100),
            'connection_pool_health': deque(maxlen=100)
        }
    
    async def track_migration_performance(self, migration_func):
        """Decorator for tracking migration performance."""
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss
        
        try:
            result = await migration_func()
            
            # Record successful metrics
            processing_time = time.time() - start_time
            memory_used = psutil.Process().memory_info().rss - start_memory
            
            self.metrics['processing_speed'].append(processing_time)
            self.metrics['memory_usage'].append(memory_used)
            
            return result
            
        except Exception as e:
            # Record error metrics
            self.metrics['error_rates'].append(1)
            raise e
```

#### 3.2 Adaptive Batch Sizing
```python
class AdaptiveBatchProcessor:
    def __init__(self):
        self.performance_history = []
        self.optimal_batch_size = 1000
    
    def calculate_optimal_batch_size(self):
        """Dynamically adjust batch size based on performance."""
        if len(self.performance_history) < 5:
            return self.optimal_batch_size
        
        recent_performance = self.performance_history[-5:]
        avg_processing_time = sum(p['time'] for p in recent_performance) / len(recent_performance)
        avg_error_rate = sum(p['errors'] for p in recent_performance) / len(recent_performance)
        
        if avg_error_rate > 0.05:  # High error rate
            self.optimal_batch_size = max(100, self.optimal_batch_size // 2)
        elif avg_processing_time < 5:  # Fast processing
            self.optimal_batch_size = min(2000, self.optimal_batch_size * 1.2)
        
        return int(self.optimal_batch_size)
```

---

## Implementation Priority

### Phase 1: Critical Fixes (Week 1)
1. **Database schema expansion** - Fix numeric precision constraints
2. **Enhanced error recovery** - Implement transaction isolation
3. **Validation improvements** - Pre-flight data checks

**Expected Impact:** STAGING migration success rate: 91.7% → 97%+

### Phase 2: Performance Optimization (Week 2)
1. **Parallel processing** - Implement concurrent batch processing
2. **Connection pool optimization** - Improve database connection management
3. **Adaptive batch sizing** - Dynamic batch size optimization

**Expected Impact:** Processing time reduction: 50-70%

### Phase 3: Advanced Features (Week 3-4)
1. **Enhanced ML features** - Advanced market microstructure analysis
2. **Real-time monitoring** - Performance tracking and alerting
3. **Caching optimization** - Reduce redundant calculations

**Expected Impact:** Feature quality improvement and processing efficiency

---

## Success Metrics

### Before Optimization
- Phase 3 Success Rate: 91.7%
- Processing Time: ~6 minutes (Phase 3) + ~7 minutes (Phase 4)
- Error Recovery: Limited retry logic
- Monitoring: Basic logging

### After Optimization (Targets)
- Phase 3 Success Rate: >97%
- Processing Time: ~2-3 minutes total (50-70% reduction)
- Error Recovery: Comprehensive retry with exponential backoff
- Monitoring: Real-time performance tracking with alerts

### Key Performance Indicators
1. **Migration Success Rate:** Target >97% for all phases
2. **Processing Speed:** <1 minute per 10K records
3. **Error Recovery:** <2% final failure rate after retries
4. **Memory Efficiency:** <1GB RAM usage for full dataset
5. **Database Impact:** <5% connection pool utilization during migration

---

## Risk Mitigation

### High Risk Items
- **Schema changes:** Blue-green deployment with rollback capability
- **Parallel processing:** Gradual rollout with connection pool monitoring
- **Transaction isolation:** Comprehensive testing with large datasets

### Testing Strategy
1. **Unit Tests:** Individual component testing with mock data
2. **Integration Tests:** Full pipeline testing with staging data
3. **Performance Tests:** Load testing with production-sized datasets
4. **Regression Tests:** Ensure optimization doesn't break existing functionality

This optimization plan will significantly improve migration reliability and performance while maintaining the excellent foundation already established in the system.