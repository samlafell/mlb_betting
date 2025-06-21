# DuckDB Optimization Migration Guide

## Overview

This guide explains how to migrate from the legacy file-locking approach to the new optimized DuckDB architecture that leverages native concurrency capabilities.

## 🔍 Architecture Comparison

### Before: File-Locking Approach
```python
# Sequential operations due to file locking
Read Query 1:    [Lock] -> Execute -> [Unlock]     = 50ms
Write Operation: [Lock] -> Execute -> [Unlock]     = 200ms  
Read Query 2:    [Lock] -> Execute -> [Unlock]     = 50ms
Total: 300ms for 3 operations
```

### After: Optimized Connection Pooling
```python
# Parallel reads + batched writes
Read Query 1:    Execute (pool[0])                 = 20ms
Read Query 2:    Execute (pool[1]) // parallel     = 20ms  
Write Batch:     Execute (writer)   // parallel    = 100ms
Total: 100ms for 3 operations (70% improvement)
```

## 🚀 Migration Steps

### Step 1: Enable Optimized Mode (Gradual Migration)

The current `DatabaseCoordinator` now supports both modes for safe migration:

```python
from mlb_sharp_betting.services.database_coordinator import get_database_coordinator

# Legacy mode (current behavior)
coordinator = get_database_coordinator()

# Optimized mode (new high-performance)
coordinator = DatabaseCoordinator(use_optimized=True)
```

### Step 2: Update Service Configurations

For different workloads, use different configurations:

```python
from mlb_sharp_betting.db.optimized_connection import ConnectionConfig

# High-frequency trading (5-minute window)
trading_config = ConnectionConfig(
    read_pool_size=12,      # More readers for analysis
    write_batch_size=100,   # Smaller batches for faster response
    write_batch_timeout=1.0 # Quick batching for real-time
)

# Historical analysis 
analysis_config = ConnectionConfig(
    read_pool_size=4,       # Fewer readers needed
    write_batch_size=2000,  # Larger batches for efficiency
    write_batch_timeout=10.0 # Allow more batching
)

# Production default (balanced)
default_config = ConnectionConfig(
    read_pool_size=8,
    write_batch_size=500,
    write_batch_timeout=2.0
)
```

### Step 3: Update Data Collection Services

Replace sequential writes with batch operations:

```python
# OLD: Sequential writes (slow)
async def collect_data_old():
    for source in ['VSIN', 'SBD', 'Pinnacle']:
        data = await scrape_source(source)
        for record in data:
            await coordinator.execute_write("INSERT INTO ...", record)

# NEW: Batch writes (fast)
async def collect_data_optimized():
    coordinator = DatabaseCoordinator(use_optimized=True)
    
    all_data = []
    for source in ['VSIN', 'SBD', 'Pinnacle']:
        data = await scrape_source(source)
        all_data.extend(data)
    
    # Single batch operation
    await coordinator.execute_bulk_insert(
        "INSERT INTO raw_mlb_betting_splits VALUES (?, ?, ?, ?)",
        all_data
    )
```

## 📊 Performance Optimizations

### Read Operations
```python
# Multiple parallel reads (no more file locking!)
coordinator = DatabaseCoordinator(use_optimized=True)

# These now run in parallel
results = await asyncio.gather(
    coordinator.execute_read("SELECT * FROM games WHERE date = ?", (today,)),
    coordinator.execute_read("SELECT * FROM betting_splits WHERE ..."),
    coordinator.execute_read("SELECT * FROM sharp_signals WHERE ...")
)
```

### Write Operations with Priority
```python
# Critical pre-game operations get priority
await coordinator.execute_write(
    "INSERT INTO pre_game_signals ...", 
    data,
    priority=OperationPriority.CRITICAL
)

# Regular data collection uses normal priority
await coordinator.execute_write(
    "INSERT INTO historical_data ...",
    data,
    priority=OperationPriority.NORMAL
)
```

## 🔧 Service-Specific Migrations

### 1. Pre-Game Workflow Service
```python
# src/mlb_sharp_betting/services/pre_game_workflow.py

class PreGameWorkflowService:
    def __init__(self):
        # Enable optimized mode for critical pre-game operations
        self.coordinator = DatabaseCoordinator(
            use_optimized=True,
            config=ConnectionConfig(
                read_pool_size=8,
                write_batch_size=200,  # Smaller batches for faster response
                write_batch_timeout=1.0
            )
        )
    
    async def process_pre_game_data(self):
        # Parallel data collection
        tasks = [
            self.collect_betting_splits(),
            self.collect_sharp_signals(),
            self.collect_line_movements()
        ]
        
        results = await asyncio.gather(*tasks)
        
        # Batch insert all data
        all_data = []
        for result in results:
            all_data.extend(result)
        
        return await self.coordinator.execute_bulk_insert(
            "INSERT INTO pre_game_data ...", 
            all_data
        )
```

### 2. Data Collector Service
```python
# src/mlb_sharp_betting/services/data_collector.py

class DataCollectorService:
    def __init__(self):
        # Optimized for bulk data operations
        self.coordinator = DatabaseCoordinator(
            use_optimized=True,
            config=ConnectionConfig(
                read_pool_size=4,
                write_batch_size=1000,   # Large batches for efficiency
                write_batch_timeout=5.0  # Allow batching accumulation
            )
        )
```

### 3. Sharp Monitor Service
```python
# src/mlb_sharp_betting/services/sharp_monitor.py

class SharpMonitorService:
    def __init__(self):
        # Real-time monitoring needs fast reads
        self.coordinator = DatabaseCoordinator(
            use_optimized=True,
            config=ConnectionConfig(
                read_pool_size=12,       # Many parallel reads
                write_batch_size=100,    # Small batches for alerts
                write_batch_timeout=0.5  # Very quick response
            )
        )
```

## 🎯 Expected Performance Improvements

### By Service:

| Service | Current Time | Optimized Time | Improvement |
|---------|-------------|----------------|------------|
| Pre-game Workflow | 105-220s | 30-60s | 70%+ |
| Data Collection | 45-90s | 15-30s | 67%+ |
| Sharp Monitoring | 10-15s | 3-5s | 70%+ |
| Daily Reports | 30-60s | 10-20s | 67%+ |

### By Operation Type:

| Operation | Before | After | Improvement |
|-----------|--------|-------|------------|
| Read Throughput | 1 concurrent | 8 concurrent | 800% |
| Write Throughput | Sequential | Batched | 300% |
| Read Latency | 50ms avg | 20ms avg | 60% |
| Write Latency | 200ms avg | 80ms avg | 60% |

## ⚠️ Migration Considerations

### Memory Usage
- **Additional Memory**: Expect 300-500MB more memory usage
- **Read Pool**: ~25MB per read connection (8 connections = 200MB)
- **Write Batching**: Temporary memory for queued operations

### Monitoring
```python
# Monitor performance during migration
coordinator = DatabaseCoordinator(use_optimized=True)

# Check system health
stats = coordinator.get_performance_stats()
print(f"Read pool size: {stats['read_pool_size']}")
print(f"Write queue size: {stats['write_queue_size']}")
print(f"Status: {stats['status']}")

# Monitor queue growth
if stats['write_queue_size'] > 5000:
    logger.warning("Write queue growing large, may need tuning")
```

### Rollback Plan
```python
# If issues arise, easily rollback to legacy mode
coordinator = DatabaseCoordinator(use_optimized=False)  # Legacy mode
```

## 🔄 Gradual Migration Strategy

### Phase 1: Enable for Non-Critical Services (Week 1)
- Data collection services
- Historical analysis
- Reporting services

### Phase 2: Enable for Real-Time Services (Week 2)
- Sharp monitoring (with careful observation)
- Line movement tracking

### Phase 3: Enable for Critical Services (Week 3)
- Pre-game workflow
- Live betting signals

### Phase 4: Full Migration (Week 4)
- Remove legacy mode entirely
- Optimize configurations based on observed performance

## 📈 Monitoring Dashboard

Create a simple monitoring script:

```python
# monitoring/db_performance_monitor.py

async def monitor_database_performance():
    coordinator = get_database_coordinator()
    
    while True:
        stats = coordinator.get_performance_stats()
        
        logger.info("Database Performance Stats", **stats)
        
        # Alert on issues
        if stats['write_queue_size'] > 8000:
            logger.warning("Write queue approaching capacity")
        
        if not coordinator.is_healthy():
            logger.error("Database health check failed")
        
        await asyncio.sleep(30)  # Check every 30 seconds
```

## 🎉 Expected Results

After full migration, you should see:

1. **Pre-game workflow completing in 30-60 seconds** (vs current 105-220 seconds)
2. **Parallel read operations** eliminating read bottlenecks
3. **Reduced database lock contention** to near zero
4. **Higher throughput** for data collection and analysis
5. **Better resource utilization** with connection pooling

The optimized architecture transforms your database layer from a bottleneck into a high-performance asset that can easily handle the 5-minute pre-game window and real-time betting operations.

---

**General Balls** 