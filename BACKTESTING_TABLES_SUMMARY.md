# Backtesting Tables - Quick Reference

## Core Backtesting Tables (CONFIRMED ACTIVE)

### 1. `backtesting.strategy_performance` ✅ ACTIVE (66 rows)
**Purpose:** Main backtesting results and strategy performance metrics
- Stores ROI, win rates, strategy execution results
- Primary table for backtesting operations
- Referenced extensively in codebase for performance analysis

### 2. `backtesting.orchestrator_update_triggers` ✅ ACTIVE (44 rows)  
**Purpose:** Strategy orchestrator automation and update triggers
- Manages automated strategy updates
- Coordinates strategy lifecycle events

### 3. `backtesting.strategy_config_history` ✅ ACTIVE (6 rows)
**Purpose:** Historical tracking of strategy configuration changes
- Audit trail for strategy modifications
- Version control for strategy parameters

## Secondary Backtesting Tables (NEED ATTENTION)

### 4. `backtesting.strategy_alerts` ⚠️ EMPTY (0 rows)
**Purpose:** Strategy-based alerts and notifications
**Status:** Table exists but no data - may need population

### 5. `backtesting.strategy_config_cache` ⚠️ EMPTY (0 rows)  
**Purpose:** Cached strategy configurations for performance optimization
**Status:** Empty cache table - may be unused or needs initialization

### 6. `backtesting.strategy_lifecycle_events` ⚠️ EMPTY (0 rows)
**Purpose:** Strategy lifecycle tracking (creation, updates, deactivation)  
**Status:** No lifecycle events recorded - may need investigation

### 7. `backtesting.threshold_recommendations` ⚠️ EMPTY (0 rows)
**Purpose:** Threshold-based betting recommendations
**Status:** No threshold recommendations - feature may be inactive

### 8. `backtesting.alignment_analysis` ✅ CREATED (0 rows)
**Purpose:** Alignment analysis between backtesting and live results
**Status:** Table created successfully - ready for data population
**Action Needed:** None - table now exists and ready for use

## Related Tables in Other Schemas

### Tracking Schema (Live Backtesting Integration)
- `tracking.active_high_roi_strategies` (20 rows) - Active strategies from backtesting
- `tracking.strategy_integration_log` (138 rows) - Integration activity log  
- `tracking.strategy_performance_cache` (3 rows) - Cached performance data

### Missing Referenced Tables
- `backtesting.unified_bet_outcomes` - Referenced in cleanup operations
- `tracking.notification_log` - Referenced in cleanup operations

## Quick Action Items

1. **Create `backtesting.alignment_analysis` table** - Referenced in active code
2. **Investigate empty tables** - Determine why core tables like `strategy_alerts` are empty
3. **Remove dead references** - Clean up references to `unified_bet_outcomes` and `notification_log`
4. **Populate cache tables** - Initialize `strategy_config_cache` if needed

## Primary Data Flow
```
Raw Data → strategy_performance (main results) 
       → orchestrator_update_triggers (automation)
       → strategy_config_history (audit trail)
       → alignment_analysis (validation) [MISSING]
``` 