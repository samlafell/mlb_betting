# MLB Sharp Betting System - Database Tables Documentation

## Overview
This document provides a comprehensive listing of all database tables used in the MLB Sharp Betting System, including their purposes, current row counts, and status.

**Last Updated:** 2025-06-30
**Database:** mlb_betting (PostgreSQL 17)

## Schema Organization

### 1. BACKTESTING Schema
Core backtesting functionality for strategy performance analysis.

| Table Name | Rows | Purpose | Status |
|------------|------|---------|--------|
| `backtesting.strategy_performance` | 66 | **Core backtesting performance data** - Stores strategy execution results, ROI, win rates, and performance metrics | ✅ ACTIVE |
| `backtesting.orchestrator_update_triggers` | 44 | Strategy orchestrator update triggers and automation | ✅ ACTIVE |
| `backtesting.strategy_config_history` | 6 | Historical record of strategy configuration changes | ✅ ACTIVE |
| `backtesting.strategy_alerts` | 0 | Strategy-based alerts and notifications | ⚠️ EMPTY |
| `backtesting.strategy_config_cache` | 0 | Cached strategy configurations for performance | ⚠️ EMPTY |
| `backtesting.strategy_lifecycle_events` | 0 | Strategy lifecycle tracking (creation, updates, deactivation) | ⚠️ EMPTY |
| `backtesting.threshold_recommendations` | 0 | Threshold-based betting recommendations | ⚠️ EMPTY |

| `backtesting.alignment_analysis` | 0 | Alignment analysis between backtesting and live results | ✅ CREATED |

**Missing Tables Referenced in Code:**
- `backtesting.unified_bet_outcomes` - Referenced in cleanup operations but table doesn't exist

### 2. SPLITS Schema
MLB betting splits and game data from external sources.

| Table Name | Rows | Purpose | Status |
|------------|------|---------|--------|
| `splits.raw_mlb_betting_splits` | 36,116 | **Primary betting data** - Raw MLB betting splits data from external sources | ✅ ACTIVE |
| `splits.games` | 224 | Game information and metadata | ✅ ACTIVE |
| `splits.game_outcomes` | 0 | Game outcome results | ⚠️ EMPTY |
| `splits.sharp_actions` | 0 | Sharp betting action indicators | ⚠️ EMPTY |

### 3. TRACKING Schema
Live recommendation tracking and strategy management.

| Table Name | Rows | Purpose | Status |
|------------|------|---------|--------|
| `tracking.strategy_integration_log` | 138 | Strategy integration activity log | ✅ ACTIVE |
| `tracking.active_high_roi_strategies` | 20 | Currently active high-ROI strategies | ✅ ACTIVE |
| `tracking.active_strategy_configs` | 20 | Active strategy configurations | ✅ ACTIVE |
| `tracking.pre_game_recommendations` | 3 | Pre-game betting recommendations | ✅ ACTIVE |
| `tracking.strategy_performance_cache` | 3 | Cached strategy performance data | ✅ ACTIVE |

**Missing Tables Referenced in Code:**
- `tracking.notification_log` - Referenced in cleanup operations but table doesn't exist

### 4. TIMING_ANALYSIS Schema
Timing analysis for optimal bet placement.

| Table Name | Rows | Purpose | Status |
|------------|------|---------|--------|
| `timing_analysis.comprehensive_analyses` | 11 | Comprehensive timing analysis results | ✅ ACTIVE |
| `timing_analysis.recommendation_history` | 9 | Historical timing recommendations | ✅ ACTIVE |
| `timing_analysis.timing_bucket_performance` | 3 | Performance by timing buckets | ✅ ACTIVE |
| `timing_analysis.timing_recommendations_cache` | 1 | Cached timing recommendations | ✅ ACTIVE |

### 5. PUBLIC Schema
Development, testing, and legacy tables.

| Table Name | Rows | Purpose | Status |
|------------|------|---------|--------|
| `public.game_outcomes` | 175 | Game outcome data (legacy/development) | ✅ ACTIVE |
| `public.benchmark_performance` | 200 | Database performance benchmarking | 🧪 TEST |
| `public.benchmark_test` | 500 | Benchmark testing data | 🧪 TEST |
| `public.performance_test` | 50 | Performance testing data | 🧪 TEST |
| `public.test_parallel` | 100 | Parallel processing tests | 🧪 TEST |
| `public.test_table` | 1 | General testing table | 🧪 TEST |
| `public.games` | 0 | Games table (legacy/unused) | ⚠️ EMPTY |
| `public.concurrency_test` | 0 | Concurrency testing | 🧪 TEST - EMPTY |

## Status Legend
- ✅ **ACTIVE** - Table is actively used and has data
- ⚠️ **EMPTY** - Table exists but has no data (may need investigation)
- 🧪 **TEST** - Development/testing table
- ❌ **MISSING** - Referenced in code but doesn't exist in database

## Key Findings

### Tables for Backtesting (Primary Focus)
The core backtesting tables are:
1. **`backtesting.strategy_performance`** (66 rows) - Main backtesting results
2. **`backtesting.orchestrator_update_triggers`** (44 rows) - Strategy automation
3. **`backtesting.strategy_config_history`** (6 rows) - Configuration tracking
4. **`tracking.pre_game_recommendations`** (3 rows) - Pre-game betting recommendations which we should be using to tracks wins and losses of our recommendations - This is on a game-by-game level giving the strategy used and the outcome. Strategy Performance is higher level.

### Empty Tables Requiring Investigation
1. **`backtesting.strategy_alerts`** - Should contain strategy alerts
2. **`backtesting.strategy_config_cache`** - Performance optimization table
3. **`backtesting.strategy_lifecycle_events`** - Strategy lifecycle tracking
4. **`backtesting.threshold_recommendations`** - Threshold-based recommendations

### To Delete
1. **`splits.game_outcomes`** - Game results data
2. **`splits.sharp_actions`** - Sharp betting indicators


### Missing Tables Referenced in Code
The following tables are referenced in cleanup operations but don't exist:
- `backtesting.alignment_analysis`
- `backtesting.unified_bet_outcomes` 
- `tracking.notification_log`

These may need to be created or the references removed from the codebase.

### Data Volume Analysis
- **Largest table:** `splits.raw_mlb_betting_splits` (36,116 rows) - Primary data source
- **Most active schema:** `backtesting` - Core functionality
- **Test data:** Significant test data in `public` schema should be cleaned up periodically

## Recommendations

1. **Create Missing Tables:** Add the missing tables referenced in cleanup operations or remove the references
2. **Investigate Empty Tables:** Determine why key tables like `strategy_alerts` and `game_outcomes` are empty
3. **Clean Test Data:** Consider removing or archiving test tables in `public` schema
4. **Monitor Growth:** Set up monitoring for table growth, especially `raw_mlb_betting_splits`
5. **Documentation Updates:** Keep this document updated as schema evolves

## Schema Evolution Notes
- System migrated from DuckDB to PostgreSQL 17
- Some table references may be legacy from DuckDB era
- Phase 1 refactoring introduced centralized services that may affect table usage patterns 