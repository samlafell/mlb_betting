# MLB Sharp Betting System - Database Tables Documentation

## Overview
This document provides a comprehensive listing of all database tables used in the MLB Sharp Betting System, including their purposes, current row counts, and active usage status.

**Last Updated:** 2025-06-30 (Post Phase 3 & 4 Implementation)
**Database:** mlb_betting (PostgreSQL 17)
**Total Tables:** 29 across 6 schemas (after cleanup of 2 unused tables)

## Schema Organization

### 1. BACKTESTING Schema
Core backtesting functionality for strategy performance analysis and orchestration.

| Table Name | Rows | Purpose | Status |
|------------|------|---------|--------|
| **`backtesting.strategy_performance`** | **95** | **⭐ PRIMARY BACKTESTING TABLE** - Stores strategy execution results, ROI, win rates, and performance metrics. Actively written to by BacktestingEngine | ✅ **HIGHLY ACTIVE** |
| `backtesting.orchestrator_update_triggers` | 64 | Strategy orchestrator update triggers and automation | ✅ ACTIVE |
| `backtesting.strategy_config_history` | 26 | Historical record of strategy configuration changes and threshold adjustments | ✅ ACTIVE |
| `backtesting.active_strategies` | 46 | Currently active strategies for backtesting execution | ✅ ACTIVE |
| `backtesting.strategy_performance_summary` | 75 | Aggregated performance metrics by strategy (view-backed table) | ✅ ACTIVE |
| `backtesting.recent_config_changes` | 20 | Recent strategy configuration changes tracking | ✅ ACTIVE |
| `backtesting.alignment_analysis` | 0 | Live vs backtest alignment analysis results | 📊 **NEWLY CREATED** - Not yet populated |
| `backtesting.strategy_alerts` | 0 | Strategy-based alerts and notifications | ⚠️ **IMPLEMENTED BUT EMPTY** |
| `backtesting.strategy_config_cache` | 0 | Cached strategy configurations for performance optimization | ⚠️ **PLANNED - NOT USED** |
| `backtesting.strategy_lifecycle_events` | 0 | Strategy lifecycle tracking (creation, updates, deactivation) | ⚠️ **PLANNED - NOT USED** |
| `backtesting.threshold_recommendations` | 0 | Threshold-based betting recommendations | ⚠️ **PLANNED - NOT USED** |

### 2. SPLITS Schema
MLB betting splits and game data from external sources (primary data ingestion).

| Table Name | Rows | Purpose | Status |
|------------|------|---------|--------|
| **`splits.raw_mlb_betting_splits`** | **37,827** | **⭐ PRIMARY DATA SOURCE** - Raw MLB betting splits data from VSIN, SBD, and other sources. Core table for all analysis | ✅ **MISSION CRITICAL** |
| `splits.games` | 227 | Game information, schedules, and metadata | ✅ ACTIVE |
| `splits.game_outcomes` | 0 | Game outcome results (duplicate of public.game_outcomes) | ❌ **DEPRECATED** - Use public.game_outcomes instead |
| `splits.sharp_actions` | 0 | Sharp betting action indicators (legacy) | ❌ **DEPRECATED** - Logic moved to processors |

### 3. TRACKING Schema
Live recommendation tracking, strategy management, and performance monitoring.

| Table Name | Rows | Purpose | Status |
|------------|------|---------|--------|
| **`tracking.pre_game_recommendations`** | **5** | **⭐ RECOMMENDATION TRACKING** - Game-by-game betting recommendations with win/loss outcomes for ROI validation | ✅ **ACTIVELY TRACKED** |
| `tracking.active_high_roi_strategies` | 20 | Currently active high-ROI strategies for live betting | ✅ ACTIVE |
| `tracking.active_strategy_configs` | 20 | Live strategy configurations currently in use | ✅ ACTIVE |

### 4. TIMING_ANALYSIS Schema
Timing analysis for optimal bet placement and market movement tracking.

| Table Name | Rows | Purpose | Status |
|------------|------|---------|--------|
| `timing_analysis.comprehensive_analyses` | 11 | Comprehensive timing analysis results across all strategies | ✅ ACTIVE |
| `timing_analysis.recommendation_history` | 9 | Historical timing recommendations and their outcomes | ✅ ACTIVE |
| `timing_analysis.timing_bucket_performance` | 3 | Performance analysis by time buckets (e.g., 2hrs, 1hr, 30min before game) | ✅ ACTIVE |
| `timing_analysis.current_timing_performance` | 3 | Current timing performance metrics | ✅ ACTIVE |
| `timing_analysis.timing_recommendations_cache` | 1 | Cached timing recommendations for fast retrieval | ✅ ACTIVE |
| `timing_analysis.best_timing_by_category` | 0 | Best timing strategies by bet category | ⚠️ **EMPTY** |

### 5. PUBLIC Schema
Game outcomes, development, and testing tables.

| Table Name | Rows | Purpose | Status |
|------------|------|---------|--------|
| **`public.game_outcomes`** | **175** | **⭐ GAME RESULTS** - Official game outcomes (scores, winners) used by backtesting engine for bet outcome calculation | ✅ **ESSENTIAL FOR BACKTESTING** |
| `public.current_game_timing_status` | 27 | Current game timing and status monitoring | ✅ ACTIVE |
| `public.timing_validation_monitor` | 9 | Timing validation monitoring and alerts | ✅ ACTIVE |
| `public.benchmark_performance` | 200 | Database performance benchmarking | 🧪 **TEST DATA** |
| `public.benchmark_test` | 500 | Benchmark testing data | 🧪 **TEST DATA** |
| `public.performance_test` | 50 | Performance testing data | 🧪 **TEST DATA** |
| `public.test_parallel` | 100 | Parallel processing tests | 🧪 **TEST DATA** |
| `public.test_table` | 1 | General testing table | 🧪 **TEST DATA** |
| `public.games` | 0 | Games table (unused duplicate) | ❌ **DEPRECATED** |
| `public.concurrency_test` | 0 | Concurrency testing | 🧪 **TEST DATA - EMPTY** |

### 6. CLEAN Schema
Experimental/future betting recommendations.

| Table Name | Rows | Purpose | Status |
|------------|------|---------|--------|
| `clean.betting_recommendations` | 0 | Experimental betting recommendations table | 🚧 **EXPERIMENTAL** |

## Status Legend
- ✅ **HIGHLY ACTIVE** - Core system table with heavy read/write activity
- ✅ **MISSION CRITICAL** - Essential for system operation
- ✅ **ACTIVELY TRACKED** - Important for performance monitoring
- ✅ **ESSENTIAL FOR BACKTESTING** - Required for backtesting calculations
- ✅ **ACTIVE** - Regularly used table with data
- 📊 **NEWLY CREATED** - Recently implemented, not yet populated
- ⚠️ **IMPLEMENTED BUT EMPTY** - Table exists but not yet used
- ⚠️ **PLANNED - NOT USED** - Implemented but no active usage
- ⚠️ **EMPTY** - Table exists but has no data
- 🧪 **TEST DATA** - Development/testing table
- 🚧 **EXPERIMENTAL** - Future/experimental feature
- ❌ **DEPRECATED** - No longer used, consider removal

## Key Architecture Insights

### Core Data Flow
1. **Data Ingestion**: `splits.raw_mlb_betting_splits` (37,827 rows) ← External APIs (VSIN, SBD)
2. **Game Outcomes**: `public.game_outcomes` (175 rows) ← MLB API
3. **Strategy Analysis**: `backtesting.strategy_performance` (95 rows) ← BacktestingEngine
4. **Live Tracking**: `tracking.pre_game_recommendations` (5 rows) ← Pre-game workflow

### Phase 3 & 4 Implementation Impact
- **BacktestingEngine**: Now actively writing to `strategy_performance` table
- **New Tables**: Added alignment_analysis for live vs backtest comparison  
- **Enhanced Tracking**: Pre-game recommendations now track actual bet outcomes
- **Performance Growth**: Strategy performance table grew from 66 to 95 rows

### Tables by Priority

#### 🔥 **Mission Critical (System Cannot Function Without)**
1. `splits.raw_mlb_betting_splits` - Primary betting data source
2. `public.game_outcomes` - Required for bet outcome calculations
3. `backtesting.strategy_performance` - Core backtesting results

#### ⭐ **High Priority (Core Functionality)**
4. `tracking.pre_game_recommendations` - Live bet tracking and ROI validation
5. `backtesting.orchestrator_update_triggers` - Strategy automation
6. `tracking.active_high_roi_strategies` - Live strategy management

#### 📊 **Medium Priority (Enhancement Features)**
7. `timing_analysis.*` tables - Timing optimization
8. `backtesting.strategy_config_history` - Configuration management
9. `tracking.active_strategy_configs` - Live strategy configuration management

#### 🧹 **Cleanup Required**
- **Remove**: `splits.game_outcomes` (duplicate), `splits.sharp_actions` (deprecated)
- **Archive**: Test tables in public schema
- **Investigate**: Empty tables in backtesting schema

## Recent Changes (Phase 3 & 4)

### New Functionality ✅
- `backtesting.alignment_analysis` - Live alignment validation (Phase 3)
- Enhanced `strategy_performance` storage with 95 active results
- Real bet outcome tracking in `pre_game_recommendations`

### Performance Improvements 📈
- `raw_mlb_betting_splits`: Grew from 36,116 to 37,827 rows (+1,711 new records)
- `strategy_performance`: Increased from 66 to 95 strategy results (+44% growth)
- `pre_game_recommendations`: Now tracking 5 live recommendations

### Deprecated Tables 🗑️
- `splits.game_outcomes` - Use `public.game_outcomes` instead
- `splits.sharp_actions` - Logic moved to strategy processors

## Recommendations

### Immediate Actions 🚨
1. **Clean Up**: Remove deprecated tables (`splits.game_outcomes`, `splits.sharp_actions`)  
2. **Populate**: Investigate why `backtesting.strategy_alerts` is empty despite alert system working
3. **Archive**: Move test data from public schema to separate test database

### Monitoring Setup 📊
1. **Growth Tracking**: Monitor `raw_mlb_betting_splits` growth (currently 1,700+ rows/week)
2. **Performance**: Set alerts for `strategy_performance` table size and query performance
3. **Data Quality**: Monitor `game_outcomes` completeness for backtesting accuracy

### Future Development 🚀
1. **Implement**: Populate empty planned tables if features are needed
2. **Optimize**: Consider partitioning `raw_mlb_betting_splits` by date
3. **Enhance**: Add more comprehensive tracking to `pre_game_recommendations`

## Data Volume Summary
- **Total Active Records**: ~40,000+ across all tables
- **Primary Data Sources**: 38,000+ betting splits records
- **Backtesting Results**: 95 strategy performance records  
- **Live Tracking**: 5 pre-game recommendations with outcomes
- **Test Data**: ~850 test records (consider cleanup)

---
*This documentation reflects the current state after Phase 3 (BacktestingEngine) and Phase 4 (SchedulerEngine) implementation. Updated based on comprehensive codebase analysis and real row counts as of 2025-06-30.* 