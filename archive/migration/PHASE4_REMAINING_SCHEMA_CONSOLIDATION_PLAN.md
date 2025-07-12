# Phase 4: Remaining Schema Consolidation Plan

## Executive Summary

After the successful Phase 1-3 migration that consolidated core betting data, we have **6 remaining schemas** that can be further consolidated into our new 4-schema structure. This Phase 4 will complete the schema consolidation project by migrating all remaining operational and supplementary data.

## Current State Analysis

### Remaining Schemas (6 total)
- **`public`**: 11 tables, 22,098 total records
- **`action`**: 4 tables, 90 total records  
- **`splits`**: 2 tables, 252 total records
- **`tracking`**: 4 tables, 183 total records
- **`validation`**: 1 table, 0 records
- **`backtesting`**: 12 tables, 333 total records

### New Consolidated Schemas (Target)
- **`raw_data`**: External data ingestion and raw storage
- **`core_betting`**: Clean betting data and core business entities  
- **`analytics`**: Derived analytics and strategy outputs
- **`operational`**: System operations, monitoring, and validation

---

## Detailed Migration Plan by Schema

### 🗂️ **PUBLIC Schema → Multiple Destinations**

#### **Migration to `raw_data`** (External Data)
```sql
-- SportsbookReview raw data
public.sbr_raw_html (418 records) → raw_data.sbr_raw_html
public.sbr_parsed_games (19,436 records) → raw_data.sbr_parsed_games
```

#### **Migration to `core_betting`** (Core Business Data)
```sql
-- Game outcomes for backtesting
public.game_outcomes (1,281 records) → core_betting.game_outcomes
```

#### **Migration to `operational`** (System Operations)
```sql
-- Migration tracking
public.migration_log (17 records) → operational.migration_log
public.migration_log_phase2b (13 records) → operational.migration_log_phase2b
```

#### **Tables to DELETE** (Test/Temporary Data)
```sql
-- Performance/benchmark test tables (not production data)
public.benchmark_performance (200 records) → DELETE
public.benchmark_test (500 records) → DELETE  
public.concurrency_test (0 records) → DELETE
public.performance_test (50 records) → DELETE
public.test_parallel (100 records) → DELETE
public.test_table (1 record) → DELETE
```

### 🎯 **ACTION Schema → `core_betting`** (Team/Game Data)

```sql
-- Team reference data (enhance existing core_betting.teams)
action.dim_teams (30 records) → MERGE with core_betting.teams
action.teams_formatted (30 records) → MERGE with core_betting.teams

-- Game data with Action Network IDs
action.fact_games (15 records) → core_betting.action_network_games
action.games_with_teams (15 records) → core_betting.action_network_games_enhanced
```

**Consolidation Strategy**: Enhance `core_betting.teams` with Action Network data and create dedicated Action Network game mapping tables.

### 🔄 **SPLITS Schema → `core_betting` + `analytics`**

```sql
-- Supplementary game data
splits.games (252 records) → core_betting.supplementary_games

-- Sharp action detection results  
splits.sharp_actions (0 records) → analytics.sharp_action_indicators
```

### 📊 **TRACKING Schema → `operational`** (Strategy Tracking)

```sql
-- Active strategy management
tracking.active_high_roi_strategies (20 records) → operational.active_strategies
tracking.active_strategy_configs (20 records) → operational.strategy_configurations
tracking.strategy_integration_log (138 records) → operational.strategy_integration_log

-- Live recommendation tracking
tracking.pre_game_recommendations (5 records) → operational.pre_game_recommendations
```

### ✅ **VALIDATION Schema → `operational`** (Data Quality)

```sql
-- Strategy validation records
validation.strategy_records (0 records) → operational.strategy_validation_records
```

### ⚙️ **BACKTESTING Schema → `operational`** (Strategy Management)

```sql
-- Strategy configurations and management
backtesting.strategy_configurations (36 records) → operational.strategy_configurations
backtesting.strategy_config_history (26 records) → operational.strategy_config_history  
backtesting.strategy_config_cache (0 records) → operational.strategy_config_cache
backtesting.threshold_configurations (12 records) → operational.threshold_configurations

-- System orchestration
backtesting.orchestrator_update_triggers (64 records) → operational.orchestrator_update_triggers
backtesting.recent_config_changes (20 records) → operational.recent_config_changes

-- Monitoring and alerts
backtesting.strategy_alerts (0 records) → operational.strategy_alerts
backtesting.strategy_lifecycle_events (0 records) → operational.strategy_lifecycle_events
backtesting.alignment_analysis (0 records) → operational.alignment_analysis
backtesting.threshold_recommendations (0 records) → operational.threshold_recommendations

-- Audit and backup
backtesting.standardization_audit_log (17 records) → operational.standardization_audit_log
backtesting.strategy_performance_backup (0 records) → operational.strategy_performance_backup
```

---

## Implementation Strategy

### Phase 4A: Schema Extension (NON-DESTRUCTIVE)
1. **Extend existing consolidated schemas** with new tables
2. **Create migration scripts** for data transformation
3. **Preserve all existing data** during migration

### Phase 4B: Data Migration (NON-DESTRUCTIVE)  
1. **Migrate data** from remaining schemas to consolidated schemas
2. **Enhance existing tables** where consolidation makes sense
3. **Validate data integrity** throughout migration

### Phase 4C: Service Updates
1. **Update table registry** with new mappings
2. **Update application services** to use consolidated tables
3. **Test all functionality** with new schema structure

### Phase 4D: Legacy Cleanup (DESTRUCTIVE)
1. **Drop remaining legacy schemas** after validation
2. **Complete final schema consolidation**
3. **Generate completion report**

---

## Expected Benefits

### 📈 **Complexity Reduction**
- **Before Phase 4**: 10 schemas (4 new + 6 remaining)
- **After Phase 4**: 4 schemas (fully consolidated)
- **Improvement**: 60% additional reduction

### 🎯 **Logical Organization**
- **Team/Game Data**: All in `core_betting`
- **Strategy Management**: All in `operational`  
- **Raw External Data**: All in `raw_data`
- **Test Data**: Eliminated

### 🔧 **Maintainability**
- **Single source of truth** for each data type
- **Consistent naming conventions** across all tables
- **Clear data flow** and relationships
- **Reduced cognitive overhead** for developers

### 🔒 **Security & Governance**
- **Schema-level permissions** for all data
- **Centralized access control**
- **Audit trail** for all data access
- **Compliance-ready** structure

---

## Risk Assessment

### ✅ **Low Risk** (Well-Tested Migration Pattern)
- **Proven migration methodology** from Phases 1-3
- **Non-destructive approach** with rollback capability
- **Comprehensive validation** at each step
- **Incremental implementation** minimizes impact

### ⚠️ **Considerations**
- **Action Network integration** may need service updates
- **Strategy management services** will need table registry updates
- **Test data cleanup** should be coordinated with development team

---

## Success Criteria

### ✅ **Data Migration Success**
- **Zero data loss** during migration
- **100% data integrity** validation
- **All services functional** with new schema

### ✅ **Schema Consolidation Success**  
- **4 schemas total** (down from 10)
- **Clear logical separation** of concerns
- **Consistent naming** and structure

### ✅ **System Performance**
- **Query performance** maintained or improved
- **Connection efficiency** optimized
- **Maintenance overhead** reduced

---

## Next Steps

1. **Review and approve** this consolidation plan
2. **Create Phase 4A migration scripts** (schema extension)
3. **Execute Phase 4A** (non-destructive schema creation)
4. **Validate Phase 4A** results
5. **Proceed with Phase 4B** (data migration)

This Phase 4 consolidation will complete the schema modernization project, resulting in a clean, maintainable, and logically organized database structure that supports the system's growth and evolution.

---

**General Balls** 