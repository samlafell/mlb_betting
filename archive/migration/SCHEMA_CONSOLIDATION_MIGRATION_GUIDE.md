# MLB Sharp Betting System - Schema Consolidation Migration Guide

## Overview

This guide outlines the complete migration from the current **9+ schema structure** to a consolidated **4-schema structure** that eliminates schema sprawl and improves maintainability.

## Current vs. New Schema Structure

### Before: 9+ Schemas (Current)
```
1. public      - Mixed: raw data, game outcomes, test tables
2. splits      - Raw MLB betting splits data, games
3. mlb_betting - SportsbookReview betting data
4. backtesting - Strategy performance tracking
5. tracking    - Pre-game recommendations, active strategies
6. timing_analysis - Timing analysis results
7. clean       - Experimental betting recommendations
8. action      - Action Network team data
9. validation  - Data quality metrics (planned)
```

### After: 4 Schemas (Consolidated)
```
1. raw_data     - All external data ingestion and raw storage
2. core_betting - Clean, processed betting data and core business entities
3. analytics    - Derived analytics, signals, and strategy outputs
4. operational  - System operations, monitoring, and validation
```

## Benefits of Consolidation

✅ **Reduced Complexity**: 9+ schemas → 4 schemas (55% reduction)
✅ **Logical Grouping**: Related functionality grouped together
✅ **Clearer Data Flow**: Raw → Core → Analytics → Operational
✅ **Better Maintenance**: Easier to understand and maintain
✅ **Improved Security**: Schema-level permissions more effective
✅ **Reduced Cognitive Overhead**: Developers know exactly where to find data

## Migration Strategy

The migration is designed to be **NON-DESTRUCTIVE** and performed in phases to minimize risk and downtime.

### Phase 1: Create and Populate New Schemas (NON-DESTRUCTIVE)
- ✅ Create consolidated schema structure
- ✅ Create migration scripts
- ✅ Migrate data from old to new schemas
- ✅ Old schemas remain untouched

### Phase 2: Update Application Code
- 🔄 Update table registry to use new schemas
- 🔄 Update application code references
- 🔄 Test application functionality

### Phase 3: Drop Old Schemas (DESTRUCTIVE - Final Step)
- ⚠️ Drop old schemas after verification
- ⚠️ This step is irreversible

## Detailed Schema Mapping

### 1. RAW_DATA Schema
**Purpose**: All external data ingestion and raw storage

| New Table | Source Schema.Table | Description |
|-----------|---------------------|-------------|
| `raw_data.sbr_raw_html` | `public.sbr_raw_html` | Raw HTML from SportsbookReview |
| `raw_data.sbr_parsed_games` | `public.sbr_parsed_games` | Parsed SBR game data |
| `raw_data.raw_mlb_betting_splits` | `splits.raw_mlb_betting_splits` | Raw betting splits from VSIN/SBD |
| `raw_data.mlb_api_responses` | *New* | MLB Stats API responses |
| `raw_data.odds_api_responses` | *New* | Odds API responses |
| `raw_data.vsin_raw_data` | *New* | Raw VSIN data |
| `raw_data.parsing_status_logs` | *New* | Parsing status tracking |

### 2. CORE_BETTING Schema
**Purpose**: Clean, processed betting data and core business entities

| New Table | Source Schema.Table | Description |
|-----------|---------------------|-------------|
| `core_betting.games` | `public.games` + `splits.games` | Unified games table |
| `core_betting.game_outcomes` | `public.game_outcomes` | Game results for backtesting |
| `core_betting.teams` | `action.dim_teams` | Team reference data |
| `core_betting.betting_lines_moneyline` | `mlb_betting.moneyline` | Moneyline betting data |
| `core_betting.betting_lines_spreads` | `mlb_betting.spreads` | Spread betting data |
| `core_betting.betting_lines_totals` | `mlb_betting.totals` | Totals betting data |
| `core_betting.betting_splits` | *Derived* | Aggregated betting splits |
| `core_betting.sharp_action_indicators` | *New* | Sharp action detection |
| `core_betting.line_movements` | *New* | Line movement tracking |
| `core_betting.steam_moves` | *New* | Steam move detection |

### 3. ANALYTICS Schema
**Purpose**: Derived analytics, signals, and strategy outputs

| New Table | Source Schema.Table | Description |
|-----------|---------------------|-------------|
| `analytics.strategy_signals` | *New* | Strategy-generated signals |
| `analytics.betting_recommendations` | `clean.betting_recommendations` | Clean betting recommendations |
| `analytics.timing_analysis_results` | `timing_analysis.timing_bucket_performance` | Timing analysis data |
| `analytics.cross_market_analysis` | *New* | Cross-market opportunity analysis |
| `analytics.confidence_scores` | *New* | Confidence scoring results |
| `analytics.roi_calculations` | *New* | ROI calculation results |
| `analytics.performance_metrics` | *New* | Aggregated performance metrics |

### 4. OPERATIONAL Schema
**Purpose**: System operations, monitoring, and validation

| New Table | Source Schema.Table | Description |
|-----------|---------------------|-------------|
| `operational.strategy_performance` | `backtesting.strategy_performance` | Strategy backtesting results |
| `operational.pre_game_recommendations` | `tracking.pre_game_recommendations` | Live recommendation tracking |
| `operational.orchestrator_update_triggers` | `backtesting.orchestrator_update_triggers` | System orchestration |
| `operational.system_health_checks` | *New* | System health monitoring |
| `operational.data_quality_metrics` | *New* | Data quality validation |
| `operational.pipeline_execution_logs` | *New* | Pipeline execution tracking |
| `operational.alert_configurations` | *New* | Alert management |
| `operational.recommendation_tracking` | *New* | Recommendation outcome tracking |

## Step-by-Step Migration Instructions

### Prerequisites
1. **Backup your database** before starting
2. Ensure all current operations are stable
3. Have a rollback plan ready

### Step 1: Create Consolidated Schemas
```bash
# Run the consolidated schema creation script
psql -d mlb_betting -f sql/consolidated_schema.sql
```

### Step 2: Migrate Data (Phase 1)
```bash
# Run the non-destructive migration script
psql -d mlb_betting -f sql/schema_migration_phase1.sql

# Verify migration completed successfully
psql -d mlb_betting -c "SELECT * FROM migration_log WHERE migration_phase = 'Phase1' ORDER BY id;"
```

### Step 3: Update Table Registry (Already Completed)
The table registry has been updated to use the new schema structure with backward compatibility.

### Step 4: Update Application Code References
Update your application code to use the new table registry mappings:

```python
# Old way
query = "SELECT * FROM splits.raw_mlb_betting_splits"

# New way  
registry = get_table_registry()
table_name = registry.get_table('raw_betting_splits')
query = f"SELECT * FROM {table_name}"
# This now returns: raw_data.raw_mlb_betting_splits
```

### Step 5: Set Up Permissions
```bash
# Apply the new permission structure
psql -d mlb_betting -f sql/schema_permissions.sql
```

### Step 6: Test Application Functionality
1. Run your test suite
2. Verify data access patterns work correctly
3. Check that all services can read/write data properly
4. Validate that the new schema structure works as expected

### Step 7: Monitor and Validate
1. Monitor application logs for any schema-related errors
2. Check data quality in new schemas
3. Verify performance is maintained or improved
4. Ensure all automated processes work correctly

### Step 8: Drop Old Schemas (FINAL STEP - DESTRUCTIVE)
⚠️ **WARNING**: This step is irreversible. Only proceed after thorough testing.

```bash
# Create the cleanup script (run only after thorough verification)
psql -d mlb_betting -f sql/schema_migration_phase3_cleanup.sql
```

## Schema Permission Model

The new structure implements a role-based access control system:

### Roles and Access Patterns
```
data_collectors     → raw_data (INSERT, SELECT)
betting_processors  → raw_data (READ) + core_betting (WRITE)
analytics_processors → core_betting (READ) + analytics (WRITE)
strategy_processors → ALL (READ) + analytics/operational (WRITE)
analytics_users     → core_betting + analytics (READ ONLY)
monitoring_users    → operational (READ ONLY)
system_administrators → ALL (FULL ACCESS)
mlb_betting_app     → ALL (BROAD ACCESS for main application)
```

### Permission Usage Examples
```sql
-- Grant a user data collection permissions
SELECT grant_user_permissions('scraper_service', 'collector');

-- Grant a user analytics permissions
SELECT grant_user_permissions('data_scientist', 'analyst');

-- Check user permissions
SELECT * FROM check_user_permissions('username');
```

## Data Flow in New Structure

```
External Sources → raw_data → core_betting → analytics → operational
                      ↓            ↓           ↓           ↓
                 Raw scraped   Clean betting  Signals &   Monitoring &
                 HTML & API    lines & games  analytics   backtesting
```

## Rollback Plan

If issues are encountered during migration:

1. **Before Phase 3**: Simply use the legacy table mappings in the table registry
2. **Application Issues**: Update code to use `legacy_*` table mappings temporarily
3. **Data Issues**: The original schemas remain untouched until Phase 3

## File Structure

The migration includes these key files:

```
sql/
├── consolidated_schema.sql           # New 4-schema structure
├── schema_migration_phase1.sql       # Non-destructive data migration
├── schema_permissions.sql            # Role-based access control
└── schema_migration_phase3_cleanup.sql # Old schema cleanup (DESTRUCTIVE)

src/mlb_sharp_betting/db/
└── table_registry.py                # Updated table mappings
```

## Validation Queries

Use these queries to validate the migration:

```sql
-- Check record counts match between old and new schemas
SELECT 'old' as source, COUNT(*) FROM splits.raw_mlb_betting_splits
UNION ALL
SELECT 'new' as source, COUNT(*) FROM raw_data.raw_mlb_betting_splits;

-- Check game outcomes migration
SELECT 'old' as source, COUNT(*) FROM public.game_outcomes
UNION ALL  
SELECT 'new' as source, COUNT(*) FROM core_betting.game_outcomes;

-- Verify strategy performance migration
SELECT 'old' as source, COUNT(*) FROM backtesting.strategy_performance
UNION ALL
SELECT 'new' as source, COUNT(*) FROM operational.strategy_performance;

-- Check migration log for any errors
SELECT * FROM migration_log WHERE status = 'failed';
```

## Performance Considerations

The new schema structure should improve performance through:

1. **Better Indexing**: Targeted indexes per schema purpose
2. **Reduced Cross-Schema Joins**: Related data is co-located
3. **Clearer Query Patterns**: Developers know exactly where to find data
4. **Schema-Level Optimizations**: Can optimize each schema for its specific use case

## Monitoring Post-Migration

After migration, monitor:

1. **Query Performance**: Check for any degradation in common queries
2. **Application Logs**: Watch for schema-related errors
3. **Data Quality**: Ensure data integrity is maintained
4. **Permission Issues**: Verify all users/services have appropriate access

## Support and Troubleshooting

### Common Issues

1. **Permission Denied Errors**
   - Solution: Check role assignments with `check_user_permissions()`
   - Grant appropriate role: `SELECT grant_user_permissions('user', 'type')`

2. **Table Not Found Errors**
   - Solution: Verify table registry is updated
   - Check that migration completed successfully

3. **Foreign Key Constraint Violations**
   - Solution: Ensure game IDs are properly mapped between schemas
   - Check migration logs for any mapping issues

### Getting Help

1. Check the migration log table: `SELECT * FROM migration_log;`
2. Review application logs for schema-related errors
3. Use the permission checking functions to diagnose access issues
4. Validate data integrity with the provided validation queries

## Conclusion

This schema consolidation significantly reduces complexity while maintaining all existing functionality. The migration is designed to be safe, reversible (until Phase 3), and minimizes downtime.

The new 4-schema structure provides:
- **Clear separation of concerns**
- **Improved maintainability**
- **Better security model**
- **Reduced cognitive overhead**
- **Future scalability**

Follow the step-by-step instructions carefully, and don't proceed to Phase 3 (dropping old schemas) until you're completely confident the new structure works correctly for your use case.

---

**General Balls** 