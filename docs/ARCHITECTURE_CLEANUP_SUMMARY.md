# Architecture Cleanup Summary - July 2025

## Overview
Major cleanup of legacy code and architecture consolidation focused on:
1. Removing generic raw tables in favor of source-specific tables
2. Eliminating old sparse Action Network datasets 
3. Consolidating to unified historical (temporal) staging approach

## Files Removed

### ❌ Old Generic Raw Table Approach
- `sql/migrations/004_create_pipeline_zones.sql` → `004_create_pipeline_zones.sql.old` (backup)
- References to `betting_lines_raw`, `moneylines_raw`, `spreads_raw`, `totals_raw`

### ❌ Old Sparse Action Network Files
- `src/data/pipeline/staging_action_network_processor.py` (original sparse)
- `src/data/pipeline/staging_action_network_processor_redesigned.py` (wide redesign)
- `sql/schemas/staging_action_network_schema.sql` (original sparse schema)
- `sql/schemas/staging_action_network_odds_redesigned.sql` (wide schema)

### ❌ Redundant LONG Structure (keeping only Historical)
- `src/data/pipeline/staging_action_network_long_processor.py`
- `sql/schemas/staging_action_network_long_structure.sql`

### ❌ CLI Commands for Old Approaches
- `src/interfaces/cli/commands/staging_long.py`
- `src/interfaces/cli/commands/staging_pipeline.py`
- `src/interfaces/cli/commands/staging_redesigned.py`

### ❌ Obsolete Documentation
- `docs/STAGING_ODDS_REDESIGN.md`
- `docs/STAGING_LONG_STRUCTURE_IMPLEMENTATION.md`

## Files Created/Updated

### ✅ New Source-Specific Migration
- `sql/migrations/004_create_source_specific_zones.sql` - Replaces generic approach

### ✅ Updated Code Files
- `src/data/pipeline/raw_zone.py` - Removed generic table references
- `src/data/pipeline/pipeline_orchestrator.py` - Updated to use source-specific tables
- `src/interfaces/cli/main.py` - Removed old staging command imports

## Final Architecture

### Raw Zone (Source-Specific)
```
raw_data/
├── action_network_odds      # Action Network odds data
├── action_network_games     # Action Network games data  
├── action_network_history   # Action Network line movement history
├── sbd_betting_splits       # SBD data
├── vsin_data               # VSIN data
└── mlb_stats_api           # MLB Stats API data
```

### Staging Zone (Historical/Temporal Only)
```
staging/
├── action_network_games              # Unified games with MLB Stats API IDs
└── action_network_odds_historical    # Complete temporal odds (LONG + Historical)
```

**Key Design**: `staging.action_network_odds_historical` combines:
- **LONG structure**: One row per market type with unified columns
- **Historical data**: Every line change with exact timestamps
- **Temporal analysis**: Microsecond precision for sophisticated betting analysis

### Benefits of Cleanup

#### 🎯 **Simplified Architecture**
- Single staging table (`action_network_odds_historical`) instead of multiple approaches
- Source-specific raw tables eliminate generic table confusion
- Clear data flow: Raw → Staging (Historical) → Curated

#### ⚡ **Improved Performance**
- Eliminated redundant processors and schemas
- Unified data model reduces complexity
- 100% data density with temporal precision

#### 🔧 **Easier Maintenance**
- Single truth source for Action Network staging data
- No confusion between sparse/wide/long approaches
- Clear processor responsibilities

#### 📊 **Enhanced Analytics**
- Complete temporal data for line movement analysis
- Microsecond-precision timestamps for sharp money detection
- Unified market structure enables cross-market analysis

## Migration Impact

### ✅ What Still Works
- Action Network data collection via `consolidated_action_network_collector.py`
- Historical processing via `staging_action_network_history_processor.py`
- Historical schema and analytics via `staging_action_network_odds_historical.sql`
- SBD, VSIN, and other source-specific collectors

### ⚠️ What Changed
- Pipeline now uses source-specific raw tables only
- Staging processing consolidated to historical approach only
- CLI commands simplified (removed redundant options)

### 🚀 What's Improved
- Single staging approach eliminates architectural confusion
- Historical data provides complete temporal dimension
- Source-specific raw tables improve data organization
- Cleaner codebase with reduced maintenance burden

## Commands Available

### Current Working Commands
```bash
# Data collection (source-specific)
uv run -m src.interfaces.cli action-network collect --date today
uv run -m src.interfaces.cli data collect --source action_network --real

# Historical processing (temporal staging)
uv run -m src.data.pipeline.staging_action_network_history_processor

# Pipeline management
uv run -m src.interfaces.cli pipeline run --zone all --mode full

# Analysis and movement
uv run -m src.interfaces.cli movement analyze --input-file output/action_network_history.json
```

### Commands Removed
```bash
# Old staging approaches (no longer available)
# uv run -m src.interfaces.cli staging-long process
# uv run -m src.interfaces.cli staging process  
# uv run -m src.interfaces.cli staging-redesigned process
```

## Result
✅ **Clean, unified architecture** with source-specific raw data flowing to temporal staging data, enabling sophisticated betting analysis while eliminating architectural confusion and maintenance overhead.