# Utilities Directory

This directory contains standalone utility scripts for deployment, data collection, and system maintenance.

## Current Valid Scripts

### Data Collection & Pipeline Management

#### `action_network_quickstart.py`
Quick start script for Action Network data collection and testing.
- **Usage**: `uv run python utilities/action_network_quickstart.py`
- **Purpose**: Rapid testing of Action Network integration
- **Status**: Active

#### `run_action_network_pipeline.py`
Complete Action Network data collection pipeline orchestrator.
- **Usage**: `uv run python utilities/run_action_network_pipeline.py`
- **Purpose**: Full pipeline execution with collection and processing
- **Status**: Active

#### `load_action_network_complete_history.py`
Historical data loading utility for Action Network.
- **Usage**: `uv run python utilities/load_action_network_complete_history.py`
- **Purpose**: Bulk historical data import and processing
- **Status**: Active

### Data Quality & Deployment

#### `deploy_data_quality_improvements.py`
Deploys data quality improvements to the database.
- **Usage**: `uv run python utilities/deploy_data_quality_improvements.py`
- **Purpose**: Apply data quality schema improvements and monitoring
- **SQL Files**: References improvements in `sql/improvements/`
- **Status**: Active

#### `deploy_line_movement_improvements.py`
Deploys line movement analysis improvements.
- **Usage**: `uv run python utilities/deploy_line_movement_improvements.py`
- **Purpose**: Apply line movement tracking and analysis enhancements
- **Status**: Active

#### `validate_line_improvements.py`
Validates line movement improvement deployments.
- **Usage**: `uv run python utilities/validate_line_improvements.py`
- **Purpose**: Test and validate line movement functionality
- **Status**: Active

### Legacy Migration

#### `execute_legacy_migration.py`
Executes legacy data migration operations.
- **Usage**: `uv run python utilities/execute_legacy_migration.py`
- **Purpose**: Migrate data from legacy schemas to new pipeline structure
- **Status**: Maintenance (use with caution)

### Migration Framework

The `migration/` subdirectory contains a comprehensive migration framework:

#### Core Migration Scripts
- **`run_migration.py`**: Master orchestrator for all migration phases
- **`migration_validator.py`**: Comprehensive validation for all migration phases
- **`test_connection.py`**: Database connection testing utility

#### Phase-Specific Scripts
- **`phase1_data_analysis.py`**: Data analysis and migration planning
- **`phase2_raw_zone_migration.py`**: RAW zone data migration
- **`phase3_staging_zone_migration.py`**: STAGING zone data processing
- **`phase4_curated_zone_migration.py`**: CURATED zone feature generation

**Usage Examples**:
```bash
# Test database connectivity
uv run python utilities/migration/test_connection.py

# Run data analysis
uv run python utilities/migration/phase1_data_analysis.py

# Execute specific migration phase
uv run python utilities/migration/run_migration.py --phase 2

# Validate migration results
uv run python utilities/migration/migration_validator.py raw-zone
```

## Recommended Usage Workflow

### 1. Data Collection
```bash
# Quick test of Action Network integration
uv run python utilities/action_network_quickstart.py

# Full pipeline execution
uv run python utilities/run_action_network_pipeline.py
```

### 2. Data Quality Deployment
```bash
# Deploy data quality improvements
uv run python utilities/deploy_data_quality_improvements.py

# Deploy line movement improvements  
uv run python utilities/deploy_line_movement_improvements.py

# Validate improvements
uv run python utilities/validate_line_improvements.py
```

### 3. Historical Data Loading
```bash
# Load complete historical dataset
uv run python utilities/load_action_network_complete_history.py
```

### 4. Migration Operations (when needed)
```bash
# Test database connectivity
uv run python utilities/migration/test_connection.py

# Run migration with validation
uv run python utilities/migration/run_migration.py --phase all
```

## Recently Cleaned Up

The following testing and one-off scripts have been removed to improve project maintainability:

### Removed Files
- `test_action_network_mlb_resolution.py` - MLB game ID resolution testing
- `run_history_processor.py` - History processor testing
- `backfill_mlb_api_game_ids.py` - MLB game ID backfill utility
- `core_betting_migration/` - Entire automated refactor directory
- `analyze_existing_data.py` - One-off data analysis script
- `test_game_outcomes.py` - Game outcome service testing
- `cleanup_output_folder.py` - Output folder cleanup utility

### Cleanup Benefits
- **~20 redundant files removed**
- **~3,000+ lines of duplicate/testing code eliminated**
- **Clearer project structure with focus on production utilities**
- **Reduced maintenance overhead**

## Integration with Main CLI

Most functionality in these utilities is also available through the main CLI system:

```bash
# Data collection via main CLI
uv run -m src.interfaces.cli data collect --source action_network --real

# Data quality via main CLI
uv run -m src.interfaces.cli data-quality deploy

# Pipeline management via main CLI
uv run -m src.interfaces.cli action-network collect --date today
```

## File Status

✅ **Active**: Currently maintained and recommended for use
🔧 **Maintenance**: Use with caution, may need updates
📁 **Framework**: Part of migration framework, use as directed

## Notes

- All scripts should be run from the project root directory
- Use `uv run` for proper dependency management
- Check script documentation headers for detailed usage instructions
- Consider using the main CLI system for routine operations