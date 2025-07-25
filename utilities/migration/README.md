# MLB Betting System Pipeline Migration

This directory contains utilities for migrating data from the legacy `core_betting` schema to the new three-tier pipeline architecture (`raw_data` → `staging` → `curated`).

## Overview

The migration implements a comprehensive data transformation from a single-tier legacy system to a modern data pipeline with proper data lineage, quality controls, and feature engineering capabilities.

### Migration Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   LEGACY        │    │  NEW PIPELINE    │    │   ENHANCED      │
│                 │    │                  │    │                 │
│ curated.*  │───▶│ raw_data.*       │───▶│ staging.*       │───▶│ curated.*
│                 │    │                  │    │                 │    │
│ • Mixed data    │    │ • Source data    │    │ • Cleaned data  │    │ • ML features
│ • No lineage    │    │ • Full lineage   │    │ • Quality score │    │ • Analytics
│ • Limited QA    │    │ • Audit trail    │    │ • Normalized    │    │ • Profitability
└─────────────────┘    └──────────────────┘    └─────────────────┘    └─────────────────┘
```

## Migration Phases

### Phase 1: Data Analysis & Mapping
**Script**: `phase1_data_analysis.py`

- Analyzes existing data volume and quality
- Identifies source attribution patterns
- Generates migration complexity assessment
- Provides time and resource estimates

**Usage**:
```bash
cd utilities/migration
python phase1_data_analysis.py
```

**Outputs**: `phase1_analysis_results.json`

### Phase 2: RAW Zone Migration
**Script**: `phase2_raw_zone_migration.py`

- Migrates betting lines data to `raw_data` schema
- Preserves original source attribution and lineage
- Creates unified betting lines table
- Implements batch processing with error handling

**Tables Migrated**:
- `curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'` → `raw_data.moneylines_raw`
- `curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread'` → `raw_data.spreads_raw`
- `curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'` → `raw_data.totals_raw`
- All above → `raw_data.betting_lines_raw` (unified)

**Usage**:
```bash
cd utilities/migration
python phase2_raw_zone_migration.py
```

**Outputs**: `phase2_migration_results.json`

### Phase 3: STAGING Zone Migration
**Status**: To be implemented

Will process RAW zone data into cleaned, normalized staging tables with:
- Data quality scoring
- Team name normalization  
- Referential integrity establishment
- Validation rule application

### Phase 4: CURATED Zone Migration  
**Status**: To be implemented

Will generate analysis-ready data with:
- ML feature vectors
- Sharp action indicators
- Market efficiency scores
- Profitability classifications

## Validation & Quality Assurance

### Migration Validator
**Script**: `migration_validator.py`

Comprehensive validation system ensuring data integrity across all phases:

- **Pre-migration**: Schema readiness, permissions, source data integrity
- **Raw Zone**: Record count consistency, completeness, source attribution
- **Staging Zone**: Quality scores, referential integrity
- **Curated Zone**: Feature generation, analytics integration
- **Pipeline**: End-to-end functionality testing

**Usage**:
```bash
# Validate specific phase
python migration_validator.py pre-migration
python migration_validator.py raw-zone
python migration_validator.py all

# As part of migration
python run_migration.py --phase 2 --validate
```

## Master Orchestrator

### Migration Orchestrator
**Script**: `run_migration.py`

Master orchestration script with automated execution, validation, and rollback capabilities.

**Key Features**:
- Automated multi-phase execution
- Built-in validation checkpoints
- Comprehensive error handling
- Rollback capability
- Dry-run mode for testing
- Detailed progress reporting

**Usage Examples**:
```bash
# Full migration with validation
python run_migration.py --phase all

# Individual phases
python run_migration.py --phase 1    # Data analysis only
python run_migration.py --phase 2    # RAW zone migration

# Validation only
python run_migration.py --validate-only

# Dry run (no changes)
python run_migration.py --phase 2 --dry-run

# Skip validation (faster execution)
python run_migration.py --phase 2 --no-validate
```

## Data Volume & Performance

Based on current database analysis:

- **Total Records**: ~22,000+ betting records
- **Games**: 1,687 game records  
- **Moneylines**: 10,665 records
- **Spreads**: 1,615 records
- **Totals**: 8,822 records

**Estimated Migration Time**: 1-2 hours for Phase 2 (RAW zone)
**Batch Size**: 1,000 records per batch
**Success Rate Target**: ≥99% record migration success

## Error Handling & Recovery

### Rollback Strategy
1. **Database Backups**: Full backup before migration start
2. **Incremental Backups**: After each phase completion
3. **Transaction Safety**: Batch-level transactions with rollback capability
4. **Legacy Preservation**: `core_betting` schema kept intact until cutover

### Error Recovery
- **Batch-level recovery**: Failed batches can be retried independently  
- **Record-level logging**: Individual record failures tracked
- **Quality gates**: Automatic failure detection with thresholds
- **Manual intervention**: Clear error messages and recovery instructions

## Quality Metrics

### Success Criteria
- **Data Completeness**: 100% of existing records migrated
- **Data Quality**: ≥95% staging validation success rate  
- **Performance**: <30s full pipeline execution for daily data
- **Reliability**: 99%+ pipeline completion rate
- **Zero Data Loss**: Complete audit trail preservation

### Monitoring
- Real-time progress tracking
- Quality score validation
- Error rate monitoring
- Performance benchmarking
- Resource utilization tracking

## File Structure

```
utilities/migration/
├── README.md                          # This documentation
├── run_migration.py                   # Master orchestrator
├── phase1_data_analysis.py           # Phase 1: Analysis & mapping
├── phase2_raw_zone_migration.py      # Phase 2: RAW zone migration  
├── migration_validator.py            # Validation utilities
├── phase1_analysis_results.json      # Phase 1 outputs
├── phase2_migration_results.json     # Phase 2 outputs
├── validation_results_*.json         # Validation outputs
└── migration_results_*.json          # Master migration logs
```

## Prerequisites

1. **Database Access**: PostgreSQL with all schemas created (via migration script)
2. **Python Environment**: Python 3.11+ with project dependencies
3. **Permissions**: Read/write access to all schemas
4. **Backup**: Database backup completed and verified

## Getting Started

### 1. Pre-flight Check
```bash
# Validate system readiness
python migration_validator.py pre-migration
```

### 2. Data Analysis
```bash
# Analyze existing data
python phase1_data_analysis.py
```

### 3. Execute Migration
```bash  
# Start with dry run
python run_migration.py --phase 2 --dry-run

# Execute actual migration
python run_migration.py --phase 2
```

### 4. Validation
```bash
# Validate results
python migration_validator.py raw-zone
```

## Troubleshooting

### Common Issues

1. **Schema Missing**: Ensure pipeline migration script has been run
2. **Permission Denied**: Check database user permissions
3. **Memory Issues**: Reduce batch size in migration scripts
4. **Timeout Errors**: Increase connection timeout settings

### Debug Mode
Enable verbose logging by setting environment variable:
```bash
export LOG_LEVEL=DEBUG
python run_migration.py --phase 2
```

### Recovery Procedures
1. Check validation results for specific error details
2. Review migration logs for failed batches
3. Use targeted retry for failed records
4. Rollback to backup if necessary

## Support

For issues or questions:
1. Check validation results and logs
2. Review error messages and recommendations
3. Use dry-run mode to test changes
4. Consult database administrator for permission issues

## Future Enhancements

- **Phase 3**: STAGING zone migration with advanced cleaning
- **Phase 4**: CURATED zone with ML features  
- **Incremental Updates**: Real-time data sync
- **Performance Optimization**: Parallel processing
- **Monitoring Dashboard**: Real-time migration monitoring