# Core Betting Schema Decommission - Implementation Guide

This directory contains all the tools and scripts needed to completely decommission the `core_betting` schema and migrate all data and references to the modern 3-schema architecture (`raw_data`, `staging`, `curated`).

## Overview

The core_betting schema decommission process involves:

1. **Data Migration**: Moving all unique data from core_betting to curated schema
2. **Code Refactoring**: Updating all code references from core_betting to curated
3. **Schema Consolidation**: Merging redundant tables into unified structures
4. **Validation**: Ensuring data integrity and business logic preservation
5. **Cleanup**: Removing the core_betting schema entirely

## File Structure

```
core_betting_migration/
├── README.md                           # This guide
├── automated_code_refactor.py          # Automated code refactoring tool
├── data_migration_scripts.sql          # Complete SQL migration scripts
├── validation_and_rollback.py          # Validation and emergency rollback system
└── execution_checklist.md              # Step-by-step execution checklist
```

## Implementation Timeline

### **Phase 1: Pre-Migration Preparation (Days 1-3)**
**Estimated Time:** 16-24 hours

#### Day 1: Analysis and Validation
- [ ] Run comprehensive codebase analysis
- [ ] Execute pre-migration validation
- [ ] Create full database backup
- [ ] Review foreign key dependencies

#### Day 2: Code Preparation  
- [ ] Generate code refactoring report
- [ ] Review and test automated refactoring
- [ ] Prepare manual refactoring for complex patterns
- [ ] Update configuration files

#### Day 3: Infrastructure Preparation
- [ ] Set up monitoring and logging
- [ ] Prepare rollback procedures
- [ ] Test migration scripts in staging environment
- [ ] Schedule migration window

### **Phase 2: Data Migration (Days 4-6)**
**Estimated Time:** 24-32 hours

#### Day 4: Unique Data Migration
- [ ] Create enhanced curated schema tables
- [ ] Migrate sportsbook external mappings
- [ ] Migrate data source metadata
- [ ] Migrate teams master data
- [ ] Validate unique data migration

#### Day 5: Primary Data Migration
- [ ] Consolidate betting lines (moneyline, spreads, totals)
- [ ] Merge games and supplementary games
- [ ] Migrate game outcomes
- [ ] Update foreign key relationships
- [ ] Run comprehensive validation

#### Day 6: Validation and Testing
- [ ] Execute post-migration validation
- [ ] Test all business logic
- [ ] Performance testing
- [ ] Generate migration report

### **Phase 3: Code Refactoring (Days 7-10)**
**Estimated Time:** 32-40 hours

#### Day 7-8: Automated Refactoring
- [ ] Execute automated code refactoring
- [ ] Update CLI commands and services
- [ ] Update utility scripts
- [ ] Update documentation

#### Day 9: Manual Refactoring
- [ ] Handle complex SQL patterns
- [ ] Update foreign key constraints
- [ ] Fix betting lines consolidation queries
- [ ] Update configuration management

#### Day 10: Testing and Validation
- [ ] Run complete test suite
- [ ] Integration testing
- [ ] Performance validation
- [ ] User acceptance testing

### **Phase 4: Cleanup and Finalization (Days 11-12)**
**Estimated Time:** 8-16 hours

#### Day 11: Schema Cleanup
- [ ] Final validation checks
- [ ] Drop core_betting schema
- [ ] Clean up backup schemas
- [ ] Update database permissions

#### Day 12: Documentation and Training
- [ ] Update all documentation
- [ ] Update README and CLAUDE.md
- [ ] Create migration summary report
- [ ] Team training on new schema

## Quick Start Guide

### 1. Pre-Migration Analysis

```bash
# Generate comprehensive analysis
python utilities/core_betting_migration/automated_code_refactor.py --report-only

# Run pre-migration validation
python utilities/core_betting_migration/validation_and_rollback.py --validate-pre-migration --output-file pre_migration_report.md
```

### 2. Data Migration

```bash
# Execute the complete SQL migration (review first!)
psql -d mlb_betting -f utilities/core_betting_migration/data_migration_scripts.sql

# Monitor migration progress
psql -d mlb_betting -c "SELECT * FROM operational.v_core_betting_migration_status ORDER BY id DESC;"
```

### 3. Code Refactoring

```bash
# Dry run to see what would change
python utilities/core_betting_migration/automated_code_refactor.py --dry-run

# Execute the refactoring (creates backup first)
python utilities/core_betting_migration/automated_code_refactor.py --execute
```

### 4. Post-Migration Validation

```bash
# Run comprehensive validation
python utilities/core_betting_migration/validation_and_rollback.py --validate-post-migration --output-file post_migration_report.md
```

### 5. Emergency Rollback (if needed)

```bash
# Emergency rollback (only if critical issues found)
python utilities/core_betting_migration/validation_and_rollback.py --rollback --confirm
```

## Tool Details

### Automated Code Refactor (`automated_code_refactor.py`)

**Purpose:** Automates the replacement of core_betting schema references throughout the codebase.

**Key Features:**
- Comprehensive file scanning and pattern matching
- Schema mapping configuration for all table replacements
- Special handling for betting lines consolidation
- Automatic backup creation
- Detailed refactoring reports

**Usage Examples:**
```bash
# Analyze what changes would be made
python automated_code_refactor.py --dry-run

# Generate detailed report
python automated_code_refactor.py --report-only

# Execute refactoring with backup
python automated_code_refactor.py --execute
```

### Data Migration Scripts (`data_migration_scripts.sql`)

**Purpose:** Complete SQL scripts for migrating all data from core_betting to curated schema.

**Key Features:**
- Pre-migration setup and validation
- Enhanced curated table creation
- Unique data migration with integrity checks
- Primary data consolidation (betting lines, games, outcomes)
- Comprehensive validation and rollback support

**Sections:**
1. Pre-migration setup and backup creation
2. Enhanced curated schema table creation
3. Unique data migration (mappings, metadata, teams)
4. Primary data migration with consolidation
5. Validation and post-migration tasks
6. Schema cleanup (execute only after validation)

### Validation and Rollback (`validation_and_rollback.py`)

**Purpose:** Comprehensive validation system with emergency rollback capabilities.

**Key Features:**
- Pre-migration validation (schema accessibility, data integrity, locks, dependencies)
- Post-migration validation (row counts, data integrity, performance, business logic)
- Real-time migration monitoring
- Emergency rollback system
- Detailed validation reports

**Usage Examples:**
```bash
# Pre-migration validation
python validation_and_rollback.py --validate-pre-migration

# Post-migration validation  
python validation_and_rollback.py --validate-post-migration

# Emergency rollback
python validation_and_rollback.py --rollback --confirm
```

## Schema Mapping Reference

### Table Migrations

| Core Betting Table | Target Curated Table | Notes |
|-------------------|---------------------|-------|
| `curated.games_complete` | `curated.games_complete` | Merged with supplementary_games |
| `curated.games_complete` | `curated.games_complete` | Merged into main games table |
| `curated.game_outcomes` | `curated.game_outcomes` | Direct migration with FK updates |
| `curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'` | `curated.betting_lines_unified` | Consolidated with market_type = 'moneyline' |
| `curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's` | `curated.betting_lines_unified` | Consolidated with market_type = 'spread' |
| `curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'` | `curated.betting_lines_unified` | Consolidated with market_type = 'totals' |
| `curated.sportsbook_mappings` | `curated.sportsbook_mappings` | Enhanced with confidence scoring |
| `curated.teams_master` | `curated.teams_master` | Enhanced with external ID support |
| `curated.data_sources` | `curated.data_sources` | Enhanced with reliability tracking |
| `operational.schema_migrations` | `operational.schema_migrations` | Moved to operational schema |

### Query Pattern Updates

#### Simple Table References
```sql
-- OLD
SELECT * FROM curated.games_complete;

-- NEW
SELECT * FROM curated.games_complete;
```

#### Betting Lines Consolidation
```sql
-- OLD (Multiple queries)
SELECT * FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline' WHERE game_id = 123;
SELECT * FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread's WHERE game_id = 123;
SELECT * FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals' WHERE game_id = 123;

-- NEW (Single unified query)
SELECT * FROM curated.betting_lines_unified 
WHERE game_id = 123 AND market_type IN ('moneyline', 'spread', 'totals');
```

#### Complex Joins
```sql
-- OLD
SELECT g.*, o.outcome 
FROM curated.games_complete g
LEFT JOIN curated.game_outcomes o ON g.id = o.game_id;

-- NEW
SELECT g.*, o.outcome
FROM curated.games_complete g  
LEFT JOIN curated.game_outcomes o ON g.id = o.game_id;
```

## Risk Management

### High-Risk Areas

1. **Data Loss Risk**
   - **Mitigation:** Complete backups before migration
   - **Monitoring:** Real-time row count validation
   - **Rollback:** Automated rollback procedures

2. **Application Downtime**
   - **Mitigation:** Phased migration with feature flags
   - **Monitoring:** Health checks during migration
   - **Rollback:** Blue-green deployment capability

3. **Performance Impact**
   - **Mitigation:** Index optimization and query analysis
   - **Monitoring:** Query performance testing
   - **Rollback:** Performance comparison validation

### Validation Requirements

- [ ] **Zero data loss:** All row counts must match exactly
- [ ] **Data integrity:** All constraints and relationships preserved
- [ ] **Performance:** Query times within 10% of baseline
- [ ] **Business logic:** All existing functionality works unchanged
- [ ] **Foreign keys:** All relationships correctly updated

### Rollback Triggers

Execute emergency rollback if:
- Data loss detected (row count mismatches)
- Critical business logic failures
- Performance degradation >25%
- Foreign key constraint violations
- Application errors in production

## Success Criteria

### Technical Success
- [ ] All core_betting data migrated with 100% integrity
- [ ] All code references updated and tested
- [ ] Query performance within acceptable ranges
- [ ] All automated tests passing
- [ ] Zero production issues

### Business Success
- [ ] No impact on existing functionality
- [ ] Simplified schema reduces maintenance overhead
- [ ] Improved query performance on unified tables
- [ ] Enhanced data consistency and quality

## Support and Troubleshooting

### Common Issues

1. **Row Count Mismatches**
   - Check for concurrent writes during migration
   - Verify migration scripts completed successfully
   - Review migration logs for errors

2. **Performance Degradation**
   - Check index creation on new tables
   - Analyze query execution plans
   - Review table statistics

3. **Foreign Key Errors**
   - Verify FK constraint updates
   - Check for orphaned records
   - Review cascade settings

### Getting Help

- Review migration logs: `SELECT * FROM operational.v_core_betting_migration_status;`
- Check validation reports generated by validation scripts
- Review error logs in application and database
- Consult design document for detailed specifications

### Emergency Contacts

- Database Administrator: [Contact info]
- Application Owner: [Contact info]  
- DevOps Engineer: [Contact info]

## Post-Migration Checklist

After successful migration:

- [ ] Update all documentation
- [ ] Archive migration tools and scripts
- [ ] Schedule database maintenance (VACUUM, ANALYZE)
- [ ] Monitor performance for 24-48 hours
- [ ] Conduct post-migration review meeting
- [ ] Document lessons learned
- [ ] Plan cleanup of backup schemas (after retention period)

---

**Last Updated:** July 24, 2025  
**Version:** 1.0  
**Document Owner:** Core Betting Migration Team