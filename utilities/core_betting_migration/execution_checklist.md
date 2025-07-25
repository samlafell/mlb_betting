# Core Betting Schema Decommission - Execution Checklist

**Phase 2 Implementation**: Data Migration and Code Refactoring

## Pre-Execution Validation ✅

Before beginning Phase 2, ensure Phase 1 validation is complete:

- [ ] ✅ **Phase 1 Reports Reviewed**
  - [x] `core_betting_refactor_report.md` - 2,321 changes identified across 76 files
  - [x] `pre_migration_validation_report.md` - Database analysis complete
  - [x] `PHASE_1_COMPLETION_SUMMARY.md` - Phase 1 achievements documented
  - [x] Comprehensive backup created at `backups/pre_core_betting_migration_20250724_222636/`

- [ ] ✅ **Migration Tools Created**  
  - [x] `automated_code_refactor.py` - Code refactoring automation
  - [x] `data_migration_scripts.sql` - Complete SQL migration scripts 
  - [x] `validation_and_rollback.py` - Validation and emergency rollback system

## Phase 2A: Pre-Migration Validation

### Step 1: Database Connectivity and Access Validation
```bash
# Test database connection and validate pre-conditions
cd /Users/samlafell/Documents/programming_projects/mlb_betting_program
python utilities/core_betting_migration/validation_and_rollback.py --validate-pre-migration
```

**Expected Outcome:** All critical checks pass, warnings acceptable
- [ ] Database connectivity confirmed
- [ ] core_betting schema accessible with all 16 tables
- [ ] 44,311 records confirmed available for migration
- [ ] 25 external FK dependencies documented
- [ ] No blocking database locks
- [ ] Sufficient disk space available
- [ ] Backup availability confirmed

**If Failed:** Review and resolve critical issues before proceeding

### Step 2: Generate Final Code Refactoring Analysis
```bash
# Generate updated refactoring report
python utilities/core_betting_migration/automated_code_refactor.py --report-only
```

**Expected Outcome:** Detailed refactoring report with file-by-file changes
- [ ] Report generated successfully
- [ ] 76 files requiring changes confirmed
- [ ] Complex patterns identified for manual review
- [ ] Schema mapping rules validated

## Phase 2B: Execute Data Migration

### Step 3: Create Additional Pre-Migration Backup
```bash
# Create additional safety backup
pg_dump -h localhost -U postgres -d mlb_betting --schema=core_betting > backups/phase2_pre_migration_$(date +%Y%m%d_%H%M%S).sql
```

**Expected Outcome:** Additional backup created for safety
- [ ] Backup file created successfully
- [ ] Backup size reasonable (~7MB based on Phase 1)

### Step 4: Execute Data Migration Scripts
```bash
# Execute the complete migration (REVIEW SCRIPTS FIRST!)
psql -h localhost -U postgres -d mlb_betting -f utilities/core_betting_migration/data_migration_scripts.sql
```

**Expected Outcome:** All migration phases complete successfully
- [ ] PHASE_1: Pre-migration setup completed
- [ ] PHASE_2: Enhanced curated tables created
- [ ] PHASE_3: Unique data migrated (sportsbooks, teams, mappings)
- [ ] PHASE_4: Primary data migrated (games with consolidation)
- [ ] PHASE_5: Betting lines consolidated (moneyline, spreads, totals → unified)
- [ ] PHASE_6: External FK constraints updated
- [ ] PHASE_7: Comprehensive validation views created
- [ ] PHASE_8: Migration status summary generated

**Migration Monitoring:**
```bash
# Monitor migration progress (in separate terminal)
python utilities/core_betting_migration/validation_and_rollback.py --monitor --monitor-interval 30
```

### Step 5: Post-Migration Validation
```bash
# Comprehensive post-migration validation
python utilities/core_betting_migration/validation_and_rollback.py --validate-post-migration
```

**Expected Outcome:** All validation checks pass
- [ ] ✅ Record counts match (zero data loss)
- [ ] ✅ Data consistency validated
- [ ] ✅ Foreign key integrity confirmed
- [ ] ✅ Curated schema structure correct
- [ ] ✅ Performance indexes created
- [ ] ✅ Betting lines consolidation successful
- [ ] ✅ Games consolidation (including supplementary_games) successful
- [ ] ✅ Query performance within acceptable ranges
- [ ] ✅ External dependencies updated

**If Failed:** Execute emergency rollback:
```bash
python utilities/core_betting_migration/validation_and_rollback.py --rollback --confirm
```

## Phase 2C: Execute Code Refactoring

### Step 6: Create Code Backup
```bash
# Create backup before code refactoring
python utilities/core_betting_migration/automated_code_refactor.py --dry-run
```

**Expected Outcome:** Preview of all code changes
- [ ] Dry run completed successfully
- [ ] Changes previewed and approved
- [ ] Backup strategy confirmed

### Step 7: Execute Automated Code Refactoring
```bash
# Execute automated code refactoring with backup
python utilities/core_betting_migration/automated_code_refactor.py --execute
```

**Expected Outcome:** All core_betting references updated
- [ ] ✅ 76 files processed
- [ ] ✅ 2,321 code changes applied
- [ ] ✅ Schema mappings applied correctly
- [ ] ✅ Backup created before changes
- [ ] ✅ Complex patterns flagged for manual review

### Step 8: Manual Review and Complex Pattern Updates

**High-Priority Manual Updates:**

1. **Betting Lines Query Patterns** - Complex SQL requiring market_type filters:
```python
# Search for remaining betting lines patterns
grep -r "betting_lines_" src/ --include="*.py" --include="*.sql"
```

2. **Action Network Repository** (`src/data/database/action_network_repository.py`):
- [ ] Review 50+ references updated correctly
- [ ] Verify JOIN patterns with unified betting_lines table
- [ ] Update query filters to include market_type conditions

3. **SQL Migration Files** - Update any remaining schema references:
```bash
find sql/ -name "*.sql" -exec grep -l "core_betting" {} \;
```

4. **Configuration Updates** (`src/core/config.py`, `config.toml`):
- [ ] Update schema references in configuration
- [ ] Verify database connection settings
- [ ] Update any hardcoded table names

### Step 9: Comprehensive Testing

```bash
# Run test suite to validate code changes
uv run pytest tests/ -v

# Run integration tests specifically
uv run pytest tests/integration/ -v

# Test CLI commands
uv run -m src.interfaces.cli data status
uv run -m src.interfaces.cli database setup-action-network --test-connection
```

**Expected Outcome:** All tests pass
- [ ] ✅ Unit tests pass
- [ ] ✅ Integration tests pass
- [ ] ✅ CLI commands functional
- [ ] ✅ Database connections work
- [ ] ✅ No import errors

### Step 10: Performance and Functionality Validation

```bash
# Test key application functionality
uv run -m src.interfaces.cli data collect --source action_network --test
uv run -m src.interfaces.cli action-network collect --date today
```

**Expected Outcome:** Core functionality works
- [ ] ✅ Data collection functional
- [ ] ✅ Action Network pipeline operational
- [ ] ✅ Database queries perform within acceptable ranges
- [ ] ✅ No runtime errors

## Phase 2D: Final Validation and Cleanup

### Step 11: Final System Validation
```bash
# Run final comprehensive validation
python utilities/core_betting_migration/validation_and_rollback.py --validate-post-migration --output-file final_validation_report.md
```

**Expected Outcome:** Complete system validation
- [ ] ✅ All data integrity checks pass
- [ ] ✅ All business logic preserved
- [ ] ✅ Performance within acceptable ranges
- [ ] ✅ No foreign key violations
- [ ] ✅ Application functionality confirmed

### Step 12: Generate Phase 2 Completion Report
```bash
# Generate comprehensive completion report
cat > PHASE_2_COMPLETION_SUMMARY.md << 'EOF'
# Phase 2 Completion Summary - Core Betting Schema Decommission

**Completed:** $(date -Iseconds)
**Status:** ✅ PHASE 2 COMPLETE - DATA MIGRATION AND CODE REFACTORING SUCCESS
**Duration:** [FILL IN ACTUAL DURATION]

## Phase 2 Achievements

### ✅ Data Migration Complete
- **Tables Migrated:** All 16 core_betting tables
- **Records Migrated:** 44,311 records with zero data loss
- **Schema Consolidation:** 3 betting_lines tables → 1 unified table
- **Games Consolidation:** games + supplementary_games → games_complete
- **FK Updates:** 25 external foreign key constraints updated

### ✅ Code Refactoring Complete  
- **Files Modified:** 76 files across codebase
- **Code Changes:** 2,321 automated transformations
- **Manual Reviews:** Complex SQL patterns updated
- **Configuration Updates:** All schema references updated
- **Testing:** Complete test suite passes

### ✅ Comprehensive Validation
- **Data Integrity:** 100% validated - no data loss
- **Business Logic:** All functionality preserved
- **Performance:** Query performance within acceptable ranges
- **Foreign Keys:** All relationships intact and updated
- **Application Testing:** Core functionality confirmed

## Critical Success Metrics

| Metric | Target | Achieved | Status |
|--------|--------|----------|--------|
| Data Loss | 0 records | 0 records | ✅ Success |
| Code Coverage | 76 files | 76 files | ✅ Success |
| Test Pass Rate | 100% | 100% | ✅ Success |
| FK Integrity | All preserved | All preserved | ✅ Success |
| Performance | <10% degradation | Within range | ✅ Success |

## Next Steps for Phase 3

1. **Schema Cleanup** - Drop core_betting schema (after 48-hour validation period)
2. **Documentation Update** - Update all project documentation
3. **Performance Monitoring** - Monitor system for 24-48 hours
4. **Team Training** - Update team on new schema structure

**Confidence Level:** HIGH - Ready for production operation with new curated schema
EOF
```

### Step 13: Optional Schema Cleanup (ONLY AFTER 48-hour validation period)

⚠️ **DANGER: ONLY execute after complete validation and stakeholder approval**

```sql
-- Create final backup before cleanup
CREATE SCHEMA core_betting_archive AS SELECT * FROM curated.*;

-- Drop core_betting schema (IRREVERSIBLE!)
-- DROP SCHEMA core_betting CASCADE;
```

**Pre-Cleanup Checklist:**
- [ ] All stakeholders approve schema drop
- [ ] 48+ hours of successful operation with new schema
- [ ] All applications tested and functional
- [ ] Performance validated as acceptable
- [ ] Final backup created
- [ ] Team trained on new schema structure

## Emergency Procedures

### If Migration Fails at Any Step:

1. **Stop immediately** - Do not proceed to next step
2. **Document the failure** - Capture error messages and logs
3. **Execute emergency rollback:**
```bash
python utilities/core_betting_migration/validation_and_rollback.py --rollback --confirm
```

### If Code Refactoring Introduces Issues:

1. **Restore from code backup:**
```bash
# Restore from automated backup
cp -r backups/pre_refactor_backup_TIMESTAMP/* .
```

2. **Review specific patterns** - Focus on files with issues
3. **Apply targeted fixes** - Fix specific issues rather than re-running full refactor

### If Performance Issues Detected:

1. **Monitor query performance:**
```sql
SELECT * FROM operational.v_core_betting_migration_validation;
```

2. **Check index usage:**
```sql
SELECT schemaname, tablename, indexname, idx_scan 
FROM pg_stat_user_indexes 
WHERE schemaname = 'curated' ORDER BY idx_scan;
```

3. **Add additional indexes if needed**

## Communication Plan

### Stakeholder Updates:
- [ ] **Project Manager:** Phase 2 started
- [ ] **Development Team:** Schema changes and testing requirements  
- [ ] **Database Admin:** Migration execution and monitoring
- [ ] **QA Team:** Testing requirements post-migration
- [ ] **Project Manager:** Phase 2 completion status

### Documentation Updates Required:
- [ ] Update README.md with new schema references
- [ ] Update CLAUDE.md with Phase 2 completion
- [ ] Update API documentation with schema changes
- [ ] Update database schema documentation
- [ ] Update development setup guides

---

**Total Estimated Time:** 8-12 hours for complete Phase 2 execution
**Risk Level:** Medium (Well-prepared with comprehensive validation and rollback procedures)
**Success Criteria:** Zero data loss, all functionality preserved, performance acceptable