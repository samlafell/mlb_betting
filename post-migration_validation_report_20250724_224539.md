# Post-Migration Validation Report

**Generated:** 2025-07-24T22:45:39.976446
**Overall Status:** FAILED

## Summary

- **Total Checks:** 9
- **Passed:** 8
- **Warnings:** 0
- **Failed:** 1
- **Curated Tables Created:** 25
- **Records Migrated:** 20934
- **Migration Duration:** 0:00:00.268675

## Validation Checks

### ❌ Record Count Validation

**Status:** FAILED
**Message:** Record count validation failed: relation "operational.v_core_betting_migration_validation" does not exist
LINE 4:                 FROM operational.v_core_betting_migration_va...
                             ^


### ✅ Data Consistency

**Status:** PASSED
**Message:** Data consistency validated successfully

### ✅ Foreign Key Integrity

**Status:** PASSED
**Message:** All foreign key constraints validated

### ✅ Curated Schema Structure

**Status:** PASSED
**Message:** All expected curated tables created

### ✅ Index Creation

**Status:** PASSED
**Message:** 29 performance indexes created

### ✅ Betting Lines Consolidation

**Status:** PASSED
**Message:** Betting lines consolidated successfully: 16223 total lines

**Details:**
```json
{
  "moneyline": 10190,
  "spread": 3360,
  "totals": 2673
}
```

### ✅ Games Consolidation

**Status:** PASSED
**Message:** Games consolidated: 3186 total games (original: 3186, supplementary: 3186)

### ✅ Query Performance

**Status:** PASSED
**Message:** Query performance within acceptable ranges

**Details:**
```json
{
  "games_count": {
    "duration_ms": 0.58,
    "result_count": 3186
  },
  "betting_lines_unified": {
    "duration_ms": 5.48,
    "result_count": 10190
  }
}
```

### ✅ External Dependencies

**Status:** PASSED
**Message:** 1 external FK constraints updated to curated schema

## Recommendations

- ❌ Critical issues detected - consider rollback
- Run: python validation_and_rollback.py --rollback --confirm
-   - Record Count Validation: Record count validation failed: relation "operational.v_core_betting_migration_validation" does not exist
LINE 4:                 FROM operational.v_core_betting_migration_va...
                             ^
