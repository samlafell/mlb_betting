# Pre-Migration Validation Report

**Generated:** 2025-07-24T22:39:00.524740
**Overall Status:** WARNING

## Summary

- **Total Checks:** 9
- **Passed:** 8
- **Warnings:** 1
- **Failed:** 0
- **Core Betting Tables:** 21
- **Total Records:** 0
- **Foreign Key Dependencies:** 9

## Validation Checks

### ✅ Database Connectivity

**Status:** PASSED
**Message:** Successfully connected to database

**Details:**
```json
{
  "version": "PostgreSQL 17.5 (Homebrew) on aarch64-apple-darwin24.4.0, compiled by Apple clang version 17.0.0 (clang-1700.0.13.3), 64-bit"
}
```

### ✅ Schema Access

**Status:** PASSED
**Message:** core_betting schema accessible with 21 tables

### ✅ Data Integrity

**Status:** PASSED
**Message:** No data integrity issues found

### ⚠️ Foreign Key Dependencies

**Status:** WARNING
**Message:** Found 9 external FK dependencies

**Details:**
```json
{
  "dependencies": [
    "analytics.betting_recommendations -> games.id",
    "analytics.confidence_scores -> games.id",
    "analytics.cross_market_analysis -> games.id",
    "analytics.strategy_signals -> games.id",
    "curated.arbitrage_opportunities -> sportsbooks.id",
    "curated.arbitrage_opportunities -> sportsbooks.id",
    "curated.rlm_opportunities -> sportsbooks.id",
    "curated.steam_moves -> games.id",
    "staging.betting_splits -> games.id"
  ]
}
```

### ✅ Database Locks

**Status:** PASSED
**Message:** No blocking database locks detected

### ✅ Disk Space

**Status:** PASSED
**Message:** Database size: 342 MB

**Details:**
```json
{
  "database_size": "342 MB"
}
```

### ✅ Backup Availability

**Status:** PASSED
**Message:** Backup available: pre_core_betting_migration_20250724_222636

**Details:**
```json
{
  "backup_path": "backups/pre_core_betting_migration_20250724_222636"
}
```

### ✅ Target Schema Readiness

**Status:** PASSED
**Message:** curated schema ready for migration

### ✅ Performance Baseline

**Status:** PASSED
**Message:** Performance baseline established

**Details:**
```json
{
  "games_count": {
    "duration_ms": 0.37,
    "result_count": 3186
  },
  "betting_lines_join": {
    "duration_ms": 3.72,
    "result_count": 12410
  }
}
```

## Recommendations

- ⚠️ Review warnings before proceeding
-   - Foreign Key Dependencies: Found 9 external FK dependencies