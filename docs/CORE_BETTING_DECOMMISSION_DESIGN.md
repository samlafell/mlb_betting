# Core Betting Schema Decommissioning Design

**Document Status:** Design Specification  
**Created:** July 24, 2025  
**Version:** 1.0  

## Executive Summary

This document provides a comprehensive design for decommissioning the `core_betting` schema and migrating all data and references to the modern 3-schema architecture (`raw_data`, `staging`, `curated`). The migration will eliminate redundancy, simplify the data model, and align with the project's unified architecture.

## Current State Analysis

### Schema Structure Overview
- **Total Schemas:** 12 (including core_betting)
- **Core_betting Tables:** 16 tables containing 44,866 total records
- **Code References:** 100+ files referencing core_betting across Python, SQL, and documentation
- **Foreign Key Dependencies:** 71 relationships across schemas

### Key Findings
1. **Data Redundancy:** Significant overlap between core_betting and curated schemas
2. **Unique Data:** 4 tables contain data not available elsewhere
3. **Code Dependencies:** Extensive references throughout CLI, services, and utilities
4. **Business Logic:** Core_betting serves as primary read source for many operations

## Migration Strategy Design

### Phase 1: Data Preservation and Migration

#### 1.1 Unique Data Migration Plan

**Target Migrations:**

```yaml
curated.sportsbook_mappings (19 rows):
  target: curated.sportsbook_mappings
  reason: Critical for cross-source data correlation
  migration_type: full_copy_with_enhancements

curated.data_sources (7 rows):
  target: curated.data_sources
  reason: Source configuration and tracking
  migration_type: full_copy_with_schema_updates

curated.teams_master (30 rows):
  target: curated.teams_master
  reason: Standardized team reference data
  migration_type: full_copy_with_validation

operational.schema_migrations (3 rows):
  target: operational.schema_migrations
  reason: System evolution documentation
  migration_type: archive_copy
```

#### 1.2 Primary Data Consolidation

**Betting Lines Consolidation:**
```yaml
source_tables:
  - curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline' (12,410 rows)
  - curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals' (10,568 rows) 
  - curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread' (3,360 rows)

target_strategy:
  primary: curated.betting_lines_unified
  backup: staging.betting_lines_historical
  
consolidation_approach:
  - Merge all betting line types into unified structure
  - Preserve original timestamps and lineage
  - Add market_type discriminator column
  - Maintain all historical data
```

**Games Data Consolidation:**
```yaml
source_tables:
  - curated.games_complete (3,186 rows) - Primary
  - curated.games_complete (252 rows)
  - curated.game_outcomes (1,465 rows)

target_strategy:
  primary: curated.games_complete
  outcomes: curated.game_outcomes
  
consolidation_approach:
  - Merge games and supplementary_games into single table
  - Move outcomes to dedicated table in curated
  - Preserve MLB Stats API game IDs
  - Maintain all relationships and foreign keys
```

#### 1.3 Migration Validation Requirements

**Data Integrity Checks:**
```sql
-- Validation queries to ensure no data loss
SELECT 
    'core_betting' as source_schema,
    COUNT(*) as total_records,
    COUNT(DISTINCT external_game_id) as unique_games
FROM curated.games_complete;

-- Compare with target after migration
SELECT 
    'curated' as target_schema,
    COUNT(*) as total_records,
    COUNT(DISTINCT external_game_id) as unique_games
FROM curated.games_complete;
```

### Phase 2: Code Refactoring Strategy

#### 2.1 Schema Reference Mapping

**Code Reference Categories:**

1. **CLI Commands (5 files, 50+ references)**
   ```python
   # Current pattern:
   FROM curated.games_complete g
   
   # Target pattern:
   FROM curated.games_complete g
   ```

2. **Services Layer (3 files, 20+ references)**
   ```python
   # Current pattern:
   query = "SELECT * FROM curated.game_outcomes"
   
   # Target pattern:
   query = "SELECT * FROM curated.game_outcomes"
   ```

3. **Database Repositories (1 file, 5+ references)**
   ```python
   # Current pattern:
   table_name = "curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'"
   
   # Target pattern:
   table_name = "curated.betting_lines_unified"
   ```

4. **Utility Scripts (15+ files, 100+ references)**
   ```python
   # Bulk replacement pattern needed
   s/core_betting\./curated\./g
   ```

#### 2.2 Schema Configuration Updates

**Centralized Schema Configuration:**
```python
# src/core/schemas.py (NEW FILE)
class SchemaConfig:
    RAW = "raw_data"
    STAGING = "staging" 
    CURATED = "curated"
    
    # Legacy mapping for transition period
    LEGACY_MAPPINGS = {
        "curated.games_complete": "curated.games_complete",
        "curated.game_outcomes": "curated.game_outcomes",
        "curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'": "curated.betting_lines_unified",
        "curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals'": "curated.betting_lines_unified",
        "curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread'": "curated.betting_lines_unified",
        "curated.sportsbooks": "curated.sportsbooks",
        "curated.teams_master": "curated.teams_master",
    }
```

#### 2.3 Database Query Updates

**Query Pattern Replacements:**

1. **Simple Table References:**
   ```sql
   -- OLD
   SELECT * FROM curated.games_complete;
   
   -- NEW  
   SELECT * FROM curated.games_complete;
   ```

2. **Betting Lines Queries:**
   ```sql
   -- OLD (Multiple tables)
   SELECT * FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline'
   UNION ALL
   SELECT * FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals';
   
   -- NEW (Unified table)
   SELECT * FROM curated.betting_lines_unified 
   WHERE market_type IN ('moneyline', 'totals');
   ```

3. **Complex Joins:**
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

### Phase 3: Database Schema Updates

#### 3.1 New Table Creation Scripts

**Enhanced Curated Tables:**
```sql
-- curated.games_complete (replaces curated.games_complete + supplementary_games)
CREATE TABLE curated.games_complete (
    id SERIAL PRIMARY KEY,
    external_game_id VARCHAR(255) UNIQUE NOT NULL,
    mlb_stats_api_game_id VARCHAR(50),
    home_team_normalized VARCHAR(100) NOT NULL,
    away_team_normalized VARCHAR(100) NOT NULL,
    game_date DATE NOT NULL,
    game_datetime TIMESTAMP WITH TIME ZONE,
    season INTEGER,
    venue VARCHAR(255),
    
    -- Supplementary fields
    weather_conditions JSONB,
    attendance INTEGER,
    game_status VARCHAR(50),
    
    -- Metadata
    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
    validation_status VARCHAR(20) DEFAULT 'valid',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    migrated_from_core_betting BOOLEAN DEFAULT FALSE
);

-- curated.betting_lines_unified (replaces 3 core_betting betting_lines tables)
CREATE TABLE curated.betting_lines_unified (
    id SERIAL PRIMARY KEY,
    game_id INTEGER NOT NULL REFERENCES curated.games_complete(id),
    sportsbook_id INTEGER NOT NULL,
    market_type VARCHAR(20) NOT NULL, -- 'moneyline', 'spread', 'totals'
    side VARCHAR(10), -- 'home', 'away', 'over', 'under'
    
    -- Odds data
    odds INTEGER,
    line_value DECIMAL(10,2), -- NULL for moneyline, spread value for spreads, total for totals
    
    -- Timestamps
    odds_datetime TIMESTAMP WITH TIME ZONE NOT NULL,
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Metadata
    data_source VARCHAR(50),
    external_odds_id VARCHAR(255),
    is_current BOOLEAN DEFAULT FALSE,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    migrated_from_core_betting BOOLEAN DEFAULT FALSE,
    
    CONSTRAINT valid_market_type CHECK (market_type IN ('moneyline', 'spread', 'totals')),
    CONSTRAINT valid_side CHECK (side IN ('home', 'away', 'over', 'under'))
);

-- Enhanced indexing for performance
CREATE INDEX idx_betting_lines_unified_game_market ON curated.betting_lines_unified(game_id, market_type);
CREATE INDEX idx_betting_lines_unified_odds_datetime ON curated.betting_lines_unified(odds_datetime);
CREATE INDEX idx_betting_lines_unified_current ON curated.betting_lines_unified(is_current) WHERE is_current = TRUE;
```

#### 3.2 Migration Scripts

**Data Migration SQL:**
```sql
-- Migration script template
DO $$
DECLARE
    migration_start TIMESTAMP;
    records_migrated INTEGER := 0;
BEGIN
    migration_start := NOW();
    
    -- Log migration start
    INSERT INTO operational.schema_migrations (
        migration_name, started_at, status
    ) VALUES (
        'core_betting_to_curated_games', migration_start, 'in_progress'
    );
    
    -- Migrate games data
    INSERT INTO curated.games_complete (
        external_game_id, mlb_stats_api_game_id, home_team_normalized,
        away_team_normalized, game_date, game_datetime, season, venue,
        data_quality_score, validation_status, migrated_from_core_betting
    )
    SELECT 
        external_game_id, mlb_stats_api_game_id, home_team_normalized,
        away_team_normalized, game_date, game_datetime, season, venue,
        data_quality_score, validation_status, TRUE
    FROM curated.games_complete
    ON CONFLICT (external_game_id) DO NOTHING;
    
    GET DIAGNOSTICS records_migrated = ROW_COUNT;
    
    -- Log migration completion
    UPDATE operational.schema_migrations 
    SET completed_at = NOW(), 
        status = 'completed',
        records_migrated = records_migrated
    WHERE migration_name = 'core_betting_to_curated_games';
    
    RAISE NOTICE 'Migrated % games records from core_betting to curated', records_migrated;
END $$;
```

### Phase 4: Implementation Timeline

#### 4.1 Pre-Migration Phase (Days 1-3)
1. **Backup Creation**
   - Full database backup
   - Create `backup_core_betting` schema copy
   - Document current row counts and checksums

2. **New Schema Preparation**
   - Create enhanced curated tables
   - Set up monitoring and logging
   - Prepare rollback procedures

3. **Code Analysis**
   - Final scan for all core_betting references
   - Prepare replacement scripts
   - Set up testing environment

#### 4.2 Data Migration Phase (Days 4-6)
1. **Unique Data Migration**
   - Migrate sportsbook_external_mappings
   - Migrate data_source_metadata
   - Migrate teams master data
   - Migrate data_migrations to operational

2. **Primary Data Migration**
   - Consolidate betting lines tables
   - Merge games and supplementary_games
   - Migrate game_outcomes
   - Validate data integrity

3. **Reference Updates**
   - Update foreign key relationships
   - Refresh materialized views
   - Update statistics

#### 4.3 Code Refactoring Phase (Days 7-10)
1. **Core Code Updates**
   - Update CLI commands
   - Update service layer
   - Update repository patterns
   - Update utility scripts

2. **Configuration Updates**
   - Add schema configuration module
   - Update connection strings
   - Update query builders

3. **Testing and Validation**
   - Run comprehensive test suite
   - Validate business logic functionality
   - Performance testing

#### 4.4 Cleanup Phase (Days 11-12)
1. **Schema Removal**
   - Drop core_betting schema (after validation)
   - Remove backup schemas (if not needed)
   - Clean up migration artifacts

2. **Documentation Updates**
   - Update all documentation
   - Update README files
   - Update CLAUDE.md instructions

## Risk Assessment and Mitigation

### High-Risk Areas

1. **Data Loss Risk**
   - **Mitigation:** Complete backups, validation queries, staged rollout
   - **Monitoring:** Real-time row count tracking, checksum validation

2. **Application Downtime**
   - **Mitigation:** Blue-green deployment, feature flags, rollback procedures
   - **Monitoring:** Health checks, automated testing

3. **Performance Impact**
   - **Mitigation:** Index optimization, query plan analysis, load testing
   - **Monitoring:** Query performance metrics, response time tracking

### Rollback Strategy

```sql
-- Emergency rollback procedure
DO $$
BEGIN
    -- Restore core_betting schema from backup
    DROP SCHEMA IF EXISTS core_betting CASCADE;
    CREATE SCHEMA core_betting;
    
    -- Restore tables from backup_core_betting
    -- (Detailed restore scripts would be prepared)
    
    -- Revert code deployments
    -- (Git rollback to previous version)
    
    RAISE NOTICE 'Emergency rollback completed';
END $$;
```

## Success Criteria

### Technical Criteria
- [ ] All core_betting data successfully migrated with 100% integrity
- [ ] All code references updated and tested
- [ ] No performance degradation (response times within 10% of baseline)
- [ ] All automated tests passing
- [ ] Zero data loss verified through checksums

### Business Criteria  
- [ ] All existing functionality works without changes
- [ ] No impact on data collection or analysis workflows
- [ ] Simplified schema reduces maintenance overhead
- [ ] Improved query performance on unified tables

## Conclusion

This decommissioning design provides a comprehensive, low-risk approach to eliminating the core_betting schema while preserving all data and functionality. The phased approach allows for validation at each step and provides multiple rollback opportunities.

The migration will result in:
- **Simplified Architecture:** 3-schema design (raw_data, staging, curated)
- **Reduced Redundancy:** Consolidated betting lines and games data
- **Improved Performance:** Optimized queries on unified tables
- **Enhanced Maintainability:** Single source of truth for business data

**Estimated Timeline:** 12 days for complete migration
**Estimated Effort:** 60-80 hours of development and testing
**Risk Level:** Medium (with proper backup and validation procedures)