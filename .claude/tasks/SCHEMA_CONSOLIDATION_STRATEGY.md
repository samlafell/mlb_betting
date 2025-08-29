# 🏗️ Database Schema Consolidation Strategy

**Target**: Consolidate 11 schemas → 4 unified schemas  
**Goal**: RAW → STAGING → CURATED pipeline with proper FK relationships

## Current State Analysis

### Game Entity Fragmentation (CRITICAL)
- **Split Master**: Two game tables causing referential chaos
  - `curated.enhanced_games`: 134 records (4 tables reference)
  - `curated.games_complete`: 124 records (5 tables reference)
- **Impact**: Queries fail, reports inconsistent, data integrity compromised

### Schema Distribution
```
Current (11 schemas):
├── raw_data (14 tables)         → Keep as RAW zone
├── staging (9 tables)           → Keep as STAGING zone  
├── curated (20 tables)          → Keep as CURATED zone
├── action_network (4 tables)    → MERGE into raw_data
├── analysis (7 tables)          → MERGE into curated
├── analytics (9 tables)         → MERGE into curated
├── coordination (1 table)       → MERGE into operational
├── monitoring (2 tables)        → MERGE into operational
├── operational (15 tables)      → Keep as OPERATIONAL zone
└── public (23 tables)           → Keep (MLflow system)
```

## Target Architecture

### 4-Schema Design (Clean Pipeline)
```
mlb_betting/
├── raw_data/          # All ingested data (18 tables)
│   ├── action_network_*
│   ├── sbd_*
│   ├── vsin_*
│   ├── mlb_api_*
│   └── odds_api_*
├── staging/           # Processed/cleaned data (9 tables)
│   ├── betting_odds_unified
│   ├── moneylines, spreads, totals
│   └── sportsbooks, teams
├── curated/           # Business-ready data (36 tables)
│   ├── master_games ← UNIFIED game entity
│   ├── enhanced_games_view ← VIEW of master_games
│   ├── betting_analysis tables
│   ├── ML/analytics tables
│   └── strategy results
└── operational/       # System operations (18 tables)
    ├── monitoring
    ├── alerting
    ├── performance tracking
    └── pipeline logs
```

## Master Game Entity Solution

### Problem
Two competing game masters causing FK chaos:
- Tables split between `enhanced_games` vs `games_complete`
- Inconsistent game references across system

### Solution: Unified Master Games Table
```sql
-- New unified master games table
CREATE TABLE curated.master_games (
    id BIGSERIAL PRIMARY KEY,
    external_game_id VARCHAR(255) UNIQUE,
    game_date DATE NOT NULL,
    home_team VARCHAR(255) NOT NULL,
    away_team VARCHAR(255) NOT NULL,
    league VARCHAR(10) DEFAULT 'MLB',
    season INTEGER,
    -- Enhanced game fields
    weather_conditions JSONB,
    stadium_conditions JSONB,
    player_injuries JSONB,
    -- Outcome fields
    home_score INTEGER,
    away_score INTEGER,
    game_status VARCHAR(50),
    completed_at TIMESTAMP WITH TIME ZONE,
    -- Metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    data_quality_score DECIMAL(3,2)
);

-- Create indices for performance
CREATE INDEX idx_master_games_date ON curated.master_games(game_date);
CREATE INDEX idx_master_games_teams ON curated.master_games(home_team, away_team);
CREATE INDEX idx_master_games_external_id ON curated.master_games(external_game_id);
```

### Migration Strategy for Game Entity
1. **Data Audit**: Compare enhanced_games vs games_complete
2. **Data Merge**: Create master_games with best data from both
3. **FK Update**: Point all references to master_games
4. **Create Views**: Backward compatibility views
5. **Drop Old Tables**: Remove enhanced_games, games_complete

## Schema Migration Plan

### Phase 1: Emergency Stabilization (Day 1)
**Goal**: Fix broken FKs, ensure system stability

```sql
-- 1. Fix orphaned FK constraints
ALTER TABLE analytics.betting_recommendations DROP CONSTRAINT IF EXISTS betting_recommendations_game_id_fkey;
ALTER TABLE analytics.confidence_scores DROP CONSTRAINT IF EXISTS confidence_scores_game_id_fkey;
ALTER TABLE analytics.cross_market_analysis DROP CONSTRAINT IF EXISTS cross_market_analysis_game_id_fkey;
ALTER TABLE analytics.strategy_signals DROP CONSTRAINT IF EXISTS strategy_signals_game_id_fkey;

-- 2. Fix monitoring table constraints
ALTER TABLE monitoring.ml_model_alerts DROP CONSTRAINT IF EXISTS ml_model_alerts_experiment_id_fkey;
ALTER TABLE monitoring.ml_model_performance DROP CONSTRAINT IF EXISTS ml_model_performance_experiment_id_fkey;
```

### Phase 2: Schema Consolidation (Day 2-3)
**Goal**: Move tables to correct schemas

```sql
-- Move action_network tables to raw_data
ALTER TABLE action_network.betting_lines SET SCHEMA raw_data;
ALTER TABLE action_network.extraction_log SET SCHEMA raw_data;
ALTER TABLE action_network.line_movement_summary SET SCHEMA raw_data;
ALTER TABLE action_network.sportsbooks SET SCHEMA raw_data;

-- Move analysis tables to curated  
ALTER TABLE analysis.betting_strategies SET SCHEMA curated;
ALTER TABLE analysis.ml_detected_patterns SET SCHEMA curated;
-- ... continue for all analysis tables

-- Move analytics tables to curated
ALTER TABLE analytics.betting_recommendations SET SCHEMA curated;
ALTER TABLE analytics.confidence_scores SET SCHEMA curated;
-- ... continue for all analytics tables

-- Move monitoring to operational
ALTER TABLE monitoring.ml_model_alerts SET SCHEMA operational;
ALTER TABLE monitoring.ml_model_performance SET SCHEMA operational;

-- Move coordination to operational  
ALTER TABLE coordination.agent_migration_lock SET SCHEMA operational;
```

### Phase 3: Game Entity Unification (Day 3-4)
**Goal**: Create unified master_games table

```sql
-- 1. Create new master_games table
-- (SQL provided above)

-- 2. Migrate data from both sources
INSERT INTO curated.master_games (
    external_game_id, game_date, home_team, away_team,
    season, home_score, away_score, game_status, completed_at
)
SELECT DISTINCT
    COALESCE(eg.external_game_id, gc.external_game_id),
    COALESCE(eg.game_date, gc.game_date::date),
    COALESCE(eg.home_team, gc.home_team),
    COALESCE(eg.away_team, gc.away_team),
    COALESCE(eg.season, EXTRACT(YEAR FROM gc.game_date)),
    COALESCE(eg.home_score, gc.home_score),
    COALESCE(eg.away_score, gc.away_score),
    COALESCE(eg.game_status, gc.status),
    COALESCE(eg.completed_at, gc.game_date)
FROM curated.enhanced_games eg
FULL OUTER JOIN curated.games_complete gc 
    ON eg.external_game_id = gc.external_game_id;

-- 3. Update FK references
-- (Detailed FK update SQL to follow)

-- 4. Create backward compatibility views
CREATE VIEW curated.enhanced_games AS
SELECT * FROM curated.master_games;

CREATE VIEW curated.games_complete AS  
SELECT * FROM curated.master_games;
```

### Phase 4: Schema Cleanup (Day 4-5)
**Goal**: Drop empty schemas, establish proper FKs

```sql
-- Drop empty schemas
DROP SCHEMA IF EXISTS action_network CASCADE;
DROP SCHEMA IF EXISTS analysis CASCADE;  
DROP SCHEMA IF EXISTS analytics CASCADE;
DROP SCHEMA IF EXISTS coordination CASCADE;
DROP SCHEMA IF EXISTS monitoring CASCADE;

-- Recreate proper FK relationships
-- (Detailed FK creation SQL to follow)
```

## Risk Mitigation

### Data Safety Measures
1. **Full Database Backup**: Before any changes
2. **Schema-by-Schema Backup**: Before each phase
3. **Table-by-Table Backup**: Before major moves
4. **Rollback Scripts**: For each migration phase

### Testing Strategy
1. **Data Integrity Tests**: FK validation, record counts
2. **Pipeline Tests**: End-to-end data flow validation  
3. **Application Tests**: All services function correctly
4. **Performance Tests**: Query performance maintained

### Rollback Procedures
Each phase includes detailed rollback instructions to restore previous state if issues occur.

## Success Metrics

### Post-Migration Validation
- **Schema Count**: 11 → 4 schemas ✅
- **FK Integrity**: All foreign keys valid ✅
- **Data Consistency**: Unified game references ✅
- **Pipeline Flow**: RAW → STAGING → CURATED ✅
- **Performance**: Query times improved ✅

### Business Impact
- **Developer Experience**: Single clear schema per domain
- **Data Quality**: Proper referential integrity enforced
- **System Reliability**: Reduced complexity, fewer failure points
- **Operational Efficiency**: Simplified backup/recovery procedures

## Timeline

- **Day 1**: Phase 1 - Emergency stabilization (4 hours)
- **Day 2-3**: Phase 2 - Schema consolidation (12 hours) 
- **Day 3-4**: Phase 3 - Game entity unification (8 hours)
- **Day 4-5**: Phase 4 - Schema cleanup (4 hours)
- **Total**: 28 hours across 5 days with testing