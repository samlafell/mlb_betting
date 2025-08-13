# DATA_MODEL_IMPROVEMENTS.md

## Executive Summary

This document identifies critical weaknesses in the current staging data model and proposes comprehensive improvements to address data usability, lineage tracking, and analytical efficiency issues.

## Current State Analysis

### Staging Schema Overview

The current staging layer consists of:
- `staging.moneylines` (668 records)
- `staging.spreads` (3 records) 
- `staging.totals` (3 records)
- `staging.betting_lines` (general catch-all)
- `staging.action_network_odds_historical` (comprehensive historical table)

### Critical Issues Identified

#### 1. **Missing Source Attribution** 🚨 CRITICAL
**Problem**: Cannot determine data source (Action Network, VSIN, SBD)
- No `source` or `data_source` field in staging tables
- Impossible to track data lineage or validate against source systems
- Prevents source-specific quality scoring

**Impact**: 
- Cannot identify which collector provided the data
- Debugging issues requires manual investigation
- Cross-source analysis impossible

#### 2. **100% Missing Sportsbook Names** 🚨 CRITICAL
**Problem**: All records have `sportsbook_name` = NULL despite having `sportsbook_id`
```sql
-- Query: staging.moneylines analysis
total_records: 668 | missing_sbook_names: 668 (100%)
```

**Root Cause**: Staging processor not resolving sportsbook IDs to names
- Raw data contains sportsbook_key values (71, 123, 79, 972, etc.)
- No mapping from sportsbook_id to human-readable names
- Users cannot identify "DraftKings" vs "FanDuel" without manual lookup

#### 3. **100% Missing Team Information** 🚨 CRITICAL  
**Problem**: All records have empty team names
```sql
-- Query results
missing_home_teams: 668 (100%)
missing_away_teams: 668 (100%)
```

**Impact**: 
- Cannot identify "Yankees vs Red Sox"
- Impossible to determine which team is home/away
- Critical for betting analysis and user interfaces

#### 4. **Excessive Data Duplication** 🚨 HIGH
**Problem**: Single game generates 12+ records per sportsbook
```sql
-- Game 258900 duplication example
sportsbook_id: 972 = 12 records
sportsbook_id: 123 = 12 records  
sportsbook_id: 79 = 10 records
```

**Root Cause**: 
- Staging processor creating separate records for each bet side (home/away, over/under)
- Should be consolidated into paired records
- Creates 2-4x more records than necessary

#### 5. **Poor Data Lineage Tracking** 🚨 HIGH
**Problem**: Cannot trace staging records back to raw data
- `raw_moneylines_id` references staging records, not raw sources
- No clear path from `raw_data.action_network_odds` → staging tables
- Missing transformation metadata

#### 6. **Fragmented Bet Type Design** 🚨 MEDIUM
**Problem**: Separate tables for moneylines/spreads/totals create complexity
- Requires JOINs for comprehensive analysis
- Inconsistent schemas across bet types
- Different field names for similar concepts

#### 7. **Design Pattern Inconsistency** 🚨 MEDIUM
**Problem**: Two competing patterns in staging
- **Pattern A**: `staging.action_network_odds_historical` (comprehensive, well-designed)
- **Pattern B**: `staging.{moneylines,spreads,totals}` (fragmented, problematic)

**Confusion**: Why maintain both patterns?

## Proposed Improvements

### 1. Unified Staging Table Design

Create a single, comprehensive staging table that consolidates all bet types:

```sql
CREATE TABLE staging.betting_odds_unified (
    id BIGSERIAL PRIMARY KEY,
    
    -- SOURCE ATTRIBUTION (FIXES ISSUE #1)
    data_source VARCHAR(50) NOT NULL,  -- 'action_network', 'vsin', 'sbd'
    source_collector VARCHAR(100),     -- Specific collector used
    
    -- GAME IDENTIFICATION
    external_game_id VARCHAR(255) NOT NULL,
    mlb_stats_api_game_id VARCHAR(50),
    game_date DATE,
    home_team VARCHAR(100) NOT NULL,   -- FIXES ISSUE #3
    away_team VARCHAR(100) NOT NULL,   -- FIXES ISSUE #3
    
    -- SPORTSBOOK IDENTIFICATION (FIXES ISSUE #2)
    sportsbook_external_id VARCHAR(50) NOT NULL,
    sportsbook_id INTEGER,
    sportsbook_name VARCHAR(100) NOT NULL,  -- REQUIRED, RESOLVED FROM MAPPING
    
    -- UNIFIED BET DATA (FIXES ISSUE #6)
    market_type VARCHAR(20) NOT NULL,  -- 'moneyline', 'spread', 'total'
    
    -- MONEYLINE DATA
    home_moneyline_odds INTEGER,
    away_moneyline_odds INTEGER,
    
    -- SPREAD DATA  
    spread_line DECIMAL(4,1),
    home_spread_odds INTEGER,
    away_spread_odds INTEGER,
    
    -- TOTALS DATA
    total_line DECIMAL(4,1), 
    over_odds INTEGER,
    under_odds INTEGER,
    
    -- DATA LINEAGE (FIXES ISSUE #5)
    raw_data_table VARCHAR(100),      -- 'raw_data.action_network_odds'
    raw_data_id BIGINT,               -- Reference to source raw record
    transformation_metadata JSONB,    -- Processor details, quality scores
    
    -- QUALITY AND TIMING
    data_quality_score DECIMAL(3,2) DEFAULT 1.0,
    validation_status VARCHAR(20) DEFAULT 'pending',
    collected_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- CONSTRAINTS
    UNIQUE(external_game_id, sportsbook_external_id, market_type, collected_at),
    
    CONSTRAINT valid_market_types CHECK (market_type IN ('moneyline', 'spread', 'total')),
    CONSTRAINT valid_data_source CHECK (data_source IN ('action_network', 'vsin', 'sbd')),
    
    -- Ensure sportsbook_name is always populated
    CONSTRAINT sportsbook_name_required CHECK (sportsbook_name IS NOT NULL AND sportsbook_name != ''),
    
    -- Ensure team names are populated  
    CONSTRAINT team_names_required CHECK (
        home_team IS NOT NULL AND home_team != '' AND
        away_team IS NOT NULL AND away_team != ''
    )
);
```

### 2. Enhanced Sportsbook Resolution

Fix sportsbook name mapping in staging processor:

```python
# Enhanced sportsbook mapping with external_id resolution
SPORTSBOOK_ID_MAPPING = {
    '15': {'name': 'FanDuel', 'id': 15},
    '30': {'name': 'DraftKings', 'id': 30}, 
    '68': {'name': 'BetMGM', 'id': 68},
    '69': {'name': 'Caesars', 'id': 69},
    '71': {'name': 'Bet365', 'id': 71},
    '75': {'name': 'Pinnacle', 'id': 75},
    '79': {'name': 'Circa Sports', 'id': 79},
    '123': {'name': 'PointsBet', 'id': 123},
    '972': {'name': 'Fanatics', 'id': 972}
}

def resolve_sportsbook_info(sportsbook_external_id: str) -> dict:
    """Resolve sportsbook external ID to name and internal ID."""
    mapping = SPORTSBOOK_ID_MAPPING.get(str(sportsbook_external_id))
    if not mapping:
        raise ValueError(f"Unknown sportsbook_external_id: {sportsbook_external_id}")
    return mapping
```

### 3. Improved Deduplication Strategy

Implement intelligent record consolidation:

```python
def consolidate_bet_sides(records: List[StagingRecord]) -> List[StagingRecord]:
    """
    Consolidate separate side records into unified records.
    
    Example:
    Input:  [home_ml: -120], [away_ml: +100]  
    Output: [home_ml: -120, away_ml: +100]
    """
    consolidated = {}
    
    for record in records:
        key = (record.external_game_id, record.sportsbook_id, record.market_type)
        
        if key not in consolidated:
            consolidated[key] = create_unified_record(record)
        else:
            merge_bet_sides(consolidated[key], record)
    
    return list(consolidated.values())
```

### 4. Team Name Population Strategy

Enhanced team name extraction from game data:

```python
async def populate_team_names(record: StagingRecord) -> StagingRecord:
    """Extract team names from game data or external APIs."""
    
    # Strategy 1: Extract from raw_data JSON
    if record.raw_data and 'home_team' in record.raw_data:
        record.home_team = normalize_team_name(record.raw_data['home_team'])
        record.away_team = normalize_team_name(record.raw_data['away_team'])
        return record
    
    # Strategy 2: Lookup by game_id from games table
    game_info = await get_game_info(record.external_game_id)
    if game_info:
        record.home_team = game_info.home_team
        record.away_team = game_info.away_team
        return record
        
    # Strategy 3: MLB Stats API lookup
    mlb_game = await mlb_stats_api_lookup(record.external_game_id)
    if mlb_game:
        record.home_team = mlb_game.home_team
        record.away_team = mlb_game.away_team
        return record
    
    raise ValueError(f"Cannot resolve team names for game {record.external_game_id}")
```

### 5. Analytical Views for Backwards Compatibility

Create views that maintain existing interfaces:

```sql
-- Backwards compatibility view for existing moneylines queries
CREATE VIEW staging.moneylines_compatible AS
SELECT 
    id,
    external_game_id as game_id,
    sportsbook_id,
    sportsbook_name,
    home_moneyline_odds as home_odds,
    away_moneyline_odds as away_odds,
    home_team as home_team_normalized,
    away_team as away_team_normalized,
    data_quality_score,
    validation_status,
    processed_at
FROM staging.betting_odds_unified 
WHERE market_type = 'moneyline';

-- Enhanced analytical view
CREATE VIEW staging.v_complete_odds_analysis AS
SELECT 
    external_game_id,
    home_team,
    away_team,
    sportsbook_name,
    data_source,
    
    -- All odds in one row for easy analysis
    home_moneyline_odds,
    away_moneyline_odds,
    spread_line,
    home_spread_odds, 
    away_spread_odds,
    total_line,
    over_odds,
    under_odds,
    
    -- Quality metrics
    data_quality_score,
    collected_at,
    processed_at
    
FROM staging.betting_odds_unified
ORDER BY external_game_id, sportsbook_name;
```

## Implementation Roadmap

### Phase 1: Create Unified Table (Week 1)
1. Create `staging.betting_odds_unified` table
2. Add enhanced sportsbook mapping
3. Implement team name resolution logic
4. Create data migration script

### Phase 2: Update Staging Processor (Week 1)
1. Modify `StagingZoneProcessor` to use unified table
2. Implement deduplication logic
3. Add proper source attribution
4. Enhance data lineage tracking

### Phase 3: Create Compatibility Layer (Week 2)  
1. Create backwards-compatible views
2. Update existing queries to use new views
3. Add comprehensive analytical views
4. Document new query patterns

### Phase 4: Migration and Testing (Week 2)
1. Migrate existing staging data
2. Run parallel systems for validation
3. Update all dependent processes
4. Deprecate old fragmented tables

## Expected Benefits

### Immediate Improvements
- **Source Attribution**: Clear tracking of data origins
- **Sportsbook Resolution**: Human-readable sportsbook names
- **Team Identification**: Clear home/away team names
- **Reduced Duplication**: 50-75% fewer staging records

### Long-term Benefits
- **Simplified Analysis**: Single table for all bet types
- **Better Data Quality**: Comprehensive validation and scoring
- **Improved Performance**: Fewer JOINs, better indexing
- **Enhanced Debugging**: Clear data lineage tracking

## Migration Strategy

### Safe Migration Approach
1. **Parallel Implementation**: Run both old and new systems
2. **Gradual Transition**: Migrate one data source at a time
3. **Validation Period**: Compare results for 1-2 weeks
4. **Backwards Compatibility**: Maintain existing views during transition
5. **Rollback Plan**: Keep old tables until full validation complete

## Quality Assurance

### Data Validation Checks
- Verify 100% sportsbook name population
- Confirm team name resolution for all games
- Validate deduplication reduces record count
- Ensure source attribution is always present
- Check data lineage traceability

### Performance Testing
- Query performance on unified table vs. fragmented tables
- Index optimization for common analytical queries
- Storage efficiency comparison
- ETL processing time impact

## Conclusion

The current staging data model has critical usability issues that prevent effective analysis. The proposed unified approach addresses all identified problems while maintaining backwards compatibility and improving overall system quality.

**Key Success Metrics:**
- 0% missing sportsbook names (currently 100% missing)
- 0% missing team names (currently 100% missing) 
- 50-75% reduction in duplicate records
- 100% source attribution coverage
- Complete data lineage tracking

This improvement directly addresses the user's concerns: *"can't tell what source this came from"*, *"which team is which"*, and *"why are there two rows for what should definitely just have 1 row"*.