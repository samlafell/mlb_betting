# Betting Lines Data Quality Assessment & Improvement Plan

## Executive Summary

Analysis of three core betting lines tables reveals significant data quality issues with extensive null values across critical fields. This plan provides a comprehensive strategy to improve data population processes, schema design, and validation mechanisms.

## Current State Analysis

### Table Schemas Overview

**Common Pattern Across All Tables:**
- Foreign key relationships to `core_betting.games` and `core_betting.sportsbooks`
- Timestamp tracking (`odds_timestamp`, `created_at`, `updated_at`)
- Game information (populated via triggers)
- Source tracking (`ACTION_NETWORK`, `SPORTSBOOKREVIEW`)
- Data quality indicators

### Critical Data Quality Issues

#### 1. **Moneyline Table (`core_betting.betting_lines_moneyline`)**
- **Total Rows**: 8,869
- **Major Issues**:
  - `sportsbook_id`: Only 12/8,869 rows populated (99.9% null)
  - `sharp_action`: 0/8,869 rows populated (100% null)
  - `home_bets_percentage`: Only 10/8,869 rows populated (99.9% null)
  - Core odds data (`home_ml`): 8,821/8,869 populated (99.5% filled)

#### 2. **Spreads Table (`core_betting.betting_lines_spreads`)**
- **Total Rows**: 9,611
- **Major Issues**:
  - `sportsbook_id`: 0/9,611 rows populated (100% null)
  - `sharp_action`: 0/9,611 rows populated (100% null)
  - `home_bets_percentage`: 0/9,611 rows populated (100% null)
  - Core spread data (`home_spread`): 9,611/9,611 populated (100% filled)

#### 3. **Totals Table (`core_betting.betting_lines_totals`)**
- **Total Rows**: 7,895
- **Major Issues**:
  - `sportsbook_id`: 0/7,895 rows populated (100% null)
  - `sharp_action`: 0/7,895 rows populated (100% null)
  - `over_bets_percentage`: 0/7,895 rows populated (100% null)
  - Core totals data (`total_line`): 7,839/7,895 populated (99.3% filled)

### Data Population Programs Identified

#### Primary Population Sources:
1. **Action Network Repository** (`src/data/database/action_network_repository.py`)
   - Handles Action Network data ingestion
   - Inserts into all three betting lines tables
   - Maps sportsbook names but struggles with sportsbook IDs

2. **Game Outcome Service** (`src/services/game_outcome_service.py`)
   - Updates outcome-related fields
   - Focuses on post-game data population

## Root Cause Analysis

### 1. **Sportsbook ID Mapping Failure**
- Tables have foreign key constraints to `core_betting.sportsbooks`
- Action Network uses different sportsbook identifiers
- Mapping logic fails to resolve sportsbook names to database IDs
- Results in 99-100% null values for `sportsbook_id`

### 2. **Missing Sharp Action Data Pipeline**
- No programs currently populate `sharp_action` fields
- Advanced analytics fields remain completely unused
- Indicates missing integration with sharp money detection systems

### 3. **Incomplete Betting Percentage Data**
- Betting percentage fields are structurally sound but rarely populated
- Suggests data source limitations or extraction issues
- Action Network may not consistently provide this data

### 4. **Data Source Limitations**
- Only 2 sources identified: ACTION_NETWORK and SPORTSBOOKREVIEW
- Limited source diversity affects data completeness
- Missing integration with other betting data providers

## Improvement Plan

### Phase 1: Critical Infrastructure Fixes (Priority: HIGH)

#### 1.1 Sportsbook ID Resolution System
```sql
-- Create mapping table for external sportsbook identifiers
CREATE TABLE core_betting.sportsbook_external_mappings (
    id SERIAL PRIMARY KEY,
    sportsbook_id INTEGER REFERENCES core_betting.sportsbooks(id),
    external_source VARCHAR(50) NOT NULL,
    external_id VARCHAR(100) NOT NULL,
    external_name VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(external_source, external_id)
);

-- Populate Action Network mappings
INSERT INTO core_betting.sportsbook_external_mappings 
(sportsbook_id, external_source, external_id, external_name) VALUES
((SELECT id FROM core_betting.sportsbooks WHERE name = 'DraftKings'), 'ACTION_NETWORK', '15', 'DraftKings'),
((SELECT id FROM core_betting.sportsbooks WHERE name = 'FanDuel'), 'ACTION_NETWORK', '30', 'FanDuel'),
((SELECT id FROM core_betting.sportsbooks WHERE name = 'BetMGM'), 'ACTION_NETWORK', '68', 'BetMGM');
```

#### 1.2 Enhanced Data Validation Triggers
```sql
-- Create comprehensive validation function
CREATE OR REPLACE FUNCTION validate_betting_lines_data()
RETURNS TRIGGER AS $$
BEGIN
    -- Resolve sportsbook_id if null but sportsbook name provided
    IF NEW.sportsbook_id IS NULL AND NEW.sportsbook IS NOT NULL THEN
        SELECT s.id INTO NEW.sportsbook_id 
        FROM core_betting.sportsbooks s
        JOIN core_betting.sportsbook_external_mappings m ON s.id = m.sportsbook_id
        WHERE m.external_name = NEW.sportsbook 
        AND m.external_source = NEW.source;
    END IF;
    
    -- Set data quality based on field completeness
    NEW.data_quality = CASE 
        WHEN NEW.sportsbook_id IS NOT NULL AND 
             (NEW.home_bets_percentage IS NOT NULL OR NEW.sharp_action IS NOT NULL) 
        THEN 'HIGH'
        WHEN NEW.sportsbook_id IS NOT NULL 
        THEN 'MEDIUM'
        ELSE 'LOW'
    END;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply to all betting lines tables
CREATE TRIGGER validate_moneyline_data 
    BEFORE INSERT OR UPDATE ON core_betting.betting_lines_moneyline 
    FOR EACH ROW EXECUTE FUNCTION validate_betting_lines_data();
```

### Phase 2: Schema Enhancements (Priority: MEDIUM)

#### 2.1 Add Data Completeness Tracking
```sql
-- Add completeness score to each table
ALTER TABLE core_betting.betting_lines_moneyline 
ADD COLUMN data_completeness_score DECIMAL(3,2) DEFAULT 0.0;

ALTER TABLE core_betting.betting_lines_spreads 
ADD COLUMN data_completeness_score DECIMAL(3,2) DEFAULT 0.0;

ALTER TABLE core_betting.betting_lines_totals 
ADD COLUMN data_completeness_score DECIMAL(3,2) DEFAULT 0.0;
```

#### 2.2 Create Data Quality Monitoring Views
```sql
CREATE VIEW data_quality_dashboard AS
SELECT 
    'moneyline' as table_name,
    COUNT(*) as total_rows,
    AVG(CASE WHEN sportsbook_id IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100 as sportsbook_id_pct,
    AVG(CASE WHEN sharp_action IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100 as sharp_action_pct,
    AVG(CASE WHEN home_bets_percentage IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100 as betting_pct_pct,
    AVG(data_completeness_score) as avg_completeness
FROM core_betting.betting_lines_moneyline
UNION ALL
SELECT 
    'spreads' as table_name,
    COUNT(*) as total_rows,
    AVG(CASE WHEN sportsbook_id IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100 as sportsbook_id_pct,
    AVG(CASE WHEN sharp_action IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100 as sharp_action_pct,
    AVG(CASE WHEN home_bets_percentage IS NOT NULL THEN 1.0 ELSE 0.0 END) * 100 as betting_pct_pct,
    AVG(data_completeness_score) as avg_completeness
FROM core_betting.betting_lines_spreads;
```

### Phase 3: Program-Specific Improvements

#### 3.1 Action Network Repository Enhancement
**File**: `src/data/database/action_network_repository.py`

**Required Changes**:
1. **Implement Sportsbook ID Resolution**:
```python
async def resolve_sportsbook_id(self, external_id: str, source: str = "ACTION_NETWORK") -> int | None:
    """Resolve external sportsbook ID to internal database ID."""
    async with self.connection.get() as conn:
        result = await conn.fetchrow("""
            SELECT s.id 
            FROM core_betting.sportsbooks s
            JOIN core_betting.sportsbook_external_mappings m ON s.id = m.sportsbook_id
            WHERE m.external_id = $1 AND m.external_source = $2
        """, external_id, source)
        return result['id'] if result else None
```

2. **Add Data Completeness Calculation**:
```python
def calculate_data_completeness(self, **fields) -> float:
    """Calculate data completeness score for a record."""
    total_fields = len(fields)
    filled_fields = sum(1 for value in fields.values() if value is not None)
    return filled_fields / total_fields if total_fields > 0 else 0.0
```

#### 3.2 Sharp Action Data Integration
**New Service**: `src/services/sharp_action_detection_service.py`

**Purpose**: Populate `sharp_action` fields using existing strategy processors

**Implementation**:
```python
class SharpActionDetectionService:
    """Service to detect and populate sharp action indicators."""
    
    async def update_sharp_action_indicators(self, game_date: datetime) -> None:
        """Update sharp action indicators for games on specified date."""
        # Use existing unified strategy processors
        # Update betting lines tables with detected sharp action
        pass
```

### Phase 4: Multi-Source Data Integration (Priority: MEDIUM)

#### 4.1 Additional Data Source Integration
- **Target Sources**: 
  - Vegas Insider API
  - Odds Shark API  
  - The Odds API
  - Pinnacle API (if available)

#### 4.2 Data Source Priority System
```python
DATA_SOURCE_PRIORITY = {
    'PINNACLE': 1,        # Highest quality
    'ACTION_NETWORK': 2,
    'VEGAS_INSIDER': 3,
    'SPORTSBOOKREVIEW': 4,
    'ODDS_API': 5
}
```

#### 4.3 Conflict Resolution Strategy
- Use highest priority source for primary data
- Merge betting percentages from multiple sources
- Flag discrepancies for manual review

### Phase 5: Data Quality Monitoring & Alerting

#### 5.1 Automated Quality Checks
```python
class DataQualityMonitor:
    """Monitor data quality metrics and send alerts."""
    
    async def run_quality_checks(self) -> List[QualityAlert]:
        """Run comprehensive data quality checks."""
        alerts = []
        
        # Check for sportsbook_id null percentage > 95%
        # Check for complete absence of sharp_action data
        # Check for betting percentage data availability
        # Check for data freshness
        
        return alerts
```

#### 5.2 Daily Quality Reports
- Automated daily reports on data completeness
- Trending analysis of data quality metrics
- Integration with existing reporting infrastructure

## Implementation Roadmap

### Week 1: Critical Infrastructure
- [ ] Create sportsbook mapping table and populate
- [ ] Implement enhanced validation triggers
- [ ] Update Action Network repository with sportsbook ID resolution

### Week 2: Schema Enhancements  
- [ ] Add data completeness scoring
- [ ] Create quality monitoring views
- [ ] Implement data quality monitoring service

### Week 3: Program Improvements
- [ ] Enhance Action Network repository data population
- [ ] Implement sharp action detection service
- [ ] Add data completeness calculations

### Week 4: Integration & Monitoring
- [ ] Integrate additional data sources
- [ ] Implement automated quality checks
- [ ] Deploy monitoring and alerting

## Success Metrics

### Target Improvements (90 days):
- **Sportsbook ID Population**: From <1% to >95%
- **Sharp Action Data**: From 0% to >60%
- **Betting Percentage Data**: From <1% to >40%
- **Overall Data Completeness**: From ~20% to >80%

### Quality Gates:
- No new records with <50% data completeness
- Sportsbook ID resolution rate >95%
- Daily data quality reports with trending analysis
- Automated alerts for quality degradation

## Risk Mitigation

### Data Source Reliability:
- Multiple backup data sources
- Graceful degradation when sources are unavailable
- Historical data backfill capabilities

### Schema Migration Safety:
- Backward compatibility maintained
- Phased rollout with validation
- Rollback procedures documented

### Performance Impact:
- Index optimization for new queries
- Monitoring of trigger performance
- Batch processing for historical data updates

## Cost-Benefit Analysis

### Implementation Costs:
- **Development Time**: ~3-4 weeks
- **Infrastructure Changes**: Minimal
- **Data Source Integrations**: Variable (API costs)

### Expected Benefits:
- **Improved Analysis Accuracy**: Higher data completeness enables better betting strategies
- **Reduced Manual Data Cleanup**: Automated validation and enhancement
- **Enhanced Monitoring**: Proactive data quality management
- **Better Decision Making**: More complete data for strategy processors

This comprehensive plan addresses the fundamental data quality issues while establishing a sustainable framework for ongoing data management and improvement.