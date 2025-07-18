# Unified Betting Lines System - Implementation Summary

## System Overview

The unified betting lines system consolidates all data sources (SportsBetting Report, Sports Betting Dime, VSIN, Action Network, and MLB Stats API) into three core tables with standardized source attribution and data quality tracking.

## Current Implementation Status

### ✅ Completed Components

1. **Schema Design**
   - ✅ `core_betting.betting_lines_moneyline` - 8,870 records active
   - ✅ `core_betting.betting_lines_totals` - 7,895 records active  
   - ✅ `core_betting.betting_lines_spread` - Newly created with unified schema

2. **Core Infrastructure**
   - ✅ `UnifiedBettingLinesCollector` - Abstract base class for all collectors
   - ✅ `MLBStatsAPIGameResolver` - Game ID normalization service
   - ✅ `SportsbookMapper` - Sportsbook name standardization
   - ✅ `DataQualityCalculator` - Completeness and quality scoring

3. **Data Source Integration**
   - ✅ SportsBetting Report: 8,858 moneyline + 7,895 totals records
   - ✅ Action Network: 12 moneyline records (limited integration)
   - ✅ SBR Unified Collector: Example implementation with new pattern

### 🔄 In Progress

4. **Data Source Expansion**
   - 🔄 VSIN integration (needs collector implementation)
   - 🔄 Sports Betting Dime integration (needs collector implementation)
   - 🔄 Enhanced Action Network coverage (expand beyond 12 records)

5. **MLB Stats API Integration**
   - 🔄 Game ID resolution service (partially implemented)
   - 🔄 Team name standardization (needs MLB abbreviation mapping)
   - 🔄 Automated cross-source game matching

## Key Features

### Unified Schema
All three betting line tables share common fields:
- **Core**: `game_id`, `sportsbook_id`, `sportsbook`, `odds_timestamp`
- **Quality**: `data_completeness_score`, `source_reliability_score`, `data_quality`
- **Metadata**: `source`, `external_source_id`, `collection_batch_id`, `source_metadata`
- **Analysis**: `sharp_action`, `reverse_line_movement`, `steam_move`

### Data Quality Framework
- **Completeness Scoring**: 0.0-1.0 based on field availability
- **Reliability Scoring**: Source-specific reliability ratings
- **Quality Levels**: HIGH, MEDIUM, LOW, POOR based on combined metrics

### Source Attribution
Each record clearly identifies its data source:
```sql
source data_source_type -- SPORTSBETTING_REPORT, ACTION_NETWORK, VSIN, etc.
external_source_id VARCHAR(100) -- Source-specific game ID
```

## Database Structure

### Current Data Distribution
| Table | Source | Records | Games | Sportsbooks |
|-------|--------|---------|-------|-------------|
| moneyline | SPORTSBETTING_REPORT | 8,858 | 1,295 | 4 |
| moneyline | ACTION_NETWORK | 12 | 1 | 6 |
| totals | SPORTSBETTING_REPORT | 7,895 | 1,076 | 0 |
| spread | - | 0 | 0 | 0 |

### Core Tables Schema

#### betting_lines_moneyline
```sql
-- Core moneyline fields
home_ml, away_ml INTEGER
opening_home_ml, opening_away_ml INTEGER
closing_home_ml, closing_away_ml INTEGER

-- Betting volume
home_bets_count, away_bets_count INTEGER
home_bets_percentage, away_bets_percentage DECIMAL(5,2)
home_money_percentage, away_money_percentage DECIMAL(5,2)
```

#### betting_lines_totals
```sql
-- Core totals fields
total_line DECIMAL(4,1)
over_price, under_price INTEGER
opening_total, opening_over_price, opening_under_price INTEGER
closing_total, closing_over_price, closing_under_price INTEGER

-- Betting volume
over_bets_count, under_bets_count INTEGER
over_bets_percentage, under_bets_percentage DECIMAL(5,2)
over_money_percentage, under_money_percentage DECIMAL(5,2)

-- Outcome
total_score INTEGER
```

#### betting_lines_spread
```sql
-- Core spread fields
spread_line DECIMAL(4,1)
home_spread_price, away_spread_price INTEGER
opening_spread, opening_home_price, opening_away_price INTEGER
closing_spread, closing_home_price, closing_away_price INTEGER

-- Betting volume
home_bets_count, away_bets_count INTEGER
home_bets_percentage, away_bets_percentage DECIMAL(5,2)
home_money_percentage, away_money_percentage DECIMAL(5,2)

-- Outcome
final_score_difference INTEGER
```

## Implementation Pattern

### Collector Architecture
```python
class UnifiedBettingLinesCollector(ABC):
    """Base class for all betting line collectors."""
    
    def __init__(self, source: DataSource):
        self.source = source
        self.game_resolver = MLBStatsAPIGameResolver()
        self.sportsbook_mapper = SportsbookMapper()
        self.quality_calculator = DataQualityCalculator()
    
    @abstractmethod
    def collect_raw_data(self, **kwargs) -> List[Dict[str, Any]]:
        """Collect raw data from source."""
        pass
    
    def collect_and_store(self, **kwargs) -> CollectionResult:
        """Unified collection and storage workflow."""
        # 1. Collect raw data
        # 2. Resolve game IDs to MLB Stats API
        # 3. Map sportsbook names to IDs
        # 4. Calculate quality metrics
        # 5. Store in appropriate table
        pass
```

### Source-Specific Implementation
```python
class SBRUnifiedCollector(UnifiedBettingLinesCollector):
    """SBR collector using unified pattern."""
    
    def __init__(self):
        super().__init__(DataSource.SPORTSBETTING_REPORT)
    
    def collect_raw_data(self, sbr_game_id: str) -> List[Dict[str, Any]]:
        """Collect SBR data using existing scraping logic."""
        # Use Playwright to scrape SBR
        # Convert to unified format
        # Return standardized records
        pass
```

## Benefits Achieved

1. **Consistency**: All sources use identical schema and quality standards
2. **Scalability**: Easy to add new sources without schema changes
3. **Analytics**: Cross-source analysis and comparison capabilities
4. **Quality**: Comprehensive quality tracking and monitoring
5. **Maintainability**: Single codebase for all betting line types
6. **Performance**: Optimized indexes and query patterns

## Query Examples

### Cross-Source Line Comparison
```sql
SELECT 
    source,
    sportsbook,
    home_ml,
    away_ml,
    data_completeness_score,
    odds_timestamp
FROM core_betting.betting_lines_moneyline
WHERE game_id = 12345
ORDER BY odds_timestamp DESC;
```

### Data Quality Monitoring
```sql
SELECT 
    source,
    COUNT(*) as total_records,
    AVG(data_completeness_score) as avg_completeness,
    COUNT(CASE WHEN data_quality = 'HIGH' THEN 1 END) as high_quality_records
FROM core_betting.betting_lines_moneyline
GROUP BY source;
```

### Sharp Action Detection
```sql
SELECT DISTINCT
    g.mlb_game_id,
    g.home_team,
    g.away_team,
    COUNT(DISTINCT ml.source) as source_count,
    COUNT(CASE WHEN ml.sharp_action IS NOT NULL THEN 1 END) as sharp_indicators
FROM core_betting.games g
JOIN core_betting.betting_lines_moneyline ml ON g.id = ml.game_id
WHERE ml.sharp_action IN ('HEAVY', 'MODERATE')
GROUP BY g.mlb_game_id, g.home_team, g.away_team
HAVING COUNT(DISTINCT ml.source) >= 2;
```

## Next Steps

### Phase 1: Complete Core Integration
1. Implement VSIN collector using unified pattern
2. Implement SBD collector using unified pattern
3. Enhance Action Network collector for comprehensive coverage
4. Validate SBR collector with unified pattern

### Phase 2: MLB Stats API Normalization
1. Complete game ID resolution service
2. Add team name standardization
3. Implement automated cross-source matching
4. Add MLB Stats API direct integration

### Phase 3: Advanced Analytics
1. Implement line movement detection
2. Add sharp action correlation analysis
3. Create cross-source arbitrage detection
4. Build predictive modeling pipeline

### Phase 4: Production Optimization
1. Performance tuning for large-scale ingestion
2. Real-time data quality monitoring
3. Automated data validation and alerts
4. Comprehensive testing and validation

## Migration Notes

- **Existing Data**: SportsBetting Report and Action Network data already in correct format
- **Schema Changes**: No breaking changes to existing tables
- **Backward Compatibility**: All existing queries continue to work
- **Data Quality**: Existing records can be retroactively scored for completeness

This unified system provides a robust, scalable foundation for consolidating all betting line data sources while maintaining data quality and enabling comprehensive analytics across the entire betting ecosystem.