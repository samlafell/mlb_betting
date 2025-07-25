# Action Network Bet% and Money% Integration Design

## Overview
Design document for consistently capturing and processing bet percentage and money percentage data from Action Network History API responses.

## Current State Analysis

### ✅ Raw Data Capture (Working)
- **Storage**: `raw_data.action_network_history.raw_history` (JSONB)
- **Structure**: Bet info available in JSON format:
  ```json
  "bet_info": {
      "money": {"value": 0, "percent": 92},
      "tickets": {"value": 0, "percent": 92}
  }
  ```

### ❌ Missing Components
1. **Staging Schema**: No bet%/money% columns in `staging.action_network_odds_historical`
2. **Processing Logic**: History processor ignores bet_info data
3. **Data Pipeline**: No flow from raw → staging → curated for betting percentages

## Design Solution

### Phase 1: Schema Enhancement

#### 1.1 Add Betting Percentage Columns to Staging Table
```sql
ALTER TABLE staging.action_network_odds_historical 
ADD COLUMN IF NOT EXISTS bet_percent_tickets INTEGER,     -- Ticket percentage (0-100)
ADD COLUMN IF NOT EXISTS bet_percent_money INTEGER,       -- Money percentage (0-100)
ADD COLUMN IF NOT EXISTS bet_value_tickets BIGINT,        -- Actual ticket count (if available)
ADD COLUMN IF NOT EXISTS bet_value_money BIGINT,          -- Actual money amount (if available)
ADD COLUMN IF NOT EXISTS bet_info_available BOOLEAN DEFAULT FALSE;  -- Indicates if bet data exists

-- Add constraints to ensure valid percentage ranges
ALTER TABLE staging.action_network_odds_historical 
ADD CONSTRAINT valid_bet_percent_tickets CHECK (bet_percent_tickets >= 0 AND bet_percent_tickets <= 100),
ADD CONSTRAINT valid_bet_percent_money CHECK (bet_percent_money >= 0 AND bet_percent_money <= 100);

-- Add indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_odds_historical_bet_percentages 
ON staging.action_network_odds_historical(external_game_id, market_type, bet_percent_tickets, bet_percent_money)
WHERE bet_info_available = TRUE;

CREATE INDEX IF NOT EXISTS idx_odds_historical_sharp_indicators
ON staging.action_network_odds_historical(external_game_id, sportsbook_name, market_type, bet_percent_tickets, bet_percent_money)
WHERE bet_info_available = TRUE AND ABS(bet_percent_tickets - bet_percent_money) >= 10;
```

#### 1.2 Create Betting Percentage Views
```sql
-- View for analyzing ticket vs money divergence (sharp action indicator)
CREATE OR REPLACE VIEW staging.v_bet_money_divergence AS
SELECT 
    external_game_id,
    sportsbook_name,
    market_type,
    side,
    bet_percent_tickets,
    bet_percent_money,
    ABS(bet_percent_tickets - bet_percent_money) as percentage_divergence,
    CASE 
        WHEN bet_percent_money > bet_percent_tickets + 15 THEN 'Sharp Money Heavy'
        WHEN bet_percent_tickets > bet_percent_money + 15 THEN 'Public Money Heavy'
        WHEN ABS(bet_percent_tickets - bet_percent_money) <= 5 THEN 'Aligned'
        ELSE 'Moderate Divergence'
    END as sharp_indicator,
    updated_at
FROM staging.action_network_odds_historical
WHERE bet_info_available = TRUE
ORDER BY percentage_divergence DESC;

-- View for public vs sharp action analysis
CREATE OR REPLACE VIEW staging.v_public_sharp_action AS
SELECT 
    external_game_id,
    home_team,
    away_team,
    game_start_time,
    sportsbook_name,
    market_type,
    side,
    
    -- Current line info
    current_odds as odds,
    line_value,
    
    -- Betting percentages
    bet_percent_tickets as public_percent,
    bet_percent_money as money_percent,
    
    -- Sharp action indicators
    CASE 
        WHEN bet_percent_money > bet_percent_tickets + 10 THEN 'Sharp Fade'
        WHEN bet_percent_tickets > bet_percent_money + 10 THEN 'Public Fade'
        ELSE 'Balanced'
    END as fade_type,
    
    ABS(bet_percent_tickets - bet_percent_money) as divergence_strength
    
FROM staging.v_pre_game_line_movements vlm
INNER JOIN staging.action_network_odds_historical hist 
    ON vlm.external_game_id = hist.external_game_id 
    AND vlm.sportsbook_name = hist.sportsbook_name
    AND vlm.market_type = hist.market_type
    AND vlm.side = hist.side
    AND vlm.updated_at = hist.updated_at
WHERE hist.bet_info_available = TRUE
ORDER BY divergence_strength DESC;
```

### Phase 2: Processing Logic Enhancement

#### 2.1 Update History Processor
**File**: `src/data/pipeline/staging_action_network_history_processor.py`

**Key Changes**:
1. **Extract bet_info from each historical record**
2. **Parse tickets and money percentages**  
3. **Store in new staging columns**
4. **Add validation for percentage data**

```python
# Add to HistoricalOddsRecord dataclass
@dataclass
class HistoricalOddsRecord:
    # ... existing fields ...
    bet_percent_tickets: int | None = None
    bet_percent_money: int | None = None
    bet_value_tickets: int | None = None
    bet_value_money: int | None = None
    bet_info_available: bool = False

# Enhanced extraction method
def _extract_bet_info(self, line_data: dict) -> dict:
    """Extract betting percentage information from line data."""
    bet_info = line_data.get("bet_info", {})
    
    if not bet_info:
        return {
            "bet_percent_tickets": None,
            "bet_percent_money": None, 
            "bet_value_tickets": None,
            "bet_value_money": None,
            "bet_info_available": False
        }
    
    # Extract ticket data
    tickets_data = bet_info.get("tickets", {})
    tickets_percent = tickets_data.get("percent")
    tickets_value = tickets_data.get("value")
    
    # Extract money data  
    money_data = bet_info.get("money", {})
    money_percent = money_data.get("percent")
    money_value = money_data.get("value")
    
    return {
        "bet_percent_tickets": tickets_percent if tickets_percent is not None else None,
        "bet_percent_money": money_percent if money_percent is not None else None,
        "bet_value_tickets": tickets_value if tickets_value is not None and tickets_value > 0 else None,
        "bet_value_money": money_value if money_value is not None and money_value > 0 else None,
        "bet_info_available": any([tickets_percent is not None, money_percent is not None])
    }
```

#### 2.2 Update Staging Insert Logic
```python
async def _insert_historical_odds_record(
    self, record: HistoricalOddsRecord, conn: asyncpg.Connection
) -> None:
    """Insert historical odds record with betting percentage data."""
    await conn.execute(
        """
        INSERT INTO staging.action_network_odds_historical (
            external_game_id, mlb_stats_api_game_id, sportsbook_external_id,
            sportsbook_id, sportsbook_name, market_type, side, odds, line_value,
            updated_at, data_collection_time, data_processing_time, line_status,
            market_id, outcome_id, period, data_quality_score, validation_status,
            raw_data_id,
            -- NEW: Betting percentage columns
            bet_percent_tickets, bet_percent_money, bet_value_tickets, 
            bet_value_money, bet_info_available
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, 
            $16, $17, $18, $19, $20, $21, $22, $23, $24, $25
        )
        ON CONFLICT (external_game_id, sportsbook_external_id, market_type, side, updated_at) 
        DO UPDATE SET
            bet_percent_tickets = EXCLUDED.bet_percent_tickets,
            bet_percent_money = EXCLUDED.bet_percent_money,
            bet_value_tickets = EXCLUDED.bet_value_tickets,
            bet_value_money = EXCLUDED.bet_value_money,
            bet_info_available = EXCLUDED.bet_info_available,
            updated_at_record = NOW()
        """,
        # ... existing parameters ...
        record.bet_percent_tickets,
        record.bet_percent_money, 
        record.bet_value_tickets,
        record.bet_value_money,
        record.bet_info_available
    )
```

### Phase 3: Enhanced Line Movement Views

#### 3.1 Update Line Movement Views with Betting Data
```sql
-- Enhanced pre-game line movements with betting percentages
CREATE OR REPLACE VIEW staging.v_pre_game_line_movements_with_betting AS
SELECT 
    plm.*,
    
    -- Betting percentage data
    hist.bet_percent_tickets,
    hist.bet_percent_money,
    hist.bet_info_available,
    
    -- Sharp action indicators
    CASE 
        WHEN hist.bet_info_available AND hist.bet_percent_money > hist.bet_percent_tickets + 15 
        THEN 'Sharp Money'
        WHEN hist.bet_info_available AND hist.bet_percent_tickets > hist.bet_percent_money + 15 
        THEN 'Public Heavy'
        WHEN hist.bet_info_available AND ABS(hist.bet_percent_tickets - hist.bet_percent_money) <= 5 
        THEN 'Balanced'
        WHEN hist.bet_info_available 
        THEN 'Moderate Split'
        ELSE 'No Bet Data'
    END as betting_action_type,
    
    ABS(COALESCE(hist.bet_percent_tickets, 50) - COALESCE(hist.bet_percent_money, 50)) as betting_divergence,
    
    -- Enhanced sharp action score (combines line movement + betting data)
    CASE 
        WHEN hist.bet_info_available AND ABS(plm.odds_change) >= 10 
             AND hist.bet_percent_money > hist.bet_percent_tickets + 10
        THEN LEAST(10.0, (ABS(plm.odds_change) / 3.0) + (ABS(hist.bet_percent_money - hist.bet_percent_tickets) / 10.0))
        WHEN ABS(plm.odds_change) >= 10
        THEN LEAST(10.0, ABS(plm.odds_change) / 5.0)
        ELSE 0.0
    END as enhanced_sharp_score
    
FROM staging.v_pre_game_line_movements plm
LEFT JOIN staging.action_network_odds_historical hist
    ON plm.external_game_id = hist.external_game_id
    AND plm.sportsbook_name = hist.sportsbook_name  
    AND plm.market_type = hist.market_type
    AND plm.side = hist.side
    AND plm.updated_at = hist.updated_at;
```

### Phase 4: Analytics and Reporting

#### 4.1 Sharp Action Detection Enhancement
```sql
-- Enhanced sharp action view with betting percentage context
CREATE OR REPLACE VIEW staging.v_enhanced_sharp_action AS
SELECT 
    external_game_id,
    home_team,
    away_team,
    game_start_time,
    sportsbook_name,
    market_type,
    side,
    
    -- Line movement info
    odds_movement_display,
    movement_direction,
    movement_strength,
    odds_change,
    
    -- Betting percentage context
    bet_percent_tickets as public_percent,
    bet_percent_money as sharp_percent,
    betting_action_type,
    betting_divergence,
    
    -- Enhanced scoring
    enhanced_sharp_score,
    
    -- Timing
    minutes_before_game,
    timing_category,
    
    -- Combined indicator
    CASE 
        WHEN enhanced_sharp_score >= 7.0 THEN 'Elite Sharp Action'
        WHEN enhanced_sharp_score >= 5.0 THEN 'Strong Sharp Action'
        WHEN enhanced_sharp_score >= 3.0 THEN 'Moderate Sharp Action'
        WHEN movement_strength IN ('MAJOR', 'SIGNIFICANT') THEN 'Line Movement Only'
        WHEN betting_action_type = 'Sharp Money' THEN 'Betting Pattern Only'
        ELSE 'Standard Activity'
    END as action_classification
    
FROM staging.v_pre_game_line_movements_with_betting
WHERE (enhanced_sharp_score >= 3.0 OR betting_divergence >= 15)
  AND minutes_before_game <= 1440  -- Within 24 hours of game
ORDER BY enhanced_sharp_score DESC, betting_divergence DESC;
```

## Implementation Plan

### Phase 1: Schema Migration (Week 1)
1. **Create migration**: `018_add_betting_percentages_to_staging.sql`
2. **Test on development data**
3. **Apply to production with zero downtime**

### Phase 2: Processor Enhancement (Week 2)  
1. **Update HistoricalOddsRecord dataclass**
2. **Implement bet_info extraction logic**
3. **Enhance insert/update methods**
4. **Add comprehensive testing**

### Phase 3: View Enhancement (Week 2-3)
1. **Create betting percentage views**
2. **Enhance existing line movement views**
3. **Add sharp action enhancement views**
4. **Update CLI commands to display betting data**

### Phase 4: Analytics Integration (Week 3-4)
1. **Integrate with existing sharp action detection**
2. **Create betting pattern reports**
3. **Add divergence alerting**
4. **Performance optimization**

## Success Metrics

### Data Quality
- ✅ **Coverage**: >80% of historical records have betting percentages
- ✅ **Accuracy**: Percentages sum to logical ranges (95-105% accounting for vig)
- ✅ **Completeness**: All major sportsbooks provide betting data

### Analytical Value  
- ✅ **Sharp Detection**: Enhanced sharp action scoring improves accuracy by 25%
- ✅ **Divergence Analysis**: Identify betting vs line movement patterns
- ✅ **Timing Analysis**: Track how betting percentages change over time

### Performance
- ✅ **Processing Speed**: No significant impact on history processing time
- ✅ **Query Performance**: New indexes support efficient betting analysis
- ✅ **Storage Impact**: <10% increase in staging table size

## Risk Mitigation

### Data Availability Risk
- **Risk**: Action Network may not always provide bet_info
- **Mitigation**: Use `bet_info_available` flag for conditional analysis

### Processing Performance Risk  
- **Risk**: Additional processing overhead
- **Mitigation**: Efficient JSON extraction, indexed queries

### Schema Evolution Risk
- **Risk**: Future Action Network API changes
- **Mitigation**: Flexible JSON parsing, backward compatibility

## Testing Strategy

### Unit Tests
- Bet_info extraction logic
- Percentage validation
- Sharp action scoring algorithms

### Integration Tests  
- End-to-end history processing with betting data
- View query performance
- Data consistency checks

### Performance Tests
- Large dataset processing
- Query response times
- Memory usage validation

## Future Enhancements

### Phase 5: Historical Analysis
- Betting percentage trend analysis
- Sharp action pattern recognition
- Predictive modeling integration

### Phase 6: Real-time Integration
- Live betting percentage updates
- Real-time sharp action alerts
- Integration with other data sources

This design provides a comprehensive solution for consistently capturing and analyzing betting percentage data from Action Network, enabling enhanced sharp action detection and betting pattern analysis.