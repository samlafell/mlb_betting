# Timing Validation System Guide

## Overview

The MLB Sharp Betting system now includes comprehensive timing validation to ensure only actionable, time-sensitive betting data is processed. The system implements a **5-minute grace period rule** that prevents the storage of betting splits for games that started more than 5 minutes ago.

## Core Validation Rule

**Primary Rule**: `(current_time_utc - game_start_time_utc) <= 5 minutes`

- ✅ **Valid**: Games that haven't started or started within the last 5 minutes
- ❌ **Rejected**: Games that started more than 5 minutes ago
- 🔄 **Enhanced**: MLB API integration checks for delays and postponements

## Features

### 🛡️ Comprehensive Validation
- **Time-based filtering** before database storage
- **MLB API integration** for real-time game status
- **Timezone handling** with UTC standardization
- **Batch processing** optimization

### 📊 Monitoring & Alerting
- **Real-time metrics** tracking validation statistics
- **Rejection rate monitoring** with automated alerts
- **Daily reporting** of validation performance
- **Database-level monitoring** views and functions

### 🔧 Database Optimizations
- **Composite indexes** for efficient time-based queries
- **PostgreSQL constraints** ensuring data integrity
- **Monitoring views** for performance tracking
- **Cleanup functions** for data maintenance

## Usage Examples

### Basic Data Collection with Timing Validation
```bash
# Collect data with timing validation enabled (default)
uv run src/mlb_sharp_betting/cli.py data collect

# Force collection even if data is fresh
uv run src/mlb_sharp_betting/cli.py data collect --force

# Use dry-run mode for testing
uv run src/mlb_sharp_betting/cli.py data collect --dry-run
```

### Monitoring Timing Validation
```bash
# Check timing validation status
uv run src/mlb_sharp_betting/cli.py data timing-status

# Get detailed timing metrics
uv run src/mlb_sharp_betting/cli.py data timing-status --detailed

# Check for recently expired splits
uv run src/mlb_sharp_betting/cli.py data timing-status --check-expired
```

### Data Status with Timing Information
```bash
# Check overall data status
uv run src/mlb_sharp_betting/cli.py data status

# Get detailed data breakdown
uv run src/mlb_sharp_betting/cli.py data status --detailed
```

## CLI Commands

### `mlb-cli data timing-status`

Monitor timing validation performance and metrics.

**Options:**
- `--detailed`: Show detailed timing validation metrics
- `--check-expired`: Check for recently expired splits

**Example Output:**
```
⏰ TIMING VALIDATION STATUS
==================================================
📊 Recent Daily Metrics (7 days):
Date         Total    Valid    Expired  Rejection %
--------------------------------------------------
2024-01-15   1,245    1,198    47       🟡 3.8%
2024-01-14   1,156    1,134    22       🟢 1.9%
2024-01-13   987      965      22       🟢 2.2%

🔍 Validator Statistics:
   Total Validations: 8,456
   Valid Splits: 8,123
   Rejected Splits: 333
   Delayed Games: 12
   Postponed Games: 5

✅ No validation alerts
```

### Integration with Data Collection

The timing validation is automatically integrated into the data collection process:

```bash
# Data collection output now includes timing metrics
✅ DATA COLLECTION COMPLETED
   📥 Records Scraped: 1,245
   🔄 Records Parsed: 1,245
   💾 Records Stored: 1,198
   🎯 Sharp Indicators: 89
   ⏰ Timing Rejections: 47 (3.8%)
```

## Database Schema

### Tables and Indexes

The system includes optimized database schema for timing validation:

```sql
-- Composite indexes for efficient queries
CREATE INDEX idx_games_start_time ON splits.games(game_datetime, created_at);
CREATE INDEX idx_splits_game_timing ON splits.raw_mlb_betting_splits(game_datetime, last_updated, game_id);

-- Constraints ensuring data integrity
ALTER TABLE splits.raw_mlb_betting_splits 
ADD CONSTRAINT chk_game_datetime_not_null CHECK (game_datetime IS NOT NULL);
```

### Monitoring Views

```sql
-- View current timing validation metrics
SELECT * FROM timing_validation_monitor 
WHERE game_date >= CURRENT_DATE - INTERVAL '7 days';

-- View current games status
SELECT * FROM current_game_timing_status;
```

### Utility Functions

```sql
-- Check if game is within grace period
SELECT is_within_grace_period('2024-01-15 19:05:00'::timestamp);

-- Get grace period status
SELECT get_grace_period_status('2024-01-15 19:05:00'::timestamp);

-- Cleanup expired data
SELECT * FROM cleanup_expired_splits(30); -- Keep 30 days
```

## Configuration

### Environment Variables

```bash
# MLB API configuration for enhanced validation
MLB_API_BASE_URL=https://statsapi.mlb.com/api/v1
MLB_API_TIMEOUT=30

# Timing validation settings
TIMING_VALIDATION_GRACE_PERIOD_MINUTES=5
TIMING_VALIDATION_USE_MLB_API=true
TIMING_VALIDATION_ALERT_THRESHOLD=0.20  # 20% rejection rate
```

### Code Configuration

```python
from mlb_sharp_betting.utils.time_based_validator import get_game_time_validator

# Configure validator
validator = get_game_time_validator(grace_period_minutes=5)

# Validate splits with MLB API
result, metadata = await validator.validate_with_mlb_api(split)

# Batch validation
valid_splits, rejected_splits = validator.validate_batch(splits, use_mlb_api=True)
```

## Monitoring & Alerts

### Alert Conditions

The system automatically alerts on:

1. **High Rejection Rate**: >20% of splits rejected due to timing
2. **High API Error Rate**: >10% of validations fail due to API errors
3. **Invalid Time Rate**: >15% of splits have missing/invalid game times

### Alert Examples

```bash
⚠️  VALIDATION ALERTS:
   • High rejection rate: 23.5%
   • High API error rate: 12.3%
```

### Performance Monitoring

```python
# Get validation statistics
stats = validator.get_validation_stats()
print(f"Rejection rate: {stats['rejection_rate']:.1%}")

# Check for alerts
should_alert, reasons = validator.should_alert()
if should_alert:
    print(f"Alerts: {reasons}")
```

## Edge Cases & Special Handling

### Game Delays

The system handles delayed games through MLB API integration:

```python
# Delayed game validation
if game_status.get("status") == "Delayed":
    # Use actual start time instead of scheduled time
    actual_start = game_status.get("actual_start_time")
    # Re-validate with actual start time
```

### Postponed Games

```python
# Postponed/cancelled games
if game_status.get("status") in ["Postponed", "Cancelled", "Suspended"]:
    return ValidationResult.POSTPONED
```

### Timezone Handling

All timestamps are standardized to UTC:

```python
# Ensure timezone-aware timestamps
if game_start.tzinfo is None:
    game_start = game_start.replace(tzinfo=timezone.utc)

current_time = datetime.now(timezone.utc)
```

## Troubleshooting

### Common Issues

#### High Rejection Rate
```bash
# Check if you're running collection too late
uv run src/mlb_sharp_betting/cli.py data timing-status --check-expired

# Check current games status
uv run src/mlb_sharp_betting/cli.py data timing-status --detailed
```

#### MLB API Errors
```bash
# Check API connectivity
curl "https://statsapi.mlb.com/api/v1/schedule?sportId=1"

# Check logs for API timeout issues
tail -f logs/mlb_sharp_betting.log | grep "MLB API"
```

#### Invalid Game Times
```bash
# Check for data quality issues
uv run src/mlb_sharp_betting/cli.py data status --detailed

# Query database directly
psql -d mlb_betting -c "
SELECT COUNT(*) FROM splits.raw_mlb_betting_splits 
WHERE game_datetime IS NULL OR game_datetime > CURRENT_DATE + INTERVAL '30 days'
"
```

### Performance Issues

#### Slow Validation
```sql
-- Check if indexes exist
\d+ splits.raw_mlb_betting_splits
\d+ splits.games

-- Analyze query performance
EXPLAIN ANALYZE SELECT * FROM timing_validation_monitor;
```

#### High Memory Usage
```python
# Use batch processing for large datasets
batch_size = 100
for i in range(0, len(splits), batch_size):
    batch = splits[i:i + batch_size]
    valid_splits, rejected = validator.validate_batch(batch)
```

## Best Practices

### 1. Regular Monitoring
- Check timing validation status daily
- Monitor rejection rates and alert thresholds
- Review expired splits for pattern analysis

### 2. Data Collection Timing
- Run collection before games start when possible
- Use `--force` flag during game days for updates
- Monitor for delayed/postponed games

### 3. Performance Optimization
- Use batch validation for large datasets
- Enable MLB API validation for critical data
- Monitor database performance metrics

### 4. Error Handling
- Implement fallback validation if MLB API fails
- Log all rejected splits for analysis
- Set up alerts for unusual rejection patterns

## API Reference

### GameTimeValidator Class

```python
class GameTimeValidator:
    def __init__(self, grace_period_minutes: int = 5)
    
    def validate_split_timing(self, split: BettingSplit, current_time: Optional[datetime] = None) -> Tuple[ValidationResult, Dict[str, Any]]
    
    async def validate_with_mlb_api(self, split: BettingSplit, current_time: Optional[datetime] = None) -> Tuple[ValidationResult, Dict[str, Any]]
    
    def validate_batch(self, splits: List[BettingSplit], use_mlb_api: bool = False) -> Tuple[List[BettingSplit], List[Dict[str, Any]]]
    
    def get_validation_stats(self) -> Dict[str, Any]
    
    def should_alert(self) -> Tuple[bool, List[str]]
```

### ValidationResult Enum

```python
class ValidationResult(Enum):
    VALID = "valid"              # Within grace period
    EXPIRED = "expired"          # Beyond grace period
    DELAYED = "delayed"          # Delayed game, valid
    POSTPONED = "postponed"      # Postponed/cancelled
    INVALID_TIME = "invalid_time" # Missing/invalid time
    ERROR = "error"              # Validation error
```

## Business Impact

### Data Quality Improvements
- **Actionable Data Only**: Ensures only bettable opportunities are processed
- **Real-time Relevance**: Eliminates stale data that could mislead analysis
- **API Efficiency**: Reduces unnecessary processing of expired game data

### Risk Mitigation
- **Prevents Stale Bets**: Avoids recommendations on games that already started
- **Timing Accuracy**: Ensures recommendations are made within betting windows
- **Data Integrity**: Maintains high-quality dataset for analysis

### Operational Benefits
- **Automated Filtering**: Reduces manual data validation overhead
- **Performance Optimization**: Faster processing with pre-filtered data
- **Monitoring Visibility**: Clear metrics on data freshness and quality

---

**General Balls** ⚾

*Last Updated: January 2024* 