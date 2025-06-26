# MLB Betting Recommendation Timing Analysis System

A comprehensive system for analyzing the accuracy and profitability of betting recommendations based on their timing relative to game start. This system helps determine optimal bet placement timing for maximum profitability.

## Table of Contents

1. [Installation & Setup](#installation--setup)
2. [Quick Start](#quick-start)
3. [CLI Commands](#cli-commands)
4. [API Usage](#api-usage)
5. [Database Schema](#database-schema)
6. [Examples](#examples)
7. [Troubleshooting](#troubleshooting)

## Installation & Setup

### Prerequisites

- Python 3.11+
- PostgreSQL 17
- UV package manager
- Existing MLB betting system database

### Initial Setup

1. **Run the setup script** (recommended for first-time users):
```bash
uv run setup_timing_analysis.py
```

This interactive script will:
- Create the timing analysis database schema
- Generate sample data for demonstration
- Provide example CLI commands
- Walk you through the setup process

2. **Manual setup** (for advanced users):
```bash
# Create database schema
psql -d your_database -f sql/timing_analysis_schema.sql

# Verify installation
uv run python -m mlb_sharp_betting.cli timing --help
```

## Quick Start

### 1. Generate Sample Data (Demo)
```bash
uv run python -m mlb_sharp_betting.cli timing track \
  --source "fanduel" \
  --strategy "value_betting" \
  --bet-type "moneyline" \
  --game-start "2024-01-15 19:00:00" \
  --recommendation-time "2024-01-15 17:00:00" \
  --odds -150 \
  --stake 100
```

### 2. Run Analysis
```bash
uv run python -m mlb_sharp_betting.cli timing analyze
```

### 3. Get Real-time Recommendations
```bash
uv run python -m mlb_sharp_betting.cli timing recommend \
  --source "fanduel" \
  --strategy "value_betting" \
  --bet-type "moneyline"
```

## CLI Commands

### `timing analyze`

Performs comprehensive timing analysis across all time buckets.

**Usage:**
```bash
uv run python -m mlb_sharp_betting.cli timing analyze [OPTIONS]
```

**Options:**
- `--source TEXT`: Filter by specific sportsbook/source
- `--strategy TEXT`: Filter by betting strategy
- `--bet-type TEXT`: Filter by bet type (moneyline, spread, over_under)
- `--min-confidence INTEGER`: Minimum confidence level (1-4, default: 2)
- `--days-back INTEGER`: Days of historical data to analyze (default: 90)
- `--output FORMAT`: Output format (console, json, csv, default: console)
- `--save-results / --no-save-results`: Save results to database (default: True)

**Examples:**
```bash
# Basic analysis
uv run python -m mlb_sharp_betting.cli timing analyze

# Analyze specific source with JSON output
uv run python -m mlb_sharp_betting.cli timing analyze \
  --source "draftkings" \
  --output json

# High-confidence analysis only
uv run python -m mlb_sharp_betting.cli timing analyze \
  --min-confidence 3 \
  --output csv > timing_analysis.csv

# Strategy-specific analysis
uv run python -m mlb_sharp_betting.cli timing analyze \
  --strategy "sharp_money" \
  --bet-type "spread"
```

### `timing recommend`

Gets real-time timing recommendations for bet placement.

**Usage:**
```bash
uv run python -m mlb_sharp_betting.cli timing recommend [OPTIONS]
```

**Options:**
- `--source TEXT`: Sportsbook/source (required)
- `--strategy TEXT`: Betting strategy (required)
- `--bet-type TEXT`: Bet type (required)
- `--cache-ttl INTEGER`: Cache time-to-live in minutes (default: 15)

**Examples:**
```bash
# Get recommendation for specific combination
uv run python -m mlb_sharp_betting.cli timing recommend \
  --source "fanduel" \
  --strategy "value_betting" \
  --bet-type "moneyline"

# Get recommendation with custom cache settings
uv run python -m mlb_sharp_betting.cli timing recommend \
  --source "bet365" \
  --strategy "sharp_money" \
  --bet-type "over_under" \
  --cache-ttl 30
```

### `timing summary`

Displays a performance summary across all timing buckets.

**Usage:**
```bash
uv run python -m mlb_sharp_betting.cli timing summary [OPTIONS]
```

**Options:**
- `--source TEXT`: Filter by source
- `--strategy TEXT`: Filter by strategy
- `--bet-type TEXT`: Filter by bet type

**Example:**
```bash
uv run python -m mlb_sharp_betting.cli timing summary --source "fanduel"
```

### `timing track`

Manually track a betting recommendation with timing information.

**Usage:**
```bash
uv run python -m mlb_sharp_betting.cli timing track [OPTIONS]
```

**Options:**
- `--source TEXT`: Sportsbook/source (required)
- `--strategy TEXT`: Betting strategy (required)
- `--bet-type TEXT`: Bet type (required)
- `--game-start DATETIME`: Game start time (EST format: "YYYY-MM-DD HH:MM:SS")
- `--recommendation-time DATETIME`: When recommendation was made
- `--odds FLOAT`: Betting odds (required)
- `--stake FLOAT`: Bet amount (required)
- `--outcome TEXT`: Bet outcome (win, loss, push) - optional
- `--actual-payout FLOAT`: Actual payout if outcome known

**Example:**
```bash
uv run python -m mlb_sharp_betting.cli timing track \
  --source "draftkings" \
  --strategy "value_betting" \
  --bet-type "spread" \
  --game-start "2024-01-20 20:15:00" \
  --recommendation-time "2024-01-20 14:30:00" \
  --odds -110 \
  --stake 50 \
  --outcome "win" \
  --actual-payout 95.45
```

### `timing update-outcomes`

Updates recommendation outcomes from game results.

**Usage:**
```bash
uv run python -m mlb_sharp_betting.cli timing update-outcomes [OPTIONS]
```

**Options:**
- `--days-back INTEGER`: Days back to check for outcomes (default: 7)
- `--dry-run / --no-dry-run`: Show what would be updated without making changes

**Example:**
```bash
# Update outcomes for last 3 days
uv run python -m mlb_sharp_betting.cli timing update-outcomes --days-back 3

# Preview updates without making changes
uv run python -m mlb_sharp_betting.cli timing update-outcomes --dry-run
```

## API Usage

### Service Layer

The `TimingAnalysisService` provides programmatic access to all timing analysis functionality.

```python
from mlb_sharp_betting.services.timing_analysis_service import TimingAnalysisService
from mlb_sharp_betting.models.timing_analysis import TimingBucket
from datetime import datetime, timezone

# Initialize service
service = TimingAnalysisService()

# Perform comprehensive analysis
analysis = await service.analyze_timing_performance(
    source="fanduel",
    strategy="value_betting",
    min_confidence_level=2,
    days_back=90
)

# Get real-time recommendation
recommendation = await service.get_realtime_timing_recommendation(
    source="fanduel",
    strategy="value_betting",
    bet_type="moneyline"
)

# Track a new recommendation
tracking_result = await service.track_recommendation(
    source="draftkings",
    strategy="sharp_money",
    bet_type="spread",
    game_start_time=datetime(2024, 1, 20, 20, 15, tzinfo=timezone.utc),
    recommendation_time=datetime(2024, 1, 20, 18, 0, tzinfo=timezone.utc),
    odds=-110,
    stake=100.0
)
```

### Direct Model Usage

```python
from mlb_sharp_betting.models.timing_analysis import (
    TimingBucket, 
    TimingPerformanceMetrics,
    RealtimeTimingLookup
)
from datetime import datetime, timedelta

# Calculate timing bucket
game_start = datetime.now() + timedelta(hours=4)
recommendation_time = datetime.now()
bucket = TimingBucket.from_times(recommendation_time, game_start)
print(f"Timing bucket: {bucket.value}")  # e.g., "2_to_6_hours"

# Create performance metrics
metrics = TimingPerformanceMetrics(
    total_bets=50,
    winning_bets=32,
    total_stake=2500.0,
    total_payout=2750.0,
    average_odds=-105
)
print(f"Win rate: {metrics.win_rate:.1%}")  # e.g., "64.0%"
print(f"ROI: {metrics.roi:.1%}")  # e.g., "10.0%"
```

## Database Schema

The timing analysis system uses a dedicated schema with four main tables:

### Tables

1. **`timing_bucket_performance`** - Aggregated performance metrics by timing bucket
2. **`comprehensive_analyses`** - Complete analysis results
3. **`timing_recommendations_cache`** - Cached real-time recommendations
4. **`recommendation_history`** - Historical tracking of all recommendations

### Views

- **`current_performance_by_bucket`** - Current performance metrics by timing bucket
- **`best_timing_by_category`** - Optimal timing recommendations by category

### Key Fields

All tables include timezone-aware timestamps in EST and use generated columns for calculated metrics like win_rate and roi_percentage.

## Examples

### Example 1: Weekly Performance Analysis

```bash
# Analyze last week's performance by source
uv run python -m mlb_sharp_betting.cli timing analyze \
  --days-back 7 \
  --output csv > weekly_performance.csv

# Get summary for specific strategy
uv run python -m mlb_sharp_betting.cli timing summary \
  --strategy "sharp_money"
```

### Example 2: Pre-Game Decision Making

```bash
# Get timing recommendation before placing bet
RECOMMENDATION=$(uv run python -m mlb_sharp_betting.cli timing recommend \
  --source "fanduel" \
  --strategy "value_betting" \
  --bet-type "moneyline" \
  --output json)

echo $RECOMMENDATION | jq '.optimal_timing_bucket'
```

### Example 3: Automated Tracking Integration

```python
# Integration with existing betting detector
from mlb_sharp_betting.analyzers.timing_recommendation_tracker import TimingRecommendationTracker

tracker = TimingRecommendationTracker()

# Automatically track recommendations from your betting system
async def process_betting_opportunity(bet_data):
    # Your existing betting logic here
    
    # Track timing automatically
    await tracker.track_recommendation(
        source=bet_data['source'],
        strategy=bet_data['strategy'],
        bet_type=bet_data['bet_type'],
        game_start_time=bet_data['game_start'],
        odds=bet_data['odds'],
        stake=bet_data['stake']
    )
```

### Example 4: Batch Outcome Updates

```bash
# Update outcomes for completed games (run daily)
uv run python -m mlb_sharp_betting.cli timing update-outcomes --days-back 1

# Preview what would be updated
uv run python -m mlb_sharp_betting.cli timing update-outcomes --dry-run --days-back 7
```

## Troubleshooting

### Common Issues

1. **Database Connection Errors**
   ```bash
   # Check database connection
   psql -d your_database -c "SELECT current_database();"
   
   # Verify schema exists
   psql -d your_database -c "\dn timing_analysis"
   ```

2. **Missing Schema**
   ```bash
   # Recreate schema
   psql -d your_database -f sql/timing_analysis_schema.sql
   ```

3. **Timezone Issues**
   - All times are handled in EST as specified in project requirements
   - Ensure your system timezone is properly configured
   - Game times from MLB API are converted from UTC to EST automatically

4. **Low Sample Sizes**
   - The system requires minimum sample sizes for statistical significance
   - Use the `--min-confidence` option to filter results
   - Generate more historical data for better analysis

5. **Cache Issues**
   ```bash
   # Clear recommendation cache
   psql -d your_database -c "DELETE FROM timing_analysis.timing_recommendations_cache;"
   ```

### Debugging

Enable detailed logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

Check service status:
```python
from mlb_sharp_betting.services.timing_analysis_service import TimingAnalysisService

service = TimingAnalysisService()
# Service will log connection status and any issues
```

### Performance Optimization

For large datasets:
1. Use date filters to limit analysis scope
2. Implement database indexing on frequently queried columns
3. Use the caching system for repeated real-time recommendations
4. Consider archiving old data beyond your analysis window

## Integration with Existing Systems

The timing analysis system is designed to integrate seamlessly with your existing MLB betting infrastructure:

1. **Automatic Integration**: Use `TimingRecommendationTracker` to automatically track recommendations
2. **Manual Integration**: Use CLI commands or service methods for custom workflows  
3. **Data Pipeline**: Set up automated outcome updates using the `update-outcomes` command
4. **Real-time Decisions**: Query timing recommendations before placing bets

For questions or issues, refer to the service logs or check the database schema documentation.

**General Balls** 