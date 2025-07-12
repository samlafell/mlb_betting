# Complete 2025 MLB Season Data Collection Guide

## Overview
This guide provides multiple approaches to collect all 2025 MLB season betting data from SportsbookReview.com using your enhanced system with the new `game_datetime`, `home_team`, and `away_team` columns.

## 🎯 **Quick Start Options**

### Option 1: Optimized Collection (RECOMMENDED) ⚡
```bash
# Test optimized collection (last 7 days)
python collect_2025_season_optimized.py --test-run --dry-run

# Moderate optimization (2x-3x faster, safe)
python collect_2025_season_optimized.py --concurrent-dates 2

# Aggressive optimization (4x-5x faster, higher performance)
python collect_2025_season_optimized.py --concurrent-dates 4 --aggressive

# Full season with moderate optimization
python collect_2025_season_optimized.py --start-date 2025-01-01
```

### Option 2: Standard Collection (Original)
```bash
# Collect entire 2025 season (January 1st to today)
python collect_2025_season.py

# Custom date range
python collect_2025_season.py --start-date 2025-03-01 --end-date 2025-10-31

# Resume from checkpoint (if interrupted)
python collect_2025_season.py --resume

# Dry run to test configuration
python collect_2025_season.py --dry-run
```

### Option 3: Quick Testing First
```bash
# Test system connectivity
python quick_season_collection.py --test

# Test with last 7 days
python quick_season_collection.py

# Test specific date range
python quick_season_collection.py 2025-07-01 2025-07-07
```

### Option 3: Using Existing CLI Commands
```bash
# Check current data status
uv run -m mlb_sharp_betting.cli data status

# Collect fresh data (current approach)
uv run -m mlb_sharp_betting.cli data collect

# Run diagnostics
uv run -m mlb_sharp_betting.cli diagnostics run-full-diagnostic
```

## 📊 **Collection Scope**

### What Data Gets Collected
- **Date Range**: January 1, 2025 to current date
- **Bet Types**: Moneyline, Spreads, Totals
- **Sportsbooks**: Multiple books available on SportsbookReview
- **Game Information**: Now includes datetime, home/away teams directly in betting tables

### Expected Data Volume
- **Estimated Games**: ~2,430 games per full season
- **Betting Records**: ~15-45 records per game (3 bet types × 3-15 sportsbooks)
- **Total Records**: ~36,000-108,000 betting records for full season
- **Enhanced Data**: Each record now includes game_datetime, home_team, away_team

## 🚀 **Step-by-Step Full Season Collection**

### Step 1: Test System First
```bash
# Test connectivity and basic functionality
python quick_season_collection.py --test
```

### Step 2: Small Test Collection
```bash
# Test with a small date range (e.g., one week)
python quick_season_collection.py 2025-07-01 2025-07-07
```

### Step 3: Run Full Collection
```bash
# Start full season collection
python collect_2025_season.py

# Monitor progress - the script shows:
# [12:34:56]   25.3% - Scraped 2025-03-15 (75/296 days)
# [12:35:02]   25.7% - Scraped 2025-03-16 (76/296 days)
```

### Step 4: Verify Results
```bash
# Check what was collected
python collect_2025_season.py --verify-only

# Or check database directly
psql -d mlb_betting -c "
SELECT 
    COUNT(*) as total_games,
    MIN(game_datetime) as earliest_game,
    MAX(game_datetime) as latest_game
FROM mlb_betting.moneyline 
WHERE game_datetime >= '2025-01-01';"
```

## 💡 **Advanced Options**

### Resume from Interruption
The collection system supports checkpointing:
```bash
# If collection was interrupted, resume from checkpoint
python collect_2025_season.py --resume

# Or start fresh (ignore checkpoints)
python collect_2025_season.py --no-resume
```

### Custom Date Ranges
```bash
# Collect specific months
python collect_2025_season.py --start-date 2025-04-01 --end-date 2025-09-30

# Collect just spring training
python collect_2025_season.py --start-date 2025-02-15 --end-date 2025-03-31

# Collect just regular season (approximate)
python collect_2025_season.py --start-date 2025-03-28 --end-date 2025-09-29
```

### Output and Logging
```bash
# Custom output directory
python collect_2025_season.py --output-dir ./my_collection_results

# Results are automatically saved to:
# - ./season_2025_output/collection_results_YYYYMMDD_HHMMSS.json
# - Database tables: mlb_betting.moneyline, spreads, totals
```

## 🔧 **Technical Details**

### Collection Process
1. **Connectivity Test**: Verifies SportsbookReview.com access
2. **Historical Scraping**: Scrapes each date for all bet types
3. **Data Processing**: Parses HTML and extracts betting data
4. **Database Storage**: Stores in PostgreSQL with enhanced columns
5. **Verification**: Confirms data was stored correctly

### Enhanced Database Schema
Your betting tables now include:
```sql
-- Each betting table now has these additional columns:
game_datetime TIMESTAMP WITH TIME ZONE  -- Game date and time
home_team VARCHAR(5)                     -- Home team abbreviation
away_team VARCHAR(5)                     -- Away team abbreviation
```

### Rate Limiting
- **Delay Between Requests**: 2 seconds (configurable)
- **Max Concurrent Requests**: 3 (configurable)
- **Circuit Breaker**: Automatic retry on failures
- **Checkpoint System**: Resume from interruptions

## 📈 **Monitoring Progress**

### Real-Time Progress
The collection shows live progress:
```
🚀 COLLECTING 2025 MLB SEASON DATA
============================================================
📅 Date Range: 2025-01-01 to 2025-07-10
📊 Total Days: 191
📁 Output Directory: ./season_2025_output
🔄 Resume from Checkpoint: True
============================================================

✅ Collection orchestrator initialized
✅ Connectivity test passed

📡 Starting data collection...
[14:23:15]   1.0% - Scraped 2025-01-01 (1/191 days)
[14:23:18]   1.6% - Scraped 2025-01-02 (2/191 days)
```

### Database Verification
Check your progress anytime:
```sql
-- Check collection progress
SELECT 
    DATE(game_datetime) as game_date,
    COUNT(*) as betting_records,
    COUNT(DISTINCT home_team || ' vs ' || away_team) as unique_games
FROM mlb_betting.moneyline 
WHERE game_datetime >= '2025-01-01'
GROUP BY DATE(game_datetime)
ORDER BY game_date DESC
LIMIT 10;
```

## 🎯 **Expected Results**

### Successful Collection Shows
```
🎉 SEASON 2025 COLLECTION SUMMARY
==================================================
⏱️  Duration: 2:34:56
📅 Date Range: 2025-01-01 to 2025-07-10
📊 Total Days: 191
✅ Status: success

🔍 VERIFYING COLLECTION RESULTS
========================================
📊 Games Collected: 1,247
💰 Moneyline Records: 18,705
📈 Spreads Records: 12,470
📊 Totals Records: 15,588
📋 Total Betting Records: 46,763
📈 Average Records per Game: 37.5
```

### Database Query Results
```sql
-- Sample query showing enhanced data
SELECT 
    game_datetime,
    home_team,
    away_team,
    sportsbook,
    home_ml,
    away_ml
FROM mlb_betting.moneyline 
WHERE game_datetime >= '2025-07-01' 
ORDER BY game_datetime DESC 
LIMIT 5;

-- Results show enhanced accessibility:
--       game_datetime      | home_team | away_team | sportsbook | home_ml | away_ml
-- 2025-07-09 19:10:00-04  |    NYY    |    BOS    |  FanDuel   |  -145   |   125
-- 2025-07-09 19:10:00-04  |    NYY    |    BOS    |  DraftKings|  -150   |   130
```

## 🚨 **Troubleshooting**

### Common Issues
1. **Connection Timeout**: SportsbookReview may have rate limits
   - Solution: The script automatically handles retries
   
2. **Missing Data**: Some dates may have no games
   - Solution: This is normal (off-season, All-Star break)
   
3. **Interrupted Collection**: Process stopped mid-collection
   - Solution: Use `--resume` flag to continue from checkpoint

### Error Recovery
```bash
# If collection fails, check logs and resume
python collect_2025_season.py --resume

# If data seems incomplete, verify specific dates
python quick_season_collection.py 2025-07-01 2025-07-07
```

### Performance Tips
- **Run during off-peak hours** (SportsbookReview less busy)
- **Use resume functionality** for long collections
- **Monitor database size** (expect ~50MB per month of data)

## 📋 **Next Steps After Collection**

### Data Analysis
With enhanced betting tables, you can now easily:
```sql
-- Find all Cardinals games
SELECT * FROM mlb_betting.moneyline 
WHERE home_team = 'STL' OR away_team = 'STL';

-- Analyze betting patterns by date
SELECT DATE(game_datetime), COUNT(*) 
FROM mlb_betting.moneyline 
GROUP BY DATE(game_datetime) 
ORDER BY DATE(game_datetime);

-- Cross-reference with game outcomes
SELECT m.*, g.home_score, g.away_score
FROM mlb_betting.moneyline m
JOIN public.games g ON m.game_id = g.id
WHERE m.game_datetime >= '2025-07-01';
```

### Integration with Existing System
Your enhanced data now works seamlessly with:
- **Backtesting Engine**: Historical analysis with proper game identification
- **Strategy Processors**: Enhanced filtering by team/date
- **Recommendation System**: Better context for betting decisions

## 🎉 **Summary**

You now have multiple options to collect all 2025 season data:

1. **🚀 Full Season**: `python collect_2025_season.py`
2. **🧪 Quick Test**: `python quick_season_collection.py --test`
3. **📊 Verification**: `python collect_2025_season.py --verify-only`

The enhanced database schema with `game_datetime`, `home_team`, and `away_team` makes the collected data much more accessible and useful for analysis.

---

**General Balls** 