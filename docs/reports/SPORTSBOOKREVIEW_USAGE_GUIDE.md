# SportsbookReview Daily Extract & Verification Guide

This guide shows you how to run daily SportsbookReview data extracts and verify that the data was saved correctly in your PostgreSQL database.

## 📋 Overview

You now have three tools for running and verifying SportsbookReview data collection:

1. **`run_daily_sportsbookreview_extract.py`** - Complete Python script for extraction and verification
2. **`verify_sportsbookreview_data.sql`** - Standalone SQL verification script
3. **Manual CLI commands** - For advanced usage

## 🚀 Quick Start

### Option 1: Complete Process (Recommended)

Run extraction and verification for a specific date:

```bash
# Extract and verify data for July 5, 2025
uv run python run_daily_sportsbookreview_extract.py --date 2025-07-05

# Extract and verify yesterday's data
uv run python run_daily_sportsbookreview_extract.py --date yesterday

# Extract and verify today's data
uv run python run_daily_sportsbookreview_extract.py --date today
```

### Option 2: Verification Only

If data already exists and you just want to verify it:

```bash
# Only verify existing data (skip extraction)
uv run python run_daily_sportsbookreview_extract.py --date 2025-07-05 --verify-only
```

### Option 3: SQL-Only Verification

Use the standalone SQL script to verify data:

```bash
# Edit the date in the SQL file first, then run:
psql -h localhost -U samlafell -d mlb_betting -f verify_sportsbookreview_data.sql
```

## 📊 What Gets Extracted

For each day, the system extracts:

- **Game Information**: Teams, date/time, venue, final scores
- **Moneyline Odds**: Opening and current odds from multiple sportsbooks
- **Spread Odds**: Point spreads and prices from multiple sportsbooks  
- **Total Odds**: Over/under lines and prices from multiple sportsbooks
- **Public Betting Data**: Betting percentages for each market
- **Sportsbook Coverage**: Data from 4+ major sportsbooks (FanDuel, DraftKings, Bet365, BetRivers, etc.)

## 🔍 Verification Checks

The verification process runs 8 comprehensive checks:

1. **Games Count** - Ensures games were found for the target date
2. **Games Data Quality** - Validates completeness of game records
3. **Betting Data Count** - Confirms betting records were stored
4. **Betting Data Quality** - Checks completeness of betting data
5. **Data Consistency** - Looks for orphaned records and referential integrity
6. **Sportsbook Coverage** - Ensures multiple sportsbooks are represented
7. **Team Normalization** - Validates team names are properly formatted
8. **Date Accuracy** - Confirms all records have correct dates

## 📈 Expected Results

### Successful Extract Should Show:

```
✅ Data extraction completed successfully
✅ Data verification completed successfully

EXTRACTION RESULTS:
  Status: SUCCESS
  Games Collected: 15
  Games Stored: 15
  Betting Records: 180
  Success Rate: 100.0%

VERIFICATION RESULTS:
  Status: SUCCESS
  Checks Passed: 8/8
  Success Rate: 100.0%
  Total Games: 15
  Total Betting Records: 180
  Sportsbooks Found: 4
```

### Database Tables Populated:

- **`public.games`**: Core game information with SportsbookReview game IDs
- **`mlb_betting.moneyline`**: Moneyline odds and betting data
- **`mlb_betting.spreads`**: Point spread odds and betting data
- **`mlb_betting.totals`**: Over/under odds and betting data

## 🛠️ Advanced Usage

### Verbose Logging

Get detailed debug information:

```bash
uv run python run_daily_sportsbookreview_extract.py --date 2025-07-05 --verbose
```

### Custom Date Formats

The script accepts various date formats:

```bash
# Standard format
--date 2025-07-05

# Alternative formats
--date 07/05/2025
--date 20250705

# Relative dates
--date yesterday
--date today
```

### Batch Processing

Extract multiple days in sequence:

```bash
# Process a week of data
for date in 2025-07-01 2025-07-02 2025-07-03 2025-07-04 2025-07-05; do
    uv run python run_daily_sportsbookreview_extract.py --date $date
done
```

## 🔧 Manual Database Queries

### Quick Status Check

```sql
-- Check recent extraction activity
SELECT 
    game_date,
    COUNT(*) as games,
    MAX(created_at) as last_updated
FROM public.games 
WHERE game_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY game_date 
ORDER BY game_date DESC;
```

### Betting Data Summary

```sql
-- Summary of betting records by date
SELECT 
    g.game_date,
    COUNT(DISTINCT g.id) as games,
    COUNT(m.id) as moneyline_records,
    COUNT(s.id) as spread_records,
    COUNT(t.id) as total_records,
    COUNT(DISTINCT m.sportsbook) + 
    COUNT(DISTINCT s.sportsbook) + 
    COUNT(DISTINCT t.sportsbook) as unique_sportsbooks
FROM public.games g
LEFT JOIN mlb_betting.moneyline m ON g.id = m.game_id
LEFT JOIN mlb_betting.spreads s ON g.id = s.game_id
LEFT JOIN mlb_betting.totals t ON g.id = t.game_id
WHERE g.game_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY g.game_date
ORDER BY g.game_date DESC;
```

### Sportsbook Coverage

```sql
-- Check which sportsbooks have data for a specific date
SELECT 
    sportsbook,
    COUNT(*) as total_records,
    COUNT(CASE WHEN source_table = 'moneyline' THEN 1 END) as moneyline,
    COUNT(CASE WHEN source_table = 'spreads' THEN 1 END) as spreads,
    COUNT(CASE WHEN source_table = 'totals' THEN 1 END) as totals
FROM (
    SELECT sportsbook, 'moneyline' as source_table 
    FROM mlb_betting.moneyline m
    JOIN public.games g ON m.game_id = g.id 
    WHERE g.game_date = '2025-07-05'
    
    UNION ALL
    
    SELECT sportsbook, 'spreads' as source_table 
    FROM mlb_betting.spreads s
    JOIN public.games g ON s.game_id = g.id 
    WHERE g.game_date = '2025-07-05'
    
    UNION ALL
    
    SELECT sportsbook, 'totals' as source_table 
    FROM mlb_betting.totals t
    JOIN public.games g ON t.game_id = g.id 
    WHERE g.game_date = '2025-07-05'
) combined
GROUP BY sportsbook
ORDER BY total_records DESC;
```

## 🚨 Troubleshooting

### Common Issues

**1. No games found for date**
- Check if MLB games were scheduled for that date
- Verify SportsbookReview.com has data for that date
- Check if date format is correct

**2. Low sportsbook coverage**
- SportsbookReview may have limited data for older dates
- Some sportsbooks may not have been operating on certain dates
- Check the source website manually

**3. Database connection errors**
- Ensure PostgreSQL is running
- Verify database credentials in the script
- Check if `mlb_betting` database exists

**4. Rate limiting errors**
- The scraper has built-in rate limiting
- If you see 429 errors, increase delays between requests
- SportsbookReview may temporarily block rapid requests

### Getting Help

If you encounter issues:

1. **Check the logs**: Look at `sportsbookreview_extract.log`
2. **Run verification only**: Use `--verify-only` to check existing data
3. **Use verbose mode**: Add `--verbose` for detailed logging
4. **Check database manually**: Use the SQL verification script

### Log Files

The script creates detailed logs:

- **`sportsbookreview_extract.log`**: Complete extraction and verification logs
- **`./output/`**: Checkpoint files and intermediate results

## 📅 Daily Workflow Recommendation

For production use, set up a daily workflow:

```bash
#!/bin/bash
# daily_sportsbookreview.sh

# Extract yesterday's data
uv run python run_daily_sportsbookreview_extract.py --date yesterday

# Check return code
if [ $? -eq 0 ]; then
    echo "✅ Daily SportsbookReview extract completed successfully"
else
    echo "❌ Daily SportsbookReview extract failed"
    # Send alert email/Slack notification
fi
```

Run this script daily via cron:

```bash
# Add to crontab (run at 6 AM daily)
0 6 * * * /path/to/daily_sportsbookreview.sh
```

## 🎯 Success Metrics

A successful daily extract should achieve:

- **Games Found**: 10-15 games per day (during MLB season)
- **Sportsbook Coverage**: 4+ unique sportsbooks
- **Data Completeness**: 95%+ complete records
- **Betting Records**: 120-200 total records (15 games × 3 bet types × 4 sportsbooks)
- **Success Rate**: 95%+ scraping success rate

**General Balls** ⚾ 