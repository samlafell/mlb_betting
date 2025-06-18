# Daily MLB Game Updater

This script is designed to be run daily to fetch completed MLB game results and store them in the database with real betting lines.

## Features

- ✅ **Real Betting Lines Only**: No default values - only processes games with actual betting data
- 📅 **Daily Updates**: Checks both yesterday's and today's completed games
- 📊 **Comprehensive Reporting**: Shows game results, betting outcomes, and summary statistics
- 🎯 **Smart Processing**: Only updates games that have completed and have betting lines available

## Usage

### Manual Daily Run
```bash
# Run the daily updater
uv run test_game_updater.py

# Or use the wrapper script
uv run run_daily_update.py
```

### Automated Daily Run (Cron Job)
Add this to your crontab to run daily at 6 AM:
```bash
# Edit your crontab
crontab -e

# Add this line (adjust path as needed)
0 6 * * * cd /path/to/sports_betting_dime_splits && /usr/local/bin/uv run test_game_updater.py
```

## What It Does

### 1. Yesterday's Games
- Fetches all completed games from yesterday
- Most games will be finished by the next day
- Processes outcomes with real betting lines

### 2. Today's Games  
- Checks for any games that finished early today
- Useful for day games or games that end early

### 3. Real Betting Lines Only
- **No Default Values**: Games without betting lines are skipped
- Ensures all calculations use actual market data
- Logs warnings when betting lines are missing

### 4. Output Information
For each processed game:
- **Game**: Team matchup
- **Score**: Final score
- **Winner**: Home or Away
- **Total**: Actual total vs betting line (Over/Under result)
- **Spread**: Home spread line and cover result

### 5. Summary Statistics
- Total games processed
- Home vs Away win percentages  
- Over vs Under percentages
- Home spread cover percentages
- Database status

## Example Output

```
🏀 Daily MLB Game Updater
==================================================
📅 Running daily update on 2025-06-17

📅 Processing completed games for 2025-06-16
✅ Found 12 completed games from 2025-06-16

🏆 2025-06-16 Game: Pittsburgh Pirates @ Detroit Tigers
   Score: 4 - 7
   Winner: Home
   Total: 11 (Line: 8.5, Over: True)
   Home Spread: -1.5 (Home covered: True)

📊 Daily Update Summary
==============================
Total games processed: 12
Yesterday (2025-06-16): 12 games
Today (2025-06-17): 0 games

📈 Betting Results Summary:
   Home vs Away: 7-5 (58.3% home)
   Over vs Under: 8-4 (66.7% over)
   Home Spread: 6-6 (50.0% home cover)
```

## Important Notes

### Database Access
- Close DBeaver or other database tools before running
- DuckDB requires exclusive access for writes

### Betting Lines Requirement
- Games without betting lines will be skipped
- Ensure your betting splits data is up to date
- Run VSIN scraper before the daily updater for best results

### Error Handling
- Script exits with code 1 on failure
- Detailed error logging for troubleshooting
- Safe to re-run - won't duplicate data

## Recommended Daily Workflow

1. **Morning**: Run VSIN scraper to get latest betting lines
2. **Afternoon**: Run daily game updater to process completed games
3. **Evening**: Check results and run analysis queries

```bash
# Complete daily workflow
uv run src/mlb_sharp_betting/examples/pinnacle_demo.py  # Get betting lines
uv run test_game_updater.py                             # Update game outcomes
```

## Troubleshooting

### Database Lock Error
```
Close DBeaver or other database connections and retry
```

### No Betting Lines Found
```
Run the VSIN scraper first to populate betting splits data
```

### No Completed Games
```
Normal - not all days have completed games
Check MLB schedule for off-days
```

## Files

- `test_game_updater.py` - Main daily updater script
- `run_daily_update.py` - Simple wrapper script  
- `src/mlb_sharp_betting/services/game_updater.py` - Core updater service 