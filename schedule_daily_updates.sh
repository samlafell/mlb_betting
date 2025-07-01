#!/bin/bash
# Daily Outcome Updater Cron Job Script
#
# This script should be added to your crontab to run daily at midnight EST
# Add to crontab with: crontab -e
# Then add this line:
# 0 0 * * * /path/to/sports_betting_dime_splits/schedule_daily_updates.sh >> /path/to/logs/daily_updates.log 2>&1

# Change to project directory
cd /Users/samlafell/Documents/programming_projects/sports_betting_dime_splits

# Source environment variables
source .env

# Run the daily outcome updater
echo "$(date): Starting daily outcome update..."
uv run python update_daily_outcomes.py --days-back 7

# Also update strategy performance if needed (optional)
echo "$(date): Daily outcome update completed" 