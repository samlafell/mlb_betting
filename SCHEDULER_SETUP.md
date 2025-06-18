# MLB Betting Scheduler Setup Guide

## Overview

I've created an automated scheduler system that will:

1. **Run your entrypoint every hour** for regular data collection
2. **Alert you 5 minutes before each game starts** with fresh analysis
3. **Automatically handle daily setup** (gets today's games at 6 AM EST)

## Quick Start

### 1. Install Dependencies

First, install the required scheduler dependency:

```bash
uv sync
```

### 2. Test the Scheduler

Test that everything works:

```bash
# Test your entrypoint manually first
uv run src/mlb_sharp_betting/entrypoint.py --verbose

# Test the scheduler (will show upcoming games and schedule)
uv run run_scheduler.py
```

### 3. Run the Scheduler Permanently

Choose one of these options:

#### Option A: Background Process (Recommended)

```bash
# Start in background
nohup uv run run_scheduler.py > scheduler.log 2>&1 &
echo $! > scheduler.pid

# To stop later
kill $(cat scheduler.pid)
rm scheduler.pid

# Check logs
tail -f scheduler.log
```

#### Option B: Simple Cron Job (Hourly Only)

If you only want hourly runs without game alerts:

```bash
# Add to crontab
crontab -e

# Add this line:
0 * * * * cd /Users/samlafell/Documents/programming_projects/sports_betting_dime_splits && /usr/bin/env uv run src/mlb_sharp_betting/entrypoint.py >> logs/hourly.log 2>&1
```

#### Option C: System Service (Advanced)

For macOS using launchd:

```bash
# Create ~/Library/LaunchAgents/com.mlbbetting.scheduler.plist
# (See detailed instructions below)
```

## What the Scheduler Does

### Hourly Execution
- Runs at the top of every hour (1:00, 2:00, 3:00, etc.)
- Executes your full entrypoint pipeline
- Collects betting data from all sources
- Updates your database

### Game Alerts
- Gets today's MLB schedule at 6 AM EST
- Schedules alerts 5 minutes before each game
- Runs fresh analysis before each game
- Sends notifications (currently to console, easily extensible)

### Monitoring
- Tracks execution metrics
- Logs all activities
- Handles errors gracefully
- Shows status updates every 5 minutes

## Notifications

Currently notifications appear in the console/logs. You can easily extend this by modifying the `send_notification` method in `src/mlb_sharp_betting/services/scheduler.py` to add:

- Slack webhooks
- Discord messages  
- Email alerts
- SMS via Twilio
- Push notifications

## Advanced Setup Options

### macOS LaunchAgent (User Service)

Create `~/Library/LaunchAgents/com.mlbbetting.scheduler.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.mlbbetting.scheduler</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/env</string>
        <string>uv</string>
        <string>run</string>
        <string>run_scheduler.py</string>
    </array>
    <key>WorkingDirectory</key>
    <string>/Users/samlafell/Documents/programming_projects/sports_betting_dime_splits</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/Users/samlafell/Library/Logs/mlb-betting-scheduler.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/samlafell/Library/Logs/mlb-betting-scheduler-error.log</string>
</dict>
</plist>
```

Then load it:

```bash
launchctl load ~/Library/LaunchAgents/com.mlbbetting.scheduler.plist
```

### Linux Systemd (User Service)

Create `~/.config/systemd/user/mlb-betting-scheduler.service`:

```ini
[Unit]
Description=MLB Betting Analysis Scheduler
After=network.target

[Service]
Type=simple
WorkingDirectory=/path/to/your/project
ExecStart=/usr/bin/env uv run run_scheduler.py
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
```

Then enable it:

```bash
systemctl --user daemon-reload
systemctl --user enable mlb-betting-scheduler
systemctl --user start mlb-betting-scheduler
```

## Monitoring Commands

### Check Status
```bash
# If running in background
ps aux | grep run_scheduler.py

# Check logs
tail -f scheduler.log

# Test connection to MLB API
uv run -c "from src.mlb_sharp_betting.services.mlb_api_service import MLBStatsAPIService; api = MLBStatsAPIService(); games = api.get_games_for_date(datetime.date.today()); print(f'Found {len(games)} games today')"
```

### Management Commands

```bash
# Manual run of entrypoint
uv run src/mlb_sharp_betting/entrypoint.py

# Test scheduler without starting it
uv run -c "
from src.mlb_sharp_betting.services.scheduler import MLBBettingScheduler
import asyncio
scheduler = MLBBettingScheduler()
print(scheduler.get_status())
"

# Check what games are scheduled today
uv run -c "
from src.mlb_sharp_betting.services.mlb_api_service import MLBStatsAPIService
from datetime import date
api = MLBStatsAPIService()
games = api.get_games_for_date(date.today())
for game in games:
    print(f'{game.away_team} @ {game.home_team} at {game.game_date}')
"
```

## Customization

### Timing
Edit `src/mlb_sharp_betting/services/scheduler.py`:
- Change `alert_minutes_before_game` (default: 5 minutes)
- Modify hourly schedule (default: every hour at :00)
- Adjust daily setup time (default: 6 AM EST)

### Notifications
Add your preferred notification method in the `send_notification` method:

```python
async def send_notification(self, message: str, context: str = "alert") -> None:
    # Add your notification code here
    # Examples:
    # - Slack webhook
    # - Discord webhook  
    # - Email via SMTP
    # - SMS via Twilio
    pass
```

## Troubleshooting

### Common Issues

1. **Permission denied**: Make sure UV is installed and accessible
2. **Import errors**: Run `uv sync` to install all dependencies
3. **No games found**: Check your internet connection and MLB API access
4. **Scheduler stops**: Check logs for errors, may need to restart

### Debug Mode

Run with verbose logging:

```bash
uv run run_scheduler.py --verbose
```

### Manual Testing

Test individual components:

```bash
# Test MLB API
uv run -c "from src.mlb_sharp_betting.services.mlb_api_service import MLBStatsAPIService; print('MLB API works!')"

# Test entrypoint
uv run src/mlb_sharp_betting/entrypoint.py --dry-run

# Test scheduler setup
uv run -c "from src.mlb_sharp_betting.services.scheduler import MLBBettingScheduler; s = MLBBettingScheduler(); print('Scheduler initialized!')"
```

## Summary

Your automated betting analysis system is now ready! 🎰

- **Hourly analysis**: Keeps your data fresh
- **Game alerts**: Never miss an opportunity  
- **Automated scheduling**: No manual intervention needed
- **Easy monitoring**: Simple logs and status checks

The system will automatically:
1. Collect betting data every hour
2. Alert you before each game with fresh analysis
3. Handle errors gracefully
4. Log everything for your review

**Happy betting!** 🏈

---

*General Balls* 