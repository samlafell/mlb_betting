# MLB Betting Scheduler Setup Guide

## Overview

The MLB betting system now uses a **single consolidated scheduler** that handles:

1. **Hourly data collection** from SBD and VSIN (every hour at :00)
2. **Multi-stage pre-game workflows** (30min, 15min, 5min before each game)
3. **Daily game setup** (gets today's games at 6 AM EST)
4. **Email notifications** for all workflow results

## Quick Start

### 1. Install Dependencies

```bash
uv sync
```

### 2. Test the System

```bash
# Test data collection manually
source .env && uv run python src/mlb_sharp_betting/entrypoint.py --verbose

# Test the scheduler status
source .env && uv run python src/mlb_sharp_betting/cli.py pregame status
```

### 3. Start the Scheduler

```bash
# Start the consolidated scheduler
./start_pregame_scheduler.sh

# Monitor logs
tail -f pregame_scheduler.log

# Stop when needed
./stop_pregame_scheduler.sh
```

## What the Scheduler Does

### Hourly Data Collection
- **Runs at the top of every hour** (1:00, 2:00, 3:00, etc.)
- **Collects fresh betting splits** from SBD and VSIN
- **Updates the database** with new data
- **Console notifications** for success/failure

### Pre-Game Workflows
- **30 minutes before each game**: Data collection only
- **15 minutes before each game**: Data collection only  
- **5 minutes before each game**: Final analysis + email notification

### Daily Setup
- **Runs at 6:00 AM EST** every day
- **Fetches today's MLB schedule**
- **Schedules all workflows** for the day's games
- **Email confirmation** of scheduled games

## Schedule Example

For a game starting at **7:00 PM EST**:
- **6:30 PM**: 30-min data collection
- **6:45 PM**: 15-min data collection
- **6:55 PM**: Final pre-game analysis (with email)

Plus **hourly collections** at 1:00, 2:00, 3:00, 4:00, 5:00, 6:00 PM, etc.

## Management Commands

### Status and Monitoring
```bash
# Check scheduler status
source .env && uv run python src/mlb_sharp_betting/cli.py pregame status

# View live logs
tail -f pregame_scheduler.log

# Check if running
ps aux | grep pregame

# Manual data collection test
source .env && uv run python src/mlb_sharp_betting/entrypoint.py
```

### Start/Stop
```bash
# Start scheduler
./start_pregame_scheduler.sh

# Stop scheduler  
./stop_pregame_scheduler.sh

# Restart (stop then start)
./stop_pregame_scheduler.sh && sleep 2 && ./start_pregame_scheduler.sh
```

## Email Configuration

The scheduler sends email notifications for:
- Daily setup summaries
- Final pre-game analysis results
- Error notifications

**Required environment variables:**
```bash
# In your .env file
EMAIL_FROM_ADDRESS=your-email@gmail.com
EMAIL_APP_PASSWORD=your-gmail-app-password  
EMAIL_TO_ADDRESSES=["recipient@gmail.com"]
```

**Gmail App Password Setup:**
1. Enable 2FA on your Gmail account
2. Go to Google Account settings → Security → App passwords
3. Generate an app password for this application
4. Use that password (not your regular Gmail password)

## Notifications

### Console Notifications
Real-time notifications appear in the console:
- ✅ Successful hourly collections
- ❌ Failed collections with error details
- 📊 Data collection progress
- 🏈 Pre-game workflow triggers

### Email Notifications
Automated emails for:
- **Daily Setup**: Summary of scheduled games
- **Final Pre-Game Analysis**: Complete betting recommendations
- **Errors**: Failed collections or workflows

## Advanced Setup Options

### macOS LaunchAgent (Auto-start on login)

Create `~/Library/LaunchAgents/com.mlbbetting.pregame.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.mlbbetting.pregame</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>start_pregame_scheduler.sh</string>
    </array>
    <key>WorkingDirectory</key>
    <string>/Users/samlafell/Documents/programming_projects/sports_betting_dime_splits</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>/Users/samlafell/Library/Logs/mlb-betting-pregame.log</string>
    <key>StandardErrorPath</key>
    <string>/Users/samlafell/Library/Logs/mlb-betting-pregame-error.log</string>
</dict>
</plist>
```

Load it:
```bash
launchctl load ~/Library/LaunchAgents/com.mlbbetting.pregame.plist
```

### Linux Systemd Service

Create `~/.config/systemd/user/mlb-betting-pregame.service`:

```ini
[Unit]
Description=MLB Betting Pre-Game Scheduler
After=network.target

[Service]
Type=simple
WorkingDirectory=/path/to/your/project
ExecStart=/bin/bash start_pregame_scheduler.sh
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
```

Enable it:
```bash
systemctl --user daemon-reload
systemctl --user enable mlb-betting-pregame
systemctl --user start mlb-betting-pregame
```

## Troubleshooting

### Common Issues

**Scheduler won't start:**
```bash
# Check dependencies
uv sync

# Check environment
source .env && env | grep EMAIL

# Check logs
cat pregame_scheduler.log
```

**No hourly collections:**
- Verify scheduler is running: `ps aux | grep pregame`
- Check logs: `tail -f pregame_scheduler.log`
- Wait for next hour mark (collections run at :00)

**Email not working:**
- Verify Gmail app password setup
- Check EMAIL_* environment variables
- Test with manual run: `uv run python src/mlb_sharp_betting/entrypoint.py`

**Database locks:**
```bash
# Kill stuck processes
ps aux | grep python | grep mlb
kill <PID>

# Restart scheduler
./stop_pregame_scheduler.sh && ./start_pregame_scheduler.sh
```

### Log Locations

- **Main log**: `pregame_scheduler.log`
- **Process ID**: `pregame_scheduler.pid`  
- **Manual triggers**: `manual_triggers.log`

## Migration from Old Scheduler

If you were using the old dual-scheduler setup:

1. **Stop old schedulers**: The old `start_scheduler.sh` files have been removed
2. **Use new consolidated scheduler**: Only `start_pregame_scheduler.sh` is needed
3. **All functionality preserved**: Hourly + pre-game workflows in one scheduler
4. **No configuration changes needed**: Same email and database settings

The new scheduler is more efficient and eliminates conflicts between multiple schedulers trying to collect data at the same time.

---

*General Balls* 