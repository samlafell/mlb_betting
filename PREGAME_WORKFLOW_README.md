# MLB Pre-Game Workflow System

Automated three-stage workflow system that triggers 5 minutes before each MLB game to collect betting data, perform sharp action analysis, and send email notifications with results.

## 🎯 Overview

This system provides a completely automated workflow that:
1. **Stage 1: Pre-Game Data Collection** - Triggers main entrypoint program for betting data collection
2. **Stage 2: Betting Analysis** - Executes sharp action detection with adaptive thresholds
3. **Stage 3: Automated Notification** - Sends email with results including text output and analysis files

### Key Features

- ✅ **Separate Scheduler** - Independent from existing hourly data collection
- ✅ **3 Retry Attempts** - With exponential backoff for failed stages
- ✅ **Comprehensive Error Handling** - Proper logging and timeout handling
- ✅ **Email Notifications** - Gmail SMTP with HTML/plain text for mobile reading
- ✅ **File Attachments** - Analysis results included in emails
- ✅ **Success & Failure Notifications** - Complete workflow visibility
- ✅ **Preserves Existing Systems** - Hourly data collection and daily setup unchanged

## 🚀 Quick Start

### 1. Email Configuration

First, configure Gmail for notifications:

```bash
# Interactive email setup
uv run python src/mlb_sharp_betting/cli.py pregame configure-email

# Or set environment variables manually
export EMAIL_FROM_ADDRESS="your-gmail@gmail.com"
export EMAIL_APP_PASSWORD="your-gmail-app-password" 
export EMAIL_TO_ADDRESSES="recipient1@email.com,recipient2@email.com"
```

**Gmail Setup Requirements:**
1. Enable 2-factor authentication on your Gmail account
2. Generate an App Password at: https://myaccount.google.com/apppasswords
3. Use the generated App Password (not your regular password)

### 2. Start the Scheduler

```bash
# Start the pre-game workflow scheduler
./start_pregame_scheduler.sh

# Or use the CLI directly
uv run python src/mlb_sharp_betting/cli.py pregame start
```

The scheduler will:
- Check for MLB games daily at 6 AM EST
- Schedule workflows 5 minutes before each game
- Send email notifications with results

### 3. Monitor and Control

```bash
# Check scheduler status
./start_pregame_scheduler.sh --status
uv run python src/mlb_sharp_betting/cli.py pregame status

# View live logs
tail -f pregame_scheduler.log

# Stop the scheduler
./stop_pregame_scheduler.sh

# Restart the scheduler
./start_pregame_scheduler.sh --restart
```

## 📋 Available Commands

### Scheduler Management

```bash
# Start scheduler with custom settings
uv run python src/mlb_sharp_betting/cli.py pregame start \
    --alert-minutes 5 \
    --daily-setup-hour 6

# Show detailed status
uv run python src/mlb_sharp_betting/cli.py pregame status

# Test workflow with today's first game
uv run python src/mlb_sharp_betting/cli.py pregame test-workflow

# Test with specific game ID
uv run python src/mlb_sharp_betting/cli.py pregame test-workflow --game-pk 12345
```

### Game Information

```bash
# List today's MLB games
uv run python src/mlb_sharp_betting/cli.py pregame list-games

# List games for specific date
uv run python src/mlb_sharp_betting/cli.py pregame list-games --date 2024-07-15
```

### Email Setup

```bash
# Interactive email configuration
uv run python src/mlb_sharp_betting/cli.py pregame configure-email

# This will prompt for:
# - Gmail address
# - Gmail app password (hidden input)
# - Recipient email addresses (comma-separated)
```

## 🔧 Configuration Options

### Scheduler Settings

- `--alert-minutes`: Minutes before game start to trigger workflow (default: 5)
- `--daily-setup-hour`: Hour (EST) to run daily game setup (default: 6)
- `--project-root`: Project root directory (auto-detected by default)

### Email Settings (Environment Variables)

```bash
EMAIL_FROM_ADDRESS="your-gmail@gmail.com"
EMAIL_APP_PASSWORD="your-gmail-app-password"
EMAIL_TO_ADDRESSES="recipient1@email.com,recipient2@email.com"
```

### Optional Settings

```bash
# Timezone (defaults to EST)
MLB_TIMEZONE="America/New_York"

# Analysis timeout in seconds (default: 300)
ANALYSIS_TIMEOUT=300

# Data collection timeout in seconds (default: 180)
DATA_COLLECTION_TIMEOUT=180
```

## 📧 Email Notifications

### Email Content

Each notification includes:

1. **Subject Line**: Game matchup and workflow status
2. **HTML Body**: Mobile-optimized with game details and stage results
3. **Plain Text Body**: Same content in plain text format
4. **Attachments**: Analysis output files when available

### Sample Email Structure

```
Subject: ⚾ Workflow Complete: Pirates @ Tigers (Success)

Body:
🏈 MLB Pre-Game Workflow Results
===============================

Game: Pittsburgh Pirates @ Detroit Tigers
Scheduled Time: 2024-07-15 19:10 UTC
Status: ✅ SUCCESS
Total Duration: 42.3 seconds

📊 Stage Results:
✅ Data Collection: Success (18.2s)
✅ Betting Analysis: Success (20.1s) 
✅ Email Notification: Success (4.0s)

📈 Analysis Summary:
- Sharp Action Detected: 3 games
- Strong Signals: Moneyline fade on Pirates
- Recommendation: Follow sharp money

Files Attached:
- analysis_results_20240715_1910.txt
- sharp_signals_summary.csv
```

## 🛠 Architecture

### Components

1. **PreGameWorkflowService** (`src/mlb_sharp_betting/services/pre_game_workflow.py`)
   - Orchestrates the three-stage workflow
   - Handles retry logic and error recovery
   - Generates and sends email notifications
   - Manages file attachments

2. **PreGameScheduler** (`src/mlb_sharp_betting/services/pre_game_scheduler.py`)
   - Monitors MLB games and schedules workflows
   - Runs daily game setup at 6 AM EST
   - Manages workflow job queue
   - Provides status and metrics APIs

3. **CLI Commands** (`src/mlb_sharp_betting/cli/commands/pre_game.py`)
   - Command-line interface for all operations
   - Interactive email configuration
   - Status monitoring and troubleshooting
   - Testing and development tools

### Workflow Stages

#### Stage 1: Data Collection (3 retries, exponential backoff)
```bash
uv run python -m mlb_sharp_betting.entrypoint
```
- Triggers main data collection pipeline
- Collects VSIN and SBD betting splits
- Stores data in DuckDB database
- Timeout: 180 seconds

#### Stage 2: Betting Analysis (3 retries, exponential backoff)
```bash
uv run analysis_scripts/master_betting_detector.py --minutes 5
```
- Executes adaptive sharp action detection
- Analyzes recent betting line movements
- Generates comprehensive analysis report
- Timeout: 300 seconds

#### Stage 3: Email Notification (1 retry)
- Compiles workflow results
- Generates HTML and plain text emails
- Attaches analysis files
- Sends via Gmail SMTP
- Timeout: 60 seconds

## 📊 Monitoring and Troubleshooting

### Log Files

```bash
# Scheduler logs
tail -f pregame_scheduler.log

# Individual workflow logs (timestamped)
ls -la workflow_logs/workflow_*.log

# System logs
tail -f analysis_results.log
```

### Status Checking

```bash
# Comprehensive status
uv run python src/mlb_sharp_betting/cli.py pregame status

# Shows:
# - Running status
# - Email configuration status
# - Scheduled games for today
# - Active workflow jobs
# - Completed workflows count
# - Recent workflow history
# - Performance metrics
```

### Common Issues

#### Email Not Configured
```bash
# Error: Email settings not configured
# Solution: Run email setup
uv run python src/mlb_sharp_betting/cli.py pregame configure-email
```

#### Scheduler Not Starting
```bash
# Check dependencies
uv sync

# Check if already running
./start_pregame_scheduler.sh --status

# Force restart
./start_pregame_scheduler.sh --restart
```

#### Stage Failures
```bash
# Check specific stage logs
tail -f pregame_scheduler.log | grep "Stage"

# Test individual stages
uv run python src/mlb_sharp_betting/cli.py pregame test-workflow
```

#### No Games Detected
```bash
# Verify MLB API connection
uv run python src/mlb_sharp_betting/cli.py pregame list-games

# Check timezone settings
echo $MLB_TIMEZONE
```

## 🔐 Security Notes

### Email Credentials
- Use Gmail App Passwords, not regular passwords
- Store credentials in environment variables, not code
- Consider using `.env` file for local development
- Never commit credentials to version control

### File Permissions
```bash
# Secure the startup script
chmod 700 start_pregame_scheduler.sh

# Secure log files (optional)
chmod 600 pregame_scheduler.log
```

### Network Security
- Scheduler runs locally, no external ports opened
- Email uses Gmail's secure SMTP (TLS)
- API calls use HTTPS endpoints

## 🧪 Testing

### Test Complete Workflow
```bash
# Test with today's first game
uv run python src/mlb_sharp_betting/cli.py pregame test-workflow

# Test with specific game
uv run python src/mlb_sharp_betting/cli.py pregame test-workflow --game-pk 746234

# Dry run (no emails sent)
DISABLE_EMAIL_NOTIFICATIONS=true uv run python src/mlb_sharp_betting/cli.py pregame test-workflow
```

### Test Individual Components
```bash
# Test data collection
uv run python -m mlb_sharp_betting.entrypoint

# Test analysis
uv run analysis_scripts/master_betting_detector.py --minutes 5

# Test email configuration
python -c "
from mlb_sharp_betting.services.alert_service import AlertService
alert = AlertService()
print('Email configured:', alert.is_configured())
"
```

### Mock Testing
```bash
# Use mock data for testing
MOCK_MODE=true uv run python src/mlb_sharp_betting/cli.py pregame test-workflow
```

## 📈 Performance

### Resource Usage
- Memory: ~50-100MB typical usage
- CPU: Low, spikes during workflow execution
- Disk: Log files grow over time (consider rotation)
- Network: Minimal, only during API calls and email

### Scaling Considerations
- Single scheduler instance per project
- Supports multiple concurrent workflows
- Database locks handled automatically
- Email rate limiting respected

### Optimization Tips
```bash
# Reduce log verbosity for production
export LOG_LEVEL=WARNING

# Cleanup old workflow logs
find workflow_logs/ -name "*.log" -mtime +30 -delete

# Monitor database size
psql -h localhost -d mlb_betting -c "\dt+"
```

## 🤝 Integration

### Existing Systems
The pre-game workflow system is designed to complement existing systems:

- **Hourly Data Collection**: Continues unchanged
- **Daily Game Updates**: Preserved and enhanced
- **Alert Service**: Shared and enhanced with new email templates
- **Database**: Same DuckDB instance, no conflicts

### Custom Workflows
```python
# Create custom workflow stages
from mlb_sharp_betting.services.pre_game_workflow import PreGameWorkflowService

workflow = PreGameWorkflowService()

# Add custom stage
async def custom_analysis_stage():
    # Your custom analysis logic
    pass

# Execute with custom stages
await workflow.execute_custom_workflow(game, [custom_analysis_stage])
```

## 📝 Development

### Adding New Features

1. **New CLI Commands**: Add to `src/mlb_sharp_betting/cli/commands/pre_game.py`
2. **New Workflow Stages**: Modify `PreGameWorkflowService.execute_pre_game_workflow()`
3. **New Email Templates**: Update `_generate_email_content()` method
4. **New Metrics**: Add to scheduler's metrics collection

### Code Organization
```
src/mlb_sharp_betting/
├── services/
│   ├── pre_game_workflow.py    # Main workflow orchestration
│   ├── pre_game_scheduler.py   # Scheduling and job management
│   └── alert_service.py        # Email notifications (enhanced)
├── cli/commands/
│   └── pre_game.py            # Command-line interface
└── core/
    └── config.py              # Configuration (enhanced with email settings)
```

## 🆘 Support

### Getting Help
1. Check this README first
2. Review log files for error details
3. Test individual components
4. Check GitHub issues for known problems

### Reporting Issues
Include the following information:
- System info: `uv run python --version`
- Scheduler status: `uv run python src/mlb_sharp_betting/cli.py pregame status`
- Recent logs: `tail -n 50 pregame_scheduler.log`
- Configuration: Email setup status (no credentials)

---

**General Balls** 🏈

*Remember, this system is designed to be robust and reliable. The three-stage workflow with retry logic ensures maximum success rate, while comprehensive email notifications keep you informed of all activities.* 