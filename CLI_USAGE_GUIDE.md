# MLB Sharp Betting CLI - Complete Usage Guide 🏀

> **Updated for Phase 3C Orchestrator System** - This guide covers the latest AI-powered betting detection system.

## 🎯 Quick Start

All commands should be run from the project root directory:
```bash
# Always source environment first
source .env

# Basic command structure
uv run src/mlb_sharp_betting/cli.py [command] [options]
```

---

## 🤖 THE ORCHESTRATOR - Your AI Betting Assistant

### What is the Orchestrator?

The **Orchestrator** is the brain of your betting system. It:
- ✅ **Automatically loads backtesting results** from your database
- ✅ **Dynamically configures 47 betting strategies** based on performance
- ✅ **Orders strategies by ROI** (best performers first)
- ✅ **Adapts thresholds** based on recent performance
- ✅ **NO manual configuration needed** - it's fully automated!

### 🚀 Using the Orchestrator

```bash
# Run the orchestrator demo (shows all strategy configurations)
uv run src/mlb_sharp_betting/cli.py orchestrator-demo --minutes 300

# With debug mode (shows more details)
uv run src/mlb_sharp_betting/cli.py orchestrator-demo --minutes 300 --debug
```

**What you'll see:**
- 📊 **47 total strategies** (25 enabled, 22 disabled)  
- 🎯 **Performance-ordered list** (+86.4% ROI → -4.6% ROI)
- 🤖 **Cold start vs live modes**
- 🔥 **Real-time opportunity detection**

### ❓ Orchestrator FAQ

**Q: Does the orchestrator re-backtest automatically?**
- ❌ **No** - The orchestrator uses existing backtest results from your database
- ✅ **You control backtesting** using the backtest commands below
- 💡 **Tip:** Run backtests weekly to keep strategies updated

**Q: Does it automatically pull in fresh data?**
- ❌ **No** - The orchestrator analyzes existing data in your database  
- ✅ **You control data collection** using the data commands below
- 💡 **Tip:** Run data collection before using the orchestrator

**Q: What's the difference between orchestrator and regular detection?**
- 🤖 **Orchestrator:** Uses performance-weighted strategies from backtesting
- 🔍 **Regular Detection:** Uses fixed strategy configurations
- 🎯 **Orchestrator is smarter** and adapts based on actual results

---

## 📊 DATA MANAGEMENT

### Check Data Status
```bash
# Quick data overview
uv run src/mlb_sharp_betting/cli.py data status

# Detailed breakdown by source/type  
uv run src/mlb_sharp_betting/cli.py data status --detailed

# Database statistics
uv run src/mlb_sharp_betting/cli.py database --stats
```

### Collect Fresh Data
```bash
# Collect from all sources (VSIN, SBD, Pinnacle)
uv run src/mlb_sharp_betting/cli.py data collect

# Force collection even if data is fresh
uv run src/mlb_sharp_betting/cli.py data collect --force

# Test with mock data (safe for testing)
uv run src/mlb_sharp_betting/cli.py data collect --dry-run

# Collect specific source only
uv run src/mlb_sharp_betting/cli.py data collect --source vsin
```

### Data Cleanup
```bash
# Clean up data older than 30 days
uv run src/mlb_sharp_betting/cli.py data cleanup --days 30

# See cleanup options
uv run src/mlb_sharp_betting/cli.py data cleanup --help
```

---

## 🔍 OPPORTUNITY DETECTION 

### 🎯 Enhanced Detection (RECOMMENDED)

```bash
# Smart opportunity detection with orchestrator
uv run src/mlb_sharp_betting/cli.py detect opportunities --minutes 300

# Include debug info
uv run src/mlb_sharp_betting/cli.py detect opportunities --minutes 300 --debug

# Save results to JSON
uv run src/mlb_sharp_betting/cli.py detect opportunities --format json --output today_opportunities.json

# Smart pipeline execution
uv run src/mlb_sharp_betting/cli.py detect smart-pipeline
```

### 🔀 Cross-Market Flip Detection

```bash
# Detect spread vs moneyline contradictions (last 24h)
uv run src/mlb_sharp_betting/cli.py cross-market-flips

# Adjust confidence threshold  
uv run src/mlb_sharp_betting/cli.py cross-market-flips --min-confidence 70.0

# Look further back
uv run src/mlb_sharp_betting/cli.py cross-market-flips --hours-back 48

# Filter by source/book
uv run src/mlb_sharp_betting/cli.py cross-market-flips --source VSIN --book draftkings
```

### 🚨 Legacy Detection (DEPRECATED)

```bash
# Old detection system (will show deprecation warning)
uv run src/mlb_sharp_betting/cli.py detect-opportunities --minutes 300
```

---

## 🧪 BACKTESTING SYSTEM

### Run Backtests (Updates Orchestrator Data)

```bash
# CORRECT: Single backtest run (updates strategy performance)
uv run src/mlb_sharp_betting/cli.py backtesting run --mode single-run

# With debug output
uv run src/mlb_sharp_betting/cli.py backtesting run --mode single-run --debug

# Enhanced backtesting with fresh data collection
uv run src/mlb_sharp_betting/cli.py backtest run --detailed

# Quick backtesting (top strategies only)
uv run src/mlb_sharp_betting/cli.py backtest quick
```

### View Backtest Results

```bash
# Show current strategy performance (used by orchestrator)
uv run src/mlb_sharp_betting/cli.py show-active-strategies

# Betting performance report
uv run src/mlb_sharp_betting/cli.py performance --date 2024-01-15

# Performance in different formats
uv run src/mlb_sharp_betting/cli.py performance --format json --output performance.json
```

---

## 🎮 GAME MANAGEMENT

### Game Operations
```bash
# Show game statistics
uv run src/mlb_sharp_betting/cli.py games --stats

# Sync games from recent betting splits  
uv run src/mlb_sharp_betting/cli.py games --sync

# Backfill games from outcomes table
uv run src/mlb_sharp_betting/cli.py games --backfill
```

### Update Game Results
```bash
# Update finished games with results
uv run tests/integration/test_game_updater.py
```

---

## 🏠 PRE-GAME WORKFLOW & SCHEDULING

### Manual Pre-Game Workflow
```bash
# Run complete pre-game analysis
uv run src/mlb_sharp_betting/cli.py pregame run-workflow

# Check workflow status  
uv run src/mlb_sharp_betting/cli.py pregame status

# Run with specific date
uv run src/mlb_sharp_betting/cli.py pregame run-workflow --date 2024-01-15
```

### Automated Scheduling
```bash
# Start the automated scheduler
./start_pregame_scheduler.sh --restart

# Stop the scheduler
./stop_pregame_scheduler.sh

# Check scheduler status
uv run src/mlb_sharp_betting/cli.py pregame status

# View scheduler logs
tail -f pregame_scheduler.log
```

---

## 📈 SYSTEM STATUS & HEALTH

### System Overview
```bash
# Overall system health
uv run src/mlb_sharp_betting/cli.py status health

# Database connectivity and schema
uv run src/mlb_sharp_betting/cli.py status database

# Data pipeline status
uv run src/mlb_sharp_betting/cli.py status pipeline
```

### Database Operations
```bash
# Setup database schema
uv run src/mlb_sharp_betting/cli.py database --setup-schema

# Verify schema is correct
uv run src/mlb_sharp_betting/cli.py database --verify-schema

# Run database integrity check
uv run src/mlb_sharp_betting/cli.py database --integrity-check
```

---

## 🎯 STRATEGY MANAGEMENT

### View Active Strategies
```bash
# Show all active high-ROI strategies (used by orchestrator)
uv run src/mlb_sharp_betting/cli.py show-active-strategies

# Auto-integrate new high-ROI strategies
uv run src/mlb_sharp_betting/cli.py auto-integrate-strategies --min-roi 15.0
```

---

## 📊 REPORTING & ANALYSIS

### Daily Reports
```bash
# Generate today's betting report
uv run src/mlb_sharp_betting/cli.py daily-report generate

# Historical report for specific date
uv run src/mlb_sharp_betting/cli.py daily-report generate --date 2024-01-15

# Email report (if configured)  
uv run src/mlb_sharp_betting/cli.py daily-report email
```

### Performance Analysis
```bash
# Betting performance for yesterday
uv run src/mlb_sharp_betting/cli.py performance

# Specific date performance
uv run src/mlb_sharp_betting/cli.py performance --date 2024-01-15

# Export to CSV
uv run src/mlb_sharp_betting/cli.py performance --format csv --output results.csv
```

### Timing Analysis
```bash
# Analyze bet timing patterns
uv run src/mlb_sharp_betting/cli.py timing analyze

# Timing analysis with custom window
uv run src/mlb_sharp_betting/cli.py timing analyze --days-back 14
```

---

## 🚀 RECOMMENDED DAILY WORKFLOW

### Morning Setup (9 AM)
```bash
# 1. Check system health
source .env && uv run src/mlb_sharp_betting/cli.py status health

# 2. Collect fresh data  
uv run src/mlb_sharp_betting/cli.py data collect

# 3. Update game results from yesterday
uv run tests/integration/test_game_updater.py

# 4. Check active strategies
uv run src/mlb_sharp_betting/cli.py show-active-strategies
```

### Pre-Game Analysis (2-3 hours before games)
```bash
# 5. Run orchestrator analysis (THE MAIN EVENT!)
uv run src/mlb_sharp_betting/cli.py orchestrator-demo --minutes 300

# 6. Cross-market flip detection
uv run src/mlb_sharp_betting/cli.py cross-market-flips --min-confidence 65.0

# 7. Enhanced detection for backup
uv run src/mlb_sharp_betting/cli.py detect opportunities --minutes 300
```

### Weekly Maintenance (Sundays)
```bash
# 8. Run fresh backtests to update orchestrator data
uv run src/mlb_sharp_betting/cli.py backtesting run --mode single-run

# 9. Auto-integrate any new high-ROI strategies
uv run src/mlb_sharp_betting/cli.py auto-integrate-strategies

# 10. Clean up old data
uv run src/mlb_sharp_betting/cli.py data cleanup --days 30
```

---

## 🆚 LEGACY vs NEW COMMANDS

### ❌ OLD/DEPRECATED Commands:
```bash
# These still work but show deprecation warnings
uv run src/mlb_sharp_betting/cli.py detect-opportunities  # Use: detect opportunities
uv run -m mlb_sharp_betting.cli.commands.backtesting    # Use: backtesting run --mode single-run
uv run analysis_scripts/master_betting_detector.py      # Use: orchestrator-demo
```

### ✅ NEW/RECOMMENDED Commands:
```bash
# Modern orchestrator-powered system
uv run src/mlb_sharp_betting/cli.py orchestrator-demo
uv run src/mlb_sharp_betting/cli.py detect opportunities  
uv run src/mlb_sharp_betting/cli.py backtesting run --mode single-run
uv run src/mlb_sharp_betting/cli.py data collect
```

---

## 🔧 TROUBLESHOOTING

### Common Issues

**"No data found"**
```bash
# Solution: Collect fresh data first
uv run src/mlb_sharp_betting/cli.py data collect --force
```

**"Database connection failed"**  
```bash
# Solution: Check database status
uv run src/mlb_sharp_betting/cli.py status database
```

**"No strategies found"**
```bash
# Solution: Run backtests to populate strategy data
uv run src/mlb_sharp_betting/cli.py backtesting run --mode single-run
```

**"Orchestrator in cold start mode"**
```bash
# Solution: This is normal - orchestrator adapts as it gets more data
# Run backtests weekly to keep it updated
```

### Getting Help
```bash
# Help for any command
uv run src/mlb_sharp_betting/cli.py [command] --help

# Main help menu
uv run src/mlb_sharp_betting/cli.py --help
```

---

## 🎉 KEY TAKEAWAYS

1. **🤖 Use the Orchestrator** - It's your smartest betting assistant
2. **📊 Fresh data first** - Always collect data before analysis  
3. **🧪 Backtest weekly** - Keeps orchestrator strategies current
4. **🔍 Multiple detection methods** - Cross-market flips + enhanced detection
5. **📈 Monitor performance** - Use daily reports and performance analysis
6. **🏠 Automate routine tasks** - Use the scheduler for daily workflows

---

**General Balls** 🎾
*Your AI Betting Assistant* 