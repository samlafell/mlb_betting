# MLB Betting System - CLI User Guide

**Version**: 1.0  
**Last Updated**: July 21, 2025  
**System**: RAW → STAGING → CURATED Data Pipeline

---

## 📋 Table of Contents

1. [Getting Started](#-getting-started)
2. [Command Reference](#-command-reference)
3. [Daily Workflows](#-daily-workflows)
4. [Configuration Guide](#-configuration-guide)
5. [Troubleshooting](#-troubleshooting)
6. [Advanced Usage](#-advanced-usage)
7. [Quick Reference](#-quick-reference)

---

## 🚀 Getting Started

### Prerequisites

Before using the CLI system, ensure you have:

- **Python 3.10+** installed
- **UV package manager** installed (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- **PostgreSQL 13+** running and accessible
- **Git** for cloning the repository

### Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd mlb_betting_program
   ```

2. **Install dependencies:**
   ```bash
   uv sync --dev
   ```

3. **Configure the system:**
   ```bash
   # Copy sample configuration
   cp config.sample.toml config.toml
   
   # Edit configuration with your database settings
   nano config.toml
   ```

4. **Set up the database:**
   ```bash
   # Initialize database schemas
   uv run -m src.interfaces.cli database setup-action-network --test-connection
   
   # Create pipeline schemas
   psql -f sql/migrations/004_create_pipeline_zones.sql
   ```

5. **Verify installation:**
   ```bash
   # Test the CLI
   uv run -m src.interfaces.cli --help
   
   # Test database connection
   uv run -m src.interfaces.cli data status
   ```

### First Run

Run your first data collection to verify everything works:

```bash
# Collect some test data
uv run -m src.interfaces.cli data collect --source action_network --real

# Check the results
uv run -m src.interfaces.cli data status --detailed
```

If this completes successfully, your system is ready for production use!

---

## 📚 Command Reference

The CLI system is organized into 9 main command groups:

### Core Data Operations

#### `data` - Primary Data Collection
**Purpose**: Main data collection workflows from multiple sources

**Common Commands**:
```bash
# Collect data from specific source
uv run -m src.interfaces.cli data collect --source <source> --real

# Check collection status
uv run -m src.interfaces.cli data status [--detailed]

# Test connection to data source
uv run -m src.interfaces.cli data test --source <source>
```

**Sources**: `action_network`, `sbd`, `vsin`, `mlb_stats_api`, `odds_api`

**Examples**:
```bash
# Collect today's Action Network data
uv run -m src.interfaces.cli data collect --source action_network --real

# Get detailed status of all collections
uv run -m src.interfaces.cli data status --detailed

# Test VSIN connection
uv run -m src.interfaces.cli data test --source vsin --real
```

#### `action-network` - Action Network Operations
**Purpose**: Specialized Action Network data collection and processing

**Common Commands**:
```bash
# Run complete Action Network pipeline
uv run -m src.interfaces.cli action-network pipeline

# Display betting opportunities
uv run -m src.interfaces.cli action-network opportunities

# Process collected data
uv run -m src.interfaces.cli action-network process --batch-size 1000
```

**Examples**:
```bash
# Run complete Action Network pipeline
uv run -m src.interfaces.cli action-network pipeline

# Display current betting opportunities
uv run -m src.interfaces.cli action-network opportunities
```

#### `pipeline` - Data Pipeline Management
**Purpose**: Manage the RAW → STAGING → CURATED data pipeline with source-specific tables

**Common Commands**:
```bash
# Run full pipeline (all zones) - now uses source-specific tables
uv run -m src.interfaces.cli pipeline run --zone all --mode full

# Run specific zone
uv run -m src.interfaces.cli pipeline run --zone <zone> --mode <mode>

# Check pipeline status
uv run -m src.interfaces.cli pipeline status [--zone <zone>] [--detailed]

# Migrate existing data
uv run -m src.interfaces.cli pipeline migrate --migrate-data [--dry-run]
```

**Zones**: `raw`, `staging`, `curated`, `all`  
**Modes**: `full`, `raw_only`, `staging_only`, `curated_only`

**Pipeline Architecture**: The system now uses **source-specific raw tables** (e.g., `raw_data.action_network_odds`) flowing to **unified historical staging** (`staging.action_network_odds_historical`) for comprehensive temporal analysis.

**Examples**:
```bash
# Process Action Network data through unified historical pipeline
uv run -m src.interfaces.cli pipeline run --zone all --mode full

# Check detailed status of all pipeline zones
uv run -m src.interfaces.cli pipeline status --detailed

# Process only RAW zone for testing
uv run -m src.interfaces.cli pipeline run --zone raw --batch-size 500

# Check source-specific raw data processing
uv run -m src.interfaces.cli data status --detailed
```

### Analysis & Strategy

#### `movement` - Line Movement Analysis
**Purpose**: Analyze betting line movements and detect patterns

**Common Commands**:
```bash
# Analyze line movements
uv run -m src.interfaces.cli movement analyze --input-file <file>

# Detect reverse line movement
uv run -m src.interfaces.cli movement rlm --input-file <file>

# Identify steam moves
uv run -m src.interfaces.cli movement steam --input-file <file>
```

**Examples**:
```bash
# Analyze recent Action Network movements
uv run -m src.interfaces.cli movement analyze --input-file output/action_network_history.json

# Find reverse line movements
uv run -m src.interfaces.cli movement rlm --input-file output/action_network_history.json --threshold 0.02

# Detect steam moves with custom parameters
uv run -m src.interfaces.cli movement steam --input-file data/movements.json --min-books 5
```

#### `backtesting` - Strategy Testing
**Purpose**: Test and validate betting strategies against historical data

**Common Commands**:
```bash
# Run backtest for specific strategy
uv run -m src.interfaces.cli backtest run --strategy <strategy> --start-date <date> --end-date <date>

# Get strategy performance report
uv run -m src.interfaces.cli backtest report --strategy <strategy>

# List available strategies
uv run -m src.interfaces.cli backtest list-strategies
```

**Strategies**: `sharp_action`, `line_movement`, `consensus_fade`, `late_flip`

**Examples**:
```bash
# Test sharp action strategy for June 2024
uv run -m src.interfaces.cli backtest run --strategy sharp_action --start-date 2024-06-01 --end-date 2024-06-30

# Get comprehensive performance report
uv run -m src.interfaces.cli backtest report --strategy sharp_action --detailed

# Run multiple strategies comparison
uv run -m src.interfaces.cli backtest run --strategies sharp_action,line_movement --start-date 2024-07-01 --end-date 2024-07-15
```

#### `outcomes` - Game Outcome Tracking
**Purpose**: Update and track actual game results

**Common Commands**:
```bash
# Update outcomes for specific date
uv run -m src.interfaces.cli outcomes update --date <date>

# Update today's outcomes
uv run -m src.interfaces.cli outcomes update --date today

# Verify outcome accuracy
uv run -m src.interfaces.cli outcomes verify --date <date>
```

**Examples**:
```bash
# Update today's game outcomes
uv run -m src.interfaces.cli outcomes update --date today

# Update outcomes for specific date
uv run -m src.interfaces.cli outcomes update --date 2025-07-15

# Verify outcomes for date range
uv run -m src.interfaces.cli outcomes verify --start-date 2025-07-10 --end-date 2025-07-15
```

### System Management

#### `database` - Database Management
**Purpose**: Set up and manage database schemas and connections

**Common Commands**:
```bash
# Set up Action Network tables
uv run -m src.interfaces.cli database setup-action-network [--test-connection]

# Test database connection
uv run -m src.interfaces.cli database test-connection

# Run maintenance tasks
uv run -m src.interfaces.cli database maintenance [--vacuum] [--analyze]
```

**Examples**:
```bash
# Initialize Action Network database with connection test
uv run -m src.interfaces.cli database setup-action-network --test-connection

# Test current database connection
uv run -m src.interfaces.cli database test-connection

# Run database maintenance
uv run -m src.interfaces.cli database maintenance --vacuum --analyze
```

#### `data-quality` - Data Quality Management
**Purpose**: Monitor and improve data quality across the system

**Common Commands**:
```bash
# Deploy data quality improvements
uv run -m src.interfaces.cli data-quality deploy

# Check data quality status
uv run -m src.interfaces.cli data-quality status [--detailed]

# Run quality validation
uv run -m src.interfaces.cli data-quality validate --source <source>
```

**Examples**:
```bash
# Deploy latest data quality improvements
uv run -m src.interfaces.cli data-quality deploy

# Get detailed quality status for all sources
uv run -m src.interfaces.cli data-quality status --detailed

# Validate Action Network data quality
uv run -m src.interfaces.cli data-quality validate --source action_network
```

---

## 🔄 Daily Workflows

### Morning Data Collection Routine

**Goal**: Collect overnight data and process through pipeline

```bash
#!/bin/bash
# Daily morning collection script

echo "🌅 Starting morning data collection..."

# 1. Collect data from all sources
echo "📥 Collecting Action Network data..."
uv run -m src.interfaces.cli data collect --source action_network --real

echo "📥 Collecting VSIN data..."
uv run -m src.interfaces.cli data collect --source vsin --real

echo "📥 Collecting SBD data..."
uv run -m src.interfaces.cli data collect --source sbd --real

# 2. Check collection status
echo "📊 Checking collection status..."
uv run -m src.interfaces.cli data status --detailed

# 3. Run pipeline processing
echo "🔄 Processing data through pipeline..."
uv run -m src.interfaces.cli pipeline run --zone all --mode full

# 4. Update game outcomes
echo "🎯 Updating game outcomes..."
uv run -m src.interfaces.cli outcomes update --date today

# 5. Check final status
echo "✅ Final status check..."
uv run -m src.interfaces.cli pipeline status --detailed

echo "🎉 Morning collection complete!"
```

### Strategy Analysis Session

**Goal**: Analyze recent data and test strategies

```bash
# 1. Run Action Network pipeline
echo "📈 Running Action Network pipeline..."
uv run -m src.interfaces.cli action-network pipeline

# 2. Analyze line movements
echo "📊 Analyzing line movements..."
uv run -m src.interfaces.cli movement analyze --input-file output/action_network_history.json

# 3. Detect sharp action patterns
echo "🎯 Detecting sharp action..."
uv run -m src.interfaces.cli movement rlm --input-file output/action_network_history.json

# 4. Run strategy backtest
echo "🧪 Testing sharp action strategy..."
uv run -m src.interfaces.cli backtest run --strategy sharp_action --start-date 2025-07-14 --end-date 2025-07-21

# 5. Generate performance report
echo "📋 Generating performance report..."
uv run -m src.interfaces.cli backtest report --strategy sharp_action --detailed
```

### Weekly Maintenance

**Goal**: Keep system healthy and optimized

```bash
#!/bin/bash
# Weekly maintenance script

echo "🔧 Starting weekly maintenance..."

# 1. Data quality deployment
echo "📊 Deploying data quality improvements..."
uv run -m src.interfaces.cli data-quality deploy

# 2. Database maintenance
echo "🗄️ Running database maintenance..."
uv run -m src.interfaces.cli database maintenance --vacuum --analyze

# 3. Quality validation
echo "✅ Validating data quality..."
uv run -m src.interfaces.cli data-quality status --detailed

# 4. Pipeline health check
echo "🔄 Checking pipeline health..."
uv run -m src.interfaces.cli pipeline status --detailed

# 5. Outcome verification for past week
echo "🎯 Verifying outcomes..."
uv run -m src.interfaces.cli outcomes verify --start-date $(date -d '7 days ago' +%Y-%m-%d) --end-date $(date +%Y-%m-%d)

echo "✨ Weekly maintenance complete!"
```

---

## ⚙️ Configuration Guide

### config.toml Structure

The system uses a centralized configuration file at `config.toml`:

```toml
# Example configuration
[database]
host = "localhost"
port = 5432
name = "mlb_betting"
user = "your_user"
password = "your_password"

[api_keys]
action_network = "your_api_key"
odds_api = "your_odds_api_key"

[pipeline]
[pipeline.zones]
raw_enabled = true
staging_enabled = true
curated_enabled = false  # Not yet implemented

[pipeline.processing]
batch_size = 1000
quality_threshold = 0.85
validation_enabled = true
auto_promotion = true

[logging]
level = "INFO"
file_path = "logs/mlb_betting.log"
```

### Environment Variables

For sensitive information, use environment variables:

```bash
# Database connection
export MLB_DATABASE_URL="postgresql://user:pass@localhost:5432/mlb_betting"

# API Keys
export ACTION_NETWORK_API_KEY="your_key_here"
export ODDS_API_KEY="your_key_here"
export VSIN_API_KEY="your_key_here"

# Optional: Override log level
export LOG_LEVEL="DEBUG"
```

### Feature Flags

Control system behavior with configuration flags:

| Setting | Description | Default |
|---------|-------------|---------|
| `pipeline.zones.raw_enabled` | Enable RAW zone processing | `true` |
| `pipeline.zones.staging_enabled` | Enable STAGING zone processing | `true` |
| `pipeline.zones.curated_enabled` | Enable CURATED zone processing | `false` |
| `pipeline.validation_enabled` | Enable data validation | `true` |
| `pipeline.auto_promotion` | Auto-promote data between zones | `true` |
| `pipeline.quality_threshold` | Minimum quality score for promotion | `0.85` |

### Performance Tuning

Adjust these settings based on your system resources:

```toml
[pipeline.processing]
batch_size = 1000          # Records per batch (500-2000)
max_concurrent = 5         # Concurrent operations (3-10)
timeout_seconds = 300      # Operation timeout (60-600)

[database]
pool_size = 10            # Connection pool size (5-20)
max_overflow = 20         # Additional connections (10-50)
pool_timeout = 30         # Connection timeout (10-60)
```

---

## 🔧 Troubleshooting

### Recent Fixes (July 2025)

**Architecture Cleanup & Consolidation**: Major cleanup of legacy code and architecture
- **Issue**: Multiple redundant approaches (sparse, wide, long) for Action Network staging
- **Fix**: Consolidated to unified historical approach with source-specific raw tables
- **Status**: ✅ Complete architecture cleanup - see `docs/ARCHITECTURE_CLEANUP_SUMMARY.md`

**Database Connection Pipeline Fix**: Fixed `'DatabaseConnection' object can't be used in 'await' expression`
- **Issue**: Incorrect async patterns in pipeline orchestrator and zone processors
- **Fix**: Updated to use proper `async with db_connection.get_async_connection()` pattern
- **Status**: ✅ Resolved across all pipeline components

**Source-Specific Raw Tables**: Replaced generic tables with source-specific approach
- **Issue**: Generic raw tables (`betting_lines_raw`, `moneylines_raw`) caused confusion
- **Fix**: Implemented source-specific tables (`action_network_odds`, `sbd_betting_splits`, etc.)
- **Status**: ✅ Complete migration with improved data organization

### Common Issues

#### Database Connection Errors

**Problem**: `Database connection failed: could not connect to server`

**Solutions**:
1. Check PostgreSQL is running: `systemctl status postgresql`
2. Verify connection settings in `config.toml`
3. Test connection manually: `psql -h localhost -U username -d mlb_betting`
4. Check firewall settings and port accessibility

**Debug Command**:
```bash
uv run -m src.interfaces.cli database test-connection
```

#### API Rate Limiting

**Problem**: `Rate limit exceeded` errors from data sources

**Solutions**:
1. Check your API key limits and usage
2. Increase delays between requests in configuration
3. Use `--batch-size` flag to reduce request volume
4. Schedule collections during off-peak hours

**Debug Command**:
```bash
# Test with smaller batch size
uv run -m src.interfaces.cli data collect --source action_network --batch-size 100
```

#### Pipeline Processing Failures

**Problem**: Pipeline fails during zone processing

**Solutions**:
1. Check pipeline status: `uv run -m src.interfaces.cli pipeline status --detailed`
2. Review logs in `logs/mlb_betting.log`
3. Run pipeline with single zone: `uv run -m src.interfaces.cli pipeline run --zone raw`
4. Check data quality: `uv run -m src.interfaces.cli data-quality status`

**Debug Commands**:
```bash
# Check pipeline health
uv run -m src.interfaces.cli pipeline status --detailed

# Run pipeline in dry-run mode
uv run -m src.interfaces.cli pipeline run --dry-run

# Process specific zone only
uv run -m src.interfaces.cli pipeline run --zone raw --batch-size 100
```

#### Memory Issues

**Problem**: System runs out of memory during large operations

**Solutions**:
1. Reduce batch sizes in configuration
2. Process data in smaller date ranges
3. Increase system memory or swap
4. Use `--batch-size` flags with smaller values

**Commands**:
```bash
# Use smaller batch sizes
uv run -m src.interfaces.cli pipeline run --batch-size 500

# Process data using pipeline
uv run -m src.interfaces.cli action-network pipeline
```

### Debug Mode

Enable detailed logging for troubleshooting:

```bash
# Set debug logging level
export LOG_LEVEL="DEBUG"

# Run command with verbose output
uv run -m src.interfaces.cli data collect --source action_network --real --verbose
```

### Log Locations

System logs are stored in:
- **Main log**: `logs/mlb_betting.log`
- **Error log**: `logs/errors.log`
- **Pipeline log**: `logs/pipeline.log`
- **Database log**: `logs/database.log`

### Recovery Procedures

#### Failed Data Collection Recovery
```bash
# 1. Check what data was collected
uv run -m src.interfaces.cli data status --detailed

# 2. Re-run pipeline
uv run -m src.interfaces.cli action-network pipeline

# 3. Verify collection completed
uv run -m src.interfaces.cli data status --source action_network
```

#### Failed Pipeline Recovery
```bash
# 1. Check pipeline status
uv run -m src.interfaces.cli pipeline status --detailed

# 2. Clean up failed execution (if needed)
# This depends on specific error - check logs

# 3. Re-run pipeline from last successful point
uv run -m src.interfaces.cli pipeline run --zone staging  # If RAW completed

# 4. Verify recovery
uv run -m src.interfaces.cli pipeline status --detailed
```

---

## 🔥 Advanced Usage

### Automation with Cron

Set up automated daily collections:

```bash
# Edit crontab
crontab -e

# Add daily collection at 6 AM
0 6 * * * cd /path/to/mlb_betting_program && /path/to/uv run -m src.interfaces.cli data collect --source action_network --real >> logs/cron.log 2>&1

# Add pipeline processing at 7 AM
0 7 * * * cd /path/to/mlb_betting_program && /path/to/uv run -m src.interfaces.cli pipeline run --zone all --mode full >> logs/cron.log 2>&1
```

### Custom Scripts

Create custom scripts for complex workflows:

```python
#!/usr/bin/env python3
"""Custom data collection with email notifications"""

import subprocess
import smtplib
from datetime import datetime

def run_collection():
    try:
        # Run data collection
        result = subprocess.run([
            'uv', 'run', '-m', 'src.interfaces.cli', 
            'data', 'collect', '--source', 'action_network', '--real'
        ], capture_output=True, text=True, check=True)
        
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        return False, e.stderr

def send_notification(success, message):
    # Email notification logic here
    pass

if __name__ == "__main__":
    success, output = run_collection()
    send_notification(success, output)
```

### Integration with External Systems

#### Webhook Integration
```python
import requests
import json

def send_webhook(data):
    webhook_url = "https://your-system.com/webhook"
    requests.post(webhook_url, json=data)

# After data collection
send_webhook({
    'timestamp': datetime.now().isoformat(),
    'status': 'completed',
    'records_collected': 1500
})
```

#### Database Integration
```bash
# Export data to CSV for external analysis
psql -d mlb_betting -c "COPY (SELECT * FROM raw_data.betting_lines_raw WHERE created_at >= CURRENT_DATE) TO 'daily_export.csv' WITH CSV HEADER"
```

### Performance Monitoring

Monitor system performance with custom scripts:

```bash
#!/bin/bash
# Performance monitoring script

# Check database size
echo "📊 Database size:"
psql -d mlb_betting -c "SELECT pg_size_pretty(pg_database_size('mlb_betting'));"

# Check table sizes
echo "📋 Table sizes:"
psql -d mlb_betting -c "
SELECT 
    schemaname, 
    tablename, 
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE schemaname IN ('raw_data', 'staging', 'curated', 'core_betting')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;"

# Check recent pipeline performance
echo "⚡ Recent pipeline performance:"
uv run -m src.interfaces.cli pipeline status --detailed
```

---

## 📖 Quick Reference

### Most Common Commands

```bash
# Daily essentials
uv run -m src.interfaces.cli data collect --source action_network --real
uv run -m src.interfaces.cli pipeline run --zone all --mode full
uv run -m src.interfaces.cli data status --detailed

# Analysis
uv run -m src.interfaces.cli movement analyze --input-file output/action_network_history.json
uv run -m src.interfaces.cli backtest run --strategy sharp_action --start-date 2025-07-01 --end-date 2025-07-15

# Maintenance
uv run -m src.interfaces.cli database test-connection
uv run -m src.interfaces.cli data-quality deploy
uv run -m src.interfaces.cli outcomes update --date today
```

### Flag Reference

| Flag | Purpose | Example |
|------|---------|---------|
| `--real` | Use real API calls (vs test mode) | `--real` |
| `--dry-run` | Show what would happen without executing | `--dry-run` |
| `--detailed` | Show detailed output | `--detailed` |
| `--source` | Specify data source | `--source action_network` |
| `--date` | Specify date | `--date today` or `--date 2025-07-15` |
| `--batch-size` | Set batch processing size | `--batch-size 500` |
| `--zone` | Specify pipeline zone | `--zone raw` |
| `--mode` | Set pipeline processing mode | `--mode full` |

### Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | General error |
| `2` | Command line argument error |
| `3` | Database connection error |
| `4` | API connection error |
| `5` | Data validation error |
| `6` | Pipeline processing error |

### Log Locations

| Log Type | Location | Purpose |
|----------|----------|---------|
| Main | `logs/mlb_betting.log` | General application logs |
| Error | `logs/errors.log` | Error messages and stack traces |
| Pipeline | `logs/pipeline.log` | Pipeline processing details |
| Database | `logs/database.log` | Database operations |

### Configuration Files

| File | Purpose | Required |
|------|---------|----------|
| `config.toml` | Main configuration | Yes |
| `.env` | Environment variables | Optional |
| `logs/` | Log directory | Auto-created |
| `output/` | CLI output files | Auto-created |

---

## 📞 Getting Help

### Built-in Help
```bash
# General help
uv run -m src.interfaces.cli --help

# Command-specific help
uv run -m src.interfaces.cli data --help
uv run -m src.interfaces.cli pipeline run --help
```

### Documentation References
- **Technical Details**: `docs/SYSTEM_DESIGN_ANALYSIS.md`
- **Test Results**: `docs/COMPREHENSIVE_TEST_REPORT.md`
- **Action Network Guide**: `docs/ACTION_NETWORK_COMPLETE_GUIDE.md`
- **Development Guide**: `CLAUDE.md`

### Support Checklist

Before seeking help, run these diagnostic commands:

```bash
# 1. Test basic connectivity
uv run -m src.interfaces.cli database test-connection

# 2. Check system status
uv run -m src.interfaces.cli data status --detailed

# 3. Verify configuration
cat config.toml

# 4. Check recent logs
tail -100 logs/mlb_betting.log

# 5. Test simple operation
uv run -m src.interfaces.cli data test --source action_network

# 6. Clean up output folder if needed
uv run -m src.interfaces.cli cleanup --dry-run
```

### Maintenance Commands

#### Output Folder Cleanup
The system now uses PostgreSQL for analysis results instead of JSON files. Clean up accumulated files:

```bash
# Analyze what would be cleaned (no changes)
uv run -m src.interfaces.cli cleanup --dry-run

# Clean up output folder
uv run -m src.interfaces.cli cleanup

# Keep more URL files for manual evaluation  
uv run -m src.interfaces.cli cleanup --keep-recent 10
```

**What cleanup does**:
- Archives old analysis reports and opportunities files
- Keeps recent URL files for manual evaluation
- Shows potential space savings (typically 30-50%)
- Organizes files in `output/archive/` with timestamps

---

**Last Updated**: July 23, 2025  
**CLI Version**: 1.2  
**Pipeline Status**: Production Ready ✅ 
**Architecture**: Unified Historical Approach with Source-Specific Raw Tables 🏗️
**Database-First**: Analysis results stored in PostgreSQL with temporal precision 📊