# MLB Betting System - CLI User Guide

**Version**: 1.0  
**Last Updated**: July 21, 2025  
**System**: RAW → STAGING (Unified) → CURATED Data Pipeline

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
   
   # Create pipeline schemas with unified staging table
   psql -f sql/migrations/004_create_pipeline_zones.sql
   psql -f sql/migrations/035_create_unified_staging_table.sql
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

#### `health` - Collection Health Monitoring (NEW - Silent Failure Resolution)
**Purpose**: Monitor collection health and eliminate silent data collection failures

**Common Commands**:
```bash
# Check collection health status
uv run -m src.interfaces.cli health status [--source <source>] [--detailed]

# Detect collection gaps
uv run -m src.interfaces.cli health gaps [--threshold-hours <hours>]

# Monitor database health
uv run -m src.interfaces.cli health dead-tuples [--threshold <ratio>]

# Check circuit breaker status
uv run -m src.interfaces.cli health circuit-breakers [--source <source>]

# View active alerts
uv run -m src.interfaces.cli health alerts [--severity <level>]

# Test source connection
uv run -m src.interfaces.cli health test-connection --source <source>

# Reset circuit breaker for recovery
uv run -m src.interfaces.cli health reset-circuit-breaker --source <source>

# View historical health data
uv run -m src.interfaces.cli health history [--source <source>] [--days <n>]
```

**Examples**:
```bash
# Check overall collection health
uv run -m src.interfaces.cli health status --detailed

# Check for collection gaps (sources that stopped collecting)
uv run -m src.interfaces.cli health gaps --threshold-hours 4

# Monitor database performance issues
uv run -m src.interfaces.cli health dead-tuples --threshold 0.5

# Check circuit breaker status for Action Network
uv run -m src.interfaces.cli health circuit-breakers --source action_network

# View critical alerts requiring attention
uv run -m src.interfaces.cli health alerts --severity critical

# Test VSIN connection health
uv run -m src.interfaces.cli health test-connection --source vsin

# Reset circuit breaker after fixing issues
uv run -m src.interfaces.cli health reset-circuit-breaker --source action_network

# Review 7-day health trends
uv run -m src.interfaces.cli health history --days 7
```

**Key Features**:
- **Silent Failure Detection**: Identifies collection failures within minutes vs hours
- **Automatic Recovery**: 80%+ of failures recover without manual intervention
- **Circuit Breaker Protection**: Prevents cascade failures across data sources  
- **Gap Detection**: Alerts when sources stop collecting data silently
- **Confidence Scoring**: Real-time assessment of collection reliability
- **Recovery Strategies**: Exponential backoff, fallback sources, degraded mode
- **Database Health**: Monitors for dead tuple accumulation and performance issues

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

# 2. Check collection status and health
echo "📊 Checking collection status..."
uv run -m src.interfaces.cli data status --detailed

echo "🔍 Checking collection health..."
uv run -m src.interfaces.cli health status --detailed
uv run -m src.interfaces.cli health gaps

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

**Silent Failure Resolution System** (August 2025): Eliminated silent data collection failures
- **Issue**: Data collection operations failing without proper notification or recovery
- **Fix**: Comprehensive health monitoring with automatic recovery and alerting
- **Status**: ✅ Complete implementation - see GitHub Issue #36 resolution
- **Features**: Circuit breaker protection, gap detection, confidence scoring, automatic recovery

### Silent Failure Resolution & Health Monitoring

**Problem**: Data collection can fail silently, requiring manual detection and intervention.

**Solution**: Use the comprehensive health monitoring system to detect and resolve issues automatically.

**Health Check Commands**:
```bash
# Check overall collection health
uv run -m src.interfaces.cli health status --detailed

# Detect collection gaps (sources that stopped collecting)
uv run -m src.interfaces.cli health gaps --threshold-hours 4

# Monitor database performance issues
uv run -m src.interfaces.cli health dead-tuples

# Check circuit breaker status
uv run -m src.interfaces.cli health circuit-breakers

# View active alerts requiring attention
uv run -m src.interfaces.cli health alerts --severity critical
```

**Automatic Recovery Features**:
- **Silent Failure Detection**: Identifies failures within minutes vs hours
- **Circuit Breaker Protection**: Prevents cascade failures across sources
- **Automatic Recovery**: 80%+ of failures resolve without manual intervention
- **Gap Detection**: Alerts when sources stop collecting data
- **Confidence Scoring**: Real-time assessment of collection reliability

**Manual Recovery Commands**:
```bash
# Test connection to problematic source
uv run -m src.interfaces.cli health test-connection --source vsin

# Reset circuit breaker after fixing issues
uv run -m src.interfaces.cli health reset-circuit-breaker --source action_network

# View historical health trends
uv run -m src.interfaces.cli health history --days 7
```

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

## 🗄️ Database Schema & Line Movement Investigation

### Primary Tables for Manual Line Movement Analysis

When investigating line movements on games, focus on these key tables in **order of importance**:

#### 1. **`raw_data.action_network_odds`** - Raw Line Data (Most Current: 4,992 records)
```sql
-- Example: Find all line movements for a specific game
SELECT external_game_id, sportsbook_name, market_type, side, odds, line_value, 
       updated_at, data_collection_time
FROM raw_data.action_network_odds 
WHERE external_game_id = 'YOUR_GAME_ID'
ORDER BY market_type, side, updated_at;
```

#### 2. **`staging.action_network_odds_historical`** - Temporal Line Movement (Comprehensive)
```sql
-- Example: Track line movement progression over time
SELECT external_game_id, sportsbook_name, market_type, side, 
       odds, line_value, updated_at, is_current_odds
FROM staging.action_network_odds_historical 
WHERE external_game_id = 'YOUR_GAME_ID'
  AND market_type = 'spread'  -- or 'moneyline', 'total'
ORDER BY updated_at;
```

#### 3. **`curated.line_movements`** - Processed Movement Analysis
```sql
-- Example: Find significant line movements
SELECT lm.*, gc.home_team, gc.away_team, gc.game_datetime
FROM curated.line_movements lm
JOIN curated.games_complete gc ON lm.game_id = gc.id
WHERE lm.movement_size > 0.5  -- Significant movements
ORDER BY lm.movement_timestamp DESC;
```

#### 4. **`curated.sharp_action_indicators`** - Professional Betting Signals
```sql
-- Example: Find sharp action on recent games
SELECT sai.*, gc.home_team, gc.away_team, gc.game_datetime
FROM curated.sharp_action_indicators sai
JOIN curated.games_complete gc ON sai.game_id = gc.id
WHERE sai.confidence > 0.7  -- High confidence signals
ORDER BY sai.detected_at DESC;
```

### Quick Investigation Queries

**Find Today's Games with Line Movement:**
```sql
SELECT gc.home_team, gc.away_team, gc.game_datetime,
       COUNT(DISTINCT lm.id) as movement_count,
       MAX(lm.movement_size) as max_movement
FROM curated.games_complete gc
LEFT JOIN curated.line_movements lm ON gc.id = lm.game_id
WHERE gc.game_date = CURRENT_DATE
GROUP BY gc.id, gc.home_team, gc.away_team, gc.game_datetime
ORDER BY movement_count DESC;
```

**Find Games with Sharp Action:**
```sql
SELECT gc.home_team, gc.away_team, gc.game_datetime,
       sai.indicator_type, sai.confidence, sai.detected_at
FROM curated.games_complete gc
JOIN curated.sharp_action_indicators sai ON gc.id = sai.game_id
WHERE gc.game_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY sai.confidence DESC, sai.detected_at DESC;
```

### Complete Database Schema Map

#### Raw Data Zone (`raw_data` schema) - Source-Specific Tables
- **`raw_data.action_network_odds`** (4,992 records) - Live betting lines from Action Network API
- **`raw_data.action_network_games`** (63 records) - Game metadata from Action Network  
- **`raw_data.vsin`** (409 records) - VSIN sharp action data and betting insights
- **`raw_data.sbd_betting_splits`** - SportsBettingDime betting percentage data
- **`raw_data.mlb_stats_api`** - Official MLB game data and statistics

#### Staging Zone (`staging` schema) - Processed & Unified Data
- **`staging.action_network_odds_historical`** - **PRIMARY LINE MOVEMENT TABLE** ⭐
- **`staging.spreads`** (1,307 records) - Processed point spread data
- **`staging.moneylines`** (884 records) - Processed moneyline data  
- **`staging.totals`** (884 records) - Processed over/under totals
- **`staging.betting_odds_unified`** (608 records) - Unified odds format

#### Curated Zone (`curated` schema) - Analysis-Ready Data
- **`curated.games_complete`** (94 records) - **Master games table with all external IDs** ⭐
- **`curated.line_movements`** - **Processed line movement analysis** ⭐
- **`curated.sharp_action_indicators`** - **Professional betting pattern detection** ⭐
- **`curated.game_outcomes`** (94 records) - Final game results and outcomes
- **`curated.arbitrage_opportunities`** (27 records) - Cross-sportsbook arbitrage opportunities

#### Analysis & Analytics Zones
- **`analysis.betting_strategies`** (10 records) - Defined betting strategies and parameters
- **`analytics.betting_recommendations`** - Automated betting recommendations
- **`analytics.confidence_scores`** - Confidence scoring for betting opportunities
- **`analytics.roi_calculations`** - Return on investment calculations

### Database Connection Info
- **Host**: localhost
- **Port**: 5433  
- **Database**: mlb_betting
- **User**: samlafell
- **Password**: postgres

### Key Investigation Strategy
1. **Start with `curated.games_complete`** to find your game
2. **Use `staging.action_network_odds_historical`** for detailed line movements  
3. **Check `curated.sharp_action_indicators`** for professional betting patterns
4. **Review `curated.line_movements`** for processed movement analysis
5. **Cross-reference with `raw_data.action_network_odds`** for latest data

---

**Last Updated**: August 16, 2025  
**CLI Version**: 1.2  
**Pipeline Status**: Production Ready ✅ 
**Architecture**: Unified Historical Approach with Source-Specific Raw Tables 🏗️
**Database-First**: Analysis results stored in PostgreSQL with temporal precision 📊