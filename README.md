# 🎯 MLB Sharp Betting Analysis System

A comprehensive sports betting analysis platform focused on identifying profitable MLB betting opportunities through advanced analytics, sharp action detection, and automated backtesting.

## ⚡ New User? Start Here! (GitHub Issue #35 Solution)

**🚀 One-Command Setup for Business Users:**

```bash
# Clone the project and run:
./quick-start.sh
```

**That's it!** This addresses the complex setup barriers identified in [GitHub issue #35](https://github.com/samlafell/mlb_betting_program/issues/35). The script automatically:
- ✅ Installs all requirements
- ✅ Starts database containers
- ✅ Collects initial data  
- ✅ Generates first predictions

**Time:** 5-10 minutes | **Expertise:** None required

📖 **[→ Read the Complete Quick Start Guide](QUICK_START.md)** ← *Designed for business users*

---

## 🚀 Advanced Users - CLI System (v3.0)

The project has been completely reorganized with a unified CLI structure providing comprehensive data collection, analysis, and betting intelligence capabilities:

### 🔧 Core CLI Commands

```bash
# Main CLI entry point
uv run -m src.interfaces.cli --help

# Data Collection & Management
uv run -m src.interfaces.cli data collect --source action_network --real
uv run -m src.interfaces.cli data collect --source vsin --real
uv run -m src.interfaces.cli data collect --source sbd --real
uv run -m src.interfaces.cli data collect --parallel --real
uv run -m src.interfaces.cli data status
uv run -m src.interfaces.cli data test --source action_network --real

# Action Network Pipeline (Enhanced Integration)
uv run -m src.interfaces.cli action-network pipeline --date today
uv run -m src.interfaces.cli action-network history --days 30                    # Quick historical collection
uv run -m src.interfaces.cli action-network opportunities --hours 24

# Historical Data Collection Options
uv run -m src.interfaces.cli action-network history --days 15                    # Simple historical (recommended)
uv run -m src.interfaces.cli batch-collection collect-range --start-date 2024-01-01 --end-date 2024-01-15  # Precise dates

# Movement Analysis & Strategy Detection
# First generate historical data (creates output/action_network_history.json automatically)
uv run -m src.interfaces.cli action-network history --days 15                    
uv run -m src.interfaces.cli movement analyze --input-file output/action_network_history.json
uv run -m src.interfaces.cli movement rlm --input-file output/action_network_history.json --min-movements 50
uv run -m src.interfaces.cli movement steam --input-file output/action_network_history.json --show-details

# Backtesting & Performance
uv run -m src.interfaces.cli backtest run --start-date 2025-06-01 --end-date 2025-08-14 --strategies sharp_action
uv run -m src.interfaces.cli backtest run --start-date 2025-06-01 --end-date 2025-06-30 --strategies sharp_action --initial-bankroll 10000 --bet-size 100

# Database Management
uv run -m src.interfaces.cli database setup-action-network
uv run -m src.interfaces.cli database setup-action-network --schema-file sql/custom_schema.sql
uv run -m src.interfaces.cli database setup-action-network --test-connection

# Data Quality Management
uv run -m src.interfaces.cli data-quality deploy
uv run -m src.interfaces.cli data-quality status

# Game Outcomes
uv run -m src.interfaces.cli outcomes update --date today
uv run -m src.interfaces.cli outcomes verify --games recent

# Monitoring & Observability (NEW - Production Ready)
uv run -m src.interfaces.cli monitoring dashboard  # Start real-time web dashboard
uv run -m src.interfaces.cli monitoring status     # Check system health via API
uv run -m src.interfaces.cli monitoring live       # Real-time terminal monitoring
uv run -m src.interfaces.cli monitoring execute    # Manual break-glass pipeline execution
```

### 📊 Complete Data Collection Process

```bash
# 1. Setup database (one-time)
uv run -m src.interfaces.cli database setup-action-network

# 2. Collect data from specific sources
uv run -m src.interfaces.cli data collect --source vsin --real
uv run -m src.interfaces.cli data collect --source sbd --real

# 3. Run Action Network pipeline for today's games
uv run -m src.interfaces.cli action-network pipeline --date today
uv run -m src.interfaces.cli action-network history --days 30

# 4. Run movement analysis
uv run -m src.interfaces.cli movement analyze --input-file output/action_network_history.json --show-details

# 5. Run backtesting on strategies
uv run -m src.interfaces.cli backtest run --start-date 2025-06-01 --end-date 2025-08-15 --strategies sharp_action

# 6. Check system status
uv run -m src.interfaces.cli data status
```

### 🧪 Testing

```bash
# Run all tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=src --cov-report=html --cov-report=term-missing

# Run specific test file
uv run pytest tests/test_basic.py

# Integration Tests
uv run pytest tests/integration/

# Manual Test Scripts  
uv run pytest tests/manual/
```

## Project Structure

```
mlb_betting_program/
├── config.toml                    # Centralized configuration
├── src/                           # Unified architecture
│   ├── core/                      # Core configuration and logging
│   │   ├── config.py              # Central configuration (Pydantic v2)
│   │   ├── logging.py             # Structured logging
│   │   ├── datetime_utils.py      # Timezone handling (EST/EDT)
│   │   ├── team_utils.py          # MLB team normalization
│   │   └── sportsbook_utils.py    # Sportsbook ID resolution
│   ├── data/                      # Data collection and models
│   │   ├── collection/            # Multi-source data collectors
│   │   │   ├── consolidated_action_network_collector.py  # Primary Action Network collector
│   │   │   ├── smart_line_movement_filter.py            # Noise reduction filter
│   │   │   ├── action_network_unified_collector.py      # Enhanced Action Network
│   │   │   ├── sbd_unified_collector.py                 # SportsBettingDime collector (legacy)
│   │   │   ├── sbd_unified_collector_api.py            # SBD WordPress API collector
│   │   │   ├── vsin_unified_collector.py                # VSIN collector
│   │   │   ├── orchestrator.py                          # Collection orchestration
│   │   │   └── base.py                                  # Base collector classes
│   │   ├── database/              # Database management
│   │   │   ├── repositories/      # Repository pattern implementations
│   │   │   └── migrations/        # Database schema migrations
│   │   └── models/unified/        # Unified data models (Pydantic v2)
│   │       ├── game.py            # Game data models
│   │       ├── betting_analysis.py # Betting analysis models
│   │       ├── movement_analysis.py # Line movement models
│   │       └── base.py            # Base model classes
│   ├── analysis/                  # Strategy processors and analysis
│   │   ├── processors/            # Strategy processors
│   │   │   ├── action/            # Action Network specific processors
│   │   │   ├── sharp_action_processor.py
│   │   │   ├── line_movement_processor.py
│   │   │   └── consensus_processor.py
│   │   ├── strategies/            # Strategy orchestration
│   │   ├── backtesting/           # Backtesting engine
│   │   └── models/                # Analysis models
│   ├── interfaces/                # User interfaces
│   │   └── cli/                   # Command-line interface
│   │       ├── main.py            # Main CLI entry point
│   │       └── commands/          # CLI command modules
│   │           ├── data.py        # Data collection commands
│   │           ├── action_network.py # Action Network commands
│   │           ├── movement.py    # Movement analysis commands
│   │           ├── backtest.py    # Backtesting commands
│   │           └── database.py    # Database management commands
│   └── services/                  # Business logic services
│       ├── data/                  # Data services
│       ├── game/                  # Game management
│       ├── orchestration/         # Pipeline orchestration
│       ├── monitoring/            # System monitoring
│       ├── reporting/             # Report generation
│       ├── scheduling/            # Task scheduling
│       ├── strategy/              # Strategy services
│       └── workflow/              # Workflow management
├── tests/                         # Comprehensive test suite
│   ├── integration/               # Integration tests
│   ├── manual/                    # Manual test scripts
│   └── unit/                      # Unit tests
├── docs/                          # Documentation
├── sql/                           # Database schemas and migrations
│   ├── improvements/              # Data quality improvements
│   └── schemas/                   # Core database schemas
├── utilities/                     # Standalone utility scripts
├── logs/                          # Application logs
└── legacy/                        # Legacy code (deprecated)
    ├── action/                    # Action Network integration
    ├── sportsbookreview/          # SportsbookReview integration
    └── src/mlb_sharp_betting/     # Legacy services
```

## Configuration Management

All database settings, table names, and API configurations are centralized in `config.toml`:

```toml
[database]
# PostgreSQL connection configured in settings


[schema]
name = "splits"

[tables]
mlb_betting_splits = "raw_mlb_betting_splits"

[data_sources]
sbd = "SBD"   # SportsBettingDime
vsin = "VSIN" # VSIN

[api]
sbd_url = "https://srfeeds.sportsbettingdime.com/v2/matchups/mlb/betting-splits"
sbd_books = ["betmgm", "bet365", "fanatics", "draftkings", "caesars", "fanduel"]
```

### Benefits of Centralized Configuration

- **Single Source of Truth**: All table names and settings in one place
- **Easy Updates**: Change table names without touching code
- **Consistency**: Same naming across all scripts and SQL files
- **Environment Support**: Easy to create dev/prod configurations
- **Type Safety**: Python properties provide safe access

### Changing Table Names

To change table names, simply edit `config.toml`:

```toml
[tables]
mlb_betting_splits = "your_new_table_name"
```

All scripts and SQL queries will automatically use the new name.

## Database Schema

The main table `splits.raw_mlb_betting_splits` contains:

- **Game Information**: `game_id`, `home_team`, `away_team`, `game_datetime` (Eastern Time)
- **Split Data**: Betting percentages and counts for spread, total, and moneyline
- **Source Attribution**: `source` (SBD/VSIN) and `book` (specific sportsbook)
- **Metadata**: `last_updated` (Eastern Time), `sharp_action`, `outcome`

## Sharp Action Detection & Strategy Analysis

The system automatically identifies professional betting patterns using multiple sophisticated strategies:

### 🔥 Available Strategies
- **Sharp Action Detection**: Professional betting pattern identification
- **Line Movement Analysis**: Reverse line movement and steam detection
- **Consensus Analysis**: Public vs. sharp money patterns
- **Late Flip Detection**: Last-minute sharp action
- **Hybrid Sharp Analysis**: Multi-signal confirmation
- **Public Fade Opportunities**: Counter-public betting strategies
- **Book Conflict Analysis**: Sportsbook disagreement exploitation
- **Underdog Value Detection**: EV-positive underdog opportunities

### 📊 Strategy Execution
```bash
# Run comprehensive movement analysis (requires historical data file)
uv run -m src.interfaces.cli movement analyze --input-file output/action_network_history.json --show-details

# Detect reverse line movement opportunities
uv run -m src.interfaces.cli movement rlm --input-file output/action_network_history.json --min-movements 50

# Find steam moves across sportsbooks
uv run -m src.interfaces.cli movement steam --input-file output/action_network_history.json --show-details

# Backtest strategy performance
uv run -m src.interfaces.cli backtest run --start-date 2024-06-01 --end-date 2024-06-30 --strategies sharp_action consensus --initial-bankroll 10000
```

### 🎯 Example Output
```
🎯 PROFITABLE OPPORTUNITIES DETECTED
═══════════════════════════════════════

Game: Yankees @ Red Sox (7:05 PM ET)
├── Strategy: Sharp Action Detector
├── Signal: Fade Public (Yankees -1.5)
├── Confidence: 85%
├── Historical ROI: +12.3%
├── Recommended Action: Bet Red Sox +1.5
└── Reasoning: 67% public on Yankees, sharp money on Red Sox
```

## Complete Workflow Example

Here's a complete example workflow from setup to analysis:

### 1. Initial Setup

**🚨 CRITICAL SECURITY STEP: Configure Environment Variables**
```bash
# Copy environment template and configure secure passwords
cp .env.example .env
# Edit .env and replace ALL default passwords with secure values
# See SECURITY.md for password generation guidelines
```

```bash
# Install dependencies
uv sync

# Setup database
uv run -m src.interfaces.cli database setup-action-network --test-connection
```

**⚠️ Security Note**: Never commit `.env` files to version control. See [`SECURITY.md`](SECURITY.md) for complete security guidelines.

### 2. Collect Current Data
```bash
# Collect from individual sources
uv run -m src.interfaces.cli data collect --source vsin --real
uv run -m src.interfaces.cli data collect --source sbd --real

# Check collection status
uv run -m src.interfaces.cli data status --detailed
```

### 3. Generate Historical Data for Analysis
```bash
# Run Action Network pipeline (creates games file)
uv run -m src.interfaces.cli action-network pipeline --date today

# Generate historical line movement data
uv run -m src.interfaces.cli action-network history --days 30
```

### 4. Run Analysis
```bash
# Run comprehensive movement analysis
uv run -m src.interfaces.cli movement analyze \
  --input-file output/action_network_history.json \
  --show-details \
  --min-movements 30

# Look for specific patterns
uv run -m src.interfaces.cli movement rlm \
  --input-file output/action_network_history.json \
  --min-movements 50

# Check for steam moves
uv run -m src.interfaces.cli movement steam \
  --input-file output/action_network_history.json \
  --show-details
```

### 5. Backtest Strategies
```bash
# Test strategy performance
uv run -m src.interfaces.cli backtest run \
  --start-date 2024-06-01 \
  --end-date 2024-06-30 \
  --strategies sharp_action consensus \
  --initial-bankroll 10000 \
  --bet-size 100 \
  --min-confidence 0.7
```

### 6. Troubleshooting
```bash
# Test individual source connections
uv run -m src.interfaces.cli data test --source vsin --real

# Run data quality diagnostics
uv run -m src.interfaces.cli data-quality status

# Validate data quality
uv run -m src.interfaces.cli data status --detailed
```

### Timezone Handling

All timestamps are automatically converted from UTC (API format) to Eastern Time for better usability:
- Game times display as "2025-06-15 12:05 PM EDT"
- Last updated times show as "June 15, 2025 at 01:03 PM EDT"
- Proper handling of EST/EDT based on daylight saving time

## 🎯 Quick Reference for Daily Use

### For Business Users (Simple)
```bash
# Get today's predictions (most important!)
uv run -m src.interfaces.cli quickstart predictions

# Start web dashboard
uv run -m src.interfaces.cli monitoring dashboard
# Then visit: http://localhost:8080
```

### For Technical Users (Advanced)

1. **First-time Setup**:
   ```bash
   ./quick-start.sh  # Automated setup
   # OR
   uv sync && uv run -m src.interfaces.cli database setup-action-network
   ```

2. **Daily Data Collection**:
   ```bash
   uv run -m src.interfaces.cli data collect --source action_network --real
   uv run -m src.interfaces.cli data collect --source vsin --real
   ```

3. **Generate Analysis**:
   ```bash
   uv run -m src.interfaces.cli action-network history --days 30
   uv run -m src.interfaces.cli movement analyze --input-file output/action_network_history.json --show-details
   ```

4. **System Monitoring**:
   ```bash
   uv run -m src.interfaces.cli data status --detailed
   uv run -m src.interfaces.cli quickstart validate
   ```

## Data Sources & Pipeline Architecture

### Raw→Staging Movement Flow (New Unified Approach)

The system now uses a **unified historical approach** for moving data from raw to staging:

```
RAW (Source-Specific Tables)     →     STAGING (Historical/Temporal)
├── raw_data.action_network_odds  →  staging.action_network_odds_historical
├── raw_data.sbd_betting_splits   →  staging.sbd_historical  
├── raw_data.vsin_data           →  staging.vsin_historical
└── raw_data.mlb_stats_api       →  staging.mlb_games_historical
```

**Key Benefits:**
- **Complete Temporal Data**: Every line movement with microsecond precision timestamps
- **Unified Market Structure**: Single table with market_type (moneyline, spread, total) and side (home, away, over, under)
- **Historical Analysis**: Full line movement history for sophisticated sharp action detection
- **Source Attribution**: Clear tracking of data source and sportsbook for each record

**Processing Commands:**
```bash
# Collect raw data (source-specific tables)
uv run -m src.interfaces.cli data collect --source action_network --real

# Process through pipeline (raw → staging historical)
uv run -m src.interfaces.cli pipeline run --zone all --mode full

# Check pipeline status
uv run -m src.interfaces.cli pipeline status --detailed
```

### 3-Tier Data Pipeline
The system implements a sophisticated RAW → STAGING → CURATED architecture with **source-specific raw tables** and **unified historical staging**:

- **RAW Zone**: Source-specific tables (e.g., `raw_data.action_network_odds`, `raw_data.sbd_betting_splits`) with unprocessed external data
- **STAGING Zone**: Unified historical tables (e.g., `staging.action_network_odds_historical`) with temporal precision and complete line movement tracking
- **CURATED Zone**: Feature-enriched, analysis-ready datasets with advanced ML capabilities and cross-market analytics

### Supported Data Sources
- **Action Network**: Real-time betting lines, sharp action indicators, professional insights
- **SportsBettingDime (SBD)**: Real-time betting splits from 9+ major sportsbooks via WordPress JSON API
- **VSIN**: Professional betting insights and market analysis
- **MLB Stats API**: Official game data, statistics, and metadata
- **Odds API**: Real-time odds from multiple sportsbooks
- **Custom Sources**: Extensible framework for additional data sources

### Data Source Management
```bash
# View all available sources
uv run -m src.interfaces.cli data status --detailed

# Test source connections
uv run -m src.interfaces.cli data test --source vsin --real
uv run -m src.interfaces.cli data test --source action_network --real
```

## Recent Improvements ✨

### Production-Grade Monitoring & Observability (January 2025) 🚀
- **Real-Time Monitoring Dashboard**: FastAPI web dashboard with WebSocket updates for live pipeline status
- **Comprehensive Metrics**: 40+ Prometheus production metrics covering pipeline performance, business KPIs, and system health
- **Enterprise Security**: Production-ready API authentication, rate limiting with Redis support, IP whitelisting, and comprehensive audit logging
- **Break-Glass Controls**: Manual pipeline execution capabilities with full security controls for emergency operations
- **Enhanced Observability**: OpenTelemetry distributed tracing, correlation tracking, structured JSON logging, and performance profiling
- **CLI Integration**: Complete monitoring commands for dashboard management, health checks, performance analysis, and alert management

### VSIN Unified Collector Enhancement (July 2025)
- **Live HTML Parsing**: Direct extraction from VSIN betting splits pages with 100% data quality
- **Multi-Sportsbook Support**: 5 major sportsbooks (DraftKings, Circa, FanDuel, BetMGM, Caesars)  
- **Sharp Action Detection**: Comprehensive divergence analysis across moneyline, totals, and run line markets
- **Advanced Analytics**: 92.3% sharp action detection rate with percentage-based analysis
- **Three-Tier Integration**: Seamless RAW → STAGING → CURATED pipeline compatibility
- **Quality Scoring**: Real-time data completeness assessment and validation

### Unified Historical Pipeline Architecture (July 2025)
- **Complete architecture cleanup**: Consolidated multiple redundant approaches into unified historical staging
- **Source-Specific Raw Tables**: Replaced generic tables with source-specific approach for better data organization
- **Temporal Historical Staging**: Single `staging.action_network_odds_historical` table with microsecond precision for sophisticated betting analysis
- **Pipeline Success**: Successfully processes 7,691+ temporal records with complete line movement history
- **Architecture Documentation**: See `docs/ARCHITECTURE_CLEANUP_SUMMARY.md` for complete details

### Advanced ML & Analytics
- **32+ ML features**: Comprehensive betting analytics with implied probabilities, expected values, market efficiency scoring
- **Sharp action detection**: Multi-factor confidence scoring with professional betting pattern recognition
- **Feature engineering**: Advanced time-series features, market microstructure analysis, cross-book arbitrage detection
- **Real-time analytics**: Dynamic model adaptation with drift detection and performance monitoring

### Pydantic v2 Migration
- **Full Pydantic v2 compatibility**: All models upgraded to use latest validation features
- **Improved performance**: Faster model validation and serialization
- **Enhanced type safety**: Better error handling and field validation
- **Modern syntax**: Using `@field_validator` and `ValidationInfo` patterns

### Database Performance Optimization
- **Precision constraint fixes**: Resolved numeric overflow errors (DECIMAL(3,2) → DECIMAL(5,2))
- **Advanced indexing**: Composite indexes for 50-70% query performance improvement
- **Connection pooling**: Enhanced async connection management with health monitoring
- **Migration resilience**: Enhanced error recovery with transaction isolation

### Action Network Integration Enhancement
- **Consolidated collector**: New `consolidated_action_network_collector.py` for unified data collection
- **Historical staging processor**: Single `staging_action_network_history_processor.py` for temporal data processing
- **Unified staging approach**: Eliminated redundant sparse/wide/long processors in favor of historical temporal approach
- **Source-specific raw storage**: `raw_data.action_network_odds` flows to `staging.action_network_odds_historical`
- **Comprehensive data coverage**: 8+ sportsbooks with complete line movement history and microsecond precision
- **Database optimization**: Improved storage efficiency with temporal analysis capabilities

## Development

The project follows modern Python best practices with:
- **Unified Architecture**: Clean separation of concerns with modular services
- **Async-First Design**: Comprehensive async support for better performance
- **Type Safety**: Extensive type hints and Pydantic v2 models
- **Centralized Configuration**: Single source of truth for all settings
- **Comprehensive Testing**: Unit, integration, and performance tests
- **CLI-First Interface**: Complete command-line interface for all operations

### Development Commands
```bash
# Code quality checks
uv run ruff format     # Format code
uv run ruff check      # Lint code
uv run mypy src/       # Type checking

# Database operations
uv run -m src.interfaces.cli database setup-action-network --test-connection
uv run -m src.interfaces.cli database setup-action-network --schema-file custom_schema.sql

# Run specific analysis (requires historical data file)
uv run -m src.interfaces.cli movement analyze --input-file output/action_network_history.json --show-details --verbose
```

## Testing

Run the comprehensive test suite:
```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src --cov-report=html --cov-report=term-missing

# Run specific test types
uv run pytest tests/unit/
uv run pytest tests/integration/
uv run pytest tests/manual/
```

## Postgres
- Postgres uses Port 5433 with password postgres