# 🎯 MLB Sharp Betting Analysis System

A comprehensive sports betting analysis platform focused on identifying profitable MLB betting opportunities through advanced analytics, sharp action detection, and automated backtesting.

## 🚀 Quick Start - Unified CLI System (v3.0)

The project has been completely reorganized with a unified CLI structure providing comprehensive data collection, analysis, and betting intelligence capabilities:

### 🔧 Core CLI Commands

```bash
# Main CLI entry point
uv run -m src.interfaces.cli --help

# Data Collection & Management
uv run -m src.interfaces.cli data collect --source vsin --real
uv run -m src.interfaces.cli data collect --source sbd --real
uv run -m src.interfaces.cli data collect --parallel --real
uv run -m src.interfaces.cli data status
uv run -m src.interfaces.cli data test --source vsin --real

# Movement Analysis & Strategy Detection
# Note: Requires historical data input file (generated from Action Network)
uv run -m src.interfaces.cli movement analyze --input-file output/action_network_history.json
uv run -m src.interfaces.cli movement rlm --input-file output/action_network_history.json --min-movements 50
uv run -m src.interfaces.cli movement steam --input-file output/action_network_history.json --show-details

# Action Network Pipeline
uv run -m src.interfaces.cli action-network collect --date today
uv run -m src.interfaces.cli action-network history --days 30

# Backtesting & Performance
uv run -m src.interfaces.cli backtest run --start-date 2024-06-01 --end-date 2024-06-30 --strategies sharp_action consensus
uv run -m src.interfaces.cli backtest run --start-date 2024-06-01 --end-date 2024-06-30 --strategies sharp_action --initial-bankroll 10000 --bet-size 100

# Database Management
uv run -m src.interfaces.cli database setup-action-network
uv run -m src.interfaces.cli database setup-action-network --schema-file sql/custom_schema.sql
uv run -m src.interfaces.cli database setup-action-network --test-connection

# Game Outcomes
uv run -m src.interfaces.cli outcomes update --date today
uv run -m src.interfaces.cli outcomes verify --games recent
```

### 📊 Complete Data Collection Process

```bash
# 1. Setup database (one-time)
uv run -m src.interfaces.cli database setup-action-network

# 2. Collect data from specific sources
uv run -m src.interfaces.cli data collect --source vsin --real
uv run -m src.interfaces.cli data collect --source sbd --real

# 3. Generate Action Network historical data
uv run -m src.interfaces.cli action-network collect --date today
uv run -m src.interfaces.cli data extract-action-network-history --input-file output/action_network_games.json

# 4. Run movement analysis
uv run -m src.interfaces.cli movement analyze --input-file output/action_network_history.json --show-details

# 5. Run backtesting on strategies
uv run -m src.interfaces.cli backtest run --start-date 2024-06-01 --end-date 2024-06-30 --strategies sharp_action

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
│   │   ├── config.py
│   │   ├── logging.py
│   │   └── exceptions.py
│   ├── data/                      # Data collection and models
│   │   ├── collection/            # Multi-source data collectors
│   │   ├── database/              # Database management
│   │   └── models/unified/        # Unified data models
│   ├── analysis/                  # Strategy processors and analysis
│   │   ├── processors/            # Strategy processors
│   │   ├── strategies/            # Strategy orchestration
│   │   └── backtesting/           # Backtesting engine
│   ├── interfaces/                # User interfaces
│   │   └── cli/                   # Command-line interface
│   │       ├── main.py            # Main CLI entry point
│   │       └── commands/          # CLI command modules
│   └── services/                  # Business logic services
│       ├── data/                  # Data services
│       ├── game/                  # Game management
│       ├── orchestration/         # Pipeline orchestration
│       └── workflow/              # Workflow management
├── tests/                         # Comprehensive test suite
│   ├── integration/
│   ├── manual/
│   └── unit/
├── docs/                          # Documentation
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
```bash
# Install dependencies
uv sync

# Setup database
uv run -m src.interfaces.cli database setup-action-network --test-connection
```

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
# Collect Action Network data (creates games file)
uv run -m src.interfaces.cli action-network collect --date today

# Extract historical line movement data
uv run -m src.interfaces.cli data extract-action-network-history \
  --input-file output/action_network_games.json \
  --output-file output/action_network_history.json
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

# Run comprehensive diagnostics
uv run -m src.interfaces.cli data diagnose --comprehensive

# Validate data quality
uv run -m src.interfaces.cli data validate
```

### Timezone Handling

All timestamps are automatically converted from UTC (API format) to Eastern Time for better usability:
- Game times display as "2025-06-15 12:05 PM EDT"
- Last updated times show as "June 15, 2025 at 01:03 PM EDT"
- Proper handling of EST/EDT based on daylight saving time

## Quick Start

1. **Install Dependencies**:
   ```bash
   uv sync
   ```

2. **Setup Database** (one-time):
   ```bash
   uv run -m src.interfaces.cli database setup-action-network
   ```

3. **Collect Data from Sources**:
   ```bash
   uv run -m src.interfaces.cli data collect --source vsin --real
   uv run -m src.interfaces.cli data collect --source sbd --real
   ```

4. **Generate Historical Data for Analysis**:
   ```bash
   uv run -m src.interfaces.cli action-network collect --date today
   uv run -m src.interfaces.cli data extract-action-network-history --input-file output/action_network_games.json
   ```

5. **Run Analysis & Find Opportunities**:
   ```bash
   uv run -m src.interfaces.cli movement analyze --input-file output/action_network_history.json --show-details
   ```

6. **View System Status**:
   ```bash
   uv run -m src.interfaces.cli data status --detailed
   ```

## Data Sources

The unified system supports multiple data sources for comprehensive betting analysis:

- **Action Network**: Real-time betting lines, sharp action indicators, professional insights
- **SportsBettingDime (SBD)**: Comprehensive betting splits data and sportsbook information
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
uv run -m src.interfaces.cli data test --real  # Test all sources

# Enable/disable specific sources
uv run -m src.interfaces.cli data enable --source action_network
uv run -m src.interfaces.cli data enable --all
uv run -m src.interfaces.cli data disable --source vsin
```

## Development

The project follows modern Python best practices with:
- **Unified Architecture**: Clean separation of concerns with modular services
- **Async-First Design**: Comprehensive async support for better performance
- **Type Safety**: Extensive type hints and Pydantic models
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
