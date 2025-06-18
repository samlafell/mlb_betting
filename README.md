# MLB Betting Splits Data Pipeline

A Python-based data pipeline for collecting and analyzing MLB betting splits data from SportsBettingDime and other sources.

## Project Structure

```
sports_betting_dime_splits/
├── config.toml                    # Centralized configuration
├── config/                        # Configuration management
│   ├── __init__.py
│   └── settings.py                # Configuration loader
├── data/
│   └── raw/
│       └── mlb_betting.duckdb     # DuckDB database
├── sql/
│   ├── schema.sql                 # Database schema
│   └── queries/
│       └── verify_data.sql        # Data verification queries
├── scripts/
│   ├── parse_and_save_betting_splits.py  # Main data collection script
│   └── config_demo.py             # Configuration demo
├── examples/
│   └── python_classes.py          # Data model classes
├── tests/
│   └── test_basic.py              # Basic tests
└── docs/
    └── sample_queries.sql         # Example SQL queries
```

## Configuration Management

All database settings, table names, and API configurations are centralized in `config.toml`:

```toml
[database]
path = "data/raw/mlb_betting.duckdb"

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

## Sharp Action Detection

The system automatically identifies professional betting patterns using multiple indicators:

- **🔥 Sharp Money**: 15+ point discrepancy between bet % and stake %
- **💰 Heavy Sharp Betting**: ≥60% money from ≤40% bets
- **📉 Public Darling Fade**: >75% tickets but <60% money

### Usage
```bash
# Detect sharp action in today's games
uv run scripts/simple_sharp_detection.py

# Query sharp action from database
duckdb data/raw/mlb_betting.duckdb -c "SELECT * FROM splits.raw_mlb_betting_splits WHERE sharp_action = true;"
```

See `docs/sharp_action_detection.md` for detailed documentation.

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

2. **Run Data Collection**:
   ```bash
   uv run python scripts/parse_and_save_betting_splits.py
   ```

3. **Verify Data**:
   ```bash
   # Connect to DuckDB and run verification queries
   duckdb data/raw/mlb_betting.duckdb < sql/queries/verify_data.sql
   ```

4. **View Configuration**:
   ```bash
   uv run python scripts/config_demo.py
   ```

## Data Sources

- **SportsBettingDime (SBD)**: Primary source for betting splits data
- **VSIN**: Future integration for additional sportsbook data

## Development

The project follows DuckDB best practices with:
- Organized directory structure
- Centralized configuration management
- Comprehensive testing framework
- Clear documentation and examples

## Testing

Run the test suite:
```bash
uv run python -m pytest tests/
```
