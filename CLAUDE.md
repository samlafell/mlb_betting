# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Goals
Build a 24/7 sports betting service that will scrape various sources and be pulling down line information from these sources. Then, evaluate them pre-game against a system of strategies that have been developed. These strategies should have backtested historical performance attached to them so whenever we evaluate pre-game lines, it should only be using proven profitable systems.

## Project Organization

- **`docs/`**: All documentation (.md files) should be placed here for optimal organization.
- **`utilities/`**: Standalone utility scripts for quick testing and deployment
- **`src/`**: Main codebase with unified architecture
- **`sql/`**: Database schemas, migrations, and improvements
- **`tests/`**: Comprehensive testing suite


## Development Commands

### Package Management
- Install dependencies: `uv sync`
- Install with dev dependencies: `uv sync --dev`

### Testing
- Run tests: `uv run pytest`
- Run tests with coverage: `uv run pytest --cov=src --cov-report=html --cov-report=term-missing`
- Run specific test file: `uv run pytest tests/test_basic.py`
- Run integration tests: `uv run pytest tests/integration/`
- Run manual tests: `uv run pytest tests/manual/`

### Code Quality
- Format code: `uv run ruff format`
- Lint code: `uv run ruff check`
- Type checking: `uv run mypy src/`
- Fix linting issues: `uv run ruff check --fix`

### CLI Commands
The project has a unified CLI system accessible via the main entry point:

```bash
# Main CLI entry point
uv run -m src.interfaces.cli --help

# Data Collection & Management
uv run -m src.interfaces.cli data collect --source vsin --real
uv run -m src.interfaces.cli data collect --source sbd --real
uv run -m src.interfaces.cli data collect --parallel --real
uv run -m src.interfaces.cli data status
uv run -m src.interfaces.cli data test --source vsin --real

# Movement Analysis & Strategy Detection (requires historical data)
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

# Data Quality (post-reorganization)
uv run -m src.interfaces.cli data-quality setup
uv run -m src.interfaces.cli data-quality status
```

### Utility Scripts
Standalone utility scripts are available in the `utilities/` folder:

```bash
# Quick start interface
uv run python utilities/action_network_quickstart.py

# Data analysis scripts
uv run python utilities/analyze_existing_data.py

# Pipeline runner
uv run python utilities/run_action_network_pipeline.py

# Data quality deployment
uv run python utilities/deploy_data_quality_improvements.py phase1
uv run python utilities/deploy_data_quality_improvements.py all

# Game outcomes testing
uv run python utilities/test_game_outcomes.py
```

## Project Architecture

This is a comprehensive MLB betting analysis system with a unified layered architecture:

### Directory Structure
```
mlb_betting_program/
├── config.toml                    # Centralized configuration
├── src/                           # Unified architecture
│   ├── core/                      # Core configuration and logging
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
│   └── services/                  # Business logic services
├── tests/                         # Comprehensive test suite
├── docs/                          # Documentation
├── utilities/                     # Standalone utility scripts
└── sql/                          # Database schemas and migrations
```

### Core Components

1. **Data Collection Layer** (`src/data/collection/`)
   - Action Network API integration
   - SportsBettingDime (SBD) integration
   - VSIN data collection
   - MLB Stats API integration
   - Rate-limited data collection
   - Multi-source data validation

2. **Analysis Layer** (`src/analysis/`)
   - Strategy processors for different betting patterns
   - Backtesting engine for historical validation
   - Movement analysis for line tracking
   - Sharp action detection
   - RLM (Reverse Line Movement) detection

3. **Database Layer** (`src/data/database/`)
   - PostgreSQL integration
   - Repository pattern for data access
   - Schema migrations and management
   - Data quality monitoring
   - Sportsbook mapping system

4. **Services Layer** (`src/services/`)
   - Orchestration services for complex workflows
   - Monitoring and reporting services
   - Game outcome tracking
   - Sharp action detection service
   - Data quality improvement services

5. **CLI Interface** (`src/interfaces/cli/`)
   - Unified command-based interface for all operations
   - Modular command structure
   - Real-time monitoring capabilities
   - Data quality management commands

### Key Data Models

- **Game Data**: Central game information with team mappings
- **Betting Analysis**: Sharp action detection and pattern analysis
- **Movement Analysis**: Line movement tracking and historical patterns
- **Strategy Results**: Backtesting results and performance metrics

### Strategy Processors

The system includes multiple strategy processors located in `src/analysis/processors/`:

- **Sharp Action Processor**: Detects professional betting patterns
- **Line Movement Processor**: Analyzes betting line changes
- **Consensus Processor**: Tracks public vs. sharp money
- **Late Flip Processor**: Identifies last-minute sharp action
- **Hybrid Sharp Processor**: Combines multiple sharp indicators

### Database Schema

- Uses PostgreSQL with proper foreign key relationships
- Separate schemas for different data types (raw data, analysis results, etc.)
- Comprehensive indexing for performance
- Migration system for schema evolution

## Development Workflow

1. **Initial Setup**: 
   ```bash
   uv sync
   uv run -m src.interfaces.cli database setup-action-network
   ```

2. **Data Collection**: Use CLI commands to collect current game data
   ```bash
   uv run -m src.interfaces.cli data collect --source vsin --real
   uv run -m src.interfaces.cli data collect --source sbd --real
   ```

3. **Historical Data Generation**: For movement analysis
   ```bash
   uv run -m src.interfaces.cli action-network collect --date today
   uv run -m src.interfaces.cli data extract-action-network-history --input-file output/action_network_games.json
   ```

4. **Analysis**: Run strategy processors to identify betting opportunities
   ```bash
   uv run -m src.interfaces.cli movement analyze --input-file output/action_network_history.json
   ```

5. **Backtesting**: Validate strategies against historical data
   ```bash
   uv run -m src.interfaces.cli backtest run --start-date 2024-06-01 --end-date 2024-06-30 --strategies sharp_action
   ```

6. **Monitoring**: Track system performance and data quality
   ```bash
   uv run -m src.interfaces.cli data status --detailed
   uv run -m src.interfaces.cli data-quality status
   ```

## Configuration

- **Centralized Configuration**: All settings managed through `config.toml`
- **Database Settings**: PostgreSQL connections via environment variables or config.toml
- **Data Sources**: SBD, VSIN, Action Network, MLB Stats API, Odds API
- **Feature Flags**: Control system behavior via config
- **Rate Limiting**: Configured per data source to respect API limits

## Key Files to Understand

- `src/core/config.py`: Central configuration management (unified settings)
- `src/data/collection/orchestrator.py`: Main data collection orchestrator
- `src/analysis/strategies/orchestrator.py`: Strategy execution orchestrator
- `src/services/orchestration/pipeline_orchestration_service.py`: End-to-end pipeline management
- `src/interfaces/cli/main.py`: CLI entry point and command routing
- `src/services/sharp_action_detection_service.py`: Sharp action detection integration
- `src/services/game_outcome_service.py`: Game outcome tracking
- `src/data/database/action_network_repository.py`: Action Network data handling
- `config.toml`: Centralized configuration file

## Testing Strategy

- Unit tests for individual components (`tests/unit/`)
- Integration tests for database operations (`tests/integration/`)
- Manual test scripts for specific scenarios (`tests/manual/`)
- Coverage reporting with 80% minimum threshold
- Real-time testing with `--real` flag for live data sources

## Common Development Tasks

When working on this codebase:

1. **Adding New Strategy**: Create processor in `src/analysis/processors/`
2. **New Data Source**: Add collector in `src/data/collection/` and register in orchestrator
3. **Database Changes**: Update schema in `sql/` and add migration
4. **New CLI Command**: Add to `src/interfaces/cli/commands/`
5. **Service Integration**: Add to appropriate service layer in `src/services/`
6. **Data Quality Improvements**: Update views and functions in `sql/improvements/`

## Important Notes

- All timestamps are handled in Eastern Time for consistency
- The system uses a unified architecture with the modern `src/` structure
- Rate limiting is critical for external API integrations
- Database operations should use the repository pattern
- All external API calls should include proper error handling and retries
- Data quality monitoring is integrated with automatic sportsbook ID resolution
- The system supports multiple data sources with centralized configuration

## Data Quality Features

The system includes comprehensive data quality improvements:

- **Sportsbook Mapping System**: Automatic resolution of external sportsbook IDs
- **Data Completeness Scoring**: Real-time quality assessment
- **Sharp Action Integration**: Automatic population from strategy processors
- **Quality Monitoring**: Dashboard views and trend analysis
- **Deployment Scripts**: Available in `utilities/deploy_data_quality_improvements.py`
