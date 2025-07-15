# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Goals
Build a 24/7 sports betting service that will scrape various sources and be pulling down line information from these sources. Then, evaluate them pre-game against a system of strategies that have been developed. These strategies should have backtested historical performance attached to them so whenever we evaluate pre-game lines, it should only be using proven profitable systems.

## Development Commands

### Package Management
- Install dependencies: `uv sync`
- Install with dev dependencies: `uv sync --dev`

### Testing
- Run tests: `uv run pytest`
- Run tests with coverage: `uv run pytest --cov=src/mlb_sharp_betting --cov-report=html --cov-report=term-missing`
- Run specific test file: `uv run pytest tests/test_basic.py`

### Code Quality
- Format code: `uv run ruff format`
- Lint code: `uv run ruff check`
- Type checking: `uv run mypy src/mlb_sharp_betting`
- Fix linting issues: `uv run ruff check --fix`

### CLI Commands
The project has a comprehensive CLI system accessible via multiple entry points:

```bash
# Main CLI entry points
uv run -m mlb_sharp_betting
uv run -m mlb_sharp_betting.cli

# Core analysis and data collection
uv run -m mlb_sharp_betting.cli.commands.analysis
uv run -m mlb_sharp_betting.cli.commands.data_collection
uv run -m mlb_sharp_betting.cli.commands.daily_update

# Backtesting and strategy validation
uv run -m mlb_sharp_betting.cli.commands.backtesting
uv run -m mlb_sharp_betting.cli.commands.enhanced_backtesting
uv run -m mlb_sharp_betting.cli.commands.betting_performance

# Scheduling and workflow
uv run -m mlb_sharp_betting.cli.commands.scheduler
uv run -m mlb_sharp_betting.cli.commands.pre_game
uv run -m mlb_sharp_betting.cli.commands.pre_game_performance

# System monitoring and diagnostics
uv run -m mlb_sharp_betting.cli.commands.system_status
uv run -m mlb_sharp_betting.cli.commands.diagnostics
uv run -m mlb_sharp_betting.cli.commands.daily_report

# Specialized analysis
uv run -m mlb_sharp_betting.cli.commands.timing_analysis
uv run -m mlb_sharp_betting.cli.commands.enhanced_detection
```

## Project Architecture

This is a comprehensive MLB betting analysis system with a layered architecture:

### Core Components

1. **Data Collection Layer** (`src/data/collection/`)
   - Action Network API integration
   - Rate-limited data collection
   - Multi-source data validation

2. **Analysis Layer** (`src/analysis/`)
   - Strategy processors for different betting patterns
   - Backtesting engine for historical validation
   - Movement analysis for line tracking

3. **Database Layer** (`src/data/database/`)
   - PostgreSQL integration
   - Repository pattern for data access
   - Schema migrations and management

4. **Services Layer** (`src/services/`)
   - Orchestration services for complex workflows
   - Monitoring and reporting services
   - Game outcome tracking

5. **CLI Interface** (`src/interfaces/cli/`)
   - Command-based interface for all operations
   - Modular command structure
   - Real-time monitoring capabilities

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

1. **Data Collection**: Use CLI commands to collect current game data
2. **Analysis**: Run strategy processors to identify betting opportunities
3. **Backtesting**: Validate strategies against historical data
4. **Monitoring**: Track system performance and data quality
5. **Reporting**: Generate daily reports and performance summaries

## Configuration

- Configuration is managed through `config.toml` and environment variables
- Database connections are configured via environment variables
- Feature flags control system behavior
- Rate limiting is configured per data source

## Key Files to Understand

- `src/core/config.py`: Central configuration management
- `src/data/collection/orchestrator.py`: Main data collection orchestrator
- `src/analysis/strategies/orchestrator.py`: Strategy execution orchestrator
- `src/services/pipeline_orchestrator.py`: End-to-end pipeline management
- `src/interfaces/cli/main.py`: CLI entry point and command routing

## Testing Strategy

- Unit tests for individual components
- Integration tests for database operations
- Manual test scripts for specific scenarios
- Coverage reporting with 80% minimum threshold

## Common Development Tasks

When working on this codebase:

1. **Adding New Strategy**: Create processor in `src/analysis/processors/`
2. **New Data Source**: Add collector in `src/data/collection/`
3. **Database Changes**: Update schema and add migration
4. **New CLI Command**: Add to `src/interfaces/cli/commands/`
5. **Service Integration**: Add to appropriate service layer

## Important Notes

- All timestamps are handled in Eastern Time for consistency
- The system uses a dual-architecture approach with both legacy and modern components
- Rate limiting is critical for external API integrations
- Database operations should use the repository pattern
- All external API calls should include proper error handling and retries

## No New Work Directories
Do not build ANY new files in these directories. They are legacy. And the functionality should be replicated in the src/ folder through the various new interfaces we have.
- /src/mlb_sharp_betting
- /action/
- /sportsbookreview/