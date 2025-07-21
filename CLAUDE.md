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

## TEST EVERYTHIGN
every time you create a new feature, test it.
Run integration tests and unit tests.

Only once you confirm via Integration and Unit tests that a service is functional, can you say it is a success and document the success.

## DOCUMENT EVERYTHING
When you create a new service/functionality, document it!

if it's major, it should be documented in README.md and CLAUDE.md.

Every child folder should have their own CLAUDE.md to leave for autonomous AI agents later that are accessing the same folder for them to evaluate the CLAUDE.md and its contents to know how to work in this directory.

Every major piece of the project should be thoroughly documented in docs/.
- Data Collection, and each sub-module should be clear what's happening and how it works (this incldues scraping + parsing)
   - VSIN
   - Action Network
   - SBD
- Data Persistence
- Technical details - which tooling is used (uv/postgres/python, etc)
- Logging should be in root/logs/ and should be consistent across modules
- Recommendation Generation

## REUSE and CENTRALIZATION
- reuse when possible but only when it makes sense
- WET (write everything twice), KISS concepts apply.


## Tooling
### Pydantic
- Only use V2

### Python
- Ruff
- UV
- PyTest
- MyPy

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

# Data collection (primary workflow)
uv run -m src.interfaces.cli data collect --source action_network --real
uv run -m src.interfaces.cli data collect --source vsin --real
uv run -m src.interfaces.cli data collect --source sbd --real
uv run -m src.interfaces.cli data status

# Action Network pipeline
uv run -m src.interfaces.cli action-network pipeline
uv run -m src.interfaces.cli action-network opportunities

# Movement analysis
uv run -m src.interfaces.cli movement analyze --input-file output/action_network_history.json
uv run -m src.interfaces.cli movement rlm --input-file output/action_network_history.json
uv run -m src.interfaces.cli movement steam --input-file output/action_network_history.json

# Backtesting
uv run -m src.interfaces.cli backtest run --start-date 2024-06-01 --end-date 2024-06-30 --strategies sharp_action

# Database management
uv run -m src.interfaces.cli database setup-action-network
uv run -m src.interfaces.cli database setup-action-network --test-connection

# Data quality
uv run -m src.interfaces.cli data-quality deploy
uv run -m src.interfaces.cli data-quality status

# Game outcomes
uv run -m src.interfaces.cli outcomes update --date today
```

### Utility Scripts
Standalone utility scripts are available in the `utilities/` folder:
- **Data quality deployment**: `utilities/deploy_data_quality_improvements.py`
- **Database setup utilities**: Various setup and migration scripts
- **Testing utilities**: Development and debugging tools


## Project Architecture

This is a comprehensive MLB betting analysis system with a unified layered architecture:

### Directory Structure
```
mlb_betting_program/
├── config.toml                    # Centralized configuration
├── src/                           # Unified architecture
│   ├── core/                      # Core configuration and logging
│   │   ├── config.py              # Central configuration (Pydantic v2)
│   │   ├── datetime_utils.py      # Timezone handling (EST/EDT)
│   │   ├── team_utils.py          # MLB team normalization
│   │   └── sportsbook_utils.py    # Sportsbook ID resolution
│   ├── data/                      # Data collection and models
│   │   ├── collection/            # Multi-source data collectors
│   │   │   ├── consolidated_action_network_collector.py  # Primary Action Network
│   │   │   ├── smart_line_movement_filter.py            # Noise reduction
│   │   │   ├── orchestrator.py                          # Collection orchestration
│   │   │   └── base.py                                  # Base collector classes
│   │   ├── database/              # Database management
│   │   │   ├── repositories/      # Repository pattern
│   │   │   └── migrations/        # Schema migrations
│   │   └── models/unified/        # Unified data models (Pydantic v2)
│   │       ├── game.py            # Game data models
│   │       ├── betting_analysis.py # Betting analysis models
│   │       └── base.py            # Base model classes
│   ├── analysis/                  # Strategy processors and analysis
│   │   ├── processors/            # Strategy processors
│   │   │   └── action/            # Action Network specific processors
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
│   │           └── database.py    # Database management commands
│   └── services/                  # Business logic services
│       ├── orchestration/         # Pipeline orchestration
│       ├── monitoring/            # System monitoring
│       ├── reporting/             # Report generation
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
└── logs/                          # Application logs
```

### Core Components

1. **Data Collection Layer** (`src/data/collection/`)
   - Action Network API integration
   - SportsBettingDime (SBD) integration
   - VSIN data collection
   - MLB Stats API integration
   - Sports Book Report (SBR) integration
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

## Recent Improvements (July 2025)

### Pydantic v2 Migration
- **Complete migration**: All models updated to use Pydantic v2 syntax
- **Field validators**: Updated to use `@field_validator` decorator
- **ValidationInfo**: Using modern `ValidationInfo` instead of legacy `values` parameter
- **Settings import**: Proper `pydantic_settings.BaseSettings` import structure
- **Type safety**: Enhanced validation and error handling

### Action Network Integration Enhancement
- **Consolidated collector**: New unified `consolidated_action_network_collector.py`
- **Smart filtering**: Intelligent line movement noise reduction with `smart_line_movement_filter.py`
- **Multi-mode support**: Current, historical, and comprehensive collection modes
- **Comprehensive coverage**: 8+ major sportsbooks (DraftKings, FanDuel, BetMGM, etc.)
- **Database optimization**: Improved storage efficiency and duplicate prevention
- **Real-time validation**: Live data testing with 15 games and 402+ records successfully processed

### Database & Data Quality
- **Sportsbook ID resolution**: Automatic mapping of external sportsbook identifiers
- **Quality scoring**: Real-time data completeness assessment
- **Duplicate prevention**: Enhanced external source ID management with timestamp precision
- **EST/EDT handling**: Proper timezone consistency across all operations
- **Schema improvements**: Better indexing and performance optimization

### CLI System Enhancement
- **Command structure**: Comprehensive CLI with data, action-network, movement, backtest, database commands
- **Testing integration**: Built-in test modes with `--real` flag for live data validation
- **Status monitoring**: Real-time collection status and diagnostics
- **Error handling**: Improved error reporting and recovery mechanisms

## Development Workflow

### Primary Development Commands
```bash
# Setup and testing
uv sync                                                    # Install dependencies
uv run -m src.interfaces.cli data test --source action_network --real  # Test Action Network
uv run pytest                                             # Run test suite

# Data collection workflow
uv run -m src.interfaces.cli data collect --source action_network --real
uv run -m src.interfaces.cli data status --detailed

# Code quality
uv run ruff format && uv run ruff check && uv run mypy src/
```

## Configuration

- **Centralized Configuration**: All settings managed through `config.toml`
- **Database Settings**: PostgreSQL connections via environment variables or config.toml
- **Data Sources**: SBD, VSIN, Action Network, MLB Stats API, Odds API
- **Feature Flags**: Control system behavior via config
- **Rate Limiting**: Configured per data source to respect API limits

## Key Files to Understand

### Core Configuration & Utilities
- `src/core/config.py`: Central configuration management (Pydantic v2, unified settings)
- `src/core/datetime_utils.py`: Timezone handling utilities (EST/EDT conversion)
- `src/core/team_utils.py`: MLB team name normalization and mapping
- `src/core/sportsbook_utils.py`: Sportsbook ID resolution and mapping
- `config.toml`: Centralized configuration file

### Data Collection (Primary Focus)
- `src/data/collection/consolidated_action_network_collector.py`: **Primary Action Network collector** (recommended for new development)
- `src/data/collection/smart_line_movement_filter.py`: Intelligent noise reduction for line movements
- `src/data/collection/orchestrator.py`: Main data collection orchestration
- `src/data/collection/base.py`: Base collector classes and common functionality

### Database & Models
- `src/data/models/unified/game.py`: Game data models (Pydantic v2)
- `src/data/models/unified/betting_analysis.py`: Betting analysis models (Pydantic v2)
- `src/data/models/unified/base.py`: Base model classes with validation
- `src/data/database/`: Repository pattern implementations

### CLI Interface
- `src/interfaces/cli/main.py`: CLI entry point and command routing
- `src/interfaces/cli/commands/data.py`: Data collection commands
- `src/interfaces/cli/commands/action_network.py`: Action Network specific commands
- `src/interfaces/cli/commands/movement.py`: Movement analysis commands

### Analysis & Strategy
- `src/analysis/strategies/orchestrator.py`: Strategy execution orchestrator
- `src/analysis/processors/`: Strategy processors for different betting patterns
- `src/services/orchestration/pipeline_orchestration_service.py`: End-to-end pipeline management

### Services
- `src/services/sharp_action_detection_service.py`: Sharp action detection integration
- `src/services/game_outcome_service.py`: Game outcome tracking
- `src/services/monitoring/`: System monitoring and health checks

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
