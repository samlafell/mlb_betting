# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Goals
Build a 24/7 sports betting service that will scrape various sources and be pulling down line information from these sources. Then, evaluate them pre-game against a system of strategies that have been developed. These strategies should have backtested historical performance attached to them so whenever we evaluate pre-game lines, it should only be using proven profitable systems.

## Plan & Review

### Before starting work
- Always in plan mode to make a plan
- Test everything!
- After get the plan, make sure you Write the plan to: /Users/samlafell/Documents/programming_projects/mlb_betting_program/.claude/tasks/
- The plan should be a detailed implementation plan and the reasoning behind them, as well as tasks broken down.
- If the task require external knowledge or certain package, also research to get latest knowledge (Use Task tool for research)
- Don't over plan it, always think MVP.
- Once you write the plan, firstly ask me to review it. Do not continue until I approve the plan.

### While implementing
- You should update the plan as you work.
- After you complete tasks in the plan, you should update and append detailed descriptions of the changes you made, so following tasks can be easily hand over to other engineers.


## Project Organization

- **`docs/`**: All documentation (.md files) should be placed here for optimal organization.
  - **`docs/examples/`**: Code examples and demonstrations (pipeline usage, backtesting, workflows)
  - **`docs/testing/`**: Testing documentation and sample data
  - **`docs/reports/`**: System analysis reports and daily/migration reports
- **`utilities/`**: Standalone utility scripts for quick testing and deployment
- **`src/`**: Main codebase with unified architecture
- **`sql/`**: Database schemas, migrations, and improvements
- **`tests/`**: Comprehensive testing suite

## Project Cleanup (January 2025)

**Major redundancy cleanup performed to improve project maintainability:**

### Removed Redundant Files
- **Utilities**: Removed `deploy_line_movement_improvements_simple.py` (basic version, kept full-featured version)
- **Scripts**: Removed `collect_2025_season.py` (basic version, kept optimized version)

### Archived Legacy/Debug Files
- **Date-specific Debug Scripts**: Archived 7 July 7th specific debug scripts to `scripts/archive/july_7th_debug/`
- **Resolved Issues**: Archived 9 issue-specific debug scripts to `scripts/archive/resolved_issues/`
- **Analysis Scripts**: Consolidated flip analysis scripts, kept `enhanced_late_sharp_flip_strategy_backtest.py`, archived 4 redundant variations

### Collector Registration System Overhaul (January 2025)
- **Centralized Registry**: Implemented singleton-based centralized registry system in `src/data/collection/registry.py`
- **Eliminated Duplicates**: Reduced redundant collector registrations from 9 to 6 (33% reduction)
- **SBR Consolidation**: Removed `SPORTS_BOOK_REVIEW_DEPRECATED` enum, eliminated 75% of SBR duplicates
- **Performance Improvement**: 40% faster startup time through duplicate elimination
- **Enhanced Caching**: Automatic collector instance caching for memory efficiency
- **Alias System**: Clean source aliases for backward compatibility (`sbr` → `sports_book_review`)
- **Documentation**: Comprehensive documentation in `docs/COLLECTOR_CLEANUP_IMPROVEMENTS.md`

### Documentation Reorganization (July 2025)
- **Consolidated docs/**: Moved `reports/`, `input/`, and `examples/` directories into `docs/` for better organization
  - `examples/` → `docs/examples/` (pipeline usage, backtesting, complete workflows)
  - `input/` → `docs/testing/sample_data/` (sample data for testing and development)  
  - `reports/` → `docs/reports/` (merged with existing reports structure)
- **Enhanced Structure**: Added README files for each new docs/ subdirectory
- **Improved Navigation**: Clear categorization of examples, testing data, and reports

### Archive Structure
```
scripts/archive/
├── july_7th_debug/          # Date-specific historical debugging scripts
├── resolved_issues/         # Issue-specific debug scripts for resolved problems
analysis_scripts/archive/
├── flip_variations/         # Redundant flip strategy analysis implementations
docs/
├── examples/                # Code examples and demonstrations
├── testing/                 # Testing documentation and sample data
└── reports/                 # Comprehensive system reports
```

**Benefits**: ~20 redundant files cleaned up, ~3,000+ lines of duplicate code eliminated, clearer project structure, centralized documentation, 40% performance improvement.

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
- UV (do not use python <scipt_name>.py to run programs; use `uv run <script_name>.py`)
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
uv run -m src.interfaces.cli action-network collect --date today
uv run -m src.interfaces.cli action-network history --days 30

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
uv run -m src.interfaces.cli outcomes verify --games recent

# Monitoring & Observability (NEW - Phase 1 Complete)
uv run -m src.interfaces.cli monitoring dashboard  # Start real-time web dashboard
uv run -m src.interfaces.cli monitoring status     # Check system health via API
uv run -m src.interfaces.cli monitoring live       # Real-time terminal monitoring
uv run -m src.interfaces.cli monitoring execute    # Manual break-glass pipeline execution
uv run -m src.interfaces.cli monitoring health-check --collector vsin  # Health check specific collector
uv run -m src.interfaces.cli monitoring performance --hours 24  # Performance analysis
uv run -m src.interfaces.cli monitoring alerts --severity critical  # Alert management
```

### Utility Scripts
Standalone utility scripts are available in the `utilities/` folder:
- **Migration scripts**: Phase 2, 3, and 4 pipeline migration utilities
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
│   │           ├── action_network_pipeline.py # Action Network commands
│   │           ├── movement_analysis.py    # Movement analysis commands
│   │           ├── backtesting.py # Backtesting commands
│   │           ├── game_outcomes.py # Game outcome commands
│   │           ├── data_quality_improvement.py # Data quality commands
│   │           ├── pipeline.py    # Pipeline management
│   │           └── setup_database.py    # Database management commands
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
   - **Centralized Registry System**: Singleton-based collector management with automatic caching
   - Action Network API integration with consolidated collector
   - SportsBettingDime (SBD) WordPress JSON API integration with 9+ sportsbooks
   - VSIN data collection with enhanced HTML parsing and sharp action detection
   - MLB Stats API integration
   - Sports Book Report (SBR) integration with consolidated enum management
   - Rate-limited data collection with orchestrated coordination
   - Multi-source data validation with duplicate prevention

2. **Analysis Layer** (`src/analysis/`)
   - Strategy processors for different betting patterns
   - Backtesting engine for historical validation
   - Movement analysis for line tracking
   - Sharp action detection
   - RLM (Reverse Line Movement) detection

3. **Database Layer** (`src/data/database/`)
   - PostgreSQL integration with source-specific raw tables
   - Repository pattern for data access
   - Schema migrations and management (see `sql/migrations/004_create_source_specific_zones.sql`)
   - Unified historical staging with temporal precision
   - Data quality monitoring and sportsbook mapping system

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

### Architecture Cleanup & Consolidation
- **Unified historical approach**: Consolidated multiple redundant processors (sparse, wide, long) into single historical staging
- **Source-specific raw tables**: Replaced generic tables with source-specific approach for better organization
- **Complete cleanup**: Removed legacy files and consolidated to `staging.action_network_odds_historical` for temporal analysis
- **Pipeline fixes**: Resolved database connection async patterns across all pipeline components
- **Documentation**: Created comprehensive cleanup summary in `docs/ARCHITECTURE_CLEANUP_SUMMARY.md`

### Action Network Integration Enhancement
- **Consolidated collector**: New unified `consolidated_action_network_collector.py`
- **Historical staging processor**: Single `staging_action_network_history_processor.py` for temporal data processing
- **Smart filtering**: Intelligent line movement noise reduction with `smart_line_movement_filter.py`
- **Comprehensive coverage**: 8+ major sportsbooks with complete line movement history
- **Temporal precision**: Microsecond-level timestamps for sophisticated betting analysis
- **Pipeline integration**: Source-specific raw → unified historical staging flow

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
- **Data Sources**: SBD (WordPress JSON API), VSIN, Action Network, MLB Stats API, Odds API
- **Feature Flags**: Control system behavior via config
- **Rate Limiting**: Configured per data source to respect API limits

## Production-Grade Monitoring & Observability (Phase 1 Complete - January 2025)

The system now includes comprehensive enterprise-grade monitoring infrastructure:

### 🚀 Real-Time Monitoring Dashboard
- **FastAPI Web Dashboard**: Real-time monitoring interface with WebSocket updates
- **Live Pipeline Status**: Real-time pipeline execution tracking with detailed progress updates
- **System Health Monitoring**: Comprehensive system health checks and dependency status
- **Break-Glass Controls**: Manual pipeline execution and system override capabilities
- **Interactive UI**: Modern web interface with live JavaScript updates and responsive design

### 📊 Prometheus Metrics Integration
- **40+ Production Metrics**: Pipeline performance, business metrics, system health indicators
- **SLI/SLO Tracking**: Service Level Indicators with automatic alerting thresholds
- **Performance Monitoring**: P99 latency tracking, success rates, error rates
- **Business Metrics**: Opportunities detected, strategy performance, betting value identification
- **Resource Monitoring**: System resource usage, database performance, API call metrics

### 🔍 Enhanced Observability
- **Distributed Tracing**: OpenTelemetry integration with OTLP export capability
- **Correlation Tracking**: Request correlation IDs across all async operations
- **Structured Logging**: JSON-formatted logs with comprehensive metadata and context
- **Performance Profiling**: Detailed timing analysis for all major operations
- **Error Tracking**: Comprehensive error handling with detailed context preservation

### 🔒 Security & Access Control
- **API Key Authentication**: Secure access to break-glass endpoints with Bearer token support
- **Rate Limiting**: Production-ready rate limiting with Redis support for distributed deployments
- **IP Whitelisting**: CIDR range support for authorized network access
- **Audit Logging**: Comprehensive security event logging with correlation IDs
- **Security Headers**: Automatic security headers for all API responses

### 📡 Real-Time API Endpoints
- **Health Check**: `/health` - System health and dependency status
- **Pipeline Status**: `/api/pipelines/status` - Current and recent pipeline executions
- **System Metrics**: `/api/system/status` - Comprehensive system status with metrics
- **Break-Glass Control**: `/api/control/*` - Emergency manual pipeline execution
- **WebSocket Updates**: `/ws` - Real-time status updates for dashboard
- **Prometheus Metrics**: `/metrics` - Prometheus-compatible metrics endpoint

## Key Files to Understand

### Core Configuration & Utilities
- `src/core/config.py`: Central configuration management (Pydantic v2, unified settings)
- `src/core/datetime_utils.py`: Timezone handling utilities (EST/EDT conversion)
- `src/core/team_utils.py`: MLB team name normalization and mapping
- `src/core/sportsbook_utils.py`: Sportsbook ID resolution and mapping
- `config.toml`: Centralized configuration file

### Data Collection (Primary Focus)
- `src/data/collection/consolidated_action_network_collector.py`: **Primary Action Network collector** (recommended for new development)
- `src/data/collection/sbd_unified_collector_api.py`: **SBD WordPress JSON API collector** - real-time data from 9+ major sportsbooks
- `src/data/collection/vsin_unified_collector.py`: **Enhanced VSIN collector** with live HTML parsing, sharp action detection, and three-tier pipeline integration
- `src/data/collection/smart_line_movement_filter.py`: Intelligent noise reduction for line movements
- `src/data/collection/orchestrator.py`: Main data collection orchestration with centralized registry integration
- `src/data/collection/registry.py`: **Centralized collector registry** - singleton-based management system
- `src/data/collection/base.py`: Base collector classes and common functionality

### Database & Pipeline (Updated Architecture)
- `src/data/pipeline/staging_action_network_history_processor.py`: **Unified historical staging processor** (replaces multiple old processors)
- `sql/migrations/004_create_source_specific_zones.sql`: **New source-specific migration** (replaces generic table approach)
- **Removed legacy files**: Old sparse/wide/long processors consolidated into historical approach

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

### Monitoring & Observability (NEW - Phase 1)
- `src/interfaces/api/monitoring_dashboard.py`: **Real-time monitoring dashboard** with WebSocket updates
- `src/services/monitoring/prometheus_metrics_service.py`: **Prometheus metrics service** with 40+ production metrics
- `src/core/enhanced_logging.py`: **Enhanced logging service** with OpenTelemetry integration
- `src/interfaces/cli/commands/monitoring.py`: **Monitoring CLI commands** for dashboard, health checks, and performance analysis
- `src/core/security.py`: **Production security module** with API authentication, rate limiting, and IP whitelisting
- `docs/PRODUCTION_SECURITY_GUIDE.md`: **Security configuration guide** for production deployments

## Testing Strategy

- Unit tests for individual components (`tests/unit/`)
- Integration tests for database operations (`tests/integration/`)
- Manual test scripts for specific scenarios (`tests/manual/`)
- Coverage reporting with 80% minimum threshold
- Real-time testing with `--real` flag for live data sources

## Common Development Tasks

When working on this codebase:

1. **Adding New Strategy**: Create processor in `src/analysis/processors/`
2. **New Data Source**: Add collector in `src/data/collection/` and register using centralized registry system
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

## Collector System Development Guidelines

### Using the Centralized Registry

The new centralized collector registry provides improved performance and maintainability:

```python
# Recommended: Use centralized registry
from src.data.collection.registry import (
    initialize_all_collectors,
    get_collector_instance,
    get_collector_class
)

# Initialize registry once
initialize_all_collectors()

# Get collector instance (cached automatically)
collector = get_collector_instance("action_network", config)

# Use aliases for convenience
sbr_collector = get_collector_instance("sbr")  # Resolves to sports_book_review
```

### Migration from Old System

When migrating existing code:

1. **Replace direct imports** with registry access
2. **Update enum references** - remove deprecated `SPORTS_BOOK_REVIEW_DEPRECATED`
3. **Use source aliases** for backward compatibility
4. **Leverage instance caching** for performance

**Old Pattern** (Deprecated):
```python
from .consolidated_action_network_collector import ActionNetworkCollector
collector = ActionNetworkCollector(config)  # Manual instantiation
```

**New Pattern** (Recommended):
```python
collector = get_collector_instance("action_network", config)  # Registry-based
```

### Performance Benefits

- **40% faster startup**: Eliminated duplicate registrations
- **Automatic caching**: Instance reuse reduces memory overhead
- **Clean logging**: No duplicate "Collector registered" messages
- **Enhanced reliability**: Built-in duplicate prevention

### Documentation Resources

**User Documentation:**
- [`USER_GUIDE.md`](USER_GUIDE.md): **Complete user guide** with setup instructions, monitoring dashboard usage, and production deployment guidance

**Technical Documentation in `docs/`:**
- [`COLLECTOR_CLEANUP_IMPROVEMENTS.md`](docs/COLLECTOR_CLEANUP_IMPROVEMENTS.md): Complete cleanup overview
- [`CENTRALIZED_REGISTRY_SYSTEM.md`](docs/CENTRALIZED_REGISTRY_SYSTEM.md): Technical implementation details
- [`SBR_CONSOLIDATION_GUIDE.md`](docs/SBR_CONSOLIDATION_GUIDE.md): SBR-specific improvements
- [`DEVELOPER_MIGRATION_GUIDE.md`](docs/DEVELOPER_MIGRATION_GUIDE.md): Step-by-step migration instructions
- [`PRODUCTION_SECURITY_GUIDE.md`](docs/PRODUCTION_SECURITY_GUIDE.md): **Production security configuration** and deployment guide
