# MLB Sharp Betting Data Analysis System - Refactoring Plan

## Executive Summary

This document outlines a comprehensive refactoring plan to transform 12 monolithic scripts into a well-architected, modular Python application following best practices for maintainability, scalability, and type safety.

## Current State Analysis

### Existing Scripts (12 total)
1. **Data Collection**
   - `vsin_scraper.py` - Scrapes VSIN HTML data
   - `fetch_current_lines.py` - Fetches current betting lines

2. **Data Parsing**
   - `vsin_parser.py` - Parses VSIN HTML into structured data
   - `parse_betting_splits.py` - Basic parsing logic
   - `parse_and_save_betting_splits.py` - Parse and persist

3. **Analysis**
   - `detect_sharp_action.py` - Detects sharp betting patterns
   - `simple_sharp_detection.py` - Simplified detection logic
   - `analyze_sharp_success.py` - Analyzes success rates

4. **Data Management**
   - `save_split_to_duckdb.py` - Database persistence
   - `migrate_to_long_format.py` - Data migration
   - `update_game_results.py` - Updates game outcomes

5. **Utilities**
   - `config_demo.py` - Configuration demonstration

### Key Issues Identified
- **Code Duplication**: Database connections, configuration imports, parsing logic
- **Mixed Responsibilities**: Scripts handle multiple concerns
- **No Type Safety**: Missing type hints throughout
- **Poor Error Handling**: Basic exception handling only
- **Hard-coded Values**: URLs, selectors, thresholds scattered
- **No Validation Layer**: Data validation mixed with business logic

## Proposed Architecture

### Module Structure
```
src/
└── mlb_sharp_betting/
    ├── __init__.py
    ├── core/
    │   ├── __init__.py
    │   ├── config.py          # Enhanced configuration management
    │   ├── exceptions.py      # Custom exceptions
    │   └── logging.py         # Centralized logging
    ├── models/
    │   ├── __init__.py
    │   ├── base.py           # Base Pydantic models
    │   ├── game.py           # Game-related models
    │   ├── splits.py         # Betting split models
    │   └── sharp.py          # Sharp action models
    ├── scrapers/
    │   ├── __init__.py
    │   ├── base.py           # Base scraper interface
    │   ├── vsin.py           # VSIN scraper implementation
    │   └── sbd.py            # SportsBettingDime scraper
    ├── parsers/
    │   ├── __init__.py
    │   ├── base.py           # Base parser interface
    │   ├── vsin.py           # VSIN HTML parser
    │   └── sbd.py            # SBD JSON parser
    ├── analyzers/
    │   ├── __init__.py
    │   ├── base.py           # Base analyzer interface
    │   ├── sharp_detector.py # Sharp action detection
    │   └── success_analyzer.py # Success rate analysis
    ├── db/
    │   ├── __init__.py
    │   ├── connection.py     # Database connection manager
    │   ├── repositories.py   # Data access layer
    │   └── migrations.py     # Database migrations
    ├── services/
    │   ├── __init__.py
    │   ├── data_collector.py # Orchestrates data collection
    │   ├── game_updater.py   # Updates game results
    │   └── sharp_monitor.py  # Monitors sharp action
    └── utils/
        ├── __init__.py
        ├── team_mapper.py     # Team name normalization
        └── validators.py      # Data validators
```

### Design Patterns

1. **Repository Pattern**: Centralize database access
2. **Factory Pattern**: Create scrapers/parsers dynamically
3. **Strategy Pattern**: Different analysis strategies
4. **Dependency Injection**: Improve testability
5. **Observer Pattern**: Event-driven updates

## Implementation Phases

### Phase 1: Foundation (Week 1)
1. Set up new module structure
2. Create base models with Pydantic
3. Implement core utilities (config, logging, exceptions)
4. Add comprehensive type hints
5. Set up testing infrastructure

### Phase 2: Data Layer (Week 2)
1. Implement repository pattern for database access
2. Create scraper base classes and implementations
3. Build parser modules with proper validation
4. Add retry logic and error handling
5. Write unit tests for data layer

### Phase 3: Business Logic (Week 3)
1. Implement analyzer modules
2. Create service layer for orchestration
3. Add sharp detection algorithms
4. Build success rate analytics
5. Write integration tests

### Phase 4: Migration & Testing (Week 4)
1. Create migration scripts from old to new structure
2. Implement backward compatibility layer
3. Comprehensive testing and validation
4. Performance optimization
5. Documentation updates

## Key Improvements

### 1. Type Safety
- Full MyPy compliance with strict mode
- Pydantic models for all data structures
- Type hints for all functions and methods

### 2. Error Handling
- Custom exception hierarchy
- Graceful degradation
- Comprehensive logging
- Retry mechanisms for network operations

### 3. Configuration Management
- Environment-based configuration
- Validation of all settings
- Support for multiple environments
- Secrets management

### 4. Testing Strategy
- Unit tests for all modules (>90% coverage)
- Integration tests for workflows
- Mock external dependencies
- Property-based testing for edge cases

### 5. Performance Optimizations
- Connection pooling for database
- Concurrent scraping where applicable
- Caching for frequently accessed data
- Batch operations for database writes

## Migration Strategy

### Step 1: Parallel Development
- Develop new modules alongside existing scripts
- No breaking changes initially
- Gradual migration of functionality

### Step 2: Feature Parity
- Ensure new modules match existing functionality
- Comprehensive testing against production data
- Performance benchmarking

### Step 3: Gradual Cutover
- Replace scripts one by one
- Monitor for issues
- Keep rollback plan ready

### Step 4: Deprecation
- Mark old scripts as deprecated
- Provide migration guide
- Remove after transition period

## Success Metrics

1. **Code Quality**
   - 100% type coverage with MyPy
   - >90% test coverage
   - Zero code duplication
   - All functions < 50 lines

2. **Performance**
   - 50% reduction in execution time
   - 75% reduction in memory usage
   - Concurrent operations where applicable

3. **Maintainability**
   - Clear module boundaries
   - Comprehensive documentation
   - Easy to add new scrapers/analyzers
   - Simplified debugging

4. **Reliability**
   - Proper error handling
   - Graceful failure modes
   - Comprehensive logging
   - Monitoring capabilities

## Risk Mitigation

1. **Data Loss**: Comprehensive backups before migration
2. **Breaking Changes**: Extensive testing and gradual rollout
3. **Performance Regression**: Benchmarking and monitoring
4. **Integration Issues**: Mock testing and staging environment

## Next Steps

1. Review and approve refactoring plan
2. Set up new project structure
3. Begin Phase 1 implementation
4. Establish CI/CD pipeline
5. Create comprehensive documentation 