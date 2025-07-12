"""
Unified Core Infrastructure

Provides core infrastructure components for the unified MLB betting system:
- Configuration management
- Exception handling
- Logging system
- Base utilities

This module consolidates core functionality from all legacy systems into
a unified, consistent interface.
"""

from .config import (
    UnifiedSettings,
    DatabaseSettings,
    SchemaSettings,
    DataSourceSettings,
    APISettings,
    ScrapingSettings,
    LoggingSettings,
    BettingSettings,
    NotificationSettings,
    FeatureFlags,
    get_settings,
)
from .exceptions import (
    UnifiedBettingError,
    ValidationError,
    ConfigurationError,
    DatabaseError,
    APIError,
    DataError,
    ScrapingError,
    ParsingError,
    AnalysisError,
    StrategyError,
    BacktestingError,
    RateLimitError,
    TimeoutError,
    CircuitBreakerError,
    handle_exception,
)
from .logging import (
    LogLevel,
    LogComponent,
    UnifiedLogger,
    LoggingConfig,
    get_logger,
    setup_logging,
    get_mlb_sharp_logger,
    get_sbr_logger,
    get_action_logger,
)

__all__ = [
    # Configuration
    "UnifiedSettings",
    "DatabaseSettings", 
    "SchemaSettings",
    "DataSourceSettings",
    "APISettings",
    "ScrapingSettings",
    "LoggingSettings",
    "BettingSettings",
    "NotificationSettings",
    "FeatureFlags",
    "get_settings",
    
    # Exceptions
    "UnifiedBettingError",
    "ValidationError",
    "ConfigurationError",
    "DatabaseError",
    "APIError",
    "DataError",
    "ScrapingError",
    "ParsingError",
    "AnalysisError",
    "StrategyError",
    "BacktestingError",
    "RateLimitError",
    "TimeoutError",
    "CircuitBreakerError",
    "handle_exception",
    
    # Logging
    "LogLevel",
    "LogComponent",
    "UnifiedLogger",
    "LoggingConfig",
    "get_logger",
    "setup_logging",
    "get_mlb_sharp_logger",
    "get_sbr_logger",
    "get_action_logger",
] 