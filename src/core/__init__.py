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
    APISettings,
    BettingSettings,
    DatabaseSettings,
    DataSourceSettings,
    FeatureFlags,
    LoggingSettings,
    NotificationSettings,
    SchemaSettings,
    ScrapingSettings,
    UnifiedSettings,
    get_settings,
)
from .exceptions import (
    AnalysisError,
    APIError,
    BacktestingError,
    CircuitBreakerError,
    ConfigurationError,
    DatabaseError,
    DataError,
    ParsingError,
    RateLimitError,
    ScrapingError,
    StrategyError,
    TimeoutError,
    UnifiedBettingError,
    ValidationError,
    handle_exception,
)
from .logging import (
    LogComponent,
    LoggingConfig,
    LogLevel,
    UnifiedLogger,
    get_action_logger,
    get_logger,
    get_mlb_sharp_logger,
    get_sbr_logger,
    setup_logging,
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
