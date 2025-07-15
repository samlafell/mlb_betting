"""
Custom exceptions for the MLB Sharp Betting system.

This module defines a hierarchy of custom exceptions to provide
clear error handling throughout the application.
"""

from typing import Any


class MLBSharpBettingError(Exception):
    """Base exception for all MLB Sharp Betting system errors."""

    def __init__(
        self,
        message: str,
        details: dict[str, Any] | None = None,
        cause: Exception | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}
        self.cause = cause

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} - Details: {self.details}"
        return self.message


class ConfigurationError(MLBSharpBettingError):
    """Raised when there are configuration-related errors."""

    pass


class DatabaseError(MLBSharpBettingError):
    """Raised when database operations fail."""

    pass


class ConnectionError(DatabaseError):
    """Raised when database connection fails."""

    pass


class DatabaseConnectionError(DatabaseError):
    """Raised when database connection operations fail."""

    pass


class QueryError(DatabaseError):
    """Raised when database queries fail."""

    pass


class MigrationError(DatabaseError):
    """Raised when database migrations fail."""

    pass


class ScrapingError(MLBSharpBettingError):
    """Raised when web scraping operations fail."""

    pass


class NetworkError(ScrapingError):
    """Raised when network requests fail."""

    pass


class AuthenticationError(ScrapingError):
    """Raised when authentication fails."""

    pass


class RateLimitError(ScrapingError):
    """Raised when rate limit is exceeded."""

    pass


class ParsingError(MLBSharpBettingError):
    """Raised when data parsing fails."""

    pass


class HTMLParsingError(ParsingError):
    """Raised when HTML parsing fails."""

    pass


class JSONParsingError(ParsingError):
    """Raised when JSON parsing fails."""

    pass


class ValidationError(MLBSharpBettingError):
    """Raised when data validation fails."""

    pass


class ModelValidationError(ValidationError):
    """Raised when Pydantic model validation fails."""

    pass


class DataIntegrityError(ValidationError):
    """Raised when data integrity checks fail."""

    pass


class AnalysisError(MLBSharpBettingError):
    """Raised when analysis operations fail."""

    pass


class SharpDetectionError(AnalysisError):
    """Raised when sharp action detection fails."""

    pass


class ServiceError(MLBSharpBettingError):
    """Raised when service layer operations fail."""

    pass


class DataCollectionError(ServiceError):
    """Raised when data collection service fails."""

    pass


class GameUpdateError(ServiceError):
    """Raised when game update service fails."""

    pass


class MonitoringError(ServiceError):
    """Raised when monitoring service fails."""

    pass
