"""
Configuration management for the MLB Sharp Betting system.

This module provides environment-based configuration management with
validation using Pydantic Settings.
"""

import os
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Union

from pydantic import Field, validator
from pydantic_settings import BaseSettings

from mlb_sharp_betting.core.exceptions import ConfigurationError


class DatabaseSettings(BaseSettings):
    """Database configuration settings."""
    
    path: Path = Field(
        default=Path("data/raw/mlb_betting.duckdb"),
        description="Path to the DuckDB database file",
        env="MLB_DB_PATH"
    )
    
    connection_timeout: int = Field(
        default=30,
        ge=1,
        le=300,
        description="Database connection timeout in seconds"
    )
    
    max_connections: int = Field(
        default=1,
        ge=1,
        le=1, 
        description="Maximum number of database connections in pool"
    )
    
    query_timeout: int = Field(
        default=300,
        ge=1,
        le=3600,
        description="Query timeout in seconds"
    )
    
    class Config:
        env_prefix = ""
        case_sensitive = False
        use_enum_values = True
    
    @validator("path")
    def validate_db_path(cls, v: Path) -> Path:
        """Validate database path."""
        # Only create directory if path looks reasonable (not system PATH)
        path_str = str(v)
        if ":" not in path_str and len(path_str) < 1000:
            try:
                v.parent.mkdir(parents=True, exist_ok=True)
            except (OSError, Exception):
                # Directory creation failed, but we'll continue
                # Database connection will handle this if needed
                pass
        return v


class SchemaSettings(BaseSettings):
    """Database schema configuration."""
    
    name: str = Field(
        default="splits",
        description="Database schema name"
    )
    
    mlb_betting_splits_table: str = Field(
        default="raw_mlb_betting_splits",
        description="Main table for MLB betting splits"
    )
    
    legacy_splits_table: str = Field(
        default="splits",
        description="Legacy table name for backward compatibility"
    )
    
    class Config:
        env_prefix = ""
        case_sensitive = False
        use_enum_values = True
    
    @property
    def full_table_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.name}.{self.mlb_betting_splits_table}"


class DataSourceSettings(BaseSettings):
    """Data source configuration."""
    
    sbd_identifier: str = Field(
        default="SBD",
        description="SportsBettingDime source identifier"
    )
    
    vsin_identifier: str = Field(
        default="VSIN", 
        description="VSIN source identifier"
    )
    
    class Config:
        env_prefix = ""
        case_sensitive = False
        use_enum_values = True


class APISettings(BaseSettings):
    """API configuration settings."""
    
    sbd_base_url: str = Field(
        default="https://srfeeds.sportsbettingdime.com/v2/matchups/mlb/betting-splits",
        description="SportsBettingDime API base URL"
    )
    
    sbd_books: List[str] = Field(
        default=["betmgm", "bet365", "fanatics", "draftkings", "caesars", "fanduel"],
        description="List of sportsbooks to include in API requests"
    )
    
    request_timeout: int = Field(
        default=30,
        ge=1,
        le=300,
        description="HTTP request timeout in seconds"
    )
    
    max_retries: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Maximum number of retry attempts"
    )
    
    retry_delay: float = Field(
        default=1.0,
        ge=0.1,
        le=60.0,
        description="Delay between retry attempts in seconds"
    )
    
    rate_limit_requests: int = Field(
        default=100,
        ge=1,
        le=10000,
        description="Number of requests allowed per rate limit window"
    )
    
    rate_limit_window: int = Field(
        default=60,
        ge=1,
        le=3600,
        description="Rate limit window in seconds"
    )
    
    class Config:
        env_prefix = ""
        case_sensitive = False
        use_enum_values = True
    
    @property
    def sbd_url_with_books(self) -> str:
        """Get SBD URL with books parameter."""
        books_param = ",".join(self.sbd_books)
        return f"{self.sbd_base_url}?books={books_param}"


class VSINSettings(BaseSettings):
    """VSIN-specific settings."""
    
    base_url: str = Field(
        default="https://www.vsin.com",
        description="VSIN base URL"
    )
    
    sharp_splits_path: str = Field(
        default="/betting-splits/mlb/",
        description="Path to sharp splits page",
        env="VSIN_SHARP_SPLITS_PATH"
    )
    
    user_agent: str = Field(
        default="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        description="User agent for web requests"
    )
    
    page_load_timeout: int = Field(
        default=30,
        ge=5,
        le=120,
        description="Page load timeout in seconds"
    )
    
    class Config:
        env_prefix = ""
        case_sensitive = False
        use_enum_values = True


class LoggingSettings(BaseSettings):
    """Logging configuration."""
    
    level: str = Field(
        default="INFO",
        description="Logging level"
    )
    
    structured: bool = Field(
        default=True,
        description="Whether to use structured JSON logging"
    )
    
    log_file: Optional[Path] = Field(
        default=None,
        description="Optional log file path",
        env="MLB_LOG_FILE"
    )
    
    max_file_size: int = Field(
        default=10485760,  # 10MB
        ge=1048576,  # 1MB
        le=104857600,  # 100MB
        description="Maximum log file size in bytes"
    )
    
    backup_count: int = Field(
        default=5,
        ge=1,
        le=50,
        description="Number of backup log files to keep"
    )
    
    class Config:
        env_prefix = ""
        case_sensitive = False
        use_enum_values = True
    
    @validator("level")
    def validate_log_level(cls, v: str) -> str:
        """Validate logging level."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in valid_levels:
            raise ValueError(f"Invalid log level: {v}. Must be one of {valid_levels}")
        return v.upper()


class Settings(BaseSettings):
    """Main application settings."""
    
    # Environment
    environment: str = Field(
        default="development",
        description="Application environment",
        env="MLB_BETTING_ENV"
    )
    
    debug: bool = Field(
        default=False,
        description="Enable debug mode",
        env="MLB_BETTING_DEBUG"
    )
    
    # Nested settings
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    db_schema: SchemaSettings = Field(default_factory=SchemaSettings)
    data_sources: DataSourceSettings = Field(default_factory=DataSourceSettings)
    api: APISettings = Field(default_factory=APISettings)
    vsin: VSINSettings = Field(default_factory=VSINSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    
    # Sharp detection settings
    sharp_threshold_percentage: float = Field(
        default=10.0,
        description="Percentage threshold for detecting sharp action",
        ge=0.1,
        le=50.0,
        env="SHARP_THRESHOLD_PERCENTAGE"
    )
    
    sharp_minimum_stake: float = Field(
        default=5.0,
        description="Minimum stake percentage difference for sharp detection",
        ge=0.1,
        le=25.0,
        env="SHARP_MINIMUM_STAKE"
    )
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_nested_delimiter = "__"
        case_sensitive = False
        validate_assignment = True
    
    @validator("environment")
    def validate_environment(cls, v: str) -> str:
        """Validate environment setting."""
        valid_envs = {"development", "testing", "staging", "production"}
        if v.lower() not in valid_envs:
            raise ValueError(f"Invalid environment: {v}. Must be one of {valid_envs}")
        return v.lower()
    
    def get_insert_query(self, split_type: str) -> str:
        """
        Get INSERT query for a specific split type.
        
        Args:
            split_type: Type of betting split (spread, total, moneyline)
            
        Returns:
            SQL INSERT query string
        """
        table_name = self.db_schema.full_table_name
        
        return f"""
            INSERT INTO {table_name} (
                game_id, home_team, away_team, game_datetime, split_type, 
                last_updated, source, book, home_or_over_bets, 
                home_or_over_bets_percentage, home_or_over_stake_percentage,
                away_or_under_bets, away_or_under_bets_percentage, 
                away_or_under_stake_percentage, split_value, sharp_action, outcome
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
    
    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment == "production"
    
    @property
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.environment == "development"


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.
    
    Returns:
        Singleton Settings instance
        
    Raises:
        ConfigurationError: If settings validation fails
    """
    try:
        return Settings()
    except Exception as e:
        raise ConfigurationError(
            "Failed to load application settings",
            details={"error": str(e)},
            cause=e,
        ) from e


# Global settings instance for backward compatibility
settings = get_settings() 