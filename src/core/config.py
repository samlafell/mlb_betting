"""
Unified Configuration Management

Consolidates configuration from:
- config/settings.py (TOML-based configuration)
- src/mlb_sharp_betting/core/config.py (Pydantic-based settings)
- config/backtesting_config.json (JSON-based backtesting config)
- config.toml (main configuration file)

Provides environment-based configuration with validation, feature flags,
and backward compatibility.
"""

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

import toml
from pydantic import Field, computed_field, field_validator, model_validator
from pydantic_settings import BaseSettings

from .exceptions import ConfigurationError


class DatabaseSettings(BaseSettings):
    """Unified database configuration supporting multiple database types."""

    # PostgreSQL settings (primary)
    host: str = Field(default="localhost", description="Database host", env="DB_HOST")

    port: int = Field(default=5432, description="Database port", env="DB_PORT")

    database: str = Field(
        default="mlb_betting", description="Database name", env="DB_NAME"
    )

    user: str = Field(
        default="postgres", description="Database username", env="DB_USER"
    )

    password: str = Field(
        default="", description="Database password", env="DB_PASSWORD"
    )

    # Connection pool settings
    min_connections: int = Field(
        default=2, description="Minimum connections in pool", env="DB_MIN_CONN"
    )

    max_connections: int = Field(
        default=20, description="Maximum connections in pool", env="DB_MAX_CONN"
    )

    connection_timeout: int = Field(
        default=30,
        ge=1,
        le=300,
        description="Connection timeout in seconds",
        env="DB_TIMEOUT",
    )

    query_timeout: int = Field(
        default=300,
        ge=1,
        le=3600,
        description="Query timeout in seconds",
        env="DB_QUERY_TIMEOUT",
    )

    # Schema settings
    default_schema: str = Field(
        default="public", description="Default database schema", env="DB_SCHEMA"
    )

    # Legacy DuckDB support (for migration)
    duckdb_path: str | None = Field(
        default=None, description="Legacy DuckDB file path", env="DUCKDB_PATH"
    )

    # Read replica settings
    read_replica_connection_string: str | None = Field(
        default=None,
        description="Read replica connection string",
        env="DB_READ_REPLICA_URL",
    )

    class Config:
        env_prefix = ""
        case_sensitive = False
        use_enum_values = True
        extra = "allow"  # Allow extra fields for backward compatibility

    # Computed properties for backward compatibility with connection pool
    @computed_field
    @property
    def pool_size(self) -> int:
        """Map min_connections to pool_size for backward compatibility."""
        return self.min_connections

    @computed_field
    @property
    def max_overflow(self) -> int:
        """Map max_connections to max_overflow for backward compatibility."""
        return self.max_connections - self.min_connections

    @computed_field
    @property
    def pool_timeout(self) -> int:
        """Map connection_timeout to pool_timeout for backward compatibility."""
        return self.connection_timeout

    @computed_field
    @property
    def pool_recycle(self) -> int:
        """Default pool recycle time."""
        return 3600  # 1 hour

    @computed_field
    @property
    def connection_string(self) -> str:
        """Get PostgreSQL connection string."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    @computed_field
    @property
    def async_connection_string(self) -> str:
        """Get async PostgreSQL connection string."""
        return f"postgresql+asyncpg://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class SchemaSettings(BaseSettings):
    """Database schema configuration."""

    # Raw data schemas
    raw_data_schema: str = Field(
        default="raw_data", description="Schema for raw external data"
    )

    # Core betting data schemas
    core_betting_schema: str = Field(
        default="core_betting", description="Schema for core betting data"
    )

    # Analytics schemas
    analytics_schema: str = Field(
        default="analytics", description="Schema for analytics and derived data"
    )

    # Operational schemas
    operational_schema: str = Field(
        default="operational", description="Schema for operational data"
    )

    # Legacy schema mappings
    legacy_splits_schema: str = Field(
        default="splits", description="Legacy splits schema"
    )

    legacy_mlb_betting_schema: str = Field(
        default="mlb_betting", description="Legacy MLB betting schema"
    )

    # Table name mappings
    table_mappings: dict[str, str] = Field(
        default_factory=lambda: {
            "games": "core_betting.games",
            "odds": "core_betting.odds",
            "betting_analysis": "analytics.betting_analysis",
            "sharp_signals": "analytics.sharp_signals",
            "raw_html": "raw_data.raw_html",
            "system_logs": "operational.system_logs",
        },
        description="Logical table name to physical table mappings",
    )

    class Config:
        env_prefix = ""
        case_sensitive = False
        use_enum_values = True
        extra = "allow"  # Allow extra fields for backward compatibility

    def get_table(self, logical_name: str) -> str:
        """Get physical table name from logical name."""
        return self.table_mappings.get(logical_name, logical_name)


class DataSourceSettings(BaseSettings):
    """Data source configuration."""

    # Source identifiers
    sbd_identifier: str = Field(
        default="SBD", description="SportsBettingDime identifier"
    )

    vsin_identifier: str = Field(default="VSIN", description="VSIN identifier")

    action_network_identifier: str = Field(
        default="ACTION_NETWORK", description="Action Network identifier"
    )

    mlb_stats_api_identifier: str = Field(
        default="MLB_STATS_API", description="MLB Stats API identifier"
    )

    sportsbookreview_identifier: str = Field(
        default="SPORTSBOOKREVIEW", description="SportsbookReview identifier"
    )

    # Priority ordering for data sources
    source_priority: list[str] = Field(
        default=["MLB_STATS_API", "SPORTSBOOKREVIEW", "ACTION_NETWORK", "VSIN", "SBD"],
        description="Data source priority order",
    )

    class Config:
        env_prefix = ""
        case_sensitive = False
        use_enum_values = True
        extra = "allow"  # Allow extra fields for backward compatibility


class APISettings(BaseSettings):
    """Unified API configuration for all external services."""

    # SportsBettingDime API
    sbd_base_url: str = Field(
        default="https://srfeeds.sportsbettingdime.com/v2/matchups/mlb/betting-splits",
        description="SBD API base URL",
    )

    sbd_books: list[str] = Field(
        default=["betmgm", "bet365", "fanatics", "draftkings", "caesars", "fanduel"],
        description="SBD sportsbooks to include",
    )

    # MLB Stats API
    mlb_stats_base_url: str = Field(
        default="https://statsapi.mlb.com/api/v1",
        description="MLB Stats API base URL",
        env="MLB_STATS_API_URL",
    )

    # Action Network API
    action_network_base_url: str = Field(
        default="https://api.actionnetwork.com/web/v1",
        description="Action Network API base URL",
        env="ACTION_NETWORK_API_URL",
    )

    action_network_api_key: str | None = Field(
        default=None, description="Action Network API key", env="ACTION_NETWORK_API_KEY"
    )

    # The Odds API
    odds_api_key: str | None = Field(
        default=None, description="The Odds API key", env="ODDS_API_KEY"
    )

    odds_api_base_url: str = Field(
        default="https://api.the-odds-api.com/v4", description="The Odds API base URL"
    )

    # Request settings
    request_timeout: int = Field(
        default=30, ge=1, le=300, description="HTTP request timeout in seconds"
    )

    max_retries: int = Field(
        default=3, ge=0, le=10, description="Maximum retry attempts"
    )

    retry_delay: float = Field(
        default=1.0, ge=0.1, le=60.0, description="Delay between retries in seconds"
    )

    # Rate limiting
    rate_limit_requests: int = Field(
        default=100, ge=1, le=10000, description="Requests per rate limit window"
    )

    rate_limit_window: int = Field(
        default=60, ge=1, le=3600, description="Rate limit window in seconds"
    )

    class Config:
        env_prefix = ""
        case_sensitive = False
        use_enum_values = True
        extra = "allow"  # Allow extra fields for backward compatibility

    @computed_field
    @property
    def sbd_url_with_books(self) -> str:
        """Get SBD URL with books parameter."""
        books_param = ",".join(self.sbd_books)
        return f"{self.sbd_base_url}?books={books_param}"


class ScrapingSettings(BaseSettings):
    """Web scraping configuration."""

    # User agents for different sites
    default_user_agent: str = Field(
        default="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        description="Default user agent",
    )

    # VSIN specific settings
    vsin_base_url: str = Field(
        default="https://www.vsin.com", description="VSIN base URL"
    )

    vsin_sharp_splits_path: str = Field(
        default="/betting-splits/mlb/", description="VSIN sharp splits path"
    )

    # SportsbookReview specific settings
    sbr_base_url: str = Field(
        default="https://www.sportsbookreview.com",
        description="SportsbookReview base URL",
    )

    # Page load settings
    page_load_timeout: int = Field(
        default=30, ge=5, le=120, description="Page load timeout in seconds"
    )

    # Anti-bot measures
    enable_stealth: bool = Field(
        default=True, description="Enable stealth mode for scraping"
    )

    delay_between_requests: float = Field(
        default=1.0, ge=0.1, le=10.0, description="Delay between requests in seconds"
    )

    class Config:
        env_prefix = ""
        case_sensitive = False
        use_enum_values = True
        extra = "allow"  # Allow extra fields for backward compatibility


class LoggingSettings(BaseSettings):
    """Unified logging configuration."""

    level: str = Field(default="INFO", description="Logging level")

    structured: bool = Field(default=True, description="Use structured JSON logging")

    log_file: Path | None = Field(
        default=None, description="Optional log file path", env="LOG_FILE"
    )

    max_file_size: int = Field(
        default=10485760,  # 10MB
        ge=1048576,  # 1MB
        le=104857600,  # 100MB
        description="Maximum log file size in bytes",
    )

    backup_count: int = Field(
        default=5, ge=1, le=50, description="Number of backup log files"
    )

    # SQL operations logging
    sql_log_file: Path | None = Field(
        default=None, description="SQL operations log file", env="SQL_LOG_FILE"
    )

    log_sql_operations: bool = Field(
        default=True, description="Enable SQL operations logging"
    )

    # Performance logging
    log_performance: bool = Field(
        default=True, description="Enable performance logging"
    )

    performance_threshold_ms: int = Field(
        default=1000, ge=1, le=60000, description="Performance logging threshold in ms"
    )

    class Config:
        env_prefix = ""
        case_sensitive = False
        use_enum_values = True
        extra = "allow"  # Allow extra fields for backward compatibility

    @field_validator("level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate logging level."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"Invalid log level: {v}. Must be one of {valid_levels}")
        return v.upper()


class BettingSettings(BaseSettings):
    """Betting strategy and analysis configuration."""

    # Sharp detection thresholds
    sharp_threshold_percentage: float = Field(
        default=10.0,
        description="Sharp action detection threshold",
        ge=0.1,
        le=50.0,
        env="SHARP_THRESHOLD",
    )

    sharp_minimum_stake: float = Field(
        default=5.0,
        description="Minimum stake difference for sharp detection",
        ge=0.1,
        le=25.0,
        env="SHARP_MIN_STAKE",
    )

    # Juice filtering
    max_juice_threshold: int = Field(
        default=-160,
        description="Maximum acceptable juice for favorites",
        le=-100,
        ge=-500,
        env="MAX_JUICE",
    )

    enable_juice_filter: bool = Field(
        default=True, description="Enable juice filtering", env="ENABLE_JUICE_FILTER"
    )

    # Risk management
    max_daily_recommendations: int = Field(
        default=5, ge=1, le=50, description="Maximum daily recommendations"
    )

    min_confidence_threshold: float = Field(
        default=0.65,
        ge=0.5,
        le=1.0,
        description="Minimum confidence for recommendations",
    )

    # Backtesting settings
    min_sample_size: int = Field(
        default=15, ge=5, le=100, description="Minimum sample size for analysis"
    )

    break_even_win_rate: float = Field(
        default=0.5238, ge=0.5, le=0.7, description="Break-even win rate threshold"
    )

    class Config:
        env_prefix = ""
        case_sensitive = False
        use_enum_values = True
        extra = "allow"  # Allow extra fields for backward compatibility


class NotificationSettings(BaseSettings):
    """Notification and alerting configuration."""

    # Email settings
    email_enabled: bool = Field(
        default=False, description="Enable email notifications", env="EMAIL_ENABLED"
    )

    email_from_address: str | None = Field(
        default=None, description="From email address", env="EMAIL_FROM"
    )

    email_app_password: str | None = Field(
        default=None, description="Email app password", env="EMAIL_PASSWORD"
    )

    email_to_addresses: str = Field(
        default="", description="Comma-separated to addresses", env="EMAIL_TO"
    )

    # Slack settings
    slack_enabled: bool = Field(
        default=False, description="Enable Slack notifications", env="SLACK_ENABLED"
    )

    slack_webhook_url: str | None = Field(
        default=None, description="Slack webhook URL", env="SLACK_WEBHOOK"
    )

    # Alert settings
    alert_retention_days: int = Field(
        default=30, ge=1, le=365, description="Alert retention period in days"
    )

    max_alerts_per_hour: int = Field(
        default=10, ge=1, le=100, description="Maximum alerts per hour"
    )

    class Config:
        env_prefix = ""
        case_sensitive = False
        use_enum_values = True
        extra = "allow"  # Allow extra fields for backward compatibility

    def get_email_list(self) -> list[str]:
        """Get email addresses as a list."""
        if not self.email_to_addresses.strip():
            return []
        return [email.strip() for email in self.email_to_addresses.split(",")]


class FeatureFlags(BaseSettings):
    """Feature flag configuration for gradual rollouts."""

    # Data collection features
    enable_sportsbookreview: bool = Field(
        default=True,
        description="Enable SportsbookReview data collection",
        env="FEATURE_SBR",
    )

    enable_action_network: bool = Field(
        default=True,
        description="Enable Action Network data collection",
        env="FEATURE_ACTION",
    )

    enable_mlb_stats_api: bool = Field(
        default=True,
        description="Enable MLB Stats API integration",
        env="FEATURE_MLB_API",
    )

    # Analysis features
    enable_sharp_detection: bool = Field(
        default=True,
        description="Enable sharp action detection",
        env="FEATURE_SHARP_DETECTION",
    )

    enable_line_movement_analysis: bool = Field(
        default=True,
        description="Enable line movement analysis",
        env="FEATURE_LINE_MOVEMENT",
    )

    enable_consensus_analysis: bool = Field(
        default=True, description="Enable consensus analysis", env="FEATURE_CONSENSUS"
    )

    # System features
    enable_caching: bool = Field(
        default=True, description="Enable data caching", env="FEATURE_CACHING"
    )

    enable_async_processing: bool = Field(
        default=True, description="Enable async processing", env="FEATURE_ASYNC"
    )

    enable_performance_monitoring: bool = Field(
        default=True,
        description="Enable performance monitoring",
        env="FEATURE_PERF_MONITORING",
    )

    # Migration features
    enable_legacy_compatibility: bool = Field(
        default=True,
        description="Enable legacy system compatibility",
        env="FEATURE_LEGACY_COMPAT",
    )

    enable_unified_models: bool = Field(
        default=True,
        description="Enable unified data models",
        env="FEATURE_UNIFIED_MODELS",
    )

    class Config:
        env_prefix = ""
        case_sensitive = False
        use_enum_values = True
        extra = "allow"  # Allow extra fields for backward compatibility


class UnifiedSettings(BaseSettings):
    """
    Main unified settings class consolidating all configuration.

    Combines patterns from:
    - config/settings.py (TOML-based)
    - src/mlb_sharp_betting/core/config.py (Pydantic-based)
    - config/backtesting_config.json (JSON-based)
    """

    # Environment
    environment: str = Field(
        default="development", description="Application environment", env="ENVIRONMENT"
    )

    debug: bool = Field(default=False, description="Enable debug mode", env="DEBUG")

    # Application info
    app_name: str = Field(
        default="MLB Sharp Betting System", description="Application name"
    )

    app_version: str = Field(default="2.0.0", description="Application version")

    # Timezone settings
    timezone: str = Field(
        default="US/Eastern",
        description="Application timezone (EST as per requirements)",
    )

    # Nested settings
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    schemas: SchemaSettings = Field(default_factory=SchemaSettings)
    data_sources: DataSourceSettings = Field(default_factory=DataSourceSettings)
    api: APISettings = Field(default_factory=APISettings)
    scraping: ScrapingSettings = Field(default_factory=ScrapingSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    betting: BettingSettings = Field(default_factory=BettingSettings)
    notifications: NotificationSettings = Field(default_factory=NotificationSettings)
    features: FeatureFlags = Field(default_factory=FeatureFlags)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_nested_delimiter = "__"
        case_sensitive = False
        validate_assignment = True
        extra = "allow"  # Allow extra fields for backward compatibility

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        """Validate environment setting."""
        valid_envs = {"development", "testing", "staging", "production"}
        if v.lower() not in valid_envs:
            raise ValueError(f"Invalid environment: {v}. Must be one of {valid_envs}")
        return v.lower()

    @model_validator(mode="before")
    @classmethod
    def handle_legacy_env_vars(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Handle legacy environment variables for backward compatibility."""
        if not isinstance(values, dict):
            return values

        # Map legacy environment variables to nested settings
        legacy_mappings = {
            # Database settings
            "postgres_host": ("database", "host"),
            "postgres_port": ("database", "port"),
            "postgres_db": ("database", "database"),
            "postgres_user": ("database", "user"),
            "postgres_password": ("database", "password"),
            # API settings
            "odds_api_key": ("api", "odds_api_key"),
            # Notification settings
            "email_from_address": ("notifications", "email_from_address"),
            "email_app_password": ("notifications", "email_app_password"),
            "email_to_addresses": ("notifications", "email_to_addresses"),
        }

        # Process legacy mappings
        for legacy_key, (section, new_key) in legacy_mappings.items():
            if legacy_key in values:
                # Initialize nested section if it doesn't exist
                if section not in values:
                    values[section] = {}
                elif not isinstance(values[section], dict):
                    values[section] = {}

                # Map the legacy value to the new nested structure
                values[section][new_key] = values[legacy_key]
                # Remove the legacy key
                del values[legacy_key]

        return values

    # Computed properties
    @computed_field
    @property
    def is_production(self) -> bool:
        """Check if running in production."""
        return self.environment == "production"

    @computed_field
    @property
    def is_development(self) -> bool:
        """Check if running in development."""
        return self.environment == "development"

    @computed_field
    @property
    def is_testing(self) -> bool:
        """Check if running in testing."""
        return self.environment == "testing"

    # Utility methods
    def get_table(self, logical_name: str) -> str:
        """Get physical table name from logical name."""
        return self.schemas.get_table(logical_name)

    def get_insert_query(self, table: str, columns: list[str]) -> str:
        """
        Generate INSERT query for a table.

        Args:
            table: Logical table name
            columns: List of column names

        Returns:
            PostgreSQL INSERT query
        """
        physical_table = self.get_table(table)
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))

        return f"""
            INSERT INTO {physical_table} ({columns_str})
            VALUES ({placeholders})
        """

    def load_legacy_config(self, config_path: Path | None = None) -> dict[str, Any]:
        """
        Load legacy configuration files for backward compatibility.

        Args:
            config_path: Path to config file (TOML or JSON)

        Returns:
            Dictionary of legacy configuration
        """
        if config_path is None:
            # Try to find config files in standard locations
            project_root = Path(__file__).parent.parent.parent

            # Try config.toml first
            toml_path = project_root / "config.toml"
            if toml_path.exists():
                return toml.load(toml_path)

            # Try backtesting_config.json
            json_path = project_root / "config" / "backtesting_config.json"
            if json_path.exists():
                with open(json_path) as f:
                    return json.load(f)

        else:
            if config_path.suffix == ".toml":
                return toml.load(config_path)
            elif config_path.suffix == ".json":
                with open(config_path) as f:
                    return json.load(f)

        return {}

    def merge_legacy_config(self, legacy_config: dict[str, Any]) -> None:
        """
        Merge legacy configuration into current settings.

        Args:
            legacy_config: Legacy configuration dictionary
        """
        # This would implement logic to merge legacy configs
        # For now, we'll just validate the structure exists
        if "database" in legacy_config:
            # Legacy database config exists
            pass

        if "api" in legacy_config:
            # Legacy API config exists
            pass


@lru_cache
def get_settings() -> UnifiedSettings:
    """
    Get cached unified settings instance.

    Returns:
        Singleton UnifiedSettings instance

    Raises:
        ConfigurationError: If settings validation fails
    """
    try:
        settings = UnifiedSettings()

        # Load and merge legacy configurations for backward compatibility
        if settings.features.enable_legacy_compatibility:
            legacy_config = settings.load_legacy_config()
            if legacy_config:
                settings.merge_legacy_config(legacy_config)

        return settings

    except Exception as e:
        raise ConfigurationError(
            "Failed to load unified application settings",
            details={"error": str(e)},
            cause=e,
        ) from e


# Global settings instance for backward compatibility
settings = get_settings()
