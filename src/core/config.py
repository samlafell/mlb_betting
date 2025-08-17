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
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

from .exceptions import ConfigurationError
from .pydantic_compat import computed_field, field_validator, model_validator


class DatabaseSettings(BaseSettings):
    """Unified database configuration supporting multiple database types."""

    # PostgreSQL settings (primary) - Default to container hostname for Docker compatibility
    host: str = Field(default="postgres", description="Database host", env="DB_HOST")

    port: int = Field(default=5433, description="Database port", env="DB_PORT")

    database: str = Field(
        default="mlb_betting", description="Database name", env="DB_NAME"
    )

    user: str = Field(
        default="samlafell", description="Database username", env="DB_USER"
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

    @computed_field
    @property
    def masked_connection_string(self) -> str:
        """Get connection string with masked password for logging."""
        masked_password = "*" * len(self.password) if self.password else ""
        return f"postgresql://{self.user}:{masked_password}@{self.host}:{self.port}/{self.database}"
    
    @computed_field
    @property
    def username(self) -> str:
        """Backward compatibility alias for user field."""
        return self.user

    def validate_connection_config(self) -> dict[str, bool]:
        """
        Validate database configuration completeness.
        
        Returns:
            Dictionary with validation results for each required field
        """
        validation_results = {
            "host": bool(self.host and self.host.strip()),
            "port": 1 <= self.port <= 65535,
            "database": bool(self.database and self.database.strip()),
            "user": bool(self.user and self.user.strip()),
            "password": bool(self.password and self.password.strip()),
            "connection_timeout": 1 <= self.connection_timeout <= 300,
            "query_timeout": 1 <= self.query_timeout <= 3600,
            "min_connections": 1 <= self.min_connections <= self.max_connections,
            "max_connections": self.min_connections <= self.max_connections <= 100,
        }
        return validation_results

    def get_connection_issues(self) -> list[str]:
        """
        Get list of connection configuration issues.
        
        Returns:
            List of human-readable validation error messages
        """
        validation_results = self.validate_connection_config()
        issues = []
        
        if not validation_results["host"]:
            issues.append("Database host is required and cannot be empty")
        if not validation_results["port"]:
            issues.append("Database port must be between 1 and 65535")
        if not validation_results["database"]:
            issues.append("Database name is required and cannot be empty")
        if not validation_results["user"]:
            issues.append("Database user is required and cannot be empty")
        if not validation_results["password"]:
            issues.append("Database password is required and cannot be empty")
        if not validation_results["connection_timeout"]:
            issues.append("Connection timeout must be between 1 and 300 seconds")
        if not validation_results["query_timeout"]:
            issues.append("Query timeout must be between 1 and 3600 seconds")
        if not validation_results["min_connections"]:
            issues.append("Minimum connections must be at least 1 and not exceed maximum connections")
        if not validation_results["max_connections"]:
            issues.append("Maximum connections must be at least minimum connections and not exceed 100")
            
        return issues

    def is_configuration_complete(self) -> bool:
        """
        Check if database configuration is complete and valid.
        
        Returns:
            True if all required configuration is present and valid
        """
        validation_results = self.validate_connection_config()
        return all(validation_results.values())


class SchemaSettings(BaseSettings):
    """Database schema configuration."""

    # Pipeline zone schemas
    raw: str = Field(
        default="raw_data", description="RAW zone schema for raw external data"
    )

    staging: str = Field(
        default="staging", description="STAGING zone schema for processed data"
    )

    curated: str = Field(
        default="curated", description="CURATED zone schema for validated data"
    )

    # Raw data schemas (legacy)
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
            "games": "curated.games_complete",
            "odds": "curated.odds",
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


class MLflowSettings(BaseSettings):
    """MLflow configuration for experiment tracking and model management."""

    # MLflow Tracking Server Configuration
    tracking_uri: str = Field(
        default="http://localhost:5001",
        description="MLflow tracking server URI",
        env="MLFLOW_TRACKING_URI",
    )

    # Connection and retry settings
    max_retries: int = Field(
        default=3, ge=1, le=10, description="Maximum connection retry attempts"
    )

    retry_delay: float = Field(
        default=1.0, ge=0.1, le=10.0, description="Delay between retries in seconds"
    )

    connection_timeout: int = Field(
        default=30, ge=5, le=120, description="Connection timeout in seconds"
    )

    # Default experiment configuration
    default_experiment_name: str = Field(
        default="mlb_betting_experiments", description="Default experiment name"
    )

    # Artifact storage
    artifact_root: str | None = Field(
        default=None,
        description="MLflow artifact root directory",
        env="MLFLOW_DEFAULT_ARTIFACT_ROOT",
    )

    # Backend store (if different from tracking URI)
    backend_store_uri: str | None = Field(
        default=None,
        description="MLflow backend store URI (overrides tracking_uri for metadata)",
        env="MLFLOW_BACKEND_STORE_URI",
    )

    class Config:
        env_prefix = "MLFLOW_"
        case_sensitive = False
        use_enum_values = True
        extra = "allow"

    @computed_field
    @property
    def effective_tracking_uri(self) -> str:
        """Get effective tracking URI, preferring backend_store_uri if provided."""
        return self.backend_store_uri or self.tracking_uri


class MLPipelineSettings(BaseSettings):
    """ML Pipeline configuration settings."""

    # Feature Pipeline Settings
    feature_cache_ttl_seconds: int = Field(
        default=900, ge=60, le=3600, description="Feature cache TTL in seconds"
    )

    batch_processing_max_size: int = Field(
        default=50, ge=1, le=200, description="Maximum batch size for feature processing"
    )

    batch_processing_min_size: int = Field(
        default=5, ge=1, le=50, description="Minimum batch size for feature processing"
    )

    max_concurrent_extractions: int = Field(
        default=5, ge=1, le=20, description="Maximum concurrent feature extractions"
    )

    # Memory Management
    memory_threshold_mb: int = Field(
        default=2048, ge=512, le=8192, description="Memory threshold in MB before triggering cleanup"
    )

    memory_cleanup_trigger_mb: int = Field(
        default=500, ge=100, le=2048, description="Memory increase threshold to trigger cleanup"
    )

    # Model Loading
    model_loading_timeout_seconds: int = Field(
        default=30, ge=5, le=300, description="Timeout for model loading operations"
    )

    model_cache_size: int = Field(
        default=10, ge=1, le=50, description="Maximum number of models to keep in memory"
    )

    # Redis Feature Store
    redis_socket_timeout: float = Field(
        default=5.0, ge=1.0, le=30.0, description="Redis socket timeout in seconds"
    )

    redis_connection_pool_size: int = Field(
        default=20, ge=5, le=100, description="Redis connection pool size"
    )

    redis_max_retries: int = Field(
        default=3, ge=1, le=10, description="Maximum Redis connection retries"
    )

    redis_retry_delay_seconds: float = Field(
        default=1.0, ge=0.1, le=10.0, description="Initial retry delay in seconds"
    )

    # Prediction Service
    prediction_batch_size: int = Field(
        default=10, ge=1, le=100, description="Batch size for prediction processing"
    )

    prediction_cache_ttl_hours: int = Field(
        default=4, ge=1, le=24, description="Cache TTL for predictions in hours"
    )

    # Performance Targets
    api_response_target_ms: int = Field(
        default=100, ge=10, le=1000, description="Target API response time in milliseconds"
    )

    prediction_latency_target_ms: int = Field(
        default=500, ge=50, le=5000, description="Target prediction latency in milliseconds"
    )

    # Resource Monitoring Thresholds
    cpu_warning_threshold: float = Field(
        default=70.0, ge=10.0, le=100.0, description="CPU usage warning threshold percentage"
    )

    cpu_critical_threshold: float = Field(
        default=85.0, ge=10.0, le=100.0, description="CPU usage critical threshold percentage"
    )

    cpu_emergency_threshold: float = Field(
        default=95.0, ge=10.0, le=100.0, description="CPU usage emergency threshold percentage"
    )

    memory_warning_threshold: float = Field(
        default=75.0, ge=10.0, le=100.0, description="Memory usage warning threshold percentage"
    )

    memory_critical_threshold: float = Field(
        default=85.0, ge=10.0, le=100.0, description="Memory usage critical threshold percentage"
    )

    memory_emergency_threshold: float = Field(
        default=95.0, ge=10.0, le=100.0, description="Memory usage emergency threshold percentage"
    )

    disk_warning_threshold: float = Field(
        default=80.0, ge=10.0, le=100.0, description="Disk usage warning threshold percentage"
    )

    disk_critical_threshold: float = Field(
        default=90.0, ge=10.0, le=100.0, description="Disk usage critical threshold percentage"
    )

    disk_emergency_threshold: float = Field(
        default=95.0, ge=10.0, le=100.0, description="Disk usage emergency threshold percentage"
    )

    resource_monitoring_interval: int = Field(
        default=10, ge=5, le=300, description="Resource monitoring interval in seconds"
    )

    resource_alert_cooldown: int = Field(
        default=300, ge=60, le=3600, description="Resource alert cooldown period in seconds"
    )

    class Config:
        env_prefix = "ML_"
        case_sensitive = False
        use_enum_values = True
        extra = "allow"


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


class PipelineSettings(BaseSettings):
    """Data pipeline configuration."""

    # Zone enablement flags
    class ZoneSettings(BaseModel):
        raw_enabled: bool = Field(
            default=True, description="Enable RAW zone processing"
        )
        staging_enabled: bool = Field(
            default=True, description="Enable STAGING zone processing"
        )
        curated_enabled: bool = Field(
            default=True, description="Enable CURATED zone processing"
        )

    zones: ZoneSettings = Field(default_factory=ZoneSettings)

    # Pipeline behavior settings
    validation_enabled: bool = Field(
        default=True, description="Enable pipeline validation"
    )
    auto_promotion: bool = Field(
        default=True, description="Enable automatic zone promotion"
    )
    quality_threshold: float = Field(
        default=0.7, ge=0.0, le=1.0, description="Minimum quality threshold"
    )

    # Processing settings
    max_concurrent_batches: int = Field(
        default=3, ge=1, le=10, description="Maximum concurrent batch processing"
    )
    retry_attempts: int = Field(
        default=3,
        ge=0,
        le=10,
        description="Number of retry attempts for failed records",
    )
    batch_timeout_seconds: int = Field(
        default=300, ge=30, le=1800, description="Batch processing timeout"
    )


class MonitoringSettings(BaseSettings):
    """Monitoring and metrics configuration."""

    # Prometheus metrics configuration
    enable_prometheus: bool = Field(
        default=True,
        description="Enable Prometheus metrics",
        env="MONITORING_PROMETHEUS_ENABLED",
    )

    prometheus_port: int = Field(
        default=8000,
        ge=1024,
        le=65535,
        description="Prometheus metrics port",
        env="MONITORING_PROMETHEUS_PORT",
    )

    # Pipeline metrics bucket configuration
    pipeline_duration_buckets: list[float] = Field(
        default=[1, 5, 10, 30, 60, 120, 300, 600],
        description="Histogram buckets for pipeline duration metrics",
    )

    pipeline_stage_duration_buckets: list[float] = Field(
        default=[0.5, 1, 2, 5, 10, 30, 60, 120],
        description="Histogram buckets for pipeline stage duration metrics",
    )

    # Database query metrics buckets
    database_query_duration_buckets: list[float] = Field(
        default=[0.01, 0.05, 0.1, 0.5, 1, 2, 5],
        description="Histogram buckets for database query duration metrics",
    )

    # API call metrics buckets
    api_call_duration_buckets: list[float] = Field(
        default=[0.1, 0.5, 1, 2, 5, 10, 30],
        description="Histogram buckets for API call duration metrics",
    )

    # OpenTelemetry configuration
    enable_opentelemetry: bool = Field(
        default=False,
        description="Enable OpenTelemetry tracing",
        env="MONITORING_OTEL_ENABLED",
    )

    otlp_endpoint: str | None = Field(
        default=None, description="OTLP endpoint URL", env="MONITORING_OTLP_ENDPOINT"
    )

    # Sampling configuration
    enable_metrics_sampling: bool = Field(
        default=True, description="Enable metrics sampling for high-volume operations"
    )

    metrics_sample_rate: float = Field(
        default=0.1,
        ge=0.01,
        le=1.0,
        description="Sample rate for high-volume metrics (0.01-1.0)",
    )

    # Health check configuration
    health_check_enabled: bool = Field(
        default=True, description="Enable health check endpoint"
    )

    health_check_port: int = Field(
        default=8080, ge=1024, le=65535, description="Health check endpoint port"
    )

    class Config:
        env_prefix = ""
        case_sensitive = False
        use_enum_values = True
        extra = "allow"


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


class SecuritySettings(BaseSettings):
    """Security configuration for API endpoints and authentication."""

    # Dashboard API Security
    dashboard_api_key: str | None = Field(
        default=None,
        description="API key for dashboard break-glass endpoints",
        env="DASHBOARD_API_KEY",
    )

    # Security features
    enable_authentication: bool = Field(
        default=True,
        description="Enable authentication for sensitive endpoints",
        env="ENABLE_AUTH",
    )

    enable_rate_limiting: bool = Field(
        default=True,
        description="Enable rate limiting for API endpoints",
        env="ENABLE_RATE_LIMIT",
    )

    # Rate limiting settings for break-glass endpoints
    break_glass_rate_limit: int = Field(
        default=5,
        ge=1,
        le=100,
        description="Max break-glass requests per hour",
        env="BREAK_GLASS_RATE_LIMIT",
    )

    # Session security
    session_timeout_minutes: int = Field(
        default=60,
        ge=5,
        le=480,
        description="Session timeout in minutes",
        env="SESSION_TIMEOUT",
    )

    # IP whitelisting for break-glass endpoints
    break_glass_ip_whitelist: list[str] = Field(
        default_factory=lambda: ["127.0.0.1", "::1"],
        description="IP addresses allowed to access break-glass endpoints",
        env="BREAK_GLASS_IP_WHITELIST",
    )

    enable_ip_whitelisting: bool = Field(
        default=False,
        description="Enable IP whitelisting for break-glass endpoints",
        env="ENABLE_IP_WHITELIST",
    )

    # Redis configuration for production rate limiting
    redis_url: str | None = Field(
        default=None,
        description="Redis URL for production rate limiting",
        env="REDIS_URL",
    )

    enable_redis_rate_limiting: bool = Field(
        default=False,
        description="Enable Redis-based rate limiting",
        env="ENABLE_REDIS_RATE_LIMITING",
    )

    class Config:
        env_prefix = ""
        case_sensitive = False
        use_enum_values = True
        extra = "allow"


class DashboardSettings(BaseSettings):
    """Dashboard configuration for monitoring interface."""

    # Update intervals
    system_health_update_interval: int = Field(
        default=10,
        ge=5,
        le=300,
        description="System health update interval in seconds",
        env="DASHBOARD_UPDATE_INTERVAL",
    )

    error_recovery_delay: int = Field(
        default=30,
        ge=5,
        le=300,
        description="Error recovery delay in seconds",
        env="DASHBOARD_ERROR_DELAY",
    )

    websocket_error_delay: int = Field(
        default=15,
        ge=5,
        le=120,
        description="WebSocket error recovery delay in seconds",
        env="DASHBOARD_WS_ERROR_DELAY",
    )

    # WebSocket settings
    max_reconnect_attempts: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Maximum WebSocket reconnection attempts",
        env="DASHBOARD_MAX_RECONNECTS",
    )

    reconnect_interval: int = Field(
        default=5000,
        ge=1000,
        le=30000,
        description="WebSocket reconnection interval in milliseconds",
        env="DASHBOARD_RECONNECT_INTERVAL",
    )

    # Display settings
    recent_pipelines_limit: int = Field(
        default=5,
        ge=1,
        le=50,
        description="Number of recent pipelines to display",
        env="DASHBOARD_RECENT_LIMIT",
    )

    notification_timeout: int = Field(
        default=5000,
        ge=1000,
        le=30000,
        description="Notification display timeout in milliseconds",
        env="DASHBOARD_NOTIFICATION_TIMEOUT",
    )

    class Config:
        env_prefix = ""
        case_sensitive = False
        use_enum_values = True
        extra = "allow"


# ML System Configuration Classes
class MLflowConfig(BaseModel):
    """MLflow Model Registry Configuration"""
    tracking_uri: str = Field(default="http://localhost:5001", description="MLflow tracking server URI")
    experiment_name: str = Field(default="mlb_betting_models", description="Default experiment name")
    artifact_store: str = Field(default="s3://mlb-betting-artifacts", description="Artifact storage location")
    connection_timeout: int = Field(default=30, description="Connection timeout in seconds")
    retry_attempts: int = Field(default=3, description="Number of retry attempts")
    api_key: str = Field(default="${MLFLOW_API_KEY}", description="API key for authentication")


class RedisConfig(BaseModel):
    """Redis Feature Store Configuration"""
    host: str = Field(default="localhost", description="Redis server host")
    port: int = Field(default=6379, description="Redis server port")
    password: str = Field(default="${REDIS_PASSWORD}", description="Redis password")
    ssl_enabled: bool = Field(default=False, description="Enable SSL/TLS encryption")
    ssl_cert_path: str = Field(default="", description="Path to SSL certificate")
    ssl_key_path: str = Field(default="", description="Path to SSL private key")
    ssl_ca_path: str = Field(default="", description="Path to SSL CA certificate")
    connection_pool_size: int = Field(default=20, description="Connection pool size")
    socket_timeout: float = Field(default=5.0, description="Socket timeout in seconds")
    max_retries: int = Field(default=3, description="Maximum connection retries")
    retry_delay_seconds: float = Field(default=1.0, description="Initial retry delay in seconds")
    database: int = Field(default=0, description="Redis database number")


class ModelThresholdsConfig(BaseModel):
    """Model Performance Thresholds Configuration"""
    # Staging promotion thresholds
    staging_min_accuracy: float = Field(default=0.55, description="Minimum accuracy for staging")
    staging_min_roc_auc: float = Field(default=0.60, description="Minimum ROC AUC for staging")
    staging_min_precision: float = Field(default=0.50, description="Minimum precision for staging")
    staging_min_recall: float = Field(default=0.50, description="Minimum recall for staging")
    staging_min_training_samples: int = Field(default=50, description="Minimum training samples")
    
    # Production promotion thresholds
    production_min_accuracy: float = Field(default=0.60, description="Minimum accuracy for production")
    production_min_roc_auc: float = Field(default=0.65, description="Minimum ROC AUC for production")
    production_min_f1_score: float = Field(default=0.58, description="Minimum F1 score for production")
    production_min_roi: float = Field(default=0.05, description="Minimum ROI for production")
    production_evaluation_days: int = Field(default=7, description="Days of staging evaluation")


class PerformanceConfig(BaseModel):
    """Performance and Resource Management Configuration"""
    memory_limit_mb: int = Field(default=2048, description="Memory threshold for cleanup")
    batch_size_limit: int = Field(default=50, description="Maximum batch processing size")
    connection_pool_size: int = Field(default=20, description="Database connection pool size")
    feature_cache_ttl_seconds: int = Field(default=900, description="Feature cache TTL")
    model_cache_size: int = Field(default=10, description="Maximum models in memory")
    model_loading_timeout_seconds: int = Field(default=30, description="Model loading timeout")
    memory_cleanup_trigger_mb: int = Field(default=500, description="Memory cleanup trigger")
    max_concurrent_extractions: int = Field(default=5, description="Max concurrent extractions")
    dataframe_chunk_size: int = Field(default=1000, description="DataFrame chunk size")


class RetrainingConfig(BaseModel):
    """Automated Retraining Configuration"""
    monitoring_interval_minutes: int = Field(default=30, description="Performance monitoring interval")
    staging_evaluation_delay_seconds: int = Field(default=300, description="Staging evaluation delay")
    performance_check_interval_hours: int = Field(default=1, description="Performance check interval")
    performance_degradation_threshold: float = Field(default=0.05, description="Degradation threshold")
    data_drift_threshold: float = Field(default=0.1, description="Data drift threshold")
    auto_retraining_enabled: bool = Field(default=True, description="Enable automatic retraining")
    max_concurrent_retraining_jobs: int = Field(default=2, description="Max concurrent jobs")
    default_schedule_cron: str = Field(default="0 2 * * *", description="Default schedule")
    default_sliding_window_days: int = Field(default=90, description="Default training window")
    default_min_samples: int = Field(default=100, description="Default minimum samples")


class MLSecurityConfig(BaseModel):
    """ML System Security Configuration"""
    enable_api_authentication: bool = Field(default=True, description="Enable API authentication")
    api_key_header: str = Field(default="X-ML-API-Key", description="API key header name")
    enable_audit_logging: bool = Field(default=True, description="Enable audit logging")
    audit_log_level: str = Field(default="INFO", description="Audit log level")
    sensitive_data_masking: bool = Field(default=True, description="Mask sensitive data in logs")
    model_encryption_enabled: bool = Field(default=False, description="Enable model encryption")


class MLMonitoringConfig(BaseModel):
    """ML System Monitoring Configuration"""
    enable_prometheus_metrics: bool = Field(default=True, description="Enable Prometheus metrics")
    metrics_port: int = Field(default=9090, description="Metrics endpoint port")
    enable_model_drift_detection: bool = Field(default=True, description="Enable drift detection")
    drift_detection_interval_hours: int = Field(default=6, description="Drift detection interval")
    enable_performance_alerts: bool = Field(default=True, description="Enable performance alerts")
    alert_webhook_url: str = Field(default="${ML_ALERT_WEBHOOK}", description="Alert webhook URL")
    collect_prediction_metrics: bool = Field(default=True, description="Collect prediction metrics")
    collect_feature_metrics: bool = Field(default=True, description="Collect feature metrics")
    collect_training_metrics: bool = Field(default=True, description="Collect training metrics")
    metrics_retention_days: int = Field(default=30, description="Metrics retention period")


class MLSystemConfig(BaseModel):
    """Complete ML System Configuration"""
    mlflow: MLflowConfig = Field(default_factory=MLflowConfig)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    model_thresholds: ModelThresholdsConfig = Field(default_factory=ModelThresholdsConfig)
    performance: PerformanceConfig = Field(default_factory=PerformanceConfig)
    retraining: RetrainingConfig = Field(default_factory=RetrainingConfig)
    security: MLSecurityConfig = Field(default_factory=MLSecurityConfig)
    monitoring: MLMonitoringConfig = Field(default_factory=MLMonitoringConfig)


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
    pipeline: PipelineSettings = Field(default_factory=PipelineSettings)
    security: SecuritySettings = Field(default_factory=SecuritySettings)
    dashboard: DashboardSettings = Field(default_factory=DashboardSettings)
    monitoring: MonitoringSettings = Field(default_factory=MonitoringSettings)
    mlflow: MLflowSettings = Field(default_factory=MLflowSettings)
    ml_pipeline: MLPipelineSettings = Field(default_factory=MLPipelineSettings)
    ml: MLSystemConfig = Field(default_factory=MLSystemConfig)
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
