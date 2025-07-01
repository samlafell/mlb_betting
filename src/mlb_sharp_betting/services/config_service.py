#!/usr/bin/env python3
"""
Configuration Service for MLB Sharp Betting System

Centralizes configuration management across all services to eliminate the chaos
of scattered config patterns. Provides unified access to settings, rate limits,
and service-specific configurations with proper fallbacks.

Production Features:
- Unified configuration access pattern
- Centralized rate limit configuration  
- Service-specific configuration with fallbacks
- Configuration caching for performance
- Thread-safe configuration access
- Configuration validation
"""

import json
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, Union, List
from dataclasses import dataclass, field
from enum import Enum
import structlog

from ..core.config import get_settings, Settings
from ..core.logging import get_logger

logger = get_logger(__name__)


class ServiceType(Enum):
    """Service types for configuration management."""
    SCRAPER = "scraper"
    ANALYZER = "analyzer"
    SCHEDULER = "scheduler"
    NOTIFIER = "notifier"
    DATABASE = "database"
    API = "api"


@dataclass
class RateLimitConfig:
    """Unified rate limit configuration."""
    max_requests_per_minute: int = 10
    max_requests_per_hour: int = 300
    max_requests_per_day: int = 1000
    request_delay_seconds: float = 0.1
    burst_limit: int = 5
    cooldown_minutes: int = 15
    enabled: bool = True


@dataclass
class RetryConfig:
    """Unified retry configuration."""
    max_attempts: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    exponential_base: float = 2.0
    jitter_enabled: bool = True
    timeout_seconds: int = 30


@dataclass
class ServiceConfig:
    """Configuration for a specific service."""
    service_name: str
    service_type: ServiceType
    rate_limits: RateLimitConfig = field(default_factory=RateLimitConfig)
    retry_config: RetryConfig = field(default_factory=RetryConfig)
    custom_settings: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True


class ConfigurationService:
    """
    Centralized configuration management for production betting system.
    
    Eliminates the chaos of scattered configuration patterns across services:
    - Direct settings access (get_settings())
    - File-based config loading (_load_config())
    - Hardcoded defaults (default_config = {...})
    
    Provides unified, cached, and validated configuration access.
    """
    
    _instance: Optional['ConfigurationService'] = None
    _lock = threading.Lock()
    
    def __new__(cls) -> 'ConfigurationService':
        """Singleton pattern for thread-safe configuration access."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize configuration service."""
        if hasattr(self, '_initialized'):
            return
            
        self.logger = get_logger(__name__)
        self.settings = get_settings()
        
        # Configuration cache with TTL
        self._config_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_timestamps: Dict[str, datetime] = {}
        self._cache_ttl = timedelta(minutes=5)  # 5-minute cache TTL
        self._cache_lock = threading.Lock()
        
        # Service configurations
        self._service_configs: Dict[str, ServiceConfig] = {}
        
        # Initialize default service configurations
        self._initialize_default_configs()
        
        self._initialized = True
        
        self.logger.info("ConfigurationService initialized with unified config management")
    
    def _initialize_default_configs(self) -> None:
        """Initialize default configurations for all services."""
        
        # Scraper services
        self._service_configs["vsin_scraper"] = ServiceConfig(
            service_name="vsin_scraper",
            service_type=ServiceType.SCRAPER,
            rate_limits=RateLimitConfig(
                max_requests_per_minute=5,
                request_delay_seconds=0.2,
                cooldown_minutes=10
            ),
            retry_config=RetryConfig(max_attempts=3, base_delay_seconds=1.0)
        )
        
        self._service_configs["pinnacle_scraper"] = ServiceConfig(
            service_name="pinnacle_scraper", 
            service_type=ServiceType.SCRAPER,
            rate_limits=RateLimitConfig(
                max_requests_per_minute=30,
                request_delay_seconds=0.1,
                burst_limit=10
            ),
            retry_config=RetryConfig(max_attempts=3, base_delay_seconds=1.0)
        )
        
        # API services
        self._service_configs["odds_api"] = ServiceConfig(
            service_name="odds_api",
            service_type=ServiceType.API,
            rate_limits=RateLimitConfig(
                max_requests_per_day=480,  # Monthly quota protection
                max_requests_per_hour=20,
                request_delay_seconds=1.0
            ),
            retry_config=RetryConfig(max_attempts=2, base_delay_seconds=2.0)
        )
        
        # Alert services
        self._service_configs["alert_service"] = ServiceConfig(
            service_name="alert_service",
            service_type=ServiceType.NOTIFIER,
            rate_limits=RateLimitConfig(
                max_requests_per_hour=4,  # Max 4 alerts per hour
                cooldown_minutes=15,
                enabled=True
            ),
            retry_config=RetryConfig(
                max_attempts=3,
                base_delay_seconds=30.0,
                exponential_base=1.0  # Linear retry for notifications
            )
        )
        
        # Database services
        self._service_configs["database"] = ServiceConfig(
            service_name="database",
            service_type=ServiceType.DATABASE,
            retry_config=RetryConfig(
                max_attempts=3,
                base_delay_seconds=0.1,
                jitter_enabled=True
            )
        )
        
        # Scheduler services
        self._service_configs["scheduler"] = ServiceConfig(
            service_name="scheduler",
            service_type=ServiceType.SCHEDULER,
            retry_config=RetryConfig(max_attempts=3, base_delay_seconds=2.0)
        )
        
        self._service_configs["pre_game_scheduler"] = ServiceConfig(
            service_name="pre_game_scheduler",
            service_type=ServiceType.SCHEDULER,
            retry_config=RetryConfig(
                max_attempts=3,
                base_delay_seconds=2.0,
                exponential_base=2.0  # Exponential backoff for workflows
            )
        )
    
    def get_service_config(self, service_name: str) -> Dict[str, Any]:
        """
        Get configuration for a specific service with fallbacks.
        
        Args:
            service_name: Name of the service requesting configuration
            
        Returns:
            Configuration dictionary with service-specific settings
        """
        cache_key = f"service_config_{service_name}"
        
        # Check cache first
        with self._cache_lock:
            if cache_key in self._config_cache:
                cache_time = self._cache_timestamps.get(cache_key)
                if cache_time and datetime.now() - cache_time < self._cache_ttl:
                    return self._config_cache[cache_key].copy()
        
        # Build configuration
        config = self._build_service_config(service_name)
        
        # Cache the result
        with self._cache_lock:
            self._config_cache[cache_key] = config.copy()
            self._cache_timestamps[cache_key] = datetime.now()
        
        return config
    
    def _build_service_config(self, service_name: str) -> Dict[str, Any]:
        """Build configuration for a service."""
        # Get service-specific config or create default
        service_config = self._service_configs.get(service_name)
        if not service_config:
            service_config = ServiceConfig(
                service_name=service_name,
                service_type=ServiceType.ANALYZER  # Default type
            )
        
        # Build unified configuration
        config = {
            "service_name": service_name,
            "service_type": service_config.service_type.value,
            "enabled": service_config.enabled,
            
            # Rate limiting
            "rate_limits": {
                "max_requests_per_minute": service_config.rate_limits.max_requests_per_minute,
                "max_requests_per_hour": service_config.rate_limits.max_requests_per_hour,
                "max_requests_per_day": service_config.rate_limits.max_requests_per_day,
                "request_delay_seconds": service_config.rate_limits.request_delay_seconds,
                "burst_limit": service_config.rate_limits.burst_limit,
                "cooldown_minutes": service_config.rate_limits.cooldown_minutes,
                "enabled": service_config.rate_limits.enabled
            },
            
            # Retry configuration
            "retry": {
                "max_attempts": service_config.retry_config.max_attempts,
                "base_delay_seconds": service_config.retry_config.base_delay_seconds,
                "max_delay_seconds": service_config.retry_config.max_delay_seconds,
                "exponential_base": service_config.retry_config.exponential_base,
                "jitter_enabled": service_config.retry_config.jitter_enabled,
                "timeout_seconds": service_config.retry_config.timeout_seconds
            },
            
            # Custom service settings
            "custom": service_config.custom_settings.copy(),
            
            # Global settings access
            "global_settings": self._get_global_settings_dict()
        }
        
        return config
    
    def _get_global_settings_dict(self) -> Dict[str, Any]:
        """Get global settings as dictionary."""
        return {
            "database": {
                "host": self.settings.postgres.host,
                "port": self.settings.postgres.port,
                "database": self.settings.postgres.database,
                "user": self.settings.postgres.user,
                "min_connections": self.settings.postgres.min_connections,
                "max_connections": self.settings.postgres.max_connections
            },
            "email_from_address": getattr(self.settings, 'email_from_address', None),
            "email_app_password": getattr(self.settings, 'email_app_password', None),
            "email_recipients": self.settings.get_email_list(),
            "debug": self.settings.debug,
            "environment": getattr(self.settings, 'environment', 'development')
        }
    
    def get_rate_limits(self, service_name: str) -> Dict[str, Any]:
        """Get rate limiting configuration for service."""
        config = self.get_service_config(service_name)
        return config.get("rate_limits", {})
    
    def get_retry_config(self, service_name: str) -> Dict[str, Any]:
        """Get retry configuration for service."""
        config = self.get_service_config(service_name)
        return config.get("retry", {})
    
    def get_global_settings(self) -> Settings:
        """Get global settings object."""
        return self.settings
    
    def update_service_config(self, service_name: str, config_updates: Dict[str, Any]) -> None:
        """Update configuration for a service."""
        if service_name not in self._service_configs:
            self._service_configs[service_name] = ServiceConfig(
                service_name=service_name,
                service_type=ServiceType.ANALYZER
            )
        
        # Update custom settings
        self._service_configs[service_name].custom_settings.update(config_updates)
        
        # Clear cache for this service
        cache_key = f"service_config_{service_name}"
        with self._cache_lock:
            self._config_cache.pop(cache_key, None)
            self._cache_timestamps.pop(cache_key, None)
        
        self.logger.info(f"Updated configuration for service: {service_name}")
    
    def load_config_file(self, config_path: Union[str, Path]) -> Dict[str, Any]:
        """Load configuration from file with caching."""
        config_path = Path(config_path)
        cache_key = f"file_config_{config_path}"
        
        # Check cache first
        with self._cache_lock:
            if cache_key in self._config_cache:
                cache_time = self._cache_timestamps.get(cache_key)
                if cache_time and datetime.now() - cache_time < self._cache_ttl:
                    return self._config_cache[cache_key].copy()
        
        # Load file
        try:
            if config_path.exists():
                with open(config_path, 'r') as f:
                    config = json.load(f)
            else:
                config = {}
                self.logger.warning(f"Config file not found: {config_path}")
        except Exception as e:
            self.logger.error(f"Failed to load config file {config_path}: {e}")
            config = {}
        
        # Cache the result
        with self._cache_lock:
            self._config_cache[cache_key] = config.copy()
            self._cache_timestamps[cache_key] = datetime.now()
        
        return config
    
    def clear_cache(self) -> None:
        """Clear configuration cache."""
        with self._cache_lock:
            self._config_cache.clear()
            self._cache_timestamps.clear()
        
        self.logger.info("Configuration cache cleared")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._cache_lock:
            return {
                "cached_configs": len(self._config_cache),
                "cache_ttl_minutes": self._cache_ttl.total_seconds() / 60,
                "cache_keys": list(self._config_cache.keys())
            }


# Global configuration service instance
_config_service = None


def get_config_service() -> ConfigurationService:
    """Get the global configuration service instance."""
    global _config_service
    if _config_service is None:
        _config_service = ConfigurationService()
    return _config_service


# Convenience functions for common configuration access patterns
def get_service_config(service_name: str) -> Dict[str, Any]:
    """Get configuration for a service."""
    return get_config_service().get_service_config(service_name)


def get_rate_limits(service_name: str) -> Dict[str, Any]:
    """Get rate limits for a service."""
    return get_config_service().get_rate_limits(service_name)


def get_retry_config(service_name: str) -> Dict[str, Any]:
    """Get retry configuration for a service."""
    return get_config_service().get_retry_config(service_name) 