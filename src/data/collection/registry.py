#!/usr/bin/env python3
"""
Centralized Collector Registry

Provides a unified registration system for all data collectors to eliminate
duplicate registrations and improve initialization performance.

This module implements:
- Singleton registry pattern to prevent duplicate registrations
- Source alias mapping for backward compatibility
- Collector instance management and caching
- Registration tracking and validation
"""

from dataclasses import dataclass, field
from typing import Optional

import structlog

from .base import BaseCollector, CollectorFactory, DataSource

logger = structlog.get_logger(__name__)


@dataclass
class RegistrationInfo:
    """Information about a registered collector."""
    collector_class: type[BaseCollector]
    source: DataSource
    registered_at: str
    is_primary: bool = True
    aliases: set[str] = field(default_factory=set)


class CollectorRegistry:
    """
    Centralized collector registration system.

    Implements singleton pattern to ensure collectors are only registered once
    and provides alias mapping for backward compatibility.
    """

    _instance: Optional['CollectorRegistry'] = None
    _initialized: bool = False

    def __new__(cls) -> 'CollectorRegistry':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self._registered_collectors: dict[DataSource, RegistrationInfo] = {}
            self._source_aliases: dict[str, DataSource] = {}
            self._registration_history: set[str] = set()
            self._instance_cache: dict[str, BaseCollector] = {}
            self._setup_source_aliases()
            CollectorRegistry._initialized = True
            logger.info("Centralized collector registry initialized")

    def _setup_source_aliases(self) -> None:
        """Setup source aliases for backward compatibility."""
        # Map alternative source names to primary sources
        alias_mappings = {
            # SBD aliases
            "sports_betting_dime": DataSource.SBD,

            # SBR aliases (only map the alias, not the primary)
            "sbr": DataSource.SPORTS_BOOK_REVIEW,

            # Other aliases can be added here
        }

        for alias, primary_source in alias_mappings.items():
            self._source_aliases[alias] = primary_source
            logger.debug("Source alias registered", alias=alias, primary_source=primary_source.value)

    def register_collector(
        self,
        source: DataSource,
        collector_class: type[BaseCollector],
        allow_override: bool = False
    ) -> bool:
        """
        Register a collector for a data source.

        Args:
            source: Data source enum
            collector_class: Collector class to register
            allow_override: Whether to allow overriding existing registrations

        Returns:
            True if registered successfully, False if already registered
        """
        registration_key = f"{source.value}:{collector_class.__name__}"

        # Check for duplicate registration
        if registration_key in self._registration_history and not allow_override:
            logger.debug(
                "Collector already registered, skipping duplicate",
                source=source.value,
                collector=collector_class.__name__
            )
            return False

        # Also check if the same collector class is already registered for this source
        if source in self._registered_collectors and not allow_override:
            existing = self._registered_collectors[source]
            if existing.collector_class == collector_class:
                logger.debug(
                    "Same collector already registered for source",
                    source=source.value,
                    collector=collector_class.__name__
                )
                return False

        # Check for conflicting registration
        if source in self._registered_collectors and not allow_override:
            existing = self._registered_collectors[source]
            if existing.collector_class != collector_class:
                logger.warning(
                    "Conflicting collector registration attempted",
                    source=source.value,
                    existing=existing.collector_class.__name__,
                    attempted=collector_class.__name__
                )
                return False
            # Same class already registered
            return False

        # Register the collector
        from datetime import datetime
        registration_info = RegistrationInfo(
            collector_class=collector_class,
            source=source,
            registered_at=datetime.now().isoformat(),
            is_primary=True
        )

        self._registered_collectors[source] = registration_info
        self._registration_history.add(registration_key)

        # Also register with the existing CollectorFactory for compatibility
        # But only if not already registered to prevent duplicate logs
        if hasattr(CollectorFactory, '_collectors') and source not in CollectorFactory._collectors:
            CollectorFactory.register_collector(source, collector_class)

        logger.info(
            "Collector registered",
            collector=collector_class.__name__,
            source=source.value
        )

        return True

    def register_all_collectors(self) -> None:
        """Register all available collectors in the correct order."""
        # Import collectors dynamically to avoid circular imports
        try:
            # Primary collectors (refactored, production-ready)
            from .collectors import OddsAPICollector
            from .consolidated_action_network_collector import ActionNetworkCollector
            from .mlb_stats_api_collector import MLBStatsAPICollector
            from .sbd_unified_collector_api import SBDUnifiedCollectorAPI
            from .sbr_unified_collector import SBRUnifiedCollector
            from .vsin_unified_collector import VSINUnifiedCollector

            # Register primary collectors
            self.register_collector(DataSource.VSIN, VSINUnifiedCollector)
            self.register_collector(DataSource.SBD, SBDUnifiedCollectorAPI)
            self.register_collector(DataSource.ACTION_NETWORK, ActionNetworkCollector)
            self.register_collector(DataSource.SPORTS_BOOK_REVIEW, SBRUnifiedCollector)
            self.register_collector(DataSource.MLB_STATS_API, MLBStatsAPICollector)
            self.register_collector(DataSource.ODDS_API, OddsAPICollector)

            # Note: Deprecated SPORTS_BOOK_REVIEW_DEPRECATED enum removed
            # All SBR functionality now consolidated under SPORTS_BOOK_REVIEW

            logger.info(
                "All collectors registered successfully",
                total_collectors=len(self._registered_collectors),
                primary_sources=len([r for r in self._registered_collectors.values() if r.is_primary])
            )

        except ImportError as e:
            logger.warning(
                "Some collectors could not be imported",
                error=str(e),
                registered_count=len(self._registered_collectors)
            )

    def get_collector_class(self, source: str | DataSource) -> type[BaseCollector] | None:
        """
        Get collector class for a source, handling aliases.

        Args:
            source: Source name (string) or DataSource enum

        Returns:
            Collector class if found, None otherwise
        """
        # Convert string to DataSource if needed
        if isinstance(source, str):
            # Check aliases first
            if source in self._source_aliases:
                source = self._source_aliases[source]
            else:
                try:
                    source = DataSource(source)
                except ValueError:
                    logger.warning("Unknown data source", source=source)
                    return None

        registration_info = self._registered_collectors.get(source)
        return registration_info.collector_class if registration_info else None

    def get_collector_instance(
        self,
        source: str | DataSource,
        config: object | None = None,
        force_new: bool = False
    ) -> BaseCollector | None:
        """
        Get or create collector instance with caching.

        Args:
            source: Source name or DataSource enum
            config: Collector configuration
            force_new: Force creation of new instance

        Returns:
            Collector instance if available
        """
        # Resolve source and get collector class
        collector_class = self.get_collector_class(source)
        if not collector_class:
            return None

        # Create cache key
        source_key = source.value if isinstance(source, DataSource) else source
        cache_key = f"{source_key}:{collector_class.__name__}"

        # Return cached instance if available and not forcing new
        if not force_new and cache_key in self._instance_cache:
            logger.debug("Returning cached collector instance", source=source_key)
            return self._instance_cache[cache_key]

        # Create new instance
        try:
            if config:
                instance = collector_class(config)
            else:
                # Create default config if none provided
                from .base import CollectorConfig
                default_config = CollectorConfig(source=source)
                instance = collector_class(default_config)

            # Cache the instance
            self._instance_cache[cache_key] = instance

            logger.debug(
                "Created new collector instance",
                source=source_key,
                collector=collector_class.__name__,
                cached=True
            )

            return instance

        except Exception as e:
            logger.error(
                "Failed to create collector instance",
                source=source_key,
                collector=collector_class.__name__,
                error=str(e)
            )
            return None

    def get_registered_sources(self) -> dict[str, dict[str, any]]:
        """Get information about all registered sources."""
        sources_info = {}

        for source, info in self._registered_collectors.items():
            sources_info[source.value] = {
                "collector_class": info.collector_class.__name__,
                "registered_at": info.registered_at,
                "is_primary": info.is_primary,
                "aliases": list(info.aliases)
            }

        # Add alias information
        aliases_info = {}
        for alias, primary_source in self._source_aliases.items():
            aliases_info[alias] = primary_source.value

        return {
            "sources": sources_info,
            "aliases": aliases_info,
            "total_registrations": len(self._registered_collectors),
            "total_aliases": len(self._source_aliases)
        }

    def clear_cache(self) -> None:
        """Clear the instance cache."""
        cleared_count = len(self._instance_cache)
        self._instance_cache.clear()
        logger.info("Collector instance cache cleared", cleared_instances=cleared_count)

    def reset_registry(self) -> None:
        """Reset the entire registry (primarily for testing)."""
        self._registered_collectors.clear()
        self._registration_history.clear()
        self._instance_cache.clear()
        self._setup_source_aliases()
        logger.warning("Collector registry reset")


# Global registry instance
_registry = CollectorRegistry()


# Convenience functions for external use
def register_collector(source: DataSource, collector_class: type[BaseCollector]) -> bool:
    """Register a collector globally."""
    return _registry.register_collector(source, collector_class)


def get_collector_class(source: str | DataSource) -> type[BaseCollector] | None:
    """Get collector class for a source."""
    return _registry.get_collector_class(source)


def get_collector_instance(
    source: str | DataSource,
    config: object | None = None
) -> BaseCollector | None:
    """Get or create collector instance."""
    return _registry.get_collector_instance(source, config)


def initialize_all_collectors() -> None:
    """Initialize all available collectors."""
    _registry.register_all_collectors()


def get_registry_status() -> dict[str, any]:
    """Get registry status information."""
    return _registry.get_registered_sources()


def clear_collector_cache() -> None:
    """Clear collector instance cache."""
    _registry.clear_cache()

