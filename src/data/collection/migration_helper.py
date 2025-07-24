#!/usr/bin/env python3
"""
Migration Helper for Data Collection Refactoring

This module provides utilities to help migrate from the old collector interfaces
to the new standardized BaseCollector pattern with proper Pydantic models.

Usage:
    from src.data.collection.migration_helper import create_collector_config, migrate_collector_call
    
    # Old pattern:
    collector = SBDUnifiedCollectorAPI()
    data = collector.collect_raw_data("mlb")
    
    # New pattern:
    config = create_collector_config(DataSource.SBD, base_url="https://www.sportsbettingdime.com")
    collector = CollectorFactory.create_collector(config)
    request = CollectionRequest(source=DataSource.SBD, sport="mlb")
    data = await collector.collect_data(request)
"""

import warnings
from datetime import datetime
from typing import Any, Dict, Optional

from .base import (
    BaseCollector,
    CollectorConfig,
    CollectionRequest,
    CollectorFactory,
    DataSource
)


def create_collector_config(
    source: DataSource,
    base_url: Optional[str] = None,
    api_key: Optional[str] = None,
    rate_limit_per_minute: int = 60,
    timeout_seconds: int = 30,
    **params
) -> CollectorConfig:
    """
    Create a standardized CollectorConfig for any data source.
    
    Args:
        source: DataSource enum value
        base_url: Base URL for the collector
        api_key: API key if required
        rate_limit_per_minute: Rate limiting setting
        timeout_seconds: Request timeout
        **params: Additional collector-specific parameters
        
    Returns:
        Properly configured CollectorConfig instance
    """
    
    # Set default URLs for known sources
    default_urls = {
        DataSource.SBD: "https://www.sportsbettingdime.com",
        DataSource.SPORTS_BETTING_DIME: "https://www.sportsbettingdime.com",
        DataSource.VSIN: "https://data.vsin.com",
        DataSource.ACTION_NETWORK: "https://api.actionnetwork.com",
    }
    
    # Set source-specific defaults
    source_defaults = {
        DataSource.SBD: {
            "rate_limit_per_minute": 60,
            "timeout_seconds": 30,
            "params": {"api_path": "/wp-json/adpt/v1/mlb-odds"}
        },
        DataSource.SPORTS_BETTING_DIME: {
            "rate_limit_per_minute": 60,
            "timeout_seconds": 30,
            "params": {"api_path": "/wp-json/adpt/v1/mlb-odds"}
        },
        DataSource.VSIN: {
            "rate_limit_per_minute": 30,  # VSIN has stricter limits
            "timeout_seconds": 45
        },
        DataSource.ACTION_NETWORK: {
            "rate_limit_per_minute": 120,
            "timeout_seconds": 30
        }
    }
    
    # Apply defaults
    defaults = source_defaults.get(source, {})
    final_base_url = base_url or default_urls.get(source)
    final_rate_limit = rate_limit_per_minute or defaults.get("rate_limit_per_minute", 60)
    final_timeout = timeout_seconds or defaults.get("timeout_seconds", 30)
    
    # Merge params with defaults
    final_params = defaults.get("params", {})
    final_params.update(params)
    
    return CollectorConfig(
        source=source,
        base_url=final_base_url,
        api_key=api_key,
        rate_limit_per_minute=final_rate_limit,
        timeout_seconds=final_timeout,
        params=final_params
    )


def create_collection_request(
    source: DataSource,
    sport: str = "mlb",
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    force: bool = False,
    dry_run: bool = False,
    **additional_params
) -> CollectionRequest:
    """
    Create a standardized CollectionRequest.
    
    Args:
        source: DataSource enum value
        sport: Sport type (default: mlb)
        start_date: Start date for collection
        end_date: End date for collection
        force: Force collection even if recent data exists
        dry_run: Perform collection without storing data
        **additional_params: Additional request parameters
        
    Returns:
        Properly configured CollectionRequest instance
    """
    return CollectionRequest(
        source=source,
        start_date=start_date,
        end_date=end_date,
        sport=sport,
        force=force,
        dry_run=dry_run,
        additional_params=additional_params
    )


class DeprecatedCollectorWrapper:
    """
    Wrapper to provide backward compatibility for old collector interfaces.
    
    This allows existing code to continue working while issuing deprecation warnings.
    """
    
    def __init__(self, source: DataSource, **config_kwargs):
        self.source = source
        self.config = create_collector_config(source, **config_kwargs)
        self.collector = CollectorFactory.create_collector(self.config)
        
    def collect_raw_data(self, sport: str = "mlb", **kwargs) -> list[dict[str, Any]]:
        """Deprecated: Use collect_data with CollectionRequest instead."""
        warnings.warn(
            "collect_raw_data() is deprecated. Use collect_data() with CollectionRequest instead.",
            DeprecationWarning,
            stacklevel=2
        )
        
        import asyncio
        
        request = create_collection_request(
            self.source,
            sport=sport,
            **kwargs
        )
        
        try:
            # Try to run in existing event loop
            loop = asyncio.get_running_loop()
            # If we're in an event loop, we need to use different approach
            return asyncio.create_task(self.collector.collect_data(request)).result()
        except RuntimeError:
            # No event loop, safe to use asyncio.run
            return asyncio.run(self.collector.collect_data(request))
    
    def collect_game_data(self, sport: str = "mlb") -> int:
        """Deprecated: Use collect() method with proper async context instead."""
        warnings.warn(
            "collect_game_data() is deprecated. Use collect() method with proper async context instead.",
            DeprecationWarning,
            stacklevel=2
        )
        
        import asyncio
        
        async def _collect():
            async with self.collector:
                result = await self.collector.collect()
                return len(result.data) if result.success else 0
        
        try:
            loop = asyncio.get_running_loop()
            return asyncio.create_task(_collect()).result()
        except RuntimeError:
            return asyncio.run(_collect())
    
    def test_collection(self, sport: str = "mlb") -> dict[str, Any]:
        """Deprecated: Use test_connection() method instead."""
        warnings.warn(
            "test_collection() is deprecated. Use test_connection() method instead.",
            DeprecationWarning,
            stacklevel=2
        )
        
        import asyncio
        
        async def _test():
            async with self.collector:
                success = await self.collector.test_connection()
                return {
                    "status": "success" if success else "failed",
                    "data_source": self.source.value,
                    "test_result": success
                }
        
        try:
            loop = asyncio.get_running_loop()
            return asyncio.create_task(_test()).result()
        except RuntimeError:
            return asyncio.run(_test())


# Convenience functions for common migration patterns
def migrate_sbd_collector(**kwargs) -> BaseCollector:
    """Create a properly configured SBD collector using the new pattern."""
    config = create_collector_config(DataSource.SBD, **kwargs)
    return CollectorFactory.create_collector(config)


def migrate_vsin_collector(**kwargs) -> BaseCollector:
    """Create a properly configured VSIN collector using the new pattern."""
    config = create_collector_config(DataSource.VSIN, **kwargs)
    return CollectorFactory.create_collector(config)


def migrate_action_network_collector(**kwargs) -> BaseCollector:
    """Create a properly configured Action Network collector using the new pattern."""
    config = create_collector_config(DataSource.ACTION_NETWORK, **kwargs)
    return CollectorFactory.create_collector(config)


# Example migration usage
if __name__ == "__main__":
    import asyncio
    
    async def example_migration():
        """Example showing how to migrate from old to new patterns."""
        
        print("=== Migration Example ===")
        
        # OLD PATTERN (deprecated):
        # collector = SBDUnifiedCollectorAPI()
        # data = collector.collect_raw_data("mlb")
        
        # NEW PATTERN:
        config = create_collector_config(
            DataSource.SBD,
            base_url="https://www.sportsbettingdime.com",
            rate_limit_per_minute=60
        )
        
        collector = CollectorFactory.create_collector(config)
        
        request = create_collection_request(
            DataSource.SBD,
            sport="mlb",
            dry_run=True  # Safe for testing
        )
        
        async with collector:
            # Test connection
            connection_ok = await collector.test_connection()
            print(f"Connection test: {'PASS' if connection_ok else 'FAIL'}")
            
            if connection_ok:
                # Collect data
                data = await collector.collect_data(request)
                print(f"Collected {len(data)} records")
                
                # Validate and normalize sample record
                if data:
                    sample = data[0]
                    is_valid = collector.validate_record(sample)
                    normalized = collector.normalize_record(sample)
                    
                    print(f"Sample record valid: {is_valid}")
                    print(f"Normalized record has {len(normalized)} fields")
    
    # Run the example
    asyncio.run(example_migration())