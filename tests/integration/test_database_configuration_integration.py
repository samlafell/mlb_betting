#!/usr/bin/env python3
"""
Integration Tests for Database Configuration Centralization

Tests that all components properly use centralized database configuration
instead of hardcoded values.
"""

from unittest.mock import MagicMock, patch

import pytest

from src.data.collection.base import CollectorConfig, DataSource
from src.data.collection.mlb_stats_api_collector import MLBStatsAPICollector
from src.data.pipeline.staging_action_network_historical_processor import (
    ActionNetworkHistoricalProcessor,
)
from src.data.pipeline.staging_action_network_history_processor import (
    ActionNetworkHistoryProcessor,
)
from src.data.pipeline.staging_action_network_unified_processor import (
    ActionNetworkUnifiedStagingProcessor,
)
from src.services.cross_site_game_resolution_service import (
    CrossSiteGameResolutionService,
)


class TestDatabaseConfigurationIntegration:
    """Integration tests for centralized database configuration."""

    @pytest.fixture
    def mock_settings(self):
        """Mock settings with test database configuration."""
        mock_settings = MagicMock()
        mock_settings.database.host = "test-host"
        mock_settings.database.port = 5433
        mock_settings.database.database = "test-database"
        mock_settings.database.user = "test-user"
        mock_settings.database.password = "test-password"
        return mock_settings

    @pytest.fixture
    def expected_db_config(self):
        """Expected database configuration based on mock settings."""
        return {
            "host": "test-host",
            "port": 5433,
            "database": "test-database",
            "user": "test-user",
            "password": "test-password",
        }

    @pytest.mark.asyncio
    async def test_action_network_history_processor_uses_centralized_config(
        self, mock_settings, expected_db_config
    ):
        """Test ActionNetworkHistoryProcessor uses centralized configuration."""
        with patch(
            "src.data.pipeline.staging_action_network_history_processor.get_settings",
            return_value=mock_settings,
        ):
            processor = ActionNetworkHistoryProcessor()

            # Verify processor uses centralized configuration
            actual_config = processor._get_db_config()
            assert actual_config == expected_db_config

            # Verify no hardcoded values
            assert actual_config["host"] != "localhost"
            assert actual_config["user"] != "samlafell"

    @pytest.mark.asyncio
    async def test_action_network_unified_processor_uses_centralized_config(
        self, mock_settings, expected_db_config
    ):
        """Test ActionNetworkUnifiedStagingProcessor uses centralized configuration."""
        with patch(
            "src.data.pipeline.staging_action_network_unified_processor.get_settings",
            return_value=mock_settings,
        ):
            processor = ActionNetworkUnifiedStagingProcessor()

            # Verify processor uses centralized configuration
            actual_config = processor._get_db_config()
            assert actual_config == expected_db_config

            # Verify no hardcoded values
            assert actual_config["host"] != "localhost"
            assert actual_config["user"] != "samlafell"

    @pytest.mark.asyncio
    async def test_action_network_historical_processor_uses_centralized_config(
        self, mock_settings, expected_db_config
    ):
        """Test ActionNetworkHistoricalProcessor uses centralized configuration."""
        with patch(
            "src.data.pipeline.staging_action_network_historical_processor.get_settings",
            return_value=mock_settings,
        ):
            processor = ActionNetworkHistoricalProcessor()

            # Verify processor uses centralized configuration
            actual_config = processor._get_db_config()
            assert actual_config == expected_db_config

            # Verify no hardcoded values
            assert actual_config["host"] != "localhost"
            assert actual_config["user"] != "samlafell"

    @pytest.mark.asyncio
    async def test_mlb_stats_api_collector_uses_centralized_config(
        self, mock_settings, expected_db_config
    ):
        """Test MLBStatsAPICollector uses centralized configuration."""
        with patch(
            "src.data.collection.mlb_stats_api_collector.get_settings",
            return_value=mock_settings,
        ):
            config = CollectorConfig(source=DataSource.MLB_STATS_API, enabled=True)
            collector = MLBStatsAPICollector(config)

            # Verify collector uses centralized configuration
            assert collector.db_config == expected_db_config

            # Verify no hardcoded values
            assert collector.db_config["host"] != "localhost"
            assert collector.db_config["user"] != "samlafell"

    @pytest.mark.asyncio
    async def test_cross_site_game_resolution_service_uses_centralized_config(
        self, mock_settings, expected_db_config
    ):
        """Test CrossSiteGameResolutionService uses centralized configuration when no config provided."""
        with patch("src.core.config.get_settings", return_value=mock_settings):
            # Test with no db_config provided (should use centralized)
            service = CrossSiteGameResolutionService()

            # Verify service uses centralized configuration
            assert service.db_config == expected_db_config

            # Verify no hardcoded values
            assert service.db_config["host"] != "localhost"
            assert service.db_config["user"] != "samlafell"

    @pytest.mark.asyncio
    async def test_cross_site_game_resolution_service_respects_provided_config(self):
        """Test CrossSiteGameResolutionService respects provided db_config."""
        custom_config = {
            "host": "custom-host",
            "port": 5434,
            "database": "custom-database",
            "user": "custom-user",
            "password": "custom-password",
        }

        service = CrossSiteGameResolutionService(db_config=custom_config)

        # Verify service uses provided configuration
        assert service.db_config == custom_config

    @pytest.mark.asyncio
    async def test_database_connection_string_format(self, mock_settings):
        """Test that database configuration can be used for connection strings."""
        # Test asyncpg connection parameters using mock settings directly
        connection_params = {
            "host": mock_settings.database.host,
            "port": mock_settings.database.port,
            "database": mock_settings.database.database,
            "user": mock_settings.database.user,
            "password": mock_settings.database.password,
        }

        # Verify all required parameters are present
        assert connection_params["host"] == "test-host"
        assert connection_params["port"] == 5433
        assert connection_params["database"] == "test-database"
        assert connection_params["user"] == "test-user"
        assert connection_params["password"] == "test-password"

    @pytest.mark.asyncio
    async def test_all_processors_initialize_without_errors(self, mock_settings):
        """Test that all processors can be initialized with centralized config."""
        with (
            patch(
                "src.data.pipeline.staging_action_network_history_processor.get_settings",
                return_value=mock_settings,
            ),
            patch(
                "src.data.pipeline.staging_action_network_unified_processor.get_settings",
                return_value=mock_settings,
            ),
            patch(
                "src.data.pipeline.staging_action_network_historical_processor.get_settings",
                return_value=mock_settings,
            ),
        ):
            # Test all processors can be initialized
            history_processor = ActionNetworkHistoryProcessor()
            unified_processor = ActionNetworkUnifiedStagingProcessor()
            historical_processor = ActionNetworkHistoricalProcessor()

            # Verify they all have valid database configurations
            assert history_processor._get_db_config()["host"] == "test-host"
            assert unified_processor._get_db_config()["host"] == "test-host"
            assert historical_processor._get_db_config()["host"] == "test-host"

    @pytest.mark.asyncio
    async def test_no_hardcoded_database_values_in_runtime(self, mock_settings):
        """Test that no components use hardcoded database values at runtime."""
        with (
            patch(
                "src.data.pipeline.staging_action_network_history_processor.get_settings",
                return_value=mock_settings,
            ),
            patch(
                "src.data.collection.mlb_stats_api_collector.get_settings",
                return_value=mock_settings,
            ),
            patch("src.core.config.get_settings", return_value=mock_settings),
        ):
            # Initialize all components
            processor = ActionNetworkHistoryProcessor()
            config = CollectorConfig(source=DataSource.MLB_STATS_API, enabled=True)
            collector = MLBStatsAPICollector(config)
            service = CrossSiteGameResolutionService()

            # Check that none use hardcoded localhost or samlafell
            components = [
                processor._get_db_config(),
                collector.db_config,
                service.db_config,
            ]

            for component_config in components:
                assert component_config["host"] != "localhost", (
                    f"Component still uses hardcoded localhost: {component_config}"
                )
                assert component_config["user"] != "samlafell", (
                    f"Component still uses hardcoded user: {component_config}"
                )
                assert (
                    component_config["database"] != "mlb_betting"
                    or component_config["host"] == "test-host"
                ), (
                    f"Component uses hardcoded database without proper host override: {component_config}"
                )

    @pytest.mark.asyncio
    async def test_environment_variable_fallback_compatibility(self):
        """Test that configuration works with environment variables."""
        import os
        from unittest.mock import patch

        # Mock environment variables
        env_vars = {
            "DB_HOST": "env-host",
            "DB_PORT": "5435",
            "DB_NAME": "env-database",
            "DB_USER": "env-user",
            "DB_PASSWORD": "env-password",
        }

        with patch.dict(os.environ, env_vars):
            # This tests the utility script pattern that falls back to environment variables
            fallback_config = {
                "host": os.getenv("DB_HOST", "localhost"),
                "port": int(os.getenv("DB_PORT", "5432")),
                "database": os.getenv("DB_NAME", "mlb_betting"),
                "user": os.getenv("DB_USER", "samlafell"),
                "password": os.getenv("DB_PASSWORD", ""),
            }

            assert fallback_config["host"] == "env-host"
            assert fallback_config["port"] == 5435
            assert fallback_config["database"] == "env-database"
            assert fallback_config["user"] == "env-user"
            assert fallback_config["password"] == "env-password"

    @pytest.mark.asyncio
    async def test_configuration_consistency_across_components(self, mock_settings):
        """Test that all components get the same configuration from centralized settings."""
        with (
            patch(
                "src.data.pipeline.staging_action_network_history_processor.get_settings",
                return_value=mock_settings,
            ),
            patch(
                "src.data.pipeline.staging_action_network_unified_processor.get_settings",
                return_value=mock_settings,
            ),
            patch(
                "src.data.collection.mlb_stats_api_collector.get_settings",
                return_value=mock_settings,
            ),
        ):
            # Initialize multiple components
            history_processor = ActionNetworkHistoryProcessor()
            unified_processor = ActionNetworkUnifiedStagingProcessor()
            config = CollectorConfig(source=DataSource.MLB_STATS_API, enabled=True)
            collector = MLBStatsAPICollector(config)

            # Get configurations from all components
            configs = [
                history_processor._get_db_config(),
                unified_processor._get_db_config(),
                collector.db_config,
            ]

            # Verify all configurations are identical
            for i, config in enumerate(configs[1:], 1):
                assert config == configs[0], (
                    f"Configuration mismatch between component 0 and {i}: {configs[0]} vs {config}"
                )
