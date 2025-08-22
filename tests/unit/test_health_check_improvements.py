#!/usr/bin/env python3
"""
Test suite for health check service improvements

Tests all PR feedback improvements:
- Concurrent health checks
- Connection pooling
- Circuit breaker patterns
- Configurable values
- Proper error handling
"""

import asyncio
import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch, MagicMock

from src.services.monitoring.health_check_service import (
    HealthCheckService,
    HealthCheckConfig,
    CircuitBreakerState,
    HealthStatus,
    ServiceHealth,
)


class TestHealthCheckConfig:
    """Test configurable health check settings."""

    def test_default_config_values(self):
        """Test default configuration values are reasonable."""
        config = HealthCheckConfig()

        assert config.cache_ttl_seconds == 30
        assert config.connection_timeout_seconds == 5
        assert config.query_timeout_seconds == 10
        assert config.circuit_breaker_failure_threshold == 5
        assert config.circuit_breaker_timeout_minutes == 5
        assert config.slow_response_threshold_ms == 1000
        assert config.critical_response_threshold_ms == 5000

    def test_custom_config_values(self):
        """Test custom configuration values are applied."""
        config = HealthCheckConfig(
            cache_ttl_seconds=60,
            connection_timeout_seconds=10,
            slow_response_threshold_ms=2000,
        )

        assert config.cache_ttl_seconds == 60
        assert config.connection_timeout_seconds == 10
        assert config.slow_response_threshold_ms == 2000


class TestCircuitBreakerState:
    """Test circuit breaker functionality."""

    def test_initial_state_closed(self):
        """Test circuit breaker starts in CLOSED state."""
        cb = CircuitBreakerState()
        assert cb.state == "CLOSED"
        assert cb.failure_count == 0
        assert not cb.is_open()

    def test_circuit_breaker_opens_after_failures(self):
        """Test circuit breaker opens after threshold failures."""
        cb = CircuitBreakerState(failure_threshold=3)

        # Record failures up to threshold
        cb.record_failure()
        assert cb.state == "CLOSED"
        cb.record_failure()
        assert cb.state == "CLOSED"
        cb.record_failure()
        assert cb.state == "OPEN"
        assert cb.is_open()

    def test_circuit_breaker_half_open_after_timeout(self):
        """Test circuit breaker transitions to HALF_OPEN after timeout."""
        cb = CircuitBreakerState(
            failure_threshold=1, timeout_duration=timedelta(seconds=1)
        )

        # Trigger open state
        cb.record_failure()
        assert cb.state == "OPEN"

        # Wait for timeout (simulate)
        cb.last_failure_time = datetime.now() - timedelta(seconds=2)
        assert not cb.is_open()  # Should transition to HALF_OPEN
        assert cb.state == "HALF_OPEN"

    def test_circuit_breaker_success_resets(self):
        """Test successful operations reset circuit breaker."""
        cb = CircuitBreakerState(failure_threshold=1)

        cb.record_failure()
        assert cb.state == "OPEN"

        cb.record_success()
        assert cb.state == "CLOSED"
        assert cb.failure_count == 0
        assert cb.last_failure_time is None


class TestHealthCheckService:
    """Test improved health check service."""

    @pytest.fixture
    def health_service(self):
        """Create health check service with test configuration."""
        config = HealthCheckConfig(
            cache_ttl_seconds=1,  # Short TTL for testing
            connection_timeout_seconds=2,
            query_timeout_seconds=3,
        )
        return HealthCheckService(config)

    @pytest.mark.asyncio
    async def test_concurrent_health_checks(self, health_service):
        """Test health checks run concurrently, not sequentially."""

        # Mock individual health check methods
        with (
            patch.object(health_service, "_check_database_health") as mock_db,
            patch.object(
                health_service, "_check_data_collection_health"
            ) as mock_collection,
            patch.object(health_service, "_check_configuration_health") as mock_config,
            patch.object(health_service, "_check_logging_health") as mock_logging,
        ):
            # Configure mocks to return ServiceHealth objects directly with delays
            async def delayed_db_check():
                await asyncio.sleep(0.1)
                return ServiceHealth(
                    name="database",
                    status=HealthStatus.HEALTHY,
                    message="Test",
                    response_time_ms=100.0,
                    last_check=datetime.now(),
                    error_count=0,
                )

            async def delayed_collection_check():
                await asyncio.sleep(0.1)
                return ServiceHealth(
                    name="collection",
                    status=HealthStatus.HEALTHY,
                    message="Test",
                    response_time_ms=100.0,
                    last_check=datetime.now(),
                    error_count=0,
                )

            async def delayed_config_check():
                await asyncio.sleep(0.1)
                return ServiceHealth(
                    name="config",
                    status=HealthStatus.HEALTHY,
                    message="Test",
                    response_time_ms=100.0,
                    last_check=datetime.now(),
                    error_count=0,
                )

            async def delayed_logging_check():
                await asyncio.sleep(0.1)
                return ServiceHealth(
                    name="logging",
                    status=HealthStatus.HEALTHY,
                    message="Test",
                    response_time_ms=100.0,
                    last_check=datetime.now(),
                    error_count=0,
                )

            mock_db.side_effect = delayed_db_check
            mock_collection.side_effect = delayed_collection_check
            mock_config.side_effect = delayed_config_check
            mock_logging.side_effect = delayed_logging_check

            # Measure execution time
            start_time = asyncio.get_event_loop().time()
            result = await health_service.get_system_health()
            end_time = asyncio.get_event_loop().time()

            # If sequential, would take ~0.4s; concurrent should be ~0.1s
            execution_time = end_time - start_time
            assert execution_time < 0.25, (
                f"Health checks took {execution_time:.2f}s, likely sequential"
            )

            # Verify all checks were called
            mock_db.assert_called_once()
            mock_collection.assert_called_once()
            mock_config.assert_called_once()
            mock_logging.assert_called_once()

    @pytest.mark.asyncio
    async def test_database_circuit_breaker_prevents_calls(self, health_service):
        """Test database circuit breaker prevents calls when open."""

        # Force circuit breaker to open state
        health_service._db_circuit_breaker.state = "OPEN"

        result = await health_service._check_database_health()

        assert result.status == HealthStatus.CRITICAL
        assert "circuit breaker is OPEN" in result.message
        assert result.metadata["circuit_breaker_state"] == "OPEN"

    @pytest.mark.asyncio
    async def test_database_health_timeout_handling(self, health_service):
        """Test database health check handles timeouts properly."""

        with patch("asyncpg.connect") as mock_connect:
            # Configure mock to timeout
            mock_connect.side_effect = asyncio.TimeoutError("Connection timeout")

            result = await health_service._check_database_health()

            assert result.status == HealthStatus.CRITICAL
            assert "timeout" in result.message.lower()
            assert result.metadata["error_type"] == "TimeoutError"
            assert health_service._db_circuit_breaker.failure_count > 0

    @pytest.mark.asyncio
    async def test_collection_circuit_breaker_functionality(self, health_service):
        """Test data collection circuit breaker functionality."""

        # Force circuit breaker to open state
        health_service._collection_circuit_breaker.state = "OPEN"

        result = await health_service._check_data_collection_health()

        assert result.status == HealthStatus.CRITICAL
        assert "circuit breaker is OPEN" in result.message
        assert result.metadata["circuit_breaker_state"] == "OPEN"

    @pytest.mark.asyncio
    async def test_collection_import_error_handling(self, health_service):
        """Test proper handling of import errors in collection health check."""

        with patch(
            "src.data.collection.registry.get_collector_instance"
        ) as mock_import:
            mock_import.side_effect = ImportError("Registry not available")

            result = await health_service._check_data_collection_health()

            assert result.status == HealthStatus.UNHEALTHY
            assert "import failed" in result.message.lower()
            assert result.metadata["error_type"] == "ImportError"
            assert "recovery_action" in result.metadata

    def test_configurable_thresholds_applied(self, health_service):
        """Test that configurable thresholds are properly applied."""

        # Verify config values are used
        assert health_service.config.cache_ttl_seconds == 1
        assert health_service.config.connection_timeout_seconds == 2
        assert health_service.config.query_timeout_seconds == 3

        # Verify circuit breakers use config values
        assert (
            health_service._db_circuit_breaker.failure_threshold
            == health_service.config.circuit_breaker_failure_threshold
        )
        assert (
            health_service._collection_circuit_breaker.failure_threshold
            == health_service.config.circuit_breaker_failure_threshold
        )

    @pytest.mark.asyncio
    async def test_cache_respects_configurable_ttl(self, health_service):
        """Test health check cache respects configurable TTL."""

        # Mock all health check methods
        with (
            patch.object(health_service, "_check_database_health") as mock_db,
            patch.object(
                health_service, "_check_data_collection_health"
            ) as mock_collection,
            patch.object(health_service, "_check_configuration_health") as mock_config,
            patch.object(health_service, "_check_logging_health") as mock_logging,
        ):
            mock_db.return_value = ServiceHealth(
                name="db",
                status=HealthStatus.HEALTHY,
                message="OK",
                response_time_ms=100.0,
                last_check=datetime.now(),
                error_count=0,
            )
            mock_collection.return_value = ServiceHealth(
                name="collection",
                status=HealthStatus.HEALTHY,
                message="OK",
                response_time_ms=100.0,
                last_check=datetime.now(),
                error_count=0,
            )
            mock_config.return_value = ServiceHealth(
                name="config",
                status=HealthStatus.HEALTHY,
                message="OK",
                response_time_ms=100.0,
                last_check=datetime.now(),
                error_count=0,
            )
            mock_logging.return_value = ServiceHealth(
                name="logging",
                status=HealthStatus.HEALTHY,
                message="OK",
                response_time_ms=100.0,
                last_check=datetime.now(),
                error_count=0,
            )

            # First call should execute health checks
            result1 = await health_service.get_system_health()
            assert mock_db.call_count == 1

            # Second call within TTL should use cache
            result2 = await health_service.get_system_health()
            assert mock_db.call_count == 1  # No additional calls

            # Wait for TTL to expire
            await asyncio.sleep(1.1)  # TTL is 1 second

            # Third call should execute health checks again
            result3 = await health_service.get_system_health()
            assert mock_db.call_count == 2  # Additional call made


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
