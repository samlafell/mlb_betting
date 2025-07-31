"""
Unit tests for retry logic in data collection.

Tests retry behavior, circuit breaker patterns, and transient failure handling.
"""

import asyncio
import pytest
import time
from unittest.mock import Mock, AsyncMock

from tests.utils.retry_utils import (
    RetryConfig, RetryManager, RetryStrategy, CircuitBreaker, 
    TransientFailureSimulator, retry_async, retry_sync,
    get_api_retry_manager, get_db_retry_manager
)
from tests.mocks.collectors import MockActionNetworkCollector, CollectorMockFactory
from tests.utils.logging_utils import create_test_logger, setup_secure_test_logging
from src.data.collection.base import CollectorConfig, CollectionRequest


class TestRetryConfig:
    """Test retry configuration."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        setup_secure_test_logging()
        self.logger = create_test_logger("retry_config_test")
    
    def test_default_config(self):
        """Test default retry configuration."""
        config = RetryConfig()
        
        assert config.max_attempts == 3
        assert config.initial_delay == 0.1
        assert config.strategy == RetryStrategy.EXPONENTIAL_BACKOFF
        assert config.retry_on_exceptions == (Exception,)
        
        self.logger.info("✅ Default retry config test passed")
    
    def test_api_optimized_config(self):
        """Test API-optimized configuration."""
        config = RetryConfig.for_api_calls()
        
        assert config.max_attempts == 3
        assert config.initial_delay == 0.5
        assert config.max_delay == 30.0
        assert ConnectionError in config.retry_on_exceptions
        assert TimeoutError in config.retry_on_exceptions
        
        self.logger.info("✅ API config test passed")
    
    def test_database_optimized_config(self):
        """Test database-optimized configuration."""
        config = RetryConfig.for_database_operations()
        
        assert config.max_attempts == 5
        assert config.strategy == RetryStrategy.LINEAR_BACKOFF
        assert config.failure_threshold == 10
        
        self.logger.info("✅ Database config test passed")


class TestRetryManager:
    """Test retry manager functionality."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        setup_secure_test_logging()
        self.logger = create_test_logger("retry_manager_test")
        self.config = RetryConfig(max_attempts=3, initial_delay=0.01)  # Fast tests
        self.retry_manager = RetryManager(self.config)
    
    def test_delay_calculation_exponential(self):
        """Test exponential backoff delay calculation."""
        config = RetryConfig(
            initial_delay=0.1,
            exponential_factor=2.0,
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF
        )
        manager = RetryManager(config)
        
        assert manager.calculate_delay(1) == 0.1  # First attempt
        assert manager.calculate_delay(2) == 0.2  # Second attempt
        assert manager.calculate_delay(3) == 0.4  # Third attempt
        
        self.logger.info("✅ Exponential backoff calculation test passed")
    
    def test_delay_calculation_linear(self):
        """Test linear backoff delay calculation."""
        config = RetryConfig(
            initial_delay=0.1,
            strategy=RetryStrategy.LINEAR_BACKOFF
        )
        manager = RetryManager(config)
        
        assert manager.calculate_delay(1) == 0.1  # 0.1 * 1
        assert manager.calculate_delay(2) == 0.2  # 0.1 * 2
        assert manager.calculate_delay(3) == 0.3  # 0.1 * 3
        
        self.logger.info("✅ Linear backoff calculation test passed")
    
    def test_delay_calculation_fixed(self):
        """Test fixed delay calculation."""
        config = RetryConfig(
            initial_delay=0.5,
            strategy=RetryStrategy.FIXED_DELAY
        )
        manager = RetryManager(config)
        
        assert manager.calculate_delay(1) == 0.5
        assert manager.calculate_delay(2) == 0.5
        assert manager.calculate_delay(3) == 0.5
        
        self.logger.info("✅ Fixed delay calculation test passed")
    
    def test_max_delay_limit(self):
        """Test maximum delay limit."""
        config = RetryConfig(
            initial_delay=1.0,
            max_delay=2.0,
            exponential_factor=3.0,
            strategy=RetryStrategy.EXPONENTIAL_BACKOFF
        )
        manager = RetryManager(config)
        
        # Should be capped at max_delay
        assert manager.calculate_delay(5) == 2.0  # Would be 81 without cap
        
        self.logger.info("✅ Max delay limit test passed")
    
    @pytest.mark.asyncio
    async def test_successful_execution_no_retry(self):
        """Test successful execution without retries."""
        async def success_func():
            return "success"
        
        result = await self.retry_manager.execute_async(success_func)
        
        assert result == "success"
        assert self.retry_manager.statistics.total_attempts == 1
        assert self.retry_manager.statistics.successful_attempts == 1
        assert self.retry_manager.statistics.retry_attempts == 0
        
        self.logger.info("✅ Successful execution test passed")
    
    @pytest.mark.asyncio
    async def test_retry_on_transient_failure(self):
        """Test retry behavior on transient failures."""
        call_count = 0
        
        async def flaky_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Network issue")
            return "success"
        
        result = await self.retry_manager.execute_async(flaky_func)
        
        assert result == "success"
        assert call_count == 3
        assert self.retry_manager.statistics.total_attempts == 1  # One operation
        assert self.retry_manager.statistics.successful_attempts == 1
        
        self.logger.info("✅ Transient failure retry test passed")
    
    @pytest.mark.asyncio
    async def test_permanent_failure_no_retry(self):
        """Test no retry on permanent failures."""
        config = RetryConfig(
            max_attempts=3,
            retry_on_exceptions=(ConnectionError,)  # Only retry ConnectionError
        )
        manager = RetryManager(config)
        
        async def permanent_failure_func():
            raise ValueError("Permanent error")  # Not in retry_on_exceptions
        
        with pytest.raises(ValueError):
            await manager.execute_async(permanent_failure_func)
        
        # Should not retry ValueError
        assert manager.statistics.total_attempts == 1
        assert manager.statistics.retry_attempts == 0
        
        self.logger.info("✅ Permanent failure test passed")
    
    @pytest.mark.asyncio
    async def test_max_attempts_reached(self):
        """Test behavior when max attempts are reached."""
        async def always_fail_func():
            raise ConnectionError("Always fails")
        
        with pytest.raises(ConnectionError):
            await self.retry_manager.execute_async(always_fail_func)
        
        assert self.retry_manager.statistics.failed_attempts == 1
        assert self.retry_manager.statistics.successful_attempts == 0
        
        self.logger.info("✅ Max attempts reached test passed")
    
    def test_sync_retry_execution(self):
        """Test synchronous retry execution."""
        call_count = 0
        
        def flaky_sync_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Network issue")
            return "sync_success"
        
        result = self.retry_manager.execute_sync(flaky_sync_func)
        
        assert result == "sync_success"
        assert call_count == 2
        
        self.logger.info("✅ Sync retry execution test passed")


class TestCircuitBreaker:
    """Test circuit breaker functionality."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        setup_secure_test_logging()
        self.logger = create_test_logger("circuit_breaker_test")
        self.config = RetryConfig(failure_threshold=3, recovery_timeout=0.1)  # Fast tests
        self.circuit_breaker = CircuitBreaker(self.config)
    
    def test_initial_closed_state(self):
        """Test circuit breaker starts in closed state."""
        assert self.circuit_breaker.can_execute() is True
        
        self.logger.info("✅ Initial closed state test passed")
    
    def test_circuit_opens_after_failures(self):
        """Test circuit breaker opens after failure threshold."""
        # Record failures up to threshold
        for _ in range(self.config.failure_threshold):
            self.circuit_breaker.record_failure()
        
        # Should now be open
        assert self.circuit_breaker.can_execute() is False
        
        self.logger.info("✅ Circuit opens after failures test passed")
    
    def test_circuit_recovery_timeout(self):
        """Test circuit breaker recovery after timeout."""
        # Open the circuit
        for _ in range(self.config.failure_threshold):
            self.circuit_breaker.record_failure()
        
        assert self.circuit_breaker.can_execute() is False
        
        # Wait for recovery timeout
        time.sleep(self.config.recovery_timeout + 0.01)
        
        # Should move to half-open
        assert self.circuit_breaker.can_execute() is True
        
        self.logger.info("✅ Circuit recovery timeout test passed")
    
    def test_half_open_to_closed_transition(self):
        """Test transition from half-open to closed on success."""
        # Open the circuit
        for _ in range(self.config.failure_threshold):
            self.circuit_breaker.record_failure()
        
        # Wait for recovery
        time.sleep(self.config.recovery_timeout + 0.01)
        self.circuit_breaker.can_execute()  # Move to half-open
        
        # Record enough successes to close
        for _ in range(self.config.success_threshold):
            self.circuit_breaker.record_success()
        
        # Should be closed now
        assert self.circuit_breaker.can_execute() is True
        
        self.logger.info("✅ Half-open to closed transition test passed")


class TestTransientFailureSimulator:
    """Test transient failure simulation."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        setup_secure_test_logging()
        self.logger = create_test_logger("failure_simulator_test")
    
    def test_failure_count_simulation(self):
        """Test failure count-based simulation."""
        simulator = TransientFailureSimulator(failure_rate=0.0, failure_count=2)
        
        # First two calls should fail
        assert simulator.should_fail() is True
        assert simulator.should_fail() is True
        
        # Subsequent calls should succeed
        assert simulator.should_fail() is False
        assert simulator.should_fail() is False
        
        self.logger.info("✅ Failure count simulation test passed")
    
    def test_failure_rate_simulation(self):
        """Test failure rate-based simulation."""
        # 100% failure rate
        simulator = TransientFailureSimulator(failure_rate=1.0, failure_count=0)
        
        # Should always fail
        for _ in range(10):
            assert simulator.should_fail() is True
        
        # 0% failure rate
        simulator = TransientFailureSimulator(failure_rate=0.0, failure_count=0)
        
        # Should never fail
        for _ in range(10):
            assert simulator.should_fail() is False
        
        self.logger.info("✅ Failure rate simulation test passed")
    
    def test_simulator_reset(self):
        """Test failure simulator reset."""
        simulator = TransientFailureSimulator(failure_rate=0.0, failure_count=2)
        
        # Use up the failures
        simulator.should_fail()
        simulator.should_fail()
        
        # Reset and test again
        simulator.reset()
        assert simulator.should_fail() is True  # Should fail again after reset
        
        self.logger.info("✅ Simulator reset test passed")


class TestRetryDecorators:
    """Test retry decorators."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        setup_secure_test_logging()
        self.logger = create_test_logger("retry_decorators_test")
    
    @pytest.mark.asyncio
    async def test_async_retry_decorator(self):
        """Test async retry decorator."""
        call_count = 0
        config = RetryConfig(max_attempts=3, initial_delay=0.01)
        
        @retry_async(config)
        async def flaky_async_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Network issue")
            return "decorated_success"
        
        result = await flaky_async_func()
        
        assert result == "decorated_success"
        assert call_count == 2
        
        # Check statistics
        stats = flaky_async_func._retry_manager.statistics.get_summary()
        assert stats["total_attempts"] == 1
        assert stats["successful_attempts"] == 1
        
        self.logger.info("✅ Async retry decorator test passed")
    
    def test_sync_retry_decorator(self):
        """Test sync retry decorator."""
        call_count = 0
        config = RetryConfig(max_attempts=3, initial_delay=0.01)
        
        @retry_sync(config)
        def flaky_sync_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("Network issue")
            return "decorated_sync_success"
        
        result = flaky_sync_func()
        
        assert result == "decorated_sync_success"
        assert call_count == 2
        
        self.logger.info("✅ Sync retry decorator test passed")


class TestCollectorRetryIntegration:
    """Test retry integration with mock collectors."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        setup_secure_test_logging()
        self.logger = create_test_logger("collector_retry_test")
        self.config = CollectorConfig(
            source_name="action_network_test",
            rate_limit_requests=100,
            rate_limit_period=3600
        )
        self.collector = MockActionNetworkCollector(self.config)
    
    @pytest.mark.asyncio
    async def test_collector_transient_failure_recovery(self):
        """Test collector recovery from transient failures."""
        # Configure for 2 failures then success
        self.collector.configure_transient_failures(failure_rate=0.0, failure_count=2)
        
        request = CollectionRequest(
            source="action_network",
            date_range={"start": "2024-07-30", "end": "2024-07-30"}
        )
        
        # Should succeed after retries
        results = await self.collector.collect_data(request)
        
        assert len(results) > 0
        
        # Check retry statistics
        stats = self.collector.get_retry_statistics()
        assert stats["total_attempts"] == 1
        assert stats["successful_attempts"] == 1
        
        self.logger.info("✅ Collector transient failure recovery test passed")
    
    @pytest.mark.asyncio
    async def test_collector_retry_statistics(self):
        """Test collector retry statistics collection."""
        # Configure some transient failures
        self.collector.configure_transient_failures(failure_rate=0.5, failure_count=1)
        
        request = CollectionRequest(
            source="action_network",
            date_range={"start": "2024-07-30", "end": "2024-07-30"}
        )
        
        # Make multiple calls
        for _ in range(5):
            try:
                await self.collector.collect_data(request)
            except Exception:
                pass  # Some may fail
        
        stats = self.collector.get_retry_statistics()
        
        assert stats["total_attempts"] == 5
        assert "success_rate_percent" in stats
        assert "average_response_time_ms" in stats
        
        self.logger.info(f"✅ Retry statistics: {stats['success_rate_percent']:.1f}% success rate")
    
    def test_collector_retry_configuration(self):
        """Test collector retry configuration."""
        # Test custom retry config
        custom_config = RetryConfig(
            max_attempts=5,
            initial_delay=0.2,
            strategy=RetryStrategy.LINEAR_BACKOFF
        )
        
        self.collector.configure_retry_behavior(custom_config)
        
        assert self.collector.retry_config.max_attempts == 5
        assert self.collector.retry_config.initial_delay == 0.2
        assert self.collector.retry_config.strategy == RetryStrategy.LINEAR_BACKOFF
        
        self.logger.info("✅ Collector retry configuration test passed")
    
    def test_collector_latency_simulation(self):
        """Test collector latency simulation."""
        # Set high latency for testing
        self.collector.set_latency_simulation(base_latency=0.5, variation=0.1)
        
        assert self.collector.base_latency == 0.5
        assert self.collector.latency_variation == 0.1
        
        self.logger.info("✅ Collector latency simulation test passed")
    
    def test_collector_statistics_reset(self):
        """Test collector statistics reset."""
        # Configure some failures
        self.collector.configure_transient_failures(failure_rate=0.3, failure_count=1)
        
        # Make a call to generate statistics
        request = CollectionRequest(
            source="action_network",
            date_range={"start": "2024-07-30", "end": "2024-07-30"}
        )
        
        try:
            asyncio.run(self.collector.collect_data(request))
        except:
            pass
        
        # Verify statistics exist
        stats_before = self.collector.get_retry_statistics()
        assert stats_before["total_attempts"] > 0
        
        # Reset and verify clean state
        self.collector.reset_retry_statistics()
        stats_after = self.collector.get_retry_statistics()
        
        assert stats_after["total_attempts"] == 0
        assert stats_after["successful_attempts"] == 0
        
        self.logger.info("✅ Collector statistics reset test passed")


class TestGlobalRetryManagers:
    """Test global retry manager instances."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        setup_secure_test_logging()
        self.logger = create_test_logger("global_retry_managers_test")
    
    def test_api_retry_manager_singleton(self):
        """Test API retry manager singleton behavior."""
        manager1 = get_api_retry_manager()
        manager2 = get_api_retry_manager()
        
        assert manager1 is manager2  # Same instance
        assert manager1.config.max_attempts == 3  # API config
        
        self.logger.info("✅ API retry manager singleton test passed")
    
    def test_db_retry_manager_singleton(self):
        """Test database retry manager singleton behavior."""
        manager1 = get_db_retry_manager()
        manager2 = get_db_retry_manager()
        
        assert manager1 is manager2  # Same instance  
        assert manager1.config.max_attempts == 5  # DB config
        
        self.logger.info("✅ DB retry manager singleton test passed")
    
    @pytest.mark.asyncio
    async def test_global_manager_usage(self):
        """Test using global retry managers."""
        api_manager = get_api_retry_manager()
        
        call_count = 0
        async def api_call():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ConnectionError("API timeout")
            return {"data": "success"}
        
        result = await api_manager.execute_async(api_call)
        
        assert result["data"] == "success"
        assert call_count == 2
        
        self.logger.info("✅ Global manager usage test passed")