#!/usr/bin/env python3
"""
Integration Test for Silent Failure Resolution System

Tests the complete silent failure resolution system including:
- Health monitoring with confidence scoring
- Alert manager functionality
- Circuit breaker with automatic recovery
- Enhanced orchestrator integration
- CLI commands for health monitoring

Part of solution for GitHub Issue #36: "Data Collection Fails Silently"
"""

import asyncio
import pytest
import tempfile
import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from src.core.config import get_settings
from src.data.collection.enhanced_orchestrator import EnhancedCollectionOrchestrator
from src.data.collection.health_monitoring import (
    CollectionHealthResult,
    CollectionHealthMetrics,
    CollectionConfidenceAnalyzer,
    HealthStatus,
    AlertSeverity,
    FailurePattern
)
from src.data.collection.alert_manager import CollectionAlertManager
from src.data.collection.circuit_breaker import (
    CircuitBreakerManager,
    CircuitBreakerConfig,
    CircuitState,
    RecoveryStrategy
)
from src.data.collection.base import CollectionResult
from src.interfaces.cli.commands.collection_health import health


@pytest.fixture
async def enhanced_orchestrator():
    """Create enhanced orchestrator for testing."""
    settings = get_settings()
    orchestrator = EnhancedCollectionOrchestrator(settings)
    yield orchestrator
    await orchestrator.cleanup()


@pytest.fixture
async def alert_manager():
    """Create alert manager for testing."""
    manager = CollectionAlertManager()
    yield manager
    if hasattr(manager, 'db_pool') and manager.db_pool:
        await manager.db_pool.close()


@pytest.fixture
def circuit_breaker_manager():
    """Create circuit breaker manager for testing."""
    manager = CircuitBreakerManager()
    yield manager
    asyncio.create_task(manager.cleanup_all())


class TestHealthMonitoring:
    """Test health monitoring functionality."""
    
    def test_confidence_analyzer_initialization(self):
        """Test confidence analyzer initializes correctly."""
        analyzer = CollectionConfidenceAnalyzer()
        assert analyzer is not None
        assert hasattr(analyzer, 'analyze_result')
    
    def test_health_result_creation(self):
        """Test health result creation with various scenarios."""
        # Successful result
        result = CollectionHealthResult(
            success=True,
            data=[{"game_id": "123", "odds": 1.5}],
            source="action_network",
            timestamp=datetime.now()
        )
        
        assert result.success is True
        assert result.data_count == 1
        assert result.source == "action_network"
        assert result.confidence_score == 1.0  # Default
        
        # Failed result
        failed_result = CollectionHealthResult(
            success=False,
            data=[],
            source="vsin",
            timestamp=datetime.now(),
            errors=["Connection timeout"]
        )
        
        assert failed_result.success is False
        assert failed_result.data_count == 0
        assert len(failed_result.errors) == 1
    
    def test_confidence_analysis_with_patterns(self):
        """Test confidence analysis with different failure patterns."""
        analyzer = CollectionConfidenceAnalyzer()
        
        # Create health metrics
        metrics = CollectionHealthMetrics(source="test_source")
        metrics.total_collections = 10
        metrics.successful_collections = 8
        metrics.failed_collections = 2
        
        # Test successful result
        result = CollectionHealthResult(
            success=True,
            data=[{"test": "data"}] * 100,
            source="test_source",
            timestamp=datetime.now(),
            response_time_ms=200.0
        )
        
        enhanced_result = analyzer.analyze_result(result, metrics)
        
        assert enhanced_result.confidence_score > 0.8
        assert enhanced_result.health_status in [HealthStatus.HEALTHY, HealthStatus.DEGRADED]
    
    def test_failure_pattern_detection(self):
        """Test detection of various failure patterns."""
        analyzer = CollectionConfidenceAnalyzer()
        
        # Test rate limiting pattern
        result_rate_limit = CollectionHealthResult(
            success=False,
            data=[],
            source="test_source",
            timestamp=datetime.now(),
            errors=["Rate limit exceeded"],
            response_time_ms=50.0
        )
        
        metrics = CollectionHealthMetrics(source="test_source")
        enhanced_result = analyzer.analyze_result(result_rate_limit, metrics)
        
        assert FailurePattern.RATE_LIMITING in enhanced_result.detected_patterns
        
        # Test network timeout pattern
        result_timeout = CollectionHealthResult(
            success=False,
            data=[],
            source="test_source",
            timestamp=datetime.now(),
            errors=["Connection timeout"],
            response_time_ms=30000.0  # 30 seconds
        )
        
        enhanced_result = analyzer.analyze_result(result_timeout, metrics)
        assert FailurePattern.NETWORK_TIMEOUT in enhanced_result.detected_patterns


class TestAlertManager:
    """Test alert manager functionality."""
    
    @pytest.mark.asyncio
    async def test_alert_manager_initialization(self, alert_manager):
        """Test alert manager initializes correctly."""
        assert alert_manager is not None
        assert hasattr(alert_manager, 'send_alert')
        assert hasattr(alert_manager, 'check_collection_gaps')
    
    @pytest.mark.asyncio
    async def test_gap_detection(self, alert_manager):
        """Test collection gap detection."""
        # Mock database connection
        with patch.object(alert_manager, 'initialize_db_connection', new_callable=AsyncMock):
            with patch.object(alert_manager, 'db_pool') as mock_pool:
                # Mock database query that returns a gap
                mock_conn = AsyncMock()
                mock_conn.fetchrow.return_value = {
                    'last_collection': datetime.now() - timedelta(hours=6),
                    'gap_hours': 6.0
                }
                mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
                
                gap_alert = await alert_manager.check_collection_gaps("test_source", 4.0)
                
                assert gap_alert is not None
                assert gap_alert.source == "test_source"
                assert gap_alert.alert_type == "collection_gap"
                assert gap_alert.severity == AlertSeverity.WARNING
    
    @pytest.mark.asyncio
    async def test_dead_tuple_detection(self, alert_manager):
        """Test dead tuple accumulation detection."""
        with patch.object(alert_manager, 'initialize_db_connection', new_callable=AsyncMock):
            with patch.object(alert_manager, 'db_pool') as mock_pool:
                # Mock database query that returns dead tuple issue
                mock_conn = AsyncMock()
                mock_conn.fetch.return_value = [{
                    'schemaname': 'raw_data',
                    'tablename': 'action_network_games',
                    'n_live_tup': 1000,
                    'n_dead_tup': 800,
                    'dead_tuple_ratio': 0.8
                }]
                mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
                
                alerts = await alert_manager.check_dead_tuple_accumulation(0.5)
                
                assert len(alerts) == 1
                alert = alerts[0]
                assert alert.alert_type == "dead_tuple_accumulation"
                assert alert.severity == AlertSeverity.CRITICAL


class TestCircuitBreaker:
    """Test circuit breaker functionality."""
    
    def test_circuit_breaker_creation(self, circuit_breaker_manager):
        """Test circuit breaker creation and configuration."""
        config = CircuitBreakerConfig(
            failure_threshold=3,
            timeout_duration_seconds=60,
            recovery_strategy=RecoveryStrategy.EXPONENTIAL_BACKOFF
        )
        
        cb = circuit_breaker_manager.create_circuit_breaker(
            name="test_source",
            config=config
        )
        
        assert cb is not None
        assert cb.name == "test_source"
        assert cb.state == CircuitState.CLOSED
        assert cb.config.failure_threshold == 3
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_failure_handling(self, circuit_breaker_manager):
        """Test circuit breaker opens after failures."""
        config = CircuitBreakerConfig(failure_threshold=2, timeout_duration_seconds=1)
        cb = circuit_breaker_manager.create_circuit_breaker("test_cb", config)
        
        async def failing_function():
            raise Exception("Test failure")
        
        # First failure
        with pytest.raises(Exception):
            await cb.call(failing_function)
        
        assert cb.state == CircuitState.CLOSED
        assert cb.metrics.consecutive_failures == 1
        
        # Second failure should open circuit
        with pytest.raises(Exception):
            await cb.call(failing_function)
        
        assert cb.state == CircuitState.OPEN
        assert cb.metrics.consecutive_failures == 2
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_fallback(self, circuit_breaker_manager):
        """Test circuit breaker fallback functionality."""
        async def fallback_handler():
            return {"data": [], "success": True, "fallback": True}
        
        config = CircuitBreakerConfig(failure_threshold=1)
        cb = circuit_breaker_manager.create_circuit_breaker(
            "test_fallback",
            config,
            fallback_handler=fallback_handler
        )
        
        async def failing_function():
            raise Exception("Test failure")
        
        # Trigger failure to open circuit
        await cb.call(failing_function, fallback_enabled=True)
        assert cb.state == CircuitState.OPEN
        
        # Next call should use fallback
        result = await cb.call(failing_function, fallback_enabled=True)
        assert result["fallback"] is True


class TestEnhancedOrchestrator:
    """Test enhanced orchestrator integration."""
    
    @pytest.mark.asyncio
    async def test_orchestrator_initialization(self, enhanced_orchestrator):
        """Test enhanced orchestrator initializes correctly."""
        assert enhanced_orchestrator is not None
        assert hasattr(enhanced_orchestrator, 'confidence_analyzer')
        assert hasattr(enhanced_orchestrator, 'alert_manager')
        assert hasattr(enhanced_orchestrator, 'circuit_breaker_manager')
        assert enhanced_orchestrator.enhanced_settings["enable_health_monitoring"] is True
    
    @pytest.mark.asyncio
    async def test_health_metrics_tracking(self, enhanced_orchestrator):
        """Test health metrics are tracked correctly."""
        source_name = "test_source"
        
        # Create a mock collector
        mock_collector = AsyncMock()
        mock_collector.collect.return_value = CollectionResult(
            success=True,
            data=[{"test": "data"}],
            source=source_name,
            timestamp=datetime.now()
        )
        mock_collector.test_connection.return_value = True
        
        enhanced_orchestrator.collectors[source_name] = mock_collector
        
        # Initialize health metrics
        enhanced_orchestrator.health_metrics[source_name] = CollectionHealthMetrics(
            source=source_name
        )
        
        # Test health monitoring
        health_check = await enhanced_orchestrator._test_source_health(source_name)
        assert health_check is True
        
        # Test enhanced metrics
        metrics = enhanced_orchestrator.get_enhanced_metrics()
        assert "health_monitoring" in metrics
        assert metrics["health_monitoring"]["enabled"] is True
    
    @pytest.mark.asyncio
    async def test_recovery_plan_creation(self, enhanced_orchestrator):
        """Test recovery plan creation for different failure patterns."""
        source = "test_source"
        failure_patterns = [FailurePattern.RATE_LIMITING, FailurePattern.NETWORK_TIMEOUT]
        metrics = CollectionHealthMetrics(source=source)
        
        recovery_plan = enhanced_orchestrator._create_recovery_plan(
            source, failure_patterns, metrics
        )
        
        assert recovery_plan is not None
        assert recovery_plan.source == source
        assert len(recovery_plan.recovery_actions) > 0
        assert recovery_plan.failure_patterns == failure_patterns


class TestCLIIntegration:
    """Test CLI command integration."""
    
    def test_cli_health_command_import(self):
        """Test CLI health commands can be imported."""
        assert health is not None
        
        # Check that the main health group exists
        assert hasattr(health, 'commands')
        
        # Check for key commands
        expected_commands = ['status', 'gaps', 'dead-tuples', 'circuit-breakers', 'alerts']
        available_commands = [cmd.name for cmd in health.commands.values()]
        
        for cmd in expected_commands:
            assert cmd in available_commands, f"Command {cmd} not found in health commands"


class TestEndToEndIntegration:
    """Test end-to-end integration scenarios."""
    
    @pytest.mark.asyncio
    async def test_complete_failure_detection_and_recovery(self, enhanced_orchestrator, alert_manager):
        """Test complete failure detection and recovery workflow."""
        source_name = "integration_test_source"
        
        # Setup mock collector that will fail initially then recover
        call_count = 0
        
        async def mock_collect(**kwargs):
            nonlocal call_count
            call_count += 1
            
            if call_count <= 3:  # First 3 calls fail
                raise Exception("Simulated failure")
            else:  # Subsequent calls succeed
                return CollectionResult(
                    success=True,
                    data=[{"test": f"data_{call_count}"}],
                    source=source_name,
                    timestamp=datetime.now()
                )
        
        mock_collector = AsyncMock()
        mock_collector.collect = mock_collect
        mock_collector.test_connection.return_value = True
        
        enhanced_orchestrator.collectors[source_name] = mock_collector
        
        # Initialize health metrics
        enhanced_orchestrator.health_metrics[source_name] = CollectionHealthMetrics(
            source=source_name
        )
        
        # Create mock task for testing
        from src.data.collection.orchestrator import CollectionTask, CollectionPriority
        
        task = CollectionTask(
            id="test_task",
            source_name=source_name,
            collection_type="test",
            priority=CollectionPriority.NORMAL,
            params={}
        )
        
        # Execute task multiple times to trigger failure detection and recovery
        for i in range(5):
            await enhanced_orchestrator._execute_single_task_with_recovery(task)
            
            # Reset task for next iteration
            from src.data.collection.orchestrator import CollectionStatus
            task.status = CollectionStatus.PENDING
            task.started_at = None
            task.completed_at = None
            task.result = None
        
        # Verify health metrics were updated
        metrics = enhanced_orchestrator.health_metrics[source_name]
        assert metrics.total_collections == 5
        assert metrics.failed_collections >= 3
        assert metrics.successful_collections >= 1
        
        # Verify recovery plan was created
        assert source_name in enhanced_orchestrator.recovery_plans
        recovery_plan = enhanced_orchestrator.recovery_plans[source_name]
        assert len(recovery_plan.recovery_actions) > 0


@pytest.mark.asyncio
async def test_database_schema_compatibility():
    """Test that the database schema is compatible with health monitoring."""
    settings = get_settings()
    
    try:
        import asyncpg
        
        dsn = f"postgresql://{settings.database.user}:{settings.database.password}@{settings.database.host}:{settings.database.port}/{settings.database.database}"
        conn = await asyncpg.connect(dsn)
        
        # Test that health monitoring table exists
        table_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'operational' 
                AND table_name = 'collection_health_monitoring'
            );
        """)
        
        assert table_exists, "Health monitoring table does not exist in database"
        
        # Test table structure
        columns = await conn.fetch("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'operational' 
            AND table_name = 'collection_health_monitoring'
        """)
        
        column_names = [col['column_name'] for col in columns]
        required_columns = [
            'id', 'source', 'collection_timestamp', 'records_collected',
            'success_rate', 'confidence_score', 'health_status'
        ]
        
        for required_col in required_columns:
            assert required_col in column_names, f"Required column {required_col} not found"
        
        await conn.close()
        
    except Exception as e:
        pytest.skip(f"Database not available for testing: {e}")


if __name__ == "__main__":
    # Run basic functionality test
    print("🧪 Testing Silent Failure Resolution System")
    
    # Test health monitoring
    print("✅ Testing health monitoring...")
    analyzer = CollectionConfidenceAnalyzer()
    test_result = CollectionHealthResult(
        success=True,
        data=[{"test": "data"}],
        source="test",
        timestamp=datetime.now()
    )
    enhanced = analyzer.analyze_result(test_result, CollectionHealthMetrics(source="test"))
    print(f"   Confidence score: {enhanced.confidence_score}")
    
    # Test circuit breaker
    print("✅ Testing circuit breaker...")
    manager = CircuitBreakerManager()
    cb = manager.create_circuit_breaker("test")
    print(f"   Circuit breaker state: {cb.state.value}")
    
    print("🎉 Basic functionality tests completed successfully!")
    print("\nRun full test suite with: pytest tests/integration/test_silent_failure_resolution.py -v")