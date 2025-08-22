#!/usr/bin/env python3
"""
Integration Tests for Collection Health Monitoring

Comprehensive tests for Issue #36: Missing Collection Health Monitoring
including real-time health service, enhanced alerting, and dashboard integration.
"""

import asyncio
import pytest
import pytest_asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch

from src.core.config import UnifiedSettings
from src.data.collection.base import CollectionResult
from src.services.monitoring.realtime_collection_health_service import (
    RealTimeCollectionHealthService,
    LiveHealthMetrics,
    HealthTrend,
    PerformanceDegradation,
)
from src.services.monitoring.enhanced_alerting_service import (
    EnhancedAlertingService,
    Alert,
    AlertSeverity,
    AlertPriority,
    AlertChannel,
)
from src.services.monitoring.health_dashboard_integration import (
    HealthDashboardIntegration,
)
from src.services.monitoring.collector_health_service import (
    HealthStatus,
    HealthMonitoringOrchestrator,
)


class TestRealTimeCollectionHealthService:
    """Test real-time collection health service functionality."""

    @pytest_asyncio.fixture
    async def health_service(self):
        """Create health service instance for testing."""
        settings = UnifiedSettings()
        service = RealTimeCollectionHealthService(settings)
        
        # Mock health orchestrator
        mock_orchestrator = Mock(spec=HealthMonitoringOrchestrator)
        mock_orchestrator.monitors = {}
        
        await service.initialize(mock_orchestrator)
        yield service
        await service.stop()

    @pytest.mark.asyncio
    async def test_track_collection_attempt_success(self, health_service):
        """Test tracking successful collection attempts."""
        # Create successful collection result
        result = CollectionResult(
            success=True,
            data=[{"game_id": "test1", "home_team": "BOS", "away_team": "NYY"}],
            source="test_collector",
            timestamp=datetime.now(),
            errors=[]
        )
        result.response_time_ms = 2500.0  # 2.5 seconds in milliseconds
        result.quality_score = 0.95
        
        # Track collection attempt
        await health_service.track_collection_attempt("test_collector", result)
        
        # Verify metrics are updated
        metrics = await health_service.get_live_health_status("test_collector")
        assert metrics is not None
        assert metrics.collector_name == "test_collector"
        assert metrics.consecutive_successes == 1
        assert metrics.consecutive_failures == 0
        assert metrics.last_successful_collection is not None
        assert metrics.current_status == HealthStatus.HEALTHY

    @pytest.mark.asyncio
    async def test_track_collection_attempt_failure(self, health_service):
        """Test tracking failed collection attempts."""
        # Create failed collection result
        result = CollectionResult(
            success=False,
            data=[],
            source="test_collector",
            timestamp=datetime.now(),
            errors=["Network timeout", "Connection refused"]
        )
        result.response_time_ms = 10000.0  # 10 seconds in milliseconds
        
        # Track collection attempt
        await health_service.track_collection_attempt("test_collector", result)
        
        # Verify metrics are updated
        metrics = await health_service.get_live_health_status("test_collector")
        assert metrics is not None
        assert metrics.consecutive_failures == 1
        assert metrics.consecutive_successes == 0
        assert metrics.last_failed_collection is not None

    @pytest.mark.asyncio
    async def test_performance_degradation_detection(self, health_service):
        """Test performance degradation detection."""
        # Set up baseline
        health_service.performance_baselines["test_collector"] = {
            "response_time": 2.0,
            "success_rate": 0.95
        }
        
        # Create metrics with degraded performance
        metrics = LiveHealthMetrics(
            collector_name="test_collector",
            current_status=HealthStatus.DEGRADED,
            avg_response_time_1h=6.0,  # 3x baseline
            success_rate_1h=0.7,       # Below baseline
        )
        health_service.live_metrics["test_collector"] = metrics
        
        # Detect degradations
        degradations = await health_service.detect_performance_degradation()
        
        # Verify degradations detected
        assert len(degradations) >= 1
        response_time_degradation = next(
            (d for d in degradations if d.degradation_type == "response_time"), None
        )
        assert response_time_degradation is not None
        assert response_time_degradation.severity == AlertSeverity.CRITICAL
        assert response_time_degradation.current_value == 6.0
        assert response_time_degradation.baseline_value == 2.0

    @pytest.mark.asyncio
    async def test_failure_pattern_analysis(self, health_service):
        """Test failure pattern analysis."""
        # Create pattern of periodic failures
        collector_name = "test_collector"
        base_time = datetime.now()
        
        # Simulate regular failure pattern every 10 minutes
        for i in range(5):
            failure_time = base_time + timedelta(minutes=i * 10)
            attempt_data = {
                "timestamp": failure_time,
                "success": False,
                "response_time": 5.0,
                "data_quality": 0.5,
                "record_count": 0,
                "errors": ["Periodic system issue"]
            }
            health_service.collection_history[collector_name].append(attempt_data)
        
        # Analyze patterns
        patterns = await health_service.analyze_failure_patterns()
        
        # Verify pattern detection
        collector_patterns = [p for p in patterns if p.collector_name == collector_name]
        assert len(collector_patterns) > 0
        
        pattern = collector_patterns[0]
        assert pattern.pattern_type == "periodic_failures"
        assert pattern.frequency == 5
        assert pattern.confidence_score > 0.5

    @pytest.mark.asyncio
    async def test_failure_probability_prediction(self, health_service):
        """Test failure probability prediction."""
        collector_name = "test_collector"
        
        # Create history with increasing failure rate
        for i in range(20):
            success = i < 15  # First 15 succeed, last 5 fail
            attempt_data = {
                "timestamp": datetime.now() - timedelta(minutes=20-i),
                "success": success,
                "response_time": 3.0 + (i * 0.2),  # Increasing response time
                "data_quality": 0.9 - (i * 0.02),
                "record_count": 10 if success else 0,
                "errors": [] if success else ["Increasing issues"]
            }
            health_service.collection_history[collector_name].append(attempt_data)
        
        # Predict failure probability
        probability = await health_service.predict_failure_probability(collector_name)
        
        # Should predict high failure probability due to recent failures
        assert probability > 0.3
        assert probability <= 0.95  # Capped at reasonable maximum

    @pytest.mark.asyncio
    async def test_recovery_actions(self, health_service):
        """Test automated recovery actions."""
        collector_name = "test_collector"
        failure_context = {
            "consecutive_failures": 3,
            "error_type": "connection_timeout"
        }
        
        # Trigger recovery actions
        recovery_results = await health_service.trigger_recovery_actions(
            collector_name, failure_context
        )
        
        # Verify recovery actions were attempted
        assert len(recovery_results) > 0
        
        # Check action types
        action_types = [r.action_type for r in recovery_results]
        assert "circuit_breaker_reset" in action_types
        assert "health_check_diagnostic" in action_types
        assert "configuration_validation" in action_types
        
        # Verify all actions have results
        for result in recovery_results:
            assert result.collector_name == collector_name
            assert result.execution_time >= 0
            assert result.result_message is not None


class TestEnhancedAlertingService:
    """Test enhanced alerting service functionality."""

    @pytest_asyncio.fixture
    async def alerting_service(self):
        """Create alerting service instance for testing."""
        settings = UnifiedSettings()
        service = EnhancedAlertingService(settings)
        await service.initialize()
        yield service
        await service.stop()

    @pytest.mark.asyncio
    async def test_send_basic_alert(self, alerting_service):
        """Test sending basic alerts."""
        delivery_results = await alerting_service.send_alert(
            alert_type="test_alert",
            severity=AlertSeverity.WARNING,
            title="Test Alert",
            message="This is a test alert message",
            collector_name="test_collector",
            metadata={"test_key": "test_value"}
        )
        
        # Verify alert was delivered (at least to console)
        assert len(delivery_results) > 0
        console_result = next(
            (r for r in delivery_results if r.channel == AlertChannel.CONSOLE), None
        )
        assert console_result is not None
        assert console_result.success is True

    @pytest.mark.asyncio
    async def test_alert_throttling(self, alerting_service):
        """Test alert throttling functionality."""
        # Send first alert
        results1 = await alerting_service.send_alert(
            alert_type="throttle_test",
            severity=AlertSeverity.WARNING,
            title="First Alert",
            message="First test message",
            collector_name="test_collector"
        )
        
        # Send second alert immediately (should be throttled)
        results2 = await alerting_service.send_alert(
            alert_type="throttle_test",
            severity=AlertSeverity.WARNING,
            title="Second Alert", 
            message="Second test message",
            collector_name="test_collector"
        )
        
        # First alert should be delivered, second should be throttled
        assert len(results1) > 0
        assert len(results2) == 0

    @pytest.mark.asyncio
    async def test_performance_degradation_alert(self, alerting_service):
        """Test performance degradation alert generation."""
        # Create performance degradation
        degradation = PerformanceDegradation(
            collector_name="test_collector",
            degradation_type="response_time",
            severity=AlertSeverity.CRITICAL,
            current_value=10.0,
            baseline_value=2.0,
            deviation_percentage=400.0,
            detection_time=datetime.now(),
            recommended_actions=["Check network", "Restart service"]
        )
        
        # Send degradation alert
        delivery_results = await alerting_service.send_performance_degradation_alert(degradation)
        
        # Verify alert was created and delivered
        assert len(delivery_results) > 0
        successful_deliveries = [r for r in delivery_results if r.success]
        assert len(successful_deliveries) > 0

    @pytest.mark.asyncio
    async def test_alert_acknowledgment_and_resolution(self, alerting_service):
        """Test alert acknowledgment and resolution."""
        # Create alert
        await alerting_service.send_alert(
            alert_type="test_resolution",
            severity=AlertSeverity.WARNING,
            title="Test Resolution Alert",
            message="Alert for testing resolution",
            collector_name="test_collector"
        )
        
        # Get active alerts to find the alert ID
        active_alerts = await alerting_service.get_active_alerts()
        assert len(active_alerts) > 0
        
        test_alert = active_alerts[0]
        
        # Acknowledge alert
        ack_success = await alerting_service.acknowledge_alert(
            test_alert.id, "test_user"
        )
        assert ack_success is True
        
        # Verify acknowledgment
        updated_alert = alerting_service.active_alerts.get(test_alert.id)
        assert updated_alert is not None
        assert updated_alert.acknowledged is True
        
        # Resolve alert
        resolve_success = await alerting_service.resolve_alert(
            test_alert.id, "test_user", "Issue resolved"
        )
        assert resolve_success is True
        
        # Verify resolution (should be removed from active alerts)
        assert test_alert.id not in alerting_service.active_alerts

    @pytest.mark.asyncio
    async def test_alert_statistics(self, alerting_service):
        """Test alert statistics generation."""
        # Create multiple alerts with different severities
        await alerting_service.send_alert(
            alert_type="stats_test",
            severity=AlertSeverity.CRITICAL,
            title="Critical Alert",
            message="Critical test alert",
            collector_name="collector1"
        )
        
        await alerting_service.send_alert(
            alert_type="stats_test",
            severity=AlertSeverity.WARNING,
            title="Warning Alert", 
            message="Warning test alert",
            collector_name="collector2"
        )
        
        # Get statistics
        stats = await alerting_service.get_alert_statistics()
        
        # Verify statistics structure
        assert "active_alerts" in stats
        assert "alerts_by_severity" in stats
        assert "alerts_by_type" in stats
        assert "delivery_statistics" in stats
        
        # Verify alert counts
        assert stats["active_alerts"] >= 2
        assert stats["alerts_by_severity"].get("critical", 0) >= 1
        assert stats["alerts_by_severity"].get("warning", 0) >= 1


class TestHealthDashboardIntegration:
    """Test health dashboard integration functionality."""

    @pytest_asyncio.fixture
    async def dashboard_integration(self):
        """Create dashboard integration service for testing."""
        settings = UnifiedSettings()
        service = HealthDashboardIntegration(settings)
        
        # Mock the underlying services
        with patch('src.services.monitoring.health_dashboard_integration.get_realtime_health_service') as mock_realtime, \
             patch('src.services.monitoring.health_dashboard_integration.get_enhanced_alerting_service') as mock_alerting:
            
            # Create mock services
            mock_realtime_service = Mock(spec=RealTimeCollectionHealthService)
            mock_alerting_service = Mock(spec=EnhancedAlertingService)
            
            mock_realtime.return_value = mock_realtime_service
            mock_alerting.return_value = mock_alerting_service
            
            # Setup mock returns
            mock_realtime_service.get_all_live_health_status.return_value = {
                "test_collector": LiveHealthMetrics(
                    collector_name="test_collector",
                    current_status=HealthStatus.HEALTHY,
                    success_rate_1h=0.95,
                    success_rate_24h=0.93,
                    avg_response_time_1h=2.1,
                    data_quality_score=0.89,
                    consecutive_failures=0,
                    consecutive_successes=10,
                    health_trend=HealthTrend.STABLE
                )
            }
            
            mock_alerting_service.get_alert_statistics.return_value = {
                "active_alerts": 2,
                "alerts_by_severity": {"warning": 1, "critical": 1},
                "escalated_alerts": 1
            }
            
            mock_alerting_service.get_active_alerts.return_value = [
                Alert(
                    id="test_alert_1",
                    alert_type="test_alert",
                    severity=AlertSeverity.WARNING,
                    priority=AlertPriority.MEDIUM,
                    title="Test Alert",
                    message="Test alert message",
                    collector_name="test_collector"
                )
            ]
            
            await service.initialize()
            yield service
            await service.stop()

    @pytest.mark.asyncio
    async def test_get_dashboard_health_summary(self, dashboard_integration):
        """Test dashboard health summary generation."""
        summary = await dashboard_integration.get_dashboard_health_summary()
        
        # Verify summary structure
        assert "overall_status" in summary
        assert "total_collectors" in summary
        assert "healthy_collectors" in summary
        assert "degraded_collectors" in summary
        assert "critical_collectors" in summary
        assert "active_alerts" in summary
        assert "avg_success_rate" in summary
        assert "avg_response_time" in summary
        assert "last_updated" in summary
        
        # Verify data makes sense
        assert summary["total_collectors"] >= 0
        assert 0.0 <= summary["avg_success_rate"] <= 1.0
        assert summary["avg_response_time"] >= 0.0

    @pytest.mark.asyncio
    async def test_get_collector_health_widgets(self, dashboard_integration):
        """Test collector health widget data generation."""
        widget_data = await dashboard_integration.get_collector_health_widgets()
        
        # Verify structure
        assert "collectors" in widget_data
        assert "summary_stats" in widget_data
        assert "last_updated" in widget_data
        
        # Verify collector data
        collectors = widget_data["collectors"]
        if collectors:
            collector = collectors[0]
            assert "name" in collector
            assert "status" in collector
            assert "success_rate_1h" in collector
            assert "health_trend" in collector
            assert "status_color" in collector
            assert "trend_indicator" in collector
        
        # Verify summary stats
        summary_stats = widget_data["summary_stats"]
        assert "total" in summary_stats
        assert "healthy" in summary_stats
        assert "health_percentage" in summary_stats

    @pytest.mark.asyncio
    async def test_get_performance_trends(self, dashboard_integration):
        """Test performance trend data generation."""
        trends = await dashboard_integration.get_performance_trends(hours=24)
        
        # Verify structure
        assert "trends" in trends
        assert "time_labels" in trends
        assert "generated_at" in trends
        
        # Verify trends data structure
        trends_data = trends["trends"]
        if trends_data:
            collector_trends = list(trends_data.values())[0]
            assert "success_rate" in collector_trends
            assert "response_time" in collector_trends
            assert "data_quality" in collector_trends
            
            # Verify time series structure
            success_trend = collector_trends["success_rate"]
            if success_trend:
                data_point = success_trend[0]
                assert "timestamp" in data_point
                assert "value" in data_point

    @pytest.mark.asyncio
    async def test_get_alert_dashboard_data(self, dashboard_integration):
        """Test alert dashboard data generation."""
        alert_data = await dashboard_integration.get_alert_dashboard_data()
        
        # Verify structure
        assert "active_alerts" in alert_data
        assert "alert_summary" in alert_data
        assert "recent_activity" in alert_data
        assert "last_updated" in alert_data
        
        # Verify active alerts format
        active_alerts = alert_data["active_alerts"]
        if active_alerts:
            alert = active_alerts[0]
            assert "id" in alert
            assert "title" in alert
            assert "severity" in alert
            assert "severity_color" in alert
            assert "age_minutes" in alert
        
        # Verify alert summary
        alert_summary = alert_data["alert_summary"]
        assert "total_active" in alert_summary
        assert "by_severity" in alert_summary

    @pytest.mark.asyncio
    async def test_websocket_broadcast(self, dashboard_integration):
        """Test WebSocket broadcast functionality."""
        # Mock WebSocket connection
        mock_websocket = Mock()
        mock_websocket.send_text = AsyncMock()
        
        # Register connection
        await dashboard_integration.register_websocket_connection(mock_websocket)
        assert mock_websocket in dashboard_integration.websocket_connections
        
        # Broadcast update
        test_data = {"test": "data", "timestamp": datetime.now().isoformat()}
        await dashboard_integration.broadcast_health_update(test_data)
        
        # Verify message was sent
        mock_websocket.send_text.assert_called_once()
        call_args = mock_websocket.send_text.call_args[0][0]
        message = eval(call_args)  # Parse JSON string
        assert message["type"] == "health_update"
        assert message["data"]["test"] == "data"
        
        # Unregister connection
        await dashboard_integration.unregister_websocket_connection(mock_websocket)
        assert mock_websocket not in dashboard_integration.websocket_connections


class TestIntegrationScenarios:
    """Test integrated scenarios combining multiple services."""

    @pytest.mark.asyncio
    async def test_end_to_end_failure_detection_and_alerting(self):
        """Test complete failure detection and alerting flow."""
        settings = UnifiedSettings()
        
        # Initialize services
        health_service = RealTimeCollectionHealthService(settings)
        alerting_service = EnhancedAlertingService(settings)
        
        # Mock health orchestrator
        mock_orchestrator = Mock(spec=HealthMonitoringOrchestrator)
        mock_orchestrator.monitors = {}
        
        await health_service.initialize(mock_orchestrator)
        await alerting_service.initialize()
        
        try:
            collector_name = "integration_test_collector"
            
            # Simulate multiple collection failures
            for i in range(5):
                result = CollectionResult(
                    success=False,
                    data=[],
                    source=collector_name,
                    timestamp=datetime.now(),
                    errors=[f"Connection failed - attempt {i+1}"]
                )
                result.response_time_ms = (8.0 + i) * 1000  # Convert to milliseconds
                
                await health_service.track_collection_attempt(collector_name, result)
            
            # Check that health status reflects failures
            metrics = await health_service.get_live_health_status(collector_name)
            assert metrics is not None
            assert metrics.consecutive_failures == 5
            assert metrics.current_status in [HealthStatus.DEGRADED, HealthStatus.CRITICAL]
            
            # Detect performance degradations
            degradations = await health_service.detect_performance_degradation()
            collector_degradations = [d for d in degradations if d.collector_name == collector_name]
            
            # Send alerts for detected issues
            for degradation in collector_degradations:
                delivery_results = await alerting_service.send_performance_degradation_alert(degradation)
                assert len(delivery_results) > 0
            
            # Verify alerts were created
            active_alerts = await alerting_service.get_active_alerts()
            collector_alerts = [a for a in active_alerts if a.collector_name == collector_name]
            assert len(collector_alerts) > 0
            
            # Trigger recovery actions
            failure_context = {"consecutive_failures": 5, "alert_triggered": True}
            recovery_results = await health_service.trigger_recovery_actions(
                collector_name, failure_context
            )
            assert len(recovery_results) > 0
            
            # Send recovery alert
            recovery_success = any(r.success for r in recovery_results)
            await alerting_service.send_recovery_alert(
                collector_name, recovery_success, {"recovery_attempts": len(recovery_results)}
            )
            
        finally:
            await health_service.stop()
            await alerting_service.stop()

    @pytest.mark.asyncio
    async def test_dashboard_integration_with_real_services(self):
        """Test dashboard integration with real service instances."""
        settings = UnifiedSettings()
        
        # Initialize all services
        health_service = RealTimeCollectionHealthService(settings) 
        alerting_service = EnhancedAlertingService(settings)
        dashboard_integration = HealthDashboardIntegration(settings)
        
        # Mock health orchestrator
        mock_orchestrator = Mock(spec=HealthMonitoringOrchestrator)
        mock_orchestrator.monitors = {}
        
        await health_service.initialize(mock_orchestrator)
        await alerting_service.initialize()
        
        # Mock service instances in dashboard integration
        dashboard_integration.realtime_health_service = health_service
        dashboard_integration.alerting_service = alerting_service
        
        try:
            # Create some test data
            collector_name = "dashboard_test_collector"
            
            # Add some collection history
            result = CollectionResult(
                success=True,
                data=[{"test": "data"}],
                source=collector_name,
                timestamp=datetime.now(),
                errors=[]
            )
            result.response_time_ms = 2300.0  # 2.3 seconds in milliseconds
            result.quality_score = 0.92
            
            await health_service.track_collection_attempt(collector_name, result)
            
            # Create an alert
            await alerting_service.send_alert(
                alert_type="dashboard_test",
                severity=AlertSeverity.WARNING,
                title="Dashboard Test Alert",
                message="Testing dashboard integration",
                collector_name=collector_name
            )
            
            # Test dashboard data retrieval
            health_summary = await dashboard_integration.get_dashboard_health_summary()
            assert health_summary["total_collectors"] > 0
            
            collector_widgets = await dashboard_integration.get_collector_health_widgets()
            assert len(collector_widgets["collectors"]) > 0
            
            alert_data = await dashboard_integration.get_alert_dashboard_data()
            assert alert_data["alert_summary"]["total_active"] > 0
            
            trends = await dashboard_integration.get_performance_trends(hours=1)
            assert "trends" in trends
            
        finally:
            await health_service.stop()
            await alerting_service.stop()
            await dashboard_integration.stop()


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v", "--asyncio-mode=auto"])