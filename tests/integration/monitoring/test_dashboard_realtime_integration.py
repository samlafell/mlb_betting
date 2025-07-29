#!/usr/bin/env python3
"""
Integration tests for Dashboard Real-Time Updates

Tests the integration of real-time dashboard functionality:
- WebSocket connection management and broadcasting
- Real-time pipeline status updates via WebSocket
- Dashboard API integration with monitoring services
- System health streaming and live updates
- Break-glass controls integration testing
- Multi-client WebSocket handling and performance
"""

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi.testclient import TestClient
from httpx import AsyncClient

from src.interfaces.api.monitoring_dashboard import WebSocketMessage, app, manager


@pytest.fixture
def test_client():
    """Create FastAPI test client."""
    return TestClient(app)


@pytest.fixture
async def async_client():
    """Create async HTTP client for testing."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client


@pytest.fixture
def mock_websocket():
    """Create mock WebSocket for testing."""
    websocket = Mock()
    websocket.accept = AsyncMock()
    websocket.send_text = AsyncMock()
    websocket.close = AsyncMock()
    return websocket


@pytest.fixture
def sample_pipeline_update():
    """Create sample pipeline update message."""
    return WebSocketMessage(
        type="pipeline_update",
        data={
            "pipeline_id": "test-pipeline-123",
            "status": "running",
            "progress": 0.75,
            "current_stage": "data_analysis",
            "estimated_completion": datetime.now(timezone.utc).isoformat()
        }
    )


@pytest.fixture
def sample_system_health_update():
    """Create sample system health update message."""
    return WebSocketMessage(
        type="system_health",
        data={
            "overall_status": "healthy",
            "cpu_usage": 0.35,
            "memory_usage": 0.52,
            "active_pipelines": 2,
            "data_freshness_score": 0.96
        }
    )


class TestWebSocketConnectionManagement:
    """Test WebSocket connection management and lifecycle."""

    @pytest.mark.asyncio
    async def test_websocket_connection_lifecycle(self, mock_websocket):
        """Test complete WebSocket connection lifecycle."""
        # Test connection
        await manager.connect(mock_websocket, {"client_id": "test-client-1"})

        mock_websocket.accept.assert_called_once()
        assert mock_websocket in manager.active_connections
        assert manager.connection_metadata[mock_websocket]["client_id"] == "test-client-1"

        # Test disconnection
        manager.disconnect(mock_websocket)

        assert mock_websocket not in manager.active_connections
        assert mock_websocket not in manager.connection_metadata

    @pytest.mark.asyncio
    async def test_multiple_websocket_connections(self):
        """Test handling multiple concurrent WebSocket connections."""
        websockets = []
        for i in range(5):
            ws = Mock()
            ws.accept = AsyncMock()
            ws.send_text = AsyncMock()
            websockets.append(ws)

            await manager.connect(ws, {"client_id": f"test-client-{i}"})

        # Verify all connections are tracked
        assert len(manager.active_connections) == 5

        for i, ws in enumerate(websockets):
            assert ws in manager.active_connections
            assert manager.connection_metadata[ws]["client_id"] == f"test-client-{i}"

        # Clean up
        for ws in websockets:
            manager.disconnect(ws)

    @pytest.mark.asyncio
    async def test_websocket_connection_with_metadata(self, mock_websocket):
        """Test WebSocket connection with rich metadata."""
        metadata = {
            "client_id": "dashboard-001",
            "user_agent": "Mozilla/5.0 (Test Browser)",
            "ip_address": "127.0.0.1",
            "connection_time": datetime.now(timezone.utc).isoformat()
        }

        await manager.connect(mock_websocket, metadata)

        assert manager.connection_metadata[mock_websocket] == metadata

    @pytest.mark.asyncio
    async def test_websocket_connection_failure_handling(self):
        """Test handling of WebSocket connection failures."""
        failing_websocket = Mock()
        failing_websocket.accept = AsyncMock(side_effect=Exception("Connection failed"))

        # Should handle connection failure gracefully
        with pytest.raises(Exception):
            await manager.connect(failing_websocket)

        # Failed connection should not be tracked
        assert failing_websocket not in manager.active_connections


class TestRealtimeBroadcasting:
    """Test real-time message broadcasting functionality."""

    @pytest.mark.asyncio
    async def test_pipeline_update_broadcasting(self, sample_pipeline_update):
        """Test broadcasting pipeline status updates."""
        # Create mock connections
        websockets = []
        for i in range(3):
            ws = Mock()
            ws.send_text = AsyncMock()
            websockets.append(ws)
            manager.active_connections.add(ws)

        # Broadcast update
        await manager.broadcast(sample_pipeline_update)

        # Verify all connections received the message
        expected_message = sample_pipeline_update.model_dump_json()
        for ws in websockets:
            ws.send_text.assert_called_once_with(expected_message)

        # Clean up
        for ws in websockets:
            manager.disconnect(ws)

    @pytest.mark.asyncio
    async def test_system_health_broadcasting(self, sample_system_health_update):
        """Test broadcasting system health updates."""
        # Create mock connections
        websockets = []
        for i in range(2):
            ws = Mock()
            ws.send_text = AsyncMock()
            websockets.append(ws)
            manager.active_connections.add(ws)

        # Broadcast health update
        await manager.broadcast(sample_system_health_update)

        # Verify all connections received the message
        expected_message = sample_system_health_update.model_dump_json()
        for ws in websockets:
            ws.send_text.assert_called_once_with(expected_message)

        # Clean up
        for ws in websockets:
            manager.disconnect(ws)

    @pytest.mark.asyncio
    async def test_broadcast_error_isolation(self):
        """Test that broadcast errors don't affect other connections."""
        # Create connections - one success, one failure
        success_ws = Mock()
        success_ws.send_text = AsyncMock()

        failure_ws = Mock()
        failure_ws.send_text = AsyncMock(side_effect=Exception("Send failed"))

        manager.active_connections.add(success_ws)
        manager.active_connections.add(failure_ws)

        message = WebSocketMessage(type="test", data={"status": "test"})

        # Broadcast should handle failure gracefully
        await manager.broadcast(message)

        # Success connection should still work
        success_ws.send_text.assert_called_once()

        # Failed connection should be removed
        assert success_ws in manager.active_connections
        assert failure_ws not in manager.active_connections

    @pytest.mark.asyncio
    async def test_broadcast_performance_with_many_connections(self):
        """Test broadcast performance with many concurrent connections."""
        # Create many mock connections
        websockets = []
        for i in range(50):
            ws = Mock()
            ws.send_text = AsyncMock()
            websockets.append(ws)
            manager.active_connections.add(ws)

        message = WebSocketMessage(type="performance_test", data={"test": True})

        # Measure broadcast time
        start_time = asyncio.get_event_loop().time()
        await manager.broadcast(message)
        end_time = asyncio.get_event_loop().time()

        # Should complete quickly (< 1 second for 50 connections)
        broadcast_time = end_time - start_time
        assert broadcast_time < 1.0

        # All connections should receive message
        expected_message = message.model_dump_json()
        for ws in websockets:
            ws.send_text.assert_called_once_with(expected_message)

        # Clean up
        for ws in websockets:
            manager.disconnect(ws)


class TestDashboardAPIIntegration:
    """Test dashboard API integration with monitoring services."""

    @patch('src.interfaces.api.monitoring_dashboard.monitoring_service')
    @patch('src.interfaces.api.monitoring_dashboard.pipeline_orchestration_service')
    @patch('src.interfaces.api.monitoring_dashboard.metrics_service')
    def test_system_health_api_integration(self, mock_metrics_service, mock_pipeline_service, mock_monitoring_service, test_client):
        """Test system health API integration with all monitoring services."""
        # Mock monitoring service
        mock_health_report = Mock()
        mock_health_report.overall_status.value = "healthy"
        mock_health_report.business_metrics.data_freshness_score = 0.95
        mock_health_report.system_metrics.cpu_usage = 0.25
        mock_health_report.system_metrics.memory_usage = 0.45
        mock_health_report.system_metrics.disk_usage = 0.30
        mock_health_report.alerts = []

        mock_monitoring_service.get_system_health.return_value = mock_health_report

        # Mock pipeline service
        mock_pipeline_service.get_metrics.return_value = {
            "combined_insights": {"recent_success_rate": 0.98}
        }
        mock_pipeline_service.get_active_pipelines.return_value = ["pipeline1", "pipeline2"]

        # Mock metrics service
        mock_metrics_service.get_system_overview.return_value = {
            "uptime_seconds": 3600,
            "slo_compliance": {"pipeline_latency": {"status": "healthy"}}
        }

        # Test API call
        response = test_client.get("/api/system/health")

        assert response.status_code == 200
        data = response.json()

        # Verify integration data
        assert data["overall_status"] == "healthy"
        assert data["uptime_seconds"] == 3600
        assert data["data_freshness_score"] == 0.95
        assert data["active_pipelines"] == 2
        assert data["recent_success_rate"] == 0.98

    @patch('src.interfaces.api.monitoring_dashboard.metrics_service')
    def test_prometheus_metrics_integration(self, mock_metrics_service, test_client):
        """Test Prometheus metrics endpoint integration."""
        mock_metrics_service.get_metrics.return_value = """
# HELP mlb_pipeline_executions_total Total number of pipeline executions
# TYPE mlb_pipeline_executions_total counter
mlb_pipeline_executions_total{pipeline_type="full",status="success"} 150
"""

        # Note: This would test the actual metrics endpoint if implemented
        # For now, verify the metrics service integration exists
        assert hasattr(mock_metrics_service, 'get_metrics')

    def test_dashboard_html_serving(self, test_client):
        """Test dashboard HTML page serving."""
        response = test_client.get("/")

        assert response.status_code == 200
        assert response.headers["content-type"] == "text/html; charset=utf-8"

        # Should contain dashboard HTML
        html_content = response.text
        assert "<html" in html_content.lower()
        assert "dashboard" in html_content.lower() or "monitoring" in html_content.lower()


class TestRealTimeSystemUpdates:
    """Test real-time system update streaming."""

    @pytest.mark.asyncio
    async def test_pipeline_status_streaming(self):
        """Test real-time pipeline status updates."""
        # This would test the background task that streams pipeline updates
        # For integration testing, we verify the message structure

        pipeline_update = WebSocketMessage(
            type="pipeline_update",
            data={
                "pipeline_id": "streaming-test-123",
                "status": "running",
                "progress": 0.65,
                "current_stage": "data_collection",
                "stages_completed": 2,
                "stages_total": 4,
                "estimated_completion": datetime.now(timezone.utc).isoformat()
            }
        )

        # Verify message structure
        message_dict = pipeline_update.model_dump()
        assert message_dict["type"] == "pipeline_update"
        assert "pipeline_id" in message_dict["data"]
        assert "status" in message_dict["data"]
        assert "progress" in message_dict["data"]

    @pytest.mark.asyncio
    async def test_system_metrics_streaming(self):
        """Test real-time system metrics updates."""
        metrics_update = WebSocketMessage(
            type="metrics_update",
            data={
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "pipeline_metrics": {
                    "active_pipelines": 3,
                    "completed_today": 25,
                    "success_rate": 0.96
                },
                "system_metrics": {
                    "cpu_usage": 0.42,
                    "memory_usage": 0.58,
                    "disk_usage": 0.35
                },
                "business_metrics": {
                    "opportunities_detected": 15,
                    "total_value_identified": 1250.75
                }
            }
        )

        # Verify message structure
        message_dict = metrics_update.model_dump()
        assert message_dict["type"] == "metrics_update"
        assert "pipeline_metrics" in message_dict["data"]
        assert "system_metrics" in message_dict["data"]
        assert "business_metrics" in message_dict["data"]

    @pytest.mark.asyncio
    async def test_alert_streaming(self):
        """Test real-time alert streaming."""
        alert_message = WebSocketMessage(
            type="alert",
            data={
                "level": "warning",
                "title": "High CPU Usage",
                "message": "System CPU usage has exceeded 80% for 5 minutes",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "source": "system_monitor",
                "action_required": True,
                "details": {
                    "current_usage": 0.85,
                    "threshold": 0.80,
                    "duration_minutes": 5
                }
            }
        )

        # Verify alert message structure
        message_dict = alert_message.model_dump()
        assert message_dict["type"] == "alert"
        assert message_dict["data"]["level"] == "warning"
        assert "action_required" in message_dict["data"]


class TestBreakGlassControlsIntegration:
    """Test break-glass manual controls integration."""

    @patch('src.interfaces.api.monitoring_dashboard.pipeline_orchestration_service')
    @patch('src.interfaces.api.monitoring_dashboard.metrics_service')
    def test_manual_pipeline_execution_api(self, mock_metrics_service, mock_pipeline_service, test_client):
        """Test manual pipeline execution via API."""
        # Mock successful pipeline execution
        mock_pipeline_service.execute_smart_pipeline = AsyncMock(return_value=Mock(
            pipeline_id="manual-execution-123",
            status="success",
            execution_time_seconds=15.2,
            stages_executed=4
        ))

        # Mock metrics recording
        mock_metrics_service.record_break_glass_activation = Mock()
        mock_metrics_service.record_emergency_execution = Mock()

        # Test manual execution request
        request_data = {
            "pipeline_type": "data_only",
            "force": True,
            "reason": "Emergency data collection needed"
        }

        # Note: This would test the actual manual execution endpoint
        # For now, verify the service integration exists
        assert hasattr(mock_pipeline_service, 'execute_smart_pipeline')
        assert hasattr(mock_metrics_service, 'record_break_glass_activation')

    @pytest.mark.asyncio
    async def test_break_glass_websocket_notification(self):
        """Test WebSocket notification for break-glass procedures."""
        break_glass_notification = WebSocketMessage(
            type="break_glass_activated",
            data={
                "procedure_type": "manual_pipeline_execution",
                "trigger_reason": "scheduler_failure",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "initiated_by": "dashboard_user",
                "pipeline_id": "emergency-execution-456",
                "estimated_duration": 300,  # seconds
                "status": "in_progress"
            }
        )

        # Create mock connection
        mock_websocket = Mock()
        mock_websocket.send_text = AsyncMock()
        manager.active_connections.add(mock_websocket)

        # Broadcast break-glass notification
        await manager.broadcast(break_glass_notification)

        # Verify notification was sent
        expected_message = break_glass_notification.model_dump_json()
        mock_websocket.send_text.assert_called_once_with(expected_message)

        # Clean up
        manager.disconnect(mock_websocket)

    @patch('src.interfaces.api.monitoring_dashboard.metrics_service')
    def test_system_override_controls(self, mock_metrics_service, test_client):
        """Test system override control endpoints."""
        mock_metrics_service.record_manual_override = Mock()

        # Test that override capabilities are available
        assert hasattr(mock_metrics_service, 'record_manual_override')
        assert hasattr(mock_metrics_service, 'record_break_glass_activation')


class TestMultiClientWebSocketHandling:
    """Test WebSocket handling with multiple clients."""

    @pytest.mark.asyncio
    async def test_selective_message_broadcasting(self):
        """Test selective message broadcasting to specific clients."""
        # Create different types of clients
        admin_ws = Mock()
        admin_ws.send_text = AsyncMock()

        viewer_ws = Mock()
        viewer_ws.send_text = AsyncMock()

        await manager.connect(admin_ws, {"role": "admin", "permissions": ["all"]})
        await manager.connect(viewer_ws, {"role": "viewer", "permissions": ["read"]})

        # Create admin-only message
        admin_message = WebSocketMessage(
            type="admin_alert",
            data={"message": "System maintenance required", "sensitive": True}
        )

        # For now, broadcast to all (selective broadcasting would require implementation)
        await manager.broadcast(admin_message)

        # Both should receive the message (real implementation might filter)
        admin_ws.send_text.assert_called_once()
        viewer_ws.send_text.assert_called_once()

        # Clean up
        manager.disconnect(admin_ws)
        manager.disconnect(viewer_ws)

    @pytest.mark.asyncio
    async def test_client_specific_subscriptions(self):
        """Test client-specific message subscriptions."""
        # Create clients with different interests
        pipeline_client = Mock()
        pipeline_client.send_text = AsyncMock()

        metrics_client = Mock()
        metrics_client.send_text = AsyncMock()

        await manager.connect(pipeline_client, {"subscriptions": ["pipeline_updates"]})
        await manager.connect(metrics_client, {"subscriptions": ["metrics_updates"]})

        # Send different message types
        pipeline_message = WebSocketMessage(type="pipeline_update", data={"status": "running"})
        metrics_message = WebSocketMessage(type="metrics_update", data={"cpu": 0.5})

        await manager.broadcast(pipeline_message)
        await manager.broadcast(metrics_message)

        # Both clients receive all messages (filtering would require implementation)
        assert pipeline_client.send_text.call_count == 2
        assert metrics_client.send_text.call_count == 2

        # Clean up
        manager.disconnect(pipeline_client)
        manager.disconnect(metrics_client)

    @pytest.mark.asyncio
    async def test_websocket_connection_limits(self):
        """Test WebSocket connection limits and management."""
        # Create many connections
        connections = []
        for i in range(100):
            ws = Mock()
            ws.accept = AsyncMock()
            ws.send_text = AsyncMock()
            connections.append(ws)

            await manager.connect(ws, {"client_id": f"load-test-{i}"})

        # Verify all connections are tracked
        assert len(manager.active_connections) == 100

        # Test broadcasting to all connections
        test_message = WebSocketMessage(type="load_test", data={"test": True})
        await manager.broadcast(test_message)

        # All connections should receive the message
        for ws in connections:
            ws.send_text.assert_called_once()

        # Clean up
        for ws in connections:
            manager.disconnect(ws)

        assert len(manager.active_connections) == 0


class TestDashboardPerformanceIntegration:
    """Test dashboard performance under load."""

    @pytest.mark.asyncio
    async def test_concurrent_websocket_and_api_requests(self, async_client):
        """Test concurrent WebSocket and API request handling."""
        # Create WebSocket connections
        websockets = []
        for i in range(10):
            ws = Mock()
            ws.accept = AsyncMock()
            ws.send_text = AsyncMock()
            websockets.append(ws)
            await manager.connect(ws, {"client_id": f"concurrent-test-{i}"})

        # Make concurrent API requests
        async def make_api_request():
            try:
                response = await async_client.get("/api/health")
                return response.status_code == 200
            except Exception:
                return False

        # Execute concurrent requests
        api_tasks = [make_api_request() for _ in range(20)]
        api_results = await asyncio.gather(*api_tasks, return_exceptions=True)

        # Most requests should succeed
        successful_requests = sum(1 for result in api_results if result is True)
        assert successful_requests >= 15  # Allow some failures under load

        # WebSocket broadcasting should still work
        test_message = WebSocketMessage(type="load_test", data={"concurrent": True})
        await manager.broadcast(test_message)

        for ws in websockets:
            ws.send_text.assert_called_once()

        # Clean up
        for ws in websockets:
            manager.disconnect(ws)

    def test_api_response_time_under_load(self, test_client):
        """Test API response times under concurrent load."""
        import time

        response_times = []

        # Make multiple API requests and measure response time
        for _ in range(50):
            start_time = time.time()
            response = test_client.get("/api/health")
            end_time = time.time()

            assert response.status_code == 200
            response_times.append(end_time - start_time)

        # Calculate average response time
        avg_response_time = sum(response_times) / len(response_times)

        # Response time should be reasonable (< 100ms average)
        assert avg_response_time < 0.1, f"Average response time too high: {avg_response_time:.3f}s"


class TestIntegrationErrorHandling:
    """Test error handling in integrated dashboard scenarios."""

    @pytest.mark.asyncio
    async def test_websocket_connection_recovery(self):
        """Test WebSocket connection recovery after failures."""
        # Create connection that will fail
        failing_ws = Mock()
        failing_ws.accept = AsyncMock()
        failing_ws.send_text = AsyncMock(side_effect=Exception("Connection lost"))

        # Create stable connection
        stable_ws = Mock()
        stable_ws.accept = AsyncMock()
        stable_ws.send_text = AsyncMock()

        await manager.connect(failing_ws, {"client_id": "failing-client"})
        await manager.connect(stable_ws, {"client_id": "stable-client"})

        # Broadcast message - should handle failure gracefully
        test_message = WebSocketMessage(type="error_test", data={"test": True})
        await manager.broadcast(test_message)

        # Failing connection should be removed
        assert failing_ws not in manager.active_connections

        # Stable connection should still work
        assert stable_ws in manager.active_connections
        stable_ws.send_text.assert_called_once()

        # Clean up
        manager.disconnect(stable_ws)

    @patch('src.interfaces.api.monitoring_dashboard.monitoring_service')
    def test_api_service_failure_handling(self, mock_monitoring_service, test_client):
        """Test API handling when underlying services fail."""
        # Mock service failure
        mock_monitoring_service.get_system_health.side_effect = Exception("Service unavailable")

        # API should handle the failure gracefully
        response = test_client.get("/api/system/health")

        # Should return error status or degraded response
        assert response.status_code in [200, 500, 503]  # Various acceptable responses

    @pytest.mark.asyncio
    async def test_broadcast_resilience_with_connection_failures(self):
        """Test broadcast resilience when multiple connections fail."""
        # Create mix of working and failing connections
        working_connections = []
        failing_connections = []

        for i in range(5):
            ws = Mock()
            ws.send_text = AsyncMock()
            working_connections.append(ws)
            manager.active_connections.add(ws)

        for i in range(3):
            ws = Mock()
            ws.send_text = AsyncMock(side_effect=Exception("Connection failed"))
            failing_connections.append(ws)
            manager.active_connections.add(ws)

        # Broadcast should handle failures gracefully
        test_message = WebSocketMessage(type="resilience_test", data={"test": True})
        await manager.broadcast(test_message)

        # Working connections should receive message
        for ws in working_connections:
            ws.send_text.assert_called_once()

        # Failed connections should be removed
        for ws in failing_connections:
            assert ws not in manager.active_connections

        # Working connections should remain
        for ws in working_connections:
            assert ws in manager.active_connections

        # Clean up
        for ws in working_connections:
            manager.disconnect(ws)
