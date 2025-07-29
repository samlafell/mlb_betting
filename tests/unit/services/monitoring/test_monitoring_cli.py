#!/usr/bin/env python3
"""
Unit tests for CLI Monitoring Commands

Tests comprehensive CLI monitoring command suite including:
- Command registration and structure
- Dashboard management commands
- Health check and status commands  
- Live monitoring capabilities
- Manual execution commands
- Integration with FastAPI dashboard
- Error handling and graceful failures
"""

import json
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from src.interfaces.cli.commands.monitoring import MonitoringCommands


@pytest.fixture
def monitoring_commands():
    """Create MonitoringCommands instance for testing."""
    return MonitoringCommands()


@pytest.fixture
def cli_runner():
    """Create Click CLI runner for testing."""
    return CliRunner()


@pytest.fixture
def mock_dashboard_response():
    """Mock successful dashboard response."""
    return {
        "status": "healthy",
        "timestamp": "2025-01-25T12:00:00Z",
        "service": "monitoring-dashboard",
        "version": "1.0.0"
    }


@pytest.fixture
def mock_system_health_response():
    """Mock system health response."""
    return {
        "overall_status": "healthy",
        "uptime_seconds": 3600.0,
        "data_freshness_score": 0.95,
        "system_load": {"cpu": 0.25, "memory": 0.45},
        "active_pipelines": 2,
        "recent_success_rate": 0.98,
        "slo_compliance": {"pipeline_latency": {"status": "healthy"}},
        "alerts": []
    }


class TestMonitoringCommandsInitialization:
    """Test MonitoringCommands class initialization."""

    def test_monitoring_commands_initialization(self, monitoring_commands):
        """Test that MonitoringCommands initializes properly."""
        assert monitoring_commands is not None
        assert hasattr(monitoring_commands, 'create_group')

    def test_create_group_returns_command_group(self, monitoring_commands):
        """Test that create_group returns a valid command group."""
        group = monitoring_commands.create_group()

        assert group is not None
        assert hasattr(group, 'commands')

        # Should have monitoring commands
        expected_commands = ['dashboard', 'status', 'live', 'execute']
        for cmd in expected_commands:
            assert cmd in group.commands


class TestDashboardCommand:
    """Test monitoring dashboard command."""

    @patch('src.interfaces.cli.commands.monitoring.uvicorn')
    def test_dashboard_command_default_options(self, mock_uvicorn, monitoring_commands, cli_runner):
        """Test dashboard command with default options."""
        group = monitoring_commands.create_group()

        result = cli_runner.invoke(group, ['dashboard'])

        assert result.exit_code == 0
        mock_uvicorn.run.assert_called_once()

        # Verify default parameters
        call_args = mock_uvicorn.run.call_args
        assert call_args[1]['host'] == '127.0.0.1'
        assert call_args[1]['port'] == 8001

    @patch('src.interfaces.cli.commands.monitoring.uvicorn')
    def test_dashboard_command_custom_options(self, mock_uvicorn, monitoring_commands, cli_runner):
        """Test dashboard command with custom host and port."""
        group = monitoring_commands.create_group()

        result = cli_runner.invoke(group, ['dashboard', '--host', '0.0.0.0', '--port', '9000'])

        assert result.exit_code == 0
        mock_uvicorn.run.assert_called_once()

        # Verify custom parameters
        call_args = mock_uvicorn.run.call_args
        assert call_args[1]['host'] == '0.0.0.0'
        assert call_args[1]['port'] == 9000

    @patch('src.interfaces.cli.commands.monitoring.uvicorn')
    def test_dashboard_command_reload_option(self, mock_uvicorn, monitoring_commands, cli_runner):
        """Test dashboard command with reload option."""
        group = monitoring_commands.create_group()

        result = cli_runner.invoke(group, ['dashboard', '--reload'])

        assert result.exit_code == 0
        mock_uvicorn.run.assert_called_once()

        # Verify reload parameter
        call_args = mock_uvicorn.run.call_args
        assert call_args[1]['reload'] is True

    @patch('src.interfaces.cli.commands.monitoring.uvicorn')
    def test_dashboard_command_error_handling(self, mock_uvicorn, monitoring_commands, cli_runner):
        """Test dashboard command error handling."""
        mock_uvicorn.run.side_effect = Exception("Server failed to start")
        group = monitoring_commands.create_group()

        result = cli_runner.invoke(group, ['dashboard'])

        # Should handle error gracefully
        assert result.exit_code == 1
        assert "Error starting dashboard" in result.output


class TestStatusCommand:
    """Test monitoring status command."""

    @patch('src.interfaces.cli.commands.monitoring.httpx.get')
    def test_status_command_success(self, mock_get, monitoring_commands, cli_runner, mock_dashboard_response):
        """Test status command with successful response."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_dashboard_response
        mock_get.return_value = mock_response

        group = monitoring_commands.create_group()
        result = cli_runner.invoke(group, ['status'])

        assert result.exit_code == 0
        assert "Dashboard Status: healthy" in result.output
        assert "Service: monitoring-dashboard" in result.output

    @patch('src.interfaces.cli.commands.monitoring.httpx.get')
    def test_status_command_custom_url(self, mock_get, monitoring_commands, cli_runner, mock_dashboard_response):
        """Test status command with custom dashboard URL."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_dashboard_response
        mock_get.return_value = mock_response

        group = monitoring_commands.create_group()
        result = cli_runner.invoke(group, ['status', '--dashboard-url', 'http://custom:8080'])

        assert result.exit_code == 0
        mock_get.assert_called_with('http://custom:8080/api/health', timeout=10)

    @patch('src.interfaces.cli.commands.monitoring.httpx.get')
    def test_status_command_connection_error(self, mock_get, monitoring_commands, cli_runner):
        """Test status command with connection error."""
        mock_get.side_effect = Exception("Connection refused")

        group = monitoring_commands.create_group()
        result = cli_runner.invoke(group, ['status'])

        assert result.exit_code == 1
        assert "Error connecting to dashboard" in result.output

    @patch('src.interfaces.cli.commands.monitoring.httpx.get')
    def test_status_command_unhealthy_response(self, mock_get, monitoring_commands, cli_runner):
        """Test status command with unhealthy dashboard response."""
        mock_response = Mock()
        mock_response.status_code = 503
        mock_response.json.return_value = {"status": "unhealthy", "error": "Service unavailable"}
        mock_get.return_value = mock_response

        group = monitoring_commands.create_group()
        result = cli_runner.invoke(group, ['status'])

        assert result.exit_code == 1
        assert "Dashboard Status: unhealthy" in result.output

    @patch('src.interfaces.cli.commands.monitoring.httpx.get')
    def test_status_command_detailed_flag(self, mock_get, monitoring_commands, cli_runner, mock_system_health_response):
        """Test status command with detailed flag."""
        # Mock health endpoint response
        mock_health_response = Mock()
        mock_health_response.status_code = 200
        mock_health_response.json.return_value = mock_dashboard_response

        # Mock system health endpoint response
        mock_system_response = Mock()
        mock_system_response.status_code = 200
        mock_system_response.json.return_value = mock_system_health_response

        # Configure mock to return different responses for different endpoints
        def mock_get_side_effect(url, **kwargs):
            if '/api/health' in url:
                return mock_health_response
            elif '/api/system/health' in url:
                return mock_system_response
            return Mock(status_code=404)

        mock_get.side_effect = mock_get_side_effect

        group = monitoring_commands.create_group()
        result = cli_runner.invoke(group, ['status', '--detailed'])

        assert result.exit_code == 0
        assert "System Health Details:" in result.output
        assert "Uptime: 1.0 hours" in result.output
        assert "Active Pipelines: 2" in result.output


class TestLiveCommand:
    """Test monitoring live command."""

    @patch('src.interfaces.cli.commands.monitoring.websockets.connect')
    @patch('src.interfaces.cli.commands.monitoring.asyncio.run')
    def test_live_command_basic(self, mock_asyncio_run, mock_ws_connect, monitoring_commands, cli_runner):
        """Test live monitoring command basic functionality."""
        group = monitoring_commands.create_group()

        result = cli_runner.invoke(group, ['live'])

        assert result.exit_code == 0
        mock_asyncio_run.assert_called_once()

    @patch('src.interfaces.cli.commands.monitoring.websockets.connect')
    @patch('src.interfaces.cli.commands.monitoring.asyncio.run')
    def test_live_command_custom_url(self, mock_asyncio_run, mock_ws_connect, monitoring_commands, cli_runner):
        """Test live monitoring with custom dashboard URL."""
        group = monitoring_commands.create_group()

        result = cli_runner.invoke(group, ['live', '--dashboard-url', 'http://custom:8080'])

        assert result.exit_code == 0
        mock_asyncio_run.assert_called_once()

    @patch('src.interfaces.cli.commands.monitoring.websockets.connect')
    @patch('src.interfaces.cli.commands.monitoring.asyncio.run')
    def test_live_command_with_filter(self, mock_asyncio_run, mock_ws_connect, monitoring_commands, cli_runner):
        """Test live monitoring with message type filter."""
        group = monitoring_commands.create_group()

        result = cli_runner.invoke(group, ['live', '--filter', 'pipeline_update'])

        assert result.exit_code == 0
        mock_asyncio_run.assert_called_once()

    @patch('src.interfaces.cli.commands.monitoring.asyncio.run')
    def test_live_command_connection_error(self, mock_asyncio_run, monitoring_commands, cli_runner):
        """Test live command with WebSocket connection error."""
        mock_asyncio_run.side_effect = Exception("WebSocket connection failed")

        group = monitoring_commands.create_group()
        result = cli_runner.invoke(group, ['live'])

        assert result.exit_code == 1
        assert "Error starting live monitoring" in result.output


class TestExecuteCommand:
    """Test monitoring execute command for break-glass procedures."""

    @patch('src.interfaces.cli.commands.monitoring.httpx.post')
    def test_execute_command_default_pipeline(self, mock_post, monitoring_commands, cli_runner):
        """Test execute command with default pipeline type."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "pipeline_id": "manual-123",
            "status": "success",
            "execution_time": 15.2
        }
        mock_post.return_value = mock_response

        group = monitoring_commands.create_group()
        result = cli_runner.invoke(group, ['execute'])

        assert result.exit_code == 0
        assert "Pipeline execution completed successfully" in result.output
        assert "Pipeline ID: manual-123" in result.output

    @patch('src.interfaces.cli.commands.monitoring.httpx.post')
    def test_execute_command_specific_pipeline_type(self, mock_post, monitoring_commands, cli_runner):
        """Test execute command with specific pipeline type."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "pipeline_id": "manual-data-456",
            "status": "success",
            "execution_time": 8.5
        }
        mock_post.return_value = mock_response

        group = monitoring_commands.create_group()
        result = cli_runner.invoke(group, ['execute', '--pipeline-type', 'data_only'])

        assert result.exit_code == 0
        # Verify correct pipeline type was requested
        call_args = mock_post.call_args
        request_data = call_args[1]['json']
        assert request_data['pipeline_type'] == 'data_only'

    @patch('src.interfaces.cli.commands.monitoring.httpx.post')
    def test_execute_command_force_flag(self, mock_post, monitoring_commands, cli_runner):
        """Test execute command with force flag."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "pipeline_id": "forced-789",
            "status": "success",
            "execution_time": 12.1
        }
        mock_post.return_value = mock_response

        group = monitoring_commands.create_group()
        result = cli_runner.invoke(group, ['execute', '--force'])

        assert result.exit_code == 0
        # Verify force flag was sent
        call_args = mock_post.call_args
        request_data = call_args[1]['json']
        assert request_data['force'] is True

    @patch('src.interfaces.cli.commands.monitoring.httpx.post')
    def test_execute_command_custom_dashboard_url(self, mock_post, monitoring_commands, cli_runner):
        """Test execute command with custom dashboard URL."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"pipeline_id": "custom-123", "status": "success"}
        mock_post.return_value = mock_response

        group = monitoring_commands.create_group()
        result = cli_runner.invoke(group, ['execute', '--dashboard-url', 'http://custom:8080'])

        assert result.exit_code == 0
        mock_post.assert_called_with(
            'http://custom:8080/api/pipeline/execute',
            json={'pipeline_type': 'full', 'force': False},
            timeout=60
        )

    @patch('src.interfaces.cli.commands.monitoring.httpx.post')
    def test_execute_command_pipeline_failure(self, mock_post, monitoring_commands, cli_runner):
        """Test execute command with pipeline execution failure."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "pipeline_id": "failed-456",
            "status": "failed",
            "error": "Database connection lost",
            "execution_time": 3.2
        }
        mock_post.return_value = mock_response

        group = monitoring_commands.create_group()
        result = cli_runner.invoke(group, ['execute'])

        assert result.exit_code == 1
        assert "Pipeline execution failed" in result.output
        assert "Error: Database connection lost" in result.output

    @patch('src.interfaces.cli.commands.monitoring.httpx.post')
    def test_execute_command_api_error(self, mock_post, monitoring_commands, cli_runner):
        """Test execute command with API error."""
        mock_response = Mock()
        mock_response.status_code = 503
        mock_response.json.return_value = {"error": "Service unavailable"}
        mock_post.return_value = mock_response

        group = monitoring_commands.create_group()
        result = cli_runner.invoke(group, ['execute'])

        assert result.exit_code == 1
        assert "Error executing pipeline" in result.output

    @patch('src.interfaces.cli.commands.monitoring.httpx.post')
    def test_execute_command_connection_error(self, mock_post, monitoring_commands, cli_runner):
        """Test execute command with connection error."""
        mock_post.side_effect = Exception("Connection timeout")

        group = monitoring_commands.create_group()
        result = cli_runner.invoke(group, ['execute'])

        assert result.exit_code == 1
        assert "Error connecting to dashboard" in result.output


class TestCommandValidation:
    """Test command parameter validation."""

    def test_pipeline_type_validation(self, monitoring_commands, cli_runner):
        """Test pipeline type parameter validation."""
        group = monitoring_commands.create_group()

        # Test invalid pipeline type
        result = cli_runner.invoke(group, ['execute', '--pipeline-type', 'invalid_type'])

        assert result.exit_code == 2  # Click parameter validation error
        assert "Invalid value for '--pipeline-type'" in result.output

    def test_valid_pipeline_types(self, monitoring_commands, cli_runner):
        """Test all valid pipeline types are accepted."""
        valid_types = ['full', 'data_only', 'analysis_only']
        group = monitoring_commands.create_group()

        for pipeline_type in valid_types:
            with patch('src.interfaces.cli.commands.monitoring.httpx.post') as mock_post:
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {"pipeline_id": "test", "status": "success"}
                mock_post.return_value = mock_response

                result = cli_runner.invoke(group, ['execute', '--pipeline-type', pipeline_type])
                assert result.exit_code == 0

    def test_url_parameter_handling(self, monitoring_commands, cli_runner):
        """Test URL parameter handling and validation."""
        group = monitoring_commands.create_group()

        # Test with various URL formats
        urls = [
            'http://localhost:8001',
            'https://monitoring.example.com',
            'http://127.0.0.1:9000'
        ]

        for url in urls:
            with patch('src.interfaces.cli.commands.monitoring.httpx.get') as mock_get:
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {"status": "healthy"}
                mock_get.return_value = mock_response

                result = cli_runner.invoke(group, ['status', '--dashboard-url', url])
                assert result.exit_code == 0
                mock_get.assert_called_with(f'{url}/api/health', timeout=10)


class TestErrorHandling:
    """Test error handling and graceful failures."""

    def test_network_timeout_handling(self, monitoring_commands, cli_runner):
        """Test network timeout handling."""
        with patch('src.interfaces.cli.commands.monitoring.httpx.get') as mock_get:
            mock_get.side_effect = Exception("Request timeout")

            group = monitoring_commands.create_group()
            result = cli_runner.invoke(group, ['status'])

            assert result.exit_code == 1
            assert "Error connecting to dashboard" in result.output

    def test_json_parsing_error_handling(self, monitoring_commands, cli_runner):
        """Test JSON parsing error handling."""
        with patch('src.interfaces.cli.commands.monitoring.httpx.get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
            mock_get.return_value = mock_response

            group = monitoring_commands.create_group()
            result = cli_runner.invoke(group, ['status'])

            assert result.exit_code == 1
            assert "Error parsing response" in result.output

    def test_keyboard_interrupt_handling(self, monitoring_commands, cli_runner):
        """Test keyboard interrupt handling in live mode."""
        with patch('src.interfaces.cli.commands.monitoring.asyncio.run') as mock_run:
            mock_run.side_effect = KeyboardInterrupt()

            group = monitoring_commands.create_group()
            result = cli_runner.invoke(group, ['live'])

            assert result.exit_code == 0
            assert "Live monitoring stopped" in result.output


class TestIntegrationWithExistingCLI:
    """Test integration with existing CLI structure."""

    def test_monitoring_group_registration(self):
        """Test that monitoring group is properly registered in main CLI."""
        from src.interfaces.cli.main import cli

        # Should have monitoring command group
        assert 'monitoring' in cli.commands

    def test_command_help_text(self, monitoring_commands, cli_runner):
        """Test that commands have proper help text."""
        group = monitoring_commands.create_group()

        # Test main group help
        result = cli_runner.invoke(group, ['--help'])
        assert result.exit_code == 0
        assert "Monitoring and observability commands" in result.output

        # Test individual command help
        commands = ['dashboard', 'status', 'live', 'execute']
        for cmd in commands:
            result = cli_runner.invoke(group, [cmd, '--help'])
            assert result.exit_code == 0
            assert len(result.output) > 0


class TestLiveMonitoringWebSocket:
    """Test live monitoring WebSocket functionality."""

    @pytest.mark.asyncio
    async def test_websocket_message_handling(self):
        """Test WebSocket message handling in live monitoring."""
        # This would test the actual WebSocket connection and message processing
        # For unit testing, we verify the structure exists

        from src.interfaces.cli.commands.monitoring import handle_websocket_message

        # Mock message
        message_data = {
            "type": "pipeline_update",
            "data": {"pipeline_id": "test-123", "status": "running"},
            "timestamp": "2025-01-25T12:00:00Z"
        }

        # Should not raise exception
        formatted_output = handle_websocket_message(json.dumps(message_data))
        assert isinstance(formatted_output, str)
        assert "pipeline_update" in formatted_output

    def test_message_filtering(self):
        """Test message type filtering in live monitoring."""
        from src.interfaces.cli.commands.monitoring import should_display_message

        # Test with filter
        assert should_display_message("pipeline_update", ["pipeline_update", "alerts"]) is True
        assert should_display_message("system_health", ["pipeline_update", "alerts"]) is False

        # Test without filter (should display all)
        assert should_display_message("pipeline_update", None) is True
        assert should_display_message("system_health", None) is True


class TestBreakGlassIntegration:
    """Test break-glass procedure integration."""

    @patch('src.interfaces.cli.commands.monitoring.get_metrics_service')
    def test_break_glass_metrics_recording(self, mock_get_metrics, monitoring_commands, cli_runner):
        """Test that break-glass procedures record appropriate metrics."""
        mock_metrics = Mock()
        mock_get_metrics.return_value = mock_metrics

        with patch('src.interfaces.cli.commands.monitoring.httpx.post') as mock_post:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"pipeline_id": "manual-123", "status": "success"}
            mock_post.return_value = mock_response

            group = monitoring_commands.create_group()
            result = cli_runner.invoke(group, ['execute', '--force'])

            assert result.exit_code == 0
            # Should record break-glass activation
            mock_metrics.record_break_glass_activation.assert_called_once()

    def test_manual_override_logging(self, monitoring_commands, cli_runner):
        """Test that manual overrides are properly logged."""
        with patch('src.interfaces.cli.commands.monitoring.httpx.post') as mock_post:
            with patch('src.interfaces.cli.commands.monitoring.logger') as mock_logger:
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {"pipeline_id": "override-456", "status": "success"}
                mock_post.return_value = mock_response

                group = monitoring_commands.create_group()
                result = cli_runner.invoke(group, ['execute', '--force'])

                assert result.exit_code == 0
                # Should log manual override
                mock_logger.warning.assert_called()


class TestCommandLineOutput:
    """Test command line output formatting."""

    def test_status_output_formatting(self, monitoring_commands, cli_runner):
        """Test status command output formatting."""
        with patch('src.interfaces.cli.commands.monitoring.httpx.get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "status": "healthy",
                "timestamp": "2025-01-25T12:00:00Z",
                "service": "monitoring-dashboard",
                "version": "1.0.0"
            }
            mock_get.return_value = mock_response

            group = monitoring_commands.create_group()
            result = cli_runner.invoke(group, ['status'])

            assert result.exit_code == 0
            # Check specific formatting
            assert "✓" in result.output or "healthy" in result.output
            assert "2025-01-25" in result.output

    def test_execute_success_output_formatting(self, monitoring_commands, cli_runner):
        """Test execute command success output formatting."""
        with patch('src.interfaces.cli.commands.monitoring.httpx.post') as mock_post:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {
                "pipeline_id": "success-789",
                "status": "success",
                "execution_time": 15.7,
                "stages_executed": 4
            }
            mock_post.return_value = mock_response

            group = monitoring_commands.create_group()
            result = cli_runner.invoke(group, ['execute'])

            assert result.exit_code == 0
            # Check specific formatting elements
            assert "✓" in result.output or "success" in result.output
            assert "15.7" in result.output  # execution time
            assert "success-789" in result.output  # pipeline ID

    def test_error_output_formatting(self, monitoring_commands, cli_runner):
        """Test error output formatting."""
        with patch('src.interfaces.cli.commands.monitoring.httpx.get') as mock_get:
            mock_get.side_effect = Exception("Connection failed")

            group = monitoring_commands.create_group()
            result = cli_runner.invoke(group, ['status'])

            assert result.exit_code == 1
            # Should have error indicator
            assert "✗" in result.output or "Error" in result.output
