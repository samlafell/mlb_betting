#!/usr/bin/env python3
"""
Integration Tests for CLI Commands Registration

Tests that CLI commands are properly registered and accessible.
"""

import pytest
from click.testing import CliRunner
from unittest.mock import patch, MagicMock

from src.interfaces.cli.main import cli
from src.interfaces.cli.commands.ml_commands import ml


class TestCLICommandsRegistration:
    """Integration tests for CLI command registration."""

    @pytest.fixture
    def runner(self):
        """Click CLI test runner."""
        return CliRunner()

    @pytest.fixture
    def mock_settings(self):
        """Mock settings to prevent database initialization during tests."""
        mock_settings = MagicMock()
        mock_settings.database.host = "test-host"
        mock_settings.database.port = 5433
        mock_settings.database.database = "test-database"
        mock_settings.database.user = "test-user"
        mock_settings.database.password = "test-password"
        return mock_settings

    def test_main_cli_help(self, runner):
        """Test that main CLI help displays correctly."""
        result = runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        assert "MLB Sharp Betting - Unified Data Collection and Analysis System" in result.output
        assert "Commands:" in result.output

    def test_ml_commands_registration(self, runner):
        """Ensure ml commands are properly registered"""
        result = runner.invoke(cli, ['ml', '--help'])
        assert result.exit_code == 0
        assert "Machine Learning pipeline management commands" in result.output
        assert "training" in result.output  # ML training subcommand should be listed

    def test_ml_training_commands_registration(self, runner):
        """Test that ML training commands are accessible"""
        result = runner.invoke(cli, ['ml', 'training', '--help'])
        assert result.exit_code == 0
        assert "ML training pipeline commands" in result.output
        
        # Check that key training commands are available
        expected_commands = ['train', 'retrain', 'evaluate', 'status', 'schedule']
        for command in expected_commands:
            assert command in result.output

    def test_data_commands_registration(self, runner):
        """Test that data commands are properly registered"""
        result = runner.invoke(cli, ['data', '--help'])
        assert result.exit_code == 0
        assert "Data collection and management commands" in result.output

    def test_monitoring_commands_registration(self, runner):
        """Test that monitoring commands are properly registered"""
        result = runner.invoke(cli, ['monitoring', '--help'])
        assert result.exit_code == 0
        # Should contain monitoring-related content

    def test_database_commands_registration(self, runner):
        """Test that database commands are properly registered"""
        result = runner.invoke(cli, ['database', '--help'])
        assert result.exit_code == 0
        # Should contain database setup commands

    def test_backtesting_commands_registration(self, runner):
        """Test that backtesting commands are properly registered"""
        result = runner.invoke(cli, ['backtesting', '--help'])
        assert result.exit_code == 0
        # Should contain backtesting-related content

    def test_movement_commands_registration(self, runner):
        """Test that movement analysis commands are properly registered"""
        result = runner.invoke(cli, ['movement', '--help'])
        assert result.exit_code == 0
        # Should contain movement analysis commands

    def test_action_network_commands_registration(self, runner):
        """Test that action network commands are properly registered"""
        result = runner.invoke(cli, ['action-network', '--help'])
        assert result.exit_code == 0
        # Should contain action network pipeline commands

    def test_outcomes_commands_registration(self, runner):
        """Test that game outcomes commands are properly registered"""
        result = runner.invoke(cli, ['outcomes', '--help'])
        assert result.exit_code == 0
        # Should contain game outcomes commands

    @patch('src.interfaces.cli.main.get_settings')
    @patch('src.interfaces.cli.main.initialize_connections')
    def test_cli_initialization_with_database_failure(self, mock_init_conn, mock_get_settings, runner):
        """Test that CLI gracefully handles database initialization failures"""
        mock_get_settings.return_value = MagicMock()
        mock_init_conn.side_effect = Exception("Database connection failed")
        
        result = runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        assert "Warning: Database initialization failed" in result.output

    @patch('src.interfaces.cli.main.get_settings')
    @patch('src.interfaces.cli.main.initialize_connections')
    def test_cli_initialization_success(self, mock_init_conn, mock_get_settings, runner, mock_settings):
        """Test that CLI initializes successfully with proper database connection"""
        mock_get_settings.return_value = mock_settings
        mock_init_conn.return_value = None  # Successful initialization
        
        result = runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        assert "Warning: Database initialization failed" not in result.output

    def test_ml_command_group_structure(self, runner):
        """Test the ML command group structure and nesting"""
        # Test main ml group
        result = runner.invoke(ml, ['--help'])
        assert result.exit_code == 0
        assert "Machine Learning pipeline management commands" in result.output
        
        # Test that training subcommand is properly nested
        assert "training" in result.output

    def test_command_integration_with_main_cli(self, runner):
        """Test that all major command groups are integrated with main CLI"""
        result = runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        
        # Check that major command groups are listed
        expected_groups = [
            'data',           # Data collection commands
            'monitoring',     # Monitoring commands
            'ml',            # ML training commands
            'database',      # Database setup commands
            'backtesting',   # Backtesting commands
            'movement',      # Movement analysis commands
            'action-network', # Action Network pipeline
            'outcomes',      # Game outcomes
        ]
        
        for group in expected_groups:
            assert group in result.output, f"Command group '{group}' not found in CLI help"

    def test_ml_training_individual_commands(self, runner):
        """Test individual ML training commands can be invoked (help only)"""
        training_commands = ['train', 'retrain', 'evaluate', 'status', 'schedule']
        
        for command in training_commands:
            result = runner.invoke(cli, ['ml', 'training', command, '--help'])
            assert result.exit_code == 0, f"ML training command '{command}' failed help test"

    def test_command_error_handling(self, runner):
        """Test that invalid commands return appropriate error codes"""
        # Test invalid main command
        result = runner.invoke(cli, ['invalid-command'])
        assert result.exit_code != 0
        
        # Test invalid ML subcommand
        result = runner.invoke(cli, ['ml', 'invalid-subcommand'])
        assert result.exit_code != 0
        
        # Test invalid ML training subcommand
        result = runner.invoke(cli, ['ml', 'training', 'invalid-training-command'])
        assert result.exit_code != 0

    def test_version_option(self, runner):
        """Test that version option works correctly"""
        result = runner.invoke(cli, ['--version'])
        assert result.exit_code == 0
        # Version should be displayed

    @patch('src.interfaces.cli.commands.ml_training.MLTrainingService')
    def test_ml_training_command_instantiation(self, mock_training_service, runner):
        """Test that ML training commands can instantiate required services"""
        mock_service = MagicMock()
        mock_training_service.return_value = mock_service
        
        # This test would require more complex mocking for full execution
        # For now, just test that the command structure is sound
        result = runner.invoke(cli, ['ml', 'training', 'status', '--help'])
        assert result.exit_code == 0

    def test_nested_command_structure_consistency(self, runner):
        """Test that nested command structure is consistent across groups"""
        # Test that commands follow consistent patterns
        groups_to_test = ['ml', 'data', 'monitoring']
        
        for group in groups_to_test:
            result = runner.invoke(cli, [group, '--help'])
            assert result.exit_code == 0
            assert "Commands:" in result.output or "Usage:" in result.output

    def test_command_imports_work_correctly(self):
        """Test that command imports don't cause circular import issues"""
        try:
            from src.interfaces.cli.commands.ml_commands import ml
            from src.interfaces.cli.commands.ml_training import ml_training_cli
            from src.interfaces.cli.main import cli
            
            # Test that commands have proper attributes
            assert hasattr(ml, 'commands')
            assert hasattr(cli, 'commands')
            assert callable(ml)
            assert callable(cli)
            
        except ImportError as e:
            pytest.fail(f"Command import failed: {e}")

    def test_ml_command_group_isolation(self, runner):
        """Test that ML command group works independently"""
        # Test ML commands can be invoked directly (not through main CLI)
        result = runner.invoke(ml, ['--help'])
        assert result.exit_code == 0
        assert "Machine Learning pipeline management commands" in result.output

    def test_command_help_text_quality(self, runner):
        """Test that command help text is informative and well-formatted"""
        # Test main CLI help
        result = runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        assert len(result.output.strip()) > 100  # Should have substantial help text
        
        # Test ML commands help
        result = runner.invoke(cli, ['ml', '--help'])
        assert result.exit_code == 0
        assert "Machine Learning" in result.output
        assert len(result.output.strip()) > 50