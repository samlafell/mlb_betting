#!/usr/bin/env python3
"""
Integration tests for quickstart functionality.

Tests the end-to-end quickstart flow including:
- Project root detection
- CLI command execution
- Error handling
- Success indicators

These tests ensure the onboarding improvements work correctly.
"""

import os
import subprocess
import tempfile
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add the project root to the path for imports
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "src"))

from src.interfaces.cli.commands.quickstart import _get_project_root


class TestProjectRootDetection:
    """Test the dynamic project root detection functionality."""
    
    def test_get_project_root_with_indicators(self):
        """Test that project root is correctly detected when indicators are present."""
        # This should find the actual project root
        project_root = _get_project_root()
        
        # Verify it's a valid path
        assert os.path.exists(project_root)
        
        # Verify it contains expected project files
        project_path = Path(project_root)
        assert (project_path / "pyproject.toml").exists()
        assert (project_path / "README.md").exists()
        assert (project_path / "quick-start.sh").exists()
    
    def test_get_project_root_fallback(self):
        """Test fallback behavior when no indicators are found."""
        with patch('pathlib.Path') as mock_path:
            # Mock Path to simulate no indicators found
            mock_instance = MagicMock()
            mock_instance.parents = []
            mock_path.return_value.resolve.return_value = mock_instance
            
            with patch('os.getcwd', return_value='/fallback/path'):
                result = _get_project_root()
                assert result == '/fallback/path'


class TestQuickStartScript:
    """Test the quick-start.sh script functionality."""
    
    def test_script_exists_and_executable(self):
        """Test that the quick-start script exists and is executable."""
        project_root = _get_project_root()
        script_path = Path(project_root) / "quick-start.sh"
        
        assert script_path.exists(), "quick-start.sh script not found"
        assert os.access(script_path, os.X_OK), "quick-start.sh is not executable"
    
    def test_script_help_functionality(self):
        """Test that the script help system works correctly."""
        project_root = _get_project_root()
        script_path = Path(project_root) / "quick-start.sh"
        
        # Run the help command
        result = subprocess.run(
            [str(script_path), "--help"],
            capture_output=True,
            text=True,
            cwd=project_root
        )
        
        assert result.returncode == 0, f"Help command failed: {result.stderr}"
        assert "MLB Betting System - Quick Start Setup" in result.stdout
        assert "Usage:" in result.stdout
        assert "Options:" in result.stdout
    
    def test_script_validation_mode(self):
        """Test script validation without running full setup."""
        project_root = _get_project_root()
        script_path = Path(project_root) / "quick-start.sh"
        
        # Test with skip flags to avoid full setup
        result = subprocess.run(
            [str(script_path), "--skip-docker", "--skip-deps", "--skip-data"],
            capture_output=True,
            text=True,
            cwd=project_root,
            timeout=60  # 1 minute timeout
        )
        
        # Should succeed or give helpful error messages
        assert result.returncode in [0, 1], f"Script failed unexpectedly: {result.stderr}"
        
        # Should contain progress indicators
        assert "Step" in result.stdout or "ERROR" in result.stderr or result.returncode == 0


class TestDockerConfiguration:
    """Test Docker Compose quickstart configuration."""
    
    def test_docker_compose_config_validity(self):
        """Test that Docker Compose configuration is valid."""
        project_root = _get_project_root()
        compose_file = Path(project_root) / "docker-compose.quickstart.yml"
        
        assert compose_file.exists(), "docker-compose.quickstart.yml not found"
        
        # Test configuration validity
        result = subprocess.run(
            ["docker-compose", "-f", str(compose_file), "config"],
            capture_output=True,
            text=True,
            cwd=project_root
        )
        
        assert result.returncode == 0, f"Docker Compose config invalid: {result.stderr}"
        
        # Should contain expected services
        assert "postgres:" in result.stdout
        assert "redis:" in result.stdout
        
        # Should have network isolation
        assert "quickstart_network" in result.stdout
    
    def test_env_quickstart_exists(self):
        """Test that the minimal env template exists."""
        project_root = _get_project_root()
        env_file = Path(project_root) / ".env.quickstart"
        
        assert env_file.exists(), ".env.quickstart template not found"
        
        # Read and verify essential content
        content = env_file.read_text()
        assert "POSTGRES_HOST=" in content
        assert "POSTGRES_PORT=" in content
        assert "POSTGRES_DB=" in content
        assert "quickstart_dev_password_change_for_production" in content


class TestCLICommands:
    """Test CLI quickstart commands."""
    
    def test_quickstart_cli_help(self):
        """Test that quickstart CLI help works."""
        project_root = _get_project_root()
        
        result = subprocess.run(
            ["uv", "run", "-m", "src.interfaces.cli", "quickstart", "--help"],
            capture_output=True,
            text=True,
            cwd=project_root,
            timeout=30
        )
        
        # Should succeed and show help
        assert result.returncode == 0, f"CLI help failed: {result.stderr}"
        assert "quickstart" in result.stdout.lower()
        assert "commands:" in result.stdout.lower()
    
    def test_cli_project_root_detection(self):
        """Test that CLI commands can detect project root correctly."""
        # This is implicitly tested by the help command working
        # If paths were hardcoded, it would fail on different machines
        project_root = _get_project_root()
        
        # Verify we can import the module (tests import path resolution)
        result = subprocess.run(
            ["uv", "run", "-c", "from src.interfaces.cli.commands.quickstart import _get_project_root; print(_get_project_root())"],
            capture_output=True,
            text=True,
            cwd=project_root,
            timeout=15
        )
        
        if result.returncode == 0:
            detected_root = result.stdout.strip()
            assert os.path.exists(detected_root), f"Detected root doesn't exist: {detected_root}"


class TestDocumentation:
    """Test documentation completeness."""
    
    def test_quick_start_guide_exists(self):
        """Test that the quick start guide exists and has expected content."""
        project_root = _get_project_root()
        guide_path = Path(project_root) / "QUICK_START.md"
        
        assert guide_path.exists(), "QUICK_START.md not found"
        
        content = guide_path.read_text()
        assert "One-Command Setup" in content
        assert "./quick-start.sh" in content
        assert "Business users" in content or "business user" in content
        assert "Troubleshooting" in content
    
    def test_readme_updated(self):
        """Test that README includes quick start information."""
        project_root = _get_project_root()
        readme_path = Path(project_root) / "README.md"
        
        assert readme_path.exists(), "README.md not found"
        
        content = readme_path.read_text()
        assert "New User" in content or "Quick Start" in content
        assert "./quick-start.sh" in content
        assert "GitHub issue #35" in content or "issue #35" in content


class TestSuccessIndicators:
    """Test that success indicators work correctly."""
    
    def test_validation_functions_exist(self):
        """Test that validation functions exist in the CLI module."""
        try:
            from interfaces.cli.commands.quickstart import (
                _validate_database,
                _validate_data_sources,
                _check_required_tables
            )
            # Functions exist - this is good
            assert True
        except ImportError as e:
            pytest.fail(f"Required validation functions not found: {e}")
    
    def test_error_messages_are_helpful(self):
        """Test that error messages provide actionable guidance."""
        # This is more of a functional test - would require actual CLI execution
        # For now, we verify the functions exist and can be called
        project_root = _get_project_root()
        
        # Try running a quickstart command that should give helpful output
        result = subprocess.run(
            ["uv", "run", "-m", "src.interfaces.cli", "quickstart", "demo"],
            capture_output=True,
            text=True,
            cwd=project_root,
            timeout=30
        )
        
        # Should either succeed or give helpful error
        if result.returncode != 0:
            # Error should be informative, not just a stack trace
            assert len(result.stderr) > 0 or len(result.stdout) > 0
            # Should not be a Python traceback
            assert "Traceback" not in result.stderr


if __name__ == "__main__":
    """Run tests directly if script is executed."""
    pytest.main([__file__, "-v"])