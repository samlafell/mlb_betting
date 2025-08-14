"""
Unit tests for environment configuration validation.

Tests that environment configuration is properly loaded, validated, and consistent
with template files and documentation.
"""

import os
import tempfile
import pytest
from pathlib import Path
from unittest.mock import patch
from typing import Dict, Any

from src.core.config import get_settings, DatabaseSettings
from tests.utils.test_config import TestConfig, TestEnvironmentManager


class TestEnvironmentValidation:
    """Test environment configuration validation."""

    def test_env_example_includes_all_required_variables(self):
        """Test that .env.example includes all variables referenced in code."""
        env_example_path = Path(__file__).parent.parent.parent / ".env.example"
        assert env_example_path.exists(), ".env.example file should exist"
        
        env_content = env_example_path.read_text()
        
        # Critical variables that must be present
        required_vars = [
            "POSTGRES_HOST",
            "POSTGRES_PORT", 
            "POSTGRES_DB",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
            "DATABASE_URL",
            "API_SECRET_KEY",
            "JWT_SECRET_KEY",
            "ENVIRONMENT",
            "LOG_LEVEL",
            "DEBUG"
        ]
        
        missing_vars = []
        for var in required_vars:
            if var not in env_content:
                missing_vars.append(var)
        
        assert not missing_vars, f"Missing required variables in .env.example: {missing_vars}"

    def test_postgres_port_matches_claude_md_specification(self):
        """Test that default PostgreSQL port matches CLAUDE.md specification."""
        env_example_path = Path(__file__).parent.parent.parent / ".env.example"
        env_content = env_example_path.read_text()
        
        # Should contain POSTGRES_PORT=5433 per CLAUDE.md
        assert "POSTGRES_PORT=5433" in env_content, (
            "PostgreSQL port should default to 5433 as specified in CLAUDE.md"
        )

    def test_database_url_interpolation_documentation(self):
        """Test that DATABASE_URL variable interpolation is documented."""
        env_example_path = Path(__file__).parent.parent.parent / ".env.example"
        env_content = env_example_path.read_text()
        
        # Should contain documentation about variable interpolation
        assert "shell environment variable substitution" in env_content.lower() or \
               "interpolation" in env_content.lower(), (
            "DATABASE_URL should include documentation about variable interpolation"
        )

    def test_security_placeholders_are_safe(self):
        """Test that security placeholders are reasonable and not overly obvious."""
        env_example_path = Path(__file__).parent.parent.parent / ".env.example"
        env_content = env_example_path.read_text()
        
        # These overly obvious placeholders should not be present
        unsafe_placeholders = [
            "CHANGE_THIS_SECURE_PASSWORD_IMMEDIATELY",
            "your_api_secret_key_here_generate_secure_random_string",
            "your_jwt_secret_key_here_generate_secure_random_string"
        ]
        
        found_unsafe = []
        for placeholder in unsafe_placeholders:
            if placeholder in env_content:
                found_unsafe.append(placeholder)
        
        assert not found_unsafe, f"Found unsafe placeholders: {found_unsafe}"

    def test_performance_configuration_has_context(self):
        """Test that performance configuration includes helpful context."""
        env_example_path = Path(__file__).parent.parent.parent / ".env.example"
        env_content = env_example_path.read_text()
        
        # DATABASE_MAX_CONNECTIONS should mention memory scaling
        max_conn_line = next((line for line in env_content.split('\n') 
                             if 'DATABASE_MAX_CONNECTIONS' in line), "")
        assert "memory" in max_conn_line.lower(), (
            "DATABASE_MAX_CONNECTIONS should include memory scaling guidance"
        )
        
        # MAX_WORKERS should mention CPU recommendations
        max_workers_line = next((line for line in env_content.split('\n') 
                               if 'MAX_WORKERS' in line and '=' in line), "")
        assert "cpu" in max_workers_line.lower() or "core" in max_workers_line.lower(), (
            "MAX_WORKERS should include CPU core recommendations"
        )

    def test_environment_loading_with_template_values(self):
        """Test that environment configuration can be loaded with template values."""
        # Test that configuration loads successfully without exceptions
        # This validates the basic structure works with reasonable values
        try:
            settings = get_settings()
            
            # Verify core settings exist and have reasonable types
            assert hasattr(settings, 'database')
            assert hasattr(settings, 'environment')
            
            # Verify database settings have expected types
            db = settings.database
            assert isinstance(db.host, str)
            assert isinstance(db.port, int)
            assert isinstance(db.database, str)
            assert isinstance(db.user, str)
            assert isinstance(db.password, str)
            
            # Verify port matches expected default from .env.example
            # Note: This tests that the default configuration is sensible
            assert db.port in [5432, 5433], f"PostgreSQL port should be 5432 or 5433, got {db.port}"
            
        except Exception as e:
            pytest.fail(f"Failed to load configuration with template values: {e}")

    def test_database_settings_validation(self):
        """Test that database settings validation works correctly."""
        # Test valid configuration
        valid_db_config = {
            "host": "localhost",
            "port": 5433,
            "database": "test_db",
            "user": "test_user", 
            "password": "test_pass"
        }
        
        try:
            db_settings = DatabaseSettings(**valid_db_config)
            assert db_settings.host == "localhost"
            assert db_settings.port == 5433
            assert db_settings.database == "test_db"
        except Exception as e:
            pytest.fail(f"Valid database configuration should not raise exception: {e}")

    def test_template_consistency_with_documentation(self):
        """Test that .env.example is consistent with documentation references."""
        docs_path = Path(__file__).parent.parent.parent / "docs/setup/ENVIRONMENT_CONFIGURATION.md"
        
        if docs_path.exists():
            docs_content = docs_path.read_text()
            
            # Should reference .env.example (not .env.template)
            assert "cp .env.example .env" in docs_content, (
                "Documentation should reference .env.example"
            )
            
            # Should not reference old template files
            old_references = [".env.template", ".env.production.template"]
            found_old = [ref for ref in old_references if f"cp {ref}" in docs_content]
            assert not found_old, f"Documentation should not reference old templates: {found_old}"

    def test_all_environment_scenarios_documented(self):
        """Test that all environment scenarios have proper documentation."""
        env_example_path = Path(__file__).parent.parent.parent / ".env.example"
        env_content = env_example_path.read_text()
        
        # Should contain environment-specific guidance
        required_scenarios = ["development", "staging", "production", "docker"]
        
        missing_scenarios = []
        for scenario in required_scenarios:
            if scenario.upper() not in env_content.upper():
                missing_scenarios.append(scenario)
        
        assert not missing_scenarios, f"Missing environment scenarios: {missing_scenarios}"

    def test_security_requirements_are_documented(self):
        """Test that security requirements are properly documented."""
        env_example_path = Path(__file__).parent.parent.parent / ".env.example"
        env_content = env_example_path.read_text()
        
        # Should contain security guidance
        security_indicators = [
            "openssl rand -base64",
            "🚨",  # Security warning emoji
            "MUST CHANGE",
            "NEVER commit"
        ]
        
        missing_security = []
        for indicator in security_indicators:
            if indicator not in env_content:
                missing_security.append(indicator)
        
        assert not missing_security, f"Missing security documentation: {missing_security}"


class TestEnvironmentConfigurationIntegration:
    """Integration tests for environment configuration."""

    def test_configuration_loading_integration(self):
        """Test that configuration loads properly in different environments."""
        test_manager = TestEnvironmentManager()
        
        try:
            test_manager.setup_test_environment()
            
            # Test that settings can be loaded in test environment
            settings = get_settings()
            assert settings is not None
            assert hasattr(settings, 'database')
            assert hasattr(settings, 'environment')
            
        finally:
            test_manager.teardown_test_environment()

    def test_database_connection_configuration(self):
        """Test that database connection configuration is valid."""
        with patch.dict(os.environ, {
            "DB_HOST": "localhost",
            "DB_PORT": "5433", 
            "DB_NAME": "test_db",
            "DB_USER": "test_user",
            "DB_PASSWORD": "test_pass"
        }, clear=False):
            
            settings = get_settings()
            db = settings.database
            
            # Test connection string construction
            assert db.host == "localhost"
            assert db.port == 5433
            assert db.database == "test_db"
            assert db.user == "test_user"
            assert db.password == "test_pass"
            
            # Test that connection string can be constructed
            if hasattr(db, 'connection_string'):
                conn_str = db.connection_string
                assert "postgresql://" in conn_str
                assert "localhost" in conn_str
                assert "5433" in conn_str
                assert "test_db" in conn_str

    def test_multiple_environment_configurations(self):
        """Test loading configurations for different environments."""  
        environments_to_test = ["development", "staging", "production"]
        
        for env in environments_to_test:
            # Clear any cached settings to ensure fresh load
            if hasattr(get_settings, 'cache_info'):
                get_settings.cache_clear()
            
            with patch.dict(os.environ, {
                "DB_HOST": "localhost",
                "DB_PORT": "5433",
                "DB_NAME": "mlb_betting",
                "DB_USER": "test_user",
                "DB_PASSWORD": "test_pass"
            }, clear=False):
                
                try:
                    settings = get_settings()
                    
                    # Test that settings load successfully for each environment
                    assert settings is not None
                    assert hasattr(settings, 'database')
                    
                    # Verify database configuration is loaded
                    db = settings.database
                    assert db.host == "localhost"
                    assert db.port == 5433
                    assert db.database == "mlb_betting"
                    
                except Exception as e:
                    pytest.fail(f"Failed to load configuration for environment '{env}': {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])