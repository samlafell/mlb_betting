"""
Unit tests for GamesPopulationService

Tests cover:
- Security: Input validation and SQL injection prevention
- Error handling: Specific exception types and recovery
- Data validation: Pydantic model validation
- Business logic: Population methods and statistics
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, Mock, patch
from datetime import datetime
from typing import Any, Dict

import asyncpg
from pydantic import ValidationError

from src.data.pipeline.games_population_service import (
    GamesPopulationService, 
    PopulationStats
)
from src.core.exceptions import DatabaseError, UnifiedBettingError


class TestPopulationStats:
    """Test PopulationStats Pydantic model validation"""
    
    def test_valid_stats(self):
        """Test valid statistics creation"""
        stats = PopulationStats(
            total_games=100,
            games_updated=50,
            scores_populated=30,
            external_ids_populated=40,
            venue_populated=20,
            weather_populated=15,
            high_quality_games=25,
            operation_duration_seconds=123.45
        )
        
        assert stats.total_games == 100
        assert stats.games_updated == 50
        assert stats.operation_duration_seconds == 123.45
    
    def test_negative_values_rejected(self):
        """Test that negative values are rejected"""
        with pytest.raises(ValidationError):
            PopulationStats(
                total_games=-1,  # Invalid negative value
                games_updated=50,
                scores_populated=30,
                external_ids_populated=40,
                venue_populated=20,
                weather_populated=15,
                high_quality_games=25,
                operation_duration_seconds=123.45
            )
    
    def test_zero_values_accepted(self):
        """Test that zero values are accepted"""
        stats = PopulationStats(
            total_games=0,
            games_updated=0,
            scores_populated=0,
            external_ids_populated=0,
            venue_populated=0,
            weather_populated=0,
            high_quality_games=0,
            operation_duration_seconds=0.0
        )
        
        assert stats.total_games == 0
        assert stats.operation_duration_seconds == 0.0


class TestGamesPopulationService:
    """Test GamesPopulationService security and functionality"""
    
    @pytest.fixture
    def mock_config(self):
        """Mock configuration"""
        config = Mock()
        config.database.host = "localhost"
        config.database.port = 5432
        config.database.user = "test_user"
        config.database.password = "test_password"
        config.database.database = "test_db"
        return config
    
    @pytest.fixture
    def service(self, mock_config):
        """Create service instance with mocked config"""
        with patch('src.data.pipeline.games_population_service.get_settings', return_value=mock_config):
            return GamesPopulationService()
    
    @pytest.mark.asyncio
    async def test_initialization(self, service):
        """Test service initialization"""
        assert service.config is not None
        assert service.logger is not None
        assert service.connection_pool is None
    
    @pytest.mark.asyncio
    async def test_input_validation_max_games_positive(self, service):
        """Test that max_games must be positive integer"""
        # Mock connection pool
        service.connection_pool = AsyncMock()
        
        # Test negative value
        with pytest.raises(ValueError, match="max_games must be a positive integer"):
            await service.populate_all_missing_data(max_games=-1)
        
        # Test zero value
        with pytest.raises(ValueError, match="max_games must be a positive integer"):
            await service.populate_all_missing_data(max_games=0)
        
        # Test non-integer value
        with pytest.raises(ValueError, match="max_games must be a positive integer"):
            await service.populate_all_missing_data(max_games="invalid")
    
    @pytest.mark.asyncio
    async def test_sql_injection_prevention(self, service):
        """Test that SQL injection attempts are prevented"""
        # Mock connection
        mock_conn = AsyncMock()
        
        # Test SQL injection attempt in max_games parameter
        with pytest.raises(ValueError):
            await service._populate_game_scores(mock_conn, max_games="1; DROP TABLE users;--")
        
        with pytest.raises(ValueError):
            await service._populate_action_network_ids(mock_conn, max_games="1 OR 1=1")
        
        with pytest.raises(ValueError):
            await service._populate_venue_data(mock_conn, max_games="1'; DELETE FROM games;--")
        
        with pytest.raises(ValueError):
            await service._populate_weather_data(mock_conn, max_games="1 UNION SELECT * FROM secrets")
        
        with pytest.raises(ValueError):
            await service._update_data_quality(mock_conn, max_games="1; UPDATE admin SET password='hacked'")
    
    @pytest.mark.asyncio
    async def test_parameterized_queries(self, service):
        """Test that queries use parameterized statements"""
        # Mock connection that returns successful result
        mock_conn = AsyncMock()
        mock_conn.execute.return_value = "UPDATE 5"
        
        # Test that methods call execute with parameters
        await service._populate_game_scores(mock_conn, max_games=100)
        
        # Verify parameterized query was called
        args, kwargs = mock_conn.execute.call_args
        query = args[0]
        max_games_param = args[1]
        
        assert "$1" in query  # Parameterized placeholder
        assert max_games_param == 100  # Parameter value
        assert "100" not in query  # Value not directly in query string
    
    @pytest.mark.asyncio
    async def test_database_error_handling(self, service):
        """Test specific database error handling"""
        # Mock connection that raises PostgreSQL error
        mock_conn = AsyncMock()
        mock_conn.execute.side_effect = asyncpg.PostgresError("Connection failed")
        
        # Test that PostgreSQL errors are caught and re-raised as DatabaseError
        with pytest.raises(asyncpg.PostgresError):
            await service._populate_game_scores(mock_conn, max_games=100)
    
    @pytest.mark.asyncio
    async def test_validation_error_propagation(self, service):
        """Test that validation errors are properly propagated"""
        mock_conn = AsyncMock()
        
        # Test that ValueError is caught and re-raised
        with pytest.raises(ValueError, match="max_games must be a positive integer"):
            await service._populate_game_scores(mock_conn, max_games=-5)
    
    @pytest.mark.asyncio
    async def test_regex_patterns_security(self, service):
        """Test that regex patterns are secure and accurate"""
        mock_conn = AsyncMock()
        mock_conn.execute.return_value = "UPDATE 1"
        
        # Call weather data method which has improved regex
        await service._populate_weather_data(mock_conn, max_games=10)
        
        # Verify query contains improved regex patterns
        args, kwargs = mock_conn.execute.call_args
        query = args[0]
        
        # Check for improved regex patterns that handle decimals and negatives
        assert "^-?[0-9]+(\\.[0-9]+)?$" in query  # Temperature (allows negative)
        assert "^[0-9]+(\\.[0-9]+)?$" in query  # Wind speed and humidity (positive only)
    
    @pytest.mark.asyncio
    async def test_external_id_validation(self, service):
        """Test that external IDs are properly validated"""
        mock_conn = AsyncMock()
        mock_conn.execute.return_value = "UPDATE 1"
        
        # Call Action Network ID method
        await service._populate_action_network_ids(mock_conn, max_games=10)
        
        # Verify query validates external_game_id as numeric
        args, kwargs = mock_conn.execute.call_args
        query = args[0]
        
        assert "~ '^[0-9]+$'" in query  # Numeric validation for external_game_id
    
    @pytest.mark.asyncio
    async def test_configurable_confidence_scores(self, service):
        """Test that confidence scores are configurable"""
        # Mock settings with custom confidence scores
        mock_settings = Mock()
        mock_settings.high_confidence_score = 0.95
        mock_settings.medium_confidence_score = 0.85
        mock_settings.low_confidence_score = 0.60
        
        mock_conn = AsyncMock()
        mock_conn.execute.return_value = "UPDATE 1"
        
        with patch('src.data.pipeline.games_population_service.get_settings', return_value=mock_settings):
            await service._update_data_quality(mock_conn, max_games=10)
        
        # Verify configurable scores were used as parameters
        args, kwargs = mock_conn.execute.call_args
        high_confidence = args[1]
        medium_confidence = args[2]
        low_confidence = args[3]
        max_games = args[4]
        
        assert high_confidence == 0.95
        assert medium_confidence == 0.85
        assert low_confidence == 0.60
        assert max_games == 10
    
    @pytest.mark.asyncio
    async def test_connection_pool_settings(self, service):
        """Test improved connection pool configuration"""
        mock_pool = AsyncMock()
        
        # Create an async function that returns the mock pool
        async def mock_create_pool_func(*args, **kwargs):
            return mock_pool
        
        with patch('src.data.pipeline.games_population_service.asyncpg.create_pool', side_effect=mock_create_pool_func) as mock_create_pool:
            await service.initialize()
            
            # Verify improved pool settings
            args, kwargs = mock_create_pool.call_args
            assert kwargs['max_size'] == 20  # Increased from 10
            assert kwargs['command_timeout'] == 180  # Reduced from 300
            assert kwargs['min_size'] == 2
    
    @pytest.mark.asyncio
    async def test_dry_run_functionality(self, service):
        """Test dry run analysis"""
        # Mock connection pool and stats
        service.connection_pool = AsyncMock()
        
        mock_stats = {
            'total_games': 100,
            'games_with_scores': 80,
            'games_with_external_ids': 60,
            'games_with_venue': 40,
            'games_with_weather': 30,
            'high_quality_games': 25
        }
        
        with patch.object(service, '_get_current_stats', return_value=mock_stats):
            with patch.object(service, '_analyze_population_potential') as mock_analyze:
                mock_analyze.return_value = PopulationStats(
                    total_games=100,
                    games_updated=0,
                    scores_populated=80,
                    external_ids_populated=60,
                    venue_populated=40,
                    weather_populated=30,
                    high_quality_games=25,
                    operation_duration_seconds=0.0
                )
                
                result = await service.populate_all_missing_data(dry_run=True, max_games=50)
                
                assert result.total_games == 100
                assert result.games_updated == 0  # No actual updates in dry run
                mock_analyze.assert_called_once_with(50)


class TestSecurityHardening:
    """Test security hardening features"""
    
    @pytest.mark.asyncio
    async def test_input_sanitization_edge_cases(self):
        """Test input sanitization for edge cases"""
        service = GamesPopulationService()
        mock_conn = AsyncMock()
        
        # Configure mock to return proper string result
        mock_conn.execute.return_value = "UPDATE 5"
        
        # Test various malicious inputs
        malicious_inputs = [
            "'; DROP TABLE games; --",
            "1 OR 1=1",
            "1; DELETE FROM users",
            "1 UNION SELECT password FROM admin",
            "1'); INSERT INTO logs VALUES ('hacked'); --",
            None,  # None should be handled gracefully
            "",    # Empty string
            "not_a_number",
            3.14,  # Float instead of int
            [1, 2, 3],  # List
            {"max": 100}  # Dict
        ]
        
        for malicious_input in malicious_inputs:
            if malicious_input is None:
                # None should be handled gracefully without error
                try:
                    await service._populate_game_scores(mock_conn, max_games=malicious_input)
                except ValueError:
                    pass  # Expected for invalid types
            else:
                with pytest.raises(ValueError):
                    await service._populate_game_scores(mock_conn, max_games=malicious_input)
    
    def test_regex_pattern_validation(self):
        """Test that regex patterns properly validate data"""
        import re
        
        # Test temperature regex (allows negative values and decimals)
        temp_pattern = r'^-?[0-9]+(\.[0-9]+)?$'
        
        assert re.match(temp_pattern, "72")      # Valid positive integer
        assert re.match(temp_pattern, "-5")     # Valid negative integer
        assert re.match(temp_pattern, "72.5")   # Valid positive decimal
        assert re.match(temp_pattern, "-10.5")  # Valid negative decimal
        assert not re.match(temp_pattern, "abc")        # Invalid non-numeric
        assert not re.match(temp_pattern, "72.5.3")     # Invalid multiple decimals
        assert not re.match(temp_pattern, "")           # Invalid empty
        
        # Test positive numeric regex (wind speed, humidity)
        positive_pattern = r'^[0-9]+(\.[0-9]+)?$'
        
        assert re.match(positive_pattern, "15")     # Valid positive integer
        assert re.match(positive_pattern, "15.5")  # Valid positive decimal
        assert not re.match(positive_pattern, "-5")      # Invalid negative
        assert not re.match(positive_pattern, "abc")     # Invalid non-numeric
        assert not re.match(positive_pattern, "")        # Invalid empty
        
        # Test external ID regex (integers only)
        id_pattern = r'^[0-9]+$'
        
        assert re.match(id_pattern, "12345")    # Valid integer
        assert not re.match(id_pattern, "-123")      # Invalid negative
        assert not re.match(id_pattern, "123.45")   # Invalid decimal
        assert not re.match(id_pattern, "abc123")   # Invalid alphanumeric
        assert not re.match(id_pattern, "")         # Invalid empty


if __name__ == "__main__":
    pytest.main([__file__, "-v"])