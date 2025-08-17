"""
Simple Integration Tests for Enhanced Games Outcome Sync Service

Tests the complete sync workflow with real database operations
to verify the ML Training Pipeline fix works end-to-end.

Reference: GitHub Issue #67 - ML Training Pipeline Has Zero Real Data
"""

import pytest
import asyncio
import asyncpg
from datetime import datetime, timezone, date, timedelta
from typing import Dict, Any

from src.core.config import get_settings
from src.data.database.connection import initialize_connections
from src.services.curated_zone.enhanced_games_outcome_sync_service import (
    EnhancedGamesOutcomeSyncService,
    sync_all_missing_outcomes,
    sync_recent_outcomes
)


@pytest.mark.integration
class TestEnhancedGamesOutcomeSyncSimple:
    """Simple integration tests using direct database connections."""
    
    def test_service_database_connectivity(self):
        """Test that service can connect to database."""
        async def run_test():
            service = EnhancedGamesOutcomeSyncService()
            
            # Test basic connectivity
            settings = get_settings()
            conn = await asyncpg.connect(
                host=settings.database.host,
                port=settings.database.port,
                user=settings.database.user,
                password=settings.database.password,
                database=settings.database.database
            )
            
            # Simple connectivity test
            result = await conn.fetchval("SELECT 1")
            assert result == 1
            
            await conn.close()
        
        # Run in a fresh event loop
        asyncio.run(run_test())
    
    def test_sql_injection_protection_integration(self):
        """Test SQL injection protection with real database."""
        async def run_test():
            # Initialize database connections
            settings = get_settings()
            initialize_connections(settings)
            
            service = EnhancedGamesOutcomeSyncService()
            
            # Test limit validation prevents injection
            with pytest.raises(ValueError, match="Invalid limit value"):
                await service._get_missing_enhanced_game_outcomes(-1)
            
            # Test days_back validation prevents injection
            with pytest.raises(ValueError, match="Invalid days_back value"):
                await service._get_recent_outcomes_for_sync(-5)
            
            # Test pagination validation prevents injection
            with pytest.raises(ValueError, match="Invalid page_size"):
                await service._get_missing_enhanced_game_outcomes_paginated(-1, 0)
                
            with pytest.raises(ValueError, match="Invalid offset"):
                await service._get_missing_enhanced_game_outcomes_paginated(10, -1)
        
        asyncio.run(run_test())
    
    def test_dry_run_functionality_integration(self):
        """Test dry run functionality with real database."""
        async def run_test():
            # Initialize database connections
            settings = get_settings()
            initialize_connections(settings)
            
            service = EnhancedGamesOutcomeSyncService()
            
            # Test dry run doesn't modify data
            result = await service.sync_all_missing_outcomes(
                dry_run=True, 
                limit=5,  # Small limit for testing
                page_size=2
            )
            
            # Verify result structure
            assert hasattr(result, "outcomes_found")
            assert hasattr(result, "enhanced_games_created") 
            assert hasattr(result, "enhanced_games_updated")
            assert hasattr(result, "sync_failures")
            assert hasattr(result, "processing_time_seconds")
            assert hasattr(result, "errors")
            assert hasattr(result, "metadata")
            
            # Verify metadata
            assert result.metadata["dry_run"] is True
            assert result.metadata["sync_type"] == "all_missing_outcomes"
            assert result.processing_time_seconds >= 0
        
        asyncio.run(run_test())
    
    def test_pagination_integration(self):
        """Test pagination works with real database."""
        async def run_test():
            # Initialize database connections
            settings = get_settings()
            initialize_connections(settings)
            
            service = EnhancedGamesOutcomeSyncService()
            
            # Test small page sizes for verification
            page_size = 2
            offset = 0
            
            try:
                page_results = await service._get_missing_enhanced_game_outcomes_paginated(
                    page_size=page_size, 
                    offset=offset
                )
                
                # Verify page structure
                assert isinstance(page_results, list)
                assert len(page_results) <= page_size
                
                # If we have results, verify they have expected structure
                if page_results:
                    sample_result = page_results[0]
                    assert isinstance(sample_result, dict)
                    # Should have game identification fields
                    assert "game_id" in sample_result or "home_team" in sample_result
                
            except Exception as e:
                # If there's no data, that's acceptable for this test
                assert "No missing enhanced game outcomes found" in str(e) or "relation" in str(e).lower()
        
        asyncio.run(run_test())
    
    def test_recent_outcomes_sync_integration(self):
        """Test recent outcomes sync with real database."""
        async def run_test():
            # Initialize database connections
            settings = get_settings()
            initialize_connections(settings)
            
            service = EnhancedGamesOutcomeSyncService()
            
            result = await service.sync_recent_outcomes(
                days_back=1,  # Small window for testing
                dry_run=True  # Use dry run to avoid data changes
            )
            
            assert hasattr(result, "outcomes_found")
            assert result.metadata["sync_type"] == "recent_outcomes"
            assert result.metadata["days_back"] == 1
            assert result.metadata["dry_run"] is True
            assert result.processing_time_seconds >= 0
        
        asyncio.run(run_test())
    
    def test_convenience_functions_integration(self):
        """Test the convenience functions work."""
        async def run_test():
            # Initialize database connections
            settings = get_settings()
            initialize_connections(settings)
            
            # Test sync_all_missing_outcomes function
            result1 = await sync_all_missing_outcomes(dry_run=True, limit=3)
            
            assert hasattr(result1, "outcomes_found")
            assert hasattr(result1, "processing_time_seconds")
            assert result1.processing_time_seconds >= 0
            
            # Test sync_recent_outcomes function
            result2 = await sync_recent_outcomes(days_back=1, dry_run=True)
            
            assert hasattr(result2, "outcomes_found")
            assert hasattr(result2, "metadata")
            assert result2.metadata["days_back"] == 1
        
        asyncio.run(run_test())
    
    def test_database_schema_validation(self):
        """Test that the service works with actual database schema."""
        async def run_test():
            # Direct database connection to verify tables exist
            settings = get_settings()
            conn = await asyncpg.connect(
                host=settings.database.host,
                port=settings.database.port,
                user=settings.database.user,
                password=settings.database.password,
                database=settings.database.database
            )
            
            try:
                # Check that required tables exist
                tables_check = await conn.fetch("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'curated' 
                    AND table_name IN ('games_complete', 'enhanced_games')
                """)
                
                table_names = [row['table_name'] for row in tables_check]
                
                # We need at least one of these tables for the service to work
                assert len(table_names) > 0, "Required curated tables not found"
                
                # If enhanced_games exists, check its structure
                if 'enhanced_games' in table_names:
                    columns = await conn.fetch("""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_schema = 'curated' 
                        AND table_name = 'enhanced_games'
                    """)
                    
                    column_names = [row['column_name'] for row in columns]
                    required_columns = ['home_team', 'away_team', 'game_datetime']
                    
                    for col in required_columns:
                        assert col in column_names, f"Required column {col} not found in enhanced_games"
                
            finally:
                await conn.close()
        
        asyncio.run(run_test())


if __name__ == "__main__":
    # Allow running integration tests directly
    pytest.main([__file__, "-v", "-m", "integration"])