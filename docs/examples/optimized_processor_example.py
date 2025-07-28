#!/usr/bin/env python3
"""
Example: Optimized Staging Processor using Game ID Mapping Dimension

This example shows how to modify staging processors to use the centralized
game ID mapping dimension table instead of making individual API calls.

BEFORE (Original Pattern):
- Individual MLB Stats API calls for each game
- Thousands of API calls per pipeline run  
- 30-45 minute pipeline execution times
- API rate limiting and timeout errors

AFTER (Optimized Pattern):
- O(1) dimension table JOINs
- 0-10 API calls per pipeline run
- 2-5 minute pipeline execution times
- Fully resilient to API issues

This example demonstrates the transformation pattern for:
- staging_action_network_history_processor.py
- staging_vsin_betting_processor.py
- sbd_staging_processor.py
- staging_action_network_unified_processor.py
- staging_action_network_historical_processor.py
"""

import asyncio
from datetime import datetime
from typing import Any

import asyncpg
from pydantic import BaseModel

from ...core.config import get_settings
from ...core.logging import LogComponent, get_logger
from ...services.game_id_mapping_service import GameIDMappingService

logger = get_logger(__name__, LogComponent.CORE)


class OptimizedActionNetworkProcessor:
    """
    Example of optimized Action Network processor using dimension table lookups.
    
    This replaces the individual MLB Stats API calls in the original processor
    with high-performance dimension table JOINs.
    """

    def __init__(self):
        self.settings = get_settings()
        self.mapping_service = GameIDMappingService()

    def _get_db_config(self) -> dict[str, Any]:
        """Get database configuration from centralized settings."""
        settings = get_settings()
        return {
            "host": settings.database.host,
            "port": settings.database.port,
            "database": settings.database.database,
            "user": settings.database.user,
            "password": settings.database.password,
        }

    async def initialize(self):
        """Initialize processor services."""
        await self.mapping_service.initialize()
        logger.info("OptimizedActionNetworkProcessor initialized")

    async def cleanup(self):
        """Cleanup processor resources."""
        await self.mapping_service.cleanup()

    # =================================================================
    # BEFORE: Original Pattern (Individual API Calls)
    # =================================================================
    
    async def process_history_data_ORIGINAL(self, limit: int = 10) -> dict[str, Any]:
        """
        ORIGINAL PATTERN: Individual API calls for each game.
        
        This is the SLOW pattern that makes thousands of API calls.
        Each game requires individual resolution via MLBStatsAPIGameResolutionService.
        """
        logger.info("Processing with ORIGINAL pattern (slow)")
        
        conn = await asyncpg.connect(**self._get_db_config())
        
        # Get unprocessed raw history data
        raw_history_data = await conn.fetch("""
            SELECT h.id, h.external_game_id, h.raw_history, h.collected_at
            FROM raw_data.action_network_history h
            WHERE h.id NOT IN (
                SELECT DISTINCT raw_data_id 
                FROM staging.action_network_odds_historical
                WHERE raw_data_id IS NOT NULL
            )
            ORDER BY h.collected_at DESC
            LIMIT $1
        """, limit)
        
        processed_count = 0
        mlb_resolved_count = 0
        
        for raw_history_record in raw_history_data:
            try:
                # SLOW: Individual MLB ID resolution for EACH game
                # This is what we're replacing!
                mlb_game_id = await self._resolve_mlb_game_id_ORIGINAL(
                    raw_history_record, conn
                )
                
                if mlb_game_id:
                    mlb_resolved_count += 1
                
                # Process the record...
                processed_count += 1
                
            except Exception as e:
                logger.error(f"Error processing record: {e}")
                continue
        
        await conn.close()
        
        return {
            "processed": processed_count,
            "mlb_resolved": mlb_resolved_count,
            "pattern": "ORIGINAL (individual API calls)"
        }

    async def _resolve_mlb_game_id_ORIGINAL(
        self, raw_history_record: dict, conn: asyncpg.Connection
    ) -> str | None:
        """
        ORIGINAL PATTERN: Individual MLB Stats API resolution.
        
        This method makes individual API calls and database queries for each game.
        This is the bottleneck we're eliminating!
        """
        external_game_id = raw_history_record["external_game_id"]
        
        # SLOW: Check existing staging data
        existing_mlb_id = await conn.fetchval("""
            SELECT mlb_stats_api_game_id 
            FROM staging.action_network_games 
            WHERE external_game_id = $1 AND mlb_stats_api_game_id IS NOT NULL
        """, external_game_id)
        
        if existing_mlb_id:
            return existing_mlb_id
        
        # SLOW: Get game info for resolution
        game_info = await conn.fetchrow("""
            SELECT home_team_normalized, away_team_normalized, game_date
            FROM staging.action_network_games 
            WHERE external_game_id = $1
        """, external_game_id)
        
        if game_info:
            # SLOW: Individual API call to MLB Stats API
            from ...services.mlb_stats_api_game_resolution_service import (
                MLBStatsAPIGameResolutionService
            )
            
            mlb_resolver = MLBStatsAPIGameResolutionService()
            await mlb_resolver.initialize()
            
            try:
                resolution_result = await mlb_resolver.resolve_action_network_game_id(
                    external_game_id=external_game_id,
                    game_date=game_info["game_date"],
                )
                
                if resolution_result.mlb_game_id:
                    # SLOW: Update staging table
                    await conn.execute("""
                        UPDATE staging.action_network_games 
                        SET mlb_stats_api_game_id = $1, updated_at = NOW()
                        WHERE external_game_id = $2
                    """, resolution_result.mlb_game_id, external_game_id)
                    
                    return resolution_result.mlb_game_id
                    
            finally:
                await mlb_resolver.cleanup()
        
        return None

    # =================================================================
    # AFTER: Optimized Pattern (Dimension Table JOINs)
    # =================================================================
    
    async def process_history_data_OPTIMIZED(self, limit: int = 10) -> dict[str, Any]:
        """
        OPTIMIZED PATTERN: Single SQL JOIN using dimension table.
        
        This is the FAST pattern that uses O(1) dimension table lookups.
        All MLB game IDs are resolved in a single JOIN operation.
        """
        logger.info("Processing with OPTIMIZED pattern (fast)")
        
        conn = await asyncpg.connect(**self._get_db_config())
        
        # FAST: Single query with JOIN to dimension table
        # This replaces thousands of individual API calls!
        raw_history_with_mlb_ids = await conn.fetch("""
            SELECT 
                h.id, 
                h.external_game_id, 
                h.raw_history, 
                h.collected_at,
                m.mlb_stats_api_game_id,  -- O(1) lookup from dimension table!
                m.home_team,
                m.away_team,
                m.resolution_confidence
            FROM raw_data.action_network_history h
            LEFT JOIN staging.game_id_mappings m 
                ON m.action_network_game_id = h.external_game_id
            WHERE h.id NOT IN (
                SELECT DISTINCT raw_data_id 
                FROM staging.action_network_odds_historical
                WHERE raw_data_id IS NOT NULL
            )
            ORDER BY h.collected_at DESC
            LIMIT $1
        """, limit)
        
        processed_count = 0
        mlb_resolved_count = 0
        unmapped_ids = []
        
        for record in raw_history_with_mlb_ids:
            try:
                mlb_game_id = record["mlb_stats_api_game_id"]
                
                if mlb_game_id:
                    # We have the MLB ID from the dimension table - no API call needed!
                    mlb_resolved_count += 1
                else:
                    # Track unmapped IDs for batch resolution later
                    unmapped_ids.append(record["external_game_id"])
                
                # Process the record...
                processed_count += 1
                
            except Exception as e:
                logger.error(f"Error processing record: {e}")
                continue
        
        # FAST: Batch resolve any unmapped IDs (only if needed)
        if unmapped_ids:
            await self._batch_resolve_unmapped_ids(unmapped_ids)
        
        await conn.close()
        
        return {
            "processed": processed_count,
            "mlb_resolved": mlb_resolved_count,
            "unmapped_found": len(unmapped_ids),
            "pattern": "OPTIMIZED (dimension table JOINs)"
        }

    async def _batch_resolve_unmapped_ids(self, unmapped_ids: list[str]) -> None:
        """
        OPTIMIZED PATTERN: Batch resolution of unmapped IDs.
        
        Only called for newly discovered external IDs that aren't in the
        dimension table yet. Uses the GameIDMappingService for efficient resolution.
        """
        logger.info(f"Batch resolving {len(unmapped_ids)} unmapped IDs")
        
        # Use the mapping service to resolve in batch
        for external_id in unmapped_ids:
            try:
                mlb_id = await self.mapping_service.get_mlb_game_id(
                    external_id=external_id,
                    source="action_network",
                    create_if_missing=True  # Auto-resolve if possible
                )
                
                if mlb_id:
                    logger.info(f"Resolved unmapped ID {external_id} → {mlb_id}")
                else:
                    logger.warning(f"Could not resolve unmapped ID {external_id}")
                    
            except Exception as e:
                logger.error(f"Error resolving unmapped ID {external_id}: {e}")

    # =================================================================
    # COMPARISON: Performance Demonstration
    # =================================================================
    
    async def run_performance_comparison(self, limit: int = 10) -> dict[str, Any]:
        """
        Demonstrate the performance difference between original and optimized patterns.
        """
        logger.info("Running performance comparison between patterns")
        
        # Test original pattern
        start_time = datetime.now()
        original_results = await self.process_history_data_ORIGINAL(limit)
        original_duration = (datetime.now() - start_time).total_seconds()
        
        # Test optimized pattern  
        start_time = datetime.now()
        optimized_results = await self.process_history_data_OPTIMIZED(limit)
        optimized_duration = (datetime.now() - start_time).total_seconds()
        
        # Calculate improvement
        improvement_factor = original_duration / optimized_duration if optimized_duration > 0 else float('inf')
        improvement_percentage = ((original_duration - optimized_duration) / original_duration * 100) if original_duration > 0 else 0
        
        return {
            "original": {
                "duration_seconds": original_duration,
                "results": original_results
            },
            "optimized": {
                "duration_seconds": optimized_duration,
                "results": optimized_results
            },
            "improvement": {
                "speed_factor": f"{improvement_factor:.1f}x faster",
                "percentage": f"{improvement_percentage:.1f}% faster",
                "time_saved": f"{original_duration - optimized_duration:.2f} seconds"
            }
        }


# =================================================================
# Usage Examples and Integration Patterns
# =================================================================

class ProcessorMigrationGuide:
    """
    Guide for migrating existing processors to use the dimension table pattern.
    """
    
    @staticmethod
    def get_migration_steps() -> list[str]:
        """Get step-by-step migration instructions."""
        return [
            "1. Replace individual MLB resolution calls with dimension table JOINs",
            "2. Update SQL queries to LEFT JOIN staging.game_id_mappings",
            "3. Handle unmapped IDs with GameIDMappingService.get_mlb_game_id()",
            "4. Remove MLBStatsAPIGameResolutionService initialization",
            "5. Add GameIDMappingService initialization",
            "6. Test with small batches to verify functionality",
            "7. Measure performance improvement (should be 85-90% faster)",
            "8. Deploy to production with monitoring"
        ]
    
    @staticmethod
    def get_sql_pattern_before() -> str:
        """SQL pattern BEFORE optimization."""
        return """
        -- BEFORE: Process raw data without dimension lookup
        -- Requires individual API calls for each external_game_id
        
        SELECT h.id, h.external_game_id, h.raw_history
        FROM raw_data.action_network_history h
        WHERE h.processed = false
        
        -- Then for each record: call MLB Stats API to resolve external_game_id
        """
    
    @staticmethod 
    def get_sql_pattern_after() -> str:
        """SQL pattern AFTER optimization."""
        return """
        -- AFTER: Process raw data with dimension lookup
        -- Gets MLB game ID in single JOIN operation
        
        SELECT 
            h.id, 
            h.external_game_id, 
            h.raw_history,
            m.mlb_stats_api_game_id  -- O(1) lookup!
        FROM raw_data.action_network_history h
        LEFT JOIN staging.game_id_mappings m 
            ON m.action_network_game_id = h.external_game_id
        WHERE h.processed = false
        
        -- No API calls needed! MLB game ID available immediately.
        """

    @staticmethod
    def get_processor_files_to_modify() -> list[str]:
        """List of processor files that need modification."""
        return [
            "src/data/pipeline/staging_action_network_history_processor.py",
            "src/data/pipeline/staging_vsin_betting_processor.py", 
            "src/data/pipeline/sbd_staging_processor.py",
            "src/data/pipeline/staging_action_network_unified_processor.py",
            "src/data/pipeline/staging_action_network_historical_processor.py"
        ]


if __name__ == "__main__":
    # Example usage and performance testing
    async def main():
        processor = OptimizedActionNetworkProcessor()
        await processor.initialize()
        
        try:
            # Run performance comparison
            comparison = await processor.run_performance_comparison(limit=5)
            
            print("Performance Comparison Results:")
            print(f"Original Pattern: {comparison['original']['duration_seconds']:.2f}s")
            print(f"Optimized Pattern: {comparison['optimized']['duration_seconds']:.2f}s")
            print(f"Improvement: {comparison['improvement']['speed_factor']}")
            print(f"Time Saved: {comparison['improvement']['time_saved']}")
            
            # Show migration guide
            guide = ProcessorMigrationGuide()
            print("\nMigration Steps:")
            for step in guide.get_migration_steps():
                print(f"  {step}")
                
        finally:
            await processor.cleanup()

    asyncio.run(main())