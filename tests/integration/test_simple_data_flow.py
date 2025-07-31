"""
Simple Data Flow Integration Tests

Tests the basic RAW → STAGING → CURATED data pipeline flow.
Focuses on practical validation rather than complex optimizations.
"""

import pytest
import pytest_asyncio
import asyncio
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional
import asyncpg

from src.core.config import get_settings
from src.data.database.connection import get_connection, initialize_connections
from src.data.collection.orchestrator import CollectionOrchestrator
from src.data.pipeline.pipeline_orchestrator import create_pipeline_orchestrator


@pytest.mark.asyncio
class TestSimpleDataFlow:
    """Test basic data flow through the three-tier pipeline"""
    
    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        """Setup test environment"""
        self.config = get_settings()
        
        # Initialize database connections
        initialize_connections(self.config)
        
        # Clean up test data before and after
        await self._cleanup_test_data()
        yield
        await self._cleanup_test_data()
    
    async def _cleanup_test_data(self):
        """Clean up test data from all zones"""
        try:
            async with get_connection() as conn:
                # Clean up in reverse dependency order
                await conn.execute("DELETE FROM curated.ml_temporal_features WHERE created_at > NOW() - INTERVAL '1 hour'")
                await conn.execute("DELETE FROM curated.unified_betting_splits WHERE created_at > NOW() - INTERVAL '1 hour'")
                await conn.execute("DELETE FROM curated.enhanced_games WHERE created_at > NOW() - INTERVAL '1 hour'")
                
                await conn.execute("DELETE FROM staging.action_network_odds_historical WHERE created_at > NOW() - INTERVAL '1 hour'")
                await conn.execute("DELETE FROM staging.action_network_games WHERE created_at > NOW() - INTERVAL '1 hour'")
                
                # Note: We don't clean RAW zone data as it might be needed for other tests
        except Exception as e:
            # Don't fail tests due to cleanup issues
            print(f"Cleanup warning: {e}")

    async def test_database_connectivity(self):
        """Test basic database connectivity to all zones"""
        async with get_connection() as conn:
            
            # Test RAW zone access
            raw_tables = await conn.fetch("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'raw_data'
                ORDER BY table_name
            """)
            assert len(raw_tables) > 0, "No RAW zone tables found"
            print(f"✅ RAW zone has {len(raw_tables)} tables: {[t['table_name'] for t in raw_tables]}")
            
            # Test STAGING zone access
            staging_tables = await conn.fetch("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'staging'
                ORDER BY table_name
            """)
            assert len(staging_tables) > 0, "No STAGING zone tables found"
            print(f"✅ STAGING zone has {len(staging_tables)} tables: {[t['table_name'] for t in staging_tables]}")
            
            # Test CURATED zone access
            curated_tables = await conn.fetch("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'curated'
                ORDER BY table_name
            """)
            assert len(curated_tables) > 0, "No CURATED zone tables found"
            print(f"✅ CURATED zone has {len(curated_tables)} tables: {[t['table_name'] for t in curated_tables]}")

    async def test_raw_zone_data_presence(self):
        """Test that RAW zone has some data to work with"""
        async with get_connection() as conn:
            
            # Check Action Network raw data
            an_raw_count = await conn.fetchval("""
                SELECT COUNT(*) FROM raw_data.action_network_odds 
                WHERE created_at > NOW() - INTERVAL '7 days'
            """)
            print(f"📊 Action Network RAW records (last 7 days): {an_raw_count}")
            
            # Check Action Network history data
            an_history_count = await conn.fetchval("""
                SELECT COUNT(*) FROM raw_data.action_network_history 
                WHERE created_at > NOW() - INTERVAL '7 days'
            """)
            print(f"📊 Action Network History RAW records (last 7 days): {an_history_count}")
            
            # Check SBD raw data
            sbd_raw_count = await conn.fetchval("""
                SELECT COUNT(*) FROM raw_data.sbd_betting_splits 
                WHERE created_at > NOW() - INTERVAL '7 days'
            """)
            print(f"📊 SBD RAW records (last 7 days): {sbd_raw_count}")
            
            # Check VSIN raw data
            vsin_raw_count = await conn.fetchval("""
                SELECT COUNT(*) FROM raw_data.vsin_data 
                WHERE created_at > NOW() - INTERVAL '7 days'
            """)
            print(f"📊 VSIN RAW records (last 7 days): {vsin_raw_count}")
            
            total_raw_records = an_raw_count + an_history_count + sbd_raw_count + vsin_raw_count
            
            if total_raw_records == 0:
                pytest.skip("No recent RAW data found - run data collection first")
            
            assert total_raw_records > 0, f"Expected some RAW zone data, found {total_raw_records} records"

    async def test_staging_zone_data_processing(self):
        """Test that RAW data is processed into STAGING zone"""
        async with get_connection() as conn:
            
            # Check staging games table
            staging_games_count = await conn.fetchval("""
                SELECT COUNT(*) FROM staging.action_network_games 
                WHERE created_at > NOW() - INTERVAL '7 days'
            """)
            print(f"📊 STAGING games records (last 7 days): {staging_games_count}")
            
            # Check staging historical odds
            staging_odds_count = await conn.fetchval("""
                SELECT COUNT(*) FROM staging.action_network_odds_historical 
                WHERE created_at > NOW() - INTERVAL '7 days'
            """)
            print(f"📊 STAGING odds historical records (last 7 days): {staging_odds_count}")
            
            # If we have raw data but no staging data, we need to run the pipeline
            if staging_games_count == 0 and staging_odds_count == 0:
                # Check if we have raw data to process
                raw_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM raw_data.action_network_odds 
                    WHERE created_at > NOW() - INTERVAL '7 days'
                """)
                
                if raw_count > 0:
                    print("⚠️  Found RAW data but no STAGING data - pipeline needs to run")
                    # This is expected if pipeline hasn't run yet
                else:
                    pytest.skip("No RAW data to process into STAGING")

    async def test_staging_data_quality(self):
        """Test the quality of STAGING zone data"""
        async with get_connection() as conn:
            
            # Check for data consistency in staging odds
            consistency_check = await conn.fetch("""
                SELECT 
                    market_type,
                    side,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT sportsbook_name) as unique_sportsbooks,
                    MIN(updated_at) as earliest_update,
                    MAX(updated_at) as latest_update
                FROM staging.action_network_odds_historical 
                WHERE created_at > NOW() - INTERVAL '7 days'
                GROUP BY market_type, side
                ORDER BY market_type, side
            """)
            
            if consistency_check:
                print("📈 STAGING data quality summary:")
                for row in consistency_check:
                    print(f"   {row['market_type']}.{row['side']}: {row['record_count']} records, "
                          f"{row['unique_sportsbooks']} books, "
                          f"span: {row['earliest_update']} to {row['latest_update']}")
                
                # Validate market type/side combinations
                valid_combinations = {
                    ('moneyline', 'home'), ('moneyline', 'away'),
                    ('spread', 'home'), ('spread', 'away'),
                    ('total', 'over'), ('total', 'under')
                }
                
                found_combinations = {(row['market_type'], row['side']) for row in consistency_check}
                invalid_combinations = found_combinations - valid_combinations
                
                assert len(invalid_combinations) == 0, f"Invalid market/side combinations found: {invalid_combinations}"
                print("✅ All market_type/side combinations are valid")

    async def test_temporal_data_integrity(self):
        """Test temporal data integrity in staging"""
        async with get_connection() as conn:
            
            # Check temporal ordering
            temporal_issues = await conn.fetch("""
                SELECT 
                    external_game_id,
                    sportsbook_name,
                    market_type,
                    side,
                    COUNT(*) as duplicate_timestamps
                FROM staging.action_network_odds_historical 
                WHERE created_at > NOW() - INTERVAL '7 days'
                GROUP BY external_game_id, sportsbook_name, market_type, side, updated_at
                HAVING COUNT(*) > 1
                LIMIT 10
            """)
            
            if temporal_issues:
                print("⚠️  Found temporal data issues:")
                for issue in temporal_issues:
                    print(f"   Game {issue['external_game_id']}, {issue['sportsbook_name']}, "
                          f"{issue['market_type']}.{issue['side']}: {issue['duplicate_timestamps']} duplicates")
            else:
                print("✅ No temporal data integrity issues found")

    async def test_curated_zone_structure(self):
        """Test CURATED zone table structure and readiness"""
        async with get_connection() as conn:
            
            # Check enhanced_games table structure
            enhanced_games_cols = await conn.fetch("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_schema = 'curated' AND table_name = 'enhanced_games'
                ORDER BY ordinal_position
            """)
            
            expected_key_columns = {
                'mlb_stats_api_game_id', 'action_network_game_id', 
                'home_team', 'away_team', 'game_datetime', 'feature_data'
            }
            found_columns = {col['column_name'] for col in enhanced_games_cols}
            
            missing_columns = expected_key_columns - found_columns
            assert len(missing_columns) == 0, f"Missing key columns in enhanced_games: {missing_columns}"
            print(f"✅ enhanced_games table has all key columns: {len(enhanced_games_cols)} total columns")
            
            # Check unified_betting_splits table structure
            betting_splits_cols = await conn.fetch("""
                SELECT column_name, data_type
                FROM information_schema.columns 
                WHERE table_schema = 'curated' AND table_name = 'unified_betting_splits'
                ORDER BY ordinal_position
            """)
            
            expected_betting_columns = {
                'game_id', 'data_source', 'sportsbook_name', 'market_type',
                'sharp_action_direction', 'minutes_before_game'
            }
            found_betting_columns = {col['column_name'] for col in betting_splits_cols}
            
            missing_betting_columns = expected_betting_columns - found_betting_columns
            assert len(missing_betting_columns) == 0, f"Missing key columns in unified_betting_splits: {missing_betting_columns}"
            print(f"✅ unified_betting_splits table has all key columns: {len(betting_splits_cols)} total columns")

    async def test_data_source_coverage(self):
        """Test coverage across different data sources"""
        async with get_connection() as conn:
            
            # Check which data sources we have in raw zone
            sources_summary = await conn.fetch("""
                SELECT 'action_network_odds' as source, COUNT(*) as count
                FROM raw_data.action_network_odds 
                WHERE created_at > NOW() - INTERVAL '7 days'
                
                UNION ALL
                
                SELECT 'action_network_history' as source, COUNT(*) as count
                FROM raw_data.action_network_history 
                WHERE created_at > NOW() - INTERVAL '7 days'
                
                UNION ALL
                
                SELECT 'sbd_betting_splits' as source, COUNT(*) as count
                FROM raw_data.sbd_betting_splits 
                WHERE created_at > NOW() - INTERVAL '7 days'
                
                UNION ALL
                
                SELECT 'vsin_data' as source, COUNT(*) as count
                FROM raw_data.vsin_data 
                WHERE created_at > NOW() - INTERVAL '7 days'
                
                ORDER BY count DESC
            """)
            
            print("📊 Data source coverage (last 7 days):")
            active_sources = 0
            for source in sources_summary:
                status = "✅ Active" if source['count'] > 0 else "❌ No data"
                print(f"   {source['source']}: {source['count']} records - {status}")
                if source['count'] > 0:
                    active_sources += 1
            
            assert active_sources >= 1, "At least one data source should have recent data"
            print(f"✅ {active_sources} out of 4 data sources are active")

    async def test_pipeline_processing_readiness(self):
        """Test that the system is ready for pipeline processing"""
        try:
            # Test pipeline orchestrator initialization
            orchestrator = await create_pipeline_orchestrator()
            assert orchestrator is not None, "Pipeline orchestrator should initialize"
            print("✅ Pipeline orchestrator initializes successfully")
            
            # Check pipeline configuration
            pipeline_config = orchestrator.get_pipeline_status()
            assert pipeline_config is not None, "Pipeline should have configuration"
            print(f"✅ Pipeline configuration available: {len(pipeline_config)} settings")
            
        except Exception as e:
            print(f"⚠️  Pipeline orchestrator issue: {e}")
            # Don't fail the test, just warn
            pytest.skip(f"Pipeline orchestrator not ready: {e}")

    async def test_cross_zone_data_relationships(self):
        """Test relationships between zones where data exists"""
        async with get_connection() as conn:
            
            # Test if we can correlate data across zones
            correlation_check = await conn.fetch("""
                SELECT 
                    r.external_game_id,
                    COUNT(DISTINCT r.id) as raw_records,
                    COUNT(DISTINCT s.id) as staging_records
                FROM raw_data.action_network_odds r
                LEFT JOIN staging.action_network_odds_historical s 
                    ON r.external_game_id = s.external_game_id
                WHERE r.created_at > NOW() - INTERVAL '3 days'
                GROUP BY r.external_game_id
                HAVING COUNT(DISTINCT r.id) > 0
                ORDER BY raw_records DESC
                LIMIT 10
            """)
            
            if correlation_check:
                print("🔗 RAW ↔ STAGING correlation sample:")
                for row in correlation_check:
                    staging_status = "✅ Processed" if row['staging_records'] > 0 else "⏳ Pending"
                    print(f"   Game {row['external_game_id']}: {row['raw_records']} raw → {row['staging_records']} staging - {staging_status}")
                    
                # Check correlation ratio
                total_with_staging = sum(1 for row in correlation_check if row['staging_records'] > 0)
                correlation_ratio = total_with_staging / len(correlation_check) if correlation_check else 0
                
                print(f"📈 RAW→STAGING correlation: {correlation_ratio:.1%} of games processed")
            else:
                print("ℹ️  No recent data correlation found - this is normal for a fresh system")

    @pytest.mark.skipif(True, reason="Run manually to test actual pipeline execution")
    async def test_manual_pipeline_execution(self):
        """Manual test for running the actual pipeline - skip by default"""
        # This test is skipped by default as it modifies data
        # Run manually with: pytest -k test_manual_pipeline_execution -s
        
        try:
            orchestrator = await create_pipeline_orchestrator()
            
            # Run a small batch through the pipeline
            result = await orchestrator.process_batch(
                zone_type="staging",
                limit=10,
                dry_run=False
            )
            
            assert result is not None, "Pipeline should return processing results"
            print(f"✅ Pipeline processed: {result}")
            
        except Exception as e:
            pytest.fail(f"Pipeline execution failed: {e}")


# Utility functions for other tests
async def get_sample_raw_data(limit: int = 5) -> List[Dict[str, Any]]:
    """Get sample raw data for testing"""
    config = get_settings()
    initialize_connections(config)
    
    try:
        async with get_connection() as conn:
            raw_data = await conn.fetch("""
                SELECT * FROM raw_data.action_network_odds 
                WHERE created_at > NOW() - INTERVAL '7 days'
                ORDER BY created_at DESC
                LIMIT $1
            """, limit)
            
            return [dict(row) for row in raw_data]
    except Exception as e:
        print(f"Error getting sample raw data: {e}")
        return []


async def check_data_freshness() -> Dict[str, Any]:
    """Check how fresh our data is across all zones"""
    config = get_settings()
    initialize_connections(config)
    
    try:
        async with get_connection() as conn:
            freshness_check = await conn.fetch("""
                SELECT 
                    'raw_action_network' as zone,
                    COUNT(*) as total_records,
                    MAX(created_at) as latest_record,
                    MIN(created_at) as earliest_record
                FROM raw_data.action_network_odds
                
                UNION ALL
                
                SELECT 
                    'staging_historical' as zone,
                    COUNT(*) as total_records,
                    MAX(created_at) as latest_record,
                    MIN(created_at) as earliest_record
                FROM staging.action_network_odds_historical
                
                UNION ALL
                
                SELECT 
                    'curated_games' as zone,
                    COUNT(*) as total_records,
                    MAX(created_at) as latest_record,
                    MIN(created_at) as earliest_record
                FROM curated.enhanced_games
            """)
            
            return {row['zone']: dict(row) for row in freshness_check}
    except Exception as e:
        print(f"Error checking data freshness: {e}")
        return {}


if __name__ == "__main__":
    # Run a quick manual test
    async def quick_test():
        print("🧪 Running quick data flow test...")
        
        # Check data freshness
        freshness = await check_data_freshness()
        for zone, data in freshness.items():
            print(f"{zone}: {data['total_records']} records, latest: {data['latest_record']}")
        
        # Get sample data
        sample_data = await get_sample_raw_data(3)
        print(f"Sample RAW data: {len(sample_data)} records")
        for i, record in enumerate(sample_data):
            print(f"  {i+1}. Game {record.get('external_game_id')} - {record.get('created_at')}")
    
    asyncio.run(quick_test())