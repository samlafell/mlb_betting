#!/usr/bin/env python3
"""
Tests for Game ID Mappings Migration (019_create_game_id_mappings_dimension.sql)

Tests the database migration for creating the centralized game ID mappings
dimension table, including:
- Table creation and constraints
- Index creation and performance
- Utility functions
- Data integrity validation
- Migration rollback capability
"""

import asyncio
from datetime import date
from decimal import Decimal

import asyncpg
import pytest

from src.data.database.connection import get_connection


class TestGameIDMappingsMigration:
    """Test suite for game ID mappings migration."""

    @pytest.fixture(autouse=True)
    async def setup_and_teardown(self):
        """Setup test environment and clean up after each test."""
        # Setup - ensure clean state
        async with get_connection() as conn:
            # Drop test data if exists
            await conn.execute("DELETE FROM staging.game_id_mappings WHERE mlb_stats_api_game_id LIKE 'TEST_%'")

        yield

        # Teardown - clean up test data
        async with get_connection() as conn:
            await conn.execute("DELETE FROM staging.game_id_mappings WHERE mlb_stats_api_game_id LIKE 'TEST_%'")

    async def test_table_creation(self):
        """Test that the game_id_mappings table exists with proper structure."""
        async with get_connection() as conn:
            # Check table exists
            table_exists = await conn.fetchval("""
                SELECT EXISTS(
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'staging' 
                    AND table_name = 'game_id_mappings'
                )
            """)
            assert table_exists, "game_id_mappings table should exist"

            # Check column structure
            columns = await conn.fetch("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'staging' AND table_name = 'game_id_mappings'
                ORDER BY ordinal_position
            """)

            column_dict = {col['column_name']: col for col in columns}

            # Verify key columns exist
            expected_columns = [
                'id', 'mlb_stats_api_game_id', 'action_network_game_id',
                'vsin_game_id', 'sbd_game_id', 'sbr_game_id',
                'home_team', 'away_team', 'game_date', 'game_datetime',
                'resolution_confidence', 'primary_source', 'last_verified_at',
                'verification_attempts', 'created_at', 'updated_at'
            ]

            for col_name in expected_columns:
                assert col_name in column_dict, f"Column {col_name} should exist"

            # Verify NOT NULL constraints
            assert column_dict['mlb_stats_api_game_id']['is_nullable'] == 'NO'
            assert column_dict['home_team']['is_nullable'] == 'NO'
            assert column_dict['away_team']['is_nullable'] == 'NO'
            assert column_dict['game_date']['is_nullable'] == 'NO'

    async def test_table_constraints(self):
        """Test table constraints and validation rules."""
        async with get_connection() as conn:
            # Test unique constraint on mlb_stats_api_game_id
            await conn.execute("""
                INSERT INTO staging.game_id_mappings 
                (mlb_stats_api_game_id, action_network_game_id, home_team, away_team, game_date, primary_source)
                VALUES ('TEST_123', 'AN_123', 'LAD', 'NYY', '2024-07-01', 'action_network')
            """)

            # Should fail due to unique constraint
            with pytest.raises(asyncpg.UniqueViolationError):
                await conn.execute("""
                    INSERT INTO staging.game_id_mappings 
                    (mlb_stats_api_game_id, vsin_game_id, home_team, away_team, game_date, primary_source)
                    VALUES ('TEST_123', 'VSIN_123', 'SF', 'OAK', '2024-07-01', 'vsin')
                """)

            # Test confidence constraint (should be between 0.0 and 1.0)
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute("""
                    INSERT INTO staging.game_id_mappings 
                    (mlb_stats_api_game_id, action_network_game_id, home_team, away_team, game_date, 
                     resolution_confidence, primary_source)
                    VALUES ('TEST_124', 'AN_124', 'LAD', 'NYY', '2024-07-01', 1.5, 'action_network')
                """)

            # Test has_at_least_one_external_id constraint
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute("""
                    INSERT INTO staging.game_id_mappings 
                    (mlb_stats_api_game_id, home_team, away_team, game_date, primary_source)
                    VALUES ('TEST_125', 'LAD', 'NYY', '2024-07-01', 'manual')
                """)

            # Test valid primary_source constraint
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute("""
                    INSERT INTO staging.game_id_mappings 
                    (mlb_stats_api_game_id, action_network_game_id, home_team, away_team, game_date, primary_source)
                    VALUES ('TEST_126', 'AN_126', 'LAD', 'NYY', '2024-07-01', 'invalid_source')
                """)

    async def test_indexes_creation(self):
        """Test that all required indexes are created."""
        async with get_connection() as conn:
            indexes = await conn.fetch("""
                SELECT indexname, indexdef
                FROM pg_indexes 
                WHERE tablename = 'game_id_mappings'
                AND schemaname = 'staging'
                ORDER BY indexname
            """)

            index_names = [idx['indexname'] for idx in indexes]

            expected_indexes = [
                'game_id_mappings_pkey',  # Primary key
                'idx_game_mappings_mlb_id',
                'idx_game_mappings_action_network',
                'idx_game_mappings_vsin',
                'idx_game_mappings_sbd',
                'idx_game_mappings_sbr',
                'idx_game_mappings_date_teams',
                'idx_game_mappings_verification',
                'idx_game_mappings_external_ids'
            ]

            for expected_idx in expected_indexes:
                assert expected_idx in index_names, f"Index {expected_idx} should exist"

    async def test_utility_functions_exist(self):
        """Test that utility functions are created."""
        async with get_connection() as conn:
            # Check function existence
            functions = await conn.fetch("""
                SELECT routine_name 
                FROM information_schema.routines
                WHERE routine_schema = 'staging'
                AND routine_name IN ('find_unmapped_external_ids', 'get_game_id_mapping_stats', 'validate_game_id_mappings')
            """)

            function_names = [func['routine_name'] for func in functions]

            expected_functions = [
                'find_unmapped_external_ids',
                'get_game_id_mapping_stats',
                'validate_game_id_mappings'
            ]

            for expected_func in expected_functions:
                assert expected_func in function_names, f"Function {expected_func} should exist"

    async def test_find_unmapped_external_ids_function(self):
        """Test the find_unmapped_external_ids utility function."""
        async with get_connection() as conn:
            # Create test data in raw_data.action_network_games
            await conn.execute("""
                INSERT INTO raw_data.action_network_games 
                (external_game_id, home_team, away_team, start_time, created_at)
                VALUES ('TEST_UNMAPPED_123', 'LAD', 'NYY', '2024-07-01 19:00:00', NOW())
                ON CONFLICT (external_game_id) DO UPDATE SET updated_at = NOW()
            """)

            # Call function
            results = await conn.fetch("""
                SELECT * FROM staging.find_unmapped_external_ids('action_network', 10)
            """)

            # Should find our test unmapped ID
            test_result = None
            for result in results:
                if result['external_id'] == 'TEST_UNMAPPED_123':
                    test_result = result
                    break

            assert test_result is not None, "Should find test unmapped ID"
            assert test_result['source_type'] == 'action_network'
            assert test_result['raw_table'] == 'raw_data.action_network_games'

            # Clean up
            await conn.execute("DELETE FROM raw_data.action_network_games WHERE external_game_id = 'TEST_UNMAPPED_123'")

    async def test_get_mapping_stats_function(self):
        """Test the get_game_id_mapping_stats utility function."""
        async with get_connection() as conn:
            # Insert test data
            await conn.execute("""
                INSERT INTO staging.game_id_mappings 
                (mlb_stats_api_game_id, action_network_game_id, vsin_game_id, home_team, away_team, game_date, 
                 resolution_confidence, primary_source)
                VALUES 
                ('TEST_STATS_1', 'AN_STATS_1', 'VSIN_STATS_1', 'LAD', 'NYY', '2024-07-01', 0.95, 'action_network'),
                ('TEST_STATS_2', 'AN_STATS_2', NULL, 'SF', 'OAK', '2024-07-01', 0.85, 'action_network')
            """)

            # Call function
            stats = await conn.fetchrow("SELECT * FROM staging.get_game_id_mapping_stats()")

            assert stats['total_mappings'] >= 2, "Should have at least 2 test mappings"
            assert stats['action_network_count'] >= 2, "Should have at least 2 Action Network mappings"
            assert stats['vsin_count'] >= 1, "Should have at least 1 VSIN mapping"
            assert float(stats['avg_confidence']) > 0.0, "Should have positive average confidence"
            assert stats['last_updated'] is not None, "Should have last updated timestamp"

    async def test_validate_mappings_function(self):
        """Test the validate_game_id_mappings utility function."""
        async with get_connection() as conn:
            # Insert test data with low confidence
            await conn.execute("""
                INSERT INTO staging.game_id_mappings 
                (mlb_stats_api_game_id, action_network_game_id, home_team, away_team, game_date, 
                 resolution_confidence, primary_source, last_verified_at)
                VALUES 
                ('TEST_LOW_CONF', 'AN_LOW_CONF', 'LAD', 'NYY', '2024-07-01', 0.5, 'action_network', NOW() - INTERVAL '40 days')
            """)

            # Call function
            validations = await conn.fetch("SELECT * FROM staging.validate_game_id_mappings()")

            # Convert to dict for easier testing
            validation_dict = {v['validation_type']: v for v in validations}

            # Should detect low confidence mapping
            if 'low_confidence_mappings' in validation_dict:
                assert validation_dict['low_confidence_mappings']['issue_count'] >= 1

            # Should detect old unverified mapping
            if 'unverified_mappings' in validation_dict:
                assert validation_dict['unverified_mappings']['issue_count'] >= 1

    async def test_data_insertion_and_retrieval(self):
        """Test basic data insertion and retrieval operations."""
        async with get_connection() as conn:
            # Insert test record
            test_data = {
                'mlb_stats_api_game_id': 'TEST_INSERT_123',
                'action_network_game_id': 'AN_INSERT_123',
                'vsin_game_id': 'VSIN_INSERT_123',
                'home_team': 'LAD',
                'away_team': 'NYY',
                'game_date': date(2024, 7, 1),
                'resolution_confidence': Decimal('0.95'),
                'primary_source': 'action_network'
            }

            await conn.execute("""
                INSERT INTO staging.game_id_mappings 
                (mlb_stats_api_game_id, action_network_game_id, vsin_game_id, home_team, away_team, 
                 game_date, resolution_confidence, primary_source)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """, *test_data.values())

            # Retrieve and verify
            retrieved = await conn.fetchrow("""
                SELECT * FROM staging.game_id_mappings 
                WHERE mlb_stats_api_game_id = $1
            """, test_data['mlb_stats_api_game_id'])

            assert retrieved is not None, "Should retrieve inserted record"
            assert retrieved['mlb_stats_api_game_id'] == test_data['mlb_stats_api_game_id']
            assert retrieved['action_network_game_id'] == test_data['action_network_game_id']
            assert retrieved['vsin_game_id'] == test_data['vsin_game_id']
            assert retrieved['home_team'] == test_data['home_team']
            assert retrieved['away_team'] == test_data['away_team']
            assert retrieved['game_date'] == test_data['game_date']
            assert retrieved['resolution_confidence'] == test_data['resolution_confidence']
            assert retrieved['primary_source'] == test_data['primary_source']
            assert retrieved['created_at'] is not None
            assert retrieved['updated_at'] is not None

    async def test_index_performance(self):
        """Test that indexes provide expected performance benefits."""
        async with get_connection() as conn:
            # Insert test data for performance testing
            test_records = []
            for i in range(100):
                test_records.append((
                    f'TEST_PERF_{i}',
                    f'AN_PERF_{i}',
                    f'VSIN_PERF_{i}',
                    'LAD',
                    'NYY',
                    '2024-07-01',
                    0.95,
                    'action_network'
                ))

            await conn.executemany("""
                INSERT INTO staging.game_id_mappings 
                (mlb_stats_api_game_id, action_network_game_id, vsin_game_id, home_team, away_team, 
                 game_date, resolution_confidence, primary_source)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """, test_records)

            # Test index usage with EXPLAIN
            explain_result = await conn.fetch("""
                EXPLAIN (FORMAT JSON) 
                SELECT mlb_stats_api_game_id 
                FROM staging.game_id_mappings 
                WHERE action_network_game_id = 'AN_PERF_50'
            """)

            # Should use index scan (not sequential scan)
            plan = explain_result[0]['QUERY PLAN'][0]['Plan']
            assert 'Index' in plan['Node Type'], "Should use index scan for action_network_game_id lookup"

    async def test_migration_rollback_compatibility(self):
        """Test that migration can be rolled back safely."""
        async with get_connection() as conn:
            # Test that we can drop the created elements (simulating rollback)

            # Drop functions (in reverse dependency order)
            await conn.execute("DROP FUNCTION IF EXISTS staging.find_unmapped_external_ids(VARCHAR, INTEGER)")
            await conn.execute("DROP FUNCTION IF EXISTS staging.get_game_id_mapping_stats()")
            await conn.execute("DROP FUNCTION IF EXISTS staging.validate_game_id_mappings()")

            # Drop table (indexes will be dropped automatically)
            await conn.execute("DROP TABLE IF EXISTS staging.game_id_mappings")

            # Verify table no longer exists
            table_exists = await conn.fetchval("""
                SELECT EXISTS(
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'staging' 
                    AND table_name = 'game_id_mappings'
                )
            """)
            assert not table_exists, "Table should not exist after rollback"

            # Re-create table for other tests (simulate re-running migration)
            await self._recreate_table_and_functions(conn)

    async def _recreate_table_and_functions(self, conn):
        """Helper method to recreate table and functions for rollback test."""
        # Read and execute the migration SQL
        # This is a simplified version - in practice, you'd read the actual migration file
        await conn.execute("""
            CREATE TABLE staging.game_id_mappings (
                id BIGSERIAL PRIMARY KEY,
                mlb_stats_api_game_id VARCHAR(50) UNIQUE NOT NULL,
                action_network_game_id VARCHAR(255),
                vsin_game_id VARCHAR(255), 
                sbd_game_id VARCHAR(255),
                sbr_game_id VARCHAR(255),
                home_team VARCHAR(100) NOT NULL,
                away_team VARCHAR(100) NOT NULL,
                game_date DATE NOT NULL,
                game_datetime TIMESTAMPTZ,
                resolution_confidence DECIMAL(3,2) DEFAULT 1.0,
                primary_source VARCHAR(50),
                last_verified_at TIMESTAMPTZ DEFAULT NOW(),
                verification_attempts INTEGER DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                CONSTRAINT valid_confidence CHECK (resolution_confidence BETWEEN 0.0 AND 1.0),
                CONSTRAINT has_at_least_one_external_id CHECK (
                    action_network_game_id IS NOT NULL OR
                    vsin_game_id IS NOT NULL OR 
                    sbd_game_id IS NOT NULL OR
                    sbr_game_id IS NOT NULL
                ),
                CONSTRAINT valid_primary_source CHECK (
                    primary_source IN ('action_network', 'vsin', 'sbd', 'sbr', 'manual')
                )
            );
        """)

        # Recreate key indexes
        await conn.execute("""
            CREATE INDEX idx_game_mappings_action_network 
            ON staging.game_id_mappings(action_network_game_id) 
            WHERE action_network_game_id IS NOT NULL;
        """)

        # Recreate essential functions (simplified versions)
        await conn.execute("""
            CREATE OR REPLACE FUNCTION staging.get_game_id_mapping_stats()
            RETURNS TABLE(
                total_mappings BIGINT,
                action_network_count BIGINT,
                vsin_count BIGINT,
                sbd_count BIGINT,
                sbr_count BIGINT,
                avg_confidence DECIMAL,
                last_updated TIMESTAMPTZ
            ) AS $$
            BEGIN
                RETURN QUERY
                SELECT 
                    COUNT(*) as total_mappings,
                    COUNT(m.action_network_game_id) as action_network_count,
                    COUNT(m.vsin_game_id) as vsin_count,
                    COUNT(m.sbd_game_id) as sbd_count,
                    COUNT(m.sbr_game_id) as sbr_count,
                    AVG(m.resolution_confidence) as avg_confidence,
                    MAX(m.updated_at) as last_updated
                FROM staging.game_id_mappings m;
            END;
            $$ LANGUAGE plpgsql;
        """)

        await conn.execute("""
            CREATE OR REPLACE FUNCTION staging.validate_game_id_mappings()
            RETURNS TABLE(
                validation_type VARCHAR,
                issue_count BIGINT,
                sample_mlb_id VARCHAR
            ) AS $$
            BEGIN
                RETURN QUERY
                SELECT 
                    'low_confidence_mappings'::VARCHAR as validation_type,
                    COUNT(*) as issue_count,
                    MIN(mlb_stats_api_game_id)::VARCHAR as sample_mlb_id
                FROM staging.game_id_mappings
                WHERE resolution_confidence < 0.8;
                
                RETURN QUERY
                SELECT 
                    'unverified_mappings'::VARCHAR as validation_type,
                    COUNT(*) as issue_count,
                    MIN(mlb_stats_api_game_id)::VARCHAR as sample_mlb_id
                FROM staging.game_id_mappings
                WHERE last_verified_at < NOW() - INTERVAL '30 days';
            END;
            $$ LANGUAGE plpgsql;
        """)

        await conn.execute("""
            CREATE OR REPLACE FUNCTION staging.find_unmapped_external_ids(
                source_filter VARCHAR DEFAULT NULL,
                limit_results INTEGER DEFAULT 100
            )
            RETURNS TABLE(
                external_id VARCHAR,
                source_type VARCHAR,
                home_team VARCHAR,
                away_team VARCHAR,
                game_date DATE,
                raw_table VARCHAR
            ) AS $$
            BEGIN
                -- Simplified version for testing
                RETURN QUERY
                SELECT 
                    'TEST_123'::VARCHAR as external_id,
                    'action_network'::VARCHAR as source_type,
                    'LAD'::VARCHAR as home_team,
                    'NYY'::VARCHAR as away_team,
                    '2024-07-01'::DATE as game_date,
                    'raw_data.action_network_games'::VARCHAR as raw_table
                WHERE FALSE; -- Return no rows in test
            END;
            $$ LANGUAGE plpgsql;
        """)


# Integration test that requires the actual database
@pytest.mark.asyncio
async def test_migration_integration():
    """Integration test for the complete migration."""
    # Setup - ensure clean state
    async with get_connection() as conn:
        await conn.execute("DELETE FROM staging.game_id_mappings WHERE mlb_stats_api_game_id LIKE 'TEST_%'")

    try:
        test_instance = TestGameIDMappingsMigration()

        # Run key tests
        await test_instance.test_table_creation()
        await test_instance.test_table_constraints()
        await test_instance.test_indexes_creation()
        await test_instance.test_utility_functions_exist()
        await test_instance.test_get_mapping_stats_function()
        await test_instance.test_data_insertion_and_retrieval()
        print("✅ All migration tests passed!")

    finally:
        # Cleanup
        async with get_connection() as conn:
            await conn.execute("DELETE FROM staging.game_id_mappings WHERE mlb_stats_api_game_id LIKE 'TEST_%'")


if __name__ == "__main__":
    asyncio.run(test_migration_integration())
