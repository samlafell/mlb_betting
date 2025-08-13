"""
Integration test for unified staging implementation.

Tests the complete flow from raw data to unified staging table storage.
"""

import pytest
import asyncio
import asyncpg
from datetime import datetime, timezone
from decimal import Decimal

from src.core.config import get_settings
from src.core.sportsbook_utils import resolve_sportsbook_info_static as resolve_sportsbook_info


class TestUnifiedStagingIntegration:
    """Integration tests for unified staging table."""
    
    @pytest.mark.asyncio
    async def test_insert_unified_staging_record(self):
        """Test inserting a record into the unified staging table."""
        config = get_settings()
        
        conn = await asyncpg.connect(
            host=config.database.host,
            port=config.database.port,
            user=config.database.user,
            password=config.database.password,
            database=config.database.database
        )
        
        try:
            # Test data that satisfies all constraints
            test_data = {
                'data_source': 'action_network',
                'source_collector': 'test_collector',
                'external_game_id': 'test_game_12345',
                'mlb_stats_api_game_id': None,
                'game_date': '2024-08-12',
                'home_team': 'New York Yankees',
                'away_team': 'Boston Red Sox',
                'sportsbook_external_id': '15',
                'sportsbook_id': 15,
                'sportsbook_name': 'FanDuel',
                'market_type': 'moneyline',
                'home_moneyline_odds': -120,
                'away_moneyline_odds': 100,
                'spread_line': None,
                'home_spread_odds': None,
                'away_spread_odds': None,
                'total_line': None,
                'over_odds': None,
                'under_odds': None,
                'raw_data_table': 'raw_data.action_network_odds',
                'raw_data_id': 123,
                'transformation_metadata': '{"processor": "test", "version": "1.0"}',
                'data_quality_score': 0.95,
                'validation_status': 'valid',
                'validation_errors': None,
                'collected_at': datetime.now(timezone.utc),
                'processed_at': datetime.now(timezone.utc)
            }
            
            # Insert test record
            insert_query = """
            INSERT INTO staging.betting_odds_unified (
                data_source, source_collector, external_game_id, mlb_stats_api_game_id,
                game_date, home_team, away_team, sportsbook_external_id, sportsbook_id,
                sportsbook_name, market_type, home_moneyline_odds, away_moneyline_odds,
                spread_line, home_spread_odds, away_spread_odds, total_line, over_odds,
                under_odds, raw_data_table, raw_data_id, transformation_metadata,
                data_quality_score, validation_status, validation_errors, collected_at,
                processed_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27
            ) RETURNING id
            """
            
            record_id = await conn.fetchval(insert_query, *test_data.values())
            assert record_id is not None
            
            # Verify record was inserted correctly
            select_query = """
            SELECT * FROM staging.betting_odds_unified WHERE id = $1
            """
            
            record = await conn.fetchrow(select_query, record_id)
            assert record is not None
            assert record['data_source'] == 'action_network'
            assert record['sportsbook_name'] == 'FanDuel'
            assert record['home_team'] == 'New York Yankees'
            assert record['away_team'] == 'Boston Red Sox'
            assert record['home_moneyline_odds'] == -120
            assert record['away_moneyline_odds'] == 100
            
            print(f"✅ Successfully inserted and verified unified staging record {record_id}")
            
        finally:
            await conn.close()
    
    @pytest.mark.asyncio
    async def test_constraint_validation(self):
        """Test that database constraints work as expected."""
        config = get_settings()
        
        conn = await asyncpg.connect(
            host=config.database.host,
            port=config.database.port,
            user=config.database.user,
            password=config.database.password,
            database=config.database.database
        )
        
        try:
            # Test 1: Missing required field (should fail)
            with pytest.raises(Exception):  # Should violate NOT NULL constraint
                await conn.execute("""
                    INSERT INTO staging.betting_odds_unified (
                        data_source, external_game_id, home_team, away_team, 
                        sportsbook_external_id, sportsbook_name, market_type
                    ) VALUES (
                        'action_network', 'test_game', 'Yankees', 'Red Sox',
                        '15', '', 'moneyline'  -- Empty sportsbook_name should fail
                    )
                """)
            
            # Test 2: Invalid data_source (should fail)
            with pytest.raises(Exception):  # Should violate CHECK constraint
                await conn.execute("""
                    INSERT INTO staging.betting_odds_unified (
                        data_source, external_game_id, home_team, away_team, 
                        sportsbook_external_id, sportsbook_name, market_type
                    ) VALUES (
                        'invalid_source', 'test_game', 'Yankees', 'Red Sox',
                        '15', 'FanDuel', 'moneyline'
                    )
                """)
            
            # Test 3: Empty team names (should fail)
            with pytest.raises(Exception):  # Should violate CHECK constraint
                await conn.execute("""
                    INSERT INTO staging.betting_odds_unified (
                        data_source, external_game_id, home_team, away_team, 
                        sportsbook_external_id, sportsbook_name, market_type
                    ) VALUES (
                        'action_network', 'test_game', '', 'Red Sox',
                        '15', 'FanDuel', 'moneyline'
                    )
                """)
            
            print("✅ All constraint validation tests passed")
            
        finally:
            await conn.close()
    
    @pytest.mark.asyncio
    async def test_sportsbook_resolution_database_integration(self):
        """Test sportsbook resolution integrated with database storage."""
        config = get_settings()
        
        conn = await asyncpg.connect(
            host=config.database.host,
            port=config.database.port,
            user=config.database.user,
            password=config.database.password,
            database=config.database.database
        )
        
        try:
            # Test all known sportsbook IDs
            test_sportsbooks = ['15', '30', '68', '69', '71', '75', '79', '123', '972']
            
            for i, sportsbook_id in enumerate(test_sportsbooks):
                # Resolve sportsbook info
                sportsbook_info = resolve_sportsbook_info(sportsbook_id)
                
                # Insert record with resolved sportsbook info
                test_data = {
                    'data_source': 'action_network',
                    'source_collector': 'test_collector',
                    'external_game_id': f'test_game_{i}',
                    'game_date': '2024-08-12',
                    'home_team': 'Test Home',
                    'away_team': 'Test Away',
                    'sportsbook_external_id': sportsbook_id,
                    'sportsbook_id': sportsbook_info['id'],
                    'sportsbook_name': sportsbook_info['name'],
                    'market_type': 'moneyline',
                    'home_moneyline_odds': -110,
                    'raw_data_table': 'raw_data.test',
                    'raw_data_id': i + 1,
                    'collected_at': datetime.now(timezone.utc)
                }
                
                insert_query = """
                INSERT INTO staging.betting_odds_unified (
                    data_source, source_collector, external_game_id, game_date,
                    home_team, away_team, sportsbook_external_id, sportsbook_id,
                    sportsbook_name, market_type, home_moneyline_odds, 
                    raw_data_table, raw_data_id, collected_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
                ) RETURNING id
                """
                
                record_id = await conn.fetchval(insert_query, *test_data.values())
                assert record_id is not None
                
                print(f"✅ Inserted {sportsbook_info['name']} (ID: {sportsbook_id}) record {record_id}")
            
            # Verify all records were inserted with correct sportsbook names
            count_query = """
            SELECT COUNT(*) FROM staging.betting_odds_unified 
            WHERE sportsbook_name IS NOT NULL AND sportsbook_name != ''
            """
            
            count = await conn.fetchval(count_query)
            assert count == len(test_sportsbooks)
            
            print(f"✅ Successfully inserted {count} records with resolved sportsbook names")
            
        finally:
            await conn.close()
    
    @pytest.mark.asyncio
    async def test_backwards_compatibility_views(self):
        """Test that backwards compatibility views work correctly."""
        config = get_settings()
        
        conn = await asyncpg.connect(
            host=config.database.host,
            port=config.database.port,
            user=config.database.user,
            password=config.database.password,
            database=config.database.database
        )
        
        try:
            # Insert test records for different market types
            test_records = [
                {
                    'market_type': 'moneyline',
                    'home_moneyline_odds': -120,
                    'away_moneyline_odds': 100,
                    'spread_line': None,
                    'total_line': None
                },
                {
                    'market_type': 'spread',
                    'home_moneyline_odds': None,
                    'away_moneyline_odds': None,
                    'spread_line': Decimal('-1.5'),
                    'home_spread_odds': -110,
                    'away_spread_odds': -110,
                    'total_line': None
                },
                {
                    'market_type': 'total',
                    'home_moneyline_odds': None,
                    'away_moneyline_odds': None,
                    'spread_line': None,
                    'total_line': Decimal('8.5'),
                    'over_odds': -105,
                    'under_odds': -115
                }
            ]
            
            inserted_ids = []
            for i, record_data in enumerate(test_records):
                base_data = {
                    'data_source': 'action_network',
                    'external_game_id': f'compat_test_game_{i}',
                    'home_team': 'Test Home',
                    'away_team': 'Test Away',
                    'sportsbook_external_id': '15',
                    'sportsbook_name': 'FanDuel',
                    'raw_data_table': 'raw_data.test',
                    'raw_data_id': i + 100,
                    'collected_at': datetime.now(timezone.utc)
                }
                
                combined_data = {**base_data, **record_data}
                
                insert_query = """
                INSERT INTO staging.betting_odds_unified (
                    data_source, external_game_id, home_team, away_team,
                    sportsbook_external_id, sportsbook_name, market_type,
                    home_moneyline_odds, away_moneyline_odds, spread_line,
                    home_spread_odds, away_spread_odds, total_line,
                    over_odds, under_odds, raw_data_table, raw_data_id, collected_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
                ) RETURNING id
                """
                
                record_id = await conn.fetchval(insert_query, *combined_data.values())
                inserted_ids.append(record_id)
            
            # Test moneylines_compatible view
            moneylines = await conn.fetch("SELECT * FROM staging.moneylines_compatible")
            assert len(moneylines) >= 1
            assert any(r['home_odds'] == -120 for r in moneylines)
            
            # Test spreads_compatible view
            spreads = await conn.fetch("SELECT * FROM staging.spreads_compatible")
            assert len(spreads) >= 1
            assert any(r['line_value'] == Decimal('-1.5') for r in spreads)
            
            # Test totals_compatible view
            totals = await conn.fetch("SELECT * FROM staging.totals_compatible")
            assert len(totals) >= 1
            assert any(r['line_value'] == Decimal('8.5') for r in totals)
            
            print("✅ All backwards compatibility views working correctly")
            
        finally:
            await conn.close()


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])