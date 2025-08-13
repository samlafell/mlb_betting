"""
Test suite for unified staging implementation.

Tests all aspects of the DATA_MODEL_IMPROVEMENTS.md implementation including:
1. Source attribution tracking
2. Sportsbook resolution
3. Team name population
4. Data consolidation
5. Data lineage tracking
6. Quality metrics
7. Database integration
"""

import pytest
import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, patch, MagicMock

from src.core.sportsbook_utils import resolve_sportsbook_info_static as resolve_sportsbook_info, SportsbookResolutionError
from src.core.team_utils import populate_team_names, TeamResolutionError, TeamInfo
from src.data.pipeline.unified_staging_processor import UnifiedStagingProcessor, UnifiedStagingRecord
from src.data.pipeline.zone_interface import ZoneConfig, ZoneType, DataRecord


class TestSportsbookResolution:
    """Test sportsbook resolution functionality (Issue #2)."""
    
    def test_resolve_known_sportsbook_ids(self):
        """Test resolution of known sportsbook IDs."""
        # Test Action Network sportsbook IDs from database analysis
        test_cases = [
            ('15', 'FanDuel'),
            ('30', 'DraftKings'),
            ('68', 'BetMGM'),
            ('69', 'Caesars'),
            ('71', 'Bet365'),
            ('75', 'Pinnacle'),
            ('79', 'Circa Sports'),
            ('123', 'PointsBet'),
            ('972', 'Fanatics')
        ]
        
        for sportsbook_id, expected_name in test_cases:
            info = resolve_sportsbook_info(sportsbook_id)
            assert info['name'] == expected_name
            assert info['id'] == int(sportsbook_id)
            assert info['external_id'] == sportsbook_id
            assert info['is_active'] is True
    
    def test_resolve_string_sportsbook_names(self):
        """Test resolution of string sportsbook names."""
        test_cases = [
            ('draftkings', 'DraftKings'),
            ('fanduel', 'FanDuel'),
            ('betmgm', 'BetMGM'),
            ('pinnacle', 'Pinnacle')
        ]
        
        for name_input, expected_name in test_cases:
            info = resolve_sportsbook_info(name_input)
            assert info['name'] == expected_name
    
    def test_unknown_sportsbook_raises_error(self):
        """Test that unknown sportsbook IDs raise appropriate errors."""
        with pytest.raises(SportsbookResolutionError):
            resolve_sportsbook_info('99999')
        
        with pytest.raises(SportsbookResolutionError):
            resolve_sportsbook_info('unknown_book')
    
    def test_empty_sportsbook_id_raises_error(self):
        """Test that empty sportsbook ID raises appropriate error."""
        with pytest.raises(SportsbookResolutionError):
            resolve_sportsbook_info('')
        
        with pytest.raises(SportsbookResolutionError):
            resolve_sportsbook_info(None)


class TestTeamResolution:
    """Test team name resolution functionality (Issue #3)."""
    
    @pytest.mark.asyncio
    async def test_extract_from_raw_data_direct_format(self):
        """Test team extraction from direct home_team/away_team format."""
        raw_data = {
            'home_team': 'New York Yankees',
            'away_team': 'Boston Red Sox'
        }
        
        team_info = await populate_team_names(
            external_game_id='test_game_123',
            raw_data=raw_data
        )
        
        assert team_info.home_team == 'NYY'  # Normalized to abbreviation
        assert team_info.away_team == 'BOS'  # Normalized to abbreviation
        assert team_info.source == 'raw_data_direct'
    
    @pytest.mark.asyncio
    async def test_extract_from_raw_data_game_object(self):
        """Test team extraction from game object format."""
        raw_data = {
            'game': {
                'home_team': 'Houston Astros',
                'away_team': 'Oakland Athletics'
            }
        }
        
        team_info = await populate_team_names(
            external_game_id='test_game_456',
            raw_data=raw_data
        )
        
        assert team_info.home_team == 'HOU'  # Normalized to abbreviation
        assert team_info.away_team == 'OAK'  # Normalized to abbreviation
        assert team_info.source == 'raw_data_game_object'
    
    @pytest.mark.asyncio
    async def test_team_resolution_failure_handling(self):
        """Test graceful handling of team resolution failures."""
        with pytest.raises(TeamResolutionError):
            await populate_team_names(
                external_game_id='unknown_game',
                raw_data=None  # No data provided
            )


class TestUnifiedStagingProcessor:
    """Test the unified staging processor implementation."""
    
    @pytest.fixture
    def processor(self):
        """Create a unified staging processor for testing."""
        config = ZoneConfig(
            zone_type=ZoneType.STAGING,
            schema_name='staging'
        )
        return UnifiedStagingProcessor(config)
    
    @pytest.fixture
    def sample_raw_record(self):
        """Create a sample raw record for testing."""
        return DataRecord(
            id=1,
            external_id='test_record_123',
            raw_data={
                'home_team': 'Yankees',
                'away_team': 'Red Sox',
                'moneyline': [
                    {'book_id': '15', 'side': 'home', 'odds': -120},
                    {'book_id': '15', 'side': 'away', 'odds': 100}
                ],
                'spread': [
                    {'book_id': '15', 'side': 'home', 'odds': -110, 'value': -1.5},
                    {'book_id': '15', 'side': 'away', 'odds': -110, 'value': 1.5}
                ]
            },
            collected_at=datetime.now(timezone.utc),
            processed_at=None
        )
    
    @pytest.mark.asyncio
    async def test_source_attribution_population(self, processor, sample_raw_record):
        """Test source attribution tracking (Issue #1)."""
        with patch.object(processor, '_resolve_sportsbook_info'), \
             patch.object(processor, '_populate_team_names'), \
             patch.object(processor, '_extract_unified_betting_data'), \
             patch.object(processor, '_add_data_lineage'), \
             patch.object(processor, '_calculate_quality_metrics'), \
             patch.object(processor, '_validate_unified_record'):
            
            result = await processor.process_record(
                sample_raw_record,
                data_source='action_network',
                source_collector='test_collector'
            )
            
            assert result.data_source == 'action_network'
            assert result.source_collector == 'test_collector'
    
    @pytest.mark.asyncio
    async def test_sportsbook_resolution_integration(self, processor, sample_raw_record):
        """Test sportsbook resolution in unified processor (Issue #2)."""
        # Set sportsbook_external_id in sample record
        sample_raw_record.sportsbook_external_id = '15'
        
        with patch.object(processor, '_populate_source_attribution'), \
             patch.object(processor, '_populate_team_names'), \
             patch.object(processor, '_extract_unified_betting_data'), \
             patch.object(processor, '_add_data_lineage'), \
             patch.object(processor, '_calculate_quality_metrics'), \
             patch.object(processor, '_validate_unified_record'):
            
            result = await processor.process_record(sample_raw_record)
            
            assert result.sportsbook_external_id == '15'
            assert result.sportsbook_name == 'FanDuel'
            assert result.sportsbook_id == 15
    
    @pytest.mark.asyncio
    async def test_team_name_population_integration(self, processor, sample_raw_record):
        """Test team name population in unified processor (Issue #3)."""
        with patch.object(processor, '_populate_source_attribution'), \
             patch.object(processor, '_resolve_sportsbook_info'), \
             patch.object(processor, '_extract_unified_betting_data'), \
             patch.object(processor, '_add_data_lineage'), \
             patch.object(processor, '_calculate_quality_metrics'), \
             patch.object(processor, '_validate_unified_record'):
            
            result = await processor.process_record(sample_raw_record)
            
            assert result.home_team is not None
            assert result.away_team is not None
            assert result.home_team != result.away_team
    
    @pytest.mark.asyncio
    async def test_betting_data_consolidation(self, processor, sample_raw_record):
        """Test betting data consolidation (Issue #4 & #6)."""
        with patch.object(processor, '_populate_source_attribution'), \
             patch.object(processor, '_resolve_sportsbook_info'), \
             patch.object(processor, '_populate_team_names'), \
             patch.object(processor, '_add_data_lineage'), \
             patch.object(processor, '_calculate_quality_metrics'), \
             patch.object(processor, '_validate_unified_record'):
            
            result = await processor.process_record(sample_raw_record)
            
            # Should consolidate moneyline and spread data into single record
            assert result.home_moneyline_odds == -120
            assert result.away_moneyline_odds == 100
            assert result.spread_line == Decimal('-1.5')  # Home team spread
            assert result.home_spread_odds == -110
            assert result.away_spread_odds == -110
    
    @pytest.mark.asyncio
    async def test_data_lineage_tracking(self, processor, sample_raw_record):
        """Test data lineage tracking (Issue #5)."""
        with patch.object(processor, '_populate_source_attribution'), \
             patch.object(processor, '_resolve_sportsbook_info'), \
             patch.object(processor, '_populate_team_names'), \
             patch.object(processor, '_extract_unified_betting_data'), \
             patch.object(processor, '_calculate_quality_metrics'), \
             patch.object(processor, '_validate_unified_record'):
            
            result = await processor.process_record(sample_raw_record)
            
            assert result.raw_data_table == 'raw_data.action_network_odds'
            assert result.raw_data_id == sample_raw_record.id
            assert result.transformation_metadata is not None
            assert 'processor' in result.transformation_metadata
            assert 'transformation_time' in result.transformation_metadata
    
    @pytest.mark.asyncio
    async def test_record_consolidation_reduces_duplicates(self, processor):
        """Test that record consolidation reduces duplicates (Issue #4)."""
        # Create multiple records for same game/sportsbook
        records = []
        for i, (market, side, odds) in enumerate([
            ('moneyline', 'home', -120),
            ('moneyline', 'away', 100),
            ('spread', 'home', -110),
            ('spread', 'away', -110)
        ]):
            record = UnifiedStagingRecord(
                id=i,
                external_game_id='game_123',
                sportsbook_external_id='15',
                sportsbook_name='FanDuel',
                home_team='Yankees',
                away_team='Red Sox',
                market_type=market
            )
            
            if market == 'moneyline':
                if side == 'home':
                    record.home_moneyline_odds = odds
                else:
                    record.away_moneyline_odds = odds
            elif market == 'spread':
                record.spread_line = Decimal('-1.5')
                if side == 'home':
                    record.home_spread_odds = odds
                else:
                    record.away_spread_odds = odds
            
            records.append(record)
        
        consolidated = await processor._consolidate_bet_records(records)
        
        # Should consolidate 4 records into 1
        assert len(consolidated) == 1
        
        unified_record = consolidated[0]
        assert unified_record.home_moneyline_odds == -120
        assert unified_record.away_moneyline_odds == 100
        assert unified_record.spread_line == Decimal('-1.5')
        assert unified_record.home_spread_odds == -110
        assert unified_record.away_spread_odds == -110


class TestQualityMetrics:
    """Test quality metrics calculation."""
    
    @pytest.fixture
    def processor(self):
        config = ZoneConfig(zone_type=ZoneType.STAGING, schema_name='staging')
        return UnifiedStagingProcessor(config)
    
    @pytest.mark.asyncio
    async def test_completeness_score_calculation(self, processor):
        """Test completeness score calculation."""
        # Complete record
        complete_record = UnifiedStagingRecord(
            external_game_id='game_123',
            sportsbook_name='FanDuel',
            home_team='Yankees',
            away_team='Red Sox',
            data_source='action_network',
            market_type='moneyline'
        )
        
        score = await processor._calculate_completeness_score(complete_record)
        assert score == 1.0
        
        # Incomplete record
        incomplete_record = UnifiedStagingRecord(
            external_game_id='game_123',
            # Missing other required fields
        )
        
        score = await processor._calculate_completeness_score(incomplete_record)
        assert score < 1.0
    
    @pytest.mark.asyncio
    async def test_accuracy_score_calculation(self, processor):
        """Test accuracy score calculation."""
        # Accurate record
        accurate_record = UnifiedStagingRecord(
            sportsbook_name='FanDuel',
            home_team='Yankees',
            away_team='Red Sox',
            home_moneyline_odds=-120,
            away_moneyline_odds=100
        )
        
        score = await processor._calculate_accuracy_score(accurate_record)
        assert score == 1.0
        
        # Inaccurate record with unreasonable odds
        inaccurate_record = UnifiedStagingRecord(
            sportsbook_name='Unknown_15',
            home_team='Unknown_Home',
            away_team='Unknown_Away',
            home_moneyline_odds=-10000  # Unreasonable odds
        )
        
        score = await processor._calculate_accuracy_score(inaccurate_record)
        assert score < 1.0


class TestDatabaseIntegration:
    """Test database integration and constraints."""
    
    @pytest.mark.asyncio
    async def test_unified_table_constraints(self):
        """Test that unified table constraints work as expected."""
        # This would require actual database connection
        # For now, we'll test the constraint logic in the processor
        
        processor_config = ZoneConfig(zone_type=ZoneType.STAGING, schema_name='staging')
        processor = UnifiedStagingProcessor(processor_config)
        
        # Test validation logic
        valid_record = UnifiedStagingRecord(
            external_game_id='game_123',
            sportsbook_name='FanDuel',
            home_team='Yankees',
            away_team='Red Sox',
            data_source='action_network',
            market_type='moneyline'
        )
        
        await processor._validate_unified_record(valid_record)
        assert valid_record.validation_status == 'valid'
        assert valid_record.validation_errors is None
        
        # Test invalid record
        invalid_record = UnifiedStagingRecord(
            # Missing required fields
        )
        
        await processor._validate_unified_record(invalid_record)
        assert invalid_record.validation_status == 'invalid'
        assert invalid_record.validation_errors is not None
        assert len(invalid_record.validation_errors) > 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])