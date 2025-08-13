"""
Unified Staging Zone Processor

Enhanced staging processor that implements the unified data model improvements.
This addresses all 7 critical issues identified in DATA_MODEL_IMPROVEMENTS.md:

1. Missing Source Attribution - Tracks data_source and source_collector
2. Missing Sportsbook Names - Resolves sportsbook_external_id to names
3. Missing Team Information - Populates home/away team names
4. Excessive Data Duplication - Consolidates bet sides into unified records
5. Poor Data Lineage - Tracks raw_data_table and raw_data_id
6. Fragmented Bet Type Design - Uses single unified table
7. Design Pattern Inconsistency - Replaces fragmented staging approach

Reference: docs/DATA_MODEL_IMPROVEMENTS.md
"""

import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

from ...core.logging import LogComponent, get_logger
from ...core.sportsbook_utils import resolve_sportsbook_info_static as resolve_sportsbook_info, SportsbookResolutionError
from ...core.team_utils import populate_team_names, TeamResolutionError, validate_team_names, normalize_team_name
from .team_mappings import get_team_mapping
from .base_processor import BaseZoneProcessor
from .zone_interface import (
    DataRecord,
    ProcessingResult,
    ProcessingStatus,
    ZoneConfig,
    ZoneFactory,
    ZoneType,
)

logger = get_logger(__name__, LogComponent.CORE)


class UnifiedStagingRecord(DataRecord):
    """Unified staging data record with complete attribution and consolidated bet data."""
    
    # Source attribution (FIXES ISSUE #1)
    data_source: str | None = None
    source_collector: str | None = None
    
    # Game identification
    external_game_id: str | None = None
    mlb_stats_api_game_id: str | None = None
    game_date: str | None = None  # Will be converted to DATE in database
    home_team: str | None = None  # FIXES ISSUE #3
    away_team: str | None = None  # FIXES ISSUE #3
    
    # Sportsbook identification (FIXES ISSUE #2)
    sportsbook_external_id: str | None = None
    sportsbook_id: int | None = None
    sportsbook_name: str | None = None  # REQUIRED, RESOLVED FROM MAPPING
    
    # Unified bet data (FIXES ISSUE #6)
    market_type: str | None = None  # 'moneyline', 'spread', 'total'
    
    # Consolidated odds data
    home_moneyline_odds: int | None = None
    away_moneyline_odds: int | None = None
    spread_line: Decimal | None = None
    home_spread_odds: int | None = None
    away_spread_odds: int | None = None
    total_line: Decimal | None = None
    over_odds: int | None = None
    under_odds: int | None = None
    
    # Data lineage (FIXES ISSUE #5)
    raw_data_table: str | None = None
    raw_data_id: int | None = None
    transformation_metadata: Dict[str, Any] | None = None
    
    # Quality and timing
    data_quality_score: float | None = None
    validation_status: str | None = None
    validation_errors: List[str] | None = None
    collected_at: datetime | None = None


class UnifiedStagingProcessor(BaseZoneProcessor):
    """
    Unified staging processor that implements the improved data model.
    
    Key improvements:
    - Source attribution tracking
    - Comprehensive sportsbook resolution
    - Team name population from multiple sources
    - Intelligent bet consolidation (reduces 50-75% of records)
    - Complete data lineage tracking
    - Single unified table design
    """
    
    def __init__(self, config: ZoneConfig):
        super().__init__(config)
        self.consolidation_cache = {}
        self.team_resolution_cache = {}
        
    async def process_record(self, record: DataRecord, **kwargs) -> DataRecord | None:
        """
        Process a single RAW record into a unified staging record.
        
        This method consolidates multiple bet sides into a single record,
        addressing the excessive duplication issue.
        """
        try:
            # Create unified staging record with field mapping
            record_data = record.model_dump()
            
            # Map external_id to external_game_id for compatibility
            if not record_data.get('external_game_id') and record_data.get('external_id'):
                record_data['external_game_id'] = record_data['external_id']
            
            unified_record = UnifiedStagingRecord(**record_data)
            
            # Step 1: Populate source attribution (FIXES ISSUE #1)
            await self._populate_source_attribution(unified_record, **kwargs)
            
            # Step 2: Resolve sportsbook information (FIXES ISSUE #2)
            await self._resolve_sportsbook_info(unified_record)
            
            # Step 3: Populate team names (FIXES ISSUE #3)
            await self._populate_team_names(unified_record)
            
            # Step 4: Extract and consolidate betting data (FIXES ISSUE #4 & #6)
            await self._extract_unified_betting_data(unified_record)
            
            # Step 5: Add data lineage (FIXES ISSUE #5)
            await self._add_data_lineage(unified_record, record)
            
            # Step 6: Calculate quality metrics
            await self._calculate_quality_metrics(unified_record)
            
            # Step 7: Final validation
            await self._validate_unified_record(unified_record)
            
            logger.debug(f"Processed unified staging record: {unified_record.external_id}")
            return unified_record
            
        except Exception as e:
            logger.error(f"Error processing unified staging record {getattr(record, 'external_id', 'unknown')}: {e}")
            return None
    
    async def process_batch_with_consolidation(self, records: List[DataRecord], **kwargs) -> List[DataRecord]:
        """
        Process a batch of records with intelligent consolidation.
        
        This method addresses the excessive duplication issue by consolidating
        multiple bet side records into unified records.
        """
        try:
            # Process individual records first
            processed_records = []
            for record in records:
                processed = await self.process_record(record, **kwargs)
                if processed:
                    processed_records.append(processed)
            
            # Consolidate records by game/sportsbook/market
            consolidated_records = await self._consolidate_bet_records(processed_records)
            
            logger.info(f"Consolidated {len(processed_records)} records into {len(consolidated_records)} unified records")
            return consolidated_records
            
        except Exception as e:
            logger.error(f"Error in batch consolidation: {e}")
            return []
    
    async def process_record_multi_bet_types(self, record: DataRecord, **kwargs) -> List[DataRecord]:
        """
        Process a single RAW record into multiple unified staging records.
        
        This method is compatible with the pipeline orchestrator's enhanced processing
        for STAGING zone that expects multiple records output.
        """
        try:
            # Process the record using unified approach
            unified_record = await self.process_record(record, **kwargs)
            
            if unified_record:
                # Return as list for compatibility with pipeline orchestrator
                return [unified_record]
            else:
                return []
                
        except Exception as e:
            logger.error(f"Error in multi-bet type processing for record {getattr(record, 'external_game_id', getattr(record, 'external_id', 'unknown'))}: {e}")
            return []
    
    async def _populate_source_attribution(self, record: UnifiedStagingRecord, **kwargs):
        """Populate source attribution information (FIXES ISSUE #1)."""
        try:
            # Extract from kwargs or infer from context - NO HARDCODED DEFAULTS
            source_data_source = kwargs.get('data_source')
            source_collector = kwargs.get('source_collector')
            
            # Try to infer from record metadata first
            if hasattr(record, 'raw_data') and isinstance(record.raw_data, dict):
                metadata = record.raw_data.get('_metadata', {})
                if metadata:
                    source_data_source = source_data_source or metadata.get('source')
                    source_collector = source_collector or metadata.get('collector')
            
            # Only set if we have valid values - avoid hardcoded assumptions
            if source_data_source:
                record.data_source = source_data_source
            if source_collector:
                record.source_collector = source_collector
            
            logger.debug(f"Set source attribution: {record.data_source} / {record.source_collector}")
            
        except Exception as e:
            logger.warning(f"Failed to populate source attribution: {e}")
            # Set defaults to ensure non-null values
            record.data_source = record.data_source or 'unknown'
            record.source_collector = record.source_collector or 'unknown'
    
    async def _resolve_sportsbook_info(self, record: UnifiedStagingRecord):
        """Resolve sportsbook information (FIXES ISSUE #2)."""
        try:
            # Get sportsbook external ID from record - handle various field names
            sportsbook_external_id = (
                getattr(record, 'sportsbook_external_id', None) or
                getattr(record, 'sportsbook_key', None) or
                getattr(record, 'sportsbook_id', None)
            )
            
            # Extract from raw data if still not found
            if not sportsbook_external_id and hasattr(record, 'raw_data') and isinstance(record.raw_data, dict):
                # Look for sportsbook ID in various formats
                for key in ['_sportsbook_key', 'book_id', 'sportsbook_id', 'sportsbook_key', 'bookmaker_id']:
                    if key in record.raw_data:
                        sportsbook_external_id = str(record.raw_data[key])
                        break
            
            if not sportsbook_external_id:
                raise SportsbookResolutionError("No sportsbook external ID found in record")
            
            # Resolve using enhanced mapping
            sportsbook_info = resolve_sportsbook_info(sportsbook_external_id)
            
            # Populate record with resolved information
            record.sportsbook_external_id = sportsbook_external_id
            record.sportsbook_id = sportsbook_info['id']
            record.sportsbook_name = sportsbook_info['name']
            
            logger.debug(f"Resolved sportsbook {sportsbook_external_id} to {record.sportsbook_name}")
            
        except SportsbookResolutionError as e:
            logger.error(f"Sportsbook resolution failed: {e}")
            # Set to unknown to prevent null constraint violations
            record.sportsbook_name = f"Unknown_{record.sportsbook_external_id}"
        except Exception as e:
            logger.error(f"Unexpected error in sportsbook resolution: {e}")
            record.sportsbook_name = "Unknown"
    
    async def _populate_team_names(self, record: UnifiedStagingRecord):
        """Populate team names and game metadata (FIXES ISSUE #3)."""
        try:
            # Get external_game_id from available fields (fallback to external_id)
            external_game_id = getattr(record, 'external_game_id', None) or getattr(record, 'external_id', None) or ''
            
            if not external_game_id:
                raise TeamResolutionError("No external game ID available for team resolution")
            
            # Strategy 1: Query games table for team names and metadata (source-specific)
            source = getattr(record, 'data_source', 'action_network')
            team_info = await self._query_game_data_by_source(external_game_id, source)
            if team_info and team_info['home_team'] and team_info['away_team']:
                record.home_team = team_info['home_team']
                record.away_team = team_info['away_team']
                record.game_date = team_info['game_date']
                
                # Try to get MLB Stats API game ID
                mlb_game_id = await self._lookup_mlb_stats_api_game_id(external_game_id, team_info)
                if mlb_game_id:
                    record.mlb_stats_api_game_id = mlb_game_id
                
                # Validate resolved team names
                if validate_team_names(record.home_team, record.away_team):
                    logger.debug(f"✅ Team resolution successful (database): {record.home_team} vs {record.away_team}")
                    return
                else:
                    logger.warning(f"Database team names failed validation: {record.home_team} vs {record.away_team}")
            
            # Strategy 2: Use team ID mapping from raw odds data
            team_ids = await self._extract_team_ids_from_raw_data(record)
            if team_ids:
                source = getattr(record, 'data_source', 'action_network')
                team_names = await self._resolve_team_ids_to_names(team_ids, source)
                if team_names and team_names['home_team'] and team_names['away_team']:
                    record.home_team = normalize_team_name(team_names['home_team'])
                    record.away_team = normalize_team_name(team_names['away_team'])
                    if validate_team_names(record.home_team, record.away_team):
                        logger.debug(f"✅ Team resolution successful (team IDs): {record.home_team} vs {record.away_team}")
                        return
            
            # Strategy 3: Use existing team resolution service from raw data
            try:
                team_info_obj = await populate_team_names(
                    external_game_id=external_game_id,
                    raw_data=getattr(record, 'raw_data', None),
                    mlb_stats_api_game_id=getattr(record, 'mlb_stats_api_game_id', None)
                )
                
                record.home_team = team_info_obj.home_team
                record.away_team = team_info_obj.away_team
                
                if validate_team_names(record.home_team, record.away_team):
                    logger.debug(f"✅ Team resolution successful (raw data): {record.home_team} vs {record.away_team}")
                    return
                else:
                    logger.warning(f"Raw data team names failed validation: {record.home_team} vs {record.away_team}")
            except TeamResolutionError:
                logger.debug("Raw data team resolution strategy failed")
            
            # All strategies failed - use informative fallback
            logger.warning(f"⚠️ All team resolution strategies failed for game {external_game_id}")
            
            # Try one final strategy: check if we can extract team abbreviations from anywhere
            try:
                # Look for any team abbreviation patterns in raw data
                if hasattr(record, 'raw_data') and isinstance(record.raw_data, dict):
                    raw_data = record.raw_data
                    
                    # Look for team abbreviations in the raw data structure
                    potential_teams = []
                    def find_team_patterns(obj, path=""):
                        if isinstance(obj, dict):
                            for key, value in obj.items():
                                if 'team' in key.lower() and isinstance(value, str) and len(value) == 3:
                                    potential_teams.append(value.upper())
                                elif isinstance(value, (dict, list)):
                                    find_team_patterns(value, f"{path}.{key}")
                        elif isinstance(obj, list):
                            for i, item in enumerate(obj):
                                find_team_patterns(item, f"{path}[{i}]")
                    
                    find_team_patterns(raw_data)
                    
                    # If we found exactly 2 team patterns, use them
                    if len(set(potential_teams)) == 2:
                        teams = list(set(potential_teams))
                        record.home_team = teams[0]
                        record.away_team = teams[1]
                        logger.info(f"✅ Extracted teams from raw data patterns: {teams[0]} vs {teams[1]}")
                        return
                
                # Final fallback: create meaningful placeholders
                record.home_team = f"UNKNOWN_HOME_{external_game_id[-6:]}"  # Use last 6 digits for brevity
                record.away_team = f"UNKNOWN_AWAY_{external_game_id[-6:]}"
                logger.warning(f"⚠️ Using fallback team names for game {external_game_id}")
                
            except Exception as fallback_error:
                logger.error(f"❌ Even fallback team resolution failed for {external_game_id}: {fallback_error}")
                record.home_team = f"ERROR_HOME"
                record.away_team = f"ERROR_AWAY"
            
        except TeamResolutionError as e:
            logger.warning(f"⚠️ Team resolution failed for game {external_game_id}: {e}")
            # Use informative placeholder values that include game ID for debugging
            record.home_team = f"FAIL_HOME_{external_game_id[-6:]}"
            record.away_team = f"FAIL_AWAY_{external_game_id[-6:]}"
        except Exception as e:
            logger.error(f"❌ Unexpected error in team resolution for game {external_game_id}: {e}")
            record.home_team = f"ERR_HOME_{external_game_id[-6:]}"
            record.away_team = f"ERR_AWAY_{external_game_id[-6:]}"
    
    async def _extract_unified_betting_data(self, record: UnifiedStagingRecord):
        """Extract and consolidate betting data (FIXES ISSUE #4 & #6)."""
        try:
            if not hasattr(record, 'raw_data') or not isinstance(record.raw_data, dict):
                return
            
            raw_data = record.raw_data
            
            # Extract all bet types from Action Network format
            betting_data = {
                'moneyline': [],
                'spread': [],
                'total': []
            }
            
            # Primary format: bet types at root level
            for bet_type in ['moneyline', 'spread', 'total']:
                if bet_type in raw_data and isinstance(raw_data[bet_type], list):
                    betting_data[bet_type].extend(raw_data[bet_type])
            
            # Legacy format: event_markets
            if 'event_markets' in raw_data:
                event_markets = raw_data['event_markets']
                for bet_type in ['moneyline', 'spread', 'total']:
                    if bet_type in event_markets and isinstance(event_markets[bet_type], list):
                        betting_data[bet_type].extend(event_markets[bet_type])
            
            # Consolidate betting data for each market type
            await self._consolidate_moneyline_data(record, betting_data['moneyline'])
            await self._consolidate_spread_data(record, betting_data['spread'])
            await self._consolidate_total_data(record, betting_data['total'])
            
            # Determine primary market type based on available data
            if record.home_moneyline_odds or record.away_moneyline_odds:
                record.market_type = 'moneyline'
            elif record.spread_line and (record.home_spread_odds or record.away_spread_odds):
                record.market_type = 'spread'
            elif record.total_line and (record.over_odds or record.under_odds):
                record.market_type = 'total'
            else:
                record.market_type = 'unknown'
            
            logger.debug(f"Extracted unified betting data for market_type: {record.market_type}")
            
        except Exception as e:
            logger.error(f"Error extracting unified betting data: {e}")
    
    async def _consolidate_moneyline_data(self, record: UnifiedStagingRecord, moneyline_data: List[Dict]):
        """Consolidate moneyline odds from multiple entries."""
        for entry in moneyline_data:
            if not isinstance(entry, dict):
                continue
                
            side = entry.get('side', '').lower()
            odds = self._safe_int_convert(entry.get('odds'))
            
            if side == 'home' and odds:
                record.home_moneyline_odds = odds
            elif side == 'away' and odds:
                record.away_moneyline_odds = odds
    
    async def _consolidate_spread_data(self, record: UnifiedStagingRecord, spread_data: List[Dict]):
        """Consolidate spread data from multiple entries."""
        for entry in spread_data:
            if not isinstance(entry, dict):
                continue
                
            side = entry.get('side', '').lower()
            odds = self._safe_int_convert(entry.get('odds'))
            line_value = self._safe_decimal_convert(entry.get('value'))
            
            if line_value is not None:
                record.spread_line = line_value
                
            if side == 'home' and odds:
                record.home_spread_odds = odds
            elif side == 'away' and odds:
                record.away_spread_odds = odds
    
    async def _consolidate_total_data(self, record: UnifiedStagingRecord, total_data: List[Dict]):
        """Consolidate total data from multiple entries."""
        for entry in total_data:
            if not isinstance(entry, dict):
                continue
                
            side = entry.get('side', '').lower()
            odds = self._safe_int_convert(entry.get('odds'))
            line_value = self._safe_decimal_convert(entry.get('value'))
            
            if line_value is not None:
                record.total_line = line_value
                
            if side == 'over' and odds:
                record.over_odds = odds
            elif side == 'under' and odds:
                record.under_odds = odds
    
    async def _add_data_lineage(self, record: UnifiedStagingRecord, source_record: DataRecord):
        """Add data lineage tracking (FIXES ISSUE #5)."""
        try:
            # Determine raw data table based on source - NO HARDCODED TABLE NAMES
            source = getattr(record, 'data_source', getattr(source_record, 'source', 'unknown'))
            table_mapping = {
                'action_network': 'raw_data.action_network_odds',
                'vsin': 'raw_data.vsin_data',
                'sbd': 'raw_data.sbd_betting_splits',
                'mlb_stats_api': 'raw_data.mlb_stats_api_games'
            }
            record.raw_data_table = table_mapping.get(source.lower(), f'raw_data.{source.lower()}_data')
            record.raw_data_id = getattr(source_record, 'id', None)
            
            # Create transformation metadata
            record.transformation_metadata = {
                'processor': 'UnifiedStagingProcessor',
                'processor_version': '1.0',
                'transformation_time': datetime.now(timezone.utc).isoformat(),
                'source_fields': list(source_record.model_dump().keys()) if hasattr(source_record, 'model_dump') else [],
                'consolidation_applied': True,
                'quality_checks_performed': ['sportsbook_resolution', 'team_resolution', 'data_validation']
            }
            
            logger.debug(f"Added data lineage: {record.raw_data_table}#{record.raw_data_id}")
            
        except Exception as e:
            logger.error(f"Error adding data lineage: {e}")
    
    async def _calculate_quality_metrics(self, record: UnifiedStagingRecord):
        """Calculate comprehensive quality metrics."""
        try:
            completeness_score = await self._calculate_completeness_score(record)
            accuracy_score = await self._calculate_accuracy_score(record)
            consistency_score = await self._calculate_consistency_score(record)
            
            # Overall quality score
            record.data_quality_score = (
                completeness_score * 0.4 +
                accuracy_score * 0.3 +
                consistency_score * 0.3
            )
            
            logger.debug(f"Quality score: {record.data_quality_score:.2f}")
            
        except Exception as e:
            logger.error(f"Error calculating quality metrics: {e}")
            record.data_quality_score = 0.5  # Default score
    
    async def _calculate_completeness_score(self, record: UnifiedStagingRecord) -> float:
        """Calculate data completeness score."""
        required_fields = [
            'external_game_id', 'sportsbook_name', 'home_team', 'away_team',
            'data_source', 'market_type'
        ]
        
        completed_fields = sum(1 for field in required_fields if getattr(record, field))
        return completed_fields / len(required_fields)
    
    async def _calculate_accuracy_score(self, record: UnifiedStagingRecord) -> float:
        """Calculate data accuracy score."""
        accuracy_score = 1.0
        
        # Check sportsbook name validity
        if record.sportsbook_name and 'unknown' in record.sportsbook_name.lower():
            accuracy_score -= 0.2
            
        # Check team name validity
        if not validate_team_names(record.home_team or '', record.away_team or ''):
            accuracy_score -= 0.3
            
        # Check odds reasonableness
        all_odds = [
            record.home_moneyline_odds, record.away_moneyline_odds,
            record.home_spread_odds, record.away_spread_odds,
            record.over_odds, record.under_odds
        ]
        
        for odds in all_odds:
            if odds and (odds < -5000 or odds > 5000):
                accuracy_score -= 0.1
                break
        
        return max(0.0, accuracy_score)
    
    async def _calculate_consistency_score(self, record: UnifiedStagingRecord) -> float:
        """Calculate data consistency score."""
        consistency_score = 1.0
        
        # Check market type consistency
        if record.market_type == 'moneyline':
            if not (record.home_moneyline_odds or record.away_moneyline_odds):
                consistency_score -= 0.3
        elif record.market_type == 'spread':
            if not record.spread_line or not (record.home_spread_odds or record.away_spread_odds):
                consistency_score -= 0.3
        elif record.market_type == 'total':
            if not record.total_line or not (record.over_odds or record.under_odds):
                consistency_score -= 0.3
        
        return max(0.0, consistency_score)
    
    async def _validate_unified_record(self, record: UnifiedStagingRecord):
        """Final validation of unified record."""
        validation_errors = []
        
        # Required field validation
        if not record.external_game_id:
            validation_errors.append("Missing external_game_id")
        if not record.sportsbook_name:
            validation_errors.append("Missing sportsbook_name")
        if not record.home_team or not record.away_team:
            validation_errors.append("Missing team names")
        if not record.data_source:
            validation_errors.append("Missing data_source")
        
        # Set validation status
        if validation_errors:
            record.validation_status = "invalid"
            record.validation_errors = validation_errors
            logger.warning(f"Validation failed for record {record.external_game_id}: {validation_errors}")
        else:
            record.validation_status = "valid"
            record.validation_errors = None
    
    async def _consolidate_bet_records(self, records: List[UnifiedStagingRecord]) -> List[UnifiedStagingRecord]:
        """Consolidate multiple records for the same game/sportsbook (FIXES ISSUE #4)."""
        consolidated = {}
        
        for record in records:
            # Create consolidation key
            key = (
                record.external_game_id,
                record.sportsbook_external_id,
                # Don't include market_type in key to allow consolidation across markets
            )
            
            if key not in consolidated:
                consolidated[key] = record
            else:
                # Merge betting data from multiple records
                existing = consolidated[key]
                
                # Merge moneyline data
                if record.home_moneyline_odds and not existing.home_moneyline_odds:
                    existing.home_moneyline_odds = record.home_moneyline_odds
                if record.away_moneyline_odds and not existing.away_moneyline_odds:
                    existing.away_moneyline_odds = record.away_moneyline_odds
                
                # Merge spread data
                if record.spread_line and not existing.spread_line:
                    existing.spread_line = record.spread_line
                if record.home_spread_odds and not existing.home_spread_odds:
                    existing.home_spread_odds = record.home_spread_odds
                if record.away_spread_odds and not existing.away_spread_odds:
                    existing.away_spread_odds = record.away_spread_odds
                
                # Merge total data
                if record.total_line and not existing.total_line:
                    existing.total_line = record.total_line
                if record.over_odds and not existing.over_odds:
                    existing.over_odds = record.over_odds
                if record.under_odds and not existing.under_odds:
                    existing.under_odds = record.under_odds
                
                # Update market type to reflect all available markets
                markets = []
                if existing.home_moneyline_odds or existing.away_moneyline_odds:
                    markets.append('moneyline')
                if existing.spread_line and (existing.home_spread_odds or existing.away_spread_odds):
                    markets.append('spread')
                if existing.total_line and (existing.over_odds or existing.under_odds):
                    markets.append('total')
                
                existing.market_type = '+'.join(markets) if len(markets) > 1 else (markets[0] if markets else 'unknown')
        
        return list(consolidated.values())
    
    async def store_records(self, records: List[DataRecord]) -> None:
        """Store records using the unified implementation."""
        unified_records = [r for r in records if isinstance(r, UnifiedStagingRecord)]
        await self.store_unified_records(unified_records)
    
    async def store_unified_records(self, records: List[UnifiedStagingRecord]) -> None:
        """Store unified records to the new unified staging table using batch operations."""
        if not records:
            logger.debug("No records to store")
            return
            
        try:
            from ...data.database.connection import get_connection
            
            async with get_connection() as connection:
                async with connection.transaction():
                    query = """
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
                    )
                    """
                    
                    # Prepare batch data for executemany operation
                    batch_data = []
                    current_timestamp = datetime.now(timezone.utc)
                    
                    for record in records:
                        try:
                            record_data = (
                                record.data_source,
                                record.source_collector,
                                record.external_game_id,
                                record.mlb_stats_api_game_id,
                                record.game_date,
                                record.home_team,
                                record.away_team,
                                record.sportsbook_external_id,
                                record.sportsbook_id,
                                record.sportsbook_name,
                                record.market_type,
                                record.home_moneyline_odds,
                                record.away_moneyline_odds,
                                record.spread_line,
                                record.home_spread_odds,
                                record.away_spread_odds,
                                record.total_line,
                                record.over_odds,
                                record.under_odds,
                                record.raw_data_table,
                                record.raw_data_id,
                                json.dumps(record.transformation_metadata) if record.transformation_metadata else None,
                                record.data_quality_score,
                                record.validation_status,
                                json.dumps(record.validation_errors) if record.validation_errors else None,
                                record.collected_at,
                                record.processed_at or current_timestamp
                            )
                            batch_data.append(record_data)
                            
                        except Exception as e:
                            logger.error(f"Error preparing batch data for record {getattr(record, 'external_game_id', 'unknown')}: {e}")
                            # Continue with other records rather than failing the entire batch
                            continue
                    
                    if not batch_data:
                        logger.warning("No valid records to insert after batch preparation")
                        return
                    
                    # Execute batch insert using executemany for optimal performance
                    logger.debug(f"Executing batch insert for {len(batch_data)} records")
                    await connection.executemany(query, batch_data)
            
            logger.info(f"Successfully stored {len(batch_data)} unified staging records using batch operations")
            
        except Exception as e:
            logger.error(f"Error storing unified records in batch: {e}")
            raise
    
    def _safe_int_convert(self, value: Any) -> Optional[int]:
        """Safely convert value to integer."""
        try:
            if value is None:
                return None
            if isinstance(value, int):
                return value
            if isinstance(value, str):
                cleaned = value.strip()
                if cleaned:
                    return int(float(cleaned))  # Handle "110.0" format
            if isinstance(value, (float, Decimal)):
                return int(value)
            return None
        except (ValueError, TypeError):
            return None
    
    def _safe_decimal_convert(self, value: Any) -> Optional[Decimal]:
        """Safely convert value to Decimal."""
        try:
            if value is None:
                return None
            if isinstance(value, Decimal):
                return value
            if isinstance(value, (int, float)):
                return Decimal(str(value))
            if isinstance(value, str):
                cleaned = value.strip()
                if cleaned and cleaned not in [".", "-", "+"]:
                    return Decimal(cleaned)
            return None
        except (ValueError, TypeError, InvalidOperation):
            return None
    
    async def _query_game_data_by_source(self, external_game_id: str, source: str = 'action_network') -> Optional[Dict[str, Any]]:
        """Query source-specific games table for team names and metadata."""
        try:
            from ...data.database.connection import get_connection
            
            # Define source-specific table mappings
            games_table_mapping = {
                'action_network': 'raw_data.action_network_games',
                'vsin': 'raw_data.vsin_games',
                'sbd': 'raw_data.sbd_games',
                'mlb_stats_api': 'raw_data.mlb_stats_api_games'
            }
            
            games_table = games_table_mapping.get(source.lower(), 'raw_data.action_network_games')
            
            async with get_connection() as connection:
                query = f"""
                SELECT external_game_id, home_team, away_team, game_date, start_time,
                       home_team_abbr, away_team_abbr
                FROM {games_table}
                WHERE external_game_id = $1
                LIMIT 1
                """
                
                row = await connection.fetchrow(query, external_game_id)
                if row:
                    # Extract game date from available sources
                    game_date = None
                    if row['game_date']:
                        game_date = str(row['game_date'])
                    elif row['start_time']:
                        # Extract date from start_time if game_date is null
                        game_date = str(row['start_time'].date())
                    
                    result = {
                        'home_team': normalize_team_name(row['home_team']) if row['home_team'] else None,
                        'away_team': normalize_team_name(row['away_team']) if row['away_team'] else None,
                        'game_date': game_date,
                        'home_team_abbr': row['home_team_abbr'],
                        'away_team_abbr': row['away_team_abbr']
                    }
                    
                    logger.debug(f"✅ Found game data for {external_game_id}: {result['home_team']} vs {result['away_team']}")
                    return result
                else:
                    logger.debug(f"❌ No game data found for {external_game_id}")
                
                return None
                
        except Exception as e:
            logger.warning(f"Failed to query Action Network game data for {external_game_id}: {e}")
            return None
    
    async def _extract_team_ids_from_raw_data(self, record: UnifiedStagingRecord) -> Optional[Dict[str, int]]:
        """Extract team IDs from raw betting data."""
        try:
            raw_data = getattr(record, 'raw_data', None)
            if not raw_data or not isinstance(raw_data, dict):
                return None
            
            team_ids = {'home_team_id': None, 'away_team_id': None}
            
            # Look through betting data for team_id fields
            for bet_type in ['moneyline', 'spread', 'total']:
                if bet_type in raw_data and isinstance(raw_data[bet_type], list):
                    for bet_entry in raw_data[bet_type]:
                        if isinstance(bet_entry, dict) and 'team_id' in bet_entry:
                            side = bet_entry.get('side', '').lower()
                            team_id = bet_entry.get('team_id')
                            if side == 'home' and team_id:
                                team_ids['home_team_id'] = team_id
                            elif side == 'away' and team_id:
                                team_ids['away_team_id'] = team_id
            
            if team_ids['home_team_id'] and team_ids['away_team_id']:
                return team_ids
                
            return None
            
        except Exception as e:
            logger.debug(f"Failed to extract team IDs from raw data: {e}")
            return None
    
    async def _resolve_team_ids_to_names(self, team_ids: Dict[str, int], source: str = 'action_network') -> Optional[Dict[str, str]]:
        """Resolve team IDs to team names using externalized configuration."""
        try:
            from ...data.database.connection import get_connection
            
            # Get team mapping for the data source - EXTERNALIZED CONFIGURATION
            team_mapping = get_team_mapping(source)
            
            home_team_id = team_ids.get('home_team_id')
            away_team_id = team_ids.get('away_team_id')
            
            if not home_team_id or not away_team_id:
                return None
            
            # Check cache first - IMPLEMENT CACHE UTILIZATION
            cache_key = f"{source}_{home_team_id}_{away_team_id}"
            if cache_key in self.team_resolution_cache:
                logger.debug(f"🔄 Using cached team resolution for {cache_key}")
                return self.team_resolution_cache[cache_key]
            
            # Try the externalized mapping
            home_team = team_mapping.get(home_team_id)
            away_team = team_mapping.get(away_team_id)
            
            if home_team and away_team:
                result = {
                    'home_team': home_team,
                    'away_team': away_team
                }
                # Cache the successful resolution
                self.team_resolution_cache[cache_key] = result
                logger.debug(f"✅ Resolved and cached team IDs via mapping: {home_team_id}→{home_team}, {away_team_id}→{away_team}")
                return result
            
            # Try to find team IDs in other games in the database (source-specific)
            # Define source-specific table mappings for database lookup
            odds_table_mapping = {
                'action_network': ('raw_data.action_network_odds', 'raw_data.action_network_games'),
                'vsin': ('raw_data.vsin_data', 'raw_data.vsin_games'),
                'sbd': ('raw_data.sbd_betting_splits', 'raw_data.sbd_games'),
                'mlb_stats_api': ('raw_data.mlb_stats_api_odds', 'raw_data.mlb_stats_api_games')
            }
            
            odds_table, games_table = odds_table_mapping.get(source.lower(), 
                ('raw_data.action_network_odds', 'raw_data.action_network_games'))
            
            async with get_connection() as connection:
                query = f"""
                SELECT DISTINCT 
                    r.raw_odds->>'team_id' as team_id,
                    g.home_team,
                    g.away_team
                FROM {odds_table} r
                JOIN {games_table} g ON r.external_game_id = g.external_game_id
                WHERE r.raw_odds ? 'team_id' 
                AND (r.raw_odds->>'team_id')::int IN ($1, $2)
                LIMIT 10
                """
                
                rows = await connection.fetch(query, home_team_id, away_team_id)
                if rows:
                    # Try to deduce team names from other games
                    for row in rows:
                        stored_team_id = int(row['team_id'])
                        if stored_team_id == home_team_id and not home_team:
                            home_team = normalize_team_name(row['home_team']) if row['home_team'] else None
                        elif stored_team_id == away_team_id and not away_team:
                            away_team = normalize_team_name(row['away_team']) if row['away_team'] else None
                    
                    if home_team and away_team:
                        result = {
                            'home_team': home_team,
                            'away_team': away_team
                        }
                        # Cache the successful database resolution
                        self.team_resolution_cache[cache_key] = result
                        logger.debug(f"✅ Resolved and cached team IDs via database lookup: {home_team_id}→{home_team}, {away_team_id}→{away_team}")
                        return result
            
            logger.debug(f"❌ Could not resolve team IDs: {home_team_id}, {away_team_id}")
            return None
            
        except Exception as e:
            logger.debug(f"Failed to resolve team IDs to names: {e}")
            return None
    
    async def _lookup_mlb_stats_api_game_id(self, external_game_id: str, team_info: Dict[str, Any]) -> Optional[str]:
        """Lookup MLB Stats API game ID based on game info."""
        try:
            from ...data.database.connection import get_connection
            
            # First, try to find it in our database if we have MLB Stats API data
            async with get_connection() as connection:
                # Check if we have any MLB Stats API data tables
                tables_query = """
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'raw_data' 
                AND table_name LIKE '%mlb_stats%'
                """
                
                tables = await connection.fetch(tables_query)
                if tables:
                    # Try to find matching game in MLB Stats data
                    mlb_query = """
                    SELECT mlb_game_id 
                    FROM raw_data.mlb_stats_games 
                    WHERE game_date = $1 
                    AND (home_team_abbr = $2 OR away_team_abbr = $2)
                    AND (home_team_abbr = $3 OR away_team_abbr = $3)
                    LIMIT 1
                    """
                    
                    game_date = team_info.get('game_date')
                    home_team = team_info.get('home_team_abbr') or team_info.get('home_team')
                    away_team = team_info.get('away_team_abbr') or team_info.get('away_team')
                    
                    if game_date and home_team and away_team:
                        row = await connection.fetchrow(mlb_query, game_date, home_team, away_team)
                        if row:
                            logger.debug(f"✅ Found MLB Stats API game ID via database: {row['mlb_game_id']}")
                            return str(row['mlb_game_id'])
            
            # For now, return None as we don't have direct MLB Stats API integration
            # In a production environment, this would:
            # 1. Call MLB Stats API with team names and game date
            # 2. Parse the response to find the matching game
            # 3. Cache the result to avoid repeated API calls
            # 4. Handle rate limiting and authentication
            
            logger.debug(f"❌ MLB Stats API game ID lookup not implemented for {external_game_id}")
            return None
            
        except Exception as e:
            logger.debug(f"Failed to lookup MLB Stats API game ID: {e}")
            return None


# Register the unified staging processor
ZoneFactory.register_zone(ZoneType.STAGING, UnifiedStagingProcessor)