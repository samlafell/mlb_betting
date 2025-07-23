"""
STAGING Zone Processor

Handles data cleaning, normalization, and validation from RAW zone.
STAGING Zone prepares data for analysis with quality control.

Reference: docs/SYSTEM_DESIGN_ANALYSIS.md
"""

import json
import re
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from pydantic import BaseModel, Field

from ...core.logging import get_logger, LogComponent
from ...core.team_utils import normalize_team_name
from ...core.datetime_utils import utc_to_est
from .base_processor import BaseZoneProcessor
from .zone_interface import (
    DataRecord,
    ProcessingResult,
    ProcessingStatus,
    ZoneConfig,
    ZoneType,
    ZoneFactory
)

logger = get_logger(__name__, LogComponent.CORE)


class StagingDataRecord(DataRecord):
    """Staging data record with normalized fields."""
    game_id: Optional[int] = None
    sportsbook_id: Optional[int] = None
    sportsbook_name: Optional[str] = None
    home_team_normalized: Optional[str] = None
    away_team_normalized: Optional[str] = None
    team_normalized: Optional[str] = None
    bet_type: Optional[str] = None
    line_value: Optional[Decimal] = None
    odds_american: Optional[int] = None
    team_type: Optional[str] = None  # 'home', 'away', 'over', 'under'
    data_completeness_score: Optional[float] = None
    data_accuracy_score: Optional[float] = None
    data_consistency_score: Optional[float] = None


class StagingZoneProcessor(BaseZoneProcessor):
    """
    STAGING Zone processor for data cleaning and normalization.
    
    Responsibilities:
    - Clean and validate raw data
    - Normalize team names and sportsbook names
    - Convert data types and formats
    - Calculate data quality metrics
    - Detect and handle duplicates
    """

    def __init__(self, config: ZoneConfig):
        super().__init__(config)
        self.team_name_cache = {}
        self.sportsbook_mapping = {}
        self._load_reference_data()

    def _load_reference_data(self):
        """Load reference data for normalization."""
        # Standard sportsbook name mappings
        self.sportsbook_mapping = {
            'draftkings': 'DraftKings',
            'fanduel': 'FanDuel', 
            'betmgm': 'BetMGM',
            'caesars': 'Caesars',
            'bet365': 'Bet365',
            'fanatics': 'Fanatics',
            'pinnacle': 'Pinnacle',
            'circa': 'Circa Sports',
            'westgate': 'Westgate',
            'pointsbet': 'PointsBet'
        }

    async def process_record(
        self, 
        record: DataRecord, 
        **kwargs
    ) -> Optional[DataRecord]:
        """
        Process a single RAW record into a clean STAGING record.
        
        Args:
            record: RAW data record to process
            **kwargs: Additional processing parameters
            
        Returns:
            Cleaned and normalized staging record
        """
        try:
            # Create staging record from raw record
            staging_record = StagingDataRecord(**record.model_dump())
            
            # Clean and normalize based on raw data
            if staging_record.raw_data:
                staging_record = await self._normalize_from_raw_data(staging_record)
            
            # Normalize team names
            staging_record = await self._normalize_team_names(staging_record)
            
            # Normalize sportsbook names
            staging_record = await self._normalize_sportsbook_names(staging_record)
            
            # Clean numeric fields
            staging_record = await self._clean_numeric_fields(staging_record)
            
            # Validate data consistency
            is_consistent = await self._validate_data_consistency(staging_record)
            if not is_consistent:
                logger.warning(f"Data consistency issues in record {staging_record.external_id}")
            
            # Calculate quality scores
            staging_record.data_completeness_score = await self._calculate_completeness_score(staging_record)
            staging_record.data_accuracy_score = await self._calculate_accuracy_score(staging_record)
            staging_record.data_consistency_score = await self._calculate_consistency_score(staging_record)
            
            # Overall quality score
            staging_record.quality_score = (
                staging_record.data_completeness_score * 0.4 +
                staging_record.data_accuracy_score * 0.3 +
                staging_record.data_consistency_score * 0.3
            )
            
            # Set processing timestamp
            staging_record.processed_at = datetime.now(timezone.utc)
            staging_record.validation_status = ProcessingStatus.COMPLETED
            
            logger.debug(f"Processed staging record: {staging_record.external_id} (quality: {staging_record.quality_score:.2f})")
            return staging_record
            
        except Exception as e:
            logger.error(f"Error processing staging record {record.external_id}: {e}")
            return None

    async def _normalize_from_raw_data(self, record: StagingDataRecord) -> StagingDataRecord:
        """Extract and normalize fields from raw_data JSON."""
        try:
            raw_data = record.raw_data
            if not isinstance(raw_data, dict):
                return record
            
            # Extract game information
            if 'game' in raw_data:
                game_data = raw_data['game']
                if 'home_team' in game_data:
                    record.home_team_normalized = str(game_data['home_team'])
                if 'away_team' in game_data:
                    record.away_team_normalized = str(game_data['away_team'])
            
            # Extract team names from various formats
            for field in ['home_team', 'away_team', 'homeTeam', 'awayTeam']:
                if field in raw_data:
                    if 'home' in field.lower():
                        record.home_team_normalized = str(raw_data[field])
                    else:
                        record.away_team_normalized = str(raw_data[field])
            
            # Extract sportsbook information
            if 'sportsbook' in raw_data:
                sportsbook = raw_data['sportsbook']
                if isinstance(sportsbook, dict):
                    record.sportsbook_name = sportsbook.get('name', sportsbook.get('key'))
                    record.sportsbook_id = sportsbook.get('id')
                else:
                    record.sportsbook_name = str(sportsbook)
            
            # Extract betting line information
            if 'outcomes' in raw_data:
                # Action Network format
                outcomes = raw_data['outcomes']
                if isinstance(outcomes, list) and outcomes:
                    first_outcome = outcomes[0]
                    record.odds_american = first_outcome.get('price')
                    record.line_value = self._safe_decimal_convert(first_outcome.get('point'))
            
            # Extract moneyline odds
            if 'moneyline' in raw_data:
                ml_data = raw_data['moneyline']
                if isinstance(ml_data, dict):
                    if 'home' in ml_data:
                        record.odds_american = ml_data['home']
                        record.team_type = 'home'
                    elif 'away' in ml_data:
                        record.odds_american = ml_data['away'] 
                        record.team_type = 'away'
            
            # Extract spread information
            if 'spread' in raw_data:
                spread_data = raw_data['spread']
                if isinstance(spread_data, dict):
                    record.line_value = self._safe_decimal_convert(spread_data.get('point', spread_data.get('line')))
                    record.odds_american = spread_data.get('price', spread_data.get('odds'))
                    record.bet_type = 'spread'
            
            # Extract total information
            if 'total' in raw_data:
                total_data = raw_data['total']
                if isinstance(total_data, dict):
                    record.line_value = self._safe_decimal_convert(total_data.get('point', total_data.get('line')))
                    if 'over' in total_data:
                        record.odds_american = total_data['over']
                        record.team_type = 'over'
                    elif 'under' in total_data:
                        record.odds_american = total_data['under']
                        record.team_type = 'under'
                    record.bet_type = 'total'
            
            return record
            
        except Exception as e:
            logger.error(f"Error normalizing raw data: {e}")
            return record

    async def _normalize_team_names(self, record: StagingDataRecord) -> StagingDataRecord:
        """Normalize team names using team_utils."""
        try:
            if record.home_team_normalized:
                normalized = normalize_team_name(record.home_team_normalized)
                record.home_team_normalized = normalized
                
            if record.away_team_normalized:
                normalized = normalize_team_name(record.away_team_normalized)
                record.away_team_normalized = normalized
                
            if record.team_normalized:
                normalized = normalize_team_name(record.team_normalized)
                record.team_normalized = normalized
            
            return record
            
        except Exception as e:
            logger.error(f"Error normalizing team names: {e}")
            return record

    async def _normalize_sportsbook_names(self, record: StagingDataRecord) -> StagingDataRecord:
        """Normalize sportsbook names."""
        try:
            if record.sportsbook_name:
                sportsbook_key = record.sportsbook_name.lower().replace(' ', '').replace('-', '')
                
                # Check mapping
                if sportsbook_key in self.sportsbook_mapping:
                    record.sportsbook_name = self.sportsbook_mapping[sportsbook_key]
                else:
                    # Clean up the name
                    record.sportsbook_name = record.sportsbook_name.title()
            
            return record
            
        except Exception as e:
            logger.error(f"Error normalizing sportsbook names: {e}")
            return record

    async def _clean_numeric_fields(self, record: StagingDataRecord) -> StagingDataRecord:
        """Clean and validate numeric fields."""
        try:
            # Clean odds (should be integers)
            if record.odds_american:
                record.odds_american = self._safe_int_convert(record.odds_american)
            
            # Clean line values (should be decimals)
            if record.line_value:
                record.line_value = self._safe_decimal_convert(record.line_value)
            
            return record
            
        except Exception as e:
            logger.error(f"Error cleaning numeric fields: {e}")
            return record

    def _safe_int_convert(self, value: Any) -> Optional[int]:
        """Safely convert value to integer."""
        try:
            if value is None:
                return None
            if isinstance(value, int):
                return value
            if isinstance(value, str):
                # Remove non-numeric characters except +/-
                cleaned = re.sub(r'[^\d\-\+]', '', value)
                if cleaned:
                    return int(cleaned)
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
                # Remove non-numeric characters except decimal point and +/-
                cleaned = re.sub(r'[^\d\.\-\+]', '', value)
                if cleaned and cleaned not in ['.', '-', '+']:
                    return Decimal(cleaned)
            return None
        except (ValueError, TypeError, InvalidOperation):
            return None

    async def _validate_data_consistency(self, record: StagingDataRecord) -> bool:
        """Validate data consistency across fields."""
        try:
            issues = []
            
            # Check if odds are reasonable
            if record.odds_american:
                if record.odds_american < -5000 or record.odds_american > 5000:
                    issues.append(f"Unusual odds: {record.odds_american}")
            
            # Check if spread values are reasonable
            if record.line_value and record.bet_type == 'spread':
                if abs(record.line_value) > 50:
                    issues.append(f"Unusual spread: {record.line_value}")
            
            # Check if total values are reasonable
            if record.line_value and record.bet_type == 'total':
                if record.line_value < 0 or record.line_value > 50:
                    issues.append(f"Unusual total: {record.line_value}")
            
            # Check team name consistency
            if (record.home_team_normalized and record.away_team_normalized and 
                record.home_team_normalized == record.away_team_normalized):
                issues.append("Home and away teams are identical")
            
            if issues:
                record.validation_errors = (record.validation_errors or []) + issues
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating data consistency: {e}")
            return False

    async def store_records(self, records: List[DataRecord]) -> None:
        """
        Store processed staging records to appropriate staging tables.
        
        Args:
            records: List of processed staging records to store
        """
        try:
            db_connection = await self.get_connection()
            
            # Group records by bet type
            records_by_type = {
                'moneyline': [],
                'spread': [],
                'total': [],
                'generic': []
            }
            
            for record in records:
                bet_type = getattr(record, 'bet_type', 'generic')
                if bet_type in records_by_type:
                    records_by_type[bet_type].append(record)
                else:
                    records_by_type['generic'].append(record)
            
            # Insert records for each type
            async with db_connection.get_async_connection() as connection:
                for bet_type, type_records in records_by_type.items():
                    if not type_records:
                        continue
                        
                    if bet_type == 'moneyline':
                        await self._insert_moneylines(connection, type_records)
                    elif bet_type == 'spread':
                        await self._insert_spreads(connection, type_records)
                    elif bet_type == 'total':
                        await self._insert_totals(connection, type_records)
                    else:
                        await self._insert_betting_lines(connection, type_records)
            
            logger.info(f"Stored {len(records)} staging records")
            
        except Exception as e:
            logger.error(f"Error storing staging records: {e}")
            raise

    async def _insert_moneylines(self, connection, records: List[DataRecord]) -> None:
        """Insert moneyline records to staging.moneylines."""
        query = """
        INSERT INTO staging.moneylines 
        (raw_id, game_id, sportsbook_id, sportsbook_name, home_odds, away_odds,
         home_team_normalized, away_team_normalized, data_quality_score, 
         validation_status, validation_errors, processed_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        """
        
        for record in records:
            await connection.execute(
                query,
                getattr(record, 'id', None),  # raw_id
                getattr(record, 'game_id', None),
                getattr(record, 'sportsbook_id', None),
                getattr(record, 'sportsbook_name', None),
                getattr(record, 'odds_american', None) if getattr(record, 'team_type', None) == 'home' else None,
                getattr(record, 'odds_american', None) if getattr(record, 'team_type', None) == 'away' else None,
                getattr(record, 'home_team_normalized', None),
                getattr(record, 'away_team_normalized', None),
                getattr(record, 'quality_score', None),
                record.validation_status.value if record.validation_status else 'pending',
                json.dumps(record.validation_errors) if record.validation_errors else None,
                record.processed_at
            )

    async def _insert_spreads(self, connection, records: List[DataRecord]) -> None:
        """Insert spread records to staging.spreads."""
        query = """
        INSERT INTO staging.spreads 
        (raw_id, game_id, sportsbook_id, sportsbook_name, spread_value, spread_odds,
         favorite_team_normalized, underdog_team_normalized, data_quality_score,
         validation_status, validation_errors, processed_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        """
        
        for record in records:
            await connection.execute(
                query,
                getattr(record, 'id', None),
                getattr(record, 'game_id', None),
                getattr(record, 'sportsbook_id', None),
                getattr(record, 'sportsbook_name', None),
                getattr(record, 'line_value', None),
                getattr(record, 'odds_american', None),
                getattr(record, 'home_team_normalized', None),  # Assuming home is favorite for now
                getattr(record, 'away_team_normalized', None),
                getattr(record, 'quality_score', None),
                record.validation_status.value if record.validation_status else 'pending',
                json.dumps(record.validation_errors) if record.validation_errors else None,
                record.processed_at
            )

    async def _insert_totals(self, connection, records: List[DataRecord]) -> None:
        """Insert total records to staging.totals."""
        query = """
        INSERT INTO staging.totals 
        (raw_id, game_id, sportsbook_id, sportsbook_name, total_points, over_odds, under_odds,
         data_quality_score, validation_status, validation_errors, processed_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        """
        
        for record in records:
            await connection.execute(
                query,
                getattr(record, 'id', None),
                getattr(record, 'game_id', None),
                getattr(record, 'sportsbook_id', None),
                getattr(record, 'sportsbook_name', None),
                getattr(record, 'line_value', None),
                getattr(record, 'odds_american', None) if getattr(record, 'team_type', None) == 'over' else None,
                getattr(record, 'odds_american', None) if getattr(record, 'team_type', None) == 'under' else None,
                getattr(record, 'quality_score', None),
                record.validation_status.value if record.validation_status else 'pending',
                json.dumps(record.validation_errors) if record.validation_errors else None,
                record.processed_at
            )

    async def _insert_betting_lines(self, connection, records: List[DataRecord]) -> None:
        """Insert generic betting lines to staging.betting_lines."""
        query = """
        INSERT INTO staging.betting_lines 
        (raw_betting_lines_id, game_id, sportsbook_id, bet_type, line_value,
         odds_american, team_type, team_normalized, data_quality_score,
         validation_status, processed_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        """
        
        for record in records:
            await connection.execute(
                query,
                getattr(record, 'id', None),
                getattr(record, 'game_id', None),
                getattr(record, 'sportsbook_id', None),
                getattr(record, 'bet_type', None),
                getattr(record, 'line_value', None),
                getattr(record, 'odds_american', None),
                getattr(record, 'team_type', None),
                getattr(record, 'team_normalized', None),
                getattr(record, 'quality_score', None),
                record.validation_status.value if record.validation_status else 'pending',
                record.processed_at
            )

    async def promote_to_next_zone(
        self, 
        records: List[DataRecord]
    ) -> ProcessingResult:
        """
        Promote validated STAGING records to CURATED zone.
        
        Args:
            records: Records to promote to CURATED
            
        Returns:
            ProcessingResult with promotion status
        """
        try:
            # Import here to avoid circular imports
            from .curated_zone import CuratedZoneProcessor
            from .zone_interface import create_zone_config, ZoneType
            
            # Create CURATED zone processor
            curated_config = create_zone_config(
                ZoneType.CURATED,
                self.settings.schemas.curated
            )
            curated_processor = CuratedZoneProcessor(curated_config)
            
            # Process records in CURATED zone
            result = await curated_processor.process_batch(records)
            
            logger.info(
                f"Promoted {result.records_successful} records from STAGING to CURATED"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error promoting records to CURATED: {e}")
            from .zone_interface import ProcessingResult, ProcessingStatus
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                records_processed=len(records),
                errors=[str(e)]
            )


# Register the STAGING zone processor
ZoneFactory.register_zone(ZoneType.STAGING, StagingZoneProcessor)