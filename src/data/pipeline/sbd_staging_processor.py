"""
SBD Staging Processor

Specialized processor for transforming SBD raw JSON data into normalized staging format.
Handles complex SBD API responses and extracts betting splits data for business analysis.

Reference: docs/july21_collection_enhancements/SBD_RAW_TO_STAGING_PIPELINE_DESIGN.md
"""

import json
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

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
    ZoneType
)

logger = get_logger(__name__, LogComponent.CORE)


class SBDGameRecord(BaseModel):
    """Normalized SBD game record for staging."""
    external_id: str
    home_team_normalized: str
    away_team_normalized: str
    game_datetime: datetime
    sport: str = "mlb"
    game_name: Optional[str] = None
    raw_game_id: Optional[str] = None


class SBDBettingSplitRecord(BaseModel):
    """Normalized SBD betting split record for staging."""
    game_id: Optional[int] = None
    sportsbook_name: str
    bet_type: str  # 'moneyline', 'spread', 'totals'
    public_bet_percentage: Optional[Decimal] = None
    public_money_percentage: Optional[Decimal] = None
    sharp_bet_percentage: Optional[Decimal] = None
    raw_sportsbook_id: Optional[str] = None
    data_source: str = "sbd"
    extraction_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class SBDSharpActionSignal(BaseModel):
    """SBD sharp action signal for analysis."""
    game_id: Optional[int] = None
    bet_type: str
    signal_type: str = "money_vs_bets_discrepancy"
    signal_strength: Decimal  # 0.00 to 1.00
    confidence_score: Decimal  # 0.00 to 1.00
    trigger_conditions: Dict[str, Any]
    detected_at: datetime


class SBDStagingProcessor(BaseZoneProcessor):
    """
    SBD-specific staging processor for raw JSON data transformation.
    
    Responsibilities:
    - Parse complex SBD API JSON responses
    - Extract and normalize game data
    - Process betting splits with sharp action detection
    - Generate data quality metrics
    - Store in appropriate staging tables
    """

    def __init__(self, config: ZoneConfig):
        super().__init__(config)
        self.sportsbook_mapping = self._load_sportsbook_mapping()
        self.processed_games_cache = {}  # Cache to avoid duplicate game processing
        
    def _load_sportsbook_mapping(self) -> Dict[str, str]:
        """Load SBD sportsbook ID to name mapping."""
        return {
            "sr:book:6": "Unibet",
            "sr:book:7612": "Betway2",
            "sr:book:17324": "MGM",
            "sr:book:18149": "DraftKings",
            "sr:book:18186": "FanDuel",
            "sr:book:27447": "SugarHouseNJ",
            "sr:book:27769": "PointsBet",
            "sr:book:28901": "Bet365NewJersey",
            "sr:book:32219": "WilliamHillNewJersey",
            # Add more mappings as discovered
        }

    async def process_sbd_raw_records(self, limit: Optional[int] = None) -> ProcessingResult:
        """
        Process SBD raw records from raw_data.sbd_betting_splits table.
        
        Args:
            limit: Optional limit on number of records to process
            
        Returns:
            ProcessingResult with processing statistics
        """
        try:
            logger.info("Starting SBD raw-to-staging processing")
            
            # Get database connection
            from ...data.database.connection import get_connection
            db_connection = get_connection()
            
            # Query unprocessed raw records
            async with db_connection.get_async_connection() as connection:
                query = """
                    SELECT id, external_matchup_id, raw_response, collected_at
                    FROM raw_data.sbd_betting_splits 
                    WHERE processed_at IS NULL
                    ORDER BY collected_at DESC
                """
                if limit:
                    query += f" LIMIT {limit}"
                    
                raw_records = await connection.fetch(query)
            
            if not raw_records:
                logger.info("No unprocessed SBD records found")
                return ProcessingResult(
                    status=ProcessingStatus.COMPLETED,
                    records_processed=0,
                    records_successful=0
                )
            
            logger.info(f"Found {len(raw_records)} unprocessed SBD records")
            
            # Process records in batches
            batch_size = 50
            total_processed = 0
            total_successful = 0
            errors = []
            
            for i in range(0, len(raw_records), batch_size):
                batch = raw_records[i:i + batch_size]
                batch_result = await self._process_sbd_batch(db_connection, batch)
                
                total_processed += batch_result.records_processed
                total_successful += batch_result.records_successful
                errors.extend(batch_result.errors or [])
                
                logger.info(
                    f"Processed batch {i//batch_size + 1}: "
                    f"{batch_result.records_successful}/{batch_result.records_processed} successful"
                )
            
            # Mark raw records as processed
            raw_ids = [record['id'] for record in raw_records]
            await self._mark_raw_records_processed(db_connection, raw_ids)
            
            result = ProcessingResult(
                status=ProcessingStatus.COMPLETED if total_successful > 0 else ProcessingStatus.FAILED,
                records_processed=total_processed,
                records_successful=total_successful,
                errors=errors
            )
            
            logger.info(
                f"SBD processing completed: {total_successful}/{total_processed} successful"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error in SBD staging processing: {e}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                records_processed=0,
                records_successful=0,
                errors=[str(e)]
            )

    async def _process_sbd_batch(
        self, 
        db_connection, 
        raw_records: List[Dict[str, Any]]
    ) -> ProcessingResult:
        """Process a batch of SBD raw records."""
        processed = 0
        successful = 0
        errors = []
        
        try:
            async with db_connection.get_async_connection() as connection:
                async with connection.transaction():
                    for raw_record in raw_records:
                        try:
                            # Parse JSON response
                            raw_response = raw_record['raw_response']
                            if isinstance(raw_response, str):
                                raw_response = json.loads(raw_response)
                            
                            # Extract game data and betting data
                            game_record = self._extract_game_data(raw_response)
                            betting_record = self._extract_betting_split_data(raw_response)
                            
                            if not game_record or not betting_record:
                                errors.append(f"Failed to extract data from record {raw_record['id']}")
                                processed += 1
                                continue
                            
                            # Get or create game ID
                            game_id = await self._get_or_create_game(connection, game_record)
                            betting_record.game_id = game_id
                            
                            # Store betting split
                            await self._store_betting_split(connection, betting_record)
                            
                            # Generate sharp action signal if detected
                            sharp_signal = self._detect_sharp_action_signal(betting_record)
                            if sharp_signal:
                                sharp_signal.game_id = game_id
                                await self._store_sharp_action_signal(connection, sharp_signal)
                            
                            successful += 1
                            processed += 1
                            
                        except Exception as e:
                            logger.error(f"Error processing SBD record {raw_record['id']}: {e}")
                            errors.append(f"Record {raw_record['id']}: {str(e)}")
                            processed += 1
                            continue
            
            return ProcessingResult(
                status=ProcessingStatus.COMPLETED,
                records_processed=processed,
                records_successful=successful,
                errors=errors
            )
            
        except Exception as e:
            logger.error(f"Error in SBD batch processing: {e}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                records_processed=processed,
                records_successful=successful,
                errors=errors + [str(e)]
            )

    def _extract_game_data(self, raw_response: Dict[str, Any]) -> Optional[SBDGameRecord]:
        """Extract and normalize game data from SBD raw response."""
        try:
            game_data = raw_response.get('game_data', {})
            
            if not game_data:
                logger.warning("No game_data found in SBD response")
                return None
            
            # Extract required fields
            external_game_id = game_data.get('external_game_id')
            home_team = game_data.get('home_team')
            away_team = game_data.get('away_team')
            game_datetime_str = game_data.get('game_datetime')
            
            if not all([external_game_id, home_team, away_team, game_datetime_str]):
                logger.warning("Missing required game data fields")
                return None
            
            # Parse datetime
            try:
                if isinstance(game_datetime_str, str):
                    game_datetime = datetime.fromisoformat(game_datetime_str.replace('Z', '+00:00'))
                else:
                    game_datetime = datetime.now(timezone.utc)
                    
                # Ensure timezone aware
                if game_datetime.tzinfo is None:
                    game_datetime = game_datetime.replace(tzinfo=timezone.utc)
                    
            except (ValueError, TypeError) as e:
                logger.warning(f"Error parsing game datetime: {e}")
                game_datetime = datetime.now(timezone.utc)
            
            return SBDGameRecord(
                external_id=external_game_id,
                home_team_normalized=normalize_team_name(home_team),
                away_team_normalized=normalize_team_name(away_team),
                game_datetime=game_datetime,
                sport=game_data.get('sport', 'mlb'),
                game_name=game_data.get('game_name'),
                raw_game_id=game_data.get('game_id')
            )
            
        except Exception as e:
            logger.error(f"Error extracting game data: {e}")
            return None

    def _extract_betting_split_data(self, raw_response: Dict[str, Any]) -> Optional[SBDBettingSplitRecord]:
        """Extract and normalize betting split data from SBD raw response."""
        try:
            betting_record = raw_response.get('betting_record', {})
            
            if not betting_record:
                logger.warning("No betting_record found in SBD response")
                return None
            
            # Extract sportsbook information
            sportsbook = betting_record.get('sportsbook')
            sportsbook_id = betting_record.get('sportsbook_id')
            bet_type = betting_record.get('bet_type')
            
            if not all([sportsbook, bet_type]):
                logger.warning("Missing required betting record fields")
                return None
            
            # Normalize sportsbook name
            normalized_sportsbook = self._normalize_sportsbook_name(sportsbook, sportsbook_id)
            
            # Extract percentages
            public_bet_pct = self._safe_decimal_convert(betting_record.get('home_bets_percentage'))
            public_money_pct = self._safe_decimal_convert(betting_record.get('home_money_percentage'))
            
            # Calculate sharp action percentage
            sharp_bet_pct = self._calculate_sharp_percentage(public_bet_pct, public_money_pct)
            
            return SBDBettingSplitRecord(
                sportsbook_name=normalized_sportsbook,
                bet_type=bet_type,
                public_bet_percentage=public_bet_pct,
                public_money_percentage=public_money_pct,
                sharp_bet_percentage=sharp_bet_pct,
                raw_sportsbook_id=sportsbook_id,
                data_source="sbd"
            )
            
        except Exception as e:
            logger.error(f"Error extracting betting split data: {e}")
            return None

    def _normalize_sportsbook_name(self, sportsbook: str, sportsbook_id: Optional[str] = None) -> str:
        """Normalize sportsbook name using ID mapping or name cleaning."""
        try:
            # Try ID mapping first
            if sportsbook_id and sportsbook_id in self.sportsbook_mapping:
                return self.sportsbook_mapping[sportsbook_id]
            
            # Clean up sportsbook name
            if sportsbook:
                # Remove common suffixes and clean up
                cleaned = sportsbook.replace('NewJersey', '').replace('2', '').strip()
                return cleaned.title()
            
            return "Unknown"
            
        except Exception as e:
            logger.error(f"Error normalizing sportsbook name: {e}")
            return sportsbook or "Unknown"

    def _safe_decimal_convert(self, value: Any) -> Optional[Decimal]:
        """Safely convert value to Decimal with validation."""
        try:
            if value is None:
                return None
            
            if isinstance(value, Decimal):
                return value
            
            if isinstance(value, (int, float)):
                decimal_value = Decimal(str(value))
                # Validate percentage range
                if 0 <= decimal_value <= 100:
                    return decimal_value
                else:
                    logger.warning(f"Percentage value out of range: {decimal_value}")
                    return None
            
            if isinstance(value, str):
                # Remove non-numeric characters except decimal point
                cleaned = re.sub(r'[^\d\.]', '', value)
                if cleaned and cleaned != '.':
                    decimal_value = Decimal(cleaned)
                    if 0 <= decimal_value <= 100:
                        return decimal_value
            
            return None
            
        except (ValueError, TypeError, InvalidOperation):
            return None

    def _calculate_sharp_percentage(
        self, 
        public_bet_pct: Optional[Decimal], 
        public_money_pct: Optional[Decimal]
    ) -> Optional[Decimal]:
        """Calculate sharp action percentage based on money vs bets discrepancy."""
        try:
            if not public_bet_pct or not public_money_pct:
                return None
            
            # Calculate discrepancy (money % - bets %)
            discrepancy = public_money_pct - public_bet_pct
            
            # Sharp action thresholds
            if discrepancy >= 25:
                # Strong sharp action: money % is 25+ points higher than bets %
                return min(Decimal('100.0'), public_money_pct + 20)
            elif discrepancy >= 15:
                # Moderate sharp action: money % is 15-24 points higher than bets %
                return min(Decimal('100.0'), public_money_pct + 10)
            elif discrepancy >= 5:
                # Light sharp action: money % is 5-14 points higher than bets %
                return min(Decimal('100.0'), public_money_pct + 5)
            else:
                # No significant sharp action detected
                return Decimal('0.0')
                
        except Exception as e:
            logger.error(f"Error calculating sharp percentage: {e}")
            return None

    def _detect_sharp_action_signal(
        self, 
        betting_record: SBDBettingSplitRecord
    ) -> Optional[SBDSharpActionSignal]:
        """Detect and create sharp action signal if significant discrepancy exists."""
        try:
            if (not betting_record.public_bet_percentage or 
                not betting_record.public_money_percentage or
                not betting_record.sharp_bet_percentage):
                return None
            
            # Only create signal if sharp action percentage > 0
            if betting_record.sharp_bet_percentage <= 0:
                return None
            
            discrepancy = betting_record.public_money_percentage - betting_record.public_bet_percentage
            
            # Determine signal strength and confidence
            if discrepancy >= 25:
                signal_strength = Decimal('1.0')  # Maximum strength
                confidence_score = Decimal('0.95')
            elif discrepancy >= 15:
                signal_strength = Decimal('0.7')
                confidence_score = Decimal('0.80')
            elif discrepancy >= 5:
                signal_strength = Decimal('0.4')
                confidence_score = Decimal('0.65')
            else:
                return None  # Below threshold
            
            trigger_conditions = {
                "public_bet_percentage": float(betting_record.public_bet_percentage),
                "public_money_percentage": float(betting_record.public_money_percentage),
                "discrepancy": float(discrepancy),
                "sportsbook": betting_record.sportsbook_name,
                "bet_type": betting_record.bet_type,
                "threshold_met": "money_vs_bets_discrepancy"
            }
            
            return SBDSharpActionSignal(
                bet_type=betting_record.bet_type,
                signal_strength=signal_strength,
                confidence_score=confidence_score,
                trigger_conditions=trigger_conditions,
                detected_at=datetime.now(timezone.utc)
            )
            
        except Exception as e:
            logger.error(f"Error detecting sharp action signal: {e}")
            return None

    async def _get_or_create_game(
        self, 
        connection, 
        game_record: SBDGameRecord
    ) -> int:
        """Get existing game ID or create new game record."""
        try:
            # Check cache first
            cache_key = game_record.external_id
            if cache_key in self.processed_games_cache:
                return self.processed_games_cache[cache_key]
            
            # Check if game already exists
            existing_game = await connection.fetchrow(
                """
                SELECT id FROM staging.games 
                WHERE external_id = $1
                """,
                game_record.external_id
            )
            
            if existing_game:
                game_id = existing_game['id']
                self.processed_games_cache[cache_key] = game_id
                return game_id
            
            # Create new game record
            game_id = await connection.fetchval(
                """
                INSERT INTO staging.games 
                (external_id, home_team_normalized, away_team_normalized, 
                 game_date, game_datetime, season, venue, data_quality_score,
                 validation_status, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                RETURNING id
                """,
                game_record.external_id,
                game_record.home_team_normalized,
                game_record.away_team_normalized,
                game_record.game_datetime.date(),
                game_record.game_datetime,
                game_record.game_datetime.year,  # season
                None,  # venue
                0.95,  # data_quality_score
                'validated',  # validation_status
                datetime.now(timezone.utc),  # created_at
                datetime.now(timezone.utc)   # updated_at
            )
            
            self.processed_games_cache[cache_key] = game_id
            logger.debug(f"Created new game: {game_record.external_id} -> {game_id}")
            
            return game_id
            
        except Exception as e:
            logger.error(f"Error getting/creating game: {e}")
            raise

    async def _store_betting_split(
        self, 
        connection, 
        betting_record: SBDBettingSplitRecord
    ) -> None:
        """Store betting split record in staging.betting_splits."""
        try:
            await connection.execute(
                """
                INSERT INTO staging.betting_splits 
                (game_id, sportsbook_name, bet_type, public_bet_percentage, 
                 public_money_percentage, sharp_bet_percentage, total_bets, 
                 total_handle, data_source, processed_at, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,
                betting_record.game_id,
                betting_record.sportsbook_name,
                betting_record.bet_type,
                betting_record.public_bet_percentage,
                betting_record.public_money_percentage,
                betting_record.sharp_bet_percentage,
                None,  # total_bets (not available from SBD)
                None,  # total_handle (not available from SBD)
                betting_record.data_source,  # data_source ('sbd')
                datetime.now(timezone.utc),  # processed_at
                datetime.now(timezone.utc)   # created_at
            )
            
            logger.debug(
                f"Stored betting split: {betting_record.sportsbook_name} "
                f"{betting_record.bet_type} for game {betting_record.game_id}"
            )
            
        except Exception as e:
            logger.error(f"Error storing betting split: {e}")
            raise

    async def _store_sharp_action_signal(
        self, 
        connection, 
        signal: SBDSharpActionSignal
    ) -> None:
        """Store sharp action signal in staging.sharp_action_signals."""
        try:
            await connection.execute(
                """
                INSERT INTO staging.sharp_action_signals 
                (game_id, bet_type, signal_type, signal_strength, confidence_score,
                 trigger_conditions, detected_at, processed_at, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                signal.game_id,
                signal.bet_type,
                signal.signal_type,
                signal.signal_strength,
                signal.confidence_score,
                json.dumps(signal.trigger_conditions),
                signal.detected_at,
                datetime.now(timezone.utc),  # processed_at
                datetime.now(timezone.utc)   # created_at
            )
            
            logger.debug(
                f"Stored sharp action signal: {signal.bet_type} strength {signal.signal_strength} "
                f"for game {signal.game_id}"
            )
            
        except Exception as e:
            logger.error(f"Error storing sharp action signal: {e}")
            raise

    async def _mark_raw_records_processed(
        self, 
        db_connection, 
        raw_ids: List[int]
    ) -> None:
        """Mark raw records as processed."""
        try:
            async with db_connection.get_async_connection() as connection:
                await connection.execute(
                    """
                    UPDATE raw_data.sbd_betting_splits 
                    SET processed_at = $1 
                    WHERE id = ANY($2)
                    """,
                    datetime.now(timezone.utc),
                    raw_ids
                )
                
                logger.info(f"Marked {len(raw_ids)} raw SBD records as processed")
            
        except Exception as e:
            logger.error(f"Error marking raw records as processed: {e}")
            raise

    async def get_processing_stats(self) -> Dict[str, Any]:
        """Get SBD processing statistics."""
        try:
            from ...data.database.connection import get_connection
            db_connection = get_connection()
            
            async with db_connection.get_async_connection() as connection:
                # Count raw records
                raw_total = await connection.fetchval(
                    "SELECT COUNT(*) FROM raw_data.sbd_betting_splits"
                )
                raw_processed = await connection.fetchval(
                    "SELECT COUNT(*) FROM raw_data.sbd_betting_splits WHERE processed_at IS NOT NULL"
                )
                
                # Count staging records
                staging_games = await connection.fetchval(
                    "SELECT COUNT(*) FROM staging.games WHERE external_id LIKE 'sr:match:%'"
                )
                staging_splits = await connection.fetchval(
                    "SELECT COUNT(*) FROM staging.betting_splits WHERE game_id IN "
                    "(SELECT id FROM staging.games WHERE external_id LIKE 'sr:match:%')"
                )
                staging_signals = await connection.fetchval(
                    "SELECT COUNT(*) FROM staging.sharp_action_signals WHERE game_id IN "
                    "(SELECT id FROM staging.games WHERE external_id LIKE 'sr:match:%')"
                )
                
                return {
                    "raw_records_total": raw_total,
                    "raw_records_processed": raw_processed,
                    "raw_records_pending": raw_total - raw_processed,
                    "staging_games": staging_games,
                    "staging_betting_splits": staging_splits,
                    "staging_sharp_signals": staging_signals,
                    "processing_rate": f"{raw_processed}/{raw_total}" if raw_total > 0 else "0/0"
                }
            
        except Exception as e:
            logger.error(f"Error getting processing stats: {e}")
            return {"error": str(e)}

    async def process_record(self, record: DataRecord, **kwargs) -> Optional[DataRecord]:
        """Process a single record - required by base class."""
        # This is handled by the specialized process_sbd_raw_records method
        # For compatibility with the base class interface
        return None

    async def store_records(self, records: List[DataRecord]) -> None:
        """Store records - required by base class."""
        # This is handled by the specialized _store_betting_split method
        # For compatibility with the base class interface
        pass


# Usage example
async def main():
    """Example usage of SBD staging processor."""
    from .zone_interface import create_zone_config, ZoneType
    from ...core.config import get_settings
    
    settings = get_settings()
    
    # Create processor
    config = create_zone_config(ZoneType.STAGING, settings.schemas.staging)
    processor = SBDStagingProcessor(config)
    
    # Process records (limit to 10 for testing)
    result = await processor.process_sbd_raw_records(limit=10)
    
    print(f"Processing result: {result}")
    
    # Get stats
    stats = await processor.get_processing_stats()
    print(f"Processing stats: {stats}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())