"""
STAGING Zone Processor

Handles data cleaning, normalization, and validation from RAW zone.
STAGING Zone prepares data for analysis with quality control.

Reference: docs/SYSTEM_DESIGN_ANALYSIS.md
"""

import json
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from ...core.logging import LogComponent, get_logger
from ...core.team_utils import normalize_team_name
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


class StagingDataRecord(DataRecord):
    """Staging data record with normalized fields."""

    game_id: int | None = None
    sportsbook_id: int | None = None
    sportsbook_name: str | None = None
    home_team_normalized: str | None = None
    away_team_normalized: str | None = None
    team_normalized: str | None = None
    bet_type: str | None = None
    line_value: Decimal | None = None
    odds_american: int | None = None
    team_type: str | None = None  # 'home', 'away', 'over', 'under'
    data_completeness_score: float | None = None
    data_accuracy_score: float | None = None
    data_consistency_score: float | None = None


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
            "draftkings": "DraftKings",
            "fanduel": "FanDuel",
            "betmgm": "BetMGM",
            "caesars": "Caesars",
            "bet365": "Bet365",
            "fanatics": "Fanatics",
            "pinnacle": "Pinnacle",
            "circa": "Circa Sports",
            "westgate": "Westgate",
            "pointsbet": "PointsBet",
        }

    async def process_record(self, record: DataRecord, **kwargs) -> DataRecord | None:
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

            # Clean and normalize based on raw data (handles both raw_data and raw_odds)
            if staging_record.raw_data or hasattr(record, 'raw_odds'):
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
                logger.warning(
                    f"Data consistency issues in record {staging_record.external_id}"
                )

            # Calculate quality scores
            staging_record.data_completeness_score = (
                await self._calculate_completeness_score(staging_record)
            )
            staging_record.data_accuracy_score = await self._calculate_accuracy_score(
                staging_record
            )
            staging_record.data_consistency_score = (
                await self._calculate_consistency_score(staging_record)
            )

            # Overall quality score
            staging_record.quality_score = (
                staging_record.data_completeness_score * 0.4
                + staging_record.data_accuracy_score * 0.3
                + staging_record.data_consistency_score * 0.3
            )

            # Set processing timestamp
            staging_record.processed_at = datetime.now(timezone.utc)
            # Map ProcessingStatus to database validation status values
            staging_record.validation_status = "valid"  # Use database-compatible status

            logger.debug(
                f"Processed staging record: {staging_record.external_id} (quality: {staging_record.quality_score:.2f})"
            )
            return staging_record

        except Exception as e:
            logger.error(f"Error processing staging record {record.external_id}: {e}")
            return None

    async def process_record_multi_bet_types(self, record: DataRecord, **kwargs) -> list[DataRecord]:
        """
        Process a single RAW record into multiple STAGING records (one per bet type).
        
        This is specifically designed for Action Network data where one raw record
        contains multiple bet types (moneyline, spread, total).

        Args:
            record: RAW data record to process
            **kwargs: Additional processing parameters

        Returns:
            List of cleaned and normalized staging records (one per bet type)
        """
        try:
            staging_records = []
            
            # Get raw odds data (Action Network format) - now stored in raw_data field
            raw_odds_data = getattr(record, 'raw_data', None)
            if not raw_odds_data:
                logger.warning(f"No raw odds data found in record {getattr(record, 'external_id', 'unknown')}")
                return []

            # Action Network format: bet types are at the top level of raw_odds_data
            if isinstance(raw_odds_data, dict):
                # Process each bet type separately - Action Network puts them at root level
                for bet_type in ['moneyline', 'spread', 'total']:
                    if bet_type in raw_odds_data:
                        bet_data = raw_odds_data[bet_type]
                        if isinstance(bet_data, list) and bet_data:
                            # Create separate staging records for each bet within the type
                            for bet_entry in bet_data:
                                staging_record = await self._create_staging_record_from_bet(record, bet_entry, bet_type)
                                if staging_record:
                                    staging_records.append(staging_record)
                                    logger.debug(f"Created {bet_type} staging record for sportsbook {bet_entry.get('book_id', 'unknown')}")
                
                # Also check for legacy event_markets format (backward compatibility)
                if 'event_markets' in raw_odds_data:
                    event_markets = raw_odds_data['event_markets']
                    for bet_type in ['moneyline', 'spread', 'total']:
                        if bet_type in event_markets:
                            bet_data = event_markets[bet_type]
                            if isinstance(bet_data, list) and bet_data:
                                for bet_entry in bet_data:
                                    staging_record = await self._create_staging_record_from_bet(record, bet_entry, bet_type)
                                    if staging_record:
                                        staging_records.append(staging_record)

            logger.debug(f"Generated {len(staging_records)} staging records from raw record {getattr(record, 'external_id', None) or 'unknown'}")
            return staging_records

        except Exception as e:
            logger.error(f"Error processing multi-bet record {getattr(record, 'external_id', None) or 'unknown'}: {e}")
            return []

    async def _create_staging_record_from_bet(self, raw_record: DataRecord, bet_entry: dict, bet_type: str) -> StagingDataRecord | None:
        """Create a staging record from a specific bet entry."""
        try:
            # Create base staging record
            staging_record = StagingDataRecord(**raw_record.model_dump())
            
            # Set bet-specific data
            staging_record.bet_type = bet_type
            staging_record.odds_american = bet_entry.get("odds")
            staging_record.team_type = bet_entry.get("side", "unknown")
            staging_record.sportsbook_id = bet_entry.get("book_id")
            
            # Set line value for spread and total bets
            if bet_type in ["spread", "total"]:
                staging_record.line_value = self._safe_decimal_convert(bet_entry.get("value"))
            
            # Set game ID from event_id
            if "event_id" in bet_entry:
                staging_record.game_id = bet_entry["event_id"]
            
            # Copy raw data for processing
            staging_record.raw_data = {"bet_entry": bet_entry, "bet_type": bet_type}
            
            # Apply all standard processing
            staging_record = await self._normalize_team_names(staging_record)
            staging_record = await self._normalize_sportsbook_names(staging_record)
            staging_record = await self._clean_numeric_fields(staging_record)
            
            # Calculate quality scores
            staging_record.data_completeness_score = await self._calculate_completeness_score(staging_record)
            staging_record.data_accuracy_score = await self._calculate_accuracy_score(staging_record)
            staging_record.data_consistency_score = await self._calculate_consistency_score(staging_record)
            
            # Overall quality score
            staging_record.quality_score = (
                staging_record.data_completeness_score * 0.4
                + staging_record.data_accuracy_score * 0.3
                + staging_record.data_consistency_score * 0.3
            )
            
            # Set processing timestamp
            staging_record.processed_at = datetime.now(timezone.utc)
            # Map ProcessingStatus to database validation status values
            staging_record.validation_status = "valid"  # Use database-compatible status
            
            return staging_record
            
        except Exception as e:
            logger.error(f"Error creating staging record from bet entry: {e}")
            return None

    async def _normalize_from_raw_data(
        self, record: StagingDataRecord
    ) -> StagingDataRecord:
        """Extract and normalize fields from raw_data JSON (Action Network event_markets format)."""
        try:
            # Handle raw_data field (contains parsed JSON from Action Network)
            raw_data = record.raw_data
            if not isinstance(raw_data, dict):
                return record

            # Handle Action Network format (bet types at root level)
            # Extract sportsbook info from any bet type data
            for bet_type in ["spread", "moneyline", "total"]:
                if bet_type in raw_data and isinstance(raw_data[bet_type], list) and raw_data[bet_type]:
                    first_bet = raw_data[bet_type][0]
                    if "book_id" in first_bet:
                        record.sportsbook_id = first_bet["book_id"]
                    break
            
            # Process each bet type (taking first entry for single-record processing)
            # Process spreads
            if "spread" in raw_data and isinstance(raw_data["spread"], list) and raw_data["spread"]:
                spread_entry = raw_data["spread"][0]
                record.bet_type = "spread"
                record.odds_american = spread_entry.get("odds")
                record.line_value = self._safe_decimal_convert(spread_entry.get("value"))
                record.team_type = spread_entry.get("side", "unknown")
                return record
            
            # Process moneylines
            if "moneyline" in raw_data and isinstance(raw_data["moneyline"], list) and raw_data["moneyline"]:
                ml_entry = raw_data["moneyline"][0]
                record.bet_type = "moneyline"
                record.odds_american = ml_entry.get("odds")
                record.team_type = ml_entry.get("side", "unknown")
                return record
            
            # Process totals
            if "total" in raw_data and isinstance(raw_data["total"], list) and raw_data["total"]:
                total_entry = raw_data["total"][0]
                record.bet_type = "total"
                record.odds_american = total_entry.get("odds")
                record.line_value = self._safe_decimal_convert(total_entry.get("value"))
                record.team_type = total_entry.get("side", "unknown")  # over/under
                return record

            # Handle legacy Action Network event_markets format (backward compatibility)
            if "event_markets" in raw_data:
                event_markets = raw_data["event_markets"]
                
                # Extract sportsbook info from any market
                for market_type, market_data in event_markets.items():
                    if isinstance(market_data, list) and market_data:
                        first_market = market_data[0]
                        if "book_id" in first_market:
                            record.sportsbook_id = first_market["book_id"]
                        break
                
                # Process spreads
                if "spread" in event_markets:
                    spread_data = event_markets["spread"]
                    if isinstance(spread_data, list) and spread_data:
                        spread_entry = spread_data[0]
                        record.bet_type = "spread"
                        record.odds_american = spread_entry.get("odds")
                        record.line_value = self._safe_decimal_convert(spread_entry.get("value"))
                        record.team_type = spread_entry.get("side", "unknown")
                        return record
                
                # Process moneylines
                if "moneyline" in event_markets:
                    ml_data = event_markets["moneyline"]
                    if isinstance(ml_data, list) and ml_data:
                        ml_entry = ml_data[0]
                        record.bet_type = "moneyline"
                        record.odds_american = ml_entry.get("odds")
                        record.team_type = ml_entry.get("side", "unknown")
                        return record
                
                # Process totals
                if "total" in event_markets:
                    total_data = event_markets["total"]
                    if isinstance(total_data, list) and total_data:
                        total_entry = total_data[0]
                        record.bet_type = "total"
                        record.odds_american = total_entry.get("odds")
                        record.line_value = self._safe_decimal_convert(total_entry.get("value"))
                        record.team_type = total_entry.get("side", "unknown")  # over/under
                        return record

            # Legacy format handling (keep for backwards compatibility)
            # Extract game information
            if "game" in raw_data:
                game_data = raw_data["game"]
                if "home_team" in game_data:
                    record.home_team_normalized = str(game_data["home_team"])
                if "away_team" in game_data:
                    record.away_team_normalized = str(game_data["away_team"])

            # Extract team names from various formats
            for field in ["home_team", "away_team", "homeTeam", "awayTeam"]:
                if field in raw_data:
                    if "home" in field.lower():
                        record.home_team_normalized = str(raw_data[field])
                    else:
                        record.away_team_normalized = str(raw_data[field])

            # Extract sportsbook information (legacy)
            if "sportsbook" in raw_data:
                sportsbook = raw_data["sportsbook"]
                if isinstance(sportsbook, dict):
                    record.sportsbook_name = sportsbook.get(
                        "name", sportsbook.get("key")
                    )
                    record.sportsbook_id = sportsbook.get("id")
                else:
                    record.sportsbook_name = str(sportsbook)

            # Extract betting line information (legacy)
            if "outcomes" in raw_data:
                outcomes = raw_data["outcomes"]
                if isinstance(outcomes, list) and outcomes:
                    first_outcome = outcomes[0]
                    record.odds_american = first_outcome.get("price")
                    record.line_value = self._safe_decimal_convert(
                        first_outcome.get("point")
                    )
                    
                    if not record.bet_type:
                        if first_outcome.get("point") is not None:
                            record.bet_type = "spread"
                        else:
                            record.bet_type = "moneyline"

            # Final bet type fallback logic
            if not record.bet_type:
                if record.line_value is not None:
                    record.bet_type = "spread"
                elif record.odds_american is not None:
                    record.bet_type = "moneyline"
                else:
                    record.bet_type = "generic"

            return record

        except Exception as e:
            logger.error(f"Error normalizing raw data: {e}")
            return record

    async def _normalize_team_names(
        self, record: StagingDataRecord
    ) -> StagingDataRecord:
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

    async def _normalize_sportsbook_names(
        self, record: StagingDataRecord
    ) -> StagingDataRecord:
        """Normalize sportsbook names."""
        try:
            if record.sportsbook_name:
                sportsbook_key = (
                    record.sportsbook_name.lower().replace(" ", "").replace("-", "")
                )

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

    async def _clean_numeric_fields(
        self, record: StagingDataRecord
    ) -> StagingDataRecord:
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

    def _safe_int_convert(self, value: Any) -> int | None:
        """Safely convert value to integer."""
        try:
            if value is None:
                return None
            if isinstance(value, int):
                return value
            if isinstance(value, str):
                # Remove non-numeric characters except +/-
                cleaned = re.sub(r"[^\d\-\+]", "", value)
                if cleaned:
                    return int(cleaned)
            if isinstance(value, (float, Decimal)):
                return int(value)
            return None
        except (ValueError, TypeError):
            return None

    def _safe_decimal_convert(self, value: Any) -> Decimal | None:
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
                cleaned = re.sub(r"[^\d\.\-\+]", "", value)
                if cleaned and cleaned not in [".", "-", "+"]:
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
            if record.line_value and record.bet_type == "spread":
                if abs(record.line_value) > 50:
                    issues.append(f"Unusual spread: {record.line_value}")

            # Check if total values are reasonable
            if record.line_value and record.bet_type == "total":
                if record.line_value < 0 or record.line_value > 50:
                    issues.append(f"Unusual total: {record.line_value}")

            # Check team name consistency
            if (
                record.home_team_normalized
                and record.away_team_normalized
                and record.home_team_normalized == record.away_team_normalized
            ):
                issues.append("Home and away teams are identical")

            if issues:
                record.validation_errors = (record.validation_errors or []) + issues
                return False

            return True

        except Exception as e:
            logger.error(f"Error validating data consistency: {e}")
            return False

    async def store_records(self, records: list[DataRecord]) -> None:
        """
        Store processed staging records to appropriate staging tables.

        Args:
            records: List of processed staging records to store
        """
        try:
            # Group records by bet type with enhanced classification
            records_by_type = {
                "moneyline": [],
                "spread": [],
                "total": [],
                "generic": [],
            }

            for record in records:
                bet_type = getattr(record, "bet_type", "generic")
                
                # Normalize bet_type variations
                if bet_type in ["moneyline", "ml", "money_line"]:
                    records_by_type["moneyline"].append(record)
                elif bet_type in ["spread", "point_spread", "ps", "handicap"]:
                    records_by_type["spread"].append(record)
                elif bet_type in ["total", "totals", "over_under", "ou", "total_points"]:
                    records_by_type["total"].append(record)
                else:
                    logger.debug(f"Record with unknown bet_type '{bet_type}' routed to generic table")
                    records_by_type["generic"].append(record)

            # Log record distribution before insertion
            for bet_type, type_records in records_by_type.items():
                if type_records:
                    logger.info(f"Will store {len(type_records)} records as '{bet_type}' type")

            # Use centralized connection management like other zone processors
            from ...data.database.connection import get_connection
            
            total_stored = 0
            async with get_connection() as connection:
                async with connection.transaction():
                    for bet_type, type_records in records_by_type.items():
                        if not type_records:
                            continue

                        try:
                            logger.info(f"Storing {len(type_records)} records of type '{bet_type}' to staging.{bet_type}s table")
                            
                            if bet_type == "moneyline":
                                await self._insert_moneylines(connection, type_records)
                            elif bet_type == "spread":
                                await self._insert_spreads(connection, type_records)
                            elif bet_type == "total":
                                await self._insert_totals(connection, type_records)
                            else:
                                await self._insert_betting_lines(connection, type_records)
                            
                            total_stored += len(type_records)
                            logger.info(f"Successfully stored {len(type_records)} {bet_type} records to database")
                            
                        except Exception as e:
                            logger.error(f"Failed to store {len(type_records)} {bet_type} records: {e}")
                            raise  # Transaction will rollback

            logger.info(f"Successfully stored {total_stored} staging records across {len([k for k, v in records_by_type.items() if v])} table types")

        except Exception as e:
            logger.error(f"Error storing staging records: {e}")
            raise

    async def _insert_moneylines(self, connection, records: list[DataRecord]) -> None:
        """Insert moneyline records to staging.moneylines."""
        query = """
        INSERT INTO staging.moneylines 
        (raw_moneylines_id, game_id, sportsbook_id, sportsbook_name, home_odds, away_odds,
         home_team_normalized, away_team_normalized, data_quality_score, 
         validation_status, validation_errors, processed_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        """

        for record in records:
            try:
                await connection.execute(
                    query,
                    getattr(record, "id", None),  # raw_moneylines_id
                    str(getattr(record, "game_id", None)) if getattr(record, "game_id", None) is not None else None,
                    getattr(record, "sportsbook_id", None),
                    getattr(record, "sportsbook_name", None),
                    getattr(record, "odds_american", None)
                    if getattr(record, "team_type", None) == "home"
                    else None,
                    getattr(record, "odds_american", None)
                    if getattr(record, "team_type", None) == "away"
                    else None,
                    getattr(record, "home_team_normalized", None),
                    getattr(record, "away_team_normalized", None),
                    getattr(record, "quality_score", 1.0),  # Use calculated quality_score field
                    getattr(record, 'validation_status', 'pending')
                    if isinstance(getattr(record, 'validation_status', None), str)
                    else "pending",
                    json.dumps(record.validation_errors)
                    if record.validation_errors
                    else None,
                    record.processed_at,
                )
            except Exception as e:
                logger.error(f"Error inserting moneyline record {getattr(record, 'id', 'unknown')}: {e}")
                logger.error(f"Record details: game_id={getattr(record, 'game_id', 'None')}, sportsbook_id={getattr(record, 'sportsbook_id', 'None')}, team_type={getattr(record, 'team_type', 'None')}")
                raise

    async def _insert_spreads(self, connection, records: list[DataRecord]) -> None:
        """Insert spread records to staging.spreads."""
        query = """
        INSERT INTO staging.spreads 
        (raw_spreads_id, game_id, sportsbook_id, sportsbook_name, line_value, home_odds, away_odds,
         home_team_normalized, away_team_normalized, data_quality_score,
         validation_status, validation_errors, processed_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        """

        for record in records:
            try:
                await connection.execute(
                    query,
                    getattr(record, "id", None),  # raw_spreads_id
                    str(getattr(record, "game_id", None)) if getattr(record, "game_id", None) is not None else None,
                    getattr(record, "sportsbook_id", None),
                    getattr(record, "sportsbook_name", None),
                    getattr(record, "line_value", None),
                    getattr(record, "odds_american", None)
                    if getattr(record, "team_type", None) == "home"
                    else None,  # home_odds
                    getattr(record, "odds_american", None)
                    if getattr(record, "team_type", None) == "away"
                    else None,  # away_odds
                    getattr(record, "home_team_normalized", None),
                    getattr(record, "away_team_normalized", None),
                    getattr(record, "quality_score", 1.0),  # Use calculated quality_score field
                    getattr(record, 'validation_status', 'pending')
                    if isinstance(getattr(record, 'validation_status', None), str)
                    else "pending",
                    json.dumps(record.validation_errors)
                    if record.validation_errors
                    else None,
                    record.processed_at,
                )
            except Exception as e:
                logger.error(f"Error inserting spread record {getattr(record, 'id', 'unknown')}: {e}")
                logger.error(f"Record details: game_id={getattr(record, 'game_id', 'None')}, line_value={getattr(record, 'line_value', 'None')}, team_type={getattr(record, 'team_type', 'None')}")
                raise

    async def _insert_totals(self, connection, records: list[DataRecord]) -> None:
        """Insert total records to staging.totals."""
        query = """
        INSERT INTO staging.totals 
        (raw_totals_id, game_id, sportsbook_id, sportsbook_name, line_value, over_odds, under_odds,
         data_quality_score, validation_status, validation_errors, processed_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        """

        for record in records:
            try:
                await connection.execute(
                    query,
                    getattr(record, "id", None),  # raw_totals_id
                    str(getattr(record, "game_id", None)) if getattr(record, "game_id", None) is not None else None,
                    getattr(record, "sportsbook_id", None),
                    getattr(record, "sportsbook_name", None),
                    getattr(record, "line_value", None),
                    getattr(record, "odds_american", None)
                    if getattr(record, "team_type", None) == "over"
                    else None,
                    getattr(record, "odds_american", None)
                    if getattr(record, "team_type", None) == "under"
                    else None,
                    getattr(record, "quality_score", 1.0),  # Use calculated quality_score field
                    getattr(record, 'validation_status', 'pending')
                    if isinstance(getattr(record, 'validation_status', None), str)
                    else "pending",
                    json.dumps(record.validation_errors)
                    if record.validation_errors
                    else None,
                    record.processed_at,
                )
            except Exception as e:
                logger.error(f"Error inserting totals record {getattr(record, 'id', 'unknown')}: {e}")
                logger.error(f"Record details: game_id={getattr(record, 'game_id', 'None')}, line_value={getattr(record, 'line_value', 'None')}, team_type={getattr(record, 'team_type', 'None')}")
                raise

    async def _insert_betting_lines(
        self, connection, records: list[DataRecord]
    ) -> None:
        """Insert generic betting lines to staging.betting_lines."""
        query = """
        INSERT INTO staging.betting_lines 
        (raw_betting_lines_id, game_id, sportsbook_id, bet_type, line_value,
         odds_american, team_type, team_normalized, data_quality_score,
         validation_status, processed_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        """

        for record in records:
            try:
                await connection.execute(
                    query,
                    getattr(record, "id", None),  # raw_betting_lines_id
                    str(getattr(record, "game_id", None)) if getattr(record, "game_id", None) is not None else None,
                    getattr(record, "sportsbook_id", None),
                    getattr(record, "bet_type", None),
                    getattr(record, "line_value", None),
                    getattr(record, "odds_american", None),
                    getattr(record, "team_type", None),
                    getattr(record, "team_normalized", None),
                    getattr(record, "quality_score", 1.0),  # Use calculated quality_score field
                    getattr(record, 'validation_status', 'pending')
                    if isinstance(getattr(record, 'validation_status', None), str)
                    else "pending",
                    record.processed_at,
                )
            except Exception as e:
                logger.error(f"Error inserting betting line record {getattr(record, 'id', 'unknown')}: {e}")
                raise

    async def promote_to_next_zone(self, records: list[DataRecord]) -> ProcessingResult:
        """
        Promote validated STAGING records to CURATED zone.

        Args:
            records: Records to promote to CURATED

        Returns:
            ProcessingResult with promotion status
        """
        try:
            # TEMPORARY: Hardcode disable CURATED zone until RAW/STAGING are stable
            # TODO: Fix config.toml loading issue for pipeline settings
            CURATED_ZONE_DISABLED = True  # Temporary hardcode

            if (
                CURATED_ZONE_DISABLED
                or not self.settings.pipeline.zones.curated_enabled
            ):
                logger.info(
                    f"CURATED zone disabled - skipping promotion of {len(records)} records"
                )
                from .zone_interface import ProcessingResult, ProcessingStatus

                return ProcessingResult(
                    status=ProcessingStatus.SKIPPED,
                    records_processed=len(records),
                    records_successful=len(records),
                    records_failed=0,
                    metadata={"reason": "curated_zone_disabled_hardcoded"},
                )

            # Import here to avoid circular imports
            from .curated_zone import CuratedZoneProcessor
            from .zone_interface import ZoneType, create_zone_config

            # Create CURATED zone processor
            curated_config = create_zone_config(
                ZoneType.CURATED, self.settings.schemas.curated
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
                errors=[str(e)],
            )


# Register the STAGING zone processor
# DEPRECATED: Replaced by UnifiedStagingProcessor
# ZoneFactory.register_zone(ZoneType.STAGING, StagingZoneProcessor)
