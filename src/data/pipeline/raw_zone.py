"""
RAW Zone Processor

Handles ingestion and storage of raw data from external sources.
RAW Zone stores data exactly as received with minimal processing.

Reference: docs/SYSTEM_DESIGN_ANALYSIS.md
"""

import json
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from pydantic import BaseModel, Field

from ...core.logging import get_logger, LogComponent
from .base_processor import BaseZoneProcessor
from .zone_interface import (
    DataRecord,
    ProcessingStatus,
    ZoneConfig,
    ZoneType,
    ZoneFactory
)

logger = get_logger(__name__, LogComponent.CORE)


class RawDataRecord(DataRecord):
    """Raw data record with source-specific fields."""
    game_external_id: Optional[str] = None
    sportsbook_id: Optional[int] = None
    sportsbook_name: Optional[str] = None
    bet_type: Optional[str] = None
    game_date: Optional[str] = None
    endpoint_url: Optional[str] = None
    response_status: Optional[int] = None
    api_endpoint: Optional[str] = None
    data_type: Optional[str] = None
    source_feed: Optional[str] = None


class RawZoneProcessor(BaseZoneProcessor):
    """
    RAW Zone processor for ingesting external data.
    
    Responsibilities:
    - Store raw data exactly as received
    - Minimal validation (structural integrity)
    - Audit trail creation
    - Data lineage tracking
    """

    def __init__(self, config: ZoneConfig):
        super().__init__(config)
        self.table_mappings = {
            'action_network_games': 'raw_data.action_network_games',
            'action_network_odds': 'raw_data.action_network_odds',
            'sbd_betting_splits': 'raw_data.sbd_betting_splits',
            'vsin_data': 'raw_data.vsin_data',
            'mlb_stats_api': 'raw_data.mlb_stats_api',
            'betting_lines_raw': 'raw_data.betting_lines_raw',
            'moneylines_raw': 'raw_data.moneylines_raw',
            'spreads_raw': 'raw_data.spreads_raw',
            'totals_raw': 'raw_data.totals_raw',
            'line_movements_raw': 'raw_data.line_movements_raw'
        }

    async def process_record(
        self, 
        record: DataRecord, 
        **kwargs
    ) -> Optional[DataRecord]:
        """
        Process a raw data record with minimal transformation.
        
        Args:
            record: Raw data record to process
            **kwargs: Additional parameters (table_name, etc.)
            
        Returns:
            Processed record ready for storage
        """
        try:
            # Cast to RawDataRecord for additional fields
            if isinstance(record, DataRecord) and not isinstance(record, RawDataRecord):
                raw_record = RawDataRecord(**record.model_dump())
            else:
                raw_record = record

            # Set processing timestamp
            raw_record.processed_at = datetime.now(timezone.utc)
            
            # Validate JSON structure if raw_data exists
            if raw_record.raw_data:
                # Ensure raw_data is valid JSON
                if isinstance(raw_record.raw_data, str):
                    try:
                        raw_record.raw_data = json.loads(raw_record.raw_data)
                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON in raw_data for record {raw_record.external_id}")
                        return None
            
            # Extract metadata from raw_data for indexing
            if raw_record.raw_data:
                raw_record = await self._extract_metadata(raw_record)
            
            # Set validation status
            raw_record.validation_status = ProcessingStatus.COMPLETED
            
            logger.debug(f"Processed raw record: {raw_record.external_id}")
            return raw_record
            
        except Exception as e:
            logger.error(f"Error processing raw record {record.external_id}: {e}")
            return None

    async def _extract_metadata(self, record: RawDataRecord) -> RawDataRecord:
        """Extract common metadata from raw_data for indexing."""
        try:
            raw_data = record.raw_data
            if not isinstance(raw_data, dict):
                return record
            
            # Extract game information
            if 'game_id' in raw_data:
                record.game_external_id = str(raw_data['game_id'])
            elif 'gameId' in raw_data:
                record.game_external_id = str(raw_data['gameId'])
            elif 'external_game_id' in raw_data:
                record.game_external_id = str(raw_data['external_game_id'])
            
            # Extract sportsbook information
            if 'sportsbook' in raw_data:
                sportsbook = raw_data['sportsbook']
                if isinstance(sportsbook, dict):
                    record.sportsbook_name = sportsbook.get('name')
                    record.sportsbook_id = sportsbook.get('id')
                else:
                    record.sportsbook_name = str(sportsbook)
            
            # Extract bet type
            if 'bet_type' in raw_data:
                record.bet_type = raw_data['bet_type']
            elif 'betType' in raw_data:
                record.bet_type = raw_data['betType']
            elif 'market' in raw_data:
                record.bet_type = raw_data['market']
            
            # Extract game date
            if 'game_date' in raw_data:
                record.game_date = raw_data['game_date']
            elif 'gameDate' in raw_data:
                record.game_date = raw_data['gameDate']
            elif 'date' in raw_data:
                record.game_date = raw_data['date']
            
            return record
            
        except Exception as e:
            logger.error(f"Error extracting metadata: {e}")
            return record

    async def store_records(self, records: List[DataRecord]) -> None:
        """
        Store raw records to appropriate tables based on source.
        
        Args:
            records: List of processed raw records to store
        """
        try:
            db_connection = await self.get_connection()
            
            # Group records by table/source type
            records_by_table = {}
            
            for record in records:
                table_name = await self._determine_table_name(record)
                if table_name not in records_by_table:
                    records_by_table[table_name] = []
                records_by_table[table_name].append(record)
            
            # Insert records for each table
            async with db_connection.get_async_connection() as connection:
                for table_name, table_records in records_by_table.items():
                    await self._insert_records_to_table(connection, table_name, table_records)
            
            logger.info(f"Stored {len(records)} raw records across {len(records_by_table)} tables")
            
        except Exception as e:
            logger.error(f"Error storing raw records: {e}")
            raise

    async def _determine_table_name(self, record: DataRecord) -> str:
        """Determine the appropriate table name based on record source and type."""
        source = record.source.lower()
        
        # Map sources to table names
        if source == 'action_network':
            if hasattr(record, 'data_type') and record.data_type == 'odds':
                return 'raw_data.action_network_odds'
            else:
                return 'raw_data.action_network_games'
        elif source == 'sbd' or source == 'sportsbettingdime':
            return 'raw_data.sbd_betting_splits'
        elif source == 'vsin':
            return 'raw_data.vsin_data'
        elif source == 'mlb_stats_api' or source == 'mlb':
            return 'raw_data.mlb_stats_api'
        else:
            # No generic tables - all data must have a specific source
            logger.error(f"Unknown source '{source}' for record - no source-specific table available")
            raise ValueError(f"No table mapping available for source: {source}")

    async def _insert_records_to_table(
        self, 
        connection, 
        table_name: str, 
        records: List[DataRecord]
    ) -> None:
        """Insert records into specific table with appropriate columns."""
        try:
            # Generate INSERT query based on table type
            if 'action_network_games' in table_name:
                await self._insert_action_network_games(connection, records)
            elif 'action_network_odds' in table_name:
                await self._insert_action_network_odds(connection, records)
            elif 'sbd_betting_splits' in table_name:
                await self._insert_sbd_betting_splits(connection, records)
            elif 'vsin_data' in table_name:
                await self._insert_vsin_data(connection, records)
            elif 'mlb_stats_api' in table_name:
                await self._insert_mlb_stats_api(connection, records)
            else:
                logger.error(f"No insertion method available for table: {table_name}")
                raise ValueError(f"No insertion method for table: {table_name}")
                
        except Exception as e:
            logger.error(f"Error inserting records to {table_name}: {e}")
            raise

    async def _insert_action_network_games(self, connection, records: List[DataRecord]) -> None:
        """Insert Action Network game records."""
        query = """
        INSERT INTO raw_data.action_network_games 
        (external_game_id, raw_response, endpoint_url, response_status, collected_at, game_date)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (external_game_id) DO UPDATE SET
            raw_response = EXCLUDED.raw_response,
            endpoint_url = EXCLUDED.endpoint_url,
            response_status = EXCLUDED.response_status,
            collected_at = EXCLUDED.collected_at,
            game_date = EXCLUDED.game_date
        """
        
        for record in records:
            await connection.execute(
                query,
                record.external_id,
                json.dumps(record.raw_data) if record.raw_data else None,
                getattr(record, 'endpoint_url', None),
                getattr(record, 'response_status', None),
                record.processed_at or datetime.now(timezone.utc),
                getattr(record, 'game_date', None)
            )

    # Source-specific insert methods only - no generic tables
    async def _insert_sbd_betting_splits(self, connection, records: List[DataRecord]) -> None:
        """Insert SBD betting splits records."""
        query = """
        INSERT INTO raw_data.sbd_betting_splits 
        (external_matchup_id, raw_response, api_endpoint, collected_at)
        VALUES ($1, $2, $3, $4)
        """
        
        for record in records:
            await connection.execute(
                query,
                record.external_id,
                json.dumps(record.raw_data) if record.raw_data else None,
                getattr(record, 'api_endpoint', None),
                record.processed_at or datetime.now(timezone.utc)
            )

    async def validate_record_custom(self, record: DataRecord) -> bool:
        """
        RAW zone specific validation.
        
        Args:
            record: Data record to validate
            
        Returns:
            True if record passes RAW zone validation
        """
        try:
            # Check if we have either external_id or raw_data
            if not record.external_id and not record.raw_data:
                record.validation_errors = record.validation_errors or []
                record.validation_errors.append("RAW record must have either external_id or raw_data")
                return False
            
            # Validate JSON structure
            if record.raw_data:
                if isinstance(record.raw_data, str):
                    try:
                        json.loads(record.raw_data)
                    except json.JSONDecodeError:
                        record.validation_errors = record.validation_errors or []
                        record.validation_errors.append("Invalid JSON in raw_data")
                        return False
            
            return True
            
        except Exception as e:
            logger.error(f"RAW validation error: {e}")
            record.validation_errors = record.validation_errors or []
            record.validation_errors.append(f"Validation exception: {e}")
            return False

    async def promote_to_next_zone(
        self, 
        records: List[DataRecord]
    ) -> "ProcessingResult":
        """
        Promote validated RAW records to STAGING zone.
        
        Args:
            records: Records to promote to STAGING
            
        Returns:
            ProcessingResult with promotion status
        """
        try:
            # Import here to avoid circular imports
            from .staging_zone import StagingZoneProcessor
            from .zone_interface import create_zone_config, ZoneType
            
            # Create STAGING zone processor
            staging_config = create_zone_config(
                ZoneType.STAGING,
                self.settings.schemas.staging
            )
            staging_processor = StagingZoneProcessor(staging_config)
            
            # Process records in STAGING zone
            result = await staging_processor.process_batch(records)
            
            logger.info(
                f"Promoted {result.records_successful} records from RAW to STAGING"
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error promoting records to STAGING: {e}")
            from .zone_interface import ProcessingResult, ProcessingStatus
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                records_processed=len(records),
                errors=[str(e)]
            )

    async def health_check(self) -> Dict[str, Any]:
        """Get RAW zone health and status information."""
        try:
            db_connection = await self.get_connection()
            # Basic health check - verify we can connect
            connection_healthy = False
            try:
                async with db_connection.get_async_connection() as connection:
                    await connection.fetchval("SELECT 1")
                    connection_healthy = True
            except Exception:
                connection_healthy = False
                
            return {
                "zone_type": self.zone_type.value,
                "schema_name": self.schema_name,
                "status": "healthy" if connection_healthy else "unhealthy",
                "connection_status": "connected" if connection_healthy else "disconnected",
                "table_mappings": len(self.table_mappings),
                "metrics": {
                    "records_processed": self._metrics.records_processed,
                    "records_successful": self._metrics.records_successful,
                    "records_failed": self._metrics.records_failed,
                    "error_rate": self._metrics.error_rate
                }
            }
        except Exception as e:
            return {
                "zone_type": self.zone_type.value,
                "schema_name": self.schema_name,
                "status": "unhealthy",
                "error": str(e),
                "connection_status": "error"
            }


# Register the RAW zone processor
ZoneFactory.register_zone(ZoneType.RAW, RawZoneProcessor)