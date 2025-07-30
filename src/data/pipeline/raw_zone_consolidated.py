#!/usr/bin/env python3
"""
Raw Zone Consolidated Processor

Consolidated RAW zone processor that integrates collection logic directly,
eliminating the need for raw_zone_adapter.py and providing unified processing.

Key Features:
1. Direct integration with collectors (no adapter layer)
2. Source-specific table handling with proper schema mapping
3. Improved batch processing and error handling
4. Unified configuration and logging
5. Better performance through reduced indirection
"""

import json
from datetime import datetime, timezone
from typing import Any

from ...core.logging import LogComponent, get_logger
from .base_processor import BaseZoneProcessor
from .zone_interface import (
    DataRecord,
    ProcessingResult,
    ZoneConfig,
)

logger = get_logger(__name__, LogComponent.CORE)


class RawDataRecord(DataRecord):
    """Enhanced raw data record with source-specific fields."""

    game_external_id: str | None = None
    sportsbook_id: int | None = None
    sportsbook_name: str | None = None
    sportsbook_key: str | None = None
    bet_type: str | None = None
    game_date: str | None = None
    endpoint_url: str | None = None
    api_endpoint: str | None = None
    response_status: int | None = None
    data_type: str | None = None
    source_feed: str | None = None
    game_pk: int | None = None  # MLB Stats API
    season: str | None = None
    season_type: str | None = None
    home_team: str | None = None
    away_team: str | None = None
    game_datetime: datetime | None = None
    venue_id: int | None = None
    venue_name: str | None = None
    game_status: str | None = None


class RawZoneConsolidatedProcessor(BaseZoneProcessor):
    """
    Consolidated RAW zone processor with integrated collection support.

    Combines raw zone processing with direct collection integration,
    eliminating the adapter pattern and improving performance.
    """

    def __init__(self, config: ZoneConfig):
        super().__init__(config)
        self.table_mappings = self._get_table_mappings()

    def _get_table_mappings(self) -> dict[str, str]:
        """Get source-specific table mappings."""
        return {
            "action_network_games": "raw_data.action_network_games",
            "action_network_odds": "raw_data.action_network_odds",
            "action_network_history": "raw_data.action_network_history",
            "sbd_betting_splits": "raw_data.sbd_betting_splits",
            "vsin_data": "raw_data.vsin_data",
            "mlb_stats_api_games": "raw_data.mlb_stats_api_games",
        }

    async def process_record(self, record: DataRecord, **kwargs) -> DataRecord | None:
        """
        Process a raw data record with enhanced validation and metadata extraction.

        Args:
            record: Raw data record to process
            **kwargs: Additional parameters

        Returns:
            Processed record ready for storage
        """
        try:
            # Cast to enhanced RawDataRecord
            if isinstance(record, DataRecord) and not isinstance(record, RawDataRecord):
                raw_record = RawDataRecord(**record.model_dump())
            else:
                raw_record = record

            # Set processing timestamp
            raw_record.processed_at = datetime.now(timezone.utc)

            # Validate and parse JSON structure
            if raw_record.raw_data:
                raw_record = await self._process_raw_data(raw_record)

            # Extract comprehensive metadata
            raw_record = await self._extract_enhanced_metadata(raw_record)

            # Set validation status
            raw_record.validation_status = "valid"

            logger.debug(
                f"Processed raw record: {raw_record.external_id} from {raw_record.source}"
            )
            return raw_record

        except Exception as e:
            logger.error(f"Error processing raw record {record.external_id}: {e}")
            return None

    async def _process_raw_data(self, record: RawDataRecord) -> RawDataRecord:
        """Process and validate raw_data field."""
        try:
            if isinstance(record.raw_data, str):
                try:
                    record.raw_data = json.loads(record.raw_data)
                except json.JSONDecodeError as e:
                    logger.error(
                        f"Invalid JSON in raw_data for record {record.external_id}: {e}"
                    )
                    record.validation_errors = record.validation_errors or []
                    record.validation_errors.append(f"Invalid JSON: {e}")
                    record.validation_status = "invalid"

            return record

        except Exception as e:
            logger.error(f"Error processing raw_data: {e}")
            return record

    async def _extract_enhanced_metadata(self, record: RawDataRecord) -> RawDataRecord:
        """Extract comprehensive metadata based on source type."""
        try:
            if not record.raw_data or not isinstance(record.raw_data, dict):
                return record

            raw_data = record.raw_data
            source = record.source.lower()

            # Source-specific metadata extraction
            if source == "action_network":
                record = await self._extract_action_network_metadata(record, raw_data)
            elif source in ["sbd", "sportsbettingdime"]:
                record = await self._extract_sbd_metadata(record, raw_data)
            elif source == "vsin":
                record = await self._extract_vsin_metadata(record, raw_data)
            elif source in ["mlb_stats_api", "mlb"]:
                record = await self._extract_mlb_metadata(record, raw_data)

            return record

        except Exception as e:
            logger.error(f"Error extracting metadata: {e}")
            return record

    async def _extract_action_network_metadata(
        self, record: RawDataRecord, raw_data: dict
    ) -> RawDataRecord:
        """Extract Action Network specific metadata."""
        # Game information
        record.game_external_id = str(
            raw_data.get("id", raw_data.get("game_id", record.external_id))
        )

        # Sportsbook information for odds data
        if "sportsbook" in raw_data:
            sportsbook = raw_data["sportsbook"]
            if isinstance(sportsbook, dict):
                record.sportsbook_name = sportsbook.get("name")
                record.sportsbook_id = sportsbook.get("id")
            else:
                record.sportsbook_name = str(sportsbook)

        # Game date
        record.game_date = raw_data.get("game_date", raw_data.get("date"))

        # Market type for odds
        if "moneyline" in raw_data or "spread" in raw_data or "total" in raw_data:
            record.data_type = "odds"
        elif "home_team" in raw_data and "away_team" in raw_data:
            record.data_type = "game"

        return record

    async def _extract_sbd_metadata(
        self, record: RawDataRecord, raw_data: dict
    ) -> RawDataRecord:
        """Extract SportsBettingDime specific metadata."""
        record.game_external_id = raw_data.get("game_id", raw_data.get("matchup_id"))
        record.data_type = "betting_splits"

        # Extract betting information
        if "betting_splits" in raw_data:
            splits = raw_data["betting_splits"]
            if isinstance(splits, dict):
                record.bet_type = splits.get("market", "unknown")

        return record

    async def _extract_vsin_metadata(
        self, record: RawDataRecord, raw_data: dict
    ) -> RawDataRecord:
        """Extract VSIN specific metadata."""
        record.game_external_id = raw_data.get("game_id")
        record.source_feed = raw_data.get("feed_type", raw_data.get("source"))

        # Extract bet type from VSIN data
        if "bet_type" in raw_data:
            record.bet_type = raw_data["bet_type"]
        elif "market" in raw_data:
            record.bet_type = raw_data["market"]

        return record

    async def _extract_mlb_metadata(
        self, record: RawDataRecord, raw_data: dict
    ) -> RawDataRecord:
        """Extract MLB Stats API specific metadata."""
        record.game_pk = raw_data.get("gamePk")
        record.game_external_id = str(raw_data.get("gamePk", record.external_id))
        record.season = raw_data.get("season")
        record.season_type = raw_data.get("seasonType")
        record.game_date = raw_data.get("gameDate")
        record.game_status = raw_data.get("status", {}).get("statusCode")

        # Team information
        teams = raw_data.get("teams", {})
        if teams:
            home_team = teams.get("home", {})
            away_team = teams.get("away", {})
            record.home_team = home_team.get("team", {}).get("abbreviation")
            record.away_team = away_team.get("team", {}).get("abbreviation")

        # Venue information
        venue = raw_data.get("venue", {})
        if venue:
            record.venue_id = venue.get("id")
            record.venue_name = venue.get("name")

        # Game datetime
        if "gameDate" in raw_data:
            try:
                record.game_datetime = datetime.fromisoformat(
                    raw_data["gameDate"].replace("Z", "+00:00")
                )
            except (ValueError, TypeError):
                pass

        return record

    async def store_records(self, records: list[DataRecord]) -> None:
        """
        Store raw records to appropriate source-specific tables.

        Args:
            records: List of processed raw records to store
        """
        try:
            # Group records by source and table type
            records_by_table = {}

            for record in records:
                table_key = self._determine_table_key(record)
                if table_key not in records_by_table:
                    records_by_table[table_key] = []
                records_by_table[table_key].append(record)

            # Insert records for each table
            from ...data.database.connection import get_connection

            async with get_connection() as connection:
                for table_key, table_records in records_by_table.items():
                    await self._insert_records_by_source(
                        connection, table_key, table_records
                    )

            logger.info(
                f"Stored {len(records)} raw records across {len(records_by_table)} tables"
            )

        except Exception as e:
            logger.error(f"Error storing raw records: {e}")
            raise

    def _determine_table_key(self, record: DataRecord) -> str:
        """Determine the appropriate table key based on record source and type."""
        source = record.source.lower()
        data_type = getattr(record, "data_type", None)

        # Determine specific table based on source and data type
        if source == "action_network":
            if data_type == "odds":
                return "action_network_odds"
            elif data_type == "history":
                return "action_network_history"
            else:
                return "action_network_games"
        elif source in ["sbd", "sportsbettingdime"]:
            return "sbd_betting_splits"
        elif source == "vsin":
            return "vsin_data"
        elif source in ["mlb_stats_api", "mlb"]:
            return "mlb_stats_api_games"
        else:
            raise ValueError(f"Unknown source '{source}' - no table mapping available")

    async def _insert_records_by_source(
        self, connection, table_key: str, records: list[DataRecord]
    ) -> None:
        """Insert records into source-specific tables."""
        try:
            if table_key == "action_network_games":
                await self._insert_action_network_games(connection, records)
            elif table_key == "action_network_odds":
                await self._insert_action_network_odds(connection, records)
            elif table_key == "action_network_history":
                await self._insert_action_network_history(connection, records)
            elif table_key == "sbd_betting_splits":
                await self._insert_sbd_betting_splits(connection, records)
            elif table_key == "vsin_data":
                await self._insert_vsin_data(connection, records)
            elif table_key == "mlb_stats_api_games":
                await self._insert_mlb_stats_api_games(connection, records)
            else:
                raise ValueError(f"No insertion method for table key: {table_key}")

        except Exception as e:
            logger.error(f"Error inserting records to {table_key}: {e}")
            raise

    async def _insert_action_network_games(
        self, connection, records: list[DataRecord]
    ) -> None:
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
                getattr(record, "endpoint_url", None),
                getattr(record, "response_status", None),
                record.processed_at or datetime.now(timezone.utc),
                getattr(record, "game_date", None),
            )

    async def _insert_action_network_odds(
        self, connection, records: list[DataRecord]
    ) -> None:
        """Insert Action Network odds records."""
        query = """
        INSERT INTO raw_data.action_network_odds 
        (external_game_id, sportsbook_key, raw_odds, collected_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (external_game_id, sportsbook_key, collected_at) DO UPDATE SET
            raw_odds = EXCLUDED.raw_odds
        """

        for record in records:
            await connection.execute(
                query,
                getattr(record, "game_external_id", record.external_id),
                getattr(record, "sportsbook_key", "unknown"),
                json.dumps(record.raw_data) if record.raw_data else "{}",
                record.processed_at or datetime.now(timezone.utc),
            )

    async def _insert_action_network_history(
        self, connection, records: list[DataRecord]
    ) -> None:
        """Insert Action Network history records."""
        query = """
        INSERT INTO raw_data.action_network_history 
        (external_game_id, raw_history, collected_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (external_game_id, collected_at) DO UPDATE SET
            raw_history = EXCLUDED.raw_history
        """

        for record in records:
            await connection.execute(
                query,
                getattr(record, "game_external_id", record.external_id),
                json.dumps(record.raw_data) if record.raw_data else "{}",
                record.processed_at or datetime.now(timezone.utc),
            )

    async def _insert_sbd_betting_splits(
        self, connection, records: list[DataRecord]
    ) -> None:
        """Insert SBD betting splits records."""
        query = """
        INSERT INTO raw_data.sbd_betting_splits 
        (external_matchup_id, raw_response, api_endpoint, collected_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (external_matchup_id, collected_at) DO UPDATE SET
            raw_response = EXCLUDED.raw_response
        """

        for record in records:
            await connection.execute(
                query,
                getattr(record, "game_external_id", record.external_id),
                json.dumps(record.raw_data) if record.raw_data else None,
                getattr(record, "api_endpoint", None),
                record.processed_at or datetime.now(timezone.utc),
            )

    async def _insert_vsin_data(self, connection, records: list[DataRecord]) -> None:
        """Insert VSIN data records."""
        query = """
        INSERT INTO raw_data.vsin_data 
        (external_id, raw_response, collected_at, created_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (external_id, collected_at) DO UPDATE SET
            raw_response = EXCLUDED.raw_response
        """

        for record in records:
            await connection.execute(
                query,
                record.external_id,
                json.dumps(record.raw_data) if record.raw_data else "{}",
                record.processed_at or datetime.now(timezone.utc),
                record.created_at or datetime.now(timezone.utc),
            )

    async def _insert_mlb_stats_api_games(
        self, connection, records: list[DataRecord]
    ) -> None:
        """Insert MLB Stats API game records."""
        query = """
        INSERT INTO raw_data.mlb_stats_api_games 
        (external_game_id, game_pk, raw_response, endpoint_url, response_status, 
         game_date, season, season_type, home_team, away_team, game_datetime, 
         venue_id, venue_name, game_status, collected_at, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
        ON CONFLICT (external_game_id) DO UPDATE SET
            raw_response = EXCLUDED.raw_response,
            game_status = EXCLUDED.game_status,
            collected_at = EXCLUDED.collected_at
        """

        for record in records:
            await connection.execute(
                query,
                record.external_id,
                getattr(record, "game_pk", None),
                json.dumps(record.raw_data) if record.raw_data else "{}",
                getattr(record, "endpoint_url", None),
                getattr(record, "response_status", 200),
                getattr(record, "game_date", None),
                getattr(record, "season", None),
                getattr(record, "season_type", None),
                getattr(record, "home_team", None),
                getattr(record, "away_team", None),
                getattr(record, "game_datetime", None),
                getattr(record, "venue_id", None),
                getattr(record, "venue_name", None),
                getattr(record, "game_status", None),
                record.processed_at or datetime.now(timezone.utc),
                record.created_at or datetime.now(timezone.utc),
            )

    # Convenience methods for direct collection integration
    async def ingest_action_network_games(
        self,
        games_data: list[dict[str, Any]],
        source_info: dict[str, Any] | None = None,
    ) -> ProcessingResult:
        """Directly ingest Action Network game data."""
        try:
            raw_records = []

            for game_data in games_data:
                record = RawDataRecord(
                    external_id=str(game_data.get("id", game_data.get("game_id", ""))),
                    source="action_network",
                    game_external_id=str(
                        game_data.get("id", game_data.get("game_id", ""))
                    ),
                    raw_data=game_data,
                    data_type="game",
                    endpoint_url=source_info.get("endpoint_url")
                    if source_info
                    else None,
                    response_status=source_info.get("response_status", 200)
                    if source_info
                    else 200,
                    game_date=game_data.get("game_date"),
                    collected_at=datetime.now(timezone.utc),
                )
                raw_records.append(record)

            return await self.process_batch(raw_records)

        except Exception as e:
            logger.error(f"Error ingesting Action Network games: {e}")
            raise

    async def ingest_action_network_odds(
        self,
        odds_data: list[dict[str, Any]],
        game_id: str | None = None,
        source_info: dict[str, Any] | None = None,
    ) -> ProcessingResult:
        """Directly ingest Action Network odds data."""
        try:
            raw_records = []

            for odds_entry in odds_data:
                sportsbook_key = odds_entry.get(
                    "sportsbook_key", odds_entry.get("key", "")
                )

                record = RawDataRecord(
                    external_id=f"{game_id}_{sportsbook_key}_{datetime.now().isoformat()}",
                    source="action_network",
                    game_external_id=game_id,
                    sportsbook_key=sportsbook_key,
                    raw_data=odds_entry,
                    data_type="odds",
                    collected_at=datetime.now(timezone.utc),
                )
                raw_records.append(record)

            return await self.process_batch(raw_records)

        except Exception as e:
            logger.error(f"Error ingesting Action Network odds: {e}")
            raise

    async def validate_record_custom(self, record: DataRecord) -> bool:
        """
        Enhanced RAW zone validation with source-specific checks.

        Args:
            record: Data record to validate

        Returns:
            True if record passes RAW zone validation
        """
        try:
            # Basic validation
            if not record.external_id and not record.raw_data:
                record.validation_errors = record.validation_errors or []
                record.validation_errors.append(
                    "RAW record must have either external_id or raw_data"
                )
                return False

            # JSON structure validation
            if record.raw_data and isinstance(record.raw_data, str):
                try:
                    json.loads(record.raw_data)
                except json.JSONDecodeError:
                    record.validation_errors = record.validation_errors or []
                    record.validation_errors.append("Invalid JSON in raw_data")
                    return False

            # Source-specific validation
            return await self._validate_source_specific(record)

        except Exception as e:
            logger.error(f"RAW validation error: {e}")
            record.validation_errors = record.validation_errors or []
            record.validation_errors.append(f"Validation exception: {e}")
            return False

    async def _validate_source_specific(self, record: DataRecord) -> bool:
        """Perform source-specific validation."""
        source = record.source.lower()

        try:
            if source == "action_network":
                return await self._validate_action_network(record)
            elif source in ["sbd", "sportsbettingdime"]:
                return await self._validate_sbd(record)
            elif source == "vsin":
                return await self._validate_vsin(record)
            elif source in ["mlb_stats_api", "mlb"]:
                return await self._validate_mlb(record)

            return True  # Unknown sources pass basic validation

        except Exception as e:
            logger.error(f"Source-specific validation error for {source}: {e}")
            return False

    async def _validate_action_network(self, record: DataRecord) -> bool:
        """Validate Action Network specific requirements."""
        if record.raw_data and isinstance(record.raw_data, dict):
            raw_data = record.raw_data

            # Check for required fields based on data type
            data_type = getattr(record, "data_type", None)

            if data_type == "game":
                required_fields = ["id", "home_team", "away_team"]
                for field in required_fields:
                    if field not in raw_data:
                        record.validation_errors = record.validation_errors or []
                        record.validation_errors.append(
                            f"Missing required field: {field}"
                        )
                        return False
            elif data_type == "odds":
                # Check for at least one market type
                market_types = ["moneyline", "spread", "total"]
                if not any(market in raw_data for market in market_types):
                    record.validation_errors = record.validation_errors or []
                    record.validation_errors.append(
                        "Missing market data (moneyline, spread, or total)"
                    )
                    return False

        return True

    async def _validate_sbd(self, record: DataRecord) -> bool:
        """Validate SBD specific requirements."""
        # SBD validation logic here
        return True

    async def _validate_vsin(self, record: DataRecord) -> bool:
        """Validate VSIN specific requirements."""
        # VSIN validation logic here
        return True

    async def _validate_mlb(self, record: DataRecord) -> bool:
        """Validate MLB Stats API specific requirements."""
        if record.raw_data and isinstance(record.raw_data, dict):
            raw_data = record.raw_data

            # Check for required MLB fields
            if "gamePk" not in raw_data:
                record.validation_errors = record.validation_errors or []
                record.validation_errors.append("Missing required field: gamePk")
                return False

        return True

    async def health_check(self) -> dict[str, Any]:
        """Enhanced health check with source-specific status."""
        try:
            connection_healthy = False
            table_status = {}

            try:
                from ...data.database.connection import get_connection

                async with get_connection() as connection:
                    await connection.fetchval("SELECT 1")
                    connection_healthy = True

                    # Check each source table
                    for table_key, table_name in self.table_mappings.items():
                        try:
                            count = await connection.fetchval(
                                f"SELECT COUNT(*) FROM {table_name}"
                            )
                            table_status[table_key] = {
                                "status": "healthy",
                                "record_count": count,
                            }
                        except Exception as e:
                            table_status[table_key] = {
                                "status": "error",
                                "error": str(e),
                            }

            except Exception:
                connection_healthy = False

            return {
                "zone_type": self.zone_type.value,
                "schema_name": self.schema_name,
                "status": "healthy" if connection_healthy else "unhealthy",
                "connection_status": "connected"
                if connection_healthy
                else "disconnected",
                "table_mappings": len(self.table_mappings),
                "table_status": table_status,
                "metrics": {
                    "records_processed": self._metrics.records_processed,
                    "records_successful": self._metrics.records_successful,
                    "records_failed": self._metrics.records_failed,
                    "error_rate": self._metrics.error_rate,
                },
            }
        except Exception as e:
            return {
                "zone_type": self.zone_type.value,
                "schema_name": self.schema_name,
                "status": "unhealthy",
                "error": str(e),
                "connection_status": "error",
            }
