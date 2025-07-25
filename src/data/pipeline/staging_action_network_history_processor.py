#!/usr/bin/env python3
"""
Action Network History Processor - Complete Line Movement Timeline

Processes raw Action Network history data from raw_data.action_network_history table
to extract complete line movement history with exact timestamps.

Key Features:
1. Processes history arrays with exact updated_at timestamps
2. Creates individual records for each line change
3. Captures opening lines, normal updates, and status changes
4. Matches closest timestamps between over/under and home/away
5. Enables complete temporal betting analysis

Data Source: raw_data.action_network_history table with JSONB raw_history column
Output: staging.action_network_odds_historical with complete temporal records
"""

import asyncio
import json
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

import asyncpg
from pydantic import BaseModel, Field, field_validator

from ...core.config import get_settings
from ...core.datetime_utils import now_est
from ...core.logging import LogComponent, get_logger
from ...core.sportsbook_utils import SportsbookResolver
from ...services.mlb_stats_api_game_resolution_service import (
    DataSource,
    MLBStatsAPIGameResolutionService,
)

logger = get_logger(__name__, LogComponent.CORE)


class HistoricalOddsRecord(BaseModel):
    """Single historical odds record with exact timestamp."""

    # Game and sportsbook identifiers
    external_game_id: str
    mlb_stats_api_game_id: str | None = None
    sportsbook_external_id: str
    sportsbook_id: int | None = None
    sportsbook_name: str | None = None

    # Market and side identification
    market_type: str  # moneyline, spread, total
    side: str  # home, away, over, under

    # Odds data
    odds: int
    line_value: Decimal | None = None

    # Critical timing information
    updated_at: datetime  # From JSON history.updated_at
    data_collection_time: datetime | None = None
    data_processing_time: datetime | None = None

    # Line status and metadata
    line_status: str | None = "normal"  # opener, normal, suspended
    is_current_odds: bool = False  # TRUE if latest odds

    # Action Network metadata
    market_id: int | None = None
    outcome_id: int | None = None
    period: str = "event"

    # Data quality
    data_quality_score: float = Field(ge=0.0, le=1.0, default=1.0)
    validation_status: str = "valid"

    # Lineage
    raw_data_id: int

    @field_validator("market_type")
    @classmethod
    def validate_market_type(cls, v):
        if v not in ["moneyline", "spread", "total"]:
            raise ValueError(f"Invalid market_type: {v}")
        return v

    @field_validator("side")
    @classmethod
    def validate_side(cls, v):
        if v not in ["home", "away", "over", "under"]:
            raise ValueError(f"Invalid side: {v}")
        return v


class ActionNetworkHistoryProcessor:
    """
    Processor for extracting complete historical odds data from history table.

    Processes raw_data.action_network_history to extract every historical odds point
    with exact updated_at timestamps, creating complete temporal records.
    """

    def __init__(self):
        self.settings = get_settings()
        self.sportsbook_resolver = SportsbookResolver(self._get_db_config())
        self.mlb_resolver = MLBStatsAPIGameResolutionService()
        self.processing_batch_id = str(uuid4())

    def _get_db_config(self) -> dict[str, Any]:
        """Get database configuration from centralized settings."""
        settings = get_settings()
        return {
            "host": settings.database.host,
            "port": settings.database.port,
            "database": settings.database.database,
            "user": settings.database.user,
            "password": settings.database.password,
        }

    async def initialize(self):
        """Initialize processor services."""
        await self.mlb_resolver.initialize()
        logger.info(
            "ActionNetworkHistoryProcessor initialized with MLB Stats API integration"
        )

    async def cleanup(self):
        """Cleanup processor resources."""
        await self.mlb_resolver.cleanup()

    async def process_history_data(self, limit: int = 10) -> dict[str, Any]:
        """
        Process raw history data to extract complete historical records with timestamps.

        This method:
        1. Retrieves unprocessed raw history data from action_network_history table
        2. Extracts history arrays with exact updated_at timestamps
        3. Creates separate records for each historical point
        4. Resolves MLB Stats API game IDs
        5. Handles both historical and current odds
        """
        logger.info(
            "Starting history data processing",
            batch_id=self.processing_batch_id,
            limit=limit,
        )

        try:
            conn = await asyncpg.connect(**self._get_db_config())

            # Get unprocessed OR updated raw history data
            raw_history_data = await conn.fetch(
                """
                SELECT id, external_game_id, raw_history, collected_at
                FROM raw_data.action_network_history 
                WHERE id NOT IN (
                    SELECT DISTINCT raw_data_id 
                    FROM staging.action_network_odds_historical
                    WHERE raw_data_id IS NOT NULL
                )
                OR id IN (
                    -- Also include records where raw data is newer than staging processing
                    SELECT rh.id
                    FROM raw_data.action_network_history rh
                    JOIN staging.action_network_odds_historical oh ON oh.raw_data_id = rh.id
                    WHERE rh.collected_at > oh.data_collection_time
                    GROUP BY rh.id
                )
                ORDER BY collected_at DESC
                LIMIT $1
            """,
                limit,
            )

            if not raw_history_data:
                logger.info("No unprocessed history data found")
                return {
                    "historical_records_processed": 0,
                    "historical_records_valid": 0,
                    "mlb_games_resolved": 0,
                }

            logger.info(f"Found {len(raw_history_data)} unprocessed history records")

            processed_count = 0
            valid_count = 0
            mlb_resolved_count = 0

            for raw_history_record in raw_history_data:
                try:
                    # Check if this is an update (raw data newer than existing staging data)
                    existing_staging_count = await conn.fetchval(
                        """
                        SELECT COUNT(*) 
                        FROM staging.action_network_odds_historical 
                        WHERE raw_data_id = $1
                    """,
                        raw_history_record["id"],
                    )

                    if existing_staging_count > 0:
                        # This is an update - delete existing staging records for this raw_data_id
                        deleted_count = await conn.fetchval(
                            """
                            DELETE FROM staging.action_network_odds_historical 
                            WHERE raw_data_id = $1
                        """,
                            raw_history_record["id"],
                        )
                        logger.info(
                            f"Updated raw data detected for game {raw_history_record['external_game_id']}: deleted {deleted_count} old staging records"
                        )

                    # Extract all historical records from this raw history record
                    historical_records = (
                        await self._extract_historical_records_from_history(
                            raw_history_record, conn
                        )
                    )

                    for historical_record in historical_records:
                        # Enhanced MLB ID resolution using multiple strategies
                        mlb_game_id = await self._resolve_mlb_game_id_enhanced(
                            historical_record, conn
                        )
                        if mlb_game_id:
                            historical_record.mlb_stats_api_game_id = mlb_game_id
                            mlb_resolved_count += 1
                        else:
                            logger.warning(
                                f"Could not resolve MLB game ID for Action Network game {historical_record.external_game_id}"
                            )

                        # Insert historical record
                        await self._insert_historical_odds_record(
                            historical_record, conn
                        )
                        processed_count += 1

                        if historical_record.validation_status == "valid":
                            valid_count += 1

                except Exception as e:
                    logger.error(
                        f"Error processing history record {raw_history_record['id']}: {e}"
                    )
                    continue

            await conn.close()

            result = {
                "historical_records_processed": processed_count,
                "historical_records_valid": valid_count,
                "mlb_games_resolved": mlb_resolved_count,
                "processing_batch_id": self.processing_batch_id,
                "structure_type": "Historical - complete temporal records from history table",
            }

            logger.info("History data processing completed", **result)
            return result

        except Exception as e:
            logger.error(f"Error in history data processing: {e}")
            raise

    async def _extract_historical_records_from_history(
        self, raw_history: dict, conn: asyncpg.Connection
    ) -> list[HistoricalOddsRecord]:
        """Extract all historical records from raw history JSON data."""
        records = []

        try:
            # Parse raw history data
            raw_history_data = raw_history["raw_history"]
            if isinstance(raw_history_data, str):
                history_data = json.loads(raw_history_data)
            else:
                history_data = raw_history_data

            external_game_id = raw_history["external_game_id"]
            collection_time = raw_history["collected_at"]

            # Process each sportsbook in the history data
            # Format: {"15": {"event": {"total": [...], "spread": [...], "moneyline": [...]}}}
            for sportsbook_key, sportsbook_data in history_data.items():
                if (
                    not isinstance(sportsbook_data, dict)
                    or "event" not in sportsbook_data
                ):
                    continue

                # Resolve sportsbook information
                try:
                    sportsbook_id_int = int(sportsbook_key)
                    sportsbook_mapping = (
                        await self.sportsbook_resolver.resolve_action_network_id(
                            sportsbook_id_int
                        )
                    )
                    sportsbook_id = (
                        sportsbook_mapping[0] if sportsbook_mapping else None
                    )
                    sportsbook_name = (
                        sportsbook_mapping[1]
                        if sportsbook_mapping
                        else f"Sportsbook_{sportsbook_key}"
                    )
                except (ValueError, TypeError):
                    logger.warning(f"Invalid sportsbook key: {sportsbook_key}")
                    continue

                event_data = sportsbook_data["event"]

                # Process each market type
                for market_type in ["moneyline", "spread", "total"]:
                    market_data = event_data.get(market_type, [])
                    if not market_data:
                        continue

                    # Process each side in the market
                    for side_data in market_data:
                        side = side_data.get("side")
                        if not side:
                            continue

                        # Extract current odds information
                        current_odds = side_data.get("odds")
                        line_value = self._safe_decimal(side_data.get("value"))
                        market_id = side_data.get("market_id")
                        outcome_id = side_data.get("outcome_id")

                        # Process historical data from history array - THIS IS THE KEY PART
                        history = side_data.get("history", [])
                        current_odds_in_history = False

                        for hist_entry in history:
                            # Parse timestamp from history entry
                            updated_at_str = hist_entry.get("updated_at")
                            if not updated_at_str:
                                continue

                            try:
                                # Parse ISO timestamp with Z suffix and handle microseconds
                                # Some timestamps have more than 6 microsecond digits, truncate to 6
                                timestamp_clean = updated_at_str.replace("Z", "+00:00")

                                # Handle microseconds with more than 6 digits
                                if "." in timestamp_clean and "+" in timestamp_clean:
                                    date_part, time_with_tz = timestamp_clean.split("+")
                                    if "." in date_part:
                                        time_base, microseconds = date_part.rsplit(
                                            ".", 1
                                        )
                                        # Truncate microseconds to 6 digits
                                        microseconds = microseconds[:6].ljust(6, "0")
                                        timestamp_clean = (
                                            f"{time_base}.{microseconds}+{time_with_tz}"
                                        )

                                updated_at = datetime.fromisoformat(timestamp_clean)
                            except (ValueError, TypeError) as e:
                                logger.warning(
                                    f"Invalid timestamp in history: {updated_at_str}, error: {e}"
                                )
                                continue

                            # Check if this is the current odds
                            hist_odds = hist_entry.get("odds")
                            is_current = (
                                hist_odds == current_odds
                                and self._safe_decimal(hist_entry.get("value"))
                                == line_value
                            )
                            if is_current:
                                current_odds_in_history = True

                            # Determine line value based on market type
                            hist_line_value = self._safe_decimal(
                                hist_entry.get("value")
                            )
                            if market_type == "moneyline":
                                final_line_value = (
                                    None  # Moneyline should always be NULL
                                )
                            else:
                                final_line_value = hist_line_value or line_value

                            # Create historical record
                            historical_record = HistoricalOddsRecord(
                                external_game_id=external_game_id,
                                sportsbook_external_id=sportsbook_key,
                                sportsbook_id=sportsbook_id,
                                sportsbook_name=sportsbook_name,
                                market_type=market_type,
                                side=side,
                                odds=hist_odds,
                                line_value=final_line_value,
                                updated_at=updated_at,
                                data_collection_time=collection_time,
                                data_processing_time=now_est(),
                                line_status=hist_entry.get("line_status", "normal"),
                                is_current_odds=is_current,
                                market_id=market_id,
                                outcome_id=outcome_id,
                                raw_data_id=raw_history["id"],
                            )

                            records.append(historical_record)

                        # If current odds not in history, add as current record
                        if not current_odds_in_history and current_odds is not None:
                            # Apply same line value logic for current odds
                            current_line_value = (
                                None if market_type == "moneyline" else line_value
                            )

                            current_record = HistoricalOddsRecord(
                                external_game_id=external_game_id,
                                sportsbook_external_id=sportsbook_key,
                                sportsbook_id=sportsbook_id,
                                sportsbook_name=sportsbook_name,
                                market_type=market_type,
                                side=side,
                                odds=current_odds,
                                line_value=current_line_value,
                                updated_at=collection_time,  # Use collection time as proxy
                                data_collection_time=collection_time,
                                data_processing_time=now_est(),
                                line_status="normal",
                                is_current_odds=True,
                                market_id=market_id,
                                outcome_id=outcome_id,
                                raw_data_id=raw_history["id"],
                            )

                            records.append(current_record)

            return records

        except Exception as e:
            logger.error(f"Error extracting historical records from history: {e}")
            return []

    async def _resolve_mlb_game_id_enhanced(
        self, record: HistoricalOddsRecord, conn: asyncpg.Connection
    ) -> str | None:
        """Enhanced MLB ID resolution using multiple strategies including raw data view."""
        try:
            # First check if we already resolved this game's MLB ID
            existing_mlb_id = await conn.fetchval(
                """
                SELECT mlb_stats_api_game_id 
                FROM staging.action_network_games 
                WHERE external_game_id = $1 AND mlb_stats_api_game_id IS NOT NULL
                """,
                record.external_game_id,
            )
            
            if existing_mlb_id:
                logger.debug(f"Using cached MLB game ID for {record.external_game_id}: {existing_mlb_id}")
                return existing_mlb_id

            # Strategy 1: Try staging data join
            game_info = await conn.fetchrow(
                """
                SELECT home_team_normalized, away_team_normalized, game_date
                FROM staging.action_network_games 
                WHERE external_game_id = $1
            """,
                record.external_game_id,
            )

            if game_info:
                # Use enhanced Action Network-specific resolver
                resolution_result = await self.mlb_resolver.resolve_action_network_game_id(
                    external_game_id=record.external_game_id,
                    game_date=game_info["game_date"],
                )

                if resolution_result.mlb_game_id:
                    # Update games table with resolved MLB game ID
                    await conn.execute(
                        """
                        UPDATE staging.action_network_games 
                        SET mlb_stats_api_game_id = $1, updated_at = NOW()
                        WHERE external_game_id = $2
                    """,
                        resolution_result.mlb_game_id,
                        record.external_game_id,
                    )

                    logger.info(
                        f"Resolved MLB game ID via staging for {record.external_game_id}: {resolution_result.mlb_game_id} (confidence: {resolution_result.confidence})"
                    )
                    return resolution_result.mlb_game_id

            # Strategy 2: Try raw data view (enhanced approach from backfill)
            logger.debug(f"Trying raw data view resolution for {record.external_game_id}")
            raw_game_info = await conn.fetchrow(
                """
                SELECT away_team, home_team, 
                       COALESCE(start_time::date, DATE(start_time)) as game_date
                FROM raw_data.v_action_network_games_readable 
                WHERE external_game_id = $1
                AND away_team IS NOT NULL 
                AND home_team IS NOT NULL
                """,
                record.external_game_id,
            )

            if raw_game_info:
                from ...core.team_utils import normalize_team_name
                
                home_team_normalized = normalize_team_name(raw_game_info["home_team"])
                away_team_normalized = normalize_team_name(raw_game_info["away_team"])
                
                # Use Action Network specific resolver with normalized team names
                resolution_result = await self.mlb_resolver.resolve_action_network_game_id(
                    external_game_id=record.external_game_id,
                    game_date=raw_game_info["game_date"]
                )
                
                # Fallback to generic resolver if Action Network specific fails
                if not resolution_result.mlb_game_id:
                    resolution_result = await self.mlb_resolver.resolve_game_id(
                        external_id=record.external_game_id,
                        source=DataSource.ACTION_NETWORK,
                        home_team=home_team_normalized,
                        away_team=away_team_normalized,
                        game_date=raw_game_info["game_date"]
                    )

                if resolution_result.mlb_game_id:
                    # Update games table if it exists, otherwise we'll rely on odds table
                    if game_info:
                        await conn.execute(
                            """
                            UPDATE staging.action_network_games 
                            SET mlb_stats_api_game_id = $1, updated_at = NOW()
                            WHERE external_game_id = $2
                        """,
                            resolution_result.mlb_game_id,
                            record.external_game_id,
                        )

                    logger.info(
                        f"Resolved MLB game ID via raw data for {record.external_game_id}: {resolution_result.mlb_game_id} "
                        f"({away_team_normalized} @ {home_team_normalized}) - confidence: {resolution_result.confidence}"
                    )
                    return resolution_result.mlb_game_id

            logger.warning(f"No game info found for Action Network game {record.external_game_id}")
            return None

        except Exception as e:
            logger.error(
                f"Error resolving MLB game ID for {record.external_game_id}: {e}"
            )
            return None

    async def _resolve_mlb_game_id(
        self, record: HistoricalOddsRecord, conn: asyncpg.Connection
    ) -> str | None:
        """Legacy method - delegates to enhanced resolver."""
        return await self._resolve_mlb_game_id_enhanced(record, conn)

    async def _insert_historical_odds_record(
        self, record: HistoricalOddsRecord, conn: asyncpg.Connection
    ) -> None:
        """Insert historical odds record into database."""
        await conn.execute(
            """
            INSERT INTO staging.action_network_odds_historical (
                external_game_id, mlb_stats_api_game_id, sportsbook_external_id,
                sportsbook_id, sportsbook_name, market_type, side,
                odds, line_value, updated_at, data_collection_time, data_processing_time,
                line_status, is_current_odds, market_id, outcome_id, period,
                data_quality_score, validation_status, raw_data_id, processed_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
            )
            ON CONFLICT (external_game_id, sportsbook_external_id, market_type, side, updated_at) 
            DO UPDATE SET
                mlb_stats_api_game_id = EXCLUDED.mlb_stats_api_game_id,
                is_current_odds = EXCLUDED.is_current_odds,
                data_processing_time = EXCLUDED.data_processing_time,
                updated_at_record = NOW()
        """,
            record.external_game_id,
            record.mlb_stats_api_game_id,
            record.sportsbook_external_id,
            record.sportsbook_id,
            record.sportsbook_name,
            record.market_type,
            record.side,
            record.odds,
            record.line_value,
            record.updated_at,
            record.data_collection_time,
            record.data_processing_time,
            record.line_status,
            record.is_current_odds,
            record.market_id,
            record.outcome_id,
            record.period,
            record.data_quality_score,
            record.validation_status,
            record.raw_data_id,
            now_est(),
        )

    def _safe_decimal(self, value: Any) -> Decimal | None:
        """Safely convert value to Decimal."""
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except (ValueError, TypeError):
            return None


# CLI entry point
async def main():
    """Run history data processing from command line."""
    processor = ActionNetworkHistoryProcessor()
    await processor.initialize()

    try:
        result = await processor.process_history_data(limit=10)  # Process more records
        print(f"History processing completed: {result}")
    finally:
        await processor.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
