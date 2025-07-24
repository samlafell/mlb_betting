#!/usr/bin/env python3
"""
Action Network Historical Odds Processor

Extracts complete line movement history with exact timestamps from JSON data.
Creates individual records for each historical odds point with updated_at timestamps.

Key Features:
1. Extracts history arrays from JSON with exact timestamps
2. Creates separate records for each line change
3. Matches closest timestamps between over/under and home/away
4. Captures complete temporal dimension of odds data
5. Enables sophisticated line movement analysis

Example JSON Processing:
{
    "history": [
        {"odds": -105, "value": 8, "updated_at": "2025-07-21T17:39:30.195056Z"},
        {"odds": -108, "value": 8, "updated_at": "2025-07-21T18:15:45Z"}
    ]
}
→ Creates 2 separate historical records with exact timestamps
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


class ActionNetworkHistoricalProcessor:
    """
    Processor for extracting complete historical odds data with timestamps.

    Processes raw Action Network JSON data to extract every historical odds point
    with its exact updated_at timestamp, creating a complete temporal record.
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
            "ActionNetworkHistoricalProcessor initialized with MLB Stats API integration"
        )

    async def cleanup(self):
        """Cleanup processor resources."""
        await self.mlb_resolver.cleanup()

    async def process_historical_odds(self, limit: int = 100) -> dict[str, Any]:
        """
        Process raw odds data to extract complete historical records with timestamps.

        This method:
        1. Retrieves unprocessed raw odds data
        2. Extracts history arrays with exact updated_at timestamps
        3. Creates separate records for each historical point
        4. Resolves MLB Stats API game IDs
        5. Handles both historical and current odds
        """
        logger.info(
            "Starting historical odds processing",
            batch_id=self.processing_batch_id,
            limit=limit,
        )

        try:
            conn = await asyncpg.connect(**self._get_db_config())

            # Get unprocessed raw odds
            raw_odds = await conn.fetch(
                """
                SELECT id, external_game_id, sportsbook_key, raw_odds, collected_at
                FROM raw_data.action_network_odds 
                WHERE id NOT IN (
                    SELECT raw_data_id FROM staging.action_network_odds_historical
                    WHERE raw_data_id IS NOT NULL
                )
                ORDER BY collected_at DESC
                LIMIT $1
            """,
                limit,
            )

            if not raw_odds:
                logger.info("No unprocessed odds found for historical processing")
                return {
                    "historical_records_processed": 0,
                    "historical_records_valid": 0,
                    "mlb_games_resolved": 0,
                }

            logger.info(
                f"Found {len(raw_odds)} unprocessed odds records for historical processing"
            )

            processed_count = 0
            valid_count = 0
            mlb_resolved_count = 0

            for raw_odds_record in raw_odds:
                try:
                    # Extract all historical records from this raw odds record
                    historical_records = await self._extract_historical_records(
                        raw_odds_record, conn
                    )

                    for historical_record in historical_records:
                        # Resolve MLB Stats API game ID if needed
                        if not historical_record.mlb_stats_api_game_id:
                            mlb_game_id = await self._resolve_mlb_game_id(
                                historical_record, conn
                            )
                            if mlb_game_id:
                                historical_record.mlb_stats_api_game_id = mlb_game_id
                                mlb_resolved_count += 1

                        # Insert historical record
                        await self._insert_historical_odds_record(
                            historical_record, conn
                        )
                        processed_count += 1

                        if historical_record.validation_status == "valid":
                            valid_count += 1

                except Exception as e:
                    logger.error(
                        f"Error processing historical odds record {raw_odds_record['id']}: {e}"
                    )
                    continue

            await conn.close()

            result = {
                "historical_records_processed": processed_count,
                "historical_records_valid": valid_count,
                "mlb_games_resolved": mlb_resolved_count,
                "processing_batch_id": self.processing_batch_id,
                "structure_type": "Historical - complete temporal records with exact timestamps",
            }

            logger.info("Historical odds processing completed", **result)
            return result

        except Exception as e:
            logger.error(f"Error in historical odds processing: {e}")
            raise

    async def _extract_historical_records(
        self, raw_odds: dict, conn: asyncpg.Connection
    ) -> list[HistoricalOddsRecord]:
        """Extract all historical records from raw odds JSON data."""
        records = []

        try:
            # Parse raw odds data
            raw_odds_data = raw_odds["raw_odds"]
            if isinstance(raw_odds_data, str):
                odds_data = json.loads(raw_odds_data)
            else:
                odds_data = raw_odds_data

            external_game_id = raw_odds["external_game_id"]
            sportsbook_key = raw_odds["sportsbook_key"]
            collection_time = raw_odds["collected_at"]

            # Resolve sportsbook information
            sportsbook_mapping = (
                await self.sportsbook_resolver.resolve_action_network_id(
                    int(sportsbook_key)
                )
            )
            sportsbook_id = sportsbook_mapping[0] if sportsbook_mapping else None
            sportsbook_name = (
                sportsbook_mapping[1]
                if sportsbook_mapping
                else f"Sportsbook_{sportsbook_key}"
            )

            # Process each market type
            for market_type in ["moneyline", "spread", "total"]:
                market_data = odds_data.get(market_type, [])
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

                    # Process historical data from history array
                    history = side_data.get("history", [])
                    current_odds_in_history = False

                    for hist_entry in history:
                        # Parse timestamp from history entry
                        updated_at_str = hist_entry.get("updated_at")
                        if not updated_at_str:
                            continue

                        try:
                            updated_at = datetime.fromisoformat(
                                updated_at_str.replace("Z", "+00:00")
                            )
                        except (ValueError, TypeError) as e:
                            logger.warning(
                                f"Invalid timestamp in history: {updated_at_str}, error: {e}"
                            )
                            continue

                        # Check if this is the current odds
                        hist_odds = hist_entry.get("odds")
                        is_current = hist_odds == current_odds
                        if is_current:
                            current_odds_in_history = True

                        # Determine line value based on market type
                        hist_line_value = self._safe_decimal(hist_entry.get("value"))
                        if market_type == "moneyline":
                            final_line_value = None  # Moneyline should always be NULL
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
                            raw_data_id=raw_odds["id"],
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
                            raw_data_id=raw_odds["id"],
                        )

                        records.append(current_record)

            return records

        except Exception as e:
            logger.error(f"Error extracting historical records: {e}")
            return []

    async def _resolve_mlb_game_id(
        self, record: HistoricalOddsRecord, conn: asyncpg.Connection
    ) -> str | None:
        """Resolve Action Network game ID to MLB Stats API game ID."""
        try:
            # Get game information for resolution
            game_info = await conn.fetchrow(
                """
                SELECT home_team_normalized, away_team_normalized, game_date
                FROM staging.action_network_games 
                WHERE external_game_id = $1
            """,
                record.external_game_id,
            )

            if not game_info:
                return None

            # Use MLB resolver to get official game ID
            resolution_result = await self.mlb_resolver.resolve_game_id(
                external_id=record.external_game_id,
                source=DataSource.ACTION_NETWORK,
                home_team=game_info["home_team_normalized"],
                away_team=game_info["away_team_normalized"],
                game_date=game_info["game_date"],
            )

            if resolution_result.mlb_game_id:
                # Update games table with resolved MLB game ID
                await conn.execute(
                    """
                    UPDATE staging.action_network_games 
                    SET mlb_stats_api_game_id = $1, updated_at = NOW()
                    WHERE external_game_id = $2 AND mlb_stats_api_game_id IS NULL
                """,
                    resolution_result.mlb_game_id,
                    record.external_game_id,
                )

                logger.debug(
                    f"Resolved MLB game ID for {record.external_game_id}: {resolution_result.mlb_game_id}"
                )
                return resolution_result.mlb_game_id

            return None

        except Exception as e:
            logger.error(
                f"Error resolving MLB game ID for {record.external_game_id}: {e}"
            )
            return None

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
    """Run historical odds processing from command line."""
    processor = ActionNetworkHistoricalProcessor()
    await processor.initialize()

    try:
        result = await processor.process_historical_odds(limit=5)  # Small test batch
        print(f"Historical processing completed: {result}")
    finally:
        await processor.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
