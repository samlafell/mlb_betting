"""
RAW Zone Adapter

Adapts existing collectors to write to the RAW zone instead of directly to core_betting.
Provides backward compatibility while implementing the new pipeline architecture.

Reference: docs/SYSTEM_DESIGN_ANALYSIS.md
"""

from datetime import datetime, timezone
from typing import Any

from ...core.config import get_settings
from ...core.logging import LogComponent, get_logger
from .raw_zone import RawDataRecord, RawZoneProcessor
from .zone_interface import ProcessingResult, ZoneType, create_zone_config

logger = get_logger(__name__, LogComponent.CORE)


class RawZoneAdapter:
    """
    Adapter that wraps existing collectors to write to RAW zone.

    This allows existing collectors to work with the new pipeline architecture
    without requiring immediate refactoring of all collection logic.
    """

    def __init__(self):
        self.settings = get_settings()
        self.raw_processor = self._create_raw_processor()

    def _create_raw_processor(self) -> RawZoneProcessor:
        """Create RAW zone processor with configuration."""
        config = create_zone_config(
            ZoneType.RAW,
            self.settings.schemas.raw,
            batch_size=1000,
            validation_enabled=True,
            auto_promotion=True,
        )
        return RawZoneProcessor(config)

    async def store_action_network_games(
        self,
        games_data: list[dict[str, Any]],
        source_info: dict[str, Any] | None = None,
    ) -> ProcessingResult:
        """
        Store Action Network game data to RAW zone.

        Args:
            games_data: List of game data dictionaries from Action Network
            source_info: Additional source information (endpoint, etc.)

        Returns:
            ProcessingResult from RAW zone processing
        """
        try:
            raw_records = []

            for game_data in games_data:
                # Create raw data record
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

            # Process through RAW zone
            result = await self.raw_processor.process_batch(raw_records)

            logger.info(
                f"Stored {result.records_successful} Action Network games to RAW zone"
            )
            return result

        except Exception as e:
            logger.error(f"Error storing Action Network games to RAW zone: {e}")
            raise

    async def store_action_network_odds(
        self,
        odds_data: list[dict[str, Any]],
        game_id: str | None = None,
        source_info: dict[str, Any] | None = None,
    ) -> ProcessingResult:
        """
        Store Action Network odds data to RAW zone.

        Args:
            odds_data: List of odds data dictionaries from Action Network
            game_id: Associated game ID
            source_info: Additional source information

        Returns:
            ProcessingResult from RAW zone processing
        """
        try:
            raw_records = []

            for odds_entry in odds_data:
                # Extract sportsbook and betting line information
                sportsbook_key = odds_entry.get(
                    "sportsbook_key", odds_entry.get("key", "")
                )

                # Create raw data record
                record = RawDataRecord(
                    external_id=f"{game_id}_{sportsbook_key}_{datetime.now().isoformat()}",
                    source="action_network",
                    game_external_id=game_id,
                    sportsbook_name=sportsbook_key,
                    raw_data=odds_entry,
                    data_type="odds",
                    collected_at=datetime.now(timezone.utc),
                )
                raw_records.append(record)

            # Process through RAW zone
            result = await self.raw_processor.process_batch(raw_records)

            logger.info(
                f"Stored {result.records_successful} Action Network odds to RAW zone"
            )
            return result

        except Exception as e:
            logger.error(f"Error storing Action Network odds to RAW zone: {e}")
            raise

    async def store_sbd_betting_splits(
        self,
        splits_data: list[dict[str, Any]],
        source_info: dict[str, Any] | None = None,
    ) -> ProcessingResult:
        """
        Store SportsBettingDime betting splits to RAW zone.

        Args:
            splits_data: List of betting splits data from SBD
            source_info: Additional source information

        Returns:
            ProcessingResult from RAW zone processing
        """
        try:
            raw_records = []

            for split_data in splits_data:
                # Create raw data record
                record = RawDataRecord(
                    external_id=split_data.get(
                        "matchup_id", f"sbd_{datetime.now().isoformat()}"
                    ),
                    source="sbd",
                    game_external_id=split_data.get("game_id"),
                    raw_data=split_data,
                    data_type="betting_splits",
                    api_endpoint=source_info.get("api_endpoint")
                    if source_info
                    else None,
                    collected_at=datetime.now(timezone.utc),
                )
                raw_records.append(record)

            # Process through RAW zone
            result = await self.raw_processor.process_batch(raw_records)

            logger.info(
                f"Stored {result.records_successful} SBD betting splits to RAW zone"
            )
            return result

        except Exception as e:
            logger.error(f"Error storing SBD betting splits to RAW zone: {e}")
            raise

    async def store_vsin_data(
        self,
        vsin_data: list[dict[str, Any]],
        data_type: str = "general",
        source_info: dict[str, Any] | None = None,
    ) -> ProcessingResult:
        """
        Store VSIN data to RAW zone.

        Args:
            vsin_data: List of VSIN data
            data_type: Type of VSIN data (game, odds, analysis, etc.)
            source_info: Additional source information

        Returns:
            ProcessingResult from RAW zone processing
        """
        try:
            raw_records = []

            for data_entry in vsin_data:
                # Create raw data record
                record = RawDataRecord(
                    external_id=data_entry.get(
                        "id", f"vsin_{datetime.now().isoformat()}"
                    ),
                    source="vsin",
                    game_external_id=data_entry.get("game_id"),
                    raw_data=data_entry,
                    data_type=data_type,
                    source_feed=source_info.get("source_feed") if source_info else None,
                    collected_at=datetime.now(timezone.utc),
                )
                raw_records.append(record)

            # Process through RAW zone
            result = await self.raw_processor.process_batch(raw_records)

            logger.info(
                f"Stored {result.records_successful} VSIN {data_type} records to RAW zone"
            )
            return result

        except Exception as e:
            logger.error(f"Error storing VSIN data to RAW zone: {e}")
            raise

    async def store_mlb_stats_data(
        self,
        mlb_data: list[dict[str, Any]],
        source_info: dict[str, Any] | None = None,
    ) -> ProcessingResult:
        """
        Store MLB Stats API data to RAW zone.

        Args:
            mlb_data: List of MLB Stats API data
            source_info: Additional source information

        Returns:
            ProcessingResult from RAW zone processing
        """
        try:
            raw_records = []

            for data_entry in mlb_data:
                # Create raw data record
                record = RawDataRecord(
                    external_id=str(data_entry.get("gamePk", data_entry.get("id", ""))),
                    source="mlb_stats_api",
                    game_external_id=str(
                        data_entry.get("gamePk", data_entry.get("id", ""))
                    ),
                    raw_data=data_entry,
                    api_endpoint=source_info.get("api_endpoint")
                    if source_info
                    else None,
                    game_date=data_entry.get("gameDate"),
                    collected_at=datetime.now(timezone.utc),
                )
                raw_records.append(record)

            # Process through RAW zone
            result = await self.raw_processor.process_batch(raw_records)

            logger.info(
                f"Stored {result.records_successful} MLB Stats API records to RAW zone"
            )
            return result

        except Exception as e:
            logger.error(f"Error storing MLB Stats API data to RAW zone: {e}")
            raise

    async def store_betting_lines(
        self, lines_data: list[dict[str, Any]], bet_type: str, source: str = "generic"
    ) -> ProcessingResult:
        """
        Store generic betting lines to RAW zone.

        Args:
            lines_data: List of betting line data
            bet_type: Type of bet (moneyline, spread, total)
            source: Data source name

        Returns:
            ProcessingResult from RAW zone processing
        """
        try:
            raw_records = []

            for line_data in lines_data:
                # Create raw data record
                record = RawDataRecord(
                    external_id=line_data.get(
                        "id", f"{source}_{bet_type}_{datetime.now().isoformat()}"
                    ),
                    source=source,
                    game_external_id=line_data.get("game_id"),
                    sportsbook_name=line_data.get("sportsbook"),
                    bet_type=bet_type,
                    raw_data=line_data,
                    game_date=line_data.get("game_date"),
                    collected_at=datetime.now(timezone.utc),
                )
                raw_records.append(record)

            # Process through RAW zone
            result = await self.raw_processor.process_batch(raw_records)

            logger.info(
                f"Stored {result.records_successful} {bet_type} lines from {source} to RAW zone"
            )
            return result

        except Exception as e:
            logger.error(f"Error storing betting lines to RAW zone: {e}")
            raise

    async def store_line_movements(
        self, movements_data: list[dict[str, Any]], source: str = "generic"
    ) -> ProcessingResult:
        """
        Store line movement data to RAW zone.

        Args:
            movements_data: List of line movement data
            source: Data source name

        Returns:
            ProcessingResult from RAW zone processing
        """
        try:
            raw_records = []

            for movement_data in movements_data:
                # Create raw data record
                record = RawDataRecord(
                    external_id=movement_data.get(
                        "id", f"{source}_movement_{datetime.now().isoformat()}"
                    ),
                    source=source,
                    game_external_id=movement_data.get("game_id"),
                    bet_type="line_movement",
                    raw_data=movement_data,
                    collected_at=datetime.now(timezone.utc),
                )
                raw_records.append(record)

            # Process through RAW zone
            result = await self.raw_processor.process_batch(raw_records)

            logger.info(
                f"Stored {result.records_successful} line movements from {source} to RAW zone"
            )
            return result

        except Exception as e:
            logger.error(f"Error storing line movements to RAW zone: {e}")
            raise

    async def get_raw_zone_status(self) -> dict[str, Any]:
        """Get RAW zone health and status information."""
        return await self.raw_processor.health_check()

    async def cleanup(self) -> None:
        """Cleanup adapter resources."""
        if self.raw_processor:
            await self.raw_processor.cleanup()


# Convenience function to create adapter instance
def create_raw_zone_adapter() -> RawZoneAdapter:
    """Create a new RAW zone adapter instance."""
    return RawZoneAdapter()
