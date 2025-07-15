from __future__ import annotations

"""Integration service: move validated SportsbookReview games into main DB (Phase-3)."""

import logging

from .data_quality_service import DataQualityService
from .data_storage_service import DataStorageService

logger = logging.getLogger(__name__)


class IntegrationService:
    """High-level orchestration: quality-check, deduplicate, store, publish events."""

    def __init__(self, storage: DataStorageService):
        self.storage = storage
        self.quality = DataQualityService()

    async def integrate(self, raw_games: list[dict]):
        """Validate, deduplicate then write to DB."""
        # 1. Quality & deduplication
        validated_games = await self.quality.process_games(raw_games)
        if not validated_games:
            logger.warning("IntegrationService: no valid games to integrate")
            return 0

        inserted = 0
        import copy

        from sportsbookreview.models.game import EnhancedGame

        for raw_game in validated_games:
            game_dict = copy.deepcopy(raw_game)

            # Transform keys to align with EnhancedGame
            # EnhancedGame now uses game_datetime, so no transformation needed
            # if 'game_date' in game_dict and 'game_datetime' not in game_dict:
            #     game_dict['game_datetime'] = game_dict.pop('game_date')

            # Remove odds_data before storing game, capture separately
            odds_records = game_dict.pop("odds_data", [])

            # Clean unknown fields
            allowed = set(EnhancedGame.model_fields.keys())
            game_clean = {k: v for k, v in game_dict.items() if k in allowed}

            try:
                await self.storage.store_game_data(
                    {"game": game_clean, "betting_data": odds_records}
                )
                inserted += 1
                logger.info(
                    "event: sportsbookreview.game.inserted id=%s",
                    game_clean["sbr_game_id"],
                )
            except Exception as exc:
                logger.error(
                    "IntegrationService failed to store game_id=%s: %s",
                    game_dict.get("sbr_game_id"),
                    exc,
                )
        return inserted
