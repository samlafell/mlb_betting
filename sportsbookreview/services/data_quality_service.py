from __future__ import annotations

"""Data quality & deduplication service for SportsbookReview (Phase-3)."""

import logging
from typing import Dict, List, Set

from ..parsers.validators import GameDataValidator

logger = logging.getLogger(__name__)


class DataQualityService:
    """Validate, clean and deduplicate parsed game records."""

    def __init__(self) -> None:
        self._seen_ids: Set[str] = set()

    async def process_games(self, games: List[Dict]) -> List[Dict]:
        """Return list of *validated & unique* game dicts."""
        cleaned: List[Dict] = []
        for game in games:
            sbr_id = str(game.get("sbr_game_id"))
            if sbr_id in self._seen_ids:
                logger.debug("Skipping duplicate sbr_game_id=%s", sbr_id)
                continue

            validated = GameDataValidator.validate_data(game)
            if validated is None:
                logger.warning("Validation failed for sbr_game_id=%s", sbr_id)
                continue

            self._seen_ids.add(sbr_id)
            cleaned.append(validated.model_dump())

        return cleaned 