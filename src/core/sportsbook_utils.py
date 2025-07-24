"""
Sportsbook Utilities

Provides utilities for mapping external sportsbook IDs to internal database IDs.
Supports Action Network and other data sources.
"""

import asyncpg


class SportsbookResolver:
    """Resolves external sportsbook IDs to internal database IDs."""

    def __init__(self, db_config: dict):
        self.db_config = db_config
        self._action_network_cache = {}
        self._cache_loaded = False

    async def load_action_network_mappings(self) -> None:
        """Load Action Network sportsbook ID mappings into cache."""
        try:
            conn = await asyncpg.connect(**self.db_config)

            try:
                mappings = await conn.fetch("""
                    SELECT 
                        m.external_id::integer as action_network_id,
                        m.sportsbook_id,
                        s.display_name,
                        s.abbreviation
                    FROM core_betting.sportsbook_external_mappings m
                    JOIN core_betting.sportsbooks s ON m.sportsbook_id = s.id
                    WHERE m.external_source = 'ACTION_NETWORK'
                """)

                for mapping in mappings:
                    self._action_network_cache[mapping["action_network_id"]] = {
                        "sportsbook_id": mapping["sportsbook_id"],
                        "display_name": mapping["display_name"],
                        "abbreviation": mapping["abbreviation"],
                    }

                self._cache_loaded = True

            finally:
                await conn.close()

        except Exception as e:
            raise Exception(f"Failed to load Action Network sportsbook mappings: {e}")

    async def resolve_action_network_id(
        self, action_network_id: int
    ) -> tuple[int, str] | None:
        """
        Resolve Action Network sportsbook ID to internal ID and display name.

        Args:
            action_network_id: External Action Network book ID

        Returns:
            Tuple of (sportsbook_id, display_name) or None if not found
        """
        if not self._cache_loaded:
            await self.load_action_network_mappings()

        mapping = self._action_network_cache.get(action_network_id)
        if mapping:
            return mapping["sportsbook_id"], mapping["display_name"]

        return None

    def get_all_action_network_mappings(self) -> dict[int, dict]:
        """Get all cached Action Network mappings."""
        return self._action_network_cache.copy()


# Standalone functions for simple usage
async def get_sportsbook_mapping(
    db_config: dict, action_network_id: int
) -> tuple[int, str] | None:
    """
    Get sportsbook mapping for Action Network ID.

    Args:
        db_config: Database connection configuration
        action_network_id: Action Network sportsbook ID

    Returns:
        Tuple of (sportsbook_id, display_name) or None
    """
    resolver = SportsbookResolver(db_config)
    return await resolver.resolve_action_network_id(action_network_id)


async def get_all_action_network_mappings(db_config: dict) -> dict[int, dict]:
    """
    Get all Action Network sportsbook mappings.

    Args:
        db_config: Database connection configuration

    Returns:
        Dictionary mapping Action Network IDs to sportsbook info
    """
    resolver = SportsbookResolver(db_config)
    await resolver.load_action_network_mappings()
    return resolver.get_all_action_network_mappings()


# Constants for common Action Network sportsbook IDs
ACTION_NETWORK_SPORTSBOOKS = {
    15: "DraftKings",
    30: "FanDuel",
    68: "BetMGM",
    69: "BetMGM (Alternative)",
    71: "PointsBet",
    75: "Barstool Sportsbook",
    79: "Caesars",
    83: "BetRivers",
    123: "WynnBET",
    972: "WynnBET",
}
