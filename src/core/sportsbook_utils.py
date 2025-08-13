"""
MLB Sportsbook Utilities

Provides sportsbook resolution and database mapping functionality.
Includes both static mapping for performance and dynamic database resolution.
"""

import asyncpg
from typing import Dict, Any, Optional, TypedDict


class SportsbookInfo(TypedDict):
    """Type definition for sportsbook information."""
    id: int
    name: str
    external_id: str
    short_name: str
    is_active: bool


class SportsbookResolutionError(Exception):
    """Raised when sportsbook resolution fails."""
    pass


# Static mapping for known Action Network sportsbooks (for performance)
STATIC_ACTION_NETWORK_MAPPING = {
    '15': {'id': 15, 'name': 'FanDuel', 'short_name': 'FD'},
    '30': {'id': 30, 'name': 'DraftKings', 'short_name': 'DK'},
    '68': {'id': 68, 'name': 'BetMGM', 'short_name': 'MGM'},
    '69': {'id': 69, 'name': 'Caesars', 'short_name': 'CZR'},
    '71': {'id': 71, 'name': 'Bet365', 'short_name': 'B365'},
    '75': {'id': 75, 'name': 'Pinnacle', 'short_name': 'PIN'},
    '79': {'id': 79, 'name': 'Circa Sports', 'short_name': 'CIRCA'},
    '123': {'id': 123, 'name': 'PointsBet', 'short_name': 'PB'},
    '972': {'id': 972, 'name': 'Fanatics', 'short_name': 'FAN'},
}

# String name mappings
STRING_NAME_MAPPING = {
    'fanduel': '15',
    'draftkings': '30', 
    'betmgm': '68',
    'caesars': '69',
    'bet365': '71',
    'pinnacle': '75',
    'circa': '79',
    'pointsbet': '123',
    'fanatics': '972',
}


def resolve_sportsbook_info_static(external_id: str) -> SportsbookInfo:
    """
    Fast static sportsbook resolution for staging pipeline.
    
    Args:
        external_id: External sportsbook ID (string or int)
        
    Returns:
        SportsbookInfo: Resolved sportsbook information
        
    Raises:
        SportsbookResolutionError: If sportsbook cannot be resolved
    """
    if not external_id:
        raise SportsbookResolutionError("Empty or None external_id provided")
    
    # Convert to string for consistent lookup
    external_id_str = str(external_id).strip()
    
    # Try direct ID lookup
    if external_id_str in STATIC_ACTION_NETWORK_MAPPING:
        mapping = STATIC_ACTION_NETWORK_MAPPING[external_id_str]
        return SportsbookInfo(
            id=mapping['id'],
            name=mapping['name'],
            external_id=external_id_str,
            short_name=mapping['short_name'],
            is_active=True
        )
    
    # Try string name lookup
    normalized_name = external_id_str.lower().replace(' ', '').replace('_', '')
    if normalized_name in STRING_NAME_MAPPING:
        resolved_id = STRING_NAME_MAPPING[normalized_name]
        mapping = STATIC_ACTION_NETWORK_MAPPING[resolved_id]
        return SportsbookInfo(
            id=mapping['id'],
            name=mapping['name'],
            external_id=resolved_id,
            short_name=mapping['short_name'],
            is_active=True
        )
    
    raise SportsbookResolutionError(f"Unknown sportsbook: {external_id}")


# Legacy alias for backwards compatibility
resolve_sportsbook_info = resolve_sportsbook_info_static


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
                        m.external_sportsbook_id::integer as action_network_id,
                        m.sportsbook_id,
                        s.display_name,
                        s.abbreviation
                    FROM curated.sportsbook_mappings m
                    JOIN curated.sportsbooks s ON m.sportsbook_id = s.id
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
