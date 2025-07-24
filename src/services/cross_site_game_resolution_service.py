#!/usr/bin/env python3
"""
Cross-Site Game ID Resolution Service

This service manages the mapping and resolution of game IDs across different data sources:
- MLB Stats API (authoritative source)
- Action Network
- VSIN
- SportsBettingDime (SBD)
- SportsBookReview (SBR)

It maintains the centralized games table and provides game ID resolution capabilities.
"""

import asyncio
from dataclasses import dataclass
from datetime import date, datetime

import asyncpg
import structlog

from ..core.datetime_utils import (
    now_est,
)
from ..core.team_utils import normalize_team_name

logger = structlog.get_logger(__name__)


@dataclass
class GameMapping:
    """Represents a game mapping across different sources."""

    staging_game_id: int | None = None
    mlb_stats_api_game_id: str | None = None
    action_network_game_id: int | None = None
    vsin_game_id: str | None = None
    sbd_game_id: str | None = None
    sbr_game_id: str | None = None
    home_team: str | None = None
    away_team: str | None = None
    game_datetime: datetime | None = None
    confidence_score: float = 0.0


@dataclass
class GameResolutionResult:
    """Result of game ID resolution attempt."""

    success: bool
    staging_game_id: int | None = None
    mapping: GameMapping | None = None
    created_new: bool = False
    error_message: str | None = None


class CrossSiteGameResolutionService:
    """
    Service for managing cross-site game ID resolution and mapping.

    This service:
    1. Maintains the centralized staging.games table
    2. Resolves games across different data sources
    3. Creates new game mappings when needed
    4. Provides game lookup capabilities
    """

    def __init__(self, db_config: dict = None):
        if db_config:
            self.db_config = db_config
        else:
            # Use centralized database configuration
            from ..core.config import get_settings

            settings = get_settings()
            self.db_config = {
                "host": settings.database.host,
                "port": settings.database.port,
                "database": settings.database.database,
                "user": settings.database.user,
                "password": settings.database.password,
            }

    async def resolve_game_by_mlb_stats_api(
        self, mlb_game_id: str
    ) -> GameResolutionResult:
        """Resolve game using MLB Stats API game ID (authoritative source)."""
        try:
            conn = await asyncpg.connect(**self.db_config)

            # First check if we already have this game mapped
            existing = await conn.fetchrow(
                """
                SELECT id, mlb_stats_api_game_id, action_network_game_id, 
                       vsin_game_id, sbd_game_id, sbr_game_id,
                       home_team, away_team, game_datetime
                FROM staging.games 
                WHERE mlb_stats_api_game_id = $1
            """,
                mlb_game_id,
            )

            if existing:
                mapping = GameMapping(
                    staging_game_id=existing["id"],
                    mlb_stats_api_game_id=existing["mlb_stats_api_game_id"],
                    action_network_game_id=existing["action_network_game_id"],
                    vsin_game_id=existing["vsin_game_id"],
                    sbd_game_id=existing["sbd_game_id"],
                    sbr_game_id=existing["sbr_game_id"],
                    home_team=existing["home_team"],
                    away_team=existing["away_team"],
                    game_datetime=existing["game_datetime"],
                    confidence_score=1.0,  # Perfect match via authoritative source
                )

                await conn.close()
                return GameResolutionResult(
                    success=True,
                    staging_game_id=existing["id"],
                    mapping=mapping,
                    created_new=False,
                )

            # Look up game data from raw MLB Stats API data
            mlb_raw_data = await conn.fetchrow(
                """
                SELECT game_pk, home_team, away_team, game_datetime, raw_response
                FROM raw_data.mlb_stats_api_games
                WHERE external_game_id = $1
            """,
                mlb_game_id,
            )

            if not mlb_raw_data:
                await conn.close()
                return GameResolutionResult(
                    success=False,
                    error_message=f"MLB Stats API game {mlb_game_id} not found in raw data",
                )

            # Create new staging game entry
            new_game_id = await self._create_staging_game(
                conn,
                home_team=mlb_raw_data["home_team"],
                away_team=mlb_raw_data["away_team"],
                game_datetime=mlb_raw_data["game_datetime"],
                mlb_stats_api=mlb_game_id,
            )

            mapping = GameMapping(
                staging_game_id=new_game_id,
                mlb_stats_api_game_id=mlb_game_id,
                home_team=mlb_raw_data["home_team"],
                away_team=mlb_raw_data["away_team"],
                game_datetime=mlb_raw_data["game_datetime"],
                confidence_score=1.0,
            )

            await conn.close()
            return GameResolutionResult(
                success=True,
                staging_game_id=new_game_id,
                mapping=mapping,
                created_new=True,
            )

        except Exception as e:
            logger.error(
                "Error resolving game by MLB Stats API ID",
                mlb_game_id=mlb_game_id,
                error=str(e),
            )
            return GameResolutionResult(success=False, error_message=str(e))

    async def resolve_game_by_action_network(
        self,
        action_network_game_id: int,
        home_team: str = None,
        away_team: str = None,
        game_datetime: datetime = None,
    ) -> GameResolutionResult:
        """Resolve game using Action Network game ID with fallback team/datetime matching."""
        try:
            conn = await asyncpg.connect(**self.db_config)

            # First check direct ID match
            existing = await conn.fetchrow(
                """
                SELECT id, mlb_stats_api_game_id, action_network_game_id, 
                       vsin_game_id, sbd_game_id, sbr_game_id,
                       home_team, away_team, game_datetime
                FROM staging.games 
                WHERE action_network_game_id = $1
            """,
                action_network_game_id,
            )

            if existing:
                mapping = self._build_mapping_from_row(existing)
                await conn.close()
                return GameResolutionResult(
                    success=True,
                    staging_game_id=existing["id"],
                    mapping=mapping,
                    created_new=False,
                )

            # Try to match by team names and date if provided
            if home_team and away_team and game_datetime:
                home_normalized = normalize_team_name(home_team)
                away_normalized = normalize_team_name(away_team)

                # Look for existing game by team and date (within 3 hours)
                existing_by_teams = await conn.fetchrow(
                    """
                    SELECT id, mlb_stats_api_game_id, action_network_game_id, 
                           vsin_game_id, sbd_game_id, sbr_game_id,
                           home_team, away_team, game_datetime
                    FROM staging.games 
                    WHERE home_team = $1 AND away_team = $2
                    AND ABS(EXTRACT(EPOCH FROM (game_datetime - $3))) < 10800  -- 3 hours
                    ORDER BY ABS(EXTRACT(EPOCH FROM (game_datetime - $3)))
                    LIMIT 1
                """,
                    home_normalized,
                    away_normalized,
                    game_datetime,
                )

                if existing_by_teams:
                    # Update with Action Network ID
                    await conn.execute(
                        """
                        UPDATE staging.games 
                        SET action_network_game_id = $1, updated_at = NOW()
                        WHERE id = $2
                    """,
                        action_network_game_id,
                        existing_by_teams["id"],
                    )

                    mapping = self._build_mapping_from_row(existing_by_teams)
                    mapping.action_network_game_id = action_network_game_id
                    mapping.confidence_score = 0.8  # High confidence team/date match

                    await conn.close()
                    return GameResolutionResult(
                        success=True,
                        staging_game_id=existing_by_teams["id"],
                        mapping=mapping,
                        created_new=False,
                    )

                # Create new game if no match found
                new_game_id = await self._create_staging_game(
                    conn,
                    home_team=home_normalized,
                    away_team=away_normalized,
                    game_datetime=game_datetime,
                    action_network=action_network_game_id,
                )

                mapping = GameMapping(
                    staging_game_id=new_game_id,
                    action_network_game_id=action_network_game_id,
                    home_team=home_normalized,
                    away_team=away_normalized,
                    game_datetime=game_datetime,
                    confidence_score=0.6,  # Moderate confidence for new game
                )

                await conn.close()
                return GameResolutionResult(
                    success=True,
                    staging_game_id=new_game_id,
                    mapping=mapping,
                    created_new=True,
                )

            await conn.close()
            return GameResolutionResult(
                success=False,
                error_message=f"Action Network game {action_network_game_id} not found and insufficient data for team matching",
            )

        except Exception as e:
            logger.error(
                "Error resolving game by Action Network ID",
                action_network_game_id=action_network_game_id,
                error=str(e),
            )
            return GameResolutionResult(success=False, error_message=str(e))

    async def resolve_game_by_teams_and_date(
        self,
        home_team: str,
        away_team: str,
        game_datetime: datetime,
        source_id: str = None,
        source_type: str = None,
    ) -> GameResolutionResult:
        """Resolve game by team names and date, optionally adding source ID."""
        try:
            conn = await asyncpg.connect(**self.db_config)

            home_normalized = normalize_team_name(home_team)
            away_normalized = normalize_team_name(away_team)

            # Look for existing game (within 6 hours to handle timezone issues)
            existing = await conn.fetchrow(
                """
                SELECT id, mlb_stats_api_game_id, action_network_game_id, 
                       vsin_game_id, sbd_game_id, sbr_game_id,
                       home_team, away_team, game_datetime
                FROM staging.games 
                WHERE home_team = $1 AND away_team = $2
                AND ABS(EXTRACT(EPOCH FROM (game_datetime - $3))) < 21600  -- 6 hours
                ORDER BY ABS(EXTRACT(EPOCH FROM (game_datetime - $3)))
                LIMIT 1
            """,
                home_normalized,
                away_normalized,
                game_datetime,
            )

            if existing:
                # Update with source ID if provided
                if source_id and source_type:
                    await self._update_source_id(
                        conn, existing["id"], source_type, source_id
                    )

                mapping = self._build_mapping_from_row(existing)
                mapping.confidence_score = 0.9  # High confidence for team/date match

                await conn.close()
                return GameResolutionResult(
                    success=True,
                    staging_game_id=existing["id"],
                    mapping=mapping,
                    created_new=False,
                )

            # Create new game
            source_kwargs = (
                {source_type: source_id} if source_id and source_type else {}
            )
            new_game_id = await self._create_staging_game(
                conn,
                home_team=home_normalized,
                away_team=away_normalized,
                game_datetime=game_datetime,
                **source_kwargs,
            )

            mapping = GameMapping(
                staging_game_id=new_game_id,
                home_team=home_normalized,
                away_team=away_normalized,
                game_datetime=game_datetime,
                confidence_score=0.7,  # Good confidence for new game with team/date
            )

            # Set source ID in mapping
            if source_id and source_type:
                setattr(mapping, f"{source_type}_game_id", source_id)

            await conn.close()
            return GameResolutionResult(
                success=True,
                staging_game_id=new_game_id,
                mapping=mapping,
                created_new=True,
            )

        except Exception as e:
            logger.error(
                "Error resolving game by teams and date",
                home_team=home_team,
                away_team=away_team,
                error=str(e),
            )
            return GameResolutionResult(success=False, error_message=str(e))

    async def update_game_mapping(self, staging_game_id: int, **source_ids) -> bool:
        """Update an existing game mapping with additional source IDs."""
        try:
            conn = await asyncpg.connect(**self.db_config)

            # Build dynamic update query
            updates = []
            params = []
            param_count = 1

            for source_type, source_id in source_ids.items():
                if source_id is not None:
                    updates.append(f"{source_type}_game_id = ${param_count}")
                    params.append(source_id)
                    param_count += 1

            if not updates:
                await conn.close()
                return False

            # Add updated_at
            updates.append(f"updated_at = ${param_count}")
            params.append(now_est())
            param_count += 1

            # Add staging_game_id for WHERE clause
            params.append(staging_game_id)

            query = f"""
                UPDATE staging.games 
                SET {", ".join(updates)}
                WHERE id = ${param_count}
            """

            result = await conn.execute(query, *params)

            await conn.close()

            success = "UPDATE 1" in str(result)
            if success:
                logger.info(
                    "Updated game mapping",
                    staging_game_id=staging_game_id,
                    source_ids=source_ids,
                )

            return success

        except Exception as e:
            logger.error(
                "Error updating game mapping",
                staging_game_id=staging_game_id,
                error=str(e),
            )
            return False

    async def get_games_for_date(self, target_date: date) -> list[GameMapping]:
        """Get all games for a specific date."""
        try:
            conn = await asyncpg.connect(**self.db_config)

            rows = await conn.fetch(
                """
                SELECT id, mlb_stats_api_game_id, action_network_game_id, 
                       vsin_game_id, sbd_game_id, sbr_game_id,
                       home_team, away_team, game_datetime
                FROM staging.games 
                WHERE DATE(game_datetime) = $1
                ORDER BY game_datetime
            """,
                target_date,
            )

            await conn.close()

            return [self._build_mapping_from_row(row) for row in rows]

        except Exception as e:
            logger.error("Error getting games for date", date=target_date, error=str(e))
            return []

    async def _create_staging_game(
        self,
        conn: asyncpg.Connection,
        home_team: str,
        away_team: str,
        game_datetime: datetime,
        **source_ids,
    ) -> int:
        """Create a new staging game entry."""
        # Build dynamic insert query
        columns = [
            "home_team",
            "away_team",
            "game_datetime",
            "created_at",
            "updated_at",
        ]
        values = [home_team, away_team, game_datetime, now_est(), now_est()]
        placeholders = ["$1", "$2", "$3", "$4", "$5"]
        param_count = 6

        for source_type, source_id in source_ids.items():
            if source_id is not None:
                columns.append(f"{source_type}_game_id")
                values.append(source_id)
                placeholders.append(f"${param_count}")
                param_count += 1

        query = f"""
            INSERT INTO staging.games ({", ".join(columns)})
            VALUES ({", ".join(placeholders)})
            RETURNING id
        """

        result = await conn.fetchrow(query, *values)
        game_id = result["id"]

        logger.info(
            "Created new staging game",
            game_id=game_id,
            home_team=home_team,
            away_team=away_team,
        )

        return game_id

    async def _update_source_id(
        self,
        conn: asyncpg.Connection,
        staging_game_id: int,
        source_type: str,
        source_id: str,
    ) -> None:
        """Update a specific source ID for an existing game."""
        query = f"""
            UPDATE staging.games 
            SET {source_type}_game_id = $1, updated_at = $2
            WHERE id = $3
        """

        await conn.execute(query, source_id, now_est(), staging_game_id)

    def _build_mapping_from_row(self, row) -> GameMapping:
        """Build GameMapping object from database row."""
        return GameMapping(
            staging_game_id=row["id"],
            mlb_stats_api_game_id=row["mlb_stats_api_game_id"],
            action_network_game_id=row["action_network_game_id"],
            vsin_game_id=row["vsin_game_id"],
            sbd_game_id=row["sbd_game_id"],
            sbr_game_id=row["sbr_game_id"],
            home_team=row["home_team"],
            away_team=row["away_team"],
            game_datetime=row["game_datetime"],
            confidence_score=1.0,  # Perfect match from database
        )


# Example usage and testing
async def test_game_resolution():
    """Test the game resolution service."""
    service = CrossSiteGameResolutionService()

    # Test resolving by teams and date
    result = await service.resolve_game_by_teams_and_date(
        home_team="Boston Red Sox",
        away_team="New York Yankees",
        game_datetime=datetime.now(),
        source_id="12345",
        source_type="action_network",
    )

    print(f"Resolution result: {result}")

    if result.success:
        # Test updating with another source ID
        updated = await service.update_game_mapping(
            result.staging_game_id, vsin="vsin_game_456"
        )
        print(f"Update result: {updated}")


if __name__ == "__main__":
    asyncio.run(test_game_resolution())
