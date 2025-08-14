#!/usr/bin/env python3
"""
Game ID Resolution Service

Integrates the MLB Stats API Game Resolution Service with the outcome checking system
to populate missing MLB Stats API game IDs and enable comprehensive outcome coverage.

This service:
1. Identifies games missing MLB Stats API IDs
2. Uses the MLB Stats API Game Resolution Service to resolve them
3. Updates the curated.games_complete table with resolved IDs
4. Enables outcome checking for previously unresolvable games
"""

import asyncio
from datetime import datetime, timedelta
from typing import Any

import structlog

from ..core.config import get_settings
from ..data.collection.base import DataSource
from ..data.database.connection import get_connection
from ..services.mlb_stats_api_game_resolution_service import (
    MatchConfidence,
    MLBStatsAPIGameResolutionService,
)

logger = structlog.get_logger(__name__)


class GameIDResolutionService:
    """
    Service for resolving missing MLB Stats API game IDs.

    Integrates with the MLB Stats API Game Resolution Service to populate
    missing game IDs in the curated.games_complete table, enabling comprehensive
    game outcome coverage.
    """

    def __init__(self):
        """Initialize the game ID resolution service."""
        self.settings = get_settings()
        self.logger = logger.bind(service="GameIDResolutionService")
        self.mlb_resolution_service = MLBStatsAPIGameResolutionService()

    async def initialize(self):
        """Initialize the service and underlying MLB resolution service."""
        await self.mlb_resolution_service.initialize()
        self.logger.info("GameIDResolutionService initialized")

    async def cleanup(self):
        """Cleanup resources."""
        await self.mlb_resolution_service.cleanup()

    async def resolve_missing_game_ids(
        self, days: int = 30, source_filter: str | None = None, dry_run: bool = False
    ) -> dict[str, Any]:
        """
        Resolve missing MLB Stats API game IDs.

        Args:
            days: Number of days to look back for games
            source_filter: Optional source filter (action_network, sbr, vsin)
            dry_run: If True, don't update database, just report what would be done

        Returns:
            Dictionary with resolution results and statistics
        """
        self.logger.info(
            "Starting game ID resolution",
            days=days,
            source_filter=source_filter,
            dry_run=dry_run,
        )

        results = {
            "processed_games": 0,
            "resolved_games": 0,
            "failed_resolutions": 0,
            "skipped_games": 0,
            "errors": [],
            "resolutions": [],
        }

        try:
            # Get games missing MLB Stats API IDs
            missing_games = await self._get_games_missing_mlb_ids(days, source_filter)

            self.logger.info("Found games missing MLB IDs", count=len(missing_games))

            if not missing_games:
                self.logger.info("No games missing MLB Stats API IDs")
                return results

            # Process each game
            for game_info in missing_games:
                results["processed_games"] += 1

                try:
                    # Determine source for resolution
                    source = self._determine_data_source(game_info)

                    if not source:
                        results["skipped_games"] += 1
                        self.logger.warning(
                            "Could not determine data source for game",
                            game_id=game_info["id"],
                        )
                        continue

                    # Resolve game ID
                    resolution_result = (
                        await self.mlb_resolution_service.resolve_game_id(
                            external_id=self._get_external_id(game_info, source),
                            source=source,
                            home_team=game_info["home_team"],
                            away_team=game_info["away_team"],
                            game_date=game_info["game_date"],
                        )
                    )

                    if (
                        resolution_result.mlb_game_id
                        and resolution_result.confidence != MatchConfidence.NONE
                    ):
                        # Update database with resolved MLB game ID
                        if not dry_run:
                            await self._update_game_with_mlb_id(
                                game_info["id"], resolution_result.mlb_game_id
                            )

                        results["resolved_games"] += 1
                        results["resolutions"].append(
                            {
                                "game_id": game_info["id"],
                                "mlb_game_id": resolution_result.mlb_game_id,
                                "confidence": resolution_result.confidence.value,
                                "method": resolution_result.match_method,
                                "home_team": game_info["home_team"],
                                "away_team": game_info["away_team"],
                                "game_date": game_info["game_date"].isoformat()
                                if game_info["game_date"]
                                else None,
                                "source": source.value,
                            }
                        )

                        self.logger.info(
                            "Resolved MLB game ID",
                            game_id=game_info["id"],
                            mlb_game_id=resolution_result.mlb_game_id,
                            confidence=resolution_result.confidence.value,
                            method=resolution_result.match_method,
                        )
                    else:
                        results["failed_resolutions"] += 1
                        self.logger.warning(
                            "Failed to resolve MLB game ID",
                            game_id=game_info["id"],
                            home_team=game_info["home_team"],
                            away_team=game_info["away_team"],
                            confidence=resolution_result.confidence.value,
                            method=resolution_result.match_method,
                        )

                except Exception as e:
                    error_msg = f"Error resolving game {game_info.get('id', 'unknown')}: {str(e)}"
                    results["errors"].append(error_msg)
                    self.logger.error(
                        "Game resolution error",
                        game_id=game_info.get("id"),
                        error=str(e),
                    )

            self.logger.info("Game ID resolution completed", results=results)
            return results

        except Exception as e:
            self.logger.error("Game ID resolution failed", error=str(e))
            results["errors"].append(f"Service error: {str(e)}")
            return results

    async def _get_games_missing_mlb_ids(
        self, days: int, source_filter: str | None = None
    ) -> list[dict[str, Any]]:
        """
        Get games missing MLB Stats API IDs.

        Args:
            days: Number of days to look back
            source_filter: Optional source filter

        Returns:
            List of game dictionaries
        """
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        # Build query based on source filter
        where_conditions = [
            "g.game_date BETWEEN $1 AND $2",
            "g.mlb_stats_api_game_id IS NULL",
        ]
        params = [start_date.date(), end_date.date()]

        if source_filter == "action_network":
            where_conditions.append("g.action_network_game_id IS NOT NULL")
        elif source_filter == "sbr":
            where_conditions.append("g.sportsbookreview_game_id IS NOT NULL")
        elif source_filter == "vsin":
            where_conditions.append("g.vsin_game_id IS NOT NULL")

        query = f"""
        SELECT 
            g.id, g.home_team, g.away_team, g.game_date, g.game_datetime,
            g.action_network_game_id, g.sportsbookreview_game_id, 
            g.vsin_game_id
        FROM curated.games_complete g
        WHERE {" AND ".join(where_conditions)}
        ORDER BY g.game_date DESC
        """

        try:
            async with get_connection() as conn:
                rows = await conn.fetch(query, *params)

                games = []
                for row in rows:
                    games.append(
                        {
                            "id": row["id"],
                            "home_team": row["home_team"],
                            "away_team": row["away_team"],
                            "game_date": row["game_date"],
                            "game_datetime": row["game_datetime"],
                            "action_network_game_id": row["action_network_game_id"],
                            "sbd_game_id": row["sbd_game_id"],
                            "vsin_game_id": row["vsin_game_id"],
                        }
                    )

                return games

        except Exception as e:
            self.logger.error("Database query error", error=str(e))
            return []

    def _determine_data_source(self, game_info: dict[str, Any]) -> DataSource | None:
        """
        Determine the data source for a game based on available external IDs.

        Args:
            game_info: Game information dictionary

        Returns:
            DataSource enum value or None
        """
        if game_info.get("action_network_game_id"):
            return DataSource.ACTION_NETWORK
        elif game_info.get("sbd_game_id"):
            return DataSource.SBD
        elif game_info.get("vsin_game_id"):
            return DataSource.VSIN

        return None

    def _get_external_id(self, game_info: dict[str, Any], source: DataSource) -> str:
        """
        Get the external ID for a game based on the source.

        Args:
            game_info: Game information dictionary
            source: Data source

        Returns:
            External ID string
        """
        source_mapping = {
            DataSource.ACTION_NETWORK: "action_network_game_id",
            DataSource.SBD: "sbd_game_id",
            DataSource.VSIN: "vsin_game_id",
        }

        field_name = source_mapping.get(source)
        if field_name:
            return str(game_info[field_name])

        raise ValueError(f"Unknown data source: {source}")

    async def _update_game_with_mlb_id(self, game_id: int, mlb_game_id: str) -> None:
        """
        Update a game with its resolved MLB Stats API game ID.

        Args:
            game_id: Game ID from curated.games_complete
            mlb_game_id: Resolved MLB Stats API game ID
        """
        query = """
        UPDATE curated.games_complete 
        SET 
            mlb_stats_api_game_id = $1,
            has_mlb_enrichment = TRUE,
            data_quality = 'HIGH',
            updated_at = NOW()
        WHERE id = $2
        """

        try:
            async with get_connection() as conn:
                await conn.execute(query, mlb_game_id, game_id)

            self.logger.debug(
                "Updated game with MLB ID", game_id=game_id, mlb_game_id=mlb_game_id
            )

        except Exception as e:
            self.logger.error(
                "Error updating game with MLB ID",
                game_id=game_id,
                mlb_game_id=mlb_game_id,
                error=str(e),
            )
            raise

    async def get_resolution_stats(self, days: int = 30) -> dict[str, Any]:
        """
        Get statistics about game ID resolution coverage.

        Args:
            days: Number of days to analyze

        Returns:
            Dictionary with resolution statistics
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        query = """
        SELECT 
            COUNT(*) as total_games,
            COUNT(g.mlb_stats_api_game_id) as games_with_mlb_id,
            COUNT(g.action_network_game_id) as action_network_games,
            COUNT(g.sportsbookreview_game_id) as sbr_games,
            COUNT(g.vsin_game_id) as vsin_games,
            COUNT(CASE WHEN g.mlb_stats_api_game_id IS NULL 
                       AND (g.action_network_game_id IS NOT NULL 
                            OR g.sportsbookreview_game_id IS NOT NULL 
                            OR g.vsin_game_id IS NOT NULL) 
                  THEN 1 END) as resolvable_missing
        FROM curated.games_complete g
        WHERE g.game_date BETWEEN $1 AND $2
        """

        try:
            async with get_connection() as conn:
                row = await conn.fetchrow(query, start_date.date(), end_date.date())

                if row:
                    total_games = row["total_games"]
                    games_with_mlb_id = row["games_with_mlb_id"]
                    resolvable_missing = row["resolvable_missing"]

                    coverage_percentage = (
                        (games_with_mlb_id / total_games * 100)
                        if total_games > 0
                        else 0
                    )
                    potential_coverage = (
                        ((games_with_mlb_id + resolvable_missing) / total_games * 100)
                        if total_games > 0
                        else 0
                    )

                    return {
                        "total_games": total_games,
                        "games_with_mlb_id": games_with_mlb_id,
                        "missing_mlb_ids": total_games - games_with_mlb_id,
                        "resolvable_missing": resolvable_missing,
                        "coverage_percentage": coverage_percentage,
                        "potential_coverage_percentage": potential_coverage,
                        "improvement_potential": potential_coverage
                        - coverage_percentage,
                        "source_breakdown": {
                            "action_network": row["action_network_games"],
                            "sbr": row["sbr_games"],
                            "vsin": row["vsin_games"],
                        },
                    }
                else:
                    return {
                        "total_games": 0,
                        "games_with_mlb_id": 0,
                        "missing_mlb_ids": 0,
                        "resolvable_missing": 0,
                        "coverage_percentage": 0,
                        "potential_coverage_percentage": 0,
                        "improvement_potential": 0,
                        "source_breakdown": {},
                    }

        except Exception as e:
            self.logger.error("Error getting resolution stats", error=str(e))
            return {}


# Service instance for easy importing
game_id_resolution_service = GameIDResolutionService()


async def resolve_missing_game_ids(
    days: int = 30, source_filter: str | None = None, dry_run: bool = False
) -> dict[str, Any]:
    """
    Convenience function to resolve missing game IDs.

    Args:
        days: Number of days to look back
        source_filter: Optional source filter
        dry_run: If True, don't update database

    Returns:
        Dictionary with resolution results
    """
    await game_id_resolution_service.initialize()
    try:
        return await game_id_resolution_service.resolve_missing_game_ids(
            days=days, source_filter=source_filter, dry_run=dry_run
        )
    finally:
        await game_id_resolution_service.cleanup()


if __name__ == "__main__":
    # Example usage
    async def main():
        # Resolve missing IDs for the last 30 days
        results = await resolve_missing_game_ids(days=30, dry_run=True)
        print(f"Resolution results: {results}")

        # Get resolution stats
        await game_id_resolution_service.initialize()
        try:
            stats = await game_id_resolution_service.get_resolution_stats(days=30)
            print(f"Resolution stats: {stats}")
        finally:
            await game_id_resolution_service.cleanup()

    asyncio.run(main())
