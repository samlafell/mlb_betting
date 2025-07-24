#!/usr/bin/env python3
"""
Cross-Source Game Matching Service

Automated service for matching games across different data sources using the unified betting lines system.
Ensures consistent game identification across VSIN, SBD, Action Network, and other sources.
"""

import asyncio
import json
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Any

import psycopg2
import structlog
from psycopg2.extras import RealDictCursor

from ..core.config import UnifiedSettings
from ..data.collection.base import DataSource
from .mlb_stats_api_game_resolution_service import (
    MatchConfidence,
    MLBStatsAPIGameResolutionService,
)

logger = structlog.get_logger(__name__)


class MatchingStrategy(Enum):
    """Different strategies for matching games."""

    EXACT_MATCH = "exact_match"
    FUZZY_MATCH = "fuzzy_match"
    DATE_TEAM_MATCH = "date_team_match"
    MLB_API_MATCH = "mlb_api_match"


@dataclass
class GameMatchCandidate:
    """Candidate for game matching."""

    external_id: str
    source: DataSource
    home_team: str
    away_team: str
    game_date: date | None
    game_datetime: datetime | None
    metadata: dict[str, Any] = field(default_factory=dict)
    confidence_score: float = 0.0


@dataclass
class CrossSourceMatch:
    """Result of cross-source game matching."""

    internal_game_id: int | None
    mlb_game_id: str | None
    matched_sources: list[DataSource]
    source_mappings: dict[DataSource, str]  # source -> external_id
    confidence: MatchConfidence
    match_strategy: MatchingStrategy
    match_details: dict[str, Any]
    created_at: datetime = field(default_factory=datetime.now)


class CrossSourceGameMatchingService:
    """
    Service for automated cross-source game matching.

    This service helps maintain consistency across different data sources by:
    1. Matching games from different sources to the same internal game ID
    2. Resolving conflicts when multiple sources report the same game
    3. Maintaining data quality and consistency
    4. Providing unified game identification across the system
    """

    def __init__(self):
        self.settings = UnifiedSettings()
        self.logger = logger.bind(component="CrossSourceGameMatchingService")
        self.mlb_resolution_service = MLBStatsAPIGameResolutionService()
        self.pending_matches = {}  # Cache for pending matches
        self.match_history = {}  # Cache for historical matches

    async def initialize(self):
        """Initialize the service."""
        await self.mlb_resolution_service.initialize()
        await self._load_existing_matches()

    async def cleanup(self):
        """Cleanup resources."""
        await self.mlb_resolution_service.cleanup()

    async def _load_existing_matches(self):
        """Load existing game matches from database."""
        try:
            with psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor,
            ) as conn:
                with conn.cursor() as cur:
                    # Load games with multiple source mappings
                    cur.execute(
                        """
                        SELECT 
                            id,
                            mlb_stats_api_game_id,
                            sportsbookreview_game_id,
                            action_network_game_id,
                            vsin_game_id,
                            sbd_game_id,
                            home_team,
                            away_team,
                            game_date,
                            game_datetime,
                            created_at
                        FROM core_betting.games
                        WHERE mlb_stats_api_game_id IS NOT NULL
                        AND game_date >= %s
                        ORDER BY game_date DESC
                    """,
                        (date.today() - timedelta(days=30),),
                    )

                    games = cur.fetchall()

                    for game in games:
                        # Build source mappings
                        source_mappings = {}
                        if game["sportsbookreview_game_id"]:
                            source_mappings[
                                DataSource.SPORTS_BOOK_REVIEW_DEPRECATED
                            ] = game["sportsbookreview_game_id"]
                        if game["action_network_game_id"]:
                            source_mappings[DataSource.ACTION_NETWORK] = str(
                                game["action_network_game_id"]
                            )
                        if game["vsin_game_id"]:
                            source_mappings[DataSource.VSIN] = game["vsin_game_id"]
                        if game["sbd_game_id"]:
                            source_mappings[DataSource.SPORTS_BETTING_DIME] = game[
                                "sbd_game_id"
                            ]

                        if len(source_mappings) > 1:
                            match = CrossSourceMatch(
                                internal_game_id=game["id"],
                                mlb_game_id=game["mlb_stats_api_game_id"],
                                matched_sources=list(source_mappings.keys()),
                                source_mappings=source_mappings,
                                confidence=MatchConfidence.HIGH,
                                match_strategy=MatchingStrategy.EXACT_MATCH,
                                match_details={
                                    "home_team": game["home_team"],
                                    "away_team": game["away_team"],
                                    "game_date": game["game_date"].isoformat()
                                    if game["game_date"]
                                    else None,
                                    "loaded_from_database": True,
                                },
                                created_at=game["created_at"],
                            )

                            self.match_history[game["mlb_stats_api_game_id"]] = match

                    self.logger.info(
                        f"Loaded {len(self.match_history)} existing cross-source matches"
                    )

        except Exception as e:
            self.logger.error("Error loading existing matches", error=str(e))

    async def match_games_for_date(self, target_date: date) -> list[CrossSourceMatch]:
        """
        Match games across all sources for a specific date.

        Args:
            target_date: Date to match games for

        Returns:
            List of CrossSourceMatch objects
        """
        try:
            # Collect candidates from all sources
            candidates = await self._collect_game_candidates(target_date)

            if not candidates:
                self.logger.info(f"No game candidates found for date {target_date}")
                return []

            # Group candidates by potential matches
            candidate_groups = self._group_candidates_by_similarity(candidates)

            # Match each group
            matches = []
            for group in candidate_groups:
                try:
                    match = await self._match_candidate_group(group)
                    if match:
                        matches.append(match)
                except Exception as e:
                    self.logger.error("Error matching candidate group", error=str(e))
                    continue

            # Store successful matches
            for match in matches:
                await self._store_cross_source_match(match)

            self.logger.info(
                f"Successfully matched {len(matches)} games for date {target_date}"
            )
            return matches

        except Exception as e:
            self.logger.error(
                "Error matching games for date", date=target_date, error=str(e)
            )
            return []

    async def _collect_game_candidates(
        self, target_date: date
    ) -> list[GameMatchCandidate]:
        """Collect game candidates from all sources for a specific date."""
        candidates = []

        try:
            with psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor,
            ) as conn:
                with conn.cursor() as cur:
                    # Collect from betting lines tables
                    sources_queries = [
                        (
                            DataSource.SPORTS_BOOK_REVIEW_DEPRECATED,
                            "core_betting.betting_lines_moneyline",
                            "sportsbookreview_game_id",
                        ),
                        (
                            DataSource.ACTION_NETWORK,
                            "core_betting.betting_lines_moneyline",
                            "action_network_game_id",
                        ),
                        (
                            DataSource.VSIN,
                            "core_betting.betting_lines_moneyline",
                            "vsin_game_id",
                        ),
                        (
                            DataSource.SPORTS_BETTING_DIME,
                            "core_betting.betting_lines_moneyline",
                            "sbd_game_id",
                        ),
                    ]

                    for source, table, id_field in sources_queries:
                        try:
                            cur.execute(
                                f"""
                                SELECT DISTINCT
                                    external_source_id,
                                    home_team,
                                    away_team,
                                    game_datetime,
                                    source_metadata,
                                    COUNT(*) as record_count
                                FROM {table}
                                WHERE source = %s
                                AND DATE(game_datetime) = %s
                                AND external_source_id IS NOT NULL
                                GROUP BY external_source_id, home_team, away_team, game_datetime, source_metadata
                            """,
                                (source.value, target_date),
                            )

                            results = cur.fetchall()

                            for row in results:
                                candidate = GameMatchCandidate(
                                    external_id=row["external_source_id"],
                                    source=source,
                                    home_team=row["home_team"],
                                    away_team=row["away_team"],
                                    game_date=target_date,
                                    game_datetime=row["game_datetime"],
                                    metadata=row["source_metadata"] or {},
                                    confidence_score=min(
                                        1.0, row["record_count"] / 10.0
                                    ),  # More records = higher confidence
                                )
                                candidates.append(candidate)

                        except Exception as e:
                            self.logger.warning(
                                f"Error collecting candidates from {source.value}",
                                error=str(e),
                            )
                            continue

                    self.logger.info(
                        f"Collected {len(candidates)} game candidates for {target_date}"
                    )
                    return candidates

        except Exception as e:
            self.logger.error("Error collecting game candidates", error=str(e))
            return []

    def _group_candidates_by_similarity(
        self, candidates: list[GameMatchCandidate]
    ) -> list[list[GameMatchCandidate]]:
        """Group candidates that likely represent the same game."""
        groups = []
        used_candidates = set()

        for i, candidate in enumerate(candidates):
            if i in used_candidates:
                continue

            # Start a new group with this candidate
            group = [candidate]
            used_candidates.add(i)

            # Find similar candidates
            for j, other_candidate in enumerate(candidates):
                if j in used_candidates or i == j:
                    continue

                if self._candidates_are_similar(candidate, other_candidate):
                    group.append(other_candidate)
                    used_candidates.add(j)

            groups.append(group)

        return groups

    def _candidates_are_similar(
        self, candidate1: GameMatchCandidate, candidate2: GameMatchCandidate
    ) -> bool:
        """Determine if two candidates represent the same game."""
        # Different sources can represent the same game
        if candidate1.source == candidate2.source:
            return False

        # Standardize team names for comparison
        home1 = self.mlb_resolution_service.standardize_team_name(candidate1.home_team)
        away1 = self.mlb_resolution_service.standardize_team_name(candidate1.away_team)
        home2 = self.mlb_resolution_service.standardize_team_name(candidate2.home_team)
        away2 = self.mlb_resolution_service.standardize_team_name(candidate2.away_team)

        # Must have valid team names
        if not all([home1, away1, home2, away2]):
            return False

        # Same teams playing
        if home1 == home2 and away1 == away2:
            return True

        # Check if game times are close (within 4 hours)
        if (
            candidate1.game_datetime
            and candidate2.game_datetime
            and abs(
                (candidate1.game_datetime - candidate2.game_datetime).total_seconds()
            )
            < 4 * 3600
        ):
            return True

        # Same date and teams
        if (
            candidate1.game_date == candidate2.game_date
            and home1 == home2
            and away1 == away2
        ):
            return True

        return False

    async def _match_candidate_group(
        self, candidates: list[GameMatchCandidate]
    ) -> CrossSourceMatch | None:
        """Match a group of candidates to create a cross-source match."""
        if not candidates:
            return None

        try:
            # Use the candidate with the highest confidence as the primary
            primary_candidate = max(candidates, key=lambda c: c.confidence_score)

            # Try to resolve with MLB Stats API
            resolution_result = await self.mlb_resolution_service.resolve_game_id(
                external_id=primary_candidate.external_id,
                source=primary_candidate.source,
                home_team=primary_candidate.home_team,
                away_team=primary_candidate.away_team,
                game_date=primary_candidate.game_date,
            )

            if resolution_result.game_id:
                # Found existing game
                match_strategy = MatchingStrategy.EXACT_MATCH
                internal_game_id = resolution_result.game_id
                mlb_game_id = resolution_result.mlb_game_id
                confidence = resolution_result.confidence

            elif resolution_result.mlb_game_id:
                # Found MLB game but need to create internal game
                match_strategy = MatchingStrategy.MLB_API_MATCH
                internal_game_id = await self._create_internal_game(
                    resolution_result, primary_candidate
                )
                mlb_game_id = resolution_result.mlb_game_id
                confidence = resolution_result.confidence

            else:
                # Create new game based on candidate data
                match_strategy = MatchingStrategy.FUZZY_MATCH
                internal_game_id = await self._create_game_from_candidate(
                    primary_candidate
                )
                mlb_game_id = None
                confidence = MatchConfidence.MEDIUM

            if not internal_game_id:
                return None

            # Build source mappings
            source_mappings = {}
            matched_sources = []

            for candidate in candidates:
                source_mappings[candidate.source] = candidate.external_id
                matched_sources.append(candidate.source)

            # Update database with all source mappings
            await self._update_game_with_source_mappings(
                internal_game_id, source_mappings
            )

            # Create match result
            match = CrossSourceMatch(
                internal_game_id=internal_game_id,
                mlb_game_id=mlb_game_id,
                matched_sources=matched_sources,
                source_mappings=source_mappings,
                confidence=confidence,
                match_strategy=match_strategy,
                match_details={
                    "primary_candidate": {
                        "external_id": primary_candidate.external_id,
                        "source": primary_candidate.source.value,
                        "home_team": primary_candidate.home_team,
                        "away_team": primary_candidate.away_team,
                        "game_date": primary_candidate.game_date.isoformat()
                        if primary_candidate.game_date
                        else None,
                        "confidence_score": primary_candidate.confidence_score,
                    },
                    "total_candidates": len(candidates),
                    "resolution_method": resolution_result.match_method,
                },
            )

            return match

        except Exception as e:
            self.logger.error("Error matching candidate group", error=str(e))
            return None

    async def _create_internal_game(
        self, resolution_result, primary_candidate: GameMatchCandidate
    ) -> int | None:
        """Create internal game record from MLB resolution result."""
        try:
            with psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor,
            ) as conn:
                with conn.cursor() as cur:
                    # Standardize team names
                    home_team = self.mlb_resolution_service.standardize_team_name(
                        primary_candidate.home_team
                    )
                    away_team = self.mlb_resolution_service.standardize_team_name(
                        primary_candidate.away_team
                    )

                    if not home_team or not away_team:
                        return None

                    cur.execute(
                        """
                        INSERT INTO core_betting.games 
                        (mlb_stats_api_game_id, home_team, away_team, game_date, game_datetime, 
                         data_quality, has_mlb_enrichment, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                        RETURNING id
                    """,
                        (
                            resolution_result.mlb_game_id,
                            home_team,
                            away_team,
                            primary_candidate.game_date,
                            primary_candidate.game_datetime,
                            "HIGH"
                            if resolution_result.confidence == MatchConfidence.HIGH
                            else "MEDIUM",
                            True,
                        ),
                    )

                    result = cur.fetchone()
                    conn.commit()

                    return result["id"]

        except Exception as e:
            self.logger.error("Error creating internal game", error=str(e))
            return None

    async def _create_game_from_candidate(
        self, candidate: GameMatchCandidate
    ) -> int | None:
        """Create game record from candidate data."""
        try:
            with psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor,
            ) as conn:
                with conn.cursor() as cur:
                    # Standardize team names
                    home_team = self.mlb_resolution_service.standardize_team_name(
                        candidate.home_team
                    )
                    away_team = self.mlb_resolution_service.standardize_team_name(
                        candidate.away_team
                    )

                    if not home_team or not away_team:
                        return None

                    cur.execute(
                        """
                        INSERT INTO core_betting.games 
                        (home_team, away_team, game_date, game_datetime, data_quality, has_mlb_enrichment, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                        RETURNING id
                    """,
                        (
                            home_team,
                            away_team,
                            candidate.game_date,
                            candidate.game_datetime,
                            "MEDIUM",
                            False,
                        ),
                    )

                    result = cur.fetchone()
                    conn.commit()

                    return result["id"]

        except Exception as e:
            self.logger.error("Error creating game from candidate", error=str(e))
            return None

    async def _update_game_with_source_mappings(
        self, game_id: int, source_mappings: dict[DataSource, str]
    ):
        """Update game record with source mappings."""
        try:
            with psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor,
            ) as conn:
                with conn.cursor() as cur:
                    # Build update query dynamically
                    update_fields = []
                    values = []

                    for source, external_id in source_mappings.items():
                        if source == DataSource.SPORTS_BOOK_REVIEW_DEPRECATED:
                            update_fields.append("sportsbookreview_game_id = %s")
                        elif source == DataSource.ACTION_NETWORK:
                            update_fields.append("action_network_game_id = %s")
                        elif source == DataSource.VSIN:
                            update_fields.append("vsin_game_id = %s")
                        elif source == DataSource.SPORTS_BETTING_DIME:
                            update_fields.append("sbd_game_id = %s")
                        else:
                            continue

                        values.append(external_id)

                    if update_fields:
                        update_fields.append("updated_at = NOW()")
                        values.append(game_id)

                        query = f"UPDATE core_betting.games SET {', '.join(update_fields)} WHERE id = %s"
                        cur.execute(query, values)
                        conn.commit()

        except Exception as e:
            self.logger.error("Error updating game with source mappings", error=str(e))

    async def _store_cross_source_match(self, match: CrossSourceMatch):
        """Store cross-source match for future reference."""
        try:
            # Store in cache
            if match.mlb_game_id:
                self.match_history[match.mlb_game_id] = match

            # Could also store in database table for audit trail
            # This is optional but useful for debugging and analytics

        except Exception as e:
            self.logger.error("Error storing cross-source match", error=str(e))

    async def find_unmatched_games(self, days_back: int = 7) -> list[dict[str, Any]]:
        """Find games that haven't been matched across sources."""
        try:
            with psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor,
            ) as conn:
                with conn.cursor() as cur:
                    # Find games with only one source
                    cur.execute(
                        """
                        SELECT 
                            id,
                            home_team,
                            away_team,
                            game_date,
                            sportsbookreview_game_id,
                            action_network_game_id,
                            vsin_game_id,
                            sbd_game_id,
                            mlb_stats_api_game_id,
                            created_at
                        FROM core_betting.games
                        WHERE game_date >= %s
                        AND (
                            (sportsbookreview_game_id IS NOT NULL)::int +
                            (action_network_game_id IS NOT NULL)::int +
                            (vsin_game_id IS NOT NULL)::int +
                            (sbd_game_id IS NOT NULL)::int
                        ) = 1
                        ORDER BY game_date DESC
                    """,
                        (date.today() - timedelta(days=days_back),),
                    )

                    unmatched_games = []
                    for row in cur.fetchall():
                        # Determine which source this game came from
                        source = None
                        external_id = None

                        if row["sportsbookreview_game_id"]:
                            source = DataSource.SPORTS_BOOK_REVIEW_DEPRECATED
                            external_id = row["sportsbookreview_game_id"]
                        elif row["action_network_game_id"]:
                            source = DataSource.ACTION_NETWORK
                            external_id = str(row["action_network_game_id"])
                        elif row["vsin_game_id"]:
                            source = DataSource.VSIN
                            external_id = row["vsin_game_id"]
                        elif row["sbd_game_id"]:
                            source = DataSource.SPORTS_BETTING_DIME
                            external_id = row["sbd_game_id"]

                        unmatched_games.append(
                            {
                                "internal_game_id": row["id"],
                                "home_team": row["home_team"],
                                "away_team": row["away_team"],
                                "game_date": row["game_date"],
                                "source": source.value if source else None,
                                "external_id": external_id,
                                "mlb_game_id": row["mlb_stats_api_game_id"],
                                "created_at": row["created_at"],
                            }
                        )

                    return unmatched_games

        except Exception as e:
            self.logger.error("Error finding unmatched games", error=str(e))
            return []

    async def get_matching_statistics(self, days_back: int = 30) -> dict[str, Any]:
        """Get statistics about cross-source matching performance."""
        try:
            with psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor,
            ) as conn:
                with conn.cursor() as cur:
                    # Get matching statistics
                    cur.execute(
                        """
                        SELECT 
                            COUNT(*) as total_games,
                            COUNT(CASE WHEN mlb_stats_api_game_id IS NOT NULL THEN 1 END) as mlb_matched,
                            COUNT(CASE WHEN 
                                (sportsbookreview_game_id IS NOT NULL)::int +
                                (action_network_game_id IS NOT NULL)::int +
                                (vsin_game_id IS NOT NULL)::int +
                                (sbd_game_id IS NOT NULL)::int >= 2 
                            THEN 1 END) as cross_source_matched,
                            COUNT(DISTINCT sportsbookreview_game_id) as sbr_games,
                            COUNT(DISTINCT action_network_game_id) as action_network_games,
                            COUNT(DISTINCT vsin_game_id) as vsin_games,
                            COUNT(DISTINCT sbd_game_id) as sbd_games,
                            AVG(data_quality) as avg_quality
                        FROM core_betting.games
                        WHERE game_date >= %s
                    """,
                        (date.today() - timedelta(days=days_back),),
                    )

                    stats = cur.fetchone()

                    # Calculate percentages
                    total_games = stats["total_games"] or 1

                    return {
                        "total_games": stats["total_games"],
                        "mlb_matched": stats["mlb_matched"],
                        "mlb_match_percentage": (stats["mlb_matched"] / total_games)
                        * 100,
                        "cross_source_matched": stats["cross_source_matched"],
                        "cross_source_match_percentage": (
                            stats["cross_source_matched"] / total_games
                        )
                        * 100,
                        "source_counts": {
                            "sports_book_review_deprecated": stats["sbr_games"],
                            "action_network": stats["action_network_games"],
                            "vsin": stats["vsin_games"],
                            "sports_betting_dime": stats["sbd_games"],
                        },
                        "cache_size": len(self.match_history),
                        "days_analyzed": days_back,
                    }

        except Exception as e:
            self.logger.error("Error getting matching statistics", error=str(e))
            return {}

    async def run_daily_matching(self, target_date: date = None) -> dict[str, Any]:
        """
        Run daily matching process for all sources.

        Args:
            target_date: Date to run matching for (default: today)

        Returns:
            Dictionary with matching results and statistics
        """
        if not target_date:
            target_date = date.today()

        try:
            self.logger.info(f"Starting daily matching for {target_date}")

            # Run matching for the target date
            matches = await self.match_games_for_date(target_date)

            # Get statistics
            stats = await self.get_matching_statistics(days_back=1)

            # Find unmatched games
            unmatched = await self.find_unmatched_games(days_back=1)

            result = {
                "date": target_date.isoformat(),
                "matches_created": len(matches),
                "matches": [
                    {
                        "internal_game_id": match.internal_game_id,
                        "mlb_game_id": match.mlb_game_id,
                        "sources": [s.value for s in match.matched_sources],
                        "confidence": match.confidence.value,
                        "strategy": match.match_strategy.value,
                    }
                    for match in matches
                ],
                "statistics": stats,
                "unmatched_games": len(unmatched),
                "unmatched_details": unmatched[:5],  # First 5 for brevity
            }

            self.logger.info(
                f"Daily matching completed for {target_date}",
                matches=len(matches),
                unmatched=len(unmatched),
            )

            return result

        except Exception as e:
            self.logger.error("Error in daily matching", date=target_date, error=str(e))
            return {
                "date": target_date.isoformat(),
                "error": str(e),
                "matches_created": 0,
            }


# Example usage
if __name__ == "__main__":

    async def main():
        service = CrossSourceGameMatchingService()
        await service.initialize()

        try:
            # Run daily matching for today
            result = await service.run_daily_matching()
            print(f"Daily matching result: {json.dumps(result, indent=2)}")

            # Get statistics
            stats = await service.get_matching_statistics()
            print(f"Matching statistics: {json.dumps(stats, indent=2)}")

            # Find unmatched games
            unmatched = await service.find_unmatched_games()
            print(f"Found {len(unmatched)} unmatched games")

        finally:
            await service.cleanup()

    asyncio.run(main())
