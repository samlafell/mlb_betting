"""
Game Outcome Repository for MLB Sharp Betting System.

This module provides data access functionality for game outcomes with PostgreSQL compatibility.
"""

from datetime import datetime

import structlog

from ..core.exceptions import MLBSharpBettingError
from ..models.game_outcome import GameOutcome, Team

logger = structlog.get_logger(__name__)


class GameOutcomeRepositoryError(MLBSharpBettingError):
    """Exception raised for game outcome repository errors."""

    pass


class GameOutcomeRepository:
    """Repository for game outcome data operations with PostgreSQL support."""

    def __init__(self, db_manager=None):
        """Initialize repository with PostgreSQL database manager."""
        self.table_name = "public.game_outcomes"
        self.db_manager = db_manager

        # Get the PostgreSQL database manager if not provided
        if self.db_manager is None:
            from ..db.connection import get_db_manager

            self.db_manager = get_db_manager()

        logger.debug("GameOutcomeRepository initialized with PostgreSQL support")

    def _ensure_table_exists(self, cursor) -> None:
        """Ensure the game outcomes table exists using PostgreSQL syntax."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            game_id VARCHAR PRIMARY KEY,
            home_team VARCHAR NOT NULL,
            away_team VARCHAR NOT NULL,
            home_score INTEGER NOT NULL,
            away_score INTEGER NOT NULL,
            over BOOLEAN NOT NULL,
            home_win BOOLEAN NOT NULL,
            home_cover_spread BOOLEAN,
            total_line DOUBLE PRECISION,
            home_spread_line DOUBLE PRECISION,
            game_date TIMESTAMP,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
        )
        """

        try:
            # Execute DDL statement with PostgreSQL cursor
            cursor.execute(create_table_sql)
            logger.debug("Game outcomes table created/verified", table=self.table_name)
        except Exception as e:
            logger.error("Failed to create game outcomes table", error=str(e))
            raise GameOutcomeRepositoryError(f"Failed to create table: {str(e)}")

    async def create_outcome(self, outcome: GameOutcome) -> GameOutcome:
        """Create a new game outcome record using PostgreSQL."""
        if not self.db_manager:
            raise GameOutcomeRepositoryError("Database manager not initialized")

        insert_sql = f"""
        INSERT INTO {self.table_name} (
            game_id, home_team, away_team, home_score, away_score,
            over, home_win, home_cover_spread, total_line, home_spread_line,
            game_date, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING *
        """

        try:
            with self.db_manager.get_cursor() as cursor:
                self._ensure_table_exists(cursor)

                cursor.execute(
                    insert_sql,
                    [
                        outcome.game_id,
                        outcome.home_team.value,
                        outcome.away_team.value,
                        outcome.home_score,
                        outcome.away_score,
                        outcome.over,
                        outcome.home_win,
                        outcome.home_cover_spread,
                        outcome.total_line,
                        outcome.home_spread_line,
                        outcome.game_date,
                        outcome.created_at,
                        outcome.updated_at,
                    ],
                )

                logger.info(
                    "Game outcome created",
                    game_id=outcome.game_id,
                    home_team=outcome.home_team.value,
                    away_team=outcome.away_team.value,
                )

                return outcome

        except Exception as e:
            logger.error(
                "Failed to create game outcome", game_id=outcome.game_id, error=str(e)
            )
            raise GameOutcomeRepositoryError(f"Failed to create outcome: {str(e)}")

    async def get_outcome_by_game_id(self, game_id: str) -> GameOutcome | None:
        """Get a game outcome by game ID using PostgreSQL."""
        if not self.db_manager:
            raise GameOutcomeRepositoryError("Database manager not initialized")

        select_sql = f"""
        SELECT * FROM {self.table_name} 
        WHERE game_id = %s
        """

        try:
            with self.db_manager.get_cursor() as cursor:
                self._ensure_table_exists(cursor)

                cursor.execute(select_sql, [game_id])
                result = cursor.fetchone()

                if result:
                    return self._row_to_outcome(result)
                return None

        except Exception as e:
            logger.error("Failed to get game outcome", game_id=game_id, error=str(e))
            raise GameOutcomeRepositoryError(f"Failed to get outcome: {str(e)}")

    async def get_outcomes_by_teams(
        self, home_team: Team, away_team: Team
    ) -> list[GameOutcome]:
        """Get all outcomes for a specific team matchup using PostgreSQL."""
        if not self.db_manager:
            raise GameOutcomeRepositoryError("Database manager not initialized")

        select_sql = f"""
        SELECT * FROM {self.table_name} 
        WHERE home_team = %s AND away_team = %s
        ORDER BY game_date DESC
        """

        try:
            with self.db_manager.get_cursor() as cursor:
                self._ensure_table_exists(cursor)

                cursor.execute(select_sql, [home_team.value, away_team.value])
                results = cursor.fetchall()

                return [self._row_to_outcome(row) for row in results]

        except Exception as e:
            logger.error(
                "Failed to get outcomes by teams",
                home_team=home_team.value,
                away_team=away_team.value,
                error=str(e),
            )
            raise GameOutcomeRepositoryError(f"Failed to get outcomes: {str(e)}")

    async def get_recent_outcomes(self, limit: int = 100) -> list[GameOutcome]:
        """Get recent game outcomes using PostgreSQL."""
        if not self.db_manager:
            raise GameOutcomeRepositoryError("Database manager not initialized")

        select_sql = f"""
        SELECT * FROM {self.table_name} 
        ORDER BY game_date DESC, created_at DESC
        LIMIT %s
        """

        try:
            with self.db_manager.get_cursor() as cursor:
                self._ensure_table_exists(cursor)

                cursor.execute(select_sql, [limit])
                results = cursor.fetchall()

                return [self._row_to_outcome(row) for row in results]

        except Exception as e:
            logger.error("Failed to get recent outcomes", limit=limit, error=str(e))
            raise GameOutcomeRepositoryError(f"Failed to get recent outcomes: {str(e)}")

    async def update_outcome(self, outcome: GameOutcome) -> GameOutcome:
        """Update an existing game outcome using PostgreSQL."""
        if not self.db_manager:
            raise GameOutcomeRepositoryError("Database manager not initialized")

        outcome.updated_at = datetime.now()

        update_sql = f"""
        UPDATE {self.table_name} 
        SET home_team = %s, away_team = %s, home_score = %s, away_score = %s,
            over = %s, home_win = %s, home_cover_spread = %s, total_line = %s,
            home_spread_line = %s, game_date = %s, updated_at = %s
        WHERE game_id = %s
        RETURNING *
        """

        try:
            with self.db_manager.get_cursor() as cursor:
                self._ensure_table_exists(cursor)

                cursor.execute(
                    update_sql,
                    [
                        outcome.home_team.value,
                        outcome.away_team.value,
                        outcome.home_score,
                        outcome.away_score,
                        outcome.over,
                        outcome.home_win,
                        outcome.home_cover_spread,
                        outcome.total_line,
                        outcome.home_spread_line,
                        outcome.game_date,
                        outcome.updated_at,
                        outcome.game_id,
                    ],
                )

                logger.info("Game outcome updated", game_id=outcome.game_id)
                return outcome

        except Exception as e:
            logger.error(
                "Failed to update game outcome", game_id=outcome.game_id, error=str(e)
            )
            raise GameOutcomeRepositoryError(f"Failed to update outcome: {str(e)}")

    async def delete_outcome(self, game_id: str) -> bool:
        """Delete a game outcome by game ID using PostgreSQL."""
        if not self.db_manager:
            raise GameOutcomeRepositoryError("Database manager not initialized")

        delete_sql = f"DELETE FROM {self.table_name} WHERE game_id = %s"

        try:
            with self.db_manager.get_cursor() as cursor:
                self._ensure_table_exists(cursor)

                cursor.execute(delete_sql, [game_id])
                deleted = cursor.rowcount > 0

                if deleted:
                    logger.info("Game outcome deleted", game_id=game_id)

                return deleted

        except Exception as e:
            logger.error("Failed to delete game outcome", game_id=game_id, error=str(e))
            raise GameOutcomeRepositoryError(f"Failed to delete outcome: {str(e)}")

    def _row_to_outcome(self, row: tuple) -> GameOutcome:
        """Convert database row to GameOutcome object."""
        try:
            # PostgreSQL returns rows as tuples, need to map to GameOutcome fields
            return GameOutcome(
                game_id=row[0],
                home_team=Team(row[1]),
                away_team=Team(row[2]),
                home_score=row[3],
                away_score=row[4],
                over=row[5],
                home_win=row[6],
                home_cover_spread=row[7],
                total_line=row[8],
                home_spread_line=row[9],
                game_date=row[10],
                created_at=row[11],
                updated_at=row[12],
            )
        except Exception as e:
            logger.error("Failed to convert row to outcome", error=str(e), row=row)
            raise GameOutcomeRepositoryError(f"Failed to convert row: {str(e)}")


__all__ = ["GameOutcomeRepository", "GameOutcomeRepositoryError"]
