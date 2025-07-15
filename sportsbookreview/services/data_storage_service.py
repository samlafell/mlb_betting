"""
Service for storing SportsbookReview data in PostgreSQL database.

Handles game data, betting odds, and MLB API enrichment with proper
error handling and data validation.

🚀 PHASE 2A MIGRATION: Updated to use new consolidated schema structure
"""

import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncpg
from pydantic import ValidationError

# Handle imports for both module usage and direct execution
try:
    # Try relative imports first (when used as module)
    from ..models.game import EnhancedGame
    from ..parsers.validators import GameDataValidator
    from .mlb_data_enrichment_service import MLBDataEnrichmentService
except ImportError:
    # If relative imports fail, add project root to path and use absolute imports
    current_dir = Path(__file__).parent
    project_root = current_dir.parent.parent
    sys.path.insert(0, str(project_root))

    from sportsbookreview.models.game import EnhancedGame
    from sportsbookreview.parsers.validators import GameDataValidator
    from sportsbookreview.services.mlb_data_enrichment_service import (
        MLBDataEnrichmentService,
    )

# Import consolidated table registry for Phase 2A migration
try:
    from src.mlb_sharp_betting.db.table_registry import get_table_registry

    TABLE_REGISTRY = get_table_registry()
except ImportError:
    # Fallback for environments where the registry isn't available
    TABLE_REGISTRY = None
    logging.warning("Table registry not available - using legacy table names")

logger = logging.getLogger(__name__)


@dataclass
class StorageStats:
    """Statistics for storage operations."""

    games_processed: int = 0
    games_inserted: int = 0
    games_updated: int = 0
    games_failed: int = 0
    betting_records_inserted: int = 0
    betting_records_failed: int = 0
    mlb_enrichments_applied: int = 0
    processing_time: float = 0.0


class DataStorageService:
    """
    Service for storing SportsbookReview data in PostgreSQL database.

    Handles game data, betting odds, and MLB API enrichment with proper
    error handling and data validation.
    """

    def __init__(
        self,
        db_connection_string: str | None = None,
        pool: asyncpg.Pool | None = None,
    ):
        """
        Initialize the storage service.

        Args:
            db_connection_string: Optional database connection string
            pool: Optional, pre-existing asyncpg connection pool
        """
        self.db_connection_string = db_connection_string
        self.mlb_enrichment_service = MLBDataEnrichmentService()

        # Storage statistics
        self.stats = StorageStats()

        # Connection pool
        self.pool: asyncpg.Pool | None = pool

    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize_connection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close_connection()

    async def initialize_connection(self):
        """Initialize database connection pool."""
        if self.pool is not None:
            logger.info("Using pre-existing connection pool.")
            return

        try:
            if self.db_connection_string:
                self.pool = await asyncpg.create_pool(
                    self.db_connection_string,
                    min_size=2,
                    max_size=10,
                    command_timeout=60,
                )
            else:
                # Create a simple asyncpg connection for testing
                self.pool = await asyncpg.create_pool(
                    host="localhost",
                    port=5432,
                    database="mlb_betting",
                    user="samlafell",
                    min_size=2,
                    max_size=10,
                    command_timeout=60,
                )
                logger.info("Using direct asyncpg connection pool")

        except Exception as e:
            logger.error(f"Failed to initialize database connection: {e}")
            raise

    async def close_connection(self):
        """Close database connection pool."""
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def store_game_data(self, game_data: dict[str, Any]) -> int | None:
        """
        Store game data in the database.

        Args:
            game_data: Dictionary containing game and betting data

        Returns:
            Game ID if successful, None otherwise
        """
        try:
            start_time = datetime.now()

            # Extract game info
            game = game_data.get("game")
            if not game:
                logger.error("No game data found in input")
                return None

            # Convert to EnhancedGame if needed
            if isinstance(game, dict):
                try:
                    game = EnhancedGame(**game)
                except ValidationError as e:
                    logger.error(f"Failed to validate game data: {e}")
                    return None

            # Apply MLB enrichment
            enriched_game = await self.apply_mlb_enrichment(game)

            # Store game in database
            game_id = await self.insert_or_update_game(enriched_game)

            if game_id:
                # Store betting data using the database auto-incrementing ID
                betting_data = game_data.get("betting_data", [])
                await self.store_betting_data(game_id, betting_data)

                # Update statistics
                self.stats.games_processed += 1
                if game_id:
                    self.stats.games_inserted += 1
                else:
                    self.stats.games_failed += 1

                processing_time = (datetime.now() - start_time).total_seconds()
                self.stats.processing_time += processing_time

                logger.debug(f"Stored game {game.sbr_game_id} with ID {game_id}")

            return game_id

        except Exception as e:
            logger.error(f"Error storing game data: {e}")
            self.stats.games_failed += 1
            return None

    async def apply_mlb_enrichment(self, game: EnhancedGame) -> EnhancedGame:
        """
        Apply MLB API enrichment to game data.

        Args:
            game: Game to enrich

        Returns:
            Enriched game data
        """
        try:
            # Import here to avoid circular imports
            from .mlb_api_service import correlate_sportsbookreview_game

            # Try to correlate with MLB API
            correlation_result = await correlate_sportsbookreview_game(
                home_team=str(game.home_team),
                away_team=str(game.away_team),
                game_datetime=game.game_datetime,
            )

            if correlation_result and correlation_result.confidence > 0.7:
                # Apply enrichment data
                mlb_game = correlation_result.mlb_game

                # Update game with MLB data
                if mlb_game and mlb_game.game_pk:
                    game.mlb_game_id = mlb_game.game_pk
                    game.mlb_correlation_confidence = correlation_result.confidence

                    # Add venue info
                    if mlb_game.venue_id and mlb_game.venue_name:
                        game.venue_name = mlb_game.venue_name
                        game.venue_id = mlb_game.venue_id

                    # Add weather data
                    if mlb_game.weather:
                        game.weather_data = mlb_game.weather

                    # Update data quality
                    game.data_quality = (
                        "HIGH" if correlation_result.confidence > 0.9 else "MEDIUM"
                    )

                    self.stats.mlb_enrichments_applied += 1

                    logger.debug(
                        f"Applied MLB enrichment to game {game.sbr_game_id} (confidence: {correlation_result.confidence:.2f})"
                    )

            return game

        except Exception as e:
            logger.error(f"Error applying MLB enrichment: {e}")
            return game

    async def store_raw_html(self, url: str, html: str) -> int | None:
        """
        Store raw HTML content in the staging table.

        Args:
            url: The source URL of the HTML content.
            html: The HTML content.

        Returns:
            The ID of the inserted row, or None on failure.
        """
        query = """
            INSERT INTO sbr_raw_html (source_url, html_content, status)
            VALUES ($1, $2, 'new')
            ON CONFLICT (source_url) DO UPDATE SET
                html_content = EXCLUDED.html_content,
                scraped_at = NOW(),
                status = 'new'
            RETURNING id;
        """
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchval(query, url, html)
                logger.info(f"Stored raw HTML for {url} with ID {result}")
                return result
        except Exception as e:
            logger.error(f"Failed to store raw HTML for {url}: {e}")
            return None

    async def store_parsed_data(
        self, raw_html_id: int, parsed_games: list[GameDataValidator]
    ):
        """
        Store parsed game data in the staging table.

        Args:
            raw_html_id: The ID of the raw HTML content in sbr_raw_html.
            parsed_games: A list of parsed game data validators.
        """
        query = """
            INSERT INTO sbr_parsed_games (raw_html_id, game_data, status)
            VALUES ($1, $2, 'new');
        """
        try:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    for game_validator in parsed_games:
                        # ------------------------------------------------------
                        # Accept either fully-validated GameDataValidator
                        # instances *or* raw dictionaries (fallback from HTML
                        # parsing).  Fallback objects are JSON-serialised
                        # directly.
                        # ------------------------------------------------------
                        if hasattr(game_validator, "model_dump_json"):
                            game_json = game_validator.model_dump_json()
                        else:
                            game_json = json.dumps(game_validator, default=str)

                        await conn.execute(query, raw_html_id, game_json)

                        # Stats tracking – consider each parsed game a
                        # processed & inserted staging record.
                        self.stats.games_processed += 1
                        self.stats.games_inserted += 1
                    logger.info(
                        f"Stored {len(parsed_games)} parsed games for raw_html_id {raw_html_id}"
                    )
            await self.update_raw_html_status(raw_html_id, "processed")
        except Exception as e:
            logger.error(
                f"Failed to store parsed data for raw_html_id {raw_html_id}: {e}"
            )
            await self.update_raw_html_status(raw_html_id, "failed")

    async def update_raw_html_status(self, raw_html_id: int, status: str):
        """Update the status of a raw HTML record."""
        query = "UPDATE sbr_raw_html SET status = $1 WHERE id = $2;"
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(query, status, raw_html_id)
        except Exception as e:
            logger.error(f"Failed to update status for raw_html_id {raw_html_id}: {e}")

    async def insert_or_update_game(self, game: EnhancedGame) -> int | None:
        """
        Insert or update game in the database using new consolidated schema.

        Args:
            game: Game to insert/update

        Returns:
            Game ID if successful, None otherwise
        """
        try:
            # Use the connection pool
            async with self.pool.acquire() as conn:
                # 🚀 PHASE 2A: Use new consolidated schema table
                games_table = (
                    TABLE_REGISTRY.get_table("games")
                    if TABLE_REGISTRY
                    else "core_betting.games"
                )

                # Check if game already exists
                check_query = (
                    f"SELECT id FROM {games_table} WHERE sportsbookreview_game_id = $1"
                )
                existing_id = await conn.fetchval(check_query, game.sbr_game_id)

                if existing_id:
                    # Update existing game
                    update_query = f"""
                        UPDATE {games_table} SET
                            mlb_stats_api_game_id = $2,
                            home_team = $3,
                            away_team = $4,
                            game_date = $5,
                            game_datetime = $6,
                            venue_name = $7,
                            venue_id = $8,
                            season = $9,
                            season_type = $10,
                            game_type = $11,
                            data_quality = $12,
                            updated_at = NOW()
                        WHERE id = $1
                        RETURNING id
                    """

                    result = await conn.fetchval(
                        update_query,
                        existing_id,
                        getattr(game, "mlb_game_id", None),
                        str(game.home_team),
                        str(game.away_team),
                        game.game_datetime.date()
                        if hasattr(game.game_datetime, "date")
                        else game.game_datetime,
                        game.game_datetime,
                        getattr(game, "venue_name", None),
                        getattr(game, "venue_id", None),
                        getattr(game, "season", game.game_datetime.year),
                        getattr(game, "season_type", "regular"),
                        getattr(game, "game_type", "regular"),
                        getattr(game, "data_quality", "medium"),
                    )
                else:
                    # Insert new game
                    insert_query = f"""
                        INSERT INTO {games_table} (
                            sportsbookreview_game_id, mlb_stats_api_game_id, home_team, away_team, 
                            game_date, game_datetime, venue_name, venue_id,
                            season, season_type, game_type, data_quality,
                            created_at, updated_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW(), NOW())
                        RETURNING id
                    """

                    result = await conn.fetchval(
                        insert_query,
                        game.sbr_game_id,
                        getattr(game, "mlb_game_id", None),
                        str(game.home_team),
                        str(game.away_team),
                        game.game_datetime.date()
                        if hasattr(game.game_datetime, "date")
                        else game.game_datetime,
                        game.game_datetime,
                        getattr(game, "venue_name", None),
                        getattr(game, "venue_id", None),
                        getattr(game, "season", game.game_datetime.year),
                        getattr(game, "season_type", "regular"),
                        getattr(game, "game_type", "regular"),
                        getattr(game, "data_quality", "medium"),
                    )

                logger.debug(f"Stored game to {games_table} with ID {result}")
                return result

        except Exception as e:
            logger.error(f"Error inserting/updating game {game.sbr_game_id}: {e}")
            return None

    async def update_mlb_enrichment(
        self, conn: asyncpg.Connection, game_id: int, game: EnhancedGame
    ):
        """
        Update MLB enrichment data for a game.

        Args:
            conn: Database connection
            game_id: Game ID
            game: Game with enrichment data
        """
        try:
            query = """
                SELECT public.update_game_mlb_enrichment(
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
                )
            """

            await conn.execute(
                query,
                game_id,
                game.mlb_game_id,
                getattr(game, "venue_name", None),
                getattr(game, "venue_id", None),
                game.weather_data.get("condition")
                if hasattr(game, "weather_data") and game.weather_data
                else None,
                game.weather_data.get("temperature")
                if hasattr(game, "weather_data") and game.weather_data
                else None,
                game.weather_data.get("wind_speed")
                if hasattr(game, "weather_data") and game.weather_data
                else None,
                game.weather_data.get("wind_direction")
                if hasattr(game, "weather_data") and game.weather_data
                else None,
                game.weather_data.get("humidity")
                if hasattr(game, "weather_data") and game.weather_data
                else None,
                game.mlb_correlation_confidence,
            )

        except Exception as e:
            logger.error(f"Error updating MLB enrichment for game {game_id}: {e}")

    async def store_betting_data(self, game_id: int, betting_data: list[Any]):
        """
        Store betting data for a game.

        Args:
            game_id: Game ID
            betting_data: List of betting data records
        """
        try:
            logger.debug(
                f"store_betting_data called with {len(betting_data)} records for game_id {game_id}"
            )
            for bet_record in betting_data:
                # Determine bet type and store accordingly
                if isinstance(bet_record, dict):
                    bet_type = (bet_record.get("bet_type") or "").lower()
                    logger.debug(
                        f"Processing bet_record: bet_type='{bet_type}', sportsbook='{bet_record.get('sportsbook')}', record={bet_record}"
                    )

                    if bet_type == "moneyline":
                        await self.store_moneyline_data(game_id, bet_record)
                    elif bet_type == "spread":
                        await self.store_spread_data(game_id, bet_record)
                    elif bet_type in ["total", "totals"]:  # Handle both variations
                        await self.store_total_data(game_id, bet_record)
                    else:
                        logger.warning(f"Unknown bet type: {bet_type}")

        except Exception as e:
            logger.error(f"Error storing betting data for game {game_id}: {e}")

    async def store_moneyline_data(self, game_id: int, data: dict[str, Any]):
        """Store moneyline betting data using new consolidated schema."""
        try:
            async with self.pool.acquire() as conn:
                # 🚀 PHASE 2A: Use new consolidated schema table
                table_name = (
                    TABLE_REGISTRY.get_table("moneyline")
                    if TABLE_REGISTRY
                    else "core_betting.betting_lines_moneyline"
                )

                # Note: home_team, away_team, and game_datetime will be populated automatically
                # by the database trigger, but we could also include them explicitly if available
                query = f"""
                    INSERT INTO {table_name} (
                        game_id, sportsbook, home_ml, away_ml, odds_timestamp,
                        home_bets_percentage, away_bets_percentage,
                        home_money_percentage, away_money_percentage,
                        sharp_action, source
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """

                await conn.execute(
                    query,
                    game_id,
                    data.get("sportsbook"),
                    data.get("home_ml"),
                    data.get("away_ml"),
                    data.get("timestamp") or datetime.now(),
                    data.get("home_bets_percentage"),
                    data.get("away_bets_percentage"),
                    data.get("home_money_percentage"),
                    data.get("away_money_percentage"),
                    data.get("sharp_action"),
                    "SPORTSBOOKREVIEW",
                )

                self.stats.betting_records_inserted += 1
                logger.debug(
                    f"Stored moneyline data to {table_name} for game {game_id}"
                )

        except Exception as e:
            logger.error(f"Error storing moneyline data: {e}")
            self.stats.betting_records_failed += 1

    async def store_spread_data(self, game_id: int, data: dict[str, Any]):
        """Store spread betting data using new consolidated schema."""
        try:
            async with self.pool.acquire() as conn:
                # 🚀 PHASE 2A: Use new consolidated schema table
                table_name = (
                    TABLE_REGISTRY.get_table("spreads")
                    if TABLE_REGISTRY
                    else "core_betting.betting_lines_spreads"
                )

                # Note: home_team, away_team, and game_datetime will be populated automatically
                # by the database trigger, but we could also include them explicitly if available
                query = f"""
                    INSERT INTO {table_name} (
                        game_id, sportsbook, home_spread, away_spread, 
                        home_spread_price, away_spread_price, odds_timestamp,
                        home_bets_percentage, away_bets_percentage,
                        home_money_percentage, away_money_percentage,
                        sharp_action, source
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                """

                await conn.execute(
                    query,
                    game_id,
                    data.get("sportsbook"),
                    data.get("home_spread"),
                    data.get("away_spread"),
                    data.get("home_spread_price"),
                    data.get("away_spread_price"),
                    data.get("timestamp") or datetime.now(),
                    data.get("home_bets_percentage"),
                    data.get("away_bets_percentage"),
                    data.get("home_money_percentage"),
                    data.get("away_money_percentage"),
                    data.get("sharp_action"),
                    "SPORTSBOOKREVIEW",
                )

                self.stats.betting_records_inserted += 1
                logger.debug(f"Stored spreads data to {table_name} for game {game_id}")

        except Exception as e:
            logger.error(f"Error storing spread data: {e}")
            self.stats.betting_records_failed += 1

    async def store_total_data(self, game_id: int, data: dict[str, Any]):
        """Store total betting data using new consolidated schema."""
        try:
            async with self.pool.acquire() as conn:
                # 🚀 PHASE 2A: Use new consolidated schema table
                table_name = (
                    TABLE_REGISTRY.get_table("totals")
                    if TABLE_REGISTRY
                    else "core_betting.betting_lines_totals"
                )

                # Note: home_team, away_team, and game_datetime will be populated automatically
                # by the database trigger, but we could also include them explicitly if available
                query = f"""
                    INSERT INTO {table_name} (
                        game_id, sportsbook, total_line, over_price, under_price,
                        odds_timestamp, over_bets_percentage, under_bets_percentage,
                        over_money_percentage, under_money_percentage,
                        sharp_action, source
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                """

                await conn.execute(
                    query,
                    game_id,
                    data.get("sportsbook"),
                    data.get("total_line"),
                    data.get("over_price"),
                    data.get("under_price"),
                    data.get("timestamp") or datetime.now(),
                    data.get("over_bets_percentage"),
                    data.get("under_bets_percentage"),
                    data.get("over_money_percentage"),
                    data.get("under_money_percentage"),
                    data.get("sharp_action"),
                    "SPORTSBOOKREVIEW",
                )

                self.stats.betting_records_inserted += 1
                logger.debug(f"Stored totals data to {table_name} for game {game_id}")

        except Exception as e:
            logger.error(f"Error storing total data: {e}")
            self.stats.betting_records_failed += 1

    async def store_batch_data(self, batch_data: list[dict[str, Any]]) -> list[int]:
        """
        Store a batch of game data.

        Args:
            batch_data: List of game data dictionaries

        Returns:
            List of game IDs for successfully stored games
        """
        stored_game_ids = []

        for game_data in batch_data:
            try:
                game_id = await self.store_game_data(game_data)
                if game_id:
                    stored_game_ids.append(game_id)

            except Exception as e:
                logger.error(f"Error storing game in batch: {e}")
                continue

        return stored_game_ids

    async def get_existing_games(self, sbr_game_ids: list[str]) -> list[str]:
        """
        Get list of existing games by SportsbookReview ID using new consolidated schema.

        Args:
            sbr_game_ids: List of SportsbookReview game IDs

        Returns:
            List of existing game IDs
        """
        try:
            async with self.pool.acquire() as conn:
                # 🚀 PHASE 2A: Use new consolidated schema table
                games_table = (
                    TABLE_REGISTRY.get_table("games")
                    if TABLE_REGISTRY
                    else "core_betting.games"
                )

                query = f"""
                    SELECT sportsbookreview_game_id 
                    FROM {games_table} 
                    WHERE sportsbookreview_game_id = ANY($1)
                """

                results = await conn.fetch(query, sbr_game_ids)
                return [row["sportsbookreview_game_id"] for row in results]

        except Exception as e:
            logger.error(f"Error checking existing games: {e}")
            return []

    async def update_game_results(self, game_id: int, home_score: int, away_score: int):
        """
        Update game results using new consolidated schema.

        Args:
            game_id: Game ID
            home_score: Home team score
            away_score: Away team score
        """
        try:
            async with self.pool.acquire() as conn:
                # 🚀 PHASE 2A: Use new consolidated schema table
                games_table = (
                    TABLE_REGISTRY.get_table("games")
                    if TABLE_REGISTRY
                    else "core_betting.games"
                )

                query = f"""
                    UPDATE {games_table} 
                    SET home_score = $2, away_score = $3, 
                        winning_team = CASE WHEN $2 > $3 THEN home_team ELSE away_team END,
                        game_status = 'final',
                        updated_at = NOW()
                    WHERE id = $1
                """

                await conn.execute(query, game_id, home_score, away_score)

        except Exception as e:
            logger.error(f"Error updating game results for {game_id}: {e}")

    def get_storage_stats(self) -> dict[str, Any]:
        """
        Get storage statistics.

        Returns:
            Dictionary containing storage statistics
        """
        return {
            "games_processed": self.stats.games_processed,
            "games_inserted": self.stats.games_inserted,
            "games_updated": self.stats.games_updated,
            "games_failed": self.stats.games_failed,
            "betting_records_inserted": self.stats.betting_records_inserted,
            "betting_records_failed": self.stats.betting_records_failed,
            "mlb_enrichments_applied": self.stats.mlb_enrichments_applied,
            "success_rate": (
                self.stats.games_inserted / max(self.stats.games_processed, 1)
            )
            * 100,
            "processing_time": self.stats.processing_time,
            "avg_processing_time": self.stats.processing_time
            / max(self.stats.games_processed, 1),
        }

    def reset_stats(self):
        """Reset storage statistics."""
        self.stats = StorageStats()


# Convenience functions
async def store_historical_data(
    scraped_data: list[dict[str, Any]], progress_callback: callable | None = None
) -> dict[str, Any]:
    """
    Convenience function to store historical data.

    Args:
        scraped_data: List of scraped game data
        progress_callback: Optional progress callback

    Returns:
        Storage statistics
    """
    async with DataStorageService() as storage:
        total_games = len(scraped_data)

        for idx, game_data in enumerate(scraped_data):
            await storage.store_game_data(game_data)

            if progress_callback:
                progress = (idx + 1) / total_games * 100
                progress_callback(progress, f"Stored {idx + 1}/{total_games} games")

        return storage.get_storage_stats()


async def store_game_batch(batch_data: list[dict[str, Any]]) -> list[int]:
    """
    Convenience function to store a batch of games.

    Args:
        batch_data: List of game data dictionaries

    Returns:
        List of game IDs
    """
    async with DataStorageService() as storage:
        return await storage.store_batch_data(batch_data)
