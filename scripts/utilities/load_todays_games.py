#!/usr/bin/env python3
"""
Load Today's Games Script

This script fetches today's MLB games from Action Network API and loads them
into the action.fact_games table, using the GameFact Pydantic model for validation.
"""

import logging
from datetime import date, datetime, time

import psycopg2
import pytz
from action.utils.actionnetwork_url_builder import ActionNetworkURLBuilder
from psycopg2.extras import RealDictCursor

from src.mlb_sharp_betting.core.config import get_settings
from src.mlb_sharp_betting.core.logging import setup_logging
from src.mlb_sharp_betting.models.actionnetwork import GameFact

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)


def get_team_mapping(db_config: dict) -> dict[str, int]:
    """
    Get team name to team_id mapping from the database.

    Args:
        db_config: Database configuration

    Returns:
        Dictionary mapping team names to team_ids
    """
    with psycopg2.connect(cursor_factory=RealDictCursor, **db_config) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT team_id, full_name, display_name, short_name
                FROM action.dim_teams 
                ORDER BY full_name;
            """)
            results = cursor.fetchall()

            # Build mapping for various name formats
            team_mapping = {}
            for row in results:
                team_id = row["team_id"]
                # Map all name variations to team_id
                team_mapping[row["full_name"]] = team_id
                team_mapping[row["display_name"]] = team_id
                team_mapping[row["short_name"]] = team_id

                # Add common variations
                if row["full_name"] == "Athletics":
                    team_mapping["Oakland Athletics"] = team_id
                elif "Angeles" in row["full_name"]:
                    team_mapping["Angels"] = team_id
                elif "Yankees" in row["full_name"]:
                    team_mapping["Yankees"] = team_id
                elif "Giants" in row["full_name"]:
                    team_mapping["Giants"] = team_id
                elif "Padres" in row["full_name"]:
                    team_mapping["Padres"] = team_id

            logger.info(
                f"Loaded team mapping for {len(results)} teams with {len(team_mapping)} name variations"
            )
            return team_mapping


def parse_game_datetime(
    game_data: dict,
) -> tuple[date, time | None, datetime | None]:
    """
    Parse game date and time from Action Network API data, converting to EST.

    Args:
        game_data: Game data from Action Network API

    Returns:
        Tuple of (date, time, datetime) all in EST
    """
    # Define EST timezone
    est = pytz.timezone("US/Eastern")

    # Try different possible field names
    datetime_fields = ["start_time", "game_time", "datetime", "scheduled_start"]

    game_datetime = None
    for field in datetime_fields:
        if field in game_data and game_data[field]:
            try:
                if isinstance(game_data[field], str):
                    # Parse ISO format datetime (usually UTC from Action Network)
                    utc_datetime = datetime.fromisoformat(
                        game_data[field].replace("Z", "+00:00")
                    )

                    # If it's naive (no timezone), assume UTC
                    if utc_datetime.tzinfo is None:
                        utc = pytz.UTC
                        utc_datetime = utc.localize(utc_datetime)

                    # Convert to EST
                    game_datetime = utc_datetime.astimezone(est)
                    break
            except ValueError as e:
                logger.debug(f"Could not parse datetime field {field}: {e}")
                continue

    if game_datetime:
        # Extract EST date and time components
        game_date = game_datetime.date()
        game_time = game_datetime.time()
        return game_date, game_time, game_datetime
    else:
        # Fallback to today's date in EST
        logger.warning(
            "Could not parse datetime from game data, using today's date in EST"
        )
        est_now = datetime.now(est)
        return est_now.date(), None, None


def extract_game_status(game_data: dict) -> str:
    """
    Extract and normalize game status from API data.

    Args:
        game_data: Game data from Action Network API

    Returns:
        Normalized game status
    """
    status_fields = ["status", "game_status", "state"]

    for field in status_fields:
        if field in game_data and game_data[field]:
            status = str(game_data[field]).lower()

            # Normalize status values
            if status in ["scheduled", "upcoming", "pre"]:
                return "scheduled"
            elif status in ["live", "in-progress", "inprogress", "active"]:
                return "live"
            elif status in ["final", "completed", "finished"]:
                return "final"
            elif status in ["postponed", "delayed"]:
                return status
            elif status in ["cancelled", "canceled"]:
                return "cancelled"
            elif status in ["suspended"]:
                return "suspended"
            else:
                return "scheduled"  # Default

    return "scheduled"  # Default if no status found


def extract_team_info(
    game_data: dict, team_mapping: dict[str, int]
) -> tuple[int | None, int | None]:
    """
    Extract home and away team IDs from game data.

    Args:
        game_data: Game data from Action Network API
        team_mapping: Mapping of team names to team_ids

    Returns:
        Tuple of (home_team_id, away_team_id)
    """
    home_team_id = None
    away_team_id = None

    # Try different data structures
    if "teams" in game_data and isinstance(game_data["teams"], list):
        teams = game_data["teams"]
        if len(teams) >= 2:
            # Usually teams[0] is home, teams[1] is away in Action Network API
            home_team = teams[0]
            away_team = teams[1]

            # Extract team names from various possible fields
            for team_field in ["full_name", "display_name", "name", "team_name"]:
                if team_field in home_team:
                    home_name = home_team[team_field]
                    if home_name in team_mapping:
                        home_team_id = team_mapping[home_name]
                        break

            for team_field in ["full_name", "display_name", "name", "team_name"]:
                if team_field in away_team:
                    away_name = away_team[team_field]
                    if away_name in team_mapping:
                        away_team_id = team_mapping[away_name]
                        break

    # Alternative structure: direct home_team/away_team fields
    elif "home_team" in game_data and "away_team" in game_data:
        for team_field in ["full_name", "display_name", "name"]:
            home_name = game_data["home_team"].get(team_field)
            if home_name and home_name in team_mapping:
                home_team_id = team_mapping[home_name]
                break

        for team_field in ["full_name", "display_name", "name"]:
            away_name = game_data["away_team"].get(team_field)
            if away_name and away_name in team_mapping:
                away_team_id = team_mapping[away_name]
                break

    return home_team_id, away_team_id


def validate_game_data(
    game_data: dict, team_mapping: dict[str, int]
) -> GameFact | None:
    """
    Validate and convert game data to GameFact model.

    Args:
        game_data: Raw game data from Action Network API
        team_mapping: Team name to ID mapping

    Returns:
        Validated GameFact instance or None if validation fails
    """
    try:
        # Extract required fields
        game_id = game_data.get("id")
        if not game_id:
            logger.warning("Game missing required 'id' field")
            return None

        # Parse datetime
        game_date, game_time, game_datetime = parse_game_datetime(game_data)

        # Extract teams
        home_team_id, away_team_id = extract_team_info(game_data, team_mapping)

        if not home_team_id or not away_team_id:
            logger.warning(f"Could not map teams for game {game_id}")
            return None

        # Build model data - use EST for season calculation
        est = pytz.timezone("US/Eastern")
        current_est = datetime.now(est)

        model_data = {
            "id_action": int(game_id),
            "dim_home_team_actionid": home_team_id,
            "dim_away_team_actionid": away_team_id,
            "dim_date": game_date,
            "dim_time": game_time,
            "dim_datetime": game_datetime,
            "game_status": extract_game_status(game_data),
            "venue_name": game_data.get("venue", {}).get("name")
            if "venue" in game_data
            else None,
            "season": current_est.year,  # Current season in EST
            "season_type": "regular",  # Default
            "game_number": 1,  # Default
        }

        return GameFact(**model_data)

    except Exception as e:
        logger.error(
            f"Validation failed for game {game_data.get('id', 'unknown')}: {e}"
        )
        return None


def upsert_game(cursor, game: GameFact):
    """
    Upsert game data using the database function.

    Args:
        cursor: Database cursor
        game: Validated GameFact instance
    """
    upsert_sql = """
    SELECT action.upsert_game(
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    );
    """

    cursor.execute(
        upsert_sql,
        (
            game.id_action,
            game.id_mlbstatsapi,
            game.dim_home_team_actionid,
            game.dim_away_team_actionid,
            game.dim_date,
            game.dim_time,
            game.dim_datetime,
            game.game_status,
            game.venue_name,
            game.season,
            game.season_type,
            game.game_number,
            game.weather_conditions,
            game.temperature,
            game.wind_speed,
            game.wind_direction,
        ),
    )


def main():
    """Main execution function."""
    logger.info("Starting today's games loading process")

    try:
        # Get database config
        settings = get_settings()
        db_config = {
            "host": settings.postgres.host,
            "port": settings.postgres.port,
            "database": settings.postgres.database,
            "user": settings.postgres.user,
            "password": settings.postgres.password,
        }

        # Get team mapping
        team_mapping = get_team_mapping(db_config)

        # Initialize Action Network URL builder
        builder = ActionNetworkURLBuilder()

        # Get today's games in EST
        est = pytz.timezone("US/Eastern")
        today = datetime.now(est)
        logger.info(f"Fetching games for {today.strftime('%Y-%m-%d')} (EST)")

        games = builder.get_games_from_api(today)
        logger.info(f"Retrieved {len(games)} games from Action Network API")

        # Validate and process games
        validated_games = []
        for game_data in games:
            game_fact = validate_game_data(game_data, team_mapping)
            if game_fact:
                validated_games.append(game_fact)
                logger.debug(f"Validated game: {game_fact.id_action}")
            else:
                logger.warning(
                    f"Failed to validate game: {game_data.get('id', 'unknown')}"
                )

        logger.info(
            f"Successfully validated {len(validated_games)} out of {len(games)} games"
        )

        # Load into database
        with psycopg2.connect(cursor_factory=RealDictCursor, **db_config) as conn:
            with conn.cursor() as cursor:
                success_count = 0
                for game in validated_games:
                    try:
                        upsert_game(cursor, game)
                        success_count += 1
                        logger.debug(f"Upserted game: {game.id_action}")
                    except Exception as e:
                        logger.error(f"Failed to upsert game {game.id_action}: {e}")
                        continue

                # Commit transaction
                conn.commit()
                logger.info(
                    f"Successfully loaded {success_count} games into action.fact_games"
                )

                # Verify the data using the view
                cursor.execute(
                    """
                    SELECT id_action, matchup, dim_date, game_status
                    FROM action.games_with_teams 
                    WHERE dim_date = %s
                    ORDER BY dim_datetime;
                """,
                    (today.date(),),
                )

                loaded_games = cursor.fetchall()
                logger.info(f"Today's games in database ({len(loaded_games)} total):")
                for game in loaded_games:
                    logger.info(
                        f"  Game {game['id_action']}: {game['matchup']} - {game['game_status']}"
                    )

    except Exception as e:
        logger.error(f"Error during games loading: {e}")
        raise

    logger.info("Today's games loading process completed successfully")


if __name__ == "__main__":
    main()
