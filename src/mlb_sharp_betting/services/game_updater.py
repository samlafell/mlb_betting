"""
Game information update service.

Fetches completed game results from the MLB Stats API and stores
outcomes in the database for analysis against betting data.
"""

import asyncio
from datetime import date, datetime, timedelta
from typing import Any

import structlog

from ..core.exceptions import MLBSharpBettingError
from ..db.connection import get_db_manager
from ..db.repositories import get_betting_split_repository, get_game_outcome_repository
from ..models.game import Game, Team
from ..models.game_outcome import GameOutcome
from ..utils.mlb_api_client import MLBStatsAPIClient

logger = structlog.get_logger(__name__)


class GameUpdateError(MLBSharpBettingError):
    """Exception for game update errors."""

    pass


class GameUpdater:
    """
    Service for updating game information and storing outcomes.

    Fetches completed game results from MLB Stats API and stores
    them in the game_outcomes table with betting result calculations.
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """
        Initialize game updater with configuration.

        Args:
            config: Configuration dictionary
        """
        self.config = config or {}
        self.db_manager = get_db_manager()
        self.outcome_repo = get_game_outcome_repository(self.db_manager)
        self.split_repo = get_betting_split_repository(self.db_manager)

        # No default betting lines - we should always use real data from splits

    async def update_games(self, games: list[Game]) -> list[Game]:
        """
        Update list of games with latest information.

        Args:
            games: List of games to update

        Returns:
            Updated games list
        """
        # For now, just return the games as-is
        # This could be enhanced to fetch real-time updates
        return games

    async def update_game_outcomes_for_date(
        self, target_date: date, use_betting_lines: bool = True
    ) -> list[GameOutcome]:
        """
        Update game outcomes for a specific date.

        Args:
            target_date: Date to fetch completed games for
            use_betting_lines: Whether to use actual betting lines from splits data

        Returns:
            List of created/updated game outcomes
        """
        outcomes = []

        try:
            logger.info("Updating game outcomes for date", date=target_date.isoformat())

            async with MLBStatsAPIClient() as api_client:
                # Get completed games for the date
                completed_games = await api_client.get_completed_games(target_date)

                if not completed_games:
                    logger.info(
                        "No completed games found for date",
                        date=target_date.isoformat(),
                    )
                    return outcomes

                logger.info(
                    "Found completed games",
                    count=len(completed_games),
                    date=target_date.isoformat(),
                )

                # Process each completed game
                for game_data in completed_games:
                    try:
                        outcome = await self._process_completed_game(
                            game_data, use_betting_lines
                        )
                        if outcome:
                            outcomes.append(outcome)
                    except Exception as e:
                        logger.error(
                            "Failed to process game",
                            game_id=game_data.get("gamePk"),
                            error=str(e),
                        )
                        continue

                logger.info(
                    "Game outcomes update completed",
                    processed=len(outcomes),
                    date=target_date.isoformat(),
                )

                return outcomes

        except Exception as e:
            logger.error(
                "Failed to update game outcomes",
                date=target_date.isoformat(),
                error=str(e),
            )
            raise GameUpdateError(f"Failed to update game outcomes: {str(e)}")

    async def update_recent_game_outcomes(
        self, days_back: int = 3, use_betting_lines: bool = True
    ) -> list[GameOutcome]:
        """
        Update game outcomes for recent completed games.

        Args:
            days_back: Number of days back to check for games
            use_betting_lines: Whether to use actual betting lines from splits data

        Returns:
            List of all created/updated game outcomes
        """
        all_outcomes = []

        try:
            logger.info("Updating recent game outcomes", days_back=days_back)

            # Check each day in the range
            for days_ago in range(days_back):
                target_date = date.today() - timedelta(days=days_ago)

                daily_outcomes = await self.update_game_outcomes_for_date(
                    target_date, use_betting_lines
                )
                all_outcomes.extend(daily_outcomes)

                # Brief pause between API calls
                await asyncio.sleep(0.5)

            logger.info(
                "Recent game outcomes update completed",
                total_outcomes=len(all_outcomes),
                days_checked=days_back,
            )

            return all_outcomes

        except Exception as e:
            logger.error("Failed to update recent game outcomes", error=str(e))
            raise GameUpdateError(f"Failed to update recent outcomes: {str(e)}")

    async def update_game_status(self, game_id: str) -> GameOutcome:
        """
        Update status and outcome for a specific game.

        Args:
            game_id: MLB game ID (gamePk)

        Returns:
            Updated game outcome
        """
        try:
            logger.info("Updating specific game status", game_id=game_id)

            async with MLBStatsAPIClient() as api_client:
                # Get live feed data for the game
                game_data = await api_client.get_game_live_feed(game_id)

                if not game_data:
                    raise GameUpdateError(f"Could not fetch data for game {game_id}")

                # Check if game is completed
                game_status = game_data.get("gameData", {}).get("status", {})
                status_code = game_status.get("statusCode")

                if status_code != "F":
                    raise GameUpdateError(
                        f"Game {game_id} is not completed (status: {status_code})"
                    )

                # Process the completed game
                outcome = await self._process_completed_game(
                    game_data, use_betting_lines=True
                )

                if not outcome:
                    raise GameUpdateError(f"Failed to process game {game_id}")

                logger.info("Game status updated successfully", game_id=game_id)
                return outcome

        except Exception as e:
            logger.error("Failed to update game status", game_id=game_id, error=str(e))
            raise GameUpdateError(f"Failed to update game {game_id}: {str(e)}")

    async def _process_completed_game(
        self, game_data: dict[str, Any], use_betting_lines: bool = True
    ) -> GameOutcome | None:
        """
        Process a completed game and store its outcome.

        Args:
            game_data: Raw game data from MLB API
            use_betting_lines: Whether to use actual betting lines

        Returns:
            Created or updated game outcome
        """
        try:
            async with MLBStatsAPIClient() as api_client:
                # Extract basic game result data
                result_data = api_client.extract_game_result(game_data)

                if not result_data:
                    logger.warning(
                        "Could not extract game result data",
                        game_id=game_data.get("gamePk"),
                    )
                    return None

                game_id = result_data["game_id"]
                home_team = result_data["home_team"]
                away_team = result_data["away_team"]
                home_score = result_data["home_score"]
                away_score = result_data["away_score"]
                home_win = result_data["home_win"]
                game_date = result_data["game_date"]

                # Get betting lines if requested
                total_line = None
                home_spread_line = None

                if use_betting_lines:
                    total_line, home_spread_line = await self._get_betting_lines(
                        home_team, away_team, game_date
                    )

                # Use default betting lines if none found or if not using betting lines
                if total_line is None or home_spread_line is None:
                    if use_betting_lines:
                        logger.warning(
                            "No betting lines found for game, using default lines",
                            game_id=game_id,
                            home_team=home_team.value,
                            away_team=away_team.value,
                        )
                    else:
                        logger.info(
                            "Using default betting lines for game outcome calculation",
                            game_id=game_id,
                            home_team=home_team.value,
                            away_team=away_team.value,
                        )

                    # Use typical MLB betting line defaults
                    total_line = 9.0  # Average MLB total runs
                    home_spread_line = -1.5  # Home team typically slight favorite

                # Calculate betting outcomes
                total_score = home_score + away_score
                over = total_score > total_line

                # For home_spread_line:
                # - If positive: home team gets points (underdog)
                # - If negative: home team gives points (favorite)
                # Home covers if: (home_score + home_spread_line) > away_score
                home_cover_spread = (home_score + home_spread_line) > away_score

                # Check if outcome already exists
                existing_outcome = await self.outcome_repo.get_outcome_by_game_id(
                    game_id
                )

                if existing_outcome:
                    # Update existing outcome
                    existing_outcome.home_score = home_score
                    existing_outcome.away_score = away_score
                    existing_outcome.over = over
                    existing_outcome.home_win = home_win
                    existing_outcome.home_cover_spread = home_cover_spread
                    existing_outcome.total_line = total_line
                    existing_outcome.home_spread_line = home_spread_line
                    existing_outcome.game_date = game_date

                    outcome = await self.outcome_repo.update_outcome(existing_outcome)
                    logger.info("Updated existing game outcome", game_id=game_id)
                else:
                    # Create new outcome
                    outcome = GameOutcome(
                        game_id=game_id,
                        home_team=home_team,
                        away_team=away_team,
                        home_score=home_score,
                        away_score=away_score,
                        over=over,
                        home_win=home_win,
                        home_cover_spread=home_cover_spread,
                        total_line=total_line,
                        home_spread_line=home_spread_line,
                        game_date=game_date,
                    )

                    outcome = await self.outcome_repo.create_outcome(outcome)
                    logger.info("Created new game outcome", game_id=game_id)

                return outcome

        except Exception as e:
            logger.error(
                "Failed to process completed game",
                game_id=game_data.get("gamePk"),
                error=str(e),
            )
            return None

    async def _get_betting_lines(
        self, home_team: Team, away_team: Team, game_date: datetime | None
    ) -> tuple[float | None, float | None]:
        """
        Get betting lines from splits data for a team matchup.

        Args:
            home_team: Home team
            away_team: Away team
            game_date: Game date for filtering

        Returns:
            Tuple of (total_line, home_spread_line) or (None, None) if not found
        """
        try:
            # Look for betting splits for this matchup
            splits = self.split_repo.find_all(
                home_team=home_team.value, away_team=away_team.value
            )

            if not splits:
                logger.debug(
                    "No betting splits found for matchup",
                    home_team=home_team.value,
                    away_team=away_team.value,
                )
                return None, None

            # Find the most recent relevant splits
            total_line = None
            home_spread_line = None
            moneyline_data = None

            # Track what we've already seen to avoid duplicate logging
            seen_moneyline_values = set()
            logged_count = 0
            max_logs_per_game = 3  # Limit logs per game to reduce spam

            for split in splits:
                # Extract line values based on split_type and split_value
                if split.split_value is not None:
                    # Handle both string and numeric split_value
                    if split.split_type == "total":
                        try:
                            if isinstance(split.split_value, (int, float)):
                                total_line = float(split.split_value)
                            elif (
                                isinstance(split.split_value, str)
                                and split.split_value.strip()
                            ):
                                total_line = float(split.split_value)
                        except (ValueError, TypeError):
                            logger.warning(
                                "Invalid total value",
                                split_value=split.split_value,
                                game_id=split.game_id,
                            )
                            continue

                    elif split.split_type == "spread":
                        try:
                            if isinstance(split.split_value, (int, float)):
                                spread_value = float(split.split_value)
                            elif (
                                isinstance(split.split_value, str)
                                and split.split_value.strip()
                            ):
                                spread_value = float(split.split_value)
                            else:
                                continue

                            # Store the raw spread value, we'll interpret it after getting moneyline data
                            home_spread_line = spread_value
                        except (ValueError, TypeError):
                            logger.warning(
                                "Invalid spread value",
                                split_value=split.split_value,
                                game_id=split.game_id,
                            )
                            continue

                    elif split.split_type == "moneyline":
                        # Store moneyline data for spread interpretation
                        if (
                            isinstance(split.split_value, str)
                            and split.split_value.strip()
                        ):
                            try:
                                import json

                                current_moneyline_data = json.loads(split.split_value)

                                # Create a hashable key from the moneyline data for deduplication
                                moneyline_key = f"{current_moneyline_data.get('home', 0)}_{current_moneyline_data.get('away', 0)}"

                                # Only log if this is a new unique value and we haven't logged too many
                                if (
                                    moneyline_key not in seen_moneyline_values
                                    and logged_count < max_logs_per_game
                                ):
                                    logger.debug(
                                        "Found moneyline data",
                                        home_team=home_team.value,
                                        away_team=away_team.value,
                                        moneyline_data=current_moneyline_data,
                                    )
                                    seen_moneyline_values.add(moneyline_key)
                                    logged_count += 1

                                moneyline_data = current_moneyline_data
                            except (json.JSONDecodeError, TypeError):
                                logger.warning(
                                    "Invalid moneyline JSON",
                                    split_value=split.split_value,
                                    game_id=split.game_id,
                                )
                                continue

            # Now interpret the spread based on moneyline data
            if home_spread_line is not None and moneyline_data is not None:
                home_odds = moneyline_data.get("home", 0)
                away_odds = moneyline_data.get("away", 0)

                logger.debug(
                    "Interpreting spread with moneyline data",
                    home_team=home_team.value,
                    away_team=away_team.value,
                    raw_spread=home_spread_line,
                    home_odds=home_odds,
                    away_odds=away_odds,
                )

                # In betting, negative odds = favorite, positive odds = underdog
                # If home team is favorite (negative odds), they give points (negative spread)
                # If home team is underdog (positive odds), they get points (positive spread)
                if (
                    home_odds < away_odds
                ):  # Home team is favorite (more negative = stronger favorite)
                    home_spread_line = -abs(home_spread_line)  # Home gives points
                else:  # Home team is underdog
                    home_spread_line = abs(home_spread_line)  # Home gets points

                logger.debug(
                    "Final spread assignment",
                    home_team=home_team.value,
                    away_team=away_team.value,
                    home_spread_line=home_spread_line,
                    interpretation="favorite gives points"
                    if home_spread_line < 0
                    else "underdog gets points",
                )

            elif home_spread_line is not None:
                # No moneyline data available, use fallback logic
                # In standard sportsbook convention, the raw spread is usually quoted for the favorite
                # Since we don't know who's favored, we'll assume the home team is a slight underdog
                # This is a fallback and should be rare if moneyline data is available
                logger.debug(
                    "No moneyline data, using fallback spread logic",
                    home_team=home_team.value,
                    away_team=away_team.value,
                    raw_spread=home_spread_line,
                )

                # For MLB, home field advantage is usually small, so assume home is underdog if unclear
                home_spread_line = abs(home_spread_line)  # Home gets points as underdog

            logger.debug(
                "Final betting lines",
                home_team=home_team.value,
                away_team=away_team.value,
                total_line=total_line,
                home_spread_line=home_spread_line,
            )

            return total_line, home_spread_line

        except Exception as e:
            logger.error(
                "Failed to get betting lines",
                home_team=home_team.value,
                away_team=away_team.value,
                error=str(e),
            )
            return None, None

    def validate_game_data(self, game: Game) -> bool:
        """
        Validate game data integrity.

        Args:
            game: Game to validate

        Returns:
            True if valid, False otherwise
        """
        try:
            # Basic validation checks
            if not game.home_team or not game.away_team:
                return False

            if game.home_team == game.away_team:
                return False

            # Add more validation as needed
            return True

        except Exception as e:
            logger.error("Game validation failed", error=str(e))
            return False


__all__ = ["GameUpdateError", "GameUpdater"]
