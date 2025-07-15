"""
Game Outcome Service

A comprehensive service for managing MLB game outcomes, including:
- Daily game updates
- Specific date updates
- Full season refresh
- Database status and statistics
"""

import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

from mlb_sharp_betting.core.logging import get_logger
from mlb_sharp_betting.db.connection import get_db_manager
from mlb_sharp_betting.db.repositories import get_game_outcome_repository
from mlb_sharp_betting.models.game_outcome import GameOutcome
from mlb_sharp_betting.services.game_updater import GameUpdater

logger = get_logger(__name__)


@dataclass
class GameUpdateSummary:
    """Summary of game update operation"""

    total_games_processed: int
    dates_processed: list[str]
    betting_statistics: dict[str, Any]
    database_status: dict[str, Any]
    execution_time_seconds: float
    use_betting_lines: bool
    errors: list[str] = None


@dataclass
class SeasonRefreshResult:
    """Result of full season refresh operation"""

    season_year: int
    start_date: date
    end_date: date
    total_games_processed: int
    dates_processed: int
    dates_skipped: int
    total_execution_time_seconds: float
    daily_summaries: list[GameUpdateSummary]
    errors: list[str] = None


class GameOutcomeService:
    """Service for managing MLB game outcomes"""

    def __init__(self):
        self.game_updater = GameUpdater()
        self.db_manager = get_db_manager()
        self.outcome_repo = get_game_outcome_repository(self.db_manager)

        # MLB season typically starts around April 1st
        self.season_start_month = 4
        self.season_start_day = 1

        logger.info("GameOutcomeService initialized")

    async def update_daily_games(
        self, use_betting_lines: bool = True
    ) -> GameUpdateSummary:
        """Update games for yesterday and today (daily update)"""
        start_time = datetime.now()

        logger.info("Starting daily game update", use_betting_lines=use_betting_lines)

        try:
            all_outcomes = []
            dates_processed = []

            # Update yesterday's games (most likely to be completed)
            yesterday = date.today() - timedelta(days=1)
            logger.info("Processing yesterday's games", date=yesterday)

            yesterday_outcomes = await self.game_updater.update_game_outcomes_for_date(
                yesterday, use_betting_lines
            )

            if yesterday_outcomes:
                all_outcomes.extend(yesterday_outcomes)
                dates_processed.append(yesterday.strftime("%Y-%m-%d"))
                logger.info(
                    "Found completed games from yesterday",
                    count=len(yesterday_outcomes),
                    date=yesterday,
                )

            # Update today's games (in case any finished early)
            today = date.today()
            logger.info("Processing today's games", date=today)

            today_outcomes = await self.game_updater.update_game_outcomes_for_date(
                today, use_betting_lines
            )

            if today_outcomes:
                all_outcomes.extend(today_outcomes)
                dates_processed.append(today.strftime("%Y-%m-%d"))
                logger.info(
                    "Found completed games from today",
                    count=len(today_outcomes),
                    date=today,
                )

            # Calculate execution time
            execution_time = (datetime.now() - start_time).total_seconds()

            # Generate summary
            summary = GameUpdateSummary(
                total_games_processed=len(all_outcomes),
                dates_processed=dates_processed,
                betting_statistics=self._calculate_betting_statistics(all_outcomes),
                database_status=await self._get_database_status(),
                execution_time_seconds=execution_time,
                use_betting_lines=use_betting_lines,
            )

            logger.info(
                "Daily game update completed",
                total_games=len(all_outcomes),
                execution_time=execution_time,
            )

            return summary

        except Exception as e:
            logger.error("Daily game update failed", error=str(e))
            raise

    async def update_specific_date(
        self, target_date: date, use_betting_lines: bool = True
    ) -> GameUpdateSummary:
        """Update games for a specific date"""
        start_time = datetime.now()

        logger.info(
            "Starting specific date update",
            date=target_date,
            use_betting_lines=use_betting_lines,
        )

        try:
            outcomes = await self.game_updater.update_game_outcomes_for_date(
                target_date, use_betting_lines
            )

            dates_processed = [target_date.strftime("%Y-%m-%d")] if outcomes else []

            # Calculate execution time
            execution_time = (datetime.now() - start_time).total_seconds()

            # Generate summary
            summary = GameUpdateSummary(
                total_games_processed=len(outcomes) if outcomes else 0,
                dates_processed=dates_processed,
                betting_statistics=self._calculate_betting_statistics(outcomes or []),
                database_status=await self._get_database_status(),
                execution_time_seconds=execution_time,
                use_betting_lines=use_betting_lines,
            )

            logger.info(
                "Specific date update completed",
                date=target_date,
                total_games=len(outcomes) if outcomes else 0,
                execution_time=execution_time,
            )

            return summary

        except Exception as e:
            logger.error("Specific date update failed", date=target_date, error=str(e))
            raise

    async def refresh_full_season(
        self,
        season_year: int | None = None,
        use_betting_lines: bool = False,
        batch_size: int = 7,
    ) -> SeasonRefreshResult:
        """Refresh game outcomes for the entire season"""
        start_time = datetime.now()

        # Determine season year
        if season_year is None:
            current_date = date.today()
            # If before April, use previous year as season year
            if current_date.month < 4:
                season_year = current_date.year - 1
            else:
                season_year = current_date.year

        # Calculate season date range
        season_start = date(season_year, self.season_start_month, self.season_start_day)
        season_end = date.today()  # Up to current date

        logger.info(
            "Starting full season refresh",
            season_year=season_year,
            start_date=season_start,
            end_date=season_end,
            use_betting_lines=use_betting_lines,
        )

        try:
            all_outcomes = []
            daily_summaries = []
            dates_processed = 0
            dates_skipped = 0
            errors = []

            # Process season in batches
            current_date = season_start

            while current_date <= season_end:
                batch_end = min(
                    current_date + timedelta(days=batch_size - 1), season_end
                )

                logger.info(
                    "Processing batch", batch_start=current_date, batch_end=batch_end
                )

                # Process each day in the batch
                batch_outcomes = []
                batch_dates = []

                for single_date in self._date_range(current_date, batch_end):
                    try:
                        outcomes = (
                            await self.game_updater.update_game_outcomes_for_date(
                                single_date, use_betting_lines
                            )
                        )

                        if outcomes:
                            batch_outcomes.extend(outcomes)
                            batch_dates.append(single_date.strftime("%Y-%m-%d"))
                            dates_processed += 1

                            logger.debug(
                                "Processed date",
                                date=single_date,
                                games_found=len(outcomes),
                            )
                        else:
                            dates_skipped += 1
                            logger.debug("No games found for date", date=single_date)

                    except Exception as e:
                        error_msg = f"Failed to process {single_date}: {str(e)}"
                        errors.append(error_msg)
                        logger.error(
                            "Date processing failed", date=single_date, error=str(e)
                        )
                        dates_skipped += 1

                # Create batch summary
                if batch_outcomes:
                    batch_summary = GameUpdateSummary(
                        total_games_processed=len(batch_outcomes),
                        dates_processed=batch_dates,
                        betting_statistics=self._calculate_betting_statistics(
                            batch_outcomes
                        ),
                        database_status=await self._get_database_status(),
                        execution_time_seconds=0,  # Not tracking individual batch time
                        use_betting_lines=use_betting_lines,
                    )
                    daily_summaries.append(batch_summary)
                    all_outcomes.extend(batch_outcomes)

                # Move to next batch
                current_date = batch_end + timedelta(days=1)

                # Small delay to avoid overwhelming the system
                await asyncio.sleep(0.1)

            # Calculate total execution time
            total_execution_time = (datetime.now() - start_time).total_seconds()

            result = SeasonRefreshResult(
                season_year=season_year,
                start_date=season_start,
                end_date=season_end,
                total_games_processed=len(all_outcomes),
                dates_processed=dates_processed,
                dates_skipped=dates_skipped,
                total_execution_time_seconds=total_execution_time,
                daily_summaries=daily_summaries,
                errors=errors if errors else None,
            )

            logger.info(
                "Full season refresh completed",
                season_year=season_year,
                total_games=len(all_outcomes),
                dates_processed=dates_processed,
                execution_time=total_execution_time,
            )

            return result

        except Exception as e:
            logger.error("Full season refresh failed", error=str(e))
            raise

    async def get_database_status(self) -> dict[str, Any]:
        """Get comprehensive database status"""
        return await self._get_database_status()

    def _calculate_betting_statistics(
        self, outcomes: list[GameOutcome]
    ) -> dict[str, Any]:
        """Calculate betting statistics from game outcomes"""
        if not outcomes:
            return {
                "total_games": 0,
                "home_wins": 0,
                "away_wins": 0,
                "home_win_percentage": 0,
                "overs": 0,
                "unders": 0,
                "over_percentage": 0,
                "home_covers": 0,
                "away_covers": 0,
                "home_cover_percentage": 0,
            }

        total_games = len(outcomes)
        home_wins = sum(1 for outcome in outcomes if outcome.home_win)
        away_wins = total_games - home_wins
        overs = sum(1 for outcome in outcomes if outcome.over)
        unders = total_games - overs
        home_covers = sum(1 for outcome in outcomes if outcome.home_cover_spread)
        away_covers = total_games - home_covers

        return {
            "total_games": total_games,
            "home_wins": home_wins,
            "away_wins": away_wins,
            "home_win_percentage": (home_wins / total_games) * 100,
            "overs": overs,
            "unders": unders,
            "over_percentage": (overs / total_games) * 100,
            "home_covers": home_covers,
            "away_covers": away_covers,
            "home_cover_percentage": (home_covers / total_games) * 100,
        }

    async def _get_database_status(self) -> dict[str, Any]:
        """Get database status and recent outcomes"""
        try:
            recent_outcomes = await self.outcome_repo.get_recent_outcomes(limit=10)

            if recent_outcomes:
                latest_date = max(outcome.game_date for outcome in recent_outcomes)
                total_outcomes = len(recent_outcomes)

                return {
                    "status": "connected",
                    "recent_outcomes_count": total_outcomes,
                    "latest_game_date": latest_date.strftime("%Y-%m-%d"),
                    "has_data": True,
                }
            else:
                return {
                    "status": "connected",
                    "recent_outcomes_count": 0,
                    "latest_game_date": None,
                    "has_data": False,
                }
        except Exception as e:
            return {"status": "error", "error": str(e), "has_data": False}

    def _date_range(self, start_date: date, end_date: date):
        """Generate date range iterator"""
        current = start_date
        while current <= end_date:
            yield current
            current += timedelta(days=1)


# Service factory function
def get_game_outcome_service() -> GameOutcomeService:
    """Get GameOutcomeService instance"""
    return GameOutcomeService()
