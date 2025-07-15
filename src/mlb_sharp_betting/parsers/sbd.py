"""SBD (SportsBettingDime) data parser."""

from datetime import datetime
from typing import Any

import pytz
import structlog
from dateutil import parser as date_parser

from ..models.game import Team
from ..models.splits import BettingSplit, DataSource, SplitType
from ..services.mlb_api_service import MLBStatsAPIService
from ..services.team_mapper import TeamMapper
from .base import BaseParser

logger = structlog.get_logger(__name__)


class SBDParser(BaseParser):
    """Parser for SportsBettingDime API data format."""

    def __init__(self, **kwargs: Any) -> None:
        """Initialize SBD parser with team mapping and MLB API service."""
        super().__init__(parser_name="SBD", **kwargs)
        self.team_mapper = TeamMapper()
        self.mlb_api_service = MLBStatsAPIService()

        # Timezone handling for SBD data
        self.utc = pytz.UTC
        self.eastern = pytz.timezone("US/Eastern")

    @property
    def target_model_class(self) -> type[BettingSplit]:
        """Get the target model class for this parser."""
        return BettingSplit

    def _parse_datetime(self, datetime_str: str) -> datetime:
        """
        Parse datetime string from SBD API and convert to EST.

        Args:
            datetime_str: ISO datetime string from API

        Returns:
            Parsed datetime converted to EST timezone-naive for consistency
        """
        try:
            # Parse ISO datetime
            dt_utc = date_parser.isoparse(datetime_str)

            # Ensure UTC timezone if naive
            if dt_utc.tzinfo is None:
                dt_utc = self.utc.localize(dt_utc)

            # Convert UTC to EST for proper date calculation
            est_tz = pytz.timezone("US/Eastern")
            dt_est = dt_utc.astimezone(est_tz)

            # Make timezone-naive EST datetime for consistency
            dt_est_naive = dt_est.replace(tzinfo=None)

            return dt_est_naive

        except Exception as e:
            logger.warning(
                "Failed to parse datetime", datetime_str=datetime_str, error=str(e)
            )
            # Return current time in EST
            est_tz = pytz.timezone("US/Eastern")
            return datetime.now(est_tz).replace(tzinfo=None)

    def _normalize_team_name(
        self, team_name: str, team_code: str = ""
    ) -> Team | None:
        """
        Normalize team name to Team enum using team mapper.

        Args:
            team_name: Team name from API (e.g., "Phillies")
            team_code: Team code from API (e.g., "PHI")

        Returns:
            Team enum or None if not found
        """
        try:
            # Try team code first (more reliable for short abbreviations)
            if team_code:
                team = self.team_mapper.map_team_name(team_code)
                if team:
                    return team

            # Fallback to team name
            team = self.team_mapper.map_team_name(team_name)
            if team:
                return team

            logger.warning(
                "Could not normalize team", team_name=team_name, team_code=team_code
            )
            return None

        except Exception as e:
            logger.error(
                "Team normalization failed",
                team_name=team_name,
                team_code=team_code,
                error=str(e),
            )
            return None

    def _get_official_game_id(self, game_data: dict[str, Any]) -> str | None:
        """
        Get the official MLB game ID using MLB Stats API.

        Args:
            game_data: Game information from SBD API

        Returns:
            Official MLB game ID or None if not found
        """
        try:
            # Extract team names from SBD data
            home_team = game_data.get("home_team", "")
            away_team = game_data.get("away_team", "")

            # Parse game datetime
            game_datetime = None
            if game_data.get("date"):
                game_datetime = self._parse_datetime(game_data["date"])

            # Get official game ID from MLB API
            official_game_id = self.mlb_api_service.get_official_game_id(
                home_team=home_team, away_team=away_team, game_datetime=game_datetime
            )

            if official_game_id:
                logger.debug(
                    "Got official game ID",
                    sbd_game_id=game_data.get("game_id", "unknown"),
                    official_game_id=official_game_id,
                    matchup=f"{away_team} @ {home_team}",
                )
                return official_game_id
            else:
                logger.warning(
                    "Could not find official game ID, using SBD game ID",
                    sbd_game_id=game_data.get("game_id", "unknown"),
                    matchup=f"{away_team} @ {home_team}",
                )
                # Fallback to SBD game ID if MLB API lookup fails
                return game_data.get("game_id")

        except Exception as e:
            logger.error(
                "Failed to get official game ID", game_data=game_data, error=str(e)
            )
            # Fallback to SBD game ID
            return game_data.get("game_id")

    def _parse_spread_split(
        self, game_data: dict[str, Any], spread_data: dict[str, Any]
    ) -> BettingSplit | None:
        """
        Parse spread betting split from SBD data.

        Args:
            game_data: Game information
            spread_data: Spread betting data

        Returns:
            BettingSplit object or None if parsing fails
        """
        try:
            home_data = spread_data.get("home", {})
            away_data = spread_data.get("away", {})

            # Parse teams
            home_team = self._normalize_team_name(
                game_data["home_team"], game_data.get("home_code", "")
            )
            away_team = self._normalize_team_name(
                game_data["away_team"], game_data.get("away_code", "")
            )

            if not home_team or not away_team:
                return None

            # Get official game ID
            official_game_id = self._get_official_game_id(game_data)
            if not official_game_id:
                logger.warning("No game ID available for spread split")
                return None

            # Parse datetime
            game_datetime = self._parse_datetime(game_data["date"])

            # Parse last updated time
            updated_str = spread_data.get("updated")
            last_updated = (
                self._parse_datetime(updated_str)
                if updated_str
                else datetime.now(pytz.UTC)
            )

            # Parse spread value (try to get actual spread value)
            spread_value = None
            if "spread" in home_data:
                try:
                    spread_value = float(home_data["spread"])
                except (ValueError, TypeError):
                    pass

            # Calculate sharp action (significant difference between bets % and stake %)
            home_bets_pct = home_data.get("betsPercentage", 0.0)
            home_stake_pct = home_data.get("stakePercentage", 0.0)
            sharp_action_detected = abs(home_bets_pct - home_stake_pct) >= 10.0

            # Convert sharp action to string format
            sharp_action = (
                "home"
                if sharp_action_detected and home_stake_pct > home_bets_pct
                else None
            )
            if sharp_action_detected and not sharp_action:
                sharp_action = "away"

            # Extract bet counts from SBD data
            home_bets_count = home_data.get("bets")
            away_bets_count = away_data.get("bets")

            betting_split = BettingSplit(
                game_id=official_game_id,  # Use official MLB game ID
                home_team=home_team,
                away_team=away_team,
                game_datetime=game_datetime,
                split_type=SplitType.SPREAD,
                split_value=spread_value,
                source=DataSource.SBD,
                book=None,  # Default book for SBD aggregated data
                last_updated=last_updated,
                home_or_over_bets_percentage=home_bets_pct,
                away_or_under_bets_percentage=away_data.get("betsPercentage", 0.0),
                home_or_over_stake_percentage=home_stake_pct,
                away_or_under_stake_percentage=away_data.get("stakePercentage", 0.0),
                home_or_over_bets=home_bets_count,
                away_or_under_bets=away_bets_count,
                sharp_action=sharp_action,
            )

            logger.debug(
                "Parsed spread split",
                game_id=official_game_id,
                spread_value=spread_value,
                sharp_action=sharp_action,
            )

            return betting_split

        except Exception as e:
            logger.error(
                "Failed to parse spread split",
                game_id=game_data.get("game_id", "unknown"),
                error=str(e),
            )
            return None

    def _parse_total_split(
        self, game_data: dict[str, Any], total_data: dict[str, Any]
    ) -> BettingSplit | None:
        """
        Parse total (over/under) betting split from SBD data.

        Args:
            game_data: Game information
            total_data: Total betting data

        Returns:
            BettingSplit object or None if parsing fails
        """
        try:
            over_data = total_data.get("over", {})
            under_data = total_data.get("under", {})

            # Parse teams
            home_team = self._normalize_team_name(
                game_data["home_team"], game_data.get("home_code", "")
            )
            away_team = self._normalize_team_name(
                game_data["away_team"], game_data.get("away_code", "")
            )

            if not home_team or not away_team:
                return None

            # Get official game ID
            official_game_id = self._get_official_game_id(game_data)
            if not official_game_id:
                logger.warning("No game ID available for total split")
                return None

            # Parse datetime
            game_datetime = self._parse_datetime(game_data["date"])

            # Parse last updated time
            updated_str = total_data.get("updated")
            last_updated = (
                self._parse_datetime(updated_str)
                if updated_str
                else datetime.now(pytz.UTC)
            )

            # Parse total value (try to get actual total value)
            total_value = None
            raw_total = over_data.get("total") or under_data.get("total")
            if raw_total:
                try:
                    total_value = float(raw_total)
                except (ValueError, TypeError):
                    pass

            # Calculate sharp action
            over_bets_pct = over_data.get("betsPercentage", 0.0)
            over_stake_pct = over_data.get("stakePercentage", 0.0)
            sharp_action_detected = abs(over_bets_pct - over_stake_pct) >= 10.0

            # Convert sharp action to string format
            sharp_action = (
                "over"
                if sharp_action_detected and over_stake_pct > over_bets_pct
                else None
            )
            if sharp_action_detected and not sharp_action:
                sharp_action = "under"

            # Extract bet counts from SBD data
            over_bets_count = over_data.get("bets")
            under_bets_count = under_data.get("bets")

            betting_split = BettingSplit(
                game_id=official_game_id,  # Use official MLB game ID
                home_team=home_team,
                away_team=away_team,
                game_datetime=game_datetime,
                split_type=SplitType.TOTAL,
                split_value=total_value,
                source=DataSource.SBD,
                book=None,
                last_updated=last_updated,
                home_or_over_bets_percentage=over_bets_pct,
                away_or_under_bets_percentage=under_data.get("betsPercentage", 0.0),
                home_or_over_stake_percentage=over_stake_pct,
                away_or_under_stake_percentage=under_data.get("stakePercentage", 0.0),
                home_or_over_bets=over_bets_count,
                away_or_under_bets=under_bets_count,
                sharp_action=sharp_action,
            )

            logger.debug(
                "Parsed total split",
                game_id=official_game_id,
                total_value=total_value,
                sharp_action=sharp_action,
            )

            return betting_split

        except Exception as e:
            logger.error(
                "Failed to parse total split",
                game_id=game_data.get("game_id", "unknown"),
                error=str(e),
            )
            return None

    def _parse_moneyline_split(
        self, game_data: dict[str, Any], ml_data: dict[str, Any]
    ) -> BettingSplit | None:
        """
        Parse moneyline betting split from SBD data.

        Args:
            game_data: Game information
            ml_data: Moneyline betting data

        Returns:
            BettingSplit object or None if parsing fails
        """
        try:
            home_data = ml_data.get("home", {})
            away_data = ml_data.get("away", {})

            # Parse teams
            home_team = self._normalize_team_name(
                game_data["home_team"], game_data.get("home_code", "")
            )
            away_team = self._normalize_team_name(
                game_data["away_team"], game_data.get("away_code", "")
            )

            if not home_team or not away_team:
                return None

            # Get official game ID
            official_game_id = self._get_official_game_id(game_data)
            if not official_game_id:
                logger.warning("No game ID available for moneyline split")
                return None

            # Parse datetime
            game_datetime = self._parse_datetime(game_data["date"])

            # Parse last updated time
            updated_str = ml_data.get("updated")
            last_updated = (
                self._parse_datetime(updated_str)
                if updated_str
                else datetime.now(pytz.UTC)
            )

            # Moneyline doesn't need a split_value (set to None)
            moneyline_value = None

            # Calculate sharp action
            home_bets_pct = home_data.get("betsPercentage", 0.0)
            home_stake_pct = home_data.get("stakePercentage", 0.0)
            sharp_action_detected = abs(home_bets_pct - home_stake_pct) >= 10.0

            # Convert sharp action to string format
            sharp_action = (
                "home"
                if sharp_action_detected and home_stake_pct > home_bets_pct
                else None
            )
            if sharp_action_detected and not sharp_action:
                sharp_action = "away"

            # Extract bet counts from SBD data
            home_bets_count = home_data.get("bets")
            away_bets_count = away_data.get("bets")

            betting_split = BettingSplit(
                game_id=official_game_id,  # Use official MLB game ID
                home_team=home_team,
                away_team=away_team,
                game_datetime=game_datetime,
                split_type=SplitType.MONEYLINE,
                split_value=moneyline_value,
                source=DataSource.SBD,
                book=None,
                last_updated=last_updated,
                home_or_over_bets_percentage=home_bets_pct,
                away_or_under_bets_percentage=away_data.get("betsPercentage", 0.0),
                home_or_over_stake_percentage=home_stake_pct,
                away_or_under_stake_percentage=away_data.get("stakePercentage", 0.0),
                home_or_over_bets=home_bets_count,
                away_or_under_bets=away_bets_count,
                sharp_action=sharp_action,
            )

            logger.debug(
                "Parsed moneyline split",
                game_id=official_game_id,
                sharp_action=sharp_action,
            )

            return betting_split

        except Exception as e:
            logger.error(
                "Failed to parse moneyline split",
                game_id=game_data.get("game_id", "unknown"),
                error=str(e),
            )
            return None

    async def parse_raw_data(self, raw_data: dict[str, Any]) -> BettingSplit | None:
        """
        Parse a single SBD game data item into a BettingSplit object.

        Args:
            raw_data: Single game data dictionary from SBD scraper

        Returns:
            BettingSplit object or None if parsing fails
        """
        try:
            # The raw_data represents a single game, but we need to return one split
            # We'll prioritize moneyline, then spread, then total
            betting_splits = raw_data.get("betting_splits", {})

            for split_type in ["moneyline", "spread", "total"]:
                split_data = betting_splits.get(split_type)
                if not split_data:
                    continue

                # Parse based on split type
                if split_type == "spread":
                    return self._parse_spread_split(raw_data, split_data)
                elif split_type == "total":
                    return self._parse_total_split(raw_data, split_data)
                elif split_type == "moneyline":
                    return self._parse_moneyline_split(raw_data, split_data)

            logger.warning(
                "No valid splits found in game data",
                game_id=raw_data.get("game_id", "unknown"),
            )
            return None

        except Exception as e:
            logger.error(
                "Failed to parse game data",
                game_id=raw_data.get("game_id", "unknown"),
                error=str(e),
            )
            return None

    def parse_all_splits(self, raw_data: list[dict[str, Any]]) -> list[BettingSplit]:
        """
        Parse all splits from SBD raw data into BettingSplit objects.
        This method extracts ALL split types from each game and uses batch processing
        to efficiently get official MLB game IDs.

        Args:
            raw_data: List of game data from SBD scraper

        Returns:
            List of BettingSplit objects
        """
        logger.info("Starting SBD data parsing", games_count=len(raw_data))

        all_splits = []

        # Step 1: Batch get official game IDs for all games
        team_pairs = []
        game_id_mapping = {}  # Map from (home_team, away_team) to official game ID

        for game_data in raw_data:
            try:
                home_team = game_data.get("home_team", "")
                away_team = game_data.get("away_team", "")

                # Parse game datetime
                game_datetime = None
                if game_data.get("date"):
                    game_datetime = self._parse_datetime(game_data["date"])

                team_pairs.append((home_team, away_team, game_datetime))

            except Exception as e:
                logger.error(
                    "Failed to prepare game for batch processing",
                    game_id=game_data.get("game_id", "unknown"),
                    error=str(e),
                )

        # Batch get official IDs
        if team_pairs:
            logger.info("Batch fetching official game IDs", count=len(team_pairs))
            game_id_mapping = self.mlb_api_service.batch_get_game_ids(team_pairs)

        # Step 2: Parse splits using the official game IDs
        for game_data in raw_data:
            try:
                home_team = game_data.get("home_team", "")
                away_team = game_data.get("away_team", "")

                # Get official game ID from batch results
                official_game_id = game_id_mapping.get((home_team, away_team))
                if not official_game_id:
                    # Fallback to SBD game ID
                    official_game_id = game_data.get("game_id")
                    logger.warning(
                        "Using SBD game ID as fallback",
                        sbd_game_id=official_game_id,
                        matchup=f"{away_team} @ {home_team}",
                    )

                # Update game_data with official ID for parsing methods
                game_data_with_official_id = game_data.copy()
                game_data_with_official_id["official_game_id"] = official_game_id

                betting_splits = game_data.get("betting_splits", {})

                # Parse each split type
                for split_type in ["spread", "total", "moneyline"]:
                    split_data = betting_splits.get(split_type)
                    if not split_data:
                        continue

                    # Parse based on split type, passing the updated game data
                    parsed_split = None
                    if split_type == "spread":
                        parsed_split = self._parse_spread_split_with_id(
                            game_data_with_official_id, split_data
                        )
                    elif split_type == "total":
                        parsed_split = self._parse_total_split_with_id(
                            game_data_with_official_id, split_data
                        )
                    elif split_type == "moneyline":
                        parsed_split = self._parse_moneyline_split_with_id(
                            game_data_with_official_id, split_data
                        )

                    if parsed_split:
                        all_splits.append(parsed_split)
                        logger.debug(
                            "Successfully parsed split",
                            official_game_id=official_game_id,
                            split_type=split_type,
                        )

            except Exception as e:
                logger.error(
                    "Failed to parse game data",
                    game_id=game_data.get("game_id", "unknown"),
                    error=str(e),
                )

        logger.info(
            "SBD parsing completed",
            games_processed=len(raw_data),
            splits_parsed=len(all_splits),
        )

        return all_splits

    def _parse_spread_split_with_id(
        self, game_data: dict[str, Any], spread_data: dict[str, Any]
    ) -> BettingSplit | None:
        """
        Parse spread betting split with pre-fetched official game ID.

        Args:
            game_data: Game information with official_game_id
            spread_data: Spread betting data

        Returns:
            BettingSplit object or None if parsing fails
        """
        try:
            home_data = spread_data.get("home", {})
            away_data = spread_data.get("away", {})

            # Parse teams
            home_team = self._normalize_team_name(
                game_data["home_team"], game_data.get("home_code", "")
            )
            away_team = self._normalize_team_name(
                game_data["away_team"], game_data.get("away_code", "")
            )

            if not home_team or not away_team:
                return None

            # Use pre-fetched official game ID
            official_game_id = game_data.get("official_game_id")
            if not official_game_id:
                logger.warning("No official game ID available for spread split")
                return None

            # Parse datetime
            game_datetime = self._parse_datetime(game_data["date"])

            # Parse last updated time
            updated_str = spread_data.get("updated")
            last_updated = (
                self._parse_datetime(updated_str)
                if updated_str
                else datetime.now(pytz.UTC)
            )

            # Parse spread value (try to get actual spread value - optional)
            spread_value = None
            if "spread" in home_data:
                try:
                    spread_value = float(home_data["spread"])
                except (ValueError, TypeError):
                    pass

            # Check if we have betting data (don't require spread value)
            home_bets_pct = home_data.get("betsPercentage", 0.0)
            away_bets_pct = away_data.get("betsPercentage", 0.0)

            # If we don't have any betting data, skip this split
            if home_bets_pct == 0.0 and away_bets_pct == 0.0:
                return None

            # Calculate sharp action (significant difference between bets % and stake %)
            home_stake_pct = home_data.get("stakePercentage", 0.0)
            sharp_action_detected = abs(home_bets_pct - home_stake_pct) >= 10.0

            # Convert sharp action to string format
            sharp_action = (
                "home"
                if sharp_action_detected and home_stake_pct > home_bets_pct
                else None
            )
            if sharp_action_detected and not sharp_action:
                sharp_action = "away"

            # Extract bet counts from SBD data
            home_bets_count = home_data.get("bets")
            away_bets_count = away_data.get("bets")

            betting_split = BettingSplit(
                game_id=official_game_id,  # Use official MLB game ID
                home_team=home_team,
                away_team=away_team,
                game_datetime=game_datetime,
                split_type=SplitType.SPREAD,
                split_value=spread_value,  # May be None
                source=DataSource.SBD,
                book=None,  # Default book for SBD aggregated data
                last_updated=last_updated,
                home_or_over_bets_percentage=home_bets_pct,
                away_or_under_bets_percentage=away_bets_pct,
                home_or_over_stake_percentage=home_stake_pct,
                away_or_under_stake_percentage=away_data.get("stakePercentage", 0.0),
                home_or_over_bets=home_bets_count,
                away_or_under_bets=away_bets_count,
                sharp_action=sharp_action,
            )

            return betting_split

        except Exception as e:
            logger.error(
                "Failed to parse spread split",
                game_id=game_data.get("official_game_id", "unknown"),
                error=str(e),
            )
            return None

    def _parse_total_split_with_id(
        self, game_data: dict[str, Any], total_data: dict[str, Any]
    ) -> BettingSplit | None:
        """
        Parse total betting split with pre-fetched official game ID.

        Args:
            game_data: Game information with official_game_id
            total_data: Total betting data

        Returns:
            BettingSplit object or None if parsing fails
        """
        try:
            over_data = total_data.get("over", {})
            under_data = total_data.get("under", {})

            # Parse teams
            home_team = self._normalize_team_name(
                game_data["home_team"], game_data.get("home_code", "")
            )
            away_team = self._normalize_team_name(
                game_data["away_team"], game_data.get("away_code", "")
            )

            if not home_team or not away_team:
                return None

            # Use pre-fetched official game ID
            official_game_id = game_data.get("official_game_id")
            if not official_game_id:
                logger.warning("No official game ID available for total split")
                return None

            # Parse datetime
            game_datetime = self._parse_datetime(game_data["date"])

            # Parse last updated time
            updated_str = total_data.get("updated")
            last_updated = (
                self._parse_datetime(updated_str)
                if updated_str
                else datetime.now(pytz.UTC)
            )

            # Parse total value (try to get actual total value - optional)
            total_value = None
            raw_total = over_data.get("total") or under_data.get("total")
            if raw_total:
                try:
                    total_value = float(raw_total)
                except (ValueError, TypeError):
                    pass

            # Check if we have betting data (don't require total value)
            over_bets_pct = over_data.get("betsPercentage", 0.0)
            under_bets_pct = under_data.get("betsPercentage", 0.0)

            # If we don't have any betting data, skip this split
            if over_bets_pct == 0.0 and under_bets_pct == 0.0:
                return None

            # Calculate sharp action
            over_stake_pct = over_data.get("stakePercentage", 0.0)
            sharp_action_detected = abs(over_bets_pct - over_stake_pct) >= 10.0

            # Convert sharp action to string format
            sharp_action = (
                "over"
                if sharp_action_detected and over_stake_pct > over_bets_pct
                else None
            )
            if sharp_action_detected and not sharp_action:
                sharp_action = "under"

            # Extract bet counts from SBD data
            over_bets_count = over_data.get("bets")
            under_bets_count = under_data.get("bets")

            betting_split = BettingSplit(
                game_id=official_game_id,  # Use official MLB game ID
                home_team=home_team,
                away_team=away_team,
                game_datetime=game_datetime,
                split_type=SplitType.TOTAL,
                split_value=total_value,  # May be None
                source=DataSource.SBD,
                book=None,
                last_updated=last_updated,
                home_or_over_bets_percentage=over_bets_pct,
                away_or_under_bets_percentage=under_bets_pct,
                home_or_over_stake_percentage=over_stake_pct,
                away_or_under_stake_percentage=under_data.get("stakePercentage", 0.0),
                home_or_over_bets=over_bets_count,
                away_or_under_bets=under_bets_count,
                sharp_action=sharp_action,
            )

            return betting_split

        except Exception as e:
            logger.error(
                "Failed to parse total split",
                game_id=game_data.get("official_game_id", "unknown"),
                error=str(e),
            )
            return None

    def _parse_moneyline_split_with_id(
        self, game_data: dict[str, Any], ml_data: dict[str, Any]
    ) -> BettingSplit | None:
        """
        Parse moneyline betting split with pre-fetched official game ID.

        Args:
            game_data: Game information with official_game_id
            ml_data: Moneyline betting data

        Returns:
            BettingSplit object or None if parsing fails
        """
        try:
            home_data = ml_data.get("home", {})
            away_data = ml_data.get("away", {})

            # Parse teams
            home_team = self._normalize_team_name(
                game_data["home_team"], game_data.get("home_code", "")
            )
            away_team = self._normalize_team_name(
                game_data["away_team"], game_data.get("away_code", "")
            )

            if not home_team or not away_team:
                return None

            # Use pre-fetched official game ID
            official_game_id = game_data.get("official_game_id")
            if not official_game_id:
                logger.warning("No official game ID available for moneyline split")
                return None

            # Parse datetime
            game_datetime = self._parse_datetime(game_data["date"])

            # Parse last updated time
            updated_str = ml_data.get("updated")
            last_updated = (
                self._parse_datetime(updated_str)
                if updated_str
                else datetime.now(pytz.UTC)
            )

            # Moneyline doesn't need a split_value (set to None)
            moneyline_value = None

            # Calculate sharp action
            home_bets_pct = home_data.get("betsPercentage", 0.0)
            home_stake_pct = home_data.get("stakePercentage", 0.0)
            sharp_action_detected = abs(home_bets_pct - home_stake_pct) >= 10.0

            # Convert sharp action to string format
            sharp_action = (
                "home"
                if sharp_action_detected and home_stake_pct > home_bets_pct
                else None
            )
            if sharp_action_detected and not sharp_action:
                sharp_action = "away"

            # Extract bet counts from SBD data
            home_bets_count = home_data.get("bets")
            away_bets_count = away_data.get("bets")

            betting_split = BettingSplit(
                game_id=official_game_id,  # Use official MLB game ID
                home_team=home_team,
                away_team=away_team,
                game_datetime=game_datetime,
                split_type=SplitType.MONEYLINE,
                split_value=moneyline_value,
                source=DataSource.SBD,
                book=None,
                last_updated=last_updated,
                home_or_over_bets_percentage=home_bets_pct,
                away_or_under_bets_percentage=away_data.get("betsPercentage", 0.0),
                home_or_over_stake_percentage=home_stake_pct,
                away_or_under_stake_percentage=away_data.get("stakePercentage", 0.0),
                home_or_over_bets=home_bets_count,
                away_or_under_bets=away_bets_count,
                sharp_action=sharp_action,
            )

            return betting_split

        except Exception as e:
            logger.error(
                "Failed to parse moneyline split",
                game_id=game_data.get("official_game_id", "unknown"),
                error=str(e),
            )
            return None

    def validate_structure(self, data: Any) -> bool:
        """
        Validate SBD data structure.

        Args:
            data: Data to validate

        Returns:
            True if structure is valid
        """
        if not isinstance(data, list):
            return False

        for item in data:
            if not isinstance(item, dict):
                return False

            # Check required fields
            required_fields = [
                "game_id",
                "betting_splits",
                "home_team",
                "away_team",
                "date",
            ]
            if not all(field in item for field in required_fields):
                return False

            # Check betting splits structure
            betting_splits = item.get("betting_splits", {})
            if not isinstance(betting_splits, dict):
                return False

        return True


__all__ = ["SBDParser"]
