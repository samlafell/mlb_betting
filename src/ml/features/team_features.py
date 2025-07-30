"""
Team Feature Extractor
Extracts team performance features with MLB Stats API enrichment
Recent form, head-to-head, pitcher matchups, venue factors, weather impact
"""

import logging
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timedelta, date
from decimal import Decimal

import polars as pl
import numpy as np

from .models import TeamFeatures, BaseFeatureExtractor

# Proper package imports

logger = logging.getLogger(__name__)


class TeamFeatureExtractor(BaseFeatureExtractor):
    """
    Team performance and contextual feature extraction
    Integrates MLB Stats API data with venue, weather, and situational factors
    """

    def __init__(self, feature_version: str = "v2.1"):
        super().__init__(feature_version)
        self.recent_form_window = 10  # Last 10 games for recent form
        self.h2h_lookback_games = 10  # Head-to-head lookback
        self.rest_days_threshold = 3  # Days rest threshold for fatigue

    def get_required_columns(self) -> List[str]:
        """Required columns for team feature extraction"""
        return [
            "game_id",
            "home_team",
            "away_team",
            "game_datetime",
            "season",
            "venue_name",
            "venue_city",
            "venue_state",
            "temperature_fahrenheit",
            "wind_speed_mph",
            "wind_direction",
            "humidity_pct",
            "weather_condition",
            "home_pitcher_name",
            "away_pitcher_name",
            "home_pitcher_era",
            "away_pitcher_era",
            "home_pitcher_throws",
            "away_pitcher_throws",
        ]

    async def extract_features(
        self, df: pl.DataFrame, game_id: int, cutoff_time: datetime
    ) -> TeamFeatures:
        """
        Extract team performance and contextual features

        Args:
            df: Enhanced games data with team and pitcher information
            game_id: Game ID for feature extraction
            cutoff_time: Feature cutoff time

        Returns:
            TeamFeatures instance
        """
        try:
            logger.info(f"Extracting team features for game {game_id}")

            # Validate data quality
            data_quality = self.validate_data_quality(df, self.get_required_columns())
            if not data_quality["is_valid"]:
                logger.warning(
                    f"Team data quality issues for game {game_id}: {data_quality['missing_columns']}"
                )

            # Get target game information
            target_game = df.filter(pl.col("game_id") == game_id)

            if target_game.is_empty():
                logger.warning(f"No game data available for game {game_id}")
                return self._create_empty_features()

            game_info = target_game.row(0, named=True)
            home_team = game_info.get("home_team")
            away_team = game_info.get("away_team")
            game_date = game_info.get("game_datetime")

            if isinstance(game_date, str):
                game_date = datetime.fromisoformat(game_date.replace("Z", "+00:00"))

            # Extract recent form features
            recent_form_features = await self._extract_recent_form_features(
                df, home_team, away_team, game_date
            )

            # Extract head-to-head features
            h2h_features = await self._extract_h2h_features(
                df, home_team, away_team, game_date
            )

            # Extract season performance features
            season_features = await self._extract_season_features(
                df, home_team, away_team, game_info.get("season"), game_date
            )

            # Extract pitcher features
            pitcher_features = await self._extract_pitcher_features(game_info)

            # Extract venue features
            venue_features = await self._extract_venue_features(
                df, game_info, home_team, away_team
            )

            # Extract weather features
            weather_features = await self._extract_weather_features(game_info)

            # Extract rest and travel features
            rest_travel_features = await self._extract_rest_travel_features(
                df, home_team, away_team, game_date
            )

            # Extract situational features
            situational_features = await self._extract_situational_features(
                game_info, game_date
            )

            # Combine all features
            features = TeamFeatures(
                feature_version=self.feature_version,
                mlb_api_last_updated=cutoff_time,
                **recent_form_features,
                **h2h_features,
                **season_features,
                **pitcher_features,
                **venue_features,
                **weather_features,
                **rest_travel_features,
                **situational_features,
            )

            logger.info(f"Successfully extracted team features for game {game_id}")
            return features

        except Exception as e:
            logger.error(f"Error extracting team features for game {game_id}: {e}")
            raise

    async def _extract_recent_form_features(
        self, df: pl.DataFrame, home_team: str, away_team: str, game_date: datetime
    ) -> Dict[str, Any]:
        """Extract recent form metrics with time decay weighting"""
        try:
            cutoff_date = game_date - timedelta(
                days=30
            )  # Look back 30 days for recent games

            # Get recent games for both teams
            home_recent = (
                df.filter(
                    (
                        (pl.col("home_team") == home_team)
                        | (pl.col("away_team") == home_team)
                    )
                    & (pl.col("game_datetime") >= cutoff_date)
                    & (pl.col("game_datetime") < game_date)
                    & pl.col("home_score").is_not_null()
                    & pl.col("away_score").is_not_null()
                )
                .sort("game_datetime", descending=True)
                .head(self.recent_form_window)
            )

            away_recent = (
                df.filter(
                    (
                        (pl.col("home_team") == away_team)
                        | (pl.col("away_team") == away_team)
                    )
                    & (pl.col("game_datetime") >= cutoff_date)
                    & (pl.col("game_datetime") < game_date)
                    & pl.col("home_score").is_not_null()
                    & pl.col("away_score").is_not_null()
                )
                .sort("game_datetime", descending=True)
                .head(self.recent_form_window)
            )

            # Calculate weighted recent form
            home_form = self._calculate_weighted_form(home_recent, home_team)
            away_form = self._calculate_weighted_form(away_recent, away_team)

            # Calculate record strings
            home_last_5 = self._get_record_string(home_recent.head(5), home_team)
            away_last_5 = self._get_record_string(away_recent.head(5), away_team)
            home_last_10 = self._get_record_string(home_recent, home_team)
            away_last_10 = self._get_record_string(away_recent, away_team)

            return {
                "home_recent_form_weighted": Decimal(str(home_form))
                if home_form is not None
                else None,
                "away_recent_form_weighted": Decimal(str(away_form))
                if away_form is not None
                else None,
                "home_last_5_record": home_last_5,
                "away_last_5_record": away_last_5,
                "home_last_10_record": home_last_10,
                "away_last_10_record": away_last_10,
            }

        except Exception as e:
            logger.error(f"Error extracting recent form features: {e}")
            return {}

    async def _extract_h2h_features(
        self, df: pl.DataFrame, home_team: str, away_team: str, game_date: datetime
    ) -> Dict[str, Any]:
        """Extract head-to-head historical performance"""
        try:
            # Get head-to-head games (last 3 years)
            cutoff_date = game_date - timedelta(days=1095)  # 3 years

            h2h_games = (
                df.filter(
                    (
                        (
                            (pl.col("home_team") == home_team)
                            & (pl.col("away_team") == away_team)
                        )
                        | (
                            (pl.col("home_team") == away_team)
                            & (pl.col("away_team") == home_team)
                        )
                    )
                    & (pl.col("game_datetime") >= cutoff_date)
                    & (pl.col("game_datetime") < game_date)
                    & pl.col("home_score").is_not_null()
                    & pl.col("away_score").is_not_null()
                )
                .sort("game_datetime", descending=True)
                .head(self.h2h_lookback_games)
            )

            if h2h_games.is_empty():
                return {
                    "h2h_home_wins_last_10": 0,
                    "h2h_away_wins_last_10": 0,
                    "h2h_total_games": 0,
                    "h2h_avg_total_runs": None,
                    "h2h_home_advantage": None,
                }

            # Count wins for each team when playing each other
            home_wins = 0
            away_wins = 0
            total_runs = []
            home_advantage_scores = []

            for row in h2h_games.rows(named=True):
                game_home = row["home_team"]
                game_away = row["away_team"]
                home_score = row.get("home_score", 0) or 0
                away_score = row.get("away_score", 0) or 0

                total_runs.append(home_score + away_score)

                # Determine winner and update counts
                if home_score > away_score:  # Home team won
                    if game_home == home_team:
                        home_wins += 1
                        home_advantage_scores.append(1.0)  # Home team won at home
                    else:
                        away_wins += 1
                        home_advantage_scores.append(0.0)  # Away team won at home
                elif away_score > home_score:  # Away team won
                    if game_away == home_team:
                        home_wins += 1
                        home_advantage_scores.append(
                            0.5
                        )  # Home team won on road (still good)
                    else:
                        away_wins += 1
                        home_advantage_scores.append(0.5)  # Away team won on road

            avg_total_runs = np.mean(total_runs) if total_runs else None
            home_advantage = (
                np.mean(home_advantage_scores) if home_advantage_scores else None
            )

            return {
                "h2h_home_wins_last_10": home_wins,
                "h2h_away_wins_last_10": away_wins,
                "h2h_total_games": h2h_games.height,
                "h2h_avg_total_runs": Decimal(str(avg_total_runs))
                if avg_total_runs is not None
                else None,
                "h2h_home_advantage": Decimal(str(home_advantage))
                if home_advantage is not None
                else None,
            }

        except Exception as e:
            logger.error(f"Error extracting H2H features: {e}")
            return {}

    async def _extract_season_features(
        self,
        df: pl.DataFrame,
        home_team: str,
        away_team: str,
        season: int,
        game_date: datetime,
    ) -> Dict[str, Any]:
        """Extract season performance metrics"""
        try:
            # Get season games up to current date
            season_games = df.filter(
                (pl.col("season") == season)
                & (pl.col("game_datetime") < game_date)
                & pl.col("home_score").is_not_null()
                & pl.col("away_score").is_not_null()
            )

            # Home team season stats
            home_season = season_games.filter(
                (pl.col("home_team") == home_team) | (pl.col("away_team") == home_team)
            )

            # Away team season stats
            away_season = season_games.filter(
                (pl.col("home_team") == away_team) | (pl.col("away_team") == away_team)
            )

            # Calculate season records and stats
            home_stats = self._calculate_season_stats(home_season, home_team)
            away_stats = self._calculate_season_stats(away_season, away_team)

            return {
                "home_season_record": home_stats.get("record"),
                "away_season_record": away_stats.get("record"),
                "home_win_pct": Decimal(str(home_stats["win_pct"]))
                if home_stats.get("win_pct") is not None
                else None,
                "away_win_pct": Decimal(str(away_stats["win_pct"]))
                if away_stats.get("win_pct") is not None
                else None,
                "home_runs_per_game": Decimal(str(home_stats["runs_per_game"]))
                if home_stats.get("runs_per_game") is not None
                else None,
                "away_runs_per_game": Decimal(str(away_stats["runs_per_game"]))
                if away_stats.get("runs_per_game") is not None
                else None,
                "home_runs_allowed_per_game": Decimal(
                    str(home_stats["runs_allowed_per_game"])
                )
                if home_stats.get("runs_allowed_per_game") is not None
                else None,
                "away_runs_allowed_per_game": Decimal(
                    str(away_stats["runs_allowed_per_game"])
                )
                if away_stats.get("runs_allowed_per_game") is not None
                else None,
            }

        except Exception as e:
            logger.error(f"Error extracting season features: {e}")
            return {}

    async def _extract_pitcher_features(
        self, game_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Extract pitcher-specific features"""
        try:
            pitcher_features = {
                "home_pitcher_season_era": None,
                "away_pitcher_season_era": None,
                "home_pitcher_whip": None,
                "away_pitcher_whip": None,
                "home_pitcher_k9": None,
                "away_pitcher_k9": None,
                "home_pitcher_hr9": None,
                "away_pitcher_hr9": None,
                "home_pitcher_vs_opponent_era": None,
                "away_pitcher_vs_opponent_era": None,
                "home_pitcher_opponent_games": 0,
                "away_pitcher_opponent_games": 0,
            }

            # Get basic pitcher info from game data
            if game_info.get("home_pitcher_era"):
                pitcher_features["home_pitcher_season_era"] = Decimal(
                    str(game_info["home_pitcher_era"])
                )

            if game_info.get("away_pitcher_era"):
                pitcher_features["away_pitcher_season_era"] = Decimal(
                    str(game_info["away_pitcher_era"])
                )

            # TODO: Integrate with MLB Stats API for detailed pitcher stats
            # This would include WHIP, K/9, HR/9, opponent-specific stats, etc.
            # For now, we'll use placeholder logic

            return pitcher_features

        except Exception as e:
            logger.error(f"Error extracting pitcher features: {e}")
            return {}

    async def _extract_venue_features(
        self,
        df: pl.DataFrame,
        game_info: Dict[str, Any],
        home_team: str,
        away_team: str,
    ) -> Dict[str, Any]:
        """Extract venue-specific performance factors"""
        try:
            venue_name = game_info.get("venue_name")

            if not venue_name:
                return {
                    "home_field_advantage_factor": None,
                    "venue_total_factor": None,
                    "venue_home_team_factor": None,
                    "venue_away_team_factor": None,
                }

            # Get historical games at this venue (last 2 years)
            cutoff_date = datetime.now() - timedelta(days=730)

            venue_games = df.filter(
                (pl.col("venue_name") == venue_name)
                & (pl.col("game_datetime") >= cutoff_date)
                & pl.col("home_score").is_not_null()
                & pl.col("away_score").is_not_null()
            )

            if venue_games.is_empty():
                return {
                    "home_field_advantage_factor": None,
                    "venue_total_factor": None,
                    "venue_home_team_factor": None,
                    "venue_away_team_factor": None,
                }

            # Calculate venue factors
            venue_stats = venue_games.select(
                [
                    (
                        (pl.col("home_score") > pl.col("away_score")).sum() / pl.len()
                    ).alias("home_win_rate"),
                    (pl.col("home_score") + pl.col("away_score"))
                    .mean()
                    .alias("avg_total_runs"),
                ]
            )

            if not venue_stats.is_empty():
                stats = venue_stats.row(0, named=True)
                home_win_rate = stats.get("home_win_rate", 0.5) or 0.5
                avg_total = stats.get("avg_total_runs", 9.0) or 9.0

                # Calculate specific team performance at venue
                home_team_games = venue_games.filter(
                    (pl.col("home_team") == home_team)
                    | (pl.col("away_team") == home_team)
                )
                away_team_games = venue_games.filter(
                    (pl.col("home_team") == away_team)
                    | (pl.col("away_team") == away_team)
                )

                home_team_factor = self._calculate_team_venue_factor(
                    home_team_games, home_team
                )
                away_team_factor = self._calculate_team_venue_factor(
                    away_team_games, away_team
                )

                return {
                    "home_field_advantage_factor": Decimal(str(home_win_rate)),
                    "venue_total_factor": Decimal(
                        str(avg_total / 9.0)
                    ),  # Normalize to league average
                    "venue_home_team_factor": Decimal(str(home_team_factor))
                    if home_team_factor is not None
                    else None,
                    "venue_away_team_factor": Decimal(str(away_team_factor))
                    if away_team_factor is not None
                    else None,
                }

            return {}

        except Exception as e:
            logger.error(f"Error extracting venue features: {e}")
            return {}

    async def _extract_weather_features(
        self, game_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Extract weather impact features"""
        try:
            temp = game_info.get("temperature_fahrenheit")
            wind_speed = game_info.get("wind_speed_mph")
            wind_direction = game_info.get("wind_direction")
            humidity = game_info.get("humidity_pct")

            # Calculate weather impact on totals
            temperature_impact = None
            wind_impact = None

            if temp is not None:
                # Warmer temperatures generally increase offense
                # Normalize around 75°F as neutral
                temperature_impact = (temp - 75) / 100.0  # Scale impact

            if wind_speed is not None:
                # Wind impact depends on direction (in/out affects totals)
                base_wind_impact = wind_speed / 20.0  # Scale wind speed

                # Adjust for direction (simplified - would need ballpark specifics)
                if wind_direction in ["N", "S"]:  # Cross-wind (minimal impact)
                    wind_impact = base_wind_impact * 0.3
                elif wind_direction in ["E", "W"]:  # In/out wind
                    wind_impact = base_wind_impact * 1.0
                else:
                    wind_impact = base_wind_impact * 0.5

            return {
                "temperature_impact_total": Decimal(str(temperature_impact))
                if temperature_impact is not None
                else None,
                "wind_impact_total": Decimal(str(wind_impact))
                if wind_impact is not None
                else None,
                "weather_advantage_home": None,  # Would need ballpark-specific analysis
                "weather_advantage_away": None,
            }

        except Exception as e:
            logger.error(f"Error extracting weather features: {e}")
            return {}

    async def _extract_rest_travel_features(
        self, df: pl.DataFrame, home_team: str, away_team: str, game_date: datetime
    ) -> Dict[str, Any]:
        """Extract rest days and travel factors"""
        try:
            # Find last games for each team
            home_last_game = (
                df.filter(
                    (
                        (pl.col("home_team") == home_team)
                        | (pl.col("away_team") == home_team)
                    )
                    & (pl.col("game_datetime") < game_date)
                    & pl.col("home_score").is_not_null()
                )
                .sort("game_datetime", descending=True)
                .head(1)
            )

            away_last_game = (
                df.filter(
                    (
                        (pl.col("home_team") == away_team)
                        | (pl.col("away_team") == away_team)
                    )
                    & (pl.col("game_datetime") < game_date)
                    & pl.col("away_score").is_not_null()
                )
                .sort("game_datetime", descending=True)
                .head(1)
            )

            home_days_rest = 0
            away_days_rest = 0

            if not home_last_game.is_empty():
                last_game_date = home_last_game.select("game_datetime").item()
                if isinstance(last_game_date, str):
                    last_game_date = datetime.fromisoformat(
                        last_game_date.replace("Z", "+00:00")
                    )
                home_days_rest = (game_date - last_game_date).days

            if not away_last_game.is_empty():
                last_game_date = away_last_game.select("game_datetime").item()
                if isinstance(last_game_date, str):
                    last_game_date = datetime.fromisoformat(
                        last_game_date.replace("Z", "+00:00")
                    )
                away_days_rest = (game_date - last_game_date).days

            # TODO: Calculate travel distance and timezone changes
            # This would require venue location data and travel calculations

            return {
                "home_days_rest": home_days_rest,
                "away_days_rest": away_days_rest,
                "away_travel_distance": None,  # Would need venue coordinates
                "away_timezone_change": None,  # Would need timezone mapping
            }

        except Exception as e:
            logger.error(f"Error extracting rest/travel features: {e}")
            return {}

    async def _extract_situational_features(
        self, game_info: Dict[str, Any], game_date: datetime
    ) -> Dict[str, Any]:
        """Extract situational and motivational factors"""
        try:
            # Basic situational analysis
            season_progress = self._calculate_season_progress(game_date)

            # TODO: Add more sophisticated situational analysis
            # - Playoff race standings
            # - Rivalry games
            # - Key player injuries
            # - Lineup strength assessment

            return {
                "home_key_players_out": 0,  # Would need injury data
                "away_key_players_out": 0,
                "home_lineup_strength": None,  # Would need lineup analysis
                "away_lineup_strength": None,
                "home_motivation_factor": None,  # Would need standings/playoff context
                "away_motivation_factor": None,
                "game_importance_score": Decimal(str(season_progress))
                if season_progress is not None
                else None,
            }

        except Exception as e:
            logger.error(f"Error extracting situational features: {e}")
            return {}

    def _calculate_weighted_form(
        self, games_df: pl.DataFrame, team: str
    ) -> Optional[float]:
        """Calculate weighted recent form with time decay"""
        if games_df.is_empty():
            return None

        wins = 0
        total_weight = 0

        for i, row in enumerate(games_df.rows(named=True)):
            # Time decay weight (more recent games weighted higher)
            weight = 1.0 / (1.0 + i * 0.1)

            # Determine if team won
            home_team = row["home_team"]
            away_team = row["away_team"]
            home_score = row.get("home_score", 0) or 0
            away_score = row.get("away_score", 0) or 0

            team_won = False
            if home_team == team and home_score > away_score:
                team_won = True
            elif away_team == team and away_score > home_score:
                team_won = True

            wins += weight if team_won else 0
            total_weight += weight

        return wins / total_weight if total_weight > 0 else 0.0

    def _get_record_string(self, games_df: pl.DataFrame, team: str) -> Optional[str]:
        """Get record string (e.g., '7-3') for recent games"""
        if games_df.is_empty():
            return None

        wins = 0
        losses = 0

        for row in games_df.rows(named=True):
            home_team = row["home_team"]
            away_team = row["away_team"]
            home_score = row.get("home_score", 0) or 0
            away_score = row.get("away_score", 0) or 0

            if home_score == away_score:  # Tie (rare in MLB)
                continue

            team_won = False
            if home_team == team and home_score > away_score:
                team_won = True
            elif away_team == team and away_score > home_score:
                team_won = True

            if team_won:
                wins += 1
            else:
                losses += 1

        return f"{wins}-{losses}" if (wins + losses) > 0 else None

    def _calculate_season_stats(
        self, games_df: pl.DataFrame, team: str
    ) -> Dict[str, Any]:
        """Calculate season statistics for a team"""
        if games_df.is_empty():
            return {
                "record": None,
                "win_pct": None,
                "runs_per_game": None,
                "runs_allowed_per_game": None,
            }

        wins = 0
        losses = 0
        runs_scored = 0
        runs_allowed = 0

        for row in games_df.rows(named=True):
            home_team = row["home_team"]
            away_team = row["away_team"]
            home_score = row.get("home_score", 0) or 0
            away_score = row.get("away_score", 0) or 0

            if home_team == team:
                # Team played at home
                if home_score > away_score:
                    wins += 1
                elif away_score > home_score:
                    losses += 1
                runs_scored += home_score
                runs_allowed += away_score
            else:
                # Team played away
                if away_score > home_score:
                    wins += 1
                elif home_score > away_score:
                    losses += 1
                runs_scored += away_score
                runs_allowed += home_score

        total_games = wins + losses
        win_pct = wins / total_games if total_games > 0 else None
        runs_per_game = runs_scored / total_games if total_games > 0 else None
        runs_allowed_per_game = runs_allowed / total_games if total_games > 0 else None

        return {
            "record": f"{wins}-{losses}" if total_games > 0 else None,
            "win_pct": win_pct,
            "runs_per_game": runs_per_game,
            "runs_allowed_per_game": runs_allowed_per_game,
        }

    def _calculate_team_venue_factor(
        self, venue_games: pl.DataFrame, team: str
    ) -> Optional[float]:
        """Calculate how well a team performs at a specific venue"""
        if venue_games.is_empty():
            return None

        team_performance = []

        for row in venue_games.rows(named=True):
            home_team = row["home_team"]
            away_team = row["away_team"]
            home_score = row.get("home_score", 0) or 0
            away_score = row.get("away_score", 0) or 0

            if home_team == team:
                # Team played at home at this venue
                performance = home_score / max(home_score + away_score, 1)
            else:
                # Team played away at this venue
                performance = away_score / max(home_score + away_score, 1)

            team_performance.append(performance)

        return np.mean(team_performance) if team_performance else None

    def _calculate_season_progress(self, game_date: datetime) -> Optional[float]:
        """Calculate how far through the season we are (0.0 to 1.0)"""
        try:
            year = game_date.year
            season_start = datetime(year, 3, 20)  # Approximate season start
            season_end = datetime(year, 10, 1)  # Approximate regular season end

            if game_date < season_start:
                return 0.0
            elif game_date > season_end:
                return 1.0
            else:
                season_length = (season_end - season_start).days
                days_elapsed = (game_date - season_start).days
                return days_elapsed / season_length

        except Exception:
            return None

    def _create_empty_features(self) -> TeamFeatures:
        """Create empty team features when no data is available"""
        return TeamFeatures(feature_version=self.feature_version)
