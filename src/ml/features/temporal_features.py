"""
Temporal Feature Extractor
Extracts line movement patterns, sharp action, and time-based features
Uses Polars for high-performance data processing
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from decimal import Decimal

import polars as pl

import numpy as np

from .models import TemporalFeatures, BaseFeatureExtractor

# Proper package imports

logger = logging.getLogger(__name__)


class TemporalFeatureExtractor(BaseFeatureExtractor):
    """
    High-performance temporal feature extraction using Polars
    Focuses on line movement velocity, RLM detection, and sharp action patterns
    """

    def __init__(self, feature_version: str = "v2.1"):
        super().__init__(feature_version)
        self.lookback_hours = 24  # Look back 24 hours for line movement analysis

    def get_required_columns(self) -> List[str]:
        """Required columns for temporal feature extraction"""
        return [
            "game_id",
            "timestamp",
            "game_start_time",
            "sportsbook_name",
            "market_type",
            "home_ml_odds",
            "away_ml_odds",
            "home_spread_line",
            "home_spread_odds",
            "total_line",
            "over_odds",
            "under_odds",
            "sharp_action_direction",
            "reverse_line_movement",
            "bet_percentage_home",
            "money_percentage_home",
            "bet_percentage_over",
            "money_percentage_over",
        ]

    async def extract_features(
        self, df, game_id: int, cutoff_time: datetime
    ) -> TemporalFeatures:
        """
        Extract temporal features with 60-minute cutoff enforcement

        Args:
            df: Historical odds and betting data
            game_id: Game ID for feature extraction
            cutoff_time: Feature cutoff time (must be >= 60min before game)

        Returns:
            TemporalFeatures instance
        """
        try:
            logger.info(f"Extracting temporal features for game {game_id}")

            # Validate data quality
            data_quality = self.validate_data_quality(df, self.get_required_columns())
            if not data_quality["is_valid"]:
                logger.warning(
                    f"Data quality issues for game {game_id}: {data_quality['missing_columns']}"
                )

            # Filter data for this game and enforce cutoff time
            game_data = df.filter(
                (pl.col("game_id") == game_id) & (pl.col("timestamp") <= cutoff_time)
            ).sort("timestamp")

            if game_data.is_empty():
                logger.warning(
                    f"No data available for game {game_id} before cutoff {cutoff_time}"
                )
                return self._create_empty_features(cutoff_time)

            # Get game start time
            game_start_time = game_data.select("game_start_time").unique().item()
            if isinstance(game_start_time, str):
                from datetime import datetime

                game_start_time = datetime.fromisoformat(
                    game_start_time.replace("Z", "+00:00")
                )

            # Calculate minutes before game
            minutes_before = int((game_start_time - cutoff_time).total_seconds() / 60)
            if minutes_before < 60:
                raise ValueError(
                    f"Data leakage prevention: cutoff time must be >= 60 minutes before game (got {minutes_before})"
                )

            # Extract line movement features
            line_movement_features = self._extract_line_movement_features(
                game_data, cutoff_time
            )

            # Extract sharp action features
            sharp_action_features = self._extract_sharp_action_features(
                game_data, cutoff_time
            )

            # Extract public vs sharp divergence
            divergence_features = self._extract_divergence_features(game_data)

            # Extract cross-sportsbook consensus features
            consensus_features = self._extract_consensus_features(game_data)

            # Extract movement patterns
            pattern_features = self._extract_movement_patterns(game_data)

            # Combine all features
            features = TemporalFeatures(
                feature_cutoff_time=cutoff_time,
                game_start_time=game_start_time,
                minutes_before_game=minutes_before,
                feature_version=self.feature_version,
                **line_movement_features,
                **sharp_action_features,
                **divergence_features,
                **consensus_features,
                **pattern_features,
            )

            logger.info(f"Successfully extracted temporal features for game {game_id}")
            return features

        except Exception as e:
            logger.error(f"Error extracting temporal features for game {game_id}: {e}")
            raise

    def _extract_line_movement_features(
        self, df: pl.DataFrame, cutoff_time: datetime
    ) -> Dict[str, Any]:
        """Extract line movement velocity and magnitude features"""
        try:
            # Get data from last 60 minutes before cutoff
            cutoff_60min = cutoff_time - timedelta(minutes=60)
            recent_data = df.filter(pl.col("timestamp") >= cutoff_60min)

            if recent_data.is_empty():
                return {
                    "line_movement_velocity_60min": None,
                    "opening_to_current_ml_home": None,
                    "opening_to_current_ml_away": None,
                    "opening_to_current_spread_home": None,
                    "opening_to_current_total": None,
                }

            # Calculate line movement velocity (changes per hour in final 60 minutes)
            velocity_data = recent_data.group_by(
                ["sportsbook_name", "market_type"]
            ).agg(
                [
                    pl.col("home_ml_odds").diff().abs().sum().alias("ml_changes"),
                    pl.col("home_spread_line")
                    .diff()
                    .abs()
                    .sum()
                    .alias("spread_changes"),
                    pl.col("total_line").diff().abs().sum().alias("total_changes"),
                    pl.col("timestamp").count().alias("data_points"),
                ]
            )

            # Average velocity across all sportsbooks
            avg_velocity = (
                velocity_data.select(
                    [
                        (
                            (
                                pl.col("ml_changes")
                                + pl.col("spread_changes")
                                + pl.col("total_changes")
                            )
                            / pl.col("data_points")
                        )
                        .mean()
                        .alias("velocity")
                    ]
                ).item()
                if not velocity_data.is_empty()
                else 0.0
            )

            # Calculate opening to current movements
            opening_data = df.sort("timestamp").head(1)
            current_data = df.sort("timestamp").tail(1)

            movements = {}
            if not opening_data.is_empty() and not current_data.is_empty():
                opening = opening_data.row(0, named=True)
                current = current_data.row(0, named=True)

                movements = {
                    "opening_to_current_ml_home": float(
                        current.get("home_ml_odds", 0) - opening.get("home_ml_odds", 0)
                    ),
                    "opening_to_current_ml_away": float(
                        current.get("away_ml_odds", 0) - opening.get("away_ml_odds", 0)
                    ),
                    "opening_to_current_spread_home": float(
                        current.get("home_spread_line", 0)
                        - opening.get("home_spread_line", 0)
                    ),
                    "opening_to_current_total": float(
                        current.get("total_line", 0) - opening.get("total_line", 0)
                    ),
                }

            return {
                "line_movement_velocity_60min": Decimal(str(avg_velocity))
                if avg_velocity
                else None,
                **movements,
            }

        except Exception as e:
            logger.error(f"Error extracting line movement features: {e}")
            return {}

    def _extract_sharp_action_features(
        self, df: pl.DataFrame, cutoff_time: datetime
    ) -> Dict[str, Any]:
        """Extract sharp action intensity and signals"""
        try:
            # Count sharp action signals
            sharp_signals = df.filter(
                pl.col("sharp_action_direction").is_not_null()
                & (pl.col("sharp_action_direction") != "none")
            ).height

            # Count reverse line movement instances
            rlm_count = df.filter(pl.col("reverse_line_movement") == True).height

            # Calculate steam move count (simultaneous moves across books)
            # Group by timestamp and count books with significant movements
            steam_moves = (
                df.group_by("timestamp")
                .agg(
                    [
                        (pl.col("home_ml_odds").diff().abs() > 10)
                        .sum()
                        .alias("ml_moves"),
                        (pl.col("home_spread_line").diff().abs() > 0.5)
                        .sum()
                        .alias("spread_moves"),
                        (pl.col("total_line").diff().abs() > 0.5)
                        .sum()
                        .alias("total_moves"),
                    ]
                )
                .filter(
                    (pl.col("ml_moves") >= 3)
                    | (pl.col("spread_moves") >= 3)
                    | (pl.col("total_moves") >= 3)
                )
                .height
            )

            # Calculate sharp action intensity (weighted by recency)
            cutoff_60min = cutoff_time - timedelta(minutes=60)
            recent_sharp = df.filter(
                (pl.col("timestamp") >= cutoff_60min)
                & pl.col("sharp_action_direction").is_not_null()
                & (pl.col("sharp_action_direction") != "none")
            )

            intensity = 0.0
            if not recent_sharp.is_empty():
                # Weight by time decay (more recent = higher weight)
                time_weights = recent_sharp.with_columns(
                    [
                        (
                            (
                                cutoff_time.timestamp()
                                - pl.col("timestamp").dt.timestamp()
                            )
                            / 3600
                        ).alias("hours_ago")
                    ]
                ).with_columns(
                    [(1.0 / (1.0 + pl.col("hours_ago"))).alias("time_weight")]
                )

                intensity = time_weights.select("time_weight").sum().item() / max(
                    recent_sharp.height, 1
                )

            return {
                "sharp_action_intensity_60min": Decimal(str(intensity))
                if intensity > 0
                else None,
                "reverse_line_movement_signals": rlm_count,
                "steam_move_count": steam_moves,
            }

        except Exception as e:
            logger.error(f"Error extracting sharp action features: {e}")
            return {}

    def _extract_divergence_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Extract public vs sharp money divergence features"""
        try:
            # Calculate money % - bet % divergence for each market
            divergence_data = df.group_by("market_type").agg(
                [
                    (pl.col("money_percentage_home") - pl.col("bet_percentage_home"))
                    .mean()
                    .alias("home_divergence"),
                    (pl.col("money_percentage_over") - pl.col("bet_percentage_over"))
                    .mean()
                    .alias("over_divergence"),
                ]
            )

            divergences = {}
            for row in divergence_data.rows(named=True):
                market = row["market_type"]
                if market in ["moneyline", "spread"]:
                    divergences["money_vs_bet_divergence_home"] = row["home_divergence"]
                    divergences["money_vs_bet_divergence_away"] = (
                        -row["home_divergence"] if row["home_divergence"] else None
                    )
                elif market == "total":
                    divergences["money_vs_bet_divergence_over"] = row["over_divergence"]
                    divergences["money_vs_bet_divergence_under"] = (
                        -row["over_divergence"] if row["over_divergence"] else None
                    )

            return divergences

        except Exception as e:
            logger.error(f"Error extracting divergence features: {e}")
            return {}

    def _extract_consensus_features(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Extract cross-sportsbook consensus and variance features"""
        try:
            # Count participating sportsbooks
            participating_books = df.select("sportsbook_name").unique().height

            # Calculate variance across sportsbooks for key metrics
            variance_data = df.group_by(["timestamp", "market_type"]).agg(
                [
                    pl.col("home_ml_odds").var().alias("ml_variance"),
                    pl.col("home_spread_line").var().alias("spread_variance"),
                    pl.col("total_line").var().alias("total_variance"),
                    pl.col("sportsbook_name").count().alias("book_count"),
                ]
            )

            # Average variances
            avg_variances = variance_data.select(
                [
                    pl.col("ml_variance").mean().alias("avg_ml_var"),
                    pl.col("spread_variance").mean().alias("avg_spread_var"),
                    pl.col("total_variance").mean().alias("avg_total_var"),
                    pl.col("book_count").mean().alias("avg_book_count"),
                ]
            )

            if not avg_variances.is_empty():
                variances = avg_variances.row(0, named=True)

                # Calculate consensus strength (inverse of variance)
                consensus_60min = 1.0 / (1.0 + (variances.get("avg_ml_var", 0) or 0))

                return {
                    "cross_sbook_consensus_60min": Decimal(str(consensus_60min)),
                    "sportsbook_variance_ml": Decimal(
                        str(variances.get("avg_ml_var", 0) or 0)
                    ),
                    "sportsbook_variance_spread": Decimal(
                        str(variances.get("avg_spread_var", 0) or 0)
                    ),
                    "sportsbook_variance_total": Decimal(
                        str(variances.get("avg_total_var", 0) or 0)
                    ),
                    "participating_sportsbooks": participating_books,
                }

            return {"participating_sportsbooks": participating_books}

        except Exception as e:
            logger.error(f"Error extracting consensus features: {e}")
            return {}

    def _extract_movement_patterns(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Extract line movement direction patterns"""
        try:
            # Calculate overall movement directions
            first_data = df.sort("timestamp").head(1)
            last_data = df.sort("timestamp").tail(1)

            if first_data.is_empty() or last_data.is_empty():
                return {}

            first = first_data.row(0, named=True)
            last = last_data.row(0, named=True)

            # ML movement direction
            ml_home_change = (last.get("home_ml_odds", 0) or 0) - (
                first.get("home_ml_odds", 0) or 0
            )
            ml_direction = "stable"
            if abs(ml_home_change) > 5:  # Significant movement threshold
                ml_direction = (
                    "toward_home" if ml_home_change < 0 else "toward_away"
                )  # Lower odds = better chance

            # Spread movement direction
            spread_change = (last.get("home_spread_line", 0) or 0) - (
                first.get("home_spread_line", 0) or 0
            )
            spread_direction = "stable"
            if abs(spread_change) > 0.5:
                spread_direction = "toward_home" if spread_change > 0 else "toward_away"

            # Total movement direction
            total_change = (last.get("total_line", 0) or 0) - (
                first.get("total_line", 0) or 0
            )
            total_direction = "stable"
            if abs(total_change) > 0.5:
                total_direction = "toward_over" if total_change > 0 else "toward_under"

            # Calculate movement consistency (how consistent movements are across books)
            movement_data = df.group_by("sportsbook_name").agg(
                [
                    (
                        pl.col("home_ml_odds").last() - pl.col("home_ml_odds").first()
                    ).alias("ml_change"),
                    (
                        pl.col("home_spread_line").last()
                        - pl.col("home_spread_line").first()
                    ).alias("spread_change"),
                    (pl.col("total_line").last() - pl.col("total_line").first()).alias(
                        "total_change"
                    ),
                ]
            )

            # Calculate consistency as inverse of variance in movements
            consistency_score = 0.0
            if not movement_data.is_empty():
                movement_vars = movement_data.select(
                    [
                        pl.col("ml_change").var().alias("ml_var"),
                        pl.col("spread_change").var().alias("spread_var"),
                        pl.col("total_change").var().alias("total_var"),
                    ]
                )

                if not movement_vars.is_empty():
                    var_row = movement_vars.row(0, named=True)
                    avg_variance = np.mean(
                        [
                            var_row.get("ml_var", 0) or 0,
                            var_row.get("spread_var", 0) or 0,
                            var_row.get("total_var", 0) or 0,
                        ]
                    )
                    consistency_score = 1.0 / (1.0 + avg_variance)

            return {
                "ml_movement_direction": ml_direction,
                "spread_movement_direction": spread_direction,
                "total_movement_direction": total_direction,
                "movement_consistency_score": Decimal(str(consistency_score)),
            }

        except Exception as e:
            logger.error(f"Error extracting movement patterns: {e}")
            return {}

    def _create_empty_features(self, cutoff_time: datetime) -> TemporalFeatures:
        """Create empty temporal features when no data is available"""
        return TemporalFeatures(
            feature_cutoff_time=cutoff_time,
            game_start_time=cutoff_time + timedelta(minutes=60),  # Minimum 60 minutes
            minutes_before_game=60,
            feature_version=self.feature_version,
        )
