"""
Centralized juice filter service to protect against heavily juiced betting lines.

This service provides a unified way to filter out bets with unacceptable juice
across all betting strategies, ensuring consistent protection of the bankroll.
"""

import json

from mlb_sharp_betting.core.config import get_settings
from mlb_sharp_betting.core.logging import get_logger


class JuiceFilterService:
    """Centralized service for filtering heavily juiced betting lines."""

    def __init__(self):
        self.settings = get_settings()
        self.logger = get_logger(__name__)
        self.juice_config = self.settings.juice_filter

    def should_filter_bet(
        self,
        moneyline_odds: str | dict | int | float,
        recommended_team: str,
        home_team: str,
        away_team: str,
        strategy_name: str | None = None,
    ) -> bool:
        """
        Determine if a bet should be filtered due to excessive juice.

        Args:
            moneyline_odds: The moneyline odds (JSON string, dict, or numeric)
            recommended_team: The team being recommended for the bet
            home_team: Home team name
            away_team: Away team name
            strategy_name: Name of the strategy making the recommendation

        Returns:
            True if the bet should be filtered (rejected), False if acceptable
        """
        if not self.juice_config.enabled:
            return False

        # Parse the moneyline odds
        odds_dict = self._parse_moneyline_odds(moneyline_odds)
        if not odds_dict:
            # If we can't parse odds, don't filter (let the bet proceed)
            return False

        # Determine which team's odds to check
        recommended_odds = self._get_recommended_team_odds(
            odds_dict, recommended_team, home_team, away_team
        )

        if recommended_odds is None:
            return False

        # Only filter if betting on a favorite (negative odds) that's too juiced
        if (
            recommended_odds < 0
            and recommended_odds < self.juice_config.max_juice_threshold
        ):
            if self.juice_config.log_filtered_bets:
                self.logger.info(
                    "Juice filter: Rejecting heavily juiced favorite",
                    strategy=strategy_name or "Unknown",
                    recommended_team=recommended_team,
                    recommended_odds=recommended_odds,
                    threshold=self.juice_config.max_juice_threshold,
                    game=f"{away_team} @ {home_team}",
                )
            return True

        return False

    def _parse_moneyline_odds(
        self, odds: str | dict | int | float
    ) -> dict[str, float] | None:
        """Parse moneyline odds from various formats."""
        try:
            if isinstance(odds, str):
                # Try to parse as JSON
                odds_dict = json.loads(odds)
            elif isinstance(odds, dict):
                odds_dict = odds
            elif isinstance(odds, (int, float)):
                # Single odds value - can't determine home/away, so skip filtering
                return None
            else:
                return None

            # Ensure we have home and away odds
            if not isinstance(odds_dict, dict):
                return None

            # Convert to standardized format
            result = {}
            if "home" in odds_dict:
                result["home"] = float(odds_dict["home"])
            if "away" in odds_dict:
                result["away"] = float(odds_dict["away"])

            return result if result else None

        except (json.JSONDecodeError, ValueError, TypeError) as e:
            self.logger.debug("Could not parse moneyline odds", odds=odds, error=str(e))
            return None

    def _get_recommended_team_odds(
        self,
        odds_dict: dict[str, float],
        recommended_team: str,
        home_team: str,
        away_team: str,
    ) -> float | None:
        """Get the odds for the recommended team."""
        try:
            # Determine if recommended team is home or away
            if recommended_team == home_team and "home" in odds_dict:
                return odds_dict["home"]
            elif recommended_team == away_team and "away" in odds_dict:
                return odds_dict["away"]
            else:
                # Try partial matching for team names
                if home_team and recommended_team in home_team and "home" in odds_dict:
                    return odds_dict["home"]
                elif (
                    away_team and recommended_team in away_team and "away" in odds_dict
                ):
                    return odds_dict["away"]

                self.logger.debug(
                    "Could not match recommended team to odds",
                    recommended_team=recommended_team,
                    home_team=home_team,
                    away_team=away_team,
                    odds_dict=odds_dict,
                )
                return None

        except (KeyError, TypeError) as e:
            self.logger.debug("Error getting recommended team odds", error=str(e))
            return None

    def get_filter_summary(self) -> dict[str, bool | int | str]:
        """Get a summary of current juice filter settings."""
        return {
            "enabled": self.juice_config.enabled,
            "max_juice_threshold": self.juice_config.max_juice_threshold,
            "applies_to_all_strategies": self.juice_config.apply_to_all_strategies,
            "logging_enabled": self.juice_config.log_filtered_bets,
            "description": f"Rejects moneyline favorites worse than {self.juice_config.max_juice_threshold}",
        }


# Global instance for easy access
_juice_filter_service = None


def get_juice_filter_service() -> JuiceFilterService:
    """Get the global juice filter service instance."""
    global _juice_filter_service
    if _juice_filter_service is None:
        _juice_filter_service = JuiceFilterService()
    return _juice_filter_service
