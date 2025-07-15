"""
Consensus Moneyline Strategy Processor

Detects scenarios where both public bets AND sharp money align on moneyline bets.
Tests both following consensus (popular side) and fading consensus (contrarian approach).

Key Strategies:
- CONSENSUS_HEAVY: Both money and bets >= 90% or <= 10%
- MIXED_CONSENSUS: Money ~80%+ with bets ~60%+ alignment

Enhanced with confidence scoring based on consensus strength and alignment.
"""

from datetime import datetime
from typing import Any

from ...models.betting_analysis import BettingSignal, ProfitableStrategy, SignalType
from .base_strategy_processor import BaseStrategyProcessor


class ConsensusProcessor(BaseStrategyProcessor):
    """
    Processor for consensus moneyline strategy detection.

    Identifies opportunities where both public betting patterns and sharp money
    strongly align on the same side, creating high-confidence signals.
    """

    def get_signal_type(self) -> SignalType:
        """Return the signal type this processor handles"""
        return SignalType.CONSENSUS_MONEYLINE

    def get_strategy_category(self) -> str:
        """Return strategy category for proper routing"""
        return "CONSENSUS_ANALYSIS"

    def get_required_tables(self) -> list[str]:
        """Return database tables required for this strategy"""
        return ["splits.raw_mlb_betting_splits"]

    def get_strategy_description(self) -> str:
        """Return human-readable description of the strategy"""
        return "Consensus moneyline strategy: Follow or fade when public and sharp money align"

    async def process(
        self, minutes_ahead: int, profitable_strategies: list[ProfitableStrategy]
    ) -> list[BettingSignal]:
        """Process consensus moneyline signals"""
        start_time, end_time = self._create_time_window(minutes_ahead)

        # Get consensus-specific strategies
        consensus_strategies = self._get_consensus_strategies(profitable_strategies)

        if not consensus_strategies:
            self.logger.warning("No profitable consensus strategies found")
            return []

        # Get raw moneyline data for consensus analysis
        raw_data = await self.repository.get_consensus_signal_data(start_time, end_time)

        if not raw_data:
            self.logger.info("No consensus data found for analysis")
            return []

        signals = []
        now_est = datetime.now(self.est)

        for row in raw_data:
            # Basic validation
            if not self._is_valid_consensus_data(row, now_est, minutes_ahead):
                continue

            # Analyze consensus patterns
            consensus_analysis = self._analyze_consensus_patterns(row)

            if not consensus_analysis:
                continue

            # Find matching profitable strategy
            matching_strategy = self._find_consensus_strategy(
                consensus_analysis, consensus_strategies
            )

            if not matching_strategy:
                continue

            # Apply juice filter for moneyline bets
            if self._should_apply_juice_filter(row):
                continue

            # Calculate confidence with consensus-specific adjustments
            confidence_data = self._calculate_consensus_confidence(
                row, consensus_analysis, matching_strategy
            )

            # Create the consensus signal
            signal = self._create_consensus_signal(
                row, consensus_analysis, matching_strategy, confidence_data
            )
            signals.append(signal)

        self._log_consensus_summary(signals, consensus_strategies, len(raw_data))
        return signals

    def _get_consensus_strategies(
        self, profitable_strategies: list[ProfitableStrategy]
    ) -> list[ProfitableStrategy]:
        """Extract consensus-related strategies"""
        consensus_strategies = []

        for strategy in profitable_strategies:
            strategy_name = strategy.strategy_name.lower()
            if any(
                keyword in strategy_name
                for keyword in [
                    "consensus",
                    "heavy_consensus",
                    "mixed_consensus",
                    "follow",
                    "fade",
                ]
            ):
                consensus_strategies.append(strategy)

        self.logger.info(f"Found {len(consensus_strategies)} consensus strategies")
        return consensus_strategies

    def _analyze_consensus_patterns(self, row: dict[str, Any]) -> dict[str, Any]:
        """
        Analyze consensus patterns in the betting data.

        Returns consensus analysis or None if no significant pattern found.
        """
        try:
            money_pct = float(row.get("money_pct", 0))
            bet_pct = float(row.get("bet_pct", 0))

            # Heavy consensus patterns (very strong alignment)
            if (money_pct >= 90 and bet_pct >= 90) or (
                money_pct <= 10 and bet_pct <= 10
            ):
                return {
                    "consensus_type": "CONSENSUS_HEAVY_HOME"
                    if money_pct >= 90
                    else "CONSENSUS_HEAVY_AWAY",
                    "recommended_side": row.get("home_team")
                    if money_pct >= 90
                    else row.get("away_team"),
                    "strategy_approach": "FOLLOW",  # Default to follow, can be fade based on strategy
                    "consensus_strength": (money_pct + bet_pct) / 2,
                    "consensus_alignment": abs(money_pct - bet_pct),
                    "sharp_public_diff": money_pct - bet_pct,
                    "money_pct": money_pct,
                    "bet_pct": bet_pct,
                }

            # Mixed consensus patterns (moderate alignment)
            elif (money_pct >= 80 and bet_pct >= 60) or (
                money_pct <= 20 and bet_pct <= 40
            ):
                return {
                    "consensus_type": "MIXED_CONSENSUS_HOME"
                    if money_pct >= 80
                    else "MIXED_CONSENSUS_AWAY",
                    "recommended_side": row.get("home_team")
                    if money_pct >= 80
                    else row.get("away_team"),
                    "strategy_approach": "FOLLOW",  # Default to follow
                    "consensus_strength": (money_pct + bet_pct) / 2,
                    "consensus_alignment": abs(money_pct - bet_pct),
                    "sharp_public_diff": money_pct - bet_pct,
                    "money_pct": money_pct,
                    "bet_pct": bet_pct,
                }

            return None

        except (ValueError, TypeError) as e:
            self.logger.warning(f"Error analyzing consensus patterns: {e}")
            return None

    def _find_consensus_strategy(
        self,
        consensus_analysis: dict[str, Any],
        consensus_strategies: list[ProfitableStrategy],
    ) -> ProfitableStrategy:
        """Find matching consensus strategy"""
        consensus_type = consensus_analysis["consensus_type"]
        consensus_strength = consensus_analysis["consensus_strength"]

        # Look for specific consensus strategy matches
        for strategy in consensus_strategies:
            strategy_name = strategy.strategy_name.lower()

            # Match consensus type and approach
            if "heavy" in consensus_type.lower() and "heavy" in strategy_name:
                if self._meets_consensus_threshold(strategy, consensus_strength):
                    # Determine if this is a follow or fade strategy
                    if "fade" in strategy_name:
                        consensus_analysis["strategy_approach"] = "FADE"
                    return strategy

            elif "mixed" in consensus_type.lower() and "mixed" in strategy_name:
                if self._meets_consensus_threshold(strategy, consensus_strength):
                    if "fade" in strategy_name:
                        consensus_analysis["strategy_approach"] = "FADE"
                    return strategy

            # General consensus matches
            elif "consensus" in strategy_name:
                if self._meets_consensus_threshold(strategy, consensus_strength):
                    if "fade" in strategy_name:
                        consensus_analysis["strategy_approach"] = "FADE"
                    return strategy

        return None

    def _meets_consensus_threshold(
        self, strategy: ProfitableStrategy, consensus_strength: float
    ) -> bool:
        """Check if consensus meets strategy thresholds"""
        # Dynamic thresholds based on strategy performance
        if strategy.win_rate >= 65:
            threshold = 75.0  # High threshold for high performers
        elif strategy.win_rate >= 60:
            threshold = 70.0  # Moderate threshold
        elif strategy.win_rate >= 55:
            threshold = 65.0  # Conservative threshold
        else:
            threshold = 80.0  # Very conservative

        return consensus_strength >= threshold

    def _calculate_consensus_confidence(
        self,
        row: dict[str, Any],
        consensus_analysis: dict[str, Any],
        matching_strategy: ProfitableStrategy,
    ) -> dict[str, Any]:
        """Calculate confidence with consensus-specific adjustments"""
        base_confidence = self._calculate_confidence(
            consensus_analysis["sharp_public_diff"],
            row.get("source", "unknown"),
            row.get("book", "unknown"),
            "moneyline",
            matching_strategy.strategy_name,
            row.get("last_updated"),
            self._normalize_game_time(row["game_datetime"]),
        )

        # Apply consensus-specific modifiers
        consensus_modifier = self._get_consensus_confidence_modifier(consensus_analysis)

        # Adjust confidence based on consensus strength and alignment
        adjusted_confidence = base_confidence["confidence_score"] * consensus_modifier
        adjusted_confidence = max(0.1, min(0.95, adjusted_confidence))

        return {
            **base_confidence,
            "confidence_score": adjusted_confidence,
            "consensus_strength": consensus_analysis["consensus_strength"],
            "consensus_alignment": consensus_analysis["consensus_alignment"],
            "consensus_modifier": consensus_modifier,
        }

    def _get_consensus_confidence_modifier(
        self, consensus_analysis: dict[str, Any]
    ) -> float:
        """Get confidence modifier based on consensus characteristics"""
        consensus_strength = consensus_analysis["consensus_strength"]
        consensus_alignment = consensus_analysis["consensus_alignment"]
        consensus_type = consensus_analysis["consensus_type"]

        # Base modifier on consensus strength
        strength_modifier = 0.8 + (consensus_strength / 100.0) * 0.4  # 0.8 to 1.2

        # Bonus for good alignment (low difference between money and bets)
        alignment_modifier = (
            1.0 + (20 - min(20, consensus_alignment)) / 100.0
        )  # Up to 1.2

        # Type-specific modifiers
        type_modifier = 1.15 if "HEAVY" in consensus_type else 1.05

        return strength_modifier * alignment_modifier * type_modifier

    def _create_consensus_signal(
        self,
        row: dict[str, Any],
        consensus_analysis: dict[str, Any],
        matching_strategy: ProfitableStrategy,
        confidence_data: dict[str, Any],
    ) -> BettingSignal:
        """Create consensus betting signal"""
        # Determine final recommendation based on strategy approach
        if consensus_analysis["strategy_approach"] == "FADE":
            # Fade strategy - recommend opposite side
            recommended_side = (
                row.get("away_team")
                if consensus_analysis["recommended_side"] == row.get("home_team")
                else row.get("home_team")
            )
            recommendation = f"FADE {consensus_analysis['recommended_side']} → BET {recommended_side}"
        else:
            # Follow strategy - recommend consensus side
            recommended_side = consensus_analysis["recommended_side"]
            recommendation = f"FOLLOW CONSENSUS → BET {recommended_side}"

        signal = self._create_betting_signal(row, matching_strategy, confidence_data)

        # Update signal with consensus-specific information
        signal.recommendation = recommendation
        signal.metadata = signal.metadata or {}
        signal.metadata.update(
            {
                "consensus_type": consensus_analysis["consensus_type"],
                "consensus_strength": consensus_analysis["consensus_strength"],
                "consensus_alignment": consensus_analysis["consensus_alignment"],
                "strategy_approach": consensus_analysis["strategy_approach"],
                "money_percentage": consensus_analysis["money_pct"],
                "bet_percentage": consensus_analysis["bet_pct"],
                "sharp_public_diff": consensus_analysis["sharp_public_diff"],
                "recommended_side": recommended_side,
            }
        )

        return signal

    def _is_valid_consensus_data(
        self, row: dict[str, Any], current_time: datetime, minutes_ahead: int
    ) -> bool:
        """Validate consensus data quality and timing"""
        try:
            # Check split type is moneyline
            if row.get("split_type") != "moneyline":
                return False

            # Check time window
            game_time = self._normalize_game_time(row["game_datetime"])
            time_diff_minutes = self._calculate_minutes_to_game(game_time, current_time)

            if not (0 <= time_diff_minutes <= minutes_ahead):
                return False

            # Check data completeness
            required_fields = ["home_team", "away_team", "money_pct", "bet_pct"]
            if not all(row.get(field) is not None for field in required_fields):
                return False

            # Check percentage validity
            money_pct = float(row.get("money_pct", 0))
            bet_pct = float(row.get("bet_pct", 0))

            if not (0 <= money_pct <= 100 and 0 <= bet_pct <= 100):
                return False

            return True

        except (ValueError, TypeError, KeyError) as e:
            self.logger.warning(f"Invalid consensus data: {e}")
            return False

    def _should_apply_juice_filter(self, row: dict[str, Any]) -> bool:
        """Check if juice filter should be applied to this consensus signal"""
        # Always apply juice filter for moneyline consensus plays
        return self._should_filter_juice(
            "moneyline",
            row.get("split_value"),
            row.get("recommended_side", row.get("home_team")),
            row.get("home_team"),
            row.get("away_team"),
        )

    def _log_consensus_summary(
        self,
        signals: list[BettingSignal],
        consensus_strategies: list[ProfitableStrategy],
        raw_data_count: int,
    ):
        """Log summary of consensus processing"""
        consensus_type_counts = {}
        approach_counts = {"FOLLOW": 0, "FADE": 0}

        for signal in signals:
            if signal.metadata:
                consensus_type = signal.metadata.get("consensus_type", "unknown")
                approach = signal.metadata.get("strategy_approach", "unknown")

                consensus_type_counts[consensus_type] = (
                    consensus_type_counts.get(consensus_type, 0) + 1
                )
                if approach in approach_counts:
                    approach_counts[approach] += 1

        self.logger.info(
            f"Consensus moneyline processing complete: {len(signals)} signals from {raw_data_count} raw records",
            extra={
                "total_signals": len(signals),
                "raw_data_count": raw_data_count,
                "consensus_strategies": len(consensus_strategies),
                "consensus_types": consensus_type_counts,
                "approaches": approach_counts,
            },
        )
