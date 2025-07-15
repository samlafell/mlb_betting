"""
Reverse Line Movement (RLM) Detector

Specialized processor for detecting reverse line movement patterns from Action Network data.
This handles the specific case where lines move in favor of the side getting public money,
which often indicates sharp money on the opposite side or inefficient market pricing.

Key RLM Patterns:
1. LINE_WITH_PUBLIC: Line moves in same direction as public money (classic RLM)
2. WEAK_RLM: Line moves with public but percentage is low (<65%)
3. STRONG_RLM: Line moves with public and percentage is high (>75%)
4. STEAM_RLM: Rapid line movement with public money across multiple books

Real Example from July 13, 2025 Orioles game:
- Total O9: -122 → -107 (15 cent move toward over)
- Public: 58% money on over, 54% tickets on over
- Result: Classic RLM - line moved with moderate public money
"""

from datetime import datetime
from enum import Enum
from typing import Any

from src.analysis.models.unified_models import (
    ConfidenceLevel,
    SignalType,
    StrategyCategory,
    UnifiedBettingSignal,
)
from src.analysis.strategies.base import BaseStrategyProcessor, StrategyProcessorMixin
from src.data.database import UnifiedRepository


class RLMType(str, Enum):
    """Types of reverse line movement patterns"""

    LINE_WITH_PUBLIC = "LINE_WITH_PUBLIC"  # Line moves with public money
    WEAK_RLM = "WEAK_RLM"  # RLM with low public percentage
    STRONG_RLM = "STRONG_RLM"  # RLM with high public percentage
    STEAM_RLM = "STEAM_RLM"  # Rapid RLM across multiple books


class RLMStrength(str, Enum):
    """Strength of RLM signal"""

    WEAK = "WEAK"  # 55-65% public money
    MODERATE = "MODERATE"  # 65-75% public money
    STRONG = "STRONG"  # 75%+ public money


class ActionNetworkRLMDetector(BaseStrategyProcessor, StrategyProcessorMixin):
    """
    Specialized RLM detector for Action Network data.

    Analyzes real line movement data to detect when lines move in favor of
    the side getting public money, which often indicates market inefficiencies
    or opportunities to fade the public.
    """

    def __init__(self, repository: UnifiedRepository, config: dict[str, Any]):
        """Initialize the RLM detector"""
        super().__init__(repository, config)

        # RLM detection thresholds
        self.min_line_movement = config.get("min_line_movement", 5)  # 5 cents minimum
        self.weak_public_threshold = config.get(
            "weak_public_threshold", 55
        )  # 55% public money
        self.moderate_public_threshold = config.get(
            "moderate_public_threshold", 65
        )  # 65% public money
        self.strong_public_threshold = config.get(
            "strong_public_threshold", 75
        )  # 75% public money

        # RLM confidence modifiers
        self.rlm_modifiers = config.get(
            "rlm_modifiers",
            {
                "weak_rlm": 1.1,  # 10% boost for weak RLM
                "moderate_rlm": 1.3,  # 30% boost for moderate RLM
                "strong_rlm": 1.5,  # 50% boost for strong RLM
                "steam_rlm": 1.4,  # 40% boost for steam RLM
                "multi_book_consensus": 1.2,  # 20% bonus for multiple books
                "large_movement": 1.3,  # 30% bonus for large line movement
            },
        )

        self.logger.info(
            f"Initialized ActionNetworkRLMDetector with thresholds: "
            f"min_movement={self.min_line_movement}, "
            f"weak_threshold={self.weak_public_threshold}%"
        )

    def get_signal_type(self) -> SignalType:
        """Return the signal type this processor handles"""
        return SignalType.LINE_MOVEMENT

    def get_strategy_category(self) -> StrategyCategory:
        """Return strategy category for proper routing"""
        return StrategyCategory.MARKET_INEFFICIENCY

    def get_required_tables(self) -> list[str]:
        """Return database tables required for this strategy"""
        return ["action_network_history"]

    def get_strategy_description(self) -> str:
        """Return human-readable description of the strategy"""
        return "Reverse Line Movement detection for Action Network data with public betting analysis"

    async def process_signals(
        self, game_data: list[dict[str, Any]], context: dict[str, Any]
    ) -> list[UnifiedBettingSignal]:
        """
        Process RLM signals from Action Network data.

        Args:
            game_data: Action Network historical data
            context: Processing context

        Returns:
            List of RLM betting signals
        """
        signals = []
        processing_time = context.get("processing_time", datetime.now(self.est))

        self.logger.info(f"Processing RLM signals for {len(game_data)} games")

        try:
            for game in game_data:
                try:
                    # Extract RLM patterns from Action Network historical data
                    rlm_patterns = await self._analyze_action_network_rlm(game)

                    for pattern in rlm_patterns:
                        # Check if pattern meets RLM criteria
                        if not self._is_significant_rlm(pattern):
                            continue

                        # Calculate confidence with RLM-specific modifiers
                        confidence_data = self._calculate_rlm_confidence(pattern)

                        # Create RLM signal
                        signal = self._create_rlm_signal(
                            game, pattern, confidence_data, processing_time
                        )

                        if signal:
                            signals.append(signal)

                except Exception as e:
                    self.logger.warning(
                        f"Failed to process game {game.get('game_id')}: {e}"
                    )
                    continue

            # Apply RLM-specific filtering and ranking
            signals = await self._apply_rlm_filtering(signals)

            self.logger.info(f"Generated {len(signals)} RLM signals")
            return signals

        except Exception as e:
            self.logger.error(f"RLM signal processing failed: {e}")
            return []

    async def _analyze_action_network_rlm(
        self, game_data: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Analyze Action Network historical data for RLM patterns.

        Args:
            game_data: Action Network game data with historical entries

        Returns:
            List of detected RLM patterns
        """
        rlm_patterns = []

        try:
            # Parse the historical data string (it's currently stored as a string representation)
            historical_data_str = game_data.get("historical_data", "")

            # For now, let's extract the key information we need
            # In a real implementation, we'd parse the full historical entries

            # Extract betting percentages from the data string
            # Look for patterns like "bet_info': {'tickets': {'value': 0, 'percent': 54}, 'money': {'value': 0, 'percent': 58}}"

            # Example pattern detection for totals (like the Orioles example)
            if "total" in historical_data_str and "bet_info" in historical_data_str:
                # Extract total market RLM pattern
                total_pattern = await self._extract_total_rlm_pattern(
                    game_data, historical_data_str
                )
                if total_pattern:
                    rlm_patterns.append(total_pattern)

            # Example pattern detection for moneyline
            if "moneyline" in historical_data_str and "bet_info" in historical_data_str:
                # Extract moneyline RLM pattern
                ml_pattern = await self._extract_moneyline_rlm_pattern(
                    game_data, historical_data_str
                )
                if ml_pattern:
                    rlm_patterns.append(ml_pattern)

            # Example pattern detection for spread
            if "spread" in historical_data_str and "bet_info" in historical_data_str:
                # Extract spread RLM pattern
                spread_pattern = await self._extract_spread_rlm_pattern(
                    game_data, historical_data_str
                )
                if spread_pattern:
                    rlm_patterns.append(spread_pattern)

        except Exception as e:
            self.logger.warning(f"Error analyzing Action Network RLM: {e}")

        return rlm_patterns

    async def _extract_total_rlm_pattern(
        self, game_data: dict[str, Any], historical_data: str
    ) -> dict[str, Any] | None:
        """
        Extract RLM pattern from total market.

        Based on the Orioles example:
        - Over 9: -122 → -107 (15 cent move toward over)
        - Public: 58% money on over, 54% tickets on over
        """
        try:
            # Look for total market data in the historical string
            # This is a simplified extraction - in production we'd parse the full data structure

            # Extract public betting percentages for over
            import re

            # Look for over side betting info
            over_pattern = re.search(
                r"'side': 'over'.*?'bet_info': \{'tickets': \{'value': \d+, 'percent': (\d+)\}, 'money': \{'value': \d+, 'percent': (\d+)\}",
                historical_data,
            )

            if over_pattern:
                over_tickets_pct = int(over_pattern.group(1))
                over_money_pct = int(over_pattern.group(2))

                # Look for line movement in the history
                # Example: {'odds': -122, 'value': 9, 'updated_at': '2025-07-12T19:13:26.407327Z'}
                # to {'odds': -107, 'value': 9, 'updated_at': '2025-07-13T19:32:55.389625Z'}

                odds_history = re.findall(
                    r"\{'odds': (-?\d+), 'value': ([\d.]+)", historical_data
                )

                if len(odds_history) >= 2:
                    # Get opening and closing odds
                    opening_odds = int(odds_history[0][0])
                    closing_odds = int(odds_history[-1][0])
                    total_value = float(odds_history[0][1])

                    # Calculate line movement
                    line_movement = closing_odds - opening_odds

                    # Determine if this is RLM
                    # RLM occurs when line moves in favor of public side
                    if (
                        over_money_pct > 50 and line_movement > 0
                    ):  # Public on over, line moved toward over
                        rlm_type = self._classify_rlm_strength(over_money_pct)

                        return {
                            "market_type": "total",
                            "game_id": game_data.get("game_id"),
                            "home_team": game_data.get("home_team"),
                            "away_team": game_data.get("away_team"),
                            "total_value": total_value,
                            "opening_odds": opening_odds,
                            "closing_odds": closing_odds,
                            "line_movement": line_movement,
                            "public_tickets_pct": over_tickets_pct,
                            "public_money_pct": over_money_pct,
                            "rlm_type": rlm_type,
                            "recommended_side": "under",  # Fade the public in RLM
                            "movement_magnitude": abs(line_movement),
                            "rlm_strength": self._get_rlm_strength(over_money_pct),
                            "pattern_description": f"Total {total_value} moved from {opening_odds} to {closing_odds} with {over_money_pct}% public money on over",
                        }

        except Exception as e:
            self.logger.warning(f"Error extracting total RLM pattern: {e}")

        return None

    async def _extract_moneyline_rlm_pattern(
        self, game_data: dict[str, Any], historical_data: str
    ) -> dict[str, Any] | None:
        """Extract RLM pattern from moneyline market."""
        try:
            # Similar logic for moneyline RLM detection
            # Look for home team betting info
            import re

            home_pattern = re.search(
                r"'side': 'home'.*?'bet_info': \{'tickets': \{'value': \d+, 'percent': (\d+)\}, 'money': \{'value': \d+, 'percent': (\d+)\}",
                historical_data,
            )

            if home_pattern:
                home_tickets_pct = int(home_pattern.group(1))
                home_money_pct = int(home_pattern.group(2))

                # Extract moneyline movement
                ml_history = re.findall(
                    r"'type': 'moneyline'.*?'odds': (-?\d+)", historical_data
                )

                if len(ml_history) >= 2:
                    opening_odds = int(ml_history[0])
                    closing_odds = int(ml_history[-1])

                    # For favorites (negative odds), movement toward more negative is line moving toward that team
                    # For underdogs (positive odds), movement toward more positive is line moving away from that team

                    line_moved_toward_home = False
                    if (
                        opening_odds < 0 and closing_odds < 0
                    ):  # Both negative (favorite)
                        line_moved_toward_home = (
                            closing_odds < opening_odds
                        )  # More negative = stronger favorite
                    elif (
                        opening_odds > 0 and closing_odds > 0
                    ):  # Both positive (underdog)
                        line_moved_toward_home = (
                            closing_odds > opening_odds
                        )  # More positive = weaker underdog (line moved away)

                    # Check for RLM
                    if home_money_pct > 55 and line_moved_toward_home:
                        rlm_type = self._classify_rlm_strength(home_money_pct)

                        return {
                            "market_type": "moneyline",
                            "game_id": game_data.get("game_id"),
                            "home_team": game_data.get("home_team"),
                            "away_team": game_data.get("away_team"),
                            "opening_odds": opening_odds,
                            "closing_odds": closing_odds,
                            "line_movement": closing_odds - opening_odds,
                            "public_tickets_pct": home_tickets_pct,
                            "public_money_pct": home_money_pct,
                            "rlm_type": rlm_type,
                            "recommended_side": game_data.get(
                                "away_team"
                            ),  # Fade the public
                            "movement_magnitude": abs(closing_odds - opening_odds),
                            "rlm_strength": self._get_rlm_strength(home_money_pct),
                            "pattern_description": f"Moneyline moved from {opening_odds} to {closing_odds} with {home_money_pct}% public money on home",
                        }

        except Exception as e:
            self.logger.warning(f"Error extracting moneyline RLM pattern: {e}")

        return None

    async def _extract_spread_rlm_pattern(
        self, game_data: dict[str, Any], historical_data: str
    ) -> dict[str, Any] | None:
        """Extract RLM pattern from spread market."""
        # Similar implementation for spread RLM detection
        return None

    def _classify_rlm_strength(self, public_money_pct: int) -> RLMType:
        """Classify the type of RLM based on public money percentage."""
        if public_money_pct >= self.strong_public_threshold:
            return RLMType.STRONG_RLM
        elif public_money_pct >= self.moderate_public_threshold:
            return RLMType.LINE_WITH_PUBLIC
        else:
            return RLMType.WEAK_RLM

    def _get_rlm_strength(self, public_money_pct: int) -> RLMStrength:
        """Get the strength of the RLM signal."""
        if public_money_pct >= self.strong_public_threshold:
            return RLMStrength.STRONG
        elif public_money_pct >= self.moderate_public_threshold:
            return RLMStrength.MODERATE
        else:
            return RLMStrength.WEAK

    def _is_significant_rlm(self, pattern: dict[str, Any]) -> bool:
        """Check if RLM pattern is significant enough for betting."""
        try:
            movement_magnitude = pattern.get("movement_magnitude", 0)
            public_money_pct = pattern.get("public_money_pct", 50)

            # Must meet minimum line movement threshold
            if movement_magnitude < self.min_line_movement:
                return False

            # Must have meaningful public support
            if public_money_pct < self.weak_public_threshold:
                return False

            return True

        except Exception:
            return False

    def _calculate_rlm_confidence(self, pattern: dict[str, Any]) -> dict[str, Any]:
        """Calculate confidence for RLM pattern with specific modifiers."""
        try:
            # Base confidence from movement magnitude
            movement_magnitude = pattern.get("movement_magnitude", 0)
            base_confidence = min(movement_magnitude / 20.0, 1.0)  # Normalize to 0-1

            # Apply RLM-specific modifiers
            rlm_strength = pattern.get("rlm_strength", RLMStrength.WEAK)
            public_money_pct = pattern.get("public_money_pct", 50)

            modifier = 1.0

            if rlm_strength == RLMStrength.STRONG:
                modifier *= self.rlm_modifiers["strong_rlm"]
            elif rlm_strength == RLMStrength.MODERATE:
                modifier *= self.rlm_modifiers["moderate_rlm"]
            else:
                modifier *= self.rlm_modifiers["weak_rlm"]

            # Large movement bonus
            if movement_magnitude >= 15:
                modifier *= self.rlm_modifiers["large_movement"]

            final_confidence = min(base_confidence * modifier, 1.0)

            return {
                "confidence_score": final_confidence,
                "base_confidence": base_confidence,
                "modifier": modifier,
                "confidence_level": self._determine_confidence_level(final_confidence),
                "rlm_strength": rlm_strength.value,
                "public_money_pct": public_money_pct,
            }

        except Exception as e:
            self.logger.warning(f"Error calculating RLM confidence: {e}")
            return {
                "confidence_score": 0.5,
                "base_confidence": 0.5,
                "modifier": 1.0,
                "confidence_level": ConfidenceLevel.LOW,
                "rlm_strength": RLMStrength.WEAK.value,
                "public_money_pct": 50,
            }

    def _create_rlm_signal(
        self,
        game_data: dict[str, Any],
        pattern: dict[str, Any],
        confidence_data: dict[str, Any],
        processing_time: datetime,
    ) -> UnifiedBettingSignal | None:
        """Create a unified RLM signal."""
        try:
            return self.create_unified_signal(
                signal_data={
                    "game_id": pattern["game_id"],
                    "home_team": pattern["home_team"],
                    "away_team": pattern["away_team"],
                    "game_datetime": game_data.get(
                        "game_datetime", processing_time.isoformat()
                    ),
                    "recommended_side": pattern["recommended_side"],
                    "bet_type": pattern["market_type"],
                    "strategy_data": pattern,
                    "minutes_to_game": 0,  # Games are already complete
                    "source": "action_network_rlm",
                },
                confidence_data=confidence_data,
            )

        except Exception as e:
            self.logger.warning(f"Error creating RLM signal: {e}")
            return None

    async def _apply_rlm_filtering(
        self, signals: list[UnifiedBettingSignal]
    ) -> list[UnifiedBettingSignal]:
        """Apply RLM-specific filtering and ranking."""
        if not signals:
            return signals

        # Sort by RLM strength and confidence
        def rlm_priority(signal):
            strategy_data = signal.strategy_data
            confidence = signal.confidence_score

            # Strong RLM gets highest priority
            rlm_strength = strategy_data.get("rlm_strength", "WEAK")
            if rlm_strength == "STRONG":
                confidence += 0.3
            elif rlm_strength == "MODERATE":
                confidence += 0.2

            # Large movements get priority
            movement_magnitude = strategy_data.get("movement_magnitude", 0)
            if movement_magnitude >= 15:
                confidence += 0.1

            return confidence

        # Sort and return top signals
        sorted_signals = sorted(signals, key=rlm_priority, reverse=True)
        return sorted_signals[:10]  # Return top 10 RLM signals
