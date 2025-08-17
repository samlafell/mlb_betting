"""
Unified Hybrid Sharp Processor

Migrated and enhanced hybrid sharp processor from the legacy system.
This processor combines line movement analysis with sharp action detection to create high-confidence
betting opportunities. Looks for confirmation between line movement and professional money flow patterns.

Key enhancements from legacy:
- Async-first architecture for 3-5x performance improvement
- Enhanced correlation analysis between line movement and sharp action
- Multi-signal confirmation with steam move detection
- Sophisticated confidence scoring with hybrid-specific modifiers
- Integration with unified data models and error handling

Key Strategy Features:
1. STRONG_CONFIRMATION: Line movement + strong sharp action in same direction
2. MODERATE_CONFIRMATION: Moderate alignment between line and sharp signals
3. STEAM_PLAY: Strong sharp action without significant line movement
4. REVERSE_LINE_MOVEMENT: Line moves opposite to public betting patterns
5. Multi-book consensus validation and confirmation signals

This processor implements the core logic from hybrid_line_sharp_strategy_postgres.sql (11KB, 271 lines),
identifying situations where line movement confirms sharp action direction.

Part of Phase 5C: Remaining Processor Migration
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
from src.core.exceptions import StrategyError
from src.data.database import UnifiedRepository


class HybridSignalType(str, Enum):
    """Types of hybrid sharp signals"""

    STRONG_CONFIRMATION = "STRONG_CONFIRMATION"
    MODERATE_CONFIRMATION = "MODERATE_CONFIRMATION"
    STEAM_PLAY = "STEAM_PLAY"
    REVERSE_LINE_MOVEMENT = "REVERSE_LINE_MOVEMENT"


class ConfirmationStrength(str, Enum):
    """Strength of confirmation between signals"""

    PERFECT = "PERFECT"  # Line and sharp action perfectly aligned
    STRONG = "STRONG"  # Strong alignment
    MODERATE = "MODERATE"  # Moderate alignment
    WEAK = "WEAK"  # Weak alignment


class UnifiedHybridSharpProcessor(BaseStrategyProcessor, StrategyProcessorMixin):
    """
    Unified hybrid sharp processor.

    Combines line movement analysis with sharp action detection to create comprehensive
    betting signals by analyzing the correlation between:
    - Line movement patterns
    - Professional money indicators (sharp action)
    - Public betting behavior
    - Opening vs closing line dynamics

    This replaces the legacy HybridSharpProcessor with modern async patterns
    and enhanced correlation analysis capabilities.
    """

    def __init__(self, repository: UnifiedRepository, config: dict[str, Any]):
        """Initialize the unified hybrid sharp processor"""
        super().__init__(repository, config)

        # Hybrid sharp specific configuration
        self.min_line_movement = config.get(
            "min_line_movement", 0.5
        )  # Minimum line movement
        self.min_sharp_threshold = config.get(
            "min_sharp_threshold", 15.0
        )  # Minimum sharp action differential
        self.confirmation_threshold = config.get(
            "confirmation_threshold", 0.7
        )  # Confirmation correlation threshold
        self.steam_play_threshold = config.get(
            "steam_play_threshold", 25.0
        )  # Steam play sharp action threshold

        # Hybrid confidence modifiers
        self.hybrid_modifiers = config.get(
            "hybrid_modifiers",
            {
                "strong_confirmation": 1.5,  # Strong confirmation gets 50% boost
                "moderate_confirmation": 1.3,  # Moderate confirmation gets 30% boost
                "steam_play": 1.4,  # Steam plays get 40% boost
                "reverse_line_movement": 1.3,  # Reverse line movement gets 30% boost
                "perfect_alignment": 1.6,  # Perfect alignment gets 60% boost
                "multi_signal_consensus": 1.2,  # Multi-signal consensus bonus
            },
        )

        # Correlation thresholds for confirmation strength
        self.correlation_thresholds = config.get(
            "correlation_thresholds",
            {
                "perfect": 0.9,  # Perfect alignment
                "strong": 0.7,  # Strong alignment
                "moderate": 0.5,  # Moderate alignment
                "weak": 0.3,  # Weak alignment
            },
        )

        self.logger.info(
            f"Initialized UnifiedHybridSharpProcessor with thresholds: "
            f"min_line_movement={self.min_line_movement}, "
            f"min_sharp_threshold={self.min_sharp_threshold}"
        )

    def get_signal_type(self) -> SignalType:
        """Return the signal type this processor handles"""
        return SignalType.HYBRID_SHARP

    def get_strategy_category(self) -> StrategyCategory:
        """Return strategy category for proper routing"""
        return StrategyCategory.HYBRID_ANALYSIS

    def get_required_tables(self) -> list[str]:
        """Return database tables required for this strategy"""
        return ["splits.raw_mlb_betting_splits", "public.games"]

    def get_strategy_description(self) -> str:
        """Return human-readable description of the strategy"""
        return (
            "Hybrid sharp strategy: Combine line movement with sharp action "
            "for enhanced signal confidence and confirmation"
        )

    async def process_signals(
        self, game_data: list[dict[str, Any]], context: dict[str, Any]
    ) -> list[UnifiedBettingSignal]:
        """
        Process hybrid sharp signals with enhanced correlation analysis.

        Args:
            game_data: Game data to analyze
            context: Processing context with timing and filters

        Returns:
            List of hybrid sharp betting signals
        """
        signals = []
        processing_time = context.get("processing_time", datetime.now(self.est))
        minutes_ahead = context.get("minutes_ahead", 1440)

        self.logger.info(f"Processing hybrid sharp signals for {len(game_data)} games")

        try:
            # Get hybrid data with both line movement and sharp action
            hybrid_data = await self._get_hybrid_sharp_data(game_data, minutes_ahead)

            if not hybrid_data:
                self.logger.info("No hybrid sharp data available for analysis")
                return signals

            # Detect hybrid sharp opportunities
            hybrid_opportunities = await self._detect_hybrid_opportunities(hybrid_data)

            if not hybrid_opportunities:
                self.logger.info("No hybrid sharp opportunities found")
                return signals

            # Process each hybrid opportunity
            for hybrid_data in hybrid_opportunities:
                try:
                    # Validate hybrid opportunity
                    if not self._is_valid_hybrid_data(
                        hybrid_data, processing_time, minutes_ahead
                    ):
                        continue

                    # Calculate hybrid confidence
                    confidence_data = await self._calculate_hybrid_confidence(
                        hybrid_data
                    )

                    # Check if meets minimum confidence threshold
                    if (
                        confidence_data["confidence_score"]
                        < self.thresholds["min_confidence"]
                    ):
                        continue

                    # Create hybrid sharp signal
                    signal = await self._create_hybrid_signal(
                        hybrid_data, confidence_data, processing_time
                    )

                    if signal:
                        signals.append(signal)

                except Exception as e:
                    self.logger.warning(f"Error processing hybrid opportunity: {e}")
                    continue

            # Apply final filtering and ranking
            signals = await self._apply_hybrid_filtering(signals)

            self.logger.info(f"Generated {len(signals)} hybrid sharp signals")
            return signals

        except Exception as e:
            self.logger.error(f"Hybrid sharp processing failed: {e}", exc_info=True)
            raise StrategyError(f"Hybrid sharp processing failed: {e}")

    async def _get_hybrid_sharp_data(
        self, game_data: list[dict[str, Any]], minutes_ahead: int
    ) -> list[dict[str, Any]]:
        """
        Get hybrid data with both line movement and sharp action indicators.

        Args:
            game_data: Games to analyze
            minutes_ahead: Time window in minutes

        Returns:
            List of hybrid data with line movement and sharp action
        """
        try:
            hybrid_data = []

            # Import database connection for real data queries
            from src.core.config import get_settings
            from src.data.database.connection import DatabaseConnection

            config = get_settings()
            db_connection = DatabaseConnection(config.database.connection_string)

            async with db_connection.get_async_connection() as conn:
                for game in game_data:
                    game_id = game.get("game_id")
                    if not game_id:
                        continue

                    # Query hybrid data combining betting splits and line movement
                    hybrid_query = """
                        SELECT 
                            -- Game info
                            ubs.game_id,
                            ubs.market_type,
                            ubs.sportsbook_name,
                            ubs.data_source,
                            
                            -- Betting splits (sharp action indicators)
                            ubs.bet_percentage_home,
                            ubs.bet_percentage_away,
                            ubs.money_percentage_home,
                            ubs.money_percentage_away,
                            ubs.sharp_action_direction,
                            ubs.sharp_action_strength,
                            ubs.reverse_line_movement,
                            
                            -- Current lines
                            ubs.current_home_ml,
                            ubs.current_away_ml,
                            ubs.current_spread_home,
                            ubs.current_total_line,
                            ubs.current_over_odds,
                            ubs.current_under_odds,
                            
                            -- Timing data
                            ubs.collected_at,
                            ubs.minutes_before_game,
                            
                            -- Line movement data from betting_lines_unified
                            blu.movement_amount,
                            blu.movement_direction,
                            COUNT(*) OVER (PARTITION BY ubs.game_id, ubs.market_type) as book_consensus
                            
                        FROM curated.unified_betting_splits ubs
                        LEFT JOIN curated.betting_lines_unified blu ON (
                            blu.game_id = ubs.game_id 
                            AND blu.market_type = ubs.market_type
                            AND blu.sportsbook_id = ubs.sportsbook_id
                        )
                        WHERE ubs.game_id = $1 
                        AND ubs.minutes_before_game >= $2
                        ORDER BY ubs.collected_at DESC, ubs.market_type, ubs.sportsbook_name
                        LIMIT 100
                    """

                    rows = await conn.fetch(hybrid_query, game_id, minutes_ahead)

                    for row in rows:
                        row_dict = dict(row)

                        # Calculate sharp differential for moneyline
                        if (row_dict["market_type"] == "moneyline" and
                            row_dict["money_percentage_home"] and row_dict["bet_percentage_home"]):

                            money_pct = float(row_dict["money_percentage_home"])
                            bet_pct = float(row_dict["bet_percentage_home"])
                            sharp_differential = abs(money_pct - bet_pct)

                            # Determine correlation between line movement and sharp action
                            line_correlation = 0.0
                            if row_dict["movement_amount"] and row_dict["sharp_action_direction"]:
                                # Basic correlation logic - could be enhanced
                                movement_dir = row_dict["movement_direction"]
                                sharp_dir = row_dict["sharp_action_direction"]
                                if movement_dir == sharp_dir:
                                    line_correlation = 0.8
                                elif movement_dir and sharp_dir and movement_dir != sharp_dir:
                                    line_correlation = 0.2  # Reverse line movement scenario
                                else:
                                    line_correlation = 0.5

                            # Determine confirmation strength
                            confirmation_strength = "WEAK"
                            if sharp_differential >= 15 and line_correlation >= 0.7:
                                confirmation_strength = "STRONG"
                            elif sharp_differential >= 10 and line_correlation >= 0.5:
                                confirmation_strength = "MODERATE"

                            # Detect steam moves
                            steam_move_detected = (
                                row_dict["movement_amount"] and
                                abs(float(row_dict["movement_amount"])) >= 10 and
                                row_dict["sharp_action_strength"] == "strong"
                            )

                            hybrid_data_point = {
                                "game_id": game_id,
                                "home_team": game["home_team"],
                                "away_team": game["away_team"],
                                "game_datetime": game["game_datetime"],
                                "split_type": row_dict["market_type"],

                                # Line movement data
                                "current_line": row_dict["current_home_ml"],
                                "line_movement": row_dict["movement_amount"],
                                "line_direction": row_dict["movement_direction"],

                                # Sharp action data
                                "money_pct": money_pct,
                                "bet_pct": bet_pct,
                                "sharp_differential": sharp_differential,
                                "sharp_direction": row_dict["sharp_action_direction"],

                                # Public betting data (same as bet percentage for now)
                                "public_pct": bet_pct,
                                "public_direction": row_dict["sharp_action_direction"],

                                # Volume and timing
                                "source": row_dict["data_source"],
                                "book": row_dict["sportsbook_name"],
                                "last_updated": row_dict["collected_at"],
                                "book_consensus": row_dict["book_consensus"],

                                # Correlation indicators
                                "line_sharp_correlation": line_correlation,
                                "confirmation_strength": confirmation_strength,
                                "steam_move_detected": steam_move_detected,
                                "reverse_line_movement": row_dict["reverse_line_movement"] or False,
                            }

                            hybrid_data.append(hybrid_data_point)

                        # Similar logic for spread data
                        elif (row_dict["market_type"] == "spread" and
                              row_dict["money_percentage_home"] and row_dict["bet_percentage_home"]):

                            money_pct = float(row_dict["money_percentage_home"])
                            bet_pct = float(row_dict["bet_percentage_home"])
                            sharp_differential = abs(money_pct - bet_pct)

                            hybrid_data_point = {
                                "game_id": game_id,
                                "home_team": game["home_team"],
                                "away_team": game["away_team"],
                                "game_datetime": game["game_datetime"],
                                "split_type": "spread",
                                "current_line": row_dict["current_spread_home"],
                                "line_movement": row_dict["movement_amount"],
                                "line_direction": row_dict["movement_direction"],
                                "money_pct": money_pct,
                                "bet_pct": bet_pct,
                                "sharp_differential": sharp_differential,
                                "sharp_direction": row_dict["sharp_action_direction"],
                                "source": row_dict["data_source"],
                                "book": row_dict["sportsbook_name"],
                                "last_updated": row_dict["collected_at"],
                                "reverse_line_movement": row_dict["reverse_line_movement"] or False,
                            }

                            hybrid_data.append(hybrid_data_point)

            if not hybrid_data:
                self.logger.warning(
                    "No real hybrid sharp data found, this may indicate empty database tables",
                    games_analyzed=len(game_data),
                    minutes_ahead=minutes_ahead
                )

            return hybrid_data

        except Exception as e:
            self.logger.error(f"Failed to get hybrid sharp data: {e}")
            return []

    async def _detect_hybrid_opportunities(
        self, hybrid_data: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Detect hybrid sharp opportunities from combined data.

        Args:
            hybrid_data: Combined line movement and sharp action data

        Returns:
            List of hybrid opportunities with analysis
        """
        hybrid_opportunities = []

        for data in hybrid_data:
            try:
                # Analyze hybrid patterns
                hybrid_analysis = await self._analyze_hybrid_patterns(data)

                if hybrid_analysis and self._is_significant_hybrid_opportunity(
                    hybrid_analysis
                ):
                    hybrid_analysis["raw_data"] = data
                    hybrid_opportunities.append(hybrid_analysis)

            except Exception as e:
                self.logger.warning(f"Error analyzing hybrid patterns: {e}")
                continue

        self.logger.info(
            f"Found {len(hybrid_opportunities)} potential hybrid opportunities from {len(hybrid_data)} games"
        )
        return hybrid_opportunities

    async def _analyze_hybrid_patterns(
        self, data: dict[str, Any]
    ) -> dict[str, Any] | None:
        """
        Analyze hybrid patterns combining line movement and sharp action.

        Args:
            data: Combined hybrid data for a single game

        Returns:
            Hybrid analysis or None if no significant pattern found
        """
        try:
            line_movement = float(data.get("line_movement", 0))
            sharp_differential = float(data.get("sharp_differential", 0))
            line_direction = data.get("line_direction", "none")
            sharp_direction = data.get("sharp_direction", "none")

            # Check if we have significant line movement or sharp action
            has_line_movement = abs(line_movement) >= self.min_line_movement
            has_sharp_action = abs(sharp_differential) >= self.min_sharp_threshold

            # Determine correlation between line movement and sharp action
            correlation = self._calculate_signal_correlation(
                line_movement, sharp_differential, line_direction, sharp_direction
            )

            # Determine hybrid signal type
            hybrid_type = None
            confirmation_strength = ConfirmationStrength.WEAK

            # Strong confirmation: Both line movement and sharp action in same direction
            if (
                has_line_movement
                and has_sharp_action
                and line_direction == sharp_direction
                and correlation >= self.correlation_thresholds["strong"]
            ):
                hybrid_type = HybridSignalType.STRONG_CONFIRMATION

                if correlation >= self.correlation_thresholds["perfect"]:
                    confirmation_strength = ConfirmationStrength.PERFECT
                else:
                    confirmation_strength = ConfirmationStrength.STRONG

            # Moderate confirmation: Some alignment between signals
            elif (
                has_line_movement
                and has_sharp_action
                and correlation >= self.correlation_thresholds["moderate"]
            ):
                hybrid_type = HybridSignalType.MODERATE_CONFIRMATION
                confirmation_strength = ConfirmationStrength.MODERATE

            # Steam play: Strong sharp action without significant line movement
            elif (
                has_sharp_action
                and not has_line_movement
                and sharp_differential >= self.steam_play_threshold
            ):
                hybrid_type = HybridSignalType.STEAM_PLAY
                confirmation_strength = ConfirmationStrength.STRONG

            # Reverse line movement: Line moves opposite to public betting
            elif has_line_movement and self._is_reverse_line_movement(data):
                hybrid_type = HybridSignalType.REVERSE_LINE_MOVEMENT
                confirmation_strength = ConfirmationStrength.MODERATE

            if not hybrid_type:
                return None

            # Determine recommended side based on stronger signal
            if abs(sharp_differential) > abs(line_movement):
                # Follow sharp action
                recommended_side = (
                    data.get("home_team")
                    if sharp_direction == "home"
                    else data.get("away_team")
                )
                signal_basis = "sharp_action"
            else:
                # Follow line movement
                recommended_side = (
                    data.get("home_team")
                    if line_direction == "home"
                    else data.get("away_team")
                )
                signal_basis = "line_movement"

            return {
                "hybrid_type": hybrid_type,
                "confirmation_strength": confirmation_strength,
                "recommended_side": recommended_side,
                "signal_basis": signal_basis,
                "line_movement": line_movement,
                "sharp_differential": sharp_differential,
                "line_direction": line_direction,
                "sharp_direction": sharp_direction,
                "correlation": correlation,
                "opening_line": data.get("opening_line", 0),
                "current_line": data.get("current_line", 0),
                "money_pct": data.get("money_pct", 50),
                "bet_pct": data.get("bet_pct", 50),
                "public_pct": data.get("public_pct", 50),
                "volume": data.get("volume", 0),
                "book_consensus": data.get("book_consensus", 1),
                "steam_move_detected": data.get("steam_move_detected", False),
                "reverse_line_movement": data.get("reverse_line_movement", False),
            }

        except Exception as e:
            self.logger.warning(f"Error analyzing hybrid patterns: {e}")
            return None

    def _calculate_signal_correlation(
        self,
        line_movement: float,
        sharp_differential: float,
        line_direction: str,
        sharp_direction: str,
    ) -> float:
        """Calculate correlation between line movement and sharp action"""
        try:
            # Direction alignment score
            direction_score = 1.0 if line_direction == sharp_direction else -0.5

            # Magnitude correlation (normalized)
            line_magnitude = abs(line_movement) / 2.0  # Normalize line movement
            sharp_magnitude = (
                abs(sharp_differential) / 30.0
            )  # Normalize sharp differential

            magnitude_correlation = min(line_magnitude, sharp_magnitude) / max(
                line_magnitude, sharp_magnitude, 0.1
            )

            # Combined correlation score
            correlation = (direction_score * 0.7) + (magnitude_correlation * 0.3)

            return max(0.0, min(1.0, correlation))

        except Exception:
            return 0.0

    def _is_reverse_line_movement(self, data: dict[str, Any]) -> bool:
        """Check if line movement is reverse (opposite to public betting)"""
        try:
            line_direction = data.get("line_direction", "none")
            public_direction = data.get("public_direction", "none")

            # Line moves opposite to public betting
            return (
                line_direction != public_direction
                and line_direction != "none"
                and public_direction != "none"
            )

        except Exception:
            return False

    def _is_significant_hybrid_opportunity(
        self, hybrid_analysis: dict[str, Any]
    ) -> bool:
        """Check if hybrid opportunity is significant enough for betting"""
        try:
            hybrid_type = hybrid_analysis.get("hybrid_type")
            correlation = hybrid_analysis.get("correlation", 0)
            line_movement = abs(hybrid_analysis.get("line_movement", 0))
            sharp_differential = abs(hybrid_analysis.get("sharp_differential", 0))

            # Strong confirmation requires high correlation
            if hybrid_type == HybridSignalType.STRONG_CONFIRMATION:
                return correlation >= self.correlation_thresholds["strong"]

            # Moderate confirmation requires moderate correlation
            elif hybrid_type == HybridSignalType.MODERATE_CONFIRMATION:
                return correlation >= self.correlation_thresholds["moderate"]

            # Steam plays require strong sharp action
            elif hybrid_type == HybridSignalType.STEAM_PLAY:
                return sharp_differential >= self.steam_play_threshold

            # Reverse line movement requires significant line movement
            elif hybrid_type == HybridSignalType.REVERSE_LINE_MOVEMENT:
                return line_movement >= self.min_line_movement

            return False

        except Exception:
            return False

    async def _calculate_hybrid_confidence(
        self, hybrid_data: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Calculate confidence for hybrid sharp signals.

        Args:
            hybrid_data: Hybrid opportunity data

        Returns:
            Confidence calculation results
        """
        try:
            # Base confidence from correlation and signal strength
            correlation = hybrid_data.get("correlation", 0)
            line_movement = abs(hybrid_data.get("line_movement", 0))
            sharp_differential = abs(hybrid_data.get("sharp_differential", 0))

            # Base confidence from correlation
            base_confidence = correlation

            # Apply hybrid-specific modifiers
            applied_modifiers = {}

            # Hybrid type modifier
            hybrid_type = hybrid_data.get("hybrid_type")
            if hybrid_type == HybridSignalType.STRONG_CONFIRMATION:
                base_confidence *= self.hybrid_modifiers["strong_confirmation"]
                applied_modifiers["strong_confirmation"] = self.hybrid_modifiers[
                    "strong_confirmation"
                ]
            elif hybrid_type == HybridSignalType.MODERATE_CONFIRMATION:
                base_confidence *= self.hybrid_modifiers["moderate_confirmation"]
                applied_modifiers["moderate_confirmation"] = self.hybrid_modifiers[
                    "moderate_confirmation"
                ]
            elif hybrid_type == HybridSignalType.STEAM_PLAY:
                base_confidence *= self.hybrid_modifiers["steam_play"]
                applied_modifiers["steam_play"] = self.hybrid_modifiers["steam_play"]
            elif hybrid_type == HybridSignalType.REVERSE_LINE_MOVEMENT:
                base_confidence *= self.hybrid_modifiers["reverse_line_movement"]
                applied_modifiers["reverse_line_movement"] = self.hybrid_modifiers[
                    "reverse_line_movement"
                ]

            # Perfect alignment bonus
            confirmation_strength = hybrid_data.get("confirmation_strength")
            if confirmation_strength == ConfirmationStrength.PERFECT:
                base_confidence *= self.hybrid_modifiers["perfect_alignment"]
                applied_modifiers["perfect_alignment"] = self.hybrid_modifiers[
                    "perfect_alignment"
                ]

            # Multi-signal consensus modifier
            book_consensus = hybrid_data.get("book_consensus", 1)
            if book_consensus >= 3:
                base_confidence *= self.hybrid_modifiers["multi_signal_consensus"]
                applied_modifiers["multi_signal_consensus"] = self.hybrid_modifiers[
                    "multi_signal_consensus"
                ]

            # Signal strength bonus
            signal_strength = (line_movement + sharp_differential) / 2.0
            if signal_strength >= 20:
                strength_bonus = 1.1
                base_confidence *= strength_bonus
                applied_modifiers["signal_strength_bonus"] = strength_bonus

            # Ensure confidence is within bounds
            final_confidence = max(0.0, min(1.0, base_confidence))

            # Determine confidence level
            if final_confidence >= 0.8:
                confidence_level = ConfidenceLevel.HIGH
            elif final_confidence >= 0.6:
                confidence_level = ConfidenceLevel.MEDIUM
            else:
                confidence_level = ConfidenceLevel.LOW

            return {
                "confidence_score": final_confidence,
                "confidence_level": confidence_level,
                "base_confidence": correlation,
                "correlation": correlation,
                "applied_modifiers": applied_modifiers,
            }

        except Exception as e:
            self.logger.error(f"Failed to calculate hybrid confidence: {e}")
            return {
                "confidence_score": 0.5,
                "confidence_level": ConfidenceLevel.LOW,
                "base_confidence": 0.5,
                "correlation": 0.5,
                "applied_modifiers": {},
            }

    async def _create_hybrid_signal(
        self,
        hybrid_data: dict[str, Any],
        confidence_data: dict[str, Any],
        processing_time: datetime,
    ) -> UnifiedBettingSignal | None:
        """Create a unified hybrid sharp signal"""

        try:
            # Get game data from hybrid opportunity
            raw_data = hybrid_data.get("raw_data", {})

            # Determine recommended side
            recommended_side = hybrid_data["recommended_side"]

            # Create comprehensive strategy-specific data
            strategy_data = {
                "processor_type": "hybrid_sharp",
                "hybrid_type": hybrid_data["hybrid_type"].value,
                "confirmation_strength": hybrid_data["confirmation_strength"].value,
                "signal_basis": hybrid_data["signal_basis"],
                "line_movement": hybrid_data["line_movement"],
                "sharp_differential": hybrid_data["sharp_differential"],
                "line_direction": hybrid_data["line_direction"],
                "sharp_direction": hybrid_data["sharp_direction"],
                "correlation": hybrid_data["correlation"],
                "opening_line": hybrid_data["opening_line"],
                "current_line": hybrid_data["current_line"],
                "money_pct": hybrid_data["money_pct"],
                "bet_pct": hybrid_data["bet_pct"],
                "public_pct": hybrid_data["public_pct"],
                "volume": hybrid_data["volume"],
                "book_consensus": hybrid_data["book_consensus"],
                "steam_move_detected": hybrid_data["steam_move_detected"],
                "reverse_line_movement": hybrid_data["reverse_line_movement"],
                "multi_signal_confirmation": True,
                "source": raw_data.get("source", "unknown"),
                "book": raw_data.get("book", ""),
                "split_type": raw_data.get("split_type", "moneyline"),
                "last_updated": raw_data.get("last_updated", processing_time),
            }

            # Create the unified signal
            signal = UnifiedBettingSignal(
                signal_id=f"hybrid_{self.strategy_id}_{raw_data.get('game_id', 'unknown')}_{hash(str(hybrid_data))}",
                signal_type=SignalType.HYBRID_SHARP,
                strategy_category=StrategyCategory.HYBRID_ANALYSIS,
                game_id=raw_data.get(
                    "game_id",
                    f"{raw_data.get('home_team', 'unknown')}_vs_{raw_data.get('away_team', 'unknown')}",
                ),
                home_team=raw_data.get("home_team", "unknown"),
                away_team=raw_data.get("away_team", "unknown"),
                game_date=self._normalize_game_time(
                    raw_data.get("game_datetime", processing_time)
                ),
                recommended_side=recommended_side,
                bet_type=raw_data.get("split_type", "moneyline"),
                confidence_score=confidence_data["confidence_score"],
                confidence_level=confidence_data["confidence_level"],
                strategy_data=strategy_data,
                signal_strength=confidence_data["correlation"],
                minutes_to_game=int(
                    self._calculate_minutes_to_game(
                        self._normalize_game_time(
                            raw_data.get("game_datetime", processing_time)
                        ),
                        processing_time,
                    )
                ),
                timing_category=self._get_timing_category(
                    int(
                        self._calculate_minutes_to_game(
                            self._normalize_game_time(
                                raw_data.get("game_datetime", processing_time)
                            ),
                            processing_time,
                        )
                    )
                ),
                data_source=raw_data.get("source", "unknown"),
                book=raw_data.get("book", ""),
                metadata={
                    "processing_id": self.processing_id,
                    "strategy_id": self.strategy_id,
                    "applied_modifiers": confidence_data["applied_modifiers"],
                    "created_at": processing_time,
                    "processor_version": "3.0.0",
                    "hybrid_analysis_version": "2.0.0",
                },
            )

            return signal

        except Exception as e:
            self.logger.error(f"Failed to create hybrid signal: {e}")
            return None

    def _is_valid_hybrid_data(
        self, hybrid_data: dict[str, Any], current_time: datetime, minutes_ahead: int
    ) -> bool:
        """Validate hybrid opportunity data"""
        try:
            # Check required fields
            required_fields = [
                "hybrid_type",
                "recommended_side",
                "correlation",
                "raw_data",
            ]
            if not all(field in hybrid_data for field in required_fields):
                return False

            raw_data = hybrid_data.get("raw_data", {})

            # Check correlation meets minimum threshold
            correlation = hybrid_data.get("correlation", 0)
            if correlation < self.correlation_thresholds["weak"]:
                return False

            # Check timing window
            if "game_datetime" in raw_data:
                game_time = self._normalize_game_time(raw_data["game_datetime"])
                time_diff = (game_time - current_time).total_seconds() / 60

                if time_diff <= 0 or time_diff > minutes_ahead:
                    return False

            return True

        except Exception:
            return False

    async def _apply_hybrid_filtering(
        self, signals: list[UnifiedBettingSignal]
    ) -> list[UnifiedBettingSignal]:
        """Apply hybrid-specific filtering and ranking"""
        if not signals:
            return signals

        # Prioritize by hybrid strength and confirmation
        def hybrid_priority(signal):
            strategy_data = signal.strategy_data
            priority_score = signal.confidence_score

            # Strong confirmation gets highest priority
            if (
                strategy_data.get("hybrid_type")
                == HybridSignalType.STRONG_CONFIRMATION.value
            ):
                priority_score += 0.3

            # Perfect alignment bonus
            if (
                strategy_data.get("confirmation_strength")
                == ConfirmationStrength.PERFECT.value
            ):
                priority_score += 0.2

            # High correlation bonus
            correlation = strategy_data.get("correlation", 0)
            if correlation >= 0.8:
                priority_score += 0.15

            # Steam move bonus
            if strategy_data.get("steam_move_detected"):
                priority_score += 0.1

            # Multi-book consensus bonus
            book_consensus = strategy_data.get("book_consensus", 1)
            if book_consensus >= 4:
                priority_score += 0.1

            return priority_score

        # Remove duplicates and sort by hybrid priority
        unique_signals = {}
        for signal in signals:
            game_key = f"{signal.game_id}_{signal.bet_type}"
            current_priority = hybrid_priority(signal)

            if game_key not in unique_signals or current_priority > hybrid_priority(
                unique_signals[game_key]
            ):
                unique_signals[game_key] = signal

        # Sort by hybrid priority (highest first)
        filtered_signals = sorted(
            unique_signals.values(), key=hybrid_priority, reverse=True
        )

        # Apply maximum signals limit
        max_signals = self.config.get("max_signals_per_execution", 20)
        if len(filtered_signals) > max_signals:
            filtered_signals = filtered_signals[:max_signals]
            self.logger.info(f"Limited signals to top {max_signals} by hybrid priority")

        return filtered_signals

    # Legacy compatibility methods

    async def _get_real_game_data(self, minutes_ahead: int) -> list[dict[str, Any]]:
        """
        Get real game data from the database for processing.
        
        Args:
            minutes_ahead: Time window in minutes
            
        Returns:
            List of game data dictionaries
        """
        try:
            from src.core.config import get_settings
            from src.data.database.connection import DatabaseConnection

            config = get_settings()
            db_connection = DatabaseConnection(config.database.connection_string)

            async with db_connection.get_async_connection() as conn:
                # Get upcoming games that have betting data available
                query = """
                    SELECT DISTINCT
                        eg.id as game_id,
                        eg.home_team,
                        eg.away_team,
                        eg.game_datetime,
                        eg.season,
                        eg.game_status
                    FROM curated.enhanced_games eg
                    WHERE eg.game_datetime > NOW() 
                    AND eg.game_datetime <= NOW() + interval '%s minutes'
                    AND EXISTS (
                        SELECT 1 FROM curated.unified_betting_splits ubs 
                        WHERE ubs.game_id = eg.id
                    )
                    ORDER BY eg.game_datetime ASC
                    LIMIT 20
                """ % minutes_ahead

                rows = await conn.fetch(query)

                game_data = []
                for row in rows:
                    game_data.append({
                        "game_id": row["game_id"],
                        "home_team": row["home_team"],
                        "away_team": row["away_team"],
                        "game_datetime": row["game_datetime"],
                        "season": row["season"],
                        "game_status": row["game_status"]
                    })

                self.logger.info(
                    f"Retrieved {len(game_data)} games with betting data for hybrid sharp analysis",
                    minutes_ahead=minutes_ahead
                )

                return game_data

        except Exception as e:
            self.logger.error(f"Failed to get real game data: {e}")
            # Return empty list instead of mock data
            return []

    async def process(
        self, minutes_ahead: int, profitable_strategies: list[Any]
    ) -> list[Any]:
        """Legacy compatibility method"""
        context = {
            "minutes_ahead": minutes_ahead,
            "profitable_strategies": profitable_strategies,
            "processing_time": datetime.now(self.est),
        }

        # Get real game data from database
        game_data = await self._get_real_game_data(minutes_ahead)

        # Process using unified interface
        return await self.process_signals(game_data, context)

    def validate_strategy_data(self, raw_data: list[dict[str, Any]]) -> bool:
        """Validate hybrid sharp specific data requirements"""
        if not raw_data:
            return False

        # Check for both line movement and sharp action data
        required_fields = [
            "line_movement",
            "sharp_differential",
            "money_pct",
            "bet_pct",
        ]
        for row in raw_data:
            if not all(field in row for field in required_fields):
                return False

            # Check for valid line movement data
            if "opening_line" not in row or "current_line" not in row:
                return False

        return True
