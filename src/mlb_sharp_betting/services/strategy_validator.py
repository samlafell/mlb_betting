"""
DEPRECATED: Strategy Validator - Unified Strategy Matching Logic

⚠️  DEPRECATION NOTICE: This service has been consolidated into StrategyValidation.
Please update imports to use:
    from ..services.strategy_validation import StrategyValidation

This file is provided for backward compatibility during the transition.
It will be removed in a future version.

MIGRATION GUIDE:
- Replace `StrategyValidator` with `StrategyValidation`
- The StrategyValidation provides all validator functionality plus:
  - Registry management (from StrategyValidationRegistry)
  - Gate validation (from ValidationGateService)
  - Daily validation (from DailyStrategyValidationService)
  - Unified interface for all validation operations
"""


from ..core.logging import get_logger
from ..models.betting_analysis import ProfitableStrategy, StrategyThresholds


class StrategyValidator:
    """Unified validator for all strategy types and signal matching"""

    def __init__(
        self,
        profitable_strategies: list[ProfitableStrategy],
        thresholds: StrategyThresholds,
    ):
        self.strategies = profitable_strategies
        self.thresholds = thresholds
        self.logger = get_logger(__name__)

        # Group strategies by type for efficient lookup
        self.strategies_by_type = self._group_strategies_by_type()

    def _group_strategies_by_type(self) -> dict[str, list[ProfitableStrategy]]:
        """Group strategies by their category for efficient matching"""
        groups = {
            "SHARP_ACTION": [],
            "OPPOSING_MARKETS": [],
            "STEAM_MOVES": [],
            "BOOK_CONFLICTS": [],
            "TOTALS": [],
            "UNDERDOG_VALUE": [],
            "CONSENSUS_STRATEGIES": [],
            "GENERAL": [],
        }

        for strategy in self.strategies:
            category = self._determine_strategy_category(strategy.strategy_name)
            groups[category].append(strategy)

        return groups

    def _determine_strategy_category(self, strategy_name: str) -> str:
        """Determine the category of a strategy to prevent inappropriate matching"""
        strategy_lower = strategy_name.lower()

        if "book_conflicts" in strategy_lower:
            return "BOOK_CONFLICTS"
        elif "opposing_markets" in strategy_lower:
            return "OPPOSING_MARKETS"
        elif (
            "steam" in strategy_lower
            or "timing" in strategy_lower
            or "late_sharp_flip" in strategy_lower
        ):
            return "STEAM_MOVES"
        elif "total" in strategy_lower and (
            "line" in strategy_lower or "sweet" in strategy_lower
        ):
            return "TOTALS"
        elif "underdog_ml" in strategy_lower or "underdog" in strategy_lower:
            return "UNDERDOG_VALUE"
        elif (
            "signal_combo" in strategy_lower
            or "consensus_moneyline" in strategy_lower
            or "public_money_fade" in strategy_lower
        ):
            return "CONSENSUS_STRATEGIES"
        elif (
            "sharp" in strategy_lower
            or "signal_combinations" in strategy_lower
            or "line_movement" in strategy_lower
            or "hybrid" in strategy_lower
        ):
            return "SHARP_ACTION"
        else:
            return "GENERAL"

    def find_matching_strategy(
        self,
        signal_type: str,
        source: str,
        book: str | None,
        split_type: str,
        signal_strength: float,
    ) -> ProfitableStrategy | None:
        """Find a profitable strategy that matches the current signal"""

        # Map signal types to strategy categories
        signal_to_category = {
            "SHARP_ACTION": "SHARP_ACTION",
            "TOTAL_SHARP": "TOTALS",
            "OPPOSING_MARKETS": "OPPOSING_MARKETS",
            "STEAM_MOVE": "STEAM_MOVES",
            "LATE_FLIP": "STEAM_MOVES",
            "BOOK_CONFLICTS": "BOOK_CONFLICTS",
            "PUBLIC_FADE": "CONSENSUS_STRATEGIES",
            "CONSENSUS_MONEYLINE": "CONSENSUS_STRATEGIES",
            "UNDERDOG_VALUE": "UNDERDOG_VALUE",
            "LINE_MOVEMENT": "SHARP_ACTION",
        }

        category = signal_to_category.get(signal_type, "GENERAL")
        candidate_strategies = self.strategies_by_type.get(category, [])

        if not candidate_strategies:
            # Fallback to general strategies if no specific category matches
            candidate_strategies = self.strategies_by_type.get("GENERAL", [])

        # 1. Try book-specific exact matches first (source-book-split_type)
        if source and book:
            book_specific_key = f"{source.upper()}-{book.lower()}"
            exact_book_matches = [
                s
                for s in candidate_strategies
                if s.split_type == split_type
                and book_specific_key.lower() in s.source_book.lower()
            ]

            for strategy in exact_book_matches:
                threshold = self.get_threshold_for_strategy(strategy, signal_strength)
                if signal_strength >= threshold:
                    return strategy

        # 2. Try source-specific matches (ignore book)
        if source:
            source_matches = [
                s
                for s in candidate_strategies
                if s.split_type == split_type
                and source.upper() in s.source_book.upper()
            ]

            for strategy in source_matches:
                threshold = self.get_threshold_for_strategy(strategy, signal_strength)
                if signal_strength >= threshold:
                    return strategy

        # 3. Try exact split_type matches (ignore source/book)
        exact_matches = [s for s in candidate_strategies if s.split_type == split_type]

        for strategy in exact_matches:
            threshold = self.get_threshold_for_strategy(strategy, signal_strength)
            if signal_strength >= threshold:
                return strategy

        # 4. Try compatible fallbacks within the same category
        for strategy in candidate_strategies:
            # Use more conservative thresholds for fallback strategies
            threshold = self.get_threshold_for_strategy(
                strategy, signal_strength, is_fallback=True
            )
            if signal_strength >= threshold:
                # Return a modified strategy indicating it's a fallback
                fallback_source_book = (
                    f"{source}-{book}" if source and book else strategy.source_book
                )
                return ProfitableStrategy(
                    strategy_name=f"{strategy.strategy_name}_FALLBACK_{split_type}",
                    source_book=fallback_source_book,
                    split_type=strategy.split_type,  # Keep original split_type
                    win_rate=strategy.win_rate,
                    roi=strategy.roi,
                    total_bets=strategy.total_bets,
                    confidence=strategy.confidence,
                    ci_lower=strategy.ci_lower,
                    ci_upper=strategy.ci_upper,
                    confidence_score=getattr(
                        strategy, "confidence_score", 0.5
                    ),  # ✅ FIX: Added missing confidence_score
                )

        return None

    def get_threshold_for_strategy(
        self,
        strategy: ProfitableStrategy,
        signal_strength: float,
        is_fallback: bool = False,
    ) -> float:
        """Calculate dynamic threshold based on strategy performance"""
        base_multiplier = 1.25 if is_fallback else 1.0

        if strategy.win_rate >= self.thresholds.high_performance_wr:
            return self.thresholds.high_performance_threshold * base_multiplier
        elif strategy.win_rate >= self.thresholds.moderate_performance_wr:
            return self.thresholds.moderate_performance_threshold * base_multiplier
        elif strategy.win_rate >= self.thresholds.low_performance_wr:
            return self.thresholds.low_performance_threshold * base_multiplier
        else:
            # Very conservative threshold for low-performing strategies
            return self.thresholds.low_performance_threshold * 1.5 * base_multiplier

    def get_strategies_by_type(self, signal_type: str) -> list[ProfitableStrategy]:
        """Get all strategies for a specific signal type"""
        signal_to_category = {
            "SHARP_ACTION": "SHARP_ACTION",
            "TOTAL_SHARP": "TOTALS",
            "OPPOSING_MARKETS": "OPPOSING_MARKETS",
            "STEAM_MOVE": "STEAM_MOVES",
            "LATE_FLIP": "STEAM_MOVES",
            "BOOK_CONFLICTS": "BOOK_CONFLICTS",
            "PUBLIC_FADE": "CONSENSUS_STRATEGIES",
            "CONSENSUS_MONEYLINE": "CONSENSUS_STRATEGIES",
            "UNDERDOG_VALUE": "UNDERDOG_VALUE",
            "LINE_MOVEMENT": "SHARP_ACTION",
        }

        category = signal_to_category.get(signal_type, "GENERAL")
        return self.strategies_by_type.get(category, [])

    def get_best_strategy_for_type(
        self, signal_type: str
    ) -> ProfitableStrategy | None:
        """Get the highest ROI strategy for a specific signal type"""
        strategies = self.get_strategies_by_type(signal_type)
        if not strategies:
            return None
        return max(strategies, key=lambda s: s.roi)

    def validate_signal_against_strategies(
        self, signal_type: str, signal_strength: float
    ) -> bool:
        """Check if a signal meets any strategy thresholds"""
        strategies = self.get_strategies_by_type(signal_type)

        for strategy in strategies:
            threshold = self.get_threshold_for_strategy(strategy, signal_strength)
            if signal_strength >= threshold:
                return True

        return False

    def get_strategy_summary(self) -> dict[str, dict]:
        """Get summary statistics for all strategy categories"""
        summary = {}

        for category, strategies in self.strategies_by_type.items():
            if not strategies:
                continue

            total_bets = sum(s.total_bets for s in strategies)
            weighted_win_rate = (
                sum(s.win_rate * s.total_bets for s in strategies) / total_bets
                if total_bets > 0
                else 0
            )
            weighted_roi = (
                sum(s.roi * s.total_bets for s in strategies) / total_bets
                if total_bets > 0
                else 0
            )

            best_strategy = max(strategies, key=lambda s: s.roi)

            summary[category] = {
                "count": len(strategies),
                "weighted_win_rate": weighted_win_rate,
                "weighted_roi": weighted_roi,
                "best_strategy": best_strategy.strategy_name,
                "best_roi": best_strategy.roi,
                "total_bets": total_bets,
            }

        return summary

    def get_threshold_config_for_source(
        self, source: str, signal_type: str
    ) -> dict[str, float]:
        """Get threshold configuration for a specific source and signal type"""
        strategies = self.get_strategies_by_type(signal_type)
        source_strategies = [
            s for s in strategies if source.upper() in s.source_book.upper()
        ]

        if not source_strategies:
            # Use best available strategy as fallback
            best_strategy = self.get_best_strategy_for_type(signal_type)
            if not best_strategy:
                return {
                    "minimum_threshold": 30.0,
                    "moderate_confidence_threshold": 25.0,
                    "high_confidence_threshold": 20.0,
                }
            strategies_to_use = [best_strategy]
        else:
            strategies_to_use = source_strategies

        # Use the best performing strategy for this source
        best_source_strategy = max(strategies_to_use, key=lambda s: s.roi)

        return {
            "minimum_threshold": self.get_threshold_for_strategy(
                best_source_strategy, 0
            ),
            "moderate_confidence_threshold": self.get_threshold_for_strategy(
                best_source_strategy, 0
            )
            * 0.8,
            "high_confidence_threshold": self.get_threshold_for_strategy(
                best_source_strategy, 0
            )
            * 0.6,
        }

    def has_profitable_strategies(self) -> bool:
        """Check if any profitable strategies are available"""
        return len(self.strategies) > 0

    def get_strategy_count_by_type(self) -> dict[str, int]:
        """Get count of strategies by type"""
        return {
            category: len(strategies)
            for category, strategies in self.strategies_by_type.items()
        }

    def filter_strategies_by_performance(
        self, min_win_rate: float = 50.0, min_roi: float = 5.0, min_bets: int = 10
    ) -> list[ProfitableStrategy]:
        """Filter strategies by performance criteria"""
        return [
            s
            for s in self.strategies
            if s.win_rate >= min_win_rate
            and s.roi >= min_roi
            and s.total_bets >= min_bets
        ]
