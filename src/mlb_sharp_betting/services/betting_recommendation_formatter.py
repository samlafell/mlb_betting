#!/usr/bin/env python3
"""
Betting Recommendation Formatter Service

This service formats betting signals into user-friendly recommendation displays
with proper stake sizing, confidence levels, and market information.
"""

import json
from datetime import datetime
from typing import Any

import structlog

from ..models.betting_analysis import BettingSignal

logger = structlog.get_logger(__name__)


class RecommendationConflictDetector:
    """Detects and resolves conflicting betting recommendations for the same game"""

    @staticmethod
    def detect_conflicts(
        signals: list[BettingSignal],
    ) -> dict[str, list[BettingSignal]]:
        """Group signals by game to detect conflicts"""
        games = {}

        for signal in signals:
            game_key = f"{signal.away_team}@{signal.home_team}"
            if game_key not in games:
                games[game_key] = []
            games[game_key].append(signal)

        # Find games with conflicting recommendations
        conflicts = {}
        for game_key, game_signals in games.items():
            if len(game_signals) > 1:
                # **FIXED**: Only include ACTUALLY conflicting signals, not all signals from the game
                conflicting_signals = (
                    RecommendationConflictDetector._check_for_conflicts(game_signals)
                )
                if conflicting_signals:
                    conflicts[game_key] = conflicting_signals

        return conflicts

    @staticmethod
    def _check_for_conflicts(signals: list[BettingSignal]) -> list[BettingSignal]:
        """Check if signals recommend conflicting outcomes for the same game"""

        # Group signals by bet type
        moneyline_recs = []
        spread_recs = []
        total_recs = []

        for signal in signals:
            if signal.split_type == "moneyline":
                moneyline_recs.append(signal)
            elif signal.split_type == "spread":
                spread_recs.append(signal)
            elif signal.split_type == "total" or "total" in signal.split_type.lower():
                total_recs.append(signal)

        conflicts = []

        # **FIXED**: Only check for conflicts WITHIN the same market type

        # Check for opposing moneyline bets (Team A ML vs Team B ML)
        for i, ml_signal1 in enumerate(moneyline_recs):
            for ml_signal2 in moneyline_recs[i + 1 :]:
                if RecommendationConflictDetector._is_conflicting_teams(
                    ml_signal1, ml_signal2
                ):
                    conflicts.extend([ml_signal1, ml_signal2])

        # Check for opposing spread bets (Team A Spread vs Team B Spread)
        for i, spread_signal1 in enumerate(spread_recs):
            for spread_signal2 in spread_recs[i + 1 :]:
                if RecommendationConflictDetector._is_conflicting_teams(
                    spread_signal1, spread_signal2
                ):
                    conflicts.extend([spread_signal1, spread_signal2])

        # Check for OVER vs UNDER conflicts in totals (same total market)
        for i, total_signal1 in enumerate(total_recs):
            for total_signal2 in total_recs[i + 1 :]:
                if RecommendationConflictDetector._is_conflicting_totals(
                    total_signal1, total_signal2
                ):
                    conflicts.extend([total_signal1, total_signal2])

        # **REMOVED**: The incorrect ML vs Spread conflict check
        # Different markets (moneyline vs spread vs total) are NOT conflicts!

        # Remove duplicates
        unique_conflicts = []
        for signal in conflicts:
            if signal not in unique_conflicts:
                unique_conflicts.append(signal)

        return unique_conflicts

    @staticmethod
    def _is_conflicting_teams(signal1: BettingSignal, signal2: BettingSignal) -> bool:
        """Check if two signals recommend opposing teams"""
        # Extract team recommendations
        rec1_team = RecommendationConflictDetector._extract_team_from_recommendation(
            signal1
        )
        rec2_team = RecommendationConflictDetector._extract_team_from_recommendation(
            signal2
        )

        # If recommending different teams, it's a conflict
        return (
            rec1_team != rec2_team and rec1_team is not None and rec2_team is not None
        )

    @staticmethod
    def _is_conflicting_totals(signal1: BettingSignal, signal2: BettingSignal) -> bool:
        """Check if two total signals recommend opposing outcomes (OVER vs UNDER)"""
        if not (signal1.recommendation and signal2.recommendation):
            return False

        rec1 = signal1.recommendation.upper()
        rec2 = signal2.recommendation.upper()

        # Check for OVER vs UNDER conflicts
        over_keywords = ["OVER", "BET OVER", "TAKE OVER"]
        under_keywords = ["UNDER", "BET UNDER", "TAKE UNDER"]

        rec1_is_over = any(keyword in rec1 for keyword in over_keywords)
        rec1_is_under = any(keyword in rec1 for keyword in under_keywords)
        rec2_is_over = any(keyword in rec2 for keyword in over_keywords)
        rec2_is_under = any(keyword in rec2 for keyword in under_keywords)

        # Conflict if one recommends OVER and the other recommends UNDER
        return (rec1_is_over and rec2_is_under) or (rec1_is_under and rec2_is_over)

    @staticmethod
    def _extract_team_from_recommendation(signal: BettingSignal) -> str:
        """Extract which team is being recommended from the signal"""
        if not signal.recommendation:
            return None

        recommendation = signal.recommendation.upper()
        home_team = signal.home_team.upper()
        away_team = signal.away_team.upper()

        if home_team in recommendation:
            return signal.home_team
        elif away_team in recommendation:
            return signal.away_team

        return None

    @staticmethod
    def resolve_conflicts(
        conflicts: dict[str, list[BettingSignal]],
    ) -> list[BettingSignal]:
        """Resolve conflicts by keeping the best performing strategy per game"""
        resolved_signals = []

        for game_key, conflicting_signals in conflicts.items():
            # **ENHANCED CONFLICT RESOLUTION**: Use strategy performance, not just confidence
            best_signal = RecommendationConflictDetector._select_best_performing_signal(
                conflicting_signals
            )

            # Add enhanced conflict resolution explanation
            other_signals = [s for s in conflicting_signals if s != best_signal]
            comparison_details = (
                RecommendationConflictDetector._format_conflict_comparison(
                    best_signal, other_signals
                )
            )

            best_signal.confidence_explanation = (
                f"⚠️  CONFLICT RESOLVED: {comparison_details}. "
                f"{best_signal.confidence_explanation or ''}"
            )

            resolved_signals.append(best_signal)

        return resolved_signals

    @staticmethod
    def _select_best_performing_signal(
        conflicting_signals: list[BettingSignal],
    ) -> BettingSignal:
        """Select the best signal based on strategy performance metrics"""

        # Calculate composite performance score for each signal
        scored_signals = []
        for signal in conflicting_signals:
            performance_score = (
                RecommendationConflictDetector._calculate_performance_score(signal)
            )
            scored_signals.append((signal, performance_score))

        # Sort by performance score (highest first)
        scored_signals.sort(key=lambda x: x[1], reverse=True)

        return scored_signals[0][0]  # Return the best performing signal

    @staticmethod
    def _calculate_performance_score(signal: BettingSignal) -> float:
        """Calculate composite performance score based on multiple factors"""

        # Base score from win rate (0-100 points)
        win_rate_score = (
            signal.win_rate if signal.win_rate else 50.0
        )  # Default to 50% if missing

        # ROI score (0-50 points, capped)
        roi_score = (
            min(max(signal.roi or 0, -20), 30) + 20
        )  # Normalize ROI to 0-50 range

        # Sample size reliability score (0-25 points)
        if signal.total_bets and signal.total_bets > 0:
            sample_size_score = min(
                signal.total_bets / 2, 25
            )  # 1 bet = 0.5 points, cap at 25
        else:
            sample_size_score = 0

        # Differential strength score (0-25 points)
        differential_score = min(
            abs(signal.differential or 0) / 2, 25
        )  # 2% diff = 1 point, cap at 25

        # Confidence bonus (0-10 points)
        confidence_score = (
            (signal.confidence_score or 0) * 10
            if signal.confidence_score <= 1
            else (signal.confidence_score or 0) / 10
        )
        confidence_bonus = min(confidence_score, 10)

        total_score = (
            win_rate_score
            + roi_score
            + sample_size_score
            + differential_score
            + confidence_bonus
        )

        return total_score

    @staticmethod
    def _format_conflict_comparison(
        best_signal: BettingSignal, other_signals: list[BettingSignal]
    ) -> str:
        """Format the comparison details for conflict resolution"""

        if not other_signals:
            return "Selected best available signal"

        other = other_signals[0]  # Compare with the main alternative

        # Extract key performance metrics
        best_win_rate = best_signal.win_rate or 0
        other_win_rate = other.win_rate or 0
        best_roi = best_signal.roi or 0
        other_roi = other.roi or 0
        best_bets = best_signal.total_bets or 0
        other_bets = other.total_bets or 0

        # Format the comparison
        comparison_parts = []

        if abs(best_win_rate - other_win_rate) >= 5:
            comparison_parts.append(
                f"Win Rate: {best_win_rate:.1f}% vs {other_win_rate:.1f}%"
            )

        if abs(best_roi - other_roi) >= 2:
            comparison_parts.append(f"ROI: {best_roi:.1f}% vs {other_roi:.1f}%")

        if best_bets != other_bets:
            comparison_parts.append(f"Sample: {best_bets} vs {other_bets} bets")

        if comparison_parts:
            return f"Selected strategy with better performance ({', '.join(comparison_parts)})"
        else:
            return f"Selected highest confidence signal ({best_signal.confidence_score:.0f}% vs {other.confidence_score:.0f}%)"


class BettingRecommendationFormatter:
    """Formats betting signals into user-friendly recommendations"""

    def __init__(self):
        self.logger = logger.bind(component="betting_recommendation_formatter")

    def format_console_recommendations(
        self, signals: list[BettingSignal], min_confidence: float
    ) -> str:
        """Format betting recommendations for console display"""

        if not signals:
            return self._format_no_recommendations(min_confidence)

        # **CRITICAL FIX**: Detect and resolve conflicts before formatting
        conflict_detector = RecommendationConflictDetector()
        conflicts = conflict_detector.detect_conflicts(signals)

        if conflicts:
            self.logger.warning(
                f"🚨 CONFLICTS DETECTED: {len(conflicts)} games have conflicting recommendations"
            )

            # **COMPLETELY REWRITTEN CONFLICT RESOLUTION**
            # Step 1: Get resolved (winning) signals for each conflict
            resolved_signals = conflict_detector.resolve_conflicts(conflicts)

            # Step 2: Identify exactly which signals are losing and should be removed
            losing_signals = set()
            winning_signals_by_game = {}

            # Map resolved signals to their games
            for resolved_signal in resolved_signals:
                game_key = f"{resolved_signal.away_team}@{resolved_signal.home_team}"
                winning_signals_by_game[game_key] = resolved_signal

            # For each game with conflicts, mark losing signals for removal
            for game_key, conflicting_signals in conflicts.items():
                winning_signal = winning_signals_by_game.get(game_key)
                if winning_signal:
                    # Mark all conflicting signals EXCEPT the winner for removal
                    for signal in conflicting_signals:
                        if signal != winning_signal:
                            losing_signals.add(
                                id(signal)
                            )  # Use id() to avoid issues with object comparison

            # Step 3: Remove only the losing signals, keep everything else
            signals = [s for s in signals if id(s) not in losing_signals]

            self.logger.info(
                f"✅ Conflict resolution complete: {len(losing_signals)} losing signals removed, {len(resolved_signals)} conflicts resolved"
            )

        # Group by confidence level
        high_confidence = [
            s
            for s in signals
            if self._get_confidence_percentage(s.confidence_score) >= 85
        ]
        moderate_confidence = [
            s
            for s in signals
            if 70 <= self._get_confidence_percentage(s.confidence_score) < 85
        ]
        low_confidence = [
            s
            for s in signals
            if 50 <= self._get_confidence_percentage(s.confidence_score) < 70
        ]

        output_lines = []

        # Display conflicts warning if any were found
        if conflicts:
            output_lines.extend(
                [
                    "🚨 CONFLICT RESOLUTION SUMMARY:",
                    f"   {len(conflicts)} games had conflicting recommendations",
                    "   Kept highest confidence signal for each game",
                    "   This prevents impossible betting scenarios",
                    "",
                ]
            )

        # High confidence recommendations
        if high_confidence:
            output_lines.append(
                f"🔥 HIGH CONFIDENCE RECOMMENDATIONS ({len(high_confidence)}):"
            )
            output_lines.append("=" * 60)

            for signal in high_confidence:
                rec_lines = self._format_enhanced_recommendation(
                    signal, is_high_confidence=True
                )
                output_lines.extend(rec_lines)

        # Moderate confidence recommendations
        if moderate_confidence:
            output_lines.append(
                f"\n⚡ MODERATE CONFIDENCE RECOMMENDATIONS ({len(moderate_confidence)}):"
            )
            output_lines.append("=" * 60)

            for signal in moderate_confidence:
                rec_lines = self._format_enhanced_recommendation(
                    signal, is_high_confidence=False
                )
                output_lines.extend(rec_lines)

        # Low confidence recommendations (show but warn)
        if low_confidence:
            output_lines.append(
                f"\n⚠️  LOW CONFIDENCE SIGNALS ({len(low_confidence)}) - PROCEED WITH CAUTION:"
            )
            output_lines.append("=" * 60)

            for signal in low_confidence:
                rec_lines = self._format_enhanced_recommendation(
                    signal, is_high_confidence=False, is_low_confidence=True
                )
                output_lines.extend(rec_lines)

        # Add disclaimer
        output_lines.extend(self._format_enhanced_disclaimer())

        return "\n".join(output_lines)

    def format_json_recommendations(
        self, signals: list[BettingSignal], min_confidence: float
    ) -> dict[str, Any]:
        """Format recommendations as JSON"""

        recommendations = []
        for signal in signals:
            bet_details = self._parse_recommendation_details(signal)

            recommendations.append(
                {
                    "game_matchup": f"{signal.away_team} @ {signal.home_team}",
                    "game_time": signal.game_time.isoformat()
                    if signal.game_time
                    else None,
                    "minutes_to_game": signal.minutes_to_game,
                    "bet_recommendation": bet_details["bet_line"],
                    "confidence_score": signal.confidence_score,
                    "confidence_level": signal.confidence_level.value
                    if signal.confidence_level
                    else "UNKNOWN",
                    "strategy_name": signal.strategy_name,
                    "signal_strength": signal.signal_strength,
                    "differential": signal.differential,
                    "source": signal.source,
                    "book": signal.book,
                    "suggested_stake": self._calculate_stake_size(
                        signal.confidence_score
                    ),
                    "market_info": bet_details["market_info"],
                    "movement_info": bet_details["movement_info"],
                    "confidence_explanation": signal.confidence_explanation,
                    "recommendation_strength": signal.recommendation_strength,
                }
            )

        return {
            "timestamp": datetime.now().isoformat(),
            "min_confidence_threshold": min_confidence,
            "total_recommendations": len(recommendations),
            "high_confidence_count": len(
                [r for r in recommendations if r["confidence_score"] >= 0.80]
            ),
            "moderate_confidence_count": len(
                [r for r in recommendations if 0.70 <= r["confidence_score"] < 0.80]
            ),
            "recommendations": recommendations,
        }

    def _format_no_recommendations(self, min_confidence: float) -> str:
        """Format message when no recommendations are found"""

        return f"""
⚠️  NO HIGH CONFIDENCE RECOMMENDATIONS FOUND

📊 Try lowering the minimum confidence threshold (currently {min_confidence}%)
💡 Or wait for more favorable betting conditions
🔄 Run 'mlb-cli detect opportunities' to see all available data
"""

    def _format_single_recommendation(
        self, signal: BettingSignal, is_high_confidence: bool
    ) -> list[str]:
        """Format a single betting recommendation"""

        lines = []

        # Game matchup and bet line
        game_matchup = f"{signal.away_team} @ {signal.home_team}"
        bet_details = self._parse_recommendation_details(signal)

        emoji = "🔥" if is_high_confidence else "⚡"
        lines.append(f"\n{emoji} {game_matchup} - {bet_details['bet_line']}")

        # Core recommendation details
        lines.append(f"   📊 Confidence: {signal.confidence_score:.0%}")
        lines.append(f"   💰 {bet_details['market_info']}")

        if bet_details["movement_info"] != "Line movement data unavailable":
            lines.append(f"   📈 {bet_details['movement_info']}")

        lines.append(
            f"   🎯 Strategy: {self._format_strategy_name(signal.strategy_name)}"
        )
        lines.append(
            f"   💵 Suggested Stake: {self._calculate_stake_size(signal.confidence_score)}"
        )

        # Optional details
        if signal.confidence_explanation:
            lines.append(f"   💡 Reasoning: {signal.confidence_explanation}")

        return lines

    def _parse_recommendation_details(self, signal: BettingSignal) -> dict[str, str]:
        """Parse signal details into display format"""

        try:
            # Parse split_value JSON if it exists
            if signal.split_value:
                split_data = json.loads(signal.split_value)
            else:
                split_data = {}

            # Determine bet line based on signal type and recommendation
            bet_line = "UNKNOWN BET"
            market_info = "Market data unavailable"
            movement_info = "Line movement data unavailable"

            if signal.split_type == "moneyline":
                bet_line, market_info = self._format_moneyline_bet(signal, split_data)

            elif signal.split_type == "total":
                bet_line, market_info = self._format_total_bet(signal, split_data)

            elif signal.split_type == "spread":
                bet_line, market_info = self._format_spread_bet(signal, split_data)

            else:
                bet_line = signal.recommendation or "BET RECOMMENDATION"
                market_info = f"Signal strength: {signal.signal_strength:.1f}"

            # Movement info based on signal source
            movement_info = self._format_movement_info(signal)

            return {
                "bet_line": bet_line,
                "market_info": market_info,
                "movement_info": movement_info,
            }

        except Exception as e:
            self.logger.warning("Failed to parse recommendation details", error=str(e))
            return {
                "bet_line": signal.recommendation or "BET RECOMMENDATION",
                "market_info": f"Signal strength: {signal.signal_strength:.1f}",
                "movement_info": f"Source: {signal.source}",
            }

    def _format_moneyline_bet(
        self, signal: BettingSignal, split_data: dict
    ) -> tuple[str, str]:
        """Format moneyline bet details"""

        if "home" in split_data and "away" in split_data:
            home_odds = split_data["home"]
            away_odds = split_data["away"]

            if (
                signal.recommendation
                and signal.home_team.upper() in signal.recommendation.upper()
            ):
                bet_line = f"{signal.home_team} ML ({home_odds:+d})"
            elif (
                signal.recommendation
                and signal.away_team.upper() in signal.recommendation.upper()
            ):
                bet_line = f"{signal.away_team} ML ({away_odds:+d})"
            else:
                bet_line = signal.recommendation or "MONEYLINE BET"

            market_info = f"Sharp Money: {signal.differential:.1f}% differential"
        else:
            bet_line = signal.recommendation or "MONEYLINE BET"
            market_info = f"Signal strength: {signal.signal_strength:.1f}"

        return bet_line, market_info

    def _format_total_bet(
        self, signal: BettingSignal, split_data: dict
    ) -> tuple[str, str]:
        """Format total bet details"""

        # Extract total from recommendation if possible
        if signal.recommendation:
            if "OVER" in signal.recommendation.upper():
                bet_line = signal.recommendation.replace("BET ", "")
            elif "UNDER" in signal.recommendation.upper():
                bet_line = signal.recommendation.replace("BET ", "")
            else:
                bet_line = signal.recommendation
        else:
            bet_line = "TOTAL BET"

        market_info = f"Total differential: {signal.differential:.1f}%"

        return bet_line, market_info

    def _format_spread_bet(
        self, signal: BettingSignal, split_data: dict
    ) -> tuple[str, str]:
        """Format spread bet details"""

        bet_line = signal.recommendation or "SPREAD BET"
        market_info = f"Spread differential: {signal.differential:.1f}%"

        return bet_line, market_info

    def _format_movement_info(self, signal: BettingSignal) -> str:
        """Format line movement information"""

        if signal.source == "VSIN":
            return f"VSIN {signal.book} data: {signal.differential:+.1f}% edge"
        elif signal.differential and signal.differential != 0:
            return f"Line moved {signal.differential:+.1f}% in favor"
        else:
            return f"Data source: {signal.source}"

    def _format_strategy_name(self, strategy_name: str) -> str:
        """Format strategy name for display"""

        if not strategy_name:
            return "Unknown Strategy"

        # Clean up strategy names
        formatted = strategy_name.replace("_", " ").title()

        # Common replacements
        replacements = {
            "Vsin": "VSIN",
            "Ml": "ML",
            "Dk": "DK",
            "Roi": "ROI",
            "Api": "API",
        }

        for old, new in replacements.items():
            formatted = formatted.replace(old, new)

        return formatted

    def _calculate_stake_size(self, confidence_score: float) -> str:
        """Calculate suggested stake size based on confidence"""

        # Convert to percentage if needed (handle both 0-1 and 0-100 scales)
        confidence_pct = (
            confidence_score * 100 if confidence_score <= 1.0 else confidence_score
        )

        if confidence_pct >= 90:
            return "4-5 units (MAX BET - ELITE EDGE)"
        elif confidence_pct >= 85:
            return "3-4 units (HIGH CONVICTION)"
        elif confidence_pct >= 75:
            return "2-3 units (STRONG)"
        elif confidence_pct >= 65:
            return "1.5-2 units (MODERATE)"
        elif confidence_pct >= 50:
            return "1 unit (LIGHT)"
        elif confidence_pct >= 35:
            return "0.5 units (MINIMAL)"
        else:
            return "AVOID BET (confidence too low)"

    def _format_disclaimer(self) -> list[str]:
        """Format betting disclaimer"""

        return [
            "",
            "⚠️  BETTING DISCLAIMER:",
            "   • These are algorithm-generated recommendations",
            "   • Past performance does not guarantee future results",
            "   • Only bet what you can afford to lose",
            "   • Consider your bankroll management strategy",
        ]

    def _get_confidence_percentage(self, confidence_score: float) -> float:
        """Convert confidence score to percentage (handle both 0-1 and 0-100 scales)"""
        return confidence_score * 100 if confidence_score <= 1.0 else confidence_score

    def _format_enhanced_recommendation(
        self,
        signal: BettingSignal,
        is_high_confidence: bool,
        is_low_confidence: bool = False,
    ) -> list[str]:
        """Format a single betting recommendation with enhanced clarity"""

        lines = []

        # Game matchup
        game_matchup = f"{signal.away_team} @ {signal.home_team}"

        # Parse bet details with clear specifications
        bet_details = self._parse_enhanced_recommendation_details(signal)

        emoji = "🔥" if is_high_confidence else "⚡" if not is_low_confidence else "⚠️"

        # **CLEAR BET SPECIFICATION** - Address user's concern about unclear betting
        lines.append(f"\n{emoji} {bet_details['clear_bet_line']}")
        lines.append(f"   🏟️  Game: {game_matchup}")

        # Enhanced confidence display
        confidence_pct = self._get_confidence_percentage(signal.confidence_score)
        lines.append(f"   📊 Confidence: {confidence_pct:.0f}%")

        # **CLEAR DIFFERENTIAL EXPLANATION** - Address user's concern
        lines.append(
            f"   💰 Sharp Edge: {signal.differential:.1f}% money/bet differential"
        )

        if bet_details["line_info"]:
            lines.append(f"   📈 Line: {bet_details['line_info']}")

        lines.append(
            f"   🎯 Strategy: {self._format_strategy_name(signal.strategy_name)}"
        )

        # **IMPROVED STAKE SIZING** - Address user's concern about stakes not matching confidence
        stake_size = self._calculate_stake_size(signal.confidence_score)
        lines.append(f"   💵 Suggested Stake: {stake_size}")

        # Warning for low confidence
        if is_low_confidence:
            lines.append("   ⚠️  WARNING: Low confidence - consider avoiding this bet")

        # Enhanced reasoning
        if signal.confidence_explanation:
            lines.append(f"   🧠 Reasoning: {signal.confidence_explanation}")

        return lines

    def _parse_enhanced_recommendation_details(
        self, signal: BettingSignal
    ) -> dict[str, str]:
        """Parse signal details into enhanced display format with clear bet specifications"""

        try:
            # Parse split_value JSON if it exists
            if signal.split_value:
                split_data = json.loads(signal.split_value)
            else:
                split_data = {}

            # **CLEAR BET SPECIFICATIONS** - Address user's main concern
            clear_bet_line = "UNKNOWN BET"
            line_info = ""

            if signal.split_type == "moneyline":
                clear_bet_line, line_info = self._format_enhanced_moneyline_bet(
                    signal, split_data
                )

            elif signal.split_type == "total":
                clear_bet_line, line_info = self._format_enhanced_total_bet(
                    signal, split_data
                )

            elif signal.split_type == "spread":
                clear_bet_line, line_info = self._format_enhanced_spread_bet(
                    signal, split_data
                )

            else:
                clear_bet_line = signal.recommendation or "BET RECOMMENDATION"
                line_info = f"Signal strength: {signal.signal_strength:.1f}"

            return {"clear_bet_line": clear_bet_line, "line_info": line_info}

        except Exception as e:
            self.logger.warning(
                "Failed to parse enhanced recommendation details", error=str(e)
            )
            return {
                "clear_bet_line": signal.recommendation or "BET RECOMMENDATION",
                "line_info": f"Source: {signal.source}",
            }

    def _format_enhanced_moneyline_bet(
        self, signal: BettingSignal, split_data: dict
    ) -> tuple[str, str]:
        """Format moneyline bet with clear team and odds"""

        if "home" in split_data and "away" in split_data:
            home_odds = split_data["home"]
            away_odds = split_data["away"]

            # Determine which team to bet based on recommendation
            if (
                signal.recommendation
                and signal.home_team.upper() in signal.recommendation.upper()
            ):
                clear_bet_line = f"🎯 {signal.home_team} Moneyline ({home_odds:+d})"
                line_info = f"{signal.home_team} at {home_odds:+d} odds"
            elif (
                signal.recommendation
                and signal.away_team.upper() in signal.recommendation.upper()
            ):
                clear_bet_line = f"🎯 {signal.away_team} Moneyline ({away_odds:+d})"
                line_info = f"{signal.away_team} at {away_odds:+d} odds"
            else:
                # Default to differential direction
                if signal.differential > 0:
                    clear_bet_line = f"🎯 {signal.home_team} Moneyline ({home_odds:+d})"
                    line_info = f"{signal.home_team} at {home_odds:+d} odds"
                else:
                    clear_bet_line = f"🎯 {signal.away_team} Moneyline ({away_odds:+d})"
                    line_info = f"{signal.away_team} at {away_odds:+d} odds"
        else:
            clear_bet_line = signal.recommendation or "MONEYLINE BET"
            line_info = "Odds data unavailable"

        return clear_bet_line, line_info

    def _format_enhanced_total_bet(
        self, signal: BettingSignal, split_data: dict
    ) -> tuple[str, str]:
        """Format total bet with clear over/under specification"""

        # Extract total from recommendation if possible
        if signal.recommendation:
            if "OVER" in signal.recommendation.upper():
                clear_bet_line = f"🎯 {signal.recommendation.replace('BET ', '')}"
                line_info = "Betting the Over"
            elif "UNDER" in signal.recommendation.upper():
                clear_bet_line = f"🎯 {signal.recommendation.replace('BET ', '')}"
                line_info = "Betting the Under"
            else:
                clear_bet_line = f"🎯 {signal.recommendation}"
                line_info = "Total bet"
        else:
            # Use differential to determine direction
            if signal.differential > 0:
                clear_bet_line = "🎯 OVER Total"
                line_info = "Betting the Over"
            else:
                clear_bet_line = "🎯 UNDER Total"
                line_info = "Betting the Under"

        return clear_bet_line, line_info

    def _format_enhanced_spread_bet(
        self, signal: BettingSignal, split_data: dict
    ) -> tuple[str, str]:
        """Format spread bet with clear team and line"""

        if signal.recommendation:
            if signal.home_team.upper() in signal.recommendation.upper():
                clear_bet_line = f"🎯 {signal.home_team} Spread"
                line_info = f"{signal.home_team} to cover the spread"
            elif signal.away_team.upper() in signal.recommendation.upper():
                clear_bet_line = f"🎯 {signal.away_team} Spread"
                line_info = f"{signal.away_team} to cover the spread"
            else:
                clear_bet_line = signal.recommendation
                line_info = "Spread bet"
        else:
            # Use differential to determine direction
            if signal.differential > 0:
                clear_bet_line = f"🎯 {signal.home_team} Spread"
                line_info = f"{signal.home_team} to cover the spread"
            else:
                clear_bet_line = f"🎯 {signal.away_team} Spread"
                line_info = f"{signal.away_team} to cover the spread"

        return clear_bet_line, line_info

    def _format_enhanced_disclaimer(self) -> list[str]:
        """Format enhanced betting disclaimer"""

        return [
            "",
            "⚠️  ENHANCED BETTING DISCLAIMER:",
            "   • These are algorithm-generated recommendations with improved validation",
            "   • Conflict detection prevents impossible betting scenarios",
            "   • Confidence scores now properly scale with differential strength",
            "   • Stake sizing matches confidence levels appropriately",
            "   • Past performance does not guarantee future results",
            "   • Only bet what you can afford to lose",
            "   • Always verify lines and odds before placing bets",
            "   • This service is for informational purposes only",
        ]
