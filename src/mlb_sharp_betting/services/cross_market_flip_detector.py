"""
Cross-Market Flip Detection Service

Detects when sharp money flips between different bet types (spread vs moneyline vs total)
within the same game, especially late changes that contradict early action.

Addresses the specific case where early spread action on one team is followed by
late moneyline action on the opposite team.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

import structlog

from ..db.connection import DatabaseManager
from ..models.splits import BookType, DataSource, SplitType
from ..models.timing_analysis import TimingBucket

logger = structlog.get_logger(__name__)


class FlipType(Enum):
    """Types of market flips we can detect."""

    SAME_MARKET_FLIP = "same_market_flip"  # ML early home, ML late away
    CROSS_MARKET_CONTRADICTION = (
        "cross_market_contradiction"  # Spread early home, ML late away
    )
    WEAK_LATE_CONTRADICTION = (
        "weak_late_contradiction"  # Strong early signal, weak late opposite
    )
    TRIPLE_MARKET_CONFLICT = "triple_market_conflict"  # All three markets disagree


class SignalStrength(Enum):
    """Signal strength classification."""

    VERY_STRONG = "very_strong"  # 25%+ differential
    STRONG = "strong"  # 15-25% differential
    MODERATE = "moderate"  # 10-15% differential
    WEAK = "weak"  # 8-10% differential


@dataclass
class MarketSignal:
    """Represents a betting signal from a specific market."""

    split_type: SplitType
    source: DataSource
    book: BookType | None
    recommended_team: str  # Team name or 'OVER'/'UNDER'
    differential: float
    stake_percentage: float
    bet_percentage: float
    timestamp: datetime
    hours_before_game: float
    strength: SignalStrength
    timing_bucket: TimingBucket


@dataclass
class FlipDetection:
    """Represents a detected flip between markets."""

    game_id: str
    home_team: str
    away_team: str
    game_datetime: datetime
    flip_type: FlipType

    # Early and late signals
    early_signal: MarketSignal
    late_signal: MarketSignal

    # Analysis
    hours_between_signals: float
    confidence_score: float  # 0-100
    strategy_recommendation: str  # Team to bet on
    reasoning: str

    # Risk factors
    risk_factors: list[str]


class CrossMarketFlipDetector:
    """Service to detect cross-market betting flips."""

    # BACKTESTED TOTAL FLIP PERFORMANCE - ALL COMBINATIONS ARE UNPROFITABLE
    TOTAL_FLIP_BANNED_COMBINATIONS = {
        "VSIN-circa": True,  # 18.2% win rate, -65.29% ROI - TERRIBLE
        "VSIN-draftkings": True,  # 40.3% win rate, -23.16% ROI - UNPROFITABLE
        "SBD-NULL": True,  # 7.9% win rate, -84.93% ROI - CATASTROPHIC
    }

    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager

        # Configuration - Made more conservative to reduce false positives
        self.min_sharp_threshold = 12.0  # Increased from 8.0 - require stronger signals
        self.early_cutoff_hours = (
            4.0  # Increased from 3.0 - require truly early signals
        )
        self.late_cutoff_hours = 2.0  # Keep at 2.0 for late signals
        self.min_confidence_threshold = (
            75.0  # Increased from 60.0 - require high confidence
        )
        self.min_signal_strength_diff = (
            10.0  # New: minimum differential between early/late signals
        )

    async def detect_flips_for_game(
        self,
        game_id: str,
        source: DataSource | None = None,
        book: BookType | None = None,
    ) -> list[FlipDetection]:
        """
        Detect cross-market flips for a specific game.

        Args:
            game_id: Game identifier
            source: Optional source filter
            book: Optional book filter

        Returns:
            List of detected flips for the game
        """
        try:
            # Get all market signals for this game
            signals = await self._get_game_signals(game_id, source, book)

            if len(signals) < 2:
                logger.debug(
                    "Insufficient signals for flip detection",
                    game_id=game_id,
                    signal_count=len(signals),
                )
                return []

            # Group signals by timing
            early_signals = [
                s for s in signals if s.hours_before_game >= self.early_cutoff_hours
            ]
            late_signals = [
                s for s in signals if s.hours_before_game <= self.late_cutoff_hours
            ]

            if not early_signals or not late_signals:
                logger.debug(
                    "Missing early or late signals",
                    game_id=game_id,
                    early_count=len(early_signals),
                    late_count=len(late_signals),
                )
                return []

            # Detect flips between early and late signals
            flips = []
            for early in early_signals:
                for late in late_signals:
                    flip = self._analyze_signal_pair(early, late, game_id)
                    if flip and flip.confidence_score >= self.min_confidence_threshold:
                        flips.append(flip)

            # Also detect contradictions within the same timing bucket
            # This catches cases like TOTAL vs SPREAD both being "early" but contradictory
            same_timing_contradictions = []

            # Check early signals against each other (only if strong signals)
            strong_early_signals = [
                s for s in early_signals if abs(s.differential) >= 15.0
            ]
            for i, signal1 in enumerate(strong_early_signals):
                for signal2 in strong_early_signals[i + 1 :]:
                    try:
                        contradiction = self._analyze_same_timing_contradiction(
                            signal1, signal2, game_id, "early"
                        )
                        if (
                            contradiction
                            and contradiction.confidence_score
                            >= self.min_confidence_threshold
                        ):
                            same_timing_contradictions.append(contradiction)
                    except Exception as e:
                        logger.debug(
                            "Failed to analyze same timing contradiction", error=str(e)
                        )

            # Check late signals against each other (only if strong signals)
            strong_late_signals = [
                s for s in late_signals if abs(s.differential) >= 15.0
            ]
            for i, signal1 in enumerate(strong_late_signals):
                for signal2 in strong_late_signals[i + 1 :]:
                    try:
                        contradiction = self._analyze_same_timing_contradiction(
                            signal1, signal2, game_id, "late"
                        )
                        if (
                            contradiction
                            and contradiction.confidence_score
                            >= self.min_confidence_threshold
                        ):
                            same_timing_contradictions.append(contradiction)
                    except Exception as e:
                        logger.debug(
                            "Failed to analyze same timing contradiction", error=str(e)
                        )

            flips.extend(same_timing_contradictions)

            # Remove duplicates and rank by confidence - more aggressive deduplication
            unique_flips = self._deduplicate_flips_aggressively(flips)

            logger.info(
                "Detected cross-market flips",
                game_id=game_id,
                flip_count=len(unique_flips),
                total_analyzed=len(flips),
            )

            return unique_flips

        except Exception as e:
            logger.error(
                "Failed to detect flips for game", game_id=game_id, error=str(e)
            )
            return []

    async def detect_recent_flips(
        self, hours_back: int = 24, min_confidence: float = 50.0
    ) -> list[FlipDetection]:
        """
        Detect cross-market flips in recent games.

        Args:
            hours_back: How many hours back to search
            min_confidence: Minimum confidence score for inclusion

        Returns:
            List of recent flip detections
        """
        try:
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_back)

            # Get recent games with significant signals
            query = """
            SELECT DISTINCT game_id, home_team, away_team, game_datetime
            FROM splits.raw_mlb_betting_splits
            WHERE game_datetime >= %s
            AND ABS(home_or_over_stake_percentage - home_or_over_bets_percentage) >= %s
            ORDER BY game_datetime DESC
            """

            results = self.db_manager.execute_query(
                query, (cutoff_time, self.min_sharp_threshold), fetch=True
            )

            all_flips = []
            for row in results or []:
                game_id = row[0]
                flips = await self.detect_flips_for_game(game_id)
                all_flips.extend(
                    [f for f in flips if f.confidence_score >= min_confidence]
                )

            # Sort by confidence score
            all_flips.sort(key=lambda x: x.confidence_score, reverse=True)

            logger.info(
                "Detected recent cross-market flips",
                hours_back=hours_back,
                total_flips=len(all_flips),
            )

            return all_flips

        except Exception as e:
            logger.error("Failed to detect recent flips", error=str(e))
            return []

    async def detect_todays_flips_with_summary(
        self, min_confidence: float = 50.0
    ) -> tuple[list[FlipDetection], dict[str, Any]]:
        """
        Detect today's cross-market flips and provide a summary.

        Args:
            min_confidence: Minimum confidence score for inclusion

        Returns:
            Tuple of (flips, summary_stats)
        """
        try:
            # Get today's games in EST timezone (since MLB games are scheduled in EST)
            import pytz

            est = pytz.timezone("America/New_York")

            # Get current time in EST
            now_est = datetime.now(est)

            # Use the current date in EST, but start from current time instead of midnight
            # This ensures we only get games from today forward, not yesterday's games
            today_start = now_est  # Start from current time
            tomorrow_end = (now_est + timedelta(days=1)).replace(
                hour=23, minute=59, second=59
            )

            # Convert to UTC for database query (since game_datetime is stored in UTC)
            today = today_start.astimezone(timezone.utc)
            tomorrow = tomorrow_end.astimezone(timezone.utc)

            query = """
            SELECT DISTINCT game_id, home_team, away_team, game_datetime
            FROM splits.raw_mlb_betting_splits
            WHERE game_datetime >= %s AND game_datetime < %s
            AND ABS(home_or_over_stake_percentage - home_or_over_bets_percentage) >= %s
            ORDER BY game_datetime
            """

            results = self.db_manager.execute_query(
                query, (today, tomorrow, self.min_sharp_threshold), fetch=True
            )

            games_evaluated = len(results or [])
            all_flips = []
            games_with_flips = 0

            for row in results or []:
                game_id = row[0]
                flips = await self.detect_flips_for_game(game_id)
                quality_flips = [
                    f for f in flips if f.confidence_score >= min_confidence
                ]

                if quality_flips:
                    games_with_flips += 1
                    all_flips.extend(quality_flips)

            # Sort by confidence score
            all_flips.sort(key=lambda x: x.confidence_score, reverse=True)

            summary = {
                "games_evaluated": games_evaluated,
                "games_with_flips": games_with_flips,
                "total_flips_found": len(all_flips),
                "avg_confidence": sum(f.confidence_score for f in all_flips)
                / len(all_flips)
                if all_flips
                else 0,
                "flip_types": {},
                "recommended_bets": len(
                    [f for f in all_flips if f.strategy_recommendation != "NO BET"]
                ),
            }

            # Count flip types
            for flip in all_flips:
                flip_type = flip.flip_type.value
                summary["flip_types"][flip_type] = (
                    summary["flip_types"].get(flip_type, 0) + 1
                )

            if games_evaluated == 0:
                logger.info("No games found for today's cross-market flip analysis")
            elif len(all_flips) == 0:
                logger.info(
                    f"Evaluated {games_evaluated} games today - no cross-market flips detected above {min_confidence}% confidence threshold"
                )
            else:
                logger.info("Completed today's cross-market flip analysis", **summary)

            return all_flips, summary

        except Exception as e:
            logger.error("Failed to detect today's flips with summary", error=str(e))
            return [], {"error": str(e)}

    async def _get_game_signals(
        self,
        game_id: str,
        source: DataSource | None = None,
        book: BookType | None = None,
    ) -> list[MarketSignal]:
        """Get all significant betting signals for a game."""

        query = """
        SELECT 
            game_id, home_team, away_team, game_datetime,
            split_type, source, book,
            home_or_over_stake_percentage, home_or_over_bets_percentage,
            (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
            last_updated,
            EXTRACT(EPOCH FROM (game_datetime - last_updated))/3600 as hours_before_game
        FROM splits.raw_mlb_betting_splits
        WHERE game_id = %s
        AND ABS(home_or_over_stake_percentage - home_or_over_bets_percentage) >= %s
        AND last_updated < game_datetime
        """

        params = [game_id, self.min_sharp_threshold]

        if source:
            query += " AND source = %s"
            params.append(source.value)

        if book:
            query += " AND book = %s"
            params.append(book.value)

        query += " ORDER BY last_updated ASC"

        results = self.db_manager.execute_query(query, tuple(params), fetch=True)

        signals = []
        for row in results or []:
            signal = self._create_market_signal(row)
            if signal:
                signals.append(signal)

        return signals

    def _create_market_signal(self, row: tuple) -> MarketSignal | None:
        """Create a MarketSignal from database row."""
        try:
            (
                game_id,
                home_team,
                away_team,
                game_datetime,
                split_type_str,
                source_str,
                book_str,
                stake_pct,
                bet_pct,
                differential,
                last_updated,
                hours_before,
            ) = row

            # Convert all numeric values to float to avoid Decimal/float arithmetic errors
            stake_pct = float(stake_pct) if stake_pct is not None else 0.0
            bet_pct = float(bet_pct) if bet_pct is not None else 0.0
            differential = float(differential) if differential is not None else 0.0
            hours_before = float(hours_before) if hours_before is not None else 0.0

            # Determine recommended team
            if split_type_str == "total":
                recommended_team = "OVER" if differential > 0 else "UNDER"
            else:
                recommended_team = home_team if differential > 0 else away_team

            # Classify signal strength
            abs_diff = abs(differential)
            if abs_diff >= 25:
                strength = SignalStrength.VERY_STRONG
            elif abs_diff >= 15:
                strength = SignalStrength.STRONG
            elif abs_diff >= 10:
                strength = SignalStrength.MODERATE
            else:
                strength = SignalStrength.WEAK

            # Determine timing bucket using the standard enum
            timing_bucket = TimingBucket.from_hours_before_game(hours_before)

            # Skip VERY_LATE signals (within 2 hours) as they're too late for analysis
            if hours_before < 1.0:
                return None

            return MarketSignal(
                split_type=SplitType(split_type_str),
                source=DataSource(source_str),
                book=BookType(book_str) if book_str else None,
                recommended_team=recommended_team,
                differential=differential,
                stake_percentage=stake_pct,
                bet_percentage=bet_pct,
                timestamp=last_updated,
                hours_before_game=hours_before,
                strength=strength,
                timing_bucket=timing_bucket,
            )

        except Exception as e:
            logger.debug("Failed to create market signal", error=str(e))
            return None

    def _is_total_flip_banned(self, early: MarketSignal, late: MarketSignal) -> bool:
        """Check if this total flip combination is banned due to poor backtest performance."""

        # Only check total market flips
        if early.split_type != SplitType.TOTAL and late.split_type != SplitType.TOTAL:
            return False

        # Check if this source/book combination is banned
        source_book_combo = (
            f"{early.source.value}-{early.book.value if early.book else 'NULL'}"
        )

        if source_book_combo in self.TOTAL_FLIP_BANNED_COMBINATIONS:
            logger.debug(
                "Total flip banned due to poor backtest performance",
                source_book_combo=source_book_combo,
                early_type=early.split_type.value,
                late_type=late.split_type.value,
            )
            return True

        return False

    def _analyze_signal_pair(
        self, early: MarketSignal, late: MarketSignal, game_id: str
    ) -> FlipDetection | None:
        """Analyze a pair of signals for flip patterns."""

        # Check if this total flip combination is banned
        if self._is_total_flip_banned(early, late):
            return None

        # Must be from same source/book
        if early.source != late.source or early.book != late.book:
            return None

        # Must have conflicting recommendations for team bets
        if early.split_type != SplitType.TOTAL and late.split_type != SplitType.TOTAL:
            if early.recommended_team == late.recommended_team:
                return None  # Not a flip

        # Require minimum signal strength for both signals
        if (
            abs(early.differential) < self.min_sharp_threshold
            or abs(late.differential) < self.min_sharp_threshold
        ):
            return None

        # Require meaningful difference between signal strengths
        signal_strength_diff = abs(abs(early.differential) - abs(late.differential))
        if signal_strength_diff < self.min_signal_strength_diff:
            return None

        # Determine flip type
        flip_type = self._classify_flip_type(early, late)
        if flip_type is None:
            return None

        # Calculate confidence score (more conservative)
        confidence = self._calculate_confidence_conservative(early, late)

        # Early exit if confidence too low
        if confidence < self.min_confidence_threshold:
            return None

        # Generate strategy recommendation
        strategy_rec, reasoning = self._generate_strategy_recommendation(
            early, late, flip_type
        )

        # Identify risk factors
        risk_factors = self._identify_risk_factors(early, late)

        # Get game info
        game_info = self._get_game_info(game_id)

        return FlipDetection(
            game_id=game_id,
            home_team=game_info.get("home_team", ""),
            away_team=game_info.get("away_team", ""),
            game_datetime=game_info.get("game_datetime", datetime.now(timezone.utc)),
            flip_type=flip_type,
            early_signal=early,
            late_signal=late,
            hours_between_signals=abs(early.hours_before_game - late.hours_before_game),
            confidence_score=confidence,
            strategy_recommendation=strategy_rec,
            reasoning=reasoning,
            risk_factors=risk_factors,
        )

    def _classify_flip_type(
        self, early: MarketSignal, late: MarketSignal
    ) -> FlipType | None:
        """Classify the type of flip between two signals."""

        # Same market, opposite teams
        if early.split_type == late.split_type:
            if early.recommended_team != late.recommended_team:
                return FlipType.SAME_MARKET_FLIP

        # Cross-market contradiction (different bet types, opposite teams)
        elif early.split_type != late.split_type:
            if (
                early.recommended_team != late.recommended_team
                and early.split_type != SplitType.TOTAL
                and late.split_type != SplitType.TOTAL
            ):
                return FlipType.CROSS_MARKET_CONTRADICTION

        # Weak late contradiction (strong early, weak late opposite)
        if (
            early.strength in [SignalStrength.STRONG, SignalStrength.VERY_STRONG]
            and late.strength == SignalStrength.WEAK
            and early.recommended_team != late.recommended_team
        ):
            return FlipType.WEAK_LATE_CONTRADICTION

        return None

    def _calculate_confidence_conservative(
        self, early: MarketSignal, late: MarketSignal
    ) -> float:
        """Calculate confidence score for the flip detection with conservative approach.

        IMPORTANT: This strategy has NOT been backtested yet. Confidence should be
        conservative until we have real performance data.
        """

        # ⚠️ CRITICAL: Since cross-market flips have NO backtesting results,
        # we must cap confidence much lower than strategies that are proven
        MAX_CONFIDENCE_UNTESTED = 65.0  # No untested strategy should exceed 65%

        # Start with lower base confidence for untested strategies
        base_confidence = 15.0  # Reduced from 20.0

        # Early signal strength factor (more conservative)
        early_strength_factor = {
            SignalStrength.VERY_STRONG: 20,  # Reduced from 30
            SignalStrength.STRONG: 15,  # Reduced from 20
            SignalStrength.MODERATE: 8,  # Reduced from 10
            SignalStrength.WEAK: 0,  # No change
        }[early.strength]

        # Late signal strength factor (penalize strong late signals more)
        late_strength_factor = {
            SignalStrength.VERY_STRONG: -20,  # Strong late signals reduce confidence significantly
            SignalStrength.STRONG: -10,
            SignalStrength.MODERATE: -5,
            SignalStrength.WEAK: 8,  # Reduced from 10
        }[late.strength]

        # Timing factor (require larger gaps for higher confidence) - more conservative
        hours_gap = abs(early.hours_before_game - late.hours_before_game)
        if hours_gap >= 8:
            timing_factor = 15  # Reduced from 20
        elif hours_gap >= 6:
            timing_factor = 10  # Reduced from 15
        elif hours_gap >= 4:
            timing_factor = 5  # Reduced from 10
        else:
            timing_factor = 0  # Short gaps get no bonus

        # Source reliability factor - reduced
        source_factor = 10 if early.source == DataSource.VSIN else 5  # Reduced

        # Book reliability factor - reduced
        book_factor = 0
        if early.book:
            try:
                if early.book == BookType.CIRCA:
                    book_factor = 5  # Reduced from 10
                elif early.book.value in ["draftkings", "fanduel"]:
                    book_factor = 3  # Reduced from 8
                else:
                    book_factor = 2  # Reduced from 5
            except:
                # Handle any book type issues gracefully
                book_factor = 2

        # Signal differential factor (reward larger differences) - more conservative
        diff_factor = min(
            10, abs(early.differential - late.differential) * 0.3
        )  # Reduced

        # Calculate total confidence
        confidence = (
            base_confidence
            + early_strength_factor
            + late_strength_factor
            + timing_factor
            + source_factor
            + book_factor
            + diff_factor
        )

        # Apply penalties for risk factors
        if early.hours_before_game < 6:
            confidence -= 10  # Early signal not very early

        if late.hours_before_game > 2:
            confidence -= 5  # Late signal not very late

        # ⚠️ CRITICAL: Cap confidence for untested strategies
        # No strategy should claim 100% confidence without backtesting proof
        confidence = min(confidence, MAX_CONFIDENCE_UNTESTED)

        # Ensure confidence is within bounds
        return max(0.0, min(MAX_CONFIDENCE_UNTESTED, confidence))

    def _generate_strategy_recommendation(
        self, early: MarketSignal, late: MarketSignal, flip_type: FlipType
    ) -> tuple[str, str]:
        """Generate betting strategy recommendation."""

        if flip_type == FlipType.CROSS_MARKET_CONTRADICTION:
            # Follow early signal, fade late signal
            strategy = f"BET {early.recommended_team}"
            reasoning = (
                f"Early {early.split_type.value} signal on {early.recommended_team} "
                f"({early.differential:+.1f}%) at {early.hours_before_game:.1f}h before game "
                f"vs late {late.split_type.value} signal on {late.recommended_team} "
                f"({late.differential:+.1f}%) at {late.hours_before_game:.1f}h before. "
                f"Follow early sharp action, fade late flip."
            )

        elif flip_type == FlipType.SAME_MARKET_FLIP:
            strategy = f"BET {early.recommended_team}"
            reasoning = (
                f"Same market flip: {early.split_type.value} moved from "
                f"{early.recommended_team} to {late.recommended_team}. "
                f"Follow early signal, fade late movement."
            )

        elif flip_type == FlipType.WEAK_LATE_CONTRADICTION:
            strategy = f"BET {early.recommended_team}"
            reasoning = (
                f"Strong early {early.split_type.value} signal on {early.recommended_team} "
                f"({early.differential:+.1f}%) contradicted by weak late "
                f"{late.split_type.value} signal ({late.differential:+.1f}%). "
                f"Follow stronger early signal."
            )

        else:
            strategy = "NO BET"
            reasoning = "Unclear flip pattern, avoid betting."

        return strategy, reasoning

    def _identify_risk_factors(
        self, early: MarketSignal, late: MarketSignal
    ) -> list[str]:
        """Identify risk factors for the flip detection."""

        risk_factors = []

        # Late signal strength risks
        if late.strength in [SignalStrength.STRONG, SignalStrength.VERY_STRONG]:
            risk_factors.append(
                f"Strong late signal ({abs(late.differential):.1f}%) may indicate legitimate sharp move"
            )

        # Timing risks
        if early.hours_before_game < 6:
            risk_factors.append("Early signal not very early, may be less reliable")

        if late.hours_before_game > 2:
            risk_factors.append("Late signal not very close to game time")

        # Sample size risks (could be enhanced with actual bet volume data)
        if early.source == late.source and early.book == late.book:
            risk_factors.append("Single source/book - no confirmation from other books")

        # Market efficiency risks
        hours_gap = abs(early.hours_before_game - late.hours_before_game)
        if hours_gap < 2:
            risk_factors.append(
                "Short time gap between signals may indicate normal market movement"
            )

        return risk_factors

    def _get_game_info(self, game_id: str) -> dict[str, Any]:
        """Get basic game information."""

        query = """
        SELECT DISTINCT home_team, away_team, game_datetime
        FROM splits.raw_mlb_betting_splits
        WHERE game_id = %s
        LIMIT 1
        """

        result = self.db_manager.execute_query(query, (game_id,), fetch=True)

        if result:
            return {
                "home_team": result[0][0],
                "away_team": result[0][1],
                "game_datetime": result[0][2],
            }

        return {}

    def _deduplicate_flips_aggressively(
        self, flips: list[FlipDetection]
    ) -> list[FlipDetection]:
        """Remove duplicate flip detections and keep the best ones with aggressive filtering."""

        if not flips:
            return []

        # Group flips by game_id
        flips_by_game = {}
        for flip in flips:
            if flip.game_id not in flips_by_game:
                flips_by_game[flip.game_id] = []
            flips_by_game[flip.game_id].append(flip)

        deduplicated = []

        for game_id, game_flips in flips_by_game.items():
            if not game_flips:
                continue

            # Sort by confidence score descending
            game_flips.sort(key=lambda x: x.confidence_score, reverse=True)

            # Only keep the highest confidence flip per game to avoid conflicting recommendations
            best_flip = game_flips[0]

            # Additional quality checks for the best flip
            if (
                best_flip.confidence_score >= self.min_confidence_threshold
                and abs(best_flip.early_signal.differential) >= self.min_sharp_threshold
                and abs(best_flip.late_signal.differential) >= self.min_sharp_threshold
            ):
                # Ensure there's meaningful difference between early and late signals
                signal_diff = abs(
                    abs(best_flip.early_signal.differential)
                    - abs(best_flip.late_signal.differential)
                )
                if signal_diff >= self.min_signal_strength_diff:
                    deduplicated.append(best_flip)
                else:
                    logger.debug(
                        "Filtered flip due to insufficient signal difference",
                        game_id=game_id,
                        signal_diff=signal_diff,
                        min_required=self.min_signal_strength_diff,
                    )
            else:
                logger.debug(
                    "Filtered flip due to quality checks",
                    game_id=game_id,
                    confidence=best_flip.confidence_score,
                    early_diff=abs(best_flip.early_signal.differential),
                    late_diff=abs(best_flip.late_signal.differential),
                )

        # Sort final results by confidence score
        deduplicated.sort(key=lambda x: x.confidence_score, reverse=True)

        return deduplicated

    def _analyze_same_timing_contradiction(
        self,
        signal1: MarketSignal,
        signal2: MarketSignal,
        game_id: str,
        timing_label: str,
    ) -> FlipDetection | None:
        """
        Analyze two signals from the same timing bucket for contradictions.

        Args:
            signal1: First market signal
            signal2: Second market signal
            game_id: Game identifier
            timing_label: "early" or "late" timing bucket

        Returns:
            FlipDetection if contradiction found, None otherwise
        """
        try:
            # Only analyze different market types
            if signal1.split_type == signal2.split_type:
                return None

            # Check for contradictions between market types
            contradiction_type = self._classify_same_timing_contradiction(
                signal1, signal2
            )
            if not contradiction_type:
                return None

            # Get game info
            game_info = self._get_game_info(game_id)
            if not game_info:
                return None

            # Calculate confidence (lower for same-timing contradictions)
            base_confidence = self._calculate_confidence_conservative(signal1, signal2)
            adjusted_confidence = (
                base_confidence * 0.8
            )  # Reduce confidence for same-timing

            if adjusted_confidence < self.min_confidence_threshold:
                return None

            # Generate strategy recommendation
            recommendation, reasoning = self._generate_same_timing_recommendation(
                signal1, signal2, contradiction_type
            )

            # Identify risk factors
            risk_factors = self._identify_risk_factors(signal1, signal2)
            risk_factors.append(f"Same timing bucket ({timing_label}) contradiction")

            hours_between = abs(signal1.hours_before_game - signal2.hours_before_game)

            return FlipDetection(
                game_id=game_id,
                home_team=game_info["home_team"],
                away_team=game_info["away_team"],
                game_datetime=game_info["game_datetime"],
                flip_type=contradiction_type,
                early_signal=signal1
                if signal1.hours_before_game >= signal2.hours_before_game
                else signal2,
                late_signal=signal2
                if signal1.hours_before_game >= signal2.hours_before_game
                else signal1,
                hours_between_signals=hours_between,
                confidence_score=adjusted_confidence,
                strategy_recommendation=recommendation,
                reasoning=reasoning,
                risk_factors=risk_factors,
            )

        except Exception as e:
            logger.error("Failed to analyze same timing contradiction", error=str(e))
            return None

    def _classify_same_timing_contradiction(
        self, signal1: MarketSignal, signal2: MarketSignal
    ) -> FlipType | None:
        """
        Classify contradictions between signals in the same timing bucket.

        Args:
            signal1: First market signal
            signal2: Second market signal

        Returns:
            FlipType if contradiction exists, None otherwise
        """
        # Get recommended teams for each signal
        team1 = signal1.recommended_team
        team2 = signal2.recommended_team

        # Skip total vs spread/moneyline contradictions for now (complex logic)
        if "OVER" in [team1, team2] or "UNDER" in [team1, team2]:
            return None

        # Check for direct team contradictions (spread vs moneyline recommending different teams)
        if (
            signal1.split_type in ["spread", "moneyline"]
            and signal2.split_type in ["spread", "moneyline"]
            and team1 != team2
        ):
            return FlipType.CROSS_MARKET_CONTRADICTION

        return None

    def _generate_same_timing_recommendation(
        self, signal1: MarketSignal, signal2: MarketSignal, contradiction_type: FlipType
    ) -> tuple[str, str]:
        """
        Generate strategy recommendation for same-timing contradictions.

        Args:
            signal1: First market signal
            signal2: Second market signal
            contradiction_type: Type of contradiction

        Returns:
            Tuple of (recommendation, reasoning)
        """
        # For same-timing contradictions, prefer the stronger signal
        stronger_signal = (
            signal1 if signal1.strength.value > signal2.strength.value else signal2
        )
        weaker_signal = signal2 if stronger_signal == signal1 else signal1

        recommendation = stronger_signal.recommended_team
        reasoning = (
            f"Same-timing contradiction detected: {stronger_signal.split_type} "
            f"recommends {stronger_signal.recommended_team} "
            f"({stronger_signal.differential:+.1f}% differential) vs "
            f"{weaker_signal.split_type} recommends {weaker_signal.recommended_team} "
            f"({weaker_signal.differential:+.1f}% differential). "
            f"Following stronger {stronger_signal.split_type} signal."
        )

        return recommendation, reasoning
