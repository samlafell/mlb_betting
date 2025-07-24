#!/usr/bin/env python3
"""
Advanced Analytics and Pattern Detection for Unified Betting Lines

Comprehensive analytics system for detecting patterns, anomalies, and trends
in betting line data across multiple sources.
"""

import asyncio
import json
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

import psycopg2
import structlog
from psycopg2.extras import RealDictCursor

from ...core.config import UnifiedSettings

logger = structlog.get_logger(__name__)


class PatternType(Enum):
    """Types of patterns that can be detected."""

    SHARP_ACTION = "SHARP_ACTION"
    REVERSE_LINE_MOVEMENT = "REVERSE_LINE_MOVEMENT"
    STEAM_MOVE = "STEAM_MOVE"
    ARBITRAGE_OPPORTUNITY = "ARBITRAGE_OPPORTUNITY"
    LINE_DIVERGENCE = "LINE_DIVERGENCE"
    VOLUME_ANOMALY = "VOLUME_ANOMALY"
    CONSENSUS_FADE = "CONSENSUS_FADE"


class PatternSeverity(Enum):
    """Severity levels for detected patterns."""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass
class DetectedPattern:
    """Detected pattern information."""

    pattern_type: PatternType
    severity: PatternSeverity
    confidence: float
    game_id: int
    sportsbook: str
    bet_type: str
    description: str
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict[str, Any]:
        return {
            "pattern_type": self.pattern_type.value,
            "severity": self.severity.value,
            "confidence": self.confidence,
            "game_id": self.game_id,
            "sportsbook": self.sportsbook,
            "bet_type": self.bet_type,
            "description": self.description,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class LineMovement:
    """Line movement information."""

    game_id: int
    sportsbook: str
    bet_type: str
    initial_line: float
    current_line: float
    movement: float
    movement_percentage: float
    timestamp: datetime

    @property
    def is_significant(self) -> bool:
        """Check if movement is significant."""
        return abs(self.movement_percentage) > 5.0  # 5% threshold


@dataclass
class MarketAnalysis:
    """Market analysis result."""

    game_id: int
    bet_type: str
    market_consensus: dict[str, float]
    line_spread: float
    volume_distribution: dict[str, float]
    sharp_indicators: list[str]
    public_sentiment: str
    market_efficiency: float
    timestamp: datetime = field(default_factory=datetime.now)


class SharpActionDetector:
    """Detect sharp action patterns in betting data."""

    def __init__(self, settings: UnifiedSettings):
        self.settings = settings
        self.logger = logger.bind(component="SharpActionDetector")

    async def detect_sharp_action(
        self, lookback_hours: int = 24
    ) -> list[DetectedPattern]:
        """Detect sharp action patterns in recent data."""
        patterns = []

        try:
            with psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor,
            ) as conn:
                with conn.cursor() as cur:
                    # Detect sharp action in moneyline
                    cur.execute(
                        """
                        SELECT 
                            game_id,
                            sportsbook,
                            home_money_percentage,
                            away_money_percentage,
                            home_bets_percentage,
                            away_bets_percentage,
                            home_ml,
                            away_ml,
                            odds_timestamp
                        FROM core_betting.betting_lines_moneyline
                        WHERE created_at >= NOW() - INTERVAL '%s hours'
                        AND home_money_percentage IS NOT NULL
                        AND away_money_percentage IS NOT NULL
                        AND home_bets_percentage IS NOT NULL
                        AND away_bets_percentage IS NOT NULL
                        ORDER BY game_id, sportsbook, odds_timestamp
                    """,
                        (lookback_hours,),
                    )

                    rows = cur.fetchall()

                    for row in rows:
                        # Check for sharp action indicators
                        home_money_pct = row["home_money_percentage"]
                        away_money_pct = row["away_money_percentage"]
                        home_bets_pct = row["home_bets_percentage"]
                        away_bets_pct = row["away_bets_percentage"]

                        # Sharp action: money percentage significantly higher than bet percentage
                        home_sharp_differential = home_money_pct - home_bets_pct
                        away_sharp_differential = away_money_pct - away_bets_pct

                        # Detect home sharp action
                        if home_sharp_differential > 15:
                            severity = (
                                PatternSeverity.HIGH
                                if home_sharp_differential > 25
                                else PatternSeverity.MEDIUM
                            )
                            confidence = min(
                                0.95, (home_sharp_differential / 30) * 0.8 + 0.2
                            )

                            patterns.append(
                                DetectedPattern(
                                    pattern_type=PatternType.SHARP_ACTION,
                                    severity=severity,
                                    confidence=confidence,
                                    game_id=row["game_id"],
                                    sportsbook=row["sportsbook"],
                                    bet_type="moneyline",
                                    description=f"Sharp action on home team: {home_sharp_differential:.1f}% money vs bets differential",
                                    metadata={
                                        "side": "home",
                                        "money_percentage": home_money_pct,
                                        "bets_percentage": home_bets_pct,
                                        "differential": home_sharp_differential,
                                        "odds": row["home_ml"],
                                    },
                                )
                            )

                        # Detect away sharp action
                        if away_sharp_differential > 15:
                            severity = (
                                PatternSeverity.HIGH
                                if away_sharp_differential > 25
                                else PatternSeverity.MEDIUM
                            )
                            confidence = min(
                                0.95, (away_sharp_differential / 30) * 0.8 + 0.2
                            )

                            patterns.append(
                                DetectedPattern(
                                    pattern_type=PatternType.SHARP_ACTION,
                                    severity=severity,
                                    confidence=confidence,
                                    game_id=row["game_id"],
                                    sportsbook=row["sportsbook"],
                                    bet_type="moneyline",
                                    description=f"Sharp action on away team: {away_sharp_differential:.1f}% money vs bets differential",
                                    metadata={
                                        "side": "away",
                                        "money_percentage": away_money_pct,
                                        "bets_percentage": away_bets_pct,
                                        "differential": away_sharp_differential,
                                        "odds": row["away_ml"],
                                    },
                                )
                            )

        except Exception as e:
            self.logger.error("Error detecting sharp action", error=str(e))

        return patterns


class LineMovementAnalyzer:
    """Analyze line movements and detect patterns."""

    def __init__(self, settings: UnifiedSettings):
        self.settings = settings
        self.logger = logger.bind(component="LineMovementAnalyzer")

    async def analyze_line_movements(
        self, game_id: int, bet_type: str = "moneyline"
    ) -> list[LineMovement]:
        """Analyze line movements for a specific game."""
        movements = []

        try:
            with psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor,
            ) as conn:
                with conn.cursor() as cur:
                    if bet_type == "moneyline":
                        cur.execute(
                            """
                            SELECT 
                                sportsbook,
                                home_ml,
                                away_ml,
                                opening_home_ml,
                                opening_away_ml,
                                odds_timestamp
                            FROM core_betting.betting_lines_moneyline
                            WHERE game_id = %s
                            AND home_ml IS NOT NULL
                            AND away_ml IS NOT NULL
                            AND opening_home_ml IS NOT NULL
                            AND opening_away_ml IS NOT NULL
                            ORDER BY sportsbook, odds_timestamp
                        """,
                            (game_id,),
                        )

                        rows = cur.fetchall()

                        for row in rows:
                            # Calculate home line movement
                            home_movement = row["home_ml"] - row["opening_home_ml"]
                            home_movement_pct = (
                                home_movement / abs(row["opening_home_ml"])
                            ) * 100

                            movements.append(
                                LineMovement(
                                    game_id=game_id,
                                    sportsbook=row["sportsbook"],
                                    bet_type="moneyline_home",
                                    initial_line=row["opening_home_ml"],
                                    current_line=row["home_ml"],
                                    movement=home_movement,
                                    movement_percentage=home_movement_pct,
                                    timestamp=row["odds_timestamp"],
                                )
                            )

                            # Calculate away line movement
                            away_movement = row["away_ml"] - row["opening_away_ml"]
                            away_movement_pct = (
                                away_movement / abs(row["opening_away_ml"])
                            ) * 100

                            movements.append(
                                LineMovement(
                                    game_id=game_id,
                                    sportsbook=row["sportsbook"],
                                    bet_type="moneyline_away",
                                    initial_line=row["opening_away_ml"],
                                    current_line=row["away_ml"],
                                    movement=away_movement,
                                    movement_percentage=away_movement_pct,
                                    timestamp=row["odds_timestamp"],
                                )
                            )

        except Exception as e:
            self.logger.error(
                "Error analyzing line movements", game_id=game_id, error=str(e)
            )

        return movements

    async def detect_reverse_line_movement(
        self, lookback_hours: int = 6
    ) -> list[DetectedPattern]:
        """Detect reverse line movement patterns."""
        patterns = []

        try:
            with psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor,
            ) as conn:
                with conn.cursor() as cur:
                    # Get games with line movement and betting percentages
                    cur.execute(
                        """
                        SELECT 
                            ml.game_id,
                            ml.sportsbook,
                            ml.home_ml,
                            ml.away_ml,
                            ml.opening_home_ml,
                            ml.opening_away_ml,
                            ml.home_bets_percentage,
                            ml.away_bets_percentage,
                            ml.home_money_percentage,
                            ml.away_money_percentage,
                            g.home_team,
                            g.away_team
                        FROM core_betting.betting_lines_moneyline ml
                        JOIN core_betting.games g ON ml.game_id = g.id
                        WHERE ml.created_at >= NOW() - INTERVAL '%s hours'
                        AND ml.opening_home_ml IS NOT NULL
                        AND ml.opening_away_ml IS NOT NULL
                        AND ml.home_bets_percentage IS NOT NULL
                        AND ml.away_bets_percentage IS NOT NULL
                        ORDER BY ml.game_id, ml.sportsbook
                    """,
                        (lookback_hours,),
                    )

                    rows = cur.fetchall()

                    for row in rows:
                        # Calculate line movements
                        home_line_movement = row["home_ml"] - row["opening_home_ml"]
                        away_line_movement = row["away_ml"] - row["opening_away_ml"]

                        # Check for reverse line movement
                        # Home team gets more bets but line moves against them
                        if row["home_bets_percentage"] > 60 and home_line_movement > 0:
                            confidence = min(
                                0.9, (row["home_bets_percentage"] - 60) / 40 * 0.6 + 0.3
                            )

                            patterns.append(
                                DetectedPattern(
                                    pattern_type=PatternType.REVERSE_LINE_MOVEMENT,
                                    severity=PatternSeverity.HIGH
                                    if row["home_bets_percentage"] > 75
                                    else PatternSeverity.MEDIUM,
                                    confidence=confidence,
                                    game_id=row["game_id"],
                                    sportsbook=row["sportsbook"],
                                    bet_type="moneyline",
                                    description=f"Reverse line movement: {row['home_bets_percentage']:.1f}% bets on home team but line moved against them",
                                    metadata={
                                        "side": "home",
                                        "team": row["home_team"],
                                        "bets_percentage": row["home_bets_percentage"],
                                        "line_movement": home_line_movement,
                                        "opening_line": row["opening_home_ml"],
                                        "current_line": row["home_ml"],
                                    },
                                )
                            )

                        # Away team gets more bets but line moves against them
                        if row["away_bets_percentage"] > 60 and away_line_movement > 0:
                            confidence = min(
                                0.9, (row["away_bets_percentage"] - 60) / 40 * 0.6 + 0.3
                            )

                            patterns.append(
                                DetectedPattern(
                                    pattern_type=PatternType.REVERSE_LINE_MOVEMENT,
                                    severity=PatternSeverity.HIGH
                                    if row["away_bets_percentage"] > 75
                                    else PatternSeverity.MEDIUM,
                                    confidence=confidence,
                                    game_id=row["game_id"],
                                    sportsbook=row["sportsbook"],
                                    bet_type="moneyline",
                                    description=f"Reverse line movement: {row['away_bets_percentage']:.1f}% bets on away team but line moved against them",
                                    metadata={
                                        "side": "away",
                                        "team": row["away_team"],
                                        "bets_percentage": row["away_bets_percentage"],
                                        "line_movement": away_line_movement,
                                        "opening_line": row["opening_away_ml"],
                                        "current_line": row["away_ml"],
                                    },
                                )
                            )

        except Exception as e:
            self.logger.error("Error detecting reverse line movement", error=str(e))

        return patterns


class ArbitrageDetector:
    """Detect arbitrage opportunities across sportsbooks."""

    def __init__(self, settings: UnifiedSettings):
        self.settings = settings
        self.logger = logger.bind(component="ArbitrageDetector")

    async def detect_arbitrage_opportunities(
        self, min_profit_percentage: float = 1.0
    ) -> list[DetectedPattern]:
        """Detect arbitrage opportunities across sportsbooks."""
        patterns = []

        try:
            with psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor,
            ) as conn:
                with conn.cursor() as cur:
                    # Get current lines grouped by game
                    cur.execute("""
                        SELECT 
                            game_id,
                            sportsbook,
                            home_ml,
                            away_ml,
                            odds_timestamp,
                            ROW_NUMBER() OVER (PARTITION BY game_id, sportsbook ORDER BY odds_timestamp DESC) as rn
                        FROM core_betting.betting_lines_moneyline
                        WHERE created_at >= NOW() - INTERVAL '2 hours'
                        AND home_ml IS NOT NULL
                        AND away_ml IS NOT NULL
                    """)

                    rows = cur.fetchall()

                    # Group by game_id and get latest odds
                    game_odds = defaultdict(list)
                    for row in rows:
                        if row["rn"] == 1:  # Latest odds only
                            game_odds[row["game_id"]].append(
                                {
                                    "sportsbook": row["sportsbook"],
                                    "home_ml": row["home_ml"],
                                    "away_ml": row["away_ml"],
                                    "timestamp": row["odds_timestamp"],
                                }
                            )

                    # Check for arbitrage opportunities
                    for game_id, odds_list in game_odds.items():
                        if len(odds_list) < 2:
                            continue

                        # Find best home and away odds
                        best_home_odds = max(
                            odds_list,
                            key=lambda x: x["home_ml"]
                            if x["home_ml"] > 0
                            else 1 / abs(x["home_ml"]),
                        )
                        best_away_odds = max(
                            odds_list,
                            key=lambda x: x["away_ml"]
                            if x["away_ml"] > 0
                            else 1 / abs(x["away_ml"]),
                        )

                        # Calculate implied probabilities
                        home_prob = self._calculate_implied_probability(
                            best_home_odds["home_ml"]
                        )
                        away_prob = self._calculate_implied_probability(
                            best_away_odds["away_ml"]
                        )

                        total_prob = home_prob + away_prob

                        # Check for arbitrage (total probability < 1)
                        if total_prob < 1.0:
                            profit_percentage = ((1 / total_prob) - 1) * 100

                            if profit_percentage >= min_profit_percentage:
                                patterns.append(
                                    DetectedPattern(
                                        pattern_type=PatternType.ARBITRAGE_OPPORTUNITY,
                                        severity=PatternSeverity.CRITICAL
                                        if profit_percentage > 5
                                        else PatternSeverity.HIGH,
                                        confidence=0.95,  # High confidence for arbitrage
                                        game_id=game_id,
                                        sportsbook=f"{best_home_odds['sportsbook']} / {best_away_odds['sportsbook']}",
                                        bet_type="moneyline",
                                        description=f"Arbitrage opportunity: {profit_percentage:.2f}% profit potential",
                                        metadata={
                                            "profit_percentage": profit_percentage,
                                            "home_sportsbook": best_home_odds[
                                                "sportsbook"
                                            ],
                                            "away_sportsbook": best_away_odds[
                                                "sportsbook"
                                            ],
                                            "home_odds": best_home_odds["home_ml"],
                                            "away_odds": best_away_odds["away_ml"],
                                            "home_prob": home_prob,
                                            "away_prob": away_prob,
                                            "total_prob": total_prob,
                                        },
                                    )
                                )

        except Exception as e:
            self.logger.error("Error detecting arbitrage opportunities", error=str(e))

        return patterns

    def _calculate_implied_probability(self, odds: int) -> float:
        """Calculate implied probability from American odds."""
        if odds > 0:
            return 100 / (odds + 100)
        else:
            return abs(odds) / (abs(odds) + 100)


class MarketAnalyzer:
    """Analyze market conditions and efficiency."""

    def __init__(self, settings: UnifiedSettings):
        self.settings = settings
        self.logger = logger.bind(component="MarketAnalyzer")

    async def analyze_market_conditions(
        self, game_id: int, bet_type: str = "moneyline"
    ) -> MarketAnalysis:
        """Analyze market conditions for a specific game."""
        try:
            with psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor,
            ) as conn:
                with conn.cursor() as cur:
                    if bet_type == "moneyline":
                        cur.execute(
                            """
                            SELECT 
                                sportsbook,
                                home_ml,
                                away_ml,
                                home_bets_percentage,
                                away_bets_percentage,
                                home_money_percentage,
                                away_money_percentage,
                                sharp_action,
                                data_quality
                            FROM core_betting.betting_lines_moneyline
                            WHERE game_id = %s
                            AND home_ml IS NOT NULL
                            AND away_ml IS NOT NULL
                            ORDER BY odds_timestamp DESC
                        """,
                            (game_id,),
                        )

                        rows = cur.fetchall()

                        if not rows:
                            return MarketAnalysis(
                                game_id=game_id,
                                bet_type=bet_type,
                                market_consensus={},
                                line_spread=0.0,
                                volume_distribution={},
                                sharp_indicators=[],
                                public_sentiment="UNKNOWN",
                                market_efficiency=0.0,
                            )

                        # Calculate market consensus
                        home_odds = [row["home_ml"] for row in rows]
                        away_odds = [row["away_ml"] for row in rows]

                        market_consensus = {
                            "home_odds_avg": statistics.mean(home_odds),
                            "away_odds_avg": statistics.mean(away_odds),
                            "home_odds_std": statistics.stdev(home_odds)
                            if len(home_odds) > 1
                            else 0,
                            "away_odds_std": statistics.stdev(away_odds)
                            if len(away_odds) > 1
                            else 0,
                        }

                        # Calculate line spread
                        line_spread = (
                            max(home_odds)
                            - min(home_odds)
                            + max(away_odds)
                            - min(away_odds)
                        ) / 2

                        # Analyze volume distribution
                        volume_distribution = {}
                        public_sentiment = "NEUTRAL"

                        # Get betting percentages
                        home_bets = [
                            row["home_bets_percentage"]
                            for row in rows
                            if row["home_bets_percentage"] is not None
                        ]
                        away_bets = [
                            row["away_bets_percentage"]
                            for row in rows
                            if row["away_bets_percentage"] is not None
                        ]

                        if home_bets and away_bets:
                            avg_home_bets = statistics.mean(home_bets)
                            avg_away_bets = statistics.mean(away_bets)

                            volume_distribution = {
                                "home_bets_avg": avg_home_bets,
                                "away_bets_avg": avg_away_bets,
                            }

                            if avg_home_bets > 65:
                                public_sentiment = "HOME_HEAVY"
                            elif avg_away_bets > 65:
                                public_sentiment = "AWAY_HEAVY"

                        # Identify sharp indicators
                        sharp_indicators = []
                        for row in rows:
                            if row["sharp_action"]:
                                sharp_indicators.append(
                                    f"{row['sportsbook']}: {row['sharp_action']}"
                                )

                        # Calculate market efficiency (lower spread = more efficient)
                        market_efficiency = max(0, 1 - (line_spread / 100))

                        return MarketAnalysis(
                            game_id=game_id,
                            bet_type=bet_type,
                            market_consensus=market_consensus,
                            line_spread=line_spread,
                            volume_distribution=volume_distribution,
                            sharp_indicators=sharp_indicators,
                            public_sentiment=public_sentiment,
                            market_efficiency=market_efficiency,
                        )

        except Exception as e:
            self.logger.error(
                "Error analyzing market conditions", game_id=game_id, error=str(e)
            )
            return MarketAnalysis(
                game_id=game_id,
                bet_type=bet_type,
                market_consensus={},
                line_spread=0.0,
                volume_distribution={},
                sharp_indicators=[],
                public_sentiment="ERROR",
                market_efficiency=0.0,
            )


class UnifiedAnalyticsSystem:
    """Comprehensive analytics system orchestrator."""

    def __init__(self, settings: UnifiedSettings):
        self.settings = settings
        self.logger = logger.bind(component="UnifiedAnalyticsSystem")

        # Initialize analyzers
        self.sharp_action_detector = SharpActionDetector(settings)
        self.line_movement_analyzer = LineMovementAnalyzer(settings)
        self.arbitrage_detector = ArbitrageDetector(settings)
        self.market_analyzer = MarketAnalyzer(settings)

    async def run_comprehensive_analysis(
        self, lookback_hours: int = 6
    ) -> dict[str, Any]:
        """Run comprehensive analysis across all detectors."""
        try:
            # Detect patterns
            sharp_action_patterns = (
                await self.sharp_action_detector.detect_sharp_action(lookback_hours)
            )
            rlm_patterns = (
                await self.line_movement_analyzer.detect_reverse_line_movement(
                    lookback_hours
                )
            )
            arbitrage_patterns = (
                await self.arbitrage_detector.detect_arbitrage_opportunities()
            )

            # Combine all patterns
            all_patterns = sharp_action_patterns + rlm_patterns + arbitrage_patterns

            # Group patterns by type
            patterns_by_type = defaultdict(list)
            for pattern in all_patterns:
                patterns_by_type[pattern.pattern_type.value].append(pattern.to_dict())

            # Calculate summary statistics
            summary = {
                "total_patterns": len(all_patterns),
                "critical_patterns": len(
                    [p for p in all_patterns if p.severity == PatternSeverity.CRITICAL]
                ),
                "high_severity_patterns": len(
                    [p for p in all_patterns if p.severity == PatternSeverity.HIGH]
                ),
                "medium_severity_patterns": len(
                    [p for p in all_patterns if p.severity == PatternSeverity.MEDIUM]
                ),
                "low_severity_patterns": len(
                    [p for p in all_patterns if p.severity == PatternSeverity.LOW]
                ),
                "avg_confidence": statistics.mean([p.confidence for p in all_patterns])
                if all_patterns
                else 0,
            }

            self.logger.info(
                "Comprehensive analysis completed",
                total_patterns=summary["total_patterns"],
                critical=summary["critical_patterns"],
                high=summary["high_severity_patterns"],
            )

            return {
                "summary": summary,
                "patterns_by_type": dict(patterns_by_type),
                "all_patterns": [p.to_dict() for p in all_patterns],
                "timestamp": datetime.now().isoformat(),
            }

        except Exception as e:
            self.logger.error("Error running comprehensive analysis", error=str(e))
            return {}

    async def analyze_specific_game(self, game_id: int) -> dict[str, Any]:
        """Analyze a specific game comprehensively."""
        try:
            # Analyze line movements
            line_movements = await self.line_movement_analyzer.analyze_line_movements(
                game_id
            )

            # Analyze market conditions
            market_analysis = await self.market_analyzer.analyze_market_conditions(
                game_id
            )

            # Get significant movements
            significant_movements = [lm for lm in line_movements if lm.is_significant]

            result = {
                "game_id": game_id,
                "line_movements": [
                    {
                        "sportsbook": lm.sportsbook,
                        "bet_type": lm.bet_type,
                        "initial_line": lm.initial_line,
                        "current_line": lm.current_line,
                        "movement": lm.movement,
                        "movement_percentage": lm.movement_percentage,
                        "is_significant": lm.is_significant,
                    }
                    for lm in line_movements
                ],
                "significant_movements": len(significant_movements),
                "market_analysis": {
                    "consensus": market_analysis.market_consensus,
                    "line_spread": market_analysis.line_spread,
                    "volume_distribution": market_analysis.volume_distribution,
                    "sharp_indicators": market_analysis.sharp_indicators,
                    "public_sentiment": market_analysis.public_sentiment,
                    "market_efficiency": market_analysis.market_efficiency,
                },
                "timestamp": datetime.now().isoformat(),
            }

            self.logger.info(
                "Game analysis completed",
                game_id=game_id,
                significant_movements=len(significant_movements),
                sharp_indicators=len(market_analysis.sharp_indicators),
            )

            return result

        except Exception as e:
            self.logger.error("Error analyzing game", game_id=game_id, error=str(e))
            return {}

    async def get_analytics_summary(self) -> dict[str, Any]:
        """Get analytics summary for dashboard."""
        try:
            with psycopg2.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                database=self.settings.database.database,
                user=self.settings.database.user,
                password=self.settings.database.password,
                cursor_factory=RealDictCursor,
            ) as conn:
                with conn.cursor() as cur:
                    # Get recent activity
                    cur.execute("""
                        SELECT 
                            COUNT(*) as total_games,
                            COUNT(DISTINCT sportsbook) as active_sportsbooks,
                            COUNT(CASE WHEN sharp_action IS NOT NULL THEN 1 END) as sharp_action_games,
                            COUNT(CASE WHEN reverse_line_movement = true THEN 1 END) as rlm_games,
                            AVG(data_completeness_score) as avg_data_quality
                        FROM core_betting.betting_lines_moneyline
                        WHERE created_at >= NOW() - INTERVAL '24 hours'
                    """)

                    stats = cur.fetchone()

                    return {
                        "total_games_24h": stats["total_games"],
                        "active_sportsbooks": stats["active_sportsbooks"],
                        "sharp_action_games": stats["sharp_action_games"],
                        "rlm_games": stats["rlm_games"],
                        "avg_data_quality": float(stats["avg_data_quality"] or 0),
                        "last_updated": datetime.now().isoformat(),
                    }

        except Exception as e:
            self.logger.error("Error getting analytics summary", error=str(e))
            return {}


# Example usage
if __name__ == "__main__":

    async def main():
        settings = UnifiedSettings()
        analytics_system = UnifiedAnalyticsSystem(settings)

        # Run comprehensive analysis
        result = await analytics_system.run_comprehensive_analysis()
        print(json.dumps(result, indent=2))

        # Get analytics summary
        summary = await analytics_system.get_analytics_summary()
        print(json.dumps(summary, indent=2))

    asyncio.run(main())
