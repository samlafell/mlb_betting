from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class ConfidenceLevel(Enum):
    VERY_LOW = "VERY_LOW"
    LOW = "LOW"
    MODERATE = "MODERATE"
    HIGH = "HIGH"
    VERY_HIGH = "VERY_HIGH"


class SignalType(Enum):
    SHARP_ACTION = "SHARP_ACTION"
    OPPOSING_MARKETS = "OPPOSING_MARKETS"
    STEAM_MOVE = "STEAM_MOVE"
    BOOK_CONFLICTS = "BOOK_CONFLICTS"  # Updated to match processor
    TOTAL_SHARP = "TOTAL_SHARP"
    PUBLIC_FADE = "PUBLIC_FADE"
    LATE_FLIP = "LATE_FLIP"
    CONSENSUS_MONEYLINE = "CONSENSUS_MONEYLINE"  # New - consensus strategies
    UNDERDOG_VALUE = "UNDERDOG_VALUE"  # New - underdog value strategies
    LINE_MOVEMENT = "LINE_MOVEMENT"  # New - line movement strategies
    TIMING_BASED = "TIMING_BASED"  # New - timing-based strategies
    HYBRID_SHARP = "HYBRID_SHARP"  # New - hybrid sharp + line movement
    SIGNAL_COMBINATIONS = "SIGNAL_COMBINATIONS"  # New - multi-signal strategies


@dataclass
class StrategyThresholds:
    """Centralized configuration for strategy thresholds"""

    high_performance_wr: float = 65.0
    high_performance_threshold: float = 20.0
    moderate_performance_wr: float = 60.0
    moderate_performance_threshold: float = 25.0
    low_performance_wr: float = 55.0
    low_performance_threshold: float = 30.0
    minimum_bets: int = 5
    minimum_roi: float = 0.0


@dataclass
class SignalProcessorConfig:
    """Configuration for signal processors"""

    minimum_differential: float = (
        0.1  # ✅ FIX: Lowered from 1.0 to capture weak signals
    )
    maximum_differential: float = 80.0
    data_freshness_hours: int = 2
    steam_move_time_window_hours: int = 4
    book_conflict_minimum_strength: float = 5.0


@dataclass
class BettingSignal:
    """Individual betting signal with all metadata"""

    signal_type: SignalType
    home_team: str
    away_team: str
    game_time: datetime
    minutes_to_game: int
    split_type: str
    split_value: str | None
    source: str
    book: str | None
    differential: float
    signal_strength: float
    confidence_score: float
    confidence_level: ConfidenceLevel
    confidence_explanation: str
    recommendation: str
    recommendation_strength: str
    last_updated: datetime
    strategy_name: str
    win_rate: float
    roi: float
    total_bets: int
    # Signal-specific metadata
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def game_id(self) -> str:
        """Generate a game_id from home_team, away_team, and game_time"""
        game_date = self.game_time.strftime("%Y-%m-%d")
        return f"{self.away_team}@{self.home_team}_{game_date}"

    @property
    def recommended_bet(self) -> str:
        """Extract the recommended bet from the recommendation string"""
        return self.recommendation


@dataclass
class GameAnalysis:
    """Analysis results for a single game"""

    home_team: str
    away_team: str
    game_time: datetime
    minutes_to_game: int
    sharp_signals: list[BettingSignal] = field(default_factory=list)
    opposing_markets: list[BettingSignal] = field(default_factory=list)
    steam_moves: list[BettingSignal] = field(default_factory=list)
    book_conflicts: list[BettingSignal] = field(default_factory=list)

    @property
    def all_signals(self) -> list[BettingSignal]:
        """Get all signals for this game"""
        return (
            self.sharp_signals
            + self.opposing_markets
            + self.steam_moves
            + self.book_conflicts
        )

    @property
    def total_opportunities(self) -> int:
        """Total number of betting opportunities for this game"""
        return len(self.all_signals)

    @property
    def highest_confidence_signal(self) -> BettingSignal | None:
        """Get the signal with the highest confidence score"""
        if not self.all_signals:
            return None
        return max(self.all_signals, key=lambda s: s.confidence_score)


@dataclass
class StrategyPerformance:
    """Strategy performance metadata"""

    strategy_name: str
    source_book: str
    split_type: str
    win_rate: float
    roi: float
    total_bets: int
    confidence_level: str
    ci_lower: float
    ci_upper: float


@dataclass
class BettingAnalysisResult:
    """Complete analysis result with all data and metadata"""

    games: dict[tuple, GameAnalysis]  # Key: (away_team, home_team, game_time)
    strategy_performance: list[StrategyPerformance]
    analysis_metadata: dict[str, Any]
    generated_at: datetime = field(default_factory=datetime.now)

    @property
    def total_games(self) -> int:
        """Total number of games analyzed"""
        return len(self.games)

    @property
    def total_opportunities(self) -> int:
        """Total number of betting opportunities found"""
        return sum(game.total_opportunities for game in self.games.values())

    @property
    def opportunities_by_type(self) -> dict[SignalType, int]:
        """Count of opportunities by signal type"""
        counts = dict.fromkeys(SignalType, 0)
        for game in self.games.values():
            for signal in game.all_signals:
                counts[signal.signal_type] += 1
        return counts

    @property
    def games_by_confidence(self) -> dict[ConfidenceLevel, int]:
        """Count of games by highest confidence level"""
        counts = dict.fromkeys(ConfidenceLevel, 0)
        for game in self.games.values():
            highest_signal = game.highest_confidence_signal
            if highest_signal:
                counts[highest_signal.confidence_level] += 1
        return counts


@dataclass
class ProfitableStrategy:
    """Simplified strategy data for internal use"""

    strategy_name: str
    source_book: str
    split_type: str
    win_rate: float
    roi: float
    total_bets: int
    confidence: str
    ci_lower: float = 0.0
    ci_upper: float = 100.0
    confidence_score: float = 0.5  # Default confidence score as float
