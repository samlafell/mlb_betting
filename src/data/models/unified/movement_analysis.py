"""
Enhanced data models for detailed line movement analysis and betting intelligence.
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, field_validator, ValidationInfo, ConfigDict


class MovementDirection(str, Enum):
    """Direction of line movement."""

    UP = "up"
    DOWN = "down"
    STABLE = "stable"


class MarketType(str, Enum):
    """Types of betting markets."""

    MONEYLINE = "moneyline"
    SPREAD = "spread"
    TOTAL = "total"


class MovementMagnitude(str, Enum):
    """Magnitude of line movement."""

    MINOR = "minor"  # < 5 cents for ML, < 0.5 points for spread/total
    MODERATE = "moderate"  # 5-15 cents for ML, 0.5-1.0 points for spread/total
    SIGNIFICANT = "significant"  # 15-25 cents for ML, 1.0-2.0 points for spread/total
    MAJOR = "major"  # > 25 cents for ML, > 2.0 points for spread/total


class BettingPercentageSnapshot(BaseModel):
    """Point-in-time snapshot of betting percentages."""

    timestamp: datetime
    sportsbook_id: str
    market_type: MarketType
    tickets_percent: int | None = None
    money_percent: int | None = None
    tickets_count: int | None = None
    money_amount: Decimal | None = None

    model_config = ConfigDict(use_enum_values=True)


class LineMovementDetail(BaseModel):
    """Detailed information about a specific line movement."""

    timestamp: datetime
    sportsbook_id: str
    market_type: MarketType
    previous_value: int | Decimal | None = None
    new_value: int | Decimal | None = None
    previous_odds: int | None = None
    new_odds: int | None = None
    direction: MovementDirection
    magnitude: MovementMagnitude
    movement_amount: Decimal | None = None  # Absolute change
    movement_percentage: Decimal | None = None  # Percentage change

    model_config = ConfigDict(use_enum_values=True)

    @field_validator("magnitude")
    @classmethod
    def determine_magnitude(cls, v, info: ValidationInfo):
        """Automatically determine movement magnitude based on market type and change."""
        if v is not None:
            return v

        if not info.data:
            return v
        market_type = info.data.get("market_type")
        prev_odds = info.data.get("previous_odds")
        new_odds = info.data.get("new_odds")
        prev_value = info.data.get("previous_value")
        new_value = info.data.get("new_value")

        if market_type == MarketType.MONEYLINE and prev_odds and new_odds:
            change = abs(new_odds - prev_odds)
            if change < 5:
                return MovementMagnitude.MINOR
            elif change < 15:
                return MovementMagnitude.MODERATE
            elif change < 25:
                return MovementMagnitude.SIGNIFICANT
            else:
                return MovementMagnitude.MAJOR

        elif (
            market_type in [MarketType.SPREAD, MarketType.TOTAL]
            and prev_value
            and new_value
        ):
            change = abs(float(new_value) - float(prev_value))
            if change < 0.5:
                return MovementMagnitude.MINOR
            elif change < 1.0:
                return MovementMagnitude.MODERATE
            elif change < 2.0:
                return MovementMagnitude.SIGNIFICANT
            else:
                return MovementMagnitude.MAJOR

        return MovementMagnitude.MINOR


class RLMIndicator(BaseModel):
    """Reverse Line Movement indicator."""

    market_type: MarketType
    sportsbook_id: str
    line_direction: MovementDirection
    public_betting_direction: MovementDirection
    is_rlm: bool = False
    rlm_strength: str | None = None  # "weak", "moderate", "strong"
    public_percentage: int | None = None
    line_movement_amount: Decimal | None = None
    confidence_score: Decimal | None = None

    model_config = ConfigDict(use_enum_values=True)

    @field_validator("is_rlm")
    @classmethod
    def determine_rlm(cls, v, info: ValidationInfo):
        """Determine if this is reverse line movement."""
        if not info.data:
            return v
        line_dir = info.data.get("line_direction")
        public_dir = info.data.get("public_betting_direction")

        if line_dir and public_dir:
            return line_dir != public_dir and line_dir != MovementDirection.STABLE
        return False

    @field_validator("rlm_strength")
    @classmethod
    def determine_rlm_strength(cls, v, info: ValidationInfo):
        """Determine RLM strength based on public percentage and line movement."""
        if not info.data:
            return v
        if not info.data.get("is_rlm"):
            return None

        public_pct = info.data.get("public_percentage", 50)
        line_amount = info.data.get("line_movement_amount", 0)

        # Strong RLM: High public percentage but line moves opposite significantly
        if public_pct > 70 and line_amount > 1.0:
            return "strong"
        elif public_pct > 60 and line_amount > 0.5:
            return "moderate"
        elif public_pct > 55:
            return "weak"
        return None


class CrossBookMovement(BaseModel):
    """Analysis of movement across multiple sportsbooks."""

    market_type: MarketType
    timestamp: datetime
    participating_books: list[str]
    consensus_direction: MovementDirection | None = None
    consensus_strength: str | None = None  # "weak", "moderate", "strong"
    divergent_books: list[str] = []
    steam_move_detected: bool = False
    average_movement: Decimal | None = None
    movement_range: dict[str, Decimal] | None = None

    model_config = ConfigDict(use_enum_values=True)

    @field_validator("consensus_direction")
    @classmethod
    def determine_consensus(cls, v, info: ValidationInfo):
        """Determine consensus direction based on participating books."""
        # This would be calculated based on the actual movement data
        # For now, return the provided value
        return v

    @field_validator("steam_move_detected")
    @classmethod
    def detect_steam_move(cls, v, info: ValidationInfo):
        """Detect if this is a steam move (rapid movement across multiple books)."""
        if not info.data:
            return v
        participating = info.data.get("participating_books", [])
        consensus = info.data.get("consensus_direction")

        # Steam move: 3+ books moving in same direction with significant movement
        if len(participating) >= 3 and consensus != MovementDirection.STABLE:
            return True
        return False


class MarketMovementSummary(BaseModel):
    """Summary of movements for a specific market type."""

    market_type: MarketType
    total_movements: int
    significant_movements: int
    major_movements: int
    dominant_direction: MovementDirection | None = None
    average_magnitude: str | None = None
    rlm_count: int = 0
    steam_moves: int = 0
    cross_book_consensus: float | None = (
        None  # Percentage of books moving same direction
    )

    model_config = ConfigDict(use_enum_values=True)


class GameMovementAnalysis(BaseModel):
    """Comprehensive movement analysis for a single game."""

    game_id: int
    home_team: str
    away_team: str
    game_datetime: datetime
    analysis_timestamp: datetime

    # Market summaries
    moneyline_summary: MarketMovementSummary | None = None
    spread_summary: MarketMovementSummary | None = None
    total_summary: MarketMovementSummary | None = None

    # Detailed movements
    line_movements: list[LineMovementDetail] = []
    rlm_indicators: list[RLMIndicator] = []
    cross_book_movements: list[CrossBookMovement] = []
    betting_snapshots: list[BettingPercentageSnapshot] = []

    # Overall analysis
    total_movements: int = 0
    sharp_money_indicators: list[str] = []
    arbitrage_opportunities: list[dict] = []
    recommended_actions: list[str] = []

    model_config = ConfigDict(use_enum_values=True)

    @field_validator("total_movements")
    @classmethod
    def calculate_total_movements(cls, v, info: ValidationInfo):
        """Calculate total movements from detailed movements."""
        if not info.data:
            return v
        movements = info.data.get("line_movements", [])
        return len(movements)


class MovementAnalysisReport(BaseModel):
    """Comprehensive report of movement analysis across multiple games."""

    analysis_timestamp: datetime
    total_games_analyzed: int
    games_with_rlm: int
    games_with_steam_moves: int
    games_with_arbitrage: int

    # Game analyses
    game_analyses: list[GameMovementAnalysis] = []

    # Summary statistics
    total_movements: int = 0
    total_rlm_indicators: int = 0
    total_steam_moves: int = 0

    # Top opportunities
    top_rlm_opportunities: list[dict] = []
    top_steam_moves: list[dict] = []
    top_arbitrage_opportunities: list[dict] = []

    @field_validator("total_movements")
    @classmethod
    def calculate_totals(cls, v, info: ValidationInfo):
        """Calculate summary statistics from game analyses."""
        if not info.data:
            return v
        games = info.data.get("game_analyses", [])
        return sum(game.total_movements for game in games)
