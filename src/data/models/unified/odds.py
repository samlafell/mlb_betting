"""
Unified Odds Models

Consolidates odds-related models from:
- sportsbookreview/models/odds_data.py (OddsData, OddsSnapshot, LineMovement)
- src/mlb_sharp_betting/models/pinnacle.py (PinnacleMarket, PinnaclePrice)
- Action Network odds data patterns

All times are in EST as per project requirements.
"""

from datetime import datetime
from enum import Enum

from pydantic import Field, computed_field, field_validator

from .base import SourcedModel, UnifiedEntity, ValidatedModel


class MarketType(str, Enum):
    """Unified market type enumeration."""

    MONEYLINE = "moneyline"
    SPREAD = "spread"
    TOTAL = "total"
    RUN_LINE = "run_line"
    FIRST_FIVE_INNINGS = "first_five_innings"
    TEAM_TOTAL = "team_total"
    ALTERNATE_SPREAD = "alternate_spread"
    ALTERNATE_TOTAL = "alternate_total"
    PROP = "prop"


class OddsFormat(str, Enum):
    """Odds format enumeration."""

    AMERICAN = "american"  # +150, -120
    DECIMAL = "decimal"  # 2.50, 1.83
    FRACTIONAL = "fractional"  # 3/2, 5/6
    IMPLIED_PROBABILITY = "implied_probability"  # 0.40, 0.60


class MarketSide(str, Enum):
    """Market side enumeration."""

    HOME = "home"
    AWAY = "away"
    OVER = "over"
    UNDER = "under"
    YES = "yes"
    NO = "no"


class LineStatus(str, Enum):
    """Line status enumeration."""

    ACTIVE = "active"
    SUSPENDED = "suspended"
    CLOSED = "closed"
    SETTLED = "settled"


class BookType(str, Enum):
    """Unified sportsbook enumeration."""

    # Major US Sportsbooks
    DRAFTKINGS = "draftkings"
    FANDUEL = "fanduel"
    BETMGM = "betmgm"
    CAESARS = "caesars"
    BET365 = "bet365"
    FANATICS = "fanatics"

    # International/Sharp Books
    PINNACLE = "pinnacle"
    CIRCA = "circa"

    # Data Sources
    SPORTSBOOKREVIEW = "sportsbookreview"
    ACTION_NETWORK = "action_network"
    VSIN = "vsin"
    SBD = "sbd"

    # Generic
    OTHER = "other"


class OddsMovement(str, Enum):
    """Odds movement direction."""

    UP = "up"
    DOWN = "down"
    UNCHANGED = "unchanged"


class OddsData(SourcedModel, ValidatedModel):
    """
    Unified odds data model.

    Consolidates odds data from all sources with comprehensive
    line movement tracking and validation.
    """

    # Identification
    odds_id: str = Field(
        ...,
        description="Unique identifier for this odds entry",
        min_length=1,
        max_length=100,
    )

    game_id: str = Field(
        ..., description="Reference to the game", min_length=1, max_length=100
    )

    # Market information
    market_type: MarketType = Field(..., description="Type of betting market")

    market_side: MarketSide | None = Field(
        default=None, description="Side of the market (if applicable)"
    )

    sportsbook: BookType = Field(..., description="Sportsbook offering the odds")

    # Odds values
    odds_american: int | None = Field(
        default=None,
        description="American odds format (+150, -120)",
        ge=-10000,
        le=10000,
    )

    odds_decimal: float | None = Field(
        default=None, description="Decimal odds format (2.50, 1.83)", gt=1.0, le=1000.0
    )

    odds_fractional: str | None = Field(
        default=None, description="Fractional odds format (3/2, 5/6)", max_length=20
    )

    implied_probability: float | None = Field(
        default=None, description="Implied probability (0.0-1.0)", ge=0.0, le=1.0
    )

    # Line values (for spreads and totals)
    line_value: float | None = Field(
        default=None,
        description="Line value (spread: -1.5, total: 8.5)",
        ge=-50.0,
        le=50.0,
    )

    # Limits and volume
    max_bet_limit: float | None = Field(
        default=None, description="Maximum bet limit in USD", ge=0.0
    )

    volume: int | None = Field(
        default=None, description="Betting volume/handle", ge=0
    )

    # Status and timing
    line_status: LineStatus = Field(
        default=LineStatus.ACTIVE, description="Current status of the line"
    )

    timestamp: datetime = Field(..., description="When odds were recorded (EST)")

    # Opening/closing tracking
    is_opening_line: bool = Field(
        default=False, description="Whether this is the opening line"
    )

    is_closing_line: bool = Field(
        default=False, description="Whether this is the closing line"
    )

    # Validation
    @field_validator("odds_american")
    @classmethod
    def validate_american_odds(cls, v: int | None) -> int | None:
        """Validate American odds format."""
        if v is None:
            return v

        # American odds cannot be between -100 and +100 (exclusive)
        if -100 < v < 100 and v != 0:
            raise ValueError("American odds must be <= -100 or >= +100")

        return v

    @computed_field
    @property
    def primary_odds_format(self) -> OddsFormat:
        """Determine the primary odds format available."""
        if self.odds_american is not None:
            return OddsFormat.AMERICAN
        elif self.odds_decimal is not None:
            return OddsFormat.DECIMAL
        elif self.odds_fractional is not None:
            return OddsFormat.FRACTIONAL
        elif self.implied_probability is not None:
            return OddsFormat.IMPLIED_PROBABILITY
        else:
            return OddsFormat.AMERICAN  # Default

    def convert_to_american(self) -> int | None:
        """Convert odds to American format."""
        if self.odds_american is not None:
            return self.odds_american

        if self.odds_decimal is not None:
            if self.odds_decimal >= 2.0:
                return int((self.odds_decimal - 1) * 100)
            else:
                return int(-100 / (self.odds_decimal - 1))

        if self.implied_probability is not None:
            if self.implied_probability >= 0.5:
                return int(-100 / (1 / self.implied_probability - 1))
            else:
                return int((1 / self.implied_probability - 1) * 100)

        return None

    def convert_to_decimal(self) -> float | None:
        """Convert odds to decimal format."""
        if self.odds_decimal is not None:
            return self.odds_decimal

        if self.odds_american is not None:
            if self.odds_american > 0:
                return (self.odds_american / 100) + 1
            else:
                return (100 / abs(self.odds_american)) + 1

        if self.implied_probability is not None:
            return 1 / self.implied_probability

        return None

    def convert_to_implied_probability(self) -> float | None:
        """Convert odds to implied probability."""
        if self.implied_probability is not None:
            return self.implied_probability

        decimal = self.convert_to_decimal()
        if decimal is not None:
            return 1 / decimal

        return None


class OddsSnapshot(ValidatedModel):
    """
    Snapshot of odds at a specific point in time.

    Useful for capturing market state and line movement analysis.
    """

    snapshot_id: str = Field(
        ...,
        description="Unique identifier for this snapshot",
        min_length=1,
        max_length=100,
    )

    game_id: str = Field(
        ..., description="Reference to the game", min_length=1, max_length=100
    )

    timestamp: datetime = Field(..., description="When snapshot was taken (EST)")

    odds_data: list[OddsData] = Field(
        default_factory=list, description="All odds data captured in this snapshot"
    )

    # Market summary
    total_books: int = Field(
        default=0, description="Number of sportsbooks in snapshot", ge=0
    )

    markets_available: list[MarketType] = Field(
        default_factory=list, description="Market types available in snapshot"
    )

    @computed_field
    @property
    def has_moneyline(self) -> bool:
        """Check if snapshot contains moneyline odds."""
        return MarketType.MONEYLINE in self.markets_available

    @computed_field
    @property
    def has_spread(self) -> bool:
        """Check if snapshot contains spread odds."""
        return MarketType.SPREAD in self.markets_available

    @computed_field
    @property
    def has_total(self) -> bool:
        """Check if snapshot contains total odds."""
        return MarketType.TOTAL in self.markets_available

    def get_odds_for_market(self, market_type: MarketType) -> list[OddsData]:
        """Get all odds for a specific market type."""
        return [odds for odds in self.odds_data if odds.market_type == market_type]

    def get_odds_for_book(self, sportsbook: BookType) -> list[OddsData]:
        """Get all odds for a specific sportsbook."""
        return [odds for odds in self.odds_data if odds.sportsbook == sportsbook]


class LineMovement(UnifiedEntity):
    """
    Unified line movement tracking.

    Tracks how odds and lines change over time across different sportsbooks.
    """

    movement_id: str = Field(
        ...,
        description="Unique identifier for this movement",
        min_length=1,
        max_length=100,
    )

    game_id: str = Field(
        ..., description="Reference to the game", min_length=1, max_length=100
    )

    market_type: MarketType = Field(..., description="Type of betting market")

    sportsbook: BookType = Field(..., description="Sportsbook where movement occurred")

    # Previous state
    previous_odds: OddsData | None = Field(
        default=None, description="Previous odds state"
    )

    # Current state
    current_odds: OddsData = Field(..., description="Current odds state")

    # Movement analysis
    odds_movement: OddsMovement = Field(..., description="Direction of odds movement")

    line_movement: float | None = Field(
        default=None, description="Change in line value (current - previous)"
    )

    odds_change_american: int | None = Field(
        default=None, description="Change in American odds (current - previous)"
    )

    movement_magnitude: float | None = Field(
        default=None, description="Magnitude of movement (0.0-1.0)", ge=0.0, le=1.0
    )

    # Timing
    movement_timestamp: datetime = Field(
        ..., description="When movement was detected (EST)"
    )

    time_since_previous: int | None = Field(
        default=None, description="Minutes since previous movement", ge=0
    )

    # Context
    is_steam_move: bool = Field(
        default=False, description="Whether this is identified as a steam move"
    )

    is_reverse_line_movement: bool = Field(
        default=False, description="Whether this is reverse line movement"
    )

    volume_spike: bool = Field(
        default=False, description="Whether there was a volume spike"
    )

    @computed_field
    @property
    def movement_direction(self) -> str:
        """Get human-readable movement direction."""
        if self.odds_movement == OddsMovement.UP:
            return "Odds increased (less likely)"
        elif self.odds_movement == OddsMovement.DOWN:
            return "Odds decreased (more likely)"
        else:
            return "No change"

    @computed_field
    @property
    def is_significant_movement(self) -> bool:
        """Check if movement is considered significant."""
        if self.movement_magnitude is None:
            return False
        return self.movement_magnitude >= 0.1  # 10% threshold

    def calculate_movement_metrics(self) -> None:
        """Calculate movement metrics from previous and current odds."""
        if not self.previous_odds:
            self.odds_movement = OddsMovement.UNCHANGED
            return

        # Calculate odds change
        prev_american = self.previous_odds.convert_to_american()
        curr_american = self.current_odds.convert_to_american()

        if prev_american is not None and curr_american is not None:
            self.odds_change_american = curr_american - prev_american

            # Determine movement direction
            if self.odds_change_american > 0:
                self.odds_movement = OddsMovement.UP
            elif self.odds_change_american < 0:
                self.odds_movement = OddsMovement.DOWN
            else:
                self.odds_movement = OddsMovement.UNCHANGED

            # Calculate magnitude (simplified)
            if prev_american != 0:
                self.movement_magnitude = abs(self.odds_change_american) / abs(
                    prev_american
                )

        # Calculate line movement
        if (
            self.previous_odds.line_value is not None
            and self.current_odds.line_value is not None
        ):
            self.line_movement = (
                self.current_odds.line_value - self.previous_odds.line_value
            )


class MarketConsensus(ValidatedModel):
    """
    Market consensus data across multiple sportsbooks.

    Provides aggregated view of market sentiment and sharp action indicators.
    """

    consensus_id: str = Field(
        ...,
        description="Unique identifier for this consensus",
        min_length=1,
        max_length=100,
    )

    game_id: str = Field(
        ..., description="Reference to the game", min_length=1, max_length=100
    )

    market_type: MarketType = Field(..., description="Type of betting market")

    timestamp: datetime = Field(..., description="When consensus was calculated (EST)")

    # Consensus metrics
    average_odds_american: int | None = Field(
        default=None, description="Average American odds across books"
    )

    median_odds_american: int | None = Field(
        default=None, description="Median American odds across books"
    )

    odds_range_american: int | None = Field(
        default=None, description="Range of American odds (max - min)"
    )

    average_line_value: float | None = Field(
        default=None, description="Average line value across books"
    )

    line_range: float | None = Field(
        default=None, description="Range of line values (max - min)"
    )

    # Market efficiency
    market_efficiency_score: float | None = Field(
        default=None, description="Market efficiency score (0.0-1.0)", ge=0.0, le=1.0
    )

    arbitrage_opportunity: bool = Field(
        default=False, description="Whether arbitrage opportunity exists"
    )

    # Sharp action indicators
    sharp_action_detected: bool = Field(
        default=False, description="Whether sharp action is detected"
    )

    reverse_line_movement: bool = Field(
        default=False, description="Whether reverse line movement is present"
    )

    steam_move_count: int = Field(
        default=0, description="Number of steam moves detected", ge=0
    )

    # Participating books
    participating_books: list[BookType] = Field(
        default_factory=list, description="Sportsbooks included in consensus"
    )

    book_count: int = Field(default=0, description="Number of books in consensus", ge=0)

    @computed_field
    @property
    def has_sufficient_data(self) -> bool:
        """Check if consensus has sufficient data for analysis."""
        return self.book_count >= 3

    @computed_field
    @property
    def market_agreement_level(self) -> str:
        """Get market agreement level description."""
        if self.market_efficiency_score is None:
            return "Unknown"

        if self.market_efficiency_score >= 0.9:
            return "High Agreement"
        elif self.market_efficiency_score >= 0.7:
            return "Moderate Agreement"
        elif self.market_efficiency_score >= 0.5:
            return "Low Agreement"
        else:
            return "High Disagreement"

    def add_odds_data(self, odds_data: list[OddsData]) -> None:
        """
        Add odds data and recalculate consensus metrics.

        Args:
            odds_data: List of odds data to include in consensus
        """
        if not odds_data:
            return

        # Filter for this market type
        market_odds = [
            odds for odds in odds_data if odds.market_type == self.market_type
        ]

        if not market_odds:
            return

        # Calculate consensus metrics
        american_odds = [odds.convert_to_american() for odds in market_odds]
        american_odds = [odds for odds in american_odds if odds is not None]

        if american_odds:
            self.average_odds_american = int(sum(american_odds) / len(american_odds))
            self.median_odds_american = int(
                sorted(american_odds)[len(american_odds) // 2]
            )
            self.odds_range_american = max(american_odds) - min(american_odds)

        # Line values
        line_values = [
            odds.line_value for odds in market_odds if odds.line_value is not None
        ]
        if line_values:
            self.average_line_value = sum(line_values) / len(line_values)
            self.line_range = max(line_values) - min(line_values)

        # Update participating books
        self.participating_books = list(set(odds.sportsbook for odds in market_odds))
        self.book_count = len(self.participating_books)

        # Calculate market efficiency (simplified)
        if (
            self.odds_range_american is not None
            and self.average_odds_american is not None
        ):
            if self.average_odds_american != 0:
                efficiency = 1.0 - (
                    abs(self.odds_range_american) / abs(self.average_odds_american)
                )
                self.market_efficiency_score = max(0.0, min(1.0, efficiency))


class BettingMarket(UnifiedEntity):
    """
    Complete betting market representation.

    Aggregates all odds, movements, and consensus data for a specific market.
    """

    market_id: str = Field(
        ...,
        description="Unique identifier for this market",
        min_length=1,
        max_length=100,
    )

    game_id: str = Field(
        ..., description="Reference to the game", min_length=1, max_length=100
    )

    market_type: MarketType = Field(..., description="Type of betting market")

    # Market data
    current_odds: list[OddsData] = Field(
        default_factory=list, description="Current odds from all books"
    )

    historical_snapshots: list[OddsSnapshot] = Field(
        default_factory=list, description="Historical odds snapshots"
    )

    line_movements: list[LineMovement] = Field(
        default_factory=list, description="All line movements for this market"
    )

    current_consensus: MarketConsensus | None = Field(
        default=None, description="Current market consensus"
    )

    # Market status
    market_status: LineStatus = Field(
        default=LineStatus.ACTIVE, description="Current market status"
    )

    last_updated: datetime = Field(
        ..., description="When market was last updated (EST)"
    )

    @computed_field
    @property
    def available_books(self) -> list[BookType]:
        """Get list of books currently offering odds."""
        return list(set(odds.sportsbook for odds in self.current_odds))

    @computed_field
    @property
    def has_recent_movement(self) -> bool:
        """Check if market has recent line movement."""
        if not self.line_movements:
            return False

        # Check for movement in last hour
        recent_threshold = datetime.now().replace(microsecond=0)
        recent_threshold = recent_threshold.replace(hour=recent_threshold.hour - 1)

        return any(
            movement.movement_timestamp >= recent_threshold
            for movement in self.line_movements
        )

    def get_best_odds(self, side: MarketSide) -> OddsData | None:
        """
        Get best odds for a specific side.

        Args:
            side: Market side to get best odds for

        Returns:
            OddsData with best odds or None
        """
        side_odds = [odds for odds in self.current_odds if odds.market_side == side]

        if not side_odds:
            return None

        # Best odds = highest American odds (most favorable)
        best = None
        best_american = None

        for odds in side_odds:
            american = odds.convert_to_american()
            if american is not None:
                if best_american is None or american > best_american:
                    best = odds
                    best_american = american

        return best

    def calculate_market_consensus(self) -> MarketConsensus | None:
        """Calculate and return current market consensus."""
        if not self.current_odds:
            return None

        consensus = MarketConsensus(
            consensus_id=f"{self.market_id}_consensus_{int(datetime.now().timestamp())}",
            game_id=self.game_id,
            market_type=self.market_type,
            timestamp=datetime.now(),
        )

        consensus.add_odds_data(self.current_odds)
        self.current_consensus = consensus

        return consensus
