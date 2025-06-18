"""
Pinnacle API models for the MLB Sharp Betting system.

This module provides models for representing Pinnacle betting odds,
limits, and market data with comprehensive validation.
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import Field, validator

from mlb_sharp_betting.models.base import IdentifiedModel, ValidatedModel
from mlb_sharp_betting.models.game import Team


class PinnacleMarketType(str, Enum):
    """Enumeration of Pinnacle market types."""
    
    MONEYLINE = "moneyline"
    SPREAD = "spread"
    TOTAL = "total"
    TEAM_TOTAL = "team_total"
    SPECIAL = "special"


class PriceDesignation(str, Enum):
    """Enumeration of price designations."""
    
    HOME = "home"
    AWAY = "away"
    OVER = "over"
    UNDER = "under"
    NEUTRAL = "neutral"


class MarketStatus(str, Enum):
    """Enumeration of market status values."""
    
    OPEN = "open"
    CLOSED = "closed"
    SUSPENDED = "suspended"


class LimitType(str, Enum):
    """Enumeration of limit types."""
    
    MAX_RISK_STAKE = "maxRiskStake"
    MAX_WIN_STAKE = "maxWinStake"


class PinnacleAlignment(str, Enum):
    """Enumeration of participant alignments."""
    
    HOME = "home"
    AWAY = "away"
    NEUTRAL = "neutral"


class PinnacleMatchupStatus(str, Enum):
    """Enumeration of matchup status values."""
    
    PENDING = "pending"
    LIVE = "live"
    FINISHED = "finished"
    CANCELLED = "cancelled"


class PinnacleMarketStatus(str, Enum):
    """Enumeration of market status values (alias for MarketStatus)."""
    
    OPEN = "open"
    CLOSED = "closed"
    SUSPENDED = "suspended"


class PinnaclePrice(ValidatedModel):
    """
    Model representing a single price/odds for a market side.
    
    Contains the price (American odds format) and associated designation.
    """
    
    price: int = Field(
        ...,
        description="Price in American odds format (e.g., -110, +150)",
        ge=-10000,
        le=10000
    )
    
    designation: PriceDesignation = Field(
        ...,
        description="Price designation (home, away, over, under, neutral)"
    )
    
    @property
    def decimal_odds(self) -> float:
        """
        Convert American odds to decimal odds.
        
        Returns:
            Decimal odds equivalent
        """
        if self.price > 0:
            return (self.price / 100) + 1
        else:
            return (100 / abs(self.price)) + 1
    
    @property
    def implied_probability(self) -> float:
        """
        Calculate implied probability from American odds.
        
        Returns:
            Implied probability as percentage (0-100)
        """
        decimal = self.decimal_odds
        return (1 / decimal) * 100
    
    @property
    def is_favorite(self) -> bool:
        """Check if this price represents the favorite (negative odds)."""
        return self.price < 0
    
    @property
    def is_underdog(self) -> bool:
        """Check if this price represents the underdog (positive odds)."""
        return self.price > 0


class PinnacleLimit(ValidatedModel):
    """
    Model representing betting limits for a market.
    
    Contains limit amount and type information.
    """
    
    amount: Decimal = Field(
        ...,
        description="Limit amount in currency units",
        ge=0,
        max_digits=10,
        decimal_places=2
    )
    
    type: LimitType = Field(
        ...,
        description="Type of limit (maxRiskStake, maxWinStake)"
    )
    
    @validator("amount", pre=True)
    def validate_amount(cls, v: Any) -> Decimal:
        """
        Validate and convert amount to Decimal.
        
        Args:
            v: Amount value to validate
            
        Returns:
            Validated Decimal amount
        """
        if isinstance(v, (int, float)):
            return Decimal(str(v))
        elif isinstance(v, str):
            try:
                return Decimal(v)
            except Exception:
                raise ValueError(f"Invalid amount format: {v}")
        elif isinstance(v, Decimal):
            return v
        else:
            raise ValueError(f"Invalid amount type: {type(v)}")
    
    @property
    def amount_float(self) -> float:
        """Get amount as float for calculations."""
        return float(self.amount)


class PinnacleMarket(IdentifiedModel, ValidatedModel):
    """
    Model representing a complete Pinnacle betting market.
    
    Contains all market information including odds, limits, and metadata.
    """
    
    # Market identification
    matchup_id: int = Field(
        ...,
        description="Pinnacle matchup ID",
        gt=0
    )
    
    market_type: PinnacleMarketType = Field(
        ...,
        description="Type of betting market"
    )
    
    key: str = Field(
        ...,
        description="Market key identifier",
        min_length=1,
        max_length=50
    )
    
    # Game information
    home_team: Team = Field(
        ...,
        description="Home team abbreviation"
    )
    
    away_team: Team = Field(
        ...,
        description="Away team abbreviation"
    )
    
    game_datetime: datetime = Field(
        ...,
        description="Scheduled game start time"
    )
    
    # Market details
    period: int = Field(
        ...,
        description="Period/quarter for the market (0 = full game)",
        ge=0
    )
    
    status: MarketStatus = Field(
        ...,
        description="Current market status"
    )
    
    cutoff_at: datetime = Field(
        ...,
        description="When betting closes for this market"
    )
    
    # Line information
    line_value: Optional[float] = Field(
        default=None,
        description="Line value for spread/total markets"
    )
    
    # Prices and limits
    prices: List[PinnaclePrice] = Field(
        ...,
        description="List of prices for this market",
        min_items=1,
        max_items=4  # Typically 2 for most markets, up to 4 for some specials
    )
    
    limits: List[PinnacleLimit] = Field(
        ...,
        description="List of betting limits for this market",
        min_items=1
    )
    
    # Metadata
    version: int = Field(
        ...,
        description="Market version number for tracking changes",
        ge=0
    )
    
    is_alternate: bool = Field(
        default=False,
        description="Whether this is an alternate line market"
    )
    
    last_updated: datetime = Field(
        default_factory=datetime.now,
        description="When this market data was last updated"
    )
    
    @validator("prices")
    def validate_prices_consistency(cls, v: List[PinnaclePrice], values: Dict[str, Any]) -> List[PinnaclePrice]:
        """
        Validate that prices are consistent with market type.
        
        Args:
            v: List of prices to validate
            values: All field values
            
        Returns:
            Validated list of prices
        """
        if not v:
            raise ValueError("At least one price is required")
        
        market_type = values.get("market_type")
        if not market_type:
            return v
        
        # Check designation consistency based on market type
        designations = {price.designation for price in v}
        
        if market_type == PinnacleMarketType.MONEYLINE:
            required_designations = {PriceDesignation.HOME, PriceDesignation.AWAY}
            if not designations.issuperset(required_designations):
                raise ValueError(f"Moneyline market must have home and away prices, got: {designations}")
        
        elif market_type == PinnacleMarketType.SPREAD:
            required_designations = {PriceDesignation.HOME, PriceDesignation.AWAY}
            if not designations.issuperset(required_designations):
                raise ValueError(f"Spread market must have home and away prices, got: {designations}")
        
        elif market_type == PinnacleMarketType.TOTAL:
            required_designations = {PriceDesignation.OVER, PriceDesignation.UNDER}
            if not designations.issuperset(required_designations):
                raise ValueError(f"Total market must have over and under prices, got: {designations}")
        
        return v
    
    @validator("line_value")
    def validate_line_value_required(cls, v: Optional[float], values: Dict[str, Any]) -> Optional[float]:
        """
        Validate that line value is provided for markets that require it.
        
        Args:
            v: Line value to validate
            values: All field values
            
        Returns:
            Validated line value
        """
        market_type = values.get("market_type")
        if market_type in {PinnacleMarketType.SPREAD, PinnacleMarketType.TOTAL} and v is None:
            raise ValueError(f"Line value is required for {market_type} markets")
        
        return v
    
    def get_price_by_designation(self, designation: PriceDesignation) -> Optional[PinnaclePrice]:
        """
        Get price by designation.
        
        Args:
            designation: Price designation to find
            
        Returns:
            PinnaclePrice if found, None otherwise
        """
        for price in self.prices:
            if price.designation == designation:
                return price
        return None
    
    def get_home_price(self) -> Optional[PinnaclePrice]:
        """Get home team price."""
        return self.get_price_by_designation(PriceDesignation.HOME)
    
    def get_away_price(self) -> Optional[PinnaclePrice]:
        """Get away team price."""
        return self.get_price_by_designation(PriceDesignation.AWAY)
    
    def get_over_price(self) -> Optional[PinnaclePrice]:
        """Get over price."""
        return self.get_price_by_designation(PriceDesignation.OVER)
    
    def get_under_price(self) -> Optional[PinnaclePrice]:
        """Get under price."""
        return self.get_price_by_designation(PriceDesignation.UNDER)
    
    def get_max_risk_limit(self) -> Optional[PinnacleLimit]:
        """
        Get maximum risk limit.
        
        Returns:
            PinnacleLimit for max risk stake if available
        """
        for limit in self.limits:
            if limit.type == LimitType.MAX_RISK_STAKE:
                return limit
        return None
    
    @property
    def market_display_name(self) -> str:
        """Get display name for this market."""
        if self.market_type == PinnacleMarketType.MONEYLINE:
            return "Moneyline"
        elif self.market_type == PinnacleMarketType.SPREAD:
            return f"Spread ({self.line_value:+g})" if self.line_value else "Spread"
        elif self.market_type == PinnacleMarketType.TOTAL:
            return f"Total ({self.line_value})" if self.line_value else "Total"
        elif self.market_type == PinnacleMarketType.TEAM_TOTAL:
            return f"Team Total ({self.line_value})" if self.line_value else "Team Total"
        else:
            return self.market_type.value.title()
    
    @property
    def matchup_string(self) -> str:
        """Get matchup string representation."""
        return f"{self.away_team.value} @ {self.home_team.value}"
    
    @property
    def is_expired(self) -> bool:
        """Check if market is past its cutoff time."""
        return datetime.now() > self.cutoff_at
    
    @property
    def minutes_until_cutoff(self) -> float:
        """Get minutes until market cutoff."""
        if self.is_expired:
            return 0.0
        delta = self.cutoff_at - datetime.now()
        return delta.total_seconds() / 60.0
    
    class Config:
        json_schema_extra = {
            "example": {
                "matchup_id": 1610721342,
                "market_type": "moneyline",
                "key": "s;0;m",
                "home_team": "OAK",
                "away_team": "HOU",
                "game_datetime": "2025-06-17T02:05:00Z",
                "period": 0,
                "status": "open",
                "cutoff_at": "2025-06-17T22:40:00Z",
                "line_value": None,
                "prices": [
                    {"price": 169, "designation": "home"},
                    {"price": -189, "designation": "away"}
                ],
                "limits": [
                    {"amount": "2000.00", "type": "maxRiskStake"}
                ],
                "version": 3149186706,
                "is_alternate": False
            }
        }


class PinnacleOddsSnapshot(IdentifiedModel, ValidatedModel):
    """
    Model representing a timestamped snapshot of Pinnacle odds data.
    
    Used for tracking odds movements and historical analysis.
    """
    
    matchup_id: int = Field(
        ...,
        description="Pinnacle matchup ID",
        gt=0
    )
    
    markets: List[PinnacleMarket] = Field(
        ...,
        description="List of markets captured in this snapshot",
        min_items=1
    )
    
    snapshot_time: datetime = Field(
        default_factory=datetime.now,
        description="When this snapshot was taken"
    )
    
    @property
    def market_count(self) -> int:
        """Get number of markets in this snapshot."""
        return len(self.markets)
    
    def get_markets_by_type(self, market_type: PinnacleMarketType) -> List[PinnacleMarket]:
        """
        Get markets of a specific type.
        
        Args:
            market_type: Type of markets to retrieve
            
        Returns:
            List of markets matching the type
        """
        return [market for market in self.markets if market.market_type == market_type]
    
    def get_market_by_key(self, key: str) -> Optional[PinnacleMarket]:
        """
        Get market by key.
        
        Args:
            key: Market key to find
            
        Returns:
            PinnacleMarket if found, None otherwise
        """
        for market in self.markets:
            if market.key == key:
                return market
        return None
    
    class Config:
        json_schema_extra = {
            "example": {
                "matchup_id": 1610721342,
                "markets": [],
                "snapshot_time": "2025-01-01T12:00:00Z"
            }
        }


class PinnacleSport(ValidatedModel):
    """Model representing a sport in Pinnacle."""
    
    id: int = Field(..., description="Sport ID")
    name: str = Field(..., description="Sport name")
    primary_market_type: str = Field(..., description="Primary market type for this sport", alias="primaryMarketType")
    is_featured: bool = Field(default=False, description="Whether sport is featured", alias="isFeatured")
    is_hidden: bool = Field(default=False, description="Whether sport is hidden", alias="isHidden")
    is_sticky: bool = Field(default=False, description="Whether sport is sticky", alias="isSticky")
    matchup_count: int = Field(default=0, description="Number of matchups", alias="matchupCount")
    matchup_count_se: int = Field(default=0, description="Number of SE matchups", alias="matchupCountSE")
    feature_order: int = Field(default=0, description="Feature order", alias="featureOrder")


class PinnacleLeague(ValidatedModel):
    """Model representing a league in Pinnacle."""
    
    id: int = Field(..., description="League ID")
    name: str = Field(..., description="League name")
    group: str = Field(..., description="League group")
    sport: PinnacleSport = Field(..., description="Sport information")
    is_featured: bool = Field(default=False, description="Whether league is featured", alias="isFeatured")
    is_hidden: bool = Field(default=False, description="Whether league is hidden", alias="isHidden")
    is_promoted: bool = Field(default=False, description="Whether league is promoted", alias="isPromoted")
    is_sticky: bool = Field(default=False, description="Whether league is sticky", alias="isSticky")
    matchup_count: int = Field(default=0, description="Number of matchups", alias="matchupCount")
    matchup_count_se: int = Field(default=0, description="Number of SE matchups", alias="matchupCountSE")
    feature_order: int = Field(default=0, description="Feature order", alias="featureOrder")
    age_limit: int = Field(default=0, description="Age limit", alias="ageLimit")
    sequence: int = Field(default=0, description="Sequence number")


class PinnacleParticipant(ValidatedModel):
    """Model representing a participant (team/player) in a matchup."""
    
    id: Optional[int] = Field(default=None, description="Participant ID")
    name: str = Field(..., description="Participant name")
    alignment: PinnacleAlignment = Field(..., description="Participant alignment")
    order: int = Field(default=0, description="Display order")
    rotation: Optional[int] = Field(default=None, description="Rotation number")
    score: Optional[int] = Field(default=None, description="Current score")


class PinnacleSpecial(ValidatedModel):
    """Model representing special bet information."""
    
    category: str = Field(..., description="Special bet category")
    description: str = Field(..., description="Special bet description")


class PinnacleParentInfo(ValidatedModel):
    """Model representing parent matchup information."""
    
    id: int = Field(..., description="Parent matchup ID")
    participants: List[PinnacleParticipant] = Field(..., description="Parent participants")
    start_time: datetime = Field(..., description="Parent start time", alias="startTime")


class PinnacleMatchup(ValidatedModel):
    """Model representing a Pinnacle matchup."""
    
    id: int = Field(..., description="Matchup ID")
    participants: List[PinnacleParticipant] = Field(..., description="Matchup participants")
    start_time: datetime = Field(..., description="Matchup start time", alias="startTime")
    league: PinnacleLeague = Field(..., description="League information")
    status: PinnacleMatchupStatus = Field(..., description="Matchup status")
    version: int = Field(..., description="Version number")
    is_live: bool = Field(default=False, description="Whether matchup is live", alias="isLive")
    is_featured: bool = Field(default=False, description="Whether matchup is featured", alias="isFeatured")
    is_highlighted: bool = Field(default=False, description="Whether matchup is highlighted", alias="isHighlighted")
    is_promoted: bool = Field(default=False, description="Whether matchup is promoted", alias="isPromoted")
    has_live: bool = Field(default=False, description="Whether matchup has live betting", alias="hasLive")
    has_markets: bool = Field(default=False, description="Whether matchup has markets", alias="hasMarkets")
    
    # Optional fields for special bets
    special: Optional[PinnacleSpecial] = Field(default=None, description="Special bet information")
    parent: Optional[PinnacleParentInfo] = Field(default=None, description="Parent matchup information")
    type: Optional[str] = Field(default=None, description="Matchup type")
    units: Optional[str] = Field(default=None, description="Units for special bets")
    
    class Config:
        json_schema_extra = {
            "example": {
                "id": 1610704108,
                "participants": [
                    {
                        "id": 1610704109,
                        "name": "Houston Astros",
                        "alignment": "away",
                        "order": 0,
                        "rotation": 101
                    },
                    {
                        "id": 1610704110,
                        "name": "Oakland Athletics",
                        "alignment": "home",
                        "order": 1,
                        "rotation": 102
                    }
                ],
                "startTime": "2025-01-15T19:10:00Z",
                "league": {
                    "id": 246,
                    "name": "MLB",
                    "group": "USA",
                    "sport": {
                        "id": 3,
                        "name": "Baseball",
                        "primaryMarketType": "moneyline"
                    }
                },
                "status": "pending",
                "version": 426970028,
                "isLive": False
            }
        } 