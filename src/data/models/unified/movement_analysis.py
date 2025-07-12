"""
Enhanced data models for detailed line movement analysis and betting intelligence.
"""
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Union
from pydantic import BaseModel, Field, validator


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
    MINOR = "minor"      # < 5 cents for ML, < 0.5 points for spread/total
    MODERATE = "moderate"  # 5-15 cents for ML, 0.5-1.0 points for spread/total
    SIGNIFICANT = "significant"  # 15-25 cents for ML, 1.0-2.0 points for spread/total
    MAJOR = "major"      # > 25 cents for ML, > 2.0 points for spread/total


class BettingPercentageSnapshot(BaseModel):
    """Point-in-time snapshot of betting percentages."""
    timestamp: datetime
    sportsbook_id: str
    market_type: MarketType
    tickets_percent: Optional[int] = None
    money_percent: Optional[int] = None
    tickets_count: Optional[int] = None
    money_amount: Optional[Decimal] = None
    
    class Config:
        use_enum_values = True


class LineMovementDetail(BaseModel):
    """Detailed information about a specific line movement."""
    timestamp: datetime
    sportsbook_id: str
    market_type: MarketType
    previous_value: Optional[Union[int, Decimal]] = None
    new_value: Optional[Union[int, Decimal]] = None
    previous_odds: Optional[int] = None
    new_odds: Optional[int] = None
    direction: MovementDirection
    magnitude: MovementMagnitude
    movement_amount: Optional[Decimal] = None  # Absolute change
    movement_percentage: Optional[Decimal] = None  # Percentage change
    
    class Config:
        use_enum_values = True
    
    @validator('magnitude', pre=True, always=True)
    def determine_magnitude(cls, v, values):
        """Automatically determine movement magnitude based on market type and change."""
        if v is not None:
            return v
            
        market_type = values.get('market_type')
        prev_odds = values.get('previous_odds')
        new_odds = values.get('new_odds')
        prev_value = values.get('previous_value')
        new_value = values.get('new_value')
        
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
        
        elif market_type in [MarketType.SPREAD, MarketType.TOTAL] and prev_value and new_value:
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
    rlm_strength: Optional[str] = None  # "weak", "moderate", "strong"
    public_percentage: Optional[int] = None
    line_movement_amount: Optional[Decimal] = None
    confidence_score: Optional[Decimal] = None
    
    class Config:
        use_enum_values = True
    
    @validator('is_rlm', pre=True, always=True)
    def determine_rlm(cls, v, values):
        """Determine if this is reverse line movement."""
        line_dir = values.get('line_direction')
        public_dir = values.get('public_betting_direction')
        
        if line_dir and public_dir:
            return line_dir != public_dir and line_dir != MovementDirection.STABLE
        return False
    
    @validator('rlm_strength', pre=True, always=True)
    def determine_rlm_strength(cls, v, values):
        """Determine RLM strength based on public percentage and line movement."""
        if not values.get('is_rlm'):
            return None
            
        public_pct = values.get('public_percentage', 50)
        line_amount = values.get('line_movement_amount', 0)
        
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
    participating_books: List[str]
    consensus_direction: Optional[MovementDirection] = None
    consensus_strength: Optional[str] = None  # "weak", "moderate", "strong"
    divergent_books: List[str] = []
    steam_move_detected: bool = False
    average_movement: Optional[Decimal] = None
    movement_range: Optional[Dict[str, Decimal]] = None
    
    class Config:
        use_enum_values = True
    
    @validator('consensus_direction', pre=True, always=True)
    def determine_consensus(cls, v, values):
        """Determine consensus direction based on participating books."""
        # This would be calculated based on the actual movement data
        # For now, return the provided value
        return v
    
    @validator('steam_move_detected', pre=True, always=True)
    def detect_steam_move(cls, v, values):
        """Detect if this is a steam move (rapid movement across multiple books)."""
        participating = values.get('participating_books', [])
        consensus = values.get('consensus_direction')
        
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
    dominant_direction: Optional[MovementDirection] = None
    average_magnitude: Optional[str] = None
    rlm_count: int = 0
    steam_moves: int = 0
    cross_book_consensus: Optional[float] = None  # Percentage of books moving same direction
    
    class Config:
        use_enum_values = True


class GameMovementAnalysis(BaseModel):
    """Comprehensive movement analysis for a single game."""
    game_id: int
    home_team: str
    away_team: str
    game_datetime: datetime
    analysis_timestamp: datetime
    
    # Market summaries
    moneyline_summary: Optional[MarketMovementSummary] = None
    spread_summary: Optional[MarketMovementSummary] = None
    total_summary: Optional[MarketMovementSummary] = None
    
    # Detailed movements
    line_movements: List[LineMovementDetail] = []
    rlm_indicators: List[RLMIndicator] = []
    cross_book_movements: List[CrossBookMovement] = []
    betting_snapshots: List[BettingPercentageSnapshot] = []
    
    # Overall analysis
    total_movements: int = 0
    sharp_money_indicators: List[str] = []
    arbitrage_opportunities: List[Dict] = []
    recommended_actions: List[str] = []
    
    class Config:
        use_enum_values = True
    
    @validator('total_movements', pre=True, always=True)
    def calculate_total_movements(cls, v, values):
        """Calculate total movements from detailed movements."""
        movements = values.get('line_movements', [])
        return len(movements)


class MovementAnalysisReport(BaseModel):
    """Comprehensive report of movement analysis across multiple games."""
    analysis_timestamp: datetime
    total_games_analyzed: int
    games_with_rlm: int
    games_with_steam_moves: int
    games_with_arbitrage: int
    
    # Game analyses
    game_analyses: List[GameMovementAnalysis] = []
    
    # Summary statistics
    total_movements: int = 0
    total_rlm_indicators: int = 0
    total_steam_moves: int = 0
    
    # Top opportunities
    top_rlm_opportunities: List[Dict] = []
    top_steam_moves: List[Dict] = []
    top_arbitrage_opportunities: List[Dict] = []
    
    @validator('total_movements', pre=True, always=True)
    def calculate_totals(cls, v, values):
        """Calculate summary statistics from game analyses."""
        games = values.get('game_analyses', [])
        return sum(game.total_movements for game in games) 