"""
Line Movement Data Models

Models for historical line movement data from Action Network.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any
from decimal import Decimal

from pydantic import BaseModel, Field


@dataclass
class LineMovementPoint:
    """Single point in line movement history."""
    
    odds: int
    line_value: Optional[Decimal]  # For spreads/totals
    line_timestamp: datetime  # When this line was active
    line_status: str = "normal"


class ActionNetworkHistoryEntry(BaseModel):
    """Single history entry from Action Network response."""
    
    odds: int
    value: Optional[float] = None  # Line value for spreads/totals
    updated_at: str  # ISO timestamp string
    line_status: str = "normal"
    
    def to_line_movement_point(self) -> LineMovementPoint:
        """Convert to internal LineMovementPoint."""
        return LineMovementPoint(
            odds=self.odds,
            line_value=Decimal(str(self.value)) if self.value is not None else None,
            line_timestamp=datetime.fromisoformat(self.updated_at.replace('Z', '+00:00')),
            line_status=self.line_status
        )


class ActionNetworkMarketData(BaseModel):
    """Action Network market data with history."""
    
    outcome_id: int
    market_id: int
    event_id: int
    book_id: int
    type: str  # 'total', 'spread', 'moneyline'
    side: str  # 'over', 'under', 'home', 'away'
    period: str = "event"
    
    # Current line
    odds: int
    value: Optional[float] = None
    is_live: bool = False
    line_status: str = "normal"
    
    # Betting info
    bet_info: Optional[Dict[str, Any]] = None
    
    # Historical data
    history: List[ActionNetworkHistoryEntry] = Field(default_factory=list)
    
    def get_bet_type(self) -> str:
        """Convert Action Network type to our bet_type."""
        type_mapping = {
            'total': 'total',
            'spread': 'spread', 
            'moneyline': 'moneyline',
            'h2h': 'moneyline'  # Alternative name
        }
        return type_mapping.get(self.type, self.type)
    
    def get_movement_history(self) -> List[LineMovementPoint]:
        """Get all historical line movements."""
        return [entry.to_line_movement_point() for entry in self.history]


@dataclass
class LineMovementRecord:
    """Database record for line movement history."""
    
    # Foreign Keys
    game_id: int
    sportsbook_id: int
    
    # Action Network Identifiers  
    action_network_game_id: int
    action_network_book_id: int
    outcome_id: Optional[int] = None
    market_id: Optional[int] = None
    
    # Market Information
    bet_type: str  # 'moneyline', 'spread', 'total'
    side: str  # 'home', 'away', 'over', 'under'
    period: str = "event"
    
    # Line Values
    odds: int
    line_value: Optional[Decimal] = None
    line_status: str = "normal"
    
    # Timing
    line_timestamp: datetime
    collection_timestamp: datetime
    
    # Team Information (denormalized)
    home_team: str
    away_team: str
    game_datetime: datetime
    
    # Metadata
    source: str = "ACTION_NETWORK"
    is_live: bool = False


class LineMovementExtractor:
    """Extracts line movement history from Action Network data."""
    
    def __init__(self):
        self.records: List[LineMovementRecord] = []
    
    def extract_from_game_data(
        self, 
        game_data: Dict[str, Any], 
        game_id: int,
        home_team: str,
        away_team: str,
        game_datetime: datetime
    ) -> List[LineMovementRecord]:
        """
        Extract all line movement history from Action Network game data.
        
        Args:
            game_data: Raw Action Network game data
            game_id: Internal database game ID
            home_team: Home team abbreviation
            away_team: Away team abbreviation
            game_datetime: Game datetime
            
        Returns:
            List of LineMovementRecord objects
        """
        records = []
        markets = game_data.get("markets", {})
        action_network_game_id = game_data.get("id")
        
        # Process each sportsbook's markets
        for book_id_str, book_data in markets.items():
            book_id = int(book_id_str)
            event_markets = book_data.get("event", {})
            
            # Process each market type (total, spread, moneyline)
            for market_type, market_entries in event_markets.items():
                if not isinstance(market_entries, list):
                    continue
                    
                # Process each side of the market
                for entry in market_entries:
                    try:
                        market_data = ActionNetworkMarketData(**entry)
                        
                        # Extract history for this market/side
                        history_points = market_data.get_movement_history()
                        
                        # Create records for each historical point
                        for point in history_points:
                            record = LineMovementRecord(
                                game_id=game_id,
                                sportsbook_id=0,  # Will need to resolve this
                                action_network_game_id=action_network_game_id,
                                action_network_book_id=book_id,
                                outcome_id=market_data.outcome_id,
                                market_id=market_data.market_id,
                                bet_type=market_data.get_bet_type(),
                                side=market_data.side,
                                period=market_data.period,
                                odds=point.odds,
                                line_value=point.line_value,
                                line_status=point.line_status,
                                line_timestamp=point.line_timestamp,
                                collection_timestamp=datetime.now(),
                                home_team=home_team,
                                away_team=away_team,
                                game_datetime=game_datetime,
                                source="ACTION_NETWORK",
                                is_live=market_data.is_live
                            )
                            records.append(record)
                            
                    except Exception as e:
                        # Log error but continue processing
                        print(f"Error processing market entry: {e}")
                        continue
        
        return records