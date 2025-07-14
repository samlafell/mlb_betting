"""
Unified Database Repositories

Provides specialized repository implementations for all unified models:
- GameRepository: Game data operations
- OddsRepository: Odds and betting market operations  
- BettingAnalysisRepository: Betting analysis operations
- SharpDataRepository: Sharp money and signal operations
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from uuid import UUID

from pydantic import BaseModel

from ...core.logging import LogComponent, get_logger
from ..models.unified.game import UnifiedGame, GameStatus, Team
from ..models.unified.odds import BettingMarket, OddsSnapshot, MarketType, BookType
from ..models.unified.betting_analysis import BettingAnalysis, SharpAction, BettingSplit
from ..models.unified.sharp_data import SharpSignal, SharpMoney, SharpConsensus
from .base import BaseRepository
from .connection import DatabaseConnection

logger = get_logger(__name__, LogComponent.DATABASE)


# Create schemas for repository operations
class GameCreateSchema(BaseModel):
    """Schema for creating games."""
    game_id: str
    mlb_game_id: Optional[str] = None
    sbr_game_id: Optional[str] = None
    action_network_game_id: Optional[str] = None
    home_team: Team
    away_team: Team
    game_date: datetime
    game_time: datetime
    venue: Optional[str] = None
    weather_condition: Optional[str] = None
    status: GameStatus = GameStatus.SCHEDULED


class GameUpdateSchema(BaseModel):
    """Schema for updating games."""
    status: Optional[GameStatus] = None
    home_score: Optional[int] = None
    away_score: Optional[int] = None
    weather_condition: Optional[str] = None
    venue: Optional[str] = None


class OddsCreateSchema(BaseModel):
    """Schema for creating odds."""
    game_id: str
    market_type: MarketType
    sportsbook: BookType
    american_odds: Optional[int] = None
    decimal_odds: Optional[float] = None
    implied_probability: Optional[float] = None
    line_value: Optional[float] = None
    timestamp: datetime


class OddsUpdateSchema(BaseModel):
    """Schema for updating odds."""
    american_odds: Optional[int] = None
    decimal_odds: Optional[float] = None
    implied_probability: Optional[float] = None
    line_value: Optional[float] = None


class BettingAnalysisCreateSchema(BaseModel):
    """Schema for creating betting analysis."""
    game_id: str
    analysis_type: str
    confidence_score: float
    recommendation: str
    signals: List[str]
    risk_assessment: Dict[str, Any]


class BettingAnalysisUpdateSchema(BaseModel):
    """Schema for updating betting analysis."""
    confidence_score: Optional[float] = None
    recommendation: Optional[str] = None
    signals: Optional[List[str]] = None
    risk_assessment: Optional[Dict[str, Any]] = None


class SharpDataCreateSchema(BaseModel):
    """Schema for creating sharp data."""
    game_id: str
    signal_type: str
    confidence_level: str
    direction: str
    strength: float
    supporting_evidence: List[str]


class SharpDataUpdateSchema(BaseModel):
    """Schema for updating sharp data."""
    confidence_level: Optional[str] = None
    strength: Optional[float] = None
    supporting_evidence: Optional[List[str]] = None


class GameRepository(BaseRepository[UnifiedGame, GameCreateSchema, GameUpdateSchema]):
    """Repository for game data operations."""
    
    def __init__(self, connection: DatabaseConnection):
        """Initialize game repository."""
        super().__init__(
            connection=connection,
            model_class=UnifiedGame,
            table_name="games",
            primary_key="game_id"
        )
    
    async def find_by_criteria(self, **criteria) -> List[UnifiedGame]:
        """Find games by criteria."""
        start_time = self.logger.log_operation_start(
            "find_games_by_criteria",
            extra={"criteria": criteria}
        )
        
        try:
            where_clauses = []
            params = []
            param_count = 0
            
            for key, value in criteria.items():
                param_count += 1
                where_clauses.append(f"{key} = ${param_count}")
                params.append(value)
            
            where_clause = " AND ".join(where_clauses) if where_clauses else None
            query = self._build_select_query(where_clause=where_clause)
            
            results = await self.connection.execute_async(
                query,
                *params,
                fetch="all",
                table=self.table_name
            )
            
            games = [self._dict_to_model(dict(row)) for row in results]
            
            self.logger.log_operation_end(
                "find_games_by_criteria",
                start_time,
                success=True,
                extra={"count": len(games)}
            )
            
            return games
            
        except Exception as e:
            self.logger.log_operation_end("find_games_by_criteria", start_time, success=False, error=e)
            raise
    
    async def find_by_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        status: Optional[GameStatus] = None
    ) -> List[UnifiedGame]:
        """
        Find games by date range.
        
        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            status: Optional game status filter
            
        Returns:
            List of games in date range
        """
        start_time = self.logger.log_operation_start(
            "find_games_by_date_range",
            extra={
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "status": status.value if status else None
            }
        )
        
        try:
            where_clause = "game_date >= $1 AND game_date <= $2"
            params = [start_date, end_date]
            
            if status:
                where_clause += " AND status = $3"
                params.append(status.value)
            
            query = self._build_select_query(
                where_clause=where_clause,
                order_by="game_date ASC, game_time ASC"
            )
            
            results = await self.connection.execute_async(
                query,
                *params,
                fetch="all",
                table=self.table_name
            )
            
            games = [self._dict_to_model(dict(row)) for row in results]
            
            self.logger.log_operation_end(
                "find_games_by_date_range",
                start_time,
                success=True,
                extra={"count": len(games)}
            )
            
            return games
            
        except Exception as e:
            self.logger.log_operation_end("find_games_by_date_range", start_time, success=False, error=e)
            raise
    
    async def find_by_team(self, team: Team, start_date: Optional[datetime] = None) -> List[UnifiedGame]:
        """
        Find games by team.
        
        Args:
            team: Team to search for
            start_date: Optional start date filter
            
        Returns:
            List of games for the team
        """
        start_time = self.logger.log_operation_start(
            "find_games_by_team",
            extra={"team": team.value, "start_date": start_date.isoformat() if start_date else None}
        )
        
        try:
            where_clause = "(home_team = $1 OR away_team = $1)"
            params = [team.value]
            
            if start_date:
                where_clause += " AND game_date >= $2"
                params.append(start_date)
            
            query = self._build_select_query(
                where_clause=where_clause,
                order_by="game_date DESC, game_time DESC"
            )
            
            results = await self.connection.execute_async(
                query,
                *params,
                fetch="all",
                table=self.table_name
            )
            
            games = [self._dict_to_model(dict(row)) for row in results]
            
            self.logger.log_operation_end(
                "find_games_by_team",
                start_time,
                success=True,
                extra={"count": len(games)}
            )
            
            return games
            
        except Exception as e:
            self.logger.log_operation_end("find_games_by_team", start_time, success=False, error=e)
            raise
    
    async def find_by_external_id(
        self,
        external_id: str,
        source: str
    ) -> Optional[UnifiedGame]:
        """
        Find game by external ID from specific source.
        
        Args:
            external_id: External ID
            source: Source system (mlb, sbr, action)
            
        Returns:
            Game if found, None otherwise
        """
        start_time = self.logger.log_operation_start(
            "find_game_by_external_id",
            extra={"external_id": external_id, "source": source}
        )
        
        try:
            field_map = {
                "mlb": "mlb_game_id",
                "sbr": "sbr_game_id", 
                "action": "action_network_game_id"
            }
            
            if source not in field_map:
                raise ValueError(f"Invalid source: {source}")
            
            field = field_map[source]
            query = self._build_select_query(where_clause=f"{field} = $1")
            
            result = await self.connection.execute_async(
                query,
                external_id,
                fetch="one",
                table=self.table_name
            )
            
            game = self._dict_to_model(dict(result)) if result else None
            
            self.logger.log_operation_end(
                "find_game_by_external_id",
                start_time,
                success=True,
                extra={"external_id": external_id, "found": game is not None}
            )
            
            return game
            
        except Exception as e:
            self.logger.log_operation_end("find_game_by_external_id", start_time, success=False, error=e)
            raise
    
    async def get_todays_games(self) -> List[UnifiedGame]:
        """Get today's games."""
        today = datetime.now().date()
        start_date = datetime.combine(today, datetime.min.time())
        end_date = datetime.combine(today, datetime.max.time())
        
        return await self.find_by_date_range(start_date, end_date)
    
    async def get_live_games(self) -> List[UnifiedGame]:
        """Get currently live games."""
        return await self.find_by_criteria(status=GameStatus.IN_PROGRESS)


class OddsRepository(BaseRepository[BettingMarket, OddsCreateSchema, OddsUpdateSchema]):
    """Repository for odds and betting market operations."""
    
    def __init__(self, connection: DatabaseConnection):
        """Initialize odds repository."""
        super().__init__(
            connection=connection,
            model_class=BettingMarket,
            table_name="betting_markets",
            primary_key="id"
        )
    
    async def find_by_criteria(self, **criteria) -> List[BettingMarket]:
        """Find odds by criteria."""
        start_time = self.logger.log_operation_start(
            "find_odds_by_criteria",
            extra={"criteria": criteria}
        )
        
        try:
            where_clauses = []
            params = []
            param_count = 0
            
            for key, value in criteria.items():
                param_count += 1
                where_clauses.append(f"{key} = ${param_count}")
                params.append(value)
            
            where_clause = " AND ".join(where_clauses) if where_clauses else None
            query = self._build_select_query(
                where_clause=where_clause,
                order_by="timestamp DESC"
            )
            
            results = await self.connection.execute_async(
                query,
                *params,
                fetch="all",
                table=self.table_name
            )
            
            odds = [self._dict_to_model(dict(row)) for row in results]
            
            self.logger.log_operation_end(
                "find_odds_by_criteria",
                start_time,
                success=True,
                extra={"count": len(odds)}
            )
            
            return odds
            
        except Exception as e:
            self.logger.log_operation_end("find_odds_by_criteria", start_time, success=False, error=e)
            raise
    
    async def find_by_game(
        self,
        game_id: str,
        market_type: Optional[MarketType] = None,
        sportsbook: Optional[BookType] = None
    ) -> List[BettingMarket]:
        """
        Find odds by game.
        
        Args:
            game_id: Game ID
            market_type: Optional market type filter
            sportsbook: Optional sportsbook filter
            
        Returns:
            List of betting markets for the game
        """
        start_time = self.logger.log_operation_start(
            "find_odds_by_game",
            extra={
                "game_id": game_id,
                "market_type": market_type.value if market_type else None,
                "sportsbook": sportsbook.value if sportsbook else None
            }
        )
        
        try:
            where_clause = "game_id = $1"
            params = [game_id]
            param_count = 1
            
            if market_type:
                param_count += 1
                where_clause += f" AND market_type = ${param_count}"
                params.append(market_type.value)
            
            if sportsbook:
                param_count += 1
                where_clause += f" AND sportsbook = ${param_count}"
                params.append(sportsbook.value)
            
            query = self._build_select_query(
                where_clause=where_clause,
                order_by="timestamp DESC"
            )
            
            results = await self.connection.execute_async(
                query,
                *params,
                fetch="all",
                table=self.table_name
            )
            
            markets = [self._dict_to_model(dict(row)) for row in results]
            
            self.logger.log_operation_end(
                "find_odds_by_game",
                start_time,
                success=True,
                extra={"count": len(markets)}
            )
            
            return markets
            
        except Exception as e:
            self.logger.log_operation_end("find_odds_by_game", start_time, success=False, error=e)
            raise
    
    async def get_latest_odds(
        self,
        game_id: str,
        market_type: MarketType,
        sportsbook: Optional[BookType] = None
    ) -> List[BettingMarket]:
        """
        Get latest odds for a game and market type.
        
        Args:
            game_id: Game ID
            market_type: Market type
            sportsbook: Optional sportsbook filter
            
        Returns:
            Latest odds for the specified criteria
        """
        start_time = self.logger.log_operation_start(
            "get_latest_odds",
            extra={
                "game_id": game_id,
                "market_type": market_type.value,
                "sportsbook": sportsbook.value if sportsbook else None
            }
        )
        
        try:
            where_clause = "game_id = $1 AND market_type = $2"
            params = [game_id, market_type.value]
            
            if sportsbook:
                where_clause += " AND sportsbook = $3"
                params.append(sportsbook.value)
            
            # Get latest timestamp first
            latest_query = f"""
                SELECT MAX(timestamp) FROM {self.table_name}
                WHERE {where_clause}
            """
            
            latest_result = await self.connection.execute_async(
                latest_query,
                *params,
                fetch="one",
                table=self.table_name
            )
            
            if not latest_result or not latest_result[0]:
                return []
            
            latest_timestamp = latest_result[0]
            
            # Get all odds at latest timestamp
            where_clause += f" AND timestamp = ${len(params) + 1}"
            params.append(latest_timestamp)
            
            query = self._build_select_query(where_clause=where_clause)
            
            results = await self.connection.execute_async(
                query,
                *params,
                fetch="all",
                table=self.table_name
            )
            
            markets = [self._dict_to_model(dict(row)) for row in results]
            
            self.logger.log_operation_end(
                "get_latest_odds",
                start_time,
                success=True,
                extra={"count": len(markets)}
            )
            
            return markets
            
        except Exception as e:
            self.logger.log_operation_end("get_latest_odds", start_time, success=False, error=e)
            raise
    
    async def get_line_movement(
        self,
        game_id: str,
        market_type: MarketType,
        sportsbook: BookType,
        hours_back: int = 24
    ) -> List[BettingMarket]:
        """
        Get line movement history.
        
        Args:
            game_id: Game ID
            market_type: Market type
            sportsbook: Sportsbook
            hours_back: Hours to look back
            
        Returns:
            Line movement history
        """
        start_time = self.logger.log_operation_start(
            "get_line_movement",
            extra={
                "game_id": game_id,
                "market_type": market_type.value,
                "sportsbook": sportsbook.value,
                "hours_back": hours_back
            }
        )
        
        try:
            cutoff_time = datetime.now() - timedelta(hours=hours_back)
            
            where_clause = """
                game_id = $1 AND market_type = $2 AND sportsbook = $3 
                AND timestamp >= $4
            """
            params = [game_id, market_type.value, sportsbook.value, cutoff_time]
            
            query = self._build_select_query(
                where_clause=where_clause,
                order_by="timestamp ASC"
            )
            
            results = await self.connection.execute_async(
                query,
                *params,
                fetch="all",
                table=self.table_name
            )
            
            movements = [self._dict_to_model(dict(row)) for row in results]
            
            self.logger.log_operation_end(
                "get_line_movement",
                start_time,
                success=True,
                extra={"count": len(movements)}
            )
            
            return movements
            
        except Exception as e:
            self.logger.log_operation_end("get_line_movement", start_time, success=False, error=e)
            raise


class BettingAnalysisRepository(BaseRepository[BettingAnalysis, BettingAnalysisCreateSchema, BettingAnalysisUpdateSchema]):
    """Repository for betting analysis operations."""
    
    def __init__(self, connection: DatabaseConnection):
        """Initialize betting analysis repository."""
        super().__init__(
            connection=connection,
            model_class=BettingAnalysis,
            table_name="betting_analyses",
            primary_key="id"
        )
    
    async def find_by_criteria(self, **criteria) -> List[BettingAnalysis]:
        """Find analyses by criteria."""
        start_time = self.logger.log_operation_start(
            "find_analyses_by_criteria",
            extra={"criteria": criteria}
        )
        
        try:
            where_clauses = []
            params = []
            param_count = 0
            
            for key, value in criteria.items():
                param_count += 1
                where_clauses.append(f"{key} = ${param_count}")
                params.append(value)
            
            where_clause = " AND ".join(where_clauses) if where_clauses else None
            query = self._build_select_query(
                where_clause=where_clause,
                order_by="created_at DESC"
            )
            
            results = await self.connection.execute_async(
                query,
                *params,
                fetch="all",
                table=self.table_name
            )
            
            analyses = [self._dict_to_model(dict(row)) for row in results]
            
            self.logger.log_operation_end(
                "find_analyses_by_criteria",
                start_time,
                success=True,
                extra={"count": len(analyses)}
            )
            
            return analyses
            
        except Exception as e:
            self.logger.log_operation_end("find_analyses_by_criteria", start_time, success=False, error=e)
            raise
    
    async def find_by_game(self, game_id: str) -> List[BettingAnalysis]:
        """Find analyses by game."""
        return await self.find_by_criteria(game_id=game_id)
    
    async def get_high_confidence_analyses(
        self,
        min_confidence: float = 0.8,
        limit: Optional[int] = None
    ) -> List[BettingAnalysis]:
        """
        Get high confidence analyses.
        
        Args:
            min_confidence: Minimum confidence score
            limit: Optional result limit
            
        Returns:
            High confidence analyses
        """
        start_time = self.logger.log_operation_start(
            "get_high_confidence_analyses",
            extra={"min_confidence": min_confidence, "limit": limit}
        )
        
        try:
            where_clause = "confidence_score >= $1"
            params = [min_confidence]
            
            query = self._build_select_query(
                where_clause=where_clause,
                order_by="confidence_score DESC, created_at DESC",
                limit=limit
            )
            
            results = await self.connection.execute_async(
                query,
                *params,
                fetch="all",
                table=self.table_name
            )
            
            analyses = [self._dict_to_model(dict(row)) for row in results]
            
            self.logger.log_operation_end(
                "get_high_confidence_analyses",
                start_time,
                success=True,
                extra={"count": len(analyses)}
            )
            
            return analyses
            
        except Exception as e:
            self.logger.log_operation_end("get_high_confidence_analyses", start_time, success=False, error=e)
            raise


class SharpDataRepository(BaseRepository[SharpSignal, SharpDataCreateSchema, SharpDataUpdateSchema]):
    """Repository for sharp money and signal operations."""
    
    def __init__(self, connection: DatabaseConnection):
        """Initialize sharp data repository."""
        super().__init__(
            connection=connection,
            model_class=SharpSignal,
            table_name="sharp_signals",
            primary_key="id"
        )
    
    async def find_by_criteria(self, **criteria) -> List[SharpSignal]:
        """Find sharp signals by criteria."""
        start_time = self.logger.log_operation_start(
            "find_sharp_signals_by_criteria",
            extra={"criteria": criteria}
        )
        
        try:
            where_clauses = []
            params = []
            param_count = 0
            
            for key, value in criteria.items():
                param_count += 1
                where_clauses.append(f"{key} = ${param_count}")
                params.append(value)
            
            where_clause = " AND ".join(where_clauses) if where_clauses else None
            query = self._build_select_query(
                where_clause=where_clause,
                order_by="created_at DESC"
            )
            
            results = await self.connection.execute_async(
                query,
                *params,
                fetch="all",
                table=self.table_name
            )
            
            signals = [self._dict_to_model(dict(row)) for row in results]
            
            self.logger.log_operation_end(
                "find_sharp_signals_by_criteria",
                start_time,
                success=True,
                extra={"count": len(signals)}
            )
            
            return signals
            
        except Exception as e:
            self.logger.log_operation_end("find_sharp_signals_by_criteria", start_time, success=False, error=e)
            raise
    
    async def find_by_game(self, game_id: str) -> List[SharpSignal]:
        """Find sharp signals by game."""
        return await self.find_by_criteria(game_id=game_id)
    
    async def get_strong_signals(
        self,
        min_strength: float = 0.8,
        confidence_level: Optional[str] = None
    ) -> List[SharpSignal]:
        """
        Get strong sharp signals.
        
        Args:
            min_strength: Minimum signal strength
            confidence_level: Optional confidence level filter
            
        Returns:
            Strong sharp signals
        """
        start_time = self.logger.log_operation_start(
            "get_strong_signals",
            extra={"min_strength": min_strength, "confidence_level": confidence_level}
        )
        
        try:
            where_clause = "strength >= $1"
            params = [min_strength]
            
            if confidence_level:
                where_clause += " AND confidence_level = $2"
                params.append(confidence_level)
            
            query = self._build_select_query(
                where_clause=where_clause,
                order_by="strength DESC, created_at DESC"
            )
            
            results = await self.connection.execute_async(
                query,
                *params,
                fetch="all",
                table=self.table_name
            )
            
            signals = [self._dict_to_model(dict(row)) for row in results]
            
            self.logger.log_operation_end(
                "get_strong_signals",
                start_time,
                success=True,
                extra={"count": len(signals)}
            )
            
            return signals
            
        except Exception as e:
            self.logger.log_operation_end("get_strong_signals", start_time, success=False, error=e)
            raise


class UnifiedRepository:
    """
    Unified repository providing access to all specialized repositories.
    
    Acts as a facade for all data operations, providing a single entry point
    for database operations across all models.
    """
    
    def __init__(self, connection: DatabaseConnection):
        """
        Initialize unified repository.
        
        Args:
            connection: Database connection instance
        """
        self.connection = connection
        self.logger = logger.with_context(repository="UnifiedRepository")
        
        # Initialize specialized repositories
        self.games = GameRepository(connection)
        self.odds = OddsRepository(connection)
        self.betting_analysis = BettingAnalysisRepository(connection)
        self.sharp_data = SharpDataRepository(connection)
        
        self.logger.info(
            "Initialized unified repository with all specialized repositories",
            operation="init_unified_repository"
        )
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Perform health check on all repositories.
        
        Returns:
            Health check results
        """
        start_time = self.logger.log_operation_start("unified_repository_health_check")
        
        try:
            health_results = {
                "connection": await self.connection.health_check(),
                "repositories": {
                    "games": {
                        "table_exists": await self.connection.execute_async(
                            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'games')",
                            fetch="one"
                        ),
                    },
                    "betting_markets": {
                        "table_exists": await self.connection.execute_async(
                            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'betting_markets')",
                            fetch="one"
                        ),
                    },
                    "betting_analyses": {
                        "table_exists": await self.connection.execute_async(
                            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'betting_analyses')",
                            fetch="one"
                        ),
                    },
                    "sharp_signals": {
                        "table_exists": await self.connection.execute_async(
                            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'sharp_signals')",
                            fetch="one"
                        ),
                    },
                }
            }
            
            self.logger.log_operation_end(
                "unified_repository_health_check",
                start_time,
                success=True
            )
            
            return health_results
            
        except Exception as e:
            self.logger.log_operation_end(
                "unified_repository_health_check",
                start_time,
                success=False,
                error=e
            )
            raise 


# Factory function for unified repository
def get_unified_repository() -> UnifiedRepository:
    """
    Factory function to create a UnifiedRepository instance.
    
    Returns:
        UnifiedRepository: Configured repository instance
    """
    from .connection import get_connection
    connection = get_connection()
    return UnifiedRepository(connection)