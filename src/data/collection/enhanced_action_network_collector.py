#!/usr/bin/env python3
"""
Enhanced Action Network Collector with Historical Line Movement Capture

This collector extends the base Action Network functionality to capture
and store all historical line movements from the 'history' arrays.
"""

import asyncio
import structlog
from datetime import datetime
from typing import Dict, List, Any, Optional

import asyncpg

from .base import BaseCollector, CollectorConfig, CollectionRequest, CollectionResult
from ..models.line_movement import LineMovementExtractor, LineMovementRecord
from ...core.sportsbook_utils import SportsbookResolver
from ...core.datetime_utils import prepare_for_postgres
from ...core.team_utils import normalize_team_name

logger = structlog.get_logger(__name__)


class EnhancedActionNetworkCollector(BaseCollector):
    """
    Action Network collector with historical line movement capture.
    
    Features:
    - Captures all betting lines (current implementation)
    - Extracts and stores complete line movement history
    - Maintains proper relational database structure
    - Supports opening/closing line analysis
    """
    
    def __init__(self, config: CollectorConfig):
        super().__init__(config)
        self.db_config = {
            "host": "localhost",
            "port": 5432,
            "database": "mlb_betting",
            "user": "samlafell"
        }
        self.sportsbook_resolver = SportsbookResolver(self.db_config)
        self.line_movement_extractor = LineMovementExtractor()
    
    async def collect_data(self, request: CollectionRequest) -> List[Dict[str, Any]]:
        """Collect Action Network data with enhanced line movement processing."""
        
        # Use existing API call logic
        url = f"https://api.actionnetwork.com/web/v2/scoreboard/publicbetting/mlb"
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.actionnetwork.com/",
            "Origin": "https://www.actionnetwork.com",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        }
        
        # Format date for API
        date_str = request.start_date.strftime("%Y%m%d") if request.start_date else datetime.now().strftime("%Y%m%d")
        
        params = {
            "bookIds": "15,30,75,123,69,68,972,71,247,79",
            "date": date_str,
            "periods": "event",
        }
        
        logger.info("Fetching Action Network data with history", date=date_str)
        
        async with self.session.get(url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                games = data.get("games", [])
                
                # Enhanced processing: store both current lines AND historical movements
                await self._process_games_with_history(games)
                
                logger.info("Action Network data with history processed", 
                          games_count=len(games))
                return games
            else:
                raise Exception(f"API request failed with status {response.status}")
    
    async def _process_games_with_history(self, games: List[Dict[str, Any]]) -> None:
        """Process games and extract both current lines and historical movements."""
        
        conn = await asyncpg.connect(**self.db_config)
        
        try:
            for game in games:
                # Step 1: Create/get game record (existing logic)
                game_id_db = await self._ensure_game_exists(conn, game)
                
                if not game_id_db:
                    continue
                
                # Step 2: Extract team info
                teams = game.get("teams", [])
                if len(teams) < 2:
                    continue
                    
                away_team = normalize_team_name(teams[0].get("full_name", ""))
                home_team = normalize_team_name(teams[1].get("full_name", ""))
                game_datetime = datetime.fromisoformat(game.get("start_time").replace('Z', '+00:00'))
                
                # Step 3: Extract historical line movements
                movement_records = self.line_movement_extractor.extract_from_game_data(
                    game, game_id_db, home_team, away_team, game_datetime
                )
                
                # Step 4: Resolve sportsbook IDs and insert movements
                await self._insert_line_movements(conn, movement_records)
                
                logger.info("Processed game with history", 
                          game_id=game.get("id"), 
                          movements_extracted=len(movement_records))
        
        finally:
            await conn.close()
    
    async def _ensure_game_exists(self, conn: asyncpg.Connection, game: Dict[str, Any]) -> Optional[int]:
        """Ensure game exists in database, create if needed. Returns game_id."""
        
        game_id = game.get("id")
        teams = game.get("teams", [])
        start_time = game.get("start_time")
        
        if len(teams) < 2 or not game_id or not start_time:
            return None
        
        # Check if exists
        existing_game_id = await conn.fetchval("""
            SELECT id FROM core_betting.games 
            WHERE action_network_game_id = $1
        """, int(game_id))
        
        if existing_game_id:
            return existing_game_id
        
        # Create new game
        away_team = normalize_team_name(teams[0].get("full_name", ""))
        home_team = normalize_team_name(teams[1].get("full_name", ""))
        game_datetime = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        
        new_game_id = await conn.fetchval("""
            INSERT INTO core_betting.games (
                action_network_game_id,
                home_team,
                away_team,
                game_date,
                game_datetime,
                season,
                season_type,
                game_type,
                data_quality,
                created_at,
                updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            RETURNING id
        """, 
        int(game_id),
        home_team,
        away_team,
        game_datetime.date(),
        game_datetime,
        2025,  # Season
        "REG",  # Season type
        "R",  # Game type
        "HIGH",  # Data quality
        datetime.now(),
        datetime.now()
        )
        
        return new_game_id
    
    async def _insert_line_movements(self, conn: asyncpg.Connection, records: List[LineMovementRecord]) -> None:
        """Insert line movement records into database."""
        
        if not records:
            return
        
        # Resolve sportsbook IDs for all records
        for record in records:
            sportsbook_mapping = await self.sportsbook_resolver.resolve_action_network_id(
                record.action_network_book_id
            )
            if sportsbook_mapping:
                record.sportsbook_id = sportsbook_mapping[0]
            else:
                logger.warning("Unknown sportsbook ID", 
                             action_network_book_id=record.action_network_book_id)
                continue
        
        # Filter out records without sportsbook mapping
        valid_records = [r for r in records if r.sportsbook_id > 0]
        
        if not valid_records:
            return
        
        # Batch insert line movements
        insert_query = """
            INSERT INTO core_betting.line_movement_history (
                game_id, sportsbook_id, action_network_game_id, action_network_book_id,
                outcome_id, market_id, bet_type, side, period, odds, line_value,
                line_status, line_timestamp, collection_timestamp, home_team, away_team,
                game_datetime, source, is_live
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
            ON CONFLICT (action_network_game_id, action_network_book_id, bet_type, side, line_timestamp) 
            DO NOTHING
        """
        
        inserted_count = 0
        for record in valid_records:
            try:
                await conn.execute(
                    insert_query,
                    record.game_id,
                    record.sportsbook_id,
                    record.action_network_game_id,
                    record.action_network_book_id,
                    record.outcome_id,
                    record.market_id,
                    record.bet_type,
                    record.side,
                    record.period,
                    record.odds,
                    record.line_value,
                    record.line_status,
                    prepare_for_postgres(record.line_timestamp),
                    prepare_for_postgres(record.collection_timestamp),
                    record.home_team,
                    record.away_team,
                    prepare_for_postgres(record.game_datetime),
                    record.source,
                    record.is_live
                )
                inserted_count += 1
                
            except Exception as e:
                logger.error("Failed to insert movement record", error=str(e))
        
        logger.info("Line movements inserted", 
                   total_records=len(valid_records),
                   inserted_count=inserted_count)
    
    def validate_record(self, record: Dict[str, Any]) -> bool:
        """Validate Action Network game record."""
        required_fields = ["id", "teams", "start_time", "markets"]
        return all(field in record for field in required_fields)
    
    def normalize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Add collection metadata to record."""
        record["source"] = "ACTION_NETWORK"
        record["collected_at"] = datetime.now()
        return record


# Factory registration for new collector
from .base import CollectorFactory, DataSource

# Register the enhanced collector
CollectorFactory.register_collector(
    DataSource.ACTION_NETWORK, 
    EnhancedActionNetworkCollector
)