#!/usr/bin/env python3
"""
Load Action Network Complete Line Movement History for July 18th, 2025

This script processes Action Network data and inserts each historical point
from the 'history' arrays as separate records in the main betting tables.
Each record gets a unique timestamp from the Action Network history.
"""

import asyncio
import json
from datetime import datetime
from typing import Dict, List, Any, Optional

import aiohttp
import asyncpg
import structlog

# Import utilities
from src.core.datetime_utils import (
    prepare_for_postgres,
    safe_game_datetime_parse,
    now_est
)
from src.core.team_utils import (
    normalize_team_name,
    create_external_source_id
)
from src.core.sportsbook_utils import SportsbookResolver

# Configure logging
structlog.configure(
    processors=[structlog.dev.ConsoleRenderer()],
    wrapper_class=structlog.make_filtering_bound_logger(20),
    logger_factory=structlog.WriteLoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


class CompleteHistoryCollector:
    """Collect and store complete line movement history from Action Network."""
    
    def __init__(self):
        self.api_base = "https://api.actionnetwork.com"
        self.headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.actionnetwork.com/",
            "Origin": "https://www.actionnetwork.com",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        
        self.db_config = {
            "host": "localhost",
            "port": 5432,
            "database": "mlb_betting",
            "user": "samlafell"
        }
        
        self.sportsbook_resolver = SportsbookResolver(self.db_config)
        
        # Stats tracking
        self.stats = {
            "games_found": 0,
            "games_processed": 0,
            "current_lines": 0,
            "history_points": 0,
            "moneyline_inserted": 0,
            "spread_inserted": 0,
            "totals_inserted": 0,
            "total_inserted": 0
        }
    
    async def run_complete_collection(self) -> Dict[str, Any]:
        """Run complete line movement collection."""
        
        print("🚀 Action Network COMPLETE Line Movement Collection")
        print("=" * 70)
        print("🎯 Goal: Store ALL line movements in main betting tables")
        print("📊 Strategy: Each history point = separate record")
        print("🕐 Timestamps: Use Action Network updated_at times")
        print()
        
        # Step 1: Fetch Action Network data
        print("📡 Step 1: Fetching Action Network data...")
        games_data = await self._fetch_games()
        
        if not games_data:
            print("❌ No games found")
            return self.stats
        
        self.stats["games_found"] = len(games_data)
        print(f"   ✅ Found {len(games_data)} games")
        
        # Step 2: Analyze what data is available
        print("\n📊 Step 2: Analyzing available data...")
        analysis = await self._analyze_data_availability(games_data)
        
        # Step 3: Ensure games exist
        print("\n🎮 Step 3: Creating/verifying games in database...")
        game_mappings = await self._create_games(games_data)
        print(f"   ✅ {len(game_mappings)} games ready")
        
        # Step 4: Process current lines and any history
        print("\n📈 Step 4: Processing line movements...")
        await self._process_all_games(games_data, game_mappings)
        
        # Step 5: Summary
        print(f"\n📊 Final Summary:")
        print(f"   • Games processed: {self.stats['games_processed']}")
        print(f"   • Current lines captured: {self.stats['current_lines']}")
        print(f"   • Historical points: {self.stats['history_points']}")
        print(f"   • Total records inserted: {self.stats['total_inserted']}")
        print(f"     - Moneyline: {self.stats['moneyline_inserted']}")
        print(f"     - Spread: {self.stats['spread_inserted']}")
        print(f"     - Totals: {self.stats['totals_inserted']}")
        
        return self.stats
    
    async def _fetch_games(self) -> List[Dict[str, Any]]:
        """Fetch games from Action Network API."""
        
        try:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                url = f"{self.api_base}/web/v2/scoreboard/publicbetting/mlb"
                params = {
                    "bookIds": "15,30,75,123,69,68,972,71,247,79",
                    "date": "20250718",
                    "periods": "event",
                }
                
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("games", [])
                    else:
                        logger.error("API failed", status=response.status)
                        return []
        
        except Exception as e:
            logger.error("API request failed", error=str(e))
            return []
    
    async def _analyze_data_availability(self, games: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze what historical data is available."""
        
        total_markets = 0
        current_markets = 0
        markets_with_history = 0
        total_history_points = 0
        
        for game in games[:3]:  # Sample first 3 games
            markets = game.get("markets", {})
            
            for book_id, book_data in markets.items():
                event_markets = book_data.get("event", {})
                
                for market_type, market_entries in event_markets.items():
                    if isinstance(market_entries, list):
                        for entry in market_entries:
                            total_markets += 1
                            
                            # Check current data
                            if entry.get("odds") is not None:
                                current_markets += 1
                            
                            # Check historical data
                            history = entry.get("history", [])
                            if history:
                                markets_with_history += 1
                                total_history_points += len(history)
        
        print(f"   📊 Data analysis (sample):")
        print(f"      • Total markets: {total_markets}")
        print(f"      • Current lines: {current_markets}")
        print(f"      • Markets with history: {markets_with_history}")
        print(f"      • Total history points: {total_history_points}")
        
        return {
            "total_markets": total_markets,
            "current_markets": current_markets,
            "markets_with_history": markets_with_history,
            "total_history_points": total_history_points
        }
    
    async def _create_games(self, games: List[Dict[str, Any]]) -> Dict[str, int]:
        """Create/verify games in database."""
        
        game_mappings = {}
        
        try:
            conn = await asyncpg.connect(**self.db_config)
            
            for game in games:
                game_id = game.get("id")
                teams = game.get("teams", [])
                start_time = game.get("start_time")
                
                if len(teams) < 2 or not game_id or not start_time:
                    continue
                
                # Check if exists
                existing_id = await conn.fetchval("""
                    SELECT id FROM core_betting.games WHERE action_network_game_id = $1
                """, int(game_id))
                
                if existing_id:
                    game_mappings[str(game_id)] = existing_id
                    continue
                
                # Create new
                away_team = normalize_team_name(teams[0].get("full_name", ""))
                home_team = normalize_team_name(teams[1].get("full_name", ""))
                game_datetime = safe_game_datetime_parse(start_time)
                
                new_id = await conn.fetchval("""
                    INSERT INTO core_betting.games (
                        action_network_game_id, home_team, away_team, game_date,
                        game_datetime, season, season_type, game_type, data_quality,
                        created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    RETURNING id
                """,
                int(game_id), home_team, away_team, game_datetime.date(),
                game_datetime, 2025, "REG", "R", "HIGH", now_est(), now_est()
                )
                
                game_mappings[str(game_id)] = new_id
            
            await conn.close()
            
        except Exception as e:
            logger.error("Error creating games", error=str(e))
        
        return game_mappings
    
    async def _process_all_games(self, games: List[Dict[str, Any]], game_mappings: Dict[str, int]) -> None:
        """Process all games and insert line movements."""
        
        for game in games:
            game_id = str(game.get("id"))
            if game_id not in game_mappings:
                continue
            
            internal_game_id = game_mappings[game_id]
            teams = game.get("teams", [])
            
            if len(teams) < 2:
                continue
            
            away_team = normalize_team_name(teams[0].get("full_name", ""))
            home_team = normalize_team_name(teams[1].get("full_name", ""))
            game_datetime = safe_game_datetime_parse(game.get("start_time"))
            
            markets = game.get("markets", {})
            
            # Process each sportsbook
            for book_id_str, book_data in markets.items():
                book_id = int(book_id_str)
                
                # Get sportsbook mapping
                sportsbook_mapping = await self.sportsbook_resolver.resolve_action_network_id(book_id)
                if not sportsbook_mapping:
                    continue
                
                sportsbook_id, sportsbook_name = sportsbook_mapping
                
                event_markets = book_data.get("event", {})
                
                # Process each market type
                await self._process_moneyline_markets(
                    event_markets.get("moneyline", []), internal_game_id,
                    sportsbook_id, sportsbook_name, home_team, away_team,
                    game_datetime, int(game_id), book_id
                )
                
                await self._process_spread_markets(
                    event_markets.get("spread", []), internal_game_id,
                    sportsbook_id, sportsbook_name, home_team, away_team,
                    game_datetime, int(game_id), book_id
                )
                
                await self._process_totals_markets(
                    event_markets.get("total", []), internal_game_id,
                    sportsbook_id, sportsbook_name, home_team, away_team,
                    game_datetime, int(game_id), book_id
                )
            
            self.stats["games_processed"] += 1
    
    async def _process_moneyline_markets(
        self, moneyline_data: List[Dict], game_id: int, sportsbook_id: int,
        sportsbook_name: str, home_team: str, away_team: str,
        game_datetime: datetime, action_network_game_id: int, action_network_book_id: int
    ) -> None:
        """Process moneyline markets and insert records."""
        
        if len(moneyline_data) < 2:
            return
        
        home_entry = next((entry for entry in moneyline_data if entry.get("side") == "home"), None)
        away_entry = next((entry for entry in moneyline_data if entry.get("side") == "away"), None)
        
        if not home_entry or not away_entry:
            return
        
        # Get current odds
        current_home_odds = home_entry.get("odds")
        current_away_odds = away_entry.get("odds")
        
        # Get history
        home_history = home_entry.get("history", [])
        away_history = away_entry.get("history", [])
        
        if home_history and away_history:
            # Process historical data
            await self._insert_moneyline_history(
                game_id, sportsbook_id, sportsbook_name, home_team, away_team,
                game_datetime, action_network_game_id, action_network_book_id,
                home_history, away_history
            )
            self.stats["history_points"] += len(home_history)
        
        elif current_home_odds and current_away_odds:
            # Insert current line only
            await self._insert_single_moneyline(
                game_id, sportsbook_id, sportsbook_name, home_team, away_team,
                game_datetime, action_network_game_id, action_network_book_id,
                current_home_odds, current_away_odds, now_est()
            )
            self.stats["current_lines"] += 1
    
    async def _insert_moneyline_history(
        self, game_id: int, sportsbook_id: int, sportsbook_name: str,
        home_team: str, away_team: str, game_datetime: datetime,
        action_network_game_id: int, action_network_book_id: int,
        home_history: List[Dict], away_history: List[Dict]
    ) -> None:
        """Insert all moneyline history points."""
        
        # Create lookup for away odds by timestamp
        away_odds_lookup = {}
        for away_point in away_history:
            timestamp = away_point.get("updated_at")
            if timestamp:
                away_odds_lookup[timestamp] = away_point.get("odds")
        
        # Process each home history point
        for home_point in home_history:
            home_odds = home_point.get("odds")
            timestamp = home_point.get("updated_at")
            
            if not home_odds or not timestamp:
                continue
            
            # Find matching away odds
            away_odds = away_odds_lookup.get(timestamp)
            if not away_odds:
                continue  # Skip if no matching away odds
            
            # Parse timestamp
            odds_timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            
            await self._insert_single_moneyline(
                game_id, sportsbook_id, sportsbook_name, home_team, away_team,
                game_datetime, action_network_game_id, action_network_book_id,
                home_odds, away_odds, odds_timestamp
            )
    
    async def _insert_single_moneyline(
        self, game_id: int, sportsbook_id: int, sportsbook_name: str,
        home_team: str, away_team: str, game_datetime: datetime,
        action_network_game_id: int, action_network_book_id: int,
        home_odds: int, away_odds: int, odds_timestamp: datetime
    ) -> None:
        """Insert a single moneyline record."""
        
        try:
            conn = await asyncpg.connect(**self.db_config)
            
            # Create unique external source ID with timestamp
            timestamp_str = odds_timestamp.strftime("%Y%m%d_%H%M%S_%f")
            external_source_id = f"AN_{action_network_game_id}_{action_network_book_id}_ML_{timestamp_str}"
            
            await conn.execute("""
                INSERT INTO core_betting.betting_lines_moneyline (
                    game_id, external_source_id, sportsbook_id, sportsbook,
                    home_team, away_team, home_ml, away_ml, odds_timestamp,
                    collection_method, source, game_datetime, created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            """,
            game_id, external_source_id, sportsbook_id, sportsbook_name,
            home_team, away_team, home_odds, away_odds,
            prepare_for_postgres(odds_timestamp), "API", "ACTION_NETWORK",
            prepare_for_postgres(game_datetime), now_est(), prepare_for_postgres(odds_timestamp)
            )
            
            self.stats["moneyline_inserted"] += 1
            self.stats["total_inserted"] += 1
            
            await conn.close()
            
        except Exception as e:
            logger.error("Error inserting moneyline", error=str(e)[:100])
    
    async def _process_spread_markets(
        self, spread_data: List[Dict], game_id: int, sportsbook_id: int,
        sportsbook_name: str, home_team: str, away_team: str,
        game_datetime: datetime, action_network_game_id: int, action_network_book_id: int
    ) -> None:
        """Process spread markets."""
        
        if len(spread_data) < 2:
            return
        
        home_entry = next((entry for entry in spread_data if entry.get("side") == "home"), None)
        
        if not home_entry:
            return
        
        # Get current data
        current_line = home_entry.get("value")
        current_price = home_entry.get("odds")
        
        # Get history
        history = home_entry.get("history", [])
        
        if history:
            # Process historical data
            for hist_point in history:
                spread_line = hist_point.get("value")
                spread_price = hist_point.get("odds")
                timestamp = hist_point.get("updated_at")
                
                if spread_line and spread_price and timestamp:
                    odds_timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    await self._insert_single_spread(
                        game_id, sportsbook_id, sportsbook_name, home_team, away_team,
                        game_datetime, action_network_game_id, action_network_book_id,
                        spread_line, spread_price, odds_timestamp
                    )
            
            self.stats["history_points"] += len(history)
        
        elif current_line and current_price:
            # Insert current line only
            await self._insert_single_spread(
                game_id, sportsbook_id, sportsbook_name, home_team, away_team,
                game_datetime, action_network_game_id, action_network_book_id,
                current_line, current_price, now_est()
            )
            self.stats["current_lines"] += 1
    
    async def _insert_single_spread(
        self, game_id: int, sportsbook_id: int, sportsbook_name: str,
        home_team: str, away_team: str, game_datetime: datetime,
        action_network_game_id: int, action_network_book_id: int,
        spread_line: float, spread_price: int, odds_timestamp: datetime
    ) -> None:
        """Insert a single spread record."""
        
        try:
            conn = await asyncpg.connect(**self.db_config)
            
            timestamp_str = odds_timestamp.strftime("%Y%m%d_%H%M%S_%f")
            external_source_id = f"AN_{action_network_game_id}_{action_network_book_id}_SP_{timestamp_str}"
            
            await conn.execute("""
                INSERT INTO core_betting.betting_lines_spread (
                    game_id, external_source_id, sportsbook_id, sportsbook,
                    home_team, away_team, spread_line, home_spread_price, away_spread_price,
                    odds_timestamp, collection_method, source, game_datetime, created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            """,
            game_id, external_source_id, sportsbook_id, sportsbook_name,
            home_team, away_team, spread_line, spread_price, -110,  # Default away price
            prepare_for_postgres(odds_timestamp), "API", "ACTION_NETWORK",
            prepare_for_postgres(game_datetime), now_est(), prepare_for_postgres(odds_timestamp)
            )
            
            self.stats["spread_inserted"] += 1
            self.stats["total_inserted"] += 1
            
            await conn.close()
            
        except Exception as e:
            logger.error("Error inserting spread", error=str(e)[:100])
    
    async def _process_totals_markets(
        self, totals_data: List[Dict], game_id: int, sportsbook_id: int,
        sportsbook_name: str, home_team: str, away_team: str,
        game_datetime: datetime, action_network_game_id: int, action_network_book_id: int
    ) -> None:
        """Process totals markets."""
        
        if len(totals_data) < 2:
            return
        
        over_entry = next((entry for entry in totals_data if entry.get("side") == "over"), None)
        
        if not over_entry:
            return
        
        # Get current data
        current_total = over_entry.get("value")
        current_price = over_entry.get("odds")
        
        # Get history
        history = over_entry.get("history", [])
        
        if history:
            # Process historical data
            for hist_point in history:
                total_line = hist_point.get("value")
                total_price = hist_point.get("odds")
                timestamp = hist_point.get("updated_at")
                
                if total_line and total_price and timestamp:
                    odds_timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    await self._insert_single_total(
                        game_id, sportsbook_id, sportsbook_name, home_team, away_team,
                        game_datetime, action_network_game_id, action_network_book_id,
                        total_line, total_price, odds_timestamp
                    )
            
            self.stats["history_points"] += len(history)
        
        elif current_total and current_price:
            # Insert current line only
            await self._insert_single_total(
                game_id, sportsbook_id, sportsbook_name, home_team, away_team,
                game_datetime, action_network_game_id, action_network_book_id,
                current_total, current_price, now_est()
            )
            self.stats["current_lines"] += 1
    
    async def _insert_single_total(
        self, game_id: int, sportsbook_id: int, sportsbook_name: str,
        home_team: str, away_team: str, game_datetime: datetime,
        action_network_game_id: int, action_network_book_id: int,
        total_line: float, total_price: int, odds_timestamp: datetime
    ) -> None:
        """Insert a single totals record."""
        
        try:
            conn = await asyncpg.connect(**self.db_config)
            
            timestamp_str = odds_timestamp.strftime("%Y%m%d_%H%M%S_%f")
            external_source_id = f"AN_{action_network_game_id}_{action_network_book_id}_TO_{timestamp_str}"
            
            await conn.execute("""
                INSERT INTO core_betting.betting_lines_totals (
                    game_id, external_source_id, sportsbook_id, sportsbook,
                    home_team, away_team, total_line, over_price, under_price,
                    odds_timestamp, collection_method, source, game_datetime, created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            """,
            game_id, external_source_id, sportsbook_id, sportsbook_name,
            home_team, away_team, total_line, total_price, -110,  # Default under price
            prepare_for_postgres(odds_timestamp), "API", "ACTION_NETWORK",
            prepare_for_postgres(game_datetime), now_est(), prepare_for_postgres(odds_timestamp)
            )
            
            self.stats["totals_inserted"] += 1
            self.stats["total_inserted"] += 1
            
            await conn.close()
            
        except Exception as e:
            logger.error("Error inserting totals", error=str(e)[:100])


async def main():
    """Run the complete line movement collection."""
    
    collector = CompleteHistoryCollector()
    results = await collector.run_complete_collection()
    
    print(f"\n" + "=" * 70)
    print(f"🎯 COLLECTION COMPLETE")
    print(f"=" * 70)
    
    if results["total_inserted"] > 0:
        print(f"🎉 SUCCESS: Inserted {results['total_inserted']} betting line records")
        print(f"\n🔍 Verification Examples:")
        print(f"   -- See all moneyline records for a team:")
        print(f"   SELECT home_team, away_team, sportsbook, home_ml, away_ml, updated_at")
        print(f"   FROM core_betting.betting_lines_moneyline")
        print(f"   WHERE home_team = 'CHC' AND date(game_datetime) = '2025-07-18'")
        print(f"   ORDER BY updated_at;")
        
    else:
        print(f"⚠️  No records inserted - likely no historical data available")
        print(f"   This is normal for future games without line movement")


if __name__ == "__main__":
    asyncio.run(main())