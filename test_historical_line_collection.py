#!/usr/bin/env python3
"""
Test Historical Line Movement Collection for July 18th, 2025

This script tests the enhanced Action Network collection that captures
historical line movements from the 'history' arrays.
"""

import asyncio
import json
from datetime import datetime
from typing import Dict, List, Any

import aiohttp
import asyncpg
import structlog

# Configure logging
structlog.configure(
    processors=[
        structlog.dev.ConsoleRenderer()
    ],
    wrapper_class=structlog.make_filtering_bound_logger(20),  # INFO level
    logger_factory=structlog.WriteLoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


class HistoricalLineCollectionTest:
    """Test historical line movement collection from Action Network."""
    
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
    
    async def run_test(self) -> Dict[str, Any]:
        """Run comprehensive test of historical line collection."""
        
        print("🧪 TESTING: Historical Line Movement Collection for July 18th, 2025")
        print("=" * 80)
        print("🎯 Testing Goals:")
        print("   • Capture historical line movements from Action Network")
        print("   • Store all history points with timestamps")
        print("   • Validate opening vs closing line analysis")
        print("   • Test database integration")
        print()
        
        # Step 1: Fetch and analyze raw data
        print("📡 Step 1: Fetching Action Network data with history...")
        games_data = await self._fetch_games_with_history()
        
        if not games_data:
            print("❌ No games data found")
            return {}
        
        print(f"   ✅ Found {len(games_data)} games")
        
        # Step 2: Analyze historical data availability
        print("\n📊 Step 2: Analyzing historical data availability...")
        history_analysis = await self._analyze_historical_data(games_data)
        
        # Step 3: Extract and process history
        print("\n🔄 Step 3: Processing historical line movements...")
        movements = await self._extract_line_movements(games_data)
        
        # Step 4: Insert into database
        print("\n💾 Step 4: Inserting historical movements into database...")
        insertion_results = await self._insert_movements(movements)
        
        # Step 5: Validate results
        print("\n✅ Step 5: Validating historical data capture...")
        validation_results = await self._validate_historical_capture()
        
        return {
            "games_processed": len(games_data),
            "history_analysis": history_analysis,
            "movements_extracted": len(movements),
            "insertion_results": insertion_results,
            "validation_results": validation_results
        }
    
    async def _fetch_games_with_history(self) -> List[Dict[str, Any]]:
        """Fetch games data from Action Network API."""
        
        try:
            async with aiohttp.ClientSession(headers=self.headers) as session:
                url = f"{self.api_base}/web/v2/scoreboard/publicbetting/mlb"
                params = {
                    "bookIds": "15,30,75,123,69,68,972,71,247,79",
                    "date": "20250718",  # July 18th, 2025
                    "periods": "event",
                }
                
                logger.info("Fetching Action Network data", url=url, params=params)
                
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data.get("games", [])
                    else:
                        logger.error("API request failed", status=response.status)
                        return []
        
        except Exception as e:
            logger.error("Failed to fetch Action Network data", error=str(e))
            return []
    
    async def _analyze_historical_data(self, games: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze availability of historical data."""
        
        total_markets = 0
        markets_with_history = 0
        total_history_points = 0
        sportsbooks_with_history = set()
        
        sample_histories = []
        
        for game in games:
            game_id = game.get("id")
            teams = game.get("teams", [])
            if len(teams) >= 2:
                game_info = f"{teams[0].get('full_name', 'Unknown')} @ {teams[1].get('full_name', 'Unknown')}"
            else:
                game_info = f"Game {game_id}"
            
            markets = game.get("markets", {})
            
            for book_id_str, book_data in markets.items():
                book_id = int(book_id_str)
                event_markets = book_data.get("event", {})
                
                for market_type, market_entries in event_markets.items():
                    if not isinstance(market_entries, list):
                        continue
                    
                    for entry in market_entries:
                        total_markets += 1
                        history = entry.get("history", [])
                        
                        if history:
                            markets_with_history += 1
                            total_history_points += len(history)
                            sportsbooks_with_history.add(book_id)
                            
                            # Collect sample for detailed analysis
                            if len(sample_histories) < 5:
                                sample_histories.append({
                                    "game": game_info,
                                    "book_id": book_id,
                                    "market_type": market_type,
                                    "side": entry.get("side"),
                                    "current_odds": entry.get("odds"),
                                    "current_value": entry.get("value"),
                                    "history_count": len(history),
                                    "first_entry": history[0] if history else None,
                                    "last_entry": history[-1] if history else None
                                })
        
        history_coverage_pct = (markets_with_history / total_markets * 100) if total_markets > 0 else 0
        
        print(f"   📊 Historical data analysis:")
        print(f"      • Total markets: {total_markets}")
        print(f"      • Markets with history: {markets_with_history} ({history_coverage_pct:.1f}%)")
        print(f"      • Total history points: {total_history_points}")
        print(f"      • Sportsbooks with history: {len(sportsbooks_with_history)}")
        print(f"      • Avg history per market: {total_history_points/markets_with_history:.1f}" if markets_with_history > 0 else "      • No markets with history")
        
        # Show sample histories
        if sample_histories:
            print(f"\n   🔍 Sample historical data:")
            for i, sample in enumerate(sample_histories[:3]):
                print(f"      {i+1}. {sample['game']} | Book {sample['book_id']} | {sample['market_type']} {sample['side']}")
                print(f"         Current: {sample['current_value']} @ {sample['current_odds']:+d}")
                print(f"         History: {sample['history_count']} points")
                if sample['first_entry'] and sample['last_entry']:
                    first = sample['first_entry']
                    last = sample['last_entry']
                    print(f"         Opening: {first.get('value')} @ {first.get('odds'):+d} | {first.get('updated_at')}")
                    print(f"         Latest:  {last.get('value')} @ {last.get('odds'):+d} | {last.get('updated_at')}")
        
        return {
            "total_markets": total_markets,
            "markets_with_history": markets_with_history,
            "history_coverage_pct": history_coverage_pct,
            "total_history_points": total_history_points,
            "sportsbooks_with_history": len(sportsbooks_with_history),
            "sample_histories": sample_histories
        }
    
    async def _extract_line_movements(self, games: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract line movements from games data."""
        
        movements = []
        
        for game in games:
            game_id = game.get("id")
            teams = game.get("teams", [])
            start_time = game.get("start_time")
            
            if len(teams) < 2 or not game_id or not start_time:
                continue
            
            away_team = teams[0].get("full_name", "").split()[-1]  # Get team abbreviation
            home_team = teams[1].get("full_name", "").split()[-1]
            game_datetime = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            
            markets = game.get("markets", {})
            
            for book_id_str, book_data in markets.items():
                book_id = int(book_id_str)
                event_markets = book_data.get("event", {})
                
                for market_type, market_entries in event_markets.items():
                    if not isinstance(market_entries, list):
                        continue
                    
                    for entry in market_entries:
                        history = entry.get("history", [])
                        
                        if not history:
                            continue
                        
                        # Map Action Network types to our types
                        bet_type_mapping = {
                            "total": "total",
                            "spread": "spread",
                            "moneyline": "moneyline",
                            "h2h": "moneyline"
                        }
                        
                        bet_type = bet_type_mapping.get(market_type, market_type)
                        side = entry.get("side", "unknown")
                        
                        # Process each history point
                        for hist_point in history:
                            movement = {
                                "action_network_game_id": game_id,
                                "action_network_book_id": book_id,
                                "outcome_id": entry.get("outcome_id"),
                                "market_id": entry.get("market_id"),
                                "bet_type": bet_type,
                                "side": side,
                                "odds": hist_point.get("odds"),
                                "line_value": hist_point.get("value"),
                                "line_timestamp": hist_point.get("updated_at"),
                                "home_team": home_team,
                                "away_team": away_team,
                                "game_datetime": start_time
                            }
                            movements.append(movement)
        
        print(f"   ✅ Extracted {len(movements)} line movement records")
        return movements
    
    async def _insert_movements(self, movements: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Insert line movements into database."""
        
        if not movements:
            return {"inserted": 0, "errors": 0}
        
        try:
            conn = await asyncpg.connect(**self.db_config)
            
            # First, create any missing games
            games_created = await self._ensure_games_exist(conn, movements)
            
            # Get sportsbook mappings
            sportsbook_mappings = await self._get_sportsbook_mappings(conn)
            
            inserted_count = 0
            error_count = 0
            
            # Insert movements in batches
            for movement in movements:
                try:
                    # Get internal IDs
                    game_id = await self._get_game_id(conn, movement["action_network_game_id"])
                    sportsbook_id = sportsbook_mappings.get(movement["action_network_book_id"])
                    
                    if not game_id or not sportsbook_id:
                        error_count += 1
                        continue
                    
                    # Parse timestamp
                    line_timestamp = datetime.fromisoformat(movement["line_timestamp"].replace('Z', '+00:00'))
                    game_datetime = datetime.fromisoformat(movement["game_datetime"].replace('Z', '+00:00'))
                    
                    await conn.execute("""
                        INSERT INTO core_betting.line_movement_history (
                            game_id, sportsbook_id, action_network_game_id, action_network_book_id,
                            outcome_id, market_id, bet_type, side, odds, line_value,
                            line_timestamp, home_team, away_team, game_datetime, source
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                        ON CONFLICT (action_network_game_id, action_network_book_id, bet_type, side, line_timestamp) 
                        DO NOTHING
                    """,
                    game_id, sportsbook_id, movement["action_network_game_id"], movement["action_network_book_id"],
                    movement["outcome_id"], movement["market_id"], movement["bet_type"], movement["side"],
                    movement["odds"], movement["line_value"], line_timestamp, movement["home_team"],
                    movement["away_team"], game_datetime, "ACTION_NETWORK"
                    )
                    
                    inserted_count += 1
                    
                except Exception as e:
                    error_count += 1
                    if error_count <= 5:  # Log first few errors
                        logger.error("Error inserting movement", error=str(e))
            
            await conn.close()
            
            print(f"   ✅ Inserted: {inserted_count} movements")
            print(f"   ⚠️  Errors: {error_count} movements")
            
            return {
                "inserted": inserted_count,
                "errors": error_count,
                "games_created": games_created
            }
        
        except Exception as e:
            logger.error("Database insertion failed", error=str(e))
            return {"inserted": 0, "errors": len(movements)}
    
    async def _ensure_games_exist(self, conn: asyncpg.Connection, movements: List[Dict[str, Any]]) -> int:
        """Ensure all games exist in database."""
        
        unique_games = {}
        for movement in movements:
            game_id = movement["action_network_game_id"]
            if game_id not in unique_games:
                unique_games[game_id] = {
                    "home_team": movement["home_team"],
                    "away_team": movement["away_team"],
                    "game_datetime": movement["game_datetime"]
                }
        
        games_created = 0
        
        for game_id, game_info in unique_games.items():
            existing = await conn.fetchval("""
                SELECT id FROM core_betting.games WHERE action_network_game_id = $1
            """, game_id)
            
            if not existing:
                game_datetime = datetime.fromisoformat(game_info["game_datetime"].replace('Z', '+00:00'))
                
                await conn.execute("""
                    INSERT INTO core_betting.games (
                        action_network_game_id, home_team, away_team, game_date, game_datetime,
                        season, season_type, game_type, data_quality, created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,
                game_id, game_info["home_team"], game_info["away_team"], game_datetime.date(),
                game_datetime, 2025, "REG", "R", "HIGH", datetime.now(), datetime.now()
                )
                games_created += 1
        
        return games_created
    
    async def _get_sportsbook_mappings(self, conn: asyncpg.Connection) -> Dict[int, int]:
        """Get Action Network to internal sportsbook ID mappings."""
        
        mappings = await conn.fetch("""
            SELECT external_id::integer as action_network_id, sportsbook_id
            FROM core_betting.sportsbook_external_mappings
            WHERE external_source = 'ACTION_NETWORK'
        """)
        
        return {mapping["action_network_id"]: mapping["sportsbook_id"] for mapping in mappings}
    
    async def _get_game_id(self, conn: asyncpg.Connection, action_network_game_id: int) -> int:
        """Get internal game ID from Action Network game ID."""
        
        return await conn.fetchval("""
            SELECT id FROM core_betting.games WHERE action_network_game_id = $1
        """, action_network_game_id)
    
    async def _validate_historical_capture(self) -> Dict[str, Any]:
        """Validate that historical data was captured correctly."""
        
        try:
            conn = await asyncpg.connect(**self.db_config)
            
            # Basic counts
            total_movements = await conn.fetchval("""
                SELECT COUNT(*) FROM core_betting.line_movement_history 
                WHERE DATE(game_datetime) = '2025-07-18'
            """)
            
            games_with_history = await conn.fetchval("""
                SELECT COUNT(DISTINCT game_id) FROM core_betting.line_movement_history 
                WHERE DATE(game_datetime) = '2025-07-18'
            """)
            
            sportsbooks_with_history = await conn.fetchval("""
                SELECT COUNT(DISTINCT sportsbook_id) FROM core_betting.line_movement_history 
                WHERE DATE(game_datetime) = '2025-07-18'
            """)
            
            # Sample opening vs closing analysis
            sample_movements = await conn.fetch("""
                SELECT 
                    lmh.home_team,
                    lmh.away_team,
                    s.display_name as sportsbook,
                    lmh.bet_type,
                    lmh.side,
                    COUNT(*) as movement_count,
                    MIN(lmh.line_timestamp) as first_timestamp,
                    MAX(lmh.line_timestamp) as last_timestamp,
                    (ARRAY_AGG(lmh.odds ORDER BY lmh.line_timestamp))[1] as opening_odds,
                    (ARRAY_AGG(lmh.odds ORDER BY lmh.line_timestamp DESC))[1] as closing_odds
                FROM core_betting.line_movement_history lmh
                JOIN core_betting.sportsbooks s ON lmh.sportsbook_id = s.id
                WHERE DATE(lmh.game_datetime) = '2025-07-18'
                GROUP BY lmh.home_team, lmh.away_team, s.display_name, lmh.bet_type, lmh.side
                HAVING COUNT(*) > 1
                ORDER BY movement_count DESC
                LIMIT 5
            """)
            
            await conn.close()
            
            print(f"   📊 Historical data validation:")
            print(f"      • Total movements captured: {total_movements}")
            print(f"      • Games with history: {games_with_history}")
            print(f"      • Sportsbooks with history: {sportsbooks_with_history}")
            
            if sample_movements:
                print(f"   🎯 Sample opening vs closing analysis:")
                for movement in sample_movements:
                    odds_change = movement["closing_odds"] - movement["opening_odds"]
                    direction = "📈" if odds_change > 0 else "📉" if odds_change < 0 else "➡️"
                    print(f"      {direction} {movement['away_team']} @ {movement['home_team']} | {movement['sportsbook']}")
                    print(f"         {movement['bet_type']} {movement['side']}: {movement['opening_odds']:+d} → {movement['closing_odds']:+d} ({movement['movement_count']} moves)")
            
            return {
                "total_movements": total_movements,
                "games_with_history": games_with_history,
                "sportsbooks_with_history": sportsbooks_with_history,
                "sample_movements": len(sample_movements)
            }
        
        except Exception as e:
            logger.error("Validation failed", error=str(e))
            return {}


async def main():
    """Run the historical line movement collection test."""
    
    tester = HistoricalLineCollectionTest()
    results = await tester.run_test()
    
    print("\n" + "=" * 80)
    print("🎯 HISTORICAL LINE MOVEMENT TEST RESULTS")
    print("=" * 80)
    
    if results:
        print(f"📊 Summary:")
        print(f"   • Games processed: {results.get('games_processed', 0)}")
        print(f"   • Movements extracted: {results.get('movements_extracted', 0)}")
        
        insertion = results.get('insertion_results', {})
        print(f"   • Movements inserted: {insertion.get('inserted', 0)}")
        print(f"   • Insertion errors: {insertion.get('errors', 0)}")
        print(f"   • Games created: {insertion.get('games_created', 0)}")
        
        validation = results.get('validation_results', {})
        if validation:
            print(f"   • Final movements in DB: {validation.get('total_movements', 0)}")
            print(f"   • Games with history: {validation.get('games_with_history', 0)}")
        
        # Success assessment
        movements_inserted = insertion.get('inserted', 0)
        if movements_inserted > 100:
            print("\n🎉 TEST PASSED - Historical line movement capture working!")
        elif movements_inserted > 0:
            print("\n⚠️  TEST PARTIAL - Some historical data captured, investigate coverage")
        else:
            print("\n❌ TEST FAILED - No historical movements captured")
    
    else:
        print("❌ TEST FAILED - No results generated")


if __name__ == "__main__":
    asyncio.run(main())