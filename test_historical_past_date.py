#!/usr/bin/env python3
"""
Test Historical Line Movement with Past Date

Test Action Network historical data availability with a recent past date
to verify the API does provide historical line movements.
"""

import asyncio
import json
from datetime import datetime, timedelta

import aiohttp
import structlog

# Configure logging
structlog.configure(
    processors=[structlog.dev.ConsoleRenderer()],
    wrapper_class=structlog.make_filtering_bound_logger(20),
    logger_factory=structlog.WriteLoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()


async def test_historical_data_availability():
    """Test historical data availability across different dates."""
    
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.actionnetwork.com/",
        "Origin": "https://www.actionnetwork.com",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    
    # Test multiple dates to understand data availability
    test_dates = [
        # Recent past dates that should have games and history
        "20250717",  # July 17th (yesterday from our target)
        "20250716",  # July 16th
        "20250715",  # July 15th
        "20250714",  # July 14th
        "20250713",  # July 13th
    ]
    
    print("🧪 TESTING: Historical Data Availability Across Dates")
    print("=" * 70)
    
    async with aiohttp.ClientSession(headers=headers) as session:
        
        for date_str in test_dates:
            print(f"\n📅 Testing date: {date_str}")
            
            url = "https://api.actionnetwork.com/web/v2/scoreboard/publicbetting/mlb"
            params = {
                "bookIds": "15,30,75,123,69,68,972,71,247,79",
                "date": date_str,
                "periods": "event",
            }
            
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        games = data.get("games", [])
                        
                        if not games:
                            print(f"   ❌ No games found for {date_str}")
                            continue
                        
                        print(f"   ✅ Found {len(games)} games")
                        
                        # Analyze historical data in first game
                        sample_game = games[0]
                        teams = sample_game.get("teams", [])
                        game_name = "Unknown Game"
                        if len(teams) >= 2:
                            game_name = f"{teams[0].get('full_name', 'Team1')} @ {teams[1].get('full_name', 'Team2')}"
                        
                        print(f"   🎯 Sample game: {game_name}")
                        
                        markets = sample_game.get("markets", {})
                        total_markets = 0
                        markets_with_history = 0
                        total_history_points = 0
                        
                        # Check first sportsbook's data
                        for book_id, book_data in list(markets.items())[:3]:  # Check first 3 books
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
                                        
                                        # Show sample history
                                        if markets_with_history == 1:
                                            print(f"      📊 Sample {market_type} {entry.get('side')} (Book {book_id}):")
                                            print(f"         Current: {entry.get('value')} @ {entry.get('odds'):+d}")
                                            print(f"         History: {len(history)} points")
                                            
                                            if len(history) >= 2:
                                                first = history[0]
                                                last = history[-1]
                                                print(f"         First: {first.get('value')} @ {first.get('odds'):+d} | {first.get('updated_at')}")
                                                print(f"         Last:  {last.get('value')} @ {last.get('odds'):+d} | {last.get('updated_at')}")
                        
                        history_pct = (markets_with_history / total_markets * 100) if total_markets > 0 else 0
                        print(f"   📈 History coverage: {markets_with_history}/{total_markets} markets ({history_pct:.1f}%)")
                        print(f"   📊 Total history points: {total_history_points}")
                        
                        # If we found history, save sample for analysis
                        if total_history_points > 0:
                            sample_file = f"sample_history_{date_str}.json"
                            with open(sample_file, 'w') as f:
                                json.dump(sample_game, f, indent=2)
                            print(f"   💾 Saved sample to {sample_file}")
                            break  # Found working date
                        
                    else:
                        print(f"   ❌ API error for {date_str}: {response.status}")
                        
            except Exception as e:
                print(f"   ❌ Request failed for {date_str}: {str(e)}")
        
        print(f"\n🎯 CONCLUSION:")
        print(f"   Historical line movement data availability depends on:")
        print(f"   • Game timing (past vs future)")
        print(f"   • Data freshness")
        print(f"   • API limitations")
        print(f"   For production, focus on live/recent game collection.")


if __name__ == "__main__":
    asyncio.run(test_historical_data_availability())