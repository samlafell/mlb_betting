#!/usr/bin/env python3
"""
Debug the scraper with storage to see what happens to the parsed data.
"""

import asyncio
import sys
from datetime import date
from pathlib import Path

# Add the sportsbookreview module to the path
sys.path.append(str(Path(__file__).parent / "sportsbookreview"))

from sportsbookreview.services.sportsbookreview_scraper import SportsbookReviewScraper
from sportsbookreview.services.data_storage_service import DataStorageService


async def debug_scraper_storage():
    """Debug the scraper with storage enabled."""
    print("=== DEBUGGING SCRAPER WITH STORAGE ===")
    
    target_date = date(2025, 7, 7)
    
    try:
        # Initialize storage service
        storage = DataStorageService()
        await storage.initialize_connection()
        print("✅ Storage service initialized")
        
        # Initialize scraper with storage
        scraper = SportsbookReviewScraper(storage_service=storage)
        await scraper.start_session()
        print("✅ Scraper initialized with storage")
        
        # Test connectivity
        if await scraper.test_connectivity():
            print("✅ Connectivity test passed")
        else:
            print("❌ Connectivity test failed")
            return
        
        # Create a custom method to intercept and log the data flow
        original_store_raw_html = storage.store_raw_html
        original_store_parsed_game = storage.store_parsed_game
        
        async def debug_store_raw_html(url, html_content, scraped_at):
            print(f"📝 Storing raw HTML: {url} ({len(html_content)} chars)")
            return await original_store_raw_html(url, html_content, scraped_at)
            
        async def debug_store_parsed_game(game_data):
            print(f"📝 Storing parsed game: {game_data.get('sbr_game_id', 'N/A')} - {game_data.get('away_team', 'N/A')} @ {game_data.get('home_team', 'N/A')}")
            print(f"   Odds data: {len(game_data.get('odds_data', []))} sportsbooks")
            return await original_store_parsed_game(game_data)
        
        # Patch the storage methods
        storage.store_raw_html = debug_store_raw_html
        storage.store_parsed_game = debug_store_parsed_game
        
        # Scrape July 7th data
        print(f"\n🔍 Scraping {target_date}...")
        await scraper.scrape_date_all_bet_types(target_date)
        
        print("✅ Scraping completed")
        
        # Get scraper stats
        stats = scraper.get_stats()
        print(f"📊 Scraper stats: {stats}")
        
        # Get storage stats
        storage_stats = storage.get_storage_stats()
        print(f"📊 Storage stats: {storage_stats}")
        
        # Close connections
        await scraper.close_session()
        await storage.close_connection()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


async def check_results():
    """Check what data was actually stored."""
    print("\n=== CHECKING STORED DATA ===")
    
    from src.mlb_sharp_betting.db.connection import get_db_manager
    db = get_db_manager()
    
    try:
        # Check raw HTML
        raw_html = db.execute_query(
            'SELECT COUNT(*) as count FROM public.sbr_raw_html WHERE DATE(scraped_at) = %s', 
            ('2025-07-07',)
        )
        print(f"Raw HTML records: {raw_html[0]['count']}")
        
        # Check parsed games
        parsed_games = db.execute_query(
            'SELECT COUNT(*) as count FROM public.sbr_parsed_games WHERE DATE(parsed_at) = %s', 
            ('2025-07-07',)
        )
        print(f"Parsed games: {parsed_games[0]['count']}")
        
        # Check final betting data
        moneyline = db.execute_query(
            'SELECT COUNT(*) as count FROM mlb_betting.moneyline WHERE DATE(created_at) = %s', 
            ('2025-07-07',)
        )
        print(f"Moneyline records: {moneyline[0]['count']}")
        
    except Exception as e:
        print(f"Error checking results: {e}")


async def main():
    """Main function."""
    print("Debug Scraper Storage for July 7th")
    print("=" * 50)
    
    await debug_scraper_storage()
    await check_results()
    
    print("\n✅ Debug completed!")


if __name__ == "__main__":
    asyncio.run(main()) 