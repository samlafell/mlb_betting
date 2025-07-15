#!/usr/bin/env python3
"""
Debug the storage methods directly with July 7th data.
"""

import asyncio
import sys
from pathlib import Path

import aiohttp

# Add the sportsbookreview module to the path
sys.path.append(str(Path(__file__).parent / "sportsbookreview"))

from sportsbookreview.parsers.sportsbookreview_parser import SportsbookReviewParser
from sportsbookreview.services.data_storage_service import DataStorageService


async def debug_storage_methods():
    """Debug the storage methods directly."""
    print("=== DEBUGGING STORAGE METHODS ===")

    url = "https://www.sportsbookreview.com/betting-odds/mlb-baseball/?date=2025-07-07"

    try:
        # Initialize storage service
        storage = DataStorageService()
        await storage.initialize_connection()
        print("✅ Storage service initialized")

        # Fetch July 7th HTML
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                html_content = await response.text()
                print(f"✅ Fetched HTML: {len(html_content)} characters")

        # Test storing raw HTML
        print("\n--- Testing store_raw_html ---")
        raw_html_id = await storage.store_raw_html(url, html_content)
        if raw_html_id:
            print(f"✅ Stored raw HTML with ID: {raw_html_id}")
        else:
            print("❌ Failed to store raw HTML")
            return

        # Parse the HTML
        print("\n--- Testing parser ---")
        parser = SportsbookReviewParser()
        parsed_games = parser.parse_page(html_content, url)
        print(f"✅ Parsed {len(parsed_games)} games")

        # Test storing parsed data
        print("\n--- Testing store_parsed_data ---")
        await storage.store_parsed_data(raw_html_id, parsed_games)
        print("✅ Stored parsed data")

        # Close storage
        await storage.close_connection()

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


async def check_stored_data():
    """Check what data was actually stored."""
    print("\n=== CHECKING STORED DATA ===")

    from src.mlb_sharp_betting.db.connection import get_db_manager

    db = get_db_manager()

    try:
        # Check July 7th raw HTML data
        raw_html = db.execute_query(
            "SELECT COUNT(*) as count FROM public.sbr_raw_html WHERE DATE(scraped_at) = %s",
            ("2025-07-07",),
        )
        print(f"July 7th raw HTML records: {raw_html[0]['count']}")

        # Check July 7th staging data
        staging = db.execute_query(
            "SELECT COUNT(*) as count FROM public.sbr_parsed_games WHERE DATE(parsed_at) = %s",
            ("2025-07-07",),
        )
        print(f"July 7th staging records: {staging[0]['count']}")

        # If we have staging data, show a sample
        if staging[0]["count"] > 0:
            sample = db.execute_query(
                "SELECT id, game_data FROM public.sbr_parsed_games WHERE DATE(parsed_at) = %s LIMIT 1",
                ("2025-07-07",),
            )
            if sample:
                import json

                game_data = sample[0]["game_data"]
                if isinstance(game_data, str):
                    game_data = json.loads(game_data)
                print(
                    f"Sample game: {game_data.get('away_team', 'N/A')} @ {game_data.get('home_team', 'N/A')}"
                )

    except Exception as e:
        print(f"Error checking stored data: {e}")


async def main():
    """Main function."""
    print("Debug Storage Methods with July 7th Data")
    print("=" * 50)

    await debug_storage_methods()
    await check_stored_data()

    print("\n✅ Debug completed!")


if __name__ == "__main__":
    asyncio.run(main())
