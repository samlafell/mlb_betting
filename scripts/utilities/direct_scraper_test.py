#!/usr/bin/env python3
"""
Direct test of SportsbookReview scraper with storage for July 7th.
"""

import asyncio
import sys
from datetime import date
from pathlib import Path

# Add the sportsbookreview module to the path
sys.path.append(str(Path(__file__).parent / "sportsbookreview"))

from sportsbookreview.services.data_storage_service import DataStorageService
from sportsbookreview.services.sportsbookreview_scraper import SportsbookReviewScraper


async def test_direct_scraper():
    """Test the scraper directly with storage."""
    print("=== DIRECT SCRAPER TEST FOR JULY 7TH ===")

    target_date = date(2025, 7, 7)

    try:
        # Initialize storage service
        storage = DataStorageService()
        await storage.initialize_connection()

        # Initialize scraper with storage
        async with SportsbookReviewScraper(storage_service=storage) as scraper:
            print("✅ Scraper initialized with storage")

            # Test connectivity
            print("Testing connectivity...")
            if await scraper.test_connectivity():
                print("✅ Connectivity test passed")
            else:
                print("❌ Connectivity test failed")
                return

            # Scrape all bet types for July 7th
            print(f"Scraping all bet types for {target_date}...")
            await scraper.scrape_date_all_bet_types(target_date)

            print("✅ Scraping completed")

            # Get scraper stats
            stats = scraper.get_stats()
            print(f"Scraper stats: {stats}")

        # Close storage
        await storage.close_connection()

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


async def check_staging_data():
    """Check if data was stored in staging."""
    print("\n=== CHECKING STAGING DATA ===")

    from src.mlb_sharp_betting.db.connection import get_db_manager

    db = get_db_manager()

    try:
        # Check staging data for July 7th
        result = db.execute_query(
            "SELECT COUNT(*) as count FROM sportsbookreview.sbr_parsed_games WHERE DATE(parsed_at) = %s",
            ("2025-07-07",),
        )
        print(f"July 7th staging records: {result[0]['count']}")

        if result[0]["count"] > 0:
            # Show sample staging data
            sample = db.execute_query(
                "SELECT id, game_data FROM sportsbookreview.sbr_parsed_games WHERE DATE(parsed_at) = %s LIMIT 3",
                ("2025-07-07",),
            )
            print("Sample staging records:")
            for row in sample:
                game_data = row["game_data"]
                if isinstance(game_data, dict):
                    print(
                        f"  ID {row['id']}: {game_data.get('away_team', 'Unknown')} @ {game_data.get('home_team', 'Unknown')}"
                    )
                else:
                    print(f"  ID {row['id']}: {str(game_data)[:100]}...")

    except Exception as e:
        print(f"Error checking staging: {e}")


async def main():
    """Main function."""
    print("Direct SportsbookReview Scraper Test")
    print("=" * 50)

    # Test direct scraper
    await test_direct_scraper()

    # Check staging data
    await check_staging_data()

    print("\n✅ Test completed!")


if __name__ == "__main__":
    asyncio.run(main())
