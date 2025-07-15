#!/usr/bin/env python3
"""
Script to check database state and run SportsbookReview scraper with storage for July 7th.
"""

import asyncio
import sys
from pathlib import Path

# Add the sportsbookreview module to the path
sys.path.append(str(Path(__file__).parent / "sportsbookreview"))

from sportsbookreview.services.data_storage_service import DataStorageService
from sportsbookreview.services.sportsbookreview_scraper import SportsbookReviewScraper
from src.mlb_sharp_betting.db.connection import get_db_manager


async def check_database_state():
    """Check current database state."""
    print("=== CHECKING DATABASE STATE ===")

    db = get_db_manager()

    try:
        # Check moneyline table
        moneyline_count = db.execute_query(
            "SELECT COUNT(*) as count FROM mlb_betting.moneyline"
        )
        print(f"Moneyline records: {moneyline_count[0]['count']}")

        # Check recent records
        recent_moneyline = db.execute_query(
            "SELECT game_date, home_team, away_team, home_ml, away_ml FROM mlb_betting.moneyline ORDER BY game_date DESC LIMIT 5"
        )
        print("Recent moneyline records:")
        for row in recent_moneyline:
            print(
                f"  {row['game_date']}: {row['away_team']} @ {row['home_team']} - {row['home_ml']}/{row['away_ml']}"
            )

        # Check July 7th data
        july_7_count = db.execute_query(
            "SELECT COUNT(*) as count FROM mlb_betting.moneyline WHERE game_date = %s",
            ("2025-07-07",),
        )
        print(f"July 7th moneyline records: {july_7_count[0]['count']}")

        # Check spreads table
        spreads_count = db.execute_query(
            "SELECT COUNT(*) as count FROM mlb_betting.spreads"
        )
        print(f"Spreads records: {spreads_count[0]['count']}")

        # Check totals table
        totals_count = db.execute_query(
            "SELECT COUNT(*) as count FROM mlb_betting.totals"
        )
        print(f"Totals records: {totals_count[0]['count']}")

        # Check staging data
        staging_count = db.execute_query(
            "SELECT COUNT(*) as count FROM sportsbookreview.sbr_parsed_games"
        )
        print(f"Staging records: {staging_count[0]['count']}")

        print()

    except Exception as e:
        print(f"Error checking database: {e}")
        return False

    return True


async def run_scraper_with_storage():
    """Run the scraper with storage service for July 7th."""
    print("=== RUNNING SCRAPER WITH STORAGE ===")

    # Initialize storage service
    storage_service = DataStorageService()

    # Test URLs for July 7th
    test_urls = [
        "https://www.sportsbookreview.com/betting-odds/mlb-baseball/?date=2025-07-07",
        "https://www.sportsbookreview.com/betting-odds/mlb-baseball/pointspread/full-game/?date=2025-07-07",
        "https://www.sportsbookreview.com/betting-odds/mlb-baseball/totals/full-game/?date=2025-07-07",
    ]

    try:
        # Initialize scraper with storage
        async with SportsbookReviewScraper(storage_service=storage_service) as scraper:
            print("Scraper initialized with storage service")

            # Process each URL
            for url in test_urls:
                print(f"\nProcessing: {url}")

                try:
                    # Scrape and store data
                    result = await scraper.scrape_url(url)

                    if result:
                        print(f"  ✅ Successfully processed {len(result)} games")

                        # Show sample data
                        if result:
                            sample_game = result[0]
                            print(
                                f"  Sample game: {sample_game.get('away_team', 'Unknown')} @ {sample_game.get('home_team', 'Unknown')}"
                            )
                            if "odds_data" in sample_game:
                                odds = sample_game["odds_data"]
                                print(f"  Sample odds: {odds}")
                    else:
                        print("  ❌ No data returned")

                except Exception as e:
                    print(f"  ❌ Error processing URL: {e}")

    except Exception as e:
        print(f"Error running scraper: {e}")
        return False

    return True


async def main():
    """Main function."""
    print("SportsbookReview Data Collection Test")
    print("=" * 50)

    # Check database state before
    if not await check_database_state():
        print("Failed to check database state")
        return

    # Run scraper with storage
    if not await run_scraper_with_storage():
        print("Failed to run scraper with storage")
        return

    print("\n=== CHECKING DATABASE STATE AFTER SCRAPING ===")

    # Check database state after
    await check_database_state()

    print("\n✅ Test completed!")


if __name__ == "__main__":
    asyncio.run(main())
