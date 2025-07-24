#!/usr/bin/env python3
"""
Test script for SportsbookReview collection for July 7th, 2025.

This script tests the complete pipeline for a single historical date
to validate the backfill process before running larger collections.

Usage:
    python test_july_7th_collection.py
"""

import asyncio
import logging
import sys
from datetime import date, datetime
from pathlib import Path

# Add project paths
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Import SportsbookReview components
from sportsbookreview.services.collection_orchestrator import CollectionOrchestrator


async def test_july_7th_collection():
    """Test collection for July 7th, 2025."""

    print("🏀 Testing SportsbookReview Collection for July 7th, 2025")
    print("=" * 60)

    # Set target date
    target_date = date(2025, 7, 7)
    output_dir = Path("./test_output")

    print(f"📅 Target Date: {target_date}")
    print(f"📁 Output Directory: {output_dir}")
    print()

    # Progress callback to show detailed progress
    def progress_callback(progress: float, message: str):
        print(f"📊 Progress: {progress:6.2f}% - {message}")

    try:
        # Initialize collection orchestrator
        print("🔧 Initializing Collection Orchestrator...")
        async with CollectionOrchestrator(
            output_dir=output_dir,
            checkpoint_interval=5,  # Small interval for testing
            enable_checkpoints=True,
            max_retries=3,
        ) as orchestrator:
            print("✅ Collection orchestrator initialized")
            print()

            # Run collection for July 7th only
            print(f"🚀 Starting collection for {target_date}...")
            start_time = datetime.now()

            results = await orchestrator.collect_date_range(
                start_date=target_date,
                end_date=target_date,  # Same date for single day
                progress_callback=progress_callback,
            )

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            print()
            print("✅ Collection completed!")
            print(f"⏱️  Total Duration: {duration:.2f} seconds")
            print()

            # Display results summary
            print("📊 COLLECTION SUMMARY")
            print("-" * 40)

            if "collection_period" in results:
                period = results["collection_period"]
                print(f"Duration: {period.get('duration_seconds', 0):.2f} seconds")

            if "scraping_results" in results:
                scraping = results["scraping_results"]
                print(f"Pages Scraped: {scraping.get('pages_scraped', 0)}")
                print(f"Pages Failed: {scraping.get('pages_failed', 0)}")
                print(
                    f"Scraping Success Rate: {scraping.get('success_rate_percent', 0):.1f}%"
                )

            if "storage_results" in results:
                storage = results["storage_results"]
                print(f"Games Processed: {storage.get('games_processed', 0)}")
                print(f"Games Stored: {storage.get('games_stored', 0)}")
                print(f"Betting Records: {storage.get('betting_records_stored', 0)}")
                print(
                    f"Storage Success Rate: {storage.get('success_rate_percent', 0):.1f}%"
                )

            if "error_summary" in results:
                errors = results["error_summary"]
                print(f"Total Errors: {errors.get('total_errors', 0)}")
                print(f"Failed URLs: {errors.get('failed_urls', 0)}")
                print(f"Error Rate: {errors.get('error_rate_percent', 0):.1f}%")

            print()

            # Validate results
            if results.get("storage_results", {}).get("games_stored", 0) > 0:
                print("✅ SUCCESS: Games were successfully collected and stored!")

                # Check database for the results
                await verify_database_results(target_date)

            else:
                print("⚠️  WARNING: No games were stored. Check logs for issues.")

            return results

    except Exception as e:
        print(f"❌ COLLECTION FAILED: {e}")
        logger.error(f"Collection failed: {e}", exc_info=True)
        return None


async def verify_database_results(target_date: date):
    """Verify that data was properly stored in the database."""
    print("🔍 VERIFYING DATABASE RESULTS")
    print("-" * 40)

    try:
        import asyncpg

        conn = await asyncpg.connect(
            host="localhost", port=5432, database="mlb_betting", user="samlafell"
        )

        # Check games for target date
        games = await conn.fetch(
            """
            SELECT 
                id,
                home_team,
                away_team,
                sportsbookreview_game_id,
                game_date
            FROM public.games 
            WHERE DATE(game_date) = $1
            ORDER BY id;
        """,
            target_date,
        )

        print(f"📊 Found {len(games)} games for {target_date}")

        if games:
            print("\nGames found:")
            for game in games[:5]:  # Show first 5
                print(
                    f"  ID {game['id']}: {game['away_team']} @ {game['home_team']} (SBR: {game['sportsbookreview_game_id']})"
                )

            if len(games) > 5:
                print(f"  ... and {len(games) - 5} more")

            # Check betting data for first game
            first_game_id = games[0]["id"]

            moneyline_count = await conn.fetchval(
                "SELECT COUNT(*) FROM mlb_betting.moneyline WHERE game_id = $1",
                first_game_id,
            )
            spread_count = await conn.fetchval(
                "SELECT COUNT(*) FROM mlb_betting.spreads WHERE game_id = $1",
                first_game_id,
            )
            total_count = await conn.fetchval(
                "SELECT COUNT(*) FROM mlb_betting.totals WHERE game_id = $1",
                first_game_id,
            )

            print(f"\n📈 Betting data for game {first_game_id}:")
            print(f"  Moneyline records: {moneyline_count}")
            print(f"  Spread records: {spread_count}")
            print(f"  Total records: {total_count}")

            if moneyline_count > 0 or spread_count > 0 or total_count > 0:
                print("✅ Betting data successfully stored!")
            else:
                print("⚠️  No betting data found for this game")

        await conn.close()

    except Exception as e:
        print(f"❌ Database verification failed: {e}")


async def main():
    """Main function."""
    print("Starting July 7th collection test...")
    print()

    results = await test_july_7th_collection()

    if results:
        print("\n🎉 Test completed successfully!")
        print("Ready to proceed with larger backfill operations.")
    else:
        print("\n💥 Test failed!")
        print("Please check logs and fix issues before running backfill.")

    print("\nGeneral Balls")


if __name__ == "__main__":
    asyncio.run(main())
