#!/usr/bin/env python3
"""
Diagnostic script to debug why game ID 777203 (NYY vs Toronto) is not being found in VSIN scraping.
"""

import asyncio
import sys
from datetime import datetime

import structlog

# Add the src directory to Python path
sys.path.insert(0, "src")

from mlb_sharp_betting.core.logging import setup_logging
from mlb_sharp_betting.db.connection import get_db_manager
from mlb_sharp_betting.scrapers.vsin import VSINScraper

# Setup logging with debug level
setup_logging(level="DEBUG")
logger = structlog.get_logger(__name__)


async def debug_vsin_scraping():
    """Debug VSIN scraping for the missing NYY vs Toronto game."""

    print("🔍 VSIN SCRAPING DIAGNOSTIC - Game ID 777203 (NYY vs Toronto)")
    print("=" * 70)

    # Test different sportsbook views
    sportsbooks = ["circa", "dk", "fanduel", "mgm", "caesars"]

    for sportsbook in sportsbooks:
        print(f"\n📊 Testing {sportsbook.upper()} sportsbook view...")
        print("-" * 40)

        try:
            scraper = VSINScraper(default_sportsbook=sportsbook)

            # Test both regular and tomorrow views
            for use_tomorrow in [False, True]:
                view_type = "Tomorrow" if use_tomorrow else "Regular"
                print(f"\n🔍 {view_type} View:")

                # Build URL
                url = scraper.build_url("mlb", sportsbook, use_tomorrow)
                print(f"   URL: {url}")

                try:
                    # Get the HTML soup
                    soup = await scraper._get_soup(url)

                    # Extract date from HTML
                    extracted_date = scraper._extract_date_from_html(soup)
                    if extracted_date:
                        print(f"   📅 Extracted Date: {extracted_date}")
                    else:
                        print("   ⚠️  Could not extract date from HTML")

                    # Look for team names in the HTML
                    team_variations = [
                        "yankees",
                        "nyy",
                        "new york yankees",
                        "ny yankees",
                        "toronto",
                        "blue jays",
                        "tor",
                        "toronto blue jays",
                        "jays",
                    ]

                    html_text = str(soup).lower()
                    found_teams = []
                    for team in team_variations:
                        if team in html_text:
                            found_teams.append(team)

                    if found_teams:
                        print(f"   ✅ Found team variations: {', '.join(found_teams)}")
                    else:
                        print("   ❌ No team variations found")

                    # Look for team links specifically
                    team_links = soup.find_all("a", href=True)
                    mlb_team_links = [
                        link
                        for link in team_links
                        if "/mlb/teams/" in link.get("href", "")
                    ]

                    if mlb_team_links:
                        print(f"   🔗 Found {len(mlb_team_links)} MLB team links:")
                        for i, link in enumerate(mlb_team_links[:10]):  # Show first 10
                            href = link.get("href", "")
                            text = link.get_text(strip=True)
                            print(f"      [{i + 1}] {text} -> {href}")

                        # Check if any contain Yankees or Toronto
                        yankee_links = [
                            link
                            for link in mlb_team_links
                            if any(
                                term in link.get_text().lower()
                                for term in ["yankee", "nyy", "new york"]
                            )
                        ]
                        toronto_links = [
                            link
                            for link in mlb_team_links
                            if any(
                                term in link.get_text().lower()
                                for term in ["toronto", "blue jay", "tor", "jays"]
                            )
                        ]

                        if yankee_links:
                            print(
                                f"   🏟️  Yankees-related links: {[link.get_text() for link in yankee_links]}"
                            )
                        if toronto_links:
                            print(
                                f"   🏟️  Toronto-related links: {[link.get_text() for link in toronto_links]}"
                            )
                    else:
                        print("   ❌ No MLB team links found")

                    # Try to find the main content table
                    main_content = scraper._extract_main_content(soup)
                    if main_content:
                        print("   ✅ Found main betting table")

                        # Parse the betting splits
                        splits = scraper._parse_betting_splits(
                            main_content, "mlb", sportsbook
                        )
                        print(f"   📊 Parsed {len(splits)} betting splits")

                        # Look for Yankees vs Toronto specifically
                        yankee_toronto_games = []
                        for split in splits:
                            away_team = str(split.get("Away Team", "")).lower()
                            home_team = str(split.get("Home Team", "")).lower()
                            game = split.get("Game", "")

                            is_yankee_game = any(
                                term in away_team or term in home_team
                                for term in ["yankee", "nyy", "new york"]
                            )
                            is_toronto_game = any(
                                term in away_team or term in home_team
                                for term in ["toronto", "blue jay", "tor", "jays"]
                            )

                            if is_yankee_game and is_toronto_game:
                                yankee_toronto_games.append(split)

                        if yankee_toronto_games:
                            print(
                                f"   🎯 Found {len(yankee_toronto_games)} Yankees vs Toronto games:"
                            )
                            for game in yankee_toronto_games:
                                print(f"      Game: {game.get('Game', 'Unknown')}")
                                print(f"      Away: {game.get('Away Team', 'Unknown')}")
                                print(f"      Home: {game.get('Home Team', 'Unknown')}")
                                print(
                                    f"      Away Line: {game.get('Away Line', 'N/A')}"
                                )
                                print(
                                    f"      Home Line: {game.get('Home Line', 'N/A')}"
                                )
                        else:
                            print(
                                "   ❌ No Yankees vs Toronto games found in parsed splits"
                            )

                            # Show all parsed games for reference
                            if splits:
                                print("   📋 All parsed games:")
                                for i, split in enumerate(splits[:5]):  # Show first 5
                                    game = split.get("Game", "Unknown")
                                    print(f"      [{i + 1}] {game}")
                    else:
                        print("   ❌ Could not find main betting table")

                except Exception as e:
                    print(f"   ❌ Error scraping {view_type.lower()} view: {e}")
                    logger.error(
                        "Scraping error",
                        sportsbook=sportsbook,
                        use_tomorrow=use_tomorrow,
                        error=str(e),
                    )

        except Exception as e:
            print(f"❌ Error with {sportsbook}: {e}")
            logger.error("Sportsbook error", sportsbook=sportsbook, error=str(e))


async def check_database_for_game():
    """Check if game ID 777203 exists in the database under any identifier."""

    print("\n🗄️  CHECKING DATABASE FOR GAME ID 777203")
    print("=" * 50)

    try:
        db_manager = get_db_manager()

        with db_manager.get_cursor() as cursor:
            # Check in betting splits table
            cursor.execute("""
                SELECT DISTINCT 
                    away_team, home_team, game_id, source, book, last_updated,
                    CASE 
                        WHEN away_team ILIKE '%yankee%' OR away_team ILIKE '%nyy%' OR away_team ILIKE '%new york%'
                        OR home_team ILIKE '%yankee%' OR home_team ILIKE '%nyy%' OR home_team ILIKE '%new york%' THEN 'Yankees'
                        ELSE 'No Yankees'
                    END as has_yankees,
                    CASE 
                        WHEN away_team ILIKE '%toronto%' OR away_team ILIKE '%blue jay%' OR away_team ILIKE '%tor%' OR away_team ILIKE '%jays%'
                        OR home_team ILIKE '%toronto%' OR home_team ILIKE '%blue jay%' OR home_team ILIKE '%tor%' OR home_team ILIKE '%jays%' THEN 'Toronto'
                        ELSE 'No Toronto'
                    END as has_toronto
                FROM splits.raw_mlb_betting_splits 
                WHERE game_id = '777203'
                   OR (
                       (away_team ILIKE '%yankee%' OR away_team ILIKE '%nyy%' OR away_team ILIKE '%new york%'
                        OR home_team ILIKE '%yankee%' OR home_team ILIKE '%nyy%' OR home_team ILIKE '%new york%')
                       AND
                       (away_team ILIKE '%toronto%' OR away_team ILIKE '%blue jay%' OR away_team ILIKE '%tor%' OR away_team ILIKE '%jays%'
                        OR home_team ILIKE '%toronto%' OR home_team ILIKE '%blue jay%' OR home_team ILIKE '%tor%' OR home_team ILIKE '%jays%')
                   )
                ORDER BY last_updated DESC
                LIMIT 10
            """)

            results = cursor.fetchall()

            if results:
                print(f"✅ Found {len(results)} potential matches:")
                print(
                    f"{'Game ID':<10} {'Away Team':<20} {'Home Team':<20} {'Source':<8} {'Book':<10} {'Last Updated'}"
                )
                print("-" * 90)

                for row in results:
                    game_id = row["game_id"] or "None"
                    away = (row["away_team"] or "Unknown")[:19]
                    home = (row["home_team"] or "Unknown")[:19]
                    source = (row["source"] or "Unknown")[:7]
                    book = (row["book"] or "Unknown")[:9]
                    updated = (
                        str(row["last_updated"])[:16] if row["last_updated"] else "None"
                    )

                    print(
                        f"{game_id:<10} {away:<20} {home:<20} {source:<8} {book:<10} {updated}"
                    )

                    # Check if this is actually the Yankees vs Toronto game
                    if (
                        row["has_yankees"] == "Yankees"
                        and row["has_toronto"] == "Toronto"
                    ):
                        print("   🎯 This appears to be the Yankees vs Toronto game!")
            else:
                print(
                    "❌ No matches found for game ID 777203 or Yankees vs Toronto combinations"
                )

            # Check for recent VSIN data
            cursor.execute("""
                SELECT COUNT(*) as vsin_count, MAX(last_updated) as latest_vsin
                FROM splits.raw_mlb_betting_splits 
                WHERE source = 'VSIN'
                  AND last_updated >= CURRENT_DATE - INTERVAL '2 days'
            """)

            vsin_stats = cursor.fetchone()
            if vsin_stats:
                print("\n📊 Recent VSIN Data (2 days):")
                print(f"   Records: {vsin_stats['vsin_count']}")
                print(f"   Latest: {vsin_stats['latest_vsin']}")

            # Check timing validation for recent games
            cursor.execute("""
                SELECT game_id, away_team, home_team, last_updated,
                       EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_updated))/3600 as hours_ago
                FROM splits.raw_mlb_betting_splits 
                WHERE source = 'VSIN'
                  AND last_updated >= CURRENT_DATE - INTERVAL '1 day'
                ORDER BY last_updated DESC
                LIMIT 5
            """)

            recent_games = cursor.fetchall()
            if recent_games:
                print("\n🕐 Recent VSIN Games (24 hours):")
                for game in recent_games:
                    game_id = game["game_id"] or "None"
                    teams = f"{game['away_team']} @ {game['home_team']}"
                    hours_ago = game["hours_ago"]
                    print(f"   {game_id}: {teams} ({hours_ago:.1f}h ago)")

    except Exception as e:
        print(f"❌ Database check failed: {e}")
        logger.error("Database check error", error=str(e))


async def test_data_collection_pipeline():
    """Test the data collection pipeline with debug logging."""

    print("\n🔄 TESTING DATA COLLECTION PIPELINE")
    print("=" * 50)

    try:
        from mlb_sharp_betting.entrypoint import DataPipeline

        # Run with debug mode
        pipeline = DataPipeline(
            sport="mlb",
            sportsbook="circa",  # Start with Circa since it's the default
            dry_run=False,  # Use real scraping
        )

        print("🚀 Running data collection pipeline...")
        metrics = await pipeline.run()

        print("\n📊 Pipeline Results:")
        print(f"   Scraped Records: {metrics.get('scraped_records', 0)}")
        print(f"   Parsed Records: {metrics.get('parsed_records', 0)}")
        print(f"   Stored Records: {metrics.get('stored_records', 0)}")
        print(f"   Sharp Indicators: {metrics.get('sharp_indicators', 0)}")
        print(f"   Errors: {metrics.get('errors', 0)}")

        # Check storage stats for timing rejections
        storage_stats = metrics.get("storage_stats", {})
        if storage_stats.get("timing_rejections", 0) > 0:
            timing_rejections = storage_stats["timing_rejections"]
            total_processed = storage_stats.get("processed", 0) + timing_rejections
            rejection_rate = (
                (timing_rejections / total_processed * 100)
                if total_processed > 0
                else 0
            )
            print(
                f"   ⏰ Timing Rejections: {timing_rejections} ({rejection_rate:.1f}%)"
            )

            if rejection_rate > 20:
                print(
                    "   ⚠️  HIGH REJECTION RATE - Many games may have already started"
                )

    except Exception as e:
        print(f"❌ Pipeline test failed: {e}")
        logger.error("Pipeline test error", error=str(e))


async def main():
    """Run all diagnostic tests."""

    print("🔬 DIAGNOSTIC SUITE FOR GAME ID 777203 (NYY vs Toronto)")
    print("=" * 70)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Test 1: VSIN Scraping
    await debug_vsin_scraping()

    # Test 2: Database Check
    await check_database_for_game()

    # Test 3: Pipeline Test (commented out to avoid too much scraping)
    # await test_data_collection_pipeline()

    print("\n🏁 DIAGNOSTIC COMPLETE")
    print("=" * 70)
    print("Next steps:")
    print("1. Check if the game appears on any sportsbook view")
    print("2. Verify team name variations in VSIN's HTML")
    print("3. Check if timing validation is rejecting the game")
    print("4. Uncomment pipeline test if needed for full end-to-end testing")


if __name__ == "__main__":
    asyncio.run(main())
