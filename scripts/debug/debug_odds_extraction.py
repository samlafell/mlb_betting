#!/usr/bin/env python3
"""
Debug script to analyze why odds extraction is failing in SportsbookReview scraper.
"""

import asyncio
import json
import re
from datetime import date

import asyncpg


async def debug_odds_extraction():
    """Debug the odds extraction process."""
    print("🔍 Debugging SportsbookReview odds extraction issues...")

    try:
        conn = await asyncpg.connect(
            host="localhost", port=5432, database="mlb_betting", user="samlafell"
        )

        # Get a failed record to analyze
        print("\n📋 Analyzing failed staging record...")
        failed_record = await conn.fetchrow("""
            SELECT id, game_data
            FROM sbr_parsed_games 
            WHERE status = 'failed' 
            AND game_data->>'odds_data' = '[]'
            LIMIT 1
        """)

        if failed_record:
            game_data = failed_record["game_data"]
            if isinstance(game_data, str):
                game_data = json.loads(game_data)

            print(f"Failed Record ID: {failed_record['id']}")
            print(f"Game: {game_data.get('away_team')} @ {game_data.get('home_team')}")
            print(f"Bet Type: {game_data.get('bet_type')}")
            print(f"Source URL: {game_data.get('source_url')}")
            print(f"Odds Data Length: {len(game_data.get('odds_data', []))}")

            # Check if there's any odds-related data in the record
            for key, value in game_data.items():
                if (
                    "odds" in key.lower()
                    or "line" in key.lower()
                    or "price" in key.lower()
                ):
                    print(f"{key}: {value}")

        await conn.close()

        # Now let's test the scraper directly
        print("\n🧪 Testing scraper directly...")

        # Import the scraper
        import sys

        sys.path.append(".")

        from sportsbookreview.services.data_storage_service import DataStorageService
        from sportsbookreview.services.sportsbookreview_scraper import (
            SportsbookReviewScraper,
        )

        # Create a test scraper
        storage = DataStorageService()
        await storage.initialize_connection()

        scraper = SportsbookReviewScraper(storage_service=storage)
        await scraper.start_session()

        # Test with a recent date
        test_date = date(2025, 7, 8)
        print(f"\n🎯 Testing scraper for {test_date}...")

        # Test moneyline URL
        moneyline_url = f"https://www.sportsbookreview.com/betting-odds/mlb-baseball/?date={test_date.strftime('%Y-%m-%d')}"
        print(f"Testing URL: {moneyline_url}")

        try:
            # Fetch the page
            html_content = await scraper.fetch_url(moneyline_url)
            print(f"✅ Successfully fetched HTML ({len(html_content)} chars)")

            # Test JSON extraction
            from sportsbookreview.parsers.sportsbookreview_parser import (
                SportsbookReviewParser,
            )

            parser = SportsbookReviewParser()

            # Look for JSON in the HTML
            print("\n🔍 Searching for JSON data in HTML...")

            # Find script tags with JSON
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(html_content, "html.parser")
            script_tags = soup.find_all("script")

            json_found = False
            for i, script in enumerate(script_tags):
                script_content = script.string
                if (
                    script_content
                    and '"props":' in script_content
                    and len(script_content) > 1000
                ):
                    print(
                        f"Found large script tag #{i} with props ({len(script_content)} chars)"
                    )

                    # Try to extract JSON
                    json_pattern = r'{"props":(.*?),"page"'
                    match = re.search(json_pattern, script_content, re.DOTALL)
                    if match:
                        print("✅ Found JSON pattern match")
                        try:
                            json_str = '{"props":' + match.group(1) + "}"
                            data = json.loads(json_str)

                            # Analyze the JSON structure
                            print("\n📊 JSON Structure Analysis:")
                            if "pageProps" in data["props"]:
                                print("  ✅ pageProps found")
                                if "oddsTables" in data["props"]["pageProps"]:
                                    print("  ✅ oddsTables found")
                                    odds_tables = data["props"]["pageProps"][
                                        "oddsTables"
                                    ]
                                    print(
                                        f"  📈 Number of odds tables: {len(odds_tables)}"
                                    )

                                    if odds_tables and len(odds_tables) > 0:
                                        first_table = odds_tables[0]
                                        print(
                                            f"  📋 First table keys: {list(first_table.keys())}"
                                        )

                                        if "oddsTableModel" in first_table:
                                            model = first_table["oddsTableModel"]
                                            print(
                                                f"  📋 Model keys: {list(model.keys())}"
                                            )

                                            if "gameRows" in model:
                                                game_rows = model["gameRows"]
                                                print(
                                                    f"  🎯 Number of game rows: {len(game_rows)}"
                                                )

                                                if game_rows:
                                                    first_game = game_rows[0]
                                                    print(
                                                        f"  🎮 First game keys: {list(first_game.keys())}"
                                                    )

                                                    # Check for odds data
                                                    if "oddsViews" in first_game:
                                                        odds_views = first_game[
                                                            "oddsViews"
                                                        ]
                                                        print(
                                                            f"  💰 Number of odds views: {len(odds_views)}"
                                                        )

                                                        if odds_views:
                                                            # Filter out None values
                                                            valid_odds_views = [
                                                                ov
                                                                for ov in odds_views
                                                                if ov is not None
                                                            ]
                                                            print(
                                                                f"  💰 Valid odds views: {len(valid_odds_views)} out of {len(odds_views)}"
                                                            )

                                                            if valid_odds_views:
                                                                first_odds = (
                                                                    valid_odds_views[0]
                                                                )
                                                                print(
                                                                    f"  💰 First valid odds keys: {list(first_odds.keys())}"
                                                                )

                                                                # Check sportsbook
                                                                provider = (
                                                                    first_odds.get(
                                                                        "sportsbook"
                                                                    )
                                                                    or first_odds.get(
                                                                        "provider"
                                                                    )
                                                                )
                                                                print(
                                                                    f"  🏪 Sportsbook: {provider}"
                                                                )

                                                                # Check for actual odds values
                                                                if (
                                                                    "currentLine"
                                                                    in first_odds
                                                                ):
                                                                    current_line = first_odds[
                                                                        "currentLine"
                                                                    ]
                                                                    print(
                                                                        f"  📊 Current line: {current_line}"
                                                                    )

                                                                if (
                                                                    "openingLine"
                                                                    in first_odds
                                                                ):
                                                                    opening_line = first_odds[
                                                                        "openingLine"
                                                                    ]
                                                                    print(
                                                                        f"  📊 Opening line: {opening_line}"
                                                                    )

                                                                # Show all odds views to understand the pattern
                                                                print(
                                                                    "\n  🔍 All valid odds views:"
                                                                )
                                                                for (
                                                                    i,
                                                                    odds_view,
                                                                ) in enumerate(
                                                                    valid_odds_views
                                                                ):
                                                                    provider = (
                                                                        odds_view.get(
                                                                            "sportsbook"
                                                                        )
                                                                        or odds_view.get(
                                                                            "provider"
                                                                        )
                                                                    )
                                                                    current = odds_view.get(
                                                                        "currentLine",
                                                                        {},
                                                                    )
                                                                    opening = odds_view.get(
                                                                        "openingLine",
                                                                        {},
                                                                    )
                                                                    print(
                                                                        f"    {i + 1}. {provider}: current={current}, opening={opening}"
                                                                    )
                                                            else:
                                                                print(
                                                                    f"  ❌ All {len(odds_views)} odds views are None!"
                                                                )
                                                                # Show the actual content
                                                                print(
                                                                    f"  🔍 Raw odds views: {odds_views}"
                                                                )
                                                    else:
                                                        print(
                                                            "  ❌ No oddsViews found in game row"
                                                        )
                                                else:
                                                    print("  ❌ No game rows found")
                                            else:
                                                print("  ❌ No gameRows found in model")
                                        else:
                                            print("  ❌ No oddsTableModel found")
                                    else:
                                        print("  ❌ No odds tables found")
                                else:
                                    print("  ❌ No oddsTables found in pageProps")
                            else:
                                print("  ❌ No pageProps found")

                            json_found = True
                            break

                        except json.JSONDecodeError as e:
                            print(f"❌ JSON decode error: {e}")
                    else:
                        print("❌ No JSON pattern match found")

            if not json_found:
                print("❌ No usable JSON data found in any script tags")

                # Let's check what script tags we do have
                print(f"\n📋 Found {len(script_tags)} script tags:")
                for i, script in enumerate(script_tags):
                    content = script.string or ""
                    print(
                        f"  Script {i}: {len(content)} chars, has props: {'props' in content}"
                    )

        except Exception as e:
            print(f"❌ Error testing scraper: {e}")
            import traceback

            traceback.print_exc()

        finally:
            await scraper.close_session()
            await storage.close_connection()

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(debug_odds_extraction())
