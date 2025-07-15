#!/usr/bin/env python3
"""
Debug the real parsing chain to see where totals data is being lost.
"""

import asyncio
import json
import re
import sys
from datetime import date

from bs4 import BeautifulSoup

sys.path.append(".")

from sportsbookreview.services.data_storage_service import DataStorageService
from sportsbookreview.services.sportsbookreview_scraper import SportsbookReviewScraper


async def debug_real_parsing_chain():
    """Debug the real parsing chain to see where totals data is lost."""
    print("🔍 Debugging real parsing chain for totals data...")

    storage = DataStorageService()
    await storage.initialize_connection()

    scraper = SportsbookReviewScraper(storage_service=storage)
    await scraper.start_session()

    test_date = date(2025, 7, 8)
    date_str = test_date.strftime("%Y-%m-%d")

    # Test totals URL specifically
    totals_url = f"https://www.sportsbookreview.com/betting-odds/mlb-baseball/totals/full-game/?date={date_str}"
    print(f"🎯 Testing TOTALS URL: {totals_url}")

    try:
        # Fetch the page
        html_content = await scraper.fetch_url(totals_url)
        print(f"✅ Fetched HTML ({len(html_content)} chars)")

        # Parse using the actual parser method
        print("\n🧪 Testing parse_bet_type_page method:")
        parsed_games = scraper.parser.parse_bet_type_page(
            html_content, "totals", test_date, totals_url
        )
        print(f"📊 Parsed {len(parsed_games)} games")

        if parsed_games:
            first_game = parsed_games[0]
            print("\n🎮 First game analysis:")
            print(f"  Game: {first_game.away_team} @ {first_game.home_team}")
            print(f"  SBR ID: {first_game.sbr_game_id}")
            print(f"  Bet Type: {first_game.bet_type}")
            print(f"  Odds Data Count: {len(first_game.odds_data)}")

            if first_game.odds_data:
                for i, odds_record in enumerate(
                    first_game.odds_data[:2]
                ):  # Show first 2
                    print(f"\n  📊 Odds Record {i + 1}:")

                    # Convert to dict if it's a Pydantic model
                    if hasattr(odds_record, "model_dump"):
                        odds_dict = odds_record.model_dump()
                    else:
                        odds_dict = odds_record

                    print(f"    Sportsbook: {odds_dict.get('sportsbook')}")
                    print(f"    Bet Type: {odds_dict.get('bet_type')}")

                    # Check all fields
                    for key, value in odds_dict.items():
                        if value is not None:
                            print(f"    {key}: {value}")

                    # Specifically check totals fields
                    total_line = odds_dict.get("total_line")
                    total_over = odds_dict.get("total_over")
                    total_under = odds_dict.get("total_under")

                    print("    🔍 Totals Check:")
                    print(f"      total_line: {total_line}")
                    print(f"      total_over: {total_over}")
                    print(f"      total_under: {total_under}")
                    print(
                        f"      Has totals: {total_line is not None and (total_over is not None or total_under is not None)}"
                    )

        # Now let's also test the JSON extraction directly
        print("\n🔍 Testing JSON extraction directly:")

        # Find script tags with JSON
        soup = BeautifulSoup(html_content, "html.parser")
        script_tags = soup.find_all("script")

        for i, script in enumerate(script_tags):
            script_content = script.string
            if (
                script_content
                and '"props":' in script_content
                and len(script_content) > 1000
            ):
                print(f"Found large script tag #{i}")

                # Try to extract JSON
                json_pattern = r'{"props":(.*?),"page"'
                match = re.search(json_pattern, script_content, re.DOTALL)
                if match:
                    try:
                        json_str = '{"props":' + match.group(1) + "}"
                        data = json.loads(json_str)

                        # Navigate to first game's first odds view
                        odds_tables = data["props"]["pageProps"]["oddsTables"]
                        first_table = odds_tables[0]
                        game_rows = first_table["oddsTableModel"]["gameRows"]
                        first_game_json = game_rows[0]
                        odds_views = first_game_json["oddsViews"]
                        valid_odds_views = [ov for ov in odds_views if ov is not None]

                        if valid_odds_views:
                            first_odds_json = valid_odds_views[0]
                            current_line = first_odds_json.get("currentLine", {})

                            print("\n📊 Raw JSON totals data:")
                            print(f"  Sportsbook: {first_odds_json.get('sportsbook')}")
                            print(f"  total: {current_line.get('total')}")
                            print(f"  overOdds: {current_line.get('overOdds')}")
                            print(f"  underOdds: {current_line.get('underOdds')}")

                            # Test the _format_odds_line method with this real data
                            print("\n🧪 Testing _format_odds_line with real JSON data:")
                            formatted = scraper.parser._format_odds_line(
                                first_odds_json, "totals"
                            )
                            print(f"  Formatted result: {formatted}")

                        break

                    except json.JSONDecodeError as e:
                        print(f"❌ JSON decode error: {e}")
                break

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()

    finally:
        await scraper.close_session()
        await storage.close_connection()


if __name__ == "__main__":
    asyncio.run(debug_real_parsing_chain())
