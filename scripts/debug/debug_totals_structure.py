#!/usr/bin/env python3
"""
Debug the totals JSON structure to see why totals data isn't being extracted.
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


async def debug_totals_structure():
    """Debug the totals JSON structure."""
    print("🔍 Debugging totals JSON structure...")

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

                        # Navigate to odds data
                        if (
                            "pageProps" in data["props"]
                            and "oddsTables" in data["props"]["pageProps"]
                        ):
                            odds_tables = data["props"]["pageProps"]["oddsTables"]
                            if odds_tables and len(odds_tables) > 0:
                                first_table = odds_tables[0]
                                if "oddsTableModel" in first_table:
                                    model = first_table["oddsTableModel"]
                                    if "gameRows" in model:
                                        game_rows = model["gameRows"]
                                        if game_rows:
                                            first_game = game_rows[0]
                                            if "oddsViews" in first_game:
                                                odds_views = first_game["oddsViews"]
                                                valid_odds_views = [
                                                    ov
                                                    for ov in odds_views
                                                    if ov is not None
                                                ]

                                                if valid_odds_views:
                                                    print(
                                                        "\n🔍 Analyzing totals JSON structure:"
                                                    )

                                                    for i, odds_view in enumerate(
                                                        valid_odds_views[:2]
                                                    ):  # Show first 2
                                                        provider = odds_view.get(
                                                            "sportsbook"
                                                        ) or odds_view.get("provider")
                                                        current_line = odds_view.get(
                                                            "currentLine", {}
                                                        )
                                                        opening_line = odds_view.get(
                                                            "openingLine", {}
                                                        )

                                                        print(
                                                            f"\n  📊 Sportsbook {i + 1}: {provider}"
                                                        )
                                                        print(
                                                            f"    Current Line Keys: {list(current_line.keys())}"
                                                        )
                                                        print(
                                                            f"    Opening Line Keys: {list(opening_line.keys())}"
                                                        )

                                                        # Check all possible total-related fields
                                                        total_fields = [
                                                            "total",
                                                            "totalLine",
                                                            "total_line",
                                                            "line",
                                                            "overOdds",
                                                            "underOdds",
                                                            "over_odds",
                                                            "under_odds",
                                                            "overTotal",
                                                            "underTotal",
                                                            "totalValue",
                                                        ]

                                                        print(
                                                            "    🔍 Current Line Totals Fields:"
                                                        )
                                                        for field in total_fields:
                                                            if field in current_line:
                                                                print(
                                                                    f"      {field}: {current_line[field]}"
                                                                )

                                                        print(
                                                            "    🔍 Opening Line Totals Fields:"
                                                        )
                                                        for field in total_fields:
                                                            if field in opening_line:
                                                                print(
                                                                    f"      {field}: {opening_line[field]}"
                                                                )

                                                        # Show full structure for first sportsbook
                                                        if i == 0:
                                                            print(
                                                                "\n    📋 Full Current Line Structure:"
                                                            )
                                                            for (
                                                                key,
                                                                value,
                                                            ) in current_line.items():
                                                                print(
                                                                    f"      {key}: {value}"
                                                                )

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
    asyncio.run(debug_totals_structure())
