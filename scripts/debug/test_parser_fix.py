#!/usr/bin/env python3
"""
Test the parser fix with different bet type URLs.
"""

import asyncio
import sys
from datetime import date

sys.path.append(".")

from sportsbookreview.services.data_storage_service import DataStorageService
from sportsbookreview.services.sportsbookreview_scraper import SportsbookReviewScraper


async def test_parser_fix():
    """Test the parser fix with all bet type URLs."""
    print("🧪 Testing parser fix with different bet type URLs...")

    storage = DataStorageService()
    await storage.initialize_connection()

    scraper = SportsbookReviewScraper(storage_service=storage)
    await scraper.start_session()

    test_date = date(2025, 7, 8)
    date_str = test_date.strftime("%Y-%m-%d")

    # Test all three bet type URLs
    urls = {
        "moneyline": f"https://www.sportsbookreview.com/betting-odds/mlb-baseball/?date={date_str}",
        "spread": f"https://www.sportsbookreview.com/betting-odds/mlb-baseball/pointspread/full-game/?date={date_str}",
        "totals": f"https://www.sportsbookreview.com/betting-odds/mlb-baseball/totals/full-game/?date={date_str}",
    }

    for bet_type, url in urls.items():
        print(f"\n🎯 Testing {bet_type.upper()} URL:")
        print(f"URL: {url}")

        try:
            # Fetch the page
            html_content = await scraper.fetch_url(url)
            print(f"✅ Fetched HTML ({len(html_content)} chars)")

            # Parse with the fixed parser
            parsed_games = scraper.parser.parse_bet_type_page(
                html_content, bet_type, test_date, url
            )
            print(f"📊 Parsed {len(parsed_games)} games")

            if parsed_games:
                # Check the first game's odds data
                first_game = parsed_games[0]
                odds_data = first_game.odds_data
                print(f"💰 First game odds data: {len(odds_data)} sportsbooks")

                if odds_data:
                    first_odds = odds_data[0]
                    # Convert Pydantic model to dict for easier access
                    if hasattr(first_odds, "model_dump"):
                        odds_dict = first_odds.model_dump()
                    else:
                        odds_dict = first_odds

                    print(f"🏪 First sportsbook: {odds_dict.get('sportsbook')}")
                    print(f"📊 Odds keys: {list(odds_dict.keys())}")

                    # Check for different bet type data
                    has_ml = (
                        "moneyline_home" in odds_dict and "moneyline_away" in odds_dict
                    )
                    has_spread = (
                        "spread_home" in odds_dict and "spread_away" in odds_dict
                    )
                    has_total = (
                        "total_line" in odds_dict
                        and odds_dict["total_line"] is not None
                    )

                    print(f"✅ Has moneyline: {has_ml}")
                    print(f"✅ Has spread: {has_spread}")
                    print(f"✅ Has totals: {has_total}")

                    # Debug totals fields
                    print(
                        f"🔍 Debug totals: total_line={odds_dict.get('total_line')}, total_over={odds_dict.get('total_over')}, total_under={odds_dict.get('total_under')}"
                    )

                    if has_ml:
                        print(
                            f"   ML: {odds_dict.get('moneyline_away')} / {odds_dict.get('moneyline_home')}"
                        )
                    if has_spread:
                        print(
                            f"   Spread: {odds_dict.get('spread_away')} / {odds_dict.get('spread_home')}"
                        )
                    if has_total:
                        print(
                            f"   Total: {odds_dict.get('total_line')} - {odds_dict.get('total_over')}/{odds_dict.get('total_under')}"
                        )
                else:
                    print("❌ No odds data extracted!")
            else:
                print("❌ No games parsed!")

        except Exception as e:
            print(f"❌ Error testing {bet_type}: {e}")
            import traceback

            traceback.print_exc()

    await scraper.close_session()
    await storage.close_connection()


if __name__ == "__main__":
    asyncio.run(test_parser_fix())
