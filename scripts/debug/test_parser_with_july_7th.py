#!/usr/bin/env python3
"""
Test the SportsbookReview parser directly with July 7th data.
"""

import asyncio
import sys
from pathlib import Path

import aiohttp

# Add the sportsbookreview module to the path
sys.path.append(str(Path(__file__).parent / "sportsbookreview"))

from sportsbookreview.parsers.sportsbookreview_parser import SportsbookReviewParser


async def test_parser_with_july_7th():
    """Test the parser directly with July 7th data."""
    print("=== TESTING PARSER WITH JULY 7TH DATA ===")

    url = "https://www.sportsbookreview.com/betting-odds/mlb-baseball/?date=2025-07-07"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            html_content = await response.text()

            print(f"HTML length: {len(html_content)} characters")

            # Initialize parser
            parser = SportsbookReviewParser()

            try:
                # Parse the page
                print("Parsing page...")
                games_data = parser.parse_page(html_content, url)

                print(f"✅ Successfully parsed {len(games_data)} games")

                # Show details of parsed games
                for i, game_data in enumerate(games_data):
                    print(f"\n--- Game {i + 1} ---")

                    # GameDataValidator is a Pydantic model, so we need to access it differently
                    game_dict = (
                        game_data.model_dump()
                        if hasattr(game_data, "model_dump")
                        else game_data.__dict__
                    )

                    print(f"Game ID: {game_dict.get('sbr_game_id', 'N/A')}")
                    print(
                        f"Teams: {game_dict.get('away_team', 'N/A')} @ {game_dict.get('home_team', 'N/A')}"
                    )
                    print(f"Date: {game_dict.get('game_date', 'N/A')}")
                    print(f"Time: {game_dict.get('game_time', 'N/A')}")
                    print(f"Bet Type: {game_dict.get('bet_type', 'N/A')}")

                    odds_data = game_dict.get("odds_data", [])
                    print(f"Odds Data: {len(odds_data)} sportsbooks")

                    # Show sample odds data
                    if odds_data:
                        for j, odds in enumerate(odds_data[:3]):  # Show first 3
                            print(
                                f"  Sportsbook {j + 1}: {odds.get('sportsbook', 'N/A')}"
                            )
                            if odds.get("moneyline_home"):
                                print(
                                    f"    Moneyline: {odds.get('moneyline_home')}/{odds.get('moneyline_away')}"
                                )
                            if odds.get("spread_home"):
                                print(
                                    f"    Spread: {odds.get('spread_home')}/{odds.get('spread_away')}"
                                )
                            if odds.get("total_line"):
                                print(
                                    f"    Total: {odds.get('total_line')} O/U {odds.get('over_price')}/{odds.get('under_price')}"
                                )

                    if i >= 2:  # Show first 3 games only
                        break

                return games_data

            except Exception as e:
                print(f"❌ Parser error: {e}")
                import traceback

                traceback.print_exc()
                return None


async def main():
    """Main function."""
    print("Test SportsbookReview Parser with July 7th Data")
    print("=" * 50)

    games_data = await test_parser_with_july_7th()

    if games_data:
        print(
            f"\n✅ Parser test completed successfully - {len(games_data)} games parsed!"
        )
    else:
        print("\n❌ Parser test failed!")


if __name__ == "__main__":
    asyncio.run(main())
