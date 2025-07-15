#!/usr/bin/env python3
"""Test script to examine current line parsing and fix the interpretation."""

import asyncio
import os
import sys

# Add src to path to avoid import issues
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))


async def test_current_line_parsing():
    """Test what's currently being extracted and how it should be interpreted."""
    print("=== Testing Current VSIN Line Parsing ===")

    try:
        from mlb_sharp_betting.scrapers.vsin import VSINScraper

        scraper = VSINScraper()
        result = await scraper.scrape_sport("mlb", "circa")

        if result.success and result.data:
            print(f"✅ VSIN scraped {len(result.data)} records")

            # Look for the Phillies @ Marlins game specifically
            phillies_marlins = None
            for record in result.data:
                game = record.get("Game", "")
                if "phillies" in game.lower() and "marlins" in game.lower():
                    phillies_marlins = record
                    break

            if phillies_marlins:
                print("\n🎯 Found Phillies @ Marlins game:")
                print(f"  Game: {phillies_marlins.get('Game')}")
                print(f"  Away Team: {phillies_marlins.get('Away Team')}")
                print(f"  Home Team: {phillies_marlins.get('Home Team')}")

                # Show ALL extracted fields
                print("\n📊 ALL Extracted Fields:")
                for key, value in phillies_marlins.items():
                    if value is not None:
                        print(f"  {key}: {value}")

                # What it should be interpreted as
                print("\n🎯 Correct Interpretation Should Be:")
                line_raw = phillies_marlins.get("Line", "")
                away_line = phillies_marlins.get("Away Line", "")
                home_line = phillies_marlins.get("Home Line", "")
                total_raw = phillies_marlins.get("Total", "")
                spread_raw = phillies_marlins.get("Spread", "")
                away_spread = phillies_marlins.get("Away Spread", "")
                home_spread = phillies_marlins.get("Home Spread", "")

                # Parse moneyline
                print("  Moneyline fields:")
                print(f"    Line: '{line_raw}'")
                print(f"    Away Line: '{away_line}'")
                print(f"    Home Line: '{home_line}'")
                if away_line and home_line:
                    print(f"    → Phillies (away): {away_line}")
                    print(f"    → Marlins (home): {home_line}")

                # Parse total
                if total_raw:
                    print(f"  Total: '{total_raw}' → {total_raw}")

                # Parse spread
                print("  Spread fields:")
                print(f"    Spread: '{spread_raw}'")
                print(f"    Away Spread: '{away_spread}'")
                print(f"    Home Spread: '{home_spread}'")
                if away_spread and home_spread:
                    print(f"    → Phillies (away): {away_spread}")
                    print(f"    → Marlins (home): {home_spread}")

            else:
                print("❌ Could not find Phillies @ Marlins game")
                print("Available games:")
                for record in result.data[:3]:
                    print(f"  - {record.get('Game')}")

        else:
            print(f"❌ VSIN scraping failed: {result.errors}")

    except Exception as e:
        print(f"❌ Test failed: {e}")


if __name__ == "__main__":
    asyncio.run(test_current_line_parsing())
