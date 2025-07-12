#!/usr/bin/env python3
"""
Check if there were actual MLB games on specific dates using the MLB Stats API.

This script will verify whether the missing data is due to no games being played
or due to scraping issues.
"""

import asyncio
import sys
from pathlib import Path
from datetime import date

# Add the src directory to the path
sys.path.append(str(Path(__file__).parent / "src"))

from mlb_sharp_betting.utils.mlb_api_client import MLBStatsAPIClient


async def check_games_for_date(client: MLBStatsAPIClient, check_date: date):
    """Check if there were MLB games on a specific date."""
    print(f"\n🔍 Checking {check_date.strftime('%B %d, %Y')} ({check_date})...")
    
    schedule_data = await client.get_schedule(check_date)
    
    if not schedule_data or "dates" not in schedule_data:
        print(f"   ❌ No schedule data available")
        return 0
    
    total_games = 0
    for date_entry in schedule_data["dates"]:
        games = date_entry.get("games", [])
        total_games += len(games)
        
        if games:
            print(f"   ✅ {len(games)} MLB games scheduled:")
            for game in games[:3]:  # Show first 3 games
                home_team = game.get("teams", {}).get("home", {}).get("team", {}).get("name", "Unknown")
                away_team = game.get("teams", {}).get("away", {}).get("team", {}).get("name", "Unknown")
                status = game.get("status", {}).get("detailedState", "Unknown")
                print(f"      {away_team} @ {home_team} ({status})")
            if len(games) > 3:
                print(f"      ... and {len(games) - 3} more games")
        else:
            print(f"   ❌ No MLB games scheduled")
    
    return total_games


async def main():
    """Check specific dates for MLB games."""
    
    # Dates you mentioned as missing
    check_dates = [
        date(2025, 5, 2),   # May 2, 2025
        date(2025, 5, 3),   # May 3, 2025
        date(2025, 5, 4),   # May 4, 2025
        date(2025, 4, 15),  # April 15, 2025 (sample spring date)
        date(2025, 3, 15),  # March 15, 2025 (season start detection date)
        date(2025, 7, 6),   # July 6, 2025 (date we DO have data for)
        date(2025, 7, 9),   # July 9, 2025 (current date)
    ]
    
    print("🔍 CHECKING MLB GAME AVAILABILITY FOR SPECIFIC DATES")
    print("=" * 60)
    print("This will verify if missing data is due to no games or scraping issues.")
    
    async with MLBStatsAPIClient() as client:
        total_games_found = 0
        
        for check_date in check_dates:
            games_count = await check_games_for_date(client, check_date)
            total_games_found += games_count
            
            # Small delay to be respectful to the API
            await asyncio.sleep(0.5)
    
    print(f"\n📊 SUMMARY:")
    print(f"   Total dates checked: {len(check_dates)}")
    print(f"   Total games found: {total_games_found}")
    
    if total_games_found == 0:
        print(f"\n🎯 CONCLUSION:")
        print(f"   No MLB games found on any of the checked dates.")
        print(f"   This suggests the 2025 MLB season hasn't started yet,")
        print(f"   or these specific dates had no games scheduled.")
        print(f"   The missing data is NOT due to scraping issues.")
    else:
        print(f"\n🎯 CONCLUSION:")
        print(f"   MLB games were found on some dates.")
        print(f"   If we have no betting data for dates with games,")
        print(f"   that indicates a scraping or processing issue.")


if __name__ == "__main__":
    asyncio.run(main()) 