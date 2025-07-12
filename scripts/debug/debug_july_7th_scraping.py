#!/usr/bin/env python3
"""
Debug script to manually fetch July 7th URLs and examine the content.
"""

import asyncio
import aiohttp
from datetime import date


async def debug_july_7th_urls():
    """Debug the July 7th URLs to see what content is returned."""
    print("=== DEBUGGING JULY 7TH URLS ===")
    
    # URLs for July 7th
    urls = {
        'moneyline': 'https://www.sportsbookreview.com/betting-odds/mlb-baseball/?date=2025-07-07',
        'spreads': 'https://www.sportsbookreview.com/betting-odds/mlb-baseball/pointspread/full-game/?date=2025-07-07',
        'totals': 'https://www.sportsbookreview.com/betting-odds/mlb-baseball/totals/full-game/?date=2025-07-07'
    }
    
    async with aiohttp.ClientSession() as session:
        for bet_type, url in urls.items():
            print(f"\n--- {bet_type.upper()} ---")
            print(f"URL: {url}")
            
            try:
                async with session.get(url) as response:
                    print(f"Status: {response.status}")
                    print(f"Content-Type: {response.headers.get('content-type', 'unknown')}")
                    
                    if response.status == 200:
                        html_content = await response.text()
                        print(f"HTML Length: {len(html_content)} characters")
                        
                        # Look for key indicators
                        if 'window.APP_STATE' in html_content:
                            print("✅ Found APP_STATE (JSON data)")
                            
                            # Extract the JSON data
                            start = html_content.find('window.APP_STATE = ')
                            if start != -1:
                                start += len('window.APP_STATE = ')
                                end = html_content.find(';</script>', start)
                                if end != -1:
                                    json_data = html_content[start:end]
                                    print(f"JSON Data Length: {len(json_data)} characters")
                                    
                                    # Check if it contains game data
                                    if '"games"' in json_data:
                                        print("✅ Found games data in JSON")
                                        
                                        # Count games
                                        game_count = json_data.count('"gameId"')
                                        print(f"Game count: {game_count}")
                                        
                                        if game_count > 0:
                                            # Show sample game data
                                            import json
                                            try:
                                                app_state = json.loads(json_data)
                                                if 'games' in app_state and app_state['games']:
                                                    sample_game = list(app_state['games'].values())[0]
                                                    print(f"Sample game: {sample_game.get('away', {}).get('name', 'Unknown')} @ {sample_game.get('home', {}).get('name', 'Unknown')}")
                                            except:
                                                print("Could not parse JSON data")
                                        else:
                                            print("❌ No games found in JSON data")
                                    else:
                                        print("❌ No games data found in JSON")
                                else:
                                    print("❌ Could not find end of JSON data")
                            else:
                                print("❌ Could not find start of JSON data")
                        else:
                            print("❌ No APP_STATE found")
                        
                        # Check for "no games" indicators
                        if 'no games' in html_content.lower():
                            print("❌ Page indicates no games")
                        
                        # Check for date-specific content
                        if '2025-07-07' in html_content:
                            print("✅ Date found in HTML content")
                        else:
                            print("❌ Date not found in HTML content")
                            
                    else:
                        print(f"❌ HTTP Error: {response.status}")
                        
            except Exception as e:
                print(f"❌ Error fetching {bet_type}: {e}")


async def main():
    """Main function."""
    print("Debug July 7th SportsbookReview URLs")
    print("=" * 50)
    
    await debug_july_7th_urls()
    
    print("\n✅ Debug completed!")


if __name__ == "__main__":
    asyncio.run(main()) 