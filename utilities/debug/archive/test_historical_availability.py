#!/usr/bin/env python3
"""
Test if SportsbookReview.com has historical betting data available for specific dates.

This will help determine if the data gaps are due to:
1. SportsbookReview.com not having historical data
2. URL structure issues for historical dates
3. Other scraping problems
"""

import asyncio
from datetime import date

import aiohttp


async def test_sbr_historical_availability():
    """Test if SportsbookReview.com has data for historical dates."""

    # Test dates - mix of dates we know have MLB games
    test_dates = [
        date(2025, 5, 2),  # May 2, 2025 (15 MLB games)
        date(2025, 5, 3),  # May 3, 2025 (15 MLB games)
        date(2025, 7, 6),  # July 6, 2025 (15 MLB games - we have some data)
        date(2025, 7, 9),  # July 9, 2025 (15 MLB games - current date)
    ]

    bet_types = ["moneyline", "spread", "total"]

    print("🔍 TESTING SPORTSBOOKREVIEW.COM HISTORICAL DATA AVAILABILITY")
    print("=" * 70)

    async with aiohttp.ClientSession() as session:
        for test_date in test_dates:
            print(f"\n📅 Testing {test_date.strftime('%B %d, %Y')} ({test_date})...")

            for bet_type in bet_types:
                # Construct the SportsbookReview URL format (correct format from scraper)
                date_str = test_date.strftime("%Y-%m-%d")
                if bet_type == "moneyline":
                    url = f"https://www.sportsbookreview.com/betting-odds/mlb-baseball/?date={date_str}"
                elif bet_type == "spread":
                    url = f"https://www.sportsbookreview.com/betting-odds/mlb-baseball/pointspread/full-game/?date={date_str}"
                elif bet_type == "total":
                    url = f"https://www.sportsbookreview.com/betting-odds/mlb-baseball/totals/full-game/?date={date_str}"

                try:
                    async with session.get(url, timeout=10) as response:
                        status = response.status
                        content_length = len(await response.text())

                        if status == 200:
                            if (
                                content_length > 50000
                            ):  # Reasonable threshold for content with data
                                print(
                                    f"   ✅ {bet_type}: {status} ({content_length:,} bytes) - Likely has data"
                                )
                            elif content_length > 10000:
                                print(
                                    f"   ⚠️  {bet_type}: {status} ({content_length:,} bytes) - Minimal content"
                                )
                            else:
                                print(
                                    f"   ❌ {bet_type}: {status} ({content_length:,} bytes) - Very little content"
                                )
                        else:
                            print(f"   ❌ {bet_type}: HTTP {status}")

                except asyncio.TimeoutError:
                    print(f"   ⏰ {bet_type}: Timeout")
                except Exception as e:
                    print(f"   ❌ {bet_type}: Error - {str(e)[:50]}")

                # Small delay between requests
                await asyncio.sleep(0.5)

    print("\n🎯 INTERPRETATION:")
    print("   ✅ = Likely has betting data (large content)")
    print("   ⚠️  = May have some data (medium content)")
    print("   ❌ = Likely no data (small content or error)")
    print("\n📝 NOTE:")
    print("   If historical dates show ❌ but current/recent dates show ✅,")
    print(
        "   it suggests SportsbookReview.com doesn't maintain historical betting data."
    )


if __name__ == "__main__":
    asyncio.run(test_sbr_historical_availability())
