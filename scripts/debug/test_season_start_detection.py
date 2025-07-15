#!/usr/bin/env python3
"""
Test Season Start Detection and Rate Limiting Improvements

This script tests:
1. Auto-detection of MLB season start date
2. Improved rate limiting performance
3. URL existence checking
"""

import asyncio
from datetime import date, timedelta

from sportsbookreview.services.data_storage_service import DataStorageService
from sportsbookreview.services.sportsbookreview_scraper import SportsbookReviewScraper


async def test_season_start_detection():
    """
    Test the season start detection functionality.
    """
    print("\n🔍 TESTING SEASON START DETECTION")
    print("=" * 50)

    storage = DataStorageService()
    await storage.initialize_connection()

    try:
        async with SportsbookReviewScraper(storage_service=storage) as scraper:
            # Test 1: Check URL existence for known dates
            print("\n🧪 Test 1: URL Existence Checking")

            # Test recent date (should exist)
            recent_date = date.today() - timedelta(days=1)
            recent_url = scraper.url_patterns["moneyline"].format(
                date=recent_date.strftime("%Y-%m-%d")
            )

            print(f"   📅 Testing recent date: {recent_date}")
            recent_exists = await scraper.check_url_exists(recent_url)
            print(f"   ✅ Recent URL exists: {recent_exists}")

            # Test old date (likely doesn't exist or no MLB data)
            old_date = date(2025, 1, 15)  # January - no MLB
            old_url = scraper.url_patterns["moneyline"].format(
                date=old_date.strftime("%Y-%m-%d")
            )

            print(f"   📅 Testing old date: {old_date}")
            old_exists = await scraper.check_url_exists(old_url)
            print(f"   ❌ Old URL exists: {old_exists}")

            # Test 2: Season start detection
            print("\n🧪 Test 2: Season Start Detection")

            # Search from March 15
            search_start = date(2025, 3, 15)
            print(f"   🔍 Searching for season start from: {search_start}")

            season_start = await scraper.find_season_start_date(search_start)

            if season_start:
                print(f"   ✅ Found season start: {season_start}")
                print(
                    f"   📊 Days from search start: {(season_start - search_start).days}"
                )

                # Verify the found date actually has data
                verify_url = scraper.url_patterns["moneyline"].format(
                    date=season_start.strftime("%Y-%m-%d")
                )
                has_data = await scraper.check_url_exists(verify_url)
                print(f"   ✅ Verified season start has data: {has_data}")
            else:
                print("   ❌ No season start found")

            # Test 3: Rate limiting performance
            print("\n🧪 Test 3: Rate Limiting Performance")

            # Check current rate limiter settings
            if scraper.unified_rate_limiter:
                service_status = scraper.unified_rate_limiter.get_service_status(
                    "sportsbookreview_scraper"
                )
                rate_limits = service_status.get("rate_limits", {})

                print(
                    f"   ⚡ Max requests per minute: {rate_limits.get('max_requests_per_minute', 'N/A')}"
                )
                print(
                    f"   ⚡ Request delay: {rate_limits.get('request_delay_seconds', 'N/A')}s"
                )
                print(
                    f"   ⚡ Max requests per hour: {rate_limits.get('max_requests_per_hour', 'N/A')}"
                )
            else:
                print("   ⚠️  UnifiedRateLimiter not available")

    finally:
        await storage.close_connection()


async def test_rate_limiting_improvements():
    """
    Test the rate limiting improvements by timing multiple requests.
    """
    print("\n⚡ TESTING RATE LIMITING IMPROVEMENTS")
    print("=" * 50)

    storage = DataStorageService()
    await storage.initialize_connection()

    try:
        async with SportsbookReviewScraper(storage_service=storage) as scraper:
            import time

            # Test multiple quick requests
            test_dates = [
                date.today() - timedelta(days=i)
                for i in range(1, 6)  # Last 5 days
            ]

            print(f"   🧪 Testing {len(test_dates)} URL checks...")

            start_time = time.time()

            for i, test_date in enumerate(test_dates, 1):
                test_url = scraper.url_patterns["moneyline"].format(
                    date=test_date.strftime("%Y-%m-%d")
                )

                request_start = time.time()
                exists = await scraper.check_url_exists(test_url)
                request_duration = time.time() - request_start

                print(
                    f"   📡 Request {i}: {test_date} | {request_duration:.2f}s | Exists: {exists}"
                )

            total_duration = time.time() - start_time
            avg_per_request = total_duration / len(test_dates)

            print("\n   📊 PERFORMANCE RESULTS:")
            print(f"   ⏱️  Total time: {total_duration:.2f}s")
            print(f"   📈 Average per request: {avg_per_request:.2f}s")
            print(f"   🚀 Requests per second: {len(test_dates) / total_duration:.2f}")

            # Compare with old rate limiting
            old_estimate = len(test_dates) * 1.0  # Old 1.0s delay
            improvement = old_estimate / total_duration

            print("   🎯 IMPROVEMENT:")
            print(f"   🐌 Old estimate (1.0s delays): {old_estimate:.1f}s")
            print(f"   ⚡ New actual: {total_duration:.1f}s")
            print(f"   📈 Performance improvement: {improvement:.1f}x faster")

    finally:
        await storage.close_connection()


async def main():
    """
    Run all tests.
    """
    print("🧪 SEASON START DETECTION & RATE LIMITING TESTS")
    print("\nTesting the improvements made to:")
    print("✅ Rate limiting: 60 req/min (was 10), 0.3s delay (was 1.0s)")
    print("✅ Season detection: Auto-find MLB season start from March 15")
    print("✅ URL checking: Smart existence checking before scraping")

    try:
        await test_season_start_detection()
        await test_rate_limiting_improvements()

        print("\n🎉 ALL TESTS COMPLETED")
        print("\n💡 READY FOR FULL SEASON COLLECTION:")
        print("   python collect_2025_season_optimized.py --concurrent-dates 4")
        print(
            "   python collect_2025_season_optimized.py --aggressive --concurrent-dates 6"
        )

    except Exception as e:
        print(f"\n❌ Tests failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
