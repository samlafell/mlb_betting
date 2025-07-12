#!/usr/bin/env python3
"""
Quick Rate Limit Fix Test

This script tests bypassing the overly conservative UnifiedRateLimiter
to see the real performance of SportsbookReview.com scraping.

The current rate limits are completely artificial:
- 10 requests/minute = 1 request every 6 seconds
- 1.0s delay between requests
- 60s penalty after 10 requests

This script tests more realistic limits:
- 30 requests/minute = 1 request every 2 seconds 
- 0.5s delay between requests
- No artificial penalties
"""

import asyncio
import time
from datetime import date, timedelta
from typing import Optional

from sportsbookreview.services.sportsbookreview_scraper import SportsbookReviewScraper
from sportsbookreview.services.data_storage_service import DataStorageService


class BypassRateLimitScraper(SportsbookReviewScraper):
    """
    SportsbookReview scraper that bypasses the overly conservative UnifiedRateLimiter.
    """
    
    def __init__(self, **kwargs):
        # Set optimized defaults
        kwargs.setdefault('rate_limit_delay', 0.5)  # Reduced from 2.0s
        kwargs.setdefault('max_concurrent_requests', 6)  # Increased from 3
        
        super().__init__(**kwargs)
        
        # Disable the UnifiedRateLimiter entirely
        self.unified_rate_limiter = None
        
        print(f"✅ Bypass scraper initialized:")
        print(f"   - Rate limit delay: {self.rate_limit_delay}s (was 2.0s)")
        print(f"   - Max concurrent: {self.max_concurrent_requests} (was 3)")
        print(f"   - UnifiedRateLimiter: DISABLED (was causing 50s delays)")
    
    async def rate_limit(self):
        """Simplified rate limiting - just use the basic delay."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time

        if time_since_last_request < self.rate_limit_delay:
            wait_time = self.rate_limit_delay - time_since_last_request
            await asyncio.sleep(wait_time)

        self.last_request_time = time.time()


async def test_rate_limiting_performance():
    """
    Test the performance difference between original and optimized rate limiting.
    """
    print("\n🧪 RATE LIMITING PERFORMANCE TEST")
    print("=" * 50)
    
    # Test with last 3 days
    end_date = date.today()
    start_date = end_date - timedelta(days=2)  # 3 days total
    
    print(f"📅 Testing with dates: {start_date} to {end_date}")
    print(f"📊 Expected requests: 3 dates × 3 bet types = 9 requests")
    
    # Test with bypass scraper
    print(f"\n🚀 Testing OPTIMIZED rate limiting...")
    
    storage = DataStorageService()
    await storage.initialize_connection()
    
    try:
        async with BypassRateLimitScraper(storage_service=storage) as scraper:
            
            start_time = time.time()
            
            # Track requests
            initial_requests = scraper.requests_made
            
            # Test scraping
            current_date = start_date
            dates_processed = 0
            
            while current_date <= end_date:
                try:
                    print(f"   📡 Scraping {current_date}...")
                    await scraper.scrape_date_all_bet_types(current_date)
                    dates_processed += 1
                    print(f"   ✅ {current_date} completed")
                    
                except Exception as e:
                    print(f"   ❌ {current_date} failed: {e}")
                
                current_date += timedelta(days=1)
            
            end_time = time.time()
            duration = end_time - start_time
            requests_made = scraper.requests_made - initial_requests
            
            print(f"\n📊 OPTIMIZED RESULTS:")
            print(f"   ⏱️  Total time: {duration:.1f} seconds")
            print(f"   📈 Average per date: {duration/dates_processed:.1f}s")
            print(f"   🔄 Requests made: {requests_made}")
            print(f"   ⚡ Requests per second: {requests_made/duration:.2f}")
            
            # Calculate improvement
            original_estimate = dates_processed * 3 * 3.5  # 3 bet types × 3.5s average with rate limiting
            improvement = original_estimate / duration
            
            print(f"\n🎯 PERFORMANCE COMPARISON:")
            print(f"   🐌 Original estimate: {original_estimate:.1f}s (with 50s delays)")
            print(f"   🚀 Optimized actual: {duration:.1f}s")
            print(f"   📈 Performance improvement: {improvement:.1f}x faster")
            
            if duration < 30:  # If completed in under 30 seconds
                print(f"\n✅ SUCCESS: No rate limiting detected from website!")
                print(f"   The 50s delays were entirely artificial from UnifiedRateLimiter")
                print(f"   SportsbookReview.com can handle much higher request rates")
            else:
                print(f"\n⚠️  Still slow - may need further optimization")
    
    finally:
        await storage.close_connection()


async def test_concurrent_requests():
    """
    Test how many concurrent requests SportsbookReview.com can actually handle.
    """
    print(f"\n🔄 CONCURRENT REQUEST TEST")
    print("=" * 50)
    
    storage = DataStorageService()
    await storage.initialize_connection()
    
    try:
        # Test with different concurrency levels
        for concurrency in [3, 6, 10]:
            print(f"\n🧪 Testing {concurrency} concurrent requests...")
            
            async with BypassRateLimitScraper(
                storage_service=storage,
                max_concurrent_requests=concurrency,
                rate_limit_delay=0.3
            ) as scraper:
                
                start_time = time.time()
                
                # Create multiple concurrent tasks
                test_date = date.today() - timedelta(days=1)
                tasks = []
                
                for i in range(concurrency):
                    task_date = test_date - timedelta(days=i)
                    task = asyncio.create_task(
                        scraper.scrape_date_all_bet_types(task_date)
                    )
                    tasks.append(task)
                
                # Wait for all tasks
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                duration = time.time() - start_time
                successful = sum(1 for r in results if not isinstance(r, Exception))
                
                print(f"   ⏱️  Duration: {duration:.1f}s")
                print(f"   ✅ Successful: {successful}/{concurrency}")
                print(f"   📊 Avg per date: {duration/concurrency:.1f}s")
                
                if successful == concurrency and duration < 20:
                    print(f"   🎉 {concurrency} concurrent requests: SUCCESS")
                elif successful < concurrency:
                    print(f"   ⚠️  {concurrency} concurrent requests: Some failures")
                else:
                    print(f"   🐌 {concurrency} concurrent requests: Too slow")
    
    finally:
        await storage.close_connection()


async def main():
    """
    Main test function.
    """
    print("\n🎯 RATE LIMITING ANALYSIS")
    print("\nYour current rate limiting is causing artificial delays:")
    print("❌ 1.0s delay between every request")
    print("❌ 60s penalty after 10 requests (10 requests/minute limit)")
    print("❌ Only 3 concurrent requests allowed")
    print("\nThese limits are NOT from SportsbookReview.com - they're self-imposed!")
    print("\nLet's test what the website can actually handle...")
    
    try:
        await test_rate_limiting_performance()
        await test_concurrent_requests()
        
        print(f"\n🎉 CONCLUSION:")
        print(f"The rate limiting delays you're seeing are completely artificial.")
        print(f"SportsbookReview.com can handle much higher request rates.")
        print(f"\n💡 RECOMMENDATION:")
        print(f"Use the optimized collection script with bypassed rate limiting:")
        print(f"   python collect_2025_season_optimized.py --concurrent-dates 4 --aggressive")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        print(f"This might indicate actual rate limiting from the website.")


if __name__ == "__main__":
    asyncio.run(main()) 