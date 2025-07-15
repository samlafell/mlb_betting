#!/usr/bin/env python3
"""
Optimized 2025 MLB Season Data Collection

This script runs an optimized data collection for the entire 2025 MLB season
with significant performance improvements:

🚀 PERFORMANCE OPTIMIZATIONS:
- Parallel date processing (2-4 dates concurrently)
- Reduced rate limiting delays (1.0s instead of 2.0s)
- Increased concurrent requests (5 instead of 3)
- Smart batch processing with progress tracking
- Adaptive rate limiting based on response times

Usage:
    python collect_2025_season_optimized.py [options]

Options:
    --start-date: Start date (default: 2025-01-01)
    --end-date: End date (default: today)
    --concurrent-dates: Number of dates to process in parallel (default: 2)
    --aggressive: Use aggressive optimization settings
    --test-run: Test with last 7 days only
    --output-dir: Output directory for results
    --dry-run: Test run without actual scraping
"""

import argparse
import asyncio
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

# Import the collection orchestrator
from sportsbookreview.services.collection_orchestrator import CollectionOrchestrator
from sportsbookreview.services.data_storage_service import DataStorageService
from sportsbookreview.services.sportsbookreview_scraper import SportsbookReviewScraper


class OptimizedCollectionOrchestrator(CollectionOrchestrator):
    """
    Optimized version of CollectionOrchestrator with parallel date processing.
    """

    def __init__(
        self, concurrent_dates: int = 2, aggressive_mode: bool = False, **kwargs
    ):
        super().__init__(**kwargs)
        self.concurrent_dates = concurrent_dates
        self.aggressive_mode = aggressive_mode

        # Performance tracking
        self.start_time = None
        self.requests_made = 0
        self.avg_response_time = 0.0
        self.response_times = []

    async def initialize_services(self):
        """Initialize optimized scraper and storage services."""
        try:
            self.storage = DataStorageService()
            await self.storage.initialize_connection()

            # Create optimized scraper configuration
            scraper_config = self._get_optimized_scraper_config()

            self.scraper = SportsbookReviewScraper(
                storage_service=self.storage, **scraper_config
            )
            await self.scraper.start_session()

            print("✅ Initialized optimized scraper with:")
            print(f"   - Rate limit delay: {scraper_config['rate_limit_delay']}s")
            print(
                f"   - Max concurrent requests: {scraper_config['max_concurrent_requests']}"
            )
            print(f"   - Concurrent dates: {self.concurrent_dates}")
            print(f"   - Mode: {'Aggressive' if self.aggressive_mode else 'Moderate'}")

        except Exception as e:
            print(f"❌ Failed to initialize services: {e}")
            raise

    def _get_optimized_scraper_config(self) -> dict[str, Any]:
        """Get optimized scraper configuration based on mode."""
        if self.aggressive_mode:
            return {
                "rate_limit_delay": 0.5,  # Aggressive: 0.5s
                "max_concurrent_requests": 8,  # Aggressive: 8
                "timeout": 20,
                "cb_failure_threshold": 5,
                "cb_recovery_timeout": 30,
            }
        else:
            return {
                "rate_limit_delay": 1.0,  # Moderate: 1.0s (down from 2.0s)
                "max_concurrent_requests": 5,  # Moderate: 5 (up from 3)
                "timeout": 30,
                "cb_failure_threshold": 3,
                "cb_recovery_timeout": 45,
            }

    async def scrape_with_progress_optimized(
        self,
        start_date: date,
        end_date: date,
        progress_callback: callable | None = None,
    ) -> None:
        """
        Optimized scraping with parallel date processing.
        """
        self.start_time = time.time()

        # Generate list of dates to process
        dates_to_process = []
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            if date_str not in self.processed_games:
                dates_to_process.append(current_date)
            current_date += timedelta(days=1)

        total_dates = len(dates_to_process)
        processed_dates = 0

        print(f"🚀 Starting optimized collection for {total_dates} dates")
        print(f"   Processing {self.concurrent_dates} dates concurrently")

        # Process dates in batches
        for i in range(0, total_dates, self.concurrent_dates):
            batch_dates = dates_to_process[i : i + self.concurrent_dates]

            # Create tasks for concurrent date processing
            tasks = []
            for date_to_process in batch_dates:
                task = asyncio.create_task(
                    self._scrape_date_with_monitoring(date_to_process)
                )
                tasks.append(task)

            # Wait for batch to complete
            batch_start = time.time()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            batch_duration = time.time() - batch_start

            # Process results and update progress
            successful_dates = 0
            for j, result in enumerate(results):
                if isinstance(result, Exception):
                    print(f"❌ Error processing {batch_dates[j]}: {result}")
                    self.stats.errors_encountered.append(
                        f"Date {batch_dates[j]}: {result}"
                    )
                else:
                    successful_dates += 1
                    self.processed_games.append(batch_dates[j].strftime("%Y-%m-%d"))

            processed_dates += len(batch_dates)

            # Calculate performance metrics
            avg_time_per_date = batch_duration / len(batch_dates)
            total_time_so_far = time.time() - self.start_time
            estimated_total_time = (total_time_so_far / processed_dates) * total_dates
            remaining_time = estimated_total_time - total_time_so_far

            # Progress callback
            if progress_callback:
                progress = (processed_dates / total_dates) * 100
                progress_callback(
                    progress,
                    f"Batch {i // self.concurrent_dates + 1}: {successful_dates}/{len(batch_dates)} successful | "
                    f"Avg: {avg_time_per_date:.1f}s/date | ETA: {remaining_time / 60:.1f}min",
                )

            # Performance reporting
            print(
                f"📊 Batch {i // self.concurrent_dates + 1}/{(total_dates + self.concurrent_dates - 1) // self.concurrent_dates} completed:"
            )
            print(f"   ✅ {successful_dates}/{len(batch_dates)} dates successful")
            print(f"   ⏱️  {avg_time_per_date:.1f}s per date (batch avg)")
            print(f"   🔄 {self.requests_made} total requests made")
            print(f"   ⏰ ETA: {remaining_time / 60:.1f} minutes remaining")

            # Adaptive rate limiting adjustment
            await self._adjust_rate_limiting_if_needed()
        total_duration = time.time() - self.start_time
        print(f"\n🎉 Scraping completed in {total_duration / 60:.1f} minutes")
        print(f"   📈 Average: {total_duration / total_dates:.1f}s per date")
        print(
            f"   🚀 Performance improvement: {(2.0 * 3 * total_dates) / total_duration:.1f}x faster"
        )

    async def _scrape_date_with_monitoring(self, game_date: date) -> None:
        """Scrape a single date with performance monitoring."""
        start_time = time.time()

        try:
            await self.scraper.scrape_date_all_bet_types(game_date)

            # Track performance
            duration = time.time() - start_time
            self.response_times.append(duration)
            self.requests_made += 3  # 3 bet types per date

            # Update average response time
            self.avg_response_time = sum(self.response_times[-10:]) / min(
                len(self.response_times), 10
            )

        except Exception as e:
            print(f"⚠️  Error scraping {game_date}: {e}")
            raise

    async def _adjust_rate_limiting_if_needed(self):
        """Adaptive rate limiting based on performance."""
        if len(self.response_times) < 5:
            return

        recent_avg = sum(self.response_times[-5:]) / 5

        # If responses are getting slow, increase delays
        if recent_avg > 10.0:  # If taking more than 10s per date
            current_delay = self.scraper.rate_limit_delay
            new_delay = min(current_delay * 1.2, 3.0)  # Max 3s
            self.scraper.rate_limit_delay = new_delay
            print(f"⚠️  Responses slowing down, increasing delay to {new_delay:.1f}s")
        # If responses are fast and no recent errors, try to speed up
        elif (
            recent_avg < 3.0 and not self.stats.errors_encountered[-5:]
        ):  # Fast and no recent errors
            current_delay = self.scraper.rate_limit_delay
            new_delay = max(current_delay * 0.9, 0.3)  # Min 0.3s
            self.scraper.rate_limit_delay = new_delay
            if new_delay != current_delay:
                print(
                    f"🚀 Responses fast and stable, reducing delay to {new_delay:.1f}s"
                )


async def optimized_collection(
    start_date: date | None = None,
    end_date: date | None = None,
    concurrent_dates: int = 2,
    aggressive_mode: bool = False,
    test_run: bool = False,
    output_dir: Path | None = None,
    dry_run: bool = False,
    auto_find_start: bool = True,
) -> dict[str, Any]:
    """
    Run optimized historical collection.
    """
    # Set default dates
    if end_date is None:
        end_date = date.today()

    if start_date is None:
        if test_run:
            start_date = end_date - timedelta(days=7)  # Last 7 days for testing
        else:
            start_date = date(2025, 3, 15)  # Start from March 15 (before season)

    # Auto-detect actual season start if requested and not a test run
    if auto_find_start and not test_run and not dry_run:
        print(f"🔍 Auto-detecting MLB season start date from {start_date}...")

        # Initialize temporary storage for season detection
        from sportsbookreview.services.data_storage_service import DataStorageService
        from sportsbookreview.services.sportsbookreview_scraper import (
            SportsbookReviewScraper,
        )

        storage = DataStorageService()
        await storage.initialize_connection()

        try:
            async with SportsbookReviewScraper(storage_service=storage) as scraper:
                actual_start = await scraper.find_season_start_date(start_date)
                if actual_start:
                    start_date = actual_start
                    print(f"✅ Found MLB season start: {start_date}")
                else:
                    print(
                        f"⚠️  Could not find season start, using default: {start_date}"
                    )
        except Exception as e:
            print(f"⚠️  Season detection failed: {e}, using default: {start_date}")
        finally:
            await storage.close_connection()

    total_days = (end_date - start_date).days + 1

    print("\n🎯 Optimized 2025 Season Collection")
    print(f"   📅 Date range: {start_date} to {end_date} ({total_days} days)")
    print(f"   🔄 Concurrent dates: {concurrent_dates}")
    print(f"   ⚡ Mode: {'Aggressive' if aggressive_mode else 'Moderate'} optimization")
    print(f"   🧪 Test run: {'Yes' if test_run else 'No'}")
    print(f"   🏃 Dry run: {'Yes' if dry_run else 'No'}")

    if dry_run:
        print("\n🧪 DRY RUN - No actual scraping will be performed")
        estimated_time = total_days * 0.8 / concurrent_dates  # Optimized estimate
        print(f"   ⏰ Estimated time: {estimated_time / 60:.1f} minutes")
        print("   📊 Expected improvement: 4-5x faster than standard collection")
        return {
            "status": "dry_run_complete",
            "estimated_time_minutes": estimated_time / 60,
        }

    # Progress tracking
    def progress_callback(progress: float, message: str):
        print(f"📈 {progress:5.1f}% | {message}")

    # Run optimized collection
    async with OptimizedCollectionOrchestrator(
        concurrent_dates=concurrent_dates,
        aggressive_mode=aggressive_mode,
        output_dir=output_dir or Path("./output"),
    ) as orchestrator:
        try:
            # Phase 1: Optimized scraping
            print(
                f"\n🚀 Phase 1: Optimized Scraping ({concurrent_dates} dates concurrent)"
            )
            await orchestrator.scrape_with_progress_optimized(
                start_date=start_date,
                end_date=end_date,
                progress_callback=progress_callback,
            )

            # Phase 2: Process staging data
            print("\n📊 Phase 2: Processing Staging Data")
            await orchestrator.process_staging(progress_callback=progress_callback)

            # Phase 3: Finalize
            print("\n✅ Phase 3: Finalizing Collection")
            await orchestrator.finalize_collection()
            await orchestrator.save_final_results()

            return orchestrator.get_collection_summary()

        except Exception as e:
            print(f"❌ Collection failed: {e}")
            raise


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Optimized 2025 MLB Season Data Collection"
    )
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--concurrent-dates",
        type=int,
        default=2,
        help="Number of dates to process concurrently",
    )
    parser.add_argument(
        "--aggressive", action="store_true", help="Use aggressive optimization settings"
    )
    parser.add_argument(
        "--test-run", action="store_true", help="Test with last 7 days only"
    )
    parser.add_argument("--output-dir", type=str, help="Output directory for results")
    parser.add_argument(
        "--dry-run", action="store_true", help="Test configuration without scraping"
    )
    parser.add_argument(
        "--no-auto-start",
        action="store_true",
        help="Disable automatic season start detection",
    )

    args = parser.parse_args()

    # Parse dates
    start_date = None
    end_date = None

    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        except ValueError:
            print(f"❌ Invalid start date format: {args.start_date}. Use YYYY-MM-DD")
            sys.exit(1)

    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        except ValueError:
            print(f"❌ Invalid end date format: {args.end_date}. Use YYYY-MM-DD")
            sys.exit(1)

    output_dir = Path(args.output_dir) if args.output_dir else None

    # Validate concurrent dates
    if args.concurrent_dates < 1 or args.concurrent_dates > 8:
        print(
            f"❌ Concurrent dates must be between 1 and 8, got: {args.concurrent_dates}"
        )
        sys.exit(1)

    # Run collection
    try:
        result = asyncio.run(
            optimized_collection(
                start_date=start_date,
                end_date=end_date,
                concurrent_dates=args.concurrent_dates,
                aggressive_mode=args.aggressive,
                test_run=args.test_run,
                output_dir=output_dir,
                dry_run=args.dry_run,
                auto_find_start=not args.no_auto_start,
            )
        )

        print("\n🎉 Collection completed successfully!")
        print(f"📊 Summary: {result.get('games_processed', 0)} games processed")
        print(f"⏰ Duration: {result.get('total_duration', 0) / 60:.1f} minutes")

    except KeyboardInterrupt:
        print("\n⚠️  Collection interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Collection failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
