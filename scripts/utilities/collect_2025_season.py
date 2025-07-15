#!/usr/bin/env python3
"""
Collect All 2025 MLB Season Data

This script runs a comprehensive data collection for the entire 2025 MLB season
using the SportsbookReview collection orchestrator.

Usage:
    python collect_2025_season.py [options]

Options:
    --start-date: Start date (default: 2025-01-01)
    --end-date: End date (default: today)
    --resume: Resume from checkpoint if available
    --output-dir: Output directory for results
    --batch-size: Batch size for processing
    --dry-run: Test run without actual scraping
"""

import argparse
import asyncio
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any

import structlog

# Import the collection orchestrator
from sportsbookreview.services.collection_orchestrator import (
    CollectionOrchestrator,
)

logger = structlog.get_logger(__name__)


class Season2025Collector:
    """Collector for the entire 2025 MLB season."""

    def __init__(
        self,
        start_date: date = date(2025, 1, 1),
        end_date: date | None = None,
        output_dir: Path | None = None,
        resume_from_checkpoint: bool = True,
        batch_size: int = 100,
        dry_run: bool = False,
    ):
        self.start_date = start_date
        self.end_date = end_date or date.today()
        self.output_dir = output_dir or Path("./season_2025_output")
        self.resume_from_checkpoint = resume_from_checkpoint
        self.batch_size = batch_size
        self.dry_run = dry_run

        # Ensure output directory exists
        self.output_dir.mkdir(exist_ok=True)

        # Calculate collection scope
        self.total_days = (self.end_date - self.start_date).days + 1

        logger.info(
            "Season 2025 Collector initialized",
            start_date=self.start_date,
            end_date=self.end_date,
            total_days=self.total_days,
            output_dir=str(self.output_dir),
            resume_from_checkpoint=self.resume_from_checkpoint,
            dry_run=self.dry_run,
        )

    def progress_callback(self, progress: float, message: str):
        """Progress callback for collection updates."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] {progress:6.1f}% - {message}")

        # Log major milestones
        if progress in [10, 25, 50, 75, 90, 100]:
            logger.info(f"Collection progress: {progress:.1f}%", message=message)

    async def collect_season_data(self) -> dict[str, Any]:
        """
        Collect all 2025 season data.

        Returns:
            Dictionary with collection results and statistics
        """
        print("🚀 COLLECTING 2025 MLB SEASON DATA")
        print("=" * 60)
        print(f"📅 Date Range: {self.start_date} to {self.end_date}")
        print(f"📊 Total Days: {self.total_days}")
        print(f"📁 Output Directory: {self.output_dir}")
        print(f"🔄 Resume from Checkpoint: {self.resume_from_checkpoint}")
        print(f"🧪 Dry Run: {self.dry_run}")
        print("=" * 60)

        if self.dry_run:
            print("🧪 DRY RUN MODE - No actual scraping will be performed")
            return {"status": "dry_run", "message": "Dry run completed successfully"}

        try:
            # Use the collection orchestrator
            async with CollectionOrchestrator(
                output_dir=self.output_dir,
                checkpoint_interval=50,  # Save checkpoint every 50 days
                enable_checkpoints=True,
            ) as orchestrator:
                print("✅ Collection orchestrator initialized")

                # Test connectivity first
                print("\n🔍 Testing connectivity...")
                if not await orchestrator.scraper.test_connectivity():
                    raise Exception("Failed connectivity test to SportsbookReview.com")
                print("✅ Connectivity test passed")

                # Run the historical collection
                print("\n📡 Starting data collection...")
                result = await orchestrator.collect_historical_data(
                    start_date=self.start_date,
                    end_date=self.end_date,
                    progress_callback=self.progress_callback,
                    resume_from_checkpoint=self.resume_from_checkpoint,
                )

                print("\n✅ COLLECTION COMPLETED!")
                return result

        except Exception as e:
            logger.error("Collection failed", error=str(e))
            print(f"❌ Collection failed: {e}")
            raise

    async def verify_collection_results(self) -> dict[str, Any]:
        """
        Verify the collection results by checking database.

        Returns:
            Dictionary with verification results
        """
        print("\n🔍 VERIFYING COLLECTION RESULTS")
        print("=" * 40)

        try:
            from sportsbookreview.services.data_storage_service import (
                DataStorageService,
            )

            storage = DataStorageService()
            await storage.initialize_connection()

            # Check recent data
            verification_results = {
                "games_collected": 0,
                "moneyline_records": 0,
                "spreads_records": 0,
                "totals_records": 0,
                "date_range_covered": f"{self.start_date} to {self.end_date}",
            }

            # Query database for verification
            async with storage.pool.acquire() as conn:
                # Count games in date range
                games_result = await conn.fetchval(
                    """
                    SELECT COUNT(*) 
                    FROM public.games 
                    WHERE game_date >= $1 AND game_date <= $2
                """,
                    self.start_date,
                    self.end_date,
                )
                verification_results["games_collected"] = games_result

                # Count betting records
                moneyline_result = await conn.fetchval(
                    """
                    SELECT COUNT(*) 
                    FROM mlb_betting.moneyline m
                    JOIN public.games g ON m.game_id = g.id
                    WHERE g.game_date >= $1 AND g.game_date <= $2
                """,
                    self.start_date,
                    self.end_date,
                )
                verification_results["moneyline_records"] = moneyline_result

                spreads_result = await conn.fetchval(
                    """
                    SELECT COUNT(*) 
                    FROM mlb_betting.spreads s
                    JOIN public.games g ON s.game_id = g.id
                    WHERE g.game_date >= $1 AND g.game_date <= $2
                """,
                    self.start_date,
                    self.end_date,
                )
                verification_results["spreads_records"] = spreads_result

                totals_result = await conn.fetchval(
                    """
                    SELECT COUNT(*) 
                    FROM mlb_betting.totals t
                    JOIN public.games g ON t.game_id = g.id
                    WHERE g.game_date >= $1 AND g.game_date <= $2
                """,
                    self.start_date,
                    self.end_date,
                )
                verification_results["totals_records"] = totals_result

            await storage.close_connection()

            # Display results
            print(f"📊 Games Collected: {verification_results['games_collected']:,}")
            print(
                f"💰 Moneyline Records: {verification_results['moneyline_records']:,}"
            )
            print(f"📈 Spreads Records: {verification_results['spreads_records']:,}")
            print(f"📊 Totals Records: {verification_results['totals_records']:,}")

            total_betting_records = (
                verification_results["moneyline_records"]
                + verification_results["spreads_records"]
                + verification_results["totals_records"]
            )
            print(f"📋 Total Betting Records: {total_betting_records:,}")

            if verification_results["games_collected"] > 0:
                avg_records_per_game = (
                    total_betting_records / verification_results["games_collected"]
                )
                print(f"📈 Average Records per Game: {avg_records_per_game:.1f}")

            return verification_results

        except Exception as e:
            logger.error("Verification failed", error=str(e))
            print(f"❌ Verification failed: {e}")
            return {"error": str(e)}

    async def run_full_collection(self) -> dict[str, Any]:
        """
        Run the complete collection and verification process.

        Returns:
            Dictionary with complete results
        """
        start_time = datetime.now()

        try:
            # Step 1: Collect data
            collection_results = await self.collect_season_data()

            # Step 2: Verify results
            verification_results = await self.verify_collection_results()

            # Step 3: Generate summary
            end_time = datetime.now()
            duration = end_time - start_time

            summary = {
                "status": "success",
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": duration.total_seconds(),
                "date_range": f"{self.start_date} to {self.end_date}",
                "total_days": self.total_days,
                "collection_results": collection_results,
                "verification_results": verification_results,
            }

            print("\n🎉 SEASON 2025 COLLECTION SUMMARY")
            print("=" * 50)
            print(f"⏱️  Duration: {duration}")
            print(f"📅 Date Range: {self.start_date} to {self.end_date}")
            print(f"📊 Total Days: {self.total_days}")
            print(f"✅ Status: {summary['status']}")

            return summary

        except Exception as e:
            logger.error("Full collection failed", error=str(e))
            return {
                "status": "failed",
                "error": str(e),
                "start_time": start_time.isoformat(),
                "end_time": datetime.now().isoformat(),
            }


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Collect all 2025 MLB season data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=date(2025, 1, 1),
        help="Start date for collection (YYYY-MM-DD, default: 2025-01-01)",
    )

    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=date.today(),
        help="End date for collection (YYYY-MM-DD, default: today)",
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./season_2025_output"),
        help="Output directory for results",
    )

    parser.add_argument(
        "--resume", action="store_true", help="Resume from checkpoint if available"
    )

    parser.add_argument(
        "--no-resume", action="store_true", help="Start fresh (ignore checkpoints)"
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Batch size for processing (default: 100)",
    )

    parser.add_argument(
        "--dry-run", action="store_true", help="Test run without actual scraping"
    )

    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only run verification (skip collection)",
    )

    return parser.parse_args()


async def main():
    """Main entry point."""
    args = parse_arguments()

    # Determine resume setting
    resume_from_checkpoint = args.resume or not args.no_resume

    # Create collector
    collector = Season2025Collector(
        start_date=args.start_date,
        end_date=args.end_date,
        output_dir=args.output_dir,
        resume_from_checkpoint=resume_from_checkpoint,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )

    try:
        if args.verify_only:
            # Only run verification
            print("🔍 VERIFICATION ONLY MODE")
            results = await collector.verify_collection_results()
        else:
            # Run full collection
            results = await collector.run_full_collection()

        # Save results to file
        results_file = (
            args.output_dir
            / f"collection_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )

        import json

        with open(results_file, "w") as f:
            json.dump(results, f, indent=2, default=str)

        print(f"\n📄 Results saved to: {results_file}")

        # Exit with appropriate code
        if results.get("status") == "success":
            print("\n🎉 Collection completed successfully!")
            return 0
        else:
            print("\n❌ Collection failed!")
            return 1

    except KeyboardInterrupt:
        print("\n⚠️  Collection interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Collection failed: {e}")
        logger.error("Main execution failed", error=str(e))
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
