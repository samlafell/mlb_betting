"""
Collection orchestrator for SportsbookReview historical data pipeline.

This service coordinates the complete data collection process from scraping
SportsbookReview.com to storing data in the database with MLB API enrichment.
"""

import asyncio
import json
import logging
import sys
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

# Handle imports for both module usage and direct execution
try:
    # Try relative imports first (when used as module)
    from .data_storage_service import DataStorageService
    from .integration_service import IntegrationService
    from .sportsbookreview_scraper import SportsbookReviewScraper
except ImportError:
    # If relative imports fail, add project root to path and use absolute imports
    current_dir = Path(__file__).parent
    project_root = current_dir.parent.parent
    sys.path.insert(0, str(project_root))

    from sportsbookreview.services.data_storage_service import DataStorageService
    from sportsbookreview.services.integration_service import IntegrationService
    from sportsbookreview.services.sportsbookreview_scraper import (
        SportsbookReviewScraper,
    )


logger = logging.getLogger(__name__)


@dataclass
class CollectionStats:
    """Statistics for the complete collection process."""

    start_time: datetime
    end_time: datetime | None = None
    total_duration: float = 0.0

    # Scraping stats
    pages_scraped: int = 0
    pages_failed: int = 0
    scraping_success_rate: float = 0.0

    # Storage stats
    games_processed: int = 0
    games_stored: int = 0
    betting_records_stored: int = 0
    storage_success_rate: float = 0.0

    # MLB enrichment stats
    mlb_enrichments_applied: int = 0
    enrichment_success_rate: float = 0.0

    # Error tracking
    errors_encountered: list[str] = None
    failed_urls: list[str] = None

    def __post_init__(self):
        if self.errors_encountered is None:
            self.errors_encountered = []
        if self.failed_urls is None:
            self.failed_urls = []


class CollectionOrchestrator:
    """
    Orchestrates the complete SportsbookReview data collection pipeline.

    Coordinates scraping, MLB API enrichment, and database storage with
    comprehensive error handling and progress tracking.
    """

    def __init__(
        self,
        output_dir: Path | None = None,
        checkpoint_interval: int = 50,
        enable_checkpoints: bool = True,
        max_retries: int = 3,
    ):
        """
        Initialize the collection orchestrator.

        Args:
            output_dir: Directory for output files and checkpoints
            checkpoint_interval: Save checkpoint every N games
            enable_checkpoints: Whether to enable checkpoint saving
            max_retries: Maximum retry attempts for failed operations
        """
        self.output_dir = output_dir or Path("./output")
        self.checkpoint_interval = checkpoint_interval
        self.enable_checkpoints = enable_checkpoints
        self.max_retries = max_retries

        # Ensure output directory exists
        self.output_dir.mkdir(exist_ok=True)

        # Initialize services
        self.scraper: SportsbookReviewScraper | None = None
        self.storage: DataStorageService | None = None

        # Collection state
        self.stats = CollectionStats(start_time=datetime.now())
        self.processed_games: list[str] = []  # Track processed game IDs
        self.checkpoint_file = self.output_dir / "collection_checkpoint.json"

    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize_services()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.cleanup_services()

    async def initialize_services(self):
        """Initialize scraper and storage services."""
        try:
            self.storage = DataStorageService()
            await self.storage.initialize_connection()

            self.scraper = SportsbookReviewScraper(storage_service=self.storage)
            await self.scraper.start_session()

            logger.info("Collection orchestrator initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize services: {e}")
            raise

    async def cleanup_services(self):
        """Clean up services."""
        try:
            if self.scraper:
                await self.scraper.close_session()

            if self.storage:
                await self.storage.close_connection()

        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

    async def collect_historical_data(
        self,
        start_date: date = date(2021, 4, 4),
        end_date: date | None = None,
        progress_callback: Callable[[float, str], None] | None = None,
        resume_from_checkpoint: bool = True,
    ) -> dict[str, Any]:
        """
        Collect complete historical data from SportsbookReview.

        Args:
            start_date: Start date for collection (default: April 4, 2021)
            end_date: End date for collection (default: current date)
            progress_callback: Optional progress callback function
            resume_from_checkpoint: Whether to resume from existing checkpoint

        Returns:
            Dictionary with collection statistics and results
        """
        if end_date is None:
            end_date = date.today()

        logger.info(
            f"Starting historical data collection from {start_date} to {end_date}"
        )

        # Load checkpoint if resuming
        if resume_from_checkpoint and self.checkpoint_file.exists():
            await self.load_checkpoint()

        try:
            # Phase 1: Test connectivity
            logger.info("Phase 1: Testing connectivity...")
            connectivity_ok = await self.scraper.test_connectivity()
            if not connectivity_ok:
                raise Exception("Failed connectivity test to SportsbookReview.com")

            # Phase 2: Scrape data
            logger.info("Phase 2: Scraping historical data...")
            await self.scrape_with_progress(
                start_date=start_date,
                end_date=end_date,
                progress_callback=progress_callback,
            )

            # Phase 3: Process staging rows into core tables
            logger.info("Phase 3: Processing data from staging area...")
            await self.process_staging(progress_callback=progress_callback)

            # Phase 4: Generate final statistics
            await self.finalize_collection()

            # Save final results
            await self.save_final_results()

            return self.get_collection_summary()

        except Exception as e:
            logger.error(f"Collection failed: {e}")
            self.stats.errors_encountered.append(str(e))
            raise

    async def process_staging(
        self,
        batch_size: int = 100,
        progress_callback: Callable[[float, str], None] | None = None,
    ):
        """Load rows from sbr_parsed_games(status='new' or 'parsed') into core tables via DataStorageService."""
        if not self.storage:
            logger.warning(
                "Storage service not initialised – skipping staging processing."
            )
            return

        processed_rows = 0
        integrator = IntegrationService(self.storage)

        # ✅ SAFEGUARD: Track bet_type distribution for monitoring
        bet_type_stats = {
            "moneyline": 0,
            "spread": 0,
            "totals": 0,
            "null_bet_type": 0,
            "skipped_odds": 0,
        }

        async with self.storage.pool.acquire() as conn:
            while True:
                rows = await conn.fetch(
                    "SELECT id, game_data FROM sbr_parsed_games WHERE status IN ('new', 'parsed') LIMIT $1",
                    batch_size,
                )
                if not rows:
                    break

                for row in rows:
                    try:
                        import json

                        game_dict = row["game_data"]
                        if isinstance(game_dict, str):
                            game_dict = json.loads(game_dict)

                        # ✅ MONITORING: Track game-level bet_type
                        game_bet_type = game_dict.get("bet_type")
                        if game_bet_type in bet_type_stats:
                            bet_type_stats[game_bet_type] += 1
                        elif not game_bet_type:
                            bet_type_stats["null_bet_type"] += 1
                            logger.warning(
                                f"Found game with null bet_type: {game_dict.get('sbr_game_id')}"
                            )

                        # Build betting_data records from odds_data list
                        odds_records = []
                        for odds in game_dict.get("odds_data", []):
                            record = {
                                "bet_type": game_dict.get("bet_type"),
                                "sportsbook": odds.get("sportsbook"),
                                "timestamp": game_dict.get("scraped_at"),
                            }

                            # ✅ SAFEGUARD: Validate bet_type before processing
                            if not record["bet_type"]:
                                # Try to get bet_type from individual odds record (new safeguard)
                                record["bet_type"] = odds.get("bet_type")

                                # If still null, log warning and skip this odds record
                                if not record["bet_type"]:
                                    logger.warning(
                                        f"Skipping odds record with null bet_type for game {game_dict.get('sbr_game_id')}"
                                    )
                                    bet_type_stats["skipped_odds"] += 1
                                    continue

                            # Map keys based on bet type
                            if record["bet_type"] == "moneyline":
                                record["home_ml"] = odds.get(
                                    "moneyline_home"
                                ) or odds.get("home_ml")
                                record["away_ml"] = odds.get(
                                    "moneyline_away"
                                ) or odds.get("away_ml")
                            elif record["bet_type"] == "spread":
                                record["home_spread"] = odds.get(
                                    "spread_home"
                                ) or odds.get("home_spread")
                                record["away_spread"] = odds.get(
                                    "spread_away"
                                ) or odds.get("away_spread")
                                record["home_spread_price"] = odds.get(
                                    "home_spread_price"
                                ) or odds.get("moneyline_home")
                                record["away_spread_price"] = odds.get(
                                    "away_spread_price"
                                ) or odds.get("moneyline_away")
                            elif record["bet_type"] in ("total", "totals"):
                                record["total_line"] = odds.get("total_line")
                                record["over_price"] = odds.get(
                                    "total_over"
                                ) or odds.get("over_price")
                                record["under_price"] = odds.get(
                                    "total_under"
                                ) or odds.get("under_price")

                            odds_records.append(record)

                        # Prepare game data for integration - preserve required validator fields
                        from sportsbookreview.models.game import EnhancedGame

                        allowed_fields = set(EnhancedGame.model_fields.keys())
                        cleaned_game = {
                            k: v for k, v in game_dict.items() if k in allowed_fields
                        }

                        # Ensure required validator fields are present
                        # Map game_date to game_datetime for validator compatibility
                        if "game_date" in game_dict:
                            cleaned_game["game_datetime"] = game_dict["game_date"]
                        elif "game_datetime" in game_dict:
                            cleaned_game["game_datetime"] = game_dict["game_datetime"]

                        # Ensure bet_type is preserved for validator
                        if "bet_type" not in cleaned_game:
                            cleaned_game["bet_type"] = game_dict.get("bet_type")

                        # Replace the odds_data with our constructed betting records
                        cleaned_game["odds_data"] = odds_records

                        # -----------------------------
                        # Per-row DB transaction
                        # -----------------------------
                        async with conn.transaction():
                            inserted = await integrator.integrate([cleaned_game])

                            # Determine appropriate status
                            if inserted > 0:
                                new_status = "loaded"
                            else:
                                # If nothing inserted, check if game already exists (duplicate)
                                sbr_id = game_dict.get("sbr_game_id")
                                existing = []
                                if sbr_id and self.storage:
                                    try:
                                        existing = (
                                            await self.storage.get_existing_games(
                                                [str(sbr_id)]
                                            )
                                        )
                                    except Exception as dup_exc:
                                        logger.debug(
                                            "Duplicate check failed for %s: %s",
                                            sbr_id,
                                            dup_exc,
                                        )

                                new_status = "duplicate" if existing else "failed"

                            # Persist status
                            await conn.execute(
                                "UPDATE sbr_parsed_games SET status=$1 WHERE id=$2",
                                new_status,
                                row["id"],
                            )

                        if inserted > 0:
                            processed_rows += 1
                        logger.debug(
                            "Staging row %s processed – inserted=%s, status=%s",
                            row["id"],
                            inserted,
                            new_status,
                        )
                    except Exception as exc:
                        logger.error(f"Failed to load staging row {row['id']}: {exc}")
                        await conn.execute(
                            "UPDATE sbr_parsed_games SET status='failed' WHERE id=$1",
                            row["id"],
                        )

                if progress_callback and processed_rows:
                    progress_callback(90, f"Promoted {processed_rows} staging rows")

        # ✅ MONITORING: Log bet_type distribution summary
        logger.info(f"Staging processing complete – promoted {processed_rows} rows")
        logger.info(f"Bet type distribution: {bet_type_stats}")

        # ✅ ALERT: Warn if significant null bet_type issues detected
        total_games = sum(bet_type_stats.values()) - bet_type_stats["skipped_odds"]
        if bet_type_stats["null_bet_type"] > 0:
            null_percentage = (
                bet_type_stats["null_bet_type"] / max(total_games, 1)
            ) * 100
            logger.warning(
                f"⚠️  NULL BET_TYPE DETECTED: {bet_type_stats['null_bet_type']} games ({null_percentage:.1f}%) had null bet_type"
            )

        if bet_type_stats["skipped_odds"] > 0:
            logger.warning(
                f"⚠️  SKIPPED ODDS: {bet_type_stats['skipped_odds']} odds records skipped due to null bet_type"
            )

    async def scrape_with_progress(
        self,
        start_date: date,
        end_date: date,
        progress_callback: Callable[[float, str], None] | None = None,
    ) -> None:
        """
        Scrape data with progress tracking and checkpointing.

        Args:
            start_date: Start date
            end_date: End date
            progress_callback: Progress callback function

        Returns:
            List of scraped game data
        """
        # This method will now orchestrate scraping into the staging area.
        # It no longer returns data.

        total_days = (end_date - start_date).days + 1
        current_date = start_date
        processed_days = 0

        while current_date <= end_date:
            try:
                # Skip if already processed (from checkpoint)
                date_str = current_date.strftime("%Y-%m-%d")
                if date_str in self.processed_games:
                    logger.debug(f"Skipping {current_date} (already processed)")
                    current_date += timedelta(days=1)
                    processed_days += 1
                    continue

                # Scrape all bet types for the day. Data is stored by the scraper.
                await self.scraper.scrape_date_all_bet_types(current_date)

                # Track processed date
                self.processed_games.append(date_str)

                logger.info(f"Scraped data for {current_date}")

                # Progress callback
                processed_days += 1
                if progress_callback:
                    progress = (
                        processed_days / total_days
                    ) * 100  # Scraping is 100% of this phase now
                    progress_callback(
                        progress,
                        f"Scraped {current_date} ({processed_days}/{total_days} days)",
                    )

                # Checkpoint saving can be added here if needed

            except Exception as e:
                logger.error(f"Error scraping {current_date}: {e}")
                self.stats.errors_encountered.append(f"Scraping {current_date}: {e}")

            finally:
                current_date += timedelta(days=1)

        logger.info("Scraping phase completed.")

    async def store_with_progress(
        self,
        scraped_data: list[dict[str, Any]],
        progress_callback: Callable[[float, str], None] | None = None,
    ) -> None:
        """
        Store scraped data with progress tracking.

        Args:
            scraped_data: List of scraped game data
            progress_callback: Progress callback function
        """
        total_games = len(scraped_data)

        # Process in batches
        batch_size = 25
        total_batches = (total_games + batch_size - 1) // batch_size

        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min((batch_idx + 1) * batch_size, total_games)
            batch = scraped_data[start_idx:end_idx]

            try:
                # Store batch
                stored_ids = await self.storage.store_batch_data(batch)

                self.stats.games_processed += len(batch)
                self.stats.games_stored += len(stored_ids)

                # Progress callback
                if progress_callback:
                    batch_progress = (batch_idx + 1) / total_batches
                    overall_progress = 50 + (
                        batch_progress * 40
                    )  # 40% for storage phase
                    progress_callback(
                        overall_progress,
                        f"Stored batch {batch_idx + 1}/{total_batches}",
                    )

                logger.info(
                    f"Stored batch {batch_idx + 1}/{total_batches} ({len(stored_ids)} games)"
                )

            except Exception as e:
                logger.error(f"Error storing batch {batch_idx + 1}: {e}")
                self.stats.errors_encountered.append(
                    f"Storage batch {batch_idx + 1}: {e}"
                )

        # Update storage stats
        self.stats.storage_success_rate = (
            self.stats.games_stored / max(self.stats.games_processed, 1)
        ) * 100

        # Get storage service stats
        storage_stats = self.storage.get_storage_stats()
        self.stats.betting_records_stored = storage_stats.get(
            "betting_records_inserted", 0
        )
        self.stats.mlb_enrichments_applied = storage_stats.get(
            "mlb_enrichments_applied", 0
        )

        if self.stats.games_processed > 0:
            self.stats.enrichment_success_rate = (
                self.stats.mlb_enrichments_applied / self.stats.games_processed
            ) * 100

    async def save_checkpoint(self, scraped_data: list[dict[str, Any]]):
        """Save collection checkpoint."""
        try:
            checkpoint_data = {
                "stats": asdict(self.stats),
                "processed_games": self.processed_games,
                "scraped_data_count": len(scraped_data),
                "timestamp": datetime.now().isoformat(),
            }

            with open(self.checkpoint_file, "w") as f:
                json.dump(checkpoint_data, f, indent=2)

            logger.debug(f"Checkpoint saved to {self.checkpoint_file}")

        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")

    async def load_checkpoint(self):
        """Load collection checkpoint."""
        try:
            with open(self.checkpoint_file) as f:
                checkpoint_data = json.load(f)

            # Restore processed games list
            self.processed_games = checkpoint_data.get("processed_games", [])

            # Restore stats
            stats_data = checkpoint_data.get("stats", {})
            if stats_data:
                # Convert start_time back to datetime
                if "start_time" in stats_data:
                    stats_data["start_time"] = datetime.fromisoformat(
                        stats_data["start_time"]
                    )
                if "end_time" in stats_data and stats_data["end_time"]:
                    stats_data["end_time"] = datetime.fromisoformat(
                        stats_data["end_time"]
                    )

                self.stats = CollectionStats(**stats_data)

            logger.info(
                f"Resumed from checkpoint with {len(self.processed_games)} processed games"
            )

        except Exception as e:
            logger.error(f"Error loading checkpoint: {e}")

    async def finalize_collection(self):
        """Finalize collection statistics."""
        self.stats.end_time = datetime.now()
        self.stats.total_duration = (
            self.stats.end_time - self.stats.start_time
        ).total_seconds()

        # --------------------------------------------------
        # Harvest runtime metrics from scraper & storage to
        # populate the summary JSON.  This replaces the
        # placeholder zeros currently emitted.
        # --------------------------------------------------

        # Scraper-side metrics
        scraper_stats = self.scraper.get_stats()
        self.stats.pages_scraped = scraper_stats.get("requests_made", 0)
        self.stats.pages_failed = scraper_stats.get("failed_urls", 0)
        self.stats.scraping_success_rate = scraper_stats.get("success_rate", 0.0)
        self.stats.failed_urls = list(self.scraper.failed_urls)

        # Storage-side metrics – relies on DataStorageService
        # updating its internal StorageStats counters. Even if
        # we only staged parsed games, we can still count them.
        if self.storage:
            s_stats = self.storage.get_storage_stats()
            self.stats.games_processed = s_stats.get("games_processed", 0)
            self.stats.games_stored = s_stats.get("games_inserted", 0)
            self.stats.betting_records_stored = s_stats.get(
                "betting_records_inserted", 0
            )

            if self.stats.games_processed:
                self.stats.storage_success_rate = (
                    self.stats.games_stored / self.stats.games_processed * 100
                )

        logger.info("Collection finalized")

    async def save_final_results(self):
        """Save final collection results."""
        try:
            results_file = (
                self.output_dir
                / f"collection_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            )

            results = {
                "collection_summary": self.get_collection_summary(),
                "detailed_stats": asdict(self.stats),
                "timestamp": datetime.now().isoformat(),
            }

            with open(results_file, "w") as f:
                json.dump(results, f, indent=2, default=str)

            logger.info(f"Final results saved to {results_file}")

            # Clean up checkpoint file
            if self.checkpoint_file.exists():
                self.checkpoint_file.unlink()

        except Exception as e:
            logger.error(f"Error saving final results: {e}")

    def get_collection_summary(self) -> dict[str, Any]:
        """
        Get collection summary statistics.

        Returns:
            Dictionary with summary statistics
        """
        return {
            "collection_period": {
                "start_time": self.stats.start_time.isoformat(),
                "end_time": self.stats.end_time.isoformat()
                if self.stats.end_time
                else None,
                "duration_seconds": self.stats.total_duration,
            },
            "scraping_results": {
                "pages_scraped": self.stats.pages_scraped,
                "pages_failed": self.stats.pages_failed,
                "success_rate_percent": round(self.stats.scraping_success_rate, 2),
            },
            "storage_results": {
                "games_processed": self.stats.games_processed,
                "games_stored": self.stats.games_stored,
                "betting_records_stored": self.stats.betting_records_stored,
                "success_rate_percent": round(self.stats.storage_success_rate, 2),
            },
            "mlb_enrichment": {
                "enrichments_applied": self.stats.mlb_enrichments_applied,
                "success_rate_percent": round(self.stats.enrichment_success_rate, 2),
            },
            "error_summary": {
                "total_errors": len(self.stats.errors_encountered),
                "failed_urls": len(self.stats.failed_urls),
                "error_rate_percent": round(
                    (
                        len(self.stats.errors_encountered)
                        / max(self.stats.pages_scraped, 1)
                    )
                    * 100,
                    2,
                ),
            },
        }

    async def collect_date_range(
        self,
        start_date: date,
        end_date: date,
        progress_callback: Callable[[float, str], None] | None = None,
    ) -> dict[str, Any]:
        """
        Collect data for a specific date range.

        Args:
            start_date: Start date
            end_date: End date
            progress_callback: Progress callback function

        Returns:
            Collection results
        """
        return await self.collect_historical_data(
            start_date=start_date,
            end_date=end_date,
            progress_callback=progress_callback,
            resume_from_checkpoint=False,
        )

    async def test_system(self) -> dict[str, Any]:
        """
        Test the complete system with a small sample.

        Returns:
            Test results
        """
        logger.info("Running system test...")

        # Test with last 3 days
        end_date = date.today()
        start_date = end_date - timedelta(days=2)

        try:
            results = await self.collect_date_range(
                start_date=start_date, end_date=end_date
            )

            results["test_status"] = "PASSED"
            logger.info("System test passed")

            return results

        except Exception as e:
            logger.error(f"System test failed: {e}")
            return {"test_status": "FAILED", "error": str(e)}


# Convenience functions
async def run_historical_collection(
    start_date: date = date(2021, 4, 4),
    end_date: date | None = None,
    output_dir: Path | None = None,
    progress_callback: Callable[[float, str], None] | None = None,
) -> dict[str, Any]:
    """
    Convenience function to run complete historical collection.

    Args:
        start_date: Start date for collection
        end_date: End date for collection
        output_dir: Output directory
        progress_callback: Progress callback function

    Returns:
        Collection results
    """
    async with CollectionOrchestrator(output_dir=output_dir) as orchestrator:
        return await orchestrator.collect_historical_data(
            start_date=start_date,
            end_date=end_date,
            progress_callback=progress_callback,
        )


async def test_collection_system(output_dir: Path | None = None) -> dict[str, Any]:
    """
    Test the collection system.

    Args:
        output_dir: Output directory

    Returns:
        Test results
    """
    async with CollectionOrchestrator(output_dir=output_dir) as orchestrator:
        return await orchestrator.test_system()


# Main execution when run directly
if __name__ == "__main__":
    import sys
    from pathlib import Path

    # Add parent directories to path for imports when run directly
    current_dir = Path(__file__).parent
    project_root = current_dir.parent.parent
    sys.path.insert(0, str(project_root))

    # Now we can import with absolute paths
    from sportsbookreview.services.data_storage_service import DataStorageService

    async def main():
        """Main function when run directly - process staging data."""
        print("🚀 COLLECTION ORCHESTRATOR - PROCESSING STAGING DATA")
        print("=" * 60)

        try:
            # Use the collection orchestrator to process staging
            async with CollectionOrchestrator() as orchestrator:
                print("✅ Collection orchestrator initialized")

                # Process staging data
                print("\n⚙️  Processing staging data...")
                await orchestrator.process_staging()
                print("✅ Staging processing complete!")

            # Verify results
            print("\n📊 Verifying recent results...")
            storage = DataStorageService()
            await storage.initialize_connection()

            try:
                # Check recent betting records
                from datetime import date, timedelta

                recent_date = date.today() - timedelta(days=1)  # Yesterday

                # 🚀 PHASE 2A: Use new consolidated schema tables
                try:
                    from src.mlb_sharp_betting.db.table_registry import (
                        get_table_registry,
                    )

                    table_registry = get_table_registry()
                    moneyline_table = table_registry.get_table("moneyline")
                    spreads_table = table_registry.get_table("spreads")
                    totals_table = table_registry.get_table("totals")
                except ImportError:
                    # Fallback to new schema tables if registry not available
                    moneyline_table = "core_betting.betting_lines_moneyline"
                    spreads_table = "core_betting.betting_lines_spreads"
                    totals_table = "core_betting.betting_lines_totals"

                results = await storage.pool.fetch(
                    f"""
                    SELECT 'moneyline' as table_name, COUNT(*) as count 
                    FROM {moneyline_table} 
                    WHERE DATE(odds_timestamp) >= $1
                    UNION ALL
                    SELECT 'spreads' as table_name, COUNT(*) as count 
                    FROM {spreads_table} 
                    WHERE DATE(odds_timestamp) >= $1
                    UNION ALL
                    SELECT 'totals' as table_name, COUNT(*) as count 
                    FROM {totals_table} 
                    WHERE DATE(odds_timestamp) >= $1
                """,
                    recent_date,
                )

                print(f"  Recent betting records (since {recent_date}):")
                total_records = 0
                for record in results:
                    count = record["count"]
                    total_records += count
                    print(f"    {record['table_name']}: {count} records")

                # Check staging status
                staging_status = await storage.pool.fetch(
                    """
                    SELECT status, COUNT(*) as count
                    FROM sbr_parsed_games 
                    WHERE DATE(parsed_at) >= $1
                    GROUP BY status
                    ORDER BY status
                """,
                    recent_date,
                )

                print("\n📦 Recent staging status:")
                for record in staging_status:
                    print(f"    {record['status']}: {record['count']} records")

                print(
                    f"\n🎉 PROCESSING COMPLETE - {total_records} recent records found"
                )

            finally:
                await storage.close_connection()

        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback

            traceback.print_exc()
            sys.exit(1)

    # Run the main function
    asyncio.run(main())
