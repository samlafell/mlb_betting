#!/usr/bin/env python3
"""
MLB Sharp Betting Data Pipeline Entrypoint

This script demonstrates the complete data pipeline:
1. Scrape betting splits data from VSIN
2. Parse and validate the raw data
3. Store validated data in PostgreSQL
4. Analyze data for sharp action indicators
5. Generate summary reports

Usage:
    python src/mlb_sharp_betting/entrypoint.py [options]

Options:
    --sport SPORT        Sport to scrape (default: mlb)
    --sportsbook BOOK    Sportsbook to use (default: circa)
    --dry-run           Run without making web requests
    --verbose           Enable verbose logging
    --output OUTPUT     Output file for results (optional)
"""

import argparse
import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import structlog

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.dev.ConsoleRenderer(colors=True),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

# Import our components
from mlb_sharp_betting.db.connection import get_db_manager
from mlb_sharp_betting.db.repositories import BettingSplitRepository, GameRepository
from mlb_sharp_betting.models.game import Team
from mlb_sharp_betting.models.splits import (
    BettingSplit,
    BookType,
    DataSource,
    SplitType,
)
from mlb_sharp_betting.services.data_service import get_data_service
from mlb_sharp_betting.services.game_manager import GameManager


class DataPipeline:
    """Complete data pipeline for MLB betting splits."""

    def __init__(
        self, sport: str = "mlb", sportsbook: str = "circa", dry_run: bool = False
    ):
        """
        Initialize the data pipeline.

        Args:
            sport: Sport to scrape data for
            sportsbook: Sportsbook to use as data source
            dry_run: If True, skip actual web requests
        """
        self.sport = sport
        self.sportsbook = sportsbook
        self.dry_run = dry_run

        # Initialize components using new consolidated services
        self.db_manager = get_db_manager()
        self.data_service = get_data_service(self.db_manager)
        self.betting_split_repo = BettingSplitRepository()
        self.game_repo = GameRepository()
        self.game_manager = GameManager(self.db_manager)

        # Initialize cross-market flip detector for pipeline integration
        try:
            from mlb_sharp_betting.services.cross_market_flip_detector import (
                CrossMarketFlipDetector,
            )

            self.flip_detector = CrossMarketFlipDetector(self.db_manager)
            self.flip_detection_enabled = True
        except ImportError:
            self.flip_detector = None
            self.flip_detection_enabled = False

        # Metrics
        self.metrics = {
            "scraped_records": 0,
            "parsed_records": 0,
            "valid_records": 0,
            "stored_records": 0,
            "games_processed": 0,
            "games_created": 0,
            "games_updated": 0,
            "sharp_indicators": 0,
            "flip_detections": 0,
            "high_confidence_flips": 0,
            "errors": 0,
            "start_time": None,
            "end_time": None,
        }

        logger.info(
            "Data pipeline initialized",
            sport=sport,
            sportsbook=sportsbook,
            dry_run=dry_run,
        )

    async def setup_database(self) -> None:
        """Set up database schema and tables using the proper schema manager."""
        logger.info("Setting up database schema via data persistence service")

        try:
            # The data persistence service will handle schema setup automatically
            # through its _ensure_schema() method in __init__
            logger.info("Database schema setup completed via repository pattern")

        except Exception as e:
            logger.error("Failed to setup database schema", error=str(e))
            raise

    async def collect_data(self) -> list[BettingSplit]:
        """Collect betting splits data from all sources."""
        logger.info(
            "Starting data collection", sport=self.sport, sportsbook=self.sportsbook
        )

        if self.dry_run:
            logger.info("Dry run mode - using mock data")
            mock_data = self._get_mock_data()
            # Convert mock data to BettingSplit objects for consistency
            splits = self._convert_mock_to_splits(mock_data)

            self.metrics["scraped_records"] = len(splits)
            self.metrics["parsed_records"] = len(splits)  # Already parsed
            logger.info(
                "Mock data collection completed successfully",
                splits_collected=len(splits),
            )
            return splits

        try:
            # Collect from all sources (SBD + VSIN) - real data
            splits = await self.data_service.collect_all_sources(sport=self.sport)

            self.metrics["scraped_records"] = len(splits)
            self.metrics["parsed_records"] = len(
                splits
            )  # Already parsed by data collector
            logger.info(
                "Data collection completed successfully", splits_collected=len(splits)
            )
            return splits

        except Exception as e:
            logger.error("Data collection failed with exception", error=str(e))
            self.metrics["errors"] += 1
            return []

    def _get_mock_data(self) -> list[dict]:
        """Generate mock data for testing."""
        return [
            {
                "Game": "Yankees @ Red Sox",
                "Spread": "-1.5",
                "Home Bets %": "65%",
                "Away Bets %": "35%",
                "Home Money %": "58%",
                "Away Money %": "42%",
                "source": DataSource.VSIN.value,
                "book": self.sportsbook,
                "sport": self.sport,
                "scraped_at": datetime.now().isoformat(),
            },
            {
                "Game": "Dodgers vs Giants",
                "Total": "8.5",
                "Over Bets %": "72%",
                "Under Bets %": "28%",
                "Over Money %": "68%",
                "Under Money %": "32%",
                "source": DataSource.VSIN.value,
                "book": self.sportsbook,
                "sport": self.sport,
                "scraped_at": datetime.now().isoformat(),
            },
            {
                "Game": "Cubs @ Cardinals",
                "Home Bets %": "45%",
                "Away Bets %": "55%",
                "Home Money %": "38%",  # Sharp action: money opposite to bets
                "Away Money %": "62%",
                "source": DataSource.VSIN.value,
                "book": self.sportsbook,
                "sport": self.sport,
                "scraped_at": datetime.now().isoformat(),
            },
            {
                "Game": "Astros @ Mariners",
                "Home Bets %": "75%",
                "Away Bets %": "25%",
                "Home Money %": "45%",  # SHARP ACTION: 30% difference!
                "Away Money %": "55%",
                "source": DataSource.VSIN.value,
                "book": self.sportsbook,
                "sport": self.sport,
                "scraped_at": datetime.now().isoformat(),
            },
        ]

    def _convert_mock_to_splits(self, mock_data: list[dict]) -> list[BettingSplit]:
        """Convert mock data to BettingSplit objects."""
        splits = []

        for i, data in enumerate(mock_data):
            try:
                # Extract percentages
                home_bets_pct = float(data.get("Home Bets %", "50").replace("%", ""))
                away_bets_pct = float(data.get("Away Bets %", "50").replace("%", ""))
                home_money_pct = float(data.get("Home Money %", "50").replace("%", ""))
                away_money_pct = float(data.get("Away Money %", "50").replace("%", ""))

                # Calculate sharp action
                sharp_action = None
                if abs(home_bets_pct - home_money_pct) >= 10.0:
                    sharp_action = "home" if home_money_pct > home_bets_pct else "away"

                # Create a basic BettingSplit from mock data
                split = BettingSplit(
                    game_id=f"mock_game_{i}",
                    home_team=Team.NYY,  # New York Yankees
                    away_team=Team.BOS,  # Boston Red Sox
                    game_datetime=datetime.now(),
                    split_type=SplitType.SPREAD,  # Default type
                    split_value=None,
                    source=DataSource.VSIN,
                    book=BookType.CIRCA,
                    last_updated=datetime.now(),
                    home_or_over_bets_percentage=home_bets_pct,
                    away_or_under_bets_percentage=away_bets_pct,
                    home_or_over_stake_percentage=home_money_pct,
                    away_or_under_stake_percentage=away_money_pct,
                    sharp_action=sharp_action,
                )
                splits.append(split)
            except Exception as e:
                logger.warning(
                    "Failed to convert mock data item", item=data, error=str(e)
                )

        return splits

    def process_games_from_splits(
        self, betting_splits: list[BettingSplit]
    ) -> dict[str, int]:
        """
        Process and store game information discovered from betting splits.

        Args:
            betting_splits: List of BettingSplit objects

        Returns:
            Dictionary with game processing statistics
        """
        logger.info("Processing games from betting splits data")

        try:
            # Convert BettingSplit objects to dictionaries for game manager
            splits_data = []
            for split in betting_splits:
                splits_data.append(
                    {
                        "game_id": split.game_id,
                        "home_team": split.home_team,
                        "away_team": split.away_team,
                        "game_datetime": split.game_datetime,
                    }
                )

            # Process games using the game manager
            game_stats = self.game_manager.process_games_from_betting_splits(
                splits_data
            )

            # Update pipeline metrics
            self.metrics["games_processed"] = game_stats.get("processed", 0)
            self.metrics["games_created"] = game_stats.get("created", 0)
            self.metrics["games_updated"] = game_stats.get("updated", 0)

            logger.info("Game processing completed", **game_stats)
            return game_stats

        except Exception as e:
            logger.error("Failed to process games from splits", error=str(e))
            self.metrics["errors"] += 1
            return {"processed": 0, "created": 0, "updated": 0, "errors": 1}

    def validate_and_store_data(
        self, betting_splits: list[BettingSplit]
    ) -> list[BettingSplit]:
        """Validate and store betting splits in the database using the repository pattern."""
        logger.info(
            "Starting data validation and storage",
            records_to_validate=len(betting_splits),
        )

        if not betting_splits:
            logger.warning("No betting splits provided for validation and storage")
            return []

        # Debug: Log first split to see if data is valid
        logger.info(
            "Sample split data",
            first_split=f"ID: {betting_splits[0].game_id}, Teams: {betting_splits[0].home_team} vs {betting_splits[0].away_team}",
        )

        try:
            # Use the data service for proper validation and storage
            storage_stats = self.data_service.store_splits(
                splits=betting_splits,
                batch_size=100,
                validate=True,
                skip_duplicates=True,
            )

            # Debug: Log detailed storage stats
            logger.info(
                "Storage stats details",
                processed=storage_stats.get("processed", 0),
                stored=storage_stats.get("stored", 0),
                skipped=storage_stats.get("skipped", 0),
                validation_errors=storage_stats.get("validation_errors", 0),
                timing_rejections=storage_stats.get("timing_rejections", 0),
            )

            # Update metrics based on storage results
            self.metrics["stored_records"] = storage_stats["stored"]
            self.metrics["valid_records"] = (
                storage_stats["stored"] + storage_stats["skipped"]
            )
            self.metrics["errors"] += storage_stats["validation_errors"]

            logger.info(
                "Data validation and storage completed using repository pattern",
                storage_stats=storage_stats,
            )

            # Return the splits that were successfully processed
            stored_count = storage_stats["stored"]
            if stored_count > 0:
                logger.info("Returning successfully stored splits", count=stored_count)
                return betting_splits[:stored_count]
            else:
                logger.warning(
                    "No splits were stored successfully - returning empty list"
                )
                return []

        except Exception as e:
            logger.error(
                "Failed to store betting splits via repository",
                error=str(e),
                traceback=True,
            )
            self.metrics["errors"] += len(betting_splits)
            return []

    def analyze_data(self) -> dict:
        """Analyze stored data for insights and sharp action."""
        logger.info("Starting data analysis")

        try:
            with self.db_manager.get_cursor() as cursor:
                # Get total splits count
                cursor.execute("SELECT COUNT(*) FROM splits.raw_mlb_betting_splits")
                total_splits = cursor.fetchone()[0]

                # Find sharp action indicators (money percentage differs significantly from bet percentage)
                cursor.execute("""
                    SELECT game_id, home_team, away_team, split_type, split_value,
                           home_or_over_bets_percentage, home_or_over_stake_percentage,
                           away_or_under_bets_percentage, away_or_under_stake_percentage,
                           (ABS(home_or_over_bets_percentage - home_or_over_stake_percentage)) as bet_money_diff
                    FROM splits.raw_mlb_betting_splits 
                    WHERE ABS(home_or_over_bets_percentage - home_or_over_stake_percentage) >= 10.0
                    ORDER BY bet_money_diff DESC
                """)
                sharp_indicators = cursor.fetchall()

                # Get splits by type
                cursor.execute("""
                    SELECT split_type, COUNT(*) 
                    FROM splits.raw_mlb_betting_splits 
                    GROUP BY split_type
                """)
                splits_by_type = dict(cursor.fetchall())

                # Get average bet vs money percentages
                cursor.execute("""
                    SELECT 
                        AVG(home_or_over_bets_percentage) as avg_home_bets,
                        AVG(home_or_over_stake_percentage) as avg_home_money,
                        AVG(away_or_under_bets_percentage) as avg_away_bets,
                        AVG(away_or_under_stake_percentage) as avg_away_money
                    FROM splits.raw_mlb_betting_splits
                """)
                averages = cursor.fetchone()

                # Update sharp action flags only for records that don't already have sharp action set
                # This preserves the specific direction ("home", "away", "over", "under") set by parsers
                cursor.execute("""
                    UPDATE splits.raw_mlb_betting_splits 
                    SET sharp_action = CASE 
                        WHEN split_type IN ('spread', 'moneyline') THEN
                            CASE WHEN home_or_over_stake_percentage > home_or_over_bets_percentage THEN 'home' ELSE 'away' END
                        WHEN split_type = 'total' THEN
                            CASE WHEN home_or_over_stake_percentage > home_or_over_bets_percentage THEN 'over' ELSE 'under' END
                    END
                    WHERE ABS(home_or_over_bets_percentage - home_or_over_stake_percentage) >= 10.0
                      AND (sharp_action IS NULL OR sharp_action = 'true' OR sharp_action = 'false')
                """)

                self.metrics["sharp_indicators"] = len(sharp_indicators)

                analysis_results = {
                    "total_splits": total_splits,
                    "sharp_indicators": sharp_indicators,
                    "sharp_count": len(sharp_indicators),
                    "splits_by_type": splits_by_type,
                    "averages": {
                        "home_bets_pct": averages[0] if averages[0] else 0,
                        "home_money_pct": averages[1] if averages[1] else 0,
                        "away_bets_pct": averages[2] if averages[2] else 0,
                        "away_money_pct": averages[3] if averages[3] else 0,
                    },
                }

                logger.info(
                    "Data analysis completed",
                    total_splits=total_splits,
                    sharp_indicators=len(sharp_indicators),
                    splits_by_type=splits_by_type,
                )

                return analysis_results

        except Exception as e:
            logger.error("Data analysis failed", error=str(e))
            self.metrics["errors"] += 1
            return {}

    async def _run_flip_detection(self) -> dict[str, Any] | None:
        """
        Run cross-market flip detection as part of the pipeline.

        Returns:
            Dictionary with flip detection results or None if disabled/failed
        """
        if not self.flip_detection_enabled or not self.flip_detector:
            logger.debug("Flip detection disabled or not available")
            return None

        try:
            logger.info("Running cross-market flip detection")

            # Run today's flip detection with summary
            flips, summary = await self.flip_detector.detect_todays_flips_with_summary(
                min_confidence=75.0  # Use high confidence threshold for pipeline
            )

            # Update metrics
            self.metrics["flip_detections"] = len(flips)
            self.metrics["high_confidence_flips"] = len(
                [f for f in flips if f.confidence_score >= 85.0]
            )

            # Log results
            if flips:
                logger.info(
                    "Cross-market flip detection completed",
                    flips_found=len(flips),
                    games_evaluated=summary.get("games_evaluated", 0),
                    high_confidence=self.metrics["high_confidence_flips"],
                )

                # Log high-confidence flips for immediate attention
                high_confidence_flips = [f for f in flips if f.confidence_score >= 85.0]
                if high_confidence_flips:
                    logger.warning(
                        "⚠️ HIGH CONFIDENCE FLIPS DETECTED IN PIPELINE",
                        count=len(high_confidence_flips),
                    )
                    for flip in high_confidence_flips[:3]:  # Show top 3
                        logger.warning(
                            f"🔥 FLIP: {flip.away_team} @ {flip.home_team} | "
                            f"{flip.confidence_score:.1f}% confidence | "
                            f"Recommendation: {flip.strategy_recommendation}"
                        )
            else:
                logger.info(
                    "Cross-market flip detection completed - no qualifying flips found",
                    games_evaluated=summary.get("games_evaluated", 0),
                )

            return {
                "flips_found": len(flips),
                "high_confidence_flips": self.metrics["high_confidence_flips"],
                "summary": summary,
                "flips": flips[:5] if flips else [],  # Include top 5 flips in results
                "execution_time": datetime.now().isoformat(),
            }

        except Exception as e:
            logger.error("Cross-market flip detection failed in pipeline", error=str(e))
            self.metrics["errors"] += 1
            return {"error": str(e), "execution_time": datetime.now().isoformat()}

    async def _update_strategy_configurations(self) -> None:
        """
        Update strategy configurations based on recent performance.
        Runs backtesting on recent data to keep strategy configurations current.
        """
        logger.info("Updating strategy configurations based on recent performance")

        try:
            # Import backtesting engine
            from mlb_sharp_betting.services.backtesting_engine import (
                get_backtesting_engine,
            )

            # Initialize backtesting engine
            backtesting_engine = get_backtesting_engine()
            await backtesting_engine.initialize()

            # Run backtest on recent data (last 7 days) to update configurations
            end_date = (datetime.now() - timedelta(days=1)).strftime(
                "%Y-%m-%d"
            )  # Yesterday
            start_date = (datetime.now() - timedelta(days=7)).strftime(
                "%Y-%m-%d"
            )  # 7 days ago

            logger.info(
                f"Running configuration update backtest: {start_date} to {end_date}"
            )

            # Run backtest (this will automatically update strategy_configurations table)
            backtest_results = await backtesting_engine.run_backtest(
                start_date=start_date,
                end_date=end_date,
                include_diagnostics=False,
                include_alignment=False,
            )

            # Log results
            strategies_analyzed = backtest_results.get("backtest_results", {}).get(
                "total_strategies", 0
            )
            profitable_strategies = backtest_results.get("backtest_results", {}).get(
                "profitable_strategies", 0
            )

            logger.info(
                "Strategy configuration update completed",
                strategies_analyzed=strategies_analyzed,
                profitable_strategies=profitable_strategies,
                execution_time=backtest_results.get("execution_time_seconds", 0),
            )

            # Update metrics
            self.metrics["strategy_configs_updated"] = strategies_analyzed

        except Exception as e:
            logger.error(f"Failed to update strategy configurations: {e}")
            # Don't fail the entire pipeline for configuration updates
            self.metrics["errors"] += 1

    def generate_report(
        self,
        analysis_results: dict,
        output_file: str | None = None,
        flip_results: dict | None = None,
    ) -> str:
        """Generate a summary report of the pipeline results."""

        duration = (
            self.metrics["end_time"] - self.metrics["start_time"]
        ).total_seconds()

        report = f"""
MLB Sharp Betting Analysis Report
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Duration: {duration:.2f} seconds

=== PIPELINE METRICS ===
Records Scraped: {self.metrics["scraped_records"]}
Records Parsed: {self.metrics["parsed_records"]}
Valid Records: {self.metrics["valid_records"]}
Records Stored: {self.metrics["stored_records"]}
Games Processed: {self.metrics["games_processed"]}
Games Created: {self.metrics["games_created"]}
Games Updated: {self.metrics["games_updated"]}
Sharp Indicators Found: {self.metrics["sharp_indicators"]}
Cross-Market Flips Detected: {self.metrics["flip_detections"]}
High Confidence Flips: {self.metrics["high_confidence_flips"]}
Errors: {self.metrics["errors"]}

=== DATA ANALYSIS ===
Total Betting Splits: {analysis_results.get("total_splits", 0)}
Sharp Action Indicators: {analysis_results.get("sharp_count", 0)}

Split Types:
"""

        for split_type, count in analysis_results.get("splits_by_type", {}).items():
            report += f"  {split_type}: {count}\n"

        averages = analysis_results.get("averages", {})
        report += f"""
Average Percentages:
  Home/Over Bets: {averages.get("home_bets_pct", 0):.1f}%
  Home/Over Money: {averages.get("home_money_pct", 0):.1f}%
  Away/Under Bets: {averages.get("away_bets_pct", 0):.1f}%
  Away/Under Money: {averages.get("away_money_pct", 0):.1f}%

=== SHARP ACTION INDICATORS ===
"""

        for sharp in analysis_results.get("sharp_indicators", [])[:5]:  # Show top 5
            bet_diff = sharp[9]  # bet_money_diff
            report += (
                f"  {sharp[1]} vs {sharp[2]} ({sharp[3]}): {bet_diff:.1f}% difference\n"
            )

        # Add flip detection results if available
        if flip_results and not flip_results.get("error"):
            report += f"""
=== CROSS-MARKET FLIP DETECTION ===
Total Flips Found: {flip_results.get("flips_found", 0)}
High Confidence Flips (≥85%): {flip_results.get("high_confidence_flips", 0)}
Games Evaluated: {flip_results.get("summary", {}).get("games_evaluated", 0)}
"""

            # Show top flips
            top_flips = flip_results.get("flips", [])
            if top_flips:
                report += "\nTop Cross-Market Flips:\n"
                for flip in top_flips:
                    report += f"  {flip.away_team} @ {flip.home_team}: {flip.confidence_score:.1f}% confidence - {flip.strategy_recommendation}\n"
                    report += f"    Reasoning: {flip.reasoning[:100]}...\n"
        elif flip_results and flip_results.get("error"):
            report += f"""
=== CROSS-MARKET FLIP DETECTION ===
Error: {flip_results.get("error")}
"""
        else:
            report += """
=== CROSS-MARKET FLIP DETECTION ===
Flip detection disabled or not available
"""

        if output_file:
            Path(output_file).write_text(report)
            logger.info("Report saved to file", output_file=output_file)

        return report

    async def run(self, output_file: str | None = None) -> dict:
        """Run the complete data pipeline."""
        self.metrics["start_time"] = datetime.now()

        logger.info("Starting MLB Sharp Betting data pipeline")

        try:
            # 1. Setup database
            await self.setup_database()

            # 2. Collect data from all sources
            collected_splits = await self.collect_data()
            if not collected_splits:
                logger.error("No data collected, aborting pipeline")
                self.metrics["end_time"] = datetime.now()
                return self.metrics

            # 3. Process games from betting splits
            game_stats = self.process_games_from_splits(collected_splits)

            # 4. Validate and store data
            valid_splits = self.validate_and_store_data(collected_splits)
            if not valid_splits:
                logger.error("No valid data to analyze")
                self.metrics["end_time"] = datetime.now()
                return self.metrics

            # 5. Analyze data
            analysis_results = self.analyze_data()

            # 6. Run cross-market flip detection (automatic after data collection)
            flip_results = await self._run_flip_detection()

            # 7. Update strategy configurations based on recent performance
            await self._update_strategy_configurations()

            # 8. Generate report
            self.metrics["end_time"] = datetime.now()
            report = self.generate_report(analysis_results, output_file, flip_results)

            print("\n" + "=" * 60)
            print(report)
            print("=" * 60)

            logger.info(
                "Data pipeline completed successfully",
                duration=(
                    self.metrics["end_time"] - self.metrics["start_time"]
                ).total_seconds(),
            )

            return self.metrics

        except Exception as e:
            logger.error("Data pipeline failed", error=str(e))
            self.metrics["errors"] += 1
            self.metrics["end_time"] = datetime.now()
            return self.metrics


async def main():
    """Main entrypoint function."""
    parser = argparse.ArgumentParser(description="MLB Sharp Betting Data Pipeline")
    parser.add_argument("--sport", default="mlb", help="Sport to scrape (default: mlb)")
    parser.add_argument(
        "--sportsbook", default="circa", help="Sportsbook to use (default: circa)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Run without making web requests"
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("--output", help="Output file for results")

    args = parser.parse_args()

    # Configure logging level
    if args.verbose:
        import logging

        logging.getLogger().setLevel(logging.DEBUG)

    # Create and run pipeline
    pipeline = DataPipeline(
        sport=args.sport, sportsbook=args.sportsbook, dry_run=args.dry_run
    )

    try:
        metrics = await pipeline.run(output_file=args.output)

        # Exit with error code if there were significant issues
        if metrics["errors"] > 0 and metrics["stored_records"] == 0:
            sys.exit(1)

        sys.exit(0)

    finally:
        # CRITICAL: Explicitly close database connection to prevent locks
        # This ensures the database lock is released for subsequent workflows
        try:
            if hasattr(pipeline, "db_manager") and pipeline.db_manager:
                pipeline.db_manager.close()
                # Use print instead of logger since logger may be closed
                print("Database connection closed successfully")
        except Exception as e:
            print(f"Warning: Error closing database connection: {e}")
            # Don't fail the entire pipeline for cleanup errors
            pass


if __name__ == "__main__":
    asyncio.run(main())
