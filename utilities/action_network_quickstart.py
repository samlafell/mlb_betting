#!/usr/bin/env python3
"""
Action Network Quick Start Script

This script provides a simple way to collect and analyze Action Network data
without dealing with complex configuration systems.

Usage:
    uv run python action_network_quickstart.py
"""

import asyncio
import json
import subprocess
from datetime import datetime
from pathlib import Path

import structlog

# Import the game outcome service
try:
    from src.services.game_outcome_service import check_game_outcomes
except ImportError:
    # Fallback if running from different directory
    check_game_outcomes = None

# Configure logging
structlog.configure(
    processors=[structlog.stdlib.filter_by_level, structlog.dev.ConsoleRenderer()],
    wrapper_class=structlog.stdlib.BoundLogger,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


class ActionNetworkQuickStart:
    """Simple Action Network data collection and analysis."""

    def __init__(self, output_dir: str = "output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    async def run_complete_pipeline(
        self, date: str = "today", max_games: int | None = None
    ):
        """Run the complete Action Network pipeline."""
        logger.info("🚀 Starting Action Network Quick Start Pipeline", date=date)

        try:
            # Step 1: Extract Game URLs
            logger.info("📡 Step 1: Extracting game URLs")
            urls_file = self._extract_game_urls(date)
            if not urls_file:
                logger.error("❌ Failed to extract game URLs")
                return False

            # Step 2: Check for completed games and update outcomes
            logger.info("🏁 Step 2: Checking for completed games and updating outcomes")
            await self._check_game_outcomes()

            # Step 3: Display URLs and opportunities
            logger.info("📊 Step 3: Displaying today's games and opportunities")
            self._display_games_and_opportunities(urls_file)

            logger.info("✅ Pipeline completed successfully!")
            return True

        except Exception as e:
            logger.error("💥 Pipeline failed", error=str(e))
            return False

    def _extract_game_urls(self, date: str) -> Path | None:
        """Extract game URLs using the existing Action Network extractor."""
        try:
            cmd = [
                "uv",
                "run",
                "python",
                "-m",
                "action.extract_todays_game_urls",
                "--date",
                date,
                "--no-test",  # Skip URL testing for speed
            ]

            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode != 0:
                logger.error("URL extraction failed", stderr=result.stderr)
                return None

            # Find the generated file
            import glob

            pattern = f"output/action_network_game_urls_{date}_*.json"
            files = glob.glob(pattern)

            if not files:
                logger.error("No URLs file generated")
                return None

            latest_file = max(files, key=lambda x: Path(x).stat().st_mtime)
            urls_file = Path(latest_file)

            # Load and log summary
            with open(urls_file) as f:
                data = json.load(f)

            total_games = len(data.get("games", []))
            logger.info(
                "✅ Game URLs extracted", total_games=total_games, file=urls_file.name
            )

            return urls_file

        except Exception as e:
            logger.error("Error extracting game URLs", error=str(e))
            return None

    async def _check_game_outcomes(self):
        """Check for completed games and update outcomes using the game outcome service."""
        try:
            if check_game_outcomes is None:
                logger.warning(
                    "⚠️ Game outcome service not available - skipping outcome check"
                )
                return

            logger.info("🔍 Checking for completed games in the last 7 days...")

            # Check outcomes for the last 7 days to catch any recently completed games
            from datetime import datetime, timedelta

            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            date_range = (
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d"),
            )

            results = await check_game_outcomes(
                date_range=date_range, force_update=False
            )

            # Log results
            if results["updated_outcomes"] > 0:
                logger.info(
                    "✅ Updated game outcomes",
                    updated=results["updated_outcomes"],
                    completed=results["completed_games"],
                    processed=results["processed_games"],
                )

                print("\n🏁 GAME OUTCOMES UPDATE:")
                print(f"   📊 Processed {results['processed_games']} games")
                print(f"   ✅ Updated {results['updated_outcomes']} completed games")
                print(f"   ⏳ Skipped {results['skipped_games']} games (not completed)")

                if results["errors"]:
                    print(f"   ⚠️  {len(results['errors'])} errors occurred")
                    for error in results["errors"][:3]:  # Show first 3 errors
                        print(f"      - {error}")

            else:
                logger.info("ℹ️ No new completed games found")
                print("\n🏁 GAME OUTCOMES: No new completed games found")

        except Exception as e:
            logger.error("Error checking game outcomes", error=str(e))
            print(f"\n❌ Error checking game outcomes: {str(e)}")

    def _display_games_and_opportunities(self, urls_file: Path):
        """Display today's games and potential opportunities."""
        try:
            # Load game URLs
            with open(urls_file) as f:
                data = json.load(f)

            games = data.get("games", [])

            print("\n" + "=" * 80)
            print("🎯 TODAY'S MLB GAMES - ACTION NETWORK OPPORTUNITIES")
            print("=" * 80)
            print(f"📊 Total Games: {len(games)}")
            print(f"📅 Date: {data.get('target_date', 'Unknown')}")
            print()

            for i, game in enumerate(games, 1):
                away_team = game.get("away_team", "Unknown")
                home_team = game.get("home_team", "Unknown")
                start_time = game.get("start_time", "Unknown")
                status = game.get("status", "Unknown")

                print(f"🏟️  Game {i}: {away_team} @ {home_team}")
                print(f"   🕐 Start Time: {start_time}")
                print(f"   📊 Status: {status}")
                print(f"   🔗 History URL: {game.get('history_url', 'N/A')}")
                print()

            print("=" * 80)
            print("🎯 NEXT STEPS:")
            print(
                "1. Run the complete Action Network pipeline to collect historical data:"
            )
            print("   uv run python -m src.interfaces.cli action-network pipeline")
            print()
            print(
                "2. Or use the movement analysis commands (after collecting historical data):"
            )
            print(
                "   uv run python -m src.interfaces.cli movement analyze -i output/historical_line_movement_*.json"
            )
            print()
            print("3. Check for RLM opportunities:")
            print(
                "   uv run python -m src.interfaces.cli movement rlm -i output/historical_line_movement_*.json"
            )
            print()
            print("4. Detect steam moves:")
            print(
                "   uv run python -m src.interfaces.cli movement steam -i output/historical_line_movement_*.json"
            )
            print()
            print("5. View latest betting opportunities:")
            print("   uv run python -m src.interfaces.cli action-network opportunities")
            print()
            print("6. Check game outcomes:")
            print("   uv run python -m src.interfaces.cli outcomes check --days 7")
            print("=" * 80)

        except Exception as e:
            logger.error("Error displaying games", error=str(e))


async def main():
    """Main entry point."""
    print("🚀 Action Network Quick Start")
    print("=" * 50)

    # Create quick start instance
    quickstart = ActionNetworkQuickStart()

    # Run the pipeline
    success = await quickstart.run_complete_pipeline(date="today")

    if success:
        print("\n✅ Quick start completed successfully!")
        print("📁 Check the 'output' directory for results")
        print("\n🎯 To run the complete pipeline with historical data collection:")
        print("   uv run python -m src.interfaces.cli action-network pipeline")
    else:
        print("\n❌ Quick start failed")
        print("📋 Check the logs above for details")


if __name__ == "__main__":
    asyncio.run(main())
