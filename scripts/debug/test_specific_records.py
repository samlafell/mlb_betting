#!/usr/bin/env python3

"""
Test script to process specific records with working odds data.
"""

import asyncio
import json
import logging
import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def test_specific_records():
    """Test processing specific records with working odds data."""
    try:
        from sportsbookreview.services.data_storage_service import DataStorageService
        from sportsbookreview.services.integration_service import IntegrationService

        storage = DataStorageService()
        await storage.initialize_connection()

        integrator = IntegrationService(storage)

        # Get the specific records with working odds data (371-378)
        async with storage.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, game_data 
                FROM sbr_parsed_games 
                WHERE id BETWEEN 371 AND 378 
                AND jsonb_array_length(game_data->'odds_data') > 0
                ORDER BY id
            """)

            logger.info(f"Found {len(rows)} records with odds data")

            for row in rows:
                row_id = row["id"]
                game_dict = row["game_data"]

                # Parse JSON if it's a string
                if isinstance(game_dict, str):
                    game_dict = json.loads(game_dict)

                logger.info(f"\n{'=' * 50}")
                logger.info(f"Processing record ID: {row_id}")
                logger.info(f"SBR Game ID: {game_dict.get('sbr_game_id')}")
                logger.info(f"Bet Type: '{game_dict.get('bet_type')}'")
                logger.info(f"Odds count: {len(game_dict.get('odds_data', []))}")

                # Show first odds record
                odds_data = game_dict.get("odds_data", [])
                if odds_data:
                    first_odds = odds_data[0]
                    logger.info(f"First odds: {first_odds}")

                # Build betting_data records like the orchestrator does
                odds_records = []
                for odds in game_dict.get("odds_data", []):
                    record = {
                        "bet_type": game_dict.get("bet_type"),
                        "sportsbook": odds.get("sportsbook"),
                        "timestamp": game_dict.get("scraped_at"),
                    }

                    # Map keys based on bet type
                    bet_type = record["bet_type"]
                    logger.debug(
                        f"Processing bet_type: '{bet_type}' for sportsbook: {odds.get('sportsbook')}"
                    )

                    if bet_type == "moneyline":
                        record["home_ml"] = odds.get("moneyline_home") or odds.get(
                            "home_ml"
                        )
                        record["away_ml"] = odds.get("moneyline_away") or odds.get(
                            "away_ml"
                        )
                    elif bet_type == "spread":
                        record["home_spread"] = odds.get("spread_home") or odds.get(
                            "home_spread"
                        )
                        record["away_spread"] = odds.get("spread_away") or odds.get(
                            "away_spread"
                        )
                        record["home_spread_price"] = odds.get("home_spread_price")
                        record["away_spread_price"] = odds.get("away_spread_price")
                    elif bet_type in ("total", "totals"):
                        record["total_line"] = odds.get("total_line")
                        record["over_price"] = odds.get("total_over") or odds.get(
                            "over_price"
                        )
                        record["under_price"] = odds.get("total_under") or odds.get(
                            "under_price"
                        )

                    odds_records.append(record)
                    logger.debug(f"Built record: {record}")

                logger.info(f"Built {len(odds_records)} betting records")

                # Test the storage directly
                if odds_records:
                    logger.info("Testing direct storage of betting data...")

                    # Create a dummy game_id for testing
                    game_id = 999999  # Use a test ID

                    try:
                        await storage.store_betting_data(game_id, odds_records)
                        logger.info(
                            f"✅ Successfully stored betting data for game_id {game_id}"
                        )
                    except Exception as e:
                        logger.error(f"❌ Failed to store betting data: {e}")

                # Only test first record to avoid spam
                break

        await storage.close_connection()

    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(test_specific_records())
