#!/usr/bin/env python3

"""
Test script to process staging data only (no scraping).
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def test_staging_only():
    """Test processing staging data only."""
    try:
        from sportsbookreview.services.collection_orchestrator import (
            CollectionOrchestrator,
        )
        from sportsbookreview.services.data_storage_service import DataStorageService

        orchestrator = CollectionOrchestrator()

        # Initialize storage service
        storage = DataStorageService()
        await storage.initialize_connection()
        orchestrator.storage = storage

        logger.info("Processing staging data...")
        await orchestrator.process_staging()
        logger.info("Staging processing complete!")

        # Check results

        async with storage.pool.acquire() as conn:
            # Check recent betting data
            recent_spreads = await conn.fetchval("""
                SELECT COUNT(*) FROM mlb_betting.spreads 
                WHERE odds_timestamp > NOW() - INTERVAL '10 minutes'
            """)

            recent_moneylines = await conn.fetchval("""
                SELECT COUNT(*) FROM mlb_betting.moneyline 
                WHERE odds_timestamp > NOW() - INTERVAL '10 minutes'
            """)

            recent_totals = await conn.fetchval("""
                SELECT COUNT(*) FROM mlb_betting.totals 
                WHERE odds_timestamp > NOW() - INTERVAL '10 minutes'
            """)

            logger.info("Recent betting data inserted:")
            logger.info(f"  Spreads: {recent_spreads}")
            logger.info(f"  Moneylines: {recent_moneylines}")
            logger.info(f"  Totals: {recent_totals}")

        await storage.close_connection()

    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)


if __name__ == "__main__":
    asyncio.run(test_staging_only())
