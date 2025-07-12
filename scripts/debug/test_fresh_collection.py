#!/usr/bin/env python3

"""
Test script to run fresh collection for today's date and verify betting odds collection.
"""

import asyncio
import logging
from datetime import date, datetime
from pathlib import Path
import sys

# Add project root to path for imports
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def test_fresh_collection():
    """Test fresh collection for today's date."""
    try:
        from sportsbookreview.services.collection_orchestrator import CollectionOrchestrator
        
        logger.info("Starting fresh collection test for July 8th, 2025...")
        
        # Initialize orchestrator
        orchestrator = CollectionOrchestrator()
        await orchestrator.initialize_services()
        
        try:
            # Test scraping today's data
            today = date(2025, 7, 8)
            logger.info(f"Collecting data for {today}")
            
            # Run collection for just today
            results = await orchestrator.collect_date_range(
                start_date=today,
                end_date=today
            )
            
            logger.info("Collection completed. Results:")
            logger.info(f"Scraping: {results.get('scraping_results', {})}")
            logger.info(f"Storage: {results.get('storage_results', {})}")
            logger.info(f"Errors: {results.get('error_summary', {})}")
            
            # Check what was actually scraped
            from sportsbookreview.services.data_storage_service import DataStorageService
            storage = DataStorageService()
            await storage.initialize_connection()
            
            async with storage.pool.acquire() as conn:
                # Check staging area for today's data
                staging_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM sbr_parsed_games 
                    WHERE game_data->>'source_url' LIKE '%2025-07-08%'
                """)
                
                # Check for records with actual odds data
                odds_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM sbr_parsed_games 
                    WHERE game_data->>'source_url' LIKE '%2025-07-08%'
                    AND jsonb_array_length(game_data->'odds_data') > 0
                """)
                
                # Check betting tables for recent data
                recent_moneyline = await conn.fetchval("""
                    SELECT COUNT(*) FROM mlb_betting.moneyline 
                    WHERE odds_timestamp >= '2025-07-08'::date
                """)
                
                recent_spreads = await conn.fetchval("""
                    SELECT COUNT(*) FROM mlb_betting.spreads 
                    WHERE odds_timestamp >= '2025-07-08'::date
                """)
                
                recent_totals = await conn.fetchval("""
                    SELECT COUNT(*) FROM mlb_betting.totals 
                    WHERE odds_timestamp >= '2025-07-08'::date
                """)
                
                logger.info(f"VERIFICATION RESULTS:")
                logger.info(f"Staging records for July 8th: {staging_count}")
                logger.info(f"Records with actual odds data: {odds_count}")
                logger.info(f"Recent moneyline records: {recent_moneyline}")
                logger.info(f"Recent spreads records: {recent_spreads}")
                logger.info(f"Recent totals records: {recent_totals}")
                
                # Sample a few records to see what was scraped
                sample_records = await conn.fetch("""
                    SELECT id, status, 
                           game_data->>'bet_type' as bet_type,
                           jsonb_array_length(game_data->'odds_data') as odds_count,
                           game_data->>'scraped_at' as scraped_at
                    FROM sbr_parsed_games 
                    WHERE game_data->>'source_url' LIKE '%2025-07-08%'
                    ORDER BY id DESC
                    LIMIT 5
                """)
                
                logger.info("Sample staging records:")
                for record in sample_records:
                    logger.info(f"  ID {record['id']}: {record['bet_type']} - {record['status']} - {record['odds_count']} odds - scraped: {record['scraped_at']}")
            
            await storage.close_connection()
            
        finally:
            await orchestrator.cleanup_services()
            
        return results
        
    except Exception as e:
        logger.error(f"Collection test failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(test_fresh_collection()) 