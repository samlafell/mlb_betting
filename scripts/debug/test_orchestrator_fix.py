#!/usr/bin/env python3
"""
Test script to verify CollectionOrchestrator multi-schema fixes.
"""

import asyncio
import logging
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

async def test_orchestrator_processing():
    """Test the CollectionOrchestrator with fixed multi-schema setup."""
    try:
        from sportsbookreview.services.data_storage_service import DataStorageService
        from sportsbookreview.services.collection_orchestrator import CollectionOrchestrator
        
        # Initialize services
        storage = DataStorageService()
        await storage.initialize_connection()
        
        orchestrator = CollectionOrchestrator()
        orchestrator.storage = storage
        
        # Check staging status before processing
        async with storage.pool.acquire() as conn:
            before_stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(*) FILTER (WHERE status = 'new') as new_rows,
                    COUNT(*) FILTER (WHERE status = 'loaded') as loaded_rows,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed_rows,
                    COUNT(*) FILTER (WHERE status = 'duplicate') as duplicate_rows
                FROM sbr_parsed_games
            """)
            
            logger.info(f"BEFORE Processing - Total: {before_stats['total_rows']}, New: {before_stats['new_rows']}, Loaded: {before_stats['loaded_rows']}, Failed: {before_stats['failed_rows']}, Duplicates: {before_stats['duplicate_rows']}")
        
        # Process staging with small batch for testing
        logger.info("Starting orchestrator processing...")
        await orchestrator.process_staging(batch_size=10)
        
        # Check staging status after processing
        async with storage.pool.acquire() as conn:
            after_stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(*) FILTER (WHERE status = 'new') as new_rows,
                    COUNT(*) FILTER (WHERE status = 'loaded') as loaded_rows,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed_rows,
                    COUNT(*) FILTER (WHERE status = 'duplicate') as duplicate_rows
                FROM sbr_parsed_games
            """)
            
            logger.info(f"AFTER Processing - Total: {after_stats['total_rows']}, New: {after_stats['new_rows']}, Loaded: {after_stats['loaded_rows']}, Failed: {after_stats['failed_rows']}, Duplicates: {after_stats['duplicate_rows']}")
            
            # Check betting data in mlb_betting schema
            moneyline_count = await conn.fetchval("SELECT COUNT(*) FROM mlb_betting.moneyline")
            spread_count = await conn.fetchval("SELECT COUNT(*) FROM mlb_betting.spreads")
            totals_count = await conn.fetchval("SELECT COUNT(*) FROM mlb_betting.totals")
            games_count = await conn.fetchval("SELECT COUNT(*) FROM public.games")
            
            logger.info(f"SCHEMA DATA - Games: {games_count}, Moneyline: {moneyline_count}, Spread: {spread_count}, Totals: {totals_count}")
            
            # Check for recent insertions
            recent_games = await conn.fetchval("""
                SELECT COUNT(*) FROM public.games 
                WHERE created_at > NOW() - INTERVAL '5 minutes'
            """)
            
            recent_moneyline = await conn.fetchval("""
                SELECT COUNT(*) FROM mlb_betting.moneyline 
                WHERE created_at > NOW() - INTERVAL '5 minutes'
            """)
            
            recent_spread = await conn.fetchval("""
                SELECT COUNT(*) FROM mlb_betting.spreads 
                WHERE created_at > NOW() - INTERVAL '5 minutes'
            """)
            
            recent_totals = await conn.fetchval("""
                SELECT COUNT(*) FROM mlb_betting.totals 
                WHERE created_at > NOW() - INTERVAL '5 minutes'
            """)
            
            logger.info(f"RECENT INSERTIONS (last 5 min) - Games: {recent_games}, Moneyline: {recent_moneyline}, Spread: {recent_spread}, Totals: {recent_totals}")
        
        await storage.close_connection()
        
        # Calculate progress
        processed = before_stats['new_rows'] - after_stats['new_rows']
        logger.info(f"✅ PROCESSING COMPLETE - Processed {processed} records")
        
        if recent_games > 0 or recent_moneyline > 0 or recent_spread > 0 or recent_totals > 0:
            logger.info("✅ SUCCESS: Data is being inserted into multi-schema tables!")
        else:
            logger.warning("⚠️  No recent insertions detected - may need further investigation")
            
    except Exception as e:
        logger.error(f"❌ Test failed: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(test_orchestrator_processing()) 