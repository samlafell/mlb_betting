#!/usr/bin/env python3
"""
Process July 7th staging data into final betting tables.
"""

import asyncio
import sys
from pathlib import Path

# Add the sportsbookreview module to the path
sys.path.append(str(Path(__file__).parent / "sportsbookreview"))

from sportsbookreview.services.collection_orchestrator import CollectionOrchestrator


async def process_july_7th_staging():
    """Process July 7th staging data into final tables."""
    print("=== PROCESSING JULY 7TH STAGING DATA ===")
    
    try:
        # Initialize collection orchestrator
        async with CollectionOrchestrator() as orchestrator:
            print("✅ Collection orchestrator initialized")
            
            # Process staging data
            print("Processing staging data...")
            await orchestrator.process_staging()
            
            print("✅ Staging processing completed")
            
            # Get stats
            storage_stats = orchestrator.storage.get_storage_stats()
            print(f"📊 Storage stats: {storage_stats}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


async def check_before_and_after():
    """Check data before and after processing."""
    print("\n=== CHECKING DATA BEFORE AND AFTER ===")
    
    from src.mlb_sharp_betting.db.connection import get_db_manager
    db = get_db_manager()
    
    try:
        # Check staging data
        print("Staging data:")
        staging_data = db.execute_query(
            "SELECT COUNT(*) as count FROM public.sbr_parsed_games WHERE status = 'new'"
        )
        print(f"  Unprocessed staging records: {staging_data[0]['count']}")
        
        # Check final tables before processing
        print("\nFinal tables BEFORE processing:")
        moneyline_before = db.execute_query(
            'SELECT COUNT(*) as count FROM mlb_betting.moneyline WHERE DATE(created_at) = %s', 
            ('2025-07-07',)
        )
        print(f"  July 7th moneyline records: {moneyline_before[0]['count']}")
        
        # Process staging data
        await process_july_7th_staging()
        
        # Check final tables after processing
        print("\nFinal tables AFTER processing:")
        moneyline_after = db.execute_query(
            'SELECT COUNT(*) as count FROM mlb_betting.moneyline WHERE DATE(created_at) = %s', 
            ('2025-07-07',)
        )
        print(f"  July 7th moneyline records: {moneyline_after[0]['count']}")
        
        spreads_after = db.execute_query(
            'SELECT COUNT(*) as count FROM mlb_betting.spreads WHERE DATE(created_at) = %s', 
            ('2025-07-07',)
        )
        print(f"  July 7th spreads records: {spreads_after[0]['count']}")
        
        totals_after = db.execute_query(
            'SELECT COUNT(*) as count FROM mlb_betting.totals WHERE DATE(created_at) = %s', 
            ('2025-07-07',)
        )
        print(f"  July 7th totals records: {totals_after[0]['count']}")
        
        # Show sample data if available
        if moneyline_after[0]['count'] > 0:
            print("\nSample July 7th moneyline data:")
            sample = db.execute_query(
                'SELECT game_id, sportsbook, home_ml, away_ml FROM mlb_betting.moneyline WHERE DATE(created_at) = %s LIMIT 5', 
                ('2025-07-07',)
            )
            for row in sample:
                print(f"  Game {row['game_id']}: {row['home_ml']}/{row['away_ml']} ({row['sportsbook']})")
        
    except Exception as e:
        print(f"Error checking data: {e}")


async def main():
    """Main function."""
    print("Process July 7th Staging Data")
    print("=" * 50)
    
    await check_before_and_after()
    
    print("\n✅ Processing completed!")


if __name__ == "__main__":
    asyncio.run(main()) 