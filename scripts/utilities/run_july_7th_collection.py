#!/usr/bin/env python3
"""
Run SportsbookReview collection for July 7th to collect and store the data.
"""

import asyncio
import sys
from datetime import date
from pathlib import Path

# Add the sportsbookreview module to the path
sys.path.append(str(Path(__file__).parent / "sportsbookreview"))

from sportsbookreview.services.collection_orchestrator import CollectionOrchestrator


async def run_july_7th_collection():
    """Run collection for July 7th."""
    print("=== RUNNING JULY 7TH COLLECTION ===")
    
    target_date = date(2025, 7, 7)
    
    try:
        # Initialize orchestrator
        async with CollectionOrchestrator() as orchestrator:
            print(f"Starting collection for {target_date}")
            
            # Run collection for the specific date
            result = await orchestrator.collect_date_range(
                start_date=target_date,
                end_date=target_date
            )
            
            if result and result.get('success'):
                print(f"✅ Collection completed successfully!")
                print(f"Games processed: {result.get('games_processed', 0)}")
                print(f"Records stored: {result.get('records_stored', 0)}")
                
                # Show details
                if 'summary' in result:
                    summary = result['summary']
                    print(f"\nSummary:")
                    print(f"  Moneyline records: {summary.get('moneyline_records', 0)}")
                    print(f"  Spreads records: {summary.get('spreads_records', 0)}")
                    print(f"  Totals records: {summary.get('totals_records', 0)}")
            else:
                print(f"❌ Collection failed")
                if result and 'error' in result:
                    print(f"Error: {result['error']}")
                    
    except Exception as e:
        print(f"❌ Error running collection: {e}")
        import traceback
        traceback.print_exc()


async def check_results():
    """Check if July 7th data was collected."""
    print("\n=== CHECKING COLLECTION RESULTS ===")
    
    from src.mlb_sharp_betting.db.connection import get_db_manager
    db = get_db_manager()
    
    try:
        # Check July 7th data
        result = db.execute_query('SELECT COUNT(*) as count FROM mlb_betting.moneyline WHERE DATE(created_at) = %s', ('2025-07-07',))
        print(f"July 7th moneyline records: {result[0]['count']}")
        
        if result[0]['count'] > 0:
            # Show sample data
            sample = db.execute_query('SELECT game_id, sportsbook, home_ml, away_ml FROM mlb_betting.moneyline WHERE DATE(created_at) = %s LIMIT 5', ('2025-07-07',))
            print("Sample July 7th records:")
            for row in sample:
                print(f"  Game {row['game_id']}: {row['home_ml']}/{row['away_ml']} ({row['sportsbook']})")
        
    except Exception as e:
        print(f"Error checking results: {e}")


async def main():
    """Main function."""
    print("SportsbookReview July 7th Collection")
    print("=" * 50)
    
    # Run collection
    await run_july_7th_collection()
    
    # Check results
    await check_results()
    
    print("\n✅ Collection completed!")


if __name__ == "__main__":
    asyncio.run(main()) 