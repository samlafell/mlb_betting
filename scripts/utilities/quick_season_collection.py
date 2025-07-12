#!/usr/bin/env python3
"""
Quick Season Collection Script

A simpler script to test and run historical collection for specific date ranges.
Perfect for testing before running the full season collection.

Usage:
    python quick_season_collection.py [start_date] [end_date]

Examples:
    python quick_season_collection.py                    # Last 7 days
    python quick_season_collection.py 2025-07-01        # July 1st to today
    python quick_season_collection.py 2025-07-01 2025-07-07  # July 1-7
"""

import asyncio
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

from sportsbookreview.services.collection_orchestrator import CollectionOrchestrator


async def quick_collection(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    test_mode: bool = False
):
    """
    Quick collection for testing or small date ranges.
    
    Args:
        start_date: Start date (default: 7 days ago)
        end_date: End date (default: today)
        test_mode: If True, just test connectivity and structure
    """
    
    # Set defaults
    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=7)
    
    total_days = (end_date - start_date).days + 1
    
    print("⚾ QUICK MLB DATA COLLECTION")
    print("=" * 40)
    print(f"📅 Date Range: {start_date} to {end_date}")
    print(f"📊 Total Days: {total_days}")
    print(f"🧪 Test Mode: {test_mode}")
    print("=" * 40)
    
    def progress_callback(progress: float, message: str):
        """Simple progress callback."""
        print(f"  {progress:5.1f}% - {message}")
    
    try:
        # Create output directory
        output_dir = Path("./quick_collection_output")
        output_dir.mkdir(exist_ok=True)
        
        async with CollectionOrchestrator(
            output_dir=output_dir,
            checkpoint_interval=10,
            enable_checkpoints=True
        ) as orchestrator:
            
            print("✅ Collection orchestrator initialized")
            
            # Test connectivity
            print("\n🔍 Testing connectivity...")
            if not await orchestrator.scraper.test_connectivity():
                raise Exception("Failed connectivity test")
            print("✅ Connectivity test passed")
            
            if test_mode:
                print("\n🧪 TEST MODE - Running system test...")
                test_result = await orchestrator.test_system()
                print(f"✅ System test result: {test_result.get('test_status', 'UNKNOWN')}")
                return test_result
            
            # Run collection
            print(f"\n📡 Starting collection...")
            result = await orchestrator.collect_historical_data(
                start_date=start_date,
                end_date=end_date,
                progress_callback=progress_callback,
                resume_from_checkpoint=False  # Always start fresh for quick tests
            )
            
            print("\n✅ Collection completed!")
            
            # Show summary
            if isinstance(result, dict):
                print("\n📊 Results Summary:")
                for key, value in result.items():
                    if isinstance(value, (int, float, str)):
                        print(f"  {key}: {value}")
            
            return result
            
    except Exception as e:
        print(f"❌ Collection failed: {e}")
        raise


async def verify_data(start_date: date, end_date: date):
    """Quick verification of collected data."""
    print("\n🔍 VERIFYING DATA")
    print("=" * 30)
    
    try:
        from sportsbookreview.services.data_storage_service import DataStorageService
        
        storage = DataStorageService()
        await storage.initialize_connection()
        
        async with storage.pool.acquire() as conn:
            # Count games
            games_count = await conn.fetchval("""
                SELECT COUNT(*) FROM public.games 
                WHERE game_date >= $1 AND game_date <= $2
            """, start_date, end_date)
            
            # Count betting records
            moneyline_count = await conn.fetchval("""
                SELECT COUNT(*) FROM mlb_betting.moneyline m
                JOIN public.games g ON m.game_id = g.id
                WHERE g.game_date >= $1 AND g.game_date <= $2
            """, start_date, end_date)
            
            spreads_count = await conn.fetchval("""
                SELECT COUNT(*) FROM mlb_betting.spreads s
                JOIN public.games g ON s.game_id = g.id
                WHERE g.game_date >= $1 AND g.game_date <= $2
            """, start_date, end_date)
            
            totals_count = await conn.fetchval("""
                SELECT COUNT(*) FROM mlb_betting.totals t
                JOIN public.games g ON t.game_id = g.id
                WHERE g.game_date >= $1 AND g.game_date <= $2
            """, start_date, end_date)
        
        await storage.close_connection()
        
        print(f"📊 Games: {games_count:,}")
        print(f"💰 Moneyline: {moneyline_count:,}")
        print(f"📈 Spreads: {spreads_count:,}")
        print(f"📊 Totals: {totals_count:,}")
        print(f"📋 Total Records: {moneyline_count + spreads_count + totals_count:,}")
        
        return {
            "games": games_count,
            "moneyline": moneyline_count,
            "spreads": spreads_count,
            "totals": totals_count
        }
        
    except Exception as e:
        print(f"❌ Verification failed: {e}")
        return None


def parse_date(date_str: str) -> date:
    """Parse date string in YYYY-MM-DD format."""
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        print(f"❌ Invalid date format: {date_str}")
        print("   Expected format: YYYY-MM-DD (e.g., 2025-07-01)")
        sys.exit(1)


async def main():
    """Main entry point."""
    
    # Parse command line arguments
    args = sys.argv[1:]
    
    # Handle different argument patterns
    if len(args) == 0:
        # No arguments - default to last 7 days
        start_date = None
        end_date = None
    elif len(args) == 1:
        # One argument - start date, end date = today
        if args[0] == "--test":
            # Test mode
            result = await quick_collection(test_mode=True)
            return 0 if result.get('test_status') == 'PASSED' else 1
        else:
            start_date = parse_date(args[0])
            end_date = None
    elif len(args) == 2:
        # Two arguments - start and end dates
        start_date = parse_date(args[0])
        end_date = parse_date(args[1])
    else:
        print("❌ Too many arguments")
        print("Usage: python quick_season_collection.py [start_date] [end_date]")
        print("       python quick_season_collection.py --test")
        return 1
    
    try:
        # Run collection
        result = await quick_collection(start_date, end_date)
        
        # Verify results
        actual_start = start_date or (date.today() - timedelta(days=7))
        actual_end = end_date or date.today()
        
        verification = await verify_data(actual_start, actual_end)
        
        if verification and sum(verification.values()) > 0:
            print("\n🎉 Collection and verification successful!")
            return 0
        else:
            print("\n⚠️  Collection completed but no data found")
            return 1
            
    except KeyboardInterrupt:
        print("\n⚠️  Collection interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Collection failed: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 