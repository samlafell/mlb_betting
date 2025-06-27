#!/usr/bin/env python3
"""
Test script to see what data processors are actually receiving
"""

import sys
import asyncio
from pathlib import Path
from datetime import datetime, timedelta

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from mlb_sharp_betting.db.connection import get_db_manager
from mlb_sharp_betting.services.betting_signal_repository import BettingSignalRepository
from mlb_sharp_betting.core.config import SignalProcessorConfig


async def test_processor_data():
    """Test what data the processors are actually receiving"""
    
    db_manager = get_db_manager()
    config = SignalProcessorConfig()
    repository = BettingSignalRepository(config)
    repository.coordinator = db_manager
    
    # Create time window (last 7 days)
    end_time = datetime.now()
    start_time = end_time - timedelta(days=7)
    
    print("🔍 TESTING PROCESSOR DATA RETRIEVAL")
    print("=" * 60)
    print(f"Time window: {start_time} to {end_time}")
    print()
    
    # Test get_public_betting_data
    print("🎯 Testing get_public_betting_data...")
    try:
        public_data = await repository.get_public_betting_data(start_time, end_time)
        print(f"✅ Retrieved {len(public_data)} records")
        
        if public_data:
            print("📊 Sample record:")
            sample = public_data[0]
            for key, value in sample.items():
                print(f"   {key}: {value}")
            print()
            
            # Analyze percentage distribution
            percentages = [r.get('home_or_over_bets_percentage') for r in public_data if r.get('home_or_over_bets_percentage')]
            if percentages:
                print(f"📈 Betting percentage range: {min(percentages):.1f}% to {max(percentages):.1f}%")
                avg_pct = sum(percentages) / len(percentages)
                print(f"📊 Average betting percentage: {avg_pct:.1f}%")
                
                # Count by ranges
                high_pct = sum(1 for p in percentages if p > 70)
                low_pct = sum(1 for p in percentages if p < 30)
                moderate = len(percentages) - high_pct - low_pct
                print(f"🔥 High public (>70%): {high_pct} records")
                print(f"❄️  Low public (<30%): {low_pct} records")
                print(f"⚖️  Moderate (30-70%): {moderate} records")
        else:
            print("❌ No data returned!")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print()
    
    # Test raw query without filters to see what's actually available
    print("🎯 Testing raw data query (no filters)...")
    try:
        with db_manager.get_cursor() as cursor:
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT game_id) as unique_games,
                    MIN(home_or_over_bets_percentage) as min_bet_pct,
                    MAX(home_or_over_bets_percentage) as max_bet_pct,
                    AVG(home_or_over_bets_percentage) as avg_bet_pct,
                    COUNT(CASE WHEN home_or_over_bets_percentage > 70 THEN 1 END) as high_public,
                    COUNT(CASE WHEN home_or_over_bets_percentage < 30 THEN 1 END) as low_public,
                    COUNT(CASE WHEN home_or_over_bets_percentage BETWEEN 30 AND 70 THEN 1 END) as moderate_public
                FROM splits.raw_mlb_betting_splits
                WHERE split_type IN ('moneyline', 'Moneyline')
                AND last_updated >= %s
                AND home_or_over_bets_percentage IS NOT NULL
            """, (start_time,))
            
            stats = cursor.fetchone()
            print(f"✅ Raw data analysis:")
            print(f"   📊 Total records: {stats['total_records']:,}")
            print(f"   🎮 Unique games: {stats['unique_games']:,}")
            print(f"   📈 Betting percentage range: {stats['min_bet_pct']:.1f}% to {stats['max_bet_pct']:.1f}%")
            print(f"   📊 Average betting percentage: {stats['avg_bet_pct']:.1f}%")
            print(f"   🔥 High public (>70%): {stats['high_public']:,} records")
            print(f"   ❄️  Low public (<30%): {stats['low_public']:,} records")
            print(f"   ⚖️  Moderate (30-70%): {stats['moderate_public']:,} records")
            
            # The key insight: how much data is being filtered out!
            total_available = stats['total_records']
            qualifying_for_fade = stats['high_public'] + stats['low_public']
            filtered_out = total_available - qualifying_for_fade
            
            print(f"\n🚨 FILTER IMPACT ANALYSIS:")
            print(f"   📋 Total available records: {total_available:,}")
            print(f"   ✅ Records that pass fade filter: {qualifying_for_fade:,}")
            print(f"   ❌ Records filtered out: {filtered_out:,} ({filtered_out/total_available*100:.1f}%)")
            
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(test_processor_data()) 