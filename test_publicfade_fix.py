#!/usr/bin/env python3
"""
Test script to verify PublicFadeProcessor gets more data after repository fix
"""

import sys
import asyncio
from pathlib import Path
from datetime import datetime, timedelta

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from mlb_sharp_betting.db.connection import get_db_manager
from mlb_sharp_betting.services.betting_signal_repository import BettingSignalRepository


async def test_publicfade_data():
    """Test PublicFadeProcessor data retrieval after fix"""
    
    db_manager = get_db_manager()
    
    # Create mock config
    class MockConfig:
        def __init__(self):
            self.minimum_differential = 2.0
    
    config = MockConfig()
    repository = BettingSignalRepository(config)
    repository.coordinator = db_manager
    
    # Create time window (next 24 hours for live test)
    start_time = datetime.now()
    end_time = start_time + timedelta(hours=24)
    
    print("🔍 TESTING PUBLIC FADE PROCESSOR DATA (AFTER FIX)")
    print("=" * 60)
    print(f"Time window: {start_time} to {end_time}")
    print()
    
    try:
        # Test the fixed get_public_betting_data
        print("🎯 Testing get_public_betting_data (FIXED)...")
        public_data = await repository.get_public_betting_data(start_time, end_time)
        
        print(f"✅ Retrieved {len(public_data)} records (should be more than before!)")
        
        if public_data:
            print("📊 Sample record structure:")
            sample = public_data[0]
            for key, value in sample.items():
                print(f"   {key}: {value}")
            print()
            
            # Analyze percentage distribution
            percentages = [r.get('home_or_over_bets_percentage') for r in public_data if r.get('home_or_over_bets_percentage')]
            if percentages:
                print(f"📈 Betting percentage distribution:")
                print(f"   Range: {min(percentages):.1f}% to {max(percentages):.1f}%")
                print(f"   Average: {sum(percentages)/len(percentages):.1f}%")
                
                # Count by fade categories
                high_fade = sum(1 for p in percentages if p > 75 or p < 25)
                moderate_fade = sum(1 for p in percentages if (p > 70 or p < 30) and not (p > 75 or p < 25))
                low_fade = len(percentages) - high_fade - moderate_fade
                
                print(f"🔥 High fade opportunities (>75% or <25%): {high_fade}")
                print(f"🔶 Moderate fade opportunities (70-75% or 25-30%): {moderate_fade}")
                print(f"⚖️  Low fade opportunities (30-70%): {low_fade}")
                
                total_fade_opportunities = high_fade + moderate_fade
                print(f"\n💡 FADE ANALYSIS:")
                print(f"   🎯 Total fade opportunities: {total_fade_opportunities}")
                print(f"   📊 Percentage of records with fade potential: {total_fade_opportunities/len(percentages)*100:.1f}%")
                
                if total_fade_opportunities > 0:
                    print(f"   ✅ PUBLIC FADE PROCESSOR SHOULD NOW GENERATE SIGNALS!")
                else:
                    print(f"   ❌ Still no fade opportunities - check processor logic")
        else:
            print("❌ Still no data returned - repository query might need more fixes")
            
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(test_publicfade_data()) 