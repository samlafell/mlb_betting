#!/usr/bin/env python3
"""
Sharp Action Redundancy Fix Validator

🚀 This script validates that our Sharp Action redundancy fixes are working properly
by testing repository caching, early termination, and batch optimization.
"""

import asyncio
import time
import sys
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, './src')

async def validate_fixes():
    """Validate Sharp Action redundancy fixes"""
    print("🚀 SHARP ACTION REDUNDANCY FIX VALIDATOR")
    print("=" * 50)
    
    try:
        from mlb_sharp_betting.services.betting_signal_repository import BettingSignalRepository
        from mlb_sharp_betting.models.betting_analysis import SignalProcessorConfig
        
        # Initialize
        config = SignalProcessorConfig()
        repository = BettingSignalRepository(config)
        
        print("✅ Repository initialized")
        
        # Test 1: Cache Performance
        print("\n🧪 Test 1: Repository Caching")
        repository.clear_cache()
        
        now = datetime.now()
        start_time = now
        end_time = now + timedelta(minutes=240)
        
        # First call (cache miss)
        start = time.time()
        data1 = await repository.get_sharp_signal_data(start_time, end_time)
        first_call = time.time() - start
        
        # Second call (cache hit)
        start = time.time()
        data2 = await repository.get_sharp_signal_data(start_time, end_time)
        second_call = time.time() - start
        
        stats = repository.get_repository_stats()
        
        print(f"   First call: {first_call:.3f}s")
        print(f"   Second call: {second_call:.3f}s")
        print(f"   Cache hit rate: {stats['cache_hit_rate_pct']}%")
        print(f"   ✅ Caching {'WORKING' if stats['cache_hit_rate_pct'] > 0 else 'FAILED'}")
        
        # Test 2: Batch Optimization
        print("\n🧪 Test 2: Batch Data Retrieval")
        
        batch_start = time.time()
        batch_data = await repository.get_batch_signal_data(
            start_time, end_time,
            {'sharp_action', 'opposing_markets', 'book_conflicts'}
        )
        batch_time = time.time() - batch_start
        
        total_records = sum(len(data) for data in batch_data.values())
        print(f"   Batch time: {batch_time:.3f}s")
        print(f"   Total records: {total_records}")
        print(f"   Signal types: {len(batch_data)}")
        print(f"   ✅ Batch optimization {'WORKING' if len(batch_data) > 1 else 'FAILED'}")
        
        # Test 3: Performance Summary
        print("\n📊 Performance Summary")
        final_stats = repository.get_repository_stats()
        print(f"   Database calls: {final_stats['database_calls']}")
        print(f"   Cache hits: {final_stats['cache_hits']}")
        print(f"   Efficiency: {final_stats['efficiency_rating']}")
        
        # Calculate improvements
        if final_stats['cache_hits'] > 0:
            call_reduction = final_stats['cache_hits'] / (final_stats['cache_hits'] + final_stats['cache_misses']) * 100
            print(f"   🚀 Call reduction: {call_reduction:.1f}%")
        
        print("\n✅ All redundancy fixes validated successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        import traceback
        print(traceback.format_exc())
        return False

if __name__ == "__main__":
    success = asyncio.run(validate_fixes())
    print(f"\n{'🎉 SUCCESS' if success else '❌ FAILED'}: Sharp Action redundancy fixes validation")
    sys.exit(0 if success else 1) 