#!/usr/bin/env python3
"""
Sharp Action Redundancy Fix Test Suite

🚀 This script tests the specific redundancy fixes we implemented:
1. Repository caching to reduce database calls
2. Early termination for empty datasets
3. Batch data retrieval optimization
4. Enhanced logging and debugging
"""

import asyncio
import time
import sys
from datetime import datetime, timedelta
from typing import Dict, List

# Add project root to path
sys.path.insert(0, './src')

async def test_redundancy_fixes():
    """Test our Sharp Action redundancy fixes"""
    print("🚀 SHARP ACTION REDUNDANCY FIX TEST SUITE")
    print("=" * 60)
    
    try:
        from mlb_sharp_betting.services.betting_signal_repository import BettingSignalRepository
        from mlb_sharp_betting.models.betting_analysis import SignalProcessorConfig
        from mlb_sharp_betting.services.adaptive_detector import AdaptiveBettingDetector
        
        # Initialize components
        config = SignalProcessorConfig()
        repository = BettingSignalRepository(config)
        detector = AdaptiveBettingDetector()
        
        test_results = {}
        
        print("✅ All components initialized successfully")
        
        # Test 1: Repository Caching Efficiency
        print("\n🧪 TEST 1: Repository Caching Efficiency")
        print("-" * 40)
        
        # Clear cache and reset stats
        repository.clear_cache()
        repository.reset_stats()
        
        now = datetime.now()
        start_time = now
        end_time = now + timedelta(minutes=240)
        
        # First call (should be cache miss)
        print("📋 First call (cache miss expected)...")
        start = time.time()
        data1 = await repository.get_sharp_signal_data(start_time, end_time)
        first_call_time = time.time() - start
        
        # Second call (should be cache hit)
        print("📋 Second call (cache hit expected)...")
        start = time.time()
        data2 = await repository.get_sharp_signal_data(start_time, end_time)
        second_call_time = time.time() - start
        
        # Validate caching
        stats = repository.get_repository_stats()
        cache_hit_rate = stats['cache_hit_rate_pct']
        
        print(f"   ✅ Cache hit rate: {cache_hit_rate}%")
        print(f"   ⏱️  First call: {first_call_time:.3f}s ({len(data1)} records)")
        print(f"   ⏱️  Second call: {second_call_time:.3f}s ({len(data2)} records)")
        
        # Calculate speedup
        speedup = ((first_call_time - second_call_time) / first_call_time * 100) if first_call_time > 0 else 0
        print(f"   🚀 Cache speedup: {speedup:.1f}%")
        
        test_results['caching'] = {
            'cache_hit_rate': cache_hit_rate,
            'speedup_percent': speedup,
            'data_consistency': len(data1) == len(data2),
            'status': 'PASS' if cache_hit_rate > 0 and len(data1) == len(data2) else 'FAIL'
        }
        
        # Test 2: Early Termination for Empty Datasets
        print("\n🧪 TEST 2: Early Termination for Empty Datasets")
        print("-" * 40)
        
        # Test with future date (should have no data)
        future_start = datetime.now() + timedelta(days=365)
        future_end = future_start + timedelta(minutes=240)
        
        print("📋 Testing empty dataset handling...")
        start_time = time.time()
        empty_data = await repository.get_sharp_signal_data(future_start, future_end)
        processing_time = time.time() - start_time
        
        print(f"   📊 Records found: {len(empty_data)}")
        print(f"   ⏱️  Processing time: {processing_time:.3f}s")
        
        early_termination_working = len(empty_data) == 0 and processing_time < 0.1
        
        test_results['early_termination'] = {
            'records_found': len(empty_data),
            'processing_time': processing_time,
            'fast_processing': processing_time < 0.1,
            'status': 'PASS' if early_termination_working else 'FAIL'
        }
        
        # Test 3: Batch Data Retrieval
        print("\n🧪 TEST 3: Batch Data Retrieval")
        print("-" * 40)
        
        print("📋 Testing batch optimization...")
        batch_start = time.time()
        batch_data = await repository.get_batch_signal_data(
            now, now + timedelta(minutes=240),
            {'sharp_action', 'opposing_markets', 'book_conflicts', 'steam_moves'}
        )
        batch_time = time.time() - batch_start
        
        total_records = sum(len(data) for data in batch_data.values())
        print(f"   ⏱️  Batch time: {batch_time:.3f}s")
        print(f"   📊 Total records: {total_records}")
        print(f"   📊 Signal types retrieved: {len(batch_data)}")
        
        # List what we got
        for signal_type, data in batch_data.items():
            print(f"      • {signal_type}: {len(data)} records")
        
        test_results['batch_retrieval'] = {
            'processing_time': batch_time,
            'total_records': total_records,
            'signal_types_count': len(batch_data),
            'status': 'PASS' if len(batch_data) > 1 else 'FAIL'
        }
        
        # Test 4: Database Call Efficiency
        print("\n🧪 TEST 4: Database Call Efficiency")
        print("-" * 40)
        
        # Reset stats for clean measurement
        repository.reset_stats()
        
        print("📋 Simulating multiple processor calls...")
        
        # Simulate what would happen with multiple processors requesting data
        tasks = [
            repository.get_sharp_signal_data(now, now + timedelta(minutes=240)),
            repository.get_sharp_signal_data(now, now + timedelta(minutes=240)),  # Duplicate
            repository.get_opposing_markets_data(now, now + timedelta(minutes=240)),
            repository.get_book_conflict_data(now, now + timedelta(minutes=240)),
            repository.get_sharp_signal_data(now, now + timedelta(minutes=240)),  # Another duplicate
        ]
        
        # Execute all tasks
        results = await asyncio.gather(*tasks)
        
        # Check efficiency
        final_stats = repository.get_repository_stats()
        
        print(f"   📊 Total method calls: {len(tasks)}")
        print(f"   📊 Database calls made: {final_stats['database_calls']}")
        print(f"   📊 Cache hits: {final_stats['cache_hits']}")
        print(f"   📊 Cache hit rate: {final_stats['cache_hit_rate_pct']}%")
        
        # Calculate call reduction
        call_reduction = ((len(tasks) - final_stats['database_calls']) / len(tasks) * 100) if len(tasks) > 0 else 0
        print(f"   🚀 Database call reduction: {call_reduction:.1f}%")
        
        test_results['database_efficiency'] = {
            'method_calls': len(tasks),
            'database_calls': final_stats['database_calls'],
            'call_reduction_percent': call_reduction,
            'efficiency_rating': final_stats['efficiency_rating'],
            'status': 'PASS' if call_reduction > 20 else 'FAIL'  # At least 20% reduction
        }
        
        # Test 5: Detector Performance
        print("\n🧪 TEST 5: Detector Performance")
        print("-" * 40)
        
        print("📋 Testing detector with optimized repository...")
        detector_start = time.time()
        signals = await detector.detect_opportunities(240)
        detector_time = time.time() - detector_start
        
        print(f"   ⏱️  Detection time: {detector_time:.3f}s")
        print(f"   📊 Signals found: {len(signals)}")
        
        # Group signals by strategy
        strategy_groups = {}
        for signal in signals:
            strategy = signal.strategy_name
            if strategy not in strategy_groups:
                strategy_groups[strategy] = []
            strategy_groups[strategy].append(signal)
        
        print(f"   📊 Strategy types: {len(strategy_groups)}")
        
        # Check for Sharp Action redundancy
        sharp_action_variants = [
            name for name in strategy_groups.keys() 
            if 'sharp' in name.lower() or 'action' in name.lower()
        ]
        
        print(f"   📊 Sharp Action variants: {len(sharp_action_variants)}")
        if sharp_action_variants:
            print(f"      • {sharp_action_variants}")
        
        test_results['detector_performance'] = {
            'detection_time': detector_time,
            'signals_found': len(signals),
            'strategy_types': len(strategy_groups),
            'sharp_action_variants': len(sharp_action_variants),
            'status': 'PASS' if len(sharp_action_variants) <= 5 else 'WARN'  # Should be consolidated
        }
        
        # Final Repository Performance Summary
        print("\n📊 FINAL REPOSITORY PERFORMANCE SUMMARY")
        print("-" * 40)
        
        final_stats = repository.get_repository_stats()
        print(f"   📊 Total database calls: {final_stats['database_calls']}")
        print(f"   📊 Total cache hits: {final_stats['cache_hits']}")
        print(f"   📊 Overall hit rate: {final_stats['cache_hit_rate_pct']}%")
        print(f"   📊 Batch optimizations: {final_stats['batch_optimizations']}")
        print(f"   📊 Efficiency rating: {final_stats['efficiency_rating']}")
        
        # Overall Assessment
        print("\n🎯 OVERALL ASSESSMENT")
        print("=" * 60)
        
        # Count passes and fails
        statuses = [result['status'] for result in test_results.values()]
        passes = statuses.count('PASS')
        fails = statuses.count('FAIL')
        warns = statuses.count('WARN')
        
        print(f"✅ Tests Passed: {passes}")
        print(f"⚠️  Tests Warning: {warns}")
        print(f"❌ Tests Failed: {fails}")
        
        # Detailed results
        for test_name, result in test_results.items():
            status = result['status']
            emoji = '✅' if status == 'PASS' else '⚠️' if status == 'WARN' else '❌'
            print(f"{emoji} {test_name.replace('_', ' ').title()}: {status}")
        
        # Performance improvements summary
        cache_hit_rate = test_results['caching']['cache_hit_rate']
        call_reduction = test_results['database_efficiency']['call_reduction_percent']
        
        print(f"\n🚀 PERFORMANCE IMPROVEMENTS ACHIEVED:")
        print(f"   • Repository cache hit rate: {cache_hit_rate}%")
        print(f"   • Database call reduction: {call_reduction:.1f}%")
        print(f"   • Early termination working: {'✅' if test_results['early_termination']['status'] == 'PASS' else '❌'}")
        print(f"   • Batch retrieval working: {'✅' if test_results['batch_retrieval']['status'] == 'PASS' else '❌'}")
        
        overall_success = fails == 0
        print(f"\n{'🎉 SUCCESS' if overall_success else '❌ NEEDS IMPROVEMENT'}: Sharp Action redundancy fixes")
        
        return overall_success, test_results
        
    except Exception as e:
        print(f"❌ Test suite failed: {e}")
        import traceback
        print(traceback.format_exc())
        return False, {}

async def main():
    """Run the test suite"""
    success, results = await test_redundancy_fixes()
    
    # Save results to file
    if results:
        import json
        report_file = f"redundancy_fixes_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\n📄 Detailed results saved to: {report_file}")
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    asyncio.run(main()) 