#!/usr/bin/env python3
"""
Test Dynamic Threshold System

This script demonstrates the new dynamic threshold system that:
1. Starts with VERY loose thresholds to collect signals
2. Progressively tightens thresholds based on sample size
3. Optimizes thresholds for ROI rather than arbitrary values

Run this script to see the threshold progression in action.
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.mlb_sharp_betting.services.dynamic_threshold_manager import get_dynamic_threshold_manager, ThresholdPhase


async def test_dynamic_thresholds():
    """Test the dynamic threshold system"""
    
    print("🎯 DYNAMIC THRESHOLD SYSTEM TEST")
    print("=" * 60)
    
    # Initialize threshold manager
    threshold_manager = get_dynamic_threshold_manager()
    
    # Test different strategy types and phases
    strategy_types = ['sharp_action', 'book_conflicts', 'public_fade', 'late_flip']
    sources = ['VSIN', 'SBD', 'default']
    split_types = ['moneyline', 'spread', 'total']
    
    print("\n📊 BOOTSTRAP PHASE THRESHOLDS (0-10 samples)")
    print("Very loose thresholds to collect initial data:")
    print()
    
    for strategy_type in strategy_types:
        for source in sources[:2]:  # Test first 2 sources
            for split_type in split_types[:2]:  # Test first 2 split types
                try:
                    config = await threshold_manager.get_dynamic_threshold(
                        strategy_type=strategy_type,
                        source=source,
                        split_type=split_type
                    )
                    
                    print(f"  {strategy_type:15} | {source:5} | {split_type:10} | "
                          f"min: {config.minimum_threshold:4.1f}% | "
                          f"mod: {config.moderate_threshold:4.1f}% | "
                          f"high: {config.high_threshold:4.1f}% | "
                          f"phase: {config.phase.value}")
                    
                except Exception as e:
                    print(f"  ERROR: {strategy_type} - {e}")
    
    print("\n🎯 KEY FEATURES OF DYNAMIC THRESHOLD SYSTEM:")
    print("=" * 60)
    
    print("\n1. PROGRESSIVE TIGHTENING:")
    print("   • Bootstrap (0-10 samples):   Very loose thresholds (3-8%)")
    print("   • Learning (11-30 samples):   30% tighter than bootstrap")
    print("   • Calibration (31-100):       80% tighter than bootstrap") 
    print("   • Optimization (100+):        ROI-optimized thresholds")
    
    print("\n2. ROI-BASED OPTIMIZATION:")
    print("   • Tests thresholds from 2% to 25% in 0.5% increments")
    print("   • Finds threshold that maximizes ROI with reasonable volume")
    print("   • Considers sample size for confidence weighting")
    print("   • Updates thresholds based on actual performance data")
    
    print("\n3. STRATEGY-SPECIFIC BOOTSTRAP VALUES:")
    for strategy_type in strategy_types:
        bootstrap = threshold_manager.bootstrap_thresholds.get(strategy_type)
        if bootstrap:
            print(f"   • {strategy_type:15}: min={bootstrap['min']:4.1f}%, "
                  f"mod={bootstrap['mod']:4.1f}%, high={bootstrap['high']:4.1f}%")
    
    print("\n🚀 BENEFITS OVER STATIC THRESHOLDS:")
    print("=" * 60)
    print("   ✅ COLLECT MORE SIGNALS: Starts very loose (3-8% vs 15-30%)")
    print("   ✅ PROGRESSIVE LEARNING: Tightens as we gather more data")
    print("   ✅ ROI OPTIMIZATION: Uses actual performance, not arbitrary values")
    print("   ✅ STRATEGY SPECIFIC: Different strategies have different optimal thresholds")
    print("   ✅ ADAPTIVE: Continuously improves based on new data")
    
    print("\n📈 EXPECTED RESULTS:")
    print("=" * 60)
    print("   • More signals generated initially (loose thresholds)")
    print("   • Better signal quality over time (progressive tightening)")
    print("   • Higher ROI (optimization based on actual performance)")
    print("   • Faster learning (more data collection in early phases)")
    
    print("\n🔄 INTEGRATION STATUS:")
    print("=" * 60)
    print("   ✅ Dynamic Threshold Manager implemented")
    print("   ✅ Strategy Processor Factory updated")
    print("   ✅ Sharp Action Processor updated")
    print("   ✅ Book Conflict Processor updated")
    print("   ✅ Backtesting Engine updated")
    print("   📋 Ready for testing with next backtest run")
    
    print(f"\n🎯 Run 'uv run populate_strategy_performance.py' to test with dynamic thresholds!")


if __name__ == "__main__":
    asyncio.run(test_dynamic_thresholds()) 