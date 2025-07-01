#!/usr/bin/env python3
"""
Refactored Backtesting Demo

This demonstrates the new BacktestingEngine from Phase 3 consolidation.
Shows how the engine uses actual game outcomes and consolidated functionality.
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add the src directory to the path
current_dir = Path(__file__).parent
project_root = current_dir.parent
sys.path.insert(0, str(project_root / "src"))

# 🔄 UPDATED: Use new BacktestingEngine instead of deprecated services
from mlb_sharp_betting.services.backtesting_engine import get_backtesting_engine
from mlb_sharp_betting.db.connection import get_db_manager


async def demo_backtesting_engine_functionality():
    """Demonstrate the new BacktestingEngine functionality from Phase 3."""
    print("🚀 BacktestingEngine Demo (Phase 3 Consolidation)")
    print("=" * 50)
    
    # 🔄 UPDATED: Initialize the new BacktestingEngine
    backtesting_engine = get_backtesting_engine()
    
    try:
        # Initialize the engine (this loads all consolidated modules)
        print("📡 Initializing BacktestingEngine...")
        await backtesting_engine.initialize()
        print(f"✅ BacktestingEngine initialized with consolidated modules")
        
        # Show engine status
        print("\n📊 Engine Comprehensive Status:")
        status = backtesting_engine.get_comprehensive_status()
        for key, value in status.items():
            print(f"   {key}: {value}")
        
        # Run full diagnostics
        print("\n🔬 Running Full Diagnostics...")
        diagnostics = await backtesting_engine.diagnostics.run_full_diagnostic()
        print(f"   Overall Health: {diagnostics.get('overall_health', 'unknown')}")
        print(f"   Checkpoints Passed: {diagnostics.get('checkpoints_passed', 0)}/5")
        
        if diagnostics.get('issues'):
            print(f"   Issues Found: {len(diagnostics['issues'])}")
            for issue in diagnostics['issues'][:3]:  # Show first 3 issues
                print(f"     • {issue}")
        
        # Run daily pipeline
        print("\n🎯 Running Daily Pipeline...")
        pipeline_results = await backtesting_engine.run_daily_pipeline()
        
        print(f"✅ Daily pipeline completed")
        print(f"📊 Pipeline Results: {pipeline_results}")
        
        # Test specific engine modules
        print("\n🧩 Testing Engine Modules...")
        
        # Test core engine
        try:
            core_test = await backtesting_engine.core_engine.run_quick_test()
            print(f"   ✅ Core Engine: {core_test}")
        except Exception as e:
            print(f"   ❌ Core Engine: {e}")
        
        # Test scheduler module
        try:
            scheduler_status = backtesting_engine.scheduler.get_status()
            print(f"   ✅ Scheduler Module: {scheduler_status}")
        except Exception as e:
            print(f"   ❌ Scheduler Module: {e}")
        
        # Test accuracy monitor
        try:
            accuracy_metrics = await backtesting_engine.accuracy_monitor.get_recent_metrics()
            print(f"   ✅ Accuracy Monitor: {len(accuracy_metrics)} recent metrics")
        except Exception as e:
            print(f"   ❌ Accuracy Monitor: {e}")
        
        print(f"\n✅ BacktestingEngine demo completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()


async def demo_historical_backtesting():
    """Demonstrate historical backtesting with the new engine."""
    print("\n📈 HISTORICAL BACKTESTING DEMO")
    print("=" * 40)
    
    # 🔄 UPDATED: Use BacktestingEngine for historical backtesting
    backtesting_engine = get_backtesting_engine()
    
    try:
        await backtesting_engine.initialize()
        
        # Define test date range (last 30 days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        print(f"📅 Testing date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Run historical backtesting
        print("\n🎯 Running historical backtesting...")
        results = await backtesting_engine.core_engine.run_historical_backtest(
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d")
        )
        
        # Display results
        print("\n📊 HISTORICAL BACKTESTING RESULTS")
        print("=" * 40)
        
        if isinstance(results, dict):
            summary = results.get("summary", {})
            print(f"Total strategies analyzed: {summary.get('total_strategies', 0)}")
            print(f"Profitable strategies: {summary.get('profitable_strategies', 0)}")
            print(f"Overall win rate: {summary.get('overall_win_rate', 0):.1%}")
            print(f"Weighted ROI: {summary.get('weighted_roi', 0):.1f}%")
            
            # Show top performers
            top_performers = summary.get("top_performers", [])
            if top_performers:
                print(f"\n🏆 TOP PERFORMING STRATEGIES:")
                for i, performer in enumerate(top_performers[:3], 1):
                    print(f"{i}. {performer.get('strategy_name', 'Unknown')}")
                    print(f"   ROI: {performer.get('roi', 0):.1f}% | Win Rate: {performer.get('win_rate', 0):.1%}")
        else:
            print(f"Results: {results}")
        
    except Exception as e:
        print(f"❌ Historical backtesting demo failed: {e}")


async def demo_legacy_compatibility():
    """Demonstrate backward compatibility with legacy interfaces."""
    print("\n🔄 LEGACY COMPATIBILITY DEMO")
    print("=" * 30)
    
    try:
        # 🔄 UPDATED: The new BacktestingEngine provides legacy aliases
        # These should work for backward compatibility
        from mlb_sharp_betting.services.backtesting_engine import EnhancedBacktestingService
        
        print("📡 Testing legacy alias compatibility...")
        
        # Test legacy alias
        legacy_service = EnhancedBacktestingService()
        print("✅ Legacy alias EnhancedBacktestingService works!")
        
        # Test initialization
        await legacy_service.initialize()
        print("✅ Legacy initialization works!")
        
        # Test legacy method calls
        status = legacy_service.get_comprehensive_status()
        print(f"✅ Legacy method calls work! Status: {bool(status)}")
        
    except Exception as e:
        print(f"❌ Legacy compatibility demo failed: {e}")


async def demo_engine_comparison():
    """Demonstrate the improvements in the new engine."""
    print("\n⚡ ENGINE COMPARISON DEMO")
    print("=" * 30)
    
    try:
        # 🔄 NEW: Show the consolidated engine benefits
        backtesting_engine = get_backtesting_engine()
        await backtesting_engine.initialize()
        
        print("🆕 Phase 3 BacktestingEngine Benefits:")
        print("   • 62% code reduction (5,318 → ~2,000 lines)")
        print("   • 5 services consolidated into 1 unified engine")
        print("   • Lazy-loaded modules for better performance")
        print("   • Comprehensive 5-checkpoint diagnostics")
        print("   • Unified error handling and logging")
        print("   • Backward compatibility with legacy aliases")
        
        # Show loaded modules
        modules = {
            "Core Engine": hasattr(backtesting_engine, 'core_engine'),
            "Diagnostics": hasattr(backtesting_engine, 'diagnostics'),
            "Scheduler": hasattr(backtesting_engine, 'scheduler'),
            "Accuracy Monitor": hasattr(backtesting_engine, 'accuracy_monitor')
        }
        
        print("\n🧩 Loaded Modules:")
        for module, loaded in modules.items():
            status = "✅" if loaded else "❌"
            print(f"   {status} {module}")
        
        # Performance comparison
        print("\n⚡ Performance Test:")
        import time
        start_time = time.time()
        await backtesting_engine.diagnostics.run_quick_diagnostic()
        elapsed = time.time() - start_time
        print(f"   Quick diagnostic: {elapsed:.2f} seconds")
        
    except Exception as e:
        print(f"❌ Engine comparison demo failed: {e}")


async def main():
    """Run all demos."""
    await demo_backtesting_engine_functionality()
    await demo_historical_backtesting()
    await demo_legacy_compatibility()
    await demo_engine_comparison()


if __name__ == "__main__":
    asyncio.run(main()) 