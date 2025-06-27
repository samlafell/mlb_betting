#!/usr/bin/env python3
"""
Refactored Backtesting Demo

This demonstrates the new SimplifiedBacktestingService with real historical data.
Shows how the service now uses actual game outcomes instead of mock data.
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add the src directory to the path
current_dir = Path(__file__).parent
project_root = current_dir.parent
sys.path.insert(0, str(project_root / "src"))

from mlb_sharp_betting.services.backtesting_service import SimplifiedBacktestingService
from mlb_sharp_betting.db.connection import get_db_manager


async def demo_real_historical_backtesting():
    """Demonstrate the real historical backtesting functionality."""
    print("🚀 Real Historical Data Backtesting Demo")
    print("=" * 50)
    
    # Initialize the service
    db_manager = get_db_manager()
    service = SimplifiedBacktestingService(db_manager)
    
    try:
        # Initialize the service (this loads all processors)
        print("📡 Initializing backtesting service...")
        await service.initialize()
        print(f"✅ Service initialized with {len(service.executors)} strategy executors")
        
        # Define test date range (last 30 days)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        print(f"\n📅 Testing date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        # Run backtesting with real historical data
        print("\n🎯 Running backtesting with real game outcomes...")
        results = await service.run_backtest(
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d")
        )
        
        # Display results
        print("\n📊 BACKTESTING RESULTS")
        print("=" * 50)
        
        summary = results.get("summary", {})
        execution_stats = results.get("execution_stats", {})
        
        print(f"Total strategies analyzed: {summary.get('total_strategies', 0)}")
        print(f"Profitable strategies: {summary.get('profitable_strategies', 0)}")
        print(f"Reliable strategies: {summary.get('reliable_strategies', 0)}")
        
        aggregate = summary.get("aggregate_metrics", {})
        if aggregate:
            print(f"\nAggregate Performance:")
            print(f"  Total bets: {aggregate.get('total_bets', 0)}")
            print(f"  Total wins: {aggregate.get('total_wins', 0)}")
            print(f"  Overall win rate: {aggregate.get('overall_win_rate', 0):.1%}")
            print(f"  Weighted ROI: {aggregate.get('weighted_roi', 0):.1f}%")
        
        print(f"\nExecution Stats:")
        print(f"  Raw results: {execution_stats.get('raw_count', 0)}")
        print(f"  Valid results: {execution_stats.get('valid_count', 0)}")
        print(f"  Final count: {execution_stats.get('deduplicated_count', 0)}")
        
        failed = execution_stats.get('failed_strategies', [])
        if failed:
            print(f"  Failed strategies: {', '.join(failed)}")
        
        # Show top performers
        top_performers = summary.get("top_performers", [])
        if top_performers:
            print(f"\n🏆 TOP PERFORMING STRATEGIES")
            print("-" * 30)
            for i, performer in enumerate(top_performers[:3], 1):
                print(f"{i}. {performer.strategy_name}")
                print(f"   ROI: {performer.roi_per_100:.1f}% | Win Rate: {performer.win_rate:.1%}")
                print(f"   Sample: {performer.total_bets} bets ({performer.sample_size_category})")
        
        # Show individual strategy details
        strategy_results = results.get("results", [])
        if strategy_results:
            print(f"\n📈 DETAILED STRATEGY RESULTS")
            print("-" * 40)
            for result in strategy_results:
                print(f"\n{result.strategy_name}:")
                print(f"  Bets: {result.total_bets} | Wins: {result.wins}")
                print(f"  Win Rate: {result.win_rate:.1%}")
                print(f"  ROI: {result.roi_per_100:.1f}%")
                print(f"  Confidence: {result.confidence_score:.1%}")
                print(f"  Sample Category: {result.sample_size_category}")
        
        print(f"\n✅ Demo completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Clean up database connection
        if hasattr(db_manager, 'close'):
            db_manager.close()


async def demo_legacy_compatibility():
    """Demonstrate backward compatibility with legacy BacktestingService."""
    print("\n🔄 Legacy Compatibility Demo")
    print("=" * 30)
    
    # Import the legacy-compatible service
    from mlb_sharp_betting.services.backtesting_service import BacktestingService
    
    # Initialize database manager for legacy service
    db_manager = get_db_manager()
    
    # Use legacy interface with proper database manager
    legacy_service = BacktestingService(db_manager)
    
    try:
        print("📡 Running legacy daily backtesting pipeline...")
        legacy_results = await legacy_service.run_daily_backtesting_pipeline()
        
        print(f"✅ Legacy interface worked!")
        print(f"   Strategies analyzed: {legacy_results.total_strategies_analyzed}")
        print(f"   Profitable strategies: {legacy_results.profitable_strategies}")
        print(f"   Data completeness: {legacy_results.data_completeness_pct:.1f}%")
        
    except Exception as e:
        print(f"❌ Legacy demo failed: {e}")
    
    finally:
        # Clean up database connection
        if hasattr(db_manager, 'close'):
            db_manager.close()


async def main():
    """Run all demos."""
    await demo_real_historical_backtesting()
    await demo_legacy_compatibility()


if __name__ == "__main__":
    asyncio.run(main()) 