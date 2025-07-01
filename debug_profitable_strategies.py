#!/usr/bin/env python3
"""
Debug Profitable Strategies Loading

This script checks what strategies are available in the database and what gets
loaded by the get_profitable_strategies method to debug the "No profitable strategies found" issue.
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.mlb_sharp_betting.db.connection import get_db_manager
from src.mlb_sharp_betting.services.betting_signal_repository import BettingSignalRepository
from src.mlb_sharp_betting.models.betting_analysis import SignalProcessorConfig


async def debug_profitable_strategies():
    """Debug what profitable strategies are available"""
    
    print("🔍 DEBUGGING PROFITABLE STRATEGIES LOADING")
    print("=" * 60)
    
    try:
        # Initialize dependencies
        config = SignalProcessorConfig()
        repository = BettingSignalRepository(config)
        
        # First, check what's in the database
        print("📊 CHECKING DATABASE CONTENTS...")
        query_all = """
        SELECT 
            strategy_name,
            source_book_type,
            split_type,
            win_rate * 100 as win_rate_pct,
            roi_per_100,
            total_bets,
            confidence_level,
            backtest_date
        FROM backtesting.strategy_performance 
        ORDER BY backtest_date DESC, roi_per_100 DESC
        LIMIT 20
        """
        
        all_results = repository.coordinator.execute_read(query_all)
        print(f"Total strategies in database: {len(all_results)}")
        
        if all_results:
            print("\n📋 TOP 20 STRATEGIES IN DATABASE:")
            print("Strategy Name | Source-Book | Split | Win Rate | ROI | Bets | Date")
            print("-" * 80)
            for row in all_results[:10]:
                print(f"{row['strategy_name'][:25]:25} | {row['source_book_type'][:10]:10} | "
                      f"{row['split_type'][:8]:8} | {row['win_rate_pct']:6.1f}% | "
                      f"{row['roi_per_100']:6.1f}% | {row['total_bets']:4d} | {row['backtest_date']}")
        
        # Check date filtering
        print(f"\n🗓️  CHECKING DATE FILTERING...")
        max_date_query = "SELECT MAX(backtest_date) as max_date FROM backtesting.strategy_performance"
        max_date_result = repository.coordinator.execute_read(max_date_query)
        max_date = max_date_result[0]['max_date'] if max_date_result else None
        print(f"Latest backtest date: {max_date}")
        
        if max_date:
            latest_date_query = """
            SELECT COUNT(*) as count 
            FROM backtesting.strategy_performance 
            WHERE backtest_date = %s
            """
            latest_count_result = repository.coordinator.execute_read(latest_date_query, (max_date,))
            latest_count = latest_count_result[0]['count'] if latest_count_result else 0
            print(f"Strategies on latest date ({max_date}): {latest_count}")
        
        # Test the actual filter criteria
        print(f"\n🎯 TESTING FILTER CRITERIA...")
        filter_query = """
        SELECT 
            strategy_name,
            source_book_type,
            split_type,
            win_rate * 100 as win_rate_pct,
            roi_per_100,
            total_bets,
            confidence_level,
            backtest_date,
            CASE 
                WHEN roi_per_100 >= 20.0 THEN 'ROI >= 20%'
                WHEN roi_per_100 >= 15.0 AND total_bets >= 8 THEN 'ROI >= 15% & Bets >= 8'
                WHEN roi_per_100 >= 10.0 AND win_rate >= 0.45 THEN 'ROI >= 10% & WR >= 45%'
                WHEN roi_per_100 >= 5.0 AND win_rate >= 0.55 AND total_bets >= 10 THEN 'ROI >= 5%, WR >= 55%, Bets >= 10'
                WHEN total_bets >= 20 AND roi_per_100 > 0.0 THEN 'Bets >= 20 & ROI > 0%'
                ELSE 'Does not meet criteria'
            END as filter_reason
        FROM backtesting.strategy_performance 
        WHERE backtest_date = (SELECT MAX(backtest_date) FROM backtesting.strategy_performance)
          AND total_bets >= 5
        ORDER BY roi_per_100 DESC
        """
        
        filter_results = repository.coordinator.execute_read(filter_query)
        
        qualifying_strategies = [r for r in filter_results if r['filter_reason'] != 'Does not meet criteria']
        non_qualifying_strategies = [r for r in filter_results if r['filter_reason'] == 'Does not meet criteria']
        
        print(f"✅ Strategies that MEET filter criteria: {len(qualifying_strategies)}")
        print(f"❌ Strategies that DON'T MEET filter criteria: {len(non_qualifying_strategies)}")
        
        if qualifying_strategies:
            print("\n🎯 QUALIFYING STRATEGIES:")
            print("Strategy Name | Source-Book | Split | Win Rate | ROI | Bets | Filter Reason")
            print("-" * 100)
            for row in qualifying_strategies[:10]:
                print(f"{row['strategy_name'][:20]:20} | {row['source_book_type'][:8]:8} | "
                      f"{row['split_type'][:8]:8} | {row['win_rate_pct']:6.1f}% | "
                      f"{row['roi_per_100']:6.1f}% | {row['total_bets']:4d} | {row['filter_reason']}")
        
        if non_qualifying_strategies:
            print("\n❌ NON-QUALIFYING STRATEGIES (showing first 10):")
            print("Strategy Name | Source-Book | Split | Win Rate | ROI | Bets")
            print("-" * 70)
            for row in non_qualifying_strategies[:10]:
                print(f"{row['strategy_name'][:20]:20} | {row['source_book_type'][:8]:8} | "
                      f"{row['split_type'][:8]:8} | {row['win_rate_pct']:6.1f}% | "
                      f"{row['roi_per_100']:6.1f}% | {row['total_bets']:4d}")
        
        # Test the repository method
        print(f"\n🧪 TESTING REPOSITORY METHOD...")
        loaded_strategies = await repository.get_profitable_strategies()
        print(f"Strategies loaded by repository.get_profitable_strategies(): {len(loaded_strategies)}")
        
        if loaded_strategies:
            print("\n📋 LOADED STRATEGIES:")
            print("Strategy Name | Source-Book | Split | Win Rate | ROI | Bets | Confidence")
            print("-" * 80)
            for strategy in loaded_strategies[:10]:
                print(f"{strategy.strategy_name[:20]:20} | {strategy.source_book[:8]:8} | "
                      f"{strategy.split_type[:8]:8} | {strategy.win_rate:6.1f}% | "
                      f"{strategy.roi:6.1f}% | {strategy.total_bets:4d} | {strategy.confidence[:15]:15}")
        
        print(f"\n✅ DIAGNOSIS COMPLETE")
        if len(loaded_strategies) == 0:
            print("🚨 ISSUE: No strategies are being loaded due to strict filtering criteria")
            print("💡 SOLUTION: Relax the filter criteria or check data quality")
        else:
            print(f"✅ SUCCESS: {len(loaded_strategies)} strategies loaded successfully")
            
    except Exception as e:
        print(f"❌ Error during debugging: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(debug_profitable_strategies()) 