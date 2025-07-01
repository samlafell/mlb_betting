#!/usr/bin/env python3
"""
Signal Generation Debugging Script

This script specifically addresses the issue seen in backtesting logs where
only `sharp_action` generates signals while all other strategies show
"NO_SIGNALS: betting data found but no signals generated".

Based on the senior engineer's 5-checkpoint debugging approach.
"""

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

from mlb_sharp_betting.core.logging import setup_universal_logger_compatibility, get_logger
from mlb_sharp_betting.services.backtesting_diagnostics import get_diagnostics_service
from mlb_sharp_betting.db.connection import get_db_manager

# Initialize logging
setup_universal_logger_compatibility()
logger = get_logger(__name__)


async def debug_no_signals_issue():
    """
    Debug the specific issue where strategies show "betting data found but no signals generated"
    """
    print("🔍 DEBUGGING: No Signals Generated Issue")
    print("=" * 60)
    print("This script addresses the issue where strategies find data but generate no signals.")
    print()
    
    try:
        # Initialize diagnostics service
        print("📋 Step 1: Initializing diagnostics service...")
        diagnostics_service = await get_diagnostics_service()
        print("✅ Diagnostics service initialized")
        print()
        
        # Check data availability first
        print("📊 Step 2: Checking data availability...")
        await diagnostics_service._checkpoint_1_data_availability()
        
        data_result = next(
            (r for r in diagnostics_service._diagnostic_results if r.checkpoint_name == "Data Availability"), 
            None
        )
        
        if data_result:
            print(f"Status: {data_result.status.value}")
            print(f"Message: {data_result.message}")
            
            if data_result.status.value == 'FAIL':
                print("❌ CRITICAL: Data availability issue found!")
                print("This explains why strategies aren't generating signals.")
                print()
                print("🚀 RECOMMENDED FIXES:")
                for rec in data_result.recommendations:
                    print(f"  • {rec}")
                print()
                return False
        print()
        
        # Test individual processors
        print("🔧 Step 3: Testing individual processors...")
        await diagnostics_service._checkpoint_4_signal_generation()
        
        strategy_diagnostics = diagnostics_service._strategy_diagnostics
        
        print(f"📊 Found {len(strategy_diagnostics)} strategies to test")
        print()
        
        # Analyze results
        working_strategies = []
        failing_strategies = []
        
        for strategy_name, diagnostic in strategy_diagnostics.items():
            if diagnostic.processor_status.value == 'PASS':
                working_strategies.append((strategy_name, diagnostic.raw_signals_found))
            else:
                failing_strategies.append((strategy_name, diagnostic.processor_status.value, diagnostic.recommendations))
        
        print("✅ WORKING STRATEGIES:")
        if working_strategies:
            for strategy_name, signal_count in working_strategies:
                print(f"  • {strategy_name}: {signal_count} signals generated")
        else:
            print("  ❌ NO WORKING STRATEGIES FOUND")
        print()
        
        print("❌ FAILING STRATEGIES:")
        if failing_strategies:
            for strategy_name, status, recommendations in failing_strategies:
                print(f"  • {strategy_name} ({status}):")
                for rec in recommendations[:2]:  # Show first 2 recommendations
                    print(f"    - {rec}")
        else:
            print("  ✅ All strategies are working")
        print()
        
        # Specific analysis for the logs issue
        print("🎯 Step 4: Analyzing your specific issue...")
        
        # Check if sharp_action is the only working one
        if len(working_strategies) == 1 and working_strategies[0][0] == 'sharp_action':
            print("🔍 ISSUE IDENTIFIED: Only 'sharp_action' is generating signals")
            print("This matches the pattern in your backtesting logs.")
            print()
            
            print("🧠 LIKELY CAUSES:")
            print("1. 📊 Data Quality: Other strategies need different data structures")
            print("2. 🎛️  Thresholds: Other strategies have too restrictive criteria")
            print("3. ⏰ Timing: Other strategies expect data at different times")
            print("4. 🔧 Implementation: Other processors have bugs or missing logic")
            print()
            
            # Detailed analysis of one failing strategy
            if failing_strategies:
                sample_strategy = failing_strategies[0]
                strategy_name = sample_strategy[0]
                
                print(f"🔬 DETAILED ANALYSIS: {strategy_name}")
                print("-" * 40)
                
                diagnostic = strategy_diagnostics[strategy_name]
                
                print(f"Status: {diagnostic.processor_status.value}")
                print(f"Raw signals found: {diagnostic.raw_signals_found}")
                print(f"Data gaps: {diagnostic.data_gaps}")
                print()
                
                print("💡 RECOMMENDATIONS FOR THIS STRATEGY:")
                for i, rec in enumerate(diagnostic.recommendations, 1):
                    print(f"{i}. {rec}")
                print()
        
        # Database inspection
        print("🗃️  Step 5: Database inspection...")
        db_manager = get_db_manager()
        
        with db_manager.get_cursor() as cursor:
            # Check splits data structure
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT game_id) as unique_games,
                    COUNT(DISTINCT split_type) as split_types,
                    COUNT(DISTINCT sportsbook) as sportsbooks,
                    MIN(last_updated) as oldest_data,
                    MAX(last_updated) as newest_data,
                    AVG(CASE WHEN moneyline_odds IS NOT NULL THEN 1 ELSE 0 END) as moneyline_coverage,
                    AVG(CASE WHEN handle_percentage IS NOT NULL THEN 1 ELSE 0 END) as handle_coverage,
                    AVG(CASE WHEN bet_percentage IS NOT NULL THEN 1 ELSE 0 END) as bet_coverage
                FROM splits.raw_mlb_betting_splits
                WHERE last_updated >= CURRENT_DATE - INTERVAL '7 days'
            """)
            
            data_stats = cursor.fetchone()
            
            print("📈 DATA STATISTICS (Last 7 days):")
            print(f"  Total records: {data_stats['total_records']}")
            print(f"  Unique games: {data_stats['unique_games']}")
            print(f"  Split types: {data_stats['split_types']}")
            print(f"  Sportsbooks: {data_stats['sportsbooks']}")
            print(f"  Moneyline coverage: {data_stats['moneyline_coverage']:.1%}")
            print(f"  Handle coverage: {data_stats['handle_coverage']:.1%}")
            print(f"  Bet coverage: {data_stats['bet_coverage']:.1%}")
            print()
            
            # Check split types distribution
            cursor.execute("""
                SELECT 
                    split_type,
                    COUNT(*) as count,
                    COUNT(DISTINCT game_id) as games
                FROM splits.raw_mlb_betting_splits
                WHERE last_updated >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY split_type
                ORDER BY count DESC
            """)
            
            split_types = cursor.fetchall()
            
            print("📊 SPLIT TYPE DISTRIBUTION:")
            for split_type in split_types:
                print(f"  {split_type['split_type']}: {split_type['count']} records, {split_type['games']} games")
            print()
        
        # Final recommendations
        print("🚀 STEP-BY-STEP DEBUGGING GUIDE:")
        print("=" * 50)
        
        if len(working_strategies) <= 1:
            print("IMMEDIATE ACTIONS (run these commands):")
            print()
            print("1. 📡 Collect fresh data:")
            print("   source .env")
            print("   uv run src/mlb_sharp_betting/cli.py data collect --force")
            print()
            
            print("2. 🔍 Run full diagnostics:")
            print("   uv run src/mlb_sharp_betting/cli.py diagnostics run-full-diagnostic --verbose")
            print()
            
            print("3. 🎯 Debug specific strategy (example):")
            if failing_strategies:
                example_strategy = failing_strategies[0][0]
                print(f"   uv run src/mlb_sharp_betting/cli.py diagnostics debug-strategy {example_strategy}")
            print()
            
            print("4. ⚡ Quick health check:")
            print("   uv run src/mlb_sharp_betting/cli.py diagnostics quick-health-check")
            print()
        
        print("DEEPER INVESTIGATION:")
        print()
        print("1. 🔧 Check processor implementations:")
        print("   - Review src/mlb_sharp_betting/analysis/processors/")
        print("   - Compare working vs failing processor logic")
        print()
        
        print("2. 📊 Validate data requirements:")
        print("   - Each strategy may need different data fields")
        print("   - Check if required fields are populated")
        print()
        
        print("3. 🎛️  Review thresholds:")
        print("   - Failing strategies may have too strict criteria")
        print("   - Test with more permissive settings")
        print()
        
        return True
        
    except Exception as e:
        print(f"❌ Debugging failed: {e}")
        logger.error(f"Signal generation debugging failed: {e}")
        return False


async def test_specific_strategy(strategy_name: str):
    """
    Test a specific strategy in isolation to understand why it's not generating signals
    """
    print(f"🧪 TESTING STRATEGY: {strategy_name}")
    print("=" * 50)
    
    try:
        # Initialize diagnostics service
        diagnostics_service = await get_diagnostics_service()
        
        # Run full diagnostic to populate strategy diagnostics
        await diagnostics_service.run_full_diagnostic()
        
        # Get specific strategy diagnostic
        strategy_diag = diagnostics_service._strategy_diagnostics.get(strategy_name)
        
        if not strategy_diag:
            print(f"❌ Strategy '{strategy_name}' not found")
            available = list(diagnostics_service._strategy_diagnostics.keys())
            print(f"Available strategies: {', '.join(available)}")
            return False
        
        print(f"📊 Status: {strategy_diag.processor_status.value}")
        print(f"🎯 Raw signals: {strategy_diag.raw_signals_found}")
        print(f"🔬 Filtered signals: {strategy_diag.filtered_signals_found}")
        print(f"📈 Data quality: {strategy_diag.data_quality_score:.2f}")
        print()
        
        if strategy_diag.data_gaps:
            print("⚠️  DATA GAPS:")
            for gap in strategy_diag.data_gaps:
                print(f"  • {gap}")
            print()
        
        if strategy_diag.recommendations:
            print("💡 RECOMMENDATIONS:")
            for i, rec in enumerate(strategy_diag.recommendations, 1):
                print(f"{i}. {rec}")
            print()
        
        if strategy_diag.threshold_analysis:
            print("🎛️  THRESHOLD ANALYSIS:")
            for key, value in strategy_diag.threshold_analysis.items():
                print(f"  {key}: {value}")
            print()
        
        return True
        
    except Exception as e:
        print(f"❌ Strategy testing failed: {e}")
        return False


if __name__ == "__main__":
    print("🔍 Signal Generation Debugging Script")
    print("Addressing the 'NO_SIGNALS' issue from backtesting logs")
    print()
    
    if len(sys.argv) > 1:
        # Test specific strategy
        strategy_name = sys.argv[1]
        success = asyncio.run(test_specific_strategy(strategy_name))
    else:
        # Run full debugging
        success = asyncio.run(debug_no_signals_issue())
    
    if not success:
        sys.exit(1)
    
    print("✅ Debugging completed successfully!")
    print()
    print("General Balls 🎾")
    print("Your AI Betting Assistant") 