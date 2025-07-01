#!/usr/bin/env python3
"""
Test script for Phase 1 strategies implementation
Tests the three new strategies: Total Sweet Spots, Underdog ML Value, Team Bias
"""

import asyncio
import sys
from pathlib import Path

# Add the src directory to the path so we can import the database coordinator
sys.path.append(str(Path(__file__).parent.parent / "src"))

from mlb_sharp_betting.services.database_coordinator import get_database_coordinator


async def test_strategy_sql(strategy_name: str, sql_file: str) -> dict:
    """Test a single strategy SQL file"""
    coordinator = get_database_coordinator()
    
    # Read the SQL file
    sql_path = Path(__file__).parent / sql_file
    if not sql_path.exists():
        return {"error": f"SQL file not found: {sql_file}"}
    
    with open(sql_path, 'r') as f:
        sql_query = f.read()
    
    try:
        print(f"\n🧪 Testing {strategy_name}...")
        print("=" * 50)
        
        # Execute the strategy query
        results = await coordinator.execute_read(sql_query)
        
        if not results:
            return {
                "strategy": strategy_name,
                "status": "WARNING",
                "message": "No results returned - may indicate insufficient data",
                "row_count": 0
            }
        
        print(f"✅ {strategy_name} executed successfully!")
        print(f"📊 Found {len(results)} strategy variants")
        
        # Show top results
        for i, result in enumerate(results[:3]):
            print(f"\n{i+1}. {result.get('strategy_variant', 'Unknown')}")
            print(f"   • Win Rate: {result.get('win_rate', 'N/A')}%")
            print(f"   • ROI: {result.get('roi_per_100_unit', 'N/A')}%")
            print(f"   • Total Bets: {result.get('total_bets', 'N/A')}")
            print(f"   • Rating: {result.get('strategy_rating', 'N/A')}")
        
        if len(results) > 3:
            print(f"\n   ... and {len(results) - 3} more variants")
        
        return {
            "strategy": strategy_name,
            "status": "SUCCESS",
            "row_count": len(results),
            "sample_results": results[:3]
        }
        
    except Exception as e:
        print(f"❌ Error testing {strategy_name}: {str(e)}")
        return {
            "strategy": strategy_name,
            "status": "ERROR",
            "error": str(e)
        }


async def test_database_prerequisites() -> bool:
    """Test that required database tables exist"""
    coordinator = get_database_coordinator()
    
    print("🔍 Checking database prerequisites...")
    
    # Check for required tables
    required_tables = [
        "mlb_betting.splits.raw_mlb_betting_splits",
        "mlb_betting.public.game_outcomes"
    ]
    
    for table in required_tables:
        try:
            result = await coordinator.execute_read(f"SELECT COUNT(*) as count FROM {table} LIMIT 1")
            count = result[0]['count'] if result else 0
            print(f"✅ {table}: {count:,} rows")
        except Exception as e:
            print(f"❌ {table}: Error - {str(e)}")
            return False
    
    # Check for recent data
    try:
        result = await coordinator.execute_read("""
            SELECT 
                COUNT(*) as total_games,
                MIN(game_datetime) as earliest_game,
                MAX(game_datetime) as latest_game,
                COUNT(DISTINCT split_type) as split_types,
                COUNT(DISTINCT source || '-' || COALESCE(book, 'UNKNOWN')) as source_books
            FROM mlb_betting.splits.raw_mlb_betting_splits
        """)
        
        if result:
            data = result[0]
            print(f"\n📋 Data Summary:")
            print(f"   • Total games: {data['total_games']:,}")
            print(f"   • Date range: {data['earliest_game']} to {data['latest_game']}")
            print(f"   • Split types: {data['split_types']}")
            print(f"   • Source-book combinations: {data['source_books']}")
        
    except Exception as e:
        print(f"⚠️  Warning: Could not get data summary - {str(e)}")
    
    return True


async def main():
    """Main test function"""
    print("🚀 Testing Phase 1 MLB Betting Strategies")
    print("=" * 60)
    
    # Test database prerequisites
    if not await test_database_prerequisites():
        print("\n❌ Database prerequisites not met. Please ensure:")
        print("   1. Database is running")
        print("   2. Required tables exist with data")
        print("   3. Recent game data is available")
        return
    
    # Test each strategy
    strategies = [
        ("Total Line Sweet Spots", "total_line_sweet_spots_strategy.sql"),
        ("Underdog ML Value", "underdog_ml_value_strategy.sql"),
        ("Team Specific Bias", "team_specific_bias_strategy.sql")
    ]
    
    results = []
    for strategy_name, sql_file in strategies:
        result = await test_strategy_sql(strategy_name, sql_file)
        results.append(result)
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 PHASE 1 STRATEGIES TEST SUMMARY")
    print("=" * 60)
    
    success_count = sum(1 for r in results if r["status"] == "SUCCESS")
    warning_count = sum(1 for r in results if r["status"] == "WARNING")
    error_count = sum(1 for r in results if r["status"] == "ERROR")
    
    print(f"✅ Successful: {success_count}")
    print(f"⚠️  Warnings: {warning_count}")
    print(f"❌ Errors: {error_count}")
    
    if success_count == len(strategies):
        print("\n🎉 All Phase 1 strategies are working correctly!")
        print("\n📈 Next steps:")
        print("   1. Run strategies on larger historical datasets")
        print("   2. Integrate into backtesting service")
        print("   3. Add to automated strategy detection")
    elif success_count > 0:
        print(f"\n✅ {success_count}/{len(strategies)} strategies working")
        print("Check errors above and ensure sufficient data")
    else:
        print("\n❌ No strategies working - check database and data availability")
    
    # Show any errors
    for result in results:
        if result["status"] == "ERROR":
            print(f"\n❌ {result['strategy']}: {result.get('error', 'Unknown error')}")
    
    print("\n🏁 Test complete!")


if __name__ == "__main__":
    asyncio.run(main()) 