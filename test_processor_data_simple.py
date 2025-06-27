#!/usr/bin/env python3
"""
Simple test script to see what data processors are actually receiving
"""

import sys
import asyncio
from pathlib import Path
from datetime import datetime, timedelta

# Add the src directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from mlb_sharp_betting.db.connection import get_db_manager


async def test_data_filtering():
    """Test what data filtering is happening in repository queries"""
    
    db_manager = get_db_manager()
    
    # Create time window (last 7 days)
    end_time = datetime.now()
    start_time = end_time - timedelta(days=7)
    
    print("🔍 TESTING DATA FILTERING ISSUE")
    print("=" * 60)
    print(f"Time window: {start_time} to {end_time}")
    print()
    
    try:
        with db_manager.get_cursor() as cursor:
            # Test 1: All available data
            print("🎯 Test 1: All available moneyline data...")
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
            if stats:
                print(f"✅ Raw data analysis:")
                print(f"   📊 Total records: {stats['total_records']:,}")
                print(f"   🎮 Unique games: {stats['unique_games']:,}")
                if stats['min_bet_pct'] is not None:
                    print(f"   📈 Betting percentage range: {stats['min_bet_pct']:.1f}% to {stats['max_bet_pct']:.1f}%")
                    print(f"   📊 Average betting percentage: {stats['avg_bet_pct']:.1f}%")
                print(f"   🔥 High public (>70%): {stats['high_public']:,} records")
                print(f"   ❄️  Low public (<30%): {stats['low_public']:,} records")
                print(f"   ⚖️  Moderate (30-70%): {stats['moderate_public']:,} records")
                
                total_available = stats['total_records']
                qualifying_for_fade = stats['high_public'] + stats['low_public']
                filtered_out = total_available - qualifying_for_fade
                
                print(f"\n🚨 FILTER IMPACT ANALYSIS:")
                print(f"   📋 Total available records: {total_available:,}")
                print(f"   ✅ Records that pass current fade filter (>70% or <30%): {qualifying_for_fade:,}")
                print(f"   ❌ Records filtered out: {filtered_out:,} ({filtered_out/total_available*100:.1f}%)")
            print()
            
            # Test 2: What the repository query actually returns
            print("🎯 Test 2: Repository query simulation (with filters)...")
            cursor.execute("""
                WITH latest_splits AS (
                    SELECT 
                        game_id, home_team, away_team, split_type, split_value,
                        home_or_over_stake_percentage, home_or_over_bets_percentage,
                        (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                        source, COALESCE(book, 'UNKNOWN') as book, game_datetime, last_updated,
                        ROW_NUMBER() OVER (
                            PARTITION BY game_id, split_type, source, COALESCE(book, 'UNKNOWN')
                            ORDER BY last_updated DESC
                        ) as rn
                    FROM splits.raw_mlb_betting_splits
                    WHERE game_datetime BETWEEN %s AND %s
                      AND home_or_over_stake_percentage IS NOT NULL 
                      AND home_or_over_bets_percentage IS NOT NULL
                      AND last_updated >= NOW() - INTERVAL '24 hours'
                )
                SELECT COUNT(*) as filtered_count
                FROM latest_splits
                WHERE rn = 1
                  AND (home_or_over_bets_percentage > 70 OR home_or_over_bets_percentage < 30)
            """, (start_time, end_time))
            
            filtered_result = cursor.fetchone()
            if filtered_result:
                print(f"✅ Repository query returns: {filtered_result['filtered_count']:,} records")
                
                if filtered_result['filtered_count'] == 0:
                    print(f"❌ THE PROBLEM: Repository is returning 0 records!")
                    print(f"   This explains why all processors generate 0 signals!")
            print()
            
            # Test 3: What if we remove the percentage filter?
            print("🎯 Test 3: Repository query WITHOUT percentage filter...")
            cursor.execute("""
                WITH latest_splits AS (
                    SELECT 
                        game_id, home_team, away_team, split_type, split_value,
                        home_or_over_stake_percentage, home_or_over_bets_percentage,
                        (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                        source, COALESCE(book, 'UNKNOWN') as book, game_datetime, last_updated,
                        ROW_NUMBER() OVER (
                            PARTITION BY game_id, split_type, source, COALESCE(book, 'UNKNOWN')
                            ORDER BY last_updated DESC
                        ) as rn
                    FROM splits.raw_mlb_betting_splits
                    WHERE game_datetime BETWEEN %s AND %s
                      AND home_or_over_stake_percentage IS NOT NULL 
                      AND home_or_over_bets_percentage IS NOT NULL
                      AND last_updated >= NOW() - INTERVAL '24 hours'
                )
                SELECT COUNT(*) as unfiltered_count,
                       MIN(home_or_over_bets_percentage) as min_pct,
                       MAX(home_or_over_bets_percentage) as max_pct,
                       AVG(home_or_over_bets_percentage) as avg_pct
                FROM latest_splits
                WHERE rn = 1
            """, (start_time, end_time))
            
            unfiltered_result = cursor.fetchone()
            if unfiltered_result:
                print(f"✅ Without percentage filter: {unfiltered_result['unfiltered_count']:,} records")
                if unfiltered_result['min_pct'] is not None:
                    print(f"   📈 Percentage range: {unfiltered_result['min_pct']:.1f}% to {unfiltered_result['max_pct']:.1f}%")
                    print(f"   📊 Average: {unfiltered_result['avg_pct']:.1f}%")
                    
                    potential_signals = unfiltered_result['unfiltered_count']
                    actual_signals = filtered_result['filtered_count'] if filtered_result else 0
                    lost_signals = potential_signals - actual_signals
                    
                    print(f"\n💡 SOLUTION INSIGHT:")
                    print(f"   🎯 Potential signals available: {potential_signals:,}")
                    print(f"   ❌ Currently filtered out: {lost_signals:,}")
                    print(f"   📈 Signal recovery potential: {lost_signals/potential_signals*100:.1f}%")
            
            print()
            print("🔧 RECOMMENDED FIXES:")
            print("   1. Remove the percentage filter from repository queries")
            print("   2. Let processors decide their own thresholds")
            print("   3. Allow strategies to analyze all available data")
            
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    asyncio.run(test_data_filtering()) 