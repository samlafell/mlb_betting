#!/usr/bin/env python3
"""
Debug script to check what data is available for backtesting
"""

import asyncio
from datetime import datetime, timedelta
from mlb_sharp_betting.db.connection import get_db_manager

async def debug_backtesting_data():
    """Check what data is available for backtesting"""
    
    print("🔍 BACKTESTING DATA DIAGNOSTIC")
    print("=" * 60)
    
    db_manager = get_db_manager()
    
    # Calculate the same date range as backtesting
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=7)
    
    print(f"📅 Backtesting looks for data from: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    try:
        with db_manager.get_cursor() as cursor:
            # Check if splits table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = 'splits' 
                    AND table_name = 'raw_mlb_betting_splits'
                )
            """)
            table_exists = cursor.fetchone()[0]
            
            print(f"📊 Table exists: {'✅ YES' if table_exists else '❌ NO'}")
            
            if not table_exists:
                print("❌ The splits.raw_mlb_betting_splits table doesn't exist!")
                print("💡 You need to run data collection first: mlb-cli run")
                return
            
            # Check total records in table
            cursor.execute("SELECT COUNT(*) FROM splits.raw_mlb_betting_splits")
            total_records = cursor.fetchone()[0]
            print(f"📊 Total records in table: {total_records:,}")
            
            if total_records == 0:
                print("❌ No data in the splits table!")
                print("💡 You need to collect data first: mlb-cli run")
                return
            
            # Check records in date range
            cursor.execute("""
                SELECT COUNT(*) FROM splits.raw_mlb_betting_splits
                WHERE last_updated >= %s AND last_updated <= %s
            """, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d') + ' 23:59:59'))
            date_range_records = cursor.fetchone()[0]
            print(f"📊 Records in backtesting date range: {date_range_records:,}")
            
            # Check records meeting backtesting criteria
            cursor.execute("""
                SELECT COUNT(*) FROM splits.raw_mlb_betting_splits
                WHERE last_updated >= %s AND last_updated <= %s
                  AND game_datetime IS NOT NULL
                  AND home_or_over_bets_percentage IS NOT NULL
                  AND home_or_over_stake_percentage IS NOT NULL
            """, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d') + ' 23:59:59'))
            qualified_records = cursor.fetchone()[0]
            print(f"📊 Records meeting backtesting criteria: {qualified_records:,}")
            
            # Check recent data
            cursor.execute("""
                SELECT MAX(last_updated) FROM splits.raw_mlb_betting_splits
            """)
            latest_update = cursor.fetchone()[0]
            print(f"📊 Most recent data: {latest_update}")
            
            # Check data by source
            cursor.execute("""
                SELECT source, COUNT(*) as count
                FROM splits.raw_mlb_betting_splits
                WHERE last_updated >= %s AND last_updated <= %s
                GROUP BY source
                ORDER BY count DESC
            """, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d') + ' 23:59:59'))
            sources = cursor.fetchall()
            
            if sources:
                print(f"\n📡 Data by source in date range:")
                for source, count in sources:
                    print(f"   • {source}: {count:,} records")
            
            # Check if we have game outcomes
            cursor.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'game_outcomes'
                )
            """)
            outcomes_table_exists = cursor.fetchone()[0]
            print(f"\n🎯 Game outcomes table exists: {'✅ YES' if outcomes_table_exists else '❌ NO'}")
            
            if outcomes_table_exists:
                cursor.execute("SELECT COUNT(*) FROM public.game_outcomes")
                outcomes_count = cursor.fetchone()[0]
                print(f"🎯 Game outcomes records: {outcomes_count:,}")
            
            # Sample some data
            cursor.execute("""
                SELECT game_id, home_team, away_team, split_type, 
                       home_or_over_bets_percentage, home_or_over_stake_percentage,
                       source, book, last_updated
                FROM splits.raw_mlb_betting_splits
                WHERE last_updated >= %s AND last_updated <= %s
                  AND game_datetime IS NOT NULL
                  AND home_or_over_bets_percentage IS NOT NULL
                  AND home_or_over_stake_percentage IS NOT NULL
                ORDER BY last_updated DESC
                LIMIT 5
            """, (start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d') + ' 23:59:59'))
            sample_data = cursor.fetchall()
            
            if sample_data:
                print(f"\n📋 Sample data meeting criteria:")
                for row in sample_data:
                    print(f"   • Game {row[0]}: {row[1]} vs {row[2]} ({row[3]}) - {row[6]}")
                    print(f"     Bets: {row[4]:.1f}%, Stakes: {row[5]:.1f}%")
            
    except Exception as e:
        print(f"❌ Error checking data: {e}")
        import traceback
        print(traceback.format_exc())
    
    print("=" * 60)
    print("🔍 DIAGNOSTIC COMPLETE")
    
    # Also check processor factory initialization
    print("\n🔧 CHECKING STRATEGY PROCESSOR FACTORY")
    try:
        from mlb_sharp_betting.services.backtesting_engine import get_backtesting_engine
        engine = get_backtesting_engine()
        await engine.initialize()
        
        # Check if core engine has processor factory
        core_engine = engine.core_engine
        await core_engine.initialize_factory()
        
        if core_engine.processor_factory:
            all_processors = core_engine.processor_factory.get_all_processors()
            print(f"✅ Processor factory initialized: {len(all_processors)} processors available")
            for name in all_processors.keys():
                print(f"   • {name}")
        else:
            print("❌ Processor factory not initialized")
            
    except Exception as e:
        print(f"❌ Error checking processor factory: {e}")

if __name__ == "__main__":
    asyncio.run(debug_backtesting_data()) 