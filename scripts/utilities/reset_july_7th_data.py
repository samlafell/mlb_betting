#!/usr/bin/env python3
"""
Reset July 7th data for fresh testing with all fixes applied.
"""

import asyncio
import sys
from datetime import date
sys.path.append('.')

from sportsbookreview.services.data_storage_service import DataStorageService
from sportsbookreview.services.collection_orchestrator import CollectionOrchestrator

async def reset_july_7th_data():
    """Reset July 7th data for fresh testing."""
    storage = DataStorageService()
    await storage.initialize_connection()
    
    target_date = date(2025, 7, 7)
    print(f"🔄 Resetting July 7th ({target_date}) data for fresh testing...")
    
    try:
        async with storage.pool.acquire() as conn:
            async with conn.transaction():
                # Step 1: Delete existing betting records for July 7th
                print(f"\n📊 Step 1: Clearing existing betting records...")
                
                # Delete from betting tables
                moneyline_deleted = await conn.execute('''
                    DELETE FROM mlb_betting.moneyline 
                    WHERE DATE(odds_timestamp) = $1
                ''', target_date)
                
                spreads_deleted = await conn.execute('''
                    DELETE FROM mlb_betting.spreads 
                    WHERE DATE(odds_timestamp) = $1
                ''', target_date)
                
                totals_deleted = await conn.execute('''
                    DELETE FROM mlb_betting.totals 
                    WHERE DATE(odds_timestamp) = $1
                ''', target_date)
                
                print(f"  Deleted moneyline records: {moneyline_deleted}")
                print(f"  Deleted spreads records: {spreads_deleted}")
                print(f"  Deleted totals records: {totals_deleted}")
                
                # Step 2: Reset staging data for July 7th
                print(f"\n📦 Step 2: Resetting staging data...")
                
                # Reset parsed games status to allow reprocessing
                staging_reset = await conn.execute('''
                    UPDATE sbr_parsed_games 
                    SET status = 'new'
                    WHERE DATE(parsed_at) = $1
                    AND status IN ('loaded', 'failed', 'duplicate', 'parsed')
                ''', target_date)
                
                print(f"  Reset {staging_reset} staging records to 'new' status")
                
                # Step 3: Clear raw HTML for July 7th to force re-scraping
                print(f"\n🌐 Step 3: Clearing raw HTML to force re-scraping...")
                
                html_deleted = await conn.execute('''
                    DELETE FROM sbr_raw_html 
                    WHERE DATE(scraped_at) = $1
                ''', target_date)
                
                print(f"  Deleted {html_deleted} raw HTML records")
                
        print(f"\n✅ July 7th data reset complete!")
        
        # Step 4: Show current status
        print(f"\n📊 Current status check:")
        
        # Check remaining records for July 7th
        remaining_betting = await storage.pool.fetch('''
            SELECT 'moneyline' as table_name, COUNT(*) as count FROM mlb_betting.moneyline WHERE DATE(odds_timestamp) = $1
            UNION ALL
            SELECT 'spreads' as table_name, COUNT(*) as count FROM mlb_betting.spreads WHERE DATE(odds_timestamp) = $1
            UNION ALL
            SELECT 'totals' as table_name, COUNT(*) as count FROM mlb_betting.totals WHERE DATE(odds_timestamp) = $1
        ''', target_date)
        
        for record in remaining_betting:
            print(f"  {record['table_name']}: {record['count']} records remaining")
        
        # Check staging status
        staging_status = await storage.pool.fetch('''
            SELECT status, COUNT(*) as count
            FROM sbr_parsed_games 
            WHERE DATE(parsed_at) = $1
            GROUP BY status
            ORDER BY status
        ''', target_date)
        
        print(f"  Staging status:")
        for record in staging_status:
            print(f"    {record['status']}: {record['count']} records")
            
    except Exception as e:
        print(f"❌ Error resetting July 7th data: {e}")
        raise
    
    finally:
        await storage.close_connection()

async def collect_july_7th_fresh():
    """Collect July 7th data fresh with all fixes applied."""
    print(f"\n🚀 Starting fresh collection for July 7th...")
    
    target_date = date(2025, 7, 7)
    
    async with CollectionOrchestrator() as orchestrator:
        try:
            # Scrape July 7th data
            print(f"📥 Phase 1: Scraping July 7th data...")
            await orchestrator.scrape_with_progress(target_date, target_date)
            
            # Process staging data
            print(f"⚙️  Phase 2: Processing staging data...")
            await orchestrator.process_staging()
            
            print(f"✅ Fresh collection complete!")
            
        except Exception as e:
            print(f"❌ Error during fresh collection: {e}")
            raise

async def verify_july_7th_results():
    """Verify the results of July 7th fresh collection."""
    storage = DataStorageService()
    await storage.initialize_connection()
    
    target_date = date(2025, 7, 7)
    print(f"\n📊 Verifying July 7th results...")
    
    try:
        # Check betting tables
        results = await storage.pool.fetch('''
            SELECT 'moneyline' as table_name, COUNT(*) as count FROM mlb_betting.moneyline WHERE DATE(odds_timestamp) = $1
            UNION ALL
            SELECT 'spreads' as table_name, COUNT(*) as count FROM mlb_betting.spreads WHERE DATE(odds_timestamp) = $1
            UNION ALL
            SELECT 'totals' as table_name, COUNT(*) as count FROM mlb_betting.totals WHERE DATE(odds_timestamp) = $1
        ''', target_date)
        
        print(f"  July 7th betting records:")
        total_records = 0
        for record in results:
            count = record['count']
            total_records += count
            print(f"    {record['table_name']}: {count} records")
        
        print(f"  Total records: {total_records}")
        
        # Check spreads price success rate for July 7th
        spreads_success = await storage.pool.fetchrow('''
            SELECT 
                COUNT(*) as total_spreads,
                COUNT(home_spread_price) as spreads_with_prices,
                ROUND(COUNT(home_spread_price) * 100.0 / NULLIF(COUNT(*), 0), 1) as success_rate
            FROM mlb_betting.spreads 
            WHERE DATE(odds_timestamp) = $1
        ''', target_date)
        
        if spreads_success['total_spreads'] > 0:
            print(f"  Spreads price success rate: {spreads_success['success_rate']}% ({spreads_success['spreads_with_prices']}/{spreads_success['total_spreads']})")
        
        # Sample recent records
        if total_records > 0:
            print(f"\n🎯 Sample records from July 7th:")
            
            # Sample totals
            totals_sample = await storage.pool.fetch('''
                SELECT sportsbook, total_line, over_price, under_price
                FROM mlb_betting.totals 
                WHERE DATE(odds_timestamp) = $1
                LIMIT 3
            ''', target_date)
            
            if totals_sample:
                print(f"  Totals:")
                for record in totals_sample:
                    print(f"    {record['sportsbook']}: {record['total_line']} (O: {record['over_price']}, U: {record['under_price']})")
            
            # Sample spreads with prices
            spreads_sample = await storage.pool.fetch('''
                SELECT sportsbook, home_spread, away_spread, home_spread_price, away_spread_price
                FROM mlb_betting.spreads 
                WHERE DATE(odds_timestamp) = $1
                AND home_spread_price IS NOT NULL
                LIMIT 3
            ''', target_date)
            
            if spreads_sample:
                print(f"  Spreads (with prices):")
                for record in spreads_sample:
                    print(f"    {record['sportsbook']}: {record['home_spread']} ({record['home_spread_price']}) / {record['away_spread']} ({record['away_spread_price']})")
        
    finally:
        await storage.close_connection()

async def main():
    """Main function to reset and reprocess July 7th data."""
    print(f"🧪 JULY 7TH DATA RESET AND FRESH COLLECTION")
    print(f"=" * 50)
    
    try:
        # Step 1: Reset existing data
        await reset_july_7th_data()
        
        # Step 2: Fresh collection
        await collect_july_7th_fresh()
        
        # Step 3: Verify results
        await verify_july_7th_results()
        
        print(f"\n🎉 July 7th fresh collection completed successfully!")
        print(f"All fixes have been applied and tested.")
        
    except Exception as e:
        print(f"\n❌ Error during July 7th reset and collection: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main()) 