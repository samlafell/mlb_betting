#!/usr/bin/env python3
"""
Process all staging data from SportsbookReview scraping into final betting tables.

This script processes data from sbr_parsed_games (status='new') into the final
mlb_betting.moneyline, mlb_betting.spreads, and mlb_betting.totals tables.
"""

import asyncio
import sys
from pathlib import Path

# Add the sportsbookreview module to the path
sys.path.append(str(Path(__file__).parent / "sportsbookreview"))

from sportsbookreview.services.collection_orchestrator import CollectionOrchestrator


async def process_all_staging_data():
    """Process all staging data into final betting tables."""
    print("🚀 PROCESSING ALL STAGING DATA INTO FINAL BETTING TABLES")
    print("=" * 70)

    try:
        # Initialize collection orchestrator
        async with CollectionOrchestrator() as orchestrator:
            print("✅ Collection orchestrator initialized")

            # Check staging data status first
            print("\n📊 Checking staging data status...")
            async with orchestrator.storage.pool.acquire() as conn:
                staging_counts = await conn.fetch("""
                    SELECT status, COUNT(*) as count
                    FROM sbr_parsed_games 
                    GROUP BY status
                    ORDER BY status
                """)

                print("Staging data status:")
                total_new = 0
                total_parsed = 0
                for record in staging_counts:
                    count = record["count"]
                    status = record["status"]
                    print(f"  {status}: {count} records")
                    if status == "new":
                        total_new = count
                    elif status == "parsed":
                        total_parsed = count

                total_to_process = total_new + total_parsed
                if total_to_process == 0:
                    print("\n⚠️  No staging data to process")
                    return
                else:
                    print(f"\n🎯 Found {total_to_process} records to process:")
                    if total_new > 0:
                        print(f"    - {total_new} new records")
                    if total_parsed > 0:
                        print(
                            f"    - {total_parsed} parsed records (will be reprocessed)"
                        )

            # Process staging data
            print("\n⚙️  Processing staging data...")
            await orchestrator.process_staging()

            print("✅ Staging processing completed!")

            # Check results
            print("\n📊 Verifying results...")
            async with orchestrator.storage.pool.acquire() as conn:
                # Check final betting tables
                betting_counts = await conn.fetch("""
                    SELECT 'moneyline' as table_name, COUNT(*) as count 
                    FROM mlb_betting.moneyline 
                    WHERE DATE(odds_timestamp) >= CURRENT_DATE - INTERVAL '7 days'
                    UNION ALL
                    SELECT 'spreads' as table_name, COUNT(*) as count 
                    FROM mlb_betting.spreads 
                    WHERE DATE(odds_timestamp) >= CURRENT_DATE - INTERVAL '7 days'
                    UNION ALL
                    SELECT 'totals' as table_name, COUNT(*) as count 
                    FROM mlb_betting.totals 
                    WHERE DATE(odds_timestamp) >= CURRENT_DATE - INTERVAL '7 days'
                """)

                print("\nRecent betting records (last 7 days):")
                total_records = 0
                for record in betting_counts:
                    count = record["count"]
                    total_records += count
                    print(f"  {record['table_name']}: {count} records")

                # Check updated staging status
                updated_staging = await conn.fetch("""
                    SELECT status, COUNT(*) as count
                    FROM sbr_parsed_games 
                    WHERE parsed_at >= NOW() - INTERVAL '1 hour'
                    GROUP BY status
                    ORDER BY status
                """)

                print("\nRecently updated staging status:")
                for record in updated_staging:
                    print(f"  {record['status']}: {record['count']} records")

                print(
                    f"\n🎉 PROCESSING COMPLETE - {total_records} total recent records"
                )

            # Get final stats
            storage_stats = orchestrator.storage.get_storage_stats()
            print("\n📈 Final storage statistics:")
            for key, value in storage_stats.items():
                print(f"  {key}: {value}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(process_all_staging_data())
