#!/usr/bin/env python3
"""
Test full processing with all fixes applied.
"""

import asyncio
import sys
from datetime import date

sys.path.append(".")

from sportsbookreview.services.collection_orchestrator import CollectionOrchestrator
from sportsbookreview.services.data_storage_service import DataStorageService


async def test_full_processing():
    """Test full processing with all fixes."""
    target_date = date(2025, 7, 9)

    print(f"🧪 TESTING FULL PROCESSING FOR {target_date}")
    print("=" * 60)

    # Process staging data with collection orchestrator
    print("\n⚙️  Processing staging data with collection orchestrator...")
    async with CollectionOrchestrator() as orchestrator:
        try:
            await orchestrator.process_staging()
            print("✅ Processing complete!")
        except Exception as e:
            print(f"❌ Processing failed: {e}")
            import traceback

            traceback.print_exc()
            return

    # Verify results
    print("\n📊 Verifying results...")
    storage = DataStorageService()
    await storage.initialize_connection()

    try:
        # Check betting tables
        results = await storage.pool.fetch(
            """
            SELECT 'moneyline' as table_name, COUNT(*) as count FROM mlb_betting.moneyline WHERE DATE(odds_timestamp) = $1
            UNION ALL
            SELECT 'spreads' as table_name, COUNT(*) as count FROM mlb_betting.spreads WHERE DATE(odds_timestamp) = $1
            UNION ALL
            SELECT 'totals' as table_name, COUNT(*) as count FROM mlb_betting.totals WHERE DATE(odds_timestamp) = $1
        """,
            target_date,
        )

        print(f"  Betting records for {target_date}:")
        total_records = 0
        for record in results:
            count = record["count"]
            total_records += count
            print(f"    {record['table_name']}: {count} records")

        if total_records > 0:
            # Check spreads success rate
            spreads_success = await storage.pool.fetchrow(
                """
                SELECT 
                    COUNT(*) as total_spreads,
                    COUNT(home_spread_price) as spreads_with_prices,
                    ROUND(COUNT(home_spread_price) * 100.0 / NULLIF(COUNT(*), 0), 1) as success_rate
                FROM mlb_betting.spreads 
                WHERE DATE(odds_timestamp) = $1
            """,
                target_date,
            )

            if spreads_success and spreads_success["total_spreads"] > 0:
                print(
                    f"  Spreads price success rate: {spreads_success['success_rate']}% ({spreads_success['spreads_with_prices']}/{spreads_success['total_spreads']})"
                )

            # Sample records
            print("\n🎯 Sample records:")

            # Moneyline samples
            ml_samples = await storage.pool.fetch(
                """
                SELECT sportsbook, home_ml, away_ml
                FROM mlb_betting.moneyline 
                WHERE DATE(odds_timestamp) = $1
                AND home_ml IS NOT NULL
                LIMIT 5
            """,
                target_date,
            )

            if ml_samples:
                print("  Moneyline (with odds):")
                for record in ml_samples:
                    print(
                        f"    {record['sportsbook']}: {record['home_ml']} / {record['away_ml']}"
                    )

            # Spreads samples
            spreads_samples = await storage.pool.fetch(
                """
                SELECT sportsbook, home_spread, away_spread, home_spread_price, away_spread_price
                FROM mlb_betting.spreads 
                WHERE DATE(odds_timestamp) = $1
                AND home_spread_price IS NOT NULL
                LIMIT 5
            """,
                target_date,
            )

            if spreads_samples:
                print("  Spreads (with prices):")
                for record in spreads_samples:
                    print(
                        f"    {record['sportsbook']}: {record['home_spread']} ({record['home_spread_price']}) / {record['away_spread']} ({record['away_spread_price']})"
                    )

            # Totals samples
            totals_samples = await storage.pool.fetch(
                """
                SELECT sportsbook, total_line, over_price, under_price
                FROM mlb_betting.totals 
                WHERE DATE(odds_timestamp) = $1
                LIMIT 5
            """,
                target_date,
            )

            if totals_samples:
                print("  Totals:")
                for record in totals_samples:
                    print(
                        f"    {record['sportsbook']}: {record['total_line']} (O: {record['over_price']}, U: {record['under_price']})"
                    )

        print("\n📈 SUMMARY:")
        print(f"  Total records processed: {total_records}")

        # Check staging status after processing
        staging_status = await storage.pool.fetch(
            """
            SELECT status, COUNT(*) as count
            FROM sbr_parsed_games 
            WHERE DATE(parsed_at) = $1
            GROUP BY status
            ORDER BY status
        """,
            target_date,
        )

        print("  Staging status after processing:")
        for record in staging_status:
            print(f"    {record['status']}: {record['count']} records")

    finally:
        await storage.close_connection()


if __name__ == "__main__":
    asyncio.run(test_full_processing())
