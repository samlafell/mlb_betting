#!/usr/bin/env python3
"""
Check what dates we have data for and test with recent data.
"""

import asyncio
import sys

sys.path.append(".")

from sportsbookreview.services.data_storage_service import DataStorageService


async def check_available_dates():
    """Check what dates we have data for."""
    storage = DataStorageService()
    await storage.initialize_connection()

    try:
        print("🔍 Checking available dates in our database...")

        # Check raw HTML dates
        raw_dates = await storage.pool.fetch("""
            SELECT DATE(scraped_at) as date, COUNT(*) as count
            FROM sbr_raw_html 
            GROUP BY DATE(scraped_at)
            ORDER BY date DESC
            LIMIT 10
        """)

        print("\n📅 Raw HTML data dates (last 10):")
        for record in raw_dates:
            print(f"  {record['date']}: {record['count']} records")

        # Check staging dates
        staging_dates = await storage.pool.fetch("""
            SELECT DATE(parsed_at) as date, status, COUNT(*) as count
            FROM sbr_parsed_games 
            GROUP BY DATE(parsed_at), status
            ORDER BY date DESC, status
            LIMIT 20
        """)

        print("\n📦 Staging data dates and status:")
        current_date = None
        for record in staging_dates:
            if record["date"] != current_date:
                if current_date is not None:
                    print()  # Add spacing between dates
                current_date = record["date"]
                print(f"  {record['date']}:")
            print(f"    {record['status']}: {record['count']} records")

        # Check betting table dates
        betting_dates = await storage.pool.fetch("""
            SELECT DATE(odds_timestamp) as date, 'moneyline' as table_name, COUNT(*) as count FROM mlb_betting.moneyline GROUP BY DATE(odds_timestamp)
            UNION ALL
            SELECT DATE(odds_timestamp) as date, 'spreads' as table_name, COUNT(*) as count FROM mlb_betting.spreads GROUP BY DATE(odds_timestamp)
            UNION ALL
            SELECT DATE(odds_timestamp) as date, 'totals' as table_name, COUNT(*) as count FROM mlb_betting.totals GROUP BY DATE(odds_timestamp)
            ORDER BY date DESC, table_name
            LIMIT 30
        """)

        print("\n🎯 Betting data dates:")
        current_date = None
        for record in betting_dates:
            if record["date"] != current_date:
                if current_date is not None:
                    print()  # Add spacing between dates
                current_date = record["date"]
                print(f"  {record['date']}:")
            print(f"    {record['table_name']}: {record['count']} records")

        # Find the most recent date with staging data that we can reprocess
        recent_staging = await storage.pool.fetchrow("""
            SELECT DATE(parsed_at) as date, COUNT(*) as total_records
            FROM sbr_parsed_games 
            WHERE status IN ('loaded', 'failed', 'duplicate', 'parsed')
            GROUP BY DATE(parsed_at)
            ORDER BY date DESC
            LIMIT 1
        """)

        if recent_staging:
            print(
                f"\n🎯 Most recent date with reprocessable staging data: {recent_staging['date']} ({recent_staging['total_records']} records)"
            )
            return recent_staging["date"]
        else:
            print("\n❌ No staging data found for reprocessing")
            return None

    finally:
        await storage.close_connection()


async def reset_and_test_date(target_date):
    """Reset and test a specific date."""
    storage = DataStorageService()
    await storage.initialize_connection()

    print(f"\n🔄 Resetting {target_date} data for fresh testing...")

    try:
        async with storage.pool.acquire() as conn:
            async with conn.transaction():
                # Delete existing betting records
                print(f"📊 Clearing existing betting records for {target_date}...")

                moneyline_deleted = await conn.execute(
                    """
                    DELETE FROM mlb_betting.moneyline 
                    WHERE DATE(odds_timestamp) = $1
                """,
                    target_date,
                )

                spreads_deleted = await conn.execute(
                    """
                    DELETE FROM mlb_betting.spreads 
                    WHERE DATE(odds_timestamp) = $1
                """,
                    target_date,
                )

                totals_deleted = await conn.execute(
                    """
                    DELETE FROM mlb_betting.totals 
                    WHERE DATE(odds_timestamp) = $1
                """,
                    target_date,
                )

                print(f"  Deleted moneyline: {moneyline_deleted}")
                print(f"  Deleted spreads: {spreads_deleted}")
                print(f"  Deleted totals: {totals_deleted}")

                # Reset staging data status
                staging_reset = await conn.execute(
                    """
                    UPDATE sbr_parsed_games 
                    SET status = 'parsed'
                    WHERE DATE(parsed_at) = $1
                    AND status IN ('loaded', 'failed', 'duplicate')
                """,
                    target_date,
                )

                print(f"  Reset {staging_reset} staging records to 'parsed' status")

        print(f"✅ {target_date} data reset complete!")

    finally:
        await storage.close_connection()


async def test_processing_with_fixes(target_date):
    """Test processing with our fixes applied."""
    from sportsbookreview.services.collection_orchestrator import CollectionOrchestrator

    print(f"\n⚙️  Testing processing for {target_date} with all fixes...")

    async with CollectionOrchestrator() as orchestrator:
        try:
            # Process staging data with our fixes
            await orchestrator.process_staging()
            print("✅ Processing complete!")

        except Exception as e:
            print(f"❌ Error during processing: {e}")
            raise


async def verify_results(target_date):
    """Verify the results after processing."""
    storage = DataStorageService()
    await storage.initialize_connection()

    print(f"\n📊 Verifying results for {target_date}...")

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

            if spreads_success["total_spreads"] > 0:
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
                LIMIT 3
            """,
                target_date,
            )

            if ml_samples:
                print("  Moneyline (with odds):")
                for record in ml_samples:
                    print(
                        f"    {record['sportsbook']}: {record['home_ml']} / {record['away_ml']}"
                    )

            # Totals samples
            totals_samples = await storage.pool.fetch(
                """
                SELECT sportsbook, total_line, over_price, under_price
                FROM mlb_betting.totals 
                WHERE DATE(odds_timestamp) = $1
                LIMIT 3
            """,
                target_date,
            )

            if totals_samples:
                print("  Totals:")
                for record in totals_samples:
                    print(
                        f"    {record['sportsbook']}: {record['total_line']} (O: {record['over_price']}, U: {record['under_price']})"
                    )

        print(f"  Total records: {total_records}")

    finally:
        await storage.close_connection()


async def main():
    """Main function."""
    print("🔍 CHECKING AVAILABLE DATES AND TESTING FIXES")
    print("=" * 50)

    try:
        # Check what dates we have
        target_date = await check_available_dates()

        if target_date:
            print(f"\n🧪 Testing with date: {target_date}")

            # Reset the target date data
            await reset_and_test_date(target_date)

            # Test processing with fixes
            await test_processing_with_fixes(target_date)

            # Verify results
            await verify_results(target_date)

            print(f"\n🎉 Testing completed for {target_date}!")
        else:
            print("\n❌ No suitable date found for testing")

    except Exception as e:
        print(f"\n❌ Error during testing: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
