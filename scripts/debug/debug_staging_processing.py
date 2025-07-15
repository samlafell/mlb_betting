#!/usr/bin/env python3
"""
Debug staging data processing to see why records aren't being processed.
"""

import asyncio
import sys
from datetime import date

sys.path.append(".")

from sportsbookreview.services.data_storage_service import DataStorageService


async def debug_staging_data():
    """Debug staging data to see what's available for processing."""
    storage = DataStorageService()
    await storage.initialize_connection()

    target_date = date(2025, 7, 9)

    try:
        print(f"🔍 Debugging staging data for {target_date}...")

        # Check staging data status
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

        print("\n📦 Staging status breakdown:")
        for record in staging_status:
            print(f"  {record['status']}: {record['count']} records")

        # Check some 'parsed' records to see their structure
        parsed_samples = await storage.pool.fetch(
            """
            SELECT id, raw_html_id, game_data, parsed_at, status
            FROM sbr_parsed_games 
            WHERE DATE(parsed_at) = $1
            AND status = 'parsed'
            LIMIT 3
        """,
            target_date,
        )

        print("\n🎯 Sample 'parsed' records:")
        for i, record in enumerate(parsed_samples, 1):
            game_data = record["game_data"]
            print(f"  Record {i}:")
            print(f"    ID: {record['id']}")
            print(f"    Raw HTML ID: {record['raw_html_id']}")
            if game_data:
                print(f"    Game Data Type: {type(game_data)}")
                print(f"    Game Data Content: {str(game_data)[:300]}...")

                # Handle if it's a string that needs parsing
                if isinstance(game_data, str):
                    import json

                    try:
                        game_data = json.loads(game_data)
                    except:
                        print("    Failed to parse game_data as JSON")
                        continue

                if isinstance(game_data, dict):
                    print(
                        f"    Teams: {game_data.get('away_team', 'N/A')} @ {game_data.get('home_team', 'N/A')}"
                    )
                    print(f"    Bet Type: {game_data.get('bet_type', 'N/A')}")
                    odds_data = game_data.get("odds_data", [])
                    print(f"    Odds Data Length: {len(odds_data)} items")

                    # Show first part of odds data
                    if odds_data:
                        odds_str = str(odds_data)[:200]
                        print(f"    Odds Data Sample: {odds_str}...")
                    else:
                        print("    Odds Data: EMPTY")

        # Check if there are any records that should be processed
        processable_count = await storage.pool.fetchval(
            """
            SELECT COUNT(*)
            FROM sbr_parsed_games 
            WHERE DATE(parsed_at) = $1
            AND status = 'parsed'
            AND game_data IS NOT NULL
        """,
            target_date,
        )

        print(f"\n📊 Records that should be processable: {processable_count}")

        # Check if there are games with actual betting odds
        odds_with_data = await storage.pool.fetch(
            """
            SELECT id, game_data,
                   CASE 
                       WHEN game_data::text LIKE '%moneyline%' THEN 'HAS_MONEYLINE'
                       ELSE 'NO_MONEYLINE'
                   END as has_moneyline,
                   CASE 
                       WHEN game_data::text LIKE '%spread%' THEN 'HAS_SPREAD'
                       ELSE 'NO_SPREAD'
                   END as has_spread,
                   CASE 
                       WHEN game_data::text LIKE '%total%' THEN 'HAS_TOTAL'
                       ELSE 'NO_TOTAL'
                   END as has_total
            FROM sbr_parsed_games 
            WHERE DATE(parsed_at) = $1
            AND status = 'parsed'
            AND game_data IS NOT NULL
            LIMIT 5
        """,
            target_date,
        )

        print("\n🎲 Betting data analysis:")
        for record in odds_with_data:
            game_data = record["game_data"]

            # Parse if it's a string
            if isinstance(game_data, str):
                import json

                try:
                    game_data = json.loads(game_data)
                except:
                    continue

            away_team = game_data.get("away_team", "N/A") if game_data else "N/A"
            home_team = game_data.get("home_team", "N/A") if game_data else "N/A"
            print(
                f"  {away_team} @ {home_team}: {record['has_moneyline']}, {record['has_spread']}, {record['has_total']}"
            )

    finally:
        await storage.close_connection()


async def test_single_record_processing():
    """Test processing a single record manually."""
    from sportsbookreview.services.integration_service import IntegrationService

    storage = DataStorageService()
    await storage.initialize_connection()

    target_date = date(2025, 7, 9)

    try:
        # Get one parsed record
        sample_record = await storage.pool.fetchrow(
            """
            SELECT id, raw_html_id, game_data, parsed_at, status
            FROM sbr_parsed_games 
            WHERE DATE(parsed_at) = $1
            AND status = 'parsed'
            AND game_data IS NOT NULL
            LIMIT 1
        """,
            target_date,
        )

        if sample_record:
            game_data = sample_record["game_data"]
            print("\n🧪 Testing single record processing...")
            print(f"  Record ID: {sample_record['id']}")
            if game_data:
                away_team = game_data.get("away_team", "N/A")
                home_team = game_data.get("home_team", "N/A")
                print(f"  Game: {away_team} @ {home_team}")
                print(f"  Bet Type: {game_data.get('bet_type', 'N/A')}")
                odds_data = game_data.get("odds_data", [])
                print(f"  Odds Data Items: {len(odds_data)}")

            # Create integration service
            integration = IntegrationService()
            await integration.initialize()

            try:
                # Process this single record - need to transform to expected format
                games_data = [dict(sample_record)]
                result = await integration.integrate_games(games_data)

                print(f"  Integration result: {result}")

            except Exception as e:
                print(f"  ❌ Error during integration: {e}")
                import traceback

                traceback.print_exc()

            finally:
                await integration.close()
        else:
            print("\n❌ No suitable record found for testing")

    finally:
        await storage.close_connection()


async def main():
    """Main function."""
    print("🔍 DEBUGGING STAGING DATA PROCESSING")
    print("=" * 50)

    try:
        await debug_staging_data()
        await test_single_record_processing()

    except Exception as e:
        print(f"\n❌ Error during debugging: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
