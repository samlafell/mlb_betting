#!/usr/bin/env python3
"""
Debug script to trace the bet_type issue in SportsbookReview data pipeline.

This script examines the data flow from staging to betting tables to identify
where the bet_type field is getting lost or corrupted.
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add project paths
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def debug_bet_type_pipeline():
    """Debug the bet_type processing pipeline."""

    print("🔍 Debugging Bet Type Processing Pipeline")
    print("=" * 60)

    try:
        import asyncpg

        conn = await asyncpg.connect(
            host="localhost", port=5432, database="mlb_betting", user="samlafell"
        )

        # Step 1: Check raw staging data
        print("\n📊 Step 1: Raw Staging Data Analysis")
        print("-" * 40)

        staging_data = await conn.fetch("""
            SELECT 
                id,
                raw_html_id,
                game_data->>'bet_type' as bet_type,
                game_data->>'sbr_game_id' as game_id,
                jsonb_array_length(game_data->'odds_data') as odds_count
            FROM public.sbr_parsed_games 
            WHERE raw_html_id IN (4, 5, 6)  -- Recent July 7th data
            ORDER BY id
            LIMIT 10;
        """)

        print(f"Found {len(staging_data)} staging records:")
        for row in staging_data:
            print(
                f"  ID {row['id']}: bet_type='{row['bet_type']}', game={row['game_id']}, odds={row['odds_count']}"
            )

        # Step 2: Examine a specific odds_data structure
        print("\n📊 Step 2: Odds Data Structure Analysis")
        print("-" * 40)

        sample_odds = await conn.fetchrow("""
            SELECT 
                game_data->>'bet_type' as bet_type,
                game_data->>'sbr_game_id' as game_id,
                game_data->'odds_data' as odds_data
            FROM public.sbr_parsed_games 
            WHERE raw_html_id IN (4, 5, 6)
              AND jsonb_array_length(game_data->'odds_data') > 0
            LIMIT 1;
        """)

        if sample_odds:
            print(f"Sample game: {sample_odds['game_id']}")
            print(f"Bet type: '{sample_odds['bet_type']}'")
            print("Odds data structure:")

            odds_data = sample_odds["odds_data"]
            if odds_data:
                for i, odds in enumerate(odds_data):
                    print(f"  Odds {i + 1}: {odds}")

        # Step 3: Simulate the collection orchestrator processing
        print("\n📊 Step 3: Simulating Collection Orchestrator Processing")
        print("-" * 40)

        # Get a sample game_data
        sample_game = await conn.fetchrow("""
            SELECT game_data
            FROM public.sbr_parsed_games 
            WHERE raw_html_id IN (4, 5, 6)
              AND jsonb_array_length(game_data->'odds_data') > 0
            LIMIT 1;
        """)

        if sample_game:
            game_dict = sample_game["game_data"]
            print(f"Type of game_dict: {type(game_dict)}")

            # Handle the case where game_data is a string (double-encoded JSON)
            if isinstance(game_dict, str):
                import json

                try:
                    game_dict = json.loads(game_dict)
                    print(f"Parsed game_dict from string, now type: {type(game_dict)}")
                except json.JSONDecodeError:
                    print("Failed to parse game_dict as JSON")
                    return

            print(f"Original bet_type: '{game_dict.get('bet_type')}'")
            print(f"Type of odds_data: {type(game_dict.get('odds_data'))}")

            # Handle the case where odds_data might be a string
            odds_data = game_dict.get("odds_data", [])
            if isinstance(odds_data, str):
                import json

                try:
                    odds_data = json.loads(odds_data)
                    print(f"Parsed odds_data from string, now type: {type(odds_data)}")
                except json.JSONDecodeError:
                    print("Failed to parse odds_data as JSON")
                    odds_data = []

            # Simulate the odds record construction from collection_orchestrator.py
            odds_records = []
            for odds in odds_data:
                record = {
                    "bet_type": game_dict.get("bet_type"),
                    "sportsbook": odds.get("sportsbook"),
                    "timestamp": game_dict.get("scraped_at"),
                }
                # Map keys based on bet type
                if record["bet_type"] == "moneyline":
                    record["home_ml"] = odds.get("moneyline_home") or odds.get(
                        "home_ml"
                    )
                    record["away_ml"] = odds.get("moneyline_away") or odds.get(
                        "away_ml"
                    )
                elif record["bet_type"] == "spread":
                    record["home_spread"] = odds.get("spread_home") or odds.get(
                        "home_spread"
                    )
                    record["away_spread"] = odds.get("spread_away") or odds.get(
                        "away_spread"
                    )
                elif record["bet_type"] in ("total", "totals"):
                    record["total_line"] = odds.get("total_line")
                    record["over_price"] = odds.get("total_over") or odds.get(
                        "over_price"
                    )
                    record["under_price"] = odds.get("total_under") or odds.get(
                        "under_price"
                    )

                odds_records.append(record)

            print(f"Generated {len(odds_records)} betting records:")
            for i, record in enumerate(odds_records):
                print(
                    f"  Record {i + 1}: bet_type='{record.get('bet_type')}', sportsbook='{record.get('sportsbook')}'"
                )
                print(f"             Keys: {list(record.keys())}")

        # Step 4: Check what's actually in the betting tables
        print("\n📊 Step 4: Betting Tables Analysis")
        print("-" * 40)

        # Check for July 7th games
        july_7_games = await conn.fetch("""
            SELECT id, home_team, away_team, sportsbookreview_game_id
            FROM public.games 
            WHERE DATE(game_date) = '2025-07-07'
            ORDER BY id;
        """)

        print(f"Found {len(july_7_games)} games for July 7th:")
        for game in july_7_games[:3]:  # Show first 3
            game_id = game["id"]
            print(f"\n  Game {game_id}: {game['away_team']} @ {game['home_team']}")

            # Check betting data for this game
            ml_count = await conn.fetchval(
                "SELECT COUNT(*) FROM mlb_betting.moneyline WHERE game_id = $1", game_id
            )
            spread_count = await conn.fetchval(
                "SELECT COUNT(*) FROM mlb_betting.spreads WHERE game_id = $1", game_id
            )
            total_count = await conn.fetchval(
                "SELECT COUNT(*) FROM mlb_betting.totals WHERE game_id = $1", game_id
            )

            print(
                f"    Moneyline: {ml_count}, Spreads: {spread_count}, Totals: {total_count}"
            )

        await conn.close()

    except Exception as e:
        print(f"❌ Debug failed: {e}")
        logger.error(f"Debug failed: {e}", exc_info=True)


async def main():
    """Main function."""
    await debug_bet_type_pipeline()
    print("\nGeneral Balls")


if __name__ == "__main__":
    asyncio.run(main())
