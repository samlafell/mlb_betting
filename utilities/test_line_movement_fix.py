#!/usr/bin/env python3
"""
Test Script: Line Movement Post-Game Filter Fix
Purpose: Verify that the new line movement views correctly filter out post-game activity
"""

import asyncio

import asyncpg

from src.core.config import get_settings
from src.core.logging import LogComponent, get_logger

logger = get_logger(__name__, LogComponent.CORE)


async def test_line_movement_filtering():
    """Test the line movement view filtering functionality."""
    settings = get_settings()

    # Connect to database
    conn = await asyncpg.connect(
        host=settings.database.host,
        port=settings.database.port,
        database=settings.database.database,
        user=settings.database.user,
        password=settings.database.password,
    )

    try:
        print("🔍 Testing Line Movement Post-Game Filtering")
        print("=" * 60)

        # Test 1: Validate no post-game movements in new view
        print("\n1. Validating no post-game movements...")
        result = await conn.fetchrow(
            "SELECT * FROM staging.validate_no_post_game_movements()"
        )

        if result:
            print(f"   Total movements: {result['total_movements']:,}")
            print(
                f"   Time range: {result['min_minutes_before']:.1f} to {result['max_minutes_before']:.1f} minutes before game"
            )
            print(f"   Post-game count: {result['post_game_count']}")
            print(f"   Status: {result['validation_status']}")

            if result["post_game_count"] == 0:
                print("   ✅ PASS: No post-game movements detected")
            else:
                print("   ❌ FAIL: Post-game movements still present")

        # Test 2: Compare old vs new view record counts
        print("\n2. Comparing view record counts...")

        # Get count from original historical table (all movements)
        all_movements = await conn.fetchval(
            "SELECT COUNT(*) FROM staging.action_network_odds_historical"
        )

        # Get count from new pre-game view
        pre_game_movements = await conn.fetchval(
            "SELECT COUNT(*) FROM staging.v_pre_game_line_movements"
        )

        # Get count from compatibility view
        compat_movements = await conn.fetchval(
            "SELECT COUNT(*) FROM staging.v_line_movements"
        )

        print(f"   All historical movements: {all_movements:,}")
        print(f"   Pre-game movements: {pre_game_movements:,}")
        print(f"   Compatibility view: {compat_movements:,}")

        if pre_game_movements < all_movements:
            filtered_out = all_movements - pre_game_movements
            percentage = (filtered_out / all_movements) * 100
            print(f"   ✅ Filtered out: {filtered_out:,} movements ({percentage:.1f}%)")
        else:
            print("   ⚠️  Warning: No movements were filtered out")

        if compat_movements == pre_game_movements:
            print("   ✅ Compatibility view working correctly")
        else:
            print("   ❌ Compatibility view count mismatch")

        # Test 3: Analyze timing distribution
        print("\n3. Analyzing movement timing distribution...")
        timing_stats = await conn.fetch("""
            SELECT 
                timing_category,
                COUNT(*) as movement_count,
                COUNT(*) FILTER (WHERE ABS(COALESCE(filtered_odds_change, 0)) >= 10) as sharp_moves,
                ROUND(AVG(movement_quality_score), 3) as avg_quality
            FROM staging.v_pre_game_line_movements
            WHERE previous_odds IS NOT NULL
            GROUP BY timing_category
            ORDER BY 
                CASE timing_category
                    WHEN 'early_week' THEN 1
                    WHEN 'day_before' THEN 2
                    WHEN 'hours_before' THEN 3
                    WHEN 'late_pregame' THEN 4
                    WHEN 'very_late' THEN 5
                END
        """)

        if timing_stats:
            print("   Timing Category     | Movements | Sharp Moves | Avg Quality")
            print("   -------------------|-----------|-------------|------------")
            for stat in timing_stats:
                print(
                    f"   {stat['timing_category']:<18} | {stat['movement_count']:>8,} | {stat['sharp_moves']:>10,} | {stat['avg_quality']:>10}"
                )
        else:
            print("   ⚠️  No timing statistics available")

        # Test 4: Sample recent games with movements
        print("\n4. Checking recent games with line movements...")
        recent_games = await conn.fetch("""
            SELECT 
                external_game_id,
                home_team,
                away_team,
                game_start_time,
                COUNT(*) as total_movements,
                COUNT(*) FILTER (WHERE timing_category = 'very_late') as very_late_moves,
                MAX(ABS(COALESCE(filtered_odds_change, 0))) as largest_move
            FROM staging.v_pre_game_line_movements
            WHERE game_date >= CURRENT_DATE - INTERVAL '3 days'
              AND previous_odds IS NOT NULL
            GROUP BY external_game_id, home_team, away_team, game_start_time
            HAVING COUNT(*) >= 10  -- Games with significant movement activity
            ORDER BY largest_move DESC
            LIMIT 5
        """)

        if recent_games:
            print("   Recent games with significant line movement activity:")
            for game in recent_games:
                start_time = (
                    game["game_start_time"].strftime("%Y-%m-%d %H:%M %Z")
                    if game["game_start_time"]
                    else "Unknown"
                )
                print(f"   📈 {game['home_team']} vs {game['away_team']}")
                print(f"      Start: {start_time}")
                print(
                    f"      Movements: {game['total_movements']}, Very Late: {game['very_late_moves']}, Largest: {game['largest_move']}"
                )
        else:
            print("   ⚠️  No recent games with significant movement activity found")

        # Test 5: Verify sharp action detection
        print("\n5. Testing sharp action detection...")
        sharp_action_count = await conn.fetchval(
            "SELECT COUNT(*) FROM staging.v_late_sharp_action"
        )

        if sharp_action_count > 0:
            print(f"   ✅ Late sharp action detected: {sharp_action_count:,} movements")

            # Get sample of sharp action
            sample_sharp = await conn.fetchrow("""
                SELECT 
                    home_team, away_team, sportsbook_name, market_type,
                    minutes_before_game, odds_change, sharp_intensity_score
                FROM staging.v_late_sharp_action
                ORDER BY sharp_intensity_score DESC
                LIMIT 1
            """)

            if sample_sharp:
                print(
                    f"   📊 Top sharp action: {sample_sharp['home_team']} vs {sample_sharp['away_team']}"
                )
                print(
                    f"      {sample_sharp['sportsbook_name']} {sample_sharp['market_type']}"
                )
                print(
                    f"      {sample_sharp['minutes_before_game']:.1f} min before, {sample_sharp['odds_change']} odds change"
                )
                print(
                    f"      Intensity score: {sample_sharp['sharp_intensity_score']:.1f}/10"
                )
        else:
            print("   ⚠️  No late sharp action detected (may be normal)")

        print("\n" + "=" * 60)
        print("🎯 Line Movement Filter Test Complete")

        # Overall assessment
        if (
            result
            and result["post_game_count"] == 0
            and pre_game_movements < all_movements
        ):
            print("✅ SUCCESS: Post-game filtering is working correctly!")
        else:
            print("❌ ISSUES DETECTED: Please review the results above")

    except Exception as e:
        logger.error(f"Test failed: {e}")
        print(f"❌ Test failed with error: {e}")

    finally:
        await conn.close()


async def main():
    """Main test execution."""
    try:
        await test_line_movement_filtering()
    except Exception as e:
        logger.error(f"Failed to run line movement test: {e}")
        print(f"Failed to run test: {e}")


if __name__ == "__main__":
    asyncio.run(main())
