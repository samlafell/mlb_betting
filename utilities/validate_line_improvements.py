#!/usr/bin/env python3
"""
Validate Line Movement Improvements

Test the corrected American odds calculations and line value detection.
"""

import asyncio
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import asyncpg

from src.core.config import get_settings


async def validate_improvements():
    """Validate the improved line movement calculations."""
    print("🔍 Validating line movement calculation improvements...")

    # Load settings
    settings = get_settings()

    # Connect to database
    try:
        conn = await asyncpg.connect(settings.database.connection_string)
        print("✅ Connected to database")

        # Test 1: Show largest corrections in American odds calculations
        print("\n📊 Test 1: Largest American odds corrections")
        corrections = await conn.fetch("""
            SELECT 
                external_game_id, sportsbook_name, side,
                previous_odds, odds,
                odds_change_raw as old_calculation,
                odds_change as new_calculation,
                (odds_change_raw - odds_change) as correction,
                movement_type
            FROM staging.v_line_movements 
            WHERE previous_odds IS NOT NULL
              AND ABS(odds_change_raw - odds_change) > 50
            ORDER BY ABS(odds_change_raw - odds_change) DESC
            LIMIT 5
        """)

        for row in corrections:
            print(
                f"   {row['external_game_id']} | {row['sportsbook_name']} | {row['side']}"
            )
            print(f"   {row['previous_odds']} → {row['odds']}")
            print(
                f"   Old calc: {row['old_calculation']}, New calc: {row['new_calculation']}"
            )
            print(
                f"   Correction: {row['correction']} points | Type: {row['movement_type']}"
            )
            print()

        # Test 2: Line value changes detection
        print("📊 Test 2: Line value changes causing false movements")
        line_changes = await conn.fetch("""
            SELECT 
                external_game_id, sportsbook_name, market_type, side,
                previous_line_value, line_value, line_value_change,
                previous_odds, odds, odds_change
            FROM staging.v_line_movements
            WHERE has_line_value_change = TRUE
              AND ABS(line_value_change) >= 1.0
            ORDER BY ABS(line_value_change) DESC
            LIMIT 5
        """)

        for row in line_changes:
            print(
                f"   {row['external_game_id']} | {row['sportsbook_name']} | {row['market_type']} {row['side']}"
            )
            print(
                f"   Line: {row['previous_line_value']} → {row['line_value']} (Δ{row['line_value_change']})"
            )
            print(
                f"   Odds: {row['previous_odds']} → {row['odds']} (Δ{row['odds_change']})"
            )
            print()

        # Test 3: Sharp movement detection
        print("📊 Test 3: High-quality sharp movements")
        sharp_movements = await conn.fetch("""
            SELECT 
                external_game_id, sportsbook_name, market_type, side,
                previous_odds, odds, filtered_odds_change,
                movement_quality_score, movement_type
            FROM staging.v_sharp_movements
            ORDER BY ABS(filtered_odds_change) DESC
            LIMIT 5
        """)

        for row in sharp_movements:
            print(
                f"   {row['external_game_id']} | {row['sportsbook_name']} | {row['market_type']} {row['side']}"
            )
            print(
                f"   {row['previous_odds']} → {row['odds']} (Δ{row['filtered_odds_change']})"
            )
            print(
                f"   Quality: {row['movement_quality_score']} | Type: {row['movement_type']}"
            )
            print()

        # Test 4: Movement type distribution
        print("📊 Test 4: Movement type distribution")
        distribution = await conn.fetch("""
            SELECT 
                movement_type,
                COUNT(*) as count,
                ROUND(AVG(movement_quality_score), 3) as avg_quality,
                ROUND(AVG(ABS(COALESCE(odds_change, 0))), 1) as avg_movement
            FROM staging.v_line_movements
            WHERE previous_odds IS NOT NULL
            GROUP BY movement_type
            ORDER BY count DESC
        """)

        for row in distribution:
            print(
                f"   {row['movement_type']:<20}: {row['count']:>6} movements | Quality: {row['avg_quality']} | Avg Movement: {row['avg_movement']}"
            )

        # Test 5: Cross-zero American odds examples
        print("\n📊 Test 5: Cross-zero American odds examples")
        cross_zero = await conn.fetch("""
            SELECT 
                external_game_id, sportsbook_name, side,
                previous_odds, odds,
                odds_change_raw, odds_change,
                movement_type
            FROM staging.v_line_movements 
            WHERE previous_odds IS NOT NULL
              AND ((previous_odds < 0 AND odds > 0) OR (previous_odds > 0 AND odds < 0))
              AND ABS(previous_odds) BETWEEN 100 AND 120
              AND ABS(odds) BETWEEN 100 AND 120
            ORDER BY ABS(odds_change_raw - odds_change) DESC
            LIMIT 3
        """)

        for row in cross_zero:
            print(
                f"   {row['external_game_id']} | {row['sportsbook_name']} | {row['side']}"
            )
            print(f"   {row['previous_odds']} → {row['odds']}")
            print(f"   Raw: {row['odds_change_raw']}, Corrected: {row['odds_change']}")
            print(f"   Type: {row['movement_type']}")
            print()

        await conn.close()
        print("✅ Validation completed successfully!")

        return True

    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False


async def main():
    """Main function."""
    success = await validate_improvements()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())
