#!/usr/bin/env python3
"""
Test script for the Game Outcome Service integration.

This script demonstrates how the game outcome service integrates with
the Action Network flow to automatically check for completed games.

General Balls
"""

import asyncio
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.services.game_outcome_service import check_game_outcomes, game_outcome_service


async def test_game_outcome_service():
    """Test the game outcome service functionality."""

    print("🏁 Testing Game Outcome Service Integration")
    print("=" * 60)

    # Test 1: Check recent outcomes
    print("\n📊 Test 1: Checking recent outcomes (last 3 days)")
    try:
        results = await check_game_outcomes()
        print(f"✅ Results: {results}")

        if results["updated_outcomes"] > 0:
            print(
                f"🎯 Successfully updated {results['updated_outcomes']} game outcomes!"
            )
        else:
            print("ℹ️  No new completed games found (this is normal)")

    except Exception as e:
        print(f"❌ Error: {e}")

    # Test 2: Get recent outcomes for display
    print("\n📋 Test 2: Getting recent outcomes for display")
    try:
        recent_outcomes = await game_outcome_service.get_recent_outcomes(days=7)
        print(f"✅ Found {len(recent_outcomes)} recent outcomes")

        if recent_outcomes:
            print("\n🏆 Recent Game Results:")
            for outcome in recent_outcomes[:3]:  # Show first 3
                print(
                    f"   {outcome['away_team']} {outcome['away_score']} - {outcome['home_score']} {outcome['home_team']}"
                )
                print(
                    f"   Winner: {outcome['home_team'] if outcome['home_win'] else outcome['away_team']}"
                )
                if outcome["over"] is not None:
                    print(
                        f"   Total: {'Over' if outcome['over'] else 'Under'} {outcome['total_line']}"
                    )
                print()

    except Exception as e:
        print(f"❌ Error: {e}")

    # Test 3: Demonstrate Action Network integration
    print("\n🚀 Test 3: Simulating Action Network integration")
    print("This is how the service integrates with the Action Network flow:")
    print("1. Action Network extracts game URLs")
    print("2. Game Outcome Service checks for completed games")
    print("3. Updates are made to core_betting.game_outcomes table")
    print("4. Results are displayed to user")

    print("\n✅ Integration test completed!")
    print("\nTo run the full Action Network pipeline with game outcome checking:")
    print("   uv run python action_network_quickstart.py")
    print("\nTo use the CLI commands:")
    print("   uv run python -m src.interfaces.cli outcomes check --days 7")
    print("   uv run python -m src.interfaces.cli outcomes recent --days 7")


if __name__ == "__main__":
    asyncio.run(test_game_outcome_service())
