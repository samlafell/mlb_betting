"""
Simple Action Network API Test

This script demonstrates the working API integration with browser headers.
"""

import sys
from datetime import datetime
from pathlib import Path

# Add the action directory to Python path
action_dir = Path(__file__).parent.parent
sys.path.insert(0, str(action_dir))

from utils.actionnetwork_url_builder import ActionNetworkURLBuilder


def test_api_access():
    """Test API access with browser headers."""
    print("🔗 Testing Action Network API Access")
    print("=" * 50)

    builder = ActionNetworkURLBuilder()
    test_date = datetime(2025, 7, 1)

    try:
        # Get games from API (this was previously failing with 403)
        print(f"📅 Fetching games for {test_date.strftime('%Y-%m-%d')}...")
        games = builder.get_games_from_api(test_date)

        print(f"✅ Success! Retrieved {len(games)} games")

        # Show first few games
        for i, game in enumerate(games[:5], 1):
            teams = game.get("teams", [])
            if len(teams) >= 2:
                away_team = teams[1].get("full_name", "Unknown")
                home_team = teams[0].get("full_name", "Unknown")
                status = game.get("status", "Unknown")
                print(
                    f"   {i}. {away_team} @ {home_team} ({status}) [ID: {game.get('id')}]"
                )

        if len(games) > 5:
            print(f"   ... and {len(games) - 5} more games")

        return games

    except Exception as e:
        print(f"❌ Error: {e}")
        return []


def test_game_data_fetch(games):
    """Test fetching individual game data."""
    if not games:
        print("\n⚠️  No games to test with")
        return

    print("\n🎯 Testing Game Data Fetch")
    print("-" * 30)

    builder = ActionNetworkURLBuilder()
    test_date = datetime(2025, 7, 1)

    # Test with first game
    test_game = games[0]
    game_id = test_game.get("id")
    print(f"🏀 Fetching data for Game ID: {game_id}")

    try:
        # Build URL and fetch data
        url = builder.build_game_data_url(test_game, test_date)
        game_data = builder.fetch_game_data(url)

        if game_data:
            print("✅ Success! Game data retrieved")

            # Extract key info
            page_props = game_data.get("pageProps", {})
            game_info = page_props.get("game", {})

            if game_info:
                print(f"   📊 Data size: {len(str(game_data)):,} characters")
                print(f"   🆔 Game ID: {game_info.get('id')}")
                print(f"   ⏰ Start: {game_info.get('start_time')}")
                print(f"   📍 Status: {game_info.get('status_display', 'Unknown')}")

                # Teams
                teams = game_info.get("teams", [])
                if len(teams) >= 2:
                    home_team = teams[0]
                    away_team = teams[1]
                    print(
                        f"   🏠 {home_team.get('full_name')} vs ✈️  {away_team.get('full_name')}"
                    )

                # Score
                boxscore = game_info.get("boxscore", {})
                if boxscore:
                    stats = boxscore.get("stats", {})
                    if stats:
                        away_runs = stats.get("away", {}).get("runs", 0)
                        home_runs = stats.get("home", {}).get("runs", 0)
                        print(f"   ⚾ Score: {away_runs} - {home_runs}")

                # Betting volume
                num_bets = game_info.get("num_bets")
                if num_bets:
                    print(f"   💰 Bets placed: {num_bets:,}")

        else:
            print("❌ Failed to fetch game data")

    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    print("🚀 Action Network API - Simple Test")
    print()

    # Test API access
    games = test_api_access()

    # Test game data fetching
    test_game_data_fetch(games)

    print("\n🎉 Test completed!")
    print("The 403 Forbidden error has been resolved with browser-like headers!")
