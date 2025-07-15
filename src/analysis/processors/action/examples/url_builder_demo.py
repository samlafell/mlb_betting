"""
Action Network URL Builder Demo

This script demonstrates how to use the Action Network URL builder
to extract build IDs and construct game data URLs dynamically.
"""

import sys
from datetime import datetime
from pathlib import Path

# Add the action directory to Python path
action_dir = Path(__file__).parent.parent
sys.path.insert(0, str(action_dir))

from utils.actionnetwork_build_extractor import (
    ActionNetworkBuildExtractor,
    get_current_build_id_info,
)
from utils.actionnetwork_url_builder import (
    ActionNetworkURLBuilder,
)


def demo_build_id_extraction():
    """Demo: Extract the current Next.js build ID from Action Network."""
    print("🔍 Testing Build ID Extraction")
    print("-" * 40)

    try:
        # Get build ID info
        info = get_current_build_id_info()

        if info["success"]:
            print(f"✅ Build ID: {info['build_id']}")
            print(f"⏰ Extracted at: {datetime.fromtimestamp(info['extracted_at'])}")
            print(f"🔗 Example URL: {info['example_url']}")
            return info["build_id"]
        else:
            print("❌ Build ID extraction failed")
            return None
    except Exception as e:
        print(f"❌ Error during build ID extraction: {e}")
        return None


def demo_url_construction(build_id: str):
    """Demo: Construct URLs for specific games."""
    print("\n🔗 Testing URL Construction")
    print("-" * 40)

    try:
        extractor = ActionNetworkBuildExtractor()

        # Test with known game data
        test_cases = [
            {
                "game_id": "257324",
                "date": "july-1-2025",
                "team_slug": "yankees-blue-jays",
                "description": "Yankees @ Blue Jays",
            },
            {
                "game_id": "257326",
                "date": "july-1-2025",
                "team_slug": "cardinals-pirates",
                "description": "Cardinals @ Pirates",
            },
        ]

        for i, test_case in enumerate(test_cases, 1):
            url = extractor.get_game_data_url(
                game_id=test_case["game_id"],
                date=test_case["date"],
                team_slug=test_case["team_slug"],
                build_id=build_id,
            )
            print(f"🏀 Game {i} ({test_case['description']}):")
            print(f"   URL: {url}")
            print(f"   ✓ Game ID: {test_case['game_id']}")
            print()

    except Exception as e:
        print(f"❌ Error during URL construction: {e}")


def demo_api_integration():
    """Demo: Use the API to get game data and build URLs."""
    print("📡 Testing API Integration (with browser headers)")
    print("-" * 40)

    try:
        builder = ActionNetworkURLBuilder()

        # Test for July 1, 2025 (the date from your examples)
        test_date = datetime(2025, 7, 1)

        print(f"📅 Fetching games for {test_date.strftime('%Y-%m-%d')}")

        # Get games from API
        games = builder.get_games_from_api(test_date)
        print(f"📊 Found {len(games)} games from API")

        if games:
            # Build URLs for first few games
            for i, game in enumerate(games[:3], 1):
                try:
                    url = builder.build_game_data_url(game, test_date)
                    print(f"\n🏀 Game {i}:")
                    print(f"   ID: {game.get('id')}")

                    # Try to extract team names
                    teams = game.get("teams", [])
                    if len(teams) >= 2:
                        away_team = teams[1].get(
                            "full_name", teams[1].get("display_name", "Unknown")
                        )
                        home_team = teams[0].get(
                            "full_name", teams[0].get("display_name", "Unknown")
                        )
                        print(f"   Teams: {away_team} @ {home_team}")

                    print(f"   Status: {game.get('status', 'Unknown')}")
                    print(f"   URL: {url}")

                except Exception as e:
                    print(f"   ❌ Error building URL for game {game.get('id')}: {e}")

            if len(games) > 3:
                print(f"\n... and {len(games) - 3} more games")

            return games  # Return games for further testing
        else:
            print("⚠️  No games found for the specified date")
            return []

    except Exception as e:
        print(f"❌ Error during API integration: {e}")
        return []


def demo_game_data_fetching(games):
    """Demo: Fetch actual game data from constructed URLs."""
    print("\n🎯 Testing Game Data Fetching")
    print("-" * 40)

    if not games:
        print("⚠️  No games available for testing")
        return

    try:
        builder = ActionNetworkURLBuilder()
        test_date = datetime(2025, 7, 1)

        # Test with first game
        test_game = games[0]
        print(f"🏀 Testing with Game ID: {test_game.get('id')}")

        # Build URL
        url = builder.build_game_data_url(test_game, test_date)
        print(f"📎 URL: {url[:80]}...")

        # Fetch actual game data
        print("📥 Fetching game data...")
        game_data = builder.fetch_game_data(url)

        if game_data:
            print("✅ Successfully fetched game data!")

            # Show some key information from the fetched data
            page_props = game_data.get("pageProps", {})
            game_info = page_props.get("game", {})

            if game_info:
                print(f"   🆔 Game ID: {game_info.get('id')}")
                print(f"   📅 Start Time: {game_info.get('start_time')}")
                print(f"   🏟️  Status: {game_info.get('status_display', 'Unknown')}")

                # Team information
                teams = game_info.get("teams", [])
                if len(teams) >= 2:
                    home_team = teams[0]
                    away_team = teams[1]
                    print(
                        f"   🏠 Home: {home_team.get('full_name')} ({home_team.get('abbr')})"
                    )
                    print(
                        f"   ✈️  Away: {away_team.get('full_name')} ({away_team.get('abbr')})"
                    )

                # Score information
                boxscore = game_info.get("boxscore", {})
                if boxscore:
                    stats = boxscore.get("stats", {})
                    if stats:
                        away_stats = stats.get("away", {})
                        home_stats = stats.get("home", {})
                        print(
                            f"   📊 Score: {away_stats.get('runs', 0)} - {home_stats.get('runs', 0)}"
                        )

                # Betting information
                num_bets = game_info.get("num_bets")
                if num_bets:
                    print(f"   💰 Number of Bets: {num_bets:,}")

                print(f"   📊 Data Size: {len(str(game_data))} characters")
            else:
                print("   ⚠️  Game info not found in expected structure")
                print(f"   📊 Raw Data Size: {len(str(game_data))} characters")
        else:
            print("❌ Failed to fetch game data")

    except Exception as e:
        print(f"❌ Error during game data fetching: {e}")


def main():
    """Run the complete demo."""
    print("🎯 Action Network URL Builder Demo")
    print("=" * 60)
    print()

    print("This demo shows how to:")
    print("1. Extract the current Next.js build ID from Action Network")
    print("2. Construct game data URLs manually")
    print(
        "3. Use the API to get games and build URLs automatically (with browser headers)"
    )
    print("4. Fetch actual game data from the constructed URLs")
    print()

    # Demo 1: Build ID extraction
    build_id = demo_build_id_extraction()

    if not build_id:
        print("\n⚠️  Skipping remaining demos due to build ID extraction failure")
        print("This could be due to:")
        print("- Network connectivity issues")
        print("- Chrome browser not installed")
        print("- Action Network website changes")
        return

    # Demo 2: Manual URL construction
    demo_url_construction(build_id)

    # Demo 3: API integration (with browser headers)
    games = demo_api_integration()

    # Demo 4: Fetch actual game data if API worked
    if games:
        demo_game_data_fetching(games)
    else:
        print("\n⚠️  Skipping game data fetching demo (no games from API)")

    print("\n✅ Demo completed!")
    print("\nNext steps:")
    print("- Integrate these utilities into your main betting application")
    print("- Set up caching for build IDs to avoid frequent extraction")
    print("- Add error handling and retry logic for production use")
    print("- The API integration now uses browser-like headers to avoid 403 errors")


if __name__ == "__main__":
    main()
