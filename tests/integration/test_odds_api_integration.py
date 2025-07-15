#!/usr/bin/env python3
"""
Test script for The Odds API integration.
Demonstrates usage tracking and smart budget management.
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from mlb_sharp_betting.services.odds_api_service import OddsAPIService, OddsData


def main():
    """Test The Odds API integration."""
    print("🏁 Testing The Odds API Integration\n")

    # Initialize service
    try:
        odds_service = OddsAPIService()
        print("✅ Odds API service initialized successfully")
    except ValueError as e:
        print(f"❌ Failed to initialize service: {e}")
        print("Make sure ODDS_API_KEY is set in your .env file")
        return

    # Check current usage status
    usage_status = odds_service.get_usage_status()
    print("\n📊 Current Usage Status:")
    print(f"   Month: {usage_status['month']}")
    print(
        f"   Used: {usage_status['used']}/{usage_status['used'] + usage_status['remaining']} credits"
    )
    print(f"   Remaining: {usage_status['remaining']} credits")
    print(f"   Usage: {usage_status['percentage_used']:.1f}%")

    # Test budget optimization
    games_needed = 5
    recommended_config = odds_service.optimize_for_budget(games_needed)
    print("\n🎯 Budget Optimization:")
    print(f"   For {games_needed} games, recommended config: '{recommended_config}'")

    cost_estimates = {"essential": 10, "standard": 30, "comprehensive": 40}

    print("\n💰 Cost Breakdown:")
    for config, cost in cost_estimates.items():
        max_games = usage_status["remaining"] // cost
        print(f"   {config.capitalize()}: {cost} credits/game → max {max_games} games")

    # Test getting today's games (free)
    print("\n🎮 Getting today's MLB games (free call)...")
    games = odds_service.get_today_games()

    if games:
        print(f"   Found {len(games)} games today:")
        for game in games[:3]:  # Show first 3
            print(f"   • {game['away_team']} @ {game['home_team']}")
            print(f"     Start: {game['commence_time']}")
    else:
        print("   No games found or API error")

    # Ask user if they want to make a paid call
    if usage_status["remaining"] >= 10:
        response = input("\n❓ Make a test odds call? (costs 10+ credits) [y/N]: ")

        if response.lower() == "y":
            print(f"\n📈 Fetching odds with '{recommended_config}' configuration...")

            odds_data = odds_service.get_mlb_odds(
                market_config=recommended_config, regions="us", odds_format="american"
            )

            if odds_data:
                print(f"   ✅ Successfully retrieved odds for {len(odds_data)} games")

                # Show detailed odds for first game
                if odds_data:
                    game = odds_data[0]
                    odds_obj = OddsData.from_odds_api(game)

                    print(
                        f"\n🎲 Sample Game: {odds_obj.away_team} @ {odds_obj.home_team}"
                    )

                    # Show moneyline odds
                    ml_odds = odds_obj.get_moneyline_odds()
                    if ml_odds:
                        print(f"   Moneyline ({ml_odds['bookmaker']}):")
                        for outcome in ml_odds["outcomes"]:
                            print(f"     {outcome['name']}: {outcome['price']}")

                    # Show spread odds if available
                    spread_odds = odds_obj.get_spread_odds()
                    if spread_odds:
                        print(f"   Spread ({spread_odds['bookmaker']}):")
                        for outcome in spread_odds["outcomes"]:
                            point = outcome.get("point", "N/A")
                            print(f"     {outcome['name']} {point}: {outcome['price']}")

                    # Show totals if available
                    total_odds = odds_obj.get_total_odds()
                    if total_odds:
                        print(f"   Totals ({total_odds['bookmaker']}):")
                        for outcome in total_odds["outcomes"]:
                            point = outcome.get("point", "N/A")
                            print(f"     {outcome['name']} {point}: {outcome['price']}")
            else:
                print("   ❌ Failed to retrieve odds")

            # Show updated usage
            updated_status = odds_service.get_usage_status()
            print("\n📊 Updated Usage:")
            print(f"   Used: {updated_status['used']} credits")
            print(f"   Remaining: {updated_status['remaining']} credits")
    else:
        print(
            f"\n⚠️  Insufficient credits remaining ({usage_status['remaining']}) for test call"
        )

    print("\n🎯 Integration Tips:")
    print("   • Use 'essential' config (moneyline only) for maximum games")
    print("   • Use 'standard' config for full betting markets")
    print("   • Monitor usage with odds_service.get_usage_status()")
    print("   • Budget automatically resets each month")
    print("   • Free game list endpoint to check schedules")


if __name__ == "__main__":
    main()
