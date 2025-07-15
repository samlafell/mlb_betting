#!/usr/bin/env python3
"""
Test script to demonstrate the new team and datetime columns in betting tables.

This script shows how you can now easily query betting data without needing
to join to the games table, since home_team, away_team, and game_datetime
are now directly available in all betting tables.
"""

import asyncio

import asyncpg


async def test_betting_tables_with_teams():
    """Test the new team and datetime columns in betting tables."""

    # Connect to database
    conn = await asyncpg.connect(
        host="localhost", port=5432, database="mlb_betting", user="samlafell"
    )

    try:
        print("🏀 Testing New Team and DateTime Columns in Betting Tables")
        print("=" * 60)

        # Test 1: Query totals with team info (no joins needed!)
        print("\n1. 📊 Recent Totals with Team Information:")
        print("-" * 45)

        totals_query = """
            SELECT 
                home_team,
                away_team,
                game_datetime,
                sportsbook,
                total_line,
                over_price,
                under_price
            FROM core_betting.betting_lines_totals 
            WHERE home_team IS NOT NULL 
            ORDER BY game_datetime DESC 
            LIMIT 5;
        """

        totals_results = await conn.fetch(totals_query)
        for row in totals_results:
            print(
                f"   {row['away_team']} @ {row['home_team']} | "
                f"{row['game_datetime'].strftime('%Y-%m-%d %H:%M')} | "
                f"{row['sportsbook']} | Total: {row['total_line']} | "
                f"O/U: {row['over_price']}/{row['under_price']}"
            )

        # Test 2: Query moneyline with team info
        print("\n2. 💰 Recent Moneyline with Team Information:")
        print("-" * 45)

        moneyline_query = """
            SELECT 
                home_team,
                away_team,
                game_datetime,
                sportsbook,
                home_ml,
                away_ml
            FROM core_betting.betting_lines_moneyline 
            WHERE home_team IS NOT NULL 
            ORDER BY game_datetime DESC 
            LIMIT 5;
        """

        moneyline_results = await conn.fetch(moneyline_query)
        for row in moneyline_results:
            print(
                f"   {row['away_team']} @ {row['home_team']} | "
                f"{row['game_datetime'].strftime('%Y-%m-%d %H:%M')} | "
                f"{row['sportsbook']} | ML: {row['away_ml']}/{row['home_ml']}"
            )

        # Test 3: Query spreads with team info
        print("\n3. 📈 Recent Spreads with Team Information:")
        print("-" * 45)

        spreads_query = """
            SELECT 
                home_team,
                away_team,
                game_datetime,
                sportsbook,
                home_spread,
                away_spread,
                home_spread_price,
                away_spread_price
            FROM core_betting.betting_lines_spreads 
            WHERE home_team IS NOT NULL 
            ORDER BY game_datetime DESC 
            LIMIT 5;
        """

        spreads_results = await conn.fetch(spreads_query)
        for row in spreads_results:
            print(
                f"   {row['away_team']} @ {row['home_team']} | "
                f"{row['game_datetime'].strftime('%Y-%m-%d %H:%M')} | "
                f"{row['sportsbook']} | Spread: {row['away_spread']}/{row['home_spread']} | "
                f"Prices: {row['away_spread_price']}/{row['home_spread_price']}"
            )

        # Test 4: Demonstrate easy filtering by team
        print("\n4. 🎯 Easy Team-Based Filtering (Example: ATL games):")
        print("-" * 50)

        team_filter_query = """
            SELECT 
                home_team,
                away_team,
                game_datetime,
                sportsbook,
                total_line,
                over_price,
                under_price
            FROM core_betting.betting_lines_totals 
            WHERE (home_team = 'ATL' OR away_team = 'ATL')
            AND home_team IS NOT NULL
            ORDER BY game_datetime DESC 
            LIMIT 5;
        """

        team_results = await conn.fetch(team_filter_query)
        for row in team_results:
            print(
                f"   {row['away_team']} @ {row['home_team']} | "
                f"{row['game_datetime'].strftime('%Y-%m-%d %H:%M')} | "
                f"{row['sportsbook']} | Total: {row['total_line']}"
            )

        # Test 5: Show data counts
        print("\n5. 📊 Data Counts Summary:")
        print("-" * 25)

        count_query = """
            SELECT 
                'betting_lines_totals' as table_name,
                COUNT(*) as total_records,
                COUNT(game_datetime) as with_datetime,
                COUNT(home_team) as with_home_team,
                COUNT(away_team) as with_away_team
            FROM core_betting.betting_lines_totals
            
            UNION ALL
            
            SELECT 
                'betting_lines_moneyline' as table_name,
                COUNT(*) as total_records,
                COUNT(game_datetime) as with_datetime,
                COUNT(home_team) as with_home_team,
                COUNT(away_team) as with_away_team
            FROM core_betting.betting_lines_moneyline
            
            UNION ALL
            
            SELECT 
                'betting_lines_spreads' as table_name,
                COUNT(*) as total_records,
                COUNT(game_datetime) as with_datetime,
                COUNT(home_team) as with_home_team,
                COUNT(away_team) as with_away_team
            FROM core_betting.betting_lines_spreads;
        """

        count_results = await conn.fetch(count_query)
        for row in count_results:
            print(
                f"   {row['table_name']}: {row['total_records']} total, "
                f"{row['with_datetime']} with datetime, "
                f"{row['with_home_team']} with home team, "
                f"{row['with_away_team']} with away team"
            )

        # Test 6: Show the power of the new structure
        print("\n6. 🚀 Power Query Example - Recent Games by Date:")
        print("-" * 50)

        power_query = """
            SELECT 
                DATE(game_datetime) as game_date,
                COUNT(DISTINCT CONCAT(home_team, '-', away_team)) as unique_games,
                COUNT(*) as total_betting_records,
                STRING_AGG(DISTINCT sportsbook, ', ') as sportsbooks
            FROM core_betting.betting_lines_totals 
            WHERE home_team IS NOT NULL 
            AND game_datetime >= NOW() - INTERVAL '7 days'
            GROUP BY DATE(game_datetime)
            ORDER BY game_date DESC;
        """

        power_results = await conn.fetch(power_query)
        for row in power_results:
            print(
                f"   {row['game_date']}: {row['unique_games']} games, "
                f"{row['total_betting_records']} records, "
                f"Books: {row['sportsbooks']}"
            )

        print("\n✅ All tests completed successfully!")
        print("\n🎉 Benefits of the new structure:")
        print("   • No more JOINs needed to get team names and game times")
        print("   • Faster queries for team-based filtering")
        print("   • More intuitive data exploration")
        print("   • Better performance for reporting and analytics")

    finally:
        await conn.close()


async def demonstrate_odds_data_model():
    """Demonstrate the updated OddsData model with team fields."""

    print("\n" + "=" * 60)
    print("📋 Updated OddsData Model Example")
    print("=" * 60)

    # Import the updated model
    try:
        from datetime import datetime

        from sportsbookreview.models.base import BetType, SportsbookName
        from sportsbookreview.models.odds_data import (
            LineMovementData,
            MarketSide,
            OddsData,
            OddsSnapshot,
        )

        # Create example odds data with team information
        odds_data = OddsData(
            game_id="sbr-2025-07-09-ATL-NYM-1",
            home_team="ATL",
            away_team="NYM",
            game_datetime=datetime(2025, 7, 9, 18, 0),
            sportsbook=SportsbookName.DRAFTKINGS,
            bet_type=BetType.TOTAL,
            market_side=MarketSide.OVER,
            line_movement=LineMovementData(
                line_value=8.5,
                odds_history=[
                    OddsSnapshot(
                        american_odds=-110,
                        decimal_odds=1.91,
                        implied_probability=0.524,
                        timestamp=datetime(2025, 7, 9, 10, 0),
                    )
                ],
            ),
        )

        print(f"✅ Created OddsData with teams: {odds_data.matchup_description}")
        print(f"   Game ID: {odds_data.game_id}")
        print(f"   Teams: {odds_data.away_team} @ {odds_data.home_team}")
        print(f"   Game Time: {odds_data.game_datetime}")
        print(f"   Sportsbook: {odds_data.sportsbook}")
        print(f"   Bet Type: {odds_data.bet_type}")
        print(f"   Market Side: {odds_data.market_side}")

    except ImportError as e:
        print(f"❌ Could not import updated model: {e}")
        print("   Make sure the model updates are in place")


if __name__ == "__main__":
    print("🏀 MLB Sharp Betting - Enhanced Tables Test")
    print("=" * 50)

    # Run the database tests
    asyncio.run(test_betting_tables_with_teams())

    # Run the model demonstration
    asyncio.run(demonstrate_odds_data_model())

    print("\n🎯 Summary:")
    print("Your betting tables now include home_team, away_team, and game_datetime")
    print("columns for easier querying and analysis without needing JOINs!")
