#!/usr/bin/env python3
"""
Strategy Bet Recreation
========================

Recreates the individual bets that a strategy would have made based on historical data.
This shows you exactly which games triggered each strategy and what the outcomes were.

Usage:
    uv run analysis_scripts/strategy_bet_recreation.py --strategy "opposing_markets_strategy_spread_preference"
"""

import argparse
import sys
from datetime import datetime

import pytz

# Add src to path for imports
sys.path.insert(0, "src")

from mlb_sharp_betting.services.database_coordinator import get_database_coordinator


class StrategyBetRecreator:
    """Recreate individual bets for specific strategies"""

    def __init__(self):
        self.coordinator = get_database_coordinator()
        self.est = pytz.timezone("US/Eastern")

    def recreate_opposing_markets_spread_bets(self):
        """Recreate bets for VSIN-DK opposing markets spread strategy"""
        print("🔍 RECREATING: VSIN-DK Opposing Markets Spread Strategy")
        print("=" * 80)

        # This strategy looks for spread bets where:
        # 1. Source = VSIN, Book = DraftKings
        # 2. Split type = spread
        # 3. Large differential between money % and bets %
        # 4. Generally follow the money (sharp action indicator)

        # Get the FINAL recommendation for each game (latest data BEFORE first pitch)
        # CRITICAL: Only track bets that the Phase 3 Orchestrator would have ACTUALLY recommended
        # Master detector only recommends bets within 20 minutes of first pitch (actionable window)
        query = """
        WITH latest_data_per_game AS (
            SELECT 
                s.home_team, s.away_team, s.game_datetime, s.split_type,
                s.home_or_over_stake_percentage, s.home_or_over_bets_percentage,
                ABS(s.home_or_over_stake_percentage - s.home_or_over_bets_percentage) as differential,
                CASE 
                    WHEN s.home_or_over_stake_percentage > s.home_or_over_bets_percentage 
                    THEN 'Home/Over has more money than bets'
                    ELSE 'Away/Under has more money than bets'
                END as money_pattern,
                s.source, s.book, s.last_updated,
                -- Calculate minutes between data update and game start
                EXTRACT('epoch' FROM (s.game_datetime - s.last_updated)) / 60 as minutes_before_game,
                -- Get the latest data point for each unique game BEFORE game starts
                ROW_NUMBER() OVER (
                    PARTITION BY s.home_team, s.away_team, s.game_datetime, s.split_type 
                    ORDER BY s.last_updated DESC
                ) as rn
            FROM splits.raw_mlb_betting_splits s
            WHERE s.source = 'VSIN'
              AND s.book = 'draftkings'  
              AND s.split_type = 'spread'
              AND s.game_datetime >= '2025-06-18'  -- Match backtesting period
              AND s.last_updated < s.game_datetime  -- CRITICAL: Only data from BEFORE first pitch
              -- MASTER DETECTOR FILTER: Only data within 20 minutes of first pitch
              -- This ensures we only track bets that would have been ACTUALLY recommended
              AND EXTRACT('epoch' FROM (s.game_datetime - s.last_updated)) / 60 <= 20
        )
        SELECT 
            l.home_team, l.away_team, l.game_datetime, l.split_type,
            l.home_or_over_stake_percentage, l.home_or_over_bets_percentage,
            l.differential, l.money_pattern, l.source, l.book, l.last_updated,
            l.minutes_before_game,
            -- Join with game outcomes  
            go.home_score, go.away_score, go.home_win, go.game_date
        FROM latest_data_per_game l
        LEFT JOIN game_outcomes go ON (
            l.home_team = go.home_team 
            AND l.away_team = go.away_team 
            AND DATE(l.game_datetime) = DATE(go.game_date)
        )
        WHERE l.rn = 1  -- Only the latest data for each game
          AND l.differential >= 10.0  -- Only games that would trigger the strategy
        ORDER BY l.game_datetime DESC, l.differential DESC
        """

        try:
            results = self.coordinator.execute_read(query)

            if not results:
                print("❌ No data found for this strategy criteria")
                return

            print(f"📊 FOUND {len(results)} ACTUAL STRATEGY BETS")
            print(
                "   Criteria: Final VSIN-DK spread data per game with >=10% differential"
            )
            print("   🚨 CRITICAL FILTER: Only data within 20 minutes of first pitch")
            print("   💡 This matches the Phase 3 Orchestrator's actionable window")
            print("   Note: One bet per game (latest actionable data only)")
            print()

            wins = 0
            losses = 0
            pending = 0

            for i, row in enumerate(results, 1):
                (
                    home,
                    away,
                    game_dt,
                    split_type,
                    stake_pct,
                    bet_pct,
                    diff,
                    pattern,
                    source,
                    book,
                    updated,
                    minutes_before,
                    home_score,
                    away_score,
                    home_win,
                    game_date,
                ) = row

                # Format game time
                if game_dt.tzinfo is None:
                    game_dt_est = self.est.localize(game_dt)
                else:
                    game_dt_est = game_dt.astimezone(self.est)

                # Determine recommended bet based on money following
                if stake_pct > bet_pct:
                    # More money on home/over than bets suggest
                    recommended = "Home" if split_type == "spread" else "Over"
                    money_side = "Home/Over"
                else:
                    # More money on away/under
                    recommended = "Away" if split_type == "spread" else "Under"
                    money_side = "Away/Under"

                # Determine game outcome and bet result
                outcome_icon, outcome_text, bet_result = self._determine_bet_outcome(
                    game_dt_est, home_score, away_score, home_win, recommended
                )

                # Update result counts
                if bet_result == "WIN":
                    wins += 1
                elif bet_result == "LOSS":
                    losses += 1
                else:
                    pending += 1

                print(f"{i:2d}. {outcome_icon} {away} @ {home}")
                print(f"    🎯 FINAL RECOMMENDATION: {recommended} (follow the money)")
                if outcome_text != "Betting Opportunity":
                    print(f"    🏆 RESULT: {outcome_text}")
                print(f"    📅 Game: {game_dt_est.strftime('%Y-%m-%d %H:%M')} EST")
                print(
                    f"    📊 Money: {stake_pct:.1f}% | Bets: {bet_pct:.1f}% | Diff: {diff:+.1f}%"
                )
                print(f"    💰 {pattern}")
                print(
                    f"    ⏰ ACTIONABLE: Data from {minutes_before:.1f} minutes before first pitch"
                )
                print(
                    f"    📍 {source}-{book} | Updated: {updated.strftime('%m-%d %H:%M')}"
                )
                print()

                # Show only first 25 by default
                if i >= 25:
                    remaining = len(results) - 25
                    if remaining > 0:
                        print(f"... and {remaining} more potential bets")
                        print("💡 This shows the most recent 25 opportunities")
                    break

            # Summary
            print("-" * 80)
            print(f"📊 SUMMARY (of {min(25, len(results))} shown):")
            total_games = wins + losses + pending
            print(f"   📊 Total Actionable Strategy Bets: {total_games}")
            if wins + losses > 0:
                win_rate = wins / (wins + losses) * 100
                print(
                    f"   ✅ Wins: {wins} | ❌ Losses: {losses} | ⏳ Pending: {pending}"
                )
                print(f"   🎯 Win Rate: {win_rate:.1f}% ({wins}/{wins + losses})")
            else:
                print(f"   ⏳ Pending: {pending} (games not yet finished)")
            print(
                "   🚨 ACTIONABLE WINDOW: Only bets within 20 minutes of first pitch"
            )
            print("   💡 This matches the Phase 3 Orchestrator behavior exactly")
            print("   📈 These are bets that would have been ACTUALLY recommended")

            print("\n💡 NOTE: This recreates strategy logic based on available data.")
            print("   Actual backtesting may use different criteria or thresholds.")
            print(
                "   🚨 IMPORTANT: Only shows bets that would be recommended in actionable window"
            )

        except Exception as e:
            print(f"❌ Error recreating strategy bets: {e}")
            import traceback

            traceback.print_exc()

    def _determine_bet_outcome(
        self, game_time_est, home_score, away_score, home_win, recommended
    ):
        """Determine the outcome of a bet based on game results"""
        now_est = datetime.now(self.est)

        # Check if game has started (6+ hours after game time means we should have results)
        hours_since_game = (now_est - game_time_est).total_seconds() / 3600

        if home_score is None or away_score is None or home_win is None:
            if hours_since_game > 6:
                # Game should be finished but we don't have data
                return (
                    "⚠️",
                    f"MISSING DATA (Game was {hours_since_game:.1f}h ago)",
                    "MISSING",
                )
            else:
                # Game not yet finished
                return "⏳", "Game not yet finished", "PENDING"

        # Game has completed results - determine if bet won
        # For spread bets, we simplify by using the game winner as a proxy
        # Real implementation would need the actual spread line and cover calculation

        # Parse the recommendation to see which team was bet on
        if recommended == "Home":
            bet_on_home = True
        elif recommended == "Away":
            bet_on_home = False
        else:
            # Unknown recommendation format
            return "📊", f"FINAL: {home_score}-{away_score}", "COMPLETED"

        # Determine if the bet won (simplified - using game winner as proxy for spread cover)
        if bet_on_home and home_win:
            return "✅", f"WIN: Home won {home_score}-{away_score}", "WIN"
        elif not bet_on_home and not home_win:
            return "✅", f"WIN: Away won {away_score}-{home_score}", "WIN"
        elif bet_on_home and not home_win:
            return "❌", f"LOSS: Home lost {home_score}-{away_score}", "LOSS"
        else:
            return "❌", f"LOSS: Away lost {away_score}-{home_score}", "LOSS"

    def recreate_strategy_bets(self, strategy_name: str):
        """Recreate bets for any strategy by name"""
        print(f"🔍 RECREATING STRATEGY: {strategy_name}")
        print("=" * 80)

        if "opposing_markets_strategy_spread_preference" in strategy_name.lower():
            self.recreate_opposing_markets_spread_bets()
        else:
            print(f"❌ Strategy recreation not yet implemented for: {strategy_name}")
            print("\n🛠️ Currently supported:")
            print("   - opposing_markets_strategy_spread_preference")
            print("\n💡 You can view the raw data that would trigger this strategy:")
            self.show_strategy_criteria(strategy_name)

    def show_strategy_criteria(self, strategy_name: str):
        """Show recent data that would match the strategy criteria"""
        print("\n📋 RECENT DATA MATCHING STRATEGY PATTERN")
        print("-" * 50)

        # Generic query for any strategy data
        query = """
        SELECT 
            home_team, away_team, game_datetime, split_type,
            home_or_over_stake_percentage, home_or_over_bets_percentage,
            ABS(home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
            source, book, last_updated
        FROM splits.raw_mlb_betting_splits
        WHERE last_updated > NOW() - INTERVAL 7 DAY
          AND ABS(home_or_over_stake_percentage - home_or_over_bets_percentage) >= 10.0
        ORDER BY differential DESC, last_updated DESC
        LIMIT 10
        """

        try:
            results = self.coordinator.execute_read(query)

            if not results:
                print("❌ No recent data found with significant differentials")
                return

            for i, row in enumerate(results, 1):
                (
                    home,
                    away,
                    game_dt,
                    split_type,
                    stake_pct,
                    bet_pct,
                    diff,
                    source,
                    book,
                    updated,
                ) = row

                print(f"{i:2d}. {away} @ {home} - {split_type.upper()}")
                print(
                    f"    📊 {stake_pct:.1f}% money vs {bet_pct:.1f}% bets = {diff:+.1f}% diff"
                )
                print(f"    📍 {source}-{book} | {updated.strftime('%m-%d %H:%M')}")
                print()

        except Exception as e:
            print(f"❌ Error showing strategy criteria: {e}")


def main():
    parser = argparse.ArgumentParser(description="Strategy Bet Recreation")
    parser.add_argument(
        "--strategy", "-s", required=True, help="Strategy name to recreate bets for"
    )

    args = parser.parse_args()

    recreator = StrategyBetRecreator()
    recreator.recreate_strategy_bets(args.strategy)


if __name__ == "__main__":
    main()
