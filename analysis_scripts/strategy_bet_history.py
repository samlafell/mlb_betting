#!/usr/bin/env python3
"""
Strategy Bet History Viewer
============================

Shows the individual bets that contributed to a specific strategy's performance.
Useful for understanding what triggers each strategy and reviewing outcomes.

Usage:
    uv run analysis_scripts/strategy_bet_history.py --strategy "VSIN-DK opposing_markets"
    uv run analysis_scripts/strategy_bet_history.py --list-strategies
"""

import argparse
import sys

import pytz

# Add src to path for imports
sys.path.insert(0, "src")

from mlb_sharp_betting.services.database_coordinator import get_database_coordinator


class StrategyBetHistoryViewer:
    """View individual bets for specific strategies"""

    def __init__(self):
        self.coordinator = get_database_coordinator()
        self.est = pytz.timezone("US/Eastern")

    def list_available_strategies(self):
        """List all strategies with their performance metrics"""
        print("📊 AVAILABLE STRATEGIES")
        print("=" * 80)

        query = """
        SELECT 
            strategy_name,
            source_book_type,
            split_type,
            total_bets,
            win_rate * 100 as win_rate_pct,
            roi_per_100,
            backtest_date
        FROM backtesting.strategy_performance 
        WHERE backtest_date = (SELECT MAX(backtest_date) FROM backtesting.strategy_performance)
          AND total_bets >= 10
        ORDER BY roi_per_100 DESC, total_bets DESC
        """

        try:
            results = self.coordinator.execute_read(query)

            if not results:
                print("❌ No strategies found in backtesting results")
                return

            for i, row in enumerate(results, 1):
                (
                    strategy_name,
                    source_book,
                    split_type,
                    total_bets,
                    win_rate,
                    roi,
                    backtest_date,
                ) = row

                # Color code based on performance
                if roi >= 15.0:
                    icon = "🔥"
                elif roi >= 10.0:
                    icon = "⭐"
                elif roi >= 5.0:
                    icon = "✅"
                else:
                    icon = "⚠️"

                print(f"{i:2d}. {icon} {strategy_name}")
                print(f"    📍 {source_book} • {split_type}")
                print(
                    f"    📊 {total_bets} bets | {win_rate:.1f}% WR | {roi:+.1f}% ROI"
                )
                print(f"    📅 Backtest: {backtest_date}")
                print()

        except Exception as e:
            print(f"❌ Error listing strategies: {e}")

    def show_strategy_bets(self, strategy_pattern: str):
        """Show individual bets for a specific strategy"""
        print(f"🔍 STRATEGY BET HISTORY: {strategy_pattern}")
        print("=" * 80)

        # First, find the exact strategy name
        strategy_query = """
        SELECT 
            strategy_name,
            source_book_type,
            split_type,
            total_bets,
            win_rate * 100 as win_rate_pct,
            roi_per_100,
            backtest_date
        FROM backtesting.strategy_performance 
        WHERE backtest_date = (SELECT MAX(backtest_date) FROM backtesting.strategy_performance)
          AND strategy_name LIKE ?
        ORDER BY roi_per_100 DESC
        LIMIT 1
        """

        try:
            strategy_results = self.coordinator.execute_read(
                strategy_query, (f"%{strategy_pattern}%",)
            )

            if not strategy_results:
                print(f"❌ No strategy found matching: {strategy_pattern}")
                print("\n💡 Use --list-strategies to see available strategies")
                return

            (
                strategy_name,
                source_book,
                split_type,
                total_bets,
                win_rate,
                roi,
                backtest_date,
            ) = strategy_results[0]

            print("📊 STRATEGY OVERVIEW")
            print(f"   Name: {strategy_name}")
            print(f"   Source: {source_book}")
            print(f"   Type: {split_type}")
            print(
                f"   Performance: {total_bets} bets | {win_rate:.1f}% WR | {roi:+.1f}% ROI"
            )
            print(f"   Backtest Date: {backtest_date}")
            print()

            # Now get the individual bets from threshold_recommendations
            bets_query = """
            SELECT 
                tr.game_id,
                tr.home_team,
                tr.away_team,
                tr.game_datetime,
                tr.recommendation,
                tr.split_type,
                tr.line_value,
                tr.stake_percentage,
                tr.bet_percentage,
                tr.differential,
                tr.outcome,
                tr.created_at,
                tr.strategy_name
            FROM backtesting.threshold_recommendations tr
            WHERE tr.strategy_name = ?
            ORDER BY tr.game_datetime DESC
            """

            bet_results = self.coordinator.execute_read(bets_query, (strategy_name,))

            if not bet_results:
                print("❌ No individual bet records found for this strategy")
                print(f"   Strategy: {strategy_name}")

                # Try alternative approach - look for any recommendations from this strategy
                alt_query = """
                SELECT COUNT(*) FROM backtesting.threshold_recommendations 
                WHERE strategy_name LIKE ?
                """
                alt_results = self.coordinator.execute_read(
                    alt_query, (f"%{strategy_pattern}%",)
                )
                if alt_results and alt_results[0][0] > 0:
                    print(
                        f"   Found {alt_results[0][0]} total recommendations for similar strategy names"
                    )

                return

            print(f"🎯 INDIVIDUAL BETS ({len(bet_results)} total)")
            print("-" * 80)

            wins = 0
            losses = 0
            total_profit = 0.0

            for i, bet in enumerate(bet_results, 1):
                (
                    game_id,
                    home,
                    away,
                    game_dt,
                    recommended_bet,
                    bet_type,
                    line_value,
                    stake_pct,
                    bet_pct,
                    differential,
                    outcome,
                    created_at,
                    strategy,
                ) = bet

                # Format game time
                if game_dt.tzinfo is None:
                    game_dt_est = self.est.localize(game_dt)
                else:
                    game_dt_est = game_dt.astimezone(self.est)

                # Outcome formatting
                if outcome == "win":
                    outcome_icon = "✅"
                    wins += 1
                elif outcome == "loss":
                    outcome_icon = "❌"
                    losses += 1
                else:
                    outcome_icon = "⏳"

                print(f"{i:2d}. {outcome_icon} {away} @ {home}")
                print(f"    🎯 {recommended_bet} ({bet_type})")
                print(f"    📅 {game_dt_est.strftime('%Y-%m-%d %H:%M')} EST")
                print(
                    f"    📊 {stake_pct:.1f}% money vs {bet_pct:.1f}% bets = {differential:+.1f}%"
                )
                print(f"    💰 Line: {line_value}")
                print(f"    🔧 Strategy: {strategy}")
                print()

                # Show only first 20 bets by default
                if i >= 20:
                    remaining = len(bet_results) - 20
                    if remaining > 0:
                        print(f"... and {remaining} more bets")
                        print(f"💡 Use --limit {len(bet_results)} to see all bets")
                    break

            # Summary
            print("-" * 80)
            print("📊 SUMMARY:")
            print(
                f"   Wins: {wins} | Losses: {losses} | Win Rate: {wins / (wins + losses) * 100:.1f}%"
            )
            print(f"   Total bets analyzed: {len(bet_results)}")

        except Exception as e:
            print(f"❌ Error showing strategy bets: {e}")
            import traceback

            traceback.print_exc()

    def show_recent_opportunities(self, strategy_pattern: str, hours: int = 24):
        """Show recent opportunities that would have triggered this strategy"""
        print(f"🕐 RECENT OPPORTUNITIES: {strategy_pattern} (Last {hours}h)")
        print("=" * 80)

        # Get the strategy details
        strategy_query = """
        SELECT 
            strategy_name,
            source_book_type,
            split_type
        FROM backtesting.strategy_performance 
        WHERE backtest_date = (SELECT MAX(backtest_date) FROM backtesting.strategy_performance)
          AND strategy_name LIKE ?
        LIMIT 1
        """

        try:
            strategy_results = self.coordinator.execute_read(
                strategy_query, (f"%{strategy_pattern}%",)
            )

            if not strategy_results:
                print(f"❌ No strategy found matching: {strategy_pattern}")
                return

            strategy_name, source_book, split_type = strategy_results[0]

            # Parse source and book from source_book string
            source = source_book.split("-")[0] if "-" in source_book else source_book
            book = source_book.split("-")[1] if "-" in source_book else None

            print(f"Strategy: {strategy_name}")
            print(f"Looking for: {source} data, {split_type} splits")
            print()

            # Find recent data that would match this strategy
            recent_query = """
            SELECT 
                home_team, away_team, game_datetime, split_type,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                ABS(home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, book, last_updated
            FROM splits.raw_mlb_betting_splits
            WHERE last_updated > NOW() - INTERVAL ? HOUR
              AND source LIKE ?
              AND split_type = ?
              AND ABS(home_or_over_stake_percentage - home_or_over_bets_percentage) >= 10.0
            ORDER BY differential DESC, last_updated DESC
            LIMIT 10
            """

            book_filter = f"%{book}%" if book else "%"
            recent_results = self.coordinator.execute_read(
                recent_query, (hours, f"%{source}%", split_type)
            )

            if not recent_results:
                print("❌ No recent opportunities found for this strategy")
                return

            print(f"🎯 RECENT MATCHES ({len(recent_results)} found):")

            for i, row in enumerate(recent_results, 1):
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

                if game_dt.tzinfo is None:
                    game_dt_est = self.est.localize(game_dt)
                else:
                    game_dt_est = game_dt.astimezone(self.est)

                print(f"{i:2d}. {away} @ {home} - {split_type.upper()}")
                print(f"    📅 Game: {game_dt_est.strftime('%Y-%m-%d %H:%M')} EST")
                print(
                    f"    📊 {stake_pct:.1f}% money vs {bet_pct:.1f}% bets = {diff:+.1f}% diff"
                )
                print(f"    📍 {source}-{book}")
                print(f"    🕐 Updated: {updated}")
                print()

        except Exception as e:
            print(f"❌ Error showing recent opportunities: {e}")


def main():
    parser = argparse.ArgumentParser(description="Strategy Bet History Viewer")
    parser.add_argument(
        "--strategy", "-s", help="Strategy name to analyze (partial match)"
    )
    parser.add_argument(
        "--list-strategies",
        "-l",
        action="store_true",
        help="List all available strategies",
    )
    parser.add_argument(
        "--recent",
        "-r",
        action="store_true",
        help="Show recent opportunities for the strategy",
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Hours to look back for recent opportunities (default: 24)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum number of bets to show (default: 20)",
    )

    args = parser.parse_args()

    viewer = StrategyBetHistoryViewer()

    if args.list_strategies:
        viewer.list_available_strategies()
    elif args.strategy:
        if args.recent:
            viewer.show_recent_opportunities(args.strategy, args.hours)
        else:
            viewer.show_strategy_bets(args.strategy)
    else:
        print("❌ Please specify --strategy or --list-strategies")
        print("\nExamples:")
        print("  uv run analysis_scripts/strategy_bet_history.py --list-strategies")
        print(
            "  uv run analysis_scripts/strategy_bet_history.py --strategy 'VSIN-DK opposing_markets'"
        )
        print(
            "  uv run analysis_scripts/strategy_bet_history.py --strategy 'VSIN-DK' --recent"
        )


if __name__ == "__main__":
    main()
