#!/usr/bin/env python3
"""
Total Market Flip Strategy Backtest
Validates profitability of total market flips by source and book before recommending them
"""

import sys

sys.path.insert(0, "../src")

from mlb_sharp_betting.db.connection import get_db_manager


def main():
    db_manager = get_db_manager()

    try:
        print("🎯 TOTAL MARKET FLIP STRATEGY - BACKTESTING BY SOURCE/BOOK")
        print("=" * 70)

        # Use the exact same query structure that found VSIN-Circa data earlier
        query = """
        WITH early_signals AS (
            SELECT 
                s.game_id,
                s.home_team,
                s.away_team,
                s.source,
                s.book,
                s.split_type,
                CASE WHEN s.home_or_over_stake_percentage > s.home_or_over_bets_percentage 
                     THEN 'OVER' ELSE 'UNDER' END as recommended_team,
                s.home_or_over_stake_percentage - s.home_or_over_bets_percentage as differential,
                s.last_updated,
                EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 as hours_before_game,
                ROW_NUMBER() OVER (
                    PARTITION BY s.game_id, s.source, COALESCE(s.book, 'NULL')
                    ORDER BY ABS(s.home_or_over_stake_percentage - s.home_or_over_bets_percentage) DESC,
                             EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 DESC
                ) as rn
            FROM splits.raw_mlb_betting_splits s
            WHERE s.split_type = 'total'  -- Only total bets
            AND ABS(s.home_or_over_stake_percentage - s.home_or_over_bets_percentage) >= 8
            AND EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 >= 4
            AND s.game_datetime >= '2025-06-01'
        ),
        late_signals AS (
            SELECT 
                s.game_id,
                s.source,
                s.book,
                s.split_type,
                CASE WHEN s.home_or_over_stake_percentage > s.home_or_over_bets_percentage 
                     THEN 'OVER' ELSE 'UNDER' END as recommended_team,
                s.home_or_over_stake_percentage - s.home_or_over_bets_percentage as differential,
                s.last_updated,
                EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 as hours_before_game,
                ROW_NUMBER() OVER (
                    PARTITION BY s.game_id, s.source, COALESCE(s.book, 'NULL')
                    ORDER BY ABS(s.home_or_over_stake_percentage - s.home_or_over_bets_percentage) DESC,
                             EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 ASC
                ) as rn
            FROM splits.raw_mlb_betting_splits s
            WHERE s.split_type = 'total'  -- Only total bets
            AND ABS(s.home_or_over_stake_percentage - s.home_or_over_bets_percentage) >= 8
            AND EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 BETWEEN 1 AND 3
            AND s.game_datetime >= '2025-06-01'
        ),
        flip_detection AS (
            SELECT DISTINCT
                e.game_id,
                e.home_team,
                e.away_team,
                e.source,
                e.book,
                e.recommended_team as early_team,
                e.differential as early_differential,
                e.hours_before_game as early_hours,
                l.recommended_team as late_team,
                l.differential as late_differential,
                l.hours_before_game as late_hours,
                CASE 
                    WHEN e.recommended_team != l.recommended_team THEN 'TOTAL_MARKET_FLIP'
                    ELSE 'NO_FLIP'
                END as flip_type
            FROM early_signals e
            INNER JOIN late_signals l ON e.game_id = l.game_id 
                                      AND e.source = l.source 
                                      AND COALESCE(e.book, 'NULL') = COALESCE(l.book, 'NULL')
            WHERE e.rn = 1 AND l.rn = 1
            AND e.recommended_team != l.recommended_team  -- Only flips
        ),
        strategy_performance AS (
            SELECT 
                fd.*,
                go.home_score,
                go.away_score,
                go.home_score + go.away_score as total_runs,
                go.total_line,
                go.over as actual_over,
                -- Strategy: Follow early signal
                CASE 
                    WHEN fd.early_team = 'OVER' AND go.over = true THEN 1
                    WHEN fd.early_team = 'UNDER' AND go.over = false THEN 1
                    ELSE 0
                END as strategy_result
            FROM flip_detection fd
            LEFT JOIN game_outcomes go ON fd.game_id = go.game_id
            WHERE go.home_score IS NOT NULL AND go.away_score IS NOT NULL
            AND go.total_line IS NOT NULL
            AND fd.flip_type = 'TOTAL_MARKET_FLIP'
        )
        SELECT 
            source,
            COALESCE(book, 'NULL') as book,
            source || '-' || COALESCE(book, 'NULL') as source_book_combo,
            COUNT(*) as total_bets,
            SUM(strategy_result) as strategy_wins,
            CASE WHEN COUNT(*) > 0 THEN ROUND((SUM(strategy_result)::numeric / COUNT(*)::numeric), 3) ELSE 0 END as win_rate,
            CASE WHEN COUNT(*) > 0 THEN ROUND(((SUM(strategy_result) * 0.909 - (COUNT(*) - SUM(strategy_result))) * 100.0 / COUNT(*))::numeric, 2) ELSE 0 END as roi,
            ROUND(AVG(ABS(early_differential))::numeric, 1) as avg_early_strength,
            ROUND(AVG(ABS(late_differential))::numeric, 1) as avg_late_strength,
            COUNT(*) FILTER (WHERE early_team = 'OVER') as over_early_signals,
            COUNT(*) FILTER (WHERE early_team = 'UNDER') as under_early_signals,
            CASE WHEN COUNT(*) FILTER (WHERE early_team = 'OVER') > 0 
                 THEN ROUND((SUM(strategy_result) FILTER (WHERE early_team = 'OVER')::numeric / COUNT(*) FILTER (WHERE early_team = 'OVER')::numeric), 3) 
                 ELSE 0 END as over_win_rate,
            CASE WHEN COUNT(*) FILTER (WHERE early_team = 'UNDER') > 0 
                 THEN ROUND((SUM(strategy_result) FILTER (WHERE early_team = 'UNDER')::numeric / COUNT(*) FILTER (WHERE early_team = 'UNDER')::numeric), 3) 
                 ELSE 0 END as under_win_rate
        FROM strategy_performance
        GROUP BY source, book
        HAVING COUNT(*) >= 1  -- Lower threshold to see all data
        ORDER BY roi DESC, total_bets DESC;
        """

        results = db_manager.execute_query(query, fetch=True)

        if results:
            print(
                f"🔍 Found {len(results)} source/book combinations with total flip data"
            )
            print()

            profitable_combinations = []
            unprofitable_combinations = []

            for i, row in enumerate(results, 1):
                (
                    source,
                    book,
                    combo,
                    bets,
                    wins,
                    win_rate,
                    roi,
                    early_strength,
                    late_strength,
                    over_signals,
                    under_signals,
                    over_win_rate,
                    under_win_rate,
                ) = row

                if roi > 5:
                    status = "🔥"
                    profitable_combinations.append(combo)
                elif roi > 0:
                    status = "✅"
                    profitable_combinations.append(combo)
                else:
                    status = "❌"
                    unprofitable_combinations.append(combo)

                print(f"{status} #{i}. {combo}")
                print(
                    f"   📊 Performance: {wins}/{bets} ({win_rate:.1%}) | ROI: {roi:.2f}%"
                )
                print(
                    f"   📈 Signal Strength: Early {early_strength:.1f}% vs Late {late_strength:.1f}%"
                )
                print(
                    f"   🎯 Direction: {over_signals} OVER vs {under_signals} UNDER early signals"
                )
                if over_signals > 0 and under_signals > 0:
                    print(
                        f"   📊 OVER Win Rate: {over_win_rate:.1%} | UNDER Win Rate: {under_win_rate:.1%}"
                    )
                print()

            # Summary
            total_profitable = len(profitable_combinations)
            total_unprofitable = len(unprofitable_combinations)

            print("🏆 TOTAL FLIP STRATEGY ASSESSMENT")
            print("=" * 50)
            print(f"✅ Profitable Combinations: {total_profitable}")
            print(f"❌ Unprofitable Combinations: {total_unprofitable}")
            print()

            if profitable_combinations:
                print("✅ APPROVED FOR RECOMMENDATIONS:")
                for combo in profitable_combinations:
                    print(f"   • {combo}")
                print()

            if unprofitable_combinations:
                print("❌ BANNED FROM RECOMMENDATIONS:")
                for combo in unprofitable_combinations:
                    print(f"   • {combo}")
                print()

            # Generate configuration for cross-market detector
            print("⚙️ CONFIGURATION FOR CROSS-MARKET DETECTOR:")
            print("```python")
            print("TOTAL_FLIP_APPROVED_COMBINATIONS = {")
            for combo in profitable_combinations:
                print(f'    "{combo}": True,')
            print("}")
            print()
            print("TOTAL_FLIP_BANNED_COMBINATIONS = {")
            for combo in unprofitable_combinations:
                print(f'    "{combo}": True,')
            print("}")
            print("```")

        else:
            print("❌ No total flip patterns detected in recent data")
            print("💡 This could indicate:")
            print("   • Total markets are more efficient")
            print("   • Signal thresholds may be too strict")
            print("   • Need longer data collection period")

    except Exception as e:
        print(f"❌ Backtest failed: {e}")
        import traceback

        traceback.print_exc()

    finally:
        db_manager.close()


if __name__ == "__main__":
    main()
