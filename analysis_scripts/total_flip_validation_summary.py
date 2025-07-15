#!/usr/bin/env python3
"""
Total Flip Validation Summary
Shows which total flip combinations are approved vs banned for detect-opportunities command
"""

import sys

sys.path.insert(0, "../src")

from mlb_sharp_betting.db.connection import get_db_manager


def main():
    print("🎯 TOTAL FLIP VALIDATION SUMMARY")
    print("=" * 60)
    print("📋 Backtested performance for all source/book combinations")
    print()

    manager = get_db_manager()

    try:
        # Run the comprehensive total flip analysis
        query = """
        WITH game_signals AS (
            SELECT  
                s.game_id,
                s.home_team,
                s.away_team,
                s.source,
                s.book,
                CASE WHEN s.home_or_over_stake_percentage > s.home_or_over_bets_percentage 
                     THEN 'OVER' ELSE 'UNDER' END as recommended_team,
                s.home_or_over_stake_percentage - s.home_or_over_bets_percentage as differential,
                EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 as hours_before_game,
                CASE 
                    WHEN EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 >= 4 THEN 'EARLY'
                    WHEN EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 BETWEEN 1 AND 3 THEN 'LATE'
                    ELSE 'OTHER'
                END as timing_category
            FROM splits.raw_mlb_betting_splits s
            WHERE s.split_type = 'total'
            AND ABS(s.home_or_over_stake_percentage - s.home_or_over_bets_percentage) >= 5
            AND s.game_datetime >= '2025-06-16'
        ),
        early_late_pairs AS (
            SELECT 
                e.game_id,
                e.home_team,
                e.away_team,
                e.source,
                e.book,
                e.recommended_team as early_team,
                e.differential as early_diff,
                e.hours_before_game as early_hours,
                l.recommended_team as late_team,
                l.differential as late_diff,
                l.hours_before_game as late_hours
            FROM game_signals e
            INNER JOIN game_signals l ON e.game_id = l.game_id 
                                      AND e.source = l.source 
                                      AND COALESCE(e.book, 'NULL') = COALESCE(l.book, 'NULL')
            WHERE e.timing_category = 'EARLY' 
            AND l.timing_category = 'LATE'
            AND e.recommended_team != l.recommended_team  -- Only flips
        ),
        flip_results AS (
            SELECT 
                fp.*,
                go.home_score,
                go.away_score,
                go.over as actual_over,
                -- Strategy: Follow early signal (fade the late movement)
                CASE 
                    WHEN fp.early_team = 'OVER' AND go.over = true THEN 1
                    WHEN fp.early_team = 'UNDER' AND go.over = false THEN 1
                    ELSE 0
                END as strategy_result
            FROM early_late_pairs fp
            LEFT JOIN game_outcomes go ON fp.game_id = go.game_id
            WHERE go.home_score IS NOT NULL AND go.away_score IS NOT NULL
            AND go.total_line IS NOT NULL
        )
        SELECT 
            source,
            COALESCE(book, 'NULL') as book,
            source || '-' || COALESCE(book, 'NULL') as combo,
            COUNT(*) as total_flips,
            SUM(strategy_result) as wins,
            ROUND((SUM(strategy_result)::numeric / COUNT(*)::numeric), 3) as win_rate,
            ROUND(((SUM(strategy_result) * 0.909 - (COUNT(*) - SUM(strategy_result))) * 100.0 / COUNT(*))::numeric, 2) as roi,
            ROUND(AVG(ABS(early_diff))::numeric, 1) as avg_early_strength,
            ROUND(AVG(ABS(late_diff))::numeric, 1) as avg_late_strength,
            COUNT(*) FILTER (WHERE early_team = 'OVER') as over_early,
            COUNT(*) FILTER (WHERE early_team = 'UNDER') as under_early
        FROM flip_results
        GROUP BY source, book
        ORDER BY total_flips DESC, roi DESC;
        """

        import pandas as pd

        with manager.get_connection() as conn:
            df = pd.read_sql(query, conn)

        if len(df) > 0:
            print("📊 TOTAL FLIP PERFORMANCE ANALYSIS")
            print(f"   Found {len(df)} source/book combinations with historical data")
            print()

            approved_combinations = []
            banned_combinations = []

            for _, row in df.iterrows():
                combo = row["combo"]
                flips = row["total_flips"]
                wins = row["wins"]
                win_rate = row["win_rate"]
                roi = row["roi"]
                early_strength = row["avg_early_strength"]
                late_strength = row["avg_late_strength"]
                over_early = row["over_early"]
                under_early = row["under_early"]

                # Determine status
                if roi > 5:
                    status = "🔥 HIGHLY PROFITABLE"
                    approved_combinations.append(combo)
                elif roi > 0:
                    status = "✅ PROFITABLE"
                    approved_combinations.append(combo)
                else:
                    status = "❌ UNPROFITABLE"
                    banned_combinations.append(combo)

                print(f"{status}: {combo}")
                print(
                    f"   📊 Performance: {wins}/{flips} ({win_rate:.1%}) | ROI: {roi:.2f}%"
                )
                print(
                    f"   📈 Signal Strength: Early {early_strength:.1f}% vs Late {late_strength:.1f}%"
                )
                print(
                    f"   🎯 Direction Bias: {over_early} OVER vs {under_early} UNDER early signals"
                )
                print()

            # Show current detector configuration
            print("⚙️ CROSS-MARKET FLIP DETECTOR CONFIGURATION")
            print("=" * 60)

            # Read current banned combinations from the detector
            from mlb_sharp_betting.services.cross_market_flip_detector import (
                CrossMarketFlipDetector,
            )

            print("✅ APPROVED FOR RECOMMENDATIONS:")
            if approved_combinations:
                for combo in approved_combinations:
                    if (
                        combo
                        not in CrossMarketFlipDetector.TOTAL_FLIP_BANNED_COMBINATIONS
                    ):
                        print(f"   • {combo} ✅ Active")
                    else:
                        print(f"   • {combo} ⚠️  Should be active but banned in code")
            else:
                print("   • None found")
            print()

            print("❌ BANNED FROM RECOMMENDATIONS:")
            if banned_combinations:
                for combo in banned_combinations:
                    if combo in CrossMarketFlipDetector.TOTAL_FLIP_BANNED_COMBINATIONS:
                        print(f"   • {combo} ✅ Correctly banned")
                    else:
                        print(f"   • {combo} ⚠️  Should be banned but not in code")
            else:
                print("   • None found")
            print()

            # Show detect-opportunities command status
            print("🎯 DETECT-OPPORTUNITIES COMMAND STATUS")
            print("=" * 60)
            print(
                "The `uv run python -m mlb_sharp_betting.cli detect-opportunities` command will:"
            )
            print()

            if approved_combinations:
                print("✅ RECOMMEND total flips from:")
                for combo in approved_combinations:
                    if (
                        combo
                        not in CrossMarketFlipDetector.TOTAL_FLIP_BANNED_COMBINATIONS
                    ):
                        print(f"   • {combo}")
                print()

            if banned_combinations:
                print("❌ REJECT total flips from:")
                for combo in banned_combinations:
                    if combo in CrossMarketFlipDetector.TOTAL_FLIP_BANNED_COMBINATIONS:
                        print(
                            f"   • {combo} (backtested performance: {df[df.combo == combo].iloc[0].roi:.1f}% ROI)"
                        )
                print()

            print(
                "🔍 All recommendations are now backed by historical performance data!"
            )

        else:
            print("❌ No total flip data found")
            print(
                "💡 This indicates total markets may be more efficient than other bet types"
            )

    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
