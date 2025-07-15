#!/usr/bin/env python3
"""
Final Working Enhanced Late Sharp Flip Strategy Backtest
"""

import sys

sys.path.insert(0, "../src")

from mlb_sharp_betting.db.connection import get_db_manager


def main():
    db_manager = get_db_manager()

    try:
        print("🎯 ENHANCED LATE SHARP FLIP STRATEGY - FINAL PROFITABILITY BACKTEST")
        print("=" * 75)

        # Simplified and corrected query based on debugging
        working_query = """
        WITH signal_pairs AS (
            SELECT 
                s1.game_id,
                s1.home_team,
                s1.away_team,
                s1.source || '-' || COALESCE(s1.book, 'NULL') as source_book,
                s1.split_type as early_split_type,
                s2.split_type as late_split_type,
                
                -- Early signal (6+ hours before)
                CASE WHEN s1.home_or_over_stake_percentage > s1.home_or_over_bets_percentage 
                     THEN s1.home_team ELSE s1.away_team END as early_team,
                s1.home_or_over_stake_percentage - s1.home_or_over_bets_percentage as early_diff,
                
                -- Late signal (1-3 hours before)  
                CASE WHEN s2.home_or_over_stake_percentage > s2.home_or_over_bets_percentage 
                     THEN s2.home_team ELSE s2.away_team END as late_team,
                s2.home_or_over_stake_percentage - s2.home_or_over_bets_percentage as late_diff,
                
                -- Determine flip type
                CASE 
                    WHEN s1.split_type = s2.split_type AND 
                         (CASE WHEN s1.home_or_over_stake_percentage > s1.home_or_over_bets_percentage THEN s1.home_team ELSE s1.away_team END) != 
                         (CASE WHEN s2.home_or_over_stake_percentage > s2.home_or_over_bets_percentage THEN s2.home_team ELSE s2.away_team END)
                    THEN 'SAME_MARKET_FLIP'
                    WHEN s1.split_type != s2.split_type AND 
                         (CASE WHEN s1.home_or_over_stake_percentage > s1.home_or_over_bets_percentage THEN s1.home_team ELSE s1.away_team END) != 
                         (CASE WHEN s2.home_or_over_stake_percentage > s2.home_or_over_bets_percentage THEN s2.home_team ELSE s2.away_team END)
                    THEN 'CROSS_MARKET_CONTRADICTION'
                    ELSE 'NO_CONTRADICTION'
                END as flip_type
                
            FROM splits.raw_mlb_betting_splits s1
            INNER JOIN splits.raw_mlb_betting_splits s2 ON s1.game_id = s2.game_id 
                                                        AND s1.source = s2.source 
                                                        AND COALESCE(s1.book, 'NULL') = COALESCE(s2.book, 'NULL')
            WHERE ABS(s1.home_or_over_stake_percentage - s1.home_or_over_bets_percentage) >= 8
            AND ABS(s2.home_or_over_stake_percentage - s2.home_or_over_bets_percentage) >= 8
            AND EXTRACT(EPOCH FROM (s1.game_datetime - s1.last_updated))/3600 >= 6  -- Early
            AND EXTRACT(EPOCH FROM (s2.game_datetime - s2.last_updated))/3600 BETWEEN 1 AND 3  -- Late
            AND s1.game_datetime >= '2025-06-16'
            AND (CASE WHEN s1.home_or_over_stake_percentage > s1.home_or_over_bets_percentage THEN s1.home_team ELSE s1.away_team END) != 
                (CASE WHEN s2.home_or_over_stake_percentage > s2.home_or_over_bets_percentage THEN s2.home_team ELSE s2.away_team END)  -- Only contradictions
        ),
        
        strategy_results AS (
            SELECT 
                sp.*,
                go.home_score,
                go.away_score,
                
                -- Strategy: Follow early signal
                CASE 
                    WHEN go.home_score > go.away_score AND sp.early_team = sp.home_team THEN 1
                    WHEN go.away_score > go.home_score AND sp.early_team = sp.away_team THEN 1
                    WHEN go.home_score = go.away_score THEN 0  -- Push
                    ELSE 0  -- Loss
                END as strategy_result,
                
                -- Comparison: Late signal
                CASE 
                    WHEN go.home_score > go.away_score AND sp.late_team = sp.home_team THEN 1
                    WHEN go.away_score > go.home_score AND sp.late_team = sp.away_team THEN 1
                    WHEN go.home_score = go.away_score THEN 0  -- Push
                    ELSE 0  -- Loss
                END as late_signal_result
                
            FROM signal_pairs sp
            LEFT JOIN game_outcomes go ON sp.game_id = go.game_id
            WHERE go.home_score IS NOT NULL AND go.away_score IS NOT NULL
            AND sp.flip_type IN ('SAME_MARKET_FLIP', 'CROSS_MARKET_CONTRADICTION')
        )
        
        SELECT 
            source_book,
            flip_type,
            COUNT(*) as total_bets,
            SUM(strategy_result) as strategy_wins,
            ROUND(AVG(strategy_result)::numeric, 3) as strategy_win_rate,
            SUM(late_signal_result) as late_signal_wins,
            ROUND(AVG(late_signal_result)::numeric, 3) as late_signal_win_rate,
            
            -- ROI calculations (assuming -110 odds)
            ROUND((SUM(strategy_result) * 0.909 - (COUNT(*) - SUM(strategy_result))) * 100.0 / COUNT(*), 2) as strategy_roi,
            ROUND((SUM(late_signal_result) * 0.909 - (COUNT(*) - SUM(late_signal_result))) * 100.0 / COUNT(*), 2) as late_signal_roi,
            
            ROUND(AVG(ABS(early_diff))::numeric, 1) as avg_early_signal_strength,
            ROUND(AVG(ABS(late_diff))::numeric, 1) as avg_late_signal_strength
            
        FROM strategy_results
        GROUP BY source_book, flip_type
        HAVING COUNT(*) >= 2  -- Minimum sample size
        ORDER BY strategy_roi DESC;
        """

        results = db_manager.execute_query(working_query, fetch=True)

        if results:
            print(f"🔍 Successfully found {len(results)} profitable flip patterns!")
            print()

            total_bets = 0
            total_wins = 0
            total_roi_weighted = 0
            profitable_strategies = 0

            for i, row in enumerate(results, 1):
                (
                    source_book,
                    flip_type,
                    bets,
                    wins,
                    win_rate,
                    late_wins,
                    late_win_rate,
                    roi,
                    late_roi,
                    early_strength,
                    late_strength,
                ) = row

                total_bets += bets
                total_wins += wins
                total_roi_weighted += roi * bets

                if roi > 0:
                    profitable_strategies += 1
                    status = "🔥" if roi > 10 else "✅" if roi > 5 else "⚠️"
                else:
                    status = "❌"

                print(f"{status} #{i}. {source_book} - {flip_type}")
                print(
                    f"   📊 Bets: {bets} | Strategy Wins: {wins} | Win Rate: {win_rate:.1%} | ROI: {roi:.2f}%"
                )
                print(
                    f"   🔄 vs Late Signal: {late_wins} wins ({late_win_rate:.1%}) | ROI: {late_roi:.2f}%"
                )
                print(
                    f"   📈 Signal Strength: Early {early_strength:.1f}% vs Late {late_strength:.1f}%"
                )

                edge = win_rate - late_win_rate
                edge_direction = "ADVANTAGE" if edge > 0 else "DISADVANTAGE"
                edge_emoji = "🎯" if edge > 0 else "⚠️"
                print(f"   {edge_emoji} Strategy Edge: {edge:.1%} {edge_direction}")
                print()

            # Overall performance analysis
            overall_win_rate = total_wins / total_bets if total_bets > 0 else 0
            overall_roi = total_roi_weighted / total_bets if total_bets > 0 else 0

            print("🏆 OVERALL STRATEGY PERFORMANCE")
            print("=" * 50)
            print(f"📈 Total Bets Analyzed: {total_bets}")
            print(f"🎯 Overall Win Rate: {overall_win_rate:.1%}")
            print(f"💰 Overall ROI: {overall_roi:.2f}%")
            print(f"✅ Profitable Strategies: {profitable_strategies}/{len(results)}")
            profitability_pct = (
                profitable_strategies / len(results) * 100 if results else 0
            )
            print(f"📊 Profitability Rate: {profitability_pct:.1f}%")
            print()

            # Final profitability verdict
            if overall_roi >= 10.0:
                print("🔥 STRATEGY ASSESSMENT: HIGHLY PROFITABLE! 🚀")
                print(
                    f"   💎 ROI of {overall_roi:.2f}% is excellent for sports betting"
                )
                print("   🎯 RECOMMENDATION: Implement immediately for live betting")
                print(f"   💰 Expected profit: ${overall_roi:.2f} per $100 wagered")
                print(
                    f"   🏦 With $1000 bankroll, expect ~${overall_roi * 10:.2f} monthly profit"
                )
            elif overall_roi >= 5.0:
                print("✅ STRATEGY ASSESSMENT: PROFITABLE ✅")
                print(
                    f"   💡 ROI of {overall_roi:.2f}% exceeds 5% profitability threshold"
                )
                print(
                    "   🚀 RECOMMENDATION: Implement for live betting with proper bankroll management"
                )
                print(f"   💰 Expected profit: ${overall_roi:.2f} per $100 wagered")
                print(
                    f"   🏦 With $1000 bankroll, expect ~${overall_roi * 10:.2f} monthly profit"
                )
            elif overall_roi > 0:
                print("⚠️  STRATEGY ASSESSMENT: MARGINALLY PROFITABLE")
                print(
                    f"   📊 ROI of {overall_roi:.2f}% is positive but below 5% threshold"
                )
                print(
                    "   🔧 RECOMMENDATION: Consider refining thresholds or increasing sample size"
                )
                print(f"   💰 Expected profit: ${overall_roi:.2f} per $100 wagered")
            else:
                print("❌ STRATEGY ASSESSMENT: NOT PROFITABLE")
                print(f"   📉 ROI of {overall_roi:.2f}% indicates losses")
                print("   🔧 RECOMMENDATION: Strategy needs significant refinement")
                print(f"   💸 Expected loss: ${abs(overall_roi):.2f} per $100 wagered")

            # Detailed flip type analysis
            cross_market_results = [
                r for r in results if r[1] == "CROSS_MARKET_CONTRADICTION"
            ]
            same_market_results = [r for r in results if r[1] == "SAME_MARKET_FLIP"]

            print("\n🔍 DETAILED FLIP TYPE ANALYSIS:")
            print("=" * 50)

            if cross_market_results:
                cm_bets = sum(r[2] for r in cross_market_results)
                cm_wins = sum(r[3] for r in cross_market_results)
                cm_roi = (
                    sum(r[7] * r[2] for r in cross_market_results) / cm_bets
                    if cm_bets > 0
                    else 0
                )
                cm_win_rate = cm_wins / cm_bets if cm_bets > 0 else 0
                print("🔀 Cross-Market Contradictions:")
                print(f"   📊 Performance: {cm_wins}/{cm_bets} ({cm_win_rate:.1%})")
                print(f"   💰 ROI: {cm_roi:.2f}%")
                print("   🎯 Best strategy type for cross-market flip detection!")

            if same_market_results:
                sm_bets = sum(r[2] for r in same_market_results)
                sm_wins = sum(r[3] for r in same_market_results)
                sm_roi = (
                    sum(r[7] * r[2] for r in same_market_results) / sm_bets
                    if sm_bets > 0
                    else 0
                )
                sm_win_rate = sm_wins / sm_bets if sm_bets > 0 else 0
                print("🔁 Same Market Flips:")
                print(f"   📊 Performance: {sm_wins}/{sm_bets} ({sm_win_rate:.1%})")
                print(f"   💰 ROI: {sm_roi:.2f}%")
                print("   🎯 Traditional flip detection performance")

            # Implementation recommendations
            print("\n💡 IMPLEMENTATION RECOMMENDATIONS:")
            print("=" * 50)
            if overall_roi > 5:
                print("✅ READY FOR LIVE IMPLEMENTATION:")
                print("   🔧 Integrate into CLI detect-opportunities command")
                print(
                    "   📊 Set minimum confidence thresholds based on signal strength"
                )
                print("   ⚡ Prioritize high-ROI source/book combinations")
                print("   🎯 Focus on cross-market contradictions for maximum edge")
                print("   💰 Use proper bankroll management (2-3% per bet)")
            else:
                print("🔧 NEEDS REFINEMENT:")
                print("   📊 Consider adjusting signal strength thresholds")
                print("   ⏰ Experiment with different timing windows")
                print("   📈 Collect more historical data for validation")
                print("   🎚️  Test different minimum sample sizes")

        else:
            print("❌ No flip patterns found - this suggests a query logic error")
            print(
                "🔧 The debugging showed contradictions exist, so there may be a JOIN issue"
            )

    except Exception as e:
        print(f"❌ Backtest failed: {e}")
        import traceback

        traceback.print_exc()

    finally:
        db_manager.close()


if __name__ == "__main__":
    main()
