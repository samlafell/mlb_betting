#!/usr/bin/env python3
"""
Enhanced Late Sharp Flip Strategy Backtesting

Comprehensive backtesting framework for the enhanced late sharp flip strategy
that detects cross-market contradictions and validates profitability.
"""

import asyncio
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta

import structlog

# Add the project root to the path
sys.path.insert(
    0, "/Users/samlafell/Documents/programming_projects/sports_betting_dime_splits/src"
)

from mlb_sharp_betting.db.connection import get_db_manager

logger = structlog.get_logger(__name__)


@dataclass
class FlipDetectionResult:
    """Results from flip detection analysis."""

    game_id: str
    home_team: str
    away_team: str
    game_datetime: datetime
    flip_type: str
    early_signal: str
    late_signal: str
    early_recommended_team: str
    late_recommended_team: str
    early_signal_strength: float
    late_signal_strength: float
    hours_between_signals: float
    confidence_level: str


@dataclass
class StrategyPerformance:
    """Strategy performance metrics."""

    source_book: str
    total_games: int
    cross_market_contradictions: int
    same_market_flips: int
    weak_late_contradictions: int

    # Strategy performance
    strategy_wins: int
    strategy_win_rate: float
    strategy_roi: float

    # Comparison metrics
    late_signal_wins: int
    late_signal_win_rate: float

    # Performance by type
    cross_market_win_rate: float
    same_market_win_rate: float
    weak_contradiction_win_rate: float

    # Confidence-based performance
    high_confidence_win_rate: float
    medium_confidence_win_rate: float

    # Signal analysis
    avg_early_signal_strength: float
    avg_late_signal_strength: float
    avg_hours_between_signals: float

    # Recent examples
    recent_examples: list[str]


class EnhancedLateSharpFlipBacktester:
    """Comprehensive backtesting for enhanced late sharp flip strategy."""

    def __init__(self):
        self.db_manager = get_db_manager()

    def get_postgresql_compatible_query(self) -> str:
        """Get PostgreSQL-compatible version of the enhanced strategy query."""
        return """
        -- ENHANCED Late Sharp Flip Strategy (PostgreSQL Compatible)
        WITH game_timeline_all_markets AS (
            SELECT 
                s.game_id,
                s.home_team,
                s.away_team,
                s.game_datetime,
                s.split_type,
                s.source,
                s.book,
                s.home_or_over_stake_percentage,
                s.home_or_over_bets_percentage,
                (s.home_or_over_stake_percentage - s.home_or_over_bets_percentage) as differential,
                s.last_updated,
                EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 as hours_before_game,
                
                CASE 
                    WHEN EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 >= 6 THEN 'EARLY'
                    WHEN EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 >= 3 THEN 'MEDIUM' 
                    WHEN EXTRACT(EPOCH FROM (s.game_datetime - s.last_updated))/3600 >= 1 THEN 'LATE'
                    ELSE 'VERY_LATE'
                END as time_period,
                
                CASE 
                    WHEN s.split_type = 'moneyline' THEN
                        CASE WHEN s.home_or_over_stake_percentage > s.home_or_over_bets_percentage THEN s.home_team ELSE s.away_team END
                    WHEN s.split_type = 'spread' THEN  
                        CASE WHEN s.home_or_over_stake_percentage > s.home_or_over_bets_percentage THEN s.home_team ELSE s.away_team END
                    WHEN s.split_type = 'total' THEN
                        CASE WHEN s.home_or_over_stake_percentage > s.home_or_over_bets_percentage THEN 'OVER' ELSE 'UNDER' END
                    ELSE NULL
                END as recommended_side
            FROM splits.raw_mlb_betting_splits s
            WHERE ABS(s.home_or_over_stake_percentage - s.home_or_over_bets_percentage) >= 8
            AND s.game_datetime >= %s
            AND s.split_type IN ('moneyline', 'spread', 'total')
        ),
        
        early_late_analysis AS (
            SELECT 
                gt.game_id,
                gt.home_team,
                gt.away_team,
                gt.game_datetime,
                gt.source,
                gt.book,
                
                (ARRAY_AGG(
                    CASE WHEN gt.time_period = 'EARLY' AND ABS(gt.differential) >= 12 
                    THEN gt.split_type || ':' || gt.recommended_side 
                    ELSE NULL END
                    ORDER BY ABS(gt.differential) DESC
                ))[1] as strongest_early_signal,
                
                MAX(CASE WHEN gt.time_period = 'EARLY' THEN ABS(gt.differential) END) as strongest_early_differential,
                
                (ARRAY_AGG(
                    CASE WHEN gt.time_period IN ('LATE', 'VERY_LATE') AND ABS(gt.differential) >= 12
                    THEN gt.split_type || ':' || gt.recommended_side 
                    ELSE NULL END
                    ORDER BY ABS(gt.differential) DESC  
                ))[1] as strongest_late_signal,
                
                MAX(CASE WHEN gt.time_period IN ('LATE', 'VERY_LATE') THEN ABS(gt.differential) END) as strongest_late_differential
                
            FROM game_timeline_all_markets gt
            GROUP BY gt.game_id, gt.home_team, gt.away_team, gt.game_datetime, gt.source, gt.book
            HAVING COUNT(CASE WHEN gt.time_period = 'EARLY' AND ABS(gt.differential) >= 12 THEN 1 END) >= 1
               AND COUNT(CASE WHEN gt.time_period IN ('LATE', 'VERY_LATE') AND ABS(gt.differential) >= 12 THEN 1 END) >= 1
        ),
        
        flip_detection AS (
            SELECT 
                ela.*,
                
                CASE 
                    WHEN ela.strongest_early_signal LIKE '%:' || ela.home_team THEN ela.home_team
                    WHEN ela.strongest_early_signal LIKE '%:' || ela.away_team THEN ela.away_team
                    ELSE NULL
                END as early_recommended_team,
                
                CASE 
                    WHEN ela.strongest_late_signal LIKE '%:' || ela.home_team THEN ela.home_team
                    WHEN ela.strongest_late_signal LIKE '%:' || ela.away_team THEN ela.away_team  
                    ELSE NULL
                END as late_recommended_team,
                
                CASE 
                    WHEN SPLIT_PART(ela.strongest_early_signal, ':', 1) = SPLIT_PART(ela.strongest_late_signal, ':', 1)
                         AND SPLIT_PART(ela.strongest_early_signal, ':', 2) != SPLIT_PART(ela.strongest_late_signal, ':', 2)
                         AND SPLIT_PART(ela.strongest_early_signal, ':', 2) IN (ela.home_team, ela.away_team)
                         AND SPLIT_PART(ela.strongest_late_signal, ':', 2) IN (ela.home_team, ela.away_team)
                    THEN 'SAME_MARKET_FLIP'
                    
                    WHEN SPLIT_PART(ela.strongest_early_signal, ':', 1) != SPLIT_PART(ela.strongest_late_signal, ':', 1)
                         AND SPLIT_PART(ela.strongest_early_signal, ':', 2) != SPLIT_PART(ela.strongest_late_signal, ':', 2)
                         AND SPLIT_PART(ela.strongest_early_signal, ':', 2) IN (ela.home_team, ela.away_team)
                         AND SPLIT_PART(ela.strongest_late_signal, ':', 2) IN (ela.home_team, ela.away_team)
                    THEN 'CROSS_MARKET_CONTRADICTION'
                    
                    WHEN ela.strongest_early_differential >= 15 
                         AND ela.strongest_late_differential BETWEEN 8 AND 15
                         AND SPLIT_PART(ela.strongest_early_signal, ':', 2) != SPLIT_PART(ela.strongest_late_signal, ':', 2)
                         AND SPLIT_PART(ela.strongest_early_signal, ':', 2) IN (ela.home_team, ela.away_team)
                         AND SPLIT_PART(ela.strongest_late_signal, ':', 2) IN (ela.home_team, ela.away_team)
                    THEN 'WEAK_LATE_CONTRADICTION'
                    
                    ELSE 'NO_FLIP_DETECTED'
                END as flip_type
                
            FROM early_late_analysis ela
            WHERE ela.strongest_early_signal IS NOT NULL 
              AND ela.strongest_late_signal IS NOT NULL
        ),
        
        strategy_results AS (
            SELECT 
                fd.*,
                go.home_score,
                go.away_score,
                
                CASE 
                    WHEN fd.early_recommended_team IS NULL THEN NULL
                    WHEN go.home_score IS NULL OR go.away_score IS NULL THEN NULL
                    WHEN go.home_score = go.away_score THEN 0
                    WHEN fd.early_recommended_team = fd.home_team AND go.home_score > go.away_score THEN 1
                    WHEN fd.early_recommended_team = fd.away_team AND go.away_score > go.home_score THEN 1
                    ELSE 0
                END as strategy_result,
                
                CASE 
                    WHEN fd.late_recommended_team IS NULL THEN NULL
                    WHEN go.home_score IS NULL OR go.away_score IS NULL THEN NULL
                    WHEN go.home_score = go.away_score THEN 0
                    WHEN fd.late_recommended_team = fd.home_team AND go.home_score > go.away_score THEN 1
                    WHEN fd.late_recommended_team = fd.away_team AND go.away_score > go.home_score THEN 1
                    ELSE 0
                END as late_signal_result
                
            FROM flip_detection fd
            LEFT JOIN game_outcomes go ON fd.game_id = go.game_id
            WHERE fd.flip_type IN ('SAME_MARKET_FLIP', 'CROSS_MARKET_CONTRADICTION', 'WEAK_LATE_CONTRADICTION')
            AND go.home_score IS NOT NULL
            AND go.away_score IS NOT NULL
        )
        
        SELECT 
            'ENHANCED_LATE_SHARP_FLIP_FADE' as strategy_name,
            sr.source || '-' || COALESCE(sr.book, 'NULL') as source_book_combo,
            
            COUNT(*) as total_games,
            COUNT(*) FILTER (WHERE sr.flip_type = 'CROSS_MARKET_CONTRADICTION') as cross_market_contradictions,
            COUNT(*) FILTER (WHERE sr.flip_type = 'SAME_MARKET_FLIP') as same_market_flips,
            COUNT(*) FILTER (WHERE sr.flip_type = 'WEAK_LATE_CONTRADICTION') as weak_late_contradictions,
            
            SUM(sr.strategy_result) as strategy_wins,
            ROUND(AVG(sr.strategy_result::decimal), 3) as strategy_win_rate,
            
            SUM(sr.late_signal_result) as late_signal_wins,
            ROUND(AVG(sr.late_signal_result::decimal), 3) as late_signal_win_rate,
            
            ROUND(AVG(sr.strategy_result::decimal) FILTER (WHERE sr.flip_type = 'CROSS_MARKET_CONTRADICTION'), 3) as cross_market_win_rate,
            ROUND(AVG(sr.strategy_result::decimal) FILTER (WHERE sr.flip_type = 'SAME_MARKET_FLIP'), 3) as same_market_win_rate,
            ROUND(AVG(sr.strategy_result::decimal) FILTER (WHERE sr.flip_type = 'WEAK_LATE_CONTRADICTION'), 3) as weak_contradiction_win_rate,
            
            ROUND(AVG(sr.strategy_result::decimal) FILTER (WHERE sr.strongest_early_differential >= 20), 3) as high_confidence_win_rate,
            ROUND(AVG(sr.strategy_result::decimal) FILTER (WHERE sr.strongest_early_differential BETWEEN 15 AND 20), 3) as medium_confidence_win_rate,
            
            ROUND((SUM(sr.strategy_result) * 0.909 - (COUNT(*) - SUM(sr.strategy_result))) * 100.0 / COUNT(*), 2) as strategy_roi_percent,
            
            ROUND(AVG(sr.strongest_early_differential), 1) as avg_early_signal_strength,
            ROUND(AVG(sr.strongest_late_differential), 1) as avg_late_signal_strength
            
        FROM strategy_results sr  
        GROUP BY sr.source, sr.book
        HAVING COUNT(*) >= %s
        ORDER BY strategy_win_rate DESC, total_games DESC
        """

    async def run_backtest(
        self, start_date: datetime | None = None, min_games: int = 5
    ) -> list[StrategyPerformance]:
        """Run comprehensive backtest of the enhanced late sharp flip strategy."""
        if not start_date:
            start_date = datetime.now() - timedelta(days=180)  # Default: 6 months

        try:
            logger.info(
                "Starting enhanced late sharp flip strategy backtest",
                start_date=start_date.date(),
                min_games=min_games,
            )

            # Execute backtest query
            query = self.get_postgresql_compatible_query()
            results = self.db_manager.execute_query(
                query, (start_date, min_games), fetch=True
            )

            if not results:
                logger.warning(
                    "No backtest results found", start_date=start_date.date()
                )
                return []

            # Convert results to StrategyPerformance objects
            performances = []
            for row in results:
                performance = StrategyPerformance(
                    source_book=row[1],  # source_book_combo
                    total_games=row[2],
                    cross_market_contradictions=row[3],
                    same_market_flips=row[4],
                    weak_late_contradictions=row[5],
                    strategy_wins=row[6],
                    strategy_win_rate=float(row[7]) if row[7] is not None else 0.0,
                    strategy_roi=float(row[15]) if row[15] is not None else 0.0,
                    late_signal_wins=row[8],
                    late_signal_win_rate=float(row[9]) if row[9] is not None else 0.0,
                    cross_market_win_rate=float(row[10])
                    if row[10] is not None
                    else 0.0,
                    same_market_win_rate=float(row[11]) if row[11] is not None else 0.0,
                    weak_contradiction_win_rate=float(row[12])
                    if row[12] is not None
                    else 0.0,
                    high_confidence_win_rate=float(row[13])
                    if row[13] is not None
                    else 0.0,
                    medium_confidence_win_rate=float(row[14])
                    if row[14] is not None
                    else 0.0,
                    avg_early_signal_strength=float(row[16])
                    if row[16] is not None
                    else 0.0,
                    avg_late_signal_strength=float(row[17])
                    if row[17] is not None
                    else 0.0,
                    avg_hours_between_signals=0.0,  # Not included in simplified query
                    recent_examples=[],  # Not included in simplified query
                )

                performances.append(performance)

            logger.info(
                "Backtest completed successfully",
                total_results=len(performances),
                total_games=sum(p.total_games for p in performances),
            )

            return performances

        except Exception as e:
            logger.error("Backtest failed", error=str(e))
            raise

    def analyze_profitability(
        self, performances: list[StrategyPerformance]
    ) -> dict[str, any]:
        """Analyze overall strategy profitability."""

        if not performances:
            return {"error": "No performance data to analyze"}

        # Overall aggregated performance
        total_games = sum(p.total_games for p in performances)
        total_wins = sum(p.strategy_wins for p in performances)
        weighted_win_rate = total_wins / total_games if total_games > 0 else 0

        # Weighted ROI calculation
        weighted_roi = (
            sum(p.strategy_roi * p.total_games for p in performances) / total_games
            if total_games > 0
            else 0
        )

        # Performance by flip type
        cross_market_games = sum(p.cross_market_contradictions for p in performances)
        same_market_games = sum(p.same_market_flips for p in performances)
        weak_contradiction_games = sum(p.weak_late_contradictions for p in performances)

        # Best performing sources
        profitable_sources = [p for p in performances if p.strategy_roi > 0]
        best_performer = (
            max(performances, key=lambda x: x.strategy_roi) if performances else None
        )

        # Risk analysis
        negative_roi_count = len([p for p in performances if p.strategy_roi < 0])
        risk_ratio = negative_roi_count / len(performances) if performances else 0

        return {
            "overall_profitability": weighted_roi > 5.0,  # Profitable if >5% ROI
            "total_games_analyzed": total_games,
            "overall_win_rate": weighted_win_rate,
            "overall_roi": weighted_roi,
            "profitable_sources": len(profitable_sources),
            "total_sources": len(performances),
            "profitability_ratio": len(profitable_sources) / len(performances)
            if performances
            else 0,
            "best_performer": {
                "source": best_performer.source_book if best_performer else None,
                "roi": best_performer.strategy_roi if best_performer else None,
                "win_rate": best_performer.strategy_win_rate
                if best_performer
                else None,
                "games": best_performer.total_games if best_performer else None,
            }
            if best_performer
            else None,
            "flip_type_distribution": {
                "cross_market_contradictions": cross_market_games,
                "same_market_flips": same_market_games,
                "weak_late_contradictions": weak_contradiction_games,
            },
            "risk_analysis": {
                "sources_with_negative_roi": negative_roi_count,
                "risk_ratio": risk_ratio,
                "recommendation": "LOW_RISK"
                if risk_ratio < 0.3
                else "MEDIUM_RISK"
                if risk_ratio < 0.6
                else "HIGH_RISK",
            },
        }

    def generate_detailed_report(self, performances: list[StrategyPerformance]) -> str:
        """Generate a detailed backtest report."""

        if not performances:
            return "❌ No backtest data available"

        profitability = self.analyze_profitability(performances)

        report = []
        report.append("🎯 ENHANCED LATE SHARP FLIP STRATEGY - BACKTEST REPORT")
        report.append("=" * 80)

        # Overall summary
        report.append("\n📊 OVERALL PERFORMANCE SUMMARY")
        report.append(
            f"   📈 Total Games Analyzed: {profitability['total_games_analyzed']}"
        )
        report.append(
            f"   🎯 Overall Win Rate: {profitability['overall_win_rate']:.1%}"
        )
        report.append(f"   💰 Overall ROI: {profitability['overall_roi']:.2f}%")
        report.append(
            f"   ✅ Profitable Sources: {profitability['profitable_sources']}/{profitability['total_sources']}"
        )

        # Profitability assessment
        is_profitable = profitability["overall_profitability"]
        if is_profitable:
            report.append("\n🔥 STRATEGY ASSESSMENT: PROFITABLE ✅")
            report.append(
                "   💡 The strategy shows positive ROI > 5% and is recommended for live betting"
            )
        else:
            report.append("\n❌ STRATEGY ASSESSMENT: NOT PROFITABLE")
            report.append(
                "   ⚠️  The strategy shows insufficient ROI and needs refinement"
            )

        # Risk analysis
        risk = profitability["risk_analysis"]
        report.append("\n⚠️  RISK ANALYSIS")
        report.append(
            f"   📊 Sources with Negative ROI: {risk['sources_with_negative_roi']}"
        )
        report.append(f"   📈 Risk Ratio: {risk['risk_ratio']:.1%}")
        report.append(f"   🎚️  Risk Level: {risk['recommendation']}")

        # Best performer
        if profitability["best_performer"]:
            best = profitability["best_performer"]
            report.append("\n🏆 BEST PERFORMING SOURCE")
            report.append(f"   📡 Source: {best['source']}")
            report.append(f"   💰 ROI: {best['roi']:.2f}%")
            report.append(f"   🎯 Win Rate: {best['win_rate']:.1%}")
            report.append(f"   📊 Games: {best['games']}")

        # Flip type distribution
        flips = profitability["flip_type_distribution"]
        report.append("\n🔄 FLIP TYPE ANALYSIS")
        report.append(
            f"   🔀 Cross-Market Contradictions: {flips['cross_market_contradictions']} games"
        )
        report.append(f"   🔁 Same Market Flips: {flips['same_market_flips']} games")
        report.append(
            f"   💫 Weak Late Contradictions: {flips['weak_late_contradictions']} games"
        )

        # Detailed performance by source
        report.append("\n📋 DETAILED PERFORMANCE BY SOURCE")
        report.append("-" * 80)

        # Sort by ROI descending
        sorted_performances = sorted(
            performances, key=lambda x: x.strategy_roi, reverse=True
        )

        for i, perf in enumerate(sorted_performances, 1):
            roi_status = (
                "✅"
                if perf.strategy_roi > 5
                else "⚠️"
                if perf.strategy_roi > 0
                else "❌"
            )

            report.append(f"\n{roi_status} #{i}. {perf.source_book}")
            report.append(
                f"   📊 Games: {perf.total_games} | Win Rate: {perf.strategy_win_rate:.1%} | ROI: {perf.strategy_roi:.2f}%"
            )
            report.append(
                f"   🔄 Flips: {perf.cross_market_contradictions}CM + {perf.same_market_flips}SM + {perf.weak_late_contradictions}WL"
            )
            report.append(
                f"   📈 Signal Strength: Early {perf.avg_early_signal_strength:.1f}% | Late {perf.avg_late_signal_strength:.1f}%"
            )

        report.append("\n" + "=" * 80)

        return "\n".join(report)

    def close(self):
        """Close database connection."""
        if self.db_manager:
            self.db_manager.close()


async def main():
    """Run the enhanced late sharp flip strategy backtest."""

    backtester = EnhancedLateSharpFlipBacktester()

    try:
        print("🚀 ENHANCED LATE SHARP FLIP STRATEGY BACKTEST")
        print("=" * 60)
        print("🔍 Analyzing cross-market flip patterns and profitability...")
        print()

        # Run backtest
        performances = await backtester.run_backtest(min_games=5)

        if not performances:
            print("❌ No sufficient data found for backtesting")
            print("💡 Ensure you have:")
            print("   • Historical betting splits data")
            print("   • Completed game outcomes")
            print("   • At least 5 games per source/book combination")
            return

        # Generate and display report
        report = backtester.generate_detailed_report(performances)
        print(report)

        # Profitability analysis
        profitability = backtester.analyze_profitability(performances)

        print("\n🎯 FINAL VERDICT")
        print("=" * 50)
        if profitability["overall_profitability"]:
            print("✅ STRATEGY IS PROFITABLE!")
            print(
                f"🚀 Ready for live implementation with {profitability['overall_roi']:.2f}% ROI"
            )
        else:
            print("❌ Strategy needs refinement")
            print(
                f"🔧 Current ROI of {profitability['overall_roi']:.2f}% is below profitability threshold"
            )

    except Exception as e:
        print(f"❌ Backtest failed: {e}")
        import traceback

        traceback.print_exc()
    finally:
        backtester.close()


if __name__ == "__main__":
    asyncio.run(main())
