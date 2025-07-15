#!/usr/bin/env python3
"""
Phase 1 Strategies Runner
Executes all three new Phase 1 strategies and provides comprehensive analysis:
1. Total Line Sweet Spots
2. Underdog ML Value
3. Team Specific Bias
"""

import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path

# Add the src directory to the path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from mlb_sharp_betting.services.database_coordinator import get_database_coordinator


async def run_strategy(strategy_name: str, sql_file: str) -> dict:
    """Run a single strategy and return results"""
    coordinator = get_database_coordinator()

    sql_path = Path(__file__).parent / sql_file
    with open(sql_path) as f:
        sql_query = f.read()

    try:
        results = await coordinator.execute_read(sql_query)
        return {
            "strategy": strategy_name,
            "status": "SUCCESS",
            "results": results or [],
            "count": len(results) if results else 0,
        }
    except Exception as e:
        return {
            "strategy": strategy_name,
            "status": "ERROR",
            "error": str(e),
            "results": [],
            "count": 0,
        }


def analyze_results(all_results: dict) -> dict:
    """Analyze all strategy results and find best opportunities"""
    summary = {
        "total_strategies": len(all_results),
        "successful_strategies": 0,
        "best_opportunities": [],
        "strategy_performance": {},
        "key_insights": [],
    }

    for strategy_name, data in all_results.items():
        if data["status"] == "SUCCESS":
            summary["successful_strategies"] += 1
            results = data["results"]

            # Find profitable strategies (ROI > 0)
            profitable = [r for r in results if r.get("roi_per_100_unit", -100) > 0]
            excellent = [
                r for r in results if "🟢" in str(r.get("strategy_rating", ""))
            ]

            summary["strategy_performance"][strategy_name] = {
                "total_variants": len(results),
                "profitable_variants": len(profitable),
                "excellent_variants": len(excellent),
                "best_roi": max([r.get("roi_per_100_unit", -100) for r in results])
                if results
                else -100,
                "avg_win_rate": sum([r.get("win_rate", 0) for r in results])
                / len(results)
                if results
                else 0,
            }

            # Add best opportunities
            for result in excellent + profitable[:3]:  # Top excellent + profitable
                if result.get("roi_per_100_unit", -100) > 5:  # Only significant ROI
                    summary["best_opportunities"].append(
                        {
                            "strategy": strategy_name,
                            "variant": result.get("strategy_variant", "Unknown"),
                            "win_rate": result.get("win_rate", 0),
                            "roi": result.get("roi_per_100_unit", 0),
                            "total_bets": result.get("total_bets", 0),
                            "rating": result.get("strategy_rating", ""),
                            "insight": result.get("strategy_insight", ""),
                        }
                    )

    # Generate key insights
    if summary["best_opportunities"]:
        best_opp = max(summary["best_opportunities"], key=lambda x: x["roi"])
        summary["key_insights"].append(
            f"🏆 Best opportunity: {best_opp['strategy']} - {best_opp['variant']} with {best_opp['roi']:.1f}% ROI"
        )

    high_win_rate = [
        opp for opp in summary["best_opportunities"] if opp["win_rate"] > 60
    ]
    if high_win_rate:
        summary["key_insights"].append(
            f"🎯 {len(high_win_rate)} strategies with >60% win rate found"
        )

    return summary


async def main():
    """Main execution function"""
    print("🚀 PHASE 1 MLB BETTING STRATEGIES ANALYSIS")
    print("=" * 60)
    print(f"📅 Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Define strategies
    strategies = {
        "Total Line Sweet Spots": "total_line_sweet_spots_strategy.sql",
        "Underdog ML Value": "underdog_ml_value_strategy.sql",
        "Team Specific Bias": "team_specific_bias_strategy.sql",
    }

    # Run all strategies
    all_results = {}
    for strategy_name, sql_file in strategies.items():
        print(f"🔍 Running {strategy_name}...")
        result = await run_strategy(strategy_name, sql_file)
        all_results[strategy_name] = result

        if result["status"] == "SUCCESS":
            print(f"✅ {strategy_name}: {result['count']} variants found")

            # Show top 2 results for each strategy
            if result["results"]:
                sorted_results = sorted(
                    result["results"],
                    key=lambda x: x.get("roi_per_100_unit", -100),
                    reverse=True,
                )
                for i, res in enumerate(sorted_results[:2]):
                    win_rate = res.get("win_rate", 0)
                    roi = res.get("roi_per_100_unit", 0)
                    variant = res.get("strategy_variant", "Unknown")
                    print(
                        f"   {i + 1}. {variant}: {win_rate:.1f}% win rate, {roi:.1f}% ROI"
                    )
        else:
            print(f"❌ {strategy_name}: {result.get('error', 'Unknown error')}")
        print()

    # Analyze all results
    print("📊 COMPREHENSIVE ANALYSIS")
    print("=" * 60)

    summary = analyze_results(all_results)

    print(
        f"✅ Successful Strategies: {summary['successful_strategies']}/{summary['total_strategies']}"
    )
    print(f"🎯 Best Opportunities Found: {len(summary['best_opportunities'])}")
    print()

    # Show strategy performance
    print("📈 STRATEGY PERFORMANCE SUMMARY")
    print("-" * 40)
    for strategy, perf in summary["strategy_performance"].items():
        print(f"\n{strategy}:")
        print(f"   • Variants: {perf['total_variants']}")
        print(f"   • Profitable: {perf['profitable_variants']}")
        print(f"   • Excellent: {perf['excellent_variants']}")
        print(f"   • Best ROI: {perf['best_roi']:.1f}%")
        print(f"   • Avg Win Rate: {perf['avg_win_rate']:.1f}%")

    # Show best opportunities
    if summary["best_opportunities"]:
        print("\n🏆 TOP BETTING OPPORTUNITIES")
        print("-" * 40)
        top_opportunities = sorted(
            summary["best_opportunities"], key=lambda x: x["roi"], reverse=True
        )[:5]

        for i, opp in enumerate(top_opportunities, 1):
            print(f"\n{i}. {opp['strategy']} - {opp['variant']}")
            print(f"   💰 ROI: {opp['roi']:.1f}%")
            print(f"   🎯 Win Rate: {opp['win_rate']:.1f}%")
            print(f"   📊 Sample Size: {opp['total_bets']} bets")
            print(f"   ⭐ Rating: {opp['rating']}")
            print(f"   💡 Insight: {opp['insight']}")

    # Show key insights
    if summary["key_insights"]:
        print("\n🔍 KEY INSIGHTS")
        print("-" * 40)
        for insight in summary["key_insights"]:
            print(f"• {insight}")

    # Save results
    output_file = (
        Path("analysis_results")
        / f"phase1_strategies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    output_file.parent.mkdir(exist_ok=True)

    with open(output_file, "w") as f:
        json.dump(
            {
                "timestamp": datetime.now().isoformat(),
                "summary": summary,
                "detailed_results": all_results,
            },
            f,
            indent=2,
            default=str,
        )

    print(f"\n💾 Results saved to: {output_file}")

    # Recommendations
    print("\n🎯 NEXT STEPS RECOMMENDATIONS")
    print("-" * 40)

    if summary["best_opportunities"]:
        print("1. 🚀 IMMEDIATE ACTION:")
        best = summary["best_opportunities"][0]
        print(f"   Focus on: {best['strategy']} - {best['variant']}")
        print(f"   Expected ROI: {best['roi']:.1f}%")
        print("   Start with small unit sizes to validate")

        print("\n2. 📈 PORTFOLIO APPROACH:")
        profitable_count = len(
            [opp for opp in summary["best_opportunities"] if opp["roi"] > 10]
        )
        if profitable_count > 1:
            print(f"   Combine top {min(profitable_count, 3)} strategies")
            print("   Allocate 1-2% of bankroll per strategy")

        print("\n3. 🔬 VALIDATION:")
        print("   • Paper trade for 1-2 weeks")
        print("   • Track actual vs predicted performance")
        print("   • Adjust thresholds based on results")

    else:
        print("1. 📊 DATA COLLECTION:")
        print("   • Need more historical data for reliable patterns")
        print("   • Consider lowering minimum sample sizes")
        print("   • Focus on data quality improvements")

    print("\n🏁 Phase 1 analysis complete!")


if __name__ == "__main__":
    asyncio.run(main())
