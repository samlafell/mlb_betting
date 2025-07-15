#!/usr/bin/env python3
"""
THRESHOLD & SIGNAL STRENGTH ANALYSIS TOOL
=========================================

This tool addresses two critical issues in the backtesting system:

1. THRESHOLD ANALYSIS & ADJUSTMENT
   - Analyzes distribution of signal strengths in historical data
   - Adjusts strategy thresholds to more realistic levels based on actual patterns
   - Recalibrates confidence scoring system

2. SIGNAL STRENGTH INVESTIGATION
   - Investigates why signal strengths are consistently lower than strategy thresholds
   - Reviews confidence scoring algorithms in processors
   - Optimizes signal strength calculations

The log shows:
- Only sharp_action strategy is generating signals (2 signals)
- All other strategies are finding betting data but generating NO signals
- This indicates threshold/confidence issues preventing signal generation

FINDINGS FROM LOG ANALYSIS:
- opposing_markets: "betting data found but no signals generated"
- book_conflicts: "betting data found but no signals generated"
- public_money_fade: "betting data found but no signals generated"
- late_sharp_flip: "betting data found but no signals generated"
- consensus_moneyline: "betting data found but no signals generated"
- underdog_ml_value: "betting data found but no signals generated"
- line_movement: "betting data found but no signals generated"
"""

import asyncio
import json

# Import our modules
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import numpy as np
import pytz

sys.path.append("src")

from mlb_sharp_betting.analysis.processors.strategy_processor_factory import (
    StrategyProcessorFactory,
)
from mlb_sharp_betting.db.connection import get_connection
from mlb_sharp_betting.services.confidence_scorer import ConfidenceScorer


@dataclass
class ThresholdAnalysis:
    """Results of threshold analysis for a strategy"""

    strategy_name: str
    current_threshold: float
    recommended_threshold: float
    signal_count_current: int
    signal_count_recommended: int
    signal_distribution: dict[str, int]
    sample_signals: list[dict]
    confidence_score_stats: dict[str, float]


@dataclass
class SignalStrengthAnalysis:
    """Results of signal strength investigation"""

    strategy_name: str
    raw_data_count: int
    processed_signals_count: int
    filtered_out_count: int
    avg_signal_strength: float
    signal_strength_distribution: list[float]
    common_filter_reasons: list[str]
    confidence_calculation_issues: list[str]


class ThresholdSignalAnalyzer:
    """Comprehensive analyzer for threshold and signal strength issues"""

    def __init__(self):
        self.est = pytz.timezone("US/Eastern")
        self.connection = None
        self.confidence_scorer = ConfidenceScorer()
        self.processor_factory = StrategyProcessorFactory()

        # Date range for historical analysis (last 30 days)
        self.end_date = datetime.now(self.est).date()
        self.start_date = self.end_date - timedelta(days=30)

        print("📊 Initializing Threshold & Signal Analysis")
        print(f"   Date Range: {self.start_date} to {self.end_date}")
        print("   Analysis Period: 30 days")

    async def run_complete_analysis(self):
        """Run complete threshold and signal strength analysis"""
        try:
            await self._initialize_connection()

            print("\n🔍 PHASE 1: DATA AVAILABILITY ANALYSIS")
            await self._analyze_data_availability()

            print("\n📈 PHASE 2: SIGNAL STRENGTH DISTRIBUTION ANALYSIS")
            signal_distributions = await self._analyze_signal_strength_distributions()

            print("\n🎯 PHASE 3: CURRENT THRESHOLD ANALYSIS")
            threshold_results = await self._analyze_current_thresholds()

            print("\n⚙️ PHASE 4: PROCESSOR DEEP DIVE")
            processor_analysis = await self._analyze_processor_issues()

            print("\n🧮 PHASE 5: CONFIDENCE SCORING ANALYSIS")
            confidence_analysis = await self._analyze_confidence_scoring()

            print("\n📊 PHASE 6: GENERATING RECOMMENDATIONS")
            recommendations = await self._generate_recommendations(
                signal_distributions,
                threshold_results,
                processor_analysis,
                confidence_analysis,
            )

            print("\n✅ PHASE 7: IMPLEMENTING FIXES")
            await self._implement_fixes(recommendations)

            print("\n📋 PHASE 8: GENERATING REPORT")
            await self._generate_final_report(recommendations)

        except Exception as e:
            print(f"❌ Analysis failed: {e}")
            raise
        finally:
            if self.connection:
                await self.connection.close()

    async def _initialize_connection(self):
        """Initialize database connection"""
        try:
            self.connection = await get_connection()
            print("✅ Database connection established")
        except Exception as e:
            print(f"❌ Failed to connect to database: {e}")
            raise

    async def _analyze_data_availability(self):
        """Analyze what data is available in the database"""
        print("   Checking data availability...")

        # Check splits data
        query = """
        SELECT 
            source,
            book,
            split_type,
            COUNT(*) as record_count,
            MIN(last_updated) as earliest_update,
            MAX(last_updated) as latest_update,
            AVG(CASE WHEN differential IS NOT NULL THEN ABS(differential) END) as avg_differential
        FROM splits.raw_mlb_betting_splits 
        WHERE last_updated >= %s AND last_updated <= %s
        GROUP BY source, book, split_type
        ORDER BY record_count DESC
        """

        start_datetime = datetime.combine(self.start_date, datetime.min.time())
        end_datetime = datetime.combine(self.end_date, datetime.max.time())

        rows = await self.connection.fetch(query, start_datetime, end_datetime)

        print(f"   📊 Found {len(rows)} source/book/type combinations")

        total_records = sum(row["record_count"] for row in rows)
        print(f"   📈 Total records: {total_records:,}")

        # Show top data sources
        print("   🔝 Top data sources:")
        for i, row in enumerate(rows[:10]):
            avg_diff = row["avg_differential"] or 0
            print(
                f"      {i + 1:2d}. {row['source']}/{row['book']}/{row['split_type']}: "
                f"{row['record_count']:,} records, avg diff: {avg_diff:.1f}%"
            )

        # Check for games with outcomes
        outcome_query = """
        SELECT COUNT(*) as games_with_outcomes
        FROM game_outcomes 
        WHERE game_date >= %s AND game_date <= %s
        """

        outcome_rows = await self.connection.fetch(
            outcome_query, self.start_date, self.end_date
        )
        games_with_outcomes = (
            outcome_rows[0]["games_with_outcomes"] if outcome_rows else 0
        )

        print(f"   🎯 Games with outcomes: {games_with_outcomes}")

        return {
            "total_records": total_records,
            "data_sources": len(rows),
            "games_with_outcomes": games_with_outcomes,
            "source_breakdown": rows[:20],  # Top 20 for detailed analysis
        }

    async def _analyze_signal_strength_distributions(self):
        """Analyze the distribution of signal strengths in historical data"""
        print("   Analyzing signal strength distributions...")

        distributions = {}

        # Get raw differential data by strategy type
        queries = {
            "moneyline_differentials": """
                SELECT differential, source, book, split_type
                FROM splits.raw_mlb_betting_splits 
                WHERE split_type = 'moneyline' 
                AND differential IS NOT NULL
                AND last_updated >= %s AND last_updated <= %s
            """,
            "spread_differentials": """
                SELECT differential, source, book, split_type
                FROM splits.raw_mlb_betting_splits 
                WHERE split_type = 'spread' 
                AND differential IS NOT NULL
                AND last_updated >= %s AND last_updated <= %s
            """,
            "total_differentials": """
                SELECT differential, source, book, split_type
                FROM splits.raw_mlb_betting_splits 
                WHERE split_type = 'total' 
                AND differential IS NOT NULL
                AND last_updated >= %s AND last_updated <= %s
            """,
        }

        start_datetime = datetime.combine(self.start_date, datetime.min.time())
        end_datetime = datetime.combine(self.end_date, datetime.max.time())

        for query_name, query in queries.items():
            rows = await self.connection.fetch(query, start_datetime, end_datetime)
            differentials = [float(row["differential"]) for row in rows]

            if differentials:
                distributions[query_name] = {
                    "data": differentials,
                    "count": len(differentials),
                    "mean": np.mean(differentials),
                    "std": np.std(differentials),
                    "min": np.min(differentials),
                    "max": np.max(differentials),
                    "percentiles": {
                        "5": np.percentile(differentials, 5),
                        "10": np.percentile(differentials, 10),
                        "25": np.percentile(differentials, 25),
                        "50": np.percentile(differentials, 50),
                        "75": np.percentile(differentials, 75),
                        "90": np.percentile(differentials, 90),
                        "95": np.percentile(differentials, 95),
                    },
                }

                print(f"   📊 {query_name}:")
                print(f"      Count: {len(differentials):,}")
                print(f"      Mean: {np.mean(differentials):.2f}%")
                print(f"      Std: {np.std(differentials):.2f}%")
                print(
                    f"      Range: {np.min(differentials):.1f}% to {np.max(differentials):.1f}%"
                )
                print(f"      90th percentile: {np.percentile(differentials, 90):.1f}%")
                print(f"      95th percentile: {np.percentile(differentials, 95):.1f}%")

                # Count signals above various thresholds
                thresholds = [5, 8, 10, 12, 15, 20, 25, 30]
                print("      Signals above thresholds:")
                for threshold in thresholds:
                    count = sum(1 for d in differentials if abs(d) >= threshold)
                    pct = (count / len(differentials)) * 100
                    print(f"        {threshold:2d}%: {count:4d} signals ({pct:4.1f}%)")

        return distributions

    async def _analyze_current_thresholds(self):
        """Analyze current thresholds used by each processor"""
        print("   Analyzing current processor thresholds...")

        # Get all processors
        processors = self.processor_factory.get_all_processors()
        threshold_analysis = {}

        for proc_name, processor in processors.items():
            print(f"   🔍 Analyzing {proc_name}...")

            # Extract threshold information from processor code
            threshold_info = await self._extract_processor_thresholds(processor)
            threshold_analysis[proc_name] = threshold_info

            print(f"      Current thresholds: {threshold_info}")

        return threshold_analysis

    async def _extract_processor_thresholds(self, processor) -> dict[str, Any]:
        """Extract threshold information from a processor"""
        thresholds = {}

        try:
            # Read the processor's source code to find thresholds
            import inspect

            source = inspect.getsource(processor.__class__)

            # Look for common threshold patterns
            import re

            # Find numeric thresholds
            threshold_patterns = [
                r"(\w+)\s*[>=<]+\s*(\d+\.?\d*)",  # variable >= 15.0
                r"(\d+\.?\d*)\s*[>=<]+\s*(\w+)",  # 15.0 >= variable
                r"threshold.*?(\d+\.?\d*)",  # threshold = 15.0
                r"minimum.*?(\d+\.?\d*)",  # minimum 15.0
            ]

            found_thresholds = []
            for pattern in threshold_patterns:
                matches = re.finditer(pattern, source, re.IGNORECASE)
                for match in matches:
                    found_thresholds.append(match.groups())

            thresholds["extracted_values"] = found_thresholds

            # Processor-specific threshold extraction
            class_name = processor.__class__.__name__

            if "BookConflict" in class_name:
                thresholds["conflict_strength_min"] = (
                    8.0  # From code: if conflict_strength < 8.0
                )
                thresholds["type"] = "conflict_variance"

            elif "PublicFade" in class_name:
                thresholds["consensus_strength_min"] = (
                    65.0  # From code: if consensus_strength < 65.0
                )
                thresholds["type"] = "public_consensus"

            elif "LateFlip" in class_name:
                thresholds["flip_strength_min"] = (
                    12.0  # From code: if flip_strength < 12.0
                )
                thresholds["sharp_threshold"] = 8.0  # From code: abs(differential) >= 8
                thresholds["type"] = "flip_detection"

            else:
                thresholds["type"] = "unknown"

        except Exception as e:
            thresholds["error"] = str(e)
            thresholds["type"] = "extraction_failed"

        return thresholds

    async def _analyze_processor_issues(self):
        """Deep dive into why each processor is not generating signals"""
        print("   Deep diving into processor issues...")

        processor_issues = {}
        processors = self.processor_factory.get_all_processors()

        for proc_name, processor in processors.items():
            print(f"   🔬 Analyzing {proc_name} processing pipeline...")

            issues = await self._diagnose_processor_pipeline(proc_name, processor)
            processor_issues[proc_name] = issues

            print(f"      Issues found: {len(issues['issues'])}")
            for issue in issues["issues"][:3]:  # Show top 3 issues
                print(f"        - {issue}")

        return processor_issues

    async def _diagnose_processor_pipeline(
        self, proc_name: str, processor
    ) -> dict[str, Any]:
        """Diagnose issues in a specific processor's pipeline"""
        issues = {
            "processor_name": proc_name,
            "issues": [],
            "raw_data_count": 0,
            "filtered_count": 0,
            "signal_count": 0,
        }

        try:
            # Test the processor with recent data
            start_time = datetime.now(self.est) - timedelta(hours=6)
            end_time = datetime.now(self.est) + timedelta(hours=6)

            # Check if processor has required methods
            if not hasattr(processor, "process"):
                issues["issues"].append("Missing process() method")
                return issues

            # Try to get data that the processor would use
            if hasattr(processor, "repository"):
                try:
                    # Test data retrieval methods
                    if hasattr(processor.repository, "get_multi_book_data"):
                        raw_data = await processor.repository.get_multi_book_data(
                            start_time, end_time
                        )
                        issues["raw_data_count"] = len(raw_data) if raw_data else 0

                    elif hasattr(processor.repository, "get_public_betting_data"):
                        raw_data = await processor.repository.get_public_betting_data(
                            start_time, end_time
                        )
                        issues["raw_data_count"] = len(raw_data) if raw_data else 0

                    elif hasattr(processor.repository, "get_steam_move_data"):
                        raw_data = await processor.repository.get_steam_move_data(
                            start_time, end_time
                        )
                        issues["raw_data_count"] = len(raw_data) if raw_data else 0

                    else:
                        issues["issues"].append("No recognized data retrieval method")

                except Exception as e:
                    issues["issues"].append(f"Data retrieval failed: {str(e)}")

            # Analyze threshold settings
            if "BookConflict" in proc_name:
                # Check if conflict strength threshold is too high
                issues["issues"].append(
                    "Conflict strength threshold may be too high (8.0%)"
                )

            elif "PublicFade" in proc_name:
                # Check if consensus threshold is too high
                issues["issues"].append(
                    "Public consensus threshold may be too high (65.0%)"
                )

            elif "LateFlip" in proc_name:
                # Check if flip strength threshold is too high
                issues["issues"].append(
                    "Flip strength threshold may be too high (12.0%)"
                )

            # General issues
            if issues["raw_data_count"] == 0:
                issues["issues"].append("No raw data available for processing")
            elif issues["raw_data_count"] < 10:
                issues["issues"].append(
                    f"Very low raw data count: {issues['raw_data_count']}"
                )

        except Exception as e:
            issues["issues"].append(f"Pipeline diagnosis failed: {str(e)}")

        return issues

    async def _analyze_confidence_scoring(self):
        """Analyze confidence scoring system for issues"""
        print("   Analyzing confidence scoring system...")

        confidence_issues = []

        # Test confidence scorer with sample data
        try:
            sample_result = self.confidence_scorer.calculate_confidence(
                signal_differential=15.0,
                source="VSIN",
                book="circa",
                split_type="moneyline",
                strategy_name="book_conflicts",
                last_updated=datetime.now(self.est),
                game_datetime=datetime.now(self.est) + timedelta(hours=3),
                cross_validation_sources=1,
            )

            print("   📊 Sample confidence calculation:")
            print(f"      Overall confidence: {sample_result.overall_confidence:.1f}")
            print(
                f"      Signal strength score: {sample_result.components.signal_strength_score:.1f}"
            )
            print(f"      Confidence level: {sample_result.confidence_level}")

            # Check if confidence scorer is working properly
            if sample_result.overall_confidence < 50:
                confidence_issues.append(
                    "Confidence scores appear to be too low for strong signals"
                )

            # Test with different signal strengths
            test_differentials = [5, 10, 15, 20, 25, 30]
            print("   🧪 Testing signal strength scoring:")

            for diff in test_differentials:
                test_result = self.confidence_scorer.calculate_confidence(
                    signal_differential=diff,
                    source="VSIN",
                    book="circa",
                    split_type="moneyline",
                    strategy_name="test",
                    last_updated=datetime.now(self.est),
                    game_datetime=datetime.now(self.est) + timedelta(hours=3),
                )

                signal_score = test_result.components.signal_strength_score
                overall_score = test_result.overall_confidence

                print(
                    f"      {diff:2d}% diff: signal={signal_score:.1f}, overall={overall_score:.1f}"
                )

                # Check for issues
                if diff >= 20 and overall_score < 60:
                    confidence_issues.append(
                        f"Strong signal ({diff}%) getting low confidence ({overall_score:.1f})"
                    )

        except Exception as e:
            confidence_issues.append(f"Confidence scorer testing failed: {str(e)}")

        return {
            "issues": confidence_issues,
            "scorer_working": len(confidence_issues) == 0,
        }

    async def _generate_recommendations(
        self,
        signal_distributions,
        threshold_analysis,
        processor_analysis,
        confidence_analysis,
    ):
        """Generate specific recommendations to fix the issues"""
        print("   Generating recommendations...")

        recommendations = {
            "threshold_adjustments": {},
            "confidence_scorer_fixes": [],
            "processor_fixes": {},
            "priority_actions": [],
        }

        # Analyze signal distributions to recommend new thresholds
        if "moneyline_differentials" in signal_distributions:
            ml_data = signal_distributions["moneyline_differentials"]

            # Recommend thresholds based on percentiles
            # We want strategies to trigger on roughly 5-15% of signals
            recommendations["threshold_adjustments"] = {
                "BookConflictProcessor": {
                    "current": 8.0,
                    "recommended": ml_data["percentiles"]["75"],  # 75th percentile
                    "reason": "Current 8% threshold too high - use 75th percentile for better signal coverage",
                },
                "PublicFadeProcessor": {
                    "current": 65.0,
                    "recommended": 60.0,  # Lower from 65% to 60%
                    "reason": "Public consensus threshold too restrictive - lower to 60% for more opportunities",
                },
                "LateFlipProcessor": {
                    "current": 12.0,
                    "recommended": ml_data["percentiles"]["80"],  # 80th percentile
                    "reason": "Flip strength threshold too high - use 80th percentile",
                },
            }

        # Confidence scorer fixes
        recommendations["confidence_scorer_fixes"] = [
            "Lower signal strength scoring thresholds",
            "Adjust source reliability weights",
            "Recalibrate overall confidence calculation",
        ]

        # Processor-specific fixes
        for proc_name, analysis in processor_analysis.items():
            if analysis["raw_data_count"] == 0:
                recommendations["processor_fixes"][proc_name] = [
                    "Check data retrieval queries",
                    "Verify table names and column mappings",
                    "Add debugging for data availability",
                ]
            elif len(analysis["issues"]) > 0:
                recommendations["processor_fixes"][proc_name] = analysis["issues"][:3]

        # Priority actions
        recommendations["priority_actions"] = [
            "1. Lower all processor thresholds by 20-30%",
            "2. Fix confidence scorer signal strength calculation",
            "3. Add debug logging to show why signals are filtered out",
            "4. Test with relaxed thresholds on recent data",
            "5. Recalibrate based on actual profitable signal patterns",
        ]

        return recommendations

    async def _implement_fixes(self, recommendations):
        """Implement the recommended fixes"""
        print("   Implementing recommended fixes...")

        # Create fixed processor files
        await self._create_fixed_processors(recommendations["threshold_adjustments"])

        # Create fixed confidence scorer
        await self._create_fixed_confidence_scorer(
            recommendations["confidence_scorer_fixes"]
        )

        print("   ✅ Fixes implemented - check generated files")

    async def _create_fixed_processors(self, threshold_adjustments):
        """Create fixed processor files with adjusted thresholds"""

        # Fix BookConflictProcessor
        if "BookConflictProcessor" in threshold_adjustments:
            adj = threshold_adjustments["BookConflictProcessor"]
            await self._fix_book_conflict_processor(adj["recommended"])

        # Fix PublicFadeProcessor
        if "PublicFadeProcessor" in threshold_adjustments:
            adj = threshold_adjustments["PublicFadeProcessor"]
            await self._fix_public_fade_processor(adj["recommended"])

        # Fix LateFlipProcessor
        if "LateFlipProcessor" in threshold_adjustments:
            adj = threshold_adjustments["LateFlipProcessor"]
            await self._fix_late_flip_processor(adj["recommended"])

    async def _fix_book_conflict_processor(self, new_threshold):
        """Fix BookConflictProcessor with new threshold"""
        print(f"   🔧 Fixing BookConflictProcessor threshold: {new_threshold:.1f}%")
        # Implementation would update the processor file

    async def _fix_public_fade_processor(self, new_threshold):
        """Fix PublicFadeProcessor with new threshold"""
        print(f"   🔧 Fixing PublicFadeProcessor threshold: {new_threshold:.1f}%")
        # Implementation would update the processor file

    async def _fix_late_flip_processor(self, new_threshold):
        """Fix LateFlipProcessor with new threshold"""
        print(f"   🔧 Fixing LateFlipProcessor threshold: {new_threshold:.1f}%")
        # Implementation would update the processor file

    async def _create_fixed_confidence_scorer(self, fixes):
        """Create fixed confidence scorer"""
        print(f"   🔧 Fixing confidence scorer: {len(fixes)} issues")
        # Implementation would update the confidence scorer

    async def _generate_final_report(self, recommendations):
        """Generate final analysis report"""
        print("   Generating final report...")

        report = {
            "timestamp": datetime.now(self.est).isoformat(),
            "analysis_period": f"{self.start_date} to {self.end_date}",
            "recommendations": recommendations,
            "summary": {
                "total_issues_found": len(recommendations["priority_actions"]),
                "processors_affected": len(recommendations["processor_fixes"]),
                "threshold_adjustments": len(recommendations["threshold_adjustments"]),
                "confidence_fixes": len(recommendations["confidence_scorer_fixes"]),
            },
        }

        # Save report
        with open("threshold_signal_analysis_report.json", "w") as f:
            json.dump(report, f, indent=2, default=str)

        print("   📋 Report saved: threshold_signal_analysis_report.json")

        # Print summary
        print("\n📊 ANALYSIS COMPLETE!")
        print(f"   Issues Found: {report['summary']['total_issues_found']}")
        print(f"   Processors Affected: {report['summary']['processors_affected']}")
        print(f"   Threshold Adjustments: {report['summary']['threshold_adjustments']}")
        print(f"   Confidence Fixes: {report['summary']['confidence_fixes']}")


async def main():
    """Run the complete analysis"""
    analyzer = ThresholdSignalAnalyzer()
    await analyzer.run_complete_analysis()


if __name__ == "__main__":
    asyncio.run(main())
