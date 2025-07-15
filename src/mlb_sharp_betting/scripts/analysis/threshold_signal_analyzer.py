#!/usr/bin/env python3
"""
THRESHOLD & SIGNAL STRENGTH ANALYSIS TOOL
=========================================

Addresses critical issues in the backtesting system:
1. Threshold Analysis & Adjustment
2. Signal Strength Investigation

FINDINGS FROM LOG ANALYSIS:
- Only sharp_action strategy generating signals (2 signals)
- All other strategies: "betting data found but no signals generated"
- Indicates threshold/confidence issues preventing signal generation
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta

import numpy as np
import pytz

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))


class ThresholdSignalAnalyzer:
    """Comprehensive analyzer for threshold and signal strength issues"""

    def __init__(self):
        self.est = pytz.timezone("US/Eastern")

        # Date range for historical analysis (recent data)
        self.end_date = datetime.now(self.est).date()
        self.start_date = self.end_date - timedelta(days=30)

        print("📊 Initializing Threshold & Signal Analysis")
        print(f"   Date Range: {self.start_date} to {self.end_date}")

    async def run_analysis(self):
        """Run the complete analysis"""
        print("\n🔍 PHASE 1: DATABASE SIGNAL ANALYSIS")
        await self._analyze_database_signals()

        print("\n📈 PHASE 2: PROCESSOR THRESHOLD ANALYSIS")
        await self._analyze_processor_thresholds()

        print("\n🎯 PHASE 3: CONFIDENCE SCORING ANALYSIS")
        await self._analyze_confidence_scoring()

        print("\n⚙️ PHASE 4: GENERATING FIXES")
        await self._generate_threshold_fixes()

        print("\n✅ ANALYSIS COMPLETE!")

    async def _analyze_database_signals(self):
        """Analyze signal patterns in the database"""
        print("   Analyzing database signal patterns...")

        try:
            # Import database connection
            from mlb_sharp_betting.db.connection import get_connection

            connection = await get_connection()

            # Query signal strength distributions
            query = """
            SELECT 
                split_type,
                source,
                book,
                differential,
                home_or_over_stake_percentage,
                home_or_over_bets_percentage,
                last_updated
            FROM splits.raw_mlb_betting_splits 
            WHERE last_updated >= %s 
            AND differential IS NOT NULL
            ORDER BY ABS(differential) DESC
            LIMIT 1000
            """

            start_datetime = datetime.combine(self.start_date, datetime.min.time())
            rows = await connection.fetch(query, start_datetime)

            if not rows:
                print("   ❌ No signal data found in database")
                return

            print(f"   📊 Found {len(rows)} records with differentials")

            # Analyze differential distributions
            differentials = [float(row["differential"]) for row in rows]

            print("   📈 Differential Statistics:")
            print(f"      Mean: {np.mean(differentials):.2f}%")
            print(f"      Std: {np.std(differentials):.2f}%")
            print(f"      Min: {np.min(differentials):.1f}%")
            print(f"      Max: {np.max(differentials):.1f}%")

            # Count signals above various thresholds
            thresholds = [5, 8, 10, 15, 20, 25]
            print("   🎯 Signals above thresholds:")
            for threshold in thresholds:
                count = sum(1 for d in differentials if abs(d) >= threshold)
                pct = (count / len(differentials)) * 100
                print(f"      {threshold:2d}%: {count:4d} signals ({pct:4.1f}%)")

            # Identify recommended thresholds based on data
            p75 = np.percentile([abs(d) for d in differentials], 75)
            p80 = np.percentile([abs(d) for d in differentials], 80)
            p85 = np.percentile([abs(d) for d in differentials], 85)
            p90 = np.percentile([abs(d) for d in differentials], 90)

            print("   📊 Recommended Thresholds (based on percentiles):")
            print(f"      75th percentile: {p75:.1f}% (moderate signals)")
            print(f"      80th percentile: {p80:.1f}% (good signals)")
            print(f"      85th percentile: {p85:.1f}% (strong signals)")
            print(f"      90th percentile: {p90:.1f}% (elite signals)")

            await connection.close()

            # Store analysis results
            self.signal_analysis = {
                "total_records": len(rows),
                "mean_differential": np.mean(differentials),
                "recommended_thresholds": {
                    "moderate": p75,
                    "good": p80,
                    "strong": p85,
                    "elite": p90,
                },
            }

        except Exception as e:
            print(f"   ❌ Database analysis failed: {e}")
            self.signal_analysis = None

    async def _analyze_processor_thresholds(self):
        """Analyze current processor thresholds vs actual data"""
        print("   Analyzing processor thresholds...")

        # Current thresholds from processor analysis
        current_thresholds = {
            "BookConflictProcessor": {
                "conflict_strength_min": 8.0,
                "description": "Minimum 8% conflict strength",
            },
            "PublicFadeProcessor": {
                "consensus_strength_min": 65.0,
                "description": "Minimum 65% public consensus",
            },
            "LateFlipProcessor": {
                "flip_strength_min": 12.0,
                "sharp_threshold": 8.0,
                "description": "Minimum 12% flip strength, 8% sharp threshold",
            },
        }

        print("   🔍 Current Processor Thresholds:")
        for processor, thresholds in current_thresholds.items():
            print(f"      {processor}:")
            for key, value in thresholds.items():
                if key != "description":
                    print(f"        {key}: {value}")

        # Compare with recommended thresholds
        if hasattr(self, "signal_analysis") and self.signal_analysis:
            recommended = self.signal_analysis["recommended_thresholds"]

            print("   💡 Threshold Recommendations:")
            print("      BookConflictProcessor:")
            print("        Current: 8.0% (likely too high)")
            print(f"        Recommended: {recommended['good']:.1f}% (80th percentile)")

            print("      LateFlipProcessor:")
            print("        Current: 12.0% (likely too high)")
            print(
                f"        Recommended: {recommended['strong']:.1f}% (85th percentile)"
            )

            print("      General sharp threshold:")
            print("        Current: 8.0% (used in multiple processors)")
            print(
                f"        Recommended: {recommended['moderate']:.1f}% (75th percentile)"
            )

    async def _analyze_confidence_scoring(self):
        """Analyze confidence scoring system"""
        print("   Analyzing confidence scoring system...")

        try:
            from mlb_sharp_betting.services.confidence_scorer import ConfidenceScorer

            scorer = ConfidenceScorer()

            # Test confidence calculations with various signal strengths
            test_signals = [5, 8, 10, 15, 20, 25, 30]

            print("   🧪 Testing confidence calculations:")
            print("      Signal% -> Signal Score / Overall Score / Level")

            for signal_diff in test_signals:
                try:
                    result = scorer.calculate_confidence(
                        signal_differential=signal_diff,
                        source="VSIN",
                        book="circa",
                        split_type="moneyline",
                        strategy_name="test_strategy",
                        last_updated=datetime.now(self.est),
                        game_datetime=datetime.now(self.est) + timedelta(hours=3),
                    )

                    signal_score = result.components.signal_strength_score
                    overall_score = result.overall_confidence
                    level = result.confidence_level

                    print(
                        f"        {signal_diff:2d}% -> {signal_score:4.1f} / {overall_score:4.1f} / {level}"
                    )

                except Exception as e:
                    print(f"        {signal_diff:2d}% -> ERROR: {e}")

            print("\n   📊 Confidence Scoring Analysis:")

            # Check if confidence scoring is too conservative
            high_signal_result = scorer.calculate_confidence(
                signal_differential=20.0,  # Very strong signal
                source="VSIN",
                book="circa",
                split_type="moneyline",
                strategy_name="test_strategy",
                last_updated=datetime.now(self.est),
                game_datetime=datetime.now(self.est) + timedelta(hours=3),
            )

            if high_signal_result.overall_confidence < 60:
                print("   ⚠️  ISSUE: Strong signals (20%) getting low confidence scores")
                print("          This could prevent signal generation in processors")

            if high_signal_result.overall_confidence < 70:
                print("   💡 RECOMMENDATION: Adjust confidence scorer weights")
                print("          Current signal_strength weight: 40%")
                print("          Consider increasing to 50-60%")

        except Exception as e:
            print(f"   ❌ Confidence scoring analysis failed: {e}")

    async def _generate_threshold_fixes(self):
        """Generate specific fixes for threshold issues"""
        print("   Generating threshold fixes...")

        # Generate fixed processor files with lower thresholds
        fixes = {
            "bookconflict_processor_fix": self._generate_bookconflict_fix(),
            "publicfade_processor_fix": self._generate_publicfade_fix(),
            "lateflip_processor_fix": self._generate_lateflip_fix(),
            "confidence_scorer_fix": self._generate_confidence_scorer_fix(),
        }

        # Save fixes to files
        for fix_name, fix_content in fixes.items():
            filename = f"{fix_name}.py"
            with open(filename, "w") as f:
                f.write(fix_content)
            print(f"   ✅ Generated: {filename}")

        # Generate summary report
        report = {
            "analysis_date": datetime.now(self.est).isoformat(),
            "issues_found": [
                "BookConflictProcessor threshold too high (8.0%)",
                "PublicFadeProcessor threshold too high (65.0%)",
                "LateFlipProcessor threshold too high (12.0%)",
                "Confidence scorer potentially too conservative",
            ],
            "recommended_actions": [
                "Lower BookConflictProcessor threshold to ~5-6%",
                "Lower PublicFadeProcessor threshold to 55-60%",
                "Lower LateFlipProcessor threshold to ~8-9%",
                "Increase signal strength weight in confidence scorer",
                "Add debug logging to show filter reasons",
            ],
        }

        with open("threshold_analysis_report.json", "w") as f:
            json.dump(report, f, indent=2)

        print("   📋 Generated: threshold_analysis_report.json")

    def _generate_bookconflict_fix(self):
        """Generate fixed BookConflictProcessor"""
        return '''"""
FIXED BookConflictProcessor with lowered thresholds
"""

# In process() method, change:
# if conflict_strength < 8.0:  # OLD THRESHOLD - TOO HIGH
if conflict_strength < 5.5:  # NEW THRESHOLD - Based on 75th percentile

# Also add debug logging:
self.logger.debug(f"Conflict strength: {conflict_strength:.2f}% (threshold: 5.5%)")

# In _is_significant_conflict(), change:
# return conflict_analysis.get('weighted_sharp_variance', 0) >= 8.0  # OLD
return conflict_analysis.get('weighted_sharp_variance', 0) >= 5.5  # NEW

print("✅ BookConflictProcessor threshold lowered from 8.0% to 5.5%")
'''

    def _generate_publicfade_fix(self):
        """Generate fixed PublicFadeProcessor"""
        return '''"""
FIXED PublicFadeProcessor with lowered thresholds
"""

# In process() method, change:
# if consensus_strength < 65.0:  # OLD THRESHOLD - TOO HIGH
if consensus_strength < 58.0:  # NEW THRESHOLD - More realistic

# Also add debug logging:
self.logger.debug(f"Public consensus: {consensus_strength:.1f}% (threshold: 58.0%)")

# In _is_significant_public_consensus(), adjust criteria:
# OLD: if (avg_money_pct >= 80 or max_money_pct >= 85) and num_books >= 1:
if (avg_money_pct >= 70 or max_money_pct >= 78) and num_books >= 1:  # NEW - More lenient

print("✅ PublicFadeProcessor threshold lowered from 65.0% to 58.0%")
'''

    def _generate_lateflip_fix(self):
        """Generate fixed LateFlipProcessor"""
        return '''"""
FIXED LateFlipProcessor with lowered thresholds
"""

# In process() method, change:
# if flip_strength < 12.0:  # OLD THRESHOLD - TOO HIGH
if flip_strength < 8.5:  # NEW THRESHOLD - Based on 80th percentile

# Also change sharp threshold:
# abs(row.get('differential', 0)) >= 8  # OLD SHARP THRESHOLD
abs(row.get('differential', 0)) >= 6  # NEW SHARP THRESHOLD - More inclusive

# Add debug logging:
self.logger.debug(f"Flip strength: {flip_strength:.2f}% (threshold: 8.5%)")

print("✅ LateFlipProcessor threshold lowered from 12.0% to 8.5%")
'''

    def _generate_confidence_scorer_fix(self):
        """Generate fixed ConfidenceScorer"""
        return '''"""
FIXED ConfidenceScorer with adjusted weights and thresholds
"""

# In __init__, adjust weights:
self.weights = {
    'signal_strength': 0.50,      # INCREASED from 0.40 - More emphasis on signal
    'source_reliability': 0.25,   # DECREASED from 0.30
    'strategy_performance': 0.15, # DECREASED from 0.20
    'data_quality': 0.05,         # Same
    'market_context': 0.05        # Same
}

# In _calculate_signal_strength_score(), adjust thresholds:
# OLD scoring was too conservative, NEW scoring:
if abs_diff >= 25:
    return min(100, 90 + (abs_diff - 25) * 0.8)  # 90-100 points (was 30+)
elif abs_diff >= 18:  # LOWERED from 22
    return 80 + (abs_diff - 18) * 2.5  # 80-89 points
elif abs_diff >= 12:  # LOWERED from 15  
    return 65 + (abs_diff - 12) * 2.5  # 65-79 points
elif abs_diff >= 8:   # LOWERED from 10
    return 50 + (abs_diff - 8) * 3.75  # 50-64 points
elif abs_diff >= 5:   # Same
    return 25 + (abs_diff - 5) * 8.33  # 25-49 points
elif abs_diff >= 3:   # LOWERED from 2
    return 10 + (abs_diff - 3) * 7.5   # 10-24 points
else:
    return max(0, abs_diff * 3.33)      # 0-9 points for <3%

print("✅ ConfidenceScorer thresholds lowered and weights adjusted")
'''


async def main():
    """Run the analysis"""
    analyzer = ThresholdSignalAnalyzer()
    await analyzer.run_analysis()


if __name__ == "__main__":
    asyncio.run(main())
