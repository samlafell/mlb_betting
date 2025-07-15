#!/usr/bin/env python3
"""
Database Signal Analysis
Analyzes actual signal distributions to determine optimal thresholds
"""

import asyncio

import asyncpg
import numpy as np


async def analyze_database_signals():
    """Analyze actual signal distributions in the database"""
    try:
        # Connect to database
        conn = await asyncpg.connect(
            "postgresql://sam:password123@localhost:5432/mlb_betting"
        )
        print("✅ Connected to database")

        # Get signal distributions
        query = """
        SELECT 
            split_type,
            differential,
            home_or_over_stake_percentage,
            home_or_over_bets_percentage,
            source,
            book
        FROM splits.raw_mlb_betting_splits 
        WHERE differential IS NOT NULL
        AND last_updated >= NOW() - INTERVAL '30 days'
        ORDER BY ABS(differential) DESC
        LIMIT 2000
        """

        rows = await conn.fetch(query)
        print(f"📊 Found {len(rows)} signal records from last 30 days")

        if not rows:
            print("❌ No signal data found")
            return

        # Extract differentials
        differentials = [float(row["differential"]) for row in rows]
        abs_diffs = [abs(d) for d in differentials]

        print("\n📈 Signal Strength Distribution Analysis:")
        print(f"  Total signals: {len(differentials):,}")
        print(f"  Mean differential: {np.mean(differentials):.2f}%")
        print(f"  Std deviation: {np.std(differentials):.2f}%")
        print(f"  Range: {min(differentials):.1f}% to {max(differentials):.1f}%")

        # Key percentiles for threshold setting
        p25 = np.percentile(abs_diffs, 25)
        p50 = np.percentile(abs_diffs, 50)
        p75 = np.percentile(abs_diffs, 75)
        p80 = np.percentile(abs_diffs, 80)
        p85 = np.percentile(abs_diffs, 85)
        p90 = np.percentile(abs_diffs, 90)
        p95 = np.percentile(abs_diffs, 95)

        print("\n📊 Percentile-Based Threshold Recommendations:")
        print(f"  25th percentile: {p25:.1f}% (very inclusive)")
        print(f"  50th percentile: {p50:.1f}% (median signal)")
        print(
            f"  75th percentile: {p75:.1f}% (good signals - RECOMMENDED for most processors)"
        )
        print(f"  80th percentile: {p80:.1f}% (strong signals)")
        print(f"  85th percentile: {p85:.1f}% (very strong)")
        print(f"  90th percentile: {p90:.1f}% (elite signals)")
        print(f"  95th percentile: {p95:.1f}% (premium signals)")

        # Count signals above various thresholds
        print("\n🎯 Signal Coverage Analysis (Current vs Recommended):")
        thresholds = [3, 5, 6, 7, 8, 10, 12, 15, 20]
        for threshold in thresholds:
            count = sum(1 for d in abs_diffs if d >= threshold)
            pct = (count / len(abs_diffs)) * 100
            status = ""

            if threshold == 8:
                status = " ← CURRENT BookConflict/Sharp threshold"
            elif threshold == 12:
                status = " ← CURRENT LateFlip threshold"
            elif abs(threshold - p75) < 1:
                status = " ← RECOMMENDED (75th percentile)"

            print(
                f"  {threshold:2d}%: {count:4d} signals ({pct:4.1f}% coverage){status}"
            )

        print("\n🔍 CURRENT THRESHOLD ANALYSIS:")
        current_8_pct = (sum(1 for d in abs_diffs if d >= 8) / len(abs_diffs)) * 100
        current_12_pct = (sum(1 for d in abs_diffs if d >= 12) / len(abs_diffs)) * 100
        current_65_signals = sum(
            1
            for row in rows
            if row["home_or_over_stake_percentage"]
            and row["home_or_over_stake_percentage"] >= 65
        )

        print(f"  BookConflict (8.0%): Only {current_8_pct:.1f}% of signals qualify")
        print(f"  LateFlip (12.0%): Only {current_12_pct:.1f}% of signals qualify")
        print(f"  PublicFade (65% consensus): {current_65_signals} qualifying records")

        print("\n💡 RECOMMENDED NEW THRESHOLDS:")
        print(f"  BookConflictProcessor: {p75:.1f}% (down from 8.0%)")
        print(f"  LateFlipProcessor: {p80:.1f}% (down from 12.0%)")
        print(f"  General sharp threshold: {p75:.1f}% (down from 8.0%)")
        print("  PublicFadeProcessor: 58% consensus (down from 65%)")

        # Split type analysis
        print("\n📊 Split Type Breakdown:")
        split_types = {}
        for row in rows:
            st = row["split_type"]
            if st not in split_types:
                split_types[st] = []
            split_types[st].append(abs(float(row["differential"])))

        for split_type, diffs in split_types.items():
            if len(diffs) > 10:  # Only show types with decent sample size
                p75_type = np.percentile(diffs, 75)
                print(
                    f"  {split_type}: {len(diffs)} signals, 75th percentile = {p75_type:.1f}%"
                )

        await conn.close()

        # Generate specific recommendations
        recommendations = {
            "issues_identified": [
                f"BookConflictProcessor threshold (8.0%) only catches {current_8_pct:.1f}% of signals",
                f"LateFlipProcessor threshold (12.0%) only catches {current_12_pct:.1f}% of signals",
                "Thresholds are set too high for real-world signal distributions",
                "Confidence scorer may be too conservative",
            ],
            "recommended_fixes": {
                "BookConflictProcessor": {
                    "old_threshold": 8.0,
                    "new_threshold": round(p75, 1),
                    "expected_improvement": f"Will catch ~25% of signals instead of {current_8_pct:.1f}%",
                },
                "LateFlipProcessor": {
                    "old_threshold": 12.0,
                    "new_threshold": round(p80, 1),
                    "expected_improvement": f"Will catch ~20% of signals instead of {current_12_pct:.1f}%",
                },
                "PublicFadeProcessor": {
                    "old_threshold": 65.0,
                    "new_threshold": 58.0,
                    "expected_improvement": "More realistic public consensus detection",
                },
            },
        }

        return recommendations

    except Exception as e:
        print(f"❌ Database analysis failed: {e}")
        return None


async def main():
    """Run the database analysis"""
    print("🔍 Analyzing actual signal distributions in database...")
    recommendations = await analyze_database_signals()

    if recommendations:
        print(
            "\n✅ Analysis complete! Check output above for specific threshold recommendations."
        )
    else:
        print("\n❌ Analysis failed - check database connection and data availability.")


if __name__ == "__main__":
    asyncio.run(main())
