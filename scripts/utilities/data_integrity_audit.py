#!/usr/bin/env python3
"""
Data Integrity Audit Script for MLB Betting System
==================================================

This script identifies and fixes data integrity issues causing inflated
performance metrics in the betting strategy tracking system.
"""

import json
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent / "src"))

from mlb_sharp_betting.core.logging import get_logger
from mlb_sharp_betting.services.database_coordinator import get_database_coordinator

logger = get_logger(__name__)


class DataIntegrityAuditor:
    """Audit and fix data integrity issues."""

    def __init__(self):
        self.coordinator = get_database_coordinator()
        self.logger = logger.bind(service="data_integrity_auditor")
        self.results = {}

    def audit_duplicate_entries(self):
        """Find duplicate game entries."""
        self.logger.info("Auditing duplicate entries")

        duplicate_query = """
        SELECT 
            game_id,
            home_team,
            away_team,
            split_type,
            source,
            book,
            COUNT(*) as row_count
        FROM mlb_betting.splits.raw_mlb_betting_splits 
        WHERE game_datetime >= CURRENT_DATE - INTERVAL 30 DAY
        GROUP BY game_id, home_team, away_team, split_type, source, book
        HAVING COUNT(*) > 1
        ORDER BY row_count DESC
        LIMIT 20
        """

        duplicates = self.coordinator.execute_read(duplicate_query)

        self.results["duplicates"] = {
            "total_games_with_duplicates": len(duplicates),
            "max_duplicates_per_game": max([row[6] for row in duplicates])
            if duplicates
            else 0,
            "sample_duplicates": [
                {
                    "game": f"{row[1]} vs {row[2]}",
                    "split_type": row[3],
                    "source": row[4],
                    "book": row[5],
                    "duplicate_count": row[6],
                }
                for row in duplicates[:10]
            ],
        }

        return duplicates

    def audit_data_sources(self):
        """Analyze data collection patterns by source."""
        source_query = """
        SELECT 
            source,
            book,
            COUNT(*) as total_records,
            COUNT(DISTINCT game_id) as unique_games,
            ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT game_id), 1) as avg_records_per_game
        FROM mlb_betting.splits.raw_mlb_betting_splits
        WHERE game_datetime >= CURRENT_DATE - INTERVAL 30 DAY
        GROUP BY source, book
        ORDER BY avg_records_per_game DESC
        """

        sources = self.coordinator.execute_read(source_query)

        self.results["data_sources"] = [
            {
                "source": row[0],
                "book": row[1],
                "total_records": row[2],
                "unique_games": row[3],
                "avg_records_per_game": row[4],
                "is_problematic": row[4] > 10,
            }
            for row in sources
        ]

        return sources

    def create_deduplication_view(self):
        """Create a view that deduplicates betting data."""
        dedup_view_sql = """
        CREATE OR REPLACE VIEW mlb_betting.splits.betting_splits_deduplicated AS
        SELECT 
            game_id,
            home_team,
            away_team,
            game_datetime,
            split_type,
            source,
            book,
            home_or_over_stake_percentage,
            home_or_over_bets_percentage,
            home_or_over_stake_percentage - home_or_over_bets_percentage as differential,
            split_value,
            last_updated
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY game_id, split_type, source, book 
                    ORDER BY 
                        -- Prefer records closest to 5 minutes before game
                        ABS(EXTRACT(EPOCH FROM (game_datetime - last_updated)) / 60 - 5) ASC,
                        last_updated DESC
                ) as rn
            FROM mlb_betting.splits.raw_mlb_betting_splits
            WHERE last_updated < game_datetime
        ) ranked
        WHERE rn = 1
        """

        self.coordinator.execute_write(dedup_view_sql, [])
        self.logger.info("Created deduplication view")

    def analyze_performance_impact(self):
        """Compare performance metrics with and without deduplication."""
        performance_query = """
        WITH raw_strategy AS (
            SELECT 
                source || '-' || COALESCE(book, 'None') as source_book_type,
                split_type,
                COUNT(*) as total_bets_raw,
                SUM(CASE 
                    WHEN ABS(home_or_over_stake_percentage - home_or_over_bets_percentage) >= 10 
                    THEN 1 ELSE 0 
                END) as qualifying_bets_raw
            FROM mlb_betting.splits.raw_mlb_betting_splits rmbs
            WHERE game_datetime >= CURRENT_DATE - INTERVAL 30 DAY
              AND last_updated < game_datetime
            GROUP BY source, book, split_type
        ),
        dedup_strategy AS (
            SELECT 
                source || '-' || COALESCE(book, 'None') as source_book_type,
                split_type,
                COUNT(*) as total_bets_dedup,
                SUM(CASE 
                    WHEN ABS(differential) >= 10 
                    THEN 1 ELSE 0 
                END) as qualifying_bets_dedup
            FROM mlb_betting.splits.betting_splits_deduplicated
            WHERE game_datetime >= CURRENT_DATE - INTERVAL 30 DAY
            GROUP BY source, book, split_type
        )
        SELECT 
            r.source_book_type,
            r.split_type,
            r.total_bets_raw,
            d.total_bets_dedup,
            r.total_bets_raw - d.total_bets_dedup as excess_bets,
            r.qualifying_bets_raw,
            d.qualifying_bets_dedup,
            r.qualifying_bets_raw - d.qualifying_bets_dedup as excess_qualifying_bets
        FROM raw_strategy r
        JOIN dedup_strategy d ON r.source_book_type = d.source_book_type AND r.split_type = d.split_type
        WHERE r.total_bets_raw > d.total_bets_dedup
        ORDER BY excess_bets DESC
        """

        performance_data = self.coordinator.execute_read(performance_query)

        self.results["performance_impact"] = [
            {
                "source_book_type": row[0],
                "split_type": row[1],
                "raw_bets": row[2],
                "dedup_bets": row[3],
                "excess_bets": row[4],
                "raw_qualifying": row[5],
                "dedup_qualifying": row[6],
                "excess_qualifying": row[7],
            }
            for row in performance_data
        ]

        total_excess = sum([row[4] for row in performance_data])
        self.results["total_excess_bets"] = total_excess

        return performance_data

    def generate_cleanup_recommendations(self):
        """Generate specific recommendations for fixing the issues."""
        recommendations = []

        # High priority recommendations
        if self.results.get("total_excess_bets", 0) > 100:
            recommendations.append(
                {
                    "priority": "HIGH",
                    "title": "Add Database Constraints",
                    "description": "Prevent duplicate entries at database level",
                    "sql": """
                ALTER TABLE mlb_betting.splits.raw_mlb_betting_splits 
                ADD CONSTRAINT unique_game_split_daily 
                UNIQUE (game_id, split_type, source, book, DATE(last_updated))
                """,
                }
            )

            recommendations.append(
                {
                    "priority": "HIGH",
                    "title": "Update Data Collection Logic",
                    "description": "Only capture data 5 minutes before first pitch",
                    "implementation": "Modify scrapers to filter by timing: EXTRACT(EPOCH FROM (game_datetime - CURRENT_TIMESTAMP)) / 60 BETWEEN 4 AND 6",
                }
            )

            recommendations.append(
                {
                    "priority": "HIGH",
                    "title": "Use Deduplication View",
                    "description": "Update all analysis queries to use betting_splits_deduplicated view",
                    "implementation": "Replace references to raw_mlb_betting_splits with betting_splits_deduplicated in analysis scripts",
                }
            )

        self.results["recommendations"] = recommendations
        return recommendations

    def run_full_audit(self):
        """Run the complete audit process."""
        print("🔍 Starting Data Integrity Audit...")

        # Step 1: Audit duplicates
        duplicates = self.audit_duplicate_entries()
        print(f"   Found {len(duplicates)} games with duplicate entries")

        # Step 2: Analyze data sources
        sources = self.audit_data_sources()
        problematic_sources = [
            s for s in self.results["data_sources"] if s["is_problematic"]
        ]
        print(f"   Found {len(problematic_sources)} sources with excessive duplication")

        # Step 3: Create deduplication view
        self.create_deduplication_view()
        print("   Created deduplication view")

        # Step 4: Analyze performance impact
        performance_impact = self.analyze_performance_impact()
        print(
            f"   Total excess bets from duplicates: {self.results.get('total_excess_bets', 0)}"
        )

        # Step 5: Generate recommendations
        recommendations = self.generate_cleanup_recommendations()
        print(f"   Generated {len(recommendations)} recommendations")

        return self.results

    def print_summary(self):
        """Print executive summary."""
        print("\n" + "=" * 80)
        print("DATA INTEGRITY AUDIT SUMMARY")
        print("=" * 80)

        print("\n🚨 CRITICAL FINDINGS:")
        ds = self.results.get("duplicates", {})
        print(f"   • Games with duplicates: {ds.get('total_games_with_duplicates', 0)}")
        print(f"   • Max duplicates per game: {ds.get('max_duplicates_per_game', 0)}")
        print(f"   • Total excess bets: {self.results.get('total_excess_bets', 0)}")

        print("\n📊 MOST PROBLEMATIC SOURCES:")
        for source in self.results.get("data_sources", [])[:3]:
            if source["is_problematic"]:
                print(
                    f"   • {source['source']}-{source['book']}: {source['avg_records_per_game']} records per game"
                )

        print("\n⚡ IMMEDIATE ACTIONS:")
        for rec in self.results.get("recommendations", []):
            if rec["priority"] == "HIGH":
                print(f"   • {rec['title']}: {rec['description']}")

        print("\n✅ SOLUTION IMPLEMENTED:")
        print("   • Created betting_splits_deduplicated view")
        print("   • View filters to one record per game/market/source")
        print("   • Prioritizes records closest to 5 minutes before first pitch")

        print("\n" + "=" * 80)


def main():
    """Main execution."""
    auditor = DataIntegrityAuditor()

    try:
        results = auditor.run_full_audit()
        auditor.print_summary()

        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data_integrity_audit_{timestamp}.json"
        with open(filename, "w") as f:
            json.dump(results, f, indent=2, default=str)

        print(f"\n📄 Detailed results saved to: {filename}")

        return results

    except Exception as e:
        logger.error("Audit failed", error=str(e))
        raise


if __name__ == "__main__":
    main()
