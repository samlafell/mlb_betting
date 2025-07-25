#!/usr/bin/env python3
"""
Phase 1: Data Analysis and Mapping

Analyzes existing core_betting data and creates mapping strategy for migration
to the RAW → STAGING → CURATED pipeline architecture.

This script provides comprehensive analysis of:
- Data volumes and distributions
- Data quality assessment
- Source attribution mapping
- Migration complexity scoring
"""

import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import get_settings
from src.core.logging import LogComponent, get_logger
from src.data.database import get_connection
from src.data.database.connection import initialize_connections

logger = get_logger(__name__, LogComponent.CORE)


class DataAnalyzer:
    """Analyzes existing core_betting data for migration planning."""

    def __init__(self):
        self.settings = get_settings()

    async def initialize(self):
        """Initialize database connection."""
        initialize_connections(self.settings)

    async def close(self):
        """Close database connections."""
        pass  # Connection will be managed per operation

    async def analyze_all_data(self) -> dict[str, Any]:
        """Comprehensive data analysis for migration planning."""
        logger.info("Starting Phase 1: Comprehensive data analysis")

        analysis_results = {
            "timestamp": datetime.now().isoformat(),
            "phase": "Phase 1 - Data Analysis",
            "tables": {},
            "summary": {},
            "migration_recommendations": [],
        }

        try:
            connection_manager = get_connection()
            async with connection_manager.get_async_connection() as conn:
                # Analyze each table type
                analysis_results["tables"]["games"] = await self._analyze_games_table(
                    conn
                )
                analysis_results["tables"][
                    "moneylines"
                ] = await self._analyze_betting_table(
                    conn, "betting_lines_moneyline", "Moneyline"
                )
                analysis_results["tables"][
                    "spreads"
                ] = await self._analyze_betting_table(
                    conn, "betting_lines_spread", "Spread"
                )
                analysis_results["tables"][
                    "totals"
                ] = await self._analyze_betting_table(
                    conn, "betting_lines_totals", "Totals"
                )
                analysis_results["tables"][
                    "sharp_actions"
                ] = await self._analyze_sharp_actions(conn)

                # Generate summary and recommendations
                analysis_results["summary"] = self._generate_summary(
                    analysis_results["tables"]
                )
                analysis_results["migration_recommendations"] = (
                    self._generate_migration_recommendations(
                        analysis_results["tables"], analysis_results["summary"]
                    )
                )

        except Exception as e:
            logger.error(f"Data analysis failed: {e}")
            analysis_results["error"] = str(e)

        return analysis_results

    async def _analyze_games_table(self, conn) -> dict[str, Any]:
        """Analyze the curated.games_complete table."""
        logger.info("Analyzing games table...")

        # Basic counts and date ranges
        basic_stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total_games,
                COUNT(DISTINCT home_team) as unique_home_teams,
                COUNT(DISTINCT away_team) as unique_away_teams,
                MIN(game_date) as earliest_date,
                MAX(game_date) as latest_date,
                COUNT(DISTINCT EXTRACT(YEAR FROM game_date)) as seasons_covered,
                COUNT(*) FILTER (WHERE game_status = 'completed') as completed_games,
                COUNT(*) FILTER (WHERE game_status = 'scheduled') as scheduled_games
            FROM curated.games_complete
        """)

        # Data quality assessment
        quality_stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) FILTER (WHERE data_quality = 'HIGH') as high_quality,
                COUNT(*) FILTER (WHERE data_quality = 'MEDIUM') as medium_quality,
                COUNT(*) FILTER (WHERE data_quality = 'LOW') as low_quality,
                AVG(mlb_correlation_confidence) as avg_confidence,
                COUNT(*) FILTER (WHERE has_mlb_enrichment = true) as enriched_games
            FROM curated.games_complete
        """)

        # Source attribution
        source_stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) FILTER (WHERE action_network_game_id IS NOT NULL) as has_action_network,
                COUNT(*) FILTER (WHERE mlb_stats_api_game_id IS NOT NULL) as has_mlb_api,
                COUNT(*) FILTER (WHERE sportsbookreview_game_id IS NOT NULL) as has_sbr,
                COUNT(*) FILTER (WHERE vsin_game_id IS NOT NULL) as has_vsin
            FROM curated.games_complete
        """)

        return {
            "table_name": "curated.games_complete",
            "basic_stats": dict(basic_stats) if basic_stats else {},
            "quality_stats": dict(quality_stats) if quality_stats else {},
            "source_stats": dict(source_stats) if source_stats else {},
            "migration_target": "staging.games",
            "migration_complexity": "medium",
            "notes": "Reference data - direct migration to staging with normalization",
        }

    async def _analyze_betting_table(
        self, conn, table_name: str, bet_type: str
    ) -> dict[str, Any]:
        """Analyze a betting lines table."""
        logger.info(f"Analyzing {table_name} table...")

        # Basic statistics
        basic_stats = await conn.fetchrow(f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT game_id) as unique_games,
                COUNT(DISTINCT sportsbook_id) as unique_sportsbooks,
                COUNT(DISTINCT source) as unique_sources,
                MIN(odds_timestamp) as earliest_timestamp,
                MAX(odds_timestamp) as latest_timestamp,
                COUNT(*) FILTER (WHERE data_quality = 'HIGH') as high_quality_records
            FROM curated.{table_name}
        """)

        # Source distribution
        source_distribution = await conn.fetch(f"""
            SELECT 
                source,
                COUNT(*) as record_count,
                COUNT(DISTINCT game_id) as game_count,
                ROUND(AVG(CASE WHEN data_quality = 'HIGH' THEN 1.0 ELSE 0.0 END) * 100, 2) as quality_percentage
            FROM curated.{table_name}
            GROUP BY source
            ORDER BY record_count DESC
        """)

        # Sportsbook distribution
        sportsbook_distribution = await conn.fetch(f"""
            SELECT 
                sportsbook,
                COUNT(*) as record_count,
                COUNT(DISTINCT game_id) as game_count
            FROM curated.{table_name}
            GROUP BY sportsbook
            ORDER BY record_count DESC
            LIMIT 10
        """)

        # Data completeness analysis
        # Handle different column names for totals vs other tables
        if table_name == "betting_lines_totals":
            completeness_stats = await conn.fetchrow(f"""
                SELECT 
                    ROUND(AVG(data_completeness_score) * 100, 2) as avg_completeness_score,
                    COUNT(*) FILTER (WHERE over_bets_percentage IS NOT NULL) as has_betting_splits,
                    COUNT(*) FILTER (WHERE external_source_id IS NOT NULL) as has_external_id,
                    ROUND(AVG(source_reliability_score) * 100, 2) as avg_reliability_score
                FROM curated.{table_name}
            """)
        else:
            completeness_stats = await conn.fetchrow(f"""
                SELECT 
                    ROUND(AVG(data_completeness_score) * 100, 2) as avg_completeness_score,
                    COUNT(*) FILTER (WHERE home_bets_percentage IS NOT NULL) as has_betting_splits,
                    COUNT(*) FILTER (WHERE external_source_id IS NOT NULL) as has_external_id,
                    ROUND(AVG(source_reliability_score) * 100, 2) as avg_reliability_score
                FROM curated.{table_name}
            """)

        return {
            "table_name": f"curated.{table_name}",
            "bet_type": bet_type,
            "basic_stats": dict(basic_stats) if basic_stats else {},
            "source_distribution": [dict(row) for row in source_distribution],
            "sportsbook_distribution": [dict(row) for row in sportsbook_distribution],
            "completeness_stats": dict(completeness_stats)
            if completeness_stats
            else {},
            "migration_targets": {
                "raw": f"raw_data.{bet_type.lower()}s_raw",
                "staging": f"staging.{bet_type.lower()}s",
                "curated": f"curated.{bet_type.lower()}s",
            },
            "migration_complexity": "high",
            "notes": f"{bet_type} betting data - requires full pipeline migration",
        }

    async def _analyze_sharp_actions(self, conn) -> dict[str, Any]:
        """Analyze sharp action indicators table."""
        logger.info("Analyzing sharp action indicators...")

        try:
            basic_stats = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total_indicators,
                    COUNT(DISTINCT game_id) as games_with_indicators,
                    COUNT(DISTINCT bet_type) as bet_types_covered,
                    AVG(confidence_score) as avg_confidence_score,
                    COUNT(*) FILTER (WHERE confidence_score >= 0.7) as high_confidence_indicators
                FROM curated.sharp_action_indicators
            """)

            # Indicator type distribution
            type_distribution = await conn.fetch("""
                SELECT 
                    indicator_type,
                    COUNT(*) as count,
                    AVG(confidence_score) as avg_confidence
                FROM curated.sharp_action_indicators
                GROUP BY indicator_type
                ORDER BY count DESC
            """)

            return {
                "table_name": "curated.sharp_action_indicators",
                "basic_stats": dict(basic_stats) if basic_stats else {},
                "type_distribution": [dict(row) for row in type_distribution],
                "migration_target": "curated.betting_analysis",
                "migration_complexity": "medium",
                "notes": "Analysis data - migrate to curated zone for enhanced features",
            }

        except Exception as e:
            logger.warning(f"Sharp action analysis failed: {e}")
            return {
                "table_name": "curated.sharp_action_indicators",
                "basic_stats": {},
                "error": str(e),
                "migration_complexity": "unknown",
            }

    def _generate_summary(self, tables: dict[str, Any]) -> dict[str, Any]:
        """Generate migration summary statistics."""
        total_records = 0
        total_games = 0
        data_sources = set()
        sportsbooks = set()

        for table_name, table_data in tables.items():
            if "basic_stats" in table_data:
                stats = table_data["basic_stats"]
                if "total_records" in stats:
                    total_records += stats["total_records"]
                elif "total_games" in stats:
                    total_games = stats["total_games"]
                elif "total_indicators" in stats:
                    total_records += stats["total_indicators"]

            # Collect sources and sportsbooks
            if "source_distribution" in table_data:
                for source in table_data["source_distribution"]:
                    data_sources.add(source["source"])

            if "sportsbook_distribution" in table_data:
                for book in table_data["sportsbook_distribution"]:
                    sportsbooks.add(book["sportsbook"])

        return {
            "total_records_to_migrate": total_records,
            "total_games": total_games,
            "unique_data_sources": len(data_sources),
            "unique_sportsbooks": len(sportsbooks),
            "data_sources_list": list(data_sources),
            "estimated_migration_time_hours": max(
                1, total_records // 10000
            ),  # ~10k records per hour
            "migration_phases_required": 4,
            "high_complexity_tables": [
                name
                for name, data in tables.items()
                if data.get("migration_complexity") == "high"
            ],
        }

    def _generate_migration_recommendations(
        self, tables: dict[str, Any], summary: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Generate specific migration recommendations."""
        recommendations = []

        # Priority order based on data volume and complexity
        if summary["total_records_to_migrate"] > 20000:
            recommendations.append(
                {
                    "priority": "HIGH",
                    "category": "Performance",
                    "title": "Use Batch Processing",
                    "description": f"With {summary['total_records_to_migrate']:,} records to migrate, implement batch processing with 1000 record chunks",
                    "implementation": "Implement batched migration with progress tracking and error recovery",
                }
            )

        # Data quality recommendations
        for table_name, table_data in tables.items():
            if "quality_stats" in table_data:
                high_quality = table_data["quality_stats"].get("high_quality", 0)
                total = table_data["basic_stats"].get("total_records", 1)
                if total > 0 and (high_quality / total) < 0.8:
                    recommendations.append(
                        {
                            "priority": "MEDIUM",
                            "category": "Data Quality",
                            "title": f"Quality Enhancement for {table_name}",
                            "description": f"Only {(high_quality / total) * 100:.1f}% of records are high quality",
                            "implementation": "Apply data quality improvements during staging migration",
                        }
                    )

        # Source mapping recommendations
        if len(summary["data_sources_list"]) > 3:
            recommendations.append(
                {
                    "priority": "HIGH",
                    "category": "Source Management",
                    "title": "Source Attribution Validation",
                    "description": f"Multiple data sources detected: {', '.join(summary['data_sources_list'])}",
                    "implementation": "Ensure proper source attribution mapping during RAW zone migration",
                }
            )

        # Timeline recommendations
        recommendations.append(
            {
                "priority": "HIGH",
                "category": "Timeline",
                "title": "Migration Duration Planning",
                "description": f"Estimated {summary['estimated_migration_time_hours']} hours for complete migration",
                "implementation": "Plan for 4-week migration timeline with weekly phases and weekend execution windows",
            }
        )

        return recommendations


async def main():
    """Main execution function."""
    analyzer = DataAnalyzer()

    try:
        await analyzer.initialize()

        print("🔍 Starting Phase 1: Data Analysis and Mapping")
        print("=" * 60)

        # Run comprehensive analysis
        results = await analyzer.analyze_all_data()

        # Display results
        print("\n📊 ANALYSIS RESULTS")
        print("-" * 40)

        if "summary" in results:
            summary = results["summary"]
            print(
                f"📈 Total Records to Migrate: {summary.get('total_records_to_migrate', 0):,}"
            )
            print(f"🎮 Total Games: {summary.get('total_games', 0):,}")
            print(f"📊 Data Sources: {summary.get('unique_data_sources', 0)}")
            print(f"📚 Sportsbooks: {summary.get('unique_sportsbooks', 0)}")
            print(
                f"⏱️ Estimated Migration Time: {summary.get('estimated_migration_time_hours', 0)} hours"
            )

        print("\n🎯 MIGRATION RECOMMENDATIONS")
        print("-" * 40)

        if "migration_recommendations" in results:
            for i, rec in enumerate(results["migration_recommendations"], 1):
                priority_emoji = "🔥" if rec["priority"] == "HIGH" else "⚠️"
                print(f"{priority_emoji} {rec['title']}")
                print(f"   Category: {rec['category']}")
                print(f"   Description: {rec['description']}")
                print()

        # Save detailed results to file
        output_file = Path("utilities/migration/phase1_analysis_results.json")
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w") as f:
            json.dump(results, f, indent=2, default=str)

        print(f"💾 Detailed analysis saved to: {output_file}")
        print("\n✅ Phase 1 Analysis Complete!")

    except Exception as e:
        logger.error(f"Phase 1 analysis failed: {e}")
        print(f"\n❌ Analysis failed: {e}")
        return 1

    finally:
        await analyzer.close()

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
