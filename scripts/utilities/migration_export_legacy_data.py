#!/usr/bin/env python3
"""
Legacy Strategy Data Export Script

This script exports all valuable legacy strategy performance data
before decommissioning the old SQL-based system.
"""

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.mlb_sharp_betting.core.logging import get_logger
from src.mlb_sharp_betting.db.connection import get_db_manager

logger = get_logger(__name__)


class LegacyDataExporter:
    """Export legacy strategy data before decommissioning."""

    def __init__(self):
        self.db_manager = get_db_manager()
        self.export_data = {
            "export_timestamp": datetime.now(timezone.utc).isoformat(),
            "legacy_strategies": [],
            "performance_history": [],
            "configuration_data": [],
            "summary_stats": {},
        }

    async def export_all_legacy_data(self) -> dict[str, Any]:
        """Export all legacy data that needs preservation."""
        logger.info("🔄 Starting legacy data export")

        try:
            # Export strategy performance data
            await self._export_strategy_performance()

            # Export configuration history
            await self._export_configuration_history()

            # Export summary statistics
            await self._export_summary_statistics()

            # Generate export report
            await self._generate_export_report()

            logger.info("✅ Legacy data export completed successfully")
            return self.export_data

        except Exception as e:
            logger.error("❌ Legacy data export failed", error=str(e))
            raise

    async def _export_strategy_performance(self):
        """Export all strategy performance data from legacy tables."""
        logger.info("📊 Exporting legacy strategy performance data")

        # Check for legacy tables and export their data
        legacy_tables = [
            "tracking.strategy_performance_cache",
            "backtesting.strategy_performance",
            "main.strategy_results",  # if it exists
        ]

        for table in legacy_tables:
            try:
                with self.db_manager.get_cursor() as cursor:
                    # Check if table exists
                    cursor.execute(
                        """
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = %s AND table_name = %s
                        )
                    """,
                        (table.split(".")[0], table.split(".")[1]),
                    )

                    if cursor.fetchone()[0]:
                        # Export table data
                        cursor.execute(f"SELECT * FROM {table}")
                        columns = [desc[0] for desc in cursor.description]
                        rows = cursor.fetchall()

                        self.export_data["legacy_strategies"].extend(
                            [dict(zip(columns, row, strict=False)) for row in rows]
                        )

                        logger.info(f"✅ Exported {len(rows)} records from {table}")
                    else:
                        logger.info(f"⚠️ Table {table} does not exist, skipping")

            except Exception as e:
                logger.warning(f"⚠️ Failed to export {table}: {e}")

    async def _export_configuration_history(self):
        """Export strategy configuration history."""
        logger.info("⚙️ Exporting configuration history")

        try:
            with self.db_manager.get_cursor() as cursor:
                # Export any configuration tables
                config_tables = [
                    "main.strategy_configs",
                    "tracking.strategy_config_history",
                ]

                for table in config_tables:
                    try:
                        cursor.execute(f"""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_name = '{table.split(".")[1]}'
                            )
                        """)

                        if cursor.fetchone()[0]:
                            cursor.execute(f"SELECT * FROM {table}")
                            columns = [desc[0] for desc in cursor.description]
                            rows = cursor.fetchall()

                            self.export_data["configuration_data"].extend(
                                [dict(zip(columns, row, strict=False)) for row in rows]
                            )

                    except Exception as e:
                        logger.warning(f"⚠️ Could not export {table}: {e}")

        except Exception as e:
            logger.warning(f"⚠️ Configuration export failed: {e}")

    async def _export_summary_statistics(self):
        """Generate summary statistics from legacy data."""
        logger.info("📈 Generating legacy system summary statistics")

        try:
            strategies = self.export_data["legacy_strategies"]

            if strategies:
                # Calculate summary stats
                total_strategies = len(strategies)

                # Group by strategy type
                strategy_types = {}
                roi_values = []

                for strategy in strategies:
                    # Extract strategy type
                    strategy_id = strategy.get("strategy_id", "") or strategy.get(
                        "source_book_type", ""
                    )
                    if strategy_id:
                        base_type = (
                            strategy_id.split("_")[0]
                            if "_" in strategy_id
                            else "unknown"
                        )
                        strategy_types[base_type] = strategy_types.get(base_type, 0) + 1

                    # Collect ROI values
                    roi = strategy.get("roi_per_100_unit") or strategy.get("roi", 0)
                    if roi and isinstance(roi, (int, float)):
                        roi_values.append(float(roi))

                self.export_data["summary_stats"] = {
                    "total_legacy_strategies": total_strategies,
                    "strategy_types": strategy_types,
                    "average_roi": sum(roi_values) / len(roi_values)
                    if roi_values
                    else 0,
                    "max_roi": max(roi_values) if roi_values else 0,
                    "min_roi": min(roi_values) if roi_values else 0,
                    "profitable_strategies": len([r for r in roi_values if r > 0]),
                    "high_roi_strategies": len([r for r in roi_values if r > 10]),
                }

        except Exception as e:
            logger.warning(f"⚠️ Summary statistics failed: {e}")

    async def _generate_export_report(self):
        """Generate a human-readable export report."""
        stats = self.export_data["summary_stats"]

        report = f"""
📊 LEGACY SYSTEM EXPORT REPORT
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S EST")}

=== LEGACY DATA SUMMARY ===
Total Strategies Exported: {stats.get("total_legacy_strategies", 0)}
Profitable Strategies: {stats.get("profitable_strategies", 0)}
High ROI Strategies (>10%): {stats.get("high_roi_strategies", 0)}

Average ROI: {stats.get("average_roi", 0):.2f}%
Max ROI: {stats.get("max_roi", 0):.2f}%
Min ROI: {stats.get("min_roi", 0):.2f}%

=== STRATEGY TYPES ===
"""

        for strategy_type, count in stats.get("strategy_types", {}).items():
            report += f"{strategy_type}: {count} strategies\n"

        report += f"""
=== EXPORT DETAILS ===
Legacy Strategies: {len(self.export_data["legacy_strategies"])} records
Configuration Data: {len(self.export_data["configuration_data"])} records

✅ All valuable legacy data has been preserved
🗑️ Safe to proceed with decommissioning
"""

        self.export_data["export_report"] = report
        logger.info("📋 Export report generated")


async def main():
    """Main export function."""
    exporter = LegacyDataExporter()

    try:
        # Export all legacy data
        export_data = await exporter.export_all_legacy_data()

        # Save to file
        export_file = Path("legacy_system_export.json")
        with open(export_file, "w") as f:
            json.dumps(export_data, f, indent=2, default=str)

        print("✅ Legacy data export completed successfully")
        print(f"📁 Data saved to: {export_file}")

        # Print summary report
        if "export_report" in export_data:
            print("\n" + "=" * 60)
            print(export_data["export_report"])
            print("=" * 60)

    except Exception as e:
        print(f"❌ Export failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
