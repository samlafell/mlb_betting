#!/usr/bin/env python3
"""
Phase 2: RAW Zone Migration

Migrates betting lines data from core_betting tables to raw_data schema tables.
This preserves the original source data with proper attribution and lineage.

Migration flow:
- core_betting.betting_lines_moneyline → raw_data.moneylines_raw
- core_betting.betting_lines_spread → raw_data.spreads_raw
- core_betting.betting_lines_totals → raw_data.totals_raw
- core_betting.betting_lines_* → raw_data.betting_lines_raw (unified)
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


class RawZoneMigrator:
    """Handles migration of data to RAW zone from core_betting tables."""

    def __init__(self, batch_size: int = 1000):
        self.settings = get_settings()
        self.batch_size = batch_size
        self.migration_stats = {
            "moneylines": {"processed": 0, "successful": 0, "failed": 0},
            "spreads": {"processed": 0, "successful": 0, "failed": 0},
            "totals": {"processed": 0, "successful": 0, "failed": 0},
            "unified": {"processed": 0, "successful": 0, "failed": 0},
        }

    async def initialize(self):
        """Initialize database connection."""
        initialize_connections(self.settings)

    async def close(self):
        """Close database connections."""
        pass  # Connection pool managed globally

    async def migrate_all_to_raw_zone(self) -> dict[str, Any]:
        """Execute complete RAW zone migration."""
        logger.info("Starting Phase 2: RAW zone migration")

        migration_results = {
            "timestamp": datetime.now().isoformat(),
            "phase": "Phase 2 - RAW Zone Migration",
            "status": "in_progress",
            "tables_migrated": {},
            "summary": {},
            "errors": [],
        }

        try:
            connection_manager = get_connection()
            async with connection_manager.get_async_connection() as conn:
                # Pre-flight checks
                await self._verify_raw_zone_tables(conn)

                # Migrate each betting table type
                migration_results["tables_migrated"][
                    "moneylines"
                ] = await self._migrate_moneylines(conn)
                migration_results["tables_migrated"][
                    "spreads"
                ] = await self._migrate_spreads(conn)
                migration_results["tables_migrated"][
                    "totals"
                ] = await self._migrate_totals(conn)
                migration_results["tables_migrated"][
                    "unified"
                ] = await self._create_unified_betting_lines(conn)

                # Generate summary
                migration_results["summary"] = self._generate_migration_summary()
                migration_results["status"] = "completed"

        except Exception as e:
            logger.error(f"RAW zone migration failed: {e}")
            migration_results["status"] = "failed"
            migration_results["error"] = str(e)
            migration_results["errors"].append(
                {
                    "timestamp": datetime.now().isoformat(),
                    "error": str(e),
                    "context": "main_migration_loop",
                }
            )

        return migration_results

    async def _verify_raw_zone_tables(self, conn):
        """Verify that RAW zone tables exist and are accessible."""
        logger.info("Verifying RAW zone table structure...")

        required_tables = [
            "raw_data.moneylines_raw",
            "raw_data.spreads_raw",
            "raw_data.totals_raw",
            "raw_data.betting_lines_raw",
        ]

        for table in required_tables:
            schema, table_name = table.split(".")
            result = await conn.fetchrow(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = $1 AND table_name = $2
                )
            """,
                schema,
                table_name,
            )

            if not result[0]:
                raise Exception(f"Required RAW zone table {table} does not exist")

        logger.info("✅ All RAW zone tables verified")

    async def _migrate_moneylines(self, conn) -> dict[str, Any]:
        """Migrate moneyline betting data to raw_data.moneylines_raw."""
        logger.info("Migrating moneylines to RAW zone...")

        table_stats = {"processed": 0, "successful": 0, "failed": 0, "batches": 0}

        try:
            # Get total count for progress tracking
            count_result = await conn.fetchrow("""
                SELECT COUNT(*) as total FROM core_betting.betting_lines_moneyline
            """)
            total_records = count_result["total"]
            logger.info(f"Migrating {total_records:,} moneyline records...")

            # Process in batches
            offset = 0
            while offset < total_records:
                batch_records = await conn.fetch(
                    """
                    SELECT 
                        id as external_id,
                        'core_betting' as source,
                        game_id,
                        sportsbook_id,
                        sportsbook,
                        home_ml,
                        away_ml,
                        home_team,
                        away_team,
                        odds_timestamp as collected_at,
                        created_at,
                        updated_at,
                        source as original_source,
                        -- Build raw_data JSONB with all original fields
                        json_build_object(
                            'id', id,
                            'game_id', game_id,
                            'sportsbook_id', sportsbook_id,
                            'sportsbook', sportsbook,
                            'home_ml', home_ml,
                            'away_ml', away_ml,
                            'home_team', home_team,
                            'away_team', away_team,
                            'odds_timestamp', odds_timestamp,
                            'home_bets_count', home_bets_count,
                            'away_bets_count', away_bets_count,
                            'home_bets_percentage', home_bets_percentage,
                            'away_bets_percentage', away_bets_percentage,
                            'home_money_percentage', home_money_percentage,
                            'away_money_percentage', away_money_percentage,
                            'source', source,
                            'data_quality', data_quality,
                            'data_completeness_score', data_completeness_score,
                            'source_metadata', source_metadata,
                            'collection_batch_id', collection_batch_id,
                            'source_reliability_score', source_reliability_score,
                            'collection_method', collection_method,
                            'external_source_id', external_source_id,
                            'source_api_version', source_api_version
                        ) as raw_data,
                        DATE(odds_timestamp) as game_date
                    FROM core_betting.betting_lines_moneyline
                    ORDER BY id
                    LIMIT $1 OFFSET $2
                """,
                    self.batch_size,
                    offset,
                )

                if not batch_records:
                    break

                # Insert batch into raw_data.moneylines_raw
                batch_successful = 0
                for record in batch_records:
                    try:
                        await conn.execute(
                            """
                            INSERT INTO raw_data.moneylines_raw (
                                external_id, source, game_external_id, sportsbook_id, 
                                sportsbook_name, home_odds, away_odds, home_team_name, 
                                away_team_name, raw_data, collected_at, game_date, 
                                created_at, updated_at
                            ) VALUES (
                                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
                            )
                        """,
                            str(record["external_id"]),
                            record["original_source"] or "CORE_BETTING",
                            str(record["game_id"]),
                            record["sportsbook_id"],
                            record["sportsbook"],
                            record["home_ml"],
                            record["away_ml"],
                            record["home_team"],
                            record["away_team"],
                            record["raw_data"],
                            record["collected_at"],
                            record["game_date"],
                            record["created_at"],
                            record["updated_at"],
                        )
                        batch_successful += 1

                    except Exception as e:
                        logger.error(
                            f"Failed to migrate moneyline record {record['external_id']}: {e}"
                        )
                        table_stats["failed"] += 1

                table_stats["processed"] += len(batch_records)
                table_stats["successful"] += batch_successful
                table_stats["batches"] += 1
                offset += self.batch_size

                # Progress logging
                progress = (offset / total_records) * 100
                logger.info(
                    f"Moneylines progress: {progress:.1f}% ({offset:,}/{total_records:,})"
                )

            self.migration_stats["moneylines"] = table_stats
            logger.info(
                f"✅ Moneylines migration completed: {table_stats['successful']:,} records"
            )

        except Exception as e:
            logger.error(f"Moneylines migration failed: {e}")
            table_stats["error"] = str(e)

        return table_stats

    async def _migrate_spreads(self, conn) -> dict[str, Any]:
        """Migrate spread betting data to raw_data.spreads_raw."""
        logger.info("Migrating spreads to RAW zone...")

        table_stats = {"processed": 0, "successful": 0, "failed": 0, "batches": 0}

        try:
            # Get total count
            count_result = await conn.fetchrow("""
                SELECT COUNT(*) as total FROM core_betting.betting_lines_spread
            """)
            total_records = count_result["total"]
            logger.info(f"Migrating {total_records:,} spread records...")

            # Process in batches
            offset = 0
            while offset < total_records:
                batch_records = await conn.fetch(
                    """
                    SELECT 
                        id as external_id,
                        'core_betting' as source,
                        game_id,
                        sportsbook_id,
                        sportsbook,
                        spread_line,
                        home_spread_price,
                        away_spread_price,
                        home_team as favorite_team,
                        away_team as underdog_team,
                        odds_timestamp as collected_at,
                        created_at,
                        updated_at,
                        source as original_source,
                        -- Build raw_data JSONB
                        json_build_object(
                            'id', id,
                            'game_id', game_id,
                            'sportsbook_id', sportsbook_id,
                            'sportsbook', sportsbook,
                            'spread_line', spread_line,
                            'home_spread_price', home_spread_price,
                            'away_spread_price', away_spread_price,
                            'favorite_team', home_team,
                            'underdog_team', away_team,
                            'odds_timestamp', odds_timestamp,
                            'home_bets_count', home_bets_count,
                            'away_bets_count', away_bets_count,
                            'home_bets_percentage', home_bets_percentage,
                            'away_bets_percentage', away_bets_percentage,
                            'home_money_percentage', home_money_percentage,
                            'away_money_percentage', away_money_percentage,
                            'source', source,
                            'data_quality', data_quality,
                            'data_completeness_score', data_completeness_score,
                            'source_metadata', source_metadata,
                            'collection_batch_id', collection_batch_id,
                            'source_reliability_score', source_reliability_score,
                            'collection_method', collection_method,
                            'external_source_id', external_source_id,
                            'source_api_version', source_api_version
                        ) as raw_data,
                        DATE(odds_timestamp) as game_date
                    FROM core_betting.betting_lines_spread
                    ORDER BY id
                    LIMIT $1 OFFSET $2
                """,
                    self.batch_size,
                    offset,
                )

                if not batch_records:
                    break

                # Insert batch
                batch_successful = 0
                for record in batch_records:
                    try:
                        await conn.execute(
                            """
                            INSERT INTO raw_data.spreads_raw (
                                external_id, source, game_external_id, sportsbook_id,
                                sportsbook_name, spread_value, spread_odds, favorite_team,
                                underdog_team, raw_data, collected_at, game_date,
                                created_at, updated_at
                            ) VALUES (
                                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
                            )
                        """,
                            str(record["external_id"]),
                            record["original_source"] or "CORE_BETTING",
                            str(record["game_id"]),
                            record["sportsbook_id"],
                            record["sportsbook"],
                            record["spread_line"],
                            record["home_spread_price"],
                            record["favorite_team"],
                            record["underdog_team"],
                            record["raw_data"],
                            record["collected_at"],
                            record["game_date"],
                            record["created_at"],
                            record["updated_at"],
                        )
                        batch_successful += 1

                    except Exception as e:
                        logger.error(
                            f"Failed to migrate spread record {record['external_id']}: {e}"
                        )
                        table_stats["failed"] += 1

                table_stats["processed"] += len(batch_records)
                table_stats["successful"] += batch_successful
                table_stats["batches"] += 1
                offset += self.batch_size

                # Progress logging
                progress = (offset / total_records) * 100
                logger.info(
                    f"Spreads progress: {progress:.1f}% ({offset:,}/{total_records:,})"
                )

            self.migration_stats["spreads"] = table_stats
            logger.info(
                f"✅ Spreads migration completed: {table_stats['successful']:,} records"
            )

        except Exception as e:
            logger.error(f"Spreads migration failed: {e}")
            table_stats["error"] = str(e)

        return table_stats

    async def _migrate_totals(self, conn) -> dict[str, Any]:
        """Migrate totals betting data to raw_data.totals_raw."""
        logger.info("Migrating totals to RAW zone...")

        table_stats = {"processed": 0, "successful": 0, "failed": 0, "batches": 0}

        try:
            # Get total count
            count_result = await conn.fetchrow("""
                SELECT COUNT(*) as total FROM core_betting.betting_lines_totals
            """)
            total_records = count_result["total"]
            logger.info(f"Migrating {total_records:,} totals records...")

            # Process in batches
            offset = 0
            while offset < total_records:
                batch_records = await conn.fetch(
                    """
                    SELECT 
                        id as external_id,
                        'core_betting' as source,
                        game_id,
                        sportsbook_id,
                        sportsbook,
                        total_line,
                        over_price,
                        under_price,
                        home_team,
                        away_team,
                        odds_timestamp as collected_at,
                        created_at,
                        updated_at,
                        source as original_source,
                        -- Build raw_data JSONB
                        json_build_object(
                            'id', id,
                            'game_id', game_id,
                            'sportsbook_id', sportsbook_id,
                            'sportsbook', sportsbook,
                            'total_line', total_line,
                            'over_price', over_price,
                            'under_price', under_price,
                            'odds_timestamp', odds_timestamp,
                            'over_bets_count', over_bets_count,
                            'under_bets_count', under_bets_count,
                            'over_bets_percentage', over_bets_percentage,
                            'under_bets_percentage', under_bets_percentage,
                            'over_money_percentage', over_money_percentage,
                            'under_money_percentage', under_money_percentage,
                            'source', source,
                            'data_quality', data_quality,
                            'data_completeness_score', data_completeness_score,
                            'source_metadata', source_metadata,
                            'collection_batch_id', collection_batch_id,
                            'source_reliability_score', source_reliability_score,
                            'collection_method', collection_method,
                            'external_source_id', external_source_id,
                            'source_api_version', source_api_version
                        ) as raw_data,
                        DATE(odds_timestamp) as game_date
                    FROM core_betting.betting_lines_totals
                    ORDER BY id
                    LIMIT $1 OFFSET $2
                """,
                    self.batch_size,
                    offset,
                )

                if not batch_records:
                    break

                # Insert batch
                batch_successful = 0
                for record in batch_records:
                    try:
                        await conn.execute(
                            """
                            INSERT INTO raw_data.totals_raw (
                                external_id, source, game_external_id, sportsbook_id,
                                sportsbook_name, total_points, over_odds, under_odds,
                                raw_data, collected_at, game_date, created_at, updated_at
                            ) VALUES (
                                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
                            )
                        """,
                            str(record["external_id"]),
                            record["original_source"] or "CORE_BETTING",
                            str(record["game_id"]),
                            record["sportsbook_id"],
                            record["sportsbook"],
                            record["total_line"],
                            record["over_price"],
                            record["under_price"],
                            record["raw_data"],
                            record["collected_at"],
                            record["game_date"],
                            record["created_at"],
                            record["updated_at"],
                        )
                        batch_successful += 1

                    except Exception as e:
                        logger.error(
                            f"Failed to migrate totals record {record['external_id']}: {e}"
                        )
                        table_stats["failed"] += 1

                table_stats["processed"] += len(batch_records)
                table_stats["successful"] += batch_successful
                table_stats["batches"] += 1
                offset += self.batch_size

                # Progress logging
                progress = (offset / total_records) * 100
                logger.info(
                    f"Totals progress: {progress:.1f}% ({offset:,}/{total_records:,})"
                )

            self.migration_stats["totals"] = table_stats
            logger.info(
                f"✅ Totals migration completed: {table_stats['successful']:,} records"
            )

        except Exception as e:
            logger.error(f"Totals migration failed: {e}")
            table_stats["error"] = str(e)

        return table_stats

    async def _create_unified_betting_lines(self, conn) -> dict[str, Any]:
        """Create unified betting lines view from raw zone tables."""
        logger.info("Creating unified betting lines in RAW zone...")

        table_stats = {"processed": 0, "successful": 0, "failed": 0}

        try:
            # Insert unified moneylines
            ml_result = await conn.execute("""
                INSERT INTO raw_data.betting_lines_raw (
                    external_id, source, game_external_id, sportsbook_id, sportsbook_name,
                    bet_type, line_value, odds_american, team_type, raw_data,
                    collected_at, game_date, created_at, updated_at
                )
                SELECT 
                    external_id,
                    source,
                    game_external_id,
                    sportsbook_id,
                    sportsbook_name,
                    'moneyline' as bet_type,
                    NULL as line_value,
                    home_odds as odds_american,
                    'home' as team_type,
                    raw_data,
                    collected_at,
                    game_date,
                    created_at,
                    updated_at
                FROM raw_data.moneylines_raw
                
                UNION ALL
                
                SELECT 
                    external_id || '_away',
                    source,
                    game_external_id,
                    sportsbook_id,
                    sportsbook_name,
                    'moneyline' as bet_type,
                    NULL as line_value,
                    away_odds as odds_american,
                    'away' as team_type,
                    raw_data,
                    collected_at,
                    game_date,
                    created_at,
                    updated_at
                FROM raw_data.moneylines_raw
                WHERE away_odds IS NOT NULL
            """)

            # Insert unified spreads
            spread_result = await conn.execute("""
                INSERT INTO raw_data.betting_lines_raw (
                    external_id, source, game_external_id, sportsbook_id, sportsbook_name,
                    bet_type, line_value, odds_american, team_type, raw_data,
                    collected_at, game_date, created_at, updated_at
                )
                SELECT 
                    external_id,
                    source,
                    game_external_id,
                    sportsbook_id,
                    sportsbook_name,
                    'spread' as bet_type,
                    spread_value as line_value,
                    spread_odds as odds_american,
                    'favorite' as team_type,
                    raw_data,
                    collected_at,
                    game_date,
                    created_at,
                    updated_at
                FROM raw_data.spreads_raw
            """)

            # Insert unified totals
            totals_result = await conn.execute("""
                INSERT INTO raw_data.betting_lines_raw (
                    external_id, source, game_external_id, sportsbook_id, sportsbook_name,
                    bet_type, line_value, odds_american, team_type, raw_data,
                    collected_at, game_date, created_at, updated_at
                )
                SELECT 
                    external_id,
                    source,
                    game_external_id,
                    sportsbook_id,
                    sportsbook_name,
                    'total' as bet_type,
                    total_points as line_value,
                    over_odds as odds_american,
                    'over' as team_type,
                    raw_data,
                    collected_at,
                    game_date,
                    created_at,
                    updated_at
                FROM raw_data.totals_raw
                
                UNION ALL
                
                SELECT 
                    external_id || '_under',
                    source,
                    game_external_id,
                    sportsbook_id,
                    sportsbook_name,
                    'total' as bet_type,
                    total_points as line_value,
                    under_odds as odds_american,
                    'under' as team_type,
                    raw_data,
                    collected_at,
                    game_date,
                    created_at,
                    updated_at
                FROM raw_data.totals_raw
                WHERE under_odds IS NOT NULL
            """)

            # Parse results (PostgreSQL returns status like "INSERT 0 1234")
            ml_count = (
                int(ml_result.split()[-1]) if ml_result.split()[-1].isdigit() else 0
            )
            spread_count = (
                int(spread_result.split()[-1])
                if spread_result.split()[-1].isdigit()
                else 0
            )
            totals_count = (
                int(totals_result.split()[-1])
                if totals_result.split()[-1].isdigit()
                else 0
            )

            total_unified = ml_count + spread_count + totals_count

            table_stats["processed"] = total_unified
            table_stats["successful"] = total_unified

            self.migration_stats["unified"] = table_stats
            logger.info(f"✅ Unified betting lines created: {total_unified:,} records")

        except Exception as e:
            logger.error(f"Unified betting lines creation failed: {e}")
            table_stats["error"] = str(e)

        return table_stats

    def _generate_migration_summary(self) -> dict[str, Any]:
        """Generate migration summary statistics."""
        total_processed = sum(
            stats["processed"] for stats in self.migration_stats.values()
        )
        total_successful = sum(
            stats["successful"] for stats in self.migration_stats.values()
        )
        total_failed = sum(stats["failed"] for stats in self.migration_stats.values())

        return {
            "total_records_processed": total_processed,
            "total_records_successful": total_successful,
            "total_records_failed": total_failed,
            "success_rate": (total_successful / total_processed * 100)
            if total_processed > 0
            else 0,
            "tables_migrated": len(
                [k for k, v in self.migration_stats.items() if v["successful"] > 0]
            ),
            "migration_stats_by_table": self.migration_stats,
            "migration_completed_at": datetime.now().isoformat(),
        }


async def main():
    """Main execution function."""
    migrator = RawZoneMigrator(batch_size=1000)

    try:
        await migrator.initialize()

        print("🚀 Starting Phase 2: RAW Zone Migration")
        print("=" * 60)

        # Run migration
        results = await migrator.migrate_all_to_raw_zone()

        # Display results
        print("\n📊 MIGRATION RESULTS")
        print("-" * 40)

        if results["status"] == "completed" and "summary" in results:
            summary = results["summary"]
            print(f"✅ Migration Status: {results['status'].upper()}")
            print(
                f"📈 Total Records Processed: {summary.get('total_records_processed', 0):,}"
            )
            print(
                f"✅ Successful Migrations: {summary.get('total_records_successful', 0):,}"
            )
            print(f"❌ Failed Migrations: {summary.get('total_records_failed', 0):,}")
            print(f"📊 Success Rate: {summary.get('success_rate', 0):.1f}%")
            print(f"🗄️ Tables Migrated: {summary.get('tables_migrated', 0)}")

            print("\n📋 Migration Details by Table:")
            for table, stats in summary.get("migration_stats_by_table", {}).items():
                print(
                    f"  {table}: {stats['successful']:,} successful, {stats['failed']} failed"
                )
        else:
            print(f"❌ Migration Status: {results['status'].upper()}")
            if "error" in results:
                print(f"Error: {results['error']}")

        # Save results to file
        output_file = Path("utilities/migration/phase2_migration_results.json")

        with open(output_file, "w") as f:
            json.dump(results, f, indent=2, default=str)

        print(f"\n💾 Migration results saved to: {output_file}")
        print("\n✅ Phase 2 RAW Zone Migration Complete!")

    except Exception as e:
        logger.error(f"Phase 2 migration failed: {e}")
        print(f"\n❌ Migration failed: {e}")
        return 1

    finally:
        await migrator.close()

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
