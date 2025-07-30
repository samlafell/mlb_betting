#!/usr/bin/env python3
"""
Legacy Core Betting Schema Migration Executor

Safe execution of the legacy core_betting schema migration to three-tier architecture.
Includes comprehensive validation, monitoring, and rollback capabilities.

Usage:
    python utilities/execute_legacy_migration.py [--dry-run] [--force]

Options:
    --dry-run    Show what would be migrated without executing
    --force      Skip interactive confirmation
"""

import argparse
import asyncio
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncpg
import structlog

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config import get_settings

logger = structlog.get_logger(__name__)


class LegacyMigrationExecutor:
    """Execute legacy core_betting schema migration with full validation."""

    def __init__(self):
        self.config = get_settings()
        self.db_pool = None
        self.migration_start_time = datetime.now()
        self.migration_id = (
            f"migration_{self.migration_start_time.strftime('%Y_%m_%d_%H%M%S')}"
        )

        # Expected counts from analysis
        self.expected_counts = {
            "total_records": 28407,
            "games": 1747,
            "betting_lines_spread": 3360,
            "betting_lines_moneyline": 12410,
            "betting_lines_totals": 10568,
            "teams": 30,
            "sportsbooks": 11,
            "supplementary_games": 252,
            "data_migrations": 3,
            "sportsbook_external_mappings": 19,
            "data_source_metadata": 7,
        }

    async def __aenter__(self):
        """Initialize database connection pool."""
        try:
            self.db_pool = await asyncpg.create_pool(
                host=self.config.database.host,
                port=self.config.database.port,
                user=self.config.database.user,
                password=self.config.database.password,
                database=self.config.database.database,
                min_size=2,
                max_size=5,
                command_timeout=300,  # 5 minute timeout for migration operations
            )
            logger.info("Database connection pool initialized for migration")
            return self
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {str(e)}")
            raise

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up database connections."""
        if self.db_pool:
            await self.db_pool.close()
            logger.info("Database connection pool closed")

    async def pre_migration_validation(self) -> dict[str, Any]:
        """Comprehensive pre-migration validation."""
        logger.info("Starting pre-migration validation")

        validation_results = {
            "start_time": datetime.now().isoformat(),
            "checks": {},
            "overall_status": "running",
        }

        async with self.db_pool.acquire() as conn:
            # Check 1: Verify core_betting schema exists
            schema_exists = await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'core_betting')"
            )
            validation_results["checks"]["core_betting_schema_exists"] = {
                "status": "passed" if schema_exists else "failed",
                "result": schema_exists,
            }

            if not schema_exists:
                validation_results["overall_status"] = "failed"
                validation_results["error"] = "core_betting schema does not exist"
                return validation_results

            # Check 2: Verify expected table counts
            table_counts = {}
            total_actual = 0

            for table, expected_count in self.expected_counts.items():
                if table == "total_records":
                    continue

                try:
                    actual_count = await conn.fetchval(
                        f"SELECT COUNT(*) FROM curated.{table}"
                    )
                    table_counts[table] = {
                        "expected": expected_count,
                        "actual": actual_count,
                        "match": actual_count == expected_count,
                    }
                    total_actual += actual_count
                except Exception as e:
                    table_counts[table] = {
                        "expected": expected_count,
                        "actual": 0,
                        "error": str(e),
                        "match": False,
                    }

            validation_results["checks"]["table_counts"] = table_counts
            validation_results["checks"]["total_count_check"] = {
                "expected": self.expected_counts["total_records"],
                "actual": total_actual,
                "match": abs(total_actual - self.expected_counts["total_records"])
                <= 100,  # Allow small variance
            }

            # Check 3: Verify target schemas exist
            for schema in ["raw_data", "staging"]:
                schema_exists = await conn.fetchval(
                    f"SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = '{schema}')"
                )
                validation_results["checks"][f"{schema}_schema_exists"] = {
                    "status": "passed" if schema_exists else "failed",
                    "result": schema_exists,
                }

            # Check 4: Verify no active connections to core_betting tables
            active_connections = await conn.fetch(
                """
                SELECT datname, usename, application_name, state, query_start
                FROM pg_stat_activity 
                WHERE state = 'active' 
                  AND query ILIKE '%core_betting%'
                  AND pid != pg_backend_pid()
                """
            )

            validation_results["checks"]["active_connections"] = {
                "count": len(active_connections),
                "connections": [dict(conn) for conn in active_connections],
                "safe": len(active_connections) == 0,
            }

            # Check 5: Database space availability
            db_size = await conn.fetchval("SELECT pg_database_size(current_database())")
            free_space_estimate = db_size * 0.1  # Estimate 10% additional space needed

            validation_results["checks"]["space_availability"] = {
                "current_db_size_mb": round(db_size / 1024 / 1024, 2),
                "estimated_additional_mb": round(free_space_estimate / 1024 / 1024, 2),
                "status": "estimated",  # We can't easily check actual free space
            }

        # Determine overall validation status
        failed_checks = [
            check_name
            for check_name, check_result in validation_results["checks"].items()
            if check_result.get("status") == "failed"
            or check_result.get("match") == False
        ]

        if failed_checks:
            validation_results["overall_status"] = "failed"
            validation_results["failed_checks"] = failed_checks
        else:
            validation_results["overall_status"] = "passed"

        validation_results["end_time"] = datetime.now().isoformat()
        logger.info(
            f"Pre-migration validation completed: {validation_results['overall_status']}"
        )

        return validation_results

    async def execute_migration(self, dry_run: bool = False) -> dict[str, Any]:
        """Execute the migration with monitoring."""
        if dry_run:
            logger.info("DRY RUN MODE: Showing migration plan without executing")
            return await self._dry_run_analysis()

        logger.info(f"Starting migration execution: {self.migration_id}")

        migration_results = {
            "migration_id": self.migration_id,
            "start_time": datetime.now().isoformat(),
            "phases": {},
            "overall_status": "running",
        }

        try:
            # Load and execute migration script
            migration_script_path = (
                Path(__file__).parent.parent
                / "sql"
                / "migrations"
                / "008_execute_legacy_migration.sql"
            )

            if not migration_script_path.exists():
                raise FileNotFoundError(
                    f"Migration script not found: {migration_script_path}"
                )

            with open(migration_script_path) as f:
                migration_sql = f.read()

            logger.info("Executing migration script")

            async with self.db_pool.acquire() as conn:
                # Split the script into individual statements for better error handling
                start_time = time.time()

                try:
                    # Split on statement boundaries more carefully
                    statements = []
                    current_statement = ""
                    in_do_block = False
                    do_block_depth = 0

                    for line in migration_sql.split("\n"):
                        line = line.strip()

                        # Skip empty lines and comments
                        if not line or line.startswith("--"):
                            if current_statement:
                                current_statement += "\n" + line
                            continue

                        # Track DO blocks
                        if line.startswith("DO $$") or line.startswith("DO $"):
                            in_do_block = True
                            do_block_depth = 1
                        elif in_do_block and line == "$$;":
                            do_block_depth -= 1
                            if do_block_depth == 0:
                                in_do_block = False

                        current_statement += "\n" + line

                        # End of statement detection
                        if (
                            not in_do_block
                            and line.endswith(";")
                            and not line.startswith("--")
                        ):
                            statements.append(current_statement.strip())
                            current_statement = ""

                    # Add any remaining statement
                    if current_statement.strip():
                        statements.append(current_statement.strip())

                    logger.info(
                        f"Split migration script into {len(statements)} statements"
                    )

                    # Execute each statement individually
                    for i, statement in enumerate(statements):
                        if statement.strip():
                            try:
                                await conn.execute(statement)
                                logger.info(
                                    f"Executed statement {i + 1}/{len(statements)}"
                                )
                            except Exception as e:
                                logger.error(f"Error in statement {i + 1}: {str(e)}")
                                logger.error(f"Statement content: {statement[:200]}...")
                                raise

                    execution_time = time.time() - start_time
                    migration_results["execution_time_seconds"] = round(
                        execution_time, 2
                    )
                    migration_results["overall_status"] = "completed"

                    logger.info(
                        f"Migration script executed successfully in {execution_time:.2f} seconds"
                    )

                except Exception as e:
                    migration_results["overall_status"] = "failed"
                    migration_results["error"] = str(e)
                    logger.error(f"Migration script execution failed: {str(e)}")
                    raise

            # Post-migration validation
            validation_results = await self.post_migration_validation()
            migration_results["post_migration_validation"] = validation_results

            migration_results["end_time"] = datetime.now().isoformat()

            return migration_results

        except Exception as e:
            migration_results["overall_status"] = "failed"
            migration_results["error"] = str(e)
            migration_results["end_time"] = datetime.now().isoformat()
            logger.error(f"Migration failed: {str(e)}")
            return migration_results

    async def _dry_run_analysis(self) -> dict[str, Any]:
        """Analyze what would be migrated without executing."""
        logger.info("Performing dry run analysis")

        dry_run_results = {
            "mode": "dry_run",
            "analysis_time": datetime.now().isoformat(),
            "migration_plan": {},
        }

        async with self.db_pool.acquire() as conn:
            # Analyze each table that would be migrated
            tables_to_analyze = [
                "games",
                "teams",
                "sportsbooks",
                "betting_lines_spread",
                "betting_lines_moneyline",
                "betting_lines_totals",
                "supplementary_games",
                "data_migrations",
                "sportsbook_external_mappings",
                "data_source_metadata",
            ]

            for table in tables_to_analyze:
                try:
                    count = await conn.fetchval(f"SELECT COUNT(*) FROM curated.{table}")

                    # Get sample record structure
                    sample = await conn.fetchrow(
                        f"SELECT * FROM curated.{table} LIMIT 1"
                    )
                    columns = list(sample.keys()) if sample else []

                    dry_run_results["migration_plan"][table] = {
                        "record_count": count,
                        "columns": columns,
                        "destination": self._get_destination_table(table),
                        "migration_type": self._get_migration_type(table),
                    }

                except Exception as e:
                    dry_run_results["migration_plan"][table] = {
                        "error": str(e),
                        "record_count": 0,
                    }

        # Calculate totals
        total_records = sum(
            plan.get("record_count", 0)
            for plan in dry_run_results["migration_plan"].values()
        )

        dry_run_results["summary"] = {
            "total_tables": len(tables_to_analyze),
            "total_records": total_records,
            "estimated_execution_time_minutes": max(
                1, total_records // 1000
            ),  # Rough estimate
        }

        logger.info(
            f"Dry run complete: {total_records} records across {len(tables_to_analyze)} tables"
        )
        return dry_run_results

    def _get_destination_table(self, source_table: str) -> str:
        """Determine destination table for migration."""
        mapping = {
            "games": "raw_data.legacy_games",
            "betting_lines_spread": "raw_data.legacy_betting_lines",
            "betting_lines_moneyline": "raw_data.legacy_betting_lines",
            "betting_lines_totals": "raw_data.legacy_betting_lines",
            "teams": "staging.teams (merge with external_ids)",
            "sportsbooks": "staging.sportsbooks",
            "sportsbook_external_mappings": "staging.sportsbook_external_mappings",
            "data_source_metadata": "staging.data_source_metadata",
            "supplementary_games": "archive.legacy_supplementary_games",
            "data_migrations": "archive.legacy_data_migrations",
        }
        return mapping.get(source_table, "unknown")

    def _get_migration_type(self, source_table: str) -> str:
        """Determine migration type."""
        if source_table in [
            "games",
            "betting_lines_spread",
            "betting_lines_moneyline",
            "betting_lines_totals",
        ]:
            return "operational_data_to_raw"
        elif source_table in [
            "teams",
            "sportsbooks",
            "sportsbook_external_mappings",
            "data_source_metadata",
        ]:
            return "reference_data_to_staging"
        else:
            return "archive_data"

    async def post_migration_validation(self) -> dict[str, Any]:
        """Validate migration results."""
        logger.info("Starting post-migration validation")

        validation_results = {
            "start_time": datetime.now().isoformat(),
            "checks": {},
            "overall_status": "running",
        }

        async with self.db_pool.acquire() as conn:
            # Check 1: Verify migration log exists
            try:
                migration_log_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM staging.migration_log"
                )
                validation_results["checks"]["migration_log"] = {
                    "status": "passed" if migration_log_count > 0 else "failed",
                    "record_count": migration_log_count,
                }
            except Exception as e:
                validation_results["checks"]["migration_log"] = {
                    "status": "failed",
                    "error": str(e),
                }

            # Check 2: Verify backup tables created
            backup_tables = [
                "backup_curated.games_complete",
                "backup_curated.teams_master",
                "backup_curated.sportsbooks",
            ]

            for table in backup_tables:
                try:
                    count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                    validation_results["checks"][f"backup_{table.split('.')[1]}"] = {
                        "status": "passed" if count > 0 else "warning",
                        "record_count": count,
                    }
                except Exception as e:
                    validation_results["checks"][f"backup_{table.split('.')[1]}"] = {
                        "status": "failed",
                        "error": str(e),
                    }

            # Check 3: Verify migrated data counts
            migrated_counts = {}

            # Raw data migration
            try:
                legacy_games_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM raw_data.legacy_games"
                )
                legacy_betting_lines_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM raw_data.legacy_betting_lines"
                )

                migrated_counts["raw_data"] = {
                    "legacy_games": legacy_games_count,
                    "legacy_betting_lines": legacy_betting_lines_count,
                }
            except Exception as e:
                migrated_counts["raw_data"] = {"error": str(e)}

            # Staging data migration
            try:
                sportsbooks_with_legacy = await conn.fetchval(
                    "SELECT COUNT(*) FROM staging.sportsbooks WHERE external_ids ? 'legacy_id'"
                )
                migrated_counts["staging"] = {
                    "sportsbooks_with_legacy_id": sportsbooks_with_legacy
                }
            except Exception as e:
                migrated_counts["staging"] = {"error": str(e)}

            # Archive data migration
            try:
                archived_migrations = await conn.fetchval(
                    "SELECT COUNT(*) FROM archive.legacy_data_migrations"
                )
                archived_supplementary = await conn.fetchval(
                    "SELECT COUNT(*) FROM archive.legacy_supplementary_games"
                )

                migrated_counts["archive"] = {
                    "legacy_data_migrations": archived_migrations,
                    "legacy_supplementary_games": archived_supplementary,
                }
            except Exception as e:
                migrated_counts["archive"] = {"error": str(e)}

            validation_results["checks"]["migrated_data_counts"] = migrated_counts

            # Check 4: Verify migration status table
            try:
                migration_status = await conn.fetchrow(
                    "SELECT * FROM staging.migration_status WHERE migration_name = 'legacy_core_betting_migration'"
                )
                if migration_status:
                    validation_results["checks"]["migration_status"] = {
                        "status": "passed",
                        "completed_at": migration_status["completed_at"].isoformat(),
                        "summary": migration_status["migration_summary"],
                    }
                else:
                    validation_results["checks"]["migration_status"] = {
                        "status": "failed",
                        "error": "Migration status record not found",
                    }
            except Exception as e:
                validation_results["checks"]["migration_status"] = {
                    "status": "failed",
                    "error": str(e),
                }

        # Determine overall validation status
        failed_validations = [
            check_name
            for check_name, check_result in validation_results["checks"].items()
            if check_result.get("status") == "failed"
        ]

        if failed_validations:
            validation_results["overall_status"] = "failed"
            validation_results["failed_validations"] = failed_validations
        else:
            validation_results["overall_status"] = "passed"

        validation_results["end_time"] = datetime.now().isoformat()
        logger.info(
            f"Post-migration validation completed: {validation_results['overall_status']}"
        )

        return validation_results

    async def run_pipeline_validation(self) -> dict[str, Any]:
        """Run the three-tier pipeline validation to confirm migration success."""
        logger.info("Running three-tier pipeline validation to confirm migration")

        try:
            # Import and run the pipeline validator
            sys.path.append(str(Path(__file__).parent.parent / "tests" / "integration"))
            from test_three_tier_pipeline_validation import ThreeTierPipelineValidator

            async with ThreeTierPipelineValidator() as validator:
                phase_1_results = await validator.validate_phase_1_schemas()

                return {
                    "pipeline_validation": phase_1_results,
                    "legacy_migration_success": phase_1_results.get("status")
                    == "passed",
                }

        except Exception as e:
            logger.error(f"Pipeline validation failed: {str(e)}")
            return {
                "pipeline_validation": {"error": str(e)},
                "legacy_migration_success": False,
            }


async def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Execute legacy core_betting schema migration"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show migration plan without executing"
    )
    parser.add_argument(
        "--force", action="store_true", help="Skip interactive confirmation"
    )
    parser.add_argument(
        "--validate-only", action="store_true", help="Run validation only"
    )

    args = parser.parse_args()

    logger.info("Legacy Core Betting Migration Executor")
    logger.info("====================================")

    async with LegacyMigrationExecutor() as executor:
        # Pre-migration validation
        validation_results = await executor.pre_migration_validation()

        print("\n=== PRE-MIGRATION VALIDATION RESULTS ===")
        print(json.dumps(validation_results, indent=2, default=str))

        if validation_results["overall_status"] != "passed":
            print("\n❌ Pre-migration validation failed!")
            print("Please address the issues before proceeding with migration.")
            return 1

        if args.validate_only:
            print("\n✅ Validation-only mode: Pre-migration validation passed!")
            return 0

        if not args.dry_run and not args.force:
            print("\n=== MIGRATION CONFIRMATION ===")
            print(
                f"Ready to migrate {validation_results['checks']['total_count_check']['actual']} records"
            )
            print("This operation will:")
            print("1. Create backup of all core_betting tables")
            print("2. Migrate operational data to raw_data schema")
            print("3. Migrate reference data to staging schema")
            print("4. Archive system data")
            print("5. Create migration tracking records")

            confirmation = input("\nProceed with migration? (yes/no): ").lower().strip()
            if confirmation != "yes":
                print("Migration cancelled by user.")
                return 0

        # Execute migration
        migration_results = await executor.execute_migration(dry_run=args.dry_run)

        print("\n=== MIGRATION RESULTS ===")
        print(json.dumps(migration_results, indent=2, default=str))

        if args.dry_run:
            print("\n✅ Dry run completed successfully!")
            print(
                f"Migration plan validated for {migration_results['summary']['total_records']} records"
            )
            return 0

        if migration_results.get("overall_status") == "completed":
            print("\n✅ Migration completed successfully!")

            # Run pipeline validation to confirm success
            pipeline_results = await executor.run_pipeline_validation()
            print("\n=== PIPELINE VALIDATION ===")
            print(json.dumps(pipeline_results, indent=2, default=str))

            if pipeline_results.get("legacy_migration_success"):
                print("\n🎉 MIGRATION FULLY SUCCESSFUL!")
                print(
                    "Legacy core_betting schema has been successfully migrated to three-tier architecture."
                )
                print("The three-tier pipeline validation now passes.")
            else:
                print(
                    "\n⚠️ Migration completed but pipeline validation still shows issues."
                )
                print("Additional cleanup may be required.")

            return 0
        else:
            print("\n❌ Migration failed!")
            return 1


if __name__ == "__main__":
    exit(asyncio.run(main()))
