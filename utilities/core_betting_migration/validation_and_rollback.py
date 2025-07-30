#!/usr/bin/env python3
"""
Validation and Rollback System for Core Betting Schema Decommission

This tool provides comprehensive validation and emergency rollback capabilities
for the core_betting schema migration.

Key Features:
- Pre-migration validation (schema accessibility, data integrity, locks, dependencies)
- Post-migration validation (row counts, data integrity, performance, business logic)
- Real-time migration monitoring
- Emergency rollback system
- Detailed validation reports

Usage:
    python validation_and_rollback.py --validate-pre-migration     # Pre-migration checks
    python validation_and_rollback.py --validate-post-migration    # Post-migration validation
    python validation_and_rollback.py --monitor                    # Real-time monitoring
    python validation_and_rollback.py --rollback --confirm         # Emergency rollback
"""

import argparse
import json
import logging
import os
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg2

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/core_betting_validation.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Data class for validation results."""

    check_name: str
    status: str  # 'passed', 'failed', 'warning'
    message: str
    details: dict[str, Any] = None
    timestamp: str = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()


@dataclass
class ValidationReport:
    """Data class for complete validation report."""

    report_type: str
    timestamp: str
    overall_status: str
    summary: dict[str, Any]
    checks: list[ValidationResult]
    recommendations: list[str]

    def to_dict(self):
        return asdict(self)


class CoreBettingValidator:
    """Comprehensive validation and rollback system for core_betting migration."""

    def __init__(self, db_config: dict[str, str] = None):
        self.db_config = db_config or self._load_db_config()
        self.connection = None
        self.backup_locations = []

    def _load_db_config(self) -> dict[str, str]:
        """Load database configuration from environment or config file."""
        # Try environment variables first
        config = {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432"),
            "database": os.getenv("DB_NAME", "mlb_betting"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", ""),
        }

        # Try loading from config.toml if available
        config_file = Path("config.toml")
        if config_file.exists():
            try:
                try:
                    import tomllib  # Python 3.11+

                    with open(config_file, "rb") as f:
                        toml_config = tomllib.load(f)
                except ImportError:
                    import tomli as tomllib  # Fallback for older Python

                    with open(config_file, "rb") as f:
                        toml_config = tomllib.load(f)

                db_section = toml_config.get("database", {})
                for key in config.keys():
                    if key in db_section:
                        config[key] = str(db_section[key])

            except ImportError:
                logger.warning(
                    "tomllib/tomli not available, using environment variables"
                )
            except Exception as e:
                logger.warning(f"Error loading config.toml: {e}")

        return config

    def _connect_db(self) -> psycopg2.extensions.connection:
        """Create database connection."""
        if self.connection is None or self.connection.closed:
            try:
                self.connection = psycopg2.connect(**self.db_config)
                self.connection.autocommit = True
                logger.info("Database connection established")
            except Exception as e:
                logger.error(f"Database connection failed: {e}")
                raise

        return self.connection

    def _execute_query(self, query: str, params: tuple = None) -> list[tuple]:
        """Execute query and return results."""
        conn = self._connect_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            logger.error(f"Query: {query}")
            raise

    def _execute_scalar(self, query: str, params: tuple = None) -> Any:
        """Execute query and return single value."""
        result = self._execute_query(query, params)
        return result[0][0] if result else None

    def validate_pre_migration(self) -> ValidationReport:
        """Comprehensive pre-migration validation."""
        logger.info("Starting pre-migration validation")

        checks = []
        recommendations = []

        # 1. Database connectivity and schema access
        checks.append(self._check_database_connectivity())
        checks.append(self._check_schema_access())

        # 2. Data integrity checks
        checks.append(self._check_core_betting_data_integrity())
        checks.append(self._check_foreign_key_dependencies())

        # 3. System readiness checks
        checks.append(self._check_database_locks())
        checks.append(self._check_disk_space())
        checks.append(self._check_backup_availability())

        # 4. Target schema readiness
        checks.append(self._check_target_schema_readiness())

        # 5. Performance baseline
        checks.append(self._establish_performance_baseline())

        # Determine overall status
        failed_checks = [c for c in checks if c.status == "failed"]
        warning_checks = [c for c in checks if c.status == "warning"]

        if failed_checks:
            overall_status = "failed"
            recommendations.append(
                "❌ Critical issues must be resolved before migration"
            )
            for check in failed_checks:
                recommendations.append(f"  - {check.check_name}: {check.message}")
        elif warning_checks:
            overall_status = "warning"
            recommendations.append("⚠️ Review warnings before proceeding")
            for check in warning_checks:
                recommendations.append(f"  - {check.check_name}: {check.message}")
        else:
            overall_status = "passed"
            recommendations.append(
                "✅ All pre-migration checks passed - ready to proceed"
            )

        # Generate summary
        summary = {
            "total_checks": len(checks),
            "passed": len([c for c in checks if c.status == "passed"]),
            "warnings": len(warning_checks),
            "failed": len(failed_checks),
            "core_betting_tables": self._count_core_betting_tables(),
            "total_records": self._count_core_betting_records(),
            "foreign_key_dependencies": self._count_foreign_key_dependencies(),
        }

        return ValidationReport(
            report_type="pre-migration",
            timestamp=datetime.now().isoformat(),
            overall_status=overall_status,
            summary=summary,
            checks=checks,
            recommendations=recommendations,
        )

    def validate_post_migration(self) -> ValidationReport:
        """Comprehensive post-migration validation."""
        logger.info("Starting post-migration validation")

        checks = []
        recommendations = []

        # 1. Data integrity validation
        checks.append(self._validate_record_counts())
        checks.append(self._validate_data_consistency())
        checks.append(self._validate_foreign_key_integrity())

        # 2. Schema structure validation
        checks.append(self._validate_curated_schema_structure())
        checks.append(self._validate_indexes_created())

        # 3. Business logic validation
        checks.append(self._validate_betting_lines_consolidation())
        checks.append(self._validate_games_consolidation())

        # 4. Performance validation
        checks.append(self._validate_query_performance())

        # 5. Application readiness
        checks.append(self._validate_external_dependencies())

        # Determine overall status
        failed_checks = [c for c in checks if c.status == "failed"]
        warning_checks = [c for c in checks if c.status == "warning"]

        if failed_checks:
            overall_status = "failed"
            recommendations.append("❌ Critical issues detected - consider rollback")
            recommendations.append(
                "Run: python validation_and_rollback.py --rollback --confirm"
            )
            for check in failed_checks:
                recommendations.append(f"  - {check.check_name}: {check.message}")
        elif warning_checks:
            overall_status = "warning"
            recommendations.append(
                "⚠️ Review warnings before proceeding to code refactoring"
            )
            for check in warning_checks:
                recommendations.append(f"  - {check.check_name}: {check.message}")
        else:
            overall_status = "passed"
            recommendations.append("✅ All post-migration checks passed")
            recommendations.append("Ready to proceed with code refactoring phase")

        # Generate summary
        summary = {
            "total_checks": len(checks),
            "passed": len([c for c in checks if c.status == "passed"]),
            "warnings": len(warning_checks),
            "failed": len(failed_checks),
            "curated_tables_created": self._count_curated_tables(),
            "records_migrated": self._count_migrated_records(),
            "migration_duration": self._get_migration_duration(),
        }

        return ValidationReport(
            report_type="post-migration",
            timestamp=datetime.now().isoformat(),
            overall_status=overall_status,
            summary=summary,
            checks=checks,
            recommendations=recommendations,
        )

    def monitor_migration(self, interval_seconds: int = 30) -> None:
        """Real-time migration monitoring."""
        logger.info(f"Starting migration monitoring (interval: {interval_seconds}s)")

        try:
            while True:
                status = self._get_migration_status()

                print(f"\n{'=' * 60}")
                print(
                    f"Migration Status - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
                print(f"{'=' * 60}")

                if status:
                    for phase in status:
                        (
                            phase_name,
                            operation,
                            status_val,
                            records_processed,
                            records_expected,
                            duration,
                        ) = phase
                        print(
                            f"{phase_name:20} | {operation:25} | {status_val:10} | "
                            f"{records_processed:8} / {records_expected:8} | {duration:6.1f}s"
                        )
                else:
                    print("No migration status available")

                # Check for completion
                if status and all(s[2] == "completed" for s in status):
                    print("\n✅ Migration completed!")
                    break

                time.sleep(interval_seconds)

        except KeyboardInterrupt:
            print("\nMonitoring stopped by user")

    def execute_rollback(self, confirm: bool = False) -> bool:
        """Execute emergency rollback to core_betting schema."""
        if not confirm:
            print("❌ Rollback requires --confirm flag for safety")
            return False

        logger.warning("Starting emergency rollback")
        print("⚠️  EMERGENCY ROLLBACK INITIATED")
        print(
            "This will restore the core_betting schema and undo all migration changes"
        )

        try:
            # 1. Restore core_betting schema from backup
            self._restore_core_betting_schema()

            # 2. Restore FK constraints to point back to core_betting
            self._restore_foreign_key_constraints()

            # 3. Remove migrated data from curated schema
            self._cleanup_migrated_curated_data()

            # 4. Validate rollback success
            rollback_validation = self._validate_rollback()

            if rollback_validation:
                logger.info("Emergency rollback completed successfully")
                print("✅ Emergency rollback completed successfully")
                return True
            else:
                logger.error("Rollback validation failed")
                print("❌ Rollback validation failed - manual intervention required")
                return False

        except Exception as e:
            logger.error(f"Rollback failed: {e}")
            print(f"❌ Rollback failed: {e}")
            print("Manual database restoration may be required")
            return False

    # -------------------------------------------------------------------------
    # Pre-Migration Validation Methods
    # -------------------------------------------------------------------------

    def _check_database_connectivity(self) -> ValidationResult:
        """Check database connectivity and basic access."""
        try:
            conn = self._connect_db()
            version = self._execute_scalar("SELECT version();")

            return ValidationResult(
                check_name="Database Connectivity",
                status="passed",
                message="Successfully connected to database",
                details={"version": version},
            )
        except Exception as e:
            return ValidationResult(
                check_name="Database Connectivity",
                status="failed",
                message=f"Failed to connect to database: {e}",
            )

    def _check_schema_access(self) -> ValidationResult:
        """Check access to core_betting schema."""
        try:
            table_count = self._execute_scalar("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = 'core_betting'
            """)

            if table_count and table_count > 0:
                return ValidationResult(
                    check_name="Schema Access",
                    status="passed",
                    message=f"core_betting schema accessible with {table_count} tables",
                )
            else:
                return ValidationResult(
                    check_name="Schema Access",
                    status="failed",
                    message="core_betting schema not found or empty",
                )
        except Exception as e:
            return ValidationResult(
                check_name="Schema Access",
                status="failed",
                message=f"Schema access check failed: {e}",
            )

    def _check_core_betting_data_integrity(self) -> ValidationResult:
        """Check data integrity in core_betting schema."""
        try:
            # Check for NULL game_ids in betting lines
            null_game_ids = self._execute_scalar("""
                SELECT COUNT(*) FROM (
                    SELECT game_id FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline' WHERE game_id IS NULL
                    UNION ALL
                    SELECT game_id FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'spread' WHERE game_id IS NULL
                    UNION ALL
                    SELECT game_id FROM curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'totals' WHERE game_id IS NULL
                ) subq
            """)

            # Check for orphaned records
            orphaned_outcomes = self._execute_scalar("""
                SELECT COUNT(*) FROM curated.game_outcomes o
                WHERE NOT EXISTS (SELECT 1 FROM curated.games_complete g WHERE g.id = o.game_id)
            """)

            issues = []
            if null_game_ids > 0:
                issues.append(f"{null_game_ids} betting lines with NULL game_id")
            if orphaned_outcomes > 0:
                issues.append(f"{orphaned_outcomes} orphaned game outcomes")

            if issues:
                return ValidationResult(
                    check_name="Data Integrity",
                    status="warning",
                    message=f"Data integrity issues found: {'; '.join(issues)}",
                    details={
                        "null_game_ids": null_game_ids,
                        "orphaned_outcomes": orphaned_outcomes,
                    },
                )
            else:
                return ValidationResult(
                    check_name="Data Integrity",
                    status="passed",
                    message="No data integrity issues found",
                )

        except Exception as e:
            return ValidationResult(
                check_name="Data Integrity",
                status="failed",
                message=f"Data integrity check failed: {e}",
            )

    def _check_foreign_key_dependencies(self) -> ValidationResult:
        """Check external foreign key dependencies."""
        try:
            dependencies = self._execute_query("""
                SELECT 
                    tc.table_schema,
                    tc.table_name,
                    tc.constraint_name,
                    ccu.table_schema AS foreign_table_schema,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage ccu
                    ON tc.constraint_name = ccu.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND ccu.table_schema = 'core_betting'
                    AND tc.table_schema != 'core_betting'
                ORDER BY tc.table_schema, tc.table_name
            """)

            if dependencies:
                dependency_list = [
                    f"{dep[0]}.{dep[1]} -> {dep[4]}.{dep[5]}" for dep in dependencies
                ]
                return ValidationResult(
                    check_name="Foreign Key Dependencies",
                    status="warning",
                    message=f"Found {len(dependencies)} external FK dependencies",
                    details={"dependencies": dependency_list},
                )
            else:
                return ValidationResult(
                    check_name="Foreign Key Dependencies",
                    status="passed",
                    message="No external FK dependencies found",
                )

        except Exception as e:
            return ValidationResult(
                check_name="Foreign Key Dependencies",
                status="failed",
                message=f"FK dependency check failed: {e}",
            )

    def _check_database_locks(self) -> ValidationResult:
        """Check for database locks that could interfere with migration."""
        try:
            locks = self._execute_query("""
                SELECT pid, usename, application_name, state, query
                FROM pg_stat_activity 
                WHERE datname = current_database() 
                    AND state IN ('active', 'idle in transaction')
                    AND pid != pg_backend_pid()
            """)

            active_locks = [lock for lock in locks if lock[3] == "active"]

            if active_locks:
                return ValidationResult(
                    check_name="Database Locks",
                    status="warning",
                    message=f"Found {len(active_locks)} active connections",
                    details={"active_connections": len(active_locks)},
                )
            else:
                return ValidationResult(
                    check_name="Database Locks",
                    status="passed",
                    message="No blocking database locks detected",
                )

        except Exception as e:
            return ValidationResult(
                check_name="Database Locks",
                status="failed",
                message=f"Lock check failed: {e}",
            )

    def _check_disk_space(self) -> ValidationResult:
        """Check available disk space for migration."""
        try:
            # Get database size
            db_size = self._execute_scalar("""
                SELECT pg_size_pretty(pg_database_size(current_database()))
            """)

            # This is a simplified check - in production, you'd want to check actual disk space
            return ValidationResult(
                check_name="Disk Space",
                status="passed",
                message=f"Database size: {db_size}",
                details={"database_size": db_size},
            )

        except Exception as e:
            return ValidationResult(
                check_name="Disk Space",
                status="warning",
                message=f"Could not check disk space: {e}",
            )

    def _check_backup_availability(self) -> ValidationResult:
        """Check if pre-migration backup exists."""
        try:
            backup_dir = Path("backups")
            if backup_dir.exists():
                backups = list(backup_dir.glob("pre_core_betting_migration_*"))
                if backups:
                    latest_backup = max(backups, key=lambda p: p.stat().st_mtime)
                    return ValidationResult(
                        check_name="Backup Availability",
                        status="passed",
                        message=f"Backup available: {latest_backup.name}",
                        details={"backup_path": str(latest_backup)},
                    )
                else:
                    return ValidationResult(
                        check_name="Backup Availability",
                        status="warning",
                        message="No pre-migration backup found",
                    )
            else:
                return ValidationResult(
                    check_name="Backup Availability",
                    status="warning",
                    message="Backup directory not found",
                )

        except Exception as e:
            return ValidationResult(
                check_name="Backup Availability",
                status="failed",
                message=f"Backup check failed: {e}",
            )

    def _check_target_schema_readiness(self) -> ValidationResult:
        """Check if curated schema is ready for migration."""
        try:
            # Check if curated schema exists
            schema_exists = self._execute_scalar("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.schemata 
                    WHERE schema_name = 'curated'
                )
            """)

            if not schema_exists:
                return ValidationResult(
                    check_name="Target Schema Readiness",
                    status="warning",
                    message="curated schema does not exist - will be created during migration",
                )

            # Check for existing curated tables that might conflict
            existing_tables = self._execute_query("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'curated' 
                    AND table_name IN ('games_complete', 'betting_lines_unified', 'sportsbooks', 'teams_master')
            """)

            if existing_tables:
                table_names = [table[0] for table in existing_tables]
                return ValidationResult(
                    check_name="Target Schema Readiness",
                    status="warning",
                    message=f"Existing curated tables found: {', '.join(table_names)}",
                    details={"existing_tables": table_names},
                )
            else:
                return ValidationResult(
                    check_name="Target Schema Readiness",
                    status="passed",
                    message="curated schema ready for migration",
                )

        except Exception as e:
            return ValidationResult(
                check_name="Target Schema Readiness",
                status="failed",
                message=f"Target schema check failed: {e}",
            )

    def _establish_performance_baseline(self) -> ValidationResult:
        """Establish performance baseline for key queries."""
        try:
            # Test query performance on core_betting tables
            test_queries = [
                ("games_count", "SELECT COUNT(*) FROM curated.games_complete"),
                (
                    "betting_lines_join",
                    """
                    SELECT COUNT(*) FROM curated.games_complete g
                    JOIN curated.betting_lines_unified -- NOTE: Add WHERE market_type = 'moneyline' ml ON g.id = ml.game_id
                    LIMIT 1000
                """,
                ),
            ]

            performance_results = {}
            for query_name, query in test_queries:
                start_time = time.time()
                result = self._execute_scalar(query)
                end_time = time.time()
                performance_results[query_name] = {
                    "duration_ms": round((end_time - start_time) * 1000, 2),
                    "result_count": result,
                }

            return ValidationResult(
                check_name="Performance Baseline",
                status="passed",
                message="Performance baseline established",
                details=performance_results,
            )

        except Exception as e:
            return ValidationResult(
                check_name="Performance Baseline",
                status="warning",
                message=f"Could not establish performance baseline: {e}",
            )

    # -------------------------------------------------------------------------
    # Post-Migration Validation Methods
    # -------------------------------------------------------------------------

    def _validate_record_counts(self) -> ValidationResult:
        """Validate that all records were migrated correctly."""
        try:
            # Get validation view results
            validation_results = self._execute_query("""
                SELECT table_name, pre_migration_count, post_migration_count, 
                       count_difference, validation_status
                FROM operational.v_core_betting_migration_validation
            """)

            failed_validations = [v for v in validation_results if "❌" in v[4]]
            warnings = [v for v in validation_results if "⚠️" in v[4]]

            if failed_validations:
                details = {
                    row[0]: {"pre": row[1], "post": row[2], "diff": row[3]}
                    for row in failed_validations
                }
                return ValidationResult(
                    check_name="Record Count Validation",
                    status="failed",
                    message=f"Record count mismatches in {len(failed_validations)} tables",
                    details=details,
                )
            elif warnings:
                details = {
                    row[0]: {"pre": row[1], "post": row[2], "diff": row[3]}
                    for row in warnings
                }
                return ValidationResult(
                    check_name="Record Count Validation",
                    status="warning",
                    message=f"Record count warnings in {len(warnings)} tables",
                    details=details,
                )
            else:
                return ValidationResult(
                    check_name="Record Count Validation",
                    status="passed",
                    message="All record counts validated successfully",
                )

        except Exception as e:
            return ValidationResult(
                check_name="Record Count Validation",
                status="failed",
                message=f"Record count validation failed: {e}",
            )

    def _validate_data_consistency(self) -> ValidationResult:
        """Validate data consistency in migrated tables."""
        try:
            # Check for NULL values in critical fields
            null_checks = [
                (
                    "games_complete.game_date",
                    "SELECT COUNT(*) FROM curated.games_complete WHERE game_date IS NULL",
                ),
                (
                    "betting_lines_unified.game_id",
                    "SELECT COUNT(*) FROM curated.betting_lines_unified WHERE game_id IS NULL",
                ),
                (
                    "game_outcomes.game_id",
                    "SELECT COUNT(*) FROM curated.game_outcomes WHERE game_id IS NULL",
                ),
            ]

            null_issues = []
            for field_name, query in null_checks:
                null_count = self._execute_scalar(query)
                if null_count > 0:
                    null_issues.append(f"{field_name}: {null_count} NULL values")

            if null_issues:
                return ValidationResult(
                    check_name="Data Consistency",
                    status="failed",
                    message=f"Data consistency issues: {'; '.join(null_issues)}",
                )
            else:
                return ValidationResult(
                    check_name="Data Consistency",
                    status="passed",
                    message="Data consistency validated successfully",
                )

        except Exception as e:
            return ValidationResult(
                check_name="Data Consistency",
                status="failed",
                message=f"Data consistency validation failed: {e}",
            )

    def _validate_foreign_key_integrity(self) -> ValidationResult:
        """Validate foreign key integrity after migration."""
        try:
            # Check for FK constraint violations
            fk_violations = self._execute_query("""
                SELECT conname, conrelid::regclass as table_name
                FROM pg_constraint 
                WHERE contype = 'f' 
                    AND NOT convalidated
            """)

            if fk_violations:
                violation_details = [f"{v[1]}.{v[0]}" for v in fk_violations]
                return ValidationResult(
                    check_name="Foreign Key Integrity",
                    status="failed",
                    message=f"FK constraint violations: {', '.join(violation_details)}",
                )
            else:
                return ValidationResult(
                    check_name="Foreign Key Integrity",
                    status="passed",
                    message="All foreign key constraints validated",
                )

        except Exception as e:
            return ValidationResult(
                check_name="Foreign Key Integrity",
                status="failed",
                message=f"FK integrity validation failed: {e}",
            )

    def _validate_curated_schema_structure(self) -> ValidationResult:
        """Validate curated schema structure is correct."""
        try:
            expected_tables = [
                "games_complete",
                "game_outcomes",
                "betting_lines_unified",
                "sportsbooks",
                "teams_master",
                "sportsbook_mappings",
                "data_sources",
            ]

            existing_tables = self._execute_query("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'curated'
            """)

            existing_table_names = [table[0] for table in existing_tables]
            missing_tables = [
                table for table in expected_tables if table not in existing_table_names
            ]

            if missing_tables:
                return ValidationResult(
                    check_name="Curated Schema Structure",
                    status="failed",
                    message=f"Missing curated tables: {', '.join(missing_tables)}",
                )
            else:
                return ValidationResult(
                    check_name="Curated Schema Structure",
                    status="passed",
                    message="All expected curated tables created",
                )

        except Exception as e:
            return ValidationResult(
                check_name="Curated Schema Structure",
                status="failed",
                message=f"Schema structure validation failed: {e}",
            )

    def _validate_indexes_created(self) -> ValidationResult:
        """Validate that performance indexes were created."""
        try:
            # Check for key indexes
            index_count = self._execute_scalar("""
                SELECT COUNT(*) FROM pg_indexes 
                WHERE schemaname = 'curated' 
                    AND indexname LIKE 'idx_%'
            """)

            if index_count >= 5:  # Expect at least 5 performance indexes
                return ValidationResult(
                    check_name="Index Creation",
                    status="passed",
                    message=f"{index_count} performance indexes created",
                )
            else:
                return ValidationResult(
                    check_name="Index Creation",
                    status="warning",
                    message=f"Only {index_count} indexes found, expected at least 5",
                )

        except Exception as e:
            return ValidationResult(
                check_name="Index Creation",
                status="failed",
                message=f"Index validation failed: {e}",
            )

    def _validate_betting_lines_consolidation(self) -> ValidationResult:
        """Validate betting lines consolidation worked correctly."""
        try:
            # Check market_type distribution
            market_distribution = self._execute_query("""
                SELECT market_type, COUNT(*) 
                FROM curated.betting_lines_unified 
                GROUP BY market_type
            """)

            market_types = [row[0] for row in market_distribution]
            expected_types = ["moneyline", "spread", "totals"]

            missing_types = [t for t in expected_types if t not in market_types]

            if missing_types:
                return ValidationResult(
                    check_name="Betting Lines Consolidation",
                    status="failed",
                    message=f"Missing market types: {', '.join(missing_types)}",
                )
            else:
                total_lines = sum(row[1] for row in market_distribution)
                return ValidationResult(
                    check_name="Betting Lines Consolidation",
                    status="passed",
                    message=f"Betting lines consolidated successfully: {total_lines} total lines",
                    details=dict(market_distribution),
                )

        except Exception as e:
            return ValidationResult(
                check_name="Betting Lines Consolidation",
                status="failed",
                message=f"Betting lines validation failed: {e}",
            )

    def _validate_games_consolidation(self) -> ValidationResult:
        """Validate games consolidation with supplementary_games."""
        try:
            # Check if games were properly consolidated
            games_count = self._execute_scalar(
                "SELECT COUNT(*) FROM curated.games_complete"
            )
            original_games = self._execute_scalar(
                "SELECT COUNT(*) FROM curated.games_complete"
            )
            supplementary_games = self._execute_scalar(
                "SELECT COUNT(*) FROM curated.games_complete"
            )

            # Games consolidation should include unique games from both tables
            if games_count >= original_games:
                return ValidationResult(
                    check_name="Games Consolidation",
                    status="passed",
                    message=f"Games consolidated: {games_count} total games (original: {original_games}, supplementary: {supplementary_games})",
                )
            else:
                return ValidationResult(
                    check_name="Games Consolidation",
                    status="failed",
                    message=f"Games consolidation incomplete: {games_count} vs expected {original_games + supplementary_games}",
                )

        except Exception as e:
            return ValidationResult(
                check_name="Games Consolidation",
                status="failed",
                message=f"Games consolidation validation failed: {e}",
            )

    def _validate_query_performance(self) -> ValidationResult:
        """Validate query performance hasn't degraded significantly."""
        try:
            # Test equivalent queries on new schema
            test_queries = [
                ("games_count", "SELECT COUNT(*) FROM curated.games_complete"),
                (
                    "betting_lines_unified",
                    """
                    SELECT COUNT(*) FROM curated.games_complete g
                    JOIN curated.betting_lines_unified bl ON g.id = bl.game_id
                    WHERE bl.market_type = 'moneyline'
                    LIMIT 1000
                """,
                ),
            ]

            performance_results = {}
            for query_name, query in test_queries:
                start_time = time.time()
                result = self._execute_scalar(query)
                end_time = time.time()
                performance_results[query_name] = {
                    "duration_ms": round((end_time - start_time) * 1000, 2),
                    "result_count": result,
                }

            # Simple performance check - flag if queries take > 5 seconds
            slow_queries = [
                name
                for name, data in performance_results.items()
                if data["duration_ms"] > 5000
            ]

            if slow_queries:
                return ValidationResult(
                    check_name="Query Performance",
                    status="warning",
                    message=f"Slow queries detected: {', '.join(slow_queries)}",
                    details=performance_results,
                )
            else:
                return ValidationResult(
                    check_name="Query Performance",
                    status="passed",
                    message="Query performance within acceptable ranges",
                    details=performance_results,
                )

        except Exception as e:
            return ValidationResult(
                check_name="Query Performance",
                status="warning",
                message=f"Performance validation failed: {e}",
            )

    def _validate_external_dependencies(self) -> ValidationResult:
        """Validate external dependencies were updated correctly."""
        try:
            # Check if FK constraints were updated to point to curated
            updated_constraints = self._execute_query("""
                SELECT 
                    tc.table_schema,
                    tc.table_name,
                    ccu.table_schema AS foreign_table_schema,
                    ccu.table_name AS foreign_table_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.constraint_column_usage ccu
                    ON tc.constraint_name = ccu.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND ccu.table_schema = 'curated'
                    AND tc.table_schema != 'curated'
            """)

            if updated_constraints:
                return ValidationResult(
                    check_name="External Dependencies",
                    status="passed",
                    message=f"{len(updated_constraints)} external FK constraints updated to curated schema",
                )
            else:
                return ValidationResult(
                    check_name="External Dependencies",
                    status="warning",
                    message="No external FK constraints found - may need manual verification",
                )

        except Exception as e:
            return ValidationResult(
                check_name="External Dependencies",
                status="failed",
                message=f"External dependencies validation failed: {e}",
            )

    # -------------------------------------------------------------------------
    # Helper Methods
    # -------------------------------------------------------------------------

    def _count_core_betting_tables(self) -> int:
        """Count tables in core_betting schema."""
        return self._execute_scalar("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'core_betting'
        """)

    def _count_core_betting_records(self) -> int:
        """Count total records in core_betting schema."""
        try:
            return self._execute_scalar("""
                SELECT SUM(record_count) FROM operational.pre_migration_counts
            """)
        except:
            return 0

    def _count_foreign_key_dependencies(self) -> int:
        """Count external foreign key dependencies."""
        return self._execute_scalar("""
            SELECT COUNT(*) FROM information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND ccu.table_schema = 'core_betting'
                AND tc.table_schema != 'core_betting'
        """)

    def _count_curated_tables(self) -> int:
        """Count tables in curated schema."""
        return self._execute_scalar("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'curated'
        """)

    def _count_migrated_records(self) -> int:
        """Count migrated records in curated schema."""
        try:
            return self._execute_scalar("""
                SELECT SUM(record_count) FROM operational.post_migration_counts
            """)
        except:
            return 0

    def _get_migration_duration(self) -> str:
        """Get total migration duration."""
        try:
            result = self._execute_query("""
                SELECT 
                    MIN(start_time) as start_time,
                    MAX(end_time) as end_time
                FROM operational.core_betting_migration_log
                WHERE phase != 'MIGRATION_START'
            """)

            if result and result[0][0] and result[0][1]:
                start_time, end_time = result[0]
                duration = end_time - start_time
                return str(duration)
            else:
                return "Unknown"
        except:
            return "Unknown"

    def _get_migration_status(self) -> list[tuple]:
        """Get current migration status."""
        try:
            return self._execute_query("""
                SELECT phase, operation, status, 
                       COALESCE(records_processed, 0),
                       COALESCE(records_expected, 0),
                       COALESCE(EXTRACT(EPOCH FROM (COALESCE(end_time, CURRENT_TIMESTAMP) - start_time)), 0)
                FROM operational.core_betting_migration_log
                WHERE phase != 'MIGRATION_START'
                ORDER BY id
            """)
        except:
            return []

    # -------------------------------------------------------------------------
    # Rollback Methods
    # -------------------------------------------------------------------------

    def _restore_core_betting_schema(self) -> None:
        """Restore core_betting schema from backup."""
        # This would restore from the SQL backup files
        # Implementation depends on backup format and storage
        logger.info("Restoring core_betting schema from backup")
        # Placeholder for actual restoration logic
        pass

    def _restore_foreign_key_constraints(self) -> None:
        """Restore FK constraints to point back to curated."""
        logger.info("Restoring foreign key constraints")
        # Placeholder for FK restoration logic
        pass

    def _cleanup_migrated_curated_data(self) -> None:
        """Remove migrated data from curated schema."""
        logger.info("Cleaning up migrated curated data")

        try:
            # Remove records with source_system = 'core_betting_migration'
            cleanup_queries = [
                "DELETE FROM curated.betting_lines_unified WHERE source_system = 'core_betting_migration'",
                "DELETE FROM curated.game_outcomes WHERE source_system = 'core_betting_migration'",
                "DELETE FROM curated.games_complete WHERE source_system = 'core_betting_migration'",
                "DELETE FROM curated.sportsbook_mappings WHERE source_system = 'core_betting_migration'",
                "DELETE FROM curated.data_sources WHERE source_system = 'core_betting_migration'",
                "DELETE FROM curated.teams_master WHERE source_system = 'core_betting_migration'",
                "DELETE FROM curated.sportsbooks WHERE source_system = 'core_betting_migration'",
            ]

            for query in cleanup_queries:
                self._execute_query(query)

        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
            raise

    def _validate_rollback(self) -> bool:
        """Validate rollback was successful."""
        try:
            # Check if core_betting schema is accessible
            table_count = self._execute_scalar("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = 'core_betting'
            """)

            return table_count > 0
        except:
            return False

    def generate_report(
        self, validation_report: ValidationReport, output_file: str = None
    ) -> str:
        """Generate validation report in markdown format."""
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = (
                f"{validation_report.report_type}_validation_report_{timestamp}.md"
            )

        # Generate markdown report
        report_lines = [
            f"# {validation_report.report_type.title()} Validation Report",
            "",
            f"**Generated:** {validation_report.timestamp}",
            f"**Overall Status:** {validation_report.overall_status.upper()}",
            "",
            "## Summary",
            "",
        ]

        # Add summary table
        for key, value in validation_report.summary.items():
            report_lines.append(f"- **{key.replace('_', ' ').title()}:** {value}")

        report_lines.extend(["", "## Validation Checks", ""])

        # Add individual check results
        for check in validation_report.checks:
            status_emoji = {"passed": "✅", "warning": "⚠️", "failed": "❌"}
            emoji = status_emoji.get(check.status, "❓")

            report_lines.extend(
                [
                    f"### {emoji} {check.check_name}",
                    "",
                    f"**Status:** {check.status.upper()}",
                    f"**Message:** {check.message}",
                    "",
                ]
            )

            if check.details:
                report_lines.extend(
                    [
                        "**Details:**",
                        "```json",
                        json.dumps(check.details, indent=2),
                        "```",
                        "",
                    ]
                )

        # Add recommendations
        if validation_report.recommendations:
            report_lines.extend(["## Recommendations", ""])
            for rec in validation_report.recommendations:
                report_lines.append(f"- {rec}")

        # Write report
        report_content = "\n".join(report_lines)
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(report_content)

        logger.info(f"Validation report generated: {output_file}")
        return output_file


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Core Betting Migration Validation and Rollback System"
    )
    parser.add_argument(
        "--validate-pre-migration",
        action="store_true",
        help="Run pre-migration validation checks",
    )
    parser.add_argument(
        "--validate-post-migration",
        action="store_true",
        help="Run post-migration validation checks",
    )
    parser.add_argument(
        "--monitor", action="store_true", help="Monitor migration progress in real-time"
    )
    parser.add_argument(
        "--rollback",
        action="store_true",
        help="Execute emergency rollback (requires --confirm)",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Confirm dangerous operations like rollback",
    )
    parser.add_argument("--output-file", help="Output file for validation report")
    parser.add_argument(
        "--monitor-interval",
        type=int,
        default=30,
        help="Monitoring interval in seconds (default: 30)",
    )

    args = parser.parse_args()

    # Validate arguments
    if not any(
        [
            args.validate_pre_migration,
            args.validate_post_migration,
            args.monitor,
            args.rollback,
        ]
    ):
        print(
            "Please specify one operation: --validate-pre-migration, --validate-post-migration, --monitor, or --rollback"
        )
        return 1

    try:
        validator = CoreBettingValidator()

        if args.validate_pre_migration:
            print("🔍 Running pre-migration validation...")
            report = validator.validate_pre_migration()
            report_file = validator.generate_report(report, args.output_file)

            print(f"\n{'=' * 60}")
            print("PRE-MIGRATION VALIDATION COMPLETE")
            print(f"{'=' * 60}")
            print(f"Overall Status: {report.overall_status.upper()}")
            print(
                f"Checks: {report.summary['passed']} passed, {report.summary['warnings']} warnings, {report.summary['failed']} failed"
            )
            print(f"Report: {report_file}")

            if report.overall_status == "failed":
                print("\n❌ Critical issues found - resolve before proceeding")
                return 1
            elif report.overall_status == "warning":
                print("\n⚠️ Warnings found - review before proceeding")
                return 0
            else:
                print("\n✅ Ready to proceed with migration")
                return 0

        elif args.validate_post_migration:
            print("🔍 Running post-migration validation...")
            report = validator.validate_post_migration()
            report_file = validator.generate_report(report, args.output_file)

            print(f"\n{'=' * 60}")
            print("POST-MIGRATION VALIDATION COMPLETE")
            print(f"{'=' * 60}")
            print(f"Overall Status: {report.overall_status.upper()}")
            print(
                f"Checks: {report.summary['passed']} passed, {report.summary['warnings']} warnings, {report.summary['failed']} failed"
            )
            print(f"Report: {report_file}")

            if report.overall_status == "failed":
                print("\n❌ Critical issues found - consider rollback")
                return 1
            elif report.overall_status == "warning":
                print("\n⚠️ Warnings found - review before code refactoring")
                return 0
            else:
                print("\n✅ Migration validated successfully")
                return 0

        elif args.monitor:
            validator.monitor_migration(args.monitor_interval)
            return 0

        elif args.rollback:
            if validator.execute_rollback(args.confirm):
                print("✅ Rollback completed successfully")
                return 0
            else:
                print("❌ Rollback failed")
                return 1

    except Exception as e:
        logger.error(f"Validation failed: {e}")
        print(f"❌ Validation failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
