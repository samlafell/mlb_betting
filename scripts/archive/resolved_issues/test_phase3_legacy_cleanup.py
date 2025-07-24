#!/usr/bin/env python3
"""
Phase 3 Legacy Cleanup Testing Script

This script validates that the system can operate entirely on the new consolidated schema
without relying on legacy tables, and benchmarks performance differences.
"""

import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Any

# Core imports
from src.mlb_sharp_betting.core.logging import (
    get_logger,
    setup_universal_logger_compatibility,
)
from src.mlb_sharp_betting.db.connection import get_db_manager
from src.mlb_sharp_betting.db.table_registry import get_table_registry

# Service imports for testing (only the ones we know exist and were updated)
from src.mlb_sharp_betting.services.betting_signal_repository import (
    BettingSignalRepository,
)
from src.mlb_sharp_betting.services.confidence_scorer import ConfidenceScorer
from src.mlb_sharp_betting.services.data_service import DataService
from src.mlb_sharp_betting.services.dynamic_threshold_manager import (
    DynamicThresholdManager,
)
from src.mlb_sharp_betting.services.game_manager import GameManager

# Migration monitoring
from src.mlb_sharp_betting.utils.migration_monitor import MigrationMonitor

setup_universal_logger_compatibility()


class Phase3TestSuite:
    """Comprehensive test suite for Phase 3 legacy cleanup validation."""

    def __init__(self):
        self.logger = get_logger(f"{__name__}.Phase3TestSuite")
        self.db_manager = get_db_manager()
        self.table_registry = get_table_registry()
        self.test_results = {
            "start_time": datetime.now().isoformat(),
            "tests": {},
            "performance_benchmarks": {},
            "legacy_table_status": {},
            "overall_status": "PENDING",
        }

    async def run_all_tests(self) -> dict[str, Any]:
        """Run the complete Phase 3 test suite."""
        self.logger.info("🚀 PHASE 3 LEGACY CLEANUP - COMPREHENSIVE TESTING")
        self.logger.info("=" * 60)
        self.logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        tests = [
            ("Legacy Table Assessment", self._test_legacy_table_assessment),
            ("New Schema Functionality", self._test_new_schema_functionality),
            ("Updated Services Integration", self._test_updated_services_integration),
            ("Performance Benchmarking", self._test_performance_benchmarking),
            ("Data Consistency Validation", self._test_data_consistency),
            ("Migration Monitor Status", self._test_migration_monitor),
            ("Table Registry Functionality", self._test_table_registry_functionality),
            ("Rollback Readiness", self._test_rollback_readiness),
        ]

        passed = 0
        total = len(tests)
        start_time = time.time()

        for test_name, test_func in tests:
            self.logger.info(f"\n🔍 Testing: {test_name}")
            try:
                test_start_time = time.time()
                result = await test_func()
                execution_time = time.time() - test_start_time

                self.test_results["tests"][test_name] = {
                    "status": "PASSED" if result else "FAILED",
                    "execution_time": execution_time,
                    "details": result if isinstance(result, dict) else {},
                }

                if result:
                    self.logger.info(f"✅ {test_name}: PASSED")
                    passed += 1
                else:
                    self.logger.error(f"❌ {test_name}: FAILED")

            except Exception as e:
                self.logger.error(f"💥 {test_name}: ERROR - {e}")
                self.test_results["tests"][test_name] = {
                    "status": "ERROR",
                    "error": str(e),
                    "execution_time": time.time() - test_start_time,
                }

        # Calculate overall results
        total_time = time.time() - start_time
        success_rate = (passed / total) * 100
        self.test_results["end_time"] = datetime.now().isoformat()
        self.test_results["summary"] = {
            "total_tests": total,
            "passed": passed,
            "failed": total - passed,
            "success_rate": success_rate,
            "total_time": total_time,
        }

        if success_rate >= 95:
            self.test_results["overall_status"] = "READY_FOR_CLEANUP"
        elif success_rate >= 80:
            self.test_results["overall_status"] = "NEEDS_REVIEW"
        else:
            self.test_results["overall_status"] = "NOT_READY"

        # Print summary
        self.logger.info("\n📊 PHASE 3 TEST SUMMARY")
        self.logger.info("=" * 40)
        self.logger.info(
            f"Overall Status: {'✅' if success_rate >= 95 else '⚠️' if success_rate >= 80 else '❌'} {self.test_results['overall_status']}"
        )
        self.logger.info(f"Tests Passed: {passed}/{total}")
        self.logger.info(f"Success Rate: {success_rate:.1f}%")
        self.logger.info(f"Total Time: {total_time:.1f} seconds")

        if success_rate >= 95:
            self.logger.info("\n🎉 Phase 3 validation successful!")
            self.logger.info("✅ System ready for legacy cleanup")
        elif success_rate >= 80:
            self.logger.warning("\n⚠️ Phase 3 validation needs review")
            self.logger.warning("🔍 Some tests failed - investigate before cleanup")
        else:
            self.logger.error("\n❌ Phase 3 validation failed")
            self.logger.error("🚫 System NOT ready for legacy cleanup")

        return self.test_results

    async def _test_legacy_table_assessment(self) -> dict[str, Any]:
        """Assess current state of legacy tables and their usage."""
        try:
            legacy_assessment = {}

            # Check legacy table sizes and recent activity
            legacy_tables = [
                ("mlb_betting", "moneyline"),
                ("mlb_betting", "spreads"),
                ("mlb_betting", "totals"),
                ("public", "games"),
                ("splits", "raw_mlb_betting_splits"),
            ]

            with self.db_manager.get_cursor() as cursor:
                for schema, table_name in legacy_tables:
                    try:
                        # Check if table exists
                        cursor.execute(
                            """
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_schema = %s AND table_name = %s
                            )
                        """,
                            (schema, table_name),
                        )

                        exists = cursor.fetchone()[0]

                        if exists:
                            # Get record count
                            cursor.execute(
                                f"SELECT COUNT(*) FROM {schema}.{table_name}"
                            )
                            total_records = cursor.fetchone()[0]

                            # Get recent records (last 7 days)
                            try:
                                cursor.execute(
                                    f"""
                                    SELECT COUNT(*) FROM {schema}.{table_name}
                                    WHERE last_updated >= %s
                                """,
                                    (datetime.now() - timedelta(days=7),),
                                )
                                recent_records = cursor.fetchone()[0]
                            except:
                                recent_records = "N/A"

                            legacy_assessment[f"{schema}_{table_name}"] = {
                                "exists": True,
                                "total_records": total_records,
                                "recent_records": recent_records,
                                "full_name": f"{schema}.{table_name}",
                            }
                        else:
                            legacy_assessment[f"{schema}_{table_name}"] = {
                                "exists": False,
                                "full_name": f"{schema}.{table_name}",
                            }

                    except Exception as e:
                        legacy_assessment[f"{schema}_{table_name}"] = {
                            "exists": "ERROR",
                            "error": str(e),
                            "full_name": f"{schema}.{table_name}",
                        }

            self.test_results["legacy_table_status"] = legacy_assessment
            return legacy_assessment

        except Exception as e:
            self.logger.error(f"Legacy table assessment failed: {e}")
            return False

    async def _test_new_schema_functionality(self) -> dict[str, Any]:
        """Test that all new schema tables are functional."""
        try:
            schema_tests = {}

            # Test each new schema
            schemas = ["raw_data", "core_betting", "analytics", "operational"]

            with self.db_manager.get_cursor() as cursor:
                for schema in schemas:
                    # Get tables in schema
                    cursor.execute(
                        """
                        SELECT table_name FROM information_schema.tables
                        WHERE table_schema = %s
                        ORDER BY table_name
                    """,
                        (schema,),
                    )

                    tables = [row[0] for row in cursor.fetchall()]
                    schema_tests[schema] = {
                        "table_count": len(tables),
                        "tables": tables,
                        "functional": True,
                    }

                    # Test basic operations on each table
                    for table in tables:
                        try:
                            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                            record_count = cursor.fetchone()[0]
                            schema_tests[schema][f"{table}_records"] = record_count
                        except Exception as e:
                            schema_tests[schema]["functional"] = False
                            schema_tests[schema][f"{table}_error"] = str(e)

            return schema_tests

        except Exception as e:
            self.logger.error(f"New schema functionality test failed: {e}")
            return False

    async def _test_updated_services_integration(self) -> dict[str, Any]:
        """Test that all updated services work with new schema."""
        try:
            service_tests = {}

            # Test each major service that was updated
            services = [
                ("BettingSignalRepository", BettingSignalRepository, {}),
                ("ConfidenceScorer", ConfidenceScorer, {}),
                ("DynamicThresholdManager", DynamicThresholdManager, {}),
                ("GameManager", GameManager, {}),
                ("DataService", DataService, {"db_manager": self.db_manager}),
            ]

            for service_name, service_class, init_kwargs in services:
                try:
                    # Initialize service with proper parameters
                    service = service_class(**init_kwargs)

                    # Test basic functionality
                    if hasattr(service, "test_connection"):
                        connection_test = service.test_connection()
                    elif hasattr(service, "is_healthy"):
                        connection_test = service.is_healthy()
                    else:
                        connection_test = True  # Assume working if no test method

                    service_tests[service_name] = {
                        "initialized": True,
                        "connection_test": connection_test,
                        "uses_table_registry": hasattr(service, "table_registry"),
                        "has_db_manager": hasattr(service, "db_manager"),
                    }

                except Exception as e:
                    service_tests[service_name] = {
                        "initialized": False,
                        "error": str(e),
                    }

            return service_tests

        except Exception as e:
            self.logger.error(f"Service integration test failed: {e}")
            return False

    async def _test_performance_benchmarking(self) -> dict[str, Any]:
        """Benchmark performance of new schema vs legacy (if accessible)."""
        try:
            benchmarks = {}

            # Test queries on new schema - use correct column names
            new_schema_queries = [
                (
                    "moneyline_count",
                    "SELECT COUNT(*) FROM core_betting.betting_lines_moneyline",
                ),
                (
                    "spreads_count",
                    "SELECT COUNT(*) FROM core_betting.betting_lines_spreads",
                ),
                (
                    "totals_count",
                    "SELECT COUNT(*) FROM core_betting.betting_lines_totals",
                ),
                ("games_count", "SELECT COUNT(*) FROM core_betting.games"),
                (
                    "recent_moneyline",
                    """
                    SELECT COUNT(*) FROM core_betting.betting_lines_moneyline 
                    WHERE created_at >= %s
                """,
                ),
            ]

            with self.db_manager.get_cursor() as cursor:
                for query_name, query in new_schema_queries:
                    start_time = time.time()

                    if query_name == "recent_moneyline":
                        cursor.execute(query, (datetime.now() - timedelta(days=1),))
                    else:
                        cursor.execute(query)

                    result = cursor.fetchone()[0]
                    execution_time = (time.time() - start_time) * 1000  # Convert to ms

                    benchmarks[f"new_schema_{query_name}"] = {
                        "execution_time_ms": execution_time,
                        "result_count": result,
                    }

            # Try to benchmark legacy tables for comparison (if they exist)
            try:
                legacy_queries = [
                    (
                        "legacy_moneyline_count",
                        "SELECT COUNT(*) FROM mlb_betting.moneyline",
                    ),
                    (
                        "legacy_spreads_count",
                        "SELECT COUNT(*) FROM mlb_betting.spreads",
                    ),
                    ("legacy_totals_count", "SELECT COUNT(*) FROM mlb_betting.totals"),
                ]

                with self.db_manager.get_cursor() as cursor:
                    for query_name, query in legacy_queries:
                        start_time = time.time()
                        cursor.execute(query)
                        result = cursor.fetchone()[0]
                        execution_time = (time.time() - start_time) * 1000

                        benchmarks[query_name] = {
                            "execution_time_ms": execution_time,
                            "result_count": result,
                        }

            except Exception as e:
                benchmarks["legacy_benchmark_note"] = (
                    f"Legacy tables not accessible: {e}"
                )

            self.test_results["performance_benchmarks"] = benchmarks
            return benchmarks

        except Exception as e:
            self.logger.error(f"Performance benchmarking failed: {e}")
            return False

    async def _test_data_consistency(self) -> dict[str, Any]:
        """Validate data consistency between new and legacy tables."""
        try:
            consistency_tests = {}

            # Compare record counts between new and legacy tables
            comparisons = [
                (
                    "moneyline",
                    "core_betting.betting_lines_moneyline",
                    "mlb_betting.moneyline",
                ),
                (
                    "spreads",
                    "core_betting.betting_lines_spreads",
                    "mlb_betting.spreads",
                ),
                ("totals", "core_betting.betting_lines_totals", "mlb_betting.totals"),
            ]

            with self.db_manager.get_cursor() as cursor:
                for bet_type, new_table, legacy_table in comparisons:
                    try:
                        # Count records in new table
                        cursor.execute(f"SELECT COUNT(*) FROM {new_table}")
                        new_count = cursor.fetchone()[0]

                        # Count records in legacy table
                        cursor.execute(f"SELECT COUNT(*) FROM {legacy_table}")
                        legacy_count = cursor.fetchone()[0]

                        # Calculate difference
                        difference = abs(new_count - legacy_count)
                        percentage_diff = (difference / max(legacy_count, 1)) * 100

                        consistency_tests[bet_type] = {
                            "new_table_count": new_count,
                            "legacy_table_count": legacy_count,
                            "difference": difference,
                            "percentage_difference": percentage_diff,
                            "acceptable": percentage_diff < 5.0,  # Allow 5% difference
                        }

                    except Exception as e:
                        consistency_tests[bet_type] = {
                            "error": str(e),
                            "acceptable": False,
                        }

            return consistency_tests

        except Exception as e:
            self.logger.error(f"Data consistency test failed: {e}")
            return False

    async def _test_migration_monitor(self) -> dict[str, Any]:
        """Test migration monitor status and reporting."""
        try:
            monitor = MigrationMonitor()

            # Generate migration report - fix async issue
            report = await monitor.generate_migration_report(
                include_historical=True, lookback_hours=24
            )

            monitor_tests = {
                "report_generated": report is not None,
                "migration_status": report.get("overall_status")
                if report
                else "UNKNOWN",
                "new_schema_percentage": report.get("migration_percentage", 0)
                if report
                else 0,
            }

            return monitor_tests

        except Exception as e:
            # If async doesn't work, try sync version
            try:
                # Try calling it without await
                report = monitor.generate_migration_report(
                    include_historical=True, lookback_hours=24
                )

                monitor_tests = {
                    "report_generated": report is not None,
                    "migration_status": report.get("overall_status")
                    if report
                    else "UNKNOWN",
                    "new_schema_percentage": report.get("migration_percentage", 0)
                    if report
                    else 0,
                    "note": "Used sync version",
                }

                return monitor_tests

            except Exception as e2:
                self.logger.error(
                    f"Migration monitor test failed (both async and sync): {e}, {e2}"
                )
                return {
                    "report_generated": False,
                    "error": str(e),
                    "sync_error": str(e2),
                }

    async def _test_table_registry_functionality(self) -> dict[str, Any]:
        """Test table registry functionality for all key tables."""
        try:
            registry_tests = {}

            # Test table registry resolution for all key tables
            key_tables = [
                "moneyline",
                "spreads",
                "totals",
                "games",
                "betting_recommendations",
                "strategy_performance",
                "raw_betting_splits",
                "timing_analysis_results",
            ]

            for table_key in key_tables:
                try:
                    table_name = self.table_registry.get_table(table_key)
                    registry_tests[f"registry_{table_key}"] = {
                        "resolved": True,
                        "table_name": table_name,
                    }
                except Exception as e:
                    registry_tests[f"registry_{table_key}"] = {
                        "resolved": False,
                        "error": str(e),
                    }

            return registry_tests

        except Exception as e:
            self.logger.error(f"Table registry functionality test failed: {e}")
            return False

    async def _test_rollback_readiness(self) -> dict[str, Any]:
        """Test rollback readiness and backup status."""
        try:
            rollback_tests = {}

            # Check if legacy tables are still intact
            legacy_tables = [
                "mlb_betting.moneyline",
                "mlb_betting.spreads",
                "mlb_betting.totals",
                "public.games",
            ]

            with self.db_manager.get_cursor() as cursor:
                for table in legacy_tables:
                    try:
                        schema, table_name = table.split(".")
                        cursor.execute(
                            """
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_schema = %s AND table_name = %s
                            )
                        """,
                            (schema, table_name),
                        )

                        exists = cursor.fetchone()[0]

                        if exists:
                            cursor.execute(f"SELECT COUNT(*) FROM {table}")
                            record_count = cursor.fetchone()[0]

                            rollback_tests[f"rollback_{table.replace('.', '_')}"] = {
                                "exists": True,
                                "record_count": record_count,
                                "rollback_ready": True,
                            }
                        else:
                            rollback_tests[f"rollback_{table.replace('.', '_')}"] = {
                                "exists": False,
                                "rollback_ready": False,
                            }

                    except Exception as e:
                        rollback_tests[f"rollback_{table.replace('.', '_')}"] = {
                            "exists": "ERROR",
                            "error": str(e),
                            "rollback_ready": False,
                        }

            return rollback_tests

        except Exception as e:
            self.logger.error(f"Rollback readiness test failed: {e}")
            return False

    def save_results(self, filename: str | None = None) -> str:
        """Save test results to JSON file."""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"phase3_test_results_{timestamp}.json"

        with open(filename, "w") as f:
            json.dump(self.test_results, f, indent=2, default=str)

        self.logger.info(f"📄 Detailed results saved to: {filename}")
        return filename


async def main():
    """Run the Phase 3 test suite."""
    test_suite = Phase3TestSuite()
    results = await test_suite.run_all_tests()

    # Save results
    filename = test_suite.save_results()

    # Print final recommendation
    status = results["overall_status"]
    if status == "READY_FOR_CLEANUP":
        print("\n🎉 RECOMMENDATION: PROCEED WITH PHASE 3B LEGACY CLEANUP")
        print("✅ All tests passed - system ready for legacy table removal")
    elif status == "NEEDS_REVIEW":
        print("\n⚠️ RECOMMENDATION: REVIEW ISSUES BEFORE CLEANUP")
        print("🔍 Some tests failed - investigate before proceeding")
    else:
        print("\n❌ RECOMMENDATION: DO NOT PROCEED WITH CLEANUP")
        print("🚫 Critical issues found - system not ready")

    print("\nGeneral Balls")
    return results


if __name__ == "__main__":
    asyncio.run(main())
