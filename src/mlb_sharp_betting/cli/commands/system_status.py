#!/usr/bin/env python3
"""System status command for monitoring the sports betting system."""

import asyncio
from datetime import datetime
from typing import Any

import click

from ...core.logging import get_logger
from ...db.connection import get_db_manager

# 🔄 UPDATED: Use new BacktestingEngine instead of deprecated EnhancedBacktestingService
from ...services.backtesting_engine import get_backtesting_engine
from ...services.scheduler_engine import get_scheduler_engine

logger = get_logger(__name__)


async def check_data_freshness_simple(db_manager) -> dict[str, Any]:
    """Simple data freshness check using direct database queries."""
    try:
        with db_manager.get_cursor() as cursor:
            # Get latest splits timestamp
            cursor.execute("""
                SELECT 
                    MAX(last_updated) as latest_update,
                    COUNT(*) as total_splits,
                    COUNT(DISTINCT CONCAT(home_team, '|', away_team)) as unique_games
                FROM splits.raw_mlb_betting_splits
            """)
            splits_info = cursor.fetchone()

            # Get latest outcomes timestamp
            cursor.execute("""
                SELECT MAX(created_at) as latest_outcome, COUNT(*) as total_outcomes
                FROM public.game_outcomes
            """)
            outcomes_info = (
                cursor.fetchone()
                if cursor.rowcount > 0
                else {"latest_outcome": None, "total_outcomes": 0}
            )

            # Calculate data age
            latest_update = splits_info["latest_update"] if splits_info else None
            data_age_hours = 0
            if latest_update:
                data_age_hours = (datetime.now() - latest_update).total_seconds() / 3600

            # Data is considered fresh if less than 6 hours old
            max_age_hours = 6
            is_fresh = data_age_hours < max_age_hours

            return {
                "is_fresh": is_fresh,
                "data_age_hours": data_age_hours,
                "max_age_hours": max_age_hours,
                "latest_splits_update": latest_update,
                "latest_outcomes_update": outcomes_info.get("latest_outcome"),
                "total_splits": splits_info.get("total_splits", 0)
                if splits_info
                else 0,
                "unique_games": splits_info.get("unique_games", 0)
                if splits_info
                else 0,
                "total_outcomes": outcomes_info.get("total_outcomes", 0),
                "needs_collection": not is_fresh,
            }
    except Exception as e:
        logger.error("Data freshness check failed", error=str(e))
        return {
            "is_fresh": False,
            "error": str(e),
            "data_age_hours": 999,  # Very old
            "needs_collection": True,
        }


async def validate_pipeline_requirements_simple() -> dict[str, bool]:
    """Simple pipeline requirements validation."""
    try:
        db_manager = get_db_manager()
        validations = {}

        # Check database connection
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("SELECT 1")
                validations["database_connection"] = True
        except Exception:
            validations["database_connection"] = False

        # Check required tables exist
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) as table_count 
                    FROM information_schema.tables 
                    WHERE table_schema = 'splits' AND table_name = 'raw_mlb_betting_splits'
                """)
                table_exists = cursor.fetchone()["table_count"] > 0
                validations["required_tables"] = table_exists
        except Exception:
            validations["required_tables"] = False

        # Check for recent data
        try:
            freshness = await check_data_freshness_simple(db_manager)
            validations["data_availability"] = freshness["total_splits"] > 0
            validations["data_freshness"] = freshness["is_fresh"]
        except Exception:
            validations["data_availability"] = False
            validations["data_freshness"] = False

        return validations
    except Exception as e:
        logger.error("Pipeline validation failed", error=str(e))
        return {"error": True, "message": str(e)}


async def verify_data_integrity_simple(db_manager) -> dict[str, Any]:
    """Simple data integrity verification."""
    try:
        with db_manager.get_cursor() as cursor:
            results = {
                "overall_health": "unknown",
                "checks_passed": 0,
                "checks_failed": 0,
                "warnings": [],
                "errors": [],
            }

            # Check 1: Data exists
            cursor.execute(
                "SELECT COUNT(*) as count FROM splits.raw_mlb_betting_splits"
            )
            splits_count = cursor.fetchone()["count"]

            if splits_count > 0:
                results["checks_passed"] += 1
            else:
                results["checks_failed"] += 1
                results["errors"].append("No betting splits data found")

            # Check 2: Recent data exists
            cursor.execute("""
                SELECT COUNT(*) as recent_count
                FROM splits.raw_mlb_betting_splits
                WHERE last_updated >= NOW() - INTERVAL '24 hours'
            """)
            recent_count = cursor.fetchone()["recent_count"]

            if recent_count > 0:
                results["checks_passed"] += 1
            else:
                results["checks_failed"] += 1
                results["warnings"].append("No recent data (last 24 hours)")

            # Check 3: Data completeness
            cursor.execute("""
                SELECT COUNT(*) as incomplete_count
                FROM splits.raw_mlb_betting_splits
                WHERE home_team IS NULL OR away_team IS NULL
            """)
            incomplete_count = cursor.fetchone()["incomplete_count"]

            if incomplete_count == 0:
                results["checks_passed"] += 1
            else:
                results["checks_failed"] += 1
                results["warnings"].append(
                    f"{incomplete_count} records with missing team data"
                )

            # Determine overall health
            if results["checks_failed"] == 0:
                results["overall_health"] = "good"
            elif results["checks_passed"] > results["checks_failed"]:
                results["overall_health"] = "fair"
            else:
                results["overall_health"] = "poor"

            return results
    except Exception as e:
        logger.error("Data integrity check failed", error=str(e))
        return {
            "overall_health": "error",
            "checks_passed": 0,
            "checks_failed": 1,
            "errors": [str(e)],
        }


@click.group()
def status_group():
    """System status and health monitoring commands (Phase 3/4 engines)."""
    pass


@status_group.command("overview")
def system_status():
    """📊 Show overall system status"""

    async def show_system_status():
        click.echo("📊 SYSTEM STATUS OVERVIEW (Phase 3/4 Engines)")
        click.echo("=" * 60)

        try:
            db_manager = get_db_manager()

            # Basic system information
            click.echo("🖥️  SYSTEM INFORMATION:")
            click.echo(
                f"   📅 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            click.echo("   📁 Database: PostgreSQL (mlb_betting)")

            # Check database connection
            try:
                with db_manager.get_cursor() as cursor:
                    cursor.execute("SELECT version()")
                    version = cursor.fetchone()[0]
                    click.echo("   ✅ Database Connection: OK")
                    if "PostgreSQL" in version:
                        pg_version = version.split()[1]
                        click.echo(f"   📋 PostgreSQL Version: {pg_version}")
            except Exception as e:
                click.echo(f"   ❌ Database Connection: FAILED ({e})")

            # Get detailed data freshness
            click.echo("\n📊 DETAILED DATA METRICS:")
            freshness_check = await check_data_freshness_simple(db_manager)

            click.echo(
                f"   📈 Total Splits: {freshness_check.get('total_splits', 0):,}"
            )
            click.echo(f"   🎮 Unique Games: {freshness_check.get('unique_games', 0)}")
            click.echo(
                f"   🏆 Game Outcomes: {freshness_check.get('total_outcomes', 0)}"
            )

            # Data freshness status
            data_age = freshness_check.get("data_age_hours", 999)
            if freshness_check.get("is_fresh", False):
                click.echo(f"   ✅ Data Status: FRESH ({data_age:.1f} hours old)")
            else:
                click.echo(f"   ⚠️  Data Status: STALE ({data_age:.1f} hours old)")
                click.echo("   💡 Consider running data collection")

            # Engine status
            click.echo("\n🔧 CONSOLIDATED ENGINES:")

            # BacktestingEngine status
            try:
                backtesting_engine = get_backtesting_engine()
                status = backtesting_engine.get_comprehensive_status()
                click.echo("   ✅ BacktestingEngine: Available")
                click.echo(f"      📊 Status: {status}")
            except Exception as e:
                click.echo(f"   ❌ BacktestingEngine: Error ({e})")

            # SchedulerEngine status
            try:
                scheduler_engine = get_scheduler_engine()
                status = scheduler_engine.get_status()
                click.echo("   ✅ SchedulerEngine: Available")
                click.echo(f"      📊 Status: {status}")
            except Exception as e:
                click.echo(f"   ❌ SchedulerEngine: Error ({e})")

            # Basic recommendations
            click.echo("\n💡 RECOMMENDATIONS:")
            if not freshness_check.get("is_fresh", False):
                click.echo("   🔄 Run data collection to refresh stale data")
            if freshness_check.get("total_splits", 0) == 0:
                click.echo("   📥 Initial data collection needed")
            if freshness_check.get("total_outcomes", 0) == 0:
                click.echo("   🏆 Game outcome data collection recommended")

            if (
                freshness_check.get("is_fresh", False)
                and freshness_check.get("total_splits", 0) > 0
            ):
                click.echo("   ✨ System is ready for analysis")

        except Exception as e:
            click.echo(f"❌ System status check failed: {e}")
        finally:
            try:
                if "db_manager" in locals():
                    db_manager.close()
            except Exception:
                pass

    try:
        asyncio.run(show_system_status())
    except Exception:
        click.echo("❌ System status check failed")
        raise


@status_group.command("health")
@click.option("--detailed", is_flag=True, help="Show detailed health information")
def health_check(detailed: bool):
    """🏥 Run comprehensive system health check"""

    async def run_health_check():
        click.echo("🏥 COMPREHENSIVE HEALTH CHECK (Phase 3/4 Engines)")
        click.echo("=" * 60)

        health_results = {
            "database": False,
            "schema": False,
            "data_pipeline": False,
            "backtesting": False,
            "data_freshness": False,
            "data_quality": False,
        }

        issues = []
        warnings = []

        try:
            # Test 1: Database Connection
            click.echo("🔍 Testing database connection...")
            try:
                db_manager = get_db_manager()
                with db_manager.get_cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.execute("SELECT version()")
                    version = cursor.fetchone()[0]

                health_results["database"] = True
                click.echo("   ✅ Database connection successful")
                if detailed:
                    click.echo(f"      📋 Version: {version}")

            except Exception as e:
                click.echo(f"   ❌ Database connection failed: {e}")
                issues.append(f"Database connection: {e}")

            # Test 2: Schema Validation
            click.echo("🔍 Validating database schema...")
            try:
                with db_manager.get_cursor() as cursor:
                    # Check required schemas
                    cursor.execute("""
                        SELECT schema_name 
                        FROM information_schema.schemata 
                        WHERE schema_name IN ('splits', 'public')
                    """)
                    schemas = [row["schema_name"] for row in cursor.fetchall()]

                    required_schemas = ["splits", "public"]
                    missing_schemas = [s for s in required_schemas if s not in schemas]

                    if not missing_schemas:
                        health_results["schema"] = True
                        click.echo("   ✅ Database schema validation passed")
                        if detailed:
                            click.echo(f"      📋 Found schemas: {', '.join(schemas)}")
                    else:
                        click.echo(
                            f"   ⚠️  Missing schemas: {', '.join(missing_schemas)}"
                        )
                        warnings.append(
                            f"Missing schemas: {', '.join(missing_schemas)}"
                        )

            except Exception as e:
                click.echo(f"   ❌ Schema validation failed: {e}")
                issues.append(f"Schema validation: {e}")

            # Test 3: Data Pipeline
            click.echo("🔍 Testing data pipeline...")
            try:
                from ...entrypoint import DataPipeline

                # Test pipeline initialization with dry run
                test_pipeline = DataPipeline(
                    sport="mlb", sportsbook="circa", dry_run=True
                )

                health_results["data_pipeline"] = True
                click.echo("   ✅ Data pipeline initialization successful")

            except Exception as e:
                click.echo(f"   ❌ Data pipeline test failed: {e}")
                issues.append(f"Data pipeline: {e}")

            # Test 4: Backtesting Service
            click.echo("🔍 Testing backtesting engine...")
            try:
                backtesting_engine = get_backtesting_engine()
                validations = await validate_pipeline_requirements_simple()

                if all(validations.values()) and "error" not in validations:
                    health_results["backtesting"] = True
                    click.echo("   ✅ Backtesting engine validation passed")
                else:
                    failed_reqs = [
                        k for k, v in validations.items() if not v and k != "error"
                    ]
                    click.echo(
                        f"   ⚠️  Backtesting validation issues: {', '.join(failed_reqs)}"
                    )
                    warnings.append(f"Backtesting issues: {', '.join(failed_reqs)}")

            except Exception as e:
                click.echo(f"   ❌ Backtesting engine test failed: {e}")
                issues.append(f"Backtesting engine: {e}")

            # Test 5: Data Freshness
            click.echo("🔍 Checking data freshness...")
            try:
                freshness_check = await check_data_freshness_simple(db_manager)

                if freshness_check["is_fresh"]:
                    health_results["data_freshness"] = True
                    click.echo(
                        f"   ✅ Data is fresh ({freshness_check['data_age_hours']:.1f} hours old)"
                    )
                else:
                    click.echo(
                        f"   ⚠️  Data is stale ({freshness_check.get('data_age_hours', 'unknown')} hours old)"
                    )
                    warnings.append("Data is stale - collection recommended")

                if detailed:
                    click.echo(
                        f"      📊 Total splits: {freshness_check.get('total_splits', 0):,}"
                    )
                    click.echo(
                        f"      🎮 Unique games: {freshness_check.get('unique_games', 0)}"
                    )

            except Exception as e:
                click.echo(f"   ❌ Data freshness check failed: {e}")
                issues.append(f"Data freshness: {e}")

            # Test 6: Data Quality
            click.echo("🔍 Analyzing data quality...")
            try:
                integrity_results = await verify_data_integrity_simple(db_manager)

                if integrity_results["overall_health"] == "good":
                    health_results["data_quality"] = True
                    click.echo("   ✅ Data quality check passed")
                else:
                    click.echo("   ⚠️  Data quality issues detected")
                    warnings.extend(integrity_results.get("warnings", []))
                    issues.extend(integrity_results.get("errors", []))

                if detailed:
                    click.echo(
                        f"      ✅ Checks passed: {integrity_results['checks_passed']}"
                    )
                    click.echo(
                        f"      ❌ Checks failed: {integrity_results['checks_failed']}"
                    )

            except Exception as e:
                click.echo(f"   ❌ Data quality check failed: {e}")
                issues.append(f"Data quality: {e}")

            # Summary
            passed_checks = sum(health_results.values())
            total_checks = len(health_results)

            click.echo("\n📊 HEALTH CHECK SUMMARY:")
            click.echo(f"   ✅ Passed: {passed_checks}/{total_checks} checks")
            click.echo(
                f"   ❌ Failed: {total_checks - passed_checks}/{total_checks} checks"
            )
            click.echo(f"   ⚠️  Warnings: {len(warnings)}")

            # Overall health rating
            health_percentage = (passed_checks / total_checks) * 100

            if health_percentage >= 90:
                overall_health = "🟢 EXCELLENT"
            elif health_percentage >= 75:
                overall_health = "🟡 GOOD"
            elif health_percentage >= 50:
                overall_health = "🟠 FAIR"
            else:
                overall_health = "🔴 POOR"

            click.echo(f"\n{overall_health} OVERALL HEALTH: {health_percentage:.0f}%")

            # Show issues and warnings
            if issues:
                click.echo("\n❌ CRITICAL ISSUES:")
                for issue in issues:
                    click.echo(f"   • {issue}")

                click.echo(
                    "\n💡 Run 'uv run -m mlb_sharp_betting.cli status fix' to attempt automatic fixes"
                )

            if warnings:
                click.echo("\n⚠️  WARNINGS:")
                for warning in warnings:
                    click.echo(f"   • {warning}")

            if not issues and not warnings:
                click.echo("\n🎉 System is healthy and ready for operation!")

        except Exception as e:
            click.echo(f"❌ Health check failed: {e}")
        finally:
            try:
                if "db_manager" in locals():
                    db_manager.close()
            except Exception:
                pass

    try:
        asyncio.run(run_health_check())
    except Exception:
        click.echo("❌ Health check failed")
        raise


@status_group.command("quick")
def quick_status():
    """⚡ Quick system status check"""

    async def run_quick_check():
        click.echo("⚡ QUICK STATUS CHECK")
        click.echo("=" * 30)

        try:
            db_manager = get_db_manager()

            # Database connection
            try:
                with db_manager.get_cursor() as cursor:
                    cursor.execute("SELECT 1")
                click.echo("✅ Database: Connected")
            except Exception:
                click.echo("❌ Database: Failed")
                return

            # Data freshness
            freshness = await check_data_freshness_simple(db_manager)
            if freshness["is_fresh"]:
                click.echo(f"✅ Data: Fresh ({freshness['data_age_hours']:.1f}h old)")
            else:
                click.echo(f"⚠️  Data: Stale ({freshness['data_age_hours']:.1f}h old)")

            # Data counts
            click.echo(f"📊 Splits: {freshness['total_splits']:,}")
            click.echo(f"🎮 Games: {freshness['unique_games']:,}")

            # Engine availability
            try:
                get_backtesting_engine()
                click.echo("✅ BacktestingEngine: Available")
            except Exception:
                click.echo("❌ BacktestingEngine: Failed")

            try:
                get_scheduler_engine()
                click.echo("✅ SchedulerEngine: Available")
            except Exception:
                click.echo("❌ SchedulerEngine: Failed")

        except Exception as e:
            click.echo(f"❌ Quick check failed: {e}")

    asyncio.run(run_quick_check())


if __name__ == "__main__":
    status_group()
