#!/usr/bin/env python3
"""
Master Migration Orchestrator

Orchestrates the complete migration from core_betting to RAW→STAGING→CURATED pipeline.
Provides automated execution with validation, rollback capability, and comprehensive reporting.

Usage:
    python run_migration.py --phase all                    # Full migration
    python run_migration.py --phase 1                      # Phase 1: Analysis only
    python run_migration.py --phase 2                      # Phase 2: RAW zone migration
    python run_migration.py --validate-only                # Validation only
    python run_migration.py --phase 2 --validate           # Run phase with validation
"""

import argparse
import asyncio
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from migration_validator import MigrationValidator

# Import migration modules
from phase1_data_analysis import DataAnalyzer
from phase2_raw_zone_migration import RawZoneMigrator

from src.core.config import get_settings
from src.core.logging import LogComponent, get_logger

logger = get_logger(__name__, LogComponent.CORE)


class MigrationOrchestrator:
    """Orchestrates the complete pipeline migration process."""

    def __init__(self, validate_steps: bool = True, dry_run: bool = False):
        self.settings = get_settings()
        self.validate_steps = validate_steps
        self.dry_run = dry_run
        self.migration_log = {
            "started_at": datetime.now().isoformat(),
            "phases_executed": [],
            "validation_results": {},
            "errors": [],
            "summary": {},
            "completed_at": None,
        }

    async def execute_migration(self, phases: list[str]) -> dict[str, Any]:
        """Execute migration phases with validation and error handling."""
        logger.info(f"Starting migration orchestration for phases: {phases}")

        if self.dry_run:
            logger.info("🔍 DRY RUN MODE - No actual changes will be made")

        try:
            # Pre-migration validation
            if self.validate_steps:
                await self._run_pre_migration_validation()

            # Execute requested phases
            if "all" in phases or "1" in phases or "phase1" in phases:
                await self._execute_phase1()

            if "all" in phases or "2" in phases or "phase2" in phases:
                await self._execute_phase2()

            if "all" in phases or "3" in phases or "phase3" in phases:
                await self._execute_phase3()

            if "all" in phases or "4" in phases or "phase4" in phases:
                await self._execute_phase4()

            # Final validation
            if self.validate_steps:
                await self._run_final_validation()

            # Generate summary
            self.migration_log["completed_at"] = datetime.now().isoformat()
            self.migration_log["summary"] = self._generate_final_summary()

        except Exception as e:
            logger.error(f"Migration orchestration failed: {e}")
            self.migration_log["errors"].append(
                {
                    "timestamp": datetime.now().isoformat(),
                    "error": str(e),
                    "context": "migration_orchestration",
                }
            )
            self.migration_log["status"] = "failed"

        return self.migration_log

    async def _run_pre_migration_validation(self):
        """Run comprehensive pre-migration validation."""
        logger.info("🔍 Running pre-migration validation...")

        validator = MigrationValidator()
        try:
            await validator.initialize()
            results = await validator.run_comprehensive_validation("pre-migration")

            self.migration_log["validation_results"]["pre_migration"] = results

            # Check if validation passed
            if results.get("summary", {}).get("overall_status") != "passed":
                critical_issues = results.get("summary", {}).get("critical_issues", 0)
                if critical_issues > 0:
                    raise Exception(
                        f"Pre-migration validation failed with {critical_issues} critical issues"
                    )

            logger.info("✅ Pre-migration validation passed")

        finally:
            await validator.close()

    async def _execute_phase1(self):
        """Execute Phase 1: Data Analysis and Mapping."""
        logger.info("🔍 Executing Phase 1: Data Analysis and Mapping")

        phase_log = {
            "phase": "Phase 1 - Data Analysis",
            "started_at": datetime.now().isoformat(),
            "status": "in_progress",
        }

        try:
            analyzer = DataAnalyzer()
            await analyzer.initialize()

            if not self.dry_run:
                results = await analyzer.analyze_all_data()
                phase_log["results"] = results
                phase_log["status"] = "completed"

                # Save analysis results
                output_file = Path("utilities/migration/phase1_analysis_results.json")
                output_file.parent.mkdir(parents=True, exist_ok=True)
                with open(output_file, "w") as f:
                    json.dump(results, f, indent=2, default=str)

                logger.info(
                    f"✅ Phase 1 completed: {results.get('summary', {}).get('total_records_to_migrate', 0):,} records analyzed"
                )
            else:
                logger.info("🔍 DRY RUN: Phase 1 analysis skipped")
                phase_log["status"] = "skipped_dry_run"

            await analyzer.close()

        except Exception as e:
            logger.error(f"Phase 1 failed: {e}")
            phase_log["status"] = "failed"
            phase_log["error"] = str(e)
            self.migration_log["errors"].append(
                {
                    "phase": "phase1",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                }
            )

        phase_log["completed_at"] = datetime.now().isoformat()
        self.migration_log["phases_executed"].append(phase_log)

    async def _execute_phase2(self):
        """Execute Phase 2: RAW Zone Migration."""
        logger.info("🚀 Executing Phase 2: RAW Zone Migration")

        phase_log = {
            "phase": "Phase 2 - RAW Zone Migration",
            "started_at": datetime.now().isoformat(),
            "status": "in_progress",
        }

        try:
            migrator = RawZoneMigrator(batch_size=1000)
            await migrator.initialize()

            if not self.dry_run:
                results = await migrator.migrate_all_to_raw_zone()
                phase_log["results"] = results

                if results.get("status") == "completed":
                    phase_log["status"] = "completed"
                    summary = results.get("summary", {})
                    logger.info(
                        f"✅ Phase 2 completed: {summary.get('total_records_successful', 0):,} records migrated"
                    )
                else:
                    phase_log["status"] = "failed"
                    logger.error(
                        f"❌ Phase 2 failed: {results.get('error', 'Unknown error')}"
                    )

                # Save migration results
                output_file = Path("utilities/migration/phase2_migration_results.json")
                with open(output_file, "w") as f:
                    json.dump(results, f, indent=2, default=str)

                # Run post-phase validation if enabled
                if self.validate_steps:
                    await self._validate_phase("raw-zone")

            else:
                logger.info("🔍 DRY RUN: Phase 2 migration skipped")
                phase_log["status"] = "skipped_dry_run"

            await migrator.close()

        except Exception as e:
            logger.error(f"Phase 2 failed: {e}")
            phase_log["status"] = "failed"
            phase_log["error"] = str(e)
            self.migration_log["errors"].append(
                {
                    "phase": "phase2",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                }
            )

        phase_log["completed_at"] = datetime.now().isoformat()
        self.migration_log["phases_executed"].append(phase_log)

    async def _execute_phase3(self):
        """Execute Phase 3: STAGING Zone Migration."""
        logger.info("🔧 Phase 3: STAGING Zone Migration (To be implemented)")

        phase_log = {
            "phase": "Phase 3 - STAGING Zone Migration",
            "started_at": datetime.now().isoformat(),
            "status": "not_implemented",
            "message": "STAGING zone migration will be implemented in next iteration",
        }

        # TODO: Implement staging zone migration
        # This would:
        # 1. Process raw_data tables into staging tables
        # 2. Apply data cleaning and normalization
        # 3. Calculate quality scores
        # 4. Establish referential integrity

        phase_log["completed_at"] = datetime.now().isoformat()
        self.migration_log["phases_executed"].append(phase_log)

    async def _execute_phase4(self):
        """Execute Phase 4: CURATED Zone Migration."""
        logger.info("✨ Phase 4: CURATED Zone Migration (To be implemented)")

        phase_log = {
            "phase": "Phase 4 - CURATED Zone Migration",
            "started_at": datetime.now().isoformat(),
            "status": "not_implemented",
            "message": "CURATED zone migration will be implemented in next iteration",
        }

        # TODO: Implement curated zone migration
        # This would:
        # 1. Process staging tables into curated tables
        # 2. Generate ML feature vectors
        # 3. Calculate sharp action scores
        # 4. Apply market efficiency analysis
        # 5. Generate profitability metrics

        phase_log["completed_at"] = datetime.now().isoformat()
        self.migration_log["phases_executed"].append(phase_log)

    async def _validate_phase(self, phase: str):
        """Validate a specific migration phase."""
        logger.info(f"🔍 Validating {phase} migration...")

        validator = MigrationValidator()
        try:
            await validator.initialize()
            results = await validator.run_comprehensive_validation(phase)

            validation_key = f"{phase}_validation"
            self.migration_log["validation_results"][validation_key] = results

            status = results.get("summary", {}).get("overall_status", "unknown")
            if status == "passed":
                logger.info(f"✅ {phase} validation passed")
            else:
                logger.warning(f"⚠️ {phase} validation had issues")

        except Exception as e:
            logger.error(f"Validation failed for {phase}: {e}")
            self.migration_log["errors"].append(
                {
                    "context": f"{phase}_validation",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat(),
                }
            )
        finally:
            await validator.close()

    async def _run_final_validation(self):
        """Run comprehensive final validation."""
        logger.info("🏁 Running final validation...")

        validator = MigrationValidator()
        try:
            await validator.initialize()
            results = await validator.run_comprehensive_validation("all")

            self.migration_log["validation_results"]["final_validation"] = results

            status = results.get("summary", {}).get("overall_status", "unknown")
            if status == "passed":
                logger.info("🎉 Final validation passed - Migration successful!")
            else:
                logger.warning("⚠️ Final validation found issues - Review required")

        except Exception as e:
            logger.error(f"Final validation failed: {e}")
        finally:
            await validator.close()

    def _generate_final_summary(self) -> dict[str, Any]:
        """Generate final migration summary."""
        completed_phases = [
            p
            for p in self.migration_log["phases_executed"]
            if p.get("status") == "completed"
        ]
        failed_phases = [
            p
            for p in self.migration_log["phases_executed"]
            if p.get("status") == "failed"
        ]

        # Extract key metrics from phase results
        total_records_migrated = 0
        if len(completed_phases) > 0:
            for phase in completed_phases:
                results = phase.get("results", {})
                if "summary" in results:
                    total_records_migrated += results["summary"].get(
                        "total_records_successful", 0
                    )

        return {
            "migration_status": "completed"
            if len(failed_phases) == 0
            else "partial_failure",
            "phases_attempted": len(self.migration_log["phases_executed"]),
            "phases_completed": len(completed_phases),
            "phases_failed": len(failed_phases),
            "total_records_migrated": total_records_migrated,
            "total_errors": len(self.migration_log["errors"]),
            "migration_duration_minutes": self._calculate_duration_minutes(),
            "dry_run_mode": self.dry_run,
        }

    def _calculate_duration_minutes(self) -> float:
        """Calculate total migration duration in minutes."""
        if self.migration_log.get("completed_at") and self.migration_log.get(
            "started_at"
        ):
            start = datetime.fromisoformat(self.migration_log["started_at"])
            end = datetime.fromisoformat(self.migration_log["completed_at"])
            return (end - start).total_seconds() / 60
        return 0.0


async def main():
    """Main execution function with argument parsing."""
    parser = argparse.ArgumentParser(
        description="MLB Betting System Pipeline Migration"
    )
    parser.add_argument(
        "--phase",
        choices=["all", "1", "2", "3", "4", "phase1", "phase2", "phase3", "phase4"],
        default="all",
        help="Migration phase to execute",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        default=True,
        help="Run validation steps (default: True)",
    )
    parser.add_argument(
        "--no-validate", action="store_true", help="Skip validation steps"
    )
    parser.add_argument(
        "--validate-only", action="store_true", help="Run validation only, no migration"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Dry run mode - no actual changes"
    )

    args = parser.parse_args()

    # Handle validation flags
    validate_steps = args.validate and not args.no_validate

    print("🚀 MLB Betting System Pipeline Migration")
    print("=" * 60)
    print(f"Phase: {args.phase}")
    print(f"Validation: {'Enabled' if validate_steps else 'Disabled'}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE MIGRATION'}")
    print()

    # Validation-only mode
    if args.validate_only:
        validator = MigrationValidator()
        try:
            await validator.initialize()
            print("🔍 Running validation-only mode...")

            phase_map = {
                "1": "pre-migration",
                "2": "raw-zone",
                "3": "staging-zone",
                "4": "curated-zone",
            }
            validation_phase = phase_map.get(args.phase, "all")

            results = await validator.run_comprehensive_validation(validation_phase)

            # Display results
            summary = results.get("summary", {})
            status_emoji = "✅" if summary.get("overall_status") == "passed" else "❌"
            print(
                f"\n{status_emoji} Validation Status: {summary.get('overall_status', 'unknown').upper()}"
            )
            print(f"📊 Success Rate: {summary.get('success_rate', 0):.1f}%")

            return 0 if summary.get("overall_status") == "passed" else 1
        finally:
            await validator.close()

    # Full migration mode
    orchestrator = MigrationOrchestrator(
        validate_steps=validate_steps, dry_run=args.dry_run
    )

    try:
        # Determine phases to execute
        if args.phase == "all":
            phases = ["1", "2"]  # Only implement phases 1 and 2 for now
        else:
            phases = [args.phase]

        # Execute migration
        results = await orchestrator.execute_migration(phases)

        # Display results
        print("\n" + "=" * 60)
        print("🏁 MIGRATION RESULTS")
        print("=" * 60)

        summary = results.get("summary", {})
        status_emoji = "✅" if summary.get("migration_status") == "completed" else "⚠️"
        print(
            f"{status_emoji} Migration Status: {summary.get('migration_status', 'unknown').upper()}"
        )
        print(
            f"📈 Phases Completed: {summary.get('phases_completed', 0)}/{summary.get('phases_attempted', 0)}"
        )
        print(f"📊 Records Migrated: {summary.get('total_records_migrated', 0):,}")
        print(f"❌ Total Errors: {summary.get('total_errors', 0)}")
        print(f"⏱️ Duration: {summary.get('migration_duration_minutes', 0):.1f} minutes")

        # Save final results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = Path(f"utilities/migration/migration_results_{timestamp}.json")
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, "w") as f:
            json.dump(results, f, indent=2, default=str)

        print(f"\n💾 Full migration log saved to: {output_file}")

        # Return appropriate exit code
        if summary.get("migration_status") == "completed":
            print("\n🎉 Migration completed successfully!")
            return 0
        else:
            print("\n⚠️ Migration completed with issues - review logs")
            return 1

    except Exception as e:
        logger.error(f"Migration orchestration failed: {e}")
        print(f"\n❌ Migration failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
