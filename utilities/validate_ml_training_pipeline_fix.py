#!/usr/bin/env python3
"""
ML Training Pipeline Validation Script

CRITICAL: Validates the fix for GitHub Issue #67 - ML Training Pipeline Has Zero Real Data

This script verifies that the enhanced_games_outcome_sync_service successfully
resolves the critical ML training pipeline issue by checking:

1. Enhanced games table has games with actual scores
2. ML trainer can successfully load real training data 
3. Minimum data threshold is met for reliable model training
4. Data pipeline integrity is maintained

Run this script after implementing the enhanced_games_outcome_sync_service
to validate that the ML training pipeline issue is fully resolved.

Usage:
    # Run full validation
    PYTHONPATH=/path/to/project uv run python utilities/validate_ml_training_pipeline_fix.py

    # Run specific validation checks
    PYTHONPATH=/path/to/project uv run python utilities/validate_ml_training_pipeline_fix.py --check enhanced-games
    PYTHONPATH=/path/to/project uv run python utilities/validate_ml_training_pipeline_fix.py --check ml-trainer
"""

import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

import click

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import get_settings
from src.core.logging import LogComponent, get_logger
from src.data.database.connection import get_connection, initialize_connections

logger = get_logger(__name__, LogComponent.CORE)


class MLTrainingPipelineValidator:
    """Validates that the ML training pipeline fix resolves the zero data issue."""
    
    def __init__(self):
        self.settings = get_settings()
        self.validation_results = {}
        
        # Initialize database connections for validation
        try:
            initialize_connections(self.settings)
        except Exception as e:
            logger.debug(f"Database connections may already be initialized: {e}")
    
    async def run_full_validation(self) -> Dict[str, Any]:
        """Run complete validation of the ML training pipeline fix."""
        
        print("🚨 CRITICAL: Validating ML Training Pipeline Fix (GitHub Issue #67)")
        print("=" * 70)
        
        validation_results = {
            "timestamp": datetime.now().isoformat(),
            "issue": "GitHub Issue #67 - ML Training Pipeline Has Zero Real Data",
            "checks": {},
            "overall_status": "unknown",
            "critical_issues": [],
            "recommendations": []
        }
        
        try:
            # Check 1: Enhanced games table data validation
            print("\n📊 Check 1: Enhanced Games Table Data Validation")
            enhanced_games_check = await self._validate_enhanced_games_data()
            validation_results["checks"]["enhanced_games"] = enhanced_games_check
            self._print_check_results("Enhanced Games Data", enhanced_games_check)
            
            # Check 2: ML trainer data loading validation  
            print("\n🤖 Check 2: ML Trainer Data Loading Validation")
            ml_trainer_check = await self._validate_ml_trainer_loading()
            validation_results["checks"]["ml_trainer"] = ml_trainer_check
            self._print_check_results("ML Trainer Loading", ml_trainer_check)
            
            # Check 3: Data pipeline integrity validation
            print("\n🔗 Check 3: Data Pipeline Integrity Validation")
            pipeline_check = await self._validate_data_pipeline_integrity()
            validation_results["checks"]["pipeline_integrity"] = pipeline_check
            self._print_check_results("Pipeline Integrity", pipeline_check)
            
            # Check 4: Minimum data threshold validation
            print("\n📈 Check 4: Minimum Data Threshold Validation")
            threshold_check = await self._validate_minimum_data_threshold()
            validation_results["checks"]["data_threshold"] = threshold_check
            self._print_check_results("Data Threshold", threshold_check)
            
            # Check 5: Sync service health validation
            print("\n🔄 Check 5: Sync Service Health Validation")
            sync_service_check = await self._validate_sync_service_health()
            validation_results["checks"]["sync_service"] = sync_service_check
            self._print_check_results("Sync Service Health", sync_service_check)
            
            # Determine overall status
            all_checks = [enhanced_games_check, ml_trainer_check, pipeline_check, threshold_check, sync_service_check]
            all_passed = all(check["status"] == "pass" for check in all_checks)
            any_critical = any(check.get("critical", False) and check["status"] == "fail" for check in all_checks)
            
            if all_passed:
                validation_results["overall_status"] = "pass"
            elif any_critical:
                validation_results["overall_status"] = "critical_failure"
            else:
                validation_results["overall_status"] = "partial_failure"
            
            # Collect critical issues and recommendations
            for check_name, check_result in validation_results["checks"].items():
                if check_result["status"] == "fail" and check_result.get("critical", False):
                    validation_results["critical_issues"].append(f"{check_name}: {check_result['message']}")
                
                if check_result.get("recommendations"):
                    validation_results["recommendations"].extend(check_result["recommendations"])
            
            # Print final summary
            self._print_validation_summary(validation_results)
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Validation failed with error: {e}")
            validation_results["overall_status"] = "error"
            validation_results["error"] = str(e)
            print(f"\n❌ Validation failed with error: {e}")
            return validation_results
    
    async def _validate_enhanced_games_data(self) -> Dict[str, Any]:
        """Validate that enhanced_games table has real game data with scores."""
        
        try:
            async with get_connection() as conn:
                # Count total enhanced games
                total_enhanced = await conn.fetchval("SELECT COUNT(*) FROM curated.enhanced_games")
                
                # Count enhanced games with scores
                enhanced_with_scores = await conn.fetchval("""
                    SELECT COUNT(*) FROM curated.enhanced_games 
                    WHERE home_score IS NOT NULL AND away_score IS NOT NULL
                """)
                
                # Get sample of enhanced games with scores
                sample_games = await conn.fetch("""
                    SELECT id, home_team, away_team, home_score, away_score, game_datetime
                    FROM curated.enhanced_games 
                    WHERE home_score IS NOT NULL AND away_score IS NOT NULL
                    ORDER BY game_datetime DESC 
                    LIMIT 5
                """)
                
                # Validation logic
                is_critical = enhanced_with_scores == 0
                status = "fail" if enhanced_with_scores == 0 else "pass"
                
                message = f"Enhanced games with scores: {enhanced_with_scores}/{total_enhanced}"
                if enhanced_with_scores == 0:
                    message += " - CRITICAL: No games with scores found!"
                elif enhanced_with_scores < 50:
                    message += " - WARNING: Insufficient data for reliable training"
                else:
                    message += " - GOOD: Sufficient data for training"
                
                recommendations = []
                if enhanced_with_scores == 0:
                    recommendations.append("Run: uv run -m src.interfaces.cli curated sync-outcomes --sync-type all")
                elif enhanced_with_scores < 50:
                    recommendations.append("Consider running outcome sync to get more historical data")
                
                return {
                    "status": status,
                    "critical": is_critical,
                    "message": message,
                    "details": {
                        "total_enhanced_games": total_enhanced,
                        "enhanced_games_with_scores": enhanced_with_scores,
                        "coverage_percentage": (enhanced_with_scores / total_enhanced * 100) if total_enhanced > 0 else 0,
                        "sample_games": [dict(game) for game in sample_games]
                    },
                    "recommendations": recommendations
                }
                
        except Exception as e:
            return {
                "status": "error",
                "critical": True,
                "message": f"Database validation failed: {e}",
                "error": str(e),
                "recommendations": ["Check database connectivity and table schema"]
            }
    
    async def _validate_ml_trainer_loading(self) -> Dict[str, Any]:
        """Validate that ML trainer can successfully load training data."""
        
        try:
            # Import ML trainer - this tests if the import works
            from src.ml.training.lightgbm_trainer import LightGBMTrainer
            
            # Create trainer instance
            trainer = LightGBMTrainer()
            
            # Test data loading with a small date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            # Attempt to load training data
            try:
                training_data = await trainer._load_training_data(start_date, end_date, use_cached_features=False)
                
                data_count = len(training_data)
                has_real_data = data_count > 0
                
                if has_real_data:
                    # Validate that the data has actual scores
                    sample_game = training_data[0] if training_data else {}
                    has_scores = sample_game.get("home_score") is not None and sample_game.get("away_score") is not None
                    
                    status = "pass" if has_scores else "fail"
                    message = f"ML trainer loaded {data_count} games with {'real' if has_scores else 'missing'} scores"
                    
                    return {
                        "status": status,
                        "critical": not has_scores,
                        "message": message,
                        "details": {
                            "training_games_loaded": data_count,
                            "has_real_scores": has_scores,
                            "sample_game": sample_game,
                            "date_range": f"{start_date.date()} to {end_date.date()}"
                        },
                        "recommendations": [] if has_scores else ["Enhanced games need to be populated with real score data"]
                    }
                else:
                    return {
                        "status": "fail", 
                        "critical": True,
                        "message": "ML trainer found no training data in date range",
                        "details": {
                            "training_games_loaded": 0,
                            "date_range": f"{start_date.date()} to {end_date.date()}"
                        },
                        "recommendations": ["Ensure enhanced_games table has recent data with scores"]
                    }
                    
            except Exception as loading_error:
                return {
                    "status": "fail",
                    "critical": True,
                    "message": f"ML trainer data loading failed: {loading_error}",
                    "error": str(loading_error),
                    "recommendations": ["Check enhanced_games table schema and data integrity"]
                }
                
        except ImportError as e:
            return {
                "status": "error",
                "critical": False,
                "message": f"ML trainer import failed: {e}",
                "error": str(e),
                "recommendations": ["ML trainer module may not be implemented yet"]
            }
        except Exception as e:
            return {
                "status": "error",
                "critical": True,
                "message": f"ML trainer validation failed: {e}",
                "error": str(e),
                "recommendations": ["Check ML trainer implementation and dependencies"]
            }
    
    async def _validate_data_pipeline_integrity(self) -> Dict[str, Any]:
        """Validate that the data pipeline maintains integrity between game_outcomes and enhanced_games."""
        
        try:
            async with get_connection() as conn:
                # Check for data consistency between game_outcomes and enhanced_games
                consistency_check = await conn.fetchrow("""
                    WITH outcome_games AS (
                        SELECT 
                            go.game_id,
                            go.home_team,
                            go.away_team,
                            go.home_score,
                            go.away_score,
                            gc.mlb_stats_api_game_id,
                            gc.action_network_game_id
                        FROM curated.game_outcomes go
                        INNER JOIN curated.games_complete gc ON go.game_id = gc.id
                        WHERE go.home_score IS NOT NULL AND go.away_score IS NOT NULL
                    ),
                    enhanced_games AS (
                        SELECT 
                            eg.mlb_stats_api_game_id,
                            eg.action_network_game_id,
                            eg.home_team,
                            eg.away_team,
                            eg.home_score,
                            eg.away_score
                        FROM curated.enhanced_games eg
                        WHERE eg.home_score IS NOT NULL AND eg.away_score IS NOT NULL
                    )
                    SELECT 
                        COUNT(og.*) as total_outcomes,
                        COUNT(eg.*) as total_enhanced,
                        COUNT(CASE 
                            WHEN eg.mlb_stats_api_game_id IS NOT NULL 
                            AND og.home_score = eg.home_score 
                            AND og.away_score = eg.away_score 
                            THEN 1 
                        END) as matching_scores
                    FROM outcome_games og
                    LEFT JOIN enhanced_games eg 
                        ON (og.mlb_stats_api_game_id = eg.mlb_stats_api_game_id 
                            OR og.action_network_game_id = eg.action_network_game_id)
                """)
                
                total_outcomes = consistency_check["total_outcomes"]
                total_enhanced = consistency_check["total_enhanced"] 
                matching_scores = consistency_check["matching_scores"]
                
                # Calculate integrity metrics
                sync_percentage = (matching_scores / total_outcomes * 100) if total_outcomes > 0 else 0
                is_good_integrity = sync_percentage >= 90
                
                status = "pass" if is_good_integrity else "fail"
                message = f"Data integrity: {matching_scores}/{total_outcomes} games synced ({sync_percentage:.1f}%)"
                
                recommendations = []
                if sync_percentage < 90:
                    recommendations.append("Run outcome sync to improve data pipeline integrity")
                if total_enhanced < total_outcomes:
                    recommendations.append("Enhanced games table missing some outcomes data")
                
                return {
                    "status": status,
                    "critical": sync_percentage < 50,
                    "message": message,
                    "details": {
                        "total_game_outcomes": total_outcomes,
                        "total_enhanced_games": total_enhanced,
                        "matching_scores": matching_scores,
                        "sync_percentage": sync_percentage,
                        "integrity_threshold": 90
                    },
                    "recommendations": recommendations
                }
                
        except Exception as e:
            return {
                "status": "error",
                "critical": True,
                "message": f"Pipeline integrity check failed: {e}",
                "error": str(e),
                "recommendations": ["Check database schema and table relationships"]
            }
    
    async def _validate_minimum_data_threshold(self) -> Dict[str, Any]:
        """Validate that sufficient data exists for reliable ML model training."""
        
        try:
            async with get_connection() as conn:
                # Get data counts and date ranges
                data_metrics = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as total_games,
                        COUNT(CASE WHEN home_score IS NOT NULL AND away_score IS NOT NULL THEN 1 END) as games_with_scores,
                        MIN(game_datetime) as earliest_game,
                        MAX(game_datetime) as latest_game,
                        COUNT(DISTINCT home_team) + COUNT(DISTINCT away_team) as unique_teams
                    FROM curated.enhanced_games
                """)
                
                total_games = data_metrics["total_games"]
                games_with_scores = data_metrics["games_with_scores"]
                unique_teams = data_metrics["unique_teams"]
                
                # ML training requirements
                min_games_for_training = 50
                min_teams_for_diversity = 20
                
                # Validation checks
                has_sufficient_games = games_with_scores >= min_games_for_training
                has_team_diversity = unique_teams >= min_teams_for_diversity
                
                overall_ready = has_sufficient_games and has_team_diversity
                status = "pass" if overall_ready else "fail"
                
                # Build message
                message_parts = []
                if has_sufficient_games:
                    message_parts.append(f"✅ Sufficient games ({games_with_scores} ≥ {min_games_for_training})")
                else:
                    message_parts.append(f"❌ Insufficient games ({games_with_scores} < {min_games_for_training})")
                
                if has_team_diversity:
                    message_parts.append(f"✅ Good team diversity ({unique_teams} teams)")
                else:
                    message_parts.append(f"❌ Low team diversity ({unique_teams} < {min_teams_for_diversity})")
                
                message = " | ".join(message_parts)
                
                recommendations = []
                if not has_sufficient_games:
                    recommendations.append("Collect more historical game data to reach minimum training threshold")
                if not has_team_diversity:
                    recommendations.append("Ensure data covers games from diverse teams for better model generalization")
                
                return {
                    "status": status,
                    "critical": not has_sufficient_games,
                    "message": message,
                    "details": {
                        "total_games": total_games,
                        "games_with_scores": games_with_scores,
                        "unique_teams": unique_teams,
                        "min_games_required": min_games_for_training,
                        "min_teams_required": min_teams_for_diversity,
                        "ml_training_ready": overall_ready,
                        "earliest_game": data_metrics["earliest_game"],
                        "latest_game": data_metrics["latest_game"]
                    },
                    "recommendations": recommendations
                }
                
        except Exception as e:
            return {
                "status": "error",
                "critical": True,
                "message": f"Data threshold validation failed: {e}",
                "error": str(e),
                "recommendations": ["Check enhanced_games table schema and data availability"]
            }
    
    async def _validate_sync_service_health(self) -> Dict[str, Any]:
        """Validate that the outcome sync service is working properly."""
        
        try:
            # Import and test sync service
            from src.services.curated_zone.enhanced_games_outcome_sync_service import EnhancedGamesOutcomeSyncService
            
            service = EnhancedGamesOutcomeSyncService()
            
            # Get service health
            health_check = await service.health_check()
            service_status = health_check.get("status", "unknown")
            
            # Get sync stats
            sync_stats = await service.get_sync_stats()
            
            is_healthy = service_status == "healthy"
            missing_games = sync_stats.get("missing_enhanced_games", 0)
            
            status = "pass" if is_healthy and missing_games == 0 else "fail"
            
            if is_healthy and missing_games == 0:
                message = "✅ Sync service healthy, no missing games"
            elif is_healthy and missing_games > 0:
                message = f"⚠️ Sync service healthy, but {missing_games} games still missing"
            else:
                message = f"❌ Sync service unhealthy: {service_status}"
            
            recommendations = []
            if missing_games > 0:
                recommendations.append("Run outcome sync to resolve missing games")
            if not is_healthy:
                recommendations.append("Check sync service configuration and database connectivity")
            
            return {
                "status": status,
                "critical": not is_healthy,
                "message": message,
                "details": {
                    "service_status": service_status,
                    "missing_enhanced_games": missing_games,
                    "sync_stats": sync_stats,
                    "health_check": health_check
                },
                "recommendations": recommendations
            }
            
        except ImportError as e:
            return {
                "status": "error",
                "critical": False,
                "message": f"Sync service import failed: {e}",
                "error": str(e),
                "recommendations": ["Sync service may not be implemented yet"]
            }
        except Exception as e:
            return {
                "status": "error",
                "critical": True,
                "message": f"Sync service validation failed: {e}",
                "error": str(e),
                "recommendations": ["Check sync service implementation and dependencies"]
            }
    
    def _print_check_results(self, check_name: str, check_result: Dict[str, Any]):
        """Print formatted check results."""
        
        status = check_result["status"]
        message = check_result["message"]
        is_critical = check_result.get("critical", False)
        
        # Status icon
        if status == "pass":
            icon = "✅"
        elif status == "fail" and is_critical:
            icon = "🚨"
        elif status == "fail":
            icon = "⚠️"
        else:
            icon = "❓"
        
        print(f"   {icon} {check_name}: {message}")
        
        # Print recommendations if any
        recommendations = check_result.get("recommendations", [])
        if recommendations:
            print(f"      💡 Recommendations:")
            for rec in recommendations:
                print(f"         - {rec}")
    
    def _print_validation_summary(self, results: Dict[str, Any]):
        """Print final validation summary."""
        
        print("\n" + "=" * 70)
        print("🏁 VALIDATION SUMMARY")
        print("=" * 70)
        
        overall_status = results["overall_status"]
        
        if overall_status == "pass":
            print("🎉 SUCCESS: ML Training Pipeline Issue Fully Resolved!")
            print("   ✅ All validation checks passed")
            print("   ✅ Enhanced games table has sufficient real data")
            print("   ✅ ML trainer can load historical data")
            print("   ✅ Data pipeline integrity is maintained")
            print("   ✅ Ready for reliable model training and predictions")
        elif overall_status == "critical_failure":
            print("🚨 CRITICAL: ML Training Pipeline Still Has Issues!")
            print("   ❌ Critical validation checks failed")
            print("   ❌ ML training pipeline cannot function properly")
            print("   ❌ Immediate action required")
        elif overall_status == "partial_failure":
            print("⚠️  PARTIAL: ML Training Pipeline Partially Fixed")
            print("   ⚠️  Some validation checks failed")
            print("   ⚠️  ML training may work but not optimally")
            print("   ⚠️  Additional improvements recommended")
        else:
            print("❓ ERROR: Validation could not be completed")
            print("   ❓ Unknown status - check error details")
        
        # Print critical issues
        critical_issues = results.get("critical_issues", [])
        if critical_issues:
            print(f"\n🚨 Critical Issues ({len(critical_issues)}):")
            for issue in critical_issues:
                print(f"   - {issue}")
        
        # Print recommendations
        recommendations = results.get("recommendations", [])
        if recommendations:
            print(f"\n💡 Recommendations ({len(recommendations)}):")
            for rec in recommendations:
                print(f"   - {rec}")
        
        print(f"\nValidation completed at: {results['timestamp']}")
        print(f"Issue: {results['issue']}")


@click.command()
@click.option(
    "--check",
    type=click.Choice(["enhanced-games", "ml-trainer", "pipeline-integrity", "data-threshold", "sync-service"]),
    help="Run specific validation check only"
)
@click.option("--output", type=click.Path(), help="Save results to JSON file")
def main(check: str, output: str):
    """
    Validate ML Training Pipeline Fix (GitHub Issue #67).
    
    Verifies that the enhanced_games_outcome_sync_service successfully
    resolves the critical ML training pipeline zero data issue.
    """
    async def run_validation():
        validator = MLTrainingPipelineValidator()
        
        if check:
            print(f"Running specific validation check: {check}")
            # Run specific check (would need individual check methods)
            results = await validator.run_full_validation()
            specific_result = results["checks"].get(check.replace("-", "_"))
            if specific_result:
                validator._print_check_results(check, specific_result)
            else:
                print(f"Check '{check}' not found")
        else:
            # Run full validation
            results = await validator.run_full_validation()
        
        # Save results if requested
        if output:
            import json
            with open(output, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            print(f"\nResults saved to: {output}")
    
    asyncio.run(run_validation())


if __name__ == "__main__":
    main()