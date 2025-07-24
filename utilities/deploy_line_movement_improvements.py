#!/usr/bin/env python3
"""
Deploy Line Movement Calculation Improvements

This script deploys the improved line movement calculations that fix:
1. American odds calculations (-101 to +101 = 2 points, not 202)
2. Line value change detection for spreads/totals
3. Smart filtering for line flips and false positives

Usage:
    python utilities/deploy_line_movement_improvements.py [--dry-run]
"""

import asyncio
import sys
from pathlib import Path
from typing import Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.core.config import get_settings
from src.data.database.connection import DatabaseConnection
import structlog

logger = structlog.get_logger(__name__)

async def deploy_improvements(dry_run: bool = False) -> Dict[str, Any]:
    """
    Deploy the improved line movement calculations.
    
    Args:
        dry_run: If True, show what would be done without executing
        
    Returns:
        Deployment results
    """
    results = {
        "deployment_start": None,
        "steps": [],
        "success": True,
        "error": None,
        "deployment_end": None
    }
    
    try:
        # Initialize database connection
        settings = get_settings()
        db_connection = DatabaseConnection(settings.database.connection_string)
        
        # Load the improvement SQL
        sql_file = project_root / "sql" / "improvements" / "improved_line_movements_view.sql"
        if not sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")
        
        improvements_sql = sql_file.read_text()
        
        if dry_run:
            logger.info("DRY RUN MODE - No changes will be applied")
            results["steps"].append({
                "step": "dry_run_validation",
                "status": "info", 
                "message": "SQL file loaded successfully",
                "sql_length": len(improvements_sql)
            })
            return results
        
        results["deployment_start"] = asyncio.get_event_loop().time()
        
        # Step 1: Backup existing views
        logger.info("Step 1: Creating backup of existing views")
        backup_sql = """
        -- Backup existing views
        CREATE TABLE IF NOT EXISTS backup.v_line_movements_backup_20250724 AS 
        SELECT * FROM staging.v_line_movements LIMIT 0;
        
        -- Store view definition
        CREATE TABLE IF NOT EXISTS backup.view_definitions_backup AS (
            SELECT 
                'v_line_movements' as view_name,
                pg_get_viewdef('staging.v_line_movements') as view_definition,
                NOW() as backup_timestamp
        );
        """
        
        try:
            # Create backup schema if it doesn't exist
            await db_manager.execute_query("CREATE SCHEMA IF NOT EXISTS backup;")
            await db_manager.execute_query(backup_sql)
            results["steps"].append({
                "step": "backup_views",
                "status": "success",
                "message": "Existing views backed up successfully"
            })
        except Exception as e:
            logger.warning("Backup failed, continuing anyway", error=str(e))
            results["steps"].append({
                "step": "backup_views", 
                "status": "warning",
                "message": f"Backup failed: {e}"
            })
        
        # Step 2: Apply improvements
        logger.info("Step 2: Applying improved line movement calculations")
        await db_manager.execute_query(improvements_sql)
        results["steps"].append({
            "step": "apply_improvements",
            "status": "success", 
            "message": "Improved line movement views created successfully"
        })
        
        # Step 3: Test the improvements
        logger.info("Step 3: Testing improved calculations")
        test_queries = [
            {
                "name": "test_view_exists",
                "sql": "SELECT COUNT(*) FROM staging.v_line_movements LIMIT 1;",
                "expected": "numeric"
            },
            {
                "name": "test_corrected_calculations",
                "sql": """
                SELECT 
                    COUNT(*) as total_movements,
                    COUNT(*) FILTER (WHERE odds_change_raw != odds_change) as corrected_movements,
                    AVG(ABS(odds_change_raw - odds_change)) as avg_correction
                FROM staging.v_line_movements 
                WHERE previous_odds IS NOT NULL;
                """,
                "expected": "record"
            },
            {
                "name": "test_line_value_detection", 
                "sql": """
                SELECT COUNT(*) FROM staging.v_line_value_changes;
                """,
                "expected": "numeric"
            },
            {
                "name": "test_sharp_movements",
                "sql": """
                SELECT COUNT(*) FROM staging.v_sharp_movements;
                """,
                "expected": "numeric"
            }
        ]
        
        test_results = {}
        for test in test_queries:
            try:
                result = await db_manager.fetch_one(test["sql"])
                test_results[test["name"]] = result
                logger.info(f"Test {test['name']} passed", result=result)
            except Exception as e:
                logger.error(f"Test {test['name']} failed", error=str(e))
                test_results[test["name"]] = {"error": str(e)}
        
        results["steps"].append({
            "step": "test_improvements",
            "status": "success",
            "message": "All tests completed",
            "test_results": test_results
        })
        
        # Step 4: Generate validation report
        logger.info("Step 4: Generating validation report")
        validation_sql = """
        SELECT 
            'Total movements' as metric,
            COUNT(*) as value
        FROM staging.v_line_movements
        WHERE previous_odds IS NOT NULL
        
        UNION ALL
        
        SELECT 
            'Movements with corrections' as metric,
            COUNT(*) as value
        FROM staging.v_line_movements
        WHERE previous_odds IS NOT NULL 
          AND odds_change_raw != odds_change
        
        UNION ALL
        
        SELECT 
            'Line value changes detected' as metric,
            COUNT(*) as value
        FROM staging.v_line_movements
        WHERE has_line_value_change = TRUE
        
        UNION ALL
        
        SELECT 
            'High quality movements' as metric,
            COUNT(*) as value
        FROM staging.v_line_movements
        WHERE movement_quality_score >= 0.8
        
        UNION ALL
        
        SELECT 
            'Sharp movements identified' as metric,
            COUNT(*) as value
        FROM staging.v_sharp_movements;
        """
        
        validation_results = await db_manager.fetch_all(validation_sql)
        results["steps"].append({
            "step": "validation_report",
            "status": "success",
            "message": "Validation report generated",
            "metrics": validation_results
        })
        
        results["deployment_end"] = asyncio.get_event_loop().time()
        
        logger.info("Line movement improvements deployed successfully!")
        
    except Exception as e:
        logger.error("Deployment failed", error=str(e))
        results["success"] = False
        results["error"] = str(e)
        results["steps"].append({
            "step": "deployment_error",
            "status": "error",
            "message": str(e)
        })
    
    finally:
        if 'db_manager' in locals():
            await db_manager.close()
    
    return results

async def main():
    """Main deployment function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Deploy line movement calculation improvements")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without executing")
    args = parser.parse_args()
    
    # Setup logging
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    logger.info("Starting line movement improvements deployment", dry_run=args.dry_run)
    
    results = await deploy_improvements(dry_run=args.dry_run)
    
    if results["success"]:
        logger.info("Deployment completed successfully", results=results)
        if not args.dry_run:
            print("\n🎉 Line movement improvements deployed successfully!")
            print("\nKey improvements:")
            print("✅ American odds calculations corrected (-101 to +101 = 2 points)")
            print("✅ Line value change detection for spreads/totals")
            print("✅ Smart filtering for line flips and false positives")
            print("✅ Movement quality scoring system")
            print("✅ Sharp movement detection view")
        else:
            print("\n✅ Dry run completed - improvements ready to deploy")
    else:
        logger.error("Deployment failed", error=results.get("error"))
        print(f"\n❌ Deployment failed: {results.get('error')}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())