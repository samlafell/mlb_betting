"""
Diagnostic script and fixes for CollectionOrchestrator multi-schema data recording issues.
Handles games in public.games and betting markets in mlb_betting schema.
"""

import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

# Add the project root to Python path for imports
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Configure detailed logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class MultiSchemaCollectionDiagnostics:
    """Diagnostic tools for debugging multi-schema CollectionOrchestrator issues."""

    def __init__(self, storage_service, integration_service=None):
        self.storage = storage_service
        self.integration = integration_service

    async def diagnose_full_pipeline(self) -> dict[str, Any]:
        """Run comprehensive diagnostics on the multi-schema pipeline."""
        results = {
            "timestamp": datetime.now().isoformat(),
            "database_connectivity": await self.check_database_connectivity(),
            "schema_validation": await self.validate_schemas(),
            "staging_table_status": await self.check_staging_table(),
            "schema_routing_test": await self.test_schema_routing(),
            "betting_data_transformation": await self.test_betting_data_transformation(),
            "integration_service_test": await self.test_integration_service(),
            "cross_schema_verification": await self.verify_cross_schema_data(),
            "recommendations": [],
        }

        # Generate recommendations based on findings
        results["recommendations"] = self.generate_recommendations(results)

        return results

    async def check_database_connectivity(self) -> dict[str, Any]:
        """Test database connectivity and schema access."""
        try:
            async with self.storage.pool.acquire() as conn:
                # Test basic connectivity
                result = await conn.fetchval("SELECT 1")

                # Check current database and user
                db_info = await conn.fetchrow("""
                    SELECT 
                        current_database() as database,
                        current_user as user,
                        current_schema() as schema
                """)

                # Check schema existence
                schemas = await conn.fetch("""
                    SELECT schema_name 
                    FROM information_schema.schemata 
                    WHERE schema_name IN ('public', 'mlb_betting')
                    ORDER BY schema_name
                """)

                schema_names = [row["schema_name"] for row in schemas]

                return {
                    "status": "connected",
                    "database_info": dict(db_info),
                    "available_schemas": schema_names,
                    "public_schema_exists": "public" in schema_names,
                    "mlb_betting_schema_exists": "mlb_betting" in schema_names,
                    "connection_test": result == 1,
                }

        except Exception as e:
            return {"status": "failed", "error": str(e)}

    async def validate_schemas(self) -> dict[str, Any]:
        """Validate both public and mlb_betting schema structures."""
        try:
            async with self.storage.pool.acquire() as conn:
                schema_info = {}

                # Check public.games table
                public_tables = await conn.fetch("""
                    SELECT table_name, table_type
                    FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = 'games'
                """)

                if public_tables:
                    games_columns = await conn.fetch("""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = 'games'
                        ORDER BY ordinal_position
                    """)

                    games_count = await conn.fetchval(
                        "SELECT COUNT(*) FROM public.games"
                    )

                    schema_info["public_games"] = {
                        "exists": True,
                        "columns": [dict(col) for col in games_columns],
                        "row_count": games_count,
                    }
                else:
                    schema_info["public_games"] = {"exists": False}

                # Check mlb_betting schema tables
                mlb_tables = await conn.fetch("""
                    SELECT table_name, table_type
                    FROM information_schema.tables
                    WHERE table_schema = 'mlb_betting' 
                    AND table_name IN ('moneyline', 'spread', 'totals')
                    ORDER BY table_name
                """)

                mlb_betting_info = {}
                for table in mlb_tables:
                    table_name = table["table_name"]

                    columns = await conn.fetch(
                        """
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = 'mlb_betting' AND table_name = $1
                        ORDER BY ordinal_position
                    """,
                        table_name,
                    )

                    try:
                        row_count = await conn.fetchval(
                            f"SELECT COUNT(*) FROM mlb_betting.{table_name}"
                        )
                    except:
                        row_count = "N/A (access denied)"

                    mlb_betting_info[table_name] = {
                        "exists": True,
                        "columns": [dict(col) for col in columns],
                        "row_count": row_count,
                    }

                # Check for missing expected tables
                expected_tables = ["moneyline", "spread", "totals"]
                existing_tables = [table["table_name"] for table in mlb_tables]
                missing_tables = [
                    t for t in expected_tables if t not in existing_tables
                ]

                schema_info["mlb_betting"] = mlb_betting_info
                schema_info["missing_mlb_betting_tables"] = missing_tables

                return {"status": "success", "schema_info": schema_info}

        except Exception as e:
            return {"status": "failed", "error": str(e)}

    async def check_staging_table(self) -> dict[str, Any]:
        """Check the staging table and analyze the data structure."""
        try:
            async with self.storage.pool.acquire() as conn:
                # Check if staging table exists
                table_exists = await conn.fetchval("""
                    SELECT EXISTS(
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_name = 'sbr_parsed_games'
                    )
                """)

                if not table_exists:
                    return {
                        "status": "missing",
                        "error": "sbr_parsed_games table does not exist",
                    }

                # Get staging table statistics by bet type
                stats = await conn.fetchrow("""
                    SELECT 
                        COUNT(*) as total_rows,
                        COUNT(*) FILTER (WHERE status = 'new') as new_rows,
                        COUNT(*) FILTER (WHERE status = 'loaded') as loaded_rows,
                        COUNT(*) FILTER (WHERE status = 'failed') as failed_rows,
                        COUNT(*) FILTER (WHERE status = 'duplicate') as duplicate_rows
                    FROM sbr_parsed_games
                """)

                # Get bet type breakdown
                bet_type_stats = await conn.fetch("""
                    SELECT 
                        game_data->>'bet_type' as bet_type,
                        COUNT(*) as count,
                        COUNT(*) FILTER (WHERE status = 'new') as new_count,
                        COUNT(*) FILTER (WHERE status = 'loaded') as loaded_count,
                        COUNT(*) FILTER (WHERE status = 'failed') as failed_count
                    FROM sbr_parsed_games
                    GROUP BY game_data->>'bet_type'
                    ORDER BY count DESC
                """)

                # Get sample records for each bet type
                sample_records = {}
                for bet_type_row in bet_type_stats:
                    bet_type = bet_type_row["bet_type"]
                    if bet_type and bet_type_row["new_count"] > 0:
                        sample = await conn.fetchrow(
                            """
                            SELECT id, game_data
                            FROM sbr_parsed_games 
                            WHERE status = 'new' AND game_data->>'bet_type' = $1
                            LIMIT 1
                        """,
                            bet_type,
                        )

                        if sample:
                            game_data = sample["game_data"]
                            if isinstance(game_data, str):
                                game_data = json.loads(game_data)

                            sample_records[bet_type] = {
                                "id": sample["id"],
                                "sbr_game_id": game_data.get("sbr_game_id"),
                                "game_date": game_data.get("game_date"),
                                "odds_count": len(game_data.get("odds_data", [])),
                                "sample_sportsbook": game_data.get("odds_data", [{}])[
                                    0
                                ].get("sportsbook")
                                if game_data.get("odds_data")
                                else None,
                            }

                return {
                    "status": "exists",
                    "overall_statistics": dict(stats),
                    "bet_type_breakdown": [dict(row) for row in bet_type_stats],
                    "sample_records": sample_records,
                }

        except Exception as e:
            return {"status": "failed", "error": str(e)}

    async def test_schema_routing(self) -> dict[str, Any]:
        """Test that data can be written to both schemas correctly."""
        try:
            async with self.storage.pool.acquire() as conn:
                routing_tests = {}

                # Test writing to public.games (if it exists)
                try:
                    await conn.fetchval("SELECT 1 FROM public.games LIMIT 1")
                    routing_tests["public_games_accessible"] = True
                except Exception as e:
                    routing_tests["public_games_accessible"] = False
                    routing_tests["public_games_error"] = str(e)

                # Test writing to mlb_betting tables
                mlb_tables = ["moneyline", "spread", "totals"]
                for table in mlb_tables:
                    try:
                        await conn.fetchval(
                            f"SELECT 1 FROM mlb_betting.{table} LIMIT 1"
                        )
                        routing_tests[f"mlb_betting_{table}_accessible"] = True
                    except Exception as e:
                        routing_tests[f"mlb_betting_{table}_accessible"] = False
                        routing_tests[f"mlb_betting_{table}_error"] = str(e)

                # Test cross-schema transaction capability
                try:
                    async with conn.transaction():
                        # This tests if we can work with both schemas in one transaction
                        await conn.fetchval("SELECT 1 FROM public.games LIMIT 1")
                        await conn.fetchval(
                            "SELECT 1 FROM mlb_betting.moneyline LIMIT 1"
                        )

                    routing_tests["cross_schema_transaction"] = True
                except Exception as e:
                    routing_tests["cross_schema_transaction"] = False
                    routing_tests["cross_schema_transaction_error"] = str(e)

                return {"status": "tested", "routing_tests": routing_tests}

        except Exception as e:
            return {"status": "failed", "error": str(e)}

    async def test_betting_data_transformation(self) -> dict[str, Any]:
        """Test the betting data transformation logic for mlb_betting tables."""
        try:
            async with self.storage.pool.acquire() as conn:
                transformation_tests = {}

                # Test each bet type transformation
                bet_types = ["moneyline", "spread", "totals"]

                for bet_type in bet_types:
                    # Get a sample record for this bet type
                    sample_record = await conn.fetchrow(
                        """
                        SELECT game_data FROM sbr_parsed_games 
                        WHERE status = 'new' AND game_data->>'bet_type' = $1
                        LIMIT 1
                    """,
                        bet_type,
                    )

                    if not sample_record:
                        transformation_tests[bet_type] = {
                            "status": "no_test_data",
                            "message": f"No {bet_type} records available for testing",
                        }
                        continue

                    game_data = sample_record["game_data"]
                    if isinstance(game_data, str):
                        game_data = json.loads(game_data)

                    # Test the transformation logic
                    transformation_result = self.test_bet_type_transformation(
                        game_data, bet_type
                    )
                    transformation_tests[bet_type] = transformation_result

                return {
                    "status": "tested",
                    "transformation_tests": transformation_tests,
                }

        except Exception as e:
            return {"status": "failed", "error": str(e)}

    def test_bet_type_transformation(
        self, game_dict: dict[str, Any], bet_type: str
    ) -> dict[str, Any]:
        """Test transformation logic for a specific bet type."""
        try:
            odds_data = game_dict.get("odds_data", [])
            transformed_records = []
            errors = []

            for odds in odds_data:
                record = {
                    "bet_type": game_dict.get("bet_type"),
                    "sportsbook": odds.get("sportsbook"),
                    "timestamp": game_dict.get("scraped_at"),
                }

                # Apply bet-type specific transformations
                if bet_type == "moneyline":
                    record["home_ml"] = odds.get("moneyline_home") or odds.get(
                        "home_ml"
                    )
                    record["away_ml"] = odds.get("moneyline_away") or odds.get(
                        "away_ml"
                    )

                    # Validate that we have the required fields
                    if not record["home_ml"] or not record["away_ml"]:
                        errors.append(
                            f"Missing moneyline data for {odds.get('sportsbook')}"
                        )

                elif bet_type == "spread":
                    record["home_spread"] = odds.get("spread_home") or odds.get(
                        "home_spread"
                    )
                    record["away_spread"] = odds.get("spread_away") or odds.get(
                        "away_spread"
                    )

                    if not record["home_spread"] or not record["away_spread"]:
                        errors.append(
                            f"Missing spread data for {odds.get('sportsbook')}"
                        )

                elif bet_type in ("total", "totals"):
                    record["total_line"] = odds.get("total_line")
                    record["over_price"] = odds.get("total_over") or odds.get(
                        "over_price"
                    )
                    record["under_price"] = odds.get("total_under") or odds.get(
                        "under_price"
                    )

                    if (
                        not record["total_line"]
                        or not record["over_price"]
                        or not record["under_price"]
                    ):
                        errors.append(
                            f"Missing totals data for {odds.get('sportsbook')}"
                        )

                transformed_records.append(record)

            return {
                "status": "success",
                "original_odds_count": len(odds_data),
                "transformed_records_count": len(transformed_records),
                "sample_transformed_record": transformed_records[0]
                if transformed_records
                else None,
                "transformation_errors": errors,
                "data_quality": {
                    "has_sportsbook_names": all(
                        r.get("sportsbook") for r in transformed_records
                    ),
                    "has_timestamps": all(
                        r.get("timestamp") for r in transformed_records
                    ),
                    "records_with_complete_data": len(
                        [
                            r
                            for r in transformed_records
                            if self.is_record_complete(r, bet_type)
                        ]
                    ),
                },
            }

        except Exception as e:
            return {"status": "failed", "error": str(e)}

    def is_record_complete(self, record: dict[str, Any], bet_type: str) -> bool:
        """Check if a transformed record has complete data for its bet type."""
        if bet_type == "moneyline":
            return bool(record.get("home_ml") and record.get("away_ml"))
        elif bet_type == "spread":
            return bool(record.get("home_spread") and record.get("away_spread"))
        elif bet_type in ("total", "totals"):
            return bool(
                record.get("total_line")
                and record.get("over_price")
                and record.get("under_price")
            )
        return False

    async def test_integration_service(self) -> dict[str, Any]:
        """Test the IntegrationService with focus on multi-schema operations."""
        if not self.integration:
            return {
                "status": "not_available",
                "error": "IntegrationService not provided",
            }

        try:
            # Test with one record of each bet type
            test_results = {}
            bet_types = ["moneyline", "spread", "totals"]

            async with self.storage.pool.acquire() as conn:
                for bet_type in bet_types:
                    sample_record = await conn.fetchrow(
                        """
                        SELECT game_data FROM sbr_parsed_games 
                        WHERE status = 'new' AND game_data->>'bet_type' = $1
                        LIMIT 1
                    """,
                        bet_type,
                    )

                    if not sample_record:
                        test_results[bet_type] = {
                            "status": "no_test_data",
                            "message": f"No {bet_type} records to test with",
                        }
                        continue

                    game_data = sample_record["game_data"]
                    if isinstance(game_data, str):
                        game_data = json.loads(game_data)

                    # Test integration (this might actually insert data - use with caution)
                    logger.info(
                        f"Testing integration for {bet_type} - SBR ID: {game_data.get('sbr_game_id')}"
                    )

                    try:
                        inserted_count = await self.integration.integrate([game_data])
                        test_results[bet_type] = {
                            "status": "tested",
                            "sbr_game_id": game_data.get("sbr_game_id"),
                            "integration_result": inserted_count,
                            "success": inserted_count > 0,
                        }
                    except Exception as integration_error:
                        test_results[bet_type] = {
                            "status": "failed",
                            "error": str(integration_error),
                            "sbr_game_id": game_data.get("sbr_game_id"),
                        }

                return {"status": "tested", "bet_type_results": test_results}

        except Exception as e:
            return {"status": "failed", "error": str(e)}

    async def verify_cross_schema_data(self) -> dict[str, Any]:
        """Verify data relationships between public.games and mlb_betting tables."""
        try:
            async with self.storage.pool.acquire() as conn:
                verification_results = {}

                # Get recent game IDs from public.games
                recent_games = await conn.fetch("""
                    SELECT id, external_game_id, game_date 
                    FROM public.games 
                    ORDER BY created_at DESC 
                    LIMIT 10
                """)

                if not recent_games:
                    return {
                        "status": "no_games_data",
                        "message": "No games found in public.games table",
                    }

                verification_results["recent_games_count"] = len(recent_games)
                verification_results["sample_game_ids"] = [
                    dict(game) for game in recent_games[:3]
                ]

                # Check for corresponding betting data in mlb_betting tables
                game_ids = [game["id"] for game in recent_games]

                for table in ["moneyline", "spread", "totals"]:
                    try:
                        betting_records = await conn.fetch(
                            f"""
                            SELECT COUNT(*) as count, MIN(created_at) as earliest, MAX(created_at) as latest
                            FROM mlb_betting.{table}
                            WHERE game_id = ANY($1)
                        """,
                            game_ids,
                        )

                        if betting_records and betting_records[0]["count"] > 0:
                            verification_results[f"{table}_records"] = dict(
                                betting_records[0]
                            )
                        else:
                            verification_results[f"{table}_records"] = {"count": 0}

                    except Exception as e:
                        verification_results[f"{table}_error"] = str(e)

                # Check for orphaned betting records (betting data without games)
                for table in ["moneyline", "spread", "totals"]:
                    try:
                        orphaned = await conn.fetchval(f"""
                            SELECT COUNT(*) 
                            FROM mlb_betting.{table} b
                            LEFT JOIN public.games g ON b.game_id = g.id
                            WHERE g.id IS NULL
                        """)

                        verification_results[f"{table}_orphaned_records"] = orphaned

                    except Exception as e:
                        verification_results[f"{table}_orphaned_check_error"] = str(e)

                return {"status": "verified", "results": verification_results}

        except Exception as e:
            return {"status": "failed", "error": str(e)}

    def generate_recommendations(self, diagnostic_results: dict[str, Any]) -> list[str]:
        """Generate specific recommendations for multi-schema issues."""
        recommendations = []

        # Database connectivity
        connectivity = diagnostic_results.get("database_connectivity", {})
        if connectivity.get("status") != "connected":
            recommendations.append("Fix database connectivity issues")
        elif not connectivity.get("mlb_betting_schema_exists", False):
            recommendations.append("Create the mlb_betting schema - it's missing")
        elif not connectivity.get("public_schema_exists", False):
            recommendations.append("Verify public schema access")

        # Schema validation
        schema_validation = diagnostic_results.get("schema_validation", {})
        if schema_validation.get("status") == "success":
            schema_info = schema_validation.get("schema_info", {})

            # Check public.games
            if not schema_info.get("public_games", {}).get("exists", False):
                recommendations.append(
                    "Create public.games table - games data has nowhere to go"
                )
            elif schema_info.get("public_games", {}).get("row_count", 0) == 0:
                recommendations.append(
                    "public.games table is empty - verify game data is being inserted"
                )

            # Check mlb_betting tables
            missing_tables = schema_info.get("missing_mlb_betting_tables", [])
            if missing_tables:
                recommendations.append(
                    f"Create missing mlb_betting tables: {', '.join(missing_tables)}"
                )

            # Check for empty betting tables
            mlb_betting = schema_info.get("mlb_betting", {})
            empty_betting_tables = []
            for table_name, info in mlb_betting.items():
                if info.get("row_count") == 0:
                    empty_betting_tables.append(table_name)

            if empty_betting_tables:
                recommendations.append(
                    f"mlb_betting tables are empty: {', '.join(empty_betting_tables)}. Verify IntegrationService is routing betting data correctly."
                )

        # Staging table issues
        staging = diagnostic_results.get("staging_table_status", {})
        if staging.get("status") == "exists":
            stats = staging.get("overall_statistics", {})
            new_rows = stats.get("new_rows", 0)
            failed_rows = stats.get("failed_rows", 0)

            if new_rows > 0:
                recommendations.append(
                    f"Process {new_rows} pending records in staging table"
                )

            if failed_rows > 0:
                recommendations.append(
                    f"Investigate {failed_rows} failed records - check logs for IntegrationService errors"
                )

            # Check bet type distribution
            bet_type_breakdown = staging.get("bet_type_breakdown", [])
            if not bet_type_breakdown:
                recommendations.append(
                    "No bet type data found in staging - verify scraping is working"
                )

        # Schema routing issues
        routing = diagnostic_results.get("schema_routing_test", {})
        if routing.get("status") == "tested":
            routing_tests = routing.get("routing_tests", {})

            if not routing_tests.get("cross_schema_transaction", True):
                recommendations.append(
                    "Cross-schema transactions are failing - this will prevent proper data insertion"
                )

            # Check individual table access
            failed_access = []
            for key, accessible in routing_tests.items():
                if "accessible" in key and not accessible:
                    table_name = key.replace("_accessible", "")
                    failed_access.append(table_name)

            if failed_access:
                recommendations.append(
                    f"Cannot access tables: {', '.join(failed_access)}. Check permissions."
                )

        # Integration service issues
        integration_test = diagnostic_results.get("integration_service_test", {})
        if integration_test.get("status") == "tested":
            bet_type_results = integration_test.get("bet_type_results", {})
            failed_integrations = []

            for bet_type, result in bet_type_results.items():
                if result.get("status") == "failed" or (
                    result.get("status") == "tested"
                    and not result.get("success", False)
                ):
                    failed_integrations.append(bet_type)

            if failed_integrations:
                recommendations.append(
                    f"IntegrationService failing for: {', '.join(failed_integrations)}. Check schema routing in IntegrationService."
                )

        # Cross-schema verification
        cross_schema = diagnostic_results.get("cross_schema_verification", {})
        if cross_schema.get("status") == "verified":
            results = cross_schema.get("results", {})

            # Check for orphaned betting records
            orphaned_counts = []
            for table in ["moneyline", "spread", "totals"]:
                orphaned = results.get(f"{table}_orphaned_records", 0)
                if orphaned > 0:
                    orphaned_counts.append(f"{table}: {orphaned}")

            if orphaned_counts:
                recommendations.append(
                    f"Found orphaned betting records (betting data without corresponding games): {', '.join(orphaned_counts)}"
                )

            # Check for missing betting data
            games_count = results.get("recent_games_count", 0)
            betting_tables_with_data = []
            for table in ["moneyline", "spread", "totals"]:
                count = results.get(f"{table}_records", {}).get("count", 0)
                if count > 0:
                    betting_tables_with_data.append(table)

            if games_count > 0 and not betting_tables_with_data:
                recommendations.append(
                    "Games exist but no betting data found - IntegrationService may not be writing to mlb_betting schema"
                )

        return recommendations


# Enhanced process_staging method with multi-schema awareness
class EnhancedMultiSchemaOrchestrator:
    """Enhanced orchestrator with multi-schema debugging."""

    async def process_staging_with_schema_awareness(
        self, batch_size: int = 100, progress_callback=None
    ):
        """Enhanced staging processor with detailed multi-schema logging."""
        if not self.storage:
            logger.warning("Storage service not initialized")
            return

        # Track schema-specific stats
        schema_stats = {
            "games_inserted_to_public": 0,
            "moneyline_inserted_to_mlb_betting": 0,
            "spread_inserted_to_mlb_betting": 0,
            "totals_inserted_to_mlb_betting": 0,
            "cross_schema_failures": 0,
        }

        try:
            from sportsbookreview.services.integration_service import IntegrationService

            integrator = IntegrationService(self.storage)
            logger.info("IntegrationService initialized for multi-schema operations")
        except ImportError as e:
            logger.error(f"Failed to import IntegrationService: {e}")
            return

        async with self.storage.pool.acquire() as conn:
            # Pre-check: verify both schemas are accessible
            try:
                await conn.fetchval("SELECT 1 FROM public.games LIMIT 1")
                logger.info("✓ public.games accessible")
            except Exception as e:
                logger.error(f"✗ Cannot access public.games: {e}")
                return

            try:
                await conn.fetchval("SELECT 1 FROM mlb_betting.moneyline LIMIT 1")
                logger.info("✓ mlb_betting schema accessible")
            except Exception as e:
                logger.error(f"✗ Cannot access mlb_betting schema: {e}")
                return

            processed_rows = 0

            while True:
                rows = await conn.fetch(
                    "SELECT id, game_data FROM sbr_parsed_games WHERE status='new' LIMIT $1",
                    batch_size,
                )

                if not rows:
                    break

                logger.info(f"Processing batch of {len(rows)} records")

                for row in rows:
                    row_id = row["id"]

                    try:
                        game_dict = row["game_data"]
                        if isinstance(game_dict, str):
                            game_dict = json.loads(game_dict)

                        bet_type = game_dict.get("bet_type")
                        sbr_id = game_dict.get("sbr_game_id")

                        logger.debug(
                            f"Processing row {row_id} - Bet Type: {bet_type}, SBR ID: {sbr_id}"
                        )

                        # Validate data structure
                        if not self.validate_game_data_structure(game_dict, bet_type):
                            logger.warning(
                                f"Row {row_id} has invalid data structure for {bet_type}"
                            )
                            await conn.execute(
                                "UPDATE sbr_parsed_games SET status='failed' WHERE id=$1",
                                row_id,
                            )
                            continue

                        # Log what we're about to process
                        odds_count = len(game_dict.get("odds_data", []))
                        logger.debug(
                            f"Row {row_id} has {odds_count} odds records for {bet_type}"
                        )

                        # Cross-schema transaction
                        async with conn.transaction():
                            try:
                                # Pre-integration check: verify target tables
                                await self.verify_target_schema_access(conn, bet_type)

                                # Call integration service
                                inserted = await integrator.integrate([game_dict])

                                logger.debug(
                                    f"Row {row_id} integration result: {inserted} records inserted"
                                )

                                # Verify insertion actually happened in correct schemas
                                verification_result = (
                                    await self.verify_post_integration(
                                        conn, game_dict, bet_type
                                    )
                                )

                                if inserted > 0 and verification_result["success"]:
                                    new_status = "loaded"
                                    processed_rows += 1

                                    # Update schema-specific stats
                                    self.update_schema_stats(
                                        schema_stats, verification_result, bet_type
                                    )

                                    logger.info(
                                        f"✓ Row {row_id} ({bet_type}) successfully processed - Game: {verification_result.get('game_found')}, Betting: {verification_result.get('betting_found')}"
                                    )

                                else:
                                    # Integration claimed success but verification failed
                                    if inserted > 0:
                                        logger.error(
                                            f"✗ Row {row_id} integration claimed success but verification failed: {verification_result}"
                                        )
                                        schema_stats["cross_schema_failures"] += 1

                                    # Check for duplicates
                                    existing = (
                                        await self.storage.get_existing_games(
                                            [str(sbr_id)]
                                        )
                                        if sbr_id
                                        else []
                                    )
                                    new_status = "duplicate" if existing else "failed"

                                    logger.warning(
                                        f"✗ Row {row_id} marked as {new_status} - Integration: {inserted}, Verification: {verification_result}"
                                    )

                                # Update status
                                await conn.execute(
                                    "UPDATE sbr_parsed_games SET status=$1 WHERE id=$2",
                                    new_status,
                                    row_id,
                                )

                            except Exception as integration_error:
                                logger.error(
                                    f"✗ Integration failed for row {row_id}: {integration_error}"
                                )
                                await conn.execute(
                                    "UPDATE sbr_parsed_games SET status='failed' WHERE id=$1",
                                    row_id,
                                )
                                schema_stats["cross_schema_failures"] += 1

                    except Exception as row_error:
                        logger.error(f"✗ Failed to process row {row_id}: {row_error}")
                        try:
                            await conn.execute(
                                "UPDATE sbr_parsed_games SET status='failed' WHERE id=$1",
                                row_id,
                            )
                        except:
                            logger.error(f"Failed to update status for row {row_id}")

                # Progress callback
                if progress_callback:
                    progress_callback(
                        90,
                        f"Processed {processed_rows} rows - Schema stats: {schema_stats}",
                    )

        logger.info("Multi-schema processing complete:")
        logger.info(f"  - Processed: {processed_rows}")
        logger.info(
            f"  - Public.games inserts: {schema_stats['games_inserted_to_public']}"
        )
        logger.info(
            f"  - MLB betting inserts: ML={schema_stats['moneyline_inserted_to_mlb_betting']}, Spread={schema_stats['spread_inserted_to_mlb_betting']}, Totals={schema_stats['totals_inserted_to_mlb_betting']}"
        )
        logger.info(
            f"  - Cross-schema failures: {schema_stats['cross_schema_failures']}"
        )

        return schema_stats

    def validate_game_data_structure(
        self, game_dict: dict[str, Any], bet_type: str
    ) -> bool:
        """Validate that game data has the required structure for multi-schema insertion."""
        required_fields = ["sbr_game_id", "bet_type", "game_date", "odds_data"]

        # Check basic required fields
        for field in required_fields:
            if not game_dict.get(field):
                logger.debug(f"Missing required field: {field}")
                return False

        # Check odds_data structure
        odds_data = game_dict.get("odds_data", [])
        if not isinstance(odds_data, list) or len(odds_data) == 0:
            logger.debug("odds_data is not a valid list or is empty")
            return False

        # Validate bet-type specific data
        for odds in odds_data:
            if not isinstance(odds, dict):
                logger.debug("Invalid odds record structure")
                return False

            if not odds.get("sportsbook"):
                logger.debug("Missing sportsbook in odds record")
                return False

            # Check bet-type specific fields
            if bet_type == "moneyline":
                if not (odds.get("moneyline_home") or odds.get("home_ml")) or not (
                    odds.get("moneyline_away") or odds.get("away_ml")
                ):
                    logger.debug(f"Missing moneyline data for {odds.get('sportsbook')}")
                    return False
            elif bet_type == "spread":
                if not (odds.get("spread_home") or odds.get("home_spread")) or not (
                    odds.get("spread_away") or odds.get("away_spread")
                ):
                    logger.debug(f"Missing spread data for {odds.get('sportsbook')}")
                    return False
            elif bet_type in ("total", "totals"):
                if (
                    not odds.get("total_line")
                    or not (odds.get("total_over") or odds.get("over_price"))
                    or not (odds.get("total_under") or odds.get("under_price"))
                ):
                    logger.debug(f"Missing totals data for {odds.get('sportsbook')}")
                    return False

        return True

    async def verify_target_schema_access(self, conn, bet_type: str) -> None:
        """Verify we can access the target schemas before attempting integration."""
        try:
            # Check public.games access
            await conn.fetchval("SELECT 1 FROM public.games LIMIT 1")
        except Exception as e:
            raise Exception(f"Cannot access public.games: {e}")

        # Check mlb_betting table access based on bet type
        table_map = {
            "moneyline": "moneyline",
            "spread": "spread",
            "total": "totals",
            "totals": "totals",
        }

        target_table = table_map.get(bet_type)
        if target_table:
            try:
                await conn.fetchval(f"SELECT 1 FROM mlb_betting.{target_table} LIMIT 1")
            except Exception as e:
                raise Exception(f"Cannot access mlb_betting.{target_table}: {e}")

    async def verify_post_integration(
        self, conn, game_dict: dict[str, Any], bet_type: str
    ) -> dict[str, Any]:
        """Verify that data was actually inserted into the correct schemas."""
        sbr_id = game_dict.get("sbr_game_id")

        verification_result = {
            "success": False,
            "game_found": False,
            "betting_found": False,
            "details": {},
        }

        try:
            # Check if game was inserted into public.games
            game_exists = await conn.fetchval(
                """
                SELECT EXISTS(
                    SELECT 1 FROM public.games 
                    WHERE sportsbookreview_game_id = $1 
                    OR game_id = $1
                )
            """,
                str(sbr_id),
            )

            verification_result["game_found"] = game_exists

            # Check if betting data was inserted into appropriate mlb_betting table
            table_map = {
                "moneyline": "moneyline",
                "spread": "spread",
                "total": "totals",
                "totals": "totals",
            }

            target_table = table_map.get(bet_type)
            if target_table:
                # Look for recent betting records that could match this game
                betting_exists = await conn.fetchval(
                    f"""
                    SELECT EXISTS(
                        SELECT 1 FROM mlb_betting.{target_table}
                        WHERE created_at > NOW() - INTERVAL '5 minutes'
                        AND (
                            game_id IN (
                                SELECT id FROM public.games 
                                WHERE sportsbookreview_game_id = $1 
                                OR game_id = $1
                            )
                            OR game_id IN (
                                SELECT id FROM public.games 
                                WHERE sportsbookreview_game_id = $1 
                                OR game_id = $1
                            )
                        )
                    )
                """,
                    str(sbr_id),
                )

                verification_result["betting_found"] = betting_exists
                verification_result["details"]["target_table"] = target_table

            # Overall success if both schemas have the data
            verification_result["success"] = (
                verification_result["game_found"]
                and verification_result["betting_found"]
            )

        except Exception as e:
            verification_result["error"] = str(e)
            logger.error(f"Post-integration verification failed: {e}")

        return verification_result

    def update_schema_stats(
        self,
        schema_stats: dict[str, int],
        verification_result: dict[str, Any],
        bet_type: str,
    ) -> None:
        """Update schema-specific statistics."""
        if verification_result.get("game_found"):
            schema_stats["games_inserted_to_public"] += 1

        if verification_result.get("betting_found"):
            table_map = {
                "moneyline": "moneyline_inserted_to_mlb_betting",
                "spread": "spread_inserted_to_mlb_betting",
                "total": "totals_inserted_to_mlb_betting",
                "totals": "totals_inserted_to_mlb_betting",
            }

            stat_key = table_map.get(bet_type)
            if stat_key:
                schema_stats[stat_key] += 1


# Quick diagnostic runner for multi-schema issues
async def run_multi_schema_diagnostics():
    """Run diagnostics specifically focused on multi-schema issues."""
    try:
        # Initialize services (adjust imports based on your structure)
        from sportsbookreview.services.data_storage_service import DataStorageService

        storage = DataStorageService()
        await storage.initialize_connection()

        # Try to import integration service
        try:
            from sportsbookreview.services.integration_service import IntegrationService

            integration = IntegrationService(storage)
        except ImportError:
            integration = None
            print("WARNING: IntegrationService not available for testing")

        diagnostics = MultiSchemaCollectionDiagnostics(storage, integration)
        results = await diagnostics.diagnose_full_pipeline()

        # Print formatted results
        print("\n" + "=" * 80)
        print("MULTI-SCHEMA COLLECTION DIAGNOSTICS")
        print("=" * 80)

        # Summary of key findings
        print("\n🔍 KEY FINDINGS:")
        connectivity = results.get("database_connectivity", {})
        if connectivity.get("status") == "connected":
            print(
                f"✓ Database connected: {connectivity.get('database_info', {}).get('database')}"
            )
            print(
                f"✓ Public schema: {'✓' if connectivity.get('public_schema_exists') else '✗'}"
            )
            print(
                f"✓ MLB betting schema: {'✓' if connectivity.get('mlb_betting_schema_exists') else '✗'}"
            )
        else:
            print("✗ Database connection failed")

        # Schema validation summary
        schema_validation = results.get("schema_validation", {})
        if schema_validation.get("status") == "success":
            schema_info = schema_validation.get("schema_info", {})

            # Public games
            public_games = schema_info.get("public_games", {})
            if public_games.get("exists"):
                row_count = public_games.get("row_count", 0)
                print(f"✓ public.games: {row_count} records")
            else:
                print("✗ public.games: missing")

            # MLB betting tables
            mlb_betting = schema_info.get("mlb_betting", {})
            for table in ["moneyline", "spread", "totals"]:
                if table in mlb_betting:
                    row_count = mlb_betting[table].get("row_count", 0)
                    print(f"✓ mlb_betting.{table}: {row_count} records")
                else:
                    print(f"✗ mlb_betting.{table}: missing")

        # Staging status
        staging = results.get("staging_table_status", {})
        if staging.get("status") == "exists":
            stats = staging.get("overall_statistics", {})
            print("\n📊 STAGING STATUS:")
            print(f"  New records: {stats.get('new_rows', 0)}")
            print(f"  Loaded: {stats.get('loaded_rows', 0)}")
            print(f"  Failed: {stats.get('failed_rows', 0)}")
            print(f"  Duplicates: {stats.get('duplicate_rows', 0)}")

        # Print detailed results
        print("\n📋 DETAILED RESULTS:")
        for section, data in results.items():
            if section == "recommendations":
                continue
            print(f"\n{section.upper().replace('_', ' ')}:")
            print(json.dumps(data, indent=2, default=str))

        # Recommendations
        print("\n💡 RECOMMENDATIONS:")
        for i, rec in enumerate(results["recommendations"], 1):
            print(f"{i}. {rec}")

        await storage.close_connection()

        return results

    except Exception as e:
        print(f"Multi-schema diagnostics failed: {e}")
        return None


if __name__ == "__main__":
    asyncio.run(run_multi_schema_diagnostics())
