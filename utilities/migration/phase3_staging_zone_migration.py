#!/usr/bin/env python3
"""
Phase 3: STAGING Zone Migration

Migrates and transforms data from RAW zone to STAGING zone with data cleaning,
normalization, and quality scoring. This phase applies business rules and
creates analysis-ready datasets.

Migration flow:
- raw_data.moneylines_raw → staging.moneylines (cleaned & normalized)
- raw_data.spreads_raw → staging.spreads (cleaned & normalized)
- raw_data.totals_raw → staging.totals (cleaned & normalized)
- Unified game records → staging.games (deduplicated & enriched)
"""

import asyncio
import json
import sys
from datetime import datetime, timezone
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


class DataQualityScorer:
    """Handles data quality scoring and validation for STAGING zone records."""

    @staticmethod
    def calculate_quality_score(record: dict, bet_type: str) -> tuple[float, list[str]]:
        """Calculate data quality score (0-9.99) and identify validation errors."""
        score = 9.99  # Start with perfect score of 9.99 (max for numeric(3,2))
        errors = []

        # Core data validation (deduct from 9.99 scale)
        if not record.get("sportsbook_name"):
            score -= 2.0  # 20% penalty
            errors.append("Missing sportsbook name")

        if not record.get("game_external_id"):
            score -= 2.5  # 25% penalty
            errors.append("Missing game reference")

        # Bet-type specific validation
        if bet_type == "moneyline":
            if not record.get("home_odds") and not record.get("away_odds"):
                score -= 3.0  # 30% penalty
                errors.append("Missing both home and away odds")
            elif not record.get("home_odds") or not record.get("away_odds"):
                score -= 1.5  # 15% penalty
                errors.append("Missing one side of odds")

        elif bet_type == "spread":
            if record.get("spread_value") is None:
                score -= 2.5  # 25% penalty
                errors.append("Missing spread value")
            if not record.get("spread_odds"):
                score -= 1.5  # 15% penalty
                errors.append("Missing spread odds")

        elif bet_type == "total":
            if record.get("total_points") is None:
                score -= 2.5  # 25% penalty
                errors.append("Missing total points value")
            if not record.get("over_odds") and not record.get("under_odds"):
                score -= 1.5  # 15% penalty
                errors.append("Missing both over/under odds")

        # Data consistency checks
        if bet_type in ["moneyline", "spread", "total"]:
            if record.get("collected_at") and record.get("created_at"):
                # Ensure collected_at is not after created_at
                try:
                    collected = record["collected_at"]
                    created = record["created_at"]
                    if collected > created:
                        score -= 0.5  # 5% penalty
                        errors.append("Collection timestamp after creation timestamp")
                except (ValueError, TypeError):
                    pass

        # Bonus points for completeness (stay within 9.99 limit)
        if record.get("source") and record.get("source") != "UNKNOWN":
            score = min(9.99, score + 0.5)  # 5% bonus, capped at 9.99

        if record.get("raw_data") and isinstance(record.get("raw_data"), dict):
            score = min(9.99, score + 0.3)  # 3% bonus, capped at 9.99

        return max(0.0, min(9.99, score)), errors


class TeamNormalizer:
    """Handles MLB team name normalization and standardization."""

    # MLB team name mappings
    TEAM_MAPPINGS = {
        # American League East
        "NYY": ["Yankees", "New York Yankees", "NY Yankees", "New York (A)"],
        "BOS": ["Red Sox", "Boston Red Sox", "Boston"],
        "TBR": ["Rays", "Tampa Bay Rays", "Tampa Bay", "TB Rays"],
        "TOR": ["Blue Jays", "Toronto Blue Jays", "Toronto"],
        "BAL": ["Orioles", "Baltimore Orioles", "Baltimore"],
        # American League Central
        "CLE": [
            "Guardians",
            "Cleveland Guardians",
            "Cleveland",
            "Indians",
            "Cleveland Indians",
        ],
        "CHW": ["White Sox", "Chicago White Sox", "Chi White Sox", "CWS"],
        "DET": ["Tigers", "Detroit Tigers", "Detroit"],
        "KCR": ["Royals", "Kansas City Royals", "Kansas City", "KC Royals"],
        "MIN": ["Twins", "Minnesota Twins", "Minnesota"],
        # American League West
        "HOU": ["Astros", "Houston Astros", "Houston"],
        "LAA": ["Angels", "Los Angeles Angels", "LA Angels", "Anaheim Angels"],
        "OAK": ["Athletics", "Oakland Athletics", "Oakland", "A's", "Oakland A's"],
        "SEA": ["Mariners", "Seattle Mariners", "Seattle"],
        "TEX": ["Rangers", "Texas Rangers", "Texas"],
        # National League East
        "ATL": ["Braves", "Atlanta Braves", "Atlanta"],
        "NYM": ["Mets", "New York Mets", "NY Mets", "New York (N)"],
        "PHI": ["Phillies", "Philadelphia Phillies", "Philadelphia"],
        "MIA": ["Marlins", "Miami Marlins", "Miami", "Florida Marlins"],
        "WSN": ["Nationals", "Washington Nationals", "Washington", "WSH"],
        # National League Central
        "MIL": ["Brewers", "Milwaukee Brewers", "Milwaukee"],
        "STL": [
            "Cardinals",
            "St. Louis Cardinals",
            "St Louis Cardinals",
            "StL Cardinals",
        ],
        "CHC": ["Cubs", "Chicago Cubs", "Chi Cubs"],
        "CIN": ["Reds", "Cincinnati Reds", "Cincinnati"],
        "PIT": ["Pirates", "Pittsburgh Pirates", "Pittsburgh"],
        # National League West
        "LAD": ["Dodgers", "Los Angeles Dodgers", "LA Dodgers", "L.A. Dodgers"],
        "SDP": ["Padres", "San Diego Padres", "San Diego"],
        "SFG": ["Giants", "San Francisco Giants", "SF Giants", "San Fran Giants"],
        "COL": ["Rockies", "Colorado Rockies", "Colorado"],
        "ARI": ["Diamondbacks", "Arizona Diamondbacks", "Arizona", "D-backs", "Dbacks"],
    }

    @classmethod
    def normalize_team_name(cls, team_name: str) -> str | None:
        """Normalize team name to standard 3-letter code."""
        if not team_name:
            return None

        team_clean = team_name.strip()

        # Direct match with team code
        if team_clean.upper() in cls.TEAM_MAPPINGS:
            return team_clean.upper()

        # Search through mappings
        for code, variants in cls.TEAM_MAPPINGS.items():
            if team_clean in variants or team_clean.upper() in [
                v.upper() for v in variants
            ]:
                return code

        # Fuzzy matching for common variations
        team_upper = team_clean.upper()
        for code, variants in cls.TEAM_MAPPINGS.items():
            for variant in variants:
                if variant.upper() in team_upper or team_upper in variant.upper():
                    return code

        # Return cleaned original if no match found
        logger.warning(f"Could not normalize team name: {team_name}")
        return team_clean.upper()[:3]


class StagingZoneMigrator:
    """Handles migration and transformation of RAW zone data to STAGING zone."""

    def __init__(self, batch_size: int = 1000):
        self.settings = get_settings()
        self.batch_size = batch_size
        self.migration_stats = {
            "games": {"processed": 0, "successful": 0, "failed": 0},
            "moneylines": {"processed": 0, "successful": 0, "failed": 0},
            "spreads": {"processed": 0, "successful": 0, "failed": 0},
            "totals": {"processed": 0, "successful": 0, "failed": 0},
        }
        self.quality_scorer = DataQualityScorer()
        self.team_normalizer = TeamNormalizer()

    async def initialize(self):
        """Initialize database connection."""
        initialize_connections(self.settings)

    async def close(self):
        """Close database connections."""
        pass  # Connection pool managed globally

    async def migrate_all_to_staging_zone(self) -> dict[str, Any]:
        """Execute complete STAGING zone migration with data cleaning."""
        logger.info("Starting Phase 3: STAGING zone migration")

        migration_results = {
            "timestamp": datetime.now().isoformat(),
            "phase": "Phase 3 - STAGING Zone Migration",
            "status": "in_progress",
            "tables_migrated": {},
            "summary": {},
            "errors": [],
        }

        try:
            connection_manager = get_connection()
            async with connection_manager.get_async_connection() as conn:
                # Pre-flight checks
                await self._verify_staging_zone_tables(conn)

                # Clear existing STAGING data for clean migration
                await self._clear_staging_tables(conn)

                # Migrate games first (needed for foreign keys)
                migration_results["tables_migrated"][
                    "games"
                ] = await self._migrate_games(conn)

                # Migrate betting tables
                migration_results["tables_migrated"][
                    "moneylines"
                ] = await self._migrate_moneylines(conn)
                migration_results["tables_migrated"][
                    "spreads"
                ] = await self._migrate_spreads(conn)
                migration_results["tables_migrated"][
                    "totals"
                ] = await self._migrate_totals(conn)

                # Generate summary
                migration_results["summary"] = self._generate_migration_summary()
                migration_results["status"] = "completed"

        except Exception as e:
            logger.error(f"STAGING zone migration failed: {e}")
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

    async def _verify_staging_zone_tables(self, conn):
        """Verify that STAGING zone tables exist and are accessible."""
        logger.info("Verifying STAGING zone table structure...")

        required_tables = [
            "staging.games",
            "staging.moneylines",
            "staging.spreads",
            "staging.totals",
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
                raise Exception(f"Required STAGING zone table {table} does not exist")

        logger.info("✅ All STAGING zone tables verified")

    async def _clear_staging_tables(self, conn):
        """Clear existing STAGING data for clean migration."""
        logger.info("Clearing STAGING zone tables for clean migration...")

        # Order matters due to foreign key constraints
        tables = [
            "staging.moneylines",
            "staging.spreads",
            "staging.totals",
            "staging.games",
        ]

        for table in tables:
            await conn.execute(f"DELETE FROM {table}")
            logger.info(f"Cleared {table}")

        logger.info("✅ STAGING zone tables cleared")

    async def _migrate_games(self, conn) -> dict[str, Any]:
        """Create normalized game records in staging.games."""
        logger.info("Migrating games to STAGING zone...")

        table_stats = {"processed": 0, "successful": 0, "failed": 0, "batches": 0}

        try:
            # Extract unique games from RAW zone moneylines
            games_query = """
                SELECT DISTINCT
                    game_external_id,
                    MIN(collected_at) as earliest_collected_at,
                    MAX(collected_at) as latest_collected_at,
                    game_date,
                    COUNT(*) as record_count
                FROM raw_data.moneylines_raw
                WHERE game_external_id IS NOT NULL
                GROUP BY game_external_id, game_date
                ORDER BY game_date DESC, game_external_id
            """

            games = await conn.fetch(games_query)
            total_games = len(games)
            logger.info(f"Processing {total_games} unique games...")

            batch_successful = 0
            for game in games:
                try:
                    # Calculate basic quality score for game (0-9.99 scale)
                    quality_score = 9.99
                    validation_status = "validated"

                    # Check if we have adequate betting line coverage
                    coverage_check = await conn.fetchrow(
                        """
                        SELECT 
                            COUNT(DISTINCT sportsbook_name) as sportsbook_count,
                            COUNT(*) as total_records
                        FROM raw_data.moneylines_raw
                        WHERE game_external_id = $1
                    """,
                        game["game_external_id"],
                    )

                    if coverage_check["sportsbook_count"] < 2:
                        quality_score -= 2.0  # 20% penalty
                        validation_status = "low_coverage"

                    if coverage_check["total_records"] < 5:
                        quality_score -= 1.0  # 10% penalty

                    # Insert game record
                    await conn.execute(
                        """
                        INSERT INTO staging.games (
                            external_id, game_date, game_datetime,
                            data_quality_score, validation_status,
                            created_at, updated_at
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                    """,
                        game["game_external_id"],
                        game["game_date"],
                        game["earliest_collected_at"],
                        quality_score,
                        validation_status,
                        datetime.now(timezone.utc),
                        datetime.now(timezone.utc),
                    )

                    batch_successful += 1

                except Exception as e:
                    logger.error(
                        f"Failed to migrate game {game['game_external_id']}: {e}"
                    )
                    table_stats["failed"] += 1

            table_stats["processed"] = total_games
            table_stats["successful"] = batch_successful
            table_stats["batches"] = 1

            self.migration_stats["games"] = table_stats
            logger.info(f"✅ Games migration completed: {batch_successful:,} games")

        except Exception as e:
            logger.error(f"Games migration failed: {e}")
            table_stats["error"] = str(e)

        return table_stats

    async def _migrate_moneylines(self, conn) -> dict[str, Any]:
        """Migrate moneyline data with cleaning and normalization."""
        logger.info("Migrating moneylines to STAGING zone...")

        table_stats = {"processed": 0, "successful": 0, "failed": 0, "batches": 0}

        try:
            # Get total count
            count_result = await conn.fetchrow("""
                SELECT COUNT(*) as total FROM raw_data.moneylines_raw
            """)
            total_records = count_result["total"]
            logger.info(f"Processing {total_records:,} moneyline records...")

            # Process in batches
            offset = 0
            while offset < total_records:
                batch_records = await conn.fetch(
                    """
                    SELECT 
                        r.*,
                        g.id as staging_game_id
                    FROM raw_data.moneylines_raw r
                    LEFT JOIN staging.games g ON r.game_external_id = g.external_id
                    ORDER BY r.id
                    LIMIT $1 OFFSET $2
                """,
                    self.batch_size,
                    offset,
                )

                if not batch_records:
                    break

                batch_successful = 0
                for record in batch_records:
                    try:
                        # Skip records without game reference
                        if not record["staging_game_id"]:
                            table_stats["failed"] += 1
                            continue

                        # Extract data for quality scoring
                        record_dict = dict(record)
                        quality_score, errors = (
                            self.quality_scorer.calculate_quality_score(
                                record_dict, "moneyline"
                            )
                        )

                        # Normalize team names
                        home_team_raw = record.get("home_team_name", "")
                        away_team_raw = record.get("away_team_name", "")
                        home_team_norm = self.team_normalizer.normalize_team_name(
                            home_team_raw
                        )
                        away_team_norm = self.team_normalizer.normalize_team_name(
                            away_team_raw
                        )

                        # Determine validation status (updated for 0-9.99 scale)
                        validation_status = (
                            "validated"
                            if quality_score >= 8.0
                            else "needs_review"
                            if quality_score >= 6.0
                            else "invalid"
                        )

                        # Insert staging record
                        await conn.execute(
                            """
                            INSERT INTO staging.moneylines (
                                raw_id, game_id, sportsbook_id, sportsbook_name,
                                home_odds, away_odds, home_team_normalized, away_team_normalized,
                                data_quality_score, validation_status, validation_errors,
                                processed_at, created_at, updated_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                        """,
                            record["id"],  # raw_id
                            record["staging_game_id"],
                            record["sportsbook_id"],
                            record["sportsbook_name"],
                            record["home_odds"],
                            record["away_odds"],
                            home_team_norm,
                            away_team_norm,
                            quality_score,
                            validation_status,
                            json.dumps(errors) if errors else None,
                            datetime.now(timezone.utc),
                            datetime.now(timezone.utc),
                            datetime.now(timezone.utc),
                        )

                        batch_successful += 1

                    except Exception as e:
                        logger.error(
                            f"Failed to migrate moneyline record {record['id']}: {e}"
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
        """Migrate spread data with cleaning and normalization."""
        logger.info("Migrating spreads to STAGING zone...")

        table_stats = {"processed": 0, "successful": 0, "failed": 0, "batches": 0}

        try:
            count_result = await conn.fetchrow("""
                SELECT COUNT(*) as total FROM raw_data.spreads_raw
            """)
            total_records = count_result["total"]
            logger.info(f"Processing {total_records:,} spread records...")

            offset = 0
            while offset < total_records:
                batch_records = await conn.fetch(
                    """
                    SELECT 
                        r.*,
                        g.id as staging_game_id
                    FROM raw_data.spreads_raw r
                    LEFT JOIN staging.games g ON r.game_external_id = g.external_id
                    ORDER BY r.id
                    LIMIT $1 OFFSET $2
                """,
                    self.batch_size,
                    offset,
                )

                if not batch_records:
                    break

                batch_successful = 0
                for record in batch_records:
                    try:
                        if not record["staging_game_id"]:
                            table_stats["failed"] += 1
                            continue

                        record_dict = dict(record)
                        quality_score, errors = (
                            self.quality_scorer.calculate_quality_score(
                                record_dict, "spread"
                            )
                        )

                        # Normalize team names
                        favorite_team_norm = self.team_normalizer.normalize_team_name(
                            record.get("favorite_team", "")
                        )
                        underdog_team_norm = self.team_normalizer.normalize_team_name(
                            record.get("underdog_team", "")
                        )

                        validation_status = (
                            "validated"
                            if quality_score >= 8.0
                            else "needs_review"
                            if quality_score >= 6.0
                            else "invalid"
                        )

                        await conn.execute(
                            """
                            INSERT INTO staging.spreads (
                                raw_id, game_id, sportsbook_id, sportsbook_name,
                                spread_value, spread_odds, favorite_team_normalized, underdog_team_normalized,
                                data_quality_score, validation_status, validation_errors,
                                processed_at, created_at, updated_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                        """,
                            record["id"],
                            record["staging_game_id"],
                            record["sportsbook_id"],
                            record["sportsbook_name"],
                            record["spread_value"],
                            record["spread_odds"],
                            favorite_team_norm,
                            underdog_team_norm,
                            quality_score,
                            validation_status,
                            json.dumps(errors) if errors else None,
                            datetime.now(timezone.utc),
                            datetime.now(timezone.utc),
                            datetime.now(timezone.utc),
                        )

                        batch_successful += 1

                    except Exception as e:
                        logger.error(
                            f"Failed to migrate spread record {record['id']}: {e}"
                        )
                        table_stats["failed"] += 1

                table_stats["processed"] += len(batch_records)
                table_stats["successful"] += batch_successful
                table_stats["batches"] += 1
                offset += self.batch_size

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
        """Migrate totals data with cleaning and normalization."""
        logger.info("Migrating totals to STAGING zone...")

        table_stats = {"processed": 0, "successful": 0, "failed": 0, "batches": 0}

        try:
            count_result = await conn.fetchrow("""
                SELECT COUNT(*) as total FROM raw_data.totals_raw
            """)
            total_records = count_result["total"]
            logger.info(f"Processing {total_records:,} totals records...")

            offset = 0
            while offset < total_records:
                batch_records = await conn.fetch(
                    """
                    SELECT 
                        r.*,
                        g.id as staging_game_id
                    FROM raw_data.totals_raw r
                    LEFT JOIN staging.games g ON r.game_external_id = g.external_id
                    ORDER BY r.id
                    LIMIT $1 OFFSET $2
                """,
                    self.batch_size,
                    offset,
                )

                if not batch_records:
                    break

                batch_successful = 0
                for record in batch_records:
                    try:
                        if not record["staging_game_id"]:
                            table_stats["failed"] += 1
                            continue

                        record_dict = dict(record)
                        quality_score, errors = (
                            self.quality_scorer.calculate_quality_score(
                                record_dict, "total"
                            )
                        )

                        validation_status = (
                            "validated"
                            if quality_score >= 8.0
                            else "needs_review"
                            if quality_score >= 6.0
                            else "invalid"
                        )

                        await conn.execute(
                            """
                            INSERT INTO staging.totals (
                                raw_id, game_id, sportsbook_id, sportsbook_name,
                                total_points, over_odds, under_odds,
                                data_quality_score, validation_status, validation_errors,
                                processed_at, created_at, updated_at
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                        """,
                            record["id"],
                            record["staging_game_id"],
                            record["sportsbook_id"],
                            record["sportsbook_name"],
                            record["total_points"],
                            record["over_odds"],
                            record["under_odds"],
                            quality_score,
                            validation_status,
                            json.dumps(errors) if errors else None,
                            datetime.now(timezone.utc),
                            datetime.now(timezone.utc),
                            datetime.now(timezone.utc),
                        )

                        batch_successful += 1

                    except Exception as e:
                        logger.error(
                            f"Failed to migrate totals record {record['id']}: {e}"
                        )
                        table_stats["failed"] += 1

                table_stats["processed"] += len(batch_records)
                table_stats["successful"] += batch_successful
                table_stats["batches"] += 1
                offset += self.batch_size

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
    migrator = StagingZoneMigrator(batch_size=1000)

    try:
        await migrator.initialize()

        print("🚀 Starting Phase 3: STAGING Zone Migration")
        print("=" * 60)

        # Run migration
        results = await migrator.migrate_all_to_staging_zone()

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
        output_file = Path("utilities/migration/phase3_migration_results.json")

        with open(output_file, "w") as f:
            json.dump(results, f, indent=2, default=str)

        print(f"\n💾 Migration results saved to: {output_file}")
        print("\n✅ Phase 3 STAGING Zone Migration Complete!")

    except Exception as e:
        logger.error(f"Phase 3 migration failed: {e}")
        print(f"\n❌ Migration failed: {e}")
        return 1

    finally:
        await migrator.close()

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
