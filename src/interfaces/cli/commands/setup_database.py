"""
Database setup CLI command for Action Network betting data.

This command sets up the Action Network database schema and tests the connection.
"""

import asyncio
from pathlib import Path

import click
import structlog

from ....data.database.action_network_repository import ActionNetworkRepository
from ....data.database.connection import get_connection

logger = structlog.get_logger(__name__)


@click.group()
def database():
    """Database setup and management commands."""
    pass


@database.command()
@click.option(
    "--schema-file",
    "-f",
    type=click.Path(path_type=Path, exists=True),
    default=Path("sql/action_network_betting_tables.sql"),
    help="Path to the SQL schema file",
)
@click.option(
    "--test-connection", "-t", is_flag=True, help="Test database connection after setup"
)
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
def setup_action_network(schema_file: Path, test_connection: bool, verbose: bool):
    """
    Set up Action Network database schema.

    Creates the action_network schema with tables for:
    - betting_lines: Main table for line movement history
    - extraction_log: Tracks extraction status for incremental updates
    - line_movement_summary: Aggregated line movement data
    - sportsbooks: Reference table for Action Network sportsbooks

    Examples:
        # Setup with default schema file
        uv run python -m src.interfaces.cli database setup-action-network

        # Setup with custom schema file
        uv run python -m src.interfaces.cli database setup-action-network -f custom_schema.sql

        # Setup and test connection
        uv run python -m src.interfaces.cli database setup-action-network -t
    """
    asyncio.run(_setup_action_network_async(schema_file, test_connection, verbose))


async def _setup_action_network_async(
    schema_file: Path, test_connection: bool, verbose: bool
):
    """Async implementation of Action Network database setup."""
    if verbose:
        logger.info(
            "Setting up Action Network database schema", schema_file=str(schema_file)
        )

    connection = None

    try:
        # Get database connection
        connection = get_connection()
        await connection.connect()

        logger.info("Connected to database successfully")

        # Read and execute schema file
        if not schema_file.exists():
            raise click.ClickException(f"Schema file not found: {schema_file}")

        with open(schema_file) as f:
            schema_sql = f.read()

        if verbose:
            logger.info(
                "Executing schema SQL",
                file_size=len(schema_sql),
                file_path=str(schema_file),
            )

        # Execute schema SQL
        await connection.execute_async(schema_sql, fetch=None, table="schema_setup")

        logger.info("✅ Action Network database schema setup completed successfully")

        # Test connection and repository if requested
        if test_connection:
            await _test_action_network_connection(connection, verbose)

    except Exception as e:
        logger.error("❌ Failed to setup Action Network database schema", error=str(e))
        raise click.ClickException(f"Database setup failed: {str(e)}")

    finally:
        if connection:
            await connection.close()


async def _test_action_network_connection(connection, verbose: bool):
    """Test the Action Network database connection and repository."""
    try:
        logger.info("Testing Action Network database connection...")

        # Initialize repository
        repository = ActionNetworkRepository(connection)

        # Run health check
        health_result = await repository.health_check()

        if verbose:
            logger.info("Database health check result", health=health_result)

        if health_result["status"] == "healthy":
            logger.info("✅ Database connection test passed")

            # Show table information
            tables = health_result.get("tables_exist", [])
            counts = health_result.get("record_counts", {})

            click.echo("\n📊 Database Tables:")
            for table in tables:
                count = counts.get(table, 0)
                click.echo(f"  • {table}: {count:,} records")

            missing_tables = health_result.get("missing_tables", [])
            if missing_tables:
                click.echo(f"\n⚠️  Missing tables: {', '.join(missing_tables)}")
        else:
            logger.error("❌ Database connection test failed", health=health_result)
            raise click.ClickException("Database health check failed")

    except Exception as e:
        logger.error("❌ Database connection test failed", error=str(e))
        raise click.ClickException(f"Connection test failed: {str(e)}")


@database.command()
@click.option("--game-id", "-g", type=int, help="Specific game ID to check")
@click.option("--book-id", "-b", type=int, help="Specific book ID to check")
@click.option(
    "--market-type",
    "-m",
    type=click.Choice(["moneyline", "spread", "total"]),
    help="Specific market type to check",
)
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
def check_data(
    game_id: int | None,
    book_id: int | None,
    market_type: str | None,
    verbose: bool,
):
    """
    Check Action Network data in the database.

    Examples:
        # Check all data
        uv run python -m src.interfaces.cli database check-data

        # Check specific game
        uv run python -m src.interfaces.cli database check-data -g 257653

        # Check specific game and market
        uv run python -m src.interfaces.cli database check-data -g 257653 -m moneyline
    """
    asyncio.run(_check_data_async(game_id, book_id, market_type, verbose))


async def _check_data_async(
    game_id: int | None,
    book_id: int | None,
    market_type: str | None,
    verbose: bool,
):
    """Async implementation of data checking."""
    connection = None

    try:
        # Get database connection
        connection = get_connection()
        await connection.connect()

        repository = ActionNetworkRepository(connection)

        if game_id:
            # Check specific game
            summary = await repository.get_line_movement_summary(
                game_id, book_id, market_type
            )

            if summary:
                click.echo(f"\n📈 Line Movement Summary for Game {game_id}:")

                for item in summary:
                    click.echo(f"\n  🏟️  {item['home_team']} vs {item['away_team']}")
                    click.echo(
                        f"  📚 {item['sportsbook_display_name']} - {item['market_type'].title()} ({item['side']})"
                    )

                    if item["opening_odds_american"] and item["closing_odds_american"]:
                        click.echo(
                            f"  📊 Opening: {item['opening_odds_american']:+d} → Closing: {item['closing_odds_american']:+d}"
                        )
                        movement = (
                            item["closing_odds_american"]
                            - item["opening_odds_american"]
                        )
                        if movement > 0:
                            click.echo(
                                f"  📈 Movement: +{movement} (line moved against)"
                            )
                        elif movement < 0:
                            click.echo(f"  📉 Movement: {movement} (line moved toward)")
                        else:
                            click.echo("  ➡️  Movement: No change")

                    if item["total_movements"]:
                        click.echo(f"  🔄 Total movements: {item['total_movements']}")

                    if item["reverse_line_movement_detected"]:
                        click.echo("  🔄 Reverse line movement detected")

                    if item["steam_moves_detected"]:
                        click.echo(f"  🚀 Steam moves: {item['steam_moves_detected']}")
            else:
                click.echo(f"❌ No data found for game {game_id}")

        else:
            # Check overall data
            health_result = await repository.health_check()

            click.echo("\n📊 Action Network Database Summary:")

            if health_result["status"] == "healthy":
                counts = health_result.get("record_counts", {})

                click.echo(f"  • Betting Lines: {counts.get('betting_lines', 0):,}")
                click.echo(f"  • Extraction Log: {counts.get('extraction_log', 0):,}")
                click.echo(
                    f"  • Line Movement Summary: {counts.get('line_movement_summary', 0):,}"
                )
                click.echo(f"  • Sportsbooks: {counts.get('sportsbooks', 0):,}")

                if counts.get("betting_lines", 0) > 0:
                    # Get some sample data
                    sample_query = """
                        SELECT 
                            COUNT(DISTINCT game_id) as unique_games,
                            COUNT(DISTINCT book_id) as unique_books,
                            COUNT(*) as total_lines,
                            MIN(line_timestamp) as earliest_line,
                            MAX(line_timestamp) as latest_line
                        FROM action_network.betting_lines
                    """

                    result = await connection.execute_async(
                        sample_query, fetch="one", table="action_network.betting_lines"
                    )

                    if result:
                        click.echo("\n📈 Data Coverage:")
                        click.echo(f"  • Unique games: {result[0]:,}")
                        click.echo(f"  • Unique sportsbooks: {result[1]:,}")
                        click.echo(f"  • Total line records: {result[2]:,}")
                        click.echo(f"  • Date range: {result[3]} to {result[4]}")
            else:
                click.echo("❌ Database health check failed")

    except Exception as e:
        logger.error("❌ Failed to check data", error=str(e))
        raise click.ClickException(f"Data check failed: {str(e)}")

    finally:
        if connection:
            await connection.close()


@database.command()
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
def test_connection(verbose: bool):
    """Test database connection."""
    asyncio.run(_test_connection_async(verbose))


async def _test_connection_async(verbose: bool):
    """Async implementation of connection testing."""
    connection = None

    try:
        logger.info("Testing database connection...")

        # Get database connection
        connection = get_connection()
        await connection.connect()

        # Test basic query
        result = await connection.execute_async(
            "SELECT version()", fetch="one", table="system"
        )

        if result:
            postgres_version = result[0]
            logger.info("✅ Database connection successful", version=postgres_version)
            click.echo(f"✅ Connected to: {postgres_version}")
        else:
            raise Exception("No response from database")

    except Exception as e:
        logger.error("❌ Database connection failed", error=str(e))
        raise click.ClickException(f"Connection failed: {str(e)}")

    finally:
        if connection:
            await connection.close()


if __name__ == "__main__":
    database()
