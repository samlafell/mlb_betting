"""
Line Movement CLI Commands

Commands for collecting and analyzing historical line movements.
"""

import asyncio
from datetime import datetime, timedelta

import click
import structlog

from ....core.config import get_settings
from ....data.collection.base import CollectionRequest, CollectorConfig, DataSource
from ....data.collection.consolidated_action_network_collector import (
    ActionNetworkCollector,
    CollectionMode,
)

logger = structlog.get_logger(__name__)


@click.group()
def line_movement():
    """Line movement collection and analysis commands."""
    pass


@line_movement.command()
@click.option(
    "--date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Date to collect (default: today)",
)
@click.option(
    "--backfill-days",
    type=int,
    default=1,
    help="Number of days to backfill (default: 1)",
)
@click.option(
    "--dry-run", is_flag=True, help="Show what would be collected without inserting"
)
def collect_historical(date: datetime | None, backfill_days: int, dry_run: bool):
    """
    Collect historical line movements from Action Network.

    This command captures the complete line movement history that Action Network
    provides in their 'history' arrays, including all odds changes with timestamps.

    Examples:
        # Collect today's games with history
        uv run -m src.interfaces.cli line-movement collect-historical

        # Collect specific date
        uv run -m src.interfaces.cli line-movement collect-historical --date 2025-07-18

        # Backfill last 7 days
        uv run -m src.interfaces.cli line-movement collect-historical --backfill-days 7
    """

    if date is None:
        date = datetime.now()

    click.echo("🎯 Collecting historical line movements")
    click.echo(f"📅 Date: {date.strftime('%Y-%m-%d')}")
    click.echo(f"📊 Backfill days: {backfill_days}")
    click.echo(f"🔍 Dry run: {dry_run}")
    click.echo()

    asyncio.run(_collect_historical_movements(date, backfill_days, dry_run))


@line_movement.command()
@click.option("--game-id", type=int, help="Specific game ID to analyze")
@click.option(
    "--date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Analyze games from specific date",
)
@click.option("--sportsbook", help="Filter by sportsbook name")
@click.option(
    "--bet-type",
    type=click.Choice(["moneyline", "spread", "total"]),
    help="Filter by bet type",
)
def analyze_movements(
    game_id: int | None,
    date: datetime | None,
    sportsbook: str | None,
    bet_type: str | None,
):
    """
    Analyze historical line movements for patterns.

    Examples:
        # Analyze all movements for a date
        uv run -m src.interfaces.cli line-movement analyze-movements --date 2025-07-18

        # Analyze specific game
        uv run -m src.interfaces.cli line-movement analyze-movements --game-id 123

        # Analyze DraftKings total movements
        uv run -m src.interfaces.cli line-movement analyze-movements --sportsbook DraftKings --bet-type total
    """

    click.echo("📊 Analyzing line movements...")
    asyncio.run(_analyze_line_movements(game_id, date, sportsbook, bet_type))


@line_movement.command()
@click.option(
    "--date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Date to check (default: today)",
)
def status(date: datetime | None):
    """
    Show line movement collection status.

    Displays:
    - Number of games with movement history
    - Total movement records
    - Coverage by sportsbook
    - Opening vs closing line analysis
    """

    if date is None:
        date = datetime.now()

    click.echo(f"📊 Line Movement Status for {date.strftime('%Y-%m-%d')}")
    asyncio.run(_show_movement_status(date))


async def _collect_historical_movements(
    target_date: datetime, backfill_days: int, dry_run: bool
):
    """Collect historical movements for specified date range."""

    config = CollectorConfig(
        source=DataSource.ACTION_NETWORK,
        enabled=True,
        rate_limit_per_minute=60,
        timeout_seconds=30,
    )

    collector = ActionNetworkCollector(config, CollectionMode.HISTORICAL)

    try:
        total_movements = 0

        for day_offset in range(backfill_days):
            current_date = target_date - timedelta(days=day_offset)

            click.echo(f"🔍 Processing {current_date.strftime('%Y-%m-%d')}...")

            request = CollectionRequest(
                source=DataSource.ACTION_NETWORK, start_date=current_date
            )

            try:
                result = await collector.collect_data(request)

                if result:
                    click.echo(f"   ✅ Collected {len(result)} games")
                    total_movements += len(result)
                else:
                    click.echo("   ❌ No data collected")

            except Exception as e:
                click.echo(f"   ❌ Error: {str(e)}")

        if dry_run:
            click.echo("\n🔍 Dry run completed - no data was inserted")
        else:
            click.echo(
                f"\n✅ Historical movement collection completed - {total_movements} total movements"
            )

    finally:
        await collector.client.close()


async def _analyze_line_movements(
    game_id: int | None,
    date: datetime | None,
    sportsbook: str | None,
    bet_type: str | None,
):
    """Analyze line movements with filters."""

    import asyncpg

    # Use centralized database configuration
    settings = get_settings()

    conn = await asyncpg.connect(
        host=settings.database.host,
        port=settings.database.port,
        database=settings.database.database,
        user=settings.database.user,
        password=settings.database.password,
    )

    try:
        # Build query based on filters
        where_clauses = []
        params = []
        param_count = 0

        if game_id:
            param_count += 1
            where_clauses.append(f"lms.game_id = ${param_count}")
            params.append(game_id)

        if date:
            param_count += 1
            where_clauses.append(f"DATE(lms.game_datetime) = ${param_count}")
            params.append(date.date())

        if sportsbook:
            param_count += 1
            where_clauses.append(f"lms.sportsbook_name ILIKE ${param_count}")
            params.append(f"%{sportsbook}%")

        if bet_type:
            param_count += 1
            where_clauses.append(f"lms.bet_type = ${param_count}")
            params.append(bet_type)

        where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        query = f"""
            SELECT 
                lms.home_team,
                lms.away_team,
                lms.sportsbook_name,
                lms.bet_type,
                lms.side,
                lms.opening_odds,
                lms.closing_odds,
                lms.odds_movement,
                lms.line_movement,
                lms.total_movements,
                lms.opening_timestamp,
                lms.closing_timestamp
            FROM core_betting.line_movement_summary lms
            {where_clause}
            ORDER BY ABS(lms.odds_movement) DESC
            LIMIT 20
        """

        movements = await conn.fetch(query, *params)

        if not movements:
            click.echo("No line movements found for the specified criteria.")
            return

        click.echo("\n📊 Top Line Movements (by odds change):")
        click.echo("=" * 100)

        for movement in movements:
            direction = "📈" if movement["odds_movement"] > 0 else "📉"
            click.echo(f"{direction} {movement['away_team']} @ {movement['home_team']}")
            click.echo(
                f"   {movement['sportsbook_name']} | {movement['bet_type']} {movement['side']}"
            )
            click.echo(
                f"   Opening: {movement['opening_odds']:+d} → Closing: {movement['closing_odds']:+d} (Change: {movement['odds_movement']:+d})"
            )
            click.echo(f"   Total movements: {movement['total_movements']}")
            click.echo()

    finally:
        await conn.close()


async def _show_movement_status(date: datetime):
    """Show line movement collection status."""

    import asyncpg

    # Use centralized database configuration
    settings = get_settings()

    conn = await asyncpg.connect(
        host=settings.database.host,
        port=settings.database.port,
        database=settings.database.database,
        user=settings.database.user,
        password=settings.database.password,
    )

    try:
        # Games with movement data
        games_count = await conn.fetchval(
            """
            SELECT COUNT(DISTINCT game_id) 
            FROM core_betting.line_movement_history 
            WHERE DATE(game_datetime) = $1
        """,
            date.date(),
        )

        # Total movement records
        total_movements = await conn.fetchval(
            """
            SELECT COUNT(*) 
            FROM core_betting.line_movement_history 
            WHERE DATE(game_datetime) = $1
        """,
            date.date(),
        )

        # Sportsbook coverage
        sportsbook_coverage = await conn.fetch(
            """
            SELECT 
                s.display_name,
                COUNT(DISTINCT lmh.game_id) as games_covered,
                COUNT(*) as total_movements
            FROM core_betting.line_movement_history lmh
            JOIN core_betting.sportsbooks s ON lmh.sportsbook_id = s.id
            WHERE DATE(lmh.game_datetime) = $1
            GROUP BY s.display_name
            ORDER BY total_movements DESC
        """,
            date.date(),
        )

        click.echo(f"📊 Games with movement data: {games_count}")
        click.echo(f"📈 Total movement records: {total_movements}")
        click.echo()

        if sportsbook_coverage:
            click.echo("🏦 Sportsbook Coverage:")
            for book in sportsbook_coverage:
                click.echo(
                    f"   {book['display_name']}: {book['games_covered']} games, {book['total_movements']} movements"
                )

    finally:
        await conn.close()


# Register commands with main CLI
def register_commands(cli_group):
    """Register line movement commands with main CLI."""
    cli_group.add_command(line_movement, name="line-movement")
