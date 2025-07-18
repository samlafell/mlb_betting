"""
Data Quality Improvement CLI Command

Orchestrates the execution of data quality improvements for betting lines tables.
Implements the comprehensive improvement plan from BETTING_LINES_DATA_QUALITY_ASSESSMENT.md
"""

import asyncio
from datetime import datetime, timedelta

import click

from src.data.database.connection import get_connection
from src.services.sharp_action_detection_service import SharpActionDetectionService


@click.group(name="data-quality")
def data_quality_group():
    """Data quality improvement commands for betting lines."""
    pass


@data_quality_group.command()
@click.option('--execute', is_flag=True, help='Execute the SQL scripts (dry-run by default)')
@click.option('--phase', type=click.Choice(['1', '2', 'all']), default='all',
              help='Which phase to run (1=mapping, 2=validation, all=both)')
def setup(execute: bool, phase: str):
    """
    Setup data quality infrastructure improvements.
    
    Phase 1: Sportsbook mapping system
    Phase 2: Data validation and completeness scoring
    """
    click.echo("🚀 Setting up data quality improvements...")

    if not execute:
        click.echo("⚠️  DRY RUN MODE - Use --execute to apply changes")

    try:
        connection = get_connection()

        asyncio.run(_run_setup(connection, execute, phase))

    except Exception as e:
        click.echo(f"❌ Setup failed: {str(e)}", err=True)
        raise click.Abort()


async def _run_setup(connection, execute: bool, phase: str):
    """Execute the setup phases."""

    sql_files = []

    if phase in ['1', 'all']:
        sql_files.append({
            'name': 'Phase 1: Sportsbook Mapping System',
            'file': 'sql/improvements/01_sportsbook_mapping_system.sql'
        })

    if phase in ['2', 'all']:
        sql_files.append({
            'name': 'Phase 2: Data Validation and Completeness',
            'file': 'sql/improvements/02_data_validation_and_completeness.sql'
        })

    await connection.connect()
    try:
        for sql_config in sql_files:
            click.echo(f"\n📋 {sql_config['name']}")

            try:
                # Read SQL file
                with open(sql_config['file']) as f:
                    sql_content = f.read()

                if execute:
                    # Execute the SQL
                    await connection.execute_async(sql_content, fetch=None, table="data_quality_setup")
                    click.echo(f"✅ {sql_config['name']} applied successfully")
                else:
                    click.echo(f"📄 Would execute: {sql_config['file']}")

            except FileNotFoundError:
                click.echo(f"⚠️  SQL file not found: {sql_config['file']}")
            except Exception as e:
                click.echo(f"❌ Failed to apply {sql_config['name']}: {str(e)}")
                raise
    finally:
        await connection.close()


@data_quality_group.command()
@click.option('--date', type=click.DateTime(formats=['%Y-%m-%d']),
              help='Target date (YYYY-MM-DD), defaults to today')
@click.option('--force-update', is_flag=True,
              help='Update existing sharp action records')
def update_sharp_action(date: datetime | None, force_update: bool):
    """Update sharp action indicators for betting lines."""

    target_date = date.date() if date else datetime.now().date()

    click.echo(f"🎯 Updating sharp action indicators for {target_date}")

    try:
        connection = get_connection()
        service = SharpActionDetectionService(connection)

        result = asyncio.run(
            service.update_sharp_action_indicators(
                target_date=target_date,
                force_update=force_update
            )
        )

        if result['success']:
            click.echo(f"✅ Processed {result['games_processed']} games")
            click.echo(f"📊 Updated {result['records_updated']} records")
        else:
            click.echo(f"❌ Update failed: {result.get('error', 'Unknown error')}")
            raise click.Abort()

    except Exception as e:
        click.echo(f"❌ Sharp action update failed: {str(e)}", err=True)
        raise click.Abort()


@data_quality_group.command()
@click.option('--start-date', type=click.DateTime(formats=['%Y-%m-%d']),
              help='Start date for backfill (YYYY-MM-DD)')
@click.option('--end-date', type=click.DateTime(formats=['%Y-%m-%d']),
              help='End date for backfill (YYYY-MM-DD), defaults to today')
@click.option('--max-days', type=int, default=30,
              help='Maximum number of days to process in one run')
def backfill_sharp_action(start_date: datetime | None,
                         end_date: datetime | None,
                         max_days: int):
    """Backfill sharp action indicators for historical data."""

    end_dt = end_date if end_date else datetime.now()
    start_dt = start_date if start_date else end_dt - timedelta(days=max_days)

    # Limit the range to max_days
    if (end_dt - start_dt).days > max_days:
        start_dt = end_dt - timedelta(days=max_days)
        click.echo(f"⚠️  Limited range to {max_days} days: {start_dt.date()} to {end_dt.date()}")

    click.echo(f"📅 Backfilling sharp action from {start_dt.date()} to {end_dt.date()}")

    try:
        connection = get_connection()
        service = SharpActionDetectionService(connection)

        # Process each date
        current_date = start_dt.date()
        total_games = 0
        total_records = 0

        while current_date <= end_dt.date():
            click.echo(f"Processing {current_date}...")

            result = asyncio.run(
                service.update_sharp_action_indicators(
                    target_date=current_date,
                    force_update=False  # Don't overwrite existing data
                )
            )

            if result['success']:
                total_games += result['games_processed']
                total_records += result['records_updated']
                click.echo(f"  ✅ {result['games_processed']} games, {result['records_updated']} records")
            else:
                click.echo(f"  ❌ Failed: {result.get('error', 'Unknown error')}")

            current_date += timedelta(days=1)

        click.echo("\n🎉 Backfill complete!")
        click.echo(f"📊 Total: {total_games} games, {total_records} records updated")

    except Exception as e:
        click.echo(f"❌ Backfill failed: {str(e)}", err=True)
        raise click.Abort()


@data_quality_group.command()
@click.option('--detailed', is_flag=True, help='Show detailed breakdown by source')
@click.option('--days', type=int, default=30, help='Number of days to analyze')
def status(detailed: bool, days: int):
    """Show current data quality status and metrics."""

    click.echo(f"📊 Data Quality Status (Last {days} days)")
    click.echo("=" * 50)

    try:
        connection = get_connection()

        asyncio.run(_show_status(connection, detailed, days))

    except Exception as e:
        click.echo(f"❌ Status check failed: {str(e)}", err=True)
        raise click.Abort()


async def _show_status(connection, detailed: bool, days: int):
    """Show data quality status."""

    async with connection.get_async_connection() as conn:
        # Overall quality dashboard
        dashboard_query = """
            SELECT * FROM core_betting.data_quality_dashboard 
            ORDER BY table_name
        """

        dashboard_rows = await conn.fetch(dashboard_query)

        click.echo("\n🏆 Overall Data Quality Dashboard:")
        click.echo("-" * 70)

        for row in dashboard_rows:
            table_name = row['table_name']
            total_rows = row['total_rows']
            sportsbook_pct = row['sportsbook_id_pct']
            sharp_action_pct = row['sharp_action_pct']
            betting_pct_pct = row['betting_pct_pct']
            avg_completeness = row['avg_completeness']

            click.echo(f"\n📋 {table_name.upper()} Table:")
            click.echo(f"  Total Records: {total_rows:,}")
            click.echo(f"  Sportsbook ID Mapping: {sportsbook_pct}%")
            click.echo(f"  Sharp Action Data: {sharp_action_pct}%")
            click.echo(f"  Betting Percentages: {betting_pct_pct}%")
            click.echo(f"  Avg Completeness: {avg_completeness}")

            # Quality indicator
            if sportsbook_pct >= 95 and avg_completeness >= 0.8:
                click.echo("  Status: 🟢 EXCELLENT")
            elif sportsbook_pct >= 80 and avg_completeness >= 0.6:
                click.echo("  Status: 🟡 GOOD")
            elif sportsbook_pct >= 50 and avg_completeness >= 0.4:
                click.echo("  Status: 🟠 NEEDS IMPROVEMENT")
            else:
                click.echo("  Status: 🔴 CRITICAL")

        # Recent trends
        if days > 0:
            trend_query = """
                SELECT * FROM core_betting.data_quality_trend 
                WHERE quality_date >= CURRENT_DATE - INTERVAL %s
                ORDER BY quality_date DESC, table_name
                LIMIT 20
            """

            trend_rows = await conn.fetch(trend_query.replace('%s', f"'{days} days'"))

            if trend_rows:
                click.echo(f"\n📈 Recent Quality Trends (Last {days} days):")
                click.echo("-" * 70)

                for row in trend_rows:
                    date_str = row['quality_date'].strftime('%Y-%m-%d')
                    table_name = row['table_name']
                    daily_records = row['daily_records']
                    avg_completeness = row['avg_completeness']
                    sportsbook_mapping_pct = row['sportsbook_mapping_pct']

                    click.echo(f"  {date_str} {table_name}: {daily_records} records, "
                             f"{avg_completeness:.3f} completeness, "
                             f"{sportsbook_mapping_pct}% mapped")

        # Data source analysis if detailed
        if detailed:
            source_query = """
                SELECT * FROM core_betting.data_source_quality_analysis
                ORDER BY avg_completeness DESC
            """

            source_rows = await conn.fetch(source_query)

            click.echo("\n🔍 Data Source Quality Analysis:")
            click.echo("-" * 70)

            for row in source_rows:
                source = row['source']
                total_records = row['total_records']
                mapping_success = row['sportsbook_mapping_success_pct']
                avg_completeness = row['avg_completeness']
                distinct_books_found = row['distinct_sportsbooks_found']
                distinct_books_mapped = row['distinct_sportsbooks_mapped']

                click.echo(f"\n📡 {source}:")
                click.echo(f"  Total Records: {total_records:,}")
                click.echo(f"  Mapping Success: {mapping_success}%")
                click.echo(f"  Avg Completeness: {avg_completeness}")
                click.echo(f"  Sportsbooks Found: {distinct_books_found}")
                click.echo(f"  Sportsbooks Mapped: {distinct_books_mapped}")


@data_quality_group.command()
def health():
    """Check health of data quality services."""

    click.echo("🏥 Data Quality Health Check")
    click.echo("=" * 40)

    try:
        connection = get_connection()

        asyncio.run(_health_check(connection))

    except Exception as e:
        click.echo(f"❌ Health check failed: {str(e)}", err=True)
        raise click.Abort()


async def _health_check(connection):
    """Perform comprehensive health check."""

    # Check database connection
    try:
        async with connection.get_async_connection() as conn:
            await conn.fetchval("SELECT 1")
        click.echo("✅ Database connection: OK")
    except Exception as e:
        click.echo(f"❌ Database connection: FAILED - {str(e)}")
        return

    # Check sharp action detection service
    try:
        service = SharpActionDetectionService(connection)
        health_result = await service.health_check()

        if health_result.get('status') == 'healthy':
            click.echo("✅ Sharp Action Detection Service: OK")
            recent_records = health_result.get('recent_sharp_action_records', 0)
            click.echo(f"  Recent activity: {recent_records} records (last 7 days)")
        else:
            click.echo(f"❌ Sharp Action Detection Service: {health_result.get('error', 'Unknown issue')}")
    except Exception as e:
        click.echo(f"❌ Sharp Action Detection Service: FAILED - {str(e)}")

    # Check data quality views
    try:
        async with connection.get_async_connection() as conn:
            await conn.fetchval("SELECT COUNT(*) FROM core_betting.data_quality_dashboard")
        click.echo("✅ Data Quality Views: OK")
    except Exception as e:
        click.echo(f"❌ Data Quality Views: FAILED - {str(e)}")

    # Check sportsbook mapping
    try:
        async with connection.get_async_connection() as conn:
            mapping_count = await conn.fetchval(
                "SELECT COUNT(*) FROM core_betting.sportsbook_external_mappings"
            )
        click.echo(f"✅ Sportsbook Mappings: {mapping_count} configured")
    except Exception as e:
        click.echo(f"❌ Sportsbook Mappings: FAILED - {str(e)}")


if __name__ == "__main__":
    data_quality_group()
