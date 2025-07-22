#!/usr/bin/env python3
"""
Main CLI interface for the unified MLB betting system.
"""

import click

from src.core.config import get_settings
from src.data.database.connection import initialize_connections
from src.interfaces.cli.commands.action_network_pipeline import action_network
from src.interfaces.cli.commands.backtesting import backtesting_group
from src.interfaces.cli.commands.batch_collection import batch_collection
from src.interfaces.cli.commands.cleanup import cleanup
from src.interfaces.cli.commands.data import DataCommands
from src.interfaces.cli.commands.data_quality_improvement import data_quality_group
from src.interfaces.cli.commands.game_outcomes import outcomes
from src.interfaces.cli.commands.movement_analysis import movement
from src.interfaces.cli.commands.pipeline import pipeline
from src.interfaces.cli.commands.setup_database import database


@click.group()
@click.version_option()
def cli():
    """
    MLB Sharp Betting - Unified Data Collection and Analysis System

    A comprehensive system for collecting, analyzing, and generating betting insights
    from multiple sportsbooks and data sources.
    """
    # Initialize database connections with settings
    try:
        settings = get_settings()
        initialize_connections(settings)
    except Exception as e:
        # Don't fail CLI startup if database isn't available
        click.echo(f"Warning: Database initialization failed: {e}", err=True)


# Create command instances
data_commands = DataCommands()

# Add command groups
cli.add_command(data_commands.create_group(), name="data")
cli.add_command(batch_collection)  # Add the batch collection commands
cli.add_command(movement)  # Add the new movement analysis commands
cli.add_command(action_network)  # Add the complete Action Network pipeline
cli.add_command(outcomes)  # Add the game outcomes commands
cli.add_command(database)  # Add the database setup and management commands
cli.add_command(backtesting_group)  # Add the backtesting commands
cli.add_command(data_quality_group)  # Add the data quality improvement commands
cli.add_command(pipeline)  # Add the pipeline management commands
cli.add_command(cleanup)  # Add the output folder cleanup command


if __name__ == "__main__":
    cli()
