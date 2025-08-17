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
from src.interfaces.cli.commands.collection_health import health
from src.interfaces.cli.commands.curated import curated
from src.interfaces.cli.commands.data import DataCommands
from src.interfaces.cli.commands.data_quality_improvement import data_quality_group
from src.interfaces.cli.commands.game_outcomes import outcomes
from src.interfaces.cli.commands.ml_commands import ml
from src.interfaces.cli.commands.monitoring import MonitoringCommands
from src.interfaces.cli.commands.movement_analysis import movement
from src.interfaces.cli.commands.pipeline import pipeline
from src.interfaces.cli.commands.predictions import create_predictions_commands
from src.interfaces.cli.commands.quickstart import quickstart
from src.interfaces.cli.commands.setup_database import database

# Old staging commands removed - consolidated into historical approach


@click.group()
@click.version_option()
def cli():
    """
    🎯 MLB Betting System - AI-Powered Sports Betting Analysis
    
    Generate profitable betting predictions using machine learning and sharp action detection.
    
    🚀 Quick Start (New Users):
        uv run -m src.interfaces.cli quickstart setup
        
    🎯 Get Today's Predictions:
        uv run -m src.interfaces.cli predictions today
        
    ⚡ Run Complete Pipeline:
        uv run -m src.interfaces.cli pipeline run-full --generate-predictions
        
    📊 View Model Performance:
        uv run -m src.interfaces.cli ml models --profitable-only
        
    📈 Start Monitoring Dashboard:
        uv run -m src.interfaces.cli monitoring dashboard
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
monitoring_commands = MonitoringCommands()
predictions_commands = create_predictions_commands()

# Add command groups
cli.add_command(quickstart)  # Add quickstart commands first for new users
cli.add_command(predictions_commands, name="predictions")  # Add predictions commands  
cli.add_command(data_commands.create_group(), name="data")
cli.add_command(
    monitoring_commands.create_group(), name="monitoring"
)  # Add monitoring commands
cli.add_command(health)  # Add the collection health monitoring commands
cli.add_command(batch_collection)  # Add the batch collection commands
cli.add_command(movement)  # Add the new movement analysis commands
cli.add_command(action_network)  # Add the complete Action Network pipeline
cli.add_command(outcomes)  # Add the game outcomes commands
cli.add_command(database)  # Add the database setup and management commands
cli.add_command(backtesting_group)  # Add the backtesting commands
cli.add_command(data_quality_group)  # Add the data quality improvement commands
cli.add_command(pipeline)  # Add the pipeline management commands
cli.add_command(curated)  # Add the CURATED zone management commands
# Old staging commands removed - use historical approach via action-network pipeline
cli.add_command(cleanup)  # Add the output folder cleanup command
cli.add_command(ml)  # Add the ML experiment management commands


if __name__ == "__main__":
    cli()
