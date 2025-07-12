#!/usr/bin/env python3
"""
Main CLI interface for the unified MLB betting system.
"""
import click

from src.interfaces.cli.commands.data import DataCommands
from src.interfaces.cli.commands.movement_analysis import movement
from src.interfaces.cli.commands.action_network_pipeline import action_network
from src.interfaces.cli.commands.game_outcomes import outcomes


@click.group()
@click.version_option()
def cli():
    """
    MLB Sharp Betting - Unified Data Collection and Analysis System
    
    A comprehensive system for collecting, analyzing, and generating betting insights
    from multiple sportsbooks and data sources.
    """
    pass


# Create command instances
data_commands = DataCommands()

# Add command groups
cli.add_command(data_commands.create_group(), name='data')
cli.add_command(movement)  # Add the new movement analysis commands
cli.add_command(action_network)  # Add the complete Action Network pipeline
cli.add_command(outcomes)  # Add the game outcomes commands


if __name__ == '__main__':
    cli() 