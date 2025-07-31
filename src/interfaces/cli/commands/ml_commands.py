"""
ML Commands Module
Unified entry point for all ML-related CLI commands
"""

import click
from src.interfaces.cli.commands.ml_training import ml_training_cli


@click.group(name="ml")
def ml():
    """
    Machine Learning pipeline management commands
    
    Provides access to training, evaluation, and model management functionality.
    """
    pass


# Add all ML subcommands to the main ml group
ml.add_command(ml_training_cli, name="training")


# Export the main command group
__all__ = ["ml"]
