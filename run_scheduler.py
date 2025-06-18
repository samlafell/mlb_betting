#!/usr/bin/env python3
"""
MLB Betting Scheduler Entry Point

This is the main entry point for the MLB betting scheduler daemon.
It runs the automated betting analysis on a schedule.
"""

if __name__ == "__main__":
    # Run the scheduler through the CLI command
    from src.mlb_sharp_betting.cli.commands.scheduler import main
    main() 