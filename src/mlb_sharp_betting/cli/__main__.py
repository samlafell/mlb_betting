#!/usr/bin/env python3
"""
Main entry point for the MLB Sharp Betting CLI package.
"""

import sys
from pathlib import Path

# Add the src directory to Python path
src_path = Path(__file__).parent.parent.parent
sys.path.insert(0, str(src_path))

# Import the CLI function from the parent module (not the package)
import mlb_sharp_betting.cli as cli_module

if __name__ == "__main__":
    cli_module.cli() 