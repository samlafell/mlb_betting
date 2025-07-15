#!/usr/bin/env python3
"""
Main entry point for the MLB Sharp Betting CLI package.
"""

import sys
from pathlib import Path

# Add the src directory to Python path
src_path = Path(__file__).parent.parent.parent
sys.path.insert(0, str(src_path))

# Import and run the CLI directly from the main module
if __name__ == "__main__":
    # Import the CLI module and run it
    import importlib.util

    cli_path = Path(__file__).parent.parent / "cli.py"
    spec = importlib.util.spec_from_file_location("cli", cli_path)
    cli_module = importlib.util.module_from_spec(spec)
    sys.modules["cli"] = cli_module
    spec.loader.exec_module(cli_module)

    cli_module.cli()
