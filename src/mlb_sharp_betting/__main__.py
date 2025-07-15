#!/usr/bin/env python3
"""
MLB Sharp Betting Package Entry Point

Allows the package to be run as a module:
    uv run python -m mlb_sharp_betting
    uv run python -m mlb_sharp_betting detect-opportunities
"""

import sys
from pathlib import Path

# Import the CLI from the cli.py file (not the cli package)
if __name__ == "__main__":
    # Import and run the main CLI
    import importlib.util

    cli_path = Path(__file__).parent / "cli.py"
    spec = importlib.util.spec_from_file_location("cli", cli_path)
    cli_module = importlib.util.module_from_spec(spec)
    sys.modules["cli"] = cli_module
    spec.loader.exec_module(cli_module)

    cli_module.cli()
