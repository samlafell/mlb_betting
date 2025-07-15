#!/usr/bin/env python3
"""
Unified CLI Entry Point

This is the main entry point for the unified MLB betting analytics CLI.
It allows running the CLI using: python -m src.interfaces.cli

Phase 4 Migration: Interface & Service Consolidation
- Single entry point for all CLI operations
- Replaces multiple scattered CLI entry points
- Modern async patterns with proper error handling
"""

import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from src.interfaces.cli.main import cli


def main():
    """Main entry point for the unified CLI."""
    try:
        # Run the Click CLI
        cli()
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
        sys.exit(0)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
