#!/usr/bin/env python3
"""
Simple wrapper to run the daily MLB game updater.

Usage:
    uv run -m mlb_sharp_betting.cli.commands.daily_update
    
This script can be run daily (via cron job or manually) to:
- Update yesterday's completed games
- Update today's completed games (if any finished early)
- Only process games that have real betting lines data
"""

import subprocess
import sys
from pathlib import Path

def main():
    """Run the daily game updater."""
    try:
        print("🚀 Starting daily MLB game update...")
        
        # Get the root directory 
        root_dir = Path(__file__).parent.parent.parent.parent.parent
        test_script = root_dir / "tests" / "integration" / "test_game_updater.py"
        
        result = subprocess.run([
            sys.executable, str(test_script)
        ], check=True, cwd=root_dir)
        print("✅ Daily update completed successfully!")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"❌ Daily update failed with exit code {e.returncode}")
        return e.returncode
    except Exception as e:
        print(f"❌ Failed to run daily update: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 