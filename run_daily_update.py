#!/usr/bin/env python3
"""
Simple wrapper to run the daily MLB game updater.

Usage:
    uv run run_daily_update.py
    
This script can be run daily (via cron job or manually) to:
- Update yesterday's completed games
- Update today's completed games (if any finished early)
- Only process games that have real betting lines data
"""

import subprocess
import sys

def main():
    """Run the daily game updater."""
    try:
        print("🚀 Starting daily MLB game update...")
        result = subprocess.run([
            sys.executable, "test_game_updater.py"
        ], check=True)
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