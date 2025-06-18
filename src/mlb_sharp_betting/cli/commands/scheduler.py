#!/usr/bin/env python3
"""
MLB Betting Scheduler Daemon

This script runs the automated MLB betting analysis scheduler.
It handles:
- Hourly data collection
- Pre-game alerts (5 minutes before each game)
- System monitoring and notifications

Usage:
    uv run -m mlb_sharp_betting.cli.commands.scheduler

The scheduler will:
1. Run your entrypoint every hour
2. Get today's MLB schedule at 6 AM EST
3. Alert you 5 minutes before each game starts
4. Run your entrypoint before each game for fresh analysis
"""

import asyncio
import sys
import signal
from pathlib import Path

from ...services.scheduler import MLBBettingScheduler


def main():
    """Main entry point for the scheduler daemon."""
    print("🏈 MLB Betting Analysis Scheduler")
    print("=" * 50)
    
    # Initialize scheduler
    scheduler = MLBBettingScheduler(
        project_root=Path(__file__).parent.parent.parent.parent.parent,
        notifications_enabled=True,
        alert_minutes_before_game=5
    )
    
    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        print(f"\n🛑 Received signal {signum}, shutting down gracefully...")
        scheduler.stop()
        print("✅ Scheduler stopped. Goodbye!")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run the scheduler
    asyncio.run(run_scheduler(scheduler))


async def run_scheduler(scheduler: MLBBettingScheduler):
    """Run the scheduler with proper async handling."""
    try:
        # Start scheduler
        scheduler.start()
        
        print("🚀 Scheduler started successfully!")
        print("\n📋 Schedule:")
        print("   🕐 Hourly data collection: Every hour at :00")
        print("   🌅 Daily setup: 6:00 AM EST (gets today's games)")
        print("   ⚾ Game alerts: 5 minutes before each game")
        print("\n💡 Notifications will appear in this console")
        print("   You can also integrate with Slack/Discord/email later")
        print("\n🎯 Your betting analysis will run:")
        print("   • Every hour for general updates")
        print("   • 5 minutes before each game for real-time analysis")
        print("\nPress Ctrl+C to stop the scheduler...")
        print("=" * 50)
        
        # Initial status
        status = scheduler.get_status()
        print(f"\n📊 Initial Status: {status['jobs_count']} jobs scheduled")
        
        # Keep the scheduler running
        while True:
            await asyncio.sleep(300)  # Check every 5 minutes
            
            # Print periodic status
            status = scheduler.get_status()
            current_time = asyncio.get_event_loop().time()
            
            print(f"\n📊 Status Update:")
            print(f"   • Jobs active: {status['jobs_count']}")
            print(f"   • Hourly runs completed: {status['metrics']['hourly_runs']}")
            print(f"   • Game alerts sent: {status['metrics']['game_alerts']}")
            print(f"   • Errors: {status['metrics']['errors']}")
            print(f"   • Games scheduled today: {status['metrics']['scheduled_games_today']}")
            
            # Show next few scheduled jobs
            jobs = status['jobs'][:3]  # Show next 3 jobs
            if jobs:
                print("   • Next jobs:")
                for job in jobs:
                    print(f"     - {job['name']}: {job['next_run']}")
                
    except KeyboardInterrupt:
        print("\n🛑 Keyboard interrupt received, shutting down...")
        scheduler.stop()
    except Exception as e:
        print(f"\n❌ Scheduler error: {e}")
        scheduler.stop()
        raise


if __name__ == "__main__":
    main() 