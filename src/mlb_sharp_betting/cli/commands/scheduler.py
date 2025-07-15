#!/usr/bin/env python3
"""
MLB Betting Unified Scheduler Daemon

This script runs the consolidated MLB betting analysis scheduler using SchedulerEngine.
It handles:
- Hourly data collection
- Pre-game alerts (5 minutes before each game)
- Backtesting automation
- System monitoring and notifications

Usage:
    uv run -m mlb_sharp_betting.cli.commands.scheduler

The scheduler will:
1. Run your entrypoint every hour
2. Get today's MLB schedule at 6 AM EST
3. Alert you 5 minutes before each game starts
4. Run your entrypoint before each game for fresh analysis
5. Convert betting signals to recommendations saved in tracking.pre_game_recommendations
"""

import asyncio
import signal
import sys
from pathlib import Path

# 🔄 UPDATED: Use new SchedulerEngine instead of deprecated services
from ...services.scheduler_engine import get_scheduler_engine


def main():
    """Main entry point for the unified scheduler daemon."""
    print("🏈 MLB Betting Unified Analysis Scheduler (Phase 4 Engine)")
    print("=" * 60)

    # Initialize scheduler engine
    scheduler_engine = get_scheduler_engine()
    scheduler_engine.project_root = Path(__file__).parent.parent.parent.parent.parent
    scheduler_engine.notifications_enabled = True
    scheduler_engine.alert_minutes = 5

    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        print(f"\n🛑 Received signal {signum}, shutting down gracefully...")
        asyncio.create_task(scheduler_engine.stop())
        print("✅ Scheduler stopped. Goodbye!")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Run the scheduler
    asyncio.run(run_scheduler(scheduler_engine))


async def run_scheduler(scheduler_engine):
    """Run the scheduler with proper async handling."""
    try:
        # Initialize and start scheduler in full mode
        await scheduler_engine.initialize()
        print("✅ SchedulerEngine initialized successfully!")

        await scheduler_engine.start(mode="full")

        print("🚀 Unified Scheduler started successfully!")
        print("\n📋 Full Schedule:")
        print("   🕐 Hourly data collection: Every hour at :00")
        print("   🌅 Daily setup: 6:00 AM EST (gets today's games)")
        print("   ⚾ Game alerts: 5 minutes before each game")
        print("   🎯 Pre-game workflows: Automated signal → recommendation conversion")
        print("   📊 Backtesting: Daily at 2 AM EST, Weekly on Mondays at 6 AM EST")
        print("   💾 Recommendations saved to: tracking.pre_game_recommendations")
        print("\n💡 Notifications will appear in this console")
        print("   You can also integrate with Slack/Discord/email later")
        print("\n🎯 Your betting analysis will run:")
        print("   • Every hour for general updates")
        print("   • 5 minutes before each game for real-time analysis")
        print("   • Signals automatically converted to trackable recommendations")
        print("\nPress Ctrl+C to stop the scheduler...")
        print("=" * 60)

        # Initial status
        status = scheduler_engine.get_status()
        print(f"\n📊 Initial Status: {status['active_jobs']} jobs scheduled")
        print(f"   Modules loaded: {list(status.get('modules_loaded', []))}")

        # Keep the scheduler running with periodic status updates
        while True:
            await asyncio.sleep(300)  # Check every 5 minutes

            # Print periodic status
            status = scheduler_engine.get_status()
            metrics = status.get("metrics", {})

            print("\n📊 Status Update:")
            print(f"   • Active jobs: {status['active_jobs']}")
            print(f"   • Scheduler starts: {metrics.get('scheduler_starts', 0)}")
            print(f"   • Hourly runs completed: {metrics.get('hourly_runs', 0)}")
            print(f"   • Game alerts sent: {metrics.get('game_alerts', 0)}")
            print(f"   • Workflows triggered: {metrics.get('workflows_triggered', 0)}")
            print(
                f"   • Successful workflows: {metrics.get('successful_workflows', 0)}"
            )
            print(f"   • Backtesting runs: {metrics.get('backtesting_runs', 0)}")
            print(f"   • Errors: {metrics.get('errors', 0)}")
            print(f"   • Games scheduled: {len(status.get('scheduled_games', []))}")

            # Show next few scheduled jobs
            jobs = status.get("job_list", [])[:3]  # Show next 3 jobs
            if jobs:
                print("   • Next jobs:")
                for job in jobs:
                    print(
                        f"     - {job.get('name', 'Unknown')}: {job.get('next_run', 'Unknown')}"
                    )

    except KeyboardInterrupt:
        print("\n🛑 Keyboard interrupt received, shutting down...")
        await scheduler_engine.stop()
    except Exception as e:
        print(f"\n❌ Scheduler error: {e}")
        await scheduler_engine.stop()
        raise


if __name__ == "__main__":
    main()
