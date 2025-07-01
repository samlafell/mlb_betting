#!/usr/bin/env python3
"""
CLI commands for the pre-game workflow system.

This module provides command-line interface commands for managing the 
three-stage pre-game workflow system that triggers before each MLB game.
Updated to use the new SchedulerEngine from Phase 4 consolidation.
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional

import click
import structlog

# 🔄 UPDATED: Use new SchedulerEngine instead of deprecated PreGameScheduler
from ...services.scheduler_engine import get_scheduler_engine
from ...services.pre_game_workflow import PreGameWorkflowService
from ...services.mlb_api_service import MLBStatsAPIService
from ...core.logging import get_logger

logger = get_logger(__name__)


@click.group(name="pregame")
def pregame_group():
    """Manage the pre-game workflow system using SchedulerEngine."""
    pass


@pregame_group.command("start")
@click.option("--alert-minutes", "-m", default=5, type=int,
              help="Minutes before game start to trigger workflow")
@click.option("--daily-setup-hour", "-h", default=6, type=int,
              help="Hour (EST) to run daily game setup")
@click.option("--project-root", "-r", type=click.Path(exists=True, path_type=Path),
              help="Project root directory (defaults to auto-detection)")
@click.option("--notifications/--no-notifications", default=True,
              help="Enable/disable email notifications")
def start_scheduler(alert_minutes: int, daily_setup_hour: int, project_root: Optional[Path], notifications: bool):
    """Start the pre-game workflow scheduler using SchedulerEngine."""
    click.echo("🏈 Starting MLB Pre-Game Workflow Scheduler (Phase 4 Engine)...")
    click.echo(f"⚙️  Alert Minutes: {alert_minutes}")
    click.echo(f"🕕 Daily Setup Hour: {daily_setup_hour} EST")
    click.echo(f"📧 Notifications: {'ENABLED' if notifications else 'DISABLED'}")
    
    try:
        # 🔄 UPDATED: Use new SchedulerEngine
        scheduler_engine = get_scheduler_engine()
        scheduler_engine.alert_minutes = alert_minutes
        scheduler_engine.daily_setup_hour = daily_setup_hour
        scheduler_engine.notifications_enabled = notifications
        
        if project_root:
            scheduler_engine.project_root = project_root
        
        # Initialize and start in pregame mode
        asyncio.run(_start_pregame_scheduler(scheduler_engine))
        
    except KeyboardInterrupt:
        click.echo("\n👋 Scheduler stopped by user")
    except Exception as e:
        click.echo(f"❌ Scheduler failed: {str(e)}", err=True)
        logger.error("Scheduler startup failed", error=str(e))
        sys.exit(1)


async def _start_pregame_scheduler(scheduler_engine):
    """Helper function to start the scheduler in pregame mode."""
    try:
        await scheduler_engine.initialize()
        click.echo("✅ SchedulerEngine initialized successfully")
        
        # Start in pregame mode
        click.echo("🚀 Starting scheduler in PRE-GAME mode...")
        await scheduler_engine.start(mode="pregame")
        
        # Run forever
        await scheduler_engine.run_forever()
        
    except Exception as e:
        logger.error("Failed to start pregame scheduler", error=str(e))
        raise


@pregame_group.command("status")
@click.option("--project-root", "-r", type=click.Path(exists=True, path_type=Path),
              help="Project root directory")
def status(project_root: Optional[Path]):
    """Show pre-game scheduler status using SchedulerEngine."""
    click.echo("📊 Pre-Game Workflow Status (Phase 4 Engine)")
    click.echo("=" * 40)
    
    try:
        # First, check if the process is actually running by checking PID file
        # This is the real indicator of whether the scheduler is running
        project_root = project_root or Path.cwd()
        pid_file = project_root / "pregame_scheduler.pid"
        
        process_running = False
        pid = None
        
        if pid_file.exists():
            try:
                with open(pid_file, 'r') as f:
                    pid = int(f.read().strip())
                
                # Check if process is actually running
                import os
                import signal
                try:
                    os.kill(pid, 0)  # This doesn't actually kill, just checks if process exists
                    process_running = True
                except (OSError, ProcessLookupError):
                    # Process doesn't exist, remove stale PID file
                    pid_file.unlink(missing_ok=True)
                    process_running = False
            except (ValueError, IOError):
                process_running = False
        
        # Display process status first (most important)
        click.echo(f"Running: {'✅ Yes' if process_running else '❌ No'}")
        if process_running and pid:
            click.echo(f"Process ID: {pid}")
        
        # Only try to get detailed status if we have a way to connect to the running instance
        # For now, we'll show basic info from a new instance but clearly indicate the limitation
        scheduler_engine = get_scheduler_engine()
        if project_root:
            scheduler_engine.project_root = project_root
        
        # Get status from a fresh instance (note: this won't show running jobs from the actual running process)
        status_info = scheduler_engine.get_status()
        
        click.echo(f"Mode: pregame")
        click.echo(f"Notifications: {'✅ Enabled' if scheduler_engine.notifications_enabled else '❌ Disabled'}")
        
        if process_running:
            click.echo(f"Alert Minutes: {scheduler_engine.alert_minutes}")
            click.echo(f"Daily Setup Hour: {scheduler_engine.daily_setup_hour} EST")
            
            # Show log file info
            log_file = project_root / "pregame_scheduler.log"
            if log_file.exists():
                click.echo(f"Log File: {log_file}")
                try:
                    import time
                    mtime = log_file.stat().st_mtime
                    last_modified = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(mtime))
                    click.echo(f"Last Log Update: {last_modified}")
                except Exception:
                    pass
            
            click.echo("\n💡 Note: The scheduler is running in a separate process.")
            click.echo("   To see detailed job status, check the log file:")
            click.echo(f"   tail -f {project_root}/pregame_scheduler.log")
        else:
            click.echo("\n🔧 To start the scheduler, run:")
            click.echo("   ./start_pregame_scheduler.sh")
        
    except Exception as e:
        click.echo(f"❌ Failed to get status: {str(e)}", err=True)
        logger.error("Failed to get scheduler status", error=str(e))
        sys.exit(1)


@pregame_group.command("test-workflow")
@click.option("--game-pk", type=int, help="Specific game ID to test (optional)")
@click.option("--project-root", "-r", type=click.Path(exists=True, path_type=Path),
              help="Project root directory")
def test_workflow(game_pk: Optional[int], project_root: Optional[Path]):
    """Test the pre-game workflow with a specific game or today's first game."""
    
    async def run_test():
        click.echo("🧪 Testing Pre-Game Workflow (Phase 4 Engine)...")
        
        try:
            # 🔄 UPDATED: Use SchedulerEngine's pregame module
            scheduler_engine = get_scheduler_engine()
            if project_root:
                scheduler_engine.project_root = project_root
            
            await scheduler_engine.initialize()
            
            # Use the workflow service directly (still exists)
            workflow_service = PreGameWorkflowService(project_root=project_root)
            mlb_api = MLBStatsAPIService()
            
            # Get game to test with
            if game_pk:
                # Find specific game by ID
                from datetime import datetime, timedelta
                today = datetime.now().date()
                yesterday = today - timedelta(days=1)
                tomorrow = today + timedelta(days=1)
                
                game = None
                for date in [yesterday, today, tomorrow]:
                    games = mlb_api.get_games_for_date(date)
                    game = next((g for g in games if g.game_pk == game_pk), None)
                    if game:
                        break
                
                if not game:
                    click.echo(f"❌ Game with ID {game_pk} not found", err=True)
                    return
            else:
                # Use today's first game
                from datetime import datetime
                today = datetime.now().date()
                games = mlb_api.get_games_for_date(today)
                
                if not games:
                    click.echo("❌ No games found for today", err=True)
                    return
                
                game = games[0]
            
            game_desc = f"{game.away_team} @ {game.home_team}"
            
            # Convert to EST for display
            import pytz
            est = pytz.timezone('US/Eastern')
            if game.game_date.tzinfo is None:
                utc = pytz.timezone('UTC')
                game_time_utc = utc.localize(game.game_date)
            else:
                game_time_utc = game.game_date
            game_time_est = game_time_utc.astimezone(est)
            
            click.echo(f"🎯 Testing with: {game_desc}")
            click.echo(f"📅 Game Time: {game_time_est.strftime('%I:%M %p EST on %B %d, %Y')}")
            
            # Execute workflow
            result = await workflow_service.execute_pre_game_workflow(game)
            
            # Display results
            click.echo(f"\n📋 Workflow Results:")
            click.echo(f"Status: {'✅ Success' if result.overall_status.value == 'success' else '❌ Failed'}")
            click.echo(f"Total Time: {result.total_execution_time:.1f}s")
            click.echo(f"Email Sent: {'✅ Yes' if result.email_sent else '❌ No'}")
            
            click.echo(f"\n🔄 Stage Results:")
            for stage_name, stage_result in result.stages.items():
                status_emoji = {"success": "✅", "failed": "❌", "skipped": "⏭️"}.get(stage_result.status.value, "❓")
                stage_display = stage_name.value.replace('_', ' ').title()
                click.echo(f"  {status_emoji} {stage_display}: {stage_result.status.value}")
                if stage_result.error_message:
                    click.echo(f"    Error: {stage_result.error_message}")
            
        except Exception as e:
            click.echo(f"❌ Test failed: {str(e)}", err=True)
            logger.error("Workflow test failed", error=str(e))
    
    asyncio.run(run_test())


@pregame_group.command("start-full")
@click.option("--project-root", "-r", type=click.Path(exists=True, path_type=Path),
              help="Project root directory")
@click.option("--notifications/--no-notifications", default=True,
              help="Enable/disable notifications")
def start_full_scheduler(project_root: Optional[Path], notifications: bool):
    """Start the full scheduler (all modes: core, pregame, backtesting)."""
    click.echo("🚀 Starting FULL SchedulerEngine (All Modes)")
    click.echo("=" * 50)
    click.echo("📡 Core MLB scheduling")
    click.echo("🏈 Pre-game workflows")  
    click.echo("🔬 Backtesting automation")
    click.echo(f"📧 Notifications: {'ENABLED' if notifications else 'DISABLED'}")
    
    try:
        # 🔄 UPDATED: Use new SchedulerEngine in full mode
        scheduler_engine = get_scheduler_engine()
        scheduler_engine.notifications_enabled = notifications
        
        if project_root:
            scheduler_engine.project_root = project_root
        
        # Start in full mode
        asyncio.run(_start_full_scheduler(scheduler_engine))
        
    except KeyboardInterrupt:
        click.echo("\n👋 Full scheduler stopped by user")
    except Exception as e:
        click.echo(f"❌ Full scheduler failed: {str(e)}", err=True)
        logger.error("Full scheduler startup failed", error=str(e))
        sys.exit(1)


async def _start_full_scheduler(scheduler_engine):
    """Helper function to start the scheduler in full mode."""
    try:
        await scheduler_engine.initialize()
        click.echo("✅ SchedulerEngine initialized successfully")
        
        # Start in full mode (all modules)
        click.echo("🚀 Starting scheduler in FULL mode...")
        await scheduler_engine.start(mode="full")
        
        # Display startup summary
        status = scheduler_engine.get_status()
        click.echo(f"\n📊 Scheduler Status:")
        click.echo(f"   Running: {'✅ Yes' if status.get('running', False) else '❌ No'}")
        click.echo(f"   Mode: {status.get('mode', 'Unknown')}")
        click.echo(f"   Active Jobs: {status.get('active_jobs', 0)}")
        
        # Run forever
        await scheduler_engine.run_forever()
        
    except Exception as e:
        logger.error("Failed to start full scheduler", error=str(e))
        raise


@pregame_group.command("configure-email")
@click.option("--from-email", prompt="Gmail address", help="Gmail address to send from")
@click.option("--app-password", prompt="Gmail app password", hide_input=True,
              help="Gmail app password (not your regular password)")
@click.option("--to-emails", prompt="Recipient emails (comma-separated)", 
              help="Comma-separated list of recipient email addresses")
def configure_email(from_email: str, app_password: str, to_emails: str):
    """Configure email settings for notifications."""
    click.echo("📧 Configuring Email Settings...")
    
    # Parse recipient emails
    recipients = [email.strip() for email in to_emails.split(',')]
    
    # Format recipients as comma-separated string
    recipients_str = ','.join(recipients)
    
    # Create environment variable instructions
    env_vars = f"""
# Add these environment variables to your shell profile (~/.bashrc, ~/.zshrc, etc.)
export EMAIL_FROM_ADDRESS="{from_email}"
export EMAIL_APP_PASSWORD="{app_password}"
export EMAIL_TO_ADDRESSES="{recipients_str}"

# Or create a .env file in your project root:
echo 'EMAIL_FROM_ADDRESS={from_email}' >> .env
echo 'EMAIL_APP_PASSWORD={app_password}' >> .env
echo 'EMAIL_TO_ADDRESSES={recipients_str}' >> .env
"""
    
    click.echo("✅ Email configuration generated!")
    click.echo("\n📝 Environment Variables:")
    click.echo(env_vars)
    
    click.echo("\n🔧 Gmail Setup Instructions:")
    click.echo("1. Enable 2-factor authentication on your Gmail account")
    click.echo("2. Generate an App Password at: https://myaccount.google.com/apppasswords")
    click.echo("3. Use the generated App Password (not your regular password)")
    click.echo("4. Set the environment variables shown above")
    
    click.echo("\n⚠️  Security Note:")
    click.echo("Store these credentials securely and never commit them to version control!")


@pregame_group.command("list-games")
@click.option("--date", "-d", help="Date to list games for (YYYY-MM-DD, defaults to today)")
def list_games(date: Optional[str]):
    """List MLB games for a specific date."""
    try:
        from datetime import datetime
        
        if date:
            game_date = datetime.strptime(date, "%Y-%m-%d").date()
        else:
            game_date = datetime.now().date()
        
        mlb_api = MLBStatsAPIService()
        games = mlb_api.get_games_for_date(game_date)
        
        click.echo(f"🏈 MLB Games for {game_date.strftime('%Y-%m-%d')}")
        click.echo("=" * 50)
        
        if not games:
            click.echo("No games found for this date.")
            return
        
        for i, game in enumerate(games, 1):
            # Convert to EST for display
            import pytz
            est = pytz.timezone('US/Eastern')
            
            if game.game_date.tzinfo is None:
                # Assume UTC if no timezone info
                utc = pytz.timezone('UTC')
                game_time_utc = utc.localize(game.game_date)
            else:
                game_time_utc = game.game_date
            
            game_time_est = game_time_utc.astimezone(est)
            game_time_display = game_time_est.strftime("%I:%M %p EST")
            
            click.echo(f"{i:2d}. {game.away_team} @ {game.home_team}")
            click.echo(f"    Time: {game_time_display} | Status: {game.status}")
            click.echo(f"    Game ID: {game.game_pk}")
            if game.venue:
                click.echo(f"    Venue: {game.venue}")
            click.echo()
        
    except ValueError:
        click.echo("❌ Invalid date format. Use YYYY-MM-DD", err=True)
    except Exception as e:
        click.echo(f"❌ Failed to list games: {str(e)}", err=True)


def main():
    """Main CLI entry point for pre-game commands."""
    pregame_group()


if __name__ == "__main__":
    main() 