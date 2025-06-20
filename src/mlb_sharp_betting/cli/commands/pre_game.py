#!/usr/bin/env python3
"""
CLI commands for the pre-game workflow system.

This module provides command-line interface commands for managing the 
three-stage pre-game workflow system that triggers before each MLB game.
"""

import asyncio
import sys
from pathlib import Path
from typing import Optional

import click
import structlog

from ...services.pre_game_scheduler import PreGameScheduler
from ...services.pre_game_workflow import PreGameWorkflowService
from ...services.mlb_api_service import MLBStatsAPIService
from ...core.logging import get_logger

logger = get_logger(__name__)


@click.group(name="pregame")
def pregame_group():
    """Manage the pre-game workflow system."""
    pass


@pregame_group.command("start")
@click.option("--alert-minutes", "-m", default=5, type=int,
              help="Minutes before game start to trigger workflow")
@click.option("--daily-setup-hour", "-h", default=6, type=int,
              help="Hour (EST) to run daily game setup")
@click.option("--project-root", "-r", type=click.Path(exists=True, path_type=Path),
              help="Project root directory (defaults to auto-detection)")
def start_scheduler(alert_minutes: int, daily_setup_hour: int, project_root: Optional[Path]):
    """Start the pre-game workflow scheduler."""
    click.echo("🏈 Starting MLB Pre-Game Workflow Scheduler...")
    
    try:
        scheduler = PreGameScheduler(
            project_root=project_root,
            alert_minutes_before_game=alert_minutes,
            daily_setup_hour=daily_setup_hour
        )
        
        # Run the scheduler
        asyncio.run(scheduler.run_forever())
        
    except KeyboardInterrupt:
        click.echo("\n👋 Scheduler stopped by user")
    except Exception as e:
        click.echo(f"❌ Scheduler failed: {str(e)}", err=True)
        logger.error("Scheduler startup failed", error=str(e))
        sys.exit(1)


@pregame_group.command("status")
@click.option("--project-root", "-r", type=click.Path(exists=True, path_type=Path),
              help="Project root directory")
def status(project_root: Optional[Path]):
    """Show pre-game scheduler status and recent workflows."""
    click.echo("📊 Pre-Game Workflow Status")
    click.echo("=" * 40)
    
    try:
        # Create a scheduler instance to check status
        scheduler = PreGameScheduler(project_root=project_root)
        status_info = scheduler.get_status()
        
        click.echo(f"Running: {'✅ Yes' if status_info['running'] else '❌ No'}")
        click.echo(f"Email Configured: {'✅ Yes' if status_info['email_configured'] else '❌ No'}")
        click.echo(f"Scheduled Games Today: {status_info['scheduled_games_today']}")
        click.echo(f"Active Workflow Jobs: {status_info['active_workflow_jobs']}")
        click.echo(f"Completed Workflows: {status_info['completed_workflows']}")
        
        click.echo("\n📈 Metrics:")
        for key, value in status_info['metrics'].items():
            formatted_key = key.replace('_', ' ').title()
            click.echo(f"  {formatted_key}: {value}")
        
        # Show recent workflows
        recent_workflows = scheduler.get_recent_workflows(limit=5)
        if recent_workflows:
            click.echo("\n🕒 Recent Workflows:")
            for workflow in recent_workflows:
                status_emoji = "✅" if workflow.overall_status.value == "success" else "❌"
                game_desc = f"{workflow.game.away_team} @ {workflow.game.home_team}"
                time_str = workflow.start_time.strftime("%Y-%m-%d %H:%M")
                click.echo(f"  {status_emoji} {game_desc} - {time_str}")
        
    except Exception as e:
        click.echo(f"❌ Failed to get status: {str(e)}", err=True)
        sys.exit(1)


@pregame_group.command("test-workflow")
@click.option("--game-pk", type=int, help="Specific game ID to test (optional)")
@click.option("--project-root", "-r", type=click.Path(exists=True, path_type=Path),
              help="Project root directory")
def test_workflow(game_pk: Optional[int], project_root: Optional[Path]):
    """Test the pre-game workflow with a specific game or today's first game."""
    
    async def run_test():
        click.echo("🧪 Testing Pre-Game Workflow...")
        
        try:
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