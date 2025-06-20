#!/usr/bin/env python3
"""
Pre-Game Workflow Scheduler

A dedicated scheduler service for triggering the three-stage pre-game workflow
5 minutes before each MLB game. This is separate from the existing scheduler
system to avoid conflicts and provide specialized pre-game functionality.

Features:
- Automatic game detection and scheduling
- Pre-game workflow execution 5 minutes before game time
- Email notifications for all workflow results
- Integration with existing MLB API service
- Comprehensive error handling and logging
- Timezone-aware scheduling (EST/UTC)
- Workflow status tracking and metrics
"""

import asyncio
import signal
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set
import structlog
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.jobstores.memory import MemoryJobStore

from .mlb_api_service import MLBStatsAPIService, MLBGameInfo
from .pre_game_workflow import PreGameWorkflowService, WorkflowResult
from ..core.logging import get_logger

logger = get_logger(__name__)


class PreGameScheduler:
    """Dedicated scheduler for pre-game workflow automation."""
    
    def __init__(self, 
                 project_root: Optional[Path] = None,
                 alert_minutes_before_game: int = 5,
                 daily_setup_hour: int = 6):
        """
        Initialize the pre-game scheduler.
        
        Args:
            project_root: Path to project root directory
            alert_minutes_before_game: Minutes before game start to trigger workflow
            daily_setup_hour: Hour (EST) to run daily game setup
        """
        self.project_root = project_root or Path(__file__).parent.parent.parent.parent
        self.alert_minutes = alert_minutes_before_game
        self.daily_setup_hour = daily_setup_hour
        
        # Services
        self.mlb_api = MLBStatsAPIService()
        self.workflow_service = PreGameWorkflowService(project_root=self.project_root)
        
        # Timezone setup
        self.est = pytz.timezone('US/Eastern')
        self.utc = pytz.timezone('UTC')
        
        # Configure scheduler
        jobstores = {
            'default': MemoryJobStore()
        }
        executors = {
            'default': AsyncIOExecutor()
        }
        job_defaults = {
            'coalesce': False,
            'max_instances': 1,
            'misfire_grace_time': 300  # 5 minutes grace period
        }
        
        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone='UTC'
        )
        
        # Tracking
        self.scheduled_games: Set[int] = set()  # Track game_pk values
        self.completed_workflows: Dict[int, WorkflowResult] = {}
        self.running = False
        
        # Metrics
        self.metrics = {
            "scheduler_starts": 0,
            "daily_setups": 0,
            "games_scheduled": 0,
            "workflows_triggered": 0,
            "successful_workflows": 0,
            "failed_workflows": 0,
            "scheduler_errors": 0
        }
        
        self.logger = logger.bind(service="pre_game_scheduler")
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info("Received shutdown signal", signal=signum)
        if self.running:
            asyncio.create_task(self.stop())
        else:
            sys.exit(0)
    
    async def start(self) -> None:
        """Start the pre-game scheduler."""
        self.logger.info("Starting Pre-Game Workflow Scheduler")
        
        try:
            # Schedule daily setup job
            self.scheduler.add_job(
                func=self._daily_setup_handler,
                trigger=CronTrigger(
                    hour=self.daily_setup_hour,
                    minute=0,
                    timezone=self.est
                ),
                id='daily_game_setup',
                name='Daily Game Setup',
                replace_existing=True
            )
            
            # Start the scheduler
            self.scheduler.start()
            self.running = True
            
            # Run initial setup
            await self._daily_setup_handler()
            
            self.metrics["scheduler_starts"] += 1
            
            self.logger.info("Pre-Game Scheduler started successfully",
                           alert_minutes=self.alert_minutes,
                           daily_setup_hour=self.daily_setup_hour,
                           email_configured=self.workflow_service.email_config.is_configured())
            
            # Display startup summary
            self._display_startup_summary()
            
        except Exception as e:
            self.logger.error("Failed to start scheduler", error=str(e))
            self.metrics["scheduler_errors"] += 1
            raise
    
    def _display_startup_summary(self):
        """Display startup summary information."""
        print("\n" + "=" * 60)
        print("🏈 MLB PRE-GAME WORKFLOW SCHEDULER")
        print("=" * 60)
        print(f"⏰ Workflow Trigger: {self.alert_minutes} minutes before each game")
        print(f"🌅 Daily Setup: {self.daily_setup_hour:02d}:00 EST")
        print(f"📧 Email Configured: {'✅ Yes' if self.workflow_service.email_config.is_configured() else '❌ No'}")
        
        if not self.workflow_service.email_config.is_configured():
            print("\n⚠️  EMAIL SETUP REQUIRED:")
            print("   Set environment variables:")
            print("   - EMAIL_FROM_ADDRESS=your-email@gmail.com")
            print("   - EMAIL_APP_PASSWORD=your-gmail-app-password")
            print("   - EMAIL_TO_ADDRESSES=['recipient@gmail.com']")
        
        print(f"\n📊 Scheduled Games Today: {len(self.scheduled_games)}")
        print("💡 Notifications will appear in this console")
        print("\nPress Ctrl+C to stop the scheduler...")
        print("=" * 60 + "\n")
    
    async def stop(self) -> None:
        """Stop the scheduler gracefully."""
        self.logger.info("Stopping Pre-Game Scheduler")
        
        try:
            if self.scheduler.running:
                self.scheduler.shutdown(wait=True)
            
            self.running = False
            
            # Display final metrics
            self._display_final_metrics()
            
            self.logger.info("Pre-Game Scheduler stopped successfully")
            
        except Exception as e:
            self.logger.error("Error during scheduler shutdown", error=str(e))
    
    def _display_final_metrics(self):
        """Display final metrics on shutdown."""
        print("\n" + "=" * 50)
        print("📊 FINAL SCHEDULER METRICS")
        print("=" * 50)
        for key, value in self.metrics.items():
            print(f"{key.replace('_', ' ').title()}: {value}")
        
        workflow_metrics = self.workflow_service.get_metrics()
        print("\n📈 WORKFLOW METRICS:")
        for key, value in workflow_metrics.items():
            print(f"{key.replace('_', ' ').title()}: {value}")
        print("=" * 50)
    
    async def _daily_setup_handler(self) -> None:
        """Daily setup: schedule workflows for all games today."""
        self.logger.info("Running daily game setup")
        
        try:
            today = datetime.now(self.utc).date()
            games = self.mlb_api.get_games_for_date(today)
            
            scheduled_count = 0
            
            for game in games:
                # Skip completed, postponed, or cancelled games
                if game.status in ['Final', 'Completed', 'Postponed', 'Cancelled']:
                    continue
                
                # Skip if already scheduled
                if game.game_pk in self.scheduled_games:
                    continue
                
                # Calculate workflow trigger time (5 minutes before game)
                if game.game_date.tzinfo is None:
                    game_time_utc = self.utc.localize(game.game_date)
                else:
                    game_time_utc = game.game_date.astimezone(self.utc)
                
                trigger_time = game_time_utc - timedelta(minutes=self.alert_minutes)
                
                # Only schedule if trigger time is in the future
                now_utc = datetime.now(self.utc)
                if trigger_time > now_utc:
                    job_id = f"pregame_workflow_{game.game_pk}"
                    
                    # Remove existing job if it exists
                    if self.scheduler.get_job(job_id):
                        self.scheduler.remove_job(job_id)
                    
                    # Schedule the workflow
                    self.scheduler.add_job(
                        func=self._game_workflow_handler,
                        trigger=DateTrigger(run_date=trigger_time),
                        args=[game],
                        id=job_id,
                        name=f"Pre-Game: {game.away_team} @ {game.home_team}",
                        replace_existing=True
                    )
                    
                    self.scheduled_games.add(game.game_pk)
                    scheduled_count += 1
                    
                    # Convert to EST for logging
                    trigger_time_est = trigger_time.astimezone(self.est)
                    game_time_est = game_time_utc.astimezone(self.est)
                    
                    self.logger.info("Scheduled pre-game workflow",
                                   game=f"{game.away_team} @ {game.home_team}",
                                   trigger_time=trigger_time_est.strftime("%I:%M %p EST"),
                                   game_time=game_time_est.strftime("%I:%M %p EST"))
            
            self.metrics["daily_setups"] += 1
            self.metrics["games_scheduled"] += scheduled_count
            
            self.logger.info("Daily setup completed",
                           games_found=len(games),
                           games_scheduled=scheduled_count,
                           total_scheduled_today=len(self.scheduled_games))
            
            # Send daily setup notification
            await self._send_daily_setup_notification(len(games), scheduled_count)
            
        except Exception as e:
            self.logger.error("Daily setup failed", error=str(e))
            self.metrics["scheduler_errors"] += 1
            await self._send_error_notification("Daily Setup Failed", str(e))
    
    async def _game_workflow_handler(self, game: MLBGameInfo) -> None:
        """Handle pre-game workflow execution for a specific game."""
        game_desc = f"{game.away_team} @ {game.home_team}"
        
        # Convert game time to EST for display
        if game.game_date.tzinfo is None:
            game_time_est = self.est.localize(game.game_date)
        else:
            game_time_est = game.game_date.astimezone(self.est)
        
        self.logger.info("Triggering pre-game workflow",
                        game=game_desc,
                        game_time=game_time_est.strftime("%I:%M %p EST"))
        
        try:
            # Execute the three-stage workflow
            workflow_result = await self.workflow_service.execute_pre_game_workflow(game)
            
            # Store result
            self.completed_workflows[game.game_pk] = workflow_result
            
            # Update metrics
            self.metrics["workflows_triggered"] += 1
            if workflow_result.overall_status.value == "success":
                self.metrics["successful_workflows"] += 1
            else:
                self.metrics["failed_workflows"] += 1
            
            # Log result
            self.logger.info("Pre-game workflow completed",
                           game=game_desc,
                           workflow_id=workflow_result.workflow_id,
                           status=workflow_result.overall_status.value,
                           email_sent=workflow_result.email_sent,
                           total_time=workflow_result.total_execution_time)
            
            # Console notification
            status_emoji = "✅" if workflow_result.overall_status.value == "success" else "❌"
            email_status = "📧 Sent" if workflow_result.email_sent else "📧 Failed"
            
            print(f"\n{status_emoji} Pre-Game Workflow: {game_desc}")
            print(f"   Time: {datetime.now().strftime('%H:%M:%S')} | Status: {workflow_result.overall_status.value.title()}")
            print(f"   Email: {email_status} | Duration: {workflow_result.total_execution_time:.1f}s")
            print(f"   Workflow ID: {workflow_result.workflow_id}")
            
        except Exception as e:
            self.logger.error("Pre-game workflow failed unexpectedly",
                            game=game_desc,
                            error=str(e))
            self.metrics["failed_workflows"] += 1
            self.metrics["scheduler_errors"] += 1
            
            # Send error notification
            await self._send_error_notification(
                f"Pre-Game Workflow Failed: {game_desc}",
                f"Unexpected error: {str(e)}"
            )
        
        finally:
            # Clean up from scheduled games tracking
            if game.game_pk in self.scheduled_games:
                self.scheduled_games.remove(game.game_pk)
    
    async def _send_daily_setup_notification(self, total_games: int, scheduled_games: int):
        """Send daily setup notification if email is configured."""
        if not self.workflow_service.email_config.is_configured():
            return
        
        try:
            today_str = datetime.now(self.est).strftime("%A, %B %d, %Y")
            
            subject = f"📅 Daily MLB Setup Complete - {scheduled_games} Games Scheduled"
            
            plain_text = f"""🏈 DAILY MLB SETUP COMPLETE

Date: {today_str}
Total Games Found: {total_games}
Pre-Game Workflows Scheduled: {scheduled_games}
Alert Time: {self.alert_minutes} minutes before each game

{scheduled_games} workflows will automatically trigger before today's games.
You'll receive analysis results via email for each game.

---
Generated by MLB Sharp Betting Analytics Platform
General Balls"""
            
            html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
    <div style="background: #d4edda; color: #155724; padding: 20px; border-radius: 8px;">
        <h1>📅 Daily MLB Setup Complete</h1>
        <p><strong>Date:</strong> {today_str}</p>
        <p><strong>Total Games Found:</strong> {total_games}</p>
        <p><strong>Pre-Game Workflows Scheduled:</strong> {scheduled_games}</p>
        <p><strong>Alert Time:</strong> {self.alert_minutes} minutes before each game</p>
        <p>{scheduled_games} workflows will automatically trigger before today's games.</p>
    </div>
</body>
</html>"""
            
            await self.workflow_service._send_email(subject, plain_text, html_content)
            
        except Exception as e:
            self.logger.warning("Failed to send daily setup notification", error=str(e))
    
    async def _send_error_notification(self, title: str, error_message: str):
        """Send error notification if email is configured."""
        if not self.workflow_service.email_config.is_configured():
            return
        
        try:
            subject = f"🚨 MLB Scheduler Error: {title}"
            
            plain_text = f"""🚨 SCHEDULER ERROR ALERT

Error: {title}
Details: {error_message}
Time: {datetime.now(self.est).strftime('%Y-%m-%d %I:%M %p EST')}

The MLB pre-game scheduler has encountered an error.
Please check system logs and investigate the issue.

---
Generated by MLB Sharp Betting Analytics Platform
General Balls"""
            
            html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
    <div style="background: #f8d7da; color: #721c24; padding: 20px; border-radius: 8px;">
        <h1>🚨 Scheduler Error Alert</h1>
        <p><strong>Error:</strong> {title}</p>
        <p><strong>Details:</strong> {error_message}</p>
        <p><strong>Time:</strong> {datetime.now(self.est).strftime('%Y-%m-%d %I:%M %p EST')}</p>
        <p>Please investigate and resolve the issue.</p>
    </div>
</body>
</html>"""
            
            await self.workflow_service._send_email(subject, plain_text, html_content)
            
        except Exception as e:
            self.logger.warning("Failed to send error notification", error=str(e))
    
    async def run_forever(self) -> None:
        """Run the scheduler until interrupted."""
        await self.start()
        
        try:
            while self.running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        finally:
            await self.stop()
    
    def get_status(self) -> Dict[str, any]:
        """Get current scheduler status."""
        active_jobs = len([job for job in self.scheduler.get_jobs() 
                          if job.id.startswith('pregame_workflow_')])
        
        return {
            "running": self.running,
            "scheduled_games_today": len(self.scheduled_games),
            "active_workflow_jobs": active_jobs,
            "completed_workflows": len(self.completed_workflows),
            "email_configured": self.workflow_service.email_config.is_configured(),
            "metrics": self.metrics
        }
    
    def get_recent_workflows(self, limit: int = 5) -> List[WorkflowResult]:
        """Get recent workflow results."""
        workflows = list(self.completed_workflows.values())
        return sorted(workflows, 
                     key=lambda w: w.start_time, 
                     reverse=True)[:limit]


async def main():
    """Main entry point for the pre-game scheduler."""
    scheduler = PreGameScheduler()
    
    try:
        await scheduler.run_forever()
    except Exception as e:
        logger.error("Scheduler failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main()) 