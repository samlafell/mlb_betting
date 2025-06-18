"""
Scheduler service for automated MLB betting analysis.

This service handles:
1. Hourly data collection runs
2. Pre-game alerts (5 minutes before game start)
3. System monitoring and error handling
"""

import asyncio
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Callable, Any
from pathlib import Path
import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.jobstores.memory import MemoryJobStore

from .mlb_api_service import MLBStatsAPIService, MLBGameInfo
from ..core.logging import get_logger

logger = get_logger(__name__)


class MLBBettingScheduler:
    """Automated scheduler for MLB betting analysis pipeline."""
    
    def __init__(self, 
                 project_root: Optional[Path] = None,
                 notifications_enabled: bool = True,
                 alert_minutes_before_game: int = 5):
        """
        Initialize the scheduler.
        
        Args:
            project_root: Path to project root directory
            notifications_enabled: Whether to enable notifications
            alert_minutes_before_game: Minutes before game start to trigger alerts
        """
        self.project_root = project_root or Path(__file__).parent.parent.parent.parent
        self.notifications_enabled = notifications_enabled
        self.alert_minutes = alert_minutes_before_game
        
        # Initialize MLB API service
        self.mlb_api = MLBStatsAPIService()
        
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
        
        # Metrics tracking
        self.metrics = {
            'hourly_runs': 0,
            'game_alerts': 0,
            'errors': 0,
            'last_hourly_run': None,
            'last_game_alert': None,
            'scheduled_games_today': 0,
            'active_alerts': 0
        }
        
        self.logger = logger.bind(service="scheduler")
        
    async def run_entrypoint(self, context: str = "scheduled") -> Dict[str, Any]:
        """
        Run the main entrypoint script.
        
        Args:
            context: Context for the run (hourly, game_alert, manual)
            
        Returns:
            Execution results and metrics
        """
        start_time = datetime.now(timezone.utc)
        self.logger.info("Starting entrypoint execution", context=context)
        
        try:
            # Change to project directory
            entrypoint_path = self.project_root / "src" / "mlb_sharp_betting" / "entrypoint.py"
            
            # Run the entrypoint with UV
            cmd = ["uv", "run", str(entrypoint_path), "--verbose"]
            
            result = subprocess.run(
                cmd,
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            end_time = datetime.now(timezone.utc)
            execution_time = (end_time - start_time).total_seconds()
            
            if result.returncode == 0:
                self.logger.info("Entrypoint executed successfully",
                               context=context,
                               execution_time=execution_time,
                               stdout_lines=len(result.stdout.splitlines()))
                
                # Update metrics
                if context == "hourly":
                    self.metrics['hourly_runs'] += 1
                    self.metrics['last_hourly_run'] = start_time
                elif context == "game_alert":
                    self.metrics['game_alerts'] += 1
                    self.metrics['last_game_alert'] = start_time
                
                return {
                    'success': True,
                    'context': context,
                    'execution_time': execution_time,
                    'start_time': start_time,
                    'end_time': end_time,
                    'stdout': result.stdout,
                    'stderr': result.stderr
                }
            else:
                self.logger.error("Entrypoint execution failed",
                                context=context,
                                returncode=result.returncode,
                                stderr=result.stderr)
                self.metrics['errors'] += 1
                
                return {
                    'success': False,
                    'context': context,
                    'error': f"Exit code {result.returncode}",
                    'stderr': result.stderr,
                    'stdout': result.stdout
                }
                
        except subprocess.TimeoutExpired:
            self.logger.error("Entrypoint execution timed out", context=context)
            self.metrics['errors'] += 1
            return {
                'success': False,
                'context': context,
                'error': "Execution timeout (5 minutes)"
            }
        except Exception as e:
            self.logger.error("Failed to execute entrypoint", 
                            context=context, error=str(e))
            self.metrics['errors'] += 1
            return {
                'success': False,
                'context': context,
                'error': str(e)
            }

    async def send_notification(self, message: str, context: str = "alert") -> None:
        """
        Send notification (placeholder for future implementation).
        
        Args:
            message: Notification message
            context: Notification context
        """
        if not self.notifications_enabled:
            return
            
        # For now, just log the notification
        # In the future, you can integrate with Slack, Discord, email, etc.
        self.logger.info("NOTIFICATION", message=message, context=context)
        
        # Print to console for immediate visibility
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n🚨 [{timestamp}] {context.upper()}: {message}\n")

    async def schedule_game_alerts_for_today(self) -> int:
        """
        Schedule alerts for all games today.
        
        Returns:
            Number of alerts scheduled
        """
        today = datetime.now(timezone.utc).date()
        games = self.mlb_api.get_games_for_date(today)
        
        scheduled_count = 0
        
        for game in games:
            # Skip games that are already completed or postponed
            if game.status in ['Final', 'Completed', 'Postponed', 'Cancelled']:
                continue
                
            # Calculate alert time (5 minutes before game)
            alert_time = game.game_date - timedelta(minutes=self.alert_minutes)
            
            # Only schedule if alert time is in the future
            if alert_time > datetime.now(timezone.utc):
                job_id = f"game_alert_{game.game_pk}"
                
                # Remove existing job if it exists
                if self.scheduler.get_job(job_id):
                    self.scheduler.remove_job(job_id)
                
                # Schedule the alert
                self.scheduler.add_job(
                    func=self.game_alert_handler,
                    trigger=DateTrigger(run_date=alert_time),
                    args=[game],
                    id=job_id,
                    name=f"Game Alert: {game.away_team} @ {game.home_team}",
                    replace_existing=True
                )
                
                scheduled_count += 1
                self.logger.info("Scheduled game alert",
                               game=f"{game.away_team} @ {game.home_team}",
                               alert_time=alert_time,
                               game_time=game.game_date)
        
        self.metrics['scheduled_games_today'] = scheduled_count
        self.metrics['active_alerts'] = len([job for job in self.scheduler.get_jobs() 
                                           if job.id.startswith('game_alert_')])
        
        self.logger.info("Game alerts scheduled for today",
                        games_found=len(games),
                        alerts_scheduled=scheduled_count)
        
        return scheduled_count

    async def game_alert_handler(self, game: MLBGameInfo) -> None:
        """
        Handle game alert - run entrypoint and send notification.
        
        Args:
            game: Game information
        """
        game_desc = f"{game.away_team} @ {game.home_team}"
        
        self.logger.info("Triggering game alert", game=game_desc)
        
        # Send pre-alert notification
        await self.send_notification(
            f"🏈 GAME ALERT: {game_desc} starts in {self.alert_minutes} minutes! "
            f"Running analysis...",
            context="game_alert"
        )
        
        # Run the entrypoint
        result = await self.run_entrypoint(context="game_alert")
        
        # Send results notification
        if result['success']:
            await self.send_notification(
                f"✅ Analysis complete for {game_desc}! "
                f"Check your system for betting opportunities. "
                f"Execution time: {result.get('execution_time', 'N/A'):.1f}s",
                context="game_alert_success"
            )
        else:
            await self.send_notification(
                f"❌ Analysis failed for {game_desc}! "
                f"Error: {result.get('error', 'Unknown error')}",
                context="game_alert_error"
            )

    async def hourly_handler(self) -> None:
        """Handle hourly execution."""
        self.logger.info("Executing hourly data collection")
        
        # Run the entrypoint
        result = await self.run_entrypoint(context="hourly")
        
        # Log results
        if result['success']:
            self.logger.info("Hourly execution completed successfully",
                           execution_time=result.get('execution_time', 0))
        else:
            self.logger.error("Hourly execution failed",
                            error=result.get('error', 'Unknown error'))
            
            # Send error notification
            await self.send_notification(
                f"❌ Hourly data collection failed: {result.get('error', 'Unknown error')}",
                context="hourly_error"
            )

    async def daily_setup_handler(self) -> None:
        """Handle daily setup - schedule game alerts for the day."""
        self.logger.info("Running daily setup")
        
        try:
            # Schedule game alerts for today
            alerts_scheduled = await self.schedule_game_alerts_for_today()
            
            # Send summary notification
            await self.send_notification(
                f"📅 Daily setup complete: {alerts_scheduled} game alerts scheduled for today",
                context="daily_setup"
            )
            
        except Exception as e:
            self.logger.error("Daily setup failed", error=str(e))
            await self.send_notification(
                f"❌ Daily setup failed: {str(e)}",
                context="daily_setup_error"
            )

    def start(self) -> None:
        """Start the scheduler with all jobs."""
        self.logger.info("Starting MLB betting scheduler")
        
        # Add hourly job
        self.scheduler.add_job(
            func=self.hourly_handler,
            trigger=CronTrigger(minute=0),  # Run at the top of every hour
            id='hourly_data_collection',
            name='Hourly Data Collection',
            replace_existing=True
        )
        
        # Add daily setup job (runs at 6 AM EST / 11 AM UTC)
        self.scheduler.add_job(
            func=self.daily_setup_handler,
            trigger=CronTrigger(hour=11, minute=0),  # 11 AM UTC = 6 AM EST
            id='daily_setup',
            name='Daily Game Alert Setup',
            replace_existing=True
        )
        
        # Start the scheduler
        self.scheduler.start()
        
        # Schedule initial game alerts for today
        asyncio.create_task(self.schedule_game_alerts_for_today())
        
        self.logger.info("Scheduler started successfully",
                        jobs_count=len(self.scheduler.get_jobs()))

    def stop(self) -> None:
        """Stop the scheduler."""
        self.logger.info("Stopping scheduler")
        self.scheduler.shutdown()

    def get_status(self) -> Dict[str, Any]:
        """Get scheduler status and metrics."""
        jobs = self.scheduler.get_jobs()
        
        return {
            'scheduler_running': self.scheduler.running,
            'jobs_count': len(jobs),
            'jobs': [
                {
                    'id': job.id,
                    'name': job.name,
                    'next_run': job.next_run_time,
                    'trigger': str(job.trigger)
                }
                for job in jobs
            ],
            'metrics': self.metrics.copy()
        }


async def main():
    """Main function for testing the scheduler."""
    import signal
    
    # Initialize scheduler
    scheduler = MLBBettingScheduler()
    
    # Set up signal handlers for graceful shutdown
    def signal_handler(signum, frame):
        print(f"\nReceived signal {signum}, shutting down...")
        scheduler.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Start scheduler
        scheduler.start()
        
        print("🚀 MLB Betting Scheduler started!")
        print("   - Hourly data collection: Every hour at :00")
        print("   - Daily setup: 6 AM EST (11 AM UTC)")
        print("   - Game alerts: 5 minutes before each game")
        print("\nPress Ctrl+C to stop...")
        
        # Keep the scheduler running
        while True:
            await asyncio.sleep(60)
            
            # Print status every 10 minutes
            if datetime.now().minute % 10 == 0:
                status = scheduler.get_status()
                print(f"\n📊 Status: {status['jobs_count']} jobs, "
                      f"{status['metrics']['hourly_runs']} hourly runs, "
                      f"{status['metrics']['game_alerts']} game alerts")
                
    except KeyboardInterrupt:
        print("\nShutting down scheduler...")
        scheduler.stop()


if __name__ == "__main__":
    asyncio.run(main()) 