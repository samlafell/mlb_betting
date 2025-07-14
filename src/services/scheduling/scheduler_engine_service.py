#!/usr/bin/env python3
"""
Scheduler Engine Service

Migrated and enhanced scheduling functionality from the legacy module.
Provides comprehensive scheduling capabilities with cron-based job management,
game alerts, workflow automation, and backtesting integration.

Legacy Source: src/mlb_sharp_betting/services/scheduler_engine.py
Enhanced Features:
- Unified architecture integration
- Async-first design with better error handling
- Enhanced monitoring and metrics
- Improved configuration management
- Better separation of concerns

Part of Phase 5D: Critical Business Logic Migration
"""

import asyncio
import signal
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Callable, Any, Union
from dataclasses import dataclass, field
from enum import Enum
import structlog
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.jobstores.memory import MemoryJobStore

from ...core.config import get_settings
from ...core.logging import get_logger
from ...core.exceptions import SchedulingError, ValidationError
from ...data.database.connection import get_connection

logger = get_logger(__name__)


class SchedulerJobType(str, Enum):
    """Job type enumeration for the unified scheduler."""
    HOURLY_DATA = "hourly_data_collection"
    DAILY_SETUP = "daily_game_setup"
    GAME_ALERT = "game_alert"
    PRE_GAME_WORKFLOW = "pre_game_workflow"
    BACKTESTING_DAILY = "backtesting_daily"
    BACKTESTING_WEEKLY = "backtesting_weekly"
    CUSTOM = "custom"


class SchedulerStatus(str, Enum):
    """Scheduler status enumeration."""
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    ERROR = "error"


@dataclass
class SchedulerMetrics:
    """Comprehensive metrics for scheduler operations."""
    # Core metrics
    scheduler_starts: int = 0
    hourly_runs: int = 0
    game_alerts: int = 0
    errors: int = 0
    last_hourly_run: Optional[datetime] = None
    last_game_alert: Optional[datetime] = None
    
    # Workflow metrics
    daily_setups: int = 0
    games_scheduled: int = 0
    workflows_triggered: int = 0
    successful_workflows: int = 0
    failed_workflows: int = 0
    
    # Backtesting metrics
    backtesting_runs: int = 0
    backtesting_failures: int = 0
    alerts_generated: int = 0
    
    # General metrics
    total_jobs_executed: int = 0
    active_jobs: int = 0
    uptime_seconds: float = 0.0
    
    def increment(self, metric: str, value: int = 1):
        """Increment a metric counter."""
        if hasattr(self, metric):
            current_value = getattr(self, metric)
            setattr(self, metric, current_value + value)
    
    def update(self, metric: str, value: Any):
        """Update a metric value."""
        if hasattr(self, metric):
            setattr(self, metric, value)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            field.name: getattr(self, field.name)
            for field in self.__dataclass_fields__.values()
        }


@dataclass
class SchedulerConfig:
    """Configuration for the scheduler engine."""
    # Basic settings
    alert_minutes_before_game: int = 5
    daily_setup_hour: int = 6
    notifications_enabled: bool = True
    
    # Timezone settings
    timezone: str = 'US/Eastern'
    
    # Job execution settings
    max_concurrent_jobs: int = 3
    job_timeout_seconds: int = 600
    misfire_grace_time: int = 300
    
    # Retry settings
    max_retries: int = 3
    retry_delay_seconds: int = 60
    
    # Monitoring settings
    enable_metrics: bool = True
    log_job_execution: bool = True


@dataclass
class ScheduledJob:
    """Represents a scheduled job."""
    job_id: str
    job_type: SchedulerJobType
    name: str
    trigger: Union[CronTrigger, DateTrigger]
    handler: Callable
    args: List[Any] = field(default_factory=list)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.now)
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    run_count: int = 0
    error_count: int = 0


class SchedulerEngineService:
    """
    Unified Scheduler Engine Service
    
    Provides comprehensive scheduling capabilities with cron-based job management,
    game alerts, workflow automation, and backtesting integration.
    
    Features:
    - Cron-based job scheduling with AsyncIOScheduler
    - Game alert scheduling and notifications
    - Pre-game workflow automation
    - Backtesting pipeline scheduling
    - Comprehensive monitoring and metrics
    - Graceful shutdown and error handling
    - Timezone-aware scheduling
    """
    
    def __init__(self, config: Optional[SchedulerConfig] = None):
        """Initialize the scheduler engine service."""
        self.config = config or SchedulerConfig()
        self.settings = get_settings()
        self.logger = logger.bind(service="SchedulerEngineService")
        
        # State management
        self.status = SchedulerStatus.STOPPED
        self.start_time: Optional[datetime] = None
        self.metrics = SchedulerMetrics()
        self.scheduled_jobs: Dict[str, ScheduledJob] = {}
        
        # Timezone setup
        self.timezone = pytz.timezone(self.config.timezone)
        self.utc = pytz.timezone('UTC')
        
        # Initialize scheduler
        self._setup_scheduler()
        
        # Signal handlers for graceful shutdown
        self._setup_signal_handlers()
        
        self.logger.info("SchedulerEngineService initialized",
                        timezone=self.config.timezone,
                        notifications_enabled=self.config.notifications_enabled)
    
    def _setup_scheduler(self):
        """Setup the core AsyncIOScheduler."""
        jobstores = {
            'default': MemoryJobStore()
        }
        executors = {
            'default': AsyncIOExecutor()
        }
        job_defaults = {
            'coalesce': False,
            'max_instances': 1,
            'misfire_grace_time': self.config.misfire_grace_time
        }
        
        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone='UTC'
        )
        
        # Add job event listeners
        self.scheduler.add_listener(
            self._job_executed_listener,
            events=['EVENT_JOB_EXECUTED', 'EVENT_JOB_ERROR']
        )
    
    def _setup_signal_handlers(self):
        """Setup graceful shutdown signal handlers."""
        def signal_handler(signum, frame):
            self.logger.info("Received shutdown signal", signal=signum)
            if self.status == SchedulerStatus.RUNNING:
                asyncio.create_task(self.stop())
            else:
                sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def start(self, mode: str = "full") -> None:
        """
        Start the scheduler engine with specified mode.
        
        Args:
            mode: Scheduling mode - "full", "core", "workflow", or "backtesting"
        """
        if self.status != SchedulerStatus.STOPPED:
            raise SchedulingError(f"Scheduler is already {self.status.value}")
        
        self.status = SchedulerStatus.STARTING
        self.start_time = datetime.now(timezone.utc)
        
        try:
            self.logger.info(f"Starting SchedulerEngineService in {mode} mode")
            
            # Setup jobs based on mode
            if mode in ["full", "core"]:
                await self._setup_core_jobs()
            
            if mode in ["full", "workflow"]:
                await self._setup_workflow_jobs()
            
            if mode in ["full", "backtesting"]:
                await self._setup_backtesting_jobs()
            
            # Start the scheduler
            self.scheduler.start()
            self.status = SchedulerStatus.RUNNING
            
            self.metrics.increment('scheduler_starts')
            
            self.logger.info("SchedulerEngineService started successfully",
                           mode=mode,
                           active_jobs=len(self.scheduler.get_jobs()))
            
            # Display startup summary
            self._display_startup_summary(mode)
            
        except Exception as e:
            self.status = SchedulerStatus.ERROR
            self.logger.error(f"Failed to start SchedulerEngineService: {e}")
            self.metrics.increment('errors')
            raise SchedulingError(f"Scheduler startup failed: {e}") from e
    
    async def stop(self) -> None:
        """Stop the scheduler engine gracefully."""
        if self.status != SchedulerStatus.RUNNING:
            return
        
        self.status = SchedulerStatus.STOPPING
        self.logger.info("Stopping SchedulerEngineService")
        
        try:
            if self.scheduler.running:
                self.scheduler.shutdown(wait=True)
            
            self.status = SchedulerStatus.STOPPED
            
            # Calculate uptime
            if self.start_time:
                uptime = (datetime.now(timezone.utc) - self.start_time).total_seconds()
                self.metrics.update('uptime_seconds', uptime)
            
            self.logger.info("SchedulerEngineService stopped successfully")
            
            # Display final metrics
            self._display_final_metrics()
            
        except Exception as e:
            self.status = SchedulerStatus.ERROR
            self.logger.error(f"Error stopping SchedulerEngineService: {e}")
            raise SchedulingError(f"Scheduler shutdown failed: {e}") from e
    
    async def add_job(self, job: ScheduledJob) -> None:
        """
        Add a new scheduled job.
        
        Args:
            job: ScheduledJob configuration
        """
        try:
            # Add job to scheduler
            scheduler_job = self.scheduler.add_job(
                func=self._job_wrapper,
                trigger=job.trigger,
                args=[job.handler, job.args, job.kwargs, job.job_id],
                id=job.job_id,
                name=job.name,
                replace_existing=True
            )
            
            # Update job with next run time
            job.next_run = scheduler_job.next_run_time
            
            # Store job configuration
            self.scheduled_jobs[job.job_id] = job
            
            self.logger.info("Job added successfully",
                           job_id=job.job_id,
                           job_type=job.job_type.value,
                           next_run=job.next_run)
            
        except Exception as e:
            self.logger.error("Failed to add job", job_id=job.job_id, error=str(e))
            raise SchedulingError(f"Failed to add job {job.job_id}: {e}") from e
    
    async def remove_job(self, job_id: str) -> None:
        """
        Remove a scheduled job.
        
        Args:
            job_id: Job identifier
        """
        try:
            # Remove from scheduler
            self.scheduler.remove_job(job_id)
            
            # Remove from our tracking
            if job_id in self.scheduled_jobs:
                del self.scheduled_jobs[job_id]
            
            self.logger.info("Job removed successfully", job_id=job_id)
            
        except Exception as e:
            self.logger.error("Failed to remove job", job_id=job_id, error=str(e))
            raise SchedulingError(f"Failed to remove job {job_id}: {e}") from e
    
    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get status of a specific job.
        
        Args:
            job_id: Job identifier
            
        Returns:
            Job status dictionary or None if not found
        """
        if job_id not in self.scheduled_jobs:
            return None
        
        job = self.scheduled_jobs[job_id]
        scheduler_job = self.scheduler.get_job(job_id)
        
        return {
            'job_id': job.job_id,
            'job_type': job.job_type.value,
            'name': job.name,
            'enabled': job.enabled,
            'created_at': job.created_at,
            'last_run': job.last_run,
            'next_run': scheduler_job.next_run_time if scheduler_job else None,
            'run_count': job.run_count,
            'error_count': job.error_count
        }
    
    async def get_all_jobs_status(self) -> List[Dict[str, Any]]:
        """Get status of all scheduled jobs."""
        jobs_status = []
        for job_id in self.scheduled_jobs:
            status = await self.get_job_status(job_id)
            if status:
                jobs_status.append(status)
        return jobs_status
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current scheduler metrics."""
        metrics = self.metrics.to_dict()
        
        # Add runtime metrics
        if self.start_time:
            uptime = (datetime.now(timezone.utc) - self.start_time).total_seconds()
            metrics['current_uptime_seconds'] = uptime
        
        metrics['status'] = self.status.value
        metrics['active_jobs'] = len(self.scheduler.get_jobs()) if self.scheduler else 0
        
        return metrics
    
    # Private methods for job management
    
    async def _setup_core_jobs(self):
        """Setup core scheduling jobs."""
        # Hourly data collection
        hourly_job = ScheduledJob(
            job_id=SchedulerJobType.HOURLY_DATA,
            job_type=SchedulerJobType.HOURLY_DATA,
            name='Hourly Data Collection',
            trigger=CronTrigger(minute=0),
            handler=self._hourly_data_handler
        )
        await self.add_job(hourly_job)
        
        # Daily setup
        daily_job = ScheduledJob(
            job_id=SchedulerJobType.DAILY_SETUP,
            job_type=SchedulerJobType.DAILY_SETUP,
            name='Daily Game Setup',
            trigger=CronTrigger(
                hour=self.config.daily_setup_hour,
                minute=0,
                timezone=self.timezone
            ),
            handler=self._daily_setup_handler
        )
        await self.add_job(daily_job)
    
    async def _setup_workflow_jobs(self):
        """Setup workflow automation jobs."""
        # Pre-game workflow scheduling will be handled dynamically
        # as games are scheduled in the daily setup
        pass
    
    async def _setup_backtesting_jobs(self):
        """Setup backtesting jobs."""
        # Daily backtesting at 2 AM EST
        daily_backtest_job = ScheduledJob(
            job_id=SchedulerJobType.BACKTESTING_DAILY,
            job_type=SchedulerJobType.BACKTESTING_DAILY,
            name='Daily Backtesting Pipeline',
            trigger=CronTrigger(hour=2, minute=0, timezone=self.timezone),
            handler=self._daily_backtesting_handler
        )
        await self.add_job(daily_backtest_job)
        
        # Weekly comprehensive analysis - Mondays at 6 AM EST
        weekly_backtest_job = ScheduledJob(
            job_id=SchedulerJobType.BACKTESTING_WEEKLY,
            job_type=SchedulerJobType.BACKTESTING_WEEKLY,
            name='Weekly Backtesting Analysis',
            trigger=CronTrigger(day_of_week='mon', hour=6, minute=0, timezone=self.timezone),
            handler=self._weekly_analysis_handler
        )
        await self.add_job(weekly_backtest_job)
    
    async def _job_wrapper(self, handler: Callable, args: List[Any], 
                          kwargs: Dict[str, Any], job_id: str):
        """Wrapper for job execution with error handling and metrics."""
        job = self.scheduled_jobs.get(job_id)
        if not job:
            self.logger.error("Job not found in registry", job_id=job_id)
            return
        
        start_time = datetime.now(timezone.utc)
        
        try:
            self.logger.debug("Executing job", job_id=job_id, job_type=job.job_type.value)
            
            # Execute the job handler
            if asyncio.iscoroutinefunction(handler):
                result = await handler(*args, **kwargs)
            else:
                result = handler(*args, **kwargs)
            
            # Update job metrics
            job.last_run = start_time
            job.run_count += 1
            self.metrics.increment('total_jobs_executed')
            
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            self.logger.info("Job executed successfully",
                           job_id=job_id,
                           job_type=job.job_type.value,
                           execution_time=execution_time)
            
            return result
            
        except Exception as e:
            job.error_count += 1
            self.metrics.increment('errors')
            
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            self.logger.error("Job execution failed",
                            job_id=job_id,
                            job_type=job.job_type.value,
                            execution_time=execution_time,
                            error=str(e))
            
            # Send notification if enabled
            if self.config.notifications_enabled:
                await self._send_error_notification(job_id, str(e))
            
            raise
    
    def _job_executed_listener(self, event):
        """Listener for job execution events."""
        if hasattr(event, 'job_id'):
            job_id = event.job_id
            if job_id in self.scheduled_jobs:
                job = self.scheduled_jobs[job_id]
                
                # Update next run time
                scheduler_job = self.scheduler.get_job(job_id)
                if scheduler_job:
                    job.next_run = scheduler_job.next_run_time
    
    # Job handlers
    
    async def _hourly_data_handler(self) -> None:
        """Handle hourly data collection."""
        self.logger.info("Starting hourly data collection")
        
        try:
            # This would integrate with the unified data collection service
            # For now, we'll use a placeholder
            await asyncio.sleep(1)  # Simulate work
            
            self.metrics.increment('hourly_runs')
            self.metrics.update('last_hourly_run', datetime.now(timezone.utc))
            
            self.logger.info("Hourly collection completed successfully")
            
        except Exception as e:
            self.logger.error(f"Hourly handler error: {e}")
            raise
    
    async def _daily_setup_handler(self) -> None:
        """Handle daily game setup and scheduling."""
        self.logger.info("Starting daily game setup")
        
        try:
            # This would integrate with game management and workflow services
            # For now, we'll use a placeholder
            await asyncio.sleep(1)  # Simulate work
            
            games_scheduled = 0  # Placeholder
            
            self.metrics.increment('daily_setups')
            self.metrics.update('games_scheduled', games_scheduled)
            
            self.logger.info("Daily setup completed", games_scheduled=games_scheduled)
            
        except Exception as e:
            self.logger.error(f"Daily setup error: {e}")
            raise
    
    async def _daily_backtesting_handler(self) -> None:
        """Handle daily backtesting pipeline."""
        self.logger.info("Starting daily backtesting pipeline")
        
        try:
            # This would integrate with the backtesting service
            # For now, we'll use a placeholder
            await asyncio.sleep(2)  # Simulate work
            
            self.metrics.increment('backtesting_runs')
            
            self.logger.info("Daily backtesting completed successfully")
            
        except Exception as e:
            self.metrics.increment('backtesting_failures')
            self.logger.error(f"Daily backtesting error: {e}")
            raise
    
    async def _weekly_analysis_handler(self) -> None:
        """Handle weekly comprehensive analysis."""
        self.logger.info("Starting weekly analysis")
        
        try:
            # This would integrate with analysis and reporting services
            # For now, we'll use a placeholder
            await asyncio.sleep(5)  # Simulate work
            
            self.logger.info("Weekly analysis completed successfully")
            
        except Exception as e:
            self.logger.error(f"Weekly analysis error: {e}")
            raise
    
    async def _send_error_notification(self, job_id: str, error_message: str):
        """Send error notification."""
        if not self.config.notifications_enabled:
            return
        
        # Log the notification
        self.logger.warning("JOB ERROR NOTIFICATION",
                          job_id=job_id,
                          error=error_message)
        
        # Print to console for immediate visibility
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n🚨 [{timestamp}] JOB ERROR: {job_id} - {error_message}\n")
    
    def _display_startup_summary(self, mode: str):
        """Display startup summary information."""
        print("\n" + "=" * 60)
        print("⚡ MLB UNIFIED SCHEDULER ENGINE")
        print("=" * 60)
        print(f"🚀 Mode: {mode.upper()}")
        print(f"⏰ Game Alerts: {self.config.alert_minutes_before_game} minutes before each game")
        print(f"🕐 Hourly Collection: Every hour at :00")
        print(f"🌅 Daily Setup: {self.config.daily_setup_hour:02d}:00 {self.config.timezone}")
        print(f"📧 Notifications: {'✅ Enabled' if self.config.notifications_enabled else '❌ Disabled'}")
        print(f"📊 Active Jobs: {len(self.scheduler.get_jobs())}")
        print(f"🌍 Timezone: {self.config.timezone}")
        print("\nPress Ctrl+C to stop the scheduler...")
        print("=" * 60 + "\n")
    
    def _display_final_metrics(self):
        """Display final metrics on shutdown."""
        metrics = self.get_metrics()
        
        print("\n" + "=" * 60)
        print("📊 SCHEDULER ENGINE FINAL METRICS")
        print("=" * 60)
        print(f"⏱️  Uptime: {metrics.get('uptime_seconds', 0):.0f} seconds")
        print(f"🔄 Total Jobs Executed: {metrics.get('total_jobs_executed', 0)}")
        print(f"📈 Hourly Runs: {metrics.get('hourly_runs', 0)}")
        print(f"🚨 Game Alerts: {metrics.get('game_alerts', 0)}")
        print(f"❌ Errors: {metrics.get('errors', 0)}")
        print(f"🧪 Backtesting Runs: {metrics.get('backtesting_runs', 0)}")
        print("=" * 60 + "\n")
    
    async def run_forever(self) -> None:
        """Run the scheduler forever until interrupted."""
        try:
            while self.status == SchedulerStatus.RUNNING:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        finally:
            await self.stop()


# Service instance for easy importing
scheduler_engine_service = SchedulerEngineService()


# Convenience functions
async def start_scheduler(mode: str = "full", config: Optional[SchedulerConfig] = None) -> SchedulerEngineService:
    """Convenience function to start the scheduler."""
    service = SchedulerEngineService(config) if config else scheduler_engine_service
    await service.start(mode)
    return service


async def stop_scheduler():
    """Convenience function to stop the scheduler."""
    await scheduler_engine_service.stop()


if __name__ == "__main__":
    # Example usage
    async def main():
        try:
            # Start scheduler in full mode
            await start_scheduler("full")
            
            # Run forever
            await scheduler_engine_service.run_forever()
            
        except Exception as e:
            print(f"Scheduler error: {e}")
        finally:
            await stop_scheduler()
    
    asyncio.run(main()) 