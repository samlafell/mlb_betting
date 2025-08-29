"""
Retraining Scheduler

Manages scheduled and triggered retraining workflows for betting strategies.
Coordinates with RetrainingTriggerService and AutomatedRetrainingEngine to
provide comprehensive retraining orchestration.

Features:
- Scheduled retraining (weekly, monthly, custom schedules)
- Trigger-based retraining coordination
- Job queue management and prioritization
- Conflict resolution and resource management
- Integration with existing monitoring infrastructure
"""

import asyncio
import heapq
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set
import croniter

from src.core.config import get_settings
from src.core.logging import LogComponent, get_logger
from .trigger_service import RetrainingTriggerService, TriggerCondition
from .automated_engine import AutomatedRetrainingEngine, RetrainingStrategy, RetrainingConfiguration


logger = get_logger(__name__, LogComponent.CORE)


class ScheduleType(str, Enum):
    """Types of retraining schedules"""
    
    CRON = "cron"          # Cron-based scheduling
    INTERVAL = "interval"   # Fixed interval scheduling
    TRIGGER = "trigger"     # Trigger-based scheduling
    MANUAL = "manual"       # Manual scheduling


class SchedulePriority(int, Enum):
    """Priority levels for scheduled jobs"""
    
    CRITICAL = 1    # Emergency retraining
    HIGH = 2        # Performance degradation
    NORMAL = 3      # Regular scheduled retraining
    LOW = 4         # Maintenance retraining


@dataclass
class RetrainingSchedule:
    """Configuration for scheduled retraining"""
    
    schedule_id: str
    schedule_name: str
    strategy_name: str
    schedule_type: ScheduleType
    
    # Schedule configuration
    cron_expression: Optional[str] = None  # For CRON type
    interval_hours: Optional[int] = None   # For INTERVAL type
    
    # Execution configuration
    retraining_strategy: RetrainingStrategy = RetrainingStrategy.FULL_RETRAINING
    configuration: Optional[RetrainingConfiguration] = None
    priority: SchedulePriority = SchedulePriority.NORMAL
    
    # State
    enabled: bool = True
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.now)
    created_by: str = "system"
    description: Optional[str] = None


@dataclass
class ScheduledJob:
    """A scheduled retraining job in the queue"""
    
    job_id: str
    schedule_id: str
    strategy_name: str
    priority: SchedulePriority
    scheduled_time: datetime
    
    retraining_strategy: RetrainingStrategy
    configuration: RetrainingConfiguration
    trigger_conditions: List[TriggerCondition] = field(default_factory=list)
    
    # Job state
    status: str = "queued"  # queued, running, completed, failed, cancelled
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Results
    retraining_job_id: Optional[str] = None
    error_message: Optional[str] = None
    
    def __lt__(self, other: 'ScheduledJob') -> bool:
        """Comparison for priority queue ordering."""
        # Lower priority number = higher priority
        if self.priority.value != other.priority.value:
            return self.priority.value < other.priority.value
        # Earlier scheduled time has higher priority
        return self.scheduled_time < other.scheduled_time


class RetrainingScheduler:
    """
    Service for managing scheduled and triggered retraining workflows.
    
    Coordinates scheduling, queue management, and execution of retraining jobs.
    Integrates with trigger service for event-driven retraining and automated
    retraining engine for job execution.
    """
    
    def __init__(
        self,
        trigger_service: RetrainingTriggerService,
        retraining_engine: AutomatedRetrainingEngine,
        max_concurrent_jobs: int = 2,
        check_interval_minutes: int = 5
    ):
        """Initialize the retraining scheduler."""
        
        self.trigger_service = trigger_service
        self.retraining_engine = retraining_engine
        self.config = get_settings()
        self.logger = logger
        
        # Configuration
        self.max_concurrent_jobs = max_concurrent_jobs
        self.check_interval = check_interval_minutes * 60  # Convert to seconds
        
        # Schedule management
        self.schedules: Dict[str, RetrainingSchedule] = {}
        
        # Job queue management (priority queue)
        self.job_queue: List[ScheduledJob] = []
        self.running_jobs: Dict[str, ScheduledJob] = {}
        self.completed_jobs: List[ScheduledJob] = []
        
        # State management
        self.scheduler_running = False
        self.last_trigger_check: Dict[str, datetime] = {}
        
        # Conflict prevention
        self.strategy_locks: Set[str] = set()  # Strategies currently being retrained
        
        self.logger.info("RetrainingScheduler initialized")
    
    async def start_scheduler(self) -> None:
        """Start the retraining scheduler."""
        
        if self.scheduler_running:
            self.logger.warning("Scheduler already running")
            return
        
        self.scheduler_running = True
        
        self.logger.info("Started retraining scheduler")
        
        # Start scheduler tasks
        scheduler_tasks = [
            asyncio.create_task(self._schedule_check_loop()),
            asyncio.create_task(self._trigger_check_loop()),
            asyncio.create_task(self._job_execution_loop()),
            asyncio.create_task(self._job_monitoring_loop()),
        ]
        
        try:
            await asyncio.gather(*scheduler_tasks)
        except Exception as e:
            self.logger.error(f"Error in scheduler tasks: {e}", exc_info=True)
        finally:
            self.scheduler_running = False
    
    async def stop_scheduler(self) -> None:
        """Stop the retraining scheduler."""
        
        self.scheduler_running = False
        self.logger.info("Stopped retraining scheduler")
    
    def add_schedule(self, schedule: RetrainingSchedule) -> None:
        """Add a new retraining schedule."""
        
        # Calculate next run time
        schedule.next_run = self._calculate_next_run(schedule)
        
        self.schedules[schedule.schedule_id] = schedule
        
        self.logger.info(
            f"Added retraining schedule: {schedule.schedule_name}",
            extra={
                "schedule_id": schedule.schedule_id,
                "strategy": schedule.strategy_name,
                "schedule_type": schedule.schedule_type.value,
                "next_run": schedule.next_run.isoformat() if schedule.next_run else None
            }
        )
    
    def remove_schedule(self, schedule_id: str) -> bool:
        """Remove a retraining schedule."""
        
        if schedule_id in self.schedules:
            schedule = self.schedules[schedule_id]
            del self.schedules[schedule_id]
            
            self.logger.info(f"Removed retraining schedule: {schedule.schedule_name}")
            return True
        
        return False
    
    def update_schedule(self, schedule_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing schedule."""
        
        if schedule_id not in self.schedules:
            return False
        
        schedule = self.schedules[schedule_id]
        
        # Apply updates
        for key, value in updates.items():
            if hasattr(schedule, key):
                setattr(schedule, key, value)
        
        # Recalculate next run if schedule changed
        if any(key in updates for key in ["cron_expression", "interval_hours", "enabled"]):
            schedule.next_run = self._calculate_next_run(schedule)
        
        self.logger.info(f"Updated retraining schedule: {schedule.schedule_name}")
        return True
    
    async def schedule_immediate_job(
        self,
        strategy_name: str,
        trigger_conditions: List[TriggerCondition],
        priority: SchedulePriority = SchedulePriority.HIGH,
        retraining_strategy: RetrainingStrategy = RetrainingStrategy.FULL_RETRAINING,
        configuration: Optional[RetrainingConfiguration] = None
    ) -> str:
        """Schedule an immediate retraining job."""
        
        job_id = str(uuid.uuid4())
        
        scheduled_job = ScheduledJob(
            job_id=job_id,
            schedule_id="immediate",
            strategy_name=strategy_name,
            priority=priority,
            scheduled_time=datetime.now(),
            retraining_strategy=retraining_strategy,
            configuration=configuration or RetrainingConfiguration(),
            trigger_conditions=trigger_conditions
        )
        
        await self._enqueue_job(scheduled_job)
        
        self.logger.info(
            f"Scheduled immediate retraining job for {strategy_name}",
            extra={
                "job_id": job_id,
                "priority": priority.name,
                "trigger_count": len(trigger_conditions)
            }
        )
        
        return job_id
    
    async def _schedule_check_loop(self) -> None:
        """Check for scheduled jobs that need to be queued."""
        
        while self.scheduler_running:
            try:
                current_time = datetime.now()
                
                for schedule in self.schedules.values():
                    if not schedule.enabled:
                        continue
                    
                    if (schedule.next_run and 
                        current_time >= schedule.next_run and
                        schedule.strategy_name not in self.strategy_locks):
                        
                        # Create scheduled job
                        await self._create_scheduled_job(schedule)
                        
                        # Update schedule for next run
                        schedule.last_run = current_time
                        schedule.next_run = self._calculate_next_run(schedule)
                
                await asyncio.sleep(self.check_interval)
                
            except Exception as e:
                self.logger.error(f"Error in schedule check loop: {e}", exc_info=True)
                await asyncio.sleep(60)
    
    async def _trigger_check_loop(self) -> None:
        """Check for trigger conditions that need retraining."""
        
        while self.scheduler_running:
            try:
                # Get strategies to check
                strategies = list(self.schedules.keys()) if self.schedules else []
                
                # Also check strategies with recent activity
                active_strategies = await self._get_active_strategies()
                strategies.extend([s for s in active_strategies if s not in strategies])
                
                for strategy in strategies:
                    if strategy in self.strategy_locks:
                        continue  # Skip if already being retrained
                    
                    # Check for new triggers
                    triggers = await self.trigger_service.check_triggers_for_strategy(strategy)
                    
                    if triggers:
                        # Determine priority based on trigger types and severity
                        priority = self._determine_trigger_priority(triggers)
                        
                        # Schedule triggered job
                        await self.schedule_immediate_job(
                            strategy_name=strategy,
                            trigger_conditions=triggers,
                            priority=priority,
                            retraining_strategy=RetrainingStrategy.FULL_RETRAINING
                        )
                
                await asyncio.sleep(self.check_interval * 2)  # Check triggers less frequently
                
            except Exception as e:
                self.logger.error(f"Error in trigger check loop: {e}", exc_info=True)
                await asyncio.sleep(300)
    
    async def _job_execution_loop(self) -> None:
        """Execute jobs from the queue."""
        
        while self.scheduler_running:
            try:
                # Check if we can run more jobs
                if len(self.running_jobs) >= self.max_concurrent_jobs:
                    await asyncio.sleep(30)
                    continue
                
                # Get next job from queue
                if not self.job_queue:
                    await asyncio.sleep(60)
                    continue
                
                # Pop highest priority job
                scheduled_job = heapq.heappop(self.job_queue)
                
                # Check if strategy is locked
                if scheduled_job.strategy_name in self.strategy_locks:
                    # Put job back in queue for later
                    heapq.heappush(self.job_queue, scheduled_job)
                    await asyncio.sleep(60)
                    continue
                
                # Execute the job
                await self._execute_scheduled_job(scheduled_job)
                
            except Exception as e:
                self.logger.error(f"Error in job execution loop: {e}", exc_info=True)
                await asyncio.sleep(60)
    
    async def _job_monitoring_loop(self) -> None:
        """Monitor running jobs for completion."""
        
        while self.scheduler_running:
            try:
                completed_jobs = []
                
                for job_id, scheduled_job in self.running_jobs.items():
                    # Check job status in retraining engine
                    if scheduled_job.retraining_job_id:
                        retraining_job = self.retraining_engine.get_job_status(
                            scheduled_job.retraining_job_id
                        )
                        
                        if retraining_job:
                            if retraining_job.status in ["completed", "failed", "cancelled"]:
                                # Job completed
                                scheduled_job.status = retraining_job.status.value
                                scheduled_job.completed_at = datetime.now()
                                
                                if retraining_job.status.value == "failed":
                                    scheduled_job.error_message = retraining_job.error_message
                                
                                completed_jobs.append(job_id)
                
                # Move completed jobs to history
                for job_id in completed_jobs:
                    scheduled_job = self.running_jobs[job_id]
                    del self.running_jobs[job_id]
                    
                    # Release strategy lock
                    if scheduled_job.strategy_name in self.strategy_locks:
                        self.strategy_locks.remove(scheduled_job.strategy_name)
                    
                    # Add to completed jobs
                    self.completed_jobs.append(scheduled_job)
                    
                    # Keep only recent history
                    if len(self.completed_jobs) > 100:
                        self.completed_jobs = self.completed_jobs[-100:]
                    
                    # Resolve triggers if job was successful
                    if scheduled_job.status == "completed":
                        for trigger in scheduled_job.trigger_conditions:
                            self.trigger_service.resolve_trigger(trigger.trigger_id)
                    
                    self.logger.info(
                        f"Scheduled job {scheduled_job.status}: {job_id}",
                        extra={
                            "strategy": scheduled_job.strategy_name,
                            "status": scheduled_job.status
                        }
                    )
                
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                self.logger.error(f"Error in job monitoring loop: {e}", exc_info=True)
                await asyncio.sleep(60)
    
    async def _create_scheduled_job(self, schedule: RetrainingSchedule) -> None:
        """Create a scheduled job from a schedule."""
        
        job_id = str(uuid.uuid4())
        
        scheduled_job = ScheduledJob(
            job_id=job_id,
            schedule_id=schedule.schedule_id,
            strategy_name=schedule.strategy_name,
            priority=schedule.priority,
            scheduled_time=schedule.next_run or datetime.now(),
            retraining_strategy=schedule.retraining_strategy,
            configuration=schedule.configuration or RetrainingConfiguration()
        )
        
        await self._enqueue_job(scheduled_job)
        
        self.logger.info(
            f"Created scheduled job from schedule: {schedule.schedule_name}",
            extra={"job_id": job_id, "schedule_id": schedule.schedule_id}
        )
    
    async def _enqueue_job(self, scheduled_job: ScheduledJob) -> None:
        """Add a job to the priority queue."""
        
        heapq.heappush(self.job_queue, scheduled_job)
        
        self.logger.debug(
            f"Enqueued job {scheduled_job.job_id}",
            extra={
                "strategy": scheduled_job.strategy_name,
                "priority": scheduled_job.priority.name,
                "queue_size": len(self.job_queue)
            }
        )
    
    async def _execute_scheduled_job(self, scheduled_job: ScheduledJob) -> None:
        """Execute a scheduled job."""
        
        scheduled_job.status = "running"
        scheduled_job.started_at = datetime.now()
        
        # Add to running jobs and lock strategy
        self.running_jobs[scheduled_job.job_id] = scheduled_job
        self.strategy_locks.add(scheduled_job.strategy_name)
        
        try:
            # Start retraining job
            retraining_job = await self.retraining_engine.trigger_retraining(
                strategy_name=scheduled_job.strategy_name,
                trigger_conditions=scheduled_job.trigger_conditions,
                retraining_strategy=scheduled_job.retraining_strategy,
                configuration=scheduled_job.configuration
            )
            
            scheduled_job.retraining_job_id = retraining_job.job_id
            
            self.logger.info(
                f"Started retraining job for scheduled job {scheduled_job.job_id}",
                extra={
                    "scheduled_job_id": scheduled_job.job_id,
                    "retraining_job_id": retraining_job.job_id,
                    "strategy": scheduled_job.strategy_name
                }
            )
            
        except Exception as e:
            # Job failed to start
            scheduled_job.status = "failed"
            scheduled_job.completed_at = datetime.now()
            scheduled_job.error_message = str(e)
            
            # Remove from running jobs and release lock
            if scheduled_job.job_id in self.running_jobs:
                del self.running_jobs[scheduled_job.job_id]
            
            if scheduled_job.strategy_name in self.strategy_locks:
                self.strategy_locks.remove(scheduled_job.strategy_name)
            
            self.completed_jobs.append(scheduled_job)
            
            self.logger.error(
                f"Failed to start retraining job for scheduled job {scheduled_job.job_id}: {e}",
                exc_info=True
            )
    
    def _calculate_next_run(self, schedule: RetrainingSchedule) -> Optional[datetime]:
        """Calculate the next run time for a schedule."""
        
        if not schedule.enabled:
            return None
        
        current_time = datetime.now()
        
        if schedule.schedule_type == ScheduleType.CRON and schedule.cron_expression:
            try:
                cron = croniter.croniter(schedule.cron_expression, current_time)
                return cron.get_next(datetime)
            except Exception as e:
                self.logger.error(f"Invalid cron expression for schedule {schedule.schedule_id}: {e}")
                return None
        
        elif schedule.schedule_type == ScheduleType.INTERVAL and schedule.interval_hours:
            if schedule.last_run:
                return schedule.last_run + timedelta(hours=schedule.interval_hours)
            else:
                return current_time + timedelta(hours=schedule.interval_hours)
        
        return None
    
    def _determine_trigger_priority(self, triggers: List[TriggerCondition]) -> SchedulePriority:
        """Determine priority based on trigger conditions."""
        
        # Check for critical triggers
        if any(t.severity.value == "critical" for t in triggers):
            return SchedulePriority.CRITICAL
        
        # Check for high severity or performance degradation
        if any(t.severity.value == "high" for t in triggers):
            return SchedulePriority.HIGH
        
        # Check for performance degradation specifically
        if any(t.trigger_type.value == "performance_degradation" for t in triggers):
            return SchedulePriority.HIGH
        
        return SchedulePriority.NORMAL
    
    async def _get_active_strategies(self) -> List[str]:
        """Get list of active strategies."""
        
        # This would typically query the database for strategies with recent activity
        # For now, return a default list
        return ["sharp_action", "line_movement", "consensus"]
    
    # Public API methods
    
    def get_schedules(self) -> List[RetrainingSchedule]:
        """Get all retraining schedules."""
        return list(self.schedules.values())
    
    def get_schedule(self, schedule_id: str) -> Optional[RetrainingSchedule]:
        """Get a specific schedule."""
        return self.schedules.get(schedule_id)
    
    def get_job_queue(self) -> List[ScheduledJob]:
        """Get current job queue."""
        return sorted(self.job_queue, key=lambda x: (x.priority.value, x.scheduled_time))
    
    def get_running_jobs(self) -> List[ScheduledJob]:
        """Get currently running jobs."""
        return list(self.running_jobs.values())
    
    def get_completed_jobs(self, limit: int = 20) -> List[ScheduledJob]:
        """Get completed job history."""
        return self.completed_jobs[-limit:]
    
    def get_job_status(self, job_id: str) -> Optional[ScheduledJob]:
        """Get status of a specific job."""
        
        # Check running jobs
        if job_id in self.running_jobs:
            return self.running_jobs[job_id]
        
        # Check completed jobs
        for job in self.completed_jobs:
            if job.job_id == job_id:
                return job
        
        # Check queue
        for job in self.job_queue:
            if job.job_id == job_id:
                return job
        
        return None
    
    async def cancel_job(self, job_id: str) -> bool:
        """Cancel a scheduled or running job."""
        
        # Check if job is in queue
        for i, job in enumerate(self.job_queue):
            if job.job_id == job_id:
                # Remove from queue
                self.job_queue.pop(i)
                heapq.heapify(self.job_queue)  # Restore heap property
                
                job.status = "cancelled"
                job.completed_at = datetime.now()
                self.completed_jobs.append(job)
                
                self.logger.info(f"Cancelled queued job: {job_id}")
                return True
        
        # Check if job is running
        if job_id in self.running_jobs:
            scheduled_job = self.running_jobs[job_id]
            
            # Cancel the retraining job
            if scheduled_job.retraining_job_id:
                await self.retraining_engine.cancel_job(scheduled_job.retraining_job_id)
            
            # Update scheduled job status
            scheduled_job.status = "cancelled"
            scheduled_job.completed_at = datetime.now()
            
            # Move to completed and release lock
            del self.running_jobs[job_id]
            if scheduled_job.strategy_name in self.strategy_locks:
                self.strategy_locks.remove(scheduled_job.strategy_name)
            
            self.completed_jobs.append(scheduled_job)
            
            self.logger.info(f"Cancelled running job: {job_id}")
            return True
        
        return False
    
    def get_scheduler_status(self) -> Dict[str, Any]:
        """Get comprehensive scheduler status."""
        
        # Queue statistics
        queue_by_priority = {}
        for job in self.job_queue:
            priority = job.priority.name
            queue_by_priority[priority] = queue_by_priority.get(priority, 0) + 1
        
        # Schedule statistics
        schedules_by_type = {}
        enabled_schedules = 0
        for schedule in self.schedules.values():
            schedule_type = schedule.schedule_type.value
            schedules_by_type[schedule_type] = schedules_by_type.get(schedule_type, 0) + 1
            if schedule.enabled:
                enabled_schedules += 1
        
        # Recent activity
        recent_completions = len([
            job for job in self.completed_jobs
            if job.completed_at and (datetime.now() - job.completed_at).hours <= 24
        ])
        
        return {
            "scheduler_running": self.scheduler_running,
            "schedules": {
                "total": len(self.schedules),
                "enabled": enabled_schedules,
                "by_type": schedules_by_type
            },
            "jobs": {
                "queued": len(self.job_queue),
                "running": len(self.running_jobs),
                "completed_24h": recent_completions,
                "queue_by_priority": queue_by_priority
            },
            "resources": {
                "max_concurrent_jobs": self.max_concurrent_jobs,
                "locked_strategies": list(self.strategy_locks),
                "available_slots": self.max_concurrent_jobs - len(self.running_jobs)
            },
            "configuration": {
                "check_interval_minutes": self.check_interval / 60,
                "max_concurrent_jobs": self.max_concurrent_jobs
            }
        }