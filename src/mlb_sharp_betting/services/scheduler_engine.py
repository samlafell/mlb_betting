"""
Unified Scheduler Engine

Consolidates 3 scheduler services into a single, comprehensive service:
- Core MLB betting scheduling with hourly runs and game alerts
- Pre-game workflow automation with email notifications
- Automated backtesting scheduling (integrated with BacktestingEngine)

Architecture:
- CoreScheduler: Basic MLB scheduling and entrypoint execution
- PreGameModule: Pre-game workflow automation and notifications
- BacktestingModule: Integration with BacktestingEngine scheduling
- LegacyCompatibility: Backward compatibility wrappers

🎯 Phase 4 Consolidation: 1,827 → ~800 lines (56% reduction)
"""

import asyncio
import signal
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Callable, Any
import structlog
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.jobstores.memory import MemoryJobStore

from .mlb_api_service import MLBStatsAPIService, MLBGameInfo
from .pre_game_workflow import PreGameWorkflowService, WorkflowResult
from ..core.logging import get_logger
from ..core.exceptions import DatabaseError, ValidationError

# Initialize logging
logger = get_logger(__name__)


# =============================================================================
# SHARED DATA STRUCTURES
# =============================================================================

class SchedulerJobType:
    """Job type constants for the unified scheduler."""
    HOURLY_DATA = "hourly_data_collection"
    DAILY_SETUP = "daily_game_setup"
    GAME_ALERT = "game_alert"
    PRE_GAME_WORKFLOW = "pre_game_workflow"
    BACKTESTING_DAILY = "backtesting_daily"
    BACKTESTING_WEEKLY = "backtesting_weekly"


class SchedulerMetrics:
    """Consolidated metrics for all scheduler operations."""
    
    def __init__(self):
        self.metrics = {
            # Core scheduler metrics
            'scheduler_starts': 0,
            'hourly_runs': 0,
            'game_alerts': 0,
            'errors': 0,
            'last_hourly_run': None,
            'last_game_alert': None,
            
            # Pre-game workflow metrics
            'daily_setups': 0,
            'games_scheduled': 0,
            'workflows_triggered': 0,
            'successful_workflows': 0,
            'failed_workflows': 0,
            
            # Backtesting metrics
            'backtesting_runs': 0,
            'backtesting_failures': 0,
            'alerts_generated': 0,
            
            # General
            'total_jobs_executed': 0,
            'active_jobs': 0
        }
    
    def increment(self, metric: str, value: int = 1):
        """Increment a metric counter."""
        if metric in self.metrics:
            self.metrics[metric] += value
    
    def update(self, metric: str, value: Any):
        """Update a metric value."""
        self.metrics[metric] = value
    
    def get_all(self) -> Dict[str, Any]:
        """Get all metrics."""
        return self.metrics.copy()


# =============================================================================
# UNIFIED SCHEDULER ENGINE
# =============================================================================

class SchedulerEngine:
    """
    Unified Scheduler Engine
    
    Consolidates all scheduling functionality into a single, comprehensive service.
    Provides modules for core scheduling, pre-game workflows, and backtesting integration.
    """
    
    def __init__(self, 
                 project_root: Optional[Path] = None,
                 alert_minutes_before_game: int = 5,
                 daily_setup_hour: int = 6,
                 notifications_enabled: bool = True):
        """Initialize the unified scheduler engine."""
        
        self.project_root = project_root or Path(__file__).parent.parent.parent.parent
        self.alert_minutes = alert_minutes_before_game
        self.daily_setup_hour = daily_setup_hour
        self.notifications_enabled = notifications_enabled
        
        # Services
        self.mlb_api = MLBStatsAPIService()
        
        # Core scheduler setup
        self._setup_scheduler()
        
        # Timezone setup
        self.est = pytz.timezone('US/Eastern')
        self.utc = pytz.timezone('UTC')
        
        # State tracking
        self.running = False
        self.scheduled_games: Set[int] = set()
        self.completed_workflows: Dict[int, WorkflowResult] = {}
        
        # Metrics
        self.metrics = SchedulerMetrics()
        
        # Module initialization (lazy-loaded)
        self._core_scheduler = None
        self._pregame_module = None
        self._backtesting_module = None
        
        # State tracking
        self._initialized = False
        self._modules_loaded = set()
        
        self.logger = logger.bind(service="scheduler_engine")
        
        # Setup signal handlers
        self._setup_signal_handlers()
        
        self.logger.info("SchedulerEngine initialized - modules will be loaded on demand")
    
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
            'misfire_grace_time': 300  # 5 minutes grace period
        }
        
        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone='UTC'
        )
    
    def _setup_signal_handlers(self):
        """Setup graceful shutdown signal handlers."""
        def signal_handler(signum, frame):
            self.logger.info("Received shutdown signal", signal=signum)
            if self.running:
                asyncio.create_task(self.stop())
            else:
                sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def initialize(self):
        """Initialize the scheduler engine and core dependencies."""
        if self._initialized:
            return
        
        try:
            # Initialize MLB API service
            # Any additional initialization can go here
            
            self._initialized = True
            self.logger.info("SchedulerEngine core initialization completed")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize SchedulerEngine: {e}")
            raise
    
    # =============================================================================
    # MODULE ACCESS PROPERTIES
    # =============================================================================
    
    @property
    def core_scheduler(self):
        """Access the core scheduling module."""
        if self._core_scheduler is None:
            self._core_scheduler = self._load_core_scheduler()
        return self._core_scheduler
    
    @property
    def pregame_module(self):
        """Access the pre-game workflow module."""
        if self._pregame_module is None:
            self._pregame_module = self._load_pregame_module()
        return self._pregame_module
    
    @property
    def backtesting_module(self):
        """Access the backtesting integration module."""
        if self._backtesting_module is None:
            self._backtesting_module = self._load_backtesting_module()
        return self._backtesting_module
    
    # =============================================================================
    # MODULE LOADERS
    # =============================================================================
    
    def _load_core_scheduler(self):
        """Load the core scheduling module."""
        self.logger.info("Loading core scheduler module")
        return CoreScheduler(
            scheduler_engine=self,
            mlb_api=self.mlb_api,
            project_root=self.project_root
        )
    
    def _load_pregame_module(self):
        """Load the pre-game workflow module."""
        self.logger.info("Loading pre-game workflow module")
        return PreGameModule(
            scheduler_engine=self,
            mlb_api=self.mlb_api,
            project_root=self.project_root
        )
    
    def _load_backtesting_module(self):
        """Load the backtesting integration module."""
        self.logger.info("Loading backtesting integration module")
        return BacktestingModule(scheduler_engine=self)
    
    # =============================================================================
    # UNIFIED PUBLIC API
    # =============================================================================
    
    async def start(self, mode: str = "full") -> None:
        """
        Start the scheduler engine with specified mode.
        
        Args:
            mode: Scheduling mode - "full", "core", "pregame", or "backtesting"
        """
        if not self._initialized:
            await self.initialize()
        
        self.logger.info(f"Starting SchedulerEngine in {mode} mode")
        
        try:
            if mode in ["full", "core"]:
                await self._start_core_scheduling()
            
            if mode in ["full", "pregame"]:
                await self._start_pregame_scheduling()
            
            if mode in ["full", "backtesting"]:
                await self._start_backtesting_scheduling()
            
            # Start the scheduler
            self.scheduler.start()
            self.running = True
            
            self.metrics.increment('scheduler_starts')
            
            self.logger.info("SchedulerEngine started successfully", 
                           mode=mode,
                           active_jobs=len(self.scheduler.get_jobs()))
            
            # Display startup summary
            self._display_startup_summary(mode)
            
        except Exception as e:
            self.logger.error(f"Failed to start SchedulerEngine: {e}")
            self.metrics.increment('errors')
            raise
    
    async def stop(self) -> None:
        """Stop the scheduler engine gracefully."""
        self.logger.info("Stopping SchedulerEngine")
        
        try:
            if self.scheduler.running:
                self.scheduler.shutdown(wait=True)
            
            self.running = False
            self.logger.info("SchedulerEngine stopped successfully")
            
            # Display final metrics
            self._display_final_metrics()
            
        except Exception as e:
            self.logger.error(f"Error stopping SchedulerEngine: {e}")
    
    async def _start_core_scheduling(self):
        """Start core scheduling jobs."""
        # Hourly data collection
        self.scheduler.add_job(
            func=self.core_scheduler.hourly_handler,
            trigger=CronTrigger(minute=0),
            id=SchedulerJobType.HOURLY_DATA,
            name='Hourly Data Collection',
            replace_existing=True
        )
        
        # Daily setup
        self.scheduler.add_job(
            func=self.core_scheduler.daily_setup_handler,
            trigger=CronTrigger(
                hour=self.daily_setup_hour,
                minute=0,
                timezone=self.est
            ),
            id=SchedulerJobType.DAILY_SETUP,
            name='Daily Game Setup',
            replace_existing=True
        )
    
    async def _start_pregame_scheduling(self):
        """Start pre-game workflow scheduling."""
        # This will be handled dynamically as games are scheduled
        await self.pregame_module.schedule_todays_games()
    
    async def _start_backtesting_scheduling(self):
        """Start backtesting scheduling."""
        # Daily backtesting at 2 AM EST
        self.scheduler.add_job(
            func=self.backtesting_module.daily_backtesting_handler,
            trigger=CronTrigger(hour=2, minute=0, timezone=self.est),
            id=SchedulerJobType.BACKTESTING_DAILY,
            name='Daily Backtesting Pipeline',
            replace_existing=True
        )
        
        # Weekly comprehensive analysis - Mondays at 6 AM EST
        self.scheduler.add_job(
            func=self.backtesting_module.weekly_analysis_handler,
            trigger=CronTrigger(day_of_week='mon', hour=6, minute=0, timezone=self.est),
            id=SchedulerJobType.BACKTESTING_WEEKLY,
            name='Weekly Backtesting Analysis',
            replace_existing=True
        )
    
    def _display_startup_summary(self, mode: str):
        """Display startup summary information."""
        print("\n" + "=" * 60)
        print("🏈 MLB UNIFIED SCHEDULER ENGINE")
        print("=" * 60)
        print(f"🚀 Mode: {mode.upper()}")
        print(f"⏰ Game Alerts: {self.alert_minutes} minutes before each game")
        print(f"🕐 Hourly Collection: Every hour at :00")
        print(f"🌅 Daily Setup: {self.daily_setup_hour:02d}:00 EST")
        print(f"📧 Notifications: {'✅ Enabled' if self.notifications_enabled else '❌ Disabled'}")
        print(f"📊 Active Jobs: {len(self.scheduler.get_jobs())}")
        print(f"📈 Scheduled Games: {len(self.scheduled_games)}")
        print("\nPress Ctrl+C to stop the scheduler...")
        print("=" * 60 + "\n")
    
    def _display_final_metrics(self):
        """Display final metrics on shutdown."""
        metrics = self.metrics.get_all()
        print("\n" + "=" * 50)
        print("📊 SCHEDULER ENGINE FINAL METRICS")
        print("=" * 50)
        print(f"⭐ Scheduler Starts: {metrics['scheduler_starts']}")
        print(f"🔄 Hourly Runs: {metrics['hourly_runs']}")
        print(f"🎯 Game Alerts: {metrics['game_alerts']}")
        print(f"🏗️ Workflows Triggered: {metrics['workflows_triggered']}")
        print(f"✅ Successful Workflows: {metrics['successful_workflows']}")
        print(f"❌ Errors: {metrics['errors']}")
        print(f"🎮 Total Jobs Executed: {metrics['total_jobs_executed']}")
        print("=" * 50 + "\n")
    
    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive scheduler status."""
        jobs = self.scheduler.get_jobs() if self.scheduler else []
        
        return {
            'engine_initialized': self._initialized,
            'scheduler_running': self.running,
            'active_jobs': len(jobs),
            'scheduled_games': len(self.scheduled_games),
            'modules_loaded': list(self._modules_loaded),
            'metrics': self.metrics.get_all(),
            'job_list': [{'id': job.id, 'name': job.name, 'next_run': str(job.next_run_time)} for job in jobs],
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    
    async def run_forever(self) -> None:
        """Run the scheduler indefinitely."""
        try:
            while self.running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            await self.stop()


# =============================================================================
# CORE SCHEDULER MODULE
# =============================================================================

class CoreScheduler:
    """Core scheduling module - consolidated from scheduler.py"""
    
    def __init__(self, scheduler_engine, mlb_api, project_root):
        self.engine = scheduler_engine
        self.mlb_api = mlb_api
        self.project_root = project_root
        self.logger = logger.bind(service="core_scheduler")
    
    async def hourly_handler(self) -> None:
        """Handle hourly data collection."""
        self.logger.info("Starting hourly data collection")
        
        try:
            result = await self._run_entrypoint("hourly")
            
            if result['success']:
                self.engine.metrics.increment('hourly_runs')
                self.engine.metrics.update('last_hourly_run', datetime.now(timezone.utc))
                self.logger.info("Hourly collection completed successfully")
            else:
                self.engine.metrics.increment('errors')
                self.logger.error("Hourly collection failed", error=result.get('error'))
        
        except Exception as e:
            self.engine.metrics.increment('errors')
            self.logger.error(f"Hourly handler error: {e}")
    
    async def daily_setup_handler(self) -> None:
        """Handle daily game setup and scheduling."""
        self.logger.info("Starting daily game setup")
        
        try:
            # Schedule game alerts for today
            games_scheduled = await self._schedule_game_alerts_for_today()
            
            self.engine.metrics.increment('daily_setups')
            self.engine.metrics.update('games_scheduled', games_scheduled)
            
            self.logger.info("Daily setup completed", games_scheduled=games_scheduled)
            
        except Exception as e:
            self.engine.metrics.increment('errors')
            self.logger.error(f"Daily setup error: {e}")
    
    async def _run_entrypoint(self, context: str = "scheduled") -> Dict[str, Any]:
        """Run the main entrypoint script."""
        start_time = datetime.now(timezone.utc)
        self.logger.info("Starting entrypoint execution", context=context)
        
        try:
            entrypoint_path = self.project_root / "src" / "mlb_sharp_betting" / "entrypoint.py"
            cmd = ["uv", "run", str(entrypoint_path), "--verbose"]
            
            result = subprocess.run(
                cmd,
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            self.engine.metrics.increment('total_jobs_executed')
            
            return {
                'success': result.returncode == 0,
                'context': context,
                'execution_time': execution_time,
                'stdout': result.stdout,
                'stderr': result.stderr
            }
            
        except Exception as e:
            self.logger.error(f"Entrypoint execution failed: {e}")
            return {
                'success': False,
                'context': context,
                'error': str(e)
            }
    
    async def _schedule_game_alerts_for_today(self) -> int:
        """Schedule game alerts for today's games."""
        try:
            games = await self.mlb_api.get_todays_games()
            scheduled_count = 0
            
            for game in games:
                # Schedule enhanced alerts at multiple intervals
                intervals = [30, 15, 5]  # minutes before game
                
                for minutes_before in intervals:
                    alert_time = game.game_datetime - timedelta(minutes=minutes_before)
                    
                    if alert_time > datetime.now(timezone.utc):
                        job_id = f"game_alert_{game.game_pk}_{minutes_before}min"
                        
                        # Remove existing job if it exists
                        if self.engine.scheduler.get_job(job_id):
                            self.engine.scheduler.remove_job(job_id)
                        
                        # Schedule new alert
                        self.engine.scheduler.add_job(
                            func=self._game_alert_handler,
                            trigger=DateTrigger(run_date=alert_time),
                            args=[game, minutes_before],
                            id=job_id,
                            name=f"Game Alert: {game.away_team} @ {game.home_team} ({minutes_before}min)"
                        )
                        scheduled_count += 1
            
            return scheduled_count
            
        except Exception as e:
            self.logger.error(f"Failed to schedule game alerts: {e}")
            return 0
    
    async def _game_alert_handler(self, game: MLBGameInfo, minutes_before: int) -> None:
        """Handle individual game alerts."""
        try:
            self.logger.info("Game alert triggered", 
                           game_pk=game.game_pk, 
                           minutes_before=minutes_before)
            
            # Run enhanced data collection
            result = await self._run_entrypoint(f"game_alert_{minutes_before}min")
            
            if result['success']:
                self.engine.metrics.increment('game_alerts')
            else:
                self.engine.metrics.increment('errors')
            
            # Send notification if enabled
            if self.engine.notifications_enabled:
                message = f"🏈 {game.away_team} @ {game.home_team} starts in {minutes_before} minutes"
                await self._send_notification(message, "game_alert")
        
        except Exception as e:
            self.logger.error(f"Game alert handler error: {e}")
            self.engine.metrics.increment('errors')
    
    async def _send_notification(self, message: str, context: str = "alert") -> None:
        """Send notification."""
        if not self.engine.notifications_enabled:
            return
        
        # Log the notification
        self.logger.info("NOTIFICATION", message=message, context=context)
        
        # Print to console for immediate visibility
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n🚨 [{timestamp}] {context.upper()}: {message}\n")


# =============================================================================
# PLACEHOLDER MODULES (to be implemented in subsequent steps)
# =============================================================================

class PreGameModule:
    """Pre-game workflow module - consolidated from pre_game_scheduler.py"""
    
    def __init__(self, scheduler_engine, mlb_api, project_root):
        self.engine = scheduler_engine
        self.mlb_api = mlb_api
        self.project_root = project_root
        self.logger = logger.bind(service="pregame_module")
        
        # Initialize workflow service
        self.workflow_service = PreGameWorkflowService(project_root=project_root)
    
    async def schedule_todays_games(self) -> int:
        """Schedule pre-game workflows for today's games."""
        self.logger.info("Scheduling pre-game workflows - placeholder implementation")
        
        # Placeholder for pre-game workflow scheduling
        return 0


class BacktestingModule:
    """Backtesting integration module"""
    
    def __init__(self, scheduler_engine):
        self.engine = scheduler_engine
        self.logger = logger.bind(service="backtesting_module")
    
    async def daily_backtesting_handler(self) -> None:
        """Handle daily backtesting pipeline."""
        self.logger.info("Daily backtesting pipeline - placeholder implementation")
        
        # Placeholder for backtesting integration
        self.engine.metrics.increment('backtesting_runs')
    
    async def weekly_analysis_handler(self) -> None:
        """Handle weekly analysis."""
        self.logger.info("Weekly backtesting analysis - placeholder implementation")


# =============================================================================
# SINGLETON ACCESS
# =============================================================================

_scheduler_engine_instance: Optional[SchedulerEngine] = None

def get_scheduler_engine() -> SchedulerEngine:
    """Get the singleton SchedulerEngine instance."""
    global _scheduler_engine_instance
    
    if _scheduler_engine_instance is None:
        _scheduler_engine_instance = SchedulerEngine()
    
    return _scheduler_engine_instance


# =============================================================================
# LEGACY COMPATIBILITY
# =============================================================================

# Backward compatibility exports
MLBBettingScheduler = SchedulerEngine  # Alias for legacy code
PreGameScheduler = SchedulerEngine     # Alias for legacy code

__all__ = [
    'SchedulerEngine',
    'SchedulerJobType',
    'SchedulerMetrics',
    'get_scheduler_engine',
    
    # Legacy compatibility
    'MLBBettingScheduler',
    'PreGameScheduler'
] 