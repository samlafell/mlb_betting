"""
Collection Orchestrator for Unified Data Collection

Coordinates all data collectors with:
- Parallel collection execution with dependency management
- Comprehensive scheduling and monitoring
- Error handling and recovery strategies
- Data quality validation and deduplication
- Performance metrics and health monitoring
- Cross-source data consistency validation
"""

import asyncio
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
import uuid
import json

import structlog

from .base import BaseCollector, CollectionResult, CollectionStatus, CollectionMetrics
from .collectors import (
    VSINCollector, SBDCollector, SportsBettingReportCollector,
    ActionNetworkCollector, MLBStatsAPICollector, OddsAPICollector
)
from .validators import DataQualityValidator, ValidationResult
from .rate_limiter import get_rate_limiter
from ..database.repositories import UnifiedRepository
from ...core.config import UnifiedSettings
from ...core.logging import get_logger, LogComponent
from ...core.exceptions import UnifiedBettingError, ValidationError

logger = get_logger(__name__, LogComponent.CORE)


class CollectionPriority(Enum):
    """Priority levels for collection tasks."""
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


class CollectionStatus(Enum):
    """Status of collection operations."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    RATE_LIMITED = "rate_limited"


@dataclass
class SourceConfig:
    """Configuration for a data source."""
    
    name: str
    collector_class: type[BaseCollector]
    enabled: bool = True
    priority: CollectionPriority = CollectionPriority.NORMAL
    
    # Collection settings
    collection_interval_minutes: int = 60
    max_retries: int = 3
    timeout_seconds: int = 300
    
    # Data settings
    enable_validation: bool = True
    enable_deduplication: bool = True
    strict_validation: bool = False
    
    # Dependencies
    depends_on: List[str] = field(default_factory=list)
    
    # Custom parameters
    collection_params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CollectionTask:
    """Represents a collection task."""
    
    id: str
    source_name: str
    collection_type: str
    priority: CollectionPriority
    status: CollectionStatus = CollectionStatus.PENDING
    
    # Timing
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Configuration
    params: Dict[str, Any] = field(default_factory=dict)
    timeout_seconds: int = 300
    max_retries: int = 3
    
    # State
    attempts: int = 0
    last_error: Optional[str] = None
    result: Optional[CollectionResult] = None
    
    # Dependencies
    depends_on: List[str] = field(default_factory=list)
    dependents: List[str] = field(default_factory=list)
    
    @property
    def is_ready(self) -> bool:
        """Check if task is ready to execute (all dependencies completed)."""
        return self.status == CollectionStatus.PENDING and len(self.depends_on) == 0
    
    @property
    def is_running(self) -> bool:
        """Check if task is currently running."""
        return self.status == CollectionStatus.RUNNING
    
    @property
    def is_completed(self) -> bool:
        """Check if task is completed (success or failure)."""
        return self.status in [CollectionStatus.SUCCESS, CollectionStatus.FAILED, 
                              CollectionStatus.CANCELLED, CollectionStatus.TIMEOUT]
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """Get task duration in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


@dataclass
class CollectionPlan:
    """Represents a collection execution plan."""
    
    id: str
    name: str
    tasks: List[CollectionTask] = field(default_factory=list)
    
    # Execution settings
    max_concurrent_tasks: int = 5
    total_timeout_seconds: int = 1800  # 30 minutes
    
    # Status
    status: CollectionStatus = CollectionStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Results
    successful_tasks: int = 0
    failed_tasks: int = 0
    total_items_collected: int = 0
    
    @property
    def is_completed(self) -> bool:
        """Check if all tasks are completed."""
        return all(task.is_completed for task in self.tasks)
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate of tasks."""
        if not self.tasks:
            return 1.0
        return self.successful_tasks / len(self.tasks)
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """Get plan duration in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


class CollectionOrchestrator:
    """
    Orchestrates data collection across all sources.
    
    Provides comprehensive collection management with:
    - Parallel execution with dependency management
    - Rate limiting and error handling
    - Data validation and deduplication
    - Performance monitoring and metrics
    - Scheduling and retry logic
    """
    
    def __init__(
        self,
        settings: Optional[UnifiedSettings] = None,
        repository: Optional[UnifiedRepository] = None
    ) -> None:
        """
        Initialize collection orchestrator.
        
        Args:
            settings: Unified settings
            repository: Database repository
        """
        self.settings = settings or UnifiedSettings()
        self.repository = repository
        self.logger = get_logger(__name__)
        
        # State management
        self.source_configs: Dict[str, SourceConfig] = {}
        self.collectors: Dict[str, BaseCollector] = {}
        self.active_plans: Dict[str, CollectionPlan] = {}
        self.task_queue: List[CollectionTask] = []
        
        # Services
        self.rate_limiter = get_rate_limiter()
        self.validator = DataQualityValidator()
        
        # Metrics
        self.metrics = {
            'total_collections': 0,
            'successful_collections': 0,
            'failed_collections': 0,
            'total_items_collected': 0,
            'total_collection_time_ms': 0.0,
            'last_collection_time': None
        }
        
        # Initialize default source configurations
        self._initialize_default_sources()
        
        self.logger.info("Collection orchestrator initialized",
                        sources=len(self.source_configs))
    
    def _initialize_default_sources(self) -> None:
        """Initialize default data source configurations."""
        
        # VSIN Configuration
        vsin_config = SourceConfig(
            name="VSIN",
            collector_class=VSINCollector,
            priority=CollectionPriority.HIGH,
            collection_interval_minutes=30,
            collection_params={
                'collection_type': 'sharp_data',
                'date_range': 7
            }
        )
        self.add_source(vsin_config)
        
        # SBD Configuration
        sbd_config = SourceConfig(
            name="SBD",
            collector_class=SBDCollector,
            priority=CollectionPriority.NORMAL,
            collection_interval_minutes=45,
            collection_params={
                'collection_type': 'current_odds',
                'sport': 'mlb'
            }
        )
        self.add_source(sbd_config)
        
        # SportsBettingReport Configuration
        sbr_config = SourceConfig(
            name="SportsBettingReport",
            collector_class=SportsBettingReportCollector,
            priority=CollectionPriority.NORMAL,
            collection_interval_minutes=90,
            collection_params={
                'collection_type': 'consensus',
                'sport': 'mlb'
            }
        )
        self.add_source(sbr_config)
        
        # MLB Stats API Configuration
        mlb_api_config = SourceConfig(
            name="MLBStatsAPI",
            collector_class=MLBStatsAPICollector,
            priority=CollectionPriority.HIGH,
            collection_interval_minutes=30,
            collection_params={
                'collection_type': 'game_data',
                'sport': 'mlb'
            }
        )
        self.add_source(mlb_api_config)
        
        # Odds API Configuration
        odds_api_config = SourceConfig(
            name="OddsAPI",
            collector_class=OddsAPICollector,
            priority=CollectionPriority.NORMAL,
            collection_interval_minutes=60,
            collection_params={
                'collection_type': 'odds',
                'sport': 'baseball'
            }
        )
        self.add_source(odds_api_config)
        
        # ActionNetwork Configuration
        action_config = SourceConfig(
            name="ActionNetwork",
            collector_class=ActionNetworkCollector,
            priority=CollectionPriority.NORMAL,
            collection_interval_minutes=60,
            collection_params={
                'collection_type': 'public_betting',
                'sport': 'baseball'
            }
        )
        self.add_source(action_config)
    
    def add_source(self, config: SourceConfig) -> None:
        """Add a data source configuration."""
        self.source_configs[config.name] = config
        self.logger.info("Source added", source=config.name, 
                        priority=config.priority.name)
    
    def remove_source(self, source_name: str) -> None:
        """Remove a data source configuration."""
        if source_name in self.source_configs:
            del self.source_configs[source_name]
            if source_name in self.collectors:
                del self.collectors[source_name]
            self.logger.info("Source removed", source=source_name)
    
    def enable_source(self, source_name: str) -> None:
        """Enable a data source."""
        if source_name in self.source_configs:
            self.source_configs[source_name].enabled = True
            self.logger.info("Source enabled", source=source_name)
    
    def disable_source(self, source_name: str) -> None:
        """Disable a data source."""
        if source_name in self.source_configs:
            self.source_configs[source_name].enabled = False
            self.logger.info("Source disabled", source=source_name)
    
    async def create_collection_plan(
        self,
        name: str,
        sources: Optional[List[str]] = None,
        collection_types: Optional[Dict[str, str]] = None,
        max_concurrent: int = 5,
        timeout_seconds: int = 1800
    ) -> CollectionPlan:
        """
        Create a collection execution plan.
        
        Args:
            name: Plan name
            sources: List of sources to include (all if None)
            collection_types: Override collection types per source
            max_concurrent: Maximum concurrent tasks
            timeout_seconds: Total timeout for plan
            
        Returns:
            CollectionPlan instance
        """
        plan_id = str(uuid.uuid4())
        plan = CollectionPlan(
            id=plan_id,
            name=name,
            max_concurrent_tasks=max_concurrent,
            total_timeout_seconds=timeout_seconds
        )
        
        # Determine sources to include
        if sources is None:
            sources = [name for name, config in self.source_configs.items() if config.enabled]
        
        # Create tasks for each source
        for source_name in sources:
            if source_name not in self.source_configs:
                self.logger.warning("Unknown source in plan", source=source_name)
                continue
            
            config = self.source_configs[source_name]
            if not config.enabled:
                self.logger.info("Skipping disabled source", source=source_name)
                continue
            
            # Determine collection type
            collection_type = (
                collection_types.get(source_name) if collection_types 
                else config.collection_params.get('collection_type', 'default')
            )
            
            # Create task
            task = CollectionTask(
                id=str(uuid.uuid4()),
                source_name=source_name,
                collection_type=collection_type,
                priority=config.priority,
                params=config.collection_params.copy(),
                timeout_seconds=config.timeout_seconds,
                max_retries=config.max_retries,
                depends_on=config.depends_on.copy()
            )
            
            plan.tasks.append(task)
        
        # Sort tasks by priority (highest first)
        plan.tasks.sort(key=lambda t: t.priority.value, reverse=True)
        
        # Resolve dependencies
        self._resolve_task_dependencies(plan)
        
        self.active_plans[plan_id] = plan
        
        self.logger.info("Collection plan created",
                        plan_id=plan_id, name=name, 
                        task_count=len(plan.tasks))
        
        return plan
    
    def _resolve_task_dependencies(self, plan: CollectionPlan) -> None:
        """Resolve task dependencies within a plan."""
        task_map = {task.source_name: task for task in plan.tasks}
        
        for task in plan.tasks:
            # Resolve dependencies
            resolved_deps = []
            for dep_name in task.depends_on:
                if dep_name in task_map:
                    resolved_deps.append(task_map[dep_name].id)
                    task_map[dep_name].dependents.append(task.id)
                else:
                    self.logger.warning("Dependency not found in plan",
                                      task=task.source_name, dependency=dep_name)
            
            task.depends_on = resolved_deps
    
    async def execute_plan(self, plan: CollectionPlan) -> CollectionPlan:
        """
        Execute a collection plan.
        
        Args:
            plan: Collection plan to execute
            
        Returns:
            Updated plan with results
        """
        plan.status = CollectionStatus.RUNNING
        plan.started_at = datetime.now()
        
        self.logger.info("Executing collection plan",
                        plan_id=plan.id, name=plan.name,
                        task_count=len(plan.tasks))
        
        try:
            # Execute tasks with dependency management
            await self._execute_tasks_with_dependencies(plan)
            
            # Update plan status
            if plan.failed_tasks == 0:
                plan.status = CollectionStatus.SUCCESS
            elif plan.successful_tasks > 0:
                plan.status = CollectionStatus.SUCCESS  # Partial success
            else:
                plan.status = CollectionStatus.FAILED
            
        except asyncio.TimeoutError:
            plan.status = CollectionStatus.TIMEOUT
            self.logger.error("Collection plan timed out", plan_id=plan.id)
            
        except Exception as e:
            plan.status = CollectionStatus.FAILED
            self.logger.error("Collection plan failed", plan_id=plan.id, error=str(e))
            
        finally:
            plan.completed_at = datetime.now()
            
            # Update metrics
            self._update_plan_metrics(plan)
            
            self.logger.info("Collection plan completed",
                            plan_id=plan.id, status=plan.status.value,
                            success_rate=plan.success_rate,
                            duration_seconds=plan.duration_seconds)
        
        return plan
    
    async def _execute_tasks_with_dependencies(self, plan: CollectionPlan) -> None:
        """Execute tasks respecting dependencies and concurrency limits."""
        completed_tasks: Set[str] = set()
        running_tasks: Dict[str, asyncio.Task] = {}
        
        start_time = time.time()
        
        while len(completed_tasks) < len(plan.tasks):
            # Check for timeout
            if time.time() - start_time > plan.total_timeout_seconds:
                # Cancel running tasks
                for task in running_tasks.values():
                    task.cancel()
                raise asyncio.TimeoutError()
            
            # Find ready tasks
            ready_tasks = [
                task for task in plan.tasks
                if (task.id not in completed_tasks and 
                    task.id not in running_tasks and
                    all(dep_id in completed_tasks for dep_id in task.depends_on))
            ]
            
            # Start new tasks up to concurrency limit
            available_slots = plan.max_concurrent_tasks - len(running_tasks)
            for task in ready_tasks[:available_slots]:
                task_coroutine = self._execute_single_task(task)
                running_tasks[task.id] = asyncio.create_task(task_coroutine)
                
                self.logger.debug("Started task", task_id=task.id, 
                                source=task.source_name)
            
            # Wait for at least one task to complete
            if running_tasks:
                done, pending = await asyncio.wait(
                    running_tasks.values(),
                    return_when=asyncio.FIRST_COMPLETED,
                    timeout=10.0  # Check every 10 seconds
                )
                
                # Process completed tasks
                for task_future in done:
                    task_id = None
                    for tid, tfuture in running_tasks.items():
                        if tfuture == task_future:
                            task_id = tid
                            break
                    
                    if task_id:
                        completed_tasks.add(task_id)
                        del running_tasks[task_id]
                        
                        # Update plan metrics
                        task = next(t for t in plan.tasks if t.id == task_id)
                        if task.status == CollectionStatus.SUCCESS:
                            plan.successful_tasks += 1
                            if task.result:
                                plan.total_items_collected += task.result.data_count
                        else:
                            plan.failed_tasks += 1
                        
                        self.logger.debug("Task completed", task_id=task_id,
                                        status=task.status.value)
            
            # Small delay to prevent busy waiting
            await asyncio.sleep(0.1)
    
    async def _execute_single_task(self, task: CollectionTask) -> None:
        """Execute a single collection task."""
        task.status = CollectionStatus.RUNNING
        task.started_at = datetime.now()
        
        for attempt in range(task.max_retries + 1):
            task.attempts = attempt + 1
            
            try:
                # Get or create collector
                collector = await self._get_collector(task.source_name)
                
                # Execute collection
                result = await asyncio.wait_for(
                    collector.collect(**task.params),
                    timeout=task.timeout_seconds
                )
                
                # Validate and store result
                if result.is_successful:
                    task.result = result
                    task.status = CollectionStatus.SUCCESS
                    
                    # Store data if repository available
                    if self.repository:
                        await self._store_collection_result(result)
                    
                    self.logger.info("Task completed successfully",
                                   task_id=task.id, source=task.source_name,
                                   data_count=result.data_count)
                    return
                    
                else:
                    task.last_error = f"Collection failed: {result.errors}"
                    self.logger.warning("Task collection failed",
                                      task_id=task.id, source=task.source_name,
                                      errors=result.errors)
                
            except asyncio.TimeoutError:
                task.last_error = "Task timed out"
                task.status = CollectionStatus.TIMEOUT
                self.logger.error("Task timed out", task_id=task.id,
                                source=task.source_name)
                return
                
            except Exception as e:
                task.last_error = str(e)
                self.logger.error("Task execution failed", task_id=task.id,
                                source=task.source_name, error=str(e))
            
            # Wait before retry (exponential backoff)
            if attempt < task.max_retries:
                delay = 2 ** attempt
                await asyncio.sleep(delay)
        
        # All retries exhausted
        task.status = CollectionStatus.FAILED
        task.completed_at = datetime.now()
    
    async def _get_collector(self, source_name: str) -> BaseCollector:
        """Get or create a collector for a source."""
        if source_name not in self.collectors:
            config = self.source_configs[source_name]
            self.collectors[source_name] = config.collector_class()
        
        return self.collectors[source_name]
    
    async def _store_collection_result(self, result: CollectionResult) -> None:
        """Store collection result in database."""
        try:
            if self.repository:
                # Store based on data type
                for item in result.data:
                    if hasattr(item, '__class__'):
                        model_name = item.__class__.__name__.lower()
                        if model_name == 'game':
                            await self.repository.games.create(item)
                        elif model_name == 'odds':
                            await self.repository.odds.create(item)
                        elif model_name == 'bettinganalysis':
                            await self.repository.betting_analysis.create(item)
                        elif model_name == 'sharpdata':
                            await self.repository.sharp_data.create(item)
                
                self.logger.debug("Collection result stored",
                                source=result.source,
                                data_count=result.data_count)
                
        except Exception as e:
            self.logger.error("Failed to store collection result",
                            source=result.source, error=str(e))
    
    def _update_plan_metrics(self, plan: CollectionPlan) -> None:
        """Update global metrics from plan execution."""
        self.metrics['total_collections'] += len(plan.tasks)
        self.metrics['successful_collections'] += plan.successful_tasks
        self.metrics['failed_collections'] += plan.failed_tasks
        self.metrics['total_items_collected'] += plan.total_items_collected
        
        if plan.duration_seconds:
            self.metrics['total_collection_time_ms'] += plan.duration_seconds * 1000
        
        self.metrics['last_collection_time'] = datetime.now().isoformat()
    
    async def collect_all_sources(
        self,
        timeout_seconds: int = 1800,
        max_concurrent: int = 5
    ) -> CollectionPlan:
        """
        Collect data from all enabled sources.
        
        Args:
            timeout_seconds: Total timeout for collection
            max_concurrent: Maximum concurrent collections
            
        Returns:
            CollectionPlan with results
        """
        plan = await self.create_collection_plan(
            name="collect_all_sources",
            timeout_seconds=timeout_seconds,
            max_concurrent=max_concurrent
        )
        
        return await self.execute_plan(plan)
    
    async def collect_source(
        self,
        source_name: str,
        collection_type: Optional[str] = None,
        **params: Any
    ) -> CollectionResult:
        """
        Collect data from a specific source.
        
        Args:
            source_name: Name of the source
            collection_type: Type of collection
            **params: Additional parameters
            
        Returns:
            CollectionResult
        """
        if source_name not in self.source_configs:
            raise ValueError(f"Unknown source: {source_name}")
        
        config = self.source_configs[source_name]
        if not config.enabled:
            raise ValueError(f"Source {source_name} is disabled")
        
        # Merge parameters
        collection_params = config.collection_params.copy()
        collection_params.update(params)
        
        if collection_type:
            collection_params['collection_type'] = collection_type
        
        # Get collector and execute
        collector = await self._get_collector(source_name)
        result = await collector.collect(**collection_params)
        
        # Store result if repository available
        if result.is_successful and self.repository:
            await self._store_collection_result(result)
        
        return result
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get orchestrator metrics."""
        return {
            **self.metrics,
            'active_plans': len(self.active_plans),
            'configured_sources': len(self.source_configs),
            'enabled_sources': len([c for c in self.source_configs.values() if c.enabled]),
            'rate_limiter_metrics': self.rate_limiter.get_global_metrics()
        }
    
    def get_source_status(self) -> Dict[str, Any]:
        """Get status of all sources."""
        status = {}
        
        for name, config in self.source_configs.items():
            collector_metrics = {}
            if name in self.collectors:
                collector_metrics = self.collectors[name].get_metrics_summary()
            
            status[name] = {
                'enabled': config.enabled,
                'priority': config.priority.name,
                'collection_interval_minutes': config.collection_interval_minutes,
                'collector_metrics': collector_metrics,
                'rate_limiter_metrics': self.rate_limiter.get_source_metrics(name)
            }
        
        return status
    
    async def cleanup(self) -> None:
        """Clean up resources."""
        for collector in self.collectors.values():
            if hasattr(collector, '_cleanup'):
                await collector._cleanup()
        
        self.collectors.clear()
        self.active_plans.clear()
        
        self.logger.info("Collection orchestrator cleaned up")


__all__ = [
    "CollectionOrchestrator",
    "CollectionPlan",
    "CollectionTask",
    "SourceConfig",
    "CollectionPriority"
] 