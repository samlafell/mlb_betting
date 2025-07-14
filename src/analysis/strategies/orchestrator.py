"""
Unified Strategy Orchestrator

Coordinates execution of multiple strategies with:
- Parallel strategy execution with resource management
- Dependency resolution and execution ordering
- Performance monitoring and resource optimization
- Error handling and recovery strategies
- Real-time progress tracking and reporting

Part of Phase 3: Strategy Integration - Unified Architecture Migration
"""

import asyncio
import uuid
from typing import Dict, List, Optional, Any, Set, Tuple
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field
import concurrent.futures

from src.core.logging import get_logger, LogComponent
from src.core.exceptions import StrategyError, AnalysisError
from src.data.database import UnifiedRepository
from src.analysis.strategies.base import BaseStrategyProcessor
from src.analysis.strategies.factory import StrategyFactory
from src.analysis.models.unified_models import (
    UnifiedBettingSignal,
    SignalType,
    StrategyCategory
)


class ExecutionStatus(str, Enum):
    """Execution status for orchestration operations"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class ExecutionPriority(str, Enum):
    """Execution priority levels"""
    CRITICAL = "critical"
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"


@dataclass
class StrategyExecutionPlan:
    """Plan for executing a set of strategies"""
    plan_id: str
    plan_name: str
    strategies: List[str]
    execution_order: List[List[str]]  # Groups of strategies that can run in parallel
    dependencies: Dict[str, List[str]] = field(default_factory=dict)
    priority: ExecutionPriority = ExecutionPriority.NORMAL
    timeout_seconds: int = 300
    max_concurrent: int = 3
    context: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class StrategyExecutionResult:
    """Result of strategy execution"""
    strategy_name: str
    execution_id: str
    status: ExecutionStatus
    signals_generated: int = 0
    execution_time_seconds: float = 0.0
    error_message: Optional[str] = None
    signals: List[UnifiedBettingSignal] = field(default_factory=list)
    performance_metrics: Dict[str, Any] = field(default_factory=dict)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


@dataclass
class OrchestrationResult:
    """Result of orchestrated execution"""
    orchestration_id: str
    plan_id: str
    status: ExecutionStatus
    total_strategies: int
    successful_strategies: int
    failed_strategies: int
    total_signals: int
    execution_time_seconds: float
    strategy_results: Dict[str, StrategyExecutionResult] = field(default_factory=dict)
    error_summary: List[str] = field(default_factory=list)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class StrategyOrchestrator:
    """
    Unified strategy orchestrator for coordinated strategy execution.
    
    Provides comprehensive orchestration capabilities including:
    - Parallel strategy execution with resource management
    - Dependency resolution and execution ordering
    - Performance monitoring and optimization
    - Error handling and recovery strategies
    - Real-time progress tracking and reporting
    
    This replaces ad-hoc strategy execution with a structured orchestration layer.
    """
    
    def __init__(self, 
                 factory: StrategyFactory,
                 repository: UnifiedRepository,
                 config: Dict[str, Any]):
        """
        Initialize the strategy orchestrator.
        
        Args:
            factory: Strategy factory for creating processors
            repository: Unified repository for data access
            config: Orchestrator configuration
        """
        self.factory = factory
        self.repository = repository
        self.config = config
        self.logger = get_logger(__name__, LogComponent.STRATEGY)
        
        # Execution management
        self._active_executions: Dict[str, OrchestrationResult] = {}
        self._execution_history: List[OrchestrationResult] = []
        
        # Resource management
        self.max_concurrent_strategies = config.get('max_concurrent_strategies', 5)
        self.default_timeout = config.get('default_timeout_seconds', 300)
        self.enable_parallel_execution = config.get('enable_parallel_execution', True)
        
        # Performance tracking
        self._performance_metrics: Dict[str, Dict[str, Any]] = {}
        
        # Thread pool for CPU-bound operations
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=config.get('thread_pool_size', 4),
            thread_name_prefix='strategy_orchestrator'
        )
        
        self.logger.info(f"Initialized StrategyOrchestrator with max_concurrent={self.max_concurrent_strategies}")
    
    async def execute_strategies(self,
                               strategy_names: List[str],
                               game_data: List[Dict[str, Any]],
                               execution_context: Optional[Dict[str, Any]] = None) -> OrchestrationResult:
        """
        Execute multiple strategies with orchestrated coordination.
        
        Args:
            strategy_names: List of strategy names to execute
            game_data: Game data to process
            execution_context: Execution context and parameters
            
        Returns:
            Orchestration result with all strategy outputs
        """
        orchestration_id = str(uuid.uuid4())
        context = execution_context or {}
        
        self.logger.info(
            f"Starting strategy orchestration",
            extra={
                'orchestration_id': orchestration_id,
                'strategies': strategy_names,
                'game_count': len(game_data)
            }
        )
        
        # Create execution plan
        plan = await self._create_execution_plan(strategy_names, context)
        
        # Initialize orchestration result
        result = OrchestrationResult(
            orchestration_id=orchestration_id,
            plan_id=plan.plan_id,
            status=ExecutionStatus.RUNNING,
            total_strategies=len(strategy_names),
            successful_strategies=0,
            failed_strategies=0,
            total_signals=0,
            execution_time_seconds=0.0,
            started_at=datetime.now()
        )
        
        self._active_executions[orchestration_id] = result
        
        try:
            # Execute strategies according to plan
            await self._execute_plan(plan, game_data, result)
            
            # Finalize result
            result.status = ExecutionStatus.COMPLETED
            result.completed_at = datetime.now()
            result.execution_time_seconds = (
                result.completed_at - result.started_at
            ).total_seconds()
            
            # Calculate summary metrics
            result.successful_strategies = sum(
                1 for r in result.strategy_results.values() 
                if r.status == ExecutionStatus.COMPLETED
            )
            result.failed_strategies = result.total_strategies - result.successful_strategies
            result.total_signals = sum(
                r.signals_generated for r in result.strategy_results.values()
            )
            
            self.logger.info(
                f"Strategy orchestration completed",
                extra={
                    'orchestration_id': orchestration_id,
                    'successful_strategies': result.successful_strategies,
                    'failed_strategies': result.failed_strategies,
                    'total_signals': result.total_signals,
                    'execution_time': result.execution_time_seconds
                }
            )
            
        except Exception as e:
            result.status = ExecutionStatus.FAILED
            result.completed_at = datetime.now()
            result.error_summary.append(f"Orchestration failed: {str(e)}")
            
            self.logger.error(
                f"Strategy orchestration failed: {e}",
                extra={'orchestration_id': orchestration_id},
                exc_info=True
            )
            
            raise StrategyError(f"Orchestration {orchestration_id} failed: {e}") from e
        
        finally:
            # Move to history and clean up
            self._execution_history.append(result)
            if orchestration_id in self._active_executions:
                del self._active_executions[orchestration_id]
        
        return result
    
    async def _create_execution_plan(self,
                                   strategy_names: List[str],
                                   context: Dict[str, Any]) -> StrategyExecutionPlan:
        """Create an optimized execution plan for the strategies"""
        plan_id = str(uuid.uuid4())
        
        # Validate all strategies are available
        available_strategies = self.factory.get_all_strategies()
        missing_strategies = [name for name in strategy_names if name not in available_strategies]
        
        if missing_strategies:
            raise StrategyError(f"Strategies not available: {missing_strategies}")
        
        # Determine execution order based on dependencies and priorities
        execution_order = await self._calculate_execution_order(strategy_names)
        
        # Create execution plan
        plan = StrategyExecutionPlan(
            plan_id=plan_id,
            plan_name=f"Execution Plan {datetime.now().strftime('%Y%m%d_%H%M%S')}",
            strategies=strategy_names,
            execution_order=execution_order,
            priority=ExecutionPriority(context.get('priority', 'normal')),
            timeout_seconds=context.get('timeout_seconds', self.default_timeout),
            max_concurrent=min(
                context.get('max_concurrent', self.max_concurrent_strategies),
                self.max_concurrent_strategies
            ),
            context=context
        )
        
        self.logger.debug(f"Created execution plan {plan_id} with {len(execution_order)} execution groups")
        return plan
    
    async def _calculate_execution_order(self, strategy_names: List[str]) -> List[List[str]]:
        """Calculate optimal execution order considering dependencies and priorities"""
        
        # Get strategy information
        strategy_info = {}
        for name in strategy_names:
            info = self.factory.get_strategy_info(name)
            if info:
                strategy_info[name] = info
        
        # Simple execution order based on priority
        # In a full implementation, this would consider actual dependencies
        high_priority = []
        normal_priority = []
        low_priority = []
        
        for name in strategy_names:
            info = strategy_info.get(name, {})
            priority = info.get('priority', 'MEDIUM')
            
            if priority == 'HIGH':
                high_priority.append(name)
            elif priority == 'LOW':
                low_priority.append(name)
            else:
                normal_priority.append(name)
        
        # Create execution groups
        execution_order = []
        
        if high_priority:
            execution_order.append(high_priority)
        if normal_priority:
            execution_order.append(normal_priority)
        if low_priority:
            execution_order.append(low_priority)
        
        return execution_order
    
    async def _execute_plan(self,
                          plan: StrategyExecutionPlan,
                          game_data: List[Dict[str, Any]],
                          result: OrchestrationResult) -> None:
        """Execute the strategies according to the plan"""
        
        for group_index, strategy_group in enumerate(plan.execution_order):
            self.logger.debug(f"Executing strategy group {group_index + 1}: {strategy_group}")
            
            if self.enable_parallel_execution and len(strategy_group) > 1:
                # Execute strategies in parallel
                await self._execute_strategies_parallel(strategy_group, game_data, plan, result)
            else:
                # Execute strategies sequentially
                await self._execute_strategies_sequential(strategy_group, game_data, plan, result)
    
    async def _execute_strategies_parallel(self,
                                         strategy_names: List[str],
                                         game_data: List[Dict[str, Any]],
                                         plan: StrategyExecutionPlan,
                                         result: OrchestrationResult) -> None:
        """Execute strategies in parallel with resource management"""
        
        # Create semaphore to limit concurrent executions
        semaphore = asyncio.Semaphore(plan.max_concurrent)
        
        # Create execution tasks
        tasks = []
        for strategy_name in strategy_names:
            task = asyncio.create_task(
                self._execute_single_strategy_with_semaphore(
                    semaphore, strategy_name, game_data, plan, result
                )
            )
            tasks.append(task)
        
        # Wait for all tasks to complete
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _execute_strategies_sequential(self,
                                           strategy_names: List[str],
                                           game_data: List[Dict[str, Any]],
                                           plan: StrategyExecutionPlan,
                                           result: OrchestrationResult) -> None:
        """Execute strategies sequentially"""
        
        for strategy_name in strategy_names:
            await self._execute_single_strategy(strategy_name, game_data, plan, result)
    
    async def _execute_single_strategy_with_semaphore(self,
                                                    semaphore: asyncio.Semaphore,
                                                    strategy_name: str,
                                                    game_data: List[Dict[str, Any]],
                                                    plan: StrategyExecutionPlan,
                                                    result: OrchestrationResult) -> None:
        """Execute a single strategy with semaphore for resource management"""
        async with semaphore:
            await self._execute_single_strategy(strategy_name, game_data, plan, result)
    
    async def _execute_single_strategy(self,
                                     strategy_name: str,
                                     game_data: List[Dict[str, Any]],
                                     plan: StrategyExecutionPlan,
                                     result: OrchestrationResult) -> None:
        """Execute a single strategy and record results"""
        
        execution_id = str(uuid.uuid4())
        strategy_result = StrategyExecutionResult(
            strategy_name=strategy_name,
            execution_id=execution_id,
            status=ExecutionStatus.RUNNING,
            started_at=datetime.now()
        )
        
        result.strategy_results[strategy_name] = strategy_result
        
        try:
            # Get strategy instance
            strategy = self.factory.get_strategy(strategy_name)
            if not strategy:
                raise StrategyError(f"Strategy {strategy_name} not available")
            
            # Execute strategy with timeout
            signals = await asyncio.wait_for(
                strategy.process_with_error_handling(game_data, plan.context),
                timeout=plan.timeout_seconds
            )
            
            # Record successful execution
            strategy_result.status = ExecutionStatus.COMPLETED
            strategy_result.completed_at = datetime.now()
            strategy_result.signals = signals
            strategy_result.signals_generated = len(signals)
            strategy_result.execution_time_seconds = (
                strategy_result.completed_at - strategy_result.started_at
            ).total_seconds()
            strategy_result.performance_metrics = strategy.get_processor_info()
            
            self.logger.debug(
                f"Strategy {strategy_name} completed successfully",
                extra={
                    'execution_id': execution_id,
                    'signals_generated': strategy_result.signals_generated,
                    'execution_time': strategy_result.execution_time_seconds
                }
            )
            
        except asyncio.TimeoutError:
            strategy_result.status = ExecutionStatus.TIMEOUT
            strategy_result.completed_at = datetime.now()
            strategy_result.error_message = f"Strategy timed out after {plan.timeout_seconds} seconds"
            result.error_summary.append(f"{strategy_name}: Timeout")
            
            self.logger.warning(f"Strategy {strategy_name} timed out")
            
        except Exception as e:
            strategy_result.status = ExecutionStatus.FAILED
            strategy_result.completed_at = datetime.now()
            strategy_result.error_message = str(e)
            result.error_summary.append(f"{strategy_name}: {str(e)}")
            
            self.logger.error(
                f"Strategy {strategy_name} failed: {e}",
                extra={'execution_id': execution_id},
                exc_info=True
            )
    
    # Strategy execution convenience methods
    
    async def execute_all_strategies(self,
                                   game_data: List[Dict[str, Any]],
                                   context: Optional[Dict[str, Any]] = None) -> OrchestrationResult:
        """Execute all available strategies"""
        all_strategies = list(self.factory.get_all_strategies().keys())
        return await self.execute_strategies(all_strategies, game_data, context)
    
    async def execute_strategies_by_category(self,
                                           category: StrategyCategory,
                                           game_data: List[Dict[str, Any]],
                                           context: Optional[Dict[str, Any]] = None) -> OrchestrationResult:
        """Execute all strategies in a specific category"""
        strategies = self.factory.get_strategies_by_category(category)
        strategy_names = [s.strategy_name.lower().replace('processor', '') for s in strategies]
        return await self.execute_strategies(strategy_names, game_data, context)
    
    async def execute_high_priority_strategies(self,
                                             game_data: List[Dict[str, Any]],
                                             context: Optional[Dict[str, Any]] = None) -> OrchestrationResult:
        """Execute only high priority strategies"""
        high_priority_strategies = []
        for name, info in self.factory.STRATEGY_REGISTRY.items():
            if info.get('priority') == 'HIGH' and info.get('status') == 'MIGRATED':
                high_priority_strategies.append(name)
        
        return await self.execute_strategies(high_priority_strategies, game_data, context)
    
    # Monitoring and status methods
    
    def get_active_executions(self) -> Dict[str, OrchestrationResult]:
        """Get all currently active executions"""
        return self._active_executions.copy()
    
    def get_execution_history(self, limit: int = 10) -> List[OrchestrationResult]:
        """Get recent execution history"""
        return self._execution_history[-limit:]
    
    def get_orchestrator_status(self) -> Dict[str, Any]:
        """Get comprehensive orchestrator status"""
        return {
            'active_executions': len(self._active_executions),
            'total_executions_completed': len(self._execution_history),
            'configuration': {
                'max_concurrent_strategies': self.max_concurrent_strategies,
                'default_timeout': self.default_timeout,
                'enable_parallel_execution': self.enable_parallel_execution
            },
            'performance_summary': self._calculate_performance_summary(),
            'last_updated': datetime.now().isoformat()
        }
    
    def _calculate_performance_summary(self) -> Dict[str, Any]:
        """Calculate performance summary from execution history"""
        if not self._execution_history:
            return {}
        
        recent_executions = self._execution_history[-10:]  # Last 10 executions
        
        total_strategies = sum(r.total_strategies for r in recent_executions)
        successful_strategies = sum(r.successful_strategies for r in recent_executions)
        total_signals = sum(r.total_signals for r in recent_executions)
        avg_execution_time = sum(r.execution_time_seconds for r in recent_executions) / len(recent_executions)
        
        return {
            'recent_executions': len(recent_executions),
            'total_strategies_executed': total_strategies,
            'success_rate': successful_strategies / total_strategies if total_strategies > 0 else 0,
            'total_signals_generated': total_signals,
            'average_execution_time_seconds': avg_execution_time,
            'signals_per_execution': total_signals / len(recent_executions)
        }
    
    async def __aenter__(self):
        """Async context manager entry"""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        # Clean up thread pool
        self._thread_pool.shutdown(wait=True)
        return False 