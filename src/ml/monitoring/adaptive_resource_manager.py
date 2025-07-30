"""
Intelligent Resource Management with Adaptive Allocation
Dynamically adjusts resource allocation based on real-time monitoring and predictive analytics
"""

import logging
import asyncio
import time
from typing import Dict, List, Optional, Any, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
import statistics
from collections import deque, defaultdict

# Import resource monitoring
try:
    from .resource_monitor import get_resource_monitor, ResourceMonitor, ResourceMetrics, ResourceAlert
except ImportError:
    # Fallback for environments where resource monitor is not available
    get_resource_monitor = None
    ResourceMonitor = None
    ResourceMetrics = None
    ResourceAlert = None

# Import configuration
try:
    from ...core.config import get_unified_config
except ImportError:
    get_unified_config = None

logger = logging.getLogger(__name__)


class ResourcePriority(str, Enum):
    """Resource allocation priorities"""
    CRITICAL = "critical"      # Mission-critical operations (predictions, model serving)
    HIGH = "high"             # Important operations (feature extraction, caching)
    NORMAL = "normal"         # Standard operations (batch processing, analytics)
    LOW = "low"              # Background operations (cleanup, maintenance)


class AllocationStrategy(str, Enum):
    """Resource allocation strategies"""
    CONSERVATIVE = "conservative"  # Prioritize stability and reliability
    BALANCED = "balanced"         # Balance performance and stability
    AGGRESSIVE = "aggressive"     # Maximize performance and throughput
    ADAPTIVE = "adaptive"         # Dynamically adjust based on conditions


@dataclass
class ResourceQuota:
    """Resource allocation quota for a component"""
    component_name: str
    priority: ResourcePriority
    
    # CPU allocation (percentage of available CPU)
    cpu_min_percent: float = 5.0
    cpu_max_percent: float = 80.0
    cpu_current_percent: float = 20.0
    
    # Memory allocation (MB)
    memory_min_mb: float = 100.0
    memory_max_mb: float = 2048.0
    memory_current_mb: float = 512.0
    
    # Disk I/O allocation (operations per second)
    disk_iops_min: int = 10
    disk_iops_max: int = 1000
    disk_iops_current: int = 100
    
    # Network bandwidth allocation (MB/s)
    network_min_mbps: float = 1.0
    network_max_mbps: float = 100.0
    network_current_mbps: float = 10.0
    
    # Dynamic adjustment parameters
    adjustment_factor: float = 1.0
    last_adjustment: datetime = field(default_factory=datetime.utcnow)
    adjustment_history: List[float] = field(default_factory=list)


@dataclass
class AllocationDecision:
    """Resource allocation decision with rationale"""
    component_name: str
    resource_type: str
    old_allocation: float
    new_allocation: float
    adjustment_factor: float
    rationale: str
    confidence: float  # 0.0 to 1.0
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ResourceDemand:
    """Resource demand prediction for a component"""
    component_name: str
    predicted_cpu_percent: float
    predicted_memory_mb: float
    predicted_disk_iops: int
    predicted_network_mbps: float
    confidence: float
    time_horizon_minutes: int
    timestamp: datetime = field(default_factory=datetime.utcnow)


class AdaptiveResourceManager:
    """
    Intelligent resource management system with adaptive allocation
    Dynamically adjusts resource quotas based on real-time monitoring and demand prediction
    """

    def __init__(self, strategy: AllocationStrategy = AllocationStrategy.ADAPTIVE):
        self.strategy = strategy
        self.resource_monitor: Optional[ResourceMonitor] = None
        self.resource_monitoring_enabled = get_resource_monitor is not None
        
        # Get configuration
        self.config = None
        if get_unified_config:
            try:
                self.config = get_unified_config()
                self.ml_config = self.config.ml_pipeline
            except Exception as e:
                logger.warning(f"Failed to load unified config: {e}")
                self.ml_config = None
        
        # Resource quotas by component
        self.resource_quotas: Dict[str, ResourceQuota] = {}
        
        # Historical data for predictive analytics
        self.resource_usage_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.demand_predictions: Dict[str, ResourceDemand] = {}
        
        # Allocation decisions history
        self.allocation_decisions: deque = deque(maxlen=500)
        
        # Management parameters
        self.adjustment_interval_seconds = 30
        self.prediction_window_minutes = 15
        self.min_adjustment_threshold = 0.05  # 5% minimum change to trigger adjustment
        self.max_adjustment_per_cycle = 0.3   # 30% maximum change per cycle
        
        # Performance tracking
        self.allocation_stats = {
            "total_adjustments": 0,
            "successful_adjustments": 0,
            "failed_adjustments": 0,
            "average_adjustment_time_ms": 0.0,
            "last_adjustment_time": None,
        }
        
        # Management task
        self._management_task: Optional[asyncio.Task] = None
        self._running = False

    async def initialize(self) -> bool:
        """Initialize the adaptive resource manager"""
        try:
            # Initialize resource monitor if available
            if self.resource_monitoring_enabled and not self.resource_monitor:
                self.resource_monitor = await get_resource_monitor()
                if not self.resource_monitor._running:
                    await self.resource_monitor.start_monitoring()
                logger.info("✅ Adaptive Resource Manager resource monitoring initialized")
            
            # Initialize default quotas for ML components
            await self._initialize_default_quotas()
            
            logger.info(f"✅ Adaptive Resource Manager initialized with {self.strategy.value} strategy")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Adaptive Resource Manager: {e}")
            return False

    async def start_management(self) -> None:
        """Start the adaptive resource management loop"""
        if self._running:
            logger.warning("Adaptive resource management already running")
            return
        
        self._running = True
        self._management_task = asyncio.create_task(self._management_loop())
        logger.info("✅ Adaptive resource management started")

    async def stop_management(self) -> None:
        """Stop the adaptive resource management"""
        self._running = False
        
        if self._management_task:
            self._management_task.cancel()
            try:
                await self._management_task
            except asyncio.CancelledError:
                pass
        
        logger.info("✅ Adaptive resource management stopped")

    async def _initialize_default_quotas(self) -> None:
        """Initialize default resource quotas for ML pipeline components"""
        # Prediction Service - Highest priority
        self.resource_quotas["prediction_service"] = ResourceQuota(
            component_name="prediction_service",
            priority=ResourcePriority.CRITICAL,
            cpu_min_percent=10.0,
            cpu_max_percent=60.0,
            cpu_current_percent=25.0,
            memory_min_mb=256.0,
            memory_max_mb=1024.0,
            memory_current_mb=512.0,
            disk_iops_min=20,
            disk_iops_max=500,
            disk_iops_current=100,
            network_min_mbps=5.0,
            network_max_mbps=50.0,
            network_current_mbps=15.0,
        )
        
        # Feature Pipeline - High priority
        self.resource_quotas["feature_pipeline"] = ResourceQuota(
            component_name="feature_pipeline",
            priority=ResourcePriority.HIGH,
            cpu_min_percent=5.0,
            cpu_max_percent=40.0,
            cpu_current_percent=20.0,
            memory_min_mb=200.0,
            memory_max_mb=800.0,
            memory_current_mb=400.0,
            disk_iops_min=15,
            disk_iops_max=300,
            disk_iops_current=75,
            network_min_mbps=2.0,
            network_max_mbps=20.0,
            network_current_mbps=8.0,
        )
        
        # Redis Feature Store - High priority
        self.resource_quotas["redis_feature_store"] = ResourceQuota(
            component_name="redis_feature_store",
            priority=ResourcePriority.HIGH,
            cpu_min_percent=3.0,
            cpu_max_percent=30.0,
            cpu_current_percent=15.0,
            memory_min_mb=150.0,
            memory_max_mb=600.0,
            memory_current_mb=300.0,
            disk_iops_min=10,
            disk_iops_max=200,
            disk_iops_current=50,
            network_min_mbps=5.0,
            network_max_mbps=40.0,
            network_current_mbps=12.0,
        )
        
        # Comprehensive Monitor - Normal priority
        self.resource_quotas["comprehensive_monitor"] = ResourceQuota(
            component_name="comprehensive_monitor",
            priority=ResourcePriority.NORMAL,
            cpu_min_percent=2.0,
            cpu_max_percent=15.0,
            cpu_current_percent=8.0,
            memory_min_mb=100.0,
            memory_max_mb=300.0,
            memory_current_mb=150.0,
            disk_iops_min=5,
            disk_iops_max=100,
            disk_iops_current=25,
            network_min_mbps=1.0,
            network_max_mbps=10.0,
            network_current_mbps=3.0,
        )
        
        logger.info(f"Initialized default quotas for {len(self.resource_quotas)} components")

    async def _management_loop(self) -> None:
        """Main adaptive resource management loop"""
        while self._running:
            try:
                start_time = datetime.utcnow()
                
                # Collect current resource usage
                await self._collect_usage_data()
                
                # Predict future resource demands
                await self._predict_resource_demands()
                
                # Make allocation decisions
                decisions = await self._make_allocation_decisions()
                
                # Apply allocation adjustments
                if decisions:
                    await self._apply_allocation_decisions(decisions)
                
                # Update statistics
                processing_time = (datetime.utcnow() - start_time).total_seconds() * 1000
                self._update_management_stats(processing_time)
                
                logger.info(
                    f"Resource management cycle complete: "
                    f"{len(decisions)} decisions in {processing_time:.1f}ms"
                )
                
                # Sleep until next management cycle
                await asyncio.sleep(self.adjustment_interval_seconds)
                
            except Exception as e:
                logger.error(f"Error in resource management loop: {e}")
                await asyncio.sleep(self.adjustment_interval_seconds)

    async def _collect_usage_data(self) -> None:
        """Collect current resource usage data for components"""
        if not self.resource_monitoring_enabled or not self.resource_monitor:
            return
        
        try:
            current_metrics = self.resource_monitor.get_current_metrics()
            timestamp = datetime.utcnow()
            
            # Store usage data for each component (simplified approach)
            for component_name in self.resource_quotas.keys():
                usage_data = {
                    "timestamp": timestamp,
                    "cpu_percent": current_metrics.process_cpu_percent,
                    "memory_mb": current_metrics.process_memory_mb,
                    "disk_usage": current_metrics.disk_usage_percent,
                    "network_connections": current_metrics.network_connections,
                }
                
                self.resource_usage_history[component_name].append(usage_data)
                
        except Exception as e:
            logger.error(f"Error collecting usage data: {e}")

    async def _predict_resource_demands(self) -> None:
        """Predict future resource demands using historical data"""
        try:
            for component_name, quota in self.resource_quotas.items():
                history = self.resource_usage_history[component_name]
                
                if len(history) < 5:  # Need minimum data points
                    continue
                
                # Simple trend analysis (could be enhanced with ML models)
                recent_cpu = [data["cpu_percent"] for data in list(history)[-10:]]
                recent_memory = [data["memory_mb"] for data in list(history)[-10:]]
                
                # Calculate trends
                cpu_trend = self._calculate_trend(recent_cpu)
                memory_trend = self._calculate_trend(recent_memory)
                
                # Predict future demand
                predicted_cpu = max(0, statistics.mean(recent_cpu) + cpu_trend * self.prediction_window_minutes)
                predicted_memory = max(0, statistics.mean(recent_memory) + memory_trend * self.prediction_window_minutes)
                
                # Create demand prediction
                self.demand_predictions[component_name] = ResourceDemand(
                    component_name=component_name,
                    predicted_cpu_percent=predicted_cpu,
                    predicted_memory_mb=predicted_memory,
                    predicted_disk_iops=quota.disk_iops_current,  # Simplified
                    predicted_network_mbps=quota.network_current_mbps,  # Simplified
                    confidence=min(1.0, len(history) / 50.0),  # Confidence increases with data
                    time_horizon_minutes=self.prediction_window_minutes,
                )
                
        except Exception as e:
            logger.error(f"Error predicting resource demands: {e}")

    def _calculate_trend(self, values: List[float]) -> float:
        """Calculate linear trend from a series of values"""
        if len(values) < 2:
            return 0.0
        
        n = len(values)
        x_values = list(range(n))
        
        # Simple linear regression
        x_mean = statistics.mean(x_values)
        y_mean = statistics.mean(values)
        
        numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, values))
        denominator = sum((x - x_mean) ** 2 for x in x_values)
        
        if denominator == 0:
            return 0.0
        
        return numerator / denominator

    async def _make_allocation_decisions(self) -> List[AllocationDecision]:
        """Make intelligent allocation decisions based on current state and predictions"""
        decisions = []
        
        try:
            for component_name, quota in self.resource_quotas.items():
                demand = self.demand_predictions.get(component_name)
                if not demand:
                    continue
                
                # Check if current resource pressure exists
                resource_pressure = False
                if self.resource_monitoring_enabled and self.resource_monitor:
                    current_metrics = self.resource_monitor.get_current_metrics()
                    resource_pressure = (
                        current_metrics.cpu_percent > 80.0 or 
                        current_metrics.memory_percent > 85.0
                    )
                
                # Make CPU allocation decision
                cpu_decision = self._make_cpu_allocation_decision(quota, demand, resource_pressure)
                if cpu_decision:
                    decisions.append(cpu_decision)
                
                # Make Memory allocation decision
                memory_decision = self._make_memory_allocation_decision(quota, demand, resource_pressure)
                if memory_decision:
                    decisions.append(memory_decision)
                
        except Exception as e:
            logger.error(f"Error making allocation decisions: {e}")
        
        return decisions

    def _make_cpu_allocation_decision(
        self, quota: ResourceQuota, demand: ResourceDemand, resource_pressure: bool
    ) -> Optional[AllocationDecision]:
        """Make CPU allocation decision for a component"""
        try:
            predicted_need = demand.predicted_cpu_percent
            current_allocation = quota.cpu_current_percent
            
            # Determine target allocation based on strategy
            if self.strategy == AllocationStrategy.CONSERVATIVE:
                target_allocation = min(quota.cpu_max_percent, predicted_need * 1.2)  # 20% buffer
            elif self.strategy == AllocationStrategy.AGGRESSIVE:
                target_allocation = min(quota.cpu_max_percent, predicted_need * 1.5)  # 50% buffer
            elif self.strategy == AllocationStrategy.BALANCED:
                target_allocation = min(quota.cpu_max_percent, predicted_need * 1.3)  # 30% buffer
            else:  # ADAPTIVE
                # Adjust based on current conditions
                if resource_pressure:
                    if quota.priority in [ResourcePriority.CRITICAL, ResourcePriority.HIGH]:
                        target_allocation = min(quota.cpu_max_percent, predicted_need * 1.1)  # Minimal buffer
                    else:
                        target_allocation = max(quota.cpu_min_percent, predicted_need * 0.9)  # Reduce allocation
                else:
                    target_allocation = min(quota.cpu_max_percent, predicted_need * 1.3)  # Standard buffer
            
            # Ensure within bounds
            target_allocation = max(quota.cpu_min_percent, min(quota.cpu_max_percent, target_allocation))
            
            # Check if adjustment is significant enough
            adjustment_needed = abs(target_allocation - current_allocation) / current_allocation
            if adjustment_needed < self.min_adjustment_threshold:
                return None
            
            # Limit adjustment magnitude
            max_change = current_allocation * self.max_adjustment_per_cycle
            if target_allocation > current_allocation:
                target_allocation = min(target_allocation, current_allocation + max_change)
            else:
                target_allocation = max(target_allocation, current_allocation - max_change)
            
            # Create decision
            return AllocationDecision(
                component_name=quota.component_name,
                resource_type="cpu_percent",
                old_allocation=current_allocation,
                new_allocation=target_allocation,
                adjustment_factor=target_allocation / current_allocation,
                rationale=f"Predicted need: {predicted_need:.1f}%, Pressure: {resource_pressure}, Strategy: {self.strategy.value}",
                confidence=demand.confidence,
            )
            
        except Exception as e:
            logger.error(f"Error making CPU allocation decision for {quota.component_name}: {e}")
            return None

    def _make_memory_allocation_decision(
        self, quota: ResourceQuota, demand: ResourceDemand, resource_pressure: bool
    ) -> Optional[AllocationDecision]:
        """Make memory allocation decision for a component"""
        try:
            predicted_need = demand.predicted_memory_mb
            current_allocation = quota.memory_current_mb
            
            # Determine target allocation based on strategy
            if self.strategy == AllocationStrategy.CONSERVATIVE:
                target_allocation = min(quota.memory_max_mb, predicted_need * 1.3)  # 30% buffer
            elif self.strategy == AllocationStrategy.AGGRESSIVE:
                target_allocation = min(quota.memory_max_mb, predicted_need * 1.6)  # 60% buffer
            elif self.strategy == AllocationStrategy.BALANCED:
                target_allocation = min(quota.memory_max_mb, predicted_need * 1.4)  # 40% buffer
            else:  # ADAPTIVE
                # Adjust based on current conditions
                if resource_pressure:
                    if quota.priority in [ResourcePriority.CRITICAL, ResourcePriority.HIGH]:
                        target_allocation = min(quota.memory_max_mb, predicted_need * 1.2)  # Minimal buffer
                    else:
                        target_allocation = max(quota.memory_min_mb, predicted_need * 0.95)  # Reduce allocation
                else:
                    target_allocation = min(quota.memory_max_mb, predicted_need * 1.4)  # Standard buffer
            
            # Ensure within bounds
            target_allocation = max(quota.memory_min_mb, min(quota.memory_max_mb, target_allocation))
            
            # Check if adjustment is significant enough
            if current_allocation > 0:
                adjustment_needed = abs(target_allocation - current_allocation) / current_allocation
                if adjustment_needed < self.min_adjustment_threshold:
                    return None
            
            # Limit adjustment magnitude
            max_change = current_allocation * self.max_adjustment_per_cycle
            if target_allocation > current_allocation:
                target_allocation = min(target_allocation, current_allocation + max_change)
            else:
                target_allocation = max(target_allocation, current_allocation - max_change)
            
            # Create decision
            return AllocationDecision(
                component_name=quota.component_name,
                resource_type="memory_mb",
                old_allocation=current_allocation,
                new_allocation=target_allocation,
                adjustment_factor=target_allocation / current_allocation if current_allocation > 0 else 1.0,
                rationale=f"Predicted need: {predicted_need:.1f}MB, Pressure: {resource_pressure}, Strategy: {self.strategy.value}",
                confidence=demand.confidence,
            )
            
        except Exception as e:
            logger.error(f"Error making memory allocation decision for {quota.component_name}: {e}")
            return None

    async def _apply_allocation_decisions(self, decisions: List[AllocationDecision]) -> None:
        """Apply allocation decisions to update resource quotas"""
        successful_adjustments = 0
        
        for decision in decisions:
            try:
                quota = self.resource_quotas.get(decision.component_name)
                if not quota:
                    continue
                
                # Update quota based on decision
                if decision.resource_type == "cpu_percent":
                    quota.cpu_current_percent = decision.new_allocation
                elif decision.resource_type == "memory_mb":
                    quota.memory_current_mb = decision.new_allocation
                
                # Update adjustment history
                quota.adjustment_factor = decision.adjustment_factor
                quota.last_adjustment = decision.timestamp
                quota.adjustment_history.append(decision.adjustment_factor)
                
                # Keep only recent history
                if len(quota.adjustment_history) > 20:
                    quota.adjustment_history = quota.adjustment_history[-20:]
                
                # Store decision in history
                self.allocation_decisions.append(decision)
                
                successful_adjustments += 1
                
                logger.info(
                    f"Applied allocation: {decision.component_name} {decision.resource_type} "
                    f"{decision.old_allocation:.1f} → {decision.new_allocation:.1f} "
                    f"({decision.rationale})"
                )
                
            except Exception as e:
                logger.error(f"Error applying allocation decision: {e}")
                self.allocation_stats["failed_adjustments"] += 1
        
        self.allocation_stats["successful_adjustments"] += successful_adjustments
        self.allocation_stats["total_adjustments"] += len(decisions)

    def _update_management_stats(self, processing_time_ms: float) -> None:
        """Update management performance statistics"""
        # Update average processing time
        current_avg = self.allocation_stats["average_adjustment_time_ms"]
        total_cycles = self.allocation_stats["total_adjustments"] + 1
        new_avg = ((current_avg * (total_cycles - 1)) + processing_time_ms) / total_cycles
        self.allocation_stats["average_adjustment_time_ms"] = new_avg
        self.allocation_stats["last_adjustment_time"] = datetime.utcnow()

    # Public API methods

    async def get_resource_quota(self, component_name: str) -> Optional[ResourceQuota]:
        """Get current resource quota for a component"""
        return self.resource_quotas.get(component_name)

    async def set_resource_quota(self, quota: ResourceQuota) -> bool:
        """Set resource quota for a component"""
        try:
            self.resource_quotas[quota.component_name] = quota
            logger.info(f"Updated resource quota for {quota.component_name}")
            return True
        except Exception as e:
            logger.error(f"Error setting resource quota: {e}")
            return False

    async def get_demand_prediction(self, component_name: str) -> Optional[ResourceDemand]:
        """Get demand prediction for a component"""
        return self.demand_predictions.get(component_name)

    async def get_allocation_history(self, component_name: Optional[str] = None) -> List[AllocationDecision]:
        """Get allocation decision history"""
        if component_name:
            return [d for d in self.allocation_decisions if d.component_name == component_name]
        return list(self.allocation_decisions)

    def get_management_stats(self) -> Dict[str, Any]:
        """Get resource management statistics"""
        stats = self.allocation_stats.copy()
        
        # Add quota information
        stats["active_quotas"] = len(self.resource_quotas)
        stats["components_with_predictions"] = len(self.demand_predictions)
        stats["total_allocation_decisions"] = len(self.allocation_decisions)
        
        # Add strategy information
        stats["allocation_strategy"] = self.strategy.value
        stats["resource_monitoring_enabled"] = self.resource_monitoring_enabled
        
        return stats

    def get_resource_summary(self) -> Dict[str, Any]:
        """Get comprehensive resource allocation summary"""
        summary = {
            "timestamp": datetime.utcnow().isoformat(),
            "strategy": self.strategy.value,
            "quotas": {},
            "predictions": {},
            "recent_decisions": [],
            "statistics": self.get_management_stats(),
        }
        
        # Add quota information
        for component_name, quota in self.resource_quotas.items():
            summary["quotas"][component_name] = {
                "priority": quota.priority.value,
                "cpu_percent": quota.cpu_current_percent,
                "memory_mb": quota.memory_current_mb,
                "last_adjustment": quota.last_adjustment.isoformat(),
                "adjustment_factor": quota.adjustment_factor,
            }
        
        # Add prediction information
        for component_name, demand in self.demand_predictions.items():
            summary["predictions"][component_name] = {
                "predicted_cpu_percent": demand.predicted_cpu_percent,
                "predicted_memory_mb": demand.predicted_memory_mb,
                "confidence": demand.confidence,
                "time_horizon_minutes": demand.time_horizon_minutes,
            }
        
        # Add recent decisions
        recent_decisions = list(self.allocation_decisions)[-10:]  # Last 10 decisions
        for decision in recent_decisions:
            summary["recent_decisions"].append({
                "component": decision.component_name,
                "resource_type": decision.resource_type,
                "old_allocation": decision.old_allocation,
                "new_allocation": decision.new_allocation,
                "rationale": decision.rationale,
                "timestamp": decision.timestamp.isoformat(),
            })
        
        return summary


# Global adaptive resource manager instance
_adaptive_resource_manager: Optional[AdaptiveResourceManager] = None


async def get_adaptive_resource_manager(
    strategy: AllocationStrategy = AllocationStrategy.ADAPTIVE
) -> AdaptiveResourceManager:
    """Get or create adaptive resource manager instance"""
    global _adaptive_resource_manager
    
    if _adaptive_resource_manager is None:
        _adaptive_resource_manager = AdaptiveResourceManager(strategy)
        await _adaptive_resource_manager.initialize()
    
    return _adaptive_resource_manager


# Export key components
__all__ = [
    "AdaptiveResourceManager",
    "ResourceQuota",
    "AllocationDecision",
    "ResourceDemand",
    "ResourcePriority",
    "AllocationStrategy",
    "get_adaptive_resource_manager",
]