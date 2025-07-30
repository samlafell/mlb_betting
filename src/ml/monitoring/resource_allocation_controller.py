"""
Resource Allocation Controller
Applies adaptive resource management decisions to ML pipeline components
"""

import logging
import asyncio
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
import weakref

# Import adaptive resource manager
try:
    from .adaptive_resource_manager import (
        AdaptiveResourceManager,
        get_adaptive_resource_manager,
        ResourceQuota,
        AllocationDecision,
        AllocationStrategy
    )
except ImportError:
    AdaptiveResourceManager = None
    get_adaptive_resource_manager = None
    ResourceQuota = None
    AllocationDecision = None
    AllocationStrategy = None

logger = logging.getLogger(__name__)


class ResourceAllocationController:
    """
    Controls resource allocation for ML pipeline components
    Applies decisions from the adaptive resource manager to actual components
    """

    def __init__(self):
        self.adaptive_manager: Optional[AdaptiveResourceManager] = None
        self.allocation_enabled = AdaptiveResourceManager is not None
        
        # Component registry - use weak references to avoid circular dependencies
        self.registered_components: Dict[str, weakref.ReferenceType] = {}
        self.component_controllers: Dict[str, Dict[str, Callable]] = {}
        
        # Allocation tracking
        self.applied_allocations: Dict[str, Dict[str, float]] = {}
        self.allocation_history: List[Dict[str, Any]] = []
        
        # Controller state
        self._running = False
        self._allocation_task: Optional[asyncio.Task] = None
        self.allocation_interval_seconds = 60  # Apply allocations every minute

    async def initialize(self, strategy: AllocationStrategy = None) -> bool:
        """Initialize the resource allocation controller"""
        if not self.allocation_enabled:
            logger.warning("Resource allocation not available - adaptive resource manager not found")
            return False
        
        try:
            # Initialize adaptive resource manager
            if strategy:
                self.adaptive_manager = AdaptiveResourceManager(strategy)
                await self.adaptive_manager.initialize()
            else:
                self.adaptive_manager = await get_adaptive_resource_manager()
            
            logger.info("✅ Resource Allocation Controller initialized")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Resource Allocation Controller: {e}")
            return False

    async def start_allocation_control(self) -> None:
        """Start the resource allocation control loop"""
        if not self.allocation_enabled or not self.adaptive_manager:
            logger.warning("Cannot start allocation control - not properly initialized")
            return
        
        if self._running:
            logger.warning("Resource allocation control already running")
            return
        
        # Start the adaptive manager if not running
        if not self.adaptive_manager._running:
            await self.adaptive_manager.start_management()
        
        self._running = True
        self._allocation_task = asyncio.create_task(self._allocation_control_loop())
        logger.info("✅ Resource allocation control started")

    async def stop_allocation_control(self) -> None:
        """Stop the resource allocation control"""
        self._running = False
        
        if self._allocation_task:
            self._allocation_task.cancel()
            try:
                await self._allocation_task
            except asyncio.CancelledError:
                pass
        
        logger.info("✅ Resource allocation control stopped")

    async def register_component(
        self,
        component_name: str,
        component_instance: Any,
        allocation_controllers: Dict[str, Callable] = None
    ) -> bool:
        """
        Register a component for resource allocation control
        
        Args:
            component_name: Name of the component
            component_instance: Instance of the component
            allocation_controllers: Dict mapping resource types to control functions
        """
        try:
            # Store weak reference to avoid circular dependencies
            self.registered_components[component_name] = weakref.ref(component_instance)
            
            # Default allocation controllers
            default_controllers = {
                "cpu_percent": self._apply_cpu_allocation,
                "memory_mb": self._apply_memory_allocation,
                "batch_size": self._apply_batch_size_allocation,
                "concurrent_operations": self._apply_concurrency_allocation,
            }
            
            # Merge with provided controllers
            if allocation_controllers:
                default_controllers.update(allocation_controllers)
            
            self.component_controllers[component_name] = default_controllers
            
            logger.info(f"Registered component '{component_name}' for resource allocation")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register component '{component_name}': {e}")
            return False

    async def unregister_component(self, component_name: str) -> bool:
        """Unregister a component from resource allocation control"""
        try:
            self.registered_components.pop(component_name, None)
            self.component_controllers.pop(component_name, None)
            self.applied_allocations.pop(component_name, None)
            
            logger.info(f"Unregistered component '{component_name}' from resource allocation")
            return True
            
        except Exception as e:
            logger.error(f"Failed to unregister component '{component_name}': {e}")
            return False

    async def _allocation_control_loop(self) -> None:
        """Main allocation control loop"""
        while self._running:
            try:
                # Get current quotas from adaptive manager
                await self._apply_current_allocations()
                
                # Clean up dead references
                await self._cleanup_dead_references()
                
                # Sleep until next allocation cycle
                await asyncio.sleep(self.allocation_interval_seconds)
                
            except Exception as e:
                logger.error(f"Error in allocation control loop: {e}")
                await asyncio.sleep(self.allocation_interval_seconds)

    async def _apply_current_allocations(self) -> None:
        """Apply current resource allocations to registered components"""
        if not self.adaptive_manager:
            return
        
        applied_count = 0
        
        for component_name in list(self.registered_components.keys()):
            try:
                # Get component instance
                component_ref = self.registered_components.get(component_name)
                if not component_ref:
                    continue
                
                component_instance = component_ref()
                if component_instance is None:
                    # Component has been garbage collected
                    await self.unregister_component(component_name)
                    continue
                
                # Get current quota for this component
                quota = await self.adaptive_manager.get_resource_quota(component_name)
                if not quota:
                    continue
                
                # Apply allocations
                allocation_applied = await self._apply_component_allocation(
                    component_name, component_instance, quota
                )
                
                if allocation_applied:
                    applied_count += 1
                
            except Exception as e:
                logger.error(f"Error applying allocation for {component_name}: {e}")
        
        if applied_count > 0:
            logger.info(f"Applied resource allocations to {applied_count} components")

    async def _apply_component_allocation(
        self, component_name: str, component_instance: Any, quota: ResourceQuota
    ) -> bool:
        """Apply resource allocation to a specific component"""
        try:
            controllers = self.component_controllers.get(component_name, {})
            current_allocations = self.applied_allocations.get(component_name, {})
            
            allocation_changes = []
            
            # Apply CPU allocation
            if "cpu_percent" in controllers:
                old_value = current_allocations.get("cpu_percent", 0)
                if abs(quota.cpu_current_percent - old_value) > 1.0:  # Significant change
                    success = await controllers["cpu_percent"](
                        component_instance, quota.cpu_current_percent
                    )
                    if success:
                        allocation_changes.append(f"CPU: {old_value:.1f}% → {quota.cpu_current_percent:.1f}%")
                        current_allocations["cpu_percent"] = quota.cpu_current_percent
            
            # Apply Memory allocation
            if "memory_mb" in controllers:
                old_value = current_allocations.get("memory_mb", 0)
                if abs(quota.memory_current_mb - old_value) > 50.0:  # Significant change
                    success = await controllers["memory_mb"](
                        component_instance, quota.memory_current_mb
                    )
                    if success:
                        allocation_changes.append(f"Memory: {old_value:.0f}MB → {quota.memory_current_mb:.0f}MB")
                        current_allocations["memory_mb"] = quota.memory_current_mb
            
            # Apply derived allocations (batch size, concurrency)
            await self._apply_derived_allocations(
                component_name, component_instance, quota, controllers, current_allocations, allocation_changes
            )
            
            # Update tracking
            self.applied_allocations[component_name] = current_allocations
            
            # Log changes
            if allocation_changes:
                logger.info(f"Applied allocations to {component_name}: {', '.join(allocation_changes)}")
                
                # Record in history
                self.allocation_history.append({
                    "timestamp": datetime.utcnow().isoformat(),
                    "component_name": component_name,
                    "changes": allocation_changes,
                    "quota": {
                        "cpu_percent": quota.cpu_current_percent,
                        "memory_mb": quota.memory_current_mb,
                    }
                })
                
                # Keep history limited
                if len(self.allocation_history) > 100:
                    self.allocation_history = self.allocation_history[-100:]
            
            return len(allocation_changes) > 0
            
        except Exception as e:
            logger.error(f"Error applying allocation to {component_name}: {e}")
            return False

    async def _apply_derived_allocations(
        self,
        component_name: str,
        component_instance: Any,
        quota: ResourceQuota,
        controllers: Dict[str, Callable],
        current_allocations: Dict[str, float],
        allocation_changes: List[str]
    ) -> None:
        """Apply derived allocations based on resource quotas"""
        try:
            # Calculate batch size based on memory allocation
            if "batch_size" in controllers:
                # Scale batch size with memory allocation
                base_batch_size = 10  # Default
                memory_factor = quota.memory_current_mb / 512.0  # Scale from 512MB baseline
                new_batch_size = max(1, int(base_batch_size * memory_factor))
                
                old_batch_size = current_allocations.get("batch_size", base_batch_size)
                if abs(new_batch_size - old_batch_size) >= 1:
                    success = await controllers["batch_size"](component_instance, new_batch_size)
                    if success:
                        allocation_changes.append(f"Batch Size: {old_batch_size} → {new_batch_size}")
                        current_allocations["batch_size"] = new_batch_size
            
            # Calculate concurrency based on CPU allocation
            if "concurrent_operations" in controllers:
                # Scale concurrency with CPU allocation
                base_concurrency = 5  # Default
                cpu_factor = quota.cpu_current_percent / 25.0  # Scale from 25% baseline
                new_concurrency = max(1, int(base_concurrency * cpu_factor))
                
                old_concurrency = current_allocations.get("concurrent_operations", base_concurrency)
                if abs(new_concurrency - old_concurrency) >= 1:
                    success = await controllers["concurrent_operations"](component_instance, new_concurrency)
                    if success:
                        allocation_changes.append(f"Concurrency: {old_concurrency} → {new_concurrency}")
                        current_allocations["concurrent_operations"] = new_concurrency
            
        except Exception as e:
            logger.error(f"Error applying derived allocations for {component_name}: {e}")

    # Default allocation controller implementations

    async def _apply_cpu_allocation(self, component_instance: Any, cpu_percent: float) -> bool:
        """Apply CPU allocation to a component"""
        try:
            # Check if component has CPU allocation method
            if hasattr(component_instance, 'set_cpu_allocation'):
                await component_instance.set_cpu_allocation(cpu_percent)
                return True
            elif hasattr(component_instance, 'cpu_allocation'):
                component_instance.cpu_allocation = cpu_percent
                return True
            
            # Default: log the allocation (components may not directly control CPU)
            logger.debug(f"CPU allocation set to {cpu_percent:.1f}% for {type(component_instance).__name__}")
            return True
            
        except Exception as e:
            logger.error(f"Error applying CPU allocation: {e}")
            return False

    async def _apply_memory_allocation(self, component_instance: Any, memory_mb: float) -> bool:
        """Apply memory allocation to a component"""
        try:
            # Check if component has memory allocation method
            if hasattr(component_instance, 'set_memory_allocation'):
                await component_instance.set_memory_allocation(memory_mb)
                return True
            elif hasattr(component_instance, 'memory_allocation_mb'):
                component_instance.memory_allocation_mb = memory_mb
                return True
            
            # Default: log the allocation
            logger.debug(f"Memory allocation set to {memory_mb:.0f}MB for {type(component_instance).__name__}")
            return True
            
        except Exception as e:
            logger.error(f"Error applying memory allocation: {e}")
            return False

    async def _apply_batch_size_allocation(self, component_instance: Any, batch_size: int) -> bool:
        """Apply batch size allocation to a component"""
        try:
            # Check if component has batch size control
            if hasattr(component_instance, 'set_batch_size'):
                await component_instance.set_batch_size(batch_size)
                return True
            elif hasattr(component_instance, 'batch_size'):
                component_instance.batch_size = batch_size
                return True
            elif hasattr(component_instance, 'max_batch_size'):
                component_instance.max_batch_size = batch_size
                return True
            
            return False  # Component doesn't support batch size control
            
        except Exception as e:
            logger.error(f"Error applying batch size allocation: {e}")
            return False

    async def _apply_concurrency_allocation(self, component_instance: Any, concurrency: int) -> bool:
        """Apply concurrency allocation to a component"""
        try:
            # Check if component has concurrency control
            if hasattr(component_instance, 'set_max_concurrent_operations'):
                await component_instance.set_max_concurrent_operations(concurrency)
                return True
            elif hasattr(component_instance, 'max_concurrent_operations'):
                component_instance.max_concurrent_operations = concurrency
                return True
            elif hasattr(component_instance, 'concurrency_limit'):
                component_instance.concurrency_limit = concurrency
                return True
            
            return False  # Component doesn't support concurrency control
            
        except Exception as e:
            logger.error(f"Error applying concurrency allocation: {e}")
            return False

    async def _cleanup_dead_references(self) -> None:
        """Clean up dead weak references"""
        dead_components = []
        
        for component_name, component_ref in self.registered_components.items():
            if component_ref() is None:
                dead_components.append(component_name)
        
        for component_name in dead_components:
            await self.unregister_component(component_name)

    # Public API methods

    def get_allocation_status(self) -> Dict[str, Any]:
        """Get current allocation status"""
        return {
            "allocation_enabled": self.allocation_enabled,
            "running": self._running,
            "registered_components": list(self.registered_components.keys()),
            "applied_allocations": self.applied_allocations.copy(),
            "allocation_history_length": len(self.allocation_history),
            "adaptive_manager_running": self.adaptive_manager._running if self.adaptive_manager else False,
        }

    def get_component_allocations(self, component_name: str) -> Optional[Dict[str, float]]:
        """Get current allocations for a specific component"""
        return self.applied_allocations.get(component_name)

    def get_allocation_history(self, component_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get allocation history"""
        if component_name:
            return [h for h in self.allocation_history if h["component_name"] == component_name]
        return self.allocation_history.copy()


# Global resource allocation controller instance
_resource_allocation_controller: Optional[ResourceAllocationController] = None


async def get_resource_allocation_controller() -> ResourceAllocationController:
    """Get or create resource allocation controller instance"""
    global _resource_allocation_controller
    
    if _resource_allocation_controller is None:
        _resource_allocation_controller = ResourceAllocationController()
        await _resource_allocation_controller.initialize()
    
    return _resource_allocation_controller


# Export key components
__all__ = [
    "ResourceAllocationController",
    "get_resource_allocation_controller",
]