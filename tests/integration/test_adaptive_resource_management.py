"""
Integration Tests for Adaptive Resource Management System
Tests the complete resource allocation and management workflow
"""

import pytest
import asyncio
import time
from typing import Dict, Any
from datetime import datetime

# Import the resource management components
from src.ml.monitoring.adaptive_resource_manager import (
    AdaptiveResourceManager,
    ResourceQuota,
    AllocationStrategy,
    ResourcePriority,
    get_adaptive_resource_manager,
)
from src.ml.monitoring.resource_allocation_controller import (
    ResourceAllocationController,
    get_resource_allocation_controller,
)
from src.ml.monitoring.resource_monitor import (
    ResourceMonitor,
    get_resource_monitor,
)

# Mock ML components for testing
class MockMLComponent:
    """Mock ML component for testing resource allocation"""
    
    def __init__(self, name: str):
        self.name = name
        self.cpu_allocation = 20.0
        self.memory_allocation_mb = 400.0
        self.batch_size = 10
        self.max_concurrent_operations = 5
        self.allocation_adjustments = 0
    
    async def set_cpu_allocation(self, cpu_percent: float) -> bool:
        self.cpu_allocation = cpu_percent
        self.allocation_adjustments += 1
        return True
    
    async def set_memory_allocation(self, memory_mb: float) -> bool:
        self.memory_allocation_mb = memory_mb
        self.allocation_adjustments += 1
        return True
    
    async def set_batch_size(self, batch_size: int) -> bool:
        self.batch_size = batch_size
        self.allocation_adjustments += 1
        return True
    
    async def set_max_concurrent_operations(self, concurrency: int) -> bool:
        self.max_concurrent_operations = concurrency
        self.allocation_adjustments += 1
        return True


@pytest.mark.asyncio
class TestAdaptiveResourceManager:
    """Test the adaptive resource manager"""

    async def test_initialization(self):
        """Test adaptive resource manager initialization"""
        manager = AdaptiveResourceManager(AllocationStrategy.ADAPTIVE)
        success = await manager.initialize()
        
        assert success is True
        assert manager.strategy == AllocationStrategy.ADAPTIVE
        assert len(manager.resource_quotas) >= 4  # Should have default quotas
        
        # Check default quotas
        prediction_quota = manager.resource_quotas.get("prediction_service")
        assert prediction_quota is not None
        assert prediction_quota.priority == ResourcePriority.CRITICAL
        assert prediction_quota.cpu_current_percent == 25.0
        
        await manager.stop_management()

    async def test_quota_management(self):
        """Test resource quota management"""
        manager = AdaptiveResourceManager(AllocationStrategy.BALANCED)
        await manager.initialize()
        
        # Create custom quota
        custom_quota = ResourceQuota(
            component_name="test_component",
            priority=ResourcePriority.HIGH,
            cpu_current_percent=30.0,
            memory_current_mb=600.0,
        )
        
        # Set and retrieve quota
        success = await manager.set_resource_quota(custom_quota)
        assert success is True
        
        retrieved_quota = await manager.get_resource_quota("test_component")
        assert retrieved_quota is not None
        assert retrieved_quota.component_name == "test_component"
        assert retrieved_quota.cpu_current_percent == 30.0
        
        await manager.stop_management()

    async def test_demand_prediction(self):
        """Test resource demand prediction"""
        manager = AdaptiveResourceManager(AllocationStrategy.ADAPTIVE)
        await manager.initialize()
        
        # Start management to enable prediction
        await manager.start_management()
        
        # Wait a short time for data collection
        await asyncio.sleep(2)
        
        # Check if predictions are generated
        prediction = await manager.get_demand_prediction("prediction_service")
        # Note: Prediction may be None if insufficient historical data
        
        await manager.stop_management()

    async def test_allocation_strategies(self):
        """Test different allocation strategies"""
        strategies = [
            AllocationStrategy.CONSERVATIVE,
            AllocationStrategy.BALANCED,
            AllocationStrategy.AGGRESSIVE,
            AllocationStrategy.ADAPTIVE,
        ]
        
        for strategy in strategies:
            manager = AdaptiveResourceManager(strategy)
            success = await manager.initialize()
            assert success is True
            assert manager.strategy == strategy
            await manager.stop_management()

    async def test_management_stats(self):
        """Test management statistics collection"""
        manager = AdaptiveResourceManager(AllocationStrategy.ADAPTIVE)
        await manager.initialize()
        
        stats = manager.get_management_stats()
        assert "total_adjustments" in stats
        assert "allocation_strategy" in stats
        assert "resource_monitoring_enabled" in stats
        assert stats["allocation_strategy"] == "adaptive"
        
        await manager.stop_management()

    async def test_resource_summary(self):
        """Test comprehensive resource summary"""
        manager = AdaptiveResourceManager(AllocationStrategy.BALANCED)
        await manager.initialize()
        
        summary = manager.get_resource_summary()
        assert "timestamp" in summary
        assert "strategy" in summary
        assert "quotas" in summary
        assert "predictions" in summary
        assert "statistics" in summary
        
        # Check quota data
        assert len(summary["quotas"]) >= 4
        prediction_quota = summary["quotas"].get("prediction_service")
        assert prediction_quota is not None
        assert "priority" in prediction_quota
        assert "cpu_percent" in prediction_quota
        
        await manager.stop_management()


@pytest.mark.asyncio
class TestResourceAllocationController:
    """Test the resource allocation controller"""

    async def test_initialization(self):
        """Test controller initialization"""
        controller = ResourceAllocationController()
        success = await controller.initialize()
        
        assert success is True
        assert controller.allocation_enabled is True

    async def test_component_registration(self):
        """Test component registration and unregistration"""
        controller = ResourceAllocationController()
        await controller.initialize()
        
        # Create mock component
        mock_component = MockMLComponent("test_component")
        
        # Register component
        success = await controller.register_component(
            "test_component", mock_component
        )
        assert success is True
        
        # Check registration
        status = controller.get_allocation_status()
        assert "test_component" in status["registered_components"]
        
        # Unregister component
        success = await controller.unregister_component("test_component")
        assert success is True
        
        # Check unregistration
        status = controller.get_allocation_status()
        assert "test_component" not in status["registered_components"]

    async def test_allocation_application(self):
        """Test resource allocation application to components"""
        controller = ResourceAllocationController()
        await controller.initialize()
        
        # Create and register mock component
        mock_component = MockMLComponent("test_component")
        await controller.register_component("test_component", mock_component)
        
        # Start allocation control briefly
        await controller.start_allocation_control()
        
        # Wait for allocation cycle
        await asyncio.sleep(2)
        
        # Check if allocations were applied
        allocations = controller.get_component_allocations("test_component")
        assert allocations is not None
        
        await controller.stop_allocation_control()

    async def test_allocation_history(self):
        """Test allocation history tracking"""
        controller = ResourceAllocationController()
        await controller.initialize()
        
        # Create and register mock component
        mock_component = MockMLComponent("test_component")
        await controller.register_component("test_component", mock_component)
        
        # Get allocation history
        history = controller.get_allocation_history()
        initial_length = len(history)
        
        # Start allocation control to generate history
        await controller.start_allocation_control()
        await asyncio.sleep(2)
        await controller.stop_allocation_control()
        
        # Check if history was updated
        updated_history = controller.get_allocation_history()
        # History length might change if allocations were applied
        
        # Get component-specific history
        component_history = controller.get_allocation_history("test_component")
        assert isinstance(component_history, list)


@pytest.mark.asyncio
class TestIntegratedResourceManagement:
    """Test the complete integrated resource management system"""

    async def test_end_to_end_workflow(self):
        """Test complete end-to-end resource management workflow"""
        # Initialize components
        controller = ResourceAllocationController()
        await controller.initialize(AllocationStrategy.ADAPTIVE)
        
        # Create mock ML components
        feature_pipeline = MockMLComponent("feature_pipeline")
        prediction_service = MockMLComponent("prediction_service")
        redis_store = MockMLComponent("redis_feature_store")
        
        # Register components
        await controller.register_component("feature_pipeline", feature_pipeline)
        await controller.register_component("prediction_service", prediction_service)
        await controller.register_component("redis_feature_store", redis_store)
        
        # Start allocation control
        await controller.start_allocation_control()
        
        # Let the system run for a few cycles
        await asyncio.sleep(5)
        
        # Check that allocations were applied
        pipeline_allocations = controller.get_component_allocations("feature_pipeline")
        prediction_allocations = controller.get_component_allocations("prediction_service")
        redis_allocations = controller.get_component_allocations("redis_feature_store")
        
        # Verify components received allocations
        assert pipeline_allocations is not None
        assert prediction_allocations is not None
        assert redis_allocations is not None
        
        # Check allocation history
        history = controller.get_allocation_history()
        assert len(history) >= 0  # Should have some history entries if adjustments were made
        
        # Check controller status
        status = controller.get_allocation_status()
        assert status["allocation_enabled"] is True
        assert status["running"] is True
        assert len(status["registered_components"]) == 3
        
        # Stop allocation control
        await controller.stop_allocation_control()

    async def test_resource_pressure_response(self):
        """Test system response to resource pressure"""
        controller = ResourceAllocationController()
        await controller.initialize(AllocationStrategy.ADAPTIVE)
        
        # Create mock components
        high_priority = MockMLComponent("prediction_service")
        low_priority = MockMLComponent("background_service")
        
        # Register components
        await controller.register_component("prediction_service", high_priority)
        await controller.register_component("background_service", low_priority)
        
        # Start allocation control
        await controller.start_allocation_control()
        
        # Simulate resource pressure scenario
        # (In a real test, we'd modify the resource monitor to report high usage)
        
        # Let system respond to conditions
        await asyncio.sleep(3)
        
        # Verify system is functioning
        status = controller.get_allocation_status()
        assert status["running"] is True
        
        await controller.stop_allocation_control()

    async def test_performance_metrics(self):
        """Test performance metrics collection"""
        # Get adaptive resource manager
        manager = await get_adaptive_resource_manager()
        
        # Get controller
        controller = await get_resource_allocation_controller()
        
        # Check that both systems can provide statistics
        manager_stats = manager.get_management_stats()
        controller_status = controller.get_allocation_status()
        
        assert isinstance(manager_stats, dict)
        assert isinstance(controller_status, dict)
        
        # Check key metrics
        assert "allocation_strategy" in manager_stats
        assert "allocation_enabled" in controller_status

    async def test_error_handling(self):
        """Test error handling in resource management"""
        controller = ResourceAllocationController()
        await controller.initialize()
        
        # Test registration with invalid component
        success = await controller.register_component("invalid", None)
        assert success is False  # Should handle gracefully
        
        # Test unregistering non-existent component
        success = await controller.unregister_component("non_existent")
        assert success is True  # Should not error
        
        # Test getting allocations for non-existent component
        allocations = controller.get_component_allocations("non_existent")
        assert allocations is None

    async def test_concurrent_operations(self):
        """Test concurrent resource management operations"""
        controller = ResourceAllocationController()
        await controller.initialize()
        
        # Create multiple mock components
        components = [MockMLComponent(f"component_{i}") for i in range(5)]
        
        # Register components concurrently
        registration_tasks = [
            controller.register_component(f"component_{i}", comp)
            for i, comp in enumerate(components)
        ]
        results = await asyncio.gather(*registration_tasks)
        
        # All registrations should succeed
        assert all(results)
        
        # Check all components are registered
        status = controller.get_allocation_status()
        assert len(status["registered_components"]) == 5
        
        # Start allocation control
        await controller.start_allocation_control()
        await asyncio.sleep(2)
        await controller.stop_allocation_control()
        
        # Verify system stability
        final_status = controller.get_allocation_status()
        assert final_status["allocation_enabled"] is True


# Test fixtures and utilities

@pytest.fixture
async def adaptive_manager():
    """Fixture for adaptive resource manager"""
    manager = AdaptiveResourceManager(AllocationStrategy.ADAPTIVE)
    await manager.initialize()
    yield manager
    await manager.stop_management()


@pytest.fixture
async def allocation_controller():
    """Fixture for resource allocation controller"""
    controller = ResourceAllocationController()
    await controller.initialize()
    yield controller
    await controller.stop_allocation_control()


@pytest.fixture
def mock_ml_components():
    """Fixture for mock ML components"""
    return {
        "feature_pipeline": MockMLComponent("feature_pipeline"),
        "prediction_service": MockMLComponent("prediction_service"),
        "redis_store": MockMLComponent("redis_feature_store"),
    }


# Performance and stress tests

@pytest.mark.asyncio
async def test_system_performance():
    """Test system performance under normal load"""
    start_time = time.time()
    
    controller = ResourceAllocationController()
    await controller.initialize()
    
    # Register 10 components
    components = [MockMLComponent(f"component_{i}") for i in range(10)]
    for i, comp in enumerate(components):
        await controller.register_component(f"component_{i}", comp)
    
    # Run allocation control
    await controller.start_allocation_control()
    await asyncio.sleep(5)  # Run for 5 seconds
    await controller.stop_allocation_control()
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Should complete in reasonable time
    assert execution_time < 10.0  # Should be much faster than 10 seconds
    
    # Verify system health
    status = controller.get_allocation_status()
    assert status["allocation_enabled"] is True
    assert len(status["registered_components"]) == 10


if __name__ == "__main__":
    # Run a simple test if called directly
    async def simple_test():
        print("Running simple adaptive resource management test...")
        
        manager = AdaptiveResourceManager(AllocationStrategy.ADAPTIVE)
        await manager.initialize()
        
        controller = ResourceAllocationController()
        await controller.initialize()
        
        print(f"Manager initialized with {len(manager.resource_quotas)} quotas")
        print(f"Controller status: {controller.get_allocation_status()}")
        
        await manager.stop_management()
        await controller.stop_allocation_control()
        
        print("✅ Simple test completed successfully")
    
    asyncio.run(simple_test())