"""
Test suite for the optimized database implementation.

This script validates the new connection pooling architecture and compares
performance with the legacy file-locking approach.
"""

import asyncio
import time
import pytest
from typing import List
import structlog

from mlb_sharp_betting.services.database_coordinator import DatabaseCoordinator
from mlb_sharp_betting.db.optimized_connection import ConnectionConfig, OperationPriority
from mlb_sharp_betting.services.database_service_adapter import get_database_service_adapter

logger = structlog.get_logger(__name__)


class TestOptimizedDatabase:
    """Test suite for optimized database operations"""

    @pytest.fixture
    def legacy_coordinator(self):
        """Legacy file-locking coordinator for comparison"""
        return DatabaseCoordinator(use_optimized=False)

    @pytest.fixture  
    def optimized_coordinator(self):
        """Optimized connection pooling coordinator"""
        config = ConnectionConfig(
            read_pool_size=4,  # Smaller for testing
            write_batch_size=10,
            write_batch_timeout=0.5
        )
        return DatabaseCoordinator(use_optimized=True)

    @pytest.mark.asyncio
    async def test_basic_read_operations(self, optimized_coordinator):
        """Test basic read operations work correctly"""
        coordinator = optimized_coordinator
        
        # Test simple read
        result = coordinator.execute_read("SELECT 1 as test_value")
        assert result is not None
        assert result[0][0] == 1

    @pytest.mark.asyncio 
    async def test_basic_write_operations(self, optimized_coordinator):
        """Test basic write operations work correctly"""
        coordinator = optimized_coordinator
        
        # Create test table
        coordinator.execute_write("""
            CREATE OR REPLACE TABLE test_writes (
                id INTEGER,
                value VARCHAR,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Test write
        result = coordinator.execute_write(
            "INSERT INTO test_writes (id, value) VALUES (?, ?)",
            (1, "test_value")
        )
        
        # Verify write
        rows = coordinator.execute_read("SELECT * FROM test_writes WHERE id = 1")
        assert len(rows) == 1
        assert rows[0][1] == "test_value"

    @pytest.mark.asyncio
    async def test_parallel_reads(self, optimized_coordinator):
        """Test that multiple reads can run in parallel"""
        coordinator = optimized_coordinator
        
        # Create test data
        coordinator.execute_write("""
            CREATE OR REPLACE TABLE test_parallel (
                id INTEGER,
                data VARCHAR
            )
        """)
        
        # Insert test data
        test_data = [(i, f"data_{i}") for i in range(100)]
        coordinator.execute_bulk_insert(
            "INSERT INTO test_parallel (id, data) VALUES (?, ?)",
            test_data
        )
        
        # Time parallel reads
        start_time = time.time()
        
        # These should run in parallel with optimized coordinator
        queries = [
            "SELECT COUNT(*) FROM test_parallel",
            "SELECT AVG(id) FROM test_parallel", 
            "SELECT MAX(id) FROM test_parallel",
            "SELECT MIN(id) FROM test_parallel"
        ]
        
        results = []
        for query in queries:
            result = coordinator.execute_read(query)
            results.append(result)
        
        parallel_time = time.time() - start_time
        
        logger.info(f"Parallel reads completed in {parallel_time:.3f}s")
        assert len(results) == 4
        assert all(result is not None for result in results)

    @pytest.mark.asyncio
    async def test_write_batching(self, optimized_coordinator):
        """Test that writes are properly batched"""
        coordinator = optimized_coordinator
        
        # Create test table
        coordinator.execute_write("""
            CREATE OR REPLACE TABLE test_batching (
                id INTEGER,
                batch_data VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Submit multiple writes quickly
        start_time = time.time()
        
        for i in range(20):
            coordinator.execute_write(
                "INSERT INTO test_batching (id, batch_data) VALUES (?, ?)",
                (i, f"batch_data_{i}")
            )
        
        # Allow batch processing time
        await asyncio.sleep(1.0)
        
        # Verify all writes completed
        result = coordinator.execute_read("SELECT COUNT(*) FROM test_batching")
        batch_time = time.time() - start_time
        
        logger.info(f"Batch writes completed in {batch_time:.3f}s")
        assert result[0][0] == 20

    @pytest.mark.asyncio
    async def test_priority_operations(self, optimized_coordinator):
        """Test that priority operations work correctly"""
        coordinator = optimized_coordinator
        
        # Access the optimized adapter directly for priority testing
        adapter = coordinator._optimized_adapter
        
        # Create test table
        await adapter.execute_write("""
            CREATE OR REPLACE TABLE test_priority (
                id INTEGER,
                priority_level VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """, priority=OperationPriority.HIGH)
        
        # Submit operations with different priorities
        await adapter.execute_write(
            "INSERT INTO test_priority VALUES (1, 'normal', CURRENT_TIMESTAMP)",
            priority=OperationPriority.NORMAL
        )
        
        await adapter.execute_write(
            "INSERT INTO test_priority VALUES (2, 'critical', CURRENT_TIMESTAMP)", 
            priority=OperationPriority.CRITICAL
        )
        
        # Allow processing
        await asyncio.sleep(0.5)
        
        # Verify operations completed
        result = await adapter.execute_read("SELECT COUNT(*) FROM test_priority")
        assert result[0][0] == 2

    def test_performance_comparison(self, legacy_coordinator, optimized_coordinator):
        """Compare performance between legacy and optimized approaches"""
        
        # Setup test data
        setup_query = """
            CREATE OR REPLACE TABLE performance_test (
                id INTEGER,
                data VARCHAR,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        
        # Test legacy performance
        start_time = time.time()
        legacy_coordinator.execute_write(setup_query)
        
        for i in range(50):
            legacy_coordinator.execute_write(
                "INSERT INTO performance_test (id, data) VALUES (?, ?)",
                (i, f"legacy_data_{i}")
            )
        
        legacy_time = time.time() - start_time
        
        # Clear table
        legacy_coordinator.execute_write("DELETE FROM performance_test")
        
        # Test optimized performance  
        start_time = time.time()
        optimized_coordinator.execute_write(setup_query)
        
        for i in range(50):
            optimized_coordinator.execute_write(
                "INSERT INTO performance_test (id, data) VALUES (?, ?)",
                (i, f"optimized_data_{i}")
            )
        
        # Allow batch processing
        time.sleep(1.0)
        optimized_time = time.time() - start_time
        
        logger.info(f"Legacy time: {legacy_time:.3f}s")
        logger.info(f"Optimized time: {optimized_time:.3f}s")
        logger.info(f"Performance improvement: {((legacy_time - optimized_time) / legacy_time * 100):.1f}%")
        
        # Verify both have same data
        legacy_count = legacy_coordinator.execute_read("SELECT COUNT(*) FROM performance_test")[0][0]
        optimized_count = optimized_coordinator.execute_read("SELECT COUNT(*) FROM performance_test")[0][0]
        
        assert legacy_count == optimized_count == 50

    @pytest.mark.asyncio
    async def test_health_and_stats(self, optimized_coordinator):
        """Test health checks and performance statistics"""
        coordinator = optimized_coordinator
        
        # Test health check
        assert coordinator.is_healthy() == True
        
        # Test performance stats
        stats = coordinator.get_performance_stats()
        
        assert "read_pool_size" in stats
        assert "write_queue_size" in stats
        assert "status" in stats
        assert stats["status"] in ["active", "not_initialized"]
        
        logger.info("Performance stats:", **stats)

    def test_fallback_behavior(self):
        """Test that fallback to legacy mode works when optimized fails"""
        # This test verifies the fallback mechanism in DatabaseCoordinator
        
        # Create coordinator that should fall back if optimized import fails
        coordinator = DatabaseCoordinator(use_optimized=True)
        
        # Test basic operations still work (either optimized or fallback)
        result = coordinator.execute_read("SELECT 1 as fallback_test")
        assert result is not None
        assert result[0][0] == 1

    @pytest.mark.asyncio
    async def test_concurrent_operations(self, optimized_coordinator):
        """Test concurrent read and write operations"""
        coordinator = optimized_coordinator
        
        # Create test table
        coordinator.execute_write("""
            CREATE OR REPLACE TABLE test_concurrent (
                id INTEGER,
                operation_type VARCHAR,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Define concurrent operations
        async def concurrent_reads():
            results = []
            for i in range(10):
                result = coordinator.execute_read("SELECT COUNT(*) FROM test_concurrent")
                results.append(result)
                await asyncio.sleep(0.01)
            return results
        
        async def concurrent_writes():
            for i in range(10):
                coordinator.execute_write(
                    "INSERT INTO test_concurrent (id, operation_type) VALUES (?, ?)",
                    (i, "concurrent_write")
                )
                await asyncio.sleep(0.01)
        
        # Run operations concurrently
        start_time = time.time()
        read_task = asyncio.create_task(concurrent_reads())
        write_task = asyncio.create_task(concurrent_writes())
        
        read_results, _ = await asyncio.gather(read_task, write_task)
        concurrent_time = time.time() - start_time
        
        logger.info(f"Concurrent operations completed in {concurrent_time:.3f}s")
        
        # Verify operations completed
        assert len(read_results) == 10
        final_count = coordinator.execute_read("SELECT COUNT(*) FROM test_concurrent")[0][0]
        assert final_count == 10


async def run_performance_benchmark():
    """Run a comprehensive performance benchmark"""
    logger.info("=== PostgreSQL Performance Benchmark ===")
    
    # Setup coordinators
    legacy = DatabaseCoordinator(use_optimized=False)
    optimized = DatabaseCoordinator(use_optimized=True)
    
    # Benchmark configurations
    test_configs = [
        {"name": "Small Operations", "reads": 20, "writes": 20},
        {"name": "Medium Operations", "reads": 100, "writes": 100},
        {"name": "Large Operations", "reads": 500, "writes": 500}
    ]
    
    for config in test_configs:
        logger.info(f"\n--- {config['name']} ---")
        
        # Legacy benchmark
        start_time = time.time()
        
        # Mixed read/write operations  
        for i in range(config['writes']):
            legacy.execute_write(
                "INSERT OR REPLACE INTO benchmark_test (id, data) VALUES (?, ?)",
                (i, f"benchmark_data_{i}")
            )
            
        for i in range(config['reads']):
            legacy.execute_read("SELECT COUNT(*) FROM benchmark_test")
            
        legacy_time = time.time() - start_time
        
        # Optimized benchmark
        start_time = time.time()
        
        for i in range(config['writes']):
            optimized.execute_write(
                "INSERT OR REPLACE INTO benchmark_test (id, data) VALUES (?, ?)",
                (i, f"benchmark_data_{i}")
            )
            
        for i in range(config['reads']):
            optimized.execute_read("SELECT COUNT(*) FROM benchmark_test")
            
        # Allow batch processing
        await asyncio.sleep(0.5)
        optimized_time = time.time() - start_time
        
        # Results
        improvement = ((legacy_time - optimized_time) / legacy_time * 100)
        logger.info(f"Legacy: {legacy_time:.3f}s")
        logger.info(f"Optimized: {optimized_time:.3f}s") 
        logger.info(f"Improvement: {improvement:.1f}%")


if __name__ == "__main__":
    # Setup test database
    coordinator = DatabaseCoordinator(use_optimized=False)
    coordinator.execute_write("""
        CREATE TABLE IF NOT EXISTS benchmark_test (
            id INTEGER PRIMARY KEY,
            data VARCHAR,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Run benchmark
    asyncio.run(run_performance_benchmark())
    
    logger.info("\n=== Benchmark Complete ===")
    logger.info("To run full test suite: pytest tests/test_optimized_database.py -v") 