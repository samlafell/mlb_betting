"""
Performance tests for data collection system.

Tests collection performance, throughput, and resource usage under load.
"""

import asyncio
import time
import psutil
from datetime import datetime, timedelta
from typing import Dict, Any, List
import pytest
from statistics import mean, median

from tests.mocks.collectors import CollectorMockFactory, create_mock_collector_environment
from tests.mocks.database import create_mock_db_pool
from tests.utils.logging_utils import create_test_logger, setup_secure_test_logging
from src.data.collection.base import CollectionRequest


class CollectionPerformanceTester:
    """Performance testing utilities for data collection."""
    
    def __init__(self):
        self.logger = create_test_logger("collection_performance")
        self.metrics: Dict[str, List[float]] = {
            "response_times": [],
            "memory_usage": [],
            "cpu_usage": [],
            "throughput": []
        }
    
    def start_monitoring(self):
        """Start system resource monitoring."""
        self.start_time = time.time()
        self.start_memory = psutil.virtual_memory().used
        self.start_cpu = psutil.cpu_percent()
    
    def record_metrics(self, response_time: float, items_processed: int):
        """Record performance metrics."""
        self.metrics["response_times"].append(response_time)
        self.metrics["memory_usage"].append(psutil.virtual_memory().used)
        self.metrics["cpu_usage"].append(psutil.cpu_percent())
        self.metrics["throughput"].append(items_processed / response_time if response_time > 0 else 0)
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary statistics."""
        if not self.metrics["response_times"]:
            return {"error": "No metrics recorded"}
        
        return {
            "response_time": {
                "mean": mean(self.metrics["response_times"]),
                "median": median(self.metrics["response_times"]),
                "min": min(self.metrics["response_times"]),
                "max": max(self.metrics["response_times"]),
                "p95": sorted(self.metrics["response_times"])[int(0.95 * len(self.metrics["response_times"]))]
            },
            "throughput": {
                "mean": mean(self.metrics["throughput"]),
                "max": max(self.metrics["throughput"])
            },
            "memory_usage": {
                "peak_mb": max(self.metrics["memory_usage"]) / (1024 * 1024),
                "avg_mb": mean(self.metrics["memory_usage"]) / (1024 * 1024)
            },
            "cpu_usage": {
                "peak_percent": max(self.metrics["cpu_usage"]),
                "avg_percent": mean(self.metrics["cpu_usage"])
            },
            "total_requests": len(self.metrics["response_times"])
        }
    
    def assert_performance_thresholds(self, thresholds: Dict[str, float]):
        """Assert performance meets specified thresholds."""
        summary = self.get_performance_summary()
        
        if "max_response_time" in thresholds:
            assert summary["response_time"]["max"] <= thresholds["max_response_time"], \
                f"Max response time {summary['response_time']['max']:.3f}s exceeds threshold {thresholds['max_response_time']}s"
        
        if "min_throughput" in thresholds:
            assert summary["throughput"]["mean"] >= thresholds["min_throughput"], \
                f"Mean throughput {summary['throughput']['mean']:.1f} below threshold {thresholds['min_throughput']}"
        
        if "max_memory_mb" in thresholds:
            assert summary["memory_usage"]["peak_mb"] <= thresholds["max_memory_mb"], \
                f"Peak memory {summary['memory_usage']['peak_mb']:.1f}MB exceeds threshold {thresholds['max_memory_mb']}MB"
        
        if "max_cpu_percent" in thresholds:
            assert summary["cpu_usage"]["peak_percent"] <= thresholds["max_cpu_percent"], \
                f"Peak CPU {summary['cpu_usage']['peak_percent']:.1f}% exceeds threshold {thresholds['max_cpu_percent']}%"


@pytest.mark.load
class TestCollectionPerformance:
    """Load tests for data collection performance."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup performance test environment."""
        setup_secure_test_logging(log_level="WARNING")  # Reduce log noise
        self.logger = create_test_logger("collection_load_test")
        self.performance_tester = CollectionPerformanceTester()
        self.mock_environment = create_mock_collector_environment()
        self.logger.info("Performance test environment setup complete")
    
    @pytest.mark.asyncio
    async def test_single_collector_throughput(self):
        """Test throughput of a single collector under load."""
        collector = self.mock_environment["collectors"]["action_network"]
        request = CollectionRequest(
            source="action_network",
            date_range={"start": "2024-07-30", "end": "2024-07-30"}
        )
        
        self.performance_tester.start_monitoring()
        
        # Collect data 50 times sequentially
        for i in range(50):
            start_time = time.time()
            results = await collector.collect_data(request)
            response_time = time.time() - start_time
            
            self.performance_tester.record_metrics(response_time, len(results))
        
        # Assert performance thresholds
        thresholds = {
            "max_response_time": 1.0,      # 1 second max per collection
            "min_throughput": 1.0,         # At least 1 item per second
            "max_memory_mb": 500,          # 500MB max memory
            "max_cpu_percent": 50          # 50% max CPU
        }
        
        self.performance_tester.assert_performance_thresholds(thresholds)
        
        summary = self.performance_tester.get_performance_summary()
        self.logger.info(f"✅ Single collector throughput test completed")
        self.logger.info(f"Mean response time: {summary['response_time']['mean']:.3f}s")
        self.logger.info(f"Mean throughput: {summary['throughput']['mean']:.1f} items/sec")
    
    @pytest.mark.asyncio
    async def test_concurrent_collectors(self):
        """Test performance with multiple collectors running concurrently."""
        collectors = list(self.mock_environment["collectors"].values())
        request = CollectionRequest(
            source="mixed",
            date_range={"start": "2024-07-30", "end": "2024-07-30"}
        )
        
        self.performance_tester.start_monitoring()
        
        async def collect_with_timing(collector):
            start_time = time.time()
            results = await collector.collect_data(request)
            response_time = time.time() - start_time
            self.performance_tester.record_metrics(response_time, len(results))
            return results
        
        # Run 10 rounds of concurrent collection
        for round_num in range(10):
            tasks = [collect_with_timing(collector) for collector in collectors]
            results = await asyncio.gather(*tasks)
            
            # Verify all collectors returned data
            assert all(len(result) > 0 for result in results)
        
        # Assert performance thresholds for concurrent operation
        thresholds = {
            "max_response_time": 2.0,      # 2 seconds max per collection (higher for concurrency)
            "min_throughput": 0.5,         # At least 0.5 items per second (lower for concurrency)
            "max_memory_mb": 1000,         # 1GB max memory
            "max_cpu_percent": 80          # 80% max CPU
        }
        
        self.performance_tester.assert_performance_thresholds(thresholds)
        
        summary = self.performance_tester.get_performance_summary()
        self.logger.info(f"✅ Concurrent collectors test completed")
        self.logger.info(f"Total requests: {summary['total_requests']}")
        self.logger.info(f"P95 response time: {summary['response_time']['p95']:.3f}s")
    
    @pytest.mark.asyncio
    async def test_high_volume_data_processing(self):
        """Test performance with high volume of data."""
        collector = self.mock_environment["collectors"]["action_network"]
        
        # Create request for large date range (simulating high volume)
        request = CollectionRequest(
            source="action_network",
            date_range={"start": "2024-07-01", "end": "2024-07-31"},  # Full month
            parameters={"high_volume": True}
        )
        
        self.performance_tester.start_monitoring()
        
        start_time = time.time()
        results = await collector.collect_data(request)
        response_time = time.time() - start_time
        
        self.performance_tester.record_metrics(response_time, len(results))
        
        # Should handle large volume without excessive resource usage
        thresholds = {
            "max_response_time": 5.0,      # 5 seconds max for large collection
            "max_memory_mb": 2000,         # 2GB max memory for large collection
            "max_cpu_percent": 90          # 90% max CPU for intensive operation
        }
        
        self.performance_tester.assert_performance_thresholds(thresholds)
        
        # Verify substantial amount of data was processed
        assert len(results) > 0
        
        summary = self.performance_tester.get_performance_summary()
        self.logger.info(f"✅ High volume test completed")
        self.logger.info(f"Processed {len(results)} items in {response_time:.3f}s")
    
    @pytest.mark.asyncio
    async def test_sustained_load(self):
        """Test performance under sustained load over time."""
        collector = self.mock_environment["collectors"]["action_network"]
        request = CollectionRequest(
            source="action_network",
            date_range={"start": "2024-07-30", "end": "2024-07-30"}
        )
        
        self.performance_tester.start_monitoring()
        
        # Run for 30 seconds of sustained load
        end_time = time.time() + 30
        request_count = 0
        
        while time.time() < end_time:
            start_time = time.time()
            results = await collector.collect_data(request)
            response_time = time.time() - start_time
            
            self.performance_tester.record_metrics(response_time, len(results))
            request_count += 1
            
            # Small delay to prevent overwhelming
            await asyncio.sleep(0.1)
        
        # Assert stable performance over time
        thresholds = {
            "max_response_time": 1.5,      # 1.5 seconds max
            "min_throughput": 0.8,         # At least 0.8 items/sec sustained
            "max_memory_mb": 800,          # 800MB max memory
            "max_cpu_percent": 70          # 70% max CPU sustained
        }
        
        self.performance_tester.assert_performance_thresholds(thresholds)
        
        summary = self.performance_tester.get_performance_summary()
        self.logger.info(f"✅ Sustained load test completed")
        self.logger.info(f"Completed {request_count} requests over 30 seconds")
        self.logger.info(f"Average throughput: {summary['throughput']['mean']:.1f} items/sec")
    
    @pytest.mark.asyncio
    async def test_error_handling_performance(self):
        """Test performance impact of error handling."""
        collector = self.mock_environment["collectors"]["action_network"]
        request = CollectionRequest(
            source="action_network",
            date_range={"start": "2024-07-30", "end": "2024-07-30"}
        )
        
        self.performance_tester.start_monitoring()
        
        # Test mix of successful and failed requests
        for i in range(20):
            # Every 5th request fails
            if i % 5 == 0:
                collector.set_failure_mode(True, f"Test failure {i}")
            else:
                collector.set_failure_mode(False)
            
            start_time = time.time()
            try:
                results = await collector.collect_data(request)
                response_time = time.time() - start_time
                self.performance_tester.record_metrics(response_time, len(results))
            except Exception:
                response_time = time.time() - start_time
                self.performance_tester.record_metrics(response_time, 0)  # 0 items for failures
        
        # Reset collector to normal operation
        collector.set_failure_mode(False)
        
        # Should handle errors without excessive performance degradation
        thresholds = {
            "max_response_time": 2.0,      # 2 seconds max even with errors
            "max_memory_mb": 600,          # 600MB max memory
            "max_cpu_percent": 60          # 60% max CPU
        }
        
        self.performance_tester.assert_performance_thresholds(thresholds)
        
        summary = self.performance_tester.get_performance_summary()
        self.logger.info(f"✅ Error handling performance test completed")
        self.logger.info(f"Mean response time with errors: {summary['response_time']['mean']:.3f}s")
    
    def test_memory_leak_detection(self):
        """Test for memory leaks during repeated operations."""
        import gc
        
        collector = self.mock_environment["collectors"]["action_network"]
        request = CollectionRequest(
            source="action_network",
            date_range={"start": "2024-07-30", "end": "2024-07-30"}
        )
        
        # Record initial memory
        gc.collect()  # Force garbage collection
        initial_memory = psutil.virtual_memory().used
        
        # Perform 100 operations
        for i in range(100):
            # Run collection
            results = asyncio.run(collector.collect_data(request))
            assert len(results) > 0
            
            # Clear any references
            del results
            
            # Periodic garbage collection
            if i % 10 == 0:
                gc.collect()
        
        # Final garbage collection
        gc.collect()
        final_memory = psutil.virtual_memory().used
        
        # Memory increase should be minimal (less than 100MB)
        memory_increase_mb = (final_memory - initial_memory) / (1024 * 1024)
        assert memory_increase_mb < 100, f"Memory leak detected: {memory_increase_mb:.1f}MB increase"
        
        self.logger.info(f"✅ Memory leak test passed")
        self.logger.info(f"Memory change: {memory_increase_mb:.1f}MB after 100 operations")


@pytest.mark.load
class TestDatabasePerformance:
    """Performance tests for database operations."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup database performance test environment."""
        setup_secure_test_logging(log_level="WARNING")
        self.logger = create_test_logger("database_performance")
        self.mock_db_pool = create_mock_db_pool()
        self.performance_tester = CollectionPerformanceTester()
    
    @pytest.mark.asyncio
    async def test_connection_pool_performance(self):
        """Test database connection pool performance."""
        self.performance_tester.start_monitoring()
        
        # Test 50 concurrent connections
        async def get_connection():
            start_time = time.time()
            async with self.mock_db_pool.acquire() as conn:
                # Simulate database operation
                await asyncio.sleep(0.01)
                response_time = time.time() - start_time
                self.performance_tester.record_metrics(response_time, 1)
        
        tasks = [get_connection() for _ in range(50)]
        await asyncio.gather(*tasks)
        
        # Assert connection pool performance
        thresholds = {
            "max_response_time": 0.1,      # 100ms max per connection
            "max_memory_mb": 200,          # 200MB max
            "max_cpu_percent": 30          # 30% max CPU
        }
        
        self.performance_tester.assert_performance_thresholds(thresholds)
        
        summary = self.performance_tester.get_performance_summary()
        self.logger.info(f"✅ Connection pool performance test completed")
        self.logger.info(f"Mean connection time: {summary['response_time']['mean']:.3f}s")
    
    @pytest.mark.asyncio
    async def test_bulk_insert_performance(self):
        """Test bulk database insert performance."""
        self.performance_tester.start_monitoring()
        
        # Simulate bulk insert of 1000 records
        async with self.mock_db_pool.acquire() as conn:
            start_time = time.time()
            
            # Mock bulk insert
            for i in range(1000):
                await conn.execute(
                    "INSERT INTO test_table (id, data) VALUES ($1, $2)",
                    i, f"test_data_{i}"
                )
            
            response_time = time.time() - start_time
            self.performance_tester.record_metrics(response_time, 1000)
        
        # Assert bulk insert performance
        thresholds = {
            "max_response_time": 2.0,      # 2 seconds for 1000 inserts
            "min_throughput": 500,         # At least 500 inserts/sec
            "max_memory_mb": 300,          # 300MB max
            "max_cpu_percent": 50          # 50% max CPU
        }
        
        self.performance_tester.assert_performance_thresholds(thresholds)
        
        summary = self.performance_tester.get_performance_summary()
        self.logger.info(f"✅ Bulk insert performance test completed")
        self.logger.info(f"Throughput: {summary['throughput']['mean']:.0f} inserts/sec")


if __name__ == "__main__":
    # Quick performance test runner
    async def run_quick_performance_test():
        setup_secure_test_logging()
        logger = create_test_logger("quick_performance_test")
        
        logger.info("🏃 Running quick performance validation...")
        
        # Test single collector performance
        mock_env = create_mock_collector_environment()
        collector = mock_env["collectors"]["action_network"]
        
        request = CollectionRequest(
            source="action_network",
            date_range={"start": "2024-07-30", "end": "2024-07-30"}
        )
        
        # Time 10 collection operations
        start_time = time.time()
        for i in range(10):
            results = await collector.collect_data(request)
            assert len(results) > 0
        
        total_time = time.time() - start_time
        avg_time = total_time / 10
        
        logger.info(f"✅ 10 collections completed in {total_time:.3f}s")
        logger.info(f"Average time per collection: {avg_time:.3f}s")
        
        # Basic performance assertion
        assert avg_time < 0.5, f"Performance degraded: {avg_time:.3f}s per collection"
        
        logger.info("🎯 Quick performance test passed")
    
    asyncio.run(run_quick_performance_test())