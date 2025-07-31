"""
Unit tests for performance benchmarking utilities.

Tests performance measurement, threshold validation, and regression detection.
"""

import asyncio
import json
import tempfile
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from tests.utils.performance_benchmarking import (
    PerformanceThresholds, PerformanceMetrics, BenchmarkResult,
    ResourceMonitor, PerformanceBenchmarker, AsyncBenchmarkContext,
    SyncBenchmarkContext, async_benchmark, sync_benchmark,
    get_performance_benchmarker
)
from tests.utils.logging_utils import create_test_logger, setup_secure_test_logging


class TestPerformanceThresholds:
    """Test performance thresholds configuration."""
    
    def test_default_thresholds(self):
        """Test default threshold values."""
        thresholds = PerformanceThresholds()
        
        assert thresholds.max_response_time_ms == 1000.0
        assert thresholds.min_throughput_ops_per_sec == 1.0
        assert thresholds.max_memory_mb == 500.0
        assert thresholds.max_cpu_percent == 50.0
        assert thresholds.max_p95_response_time_ms == 2000.0
        assert thresholds.max_error_rate_percent == 1.0
        assert thresholds.min_success_rate_percent == 99.0
    
    def test_custom_thresholds(self):
        """Test custom threshold configuration."""
        thresholds = PerformanceThresholds(
            max_response_time_ms=500.0,
            min_throughput_ops_per_sec=5.0,
            max_memory_mb=200.0,
            max_cpu_percent=30.0
        )
        
        assert thresholds.max_response_time_ms == 500.0
        assert thresholds.min_throughput_ops_per_sec == 5.0
        assert thresholds.max_memory_mb == 200.0
        assert thresholds.max_cpu_percent == 30.0


class TestResourceMonitor:
    """Test resource monitoring functionality."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test environment."""
        setup_secure_test_logging()
        self.logger = create_test_logger("resource_monitor_test")
        self.monitor = ResourceMonitor(sampling_interval=0.01)  # Fast sampling for tests
    
    def test_monitor_initialization(self):
        """Test resource monitor initialization."""
        assert self.monitor.sampling_interval == 0.01
        assert self.monitor.monitoring is False
        assert self.monitor.monitor_thread is None
        assert len(self.monitor.memory_samples) == 0
        assert len(self.monitor.cpu_samples) == 0
    
    def test_monitoring_lifecycle(self):
        """Test resource monitoring start/stop lifecycle."""
        # Start monitoring
        self.monitor.start_monitoring()
        assert self.monitor.monitoring is True
        assert self.monitor.monitor_thread is not None
        
        # Let it collect some samples
        time.sleep(0.05)
        
        # Stop monitoring
        stats = self.monitor.stop_monitoring()
        assert self.monitor.monitoring is False
        
        # Verify statistics
        assert "peak_memory_mb" in stats
        assert "avg_memory_mb" in stats
        assert "peak_cpu_percent" in stats
        assert "avg_cpu_percent" in stats
        assert stats["memory_samples_count"] > 0
        assert stats["cpu_samples_count"] > 0
        
        self.logger.info(f"✅ Collected {stats['memory_samples_count']} memory samples")
    
    def test_double_start_stop(self):
        """Test double start/stop is handled gracefully."""
        # Start twice
        self.monitor.start_monitoring()
        self.monitor.start_monitoring()  # Should not cause issues
        
        time.sleep(0.02)
        
        # Stop twice
        stats1 = self.monitor.stop_monitoring()
        stats2 = self.monitor.stop_monitoring()  # Should return empty dict
        
        assert len(stats1) > 0
        assert len(stats2) == 0
        
        self.logger.info("✅ Double start/stop handled gracefully")


@pytest.mark.asyncio
class TestPerformanceBenchmarker:
    """Test performance benchmarker functionality."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test environment."""
        setup_secure_test_logging()
        self.logger = create_test_logger("performance_benchmarker_test")
        
        # Create temporary baseline file
        self.temp_dir = tempfile.mkdtemp()
        self.baseline_file = Path(self.temp_dir) / "test_baselines.json"
    
    def test_benchmarker_initialization(self):
        """Test benchmarker initialization."""
        benchmarker = PerformanceBenchmarker()
        assert benchmarker.baseline_file is None
        assert len(benchmarker.baselines) == 0
        
        benchmarker_with_file = PerformanceBenchmarker(str(self.baseline_file))
        assert benchmarker_with_file.baseline_file == self.baseline_file
        
        self.logger.info("✅ Benchmarker initialization validated")
    
    def test_percentile_calculation(self):
        """Test percentile calculation."""
        benchmarker = PerformanceBenchmarker()
        
        # Test with empty list
        percentiles = benchmarker.calculate_percentiles([])
        assert percentiles["p50"] == 0.0
        assert percentiles["p95"] == 0.0
        assert percentiles["p99"] == 0.0
        
        # Test with values
        values = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        percentiles = benchmarker.calculate_percentiles(values)
        
        assert percentiles["p50"] == 50  # Median
        assert percentiles["p95"] == 90  # 95th percentile
        assert percentiles["p99"] == 100  # 99th percentile
        
        self.logger.info("✅ Percentile calculation validated")
    
    def test_threshold_validation(self):
        """Test threshold validation."""
        benchmarker = PerformanceBenchmarker()
        thresholds = PerformanceThresholds(
            max_response_time_ms=100.0,
            min_throughput_ops_per_sec=2.0,
            max_memory_mb=100.0,
            max_cpu_percent=25.0,
            max_error_rate_percent=1.0
        )
        
        # Create metrics that pass thresholds
        good_metrics = PerformanceMetrics(
            start_time=time.time(),
            end_time=time.time() + 1,
            duration_ms=1000,
            total_operations=10,
            successful_operations=10,
            failed_operations=0,
            ops_per_second=5.0,
            response_times_ms=[50, 60, 70],
            avg_response_time_ms=60.0,
            min_response_time_ms=50.0,
            max_response_time_ms=70.0,
            p50_response_time_ms=60.0,
            p95_response_time_ms=70.0,
            p99_response_time_ms=70.0,
            peak_memory_mb=50.0,
            avg_memory_mb=40.0,
            peak_cpu_percent=20.0,
            avg_cpu_percent=15.0,
            success_rate_percent=100.0,
            error_rate_percent=0.0,
            errors=[],
            test_id="test_good",
            timestamp=time.time(),
            environment={}
        )
        
        passed, violations = benchmarker.validate_thresholds(good_metrics, thresholds)
        assert passed is True
        assert len(violations) == 0
        
        # Create metrics that fail thresholds
        bad_metrics = PerformanceMetrics(
            start_time=time.time(),
            end_time=time.time() + 1,
            duration_ms=1000,
            total_operations=10,
            successful_operations=8,
            failed_operations=2,
            ops_per_second=1.0,  # Below threshold
            response_times_ms=[200, 300, 400],  # Above threshold
            avg_response_time_ms=300.0,  # Above threshold
            min_response_time_ms=200.0,
            max_response_time_ms=400.0,
            p50_response_time_ms=300.0,
            p95_response_time_ms=400.0,
            p99_response_time_ms=400.0,
            peak_memory_mb=200.0,  # Above threshold
            avg_memory_mb=150.0,
            peak_cpu_percent=50.0,  # Above threshold
            avg_cpu_percent=40.0,
            success_rate_percent=80.0,  # Below threshold
            error_rate_percent=20.0,  # Above threshold
            errors=["error1", "error2"],
            test_id="test_bad",
            timestamp=time.time(),
            environment={}
        )
        
        passed, violations = benchmarker.validate_thresholds(bad_metrics, thresholds)
        assert passed is False
        assert len(violations) > 0
        
        self.logger.info(f"✅ Threshold validation: {len(violations)} violations detected")
    
    def test_baseline_management(self):
        """Test baseline saving and loading."""
        benchmarker = PerformanceBenchmarker(str(self.baseline_file))
        
        # Create test metrics
        test_metrics = PerformanceMetrics(
            start_time=time.time(),
            end_time=time.time() + 1,
            duration_ms=1000,
            total_operations=10,
            successful_operations=10,
            failed_operations=0,
            ops_per_second=10.0,
            response_times_ms=[100, 110, 120],
            avg_response_time_ms=110.0,
            min_response_time_ms=100.0,
            max_response_time_ms=120.0,
            p50_response_time_ms=110.0,
            p95_response_time_ms=120.0,
            p99_response_time_ms=120.0,
            peak_memory_mb=50.0,
            avg_memory_mb=40.0,
            peak_cpu_percent=20.0,
            avg_cpu_percent=15.0,
            success_rate_percent=100.0,
            error_rate_percent=0.0,
            errors=[],
            test_id="test_baseline",
            timestamp=time.time(),
            environment={}
        )
        
        # Test first detection (should create baseline)
        regression_detected, comparison = benchmarker.detect_regression("test_baseline", test_metrics)
        assert regression_detected is False
        assert comparison is None
        assert "test_baseline" in benchmarker.baselines
        
        # Test against same baseline (should pass)
        regression_detected, comparison = benchmarker.detect_regression("test_baseline", test_metrics)
        assert regression_detected is False
        assert comparison is not None
        
        # Test regression detection
        worse_metrics = PerformanceMetrics(
            start_time=time.time(),
            end_time=time.time() + 2,
            duration_ms=2000,
            total_operations=10,
            successful_operations=10,
            failed_operations=0,
            ops_per_second=5.0,  # 50% worse
            response_times_ms=[200, 220, 240],  # Much worse
            avg_response_time_ms=220.0,  # 100% worse
            min_response_time_ms=200.0,
            max_response_time_ms=240.0,
            p50_response_time_ms=220.0,
            p95_response_time_ms=240.0,
            p99_response_time_ms=240.0,
            peak_memory_mb=100.0,  # 100% worse
            avg_memory_mb=80.0,
            peak_cpu_percent=40.0,
            avg_cpu_percent=30.0,
            success_rate_percent=100.0,
            error_rate_percent=0.0,
            errors=[],
            test_id="test_regression",
            timestamp=time.time(),
            environment={}
        )
        
        regression_detected, comparison = benchmarker.detect_regression("test_baseline", worse_metrics)
        assert regression_detected is True
        assert comparison is not None
        
        self.logger.info("✅ Baseline management and regression detection validated")
    
    async def test_async_benchmarking_context(self):
        """Test async benchmarking context manager."""
        benchmarker = PerformanceBenchmarker()
        thresholds = PerformanceThresholds(max_response_time_ms=200.0)
        
        async with benchmarker.benchmark_async("async_context_test", thresholds) as context:
            # Simulate operations
            for i in range(5):
                start = time.time()
                await asyncio.sleep(0.01)  # 10ms operation
                duration = (time.time() - start) * 1000
                context.record_operation(duration, success=True)
        
        # Verify result
        assert hasattr(context, 'result')
        assert context.result.test_name == "async_context_test"
        assert context.result.metrics.total_operations == 5
        assert context.result.metrics.successful_operations == 5
        assert context.result.passed is True
        
        self.logger.info("✅ Async benchmarking context validated")
    
    def test_sync_benchmarking_context(self):
        """Test sync benchmarking context manager."""
        benchmarker = PerformanceBenchmarker()
        thresholds = PerformanceThresholds(max_response_time_ms=200.0)
        
        with benchmarker.benchmark_sync("sync_context_test", thresholds) as context:
            # Simulate operations
            for i in range(3):
                start = time.time()
                time.sleep(0.01)  # 10ms operation
                duration = (time.time() - start) * 1000
                context.record_operation(duration, success=True)
        
        # Verify result
        assert hasattr(context, 'result')
        assert context.result.test_name == "sync_context_test"
        assert context.result.metrics.total_operations == 3
        assert context.result.metrics.successful_operations == 3
        assert context.result.passed is True
        
        self.logger.info("✅ Sync benchmarking context validated")


class TestBenchmarkDecorators:
    """Test benchmark decorators."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test environment."""
        setup_secure_test_logging()
        self.logger = create_test_logger("benchmark_decorators_test")
    
    @pytest.mark.asyncio
    async def test_async_benchmark_decorator(self):
        """Test async benchmark decorator."""
        thresholds = PerformanceThresholds(max_response_time_ms=100.0)
        
        @async_benchmark("decorated_async_test", thresholds)
        async def test_function():
            await asyncio.sleep(0.01)
            return "success"
        
        result = await test_function()
        assert result == "success"
        
        self.logger.info("✅ Async benchmark decorator validated")
    
    def test_sync_benchmark_decorator(self):
        """Test sync benchmark decorator."""
        thresholds = PerformanceThresholds(max_response_time_ms=100.0)
        
        @sync_benchmark("decorated_sync_test", thresholds)
        def test_function():
            time.sleep(0.01)
            return "success"
        
        result = test_function()
        assert result == "success"
        
        self.logger.info("✅ Sync benchmark decorator validated")
    
    @pytest.mark.asyncio
    async def test_decorator_error_handling(self):
        """Test decorator error handling."""
        thresholds = PerformanceThresholds()
        
        @async_benchmark("decorated_error_test", thresholds)
        async def failing_function():
            await asyncio.sleep(0.01)
            raise ValueError("Test error")
        
        with pytest.raises(ValueError, match="Test error"):
            await failing_function()
        
        self.logger.info("✅ Decorator error handling validated")


class TestGlobalBenchmarker:
    """Test global benchmarker functionality."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test environment."""
        setup_secure_test_logging()
        self.logger = create_test_logger("global_benchmarker_test")
    
    def test_global_benchmarker_singleton(self):
        """Test global benchmarker singleton behavior."""
        benchmarker1 = get_performance_benchmarker()
        benchmarker2 = get_performance_benchmarker()
        
        assert benchmarker1 is benchmarker2
        
        self.logger.info("✅ Global benchmarker singleton validated")
    
    def test_global_benchmarker_with_custom_baseline(self):
        """Test global benchmarker with custom baseline file."""
        # Reset global instance
        import tests.utils.performance_benchmarking
        tests.utils.performance_benchmarking._global_benchmarker = None
        
        custom_file = "custom_baselines.json"
        benchmarker = get_performance_benchmarker(custom_file)
        
        assert str(benchmarker.baseline_file) == custom_file
        
        self.logger.info("✅ Global benchmarker with custom baseline validated")


@pytest.mark.asyncio
class TestPerformanceIntegration:
    """Test performance benchmarking integration scenarios."""
    
    @pytest.fixture(autouse=True)
    def setup(self):
        """Setup test environment."""
        setup_secure_test_logging()  
        self.logger = create_test_logger("performance_integration_test")
    
    async def test_realistic_performance_scenario(self):
        """Test realistic performance benchmarking scenario."""
        benchmarker = PerformanceBenchmarker()
        
        # Realistic thresholds for a data collection operation
        thresholds = PerformanceThresholds(
            max_response_time_ms=500.0,
            min_throughput_ops_per_sec=2.0,
            max_memory_mb=100.0,
            max_cpu_percent=30.0,
            max_error_rate_percent=5.0
        )
        
        async with benchmarker.benchmark_async("realistic_data_collection", thresholds) as context:
            # Simulate data collection operations
            for i in range(10):
                start = time.time()
                
                # Simulate variable response times
                if i % 3 == 0:
                    await asyncio.sleep(0.1)  # Slower operation
                else:
                    await asyncio.sleep(0.05)  # Faster operation
                
                duration = (time.time() - start) * 1000
                
                # Simulate occasional failures
                success = i % 7 != 0  # ~14% failure rate
                error = None if success else f"Simulated error {i}"
                
                context.record_operation(duration, success, error)
        
        result = context.result
        
        # Verify realistic metrics
        assert result.metrics.total_operations == 10
        assert result.metrics.successful_operations < 10  # Some failures expected
        assert result.metrics.error_rate_percent > 0  # Some errors expected
        assert result.metrics.avg_response_time_ms > 0
        
        # Log performance summary
        self.logger.info(f"📊 Performance Summary:")
        self.logger.info(f"   - Operations: {result.metrics.total_operations}")
        self.logger.info(f"   - Success rate: {result.metrics.success_rate_percent:.1f}%")
        self.logger.info(f"   - Avg response time: {result.metrics.avg_response_time_ms:.1f}ms")
        self.logger.info(f"   - Throughput: {result.metrics.ops_per_second:.2f} ops/sec")
        self.logger.info(f"   - Peak memory: {result.metrics.peak_memory_mb:.1f}MB")
        
        if result.violations:
            self.logger.info(f"⚠️ Threshold violations:")
            for violation in result.violations:
                self.logger.info(f"   - {violation}")
        
        self.logger.info("✅ Realistic performance scenario completed")
    
    def test_performance_regression_detection(self):
        """Test performance regression detection over multiple runs."""
        with tempfile.TemporaryDirectory() as temp_dir:
            baseline_file = Path(temp_dir) / "regression_test_baselines.json"
            benchmarker = PerformanceBenchmarker(str(baseline_file))
            
            thresholds = PerformanceThresholds(max_response_time_ms=200.0)
            
            # First run - establish baseline
            with benchmarker.benchmark_sync("regression_test", thresholds) as context:
                for i in range(5):
                    start = time.time()
                    time.sleep(0.05)  # 50ms operations
                    duration = (time.time() - start) * 1000
                    context.record_operation(duration, success=True)
            
            baseline_result = context.result
            assert baseline_result.regression_detected is False
            
            # Second run - similar performance (should pass)
            with benchmarker.benchmark_sync("regression_test", thresholds) as context:
                for i in range(5):
                    start = time.time()
                    time.sleep(0.05)  # Same 50ms operations
                    duration = (time.time() - start) * 1000
                    context.record_operation(duration, success=True)
            
            similar_result = context.result
            assert similar_result.regression_detected is False
            
            # Third run - degraded performance (should detect regression)
            with benchmarker.benchmark_sync("regression_test", thresholds) as context:
                for i in range(5):
                    start = time.time()
                    time.sleep(0.1)  # 100ms operations (100% slower)
                    duration = (time.time() - start) * 1000
                    context.record_operation(duration, success=True)
            
            regression_result = context.result
            assert regression_result.regression_detected is True
            assert regression_result.baseline_comparison is not None
            
            self.logger.info("✅ Performance regression detection validated")


if __name__ == "__main__":
    # Run quick performance benchmarking tests
    async def quick_performance_test():
        setup_secure_test_logging()
        logger = create_test_logger("quick_performance_test")
        logger.info("🧪 Running quick performance benchmarking tests...")
        
        # Test thresholds
        thresholds = PerformanceThresholds()
        assert thresholds.max_response_time_ms == 1000.0
        logger.info("✅ Performance thresholds validated")
        
        # Test resource monitor
        monitor = ResourceMonitor(sampling_interval=0.01)
        monitor.start_monitoring()
        time.sleep(0.02)
        stats = monitor.stop_monitoring()
        assert "peak_memory_mb" in stats
        logger.info("✅ Resource monitor validated")
        
        # Test benchmarker
        benchmarker = PerformanceBenchmarker()
        percentiles = benchmarker.calculate_percentiles([10, 20, 30, 40, 50])
        assert percentiles["p50"] == 30
        logger.info("✅ Performance benchmarker validated")
        
        # Test async context
        async with benchmarker.benchmark_async("quick_test") as context:
            start = time.time()
            await asyncio.sleep(0.01)
            duration = (time.time() - start) * 1000
            context.record_operation(duration, success=True)
        
        assert context.result.metrics.total_operations == 1
        logger.info("✅ Async benchmarking context validated")
        
        logger.info("✅ Quick performance benchmarking tests completed")
    
    asyncio.run(quick_performance_test())