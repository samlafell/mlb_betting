"""
Performance benchmarking utilities for testing.

Provides performance measurement, threshold validation, and regression detection.
"""

import asyncio
import json
import statistics
import time
import psutil
import threading
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable, Union
from uuid import uuid4

from tests.utils.logging_utils import create_test_logger, setup_secure_test_logging


@dataclass
class PerformanceThresholds:
    """Performance thresholds for validation."""
    
    max_response_time_ms: float = 1000.0  # 1 second
    min_throughput_ops_per_sec: float = 1.0  # 1 operation per second
    max_memory_mb: float = 500.0  # 500 MB
    max_cpu_percent: float = 50.0  # 50% CPU
    max_p95_response_time_ms: float = 2000.0  # 2 seconds P95
    max_error_rate_percent: float = 1.0  # 1% error rate
    min_success_rate_percent: float = 99.0  # 99% success rate


@dataclass
class PerformanceMetrics:
    """Performance metrics collected during testing."""
    
    # Timing metrics
    start_time: float
    end_time: float
    duration_ms: float
    
    # Throughput metrics
    total_operations: int
    successful_operations: int
    failed_operations: int
    ops_per_second: float
    
    # Response time metrics
    response_times_ms: List[float]
    avg_response_time_ms: float
    min_response_time_ms: float
    max_response_time_ms: float
    p50_response_time_ms: float
    p95_response_time_ms: float
    p99_response_time_ms: float
    
    # Resource metrics
    peak_memory_mb: float
    avg_memory_mb: float
    peak_cpu_percent: float
    avg_cpu_percent: float
    
    # Quality metrics
    success_rate_percent: float
    error_rate_percent: float
    errors: List[str]
    
    # Metadata
    test_id: str
    timestamp: datetime
    environment: Dict[str, Any]


@dataclass
class BenchmarkResult:
    """Result of a performance benchmark."""
    
    test_name: str
    metrics: PerformanceMetrics
    thresholds: PerformanceThresholds
    passed: bool
    violations: List[str]
    regression_detected: bool
    baseline_comparison: Optional[Dict[str, float]] = None


class ResourceMonitor:
    """Monitors system resources during performance tests."""
    
    def __init__(self, sampling_interval: float = 0.1):
        self.sampling_interval = sampling_interval
        self.monitoring = False
        self.monitor_thread: Optional[threading.Thread] = None
        
        # Resource tracking
        self.memory_samples: List[float] = []
        self.cpu_samples: List[float] = []
        self.start_time: float = 0.0
        
        # Process reference
        self.process = psutil.Process()
    
    def start_monitoring(self):
        """Start resource monitoring."""
        if self.monitoring:
            return
        
        self.monitoring = True
        self.memory_samples.clear()
        self.cpu_samples.clear()
        self.start_time = time.time()
        
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
    
    def stop_monitoring(self) -> Dict[str, float]:
        """Stop monitoring and return resource statistics."""
        if not self.monitoring:
            return {}
        
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1.0)
        
        # Calculate statistics
        memory_stats = {
            "peak_memory_mb": max(self.memory_samples) if self.memory_samples else 0.0,
            "avg_memory_mb": statistics.mean(self.memory_samples) if self.memory_samples else 0.0,
            "memory_samples_count": len(self.memory_samples)
        }
        
        cpu_stats = {
            "peak_cpu_percent": max(self.cpu_samples) if self.cpu_samples else 0.0,
            "avg_cpu_percent": statistics.mean(self.cpu_samples) if self.cpu_samples else 0.0,
            "cpu_samples_count": len(self.cpu_samples)
        }
        
        return {**memory_stats, **cpu_stats}
    
    def _monitor_loop(self):
        """Resource monitoring loop."""
        while self.monitoring:
            try:
                # Memory usage in MB
                memory_info = self.process.memory_info()
                memory_mb = memory_info.rss / 1024 / 1024
                self.memory_samples.append(memory_mb)
                
                # CPU usage percentage
                cpu_percent = self.process.cpu_percent()
                self.cpu_samples.append(cpu_percent)
                
                time.sleep(self.sampling_interval)
                
            except Exception:
                # Continue monitoring despite errors
                pass


class PerformanceBenchmarker:
    """Performance benchmarking system with threshold validation."""
    
    def __init__(self, baseline_file: Optional[str] = None):
        self.logger = create_test_logger("performance_benchmarker")
        self.baseline_file = Path(baseline_file) if baseline_file else None
        self.baselines: Dict[str, PerformanceMetrics] = {}
        self.resource_monitor = ResourceMonitor()
        
        # Load existing baselines
        if self.baseline_file and self.baseline_file.exists():
            self._load_baselines()
    
    def _load_baselines(self):
        """Load performance baselines from file."""
        try:
            with open(self.baseline_file, 'r') as f:
                baseline_data = json.load(f)
                
            for test_name, metrics_dict in baseline_data.items():
                # Convert dict back to PerformanceMetrics
                metrics_dict['timestamp'] = datetime.fromisoformat(metrics_dict['timestamp'])
                self.baselines[test_name] = PerformanceMetrics(**metrics_dict)
                
            self.logger.info(f"Loaded {len(self.baselines)} performance baselines")
            
        except Exception as e:
            self.logger.warning(f"Failed to load baselines: {e}")
    
    def _save_baselines(self):
        """Save performance baselines to file."""
        if not self.baseline_file:
            return
        
        try:
            # Ensure directory exists
            self.baseline_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Convert metrics to serializable format
            baseline_data = {}
            for test_name, metrics in self.baselines.items():
                metrics_dict = asdict(metrics)
                metrics_dict['timestamp'] = metrics.timestamp.isoformat()
                baseline_data[test_name] = metrics_dict
            
            with open(self.baseline_file, 'w') as f:
                json.dump(baseline_data, f, indent=2)
                
            self.logger.info(f"Saved {len(self.baselines)} performance baselines")
            
        except Exception as e:
            self.logger.error(f"Failed to save baselines: {e}")
    
    @asynccontextmanager
    async def benchmark_async(self, test_name: str, thresholds: PerformanceThresholds = None):
        """Async context manager for performance benchmarking."""
        benchmark_context = AsyncBenchmarkContext(
            test_name=test_name,
            thresholds=thresholds or PerformanceThresholds(),
            benchmarker=self
        )
        
        async with benchmark_context as context:
            yield context
    
    @contextmanager
    def benchmark_sync(self, test_name: str, thresholds: PerformanceThresholds = None):
        """Sync context manager for performance benchmarking."""
        benchmark_context = SyncBenchmarkContext(
            test_name=test_name,
            thresholds=thresholds or PerformanceThresholds(),
            benchmarker=self
        )
        
        with benchmark_context as context:
            yield context
    
    def calculate_percentiles(self, values: List[float]) -> Dict[str, float]:
        """Calculate response time percentiles."""
        if not values:
            return {"p50": 0.0, "p95": 0.0, "p99": 0.0}
        
        sorted_values = sorted(values)
        n = len(sorted_values)
        
        return {
            "p50": sorted_values[int(0.5 * n)],
            "p95": sorted_values[int(0.95 * n)],
            "p99": sorted_values[int(0.99 * n)]
        }
    
    def validate_thresholds(self, metrics: PerformanceMetrics, thresholds: PerformanceThresholds) -> tuple[bool, List[str]]:
        """Validate metrics against thresholds."""
        violations = []
        
        # Response time validations
        if metrics.avg_response_time_ms > thresholds.max_response_time_ms:
            violations.append(
                f"Average response time {metrics.avg_response_time_ms:.1f}ms exceeds threshold {thresholds.max_response_time_ms:.1f}ms"
            )
        
        if metrics.p95_response_time_ms > thresholds.max_p95_response_time_ms:
            violations.append(
                f"P95 response time {metrics.p95_response_time_ms:.1f}ms exceeds threshold {thresholds.max_p95_response_time_ms:.1f}ms"
            )
        
        # Throughput validations
        if metrics.ops_per_second < thresholds.min_throughput_ops_per_sec:
            violations.append(
                f"Throughput {metrics.ops_per_second:.2f} ops/sec below threshold {thresholds.min_throughput_ops_per_sec:.2f} ops/sec"
            )
        
        # Resource validations
        if metrics.peak_memory_mb > thresholds.max_memory_mb:
            violations.append(
                f"Peak memory {metrics.peak_memory_mb:.1f}MB exceeds threshold {thresholds.max_memory_mb:.1f}MB"
            )
        
        if metrics.peak_cpu_percent > thresholds.max_cpu_percent:
            violations.append(
                f"Peak CPU {metrics.peak_cpu_percent:.1f}% exceeds threshold {thresholds.max_cpu_percent:.1f}%"
            )
        
        # Quality validations
        if metrics.error_rate_percent > thresholds.max_error_rate_percent:
            violations.append(
                f"Error rate {metrics.error_rate_percent:.1f}% exceeds threshold {thresholds.max_error_rate_percent:.1f}%"
            )
        
        if metrics.success_rate_percent < thresholds.min_success_rate_percent:
            violations.append(
                f"Success rate {metrics.success_rate_percent:.1f}% below threshold {thresholds.min_success_rate_percent:.1f}%"
            )
        
        return len(violations) == 0, violations
    
    def detect_regression(self, test_name: str, current_metrics: PerformanceMetrics, 
                         regression_threshold: float = 0.2) -> tuple[bool, Optional[Dict[str, float]]]:
        """Detect performance regression against baseline."""
        if test_name not in self.baselines:
            # No baseline - save current as baseline
            self.baselines[test_name] = current_metrics
            self._save_baselines()
            return False, None
        
        baseline = self.baselines[test_name]
        comparison = {}
        
        # Compare key metrics
        metrics_to_compare = [
            ("avg_response_time_ms", "avg_response_time_ms"),
            ("p95_response_time_ms", "p95_response_time_ms"),
            ("ops_per_second", "ops_per_second"),
            ("peak_memory_mb", "peak_memory_mb"),
            ("success_rate_percent", "success_rate_percent")
        ]
        
        regression_detected = False
        
        for metric_name, baseline_attr in metrics_to_compare:
            current_value = getattr(current_metrics, baseline_attr)
            baseline_value = getattr(baseline, baseline_attr)
            
            if baseline_value == 0:
                continue
            
            # Calculate percentage change
            if metric_name in ["ops_per_second", "success_rate_percent"]:
                # Higher is better
                change_percent = (current_value - baseline_value) / baseline_value
                if change_percent < -regression_threshold:  # Significant decrease
                    regression_detected = True
            else:
                # Lower is better
                change_percent = (current_value - baseline_value) / baseline_value
                if change_percent > regression_threshold:  # Significant increase
                    regression_detected = True
            
            comparison[metric_name] = {
                "current": current_value,
                "baseline": baseline_value,
                "change_percent": change_percent * 100,
                "regression": abs(change_percent) > regression_threshold
            }
        
        # Update baseline if this is better
        if not regression_detected and current_metrics.success_rate_percent >= baseline.success_rate_percent:
            self.baselines[test_name] = current_metrics
            self._save_baselines()
        
        return regression_detected, comparison
    
    def create_benchmark_result(self, test_name: str, metrics: PerformanceMetrics, 
                               thresholds: PerformanceThresholds) -> BenchmarkResult:
        """Create benchmark result with validation and regression detection."""
        # Validate thresholds
        passed, violations = self.validate_thresholds(metrics, thresholds)
        
        # Detect regression
        regression_detected, baseline_comparison = self.detect_regression(test_name, metrics)
        
        return BenchmarkResult(
            test_name=test_name,
            metrics=metrics,
            thresholds=thresholds,
            passed=passed and not regression_detected,
            violations=violations,
            regression_detected=regression_detected,
            baseline_comparison=baseline_comparison
        )


class BenchmarkContext:
    """Base benchmark context for tracking performance metrics."""
    
    def __init__(self, test_name: str, thresholds: PerformanceThresholds, 
                 benchmarker: PerformanceBenchmarker):
        self.test_name = test_name
        self.thresholds = thresholds
        self.benchmarker = benchmarker
        self.logger = create_test_logger(f"benchmark_{test_name}")
        
        # Metrics tracking
        self.start_time: float = 0.0
        self.end_time: float = 0.0
        self.response_times: List[float] = []
        self.operations_count: int = 0
        self.successful_operations: int = 0
        self.failed_operations: int = 0
        self.errors: List[str] = []
        
        # Test ID for correlation
        self.test_id = str(uuid4())
    
    def record_operation(self, duration_ms: float, success: bool = True, error: str = None):
        """Record a single operation's performance."""
        self.operations_count += 1
        self.response_times.append(duration_ms)
        
        if success:
            self.successful_operations += 1
        else:
            self.failed_operations += 1
            if error:
                self.errors.append(error)
    
    def get_environment_info(self) -> Dict[str, Any]:
        """Get current environment information."""
        import platform
        import sys
        
        return {
            "python_version": sys.version,
            "platform": platform.platform(),
            "cpu_count": psutil.cpu_count(),
            "memory_total_mb": psutil.virtual_memory().total / 1024 / 1024,
            "test_id": self.test_id
        }


class AsyncBenchmarkContext(BenchmarkContext):
    """Async benchmark context manager."""
    
    async def __aenter__(self):
        """Start benchmarking."""
        self.start_time = time.time()
        self.benchmarker.resource_monitor.start_monitoring()
        self.logger.info(f"Started performance benchmark: {self.test_name}")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Finish benchmarking and create results."""
        self.end_time = time.time()
        resource_stats = self.benchmarker.resource_monitor.stop_monitoring()
        
        # Calculate performance metrics
        duration_ms = (self.end_time - self.start_time) * 1000
        ops_per_second = self.operations_count / (duration_ms / 1000) if duration_ms > 0 else 0.0
        
        percentiles = self.benchmarker.calculate_percentiles(self.response_times)
        
        success_rate = (self.successful_operations / self.operations_count * 100) if self.operations_count > 0 else 100.0
        error_rate = (self.failed_operations / self.operations_count * 100) if self.operations_count > 0 else 0.0
        
        metrics = PerformanceMetrics(
            start_time=self.start_time,
            end_time=self.end_time,
            duration_ms=duration_ms,
            total_operations=self.operations_count,
            successful_operations=self.successful_operations,
            failed_operations=self.failed_operations,
            ops_per_second=ops_per_second,
            response_times_ms=self.response_times,
            avg_response_time_ms=statistics.mean(self.response_times) if self.response_times else 0.0,
            min_response_time_ms=min(self.response_times) if self.response_times else 0.0,
            max_response_time_ms=max(self.response_times) if self.response_times else 0.0,
            p50_response_time_ms=percentiles["p50"],
            p95_response_time_ms=percentiles["p95"],
            p99_response_time_ms=percentiles["p99"],
            peak_memory_mb=resource_stats.get("peak_memory_mb", 0.0),
            avg_memory_mb=resource_stats.get("avg_memory_mb", 0.0),
            peak_cpu_percent=resource_stats.get("peak_cpu_percent", 0.0),
            avg_cpu_percent=resource_stats.get("avg_cpu_percent", 0.0),
            success_rate_percent=success_rate,
            error_rate_percent=error_rate,
            errors=self.errors[:10],  # Limit error list
            test_id=self.test_id,
            timestamp=datetime.utcnow(),
            environment=self.get_environment_info()
        )
        
        # Create benchmark result
        result = self.benchmarker.create_benchmark_result(self.test_name, metrics, self.thresholds)
        
        # Log results
        if result.passed:
            self.logger.info(f"✅ Benchmark PASSED: {self.test_name}")
        else:
            self.logger.warning(f"❌ Benchmark FAILED: {self.test_name}")
            for violation in result.violations:
                self.logger.warning(f"   - {violation}")
        
        if result.regression_detected:
            self.logger.warning(f"⚠️ Performance regression detected: {self.test_name}")
        
        # Store result for access
        self.result = result


class SyncBenchmarkContext(BenchmarkContext):
    """Sync benchmark context manager."""
    
    def __enter__(self):
        """Start benchmarking."""
        self.start_time = time.time()
        self.benchmarker.resource_monitor.start_monitoring()
        self.logger.info(f"Started performance benchmark: {self.test_name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Finish benchmarking and create results."""
        self.end_time = time.time()
        resource_stats = self.benchmarker.resource_monitor.stop_monitoring()
        
        # Calculate performance metrics (same as async version)
        duration_ms = (self.end_time - self.start_time) * 1000
        ops_per_second = self.operations_count / (duration_ms / 1000) if duration_ms > 0 else 0.0
        
        percentiles = self.benchmarker.calculate_percentiles(self.response_times)
        
        success_rate = (self.successful_operations / self.operations_count * 100) if self.operations_count > 0 else 100.0
        error_rate = (self.failed_operations / self.operations_count * 100) if self.operations_count > 0 else 0.0
        
        metrics = PerformanceMetrics(
            start_time=self.start_time,
            end_time=self.end_time,
            duration_ms=duration_ms,
            total_operations=self.operations_count,
            successful_operations=self.successful_operations,
            failed_operations=self.failed_operations,
            ops_per_second=ops_per_second,
            response_times_ms=self.response_times,
            avg_response_time_ms=statistics.mean(self.response_times) if self.response_times else 0.0,
            min_response_time_ms=min(self.response_times) if self.response_times else 0.0,
            max_response_time_ms=max(self.response_times) if self.response_times else 0.0,
            p50_response_time_ms=percentiles["p50"],
            p95_response_time_ms=percentiles["p95"],
            p99_response_time_ms=percentiles["p99"],
            peak_memory_mb=resource_stats.get("peak_memory_mb", 0.0),
            avg_memory_mb=resource_stats.get("avg_memory_mb", 0.0),
            peak_cpu_percent=resource_stats.get("peak_cpu_percent", 0.0),
            avg_cpu_percent=resource_stats.get("avg_cpu_percent", 0.0),
            success_rate_percent=success_rate,
            error_rate_percent=error_rate,
            errors=self.errors[:10],
            test_id=self.test_id,
            timestamp=datetime.utcnow(),
            environment=self.get_environment_info()
        )
        
        # Create benchmark result
        result = self.benchmarker.create_benchmark_result(self.test_name, metrics, self.thresholds)
        
        # Log results
        if result.passed:
            self.logger.info(f"✅ Benchmark PASSED: {self.test_name}")
        else:
            self.logger.warning(f"❌ Benchmark FAILED: {self.test_name}")
            for violation in result.violations:
                self.logger.warning(f"   - {violation}")
        
        if result.regression_detected:
            self.logger.warning(f"⚠️ Performance regression detected: {self.test_name}")
        
        # Store result for access
        self.result = result


# Convenience functions and decorators
def async_benchmark(test_name: str, thresholds: PerformanceThresholds = None, 
                   baseline_file: str = None):
    """Decorator for async performance benchmarking."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            benchmarker = PerformanceBenchmarker(baseline_file)
            async with benchmarker.benchmark_async(test_name, thresholds) as context:
                start_time = time.time()
                try:
                    result = await func(*args, **kwargs)
                    duration_ms = (time.time() - start_time) * 1000
                    context.record_operation(duration_ms, success=True)
                    return result
                except Exception as e:
                    duration_ms = (time.time() - start_time) * 1000
                    context.record_operation(duration_ms, success=False, error=str(e))
                    raise
        return wrapper
    return decorator


def sync_benchmark(test_name: str, thresholds: PerformanceThresholds = None, 
                  baseline_file: str = None):
    """Decorator for sync performance benchmarking."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            benchmarker = PerformanceBenchmarker(baseline_file)
            with benchmarker.benchmark_sync(test_name, thresholds) as context:
                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    duration_ms = (time.time() - start_time) * 1000
                    context.record_operation(duration_ms, success=True)
                    return result
                except Exception as e:
                    duration_ms = (time.time() - start_time) * 1000
                    context.record_operation(duration_ms, success=False, error=str(e))
                    raise
        return wrapper
    return decorator


# Global benchmarker for convenience
_global_benchmarker: Optional[PerformanceBenchmarker] = None


def get_performance_benchmarker(baseline_file: str = None) -> PerformanceBenchmarker:
    """Get global performance benchmarker instance."""
    global _global_benchmarker
    
    if _global_benchmarker is None:
        default_baseline_file = baseline_file or "tests/performance_baselines.json"
        _global_benchmarker = PerformanceBenchmarker(default_baseline_file)
    
    return _global_benchmarker


if __name__ == "__main__":
    # Quick demonstration
    async def demo_async_benchmark():
        setup_secure_test_logging()
        logger = create_test_logger("demo")
        
        # Demo async benchmarking
        benchmarker = PerformanceBenchmarker()
        thresholds = PerformanceThresholds(max_response_time_ms=500, min_throughput_ops_per_sec=2.0)
        
        async with benchmarker.benchmark_async("demo_async_test", thresholds) as context:
            # Simulate some operations
            for i in range(10):
                start = time.time()
                await asyncio.sleep(0.1)  # Simulate work
                duration = (time.time() - start) * 1000
                context.record_operation(duration, success=True)
        
        logger.info(f"Async benchmark result: passed={context.result.passed}")
        
        # Demo sync benchmarking
        with benchmarker.benchmark_sync("demo_sync_test", thresholds) as context:
            # Simulate some operations
            for i in range(5):
                start = time.time()
                time.sleep(0.05)  # Simulate work
                duration = (time.time() - start) * 1000
                context.record_operation(duration, success=True)
        
        logger.info(f"Sync benchmark result: passed={context.result.passed}")
        logger.info("✅ Performance benchmarking demo completed")
    
    asyncio.run(demo_async_benchmark())