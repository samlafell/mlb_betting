"""
Performance monitoring and optimization for ML Opportunity Detection System

Provides comprehensive performance tracking including:
- Real-time performance metrics
- Resource usage monitoring
- Performance benchmarking and validation
- Optimization recommendations
- Circuit breakers for resource protection
"""

import time
import psutil
import asyncio
import statistics
from typing import Dict, Any, List, Optional, Callable, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import deque, defaultdict
from enum import Enum
import threading
import functools

from src.ml.opportunity_detection.config import get_performance_config, PerformanceConfig
from src.core.logging import get_logger, LogComponent


class PerformanceMetricType(Enum):
    """Types of performance metrics"""
    RESPONSE_TIME = "response_time"
    THROUGHPUT = "throughput"
    ERROR_RATE = "error_rate"
    RESOURCE_USAGE = "resource_usage"
    CACHE_PERFORMANCE = "cache_performance"
    ML_MODEL_PERFORMANCE = "ml_model_performance"


class PerformanceStatus(Enum):
    """Performance status levels"""
    EXCELLENT = "EXCELLENT"  # Under 50% of targets
    GOOD = "GOOD"           # 50-75% of targets
    WARNING = "WARNING"     # 75-90% of targets
    CRITICAL = "CRITICAL"   # 90-100% of targets
    FAILURE = "FAILURE"     # Over targets


@dataclass
class PerformanceMetric:
    """Individual performance metric"""
    name: str
    value: float
    unit: str
    timestamp: datetime
    target: Optional[float] = None
    threshold_warning: Optional[float] = None
    threshold_critical: Optional[float] = None
    
    @property
    def status(self) -> PerformanceStatus:
        """Determine performance status based on targets"""
        if self.target is None:
            return PerformanceStatus.GOOD
        
        percentage = (self.value / self.target) * 100
        
        if percentage <= 50:
            return PerformanceStatus.EXCELLENT
        elif percentage <= 75:
            return PerformanceStatus.GOOD
        elif percentage <= 90:
            return PerformanceStatus.WARNING
        elif percentage <= 100:
            return PerformanceStatus.CRITICAL
        else:
            return PerformanceStatus.FAILURE


@dataclass
class PerformanceBenchmark:
    """Performance benchmark results"""
    operation_name: str
    target_time_ms: float
    actual_time_ms: float
    sample_size: int
    success_rate: float
    percentiles: Dict[int, float] = field(default_factory=dict)
    
    @property
    def meets_target(self) -> bool:
        """Check if benchmark meets target"""
        return self.actual_time_ms <= self.target_time_ms
    
    @property
    def performance_ratio(self) -> float:
        """Get performance ratio (actual/target)"""
        return self.actual_time_ms / self.target_time_ms if self.target_time_ms > 0 else 1.0


class CircuitBreaker:
    """Circuit breaker for resource protection"""
    
    def __init__(self, failure_threshold: int = 10, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self._lock = threading.Lock()
    
    def can_execute(self) -> bool:
        """Check if operation can execute"""
        with self._lock:
            if self.state == "CLOSED":
                return True
            elif self.state == "OPEN":
                # Check if we should try recovery
                if (self.last_failure_time and 
                    (datetime.utcnow() - self.last_failure_time).seconds > self.recovery_timeout):
                    self.state = "HALF_OPEN"
                    return True
                return False
            else:  # HALF_OPEN
                return True
    
    def record_success(self):
        """Record successful operation"""
        with self._lock:
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
            self.failure_count = 0
    
    def record_failure(self):
        """Record failed operation"""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = datetime.utcnow()
            
            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"


class PerformanceTracker:
    """Real-time performance tracking and analysis"""
    
    def __init__(self, config: PerformanceConfig):
        self.config = config
        self.logger = get_logger("ml.performance", LogComponent.ANALYSIS)
        
        # Metric storage with time-based cleanup
        self.metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.operation_times: Dict[str, deque] = defaultdict(lambda: deque(maxlen=500))
        self.error_counts: Dict[str, int] = defaultdict(int)
        self.total_operations: Dict[str, int] = defaultdict(int)
        
        # Circuit breakers for different operations
        self.circuit_breakers: Dict[str, CircuitBreaker] = {
            'opportunity_discovery': CircuitBreaker(failure_threshold=5, recovery_timeout=30),
            'pattern_detection': CircuitBreaker(failure_threshold=10, recovery_timeout=60),
            'ml_prediction': CircuitBreaker(failure_threshold=15, recovery_timeout=120),
            'database_query': CircuitBreaker(failure_threshold=8, recovery_timeout=45)
        }
        
        # Performance benchmarks
        self.benchmarks: Dict[str, PerformanceBenchmark] = {}
        
        # Resource monitoring
        self.resource_usage_history: deque = deque(maxlen=100)
        self.last_cleanup = datetime.utcnow()
    
    def track_operation_time(self, operation_name: str, duration_ms: float, success: bool = True):
        """Track operation timing and success/failure"""
        now = datetime.utcnow()
        
        # Record operation time
        self.operation_times[operation_name].append((duration_ms, now))
        self.total_operations[operation_name] += 1
        
        # Track errors
        if not success:
            self.error_counts[operation_name] += 1
            self.circuit_breakers.get(operation_name, CircuitBreaker()).record_failure()
        else:
            self.circuit_breakers.get(operation_name, CircuitBreaker()).record_success()
        
        # Create performance metric
        target = self._get_operation_target(operation_name)
        metric = PerformanceMetric(
            name=f"{operation_name}_response_time",
            value=duration_ms,
            unit="ms",
            timestamp=now,
            target=target
        )
        
        self.metrics[operation_name].append(metric)
        
        # Log performance issues
        if target and duration_ms > target:
            self.logger.warning(f"Performance target exceeded for {operation_name}: "
                               f"{duration_ms:.1f}ms > {target:.1f}ms target")
    
    def track_resource_usage(self):
        """Track system resource usage"""
        try:
            cpu_percent = psutil.cpu_percent(interval=None)
            memory = psutil.virtual_memory()
            disk_io = psutil.disk_io_counters() if psutil.disk_io_counters() else None
            
            resource_data = {
                'timestamp': datetime.utcnow(),
                'cpu_percent': cpu_percent,
                'memory_percent': memory.percent,
                'memory_used_mb': memory.used / (1024 * 1024),
                'memory_available_mb': memory.available / (1024 * 1024),
                'disk_read_mb': disk_io.read_bytes / (1024 * 1024) if disk_io else 0,
                'disk_write_mb': disk_io.write_bytes / (1024 * 1024) if disk_io else 0
            }
            
            self.resource_usage_history.append(resource_data)
            
            # Check for resource warnings
            if cpu_percent > 80:
                self.logger.warning(f"High CPU usage: {cpu_percent:.1f}%")
            
            if memory.percent > 85:
                self.logger.warning(f"High memory usage: {memory.percent:.1f}%")
            
        except Exception as e:
            self.logger.error(f"Error tracking resource usage: {e}")
    
    def get_operation_statistics(self, operation_name: str, hours_back: int = 1) -> Dict[str, Any]:
        """Get comprehensive statistics for an operation"""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)
        
        # Get recent operation times
        recent_times = [
            duration for duration, timestamp in self.operation_times[operation_name]
            if timestamp > cutoff_time
        ]
        
        if not recent_times:
            return {
                'operation_name': operation_name,
                'sample_size': 0,
                'time_period_hours': hours_back
            }
        
        # Calculate statistics
        stats = {
            'operation_name': operation_name,
            'sample_size': len(recent_times),
            'time_period_hours': hours_back,
            'mean_ms': statistics.mean(recent_times),
            'median_ms': statistics.median(recent_times),
            'min_ms': min(recent_times),
            'max_ms': max(recent_times),
            'std_dev_ms': statistics.stdev(recent_times) if len(recent_times) > 1 else 0,
            'percentiles': {
                50: statistics.median(recent_times),
                75: self._percentile(recent_times, 75),
                90: self._percentile(recent_times, 90),
                95: self._percentile(recent_times, 95),
                99: self._percentile(recent_times, 99)
            }
        }
        
        # Add target comparison
        target = self._get_operation_target(operation_name)
        if target:
            stats['target_ms'] = target
            stats['meets_target'] = stats['mean_ms'] <= target
            stats['performance_ratio'] = stats['mean_ms'] / target
        
        # Add error rate
        total_ops = self.total_operations[operation_name]
        error_count = self.error_counts[operation_name]
        stats['error_rate'] = (error_count / total_ops) * 100 if total_ops > 0 else 0
        stats['success_rate'] = 100 - stats['error_rate']
        
        # Circuit breaker status
        circuit_breaker = self.circuit_breakers.get(operation_name)
        if circuit_breaker:
            stats['circuit_breaker_state'] = circuit_breaker.state
            stats['failure_count'] = circuit_breaker.failure_count
        
        return stats
    
    def get_resource_usage_summary(self, minutes_back: int = 30) -> Dict[str, Any]:
        """Get resource usage summary"""
        cutoff_time = datetime.utcnow() - timedelta(minutes=minutes_back)
        
        recent_usage = [
            usage for usage in self.resource_usage_history
            if usage['timestamp'] > cutoff_time
        ]
        
        if not recent_usage:
            return {'time_period_minutes': minutes_back, 'sample_size': 0}
        
        cpu_values = [u['cpu_percent'] for u in recent_usage]
        memory_values = [u['memory_percent'] for u in recent_usage]
        
        return {
            'time_period_minutes': minutes_back,
            'sample_size': len(recent_usage),
            'cpu': {
                'current': recent_usage[-1]['cpu_percent'],
                'average': statistics.mean(cpu_values),
                'max': max(cpu_values),
                'min': min(cpu_values)
            },
            'memory': {
                'current': recent_usage[-1]['memory_percent'],
                'average': statistics.mean(memory_values),
                'max': max(memory_values),
                'min': min(memory_values),
                'current_used_mb': recent_usage[-1]['memory_used_mb'],
                'current_available_mb': recent_usage[-1]['memory_available_mb']
            }
        }
    
    def run_performance_benchmark(self, operation_name: str, operation_func: Callable, 
                                 iterations: int = 100) -> PerformanceBenchmark:
        """Run performance benchmark for an operation"""
        self.logger.info(f"Running performance benchmark for {operation_name} ({iterations} iterations)")
        
        target = self._get_operation_target(operation_name)
        times = []
        successes = 0
        
        for i in range(iterations):
            start_time = time.time()
            try:
                result = operation_func()
                if asyncio.iscoroutine(result):
                    # Handle async operations
                    loop = asyncio.get_event_loop()
                    loop.run_until_complete(result)
                successes += 1
            except Exception as e:
                self.logger.error(f"Benchmark iteration {i} failed: {e}")
            
            duration_ms = (time.time() - start_time) * 1000
            times.append(duration_ms)
        
        # Calculate percentiles
        percentiles = {}
        if times:
            for p in [50, 75, 90, 95, 99]:
                percentiles[p] = self._percentile(times, p)
        
        benchmark = PerformanceBenchmark(
            operation_name=operation_name,
            target_time_ms=target or 1000.0,
            actual_time_ms=statistics.mean(times) if times else 0,
            sample_size=iterations,
            success_rate=(successes / iterations) * 100,
            percentiles=percentiles
        )
        
        self.benchmarks[operation_name] = benchmark
        
        self.logger.info(f"Benchmark completed for {operation_name}: "
                        f"{benchmark.actual_time_ms:.1f}ms avg "
                        f"({benchmark.success_rate:.1f}% success)")
        
        return benchmark
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary"""
        summary = {
            'timestamp': datetime.utcnow().isoformat(),
            'operations': {},
            'resource_usage': self.get_resource_usage_summary(),
            'circuit_breakers': {},
            'benchmarks': {},
            'overall_status': PerformanceStatus.GOOD.value
        }
        
        # Get operation statistics
        for operation_name in self.operation_times.keys():
            summary['operations'][operation_name] = self.get_operation_statistics(operation_name)
        
        # Circuit breaker status
        for name, breaker in self.circuit_breakers.items():
            summary['circuit_breakers'][name] = {
                'state': breaker.state,
                'failure_count': breaker.failure_count,
                'can_execute': breaker.can_execute()
            }
        
        # Benchmark results
        for name, benchmark in self.benchmarks.items():
            summary['benchmarks'][name] = {
                'meets_target': benchmark.meets_target,
                'performance_ratio': benchmark.performance_ratio,
                'success_rate': benchmark.success_rate,
                'sample_size': benchmark.sample_size
            }
        
        # Determine overall status
        summary['overall_status'] = self._calculate_overall_status(summary)
        
        return summary
    
    def can_execute_operation(self, operation_name: str) -> bool:
        """Check if operation can execute (circuit breaker)"""
        breaker = self.circuit_breakers.get(operation_name)
        return breaker.can_execute() if breaker else True
    
    def cleanup_old_metrics(self):
        """Clean up old metrics to manage memory"""
        now = datetime.utcnow()
        
        # Only cleanup every 10 minutes
        if (now - self.last_cleanup).seconds < 600:
            return
        
        cutoff_time = now - timedelta(hours=2)  # Keep 2 hours of detailed metrics
        
        for operation_name, times in self.operation_times.items():
            # Remove old entries
            while times and times[0][1] < cutoff_time:
                times.popleft()
        
        for metric_name, metrics in self.metrics.items():
            # Remove old metrics
            while metrics and metrics[0].timestamp < cutoff_time:
                metrics.popleft()
        
        self.last_cleanup = now
    
    def _get_operation_target(self, operation_name: str) -> Optional[float]:
        """Get performance target for operation"""
        targets = {
            'opportunity_discovery': self.config.max_discovery_time_ms,
            'opportunity_scoring': self.config.max_scoring_time_ms,
            'pattern_detection': self.config.max_pattern_time_ms,
            'explanation_generation': self.config.max_explanation_time_ms
        }
        return targets.get(operation_name)
    
    def _percentile(self, data: List[float], percentile: int) -> float:
        """Calculate percentile of data"""
        if not data:
            return 0
        
        sorted_data = sorted(data)
        index = (percentile / 100) * (len(sorted_data) - 1)
        
        if index.is_integer():
            return sorted_data[int(index)]
        else:
            lower = sorted_data[int(index)]
            upper = sorted_data[int(index) + 1]
            return lower + (upper - lower) * (index - int(index))
    
    def _calculate_overall_status(self, summary: Dict[str, Any]) -> str:
        """Calculate overall performance status"""
        status_scores = []
        
        # Check operation performance
        for operation_data in summary['operations'].values():
            if 'performance_ratio' in operation_data:
                ratio = operation_data['performance_ratio']
                if ratio <= 0.5:
                    status_scores.append(4)  # Excellent
                elif ratio <= 0.75:
                    status_scores.append(3)  # Good
                elif ratio <= 0.9:
                    status_scores.append(2)  # Warning
                elif ratio <= 1.0:
                    status_scores.append(1)  # Critical
                else:
                    status_scores.append(0)  # Failure
        
        # Check circuit breakers
        for breaker_data in summary['circuit_breakers'].values():
            if breaker_data['state'] == 'CLOSED':
                status_scores.append(4)
            elif breaker_data['state'] == 'HALF_OPEN':
                status_scores.append(2)
            else:  # OPEN
                status_scores.append(0)
        
        # Check resource usage
        resource_usage = summary['resource_usage']
        if resource_usage.get('sample_size', 0) > 0:
            cpu_avg = resource_usage['cpu']['average']
            memory_avg = resource_usage['memory']['average']
            
            cpu_score = 4 if cpu_avg < 50 else 3 if cpu_avg < 70 else 2 if cpu_avg < 85 else 0
            memory_score = 4 if memory_avg < 60 else 3 if memory_avg < 75 else 2 if memory_avg < 90 else 0
            
            status_scores.extend([cpu_score, memory_score])
        
        # Calculate overall score
        if not status_scores:
            return PerformanceStatus.GOOD.value
        
        avg_score = statistics.mean(status_scores)
        
        if avg_score >= 3.5:
            return PerformanceStatus.EXCELLENT.value
        elif avg_score >= 2.5:
            return PerformanceStatus.GOOD.value
        elif avg_score >= 1.5:
            return PerformanceStatus.WARNING.value
        elif avg_score >= 0.5:
            return PerformanceStatus.CRITICAL.value
        else:
            return PerformanceStatus.FAILURE.value


# Performance monitoring decorator
def monitor_performance(operation_name: str, track_errors: bool = True):
    """Decorator to automatically monitor function performance"""
    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            tracker = get_performance_tracker()
            
            # Check circuit breaker
            if not tracker.can_execute_operation(operation_name):
                raise Exception(f"Circuit breaker open for {operation_name}")
            
            start_time = time.time()
            success = True
            
            try:
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                return result
            except Exception as e:
                success = False
                if track_errors:
                    raise
                return None
            finally:
                duration_ms = (time.time() - start_time) * 1000
                tracker.track_operation_time(operation_name, duration_ms, success)
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            tracker = get_performance_tracker()
            
            # Check circuit breaker
            if not tracker.can_execute_operation(operation_name):
                raise Exception(f"Circuit breaker open for {operation_name}")
            
            start_time = time.time()
            success = True
            
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                success = False
                if track_errors:
                    raise
                return None
            finally:
                duration_ms = (time.time() - start_time) * 1000
                tracker.track_operation_time(operation_name, duration_ms, success)
        
        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


# Global instance
_performance_tracker: Optional[PerformanceTracker] = None


def get_performance_tracker() -> PerformanceTracker:
    """Get global performance tracker instance"""
    global _performance_tracker
    if _performance_tracker is None:
        _performance_tracker = PerformanceTracker(get_performance_config())
    return _performance_tracker


def start_resource_monitoring(interval_seconds: int = 30):
    """Start background resource monitoring"""
    async def monitor_loop():
        tracker = get_performance_tracker()
        while True:
            try:
                tracker.track_resource_usage()
                tracker.cleanup_old_metrics()
                await asyncio.sleep(interval_seconds)
            except Exception as e:
                tracker.logger.error(f"Error in resource monitoring: {e}")
                await asyncio.sleep(interval_seconds)
    
    # Start monitoring in background
    asyncio.create_task(monitor_loop())


# Performance validation utilities
async def validate_performance_targets() -> Dict[str, bool]:
    """Validate that all operations meet performance targets"""
    tracker = get_performance_tracker()
    results = {}
    
    operations = ['opportunity_discovery', 'opportunity_scoring', 'pattern_detection', 'explanation_generation']
    
    for operation in operations:
        stats = tracker.get_operation_statistics(operation)
        target_met = stats.get('meets_target', True)  # True if no data or meets target
        results[operation] = target_met
    
    return results


def generate_performance_report() -> str:
    """Generate human-readable performance report"""
    tracker = get_performance_tracker()
    summary = tracker.get_performance_summary()
    
    report_lines = [
        "=== ML Opportunity Detection Performance Report ===",
        f"Generated at: {summary['timestamp']}",
        f"Overall Status: {summary['overall_status']}",
        "",
        "=== Operation Performance ===",
    ]
    
    for operation, stats in summary['operations'].items():
        status_indicator = "✅" if stats.get('meets_target', True) else "❌"
        report_lines.extend([
            f"{status_indicator} {operation}:",
            f"  Mean Response Time: {stats['mean_ms']:.1f}ms",
            f"  Success Rate: {stats['success_rate']:.1f}%",
            f"  Sample Size: {stats['sample_size']} operations",
            ""
        ])
    
    report_lines.extend([
        "=== Resource Usage ===",
        f"CPU Usage: {summary['resource_usage']['cpu']['current']:.1f}% (avg: {summary['resource_usage']['cpu']['average']:.1f}%)",
        f"Memory Usage: {summary['resource_usage']['memory']['current']:.1f}% (avg: {summary['resource_usage']['memory']['average']:.1f}%)",
        "",
        "=== Circuit Breakers ===",
    ])
    
    for name, breaker in summary['circuit_breakers'].items():
        status_indicator = "🟢" if breaker['state'] == 'CLOSED' else ("🟡" if breaker['state'] == 'HALF_OPEN' else "🔴")
        report_lines.append(f"{status_indicator} {name}: {breaker['state']}")
    
    return "\n".join(report_lines)