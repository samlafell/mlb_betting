"""
Enhanced Resource Monitoring Service
Comprehensive CPU, Memory, Disk, and Network monitoring with intelligent alerting
Prevents resource exhaustion through proactive monitoring and adaptive management
"""

import asyncio
import logging
import time
import gc
import os
from typing import Dict, List, Optional, Any, Callable, Awaitable, NamedTuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path

import psutil

try:
    from ...core.config import get_unified_config
except ImportError:
    get_unified_config = None

logger = logging.getLogger(__name__)


class ResourceAlertLevel(str, Enum):
    """Resource alert severity levels"""
    NORMAL = "normal"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class ResourceType(str, Enum):
    """Types of resources being monitored"""
    CPU = "cpu"
    MEMORY = "memory"
    DISK = "disk"
    NETWORK = "network"
    PROCESS = "process"


@dataclass
class ResourceThreshold:
    """Resource monitoring threshold configuration"""
    resource_type: ResourceType
    metric_name: str
    warning_threshold: float
    critical_threshold: float
    emergency_threshold: float
    duration_seconds: int = 30  # Time threshold must be breached
    description: str = ""


@dataclass
class ResourceMetrics:
    """Current resource utilization metrics"""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    # CPU Metrics
    cpu_percent: float = 0.0
    cpu_per_core: List[float] = field(default_factory=list)
    load_average_1m: float = 0.0
    load_average_5m: float = 0.0
    load_average_15m: float = 0.0
    cpu_frequency: float = 0.0
    cpu_context_switches: int = 0
    cpu_interrupts: int = 0
    
    # Memory Metrics
    memory_percent: float = 0.0
    memory_total_gb: float = 0.0
    memory_available_gb: float = 0.0
    memory_used_gb: float = 0.0
    memory_cached_gb: float = 0.0
    swap_percent: float = 0.0
    swap_total_gb: float = 0.0
    swap_used_gb: float = 0.0
    
    # Disk Metrics
    disk_usage_percent: float = 0.0
    disk_total_gb: float = 0.0
    disk_used_gb: float = 0.0
    disk_free_gb: float = 0.0
    disk_read_mb_per_sec: float = 0.0
    disk_write_mb_per_sec: float = 0.0
    disk_io_ops_per_sec: float = 0.0
    
    # Network Metrics
    network_connections: int = 0
    network_bytes_sent_per_sec: float = 0.0
    network_bytes_recv_per_sec: float = 0.0
    network_packets_sent_per_sec: float = 0.0
    network_packets_recv_per_sec: float = 0.0
    
    # Process-Specific Metrics
    process_count: int = 0
    process_memory_mb: float = 0.0
    process_cpu_percent: float = 0.0
    process_threads: int = 0
    process_file_descriptors: int = 0
    process_children: int = 0
    
    # ML Pipeline Specific
    ml_models_loaded: int = 0
    ml_cache_size_mb: float = 0.0
    ml_feature_vectors_cached: int = 0
    ml_active_predictions: int = 0
    
    def get_alert_level(self, thresholds: List[ResourceThreshold]) -> ResourceAlertLevel:
        """Determine the highest alert level based on current metrics"""
        highest_level = ResourceAlertLevel.NORMAL
        
        for threshold in thresholds:
            metric_value = getattr(self, threshold.metric_name, 0.0)
            
            if metric_value >= threshold.emergency_threshold:
                return ResourceAlertLevel.EMERGENCY
            elif metric_value >= threshold.critical_threshold:
                highest_level = ResourceAlertLevel.CRITICAL
            elif metric_value >= threshold.warning_threshold and highest_level == ResourceAlertLevel.NORMAL:
                highest_level = ResourceAlertLevel.WARNING
        
        return highest_level


class ResourceAlert(NamedTuple):
    """Resource alert information"""
    timestamp: datetime
    level: ResourceAlertLevel
    resource_type: ResourceType
    metric_name: str
    current_value: float
    threshold_value: float
    description: str
    recommendations: List[str]


class ResourceMonitorConfig:
    """Resource monitoring configuration"""
    
    def __init__(self):
        # Load from unified config if available
        if get_unified_config:
            try:
                config = get_unified_config()
                ml_config = config.ml_pipeline
                
                # CPU Thresholds
                self.cpu_warning_threshold = getattr(ml_config, 'cpu_warning_threshold', 70.0)
                self.cpu_critical_threshold = getattr(ml_config, 'cpu_critical_threshold', 85.0)
                self.cpu_emergency_threshold = getattr(ml_config, 'cpu_emergency_threshold', 95.0)
                
                # Memory Thresholds
                self.memory_warning_threshold = getattr(ml_config, 'memory_warning_threshold', 75.0)
                self.memory_critical_threshold = getattr(ml_config, 'memory_critical_threshold', 85.0)
                self.memory_emergency_threshold = getattr(ml_config, 'memory_emergency_threshold', 95.0)
                
                # Disk Thresholds
                self.disk_warning_threshold = getattr(ml_config, 'disk_warning_threshold', 80.0)
                self.disk_critical_threshold = getattr(ml_config, 'disk_critical_threshold', 90.0)
                self.disk_emergency_threshold = getattr(ml_config, 'disk_emergency_threshold', 95.0)
                
                # Monitoring intervals
                self.monitoring_interval = getattr(ml_config, 'resource_monitoring_interval', 10)
                self.alert_cooldown_seconds = getattr(ml_config, 'resource_alert_cooldown', 300)
                
            except Exception as e:
                logger.warning(f"Failed to load resource monitor config: {e}")
                self._set_defaults()
        else:
            self._set_defaults()
    
    def _set_defaults(self):
        """Set default configuration values"""
        # CPU Thresholds (%)
        self.cpu_warning_threshold = 70.0
        self.cpu_critical_threshold = 85.0
        self.cpu_emergency_threshold = 95.0
        
        # Memory Thresholds (%)
        self.memory_warning_threshold = 75.0
        self.memory_critical_threshold = 85.0
        self.memory_emergency_threshold = 95.0
        
        # Disk Thresholds (%)
        self.disk_warning_threshold = 80.0
        self.disk_critical_threshold = 90.0
        self.disk_emergency_threshold = 95.0
        
        # Monitoring settings
        self.monitoring_interval = 10  # seconds
        self.alert_cooldown_seconds = 300  # 5 minutes
    
    def get_thresholds(self) -> List[ResourceThreshold]:
        """Get all configured resource thresholds"""
        return [
            ResourceThreshold(
                ResourceType.CPU, "cpu_percent",
                self.cpu_warning_threshold, self.cpu_critical_threshold, self.cpu_emergency_threshold,
                description="Overall CPU utilization"
            ),
            ResourceThreshold(
                ResourceType.MEMORY, "memory_percent",
                self.memory_warning_threshold, self.memory_critical_threshold, self.memory_emergency_threshold,
                description="Memory utilization"
            ),
            ResourceThreshold(
                ResourceType.DISK, "disk_usage_percent",
                self.disk_warning_threshold, self.disk_critical_threshold, self.disk_emergency_threshold,
                description="Disk space utilization"
            ),
            ResourceThreshold(
                ResourceType.MEMORY, "swap_percent",
                50.0, 75.0, 90.0,
                description="Swap space utilization"
            ),
            ResourceThreshold(
                ResourceType.CPU, "load_average_5m",
                psutil.cpu_count() * 0.7, psutil.cpu_count() * 0.9, psutil.cpu_count() * 1.2,
                description="System load average (5m)"
            ),
        ]


class ResourceMonitor:
    """
    Comprehensive resource monitoring service
    Monitors CPU, Memory, Disk, Network, and Process metrics with intelligent alerting
    """
    
    def __init__(self, config: Optional[ResourceMonitorConfig] = None):
        self.config = config or ResourceMonitorConfig()
        self.thresholds = self.config.get_thresholds()
        
        # State tracking
        self.current_metrics = ResourceMetrics()
        self.metrics_history: List[ResourceMetrics] = []
        self.max_history_size = 1440  # 24 hours at 1-minute intervals
        
        # Alert management
        self.active_alerts: Dict[str, ResourceAlert] = {}
        self.alert_callbacks: List[Callable[[ResourceAlert], Awaitable[None]]] = []
        self.last_alert_times: Dict[str, datetime] = {}
        
        # Monitoring state
        self._monitoring_task: Optional[asyncio.Task] = None
        self._running = False
        
        # Performance tracking
        self.monitoring_stats = {
            "monitoring_cycles": 0,
            "alerts_generated": 0,
            "avg_collection_time_ms": 0.0,
            "last_collection_time": None
        }
        
        # Resource baselines for anomaly detection
        self.baseline_cpu = 0.0
        self.baseline_memory = 0.0
        self.baseline_established = False
        
        # Get initial system info
        self._initialize_system_info()
    
    def _initialize_system_info(self):
        """Initialize system information"""
        try:
            self.cpu_count = psutil.cpu_count()
            self.cpu_count_logical = psutil.cpu_count(logical=True)
            self.memory_total = psutil.virtual_memory().total
            
            # Get disk info for root partition
            self.disk_total = psutil.disk_usage('/').total
            
            logger.info(f"Resource monitor initialized: {self.cpu_count} CPU cores, "
                       f"{self.memory_total / (1024**3):.1f}GB memory, "
                       f"{self.disk_total / (1024**3):.1f}GB disk")
                       
        except Exception as e:
            logger.error(f"Failed to initialize system info: {e}")
    
    async def start_monitoring(self):
        """Start continuous resource monitoring"""
        if self._running:
            logger.warning("Resource monitoring already running")
            return
        
        self._running = True
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        logger.info("✅ Resource monitoring started")
    
    async def stop_monitoring(self):
        """Stop resource monitoring"""
        self._running = False
        
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
        
        logger.info("✅ Resource monitoring stopped")
    
    async def _monitoring_loop(self):
        """Main monitoring loop"""
        while self._running:
            try:
                collection_start = time.time()
                
                # Collect current metrics
                await self._collect_metrics()
                
                # Check for alerts
                await self._check_alerts()
                
                # Update statistics
                collection_time = (time.time() - collection_start) * 1000
                self._update_monitoring_stats(collection_time)
                
                # Sleep until next collection
                await asyncio.sleep(self.config.monitoring_interval)
                
            except Exception as e:
                logger.error(f"Error in resource monitoring loop: {e}")
                await asyncio.sleep(self.config.monitoring_interval)
    
    async def _collect_metrics(self):
        """Collect comprehensive resource metrics"""
        try:
            metrics = ResourceMetrics()
            
            # CPU Metrics
            metrics.cpu_percent = psutil.cpu_percent(interval=1)
            metrics.cpu_per_core = psutil.cpu_percent(interval=1, percpu=True)
            
            # Load averages (Unix only)
            try:
                load_avg = psutil.getloadavg()
                metrics.load_average_1m = load_avg[0]
                metrics.load_average_5m = load_avg[1]
                metrics.load_average_15m = load_avg[2]
            except AttributeError:
                # Windows doesn't have load averages
                pass
            
            # CPU frequency
            try:
                cpu_freq = psutil.cpu_freq()
                if cpu_freq:
                    metrics.cpu_frequency = cpu_freq.current
            except (AttributeError, OSError):
                pass
            
            # CPU stats
            try:
                cpu_stats = psutil.cpu_stats()
                metrics.cpu_context_switches = cpu_stats.ctx_switches
                metrics.cpu_interrupts = cpu_stats.interrupts
            except AttributeError:
                pass
            
            # Memory Metrics
            memory = psutil.virtual_memory()
            metrics.memory_percent = memory.percent
            metrics.memory_total_gb = memory.total / (1024**3)
            metrics.memory_available_gb = memory.available / (1024**3)
            metrics.memory_used_gb = memory.used / (1024**3)
            
            # Cached memory (Linux/macOS)
            if hasattr(memory, 'cached'):
                metrics.memory_cached_gb = memory.cached / (1024**3)
            elif hasattr(memory, 'inactive'):  # macOS
                metrics.memory_cached_gb = memory.inactive / (1024**3)
            
            # Swap Metrics
            swap = psutil.swap_memory()
            metrics.swap_percent = swap.percent
            metrics.swap_total_gb = swap.total / (1024**3)
            metrics.swap_used_gb = swap.used / (1024**3)
            
            # Disk Metrics
            disk_usage = psutil.disk_usage('/')
            metrics.disk_usage_percent = (disk_usage.used / disk_usage.total) * 100
            metrics.disk_total_gb = disk_usage.total / (1024**3)
            metrics.disk_used_gb = disk_usage.used / (1024**3)
            metrics.disk_free_gb = disk_usage.free / (1024**3)
            
            # Disk I/O (if available)
            try:
                disk_io = psutil.disk_io_counters()
                if disk_io and hasattr(self, '_last_disk_io'):
                    time_delta = (metrics.timestamp - self.current_metrics.timestamp).total_seconds()
                    if time_delta > 0:
                        metrics.disk_read_mb_per_sec = (disk_io.read_bytes - self._last_disk_io.read_bytes) / (1024**2) / time_delta
                        metrics.disk_write_mb_per_sec = (disk_io.write_bytes - self._last_disk_io.write_bytes) / (1024**2) / time_delta
                        metrics.disk_io_ops_per_sec = (disk_io.read_count + disk_io.write_count - 
                                                     self._last_disk_io.read_count - self._last_disk_io.write_count) / time_delta
                
                self._last_disk_io = disk_io
            except (AttributeError, OSError):
                pass
            
            # Network Metrics
            try:
                net_connections = len(psutil.net_connections())
                metrics.network_connections = net_connections
                
                net_io = psutil.net_io_counters()
                if net_io and hasattr(self, '_last_net_io'):
                    time_delta = (metrics.timestamp - self.current_metrics.timestamp).total_seconds()
                    if time_delta > 0:
                        metrics.network_bytes_sent_per_sec = (net_io.bytes_sent - self._last_net_io.bytes_sent) / time_delta
                        metrics.network_bytes_recv_per_sec = (net_io.bytes_recv - self._last_net_io.bytes_recv) / time_delta
                        metrics.network_packets_sent_per_sec = (net_io.packets_sent - self._last_net_io.packets_sent) / time_delta
                        metrics.network_packets_recv_per_sec = (net_io.packets_recv - self._last_net_io.packets_recv) / time_delta
                
                self._last_net_io = net_io
            except (AttributeError, OSError, psutil.AccessDenied):
                pass
            
            # Process-Specific Metrics
            try:
                current_process = psutil.Process()
                metrics.process_memory_mb = current_process.memory_info().rss / (1024**2)
                metrics.process_cpu_percent = current_process.cpu_percent()
                metrics.process_threads = current_process.num_threads()
                
                try:
                    metrics.process_file_descriptors = current_process.num_fds()
                except (AttributeError, psutil.AccessDenied):
                    pass
                
                metrics.process_children = len(current_process.children())
                
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            
            # Update current metrics
            self.current_metrics = metrics
            
            # Add to history
            self.metrics_history.append(metrics)
            if len(self.metrics_history) > self.max_history_size:
                self.metrics_history.pop(0)
            
            # Establish baseline if not done
            if not self.baseline_established and len(self.metrics_history) >= 10:
                self._establish_baseline()
            
        except Exception as e:
            logger.error(f"Error collecting resource metrics: {e}")
    
    def _establish_baseline(self):
        """Establish baseline metrics for anomaly detection"""
        try:
            recent_metrics = self.metrics_history[-10:]
            
            self.baseline_cpu = sum(m.cpu_percent for m in recent_metrics) / len(recent_metrics)
            self.baseline_memory = sum(m.memory_percent for m in recent_metrics) / len(recent_metrics)
            
            self.baseline_established = True
            logger.info(f"Resource baselines established: CPU={self.baseline_cpu:.1f}%, Memory={self.baseline_memory:.1f}%")
            
        except Exception as e:
            logger.error(f"Error establishing resource baselines: {e}")
    
    async def _check_alerts(self):
        """Check current metrics against thresholds and generate alerts"""
        try:
            current_level = self.current_metrics.get_alert_level(self.thresholds)
            
            if current_level != ResourceAlertLevel.NORMAL:
                await self._generate_alerts(current_level)
            else:
                # Clear any existing alerts if resources are back to normal
                await self._clear_resolved_alerts()
                
        except Exception as e:
            logger.error(f"Error checking resource alerts: {e}")
    
    async def _generate_alerts(self, alert_level: ResourceAlertLevel):
        """Generate alerts for threshold violations"""
        alerts_generated = []
        
        for threshold in self.thresholds:
            metric_value = getattr(self.current_metrics, threshold.metric_name, 0.0)
            
            # Determine which threshold is violated
            threshold_value = 0.0
            if metric_value >= threshold.emergency_threshold:
                level = ResourceAlertLevel.EMERGENCY
                threshold_value = threshold.emergency_threshold
            elif metric_value >= threshold.critical_threshold:
                level = ResourceAlertLevel.CRITICAL
                threshold_value = threshold.critical_threshold
            elif metric_value >= threshold.warning_threshold:
                level = ResourceAlertLevel.WARNING
                threshold_value = threshold.warning_threshold
            else:
                continue  # No threshold violated
            
            alert_key = f"{threshold.resource_type.value}_{threshold.metric_name}_{level.value}"
            
            # Check cooldown period
            if alert_key in self.last_alert_times:
                time_since_last = (datetime.utcnow() - self.last_alert_times[alert_key]).total_seconds()
                if time_since_last < self.config.alert_cooldown_seconds:
                    continue
            
            # Generate recommendations
            recommendations = self._generate_recommendations(threshold, metric_value, level)
            
            # Create alert
            alert = ResourceAlert(
                timestamp=datetime.utcnow(),
                level=level,
                resource_type=threshold.resource_type,
                metric_name=threshold.metric_name,
                current_value=metric_value,
                threshold_value=threshold_value,
                description=threshold.description,
                recommendations=recommendations
            )
            
            self.active_alerts[alert_key] = alert
            self.last_alert_times[alert_key] = alert.timestamp
            alerts_generated.append(alert)
            
            logger.warning(f"Resource alert generated: {level.value.upper()} - {threshold.description} "
                          f"({metric_value:.1f} >= {threshold_value:.1f})")
        
        # Send alerts to registered callbacks
        for alert in alerts_generated:
            for callback in self.alert_callbacks:
                try:
                    await callback(alert)
                except Exception as e:
                    logger.error(f"Error in alert callback: {e}")
        
        if alerts_generated:
            self.monitoring_stats["alerts_generated"] += len(alerts_generated)
    
    def _generate_recommendations(self, threshold: ResourceThreshold, 
                                current_value: float, level: ResourceAlertLevel) -> List[str]:
        """Generate actionable recommendations based on alert"""
        recommendations = []
        
        if threshold.resource_type == ResourceType.CPU:
            if level == ResourceAlertLevel.EMERGENCY:
                recommendations.extend([
                    "🚨 IMMEDIATE: Kill non-essential processes to free CPU",
                    "🔧 URGENT: Reduce ML batch sizes and concurrent operations",
                    "⚠️ Consider restarting the service if CPU usage doesn't decrease"
                ])
            elif level == ResourceAlertLevel.CRITICAL:
                recommendations.extend([
                    "⚠️ Reduce concurrent ML operations",
                    "🔧 Lower batch processing sizes",
                    "📊 Review CPU-intensive processes"
                ])
            else:
                recommendations.extend([
                    "📊 Monitor CPU usage trends",
                    "🔧 Consider optimizing ML algorithms"
                ])
        
        elif threshold.resource_type == ResourceType.MEMORY:
            if level == ResourceAlertLevel.EMERGENCY:
                recommendations.extend([
                    "🚨 CRITICAL: Force garbage collection immediately",
                    "🧹 Clear all non-essential caches",
                    "📦 Unload unused ML models from memory",
                    "🔄 Consider service restart if memory doesn't free up"
                ])
            elif level == ResourceAlertLevel.CRITICAL:
                recommendations.extend([
                    "🧹 Clear feature vector caches",
                    "📦 Unload old ML models",
                    "🔧 Reduce batch processing sizes",
                    "♻️ Force garbage collection"
                ])
            else:
                recommendations.extend([
                    "📊 Monitor memory usage patterns",
                    "🧹 Regular cache cleanup",
                    "📦 Review model memory usage"
                ])
        
        elif threshold.resource_type == ResourceType.DISK:
            recommendations.extend([
                "🧹 Clean up temporary files and logs",
                "📦 Archive old model artifacts",
                "🗑️ Remove unused cache files",
                "📊 Review disk usage by directory"
            ])
        
        return recommendations
    
    async def _clear_resolved_alerts(self):
        """Clear alerts that are no longer valid"""
        resolved_alerts = []
        
        for alert_key, alert in list(self.active_alerts.items()):
            metric_value = getattr(self.current_metrics, alert.metric_name, 0.0)
            
            # Find the corresponding threshold
            threshold = next((t for t in self.thresholds 
                            if t.resource_type == alert.resource_type and 
                               t.metric_name == alert.metric_name), None)
            
            if threshold and metric_value < threshold.warning_threshold:
                resolved_alerts.append(alert_key)
                logger.info(f"Resource alert resolved: {alert.description} "
                           f"({metric_value:.1f} < {threshold.warning_threshold:.1f})")
        
        for alert_key in resolved_alerts:
            del self.active_alerts[alert_key]
    
    def _update_monitoring_stats(self, collection_time_ms: float):
        """Update monitoring performance statistics"""
        self.monitoring_stats["monitoring_cycles"] += 1
        self.monitoring_stats["last_collection_time"] = datetime.utcnow()
        
        # Update average collection time
        current_avg = self.monitoring_stats["avg_collection_time_ms"]
        total_cycles = self.monitoring_stats["monitoring_cycles"]
        new_avg = (current_avg * (total_cycles - 1) + collection_time_ms) / total_cycles
        self.monitoring_stats["avg_collection_time_ms"] = new_avg
    
    def register_alert_callback(self, callback: Callable[[ResourceAlert], Awaitable[None]]):
        """Register callback for resource alerts"""
        self.alert_callbacks.append(callback)
        logger.info("Resource alert callback registered")
    
    def get_current_metrics(self) -> ResourceMetrics:
        """Get current resource metrics"""
        return self.current_metrics
    
    def get_metrics_history(self, minutes: int = 60) -> List[ResourceMetrics]:
        """Get historical metrics for specified time period"""
        cutoff_time = datetime.utcnow() - timedelta(minutes=minutes)
        return [m for m in self.metrics_history if m.timestamp >= cutoff_time]
    
    def get_resource_trends(self, minutes: int = 60) -> Dict[str, Any]:
        """Analyze resource usage trends"""
        history = self.get_metrics_history(minutes)
        
        if len(history) < 2:
            return {"error": "Insufficient data for trend analysis"}
        
        # Calculate trends
        cpu_values = [m.cpu_percent for m in history]
        memory_values = [m.memory_percent for m in history]
        
        return {
            "cpu_trend": {
                "current": cpu_values[-1],
                "average": sum(cpu_values) / len(cpu_values),
                "min": min(cpu_values),
                "max": max(cpu_values),
                "trend_direction": "increasing" if cpu_values[-1] > cpu_values[0] else "decreasing"
            },
            "memory_trend": {
                "current": memory_values[-1],
                "average": sum(memory_values) / len(memory_values),
                "min": min(memory_values),
                "max": max(memory_values),
                "trend_direction": "increasing" if memory_values[-1] > memory_values[0] else "decreasing"
            },
            "analysis_period_minutes": minutes,
            "data_points": len(history)
        }
    
    def get_status(self) -> Dict[str, Any]:
        """Get comprehensive resource monitor status"""
        return {
            "monitoring_active": self._running,
            "current_metrics": {
                "timestamp": self.current_metrics.timestamp.isoformat(),
                "cpu_percent": self.current_metrics.cpu_percent,
                "memory_percent": self.current_metrics.memory_percent,
                "disk_usage_percent": self.current_metrics.disk_usage_percent,
                "process_memory_mb": self.current_metrics.process_memory_mb,
                "load_average_5m": self.current_metrics.load_average_5m,
            },
            "active_alerts": {
                key: {
                    "level": alert.level.value,
                    "description": alert.description,
                    "current_value": alert.current_value,
                    "threshold_value": alert.threshold_value,
                    "timestamp": alert.timestamp.isoformat(),
                    "recommendations": alert.recommendations
                }
                for key, alert in self.active_alerts.items()
            },
            "monitoring_stats": self.monitoring_stats.copy(),
            "configuration": {
                "cpu_thresholds": {
                    "warning": self.config.cpu_warning_threshold,
                    "critical": self.config.cpu_critical_threshold,
                    "emergency": self.config.cpu_emergency_threshold
                },
                "memory_thresholds": {
                    "warning": self.config.memory_warning_threshold,
                    "critical": self.config.memory_critical_threshold,
                    "emergency": self.config.memory_emergency_threshold
                },
                "monitoring_interval": self.config.monitoring_interval,
                "alert_cooldown_seconds": self.config.alert_cooldown_seconds
            },
            "system_info": {
                "cpu_count": getattr(self, 'cpu_count', 0),
                "cpu_count_logical": getattr(self, 'cpu_count_logical', 0),
                "memory_total_gb": getattr(self, 'memory_total', 0) / (1024**3),
                "disk_total_gb": getattr(self, 'disk_total', 0) / (1024**3)
            }
        }
    
    async def force_cleanup(self) -> Dict[str, Any]:
        """Force immediate resource cleanup"""
        cleanup_results = {}
        
        try:
            # Force garbage collection
            gc_before = len(gc.get_objects())
            gc.collect()
            gc_after = len(gc.get_objects())
            cleanup_results["garbage_collection"] = {
                "objects_before": gc_before,
                "objects_after": gc_after,
                "objects_freed": gc_before - gc_after
            }
            
            # Additional cleanup can be added here for ML-specific resources
            logger.info(f"Force cleanup completed: {cleanup_results}")
            
        except Exception as e:
            cleanup_results["error"] = str(e)
            logger.error(f"Error during force cleanup: {e}")
        
        return cleanup_results


# Global resource monitor instance
resource_monitor = ResourceMonitor()


# Convenience functions
async def get_resource_monitor() -> ResourceMonitor:
    """Get the global resource monitor instance"""
    return resource_monitor


async def get_current_resource_usage() -> ResourceMetrics:
    """Get current resource usage metrics"""
    monitor = await get_resource_monitor()
    return monitor.get_current_metrics()


async def check_resource_pressure() -> bool:
    """Check if system is under significant resource pressure"""
    monitor = await get_resource_monitor()
    metrics = monitor.get_current_metrics()
    
    # Define pressure thresholds
    cpu_pressure = metrics.cpu_percent > 85.0
    memory_pressure = metrics.memory_percent > 85.0
    disk_pressure = metrics.disk_usage_percent > 90.0
    
    return cpu_pressure or memory_pressure or disk_pressure