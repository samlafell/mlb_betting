"""
Graceful Degradation Manager
Coordinates service health monitoring and fallback strategy activation
"""

import asyncio
import logging
import time
from enum import Enum
from typing import Dict, List, Optional, Set, Callable, Any, Awaitable
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from .circuit_breaker import CircuitBreaker, CircuitBreakerState, circuit_breaker_manager

try:
    from ...core.config import get_settings
except ImportError:
    get_settings = None

logger = logging.getLogger(__name__)


class ServiceStatus(str, Enum):
    """Service health status"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class DegradationMode(str, Enum):
    """System degradation modes"""
    NORMAL = "normal"              # All services healthy
    PARTIAL = "partial"            # Some services degraded
    MINIMAL = "minimal"            # Critical services only
    EMERGENCY = "emergency"        # Basic functionality only


@dataclass
class ServiceConfig:
    """Configuration for a service in the degradation manager"""
    name: str
    priority: int = 1              # 1 = critical, 5 = optional
    health_check_interval: int = 30  # Seconds between health checks
    degradation_threshold: float = 0.5  # Failure rate that triggers degradation
    recovery_threshold: float = 0.1     # Success rate needed for recovery
    fallback_enabled: bool = True       # Whether fallback is available
    dependencies: List[str] = field(default_factory=list)  # Service dependencies


@dataclass 
class DegradationStatus:
    """Current degradation status"""
    mode: DegradationMode
    services: Dict[str, ServiceStatus] = field(default_factory=dict)
    active_fallbacks: Set[str] = field(default_factory=set)
    degraded_since: Optional[datetime] = None
    performance_impact: float = 0.0     # 0.0 = no impact, 1.0 = major impact
    recommendations: List[str] = field(default_factory=list)


class DegradationManager:
    """
    Manages graceful degradation across ML pipeline services
    Coordinates circuit breakers, fallback strategies, and performance optimization
    """
    
    def __init__(self):
        self.services: Dict[str, ServiceConfig] = {}
        self.service_health: Dict[str, ServiceStatus] = {}
        self.current_status = DegradationStatus(mode=DegradationMode.NORMAL)
        self.health_callbacks: Dict[str, Callable[[], Awaitable[bool]]] = {}
        self.fallback_callbacks: Dict[str, Callable[[], Awaitable[Any]]] = {}
        self.recovery_callbacks: Dict[str, Callable[[], Awaitable[bool]]] = {}
        
        self._monitoring_task: Optional[asyncio.Task] = None
        self._running = False
        self._lock = asyncio.Lock()
        
        # Load configuration if available
        self._load_default_services()
    
    def _load_default_services(self):
        """Load default ML pipeline services"""
        default_services = [
            ServiceConfig(name="redis", priority=2, health_check_interval=15),
            ServiceConfig(name="database", priority=1, health_check_interval=30),
            ServiceConfig(name="mlflow", priority=3, health_check_interval=60),
            ServiceConfig(name="external_apis", priority=4, health_check_interval=45,
                         dependencies=["redis"]),
            ServiceConfig(name="filesystem", priority=2, health_check_interval=120),
        ]
        
        for service in default_services:
            self.register_service(service)
    
    def register_service(self, config: ServiceConfig):
        """Register a service for degradation management"""
        self.services[config.name] = config
        self.service_health[config.name] = ServiceStatus.UNKNOWN
        logger.info(f"Registered service for degradation management: {config.name} "
                   f"(priority={config.priority})")
    
    def register_health_check(self, service_name: str, 
                            callback: Callable[[], Awaitable[bool]]):
        """Register health check callback for a service"""
        if service_name not in self.services:
            raise ValueError(f"Service {service_name} not registered")
        
        self.health_callbacks[service_name] = callback
        logger.info(f"Registered health check callback for {service_name}")
    
    def register_fallback(self, service_name: str, 
                         callback: Callable[[], Awaitable[Any]]):
        """Register fallback callback for a service"""
        if service_name not in self.services:
            raise ValueError(f"Service {service_name} not registered")
        
        self.fallback_callbacks[service_name] = callback
        logger.info(f"Registered fallback callback for {service_name}")
    
    def register_recovery(self, service_name: str,
                         callback: Callable[[], Awaitable[bool]]):
        """Register recovery callback for a service"""
        if service_name not in self.services:
            raise ValueError(f"Service {service_name} not registered")
            
        self.recovery_callbacks[service_name] = callback
        logger.info(f"Registered recovery callback for {service_name}")
    
    async def start_monitoring(self):
        """Start continuous service health monitoring"""
        if self._running:
            logger.warning("Degradation manager already running")
            return
        
        self._running = True
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        logger.info("✅ Degradation manager monitoring started")
    
    async def stop_monitoring(self):
        """Stop service health monitoring"""
        self._running = False
        
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
        
        logger.info("✅ Degradation manager monitoring stopped")
    
    async def _monitoring_loop(self):
        """Main monitoring loop"""
        while self._running:
            try:
                await self._check_all_services()
                await self._update_degradation_status()
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logger.error(f"Error in degradation monitoring loop: {e}")
                await asyncio.sleep(10)
    
    async def _check_all_services(self):
        """Check health of all registered services"""
        for service_name, config in self.services.items():
            try:
                # Check if it's time for a health check
                last_check = getattr(self, f'_last_check_{service_name}', 0)
                if time.time() - last_check < config.health_check_interval:
                    continue
                
                # Perform health check
                is_healthy = await self._check_service_health(service_name)
                await self._update_service_status(service_name, is_healthy)
                
                # Update last check time
                setattr(self, f'_last_check_{service_name}', time.time())
                
            except Exception as e:
                logger.error(f"Error checking health of {service_name}: {e}")
                await self._update_service_status(service_name, False)
    
    async def _check_service_health(self, service_name: str) -> bool:
        """Check health of a specific service"""
        # Use registered health check if available
        if service_name in self.health_callbacks:
            try:
                return await self.health_callbacks[service_name]()
            except Exception as e:
                logger.warning(f"Health check failed for {service_name}: {e}")
                return False
        
        # Fallback to circuit breaker status
        circuit_breakers = circuit_breaker_manager.get_all_status()
        if service_name in circuit_breakers:
            cb_status = circuit_breakers[service_name]
            return cb_status["state"] == "closed"
        
        # Default to unknown/healthy if no health check available
        return True
    
    async def _update_service_status(self, service_name: str, is_healthy: bool):
        """Update service status based on health check result"""
        async with self._lock:
            old_status = self.service_health[service_name]
            
            if is_healthy:
                new_status = ServiceStatus.HEALTHY
            else:
                # Check circuit breaker for more specific status
                circuit_breakers = circuit_breaker_manager.get_all_status()
                if service_name in circuit_breakers:
                    cb_state = circuit_breakers[service_name]["state"]
                    if cb_state == "open":
                        new_status = ServiceStatus.UNHEALTHY
                    elif cb_state == "half_open":
                        new_status = ServiceStatus.DEGRADED
                    else:
                        new_status = ServiceStatus.DEGRADED
                else:
                    new_status = ServiceStatus.UNHEALTHY
            
            if old_status != new_status:
                self.service_health[service_name] = new_status
                logger.info(f"Service {service_name} status: {old_status.value} -> {new_status.value}")
                
                # Trigger fallback or recovery as needed
                if new_status in [ServiceStatus.DEGRADED, ServiceStatus.UNHEALTHY]:
                    await self._activate_fallback(service_name)
                elif new_status == ServiceStatus.HEALTHY and service_name in self.current_status.active_fallbacks:
                    await self._deactivate_fallback(service_name)
    
    async def _activate_fallback(self, service_name: str):
        """Activate fallback for a service"""
        if service_name in self.current_status.active_fallbacks:
            return  # Already active
        
        if service_name in self.fallback_callbacks:
            try:
                await self.fallback_callbacks[service_name]()
                self.current_status.active_fallbacks.add(service_name)
                logger.warning(f"Activated fallback for {service_name}")
            except Exception as e:
                logger.error(f"Failed to activate fallback for {service_name}: {e}")
        else:
            logger.warning(f"No fallback available for {service_name}")
    
    async def _deactivate_fallback(self, service_name: str):
        """Deactivate fallback for a service"""
        if service_name not in self.current_status.active_fallbacks:
            return  # Not active
        
        if service_name in self.recovery_callbacks:
            try:
                recovery_success = await self.recovery_callbacks[service_name]()
                if recovery_success:
                    self.current_status.active_fallbacks.discard(service_name)
                    logger.info(f"Deactivated fallback for {service_name} - service recovered")
                else:
                    logger.warning(f"Recovery failed for {service_name} - keeping fallback active")
            except Exception as e:
                logger.error(f"Error during recovery for {service_name}: {e}")
        else:
            # No recovery callback, just deactivate
            self.current_status.active_fallbacks.discard(service_name)
            logger.info(f"Deactivated fallback for {service_name}")
    
    async def _update_degradation_status(self):
        """Update overall degradation status based on service health"""
        async with self._lock:
            old_mode = self.current_status.mode
            
            # Analyze service health
            critical_services = [name for name, config in self.services.items() 
                               if config.priority == 1]
            important_services = [name for name, config in self.services.items() 
                                if config.priority == 2]
            
            critical_unhealthy = sum(1 for service in critical_services 
                                   if self.service_health.get(service) == ServiceStatus.UNHEALTHY)
            critical_degraded = sum(1 for service in critical_services 
                                  if self.service_health.get(service) == ServiceStatus.DEGRADED)
            important_unhealthy = sum(1 for service in important_services 
                                    if self.service_health.get(service) == ServiceStatus.UNHEALTHY)
            
            # Determine degradation mode
            if critical_unhealthy > 0:
                new_mode = DegradationMode.EMERGENCY
            elif critical_degraded > 0 or important_unhealthy > 1:
                new_mode = DegradationMode.MINIMAL
            elif important_unhealthy > 0 or len(self.current_status.active_fallbacks) > 0:
                new_mode = DegradationMode.PARTIAL
            else:
                new_mode = DegradationMode.NORMAL
            
            # Update status
            if old_mode != new_mode:
                self.current_status.mode = new_mode
                if new_mode != DegradationMode.NORMAL and old_mode == DegradationMode.NORMAL:
                    self.current_status.degraded_since = datetime.utcnow()
                elif new_mode == DegradationMode.NORMAL:
                    self.current_status.degraded_since = None
                
                logger.warning(f"Degradation mode changed: {old_mode.value} -> {new_mode.value}")
            
            # Update performance impact
            self.current_status.performance_impact = self._calculate_performance_impact()
            
            # Update service status
            self.current_status.services = self.service_health.copy()
            
            # Generate recommendations
            self.current_status.recommendations = self._generate_recommendations()
    
    def _calculate_performance_impact(self) -> float:
        """Calculate overall performance impact (0.0 = none, 1.0 = severe)"""
        impact = 0.0
        
        for service_name, status in self.service_health.items():
            config = self.services[service_name]
            
            if status == ServiceStatus.UNHEALTHY:
                # Impact based on service priority (1=critical has higher impact)
                service_impact = (6 - config.priority) / 5.0  # Convert to 0.2-1.0 scale
                impact += service_impact * 0.3  # Max 30% impact per service
            elif status == ServiceStatus.DEGRADED:
                service_impact = (6 - config.priority) / 5.0
                impact += service_impact * 0.15  # Max 15% impact per degraded service
        
        return min(impact, 1.0)  # Cap at 100%
    
    def _generate_recommendations(self) -> List[str]:
        """Generate actionable recommendations based on current status"""
        recommendations = []
        
        if self.current_status.mode == DegradationMode.EMERGENCY:
            recommendations.append("🚨 CRITICAL: Essential services are down - immediate intervention required")
            recommendations.append("💡 Consider manual failover to backup systems if available")
            
        elif self.current_status.mode == DegradationMode.MINIMAL:
            recommendations.append("⚠️ ALERT: Operating in minimal mode - core functionality only")
            recommendations.append("💡 Monitor critical services closely and prepare for full recovery")
            
        elif self.current_status.mode == DegradationMode.PARTIAL:
            recommendations.append("ℹ️ INFO: Some services degraded - performance may be impacted")
            recommendations.append("💡 Review service logs and consider proactive maintenance")
        
        # Service-specific recommendations
        for service_name, status in self.service_health.items():
            if status == ServiceStatus.UNHEALTHY:
                recommendations.append(f"🔧 REPAIR: {service_name} requires immediate attention")
            elif status == ServiceStatus.DEGRADED:
                recommendations.append(f"🔍 MONITOR: {service_name} is degraded - monitor closely")
        
        # Fallback recommendations
        if self.current_status.active_fallbacks:
            fallback_list = ", ".join(self.current_status.active_fallbacks)
            recommendations.append(f"🔄 FALLBACK: Active fallbacks for: {fallback_list}")
        
        return recommendations
    
    def get_status(self) -> Dict[str, Any]:
        """Get current degradation status"""
        return {
            "mode": self.current_status.mode.value,
            "services": {name: status.value for name, status in self.current_status.services.items()},
            "active_fallbacks": list(self.current_status.active_fallbacks),
            "degraded_since": (self.current_status.degraded_since.isoformat() 
                             if self.current_status.degraded_since else None),
            "performance_impact": round(self.current_status.performance_impact, 3),
            "recommendations": self.current_status.recommendations,
            "service_configs": {
                name: {
                    "priority": config.priority,
                    "fallback_enabled": config.fallback_enabled,
                    "dependencies": config.dependencies
                }
                for name, config in self.services.items()
            }
        }
    
    async def force_degradation_mode(self, mode: DegradationMode):
        """Manually set degradation mode (for testing/emergency)"""
        async with self._lock:
            old_mode = self.current_status.mode
            self.current_status.mode = mode
            
            if mode != DegradationMode.NORMAL and old_mode == DegradationMode.NORMAL:
                self.current_status.degraded_since = datetime.utcnow()
            elif mode == DegradationMode.NORMAL:
                self.current_status.degraded_since = None
            
            logger.warning(f"Degradation mode manually set: {old_mode.value} -> {mode.value}")
    
    async def is_service_available(self, service_name: str) -> bool:
        """Check if a service is currently available (not requiring fallback)"""
        status = self.service_health.get(service_name, ServiceStatus.UNKNOWN)
        return status == ServiceStatus.HEALTHY
    
    async def should_use_fallback(self, service_name: str) -> bool:
        """Check if fallback should be used for a service"""
        return service_name in self.current_status.active_fallbacks
    
    async def get_healthy_services(self) -> List[str]:
        """Get list of currently healthy services"""
        return [name for name, status in self.service_health.items() 
                if status == ServiceStatus.HEALTHY]
    
    async def get_degraded_services(self) -> List[str]:
        """Get list of currently degraded services"""
        return [name for name, status in self.service_health.items() 
                if status in [ServiceStatus.DEGRADED, ServiceStatus.UNHEALTHY]]


# Global degradation manager instance
degradation_manager = DegradationManager()


# Convenience functions
async def get_degradation_manager() -> DegradationManager:
    """Get the global degradation manager instance"""
    return degradation_manager


async def is_service_available(service_name: str) -> bool:
    """Check if a service is available without degradation"""
    return await degradation_manager.is_service_available(service_name)


async def should_use_fallback(service_name: str) -> bool:
    """Check if fallback should be used for a service"""
    return await degradation_manager.should_use_fallback(service_name)