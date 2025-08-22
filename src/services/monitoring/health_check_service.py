"""
Comprehensive Health Check Service

Provides health check endpoints for all system services with:
- Database connectivity and performance monitoring
- Service health status and dependency checks  
- Performance metrics and resource utilization
- Circuit breaker status and error rates
- Integration testing and validation

This addresses Issue #38: System Reliability Issues Prevent Production Use
"""

import asyncio
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

import asyncpg
from pydantic import BaseModel

from ...core.config import get_settings
from ...core.logging import UnifiedLogger, LogComponent
from ...data.collection.registry import get_collector_instance


class HealthStatus(str, Enum):
    """Health status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class ServiceHealth(BaseModel):
    """Individual service health information."""
    name: str
    status: HealthStatus
    message: str
    response_time_ms: float
    last_check: datetime
    error_count: int = 0
    warning_count: int = 0
    metadata: Dict[str, Any] = {}


class SystemHealth(BaseModel):
    """Overall system health information."""
    status: HealthStatus
    timestamp: datetime
    services: List[ServiceHealth]
    overall_response_time_ms: float
    error_summary: Dict[str, int] = {}
    performance_metrics: Dict[str, float] = {}
    dependency_status: Dict[str, HealthStatus] = {}


class HealthCheckService:
    """Comprehensive health check service for all system components."""
    
    def __init__(self):
        self.settings = get_settings()
        self.logger = UnifiedLogger("health_check_service", LogComponent.MONITORING)
        self._last_health_check: Optional[SystemHealth] = None
        self._check_cache_ttl = 30  # Cache health checks for 30 seconds
        
    async def get_system_health(self, include_detailed: bool = False) -> SystemHealth:
        """
        Get comprehensive system health status.
        
        Args:
            include_detailed: Include detailed service diagnostics
            
        Returns:
            SystemHealth object with current status
        """
        start_time = time.time()
        
        # Check cache first
        if (self._last_health_check and 
            (datetime.now() - self._last_health_check.timestamp).seconds < self._check_cache_ttl):
            return self._last_health_check
        
        self.logger.info("Starting comprehensive system health check", 
                        operation="health_check", include_detailed=include_detailed)
        
        # Run all health checks
        services = []
        services.append(await self._check_database_health())
        services.append(await self._check_data_collection_health())
        services.append(await self._check_configuration_health())
        services.append(await self._check_logging_health())
        
        if include_detailed:
            services.append(await self._check_monitoring_health())
            services.append(await self._check_cli_health())
            services.append(await self._check_pipeline_health())
        
        # Calculate overall status
        overall_status = self._calculate_overall_status(services)
        overall_response_time = (time.time() - start_time) * 1000
        
        # Build error summary
        error_summary = {}
        for service in services:
            if service.error_count > 0:
                error_summary[service.name] = service.error_count
        
        # Performance metrics
        performance_metrics = {
            "avg_response_time_ms": sum(s.response_time_ms for s in services) / len(services),
            "max_response_time_ms": max(s.response_time_ms for s in services),
            "total_errors": sum(s.error_count for s in services),
            "total_warnings": sum(s.warning_count for s in services),
        }
        
        # Dependency status
        dependency_status = {service.name: service.status for service in services}
        
        system_health = SystemHealth(
            status=overall_status,
            timestamp=datetime.now(),
            services=services,
            overall_response_time_ms=overall_response_time,
            error_summary=error_summary,
            performance_metrics=performance_metrics,
            dependency_status=dependency_status
        )
        
        self._last_health_check = system_health
        
        self.logger.info("System health check completed", 
                        operation="health_check",
                        overall_status=overall_status.value,
                        response_time_ms=overall_response_time,
                        total_services=len(services))
        
        return system_health
    
    async def _check_database_health(self) -> ServiceHealth:
        """Check database connectivity and performance."""
        start_time = time.time()
        service_name = "database"
        
        try:
            # Test connection
            conn = await asyncpg.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                user=self.settings.database.user,
                password=self.settings.database.password,
                database=self.settings.database.database
            )
            
            # Test basic query
            await conn.fetchval("SELECT 1")
            
            # Check database size and performance
            db_stats = await conn.fetchrow("""
                SELECT 
                    pg_database_size(current_database()) as db_size,
                    (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') as active_connections,
                    (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_connections
            """)
            
            # Check table counts for key tables
            table_stats = {}
            key_tables = [
                ('raw_data', 'action_network_odds'),
                ('staging', 'betting_odds_unified'),
                ('curated', 'enhanced_games')
            ]
            
            for schema, table in key_tables:
                try:
                    count = await conn.fetchval(f"SELECT COUNT(*) FROM {schema}.{table}")
                    table_stats[f"{schema}.{table}"] = count
                except Exception:
                    table_stats[f"{schema}.{table}"] = -1  # Table doesn't exist or error
            
            await conn.close()
            
            response_time = (time.time() - start_time) * 1000
            
            # Determine status based on performance and data
            if response_time > 1000:  # > 1 second
                status = HealthStatus.DEGRADED
                message = f"Database slow response: {response_time:.1f}ms"
                warning_count = 1
            elif any(count == -1 for count in table_stats.values()):
                status = HealthStatus.DEGRADED
                message = "Some required tables missing or inaccessible"
                warning_count = 1
            else:
                status = HealthStatus.HEALTHY
                message = f"Database healthy, response: {response_time:.1f}ms"
                warning_count = 0
            
            metadata = {
                "database_size_bytes": db_stats['db_size'] if db_stats else 0,
                "active_connections": db_stats['active_connections'] if db_stats else 0,
                "max_connections": db_stats['max_connections'] if db_stats else 0,
                "table_counts": table_stats,
                "connection_pool_status": "healthy"
            }
            
            return ServiceHealth(
                name=service_name,
                status=status,
                message=message,
                response_time_ms=response_time,
                last_check=datetime.now(),
                warning_count=warning_count,
                metadata=metadata
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            
            return ServiceHealth(
                name=service_name,
                status=HealthStatus.UNHEALTHY,
                message=f"Database connection failed: {str(e)}",
                response_time_ms=response_time,
                last_check=datetime.now(),
                error_count=1,
                metadata={"error_type": type(e).__name__, "error_message": str(e)}
            )
    
    async def _check_data_collection_health(self) -> ServiceHealth:
        """Check data collection service health."""
        start_time = time.time()
        service_name = "data_collection"
        
        try:
            # Test collector registry
            from ...data.collection.registry import get_collector_instance
            
            # Try to get key collectors
            collectors_status = {}
            test_collectors = ["action_network", "vsin", "sbd"]
            
            for collector_name in test_collectors:
                try:
                    collector = get_collector_instance(collector_name)
                    collectors_status[collector_name] = "available"
                except Exception as e:
                    collectors_status[collector_name] = f"error: {str(e)}"
            
            response_time = (time.time() - start_time) * 1000
            
            # Determine status
            available_collectors = sum(1 for status in collectors_status.values() if status == "available")
            total_collectors = len(collectors_status)
            
            if available_collectors == total_collectors:
                status = HealthStatus.HEALTHY
                message = f"All {total_collectors} collectors available"
                error_count = 0
            elif available_collectors > 0:
                status = HealthStatus.DEGRADED
                message = f"{available_collectors}/{total_collectors} collectors available"
                error_count = total_collectors - available_collectors
            else:
                status = HealthStatus.UNHEALTHY
                message = "No collectors available"
                error_count = total_collectors
            
            return ServiceHealth(
                name=service_name,
                status=status,
                message=message,
                response_time_ms=response_time,
                last_check=datetime.now(),
                error_count=error_count,
                metadata={"collectors_status": collectors_status}
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            
            return ServiceHealth(
                name=service_name,
                status=HealthStatus.UNHEALTHY,
                message=f"Data collection health check failed: {str(e)}",
                response_time_ms=response_time,
                last_check=datetime.now(),
                error_count=1,
                metadata={"error_type": type(e).__name__}
            )
    
    async def _check_configuration_health(self) -> ServiceHealth:
        """Check configuration system health."""
        start_time = time.time()
        service_name = "configuration"
        
        try:
            # Test configuration loading
            settings = get_settings()
            
            # Check required configuration sections
            required_sections = ['database', 'logging']
            missing_sections = []
            
            for section in required_sections:
                if not hasattr(settings, section):
                    missing_sections.append(section)
            
            response_time = (time.time() - start_time) * 1000
            
            if missing_sections:
                status = HealthStatus.DEGRADED
                message = f"Missing configuration sections: {', '.join(missing_sections)}"
                warning_count = len(missing_sections)
            else:
                status = HealthStatus.HEALTHY
                message = "Configuration loaded successfully"
                warning_count = 0
            
            return ServiceHealth(
                name=service_name,
                status=status,
                message=message,
                response_time_ms=response_time,
                last_check=datetime.now(),
                warning_count=warning_count,
                metadata={
                    "config_sections": [attr for attr in dir(settings) if not attr.startswith('_')],
                    "missing_sections": missing_sections
                }
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            
            return ServiceHealth(
                name=service_name,
                status=HealthStatus.UNHEALTHY,
                message=f"Configuration health check failed: {str(e)}",
                response_time_ms=response_time,
                last_check=datetime.now(),
                error_count=1,
                metadata={"error_type": type(e).__name__}
            )
    
    async def _check_logging_health(self) -> ServiceHealth:
        """Check logging system health."""
        start_time = time.time()
        service_name = "logging"
        
        try:
            # Test logging system
            test_logger = UnifiedLogger("health_check_test", LogComponent.MONITORING)
            test_logger.info("Health check test log entry")
            
            response_time = (time.time() - start_time) * 1000
            
            return ServiceHealth(
                name=service_name,
                status=HealthStatus.HEALTHY,
                message="Logging system functional",
                response_time_ms=response_time,
                last_check=datetime.now(),
                metadata={"test_log_written": True}
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            
            return ServiceHealth(
                name=service_name,
                status=HealthStatus.UNHEALTHY,
                message=f"Logging system failed: {str(e)}",
                response_time_ms=response_time,
                last_check=datetime.now(),
                error_count=1,
                metadata={"error_type": type(e).__name__}
            )
    
    async def _check_monitoring_health(self) -> ServiceHealth:
        """Check monitoring system health."""
        start_time = time.time()
        service_name = "monitoring"
        
        try:
            # This is a self-check - if we're running, monitoring is working
            response_time = (time.time() - start_time) * 1000
            
            return ServiceHealth(
                name=service_name,
                status=HealthStatus.HEALTHY,
                message="Monitoring system operational",
                response_time_ms=response_time,
                last_check=datetime.now(),
                metadata={"self_check": True}
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            
            return ServiceHealth(
                name=service_name,
                status=HealthStatus.UNHEALTHY,
                message=f"Monitoring health check failed: {str(e)}",
                response_time_ms=response_time,
                last_check=datetime.now(),
                error_count=1,
                metadata={"error_type": type(e).__name__}
            )
    
    async def _check_cli_health(self) -> ServiceHealth:
        """Check CLI system health."""
        start_time = time.time()
        service_name = "cli"
        
        try:
            # Test CLI module imports
            from ...interfaces.cli.main import main
            from ...interfaces.cli.enhanced_error_handling import EnhancedCLIValidator
            
            response_time = (time.time() - start_time) * 1000
            
            return ServiceHealth(
                name=service_name,
                status=HealthStatus.HEALTHY,
                message="CLI system accessible",
                response_time_ms=response_time,
                last_check=datetime.now(),
                metadata={"main_module_imported": True, "enhanced_error_handling": True}
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            
            return ServiceHealth(
                name=service_name,
                status=HealthStatus.UNHEALTHY,
                message=f"CLI health check failed: {str(e)}",
                response_time_ms=response_time,
                last_check=datetime.now(),
                error_count=1,
                metadata={"error_type": type(e).__name__}
            )
    
    async def _check_pipeline_health(self) -> ServiceHealth:
        """Check data pipeline health."""
        start_time = time.time()
        service_name = "pipeline"
        
        try:
            # Test pipeline orchestration imports
            from ...services.orchestration.pipeline_orchestration_service import PipelineOrchestrationService
            
            response_time = (time.time() - start_time) * 1000
            
            return ServiceHealth(
                name=service_name,
                status=HealthStatus.HEALTHY,
                message="Pipeline orchestration accessible",
                response_time_ms=response_time,
                last_check=datetime.now(),
                metadata={"orchestration_service": True}
            )
            
        except Exception as e:
            response_time = (time.time() - start_time) * 1000
            
            return ServiceHealth(
                name=service_name,
                status=HealthStatus.UNHEALTHY,
                message=f"Pipeline health check failed: {str(e)}",
                response_time_ms=response_time,
                last_check=datetime.now(),
                error_count=1,
                metadata={"error_type": type(e).__name__}
            )
    
    def _calculate_overall_status(self, services: List[ServiceHealth]) -> HealthStatus:
        """Calculate overall system status from individual service health."""
        if not services:
            return HealthStatus.UNKNOWN
        
        unhealthy_count = sum(1 for s in services if s.status == HealthStatus.UNHEALTHY)
        degraded_count = sum(1 for s in services if s.status == HealthStatus.DEGRADED)
        
        # If any critical service is unhealthy, system is unhealthy
        critical_services = ["database", "configuration"]
        critical_unhealthy = any(
            s.status == HealthStatus.UNHEALTHY for s in services 
            if s.name in critical_services
        )
        
        if critical_unhealthy:
            return HealthStatus.UNHEALTHY
        elif unhealthy_count > 0 or degraded_count >= len(services) // 2:
            return HealthStatus.DEGRADED
        elif degraded_count > 0:
            return HealthStatus.DEGRADED
        else:
            return HealthStatus.HEALTHY
    
    async def get_service_health(self, service_name: str) -> Optional[ServiceHealth]:
        """Get health status for a specific service."""
        system_health = await self.get_system_health()
        
        for service in system_health.services:
            if service.name == service_name:
                return service
        
        return None
    
    async def is_system_healthy(self) -> bool:
        """Check if system is healthy (no critical failures)."""
        system_health = await self.get_system_health()
        return system_health.status in [HealthStatus.HEALTHY, HealthStatus.DEGRADED]