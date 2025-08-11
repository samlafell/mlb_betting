"""
Health check endpoints for MLB ML API
Comprehensive health checks for all external dependencies
"""

import logging
import asyncio
import os
from datetime import datetime
from typing import Dict, Any, Optional
import traceback

from fastapi import APIRouter, Depends, HTTPException
import redis.asyncio as redis
import asyncpg
import aiohttp
import psutil

from ..dependencies import get_redis_client, get_database_connection

# Import ML-specific components
try:
    from ...features.redis_feature_store import RedisFeatureStore
    from ...monitoring.comprehensive_monitoring import get_monitoring_system
    from ...core.config import get_unified_config
except ImportError:
    # Fallback for environments without these components
    RedisFeatureStore = None
    get_monitoring_system = None
    get_unified_config = None

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/health")
async def health_check(
    redis_client: redis.Redis = Depends(get_redis_client),
) -> Dict[str, Any]:
    """
    Comprehensive health check endpoint for the ML prediction service
    Checks all external dependencies with detailed status and metrics
    """
    start_time = datetime.utcnow()
    health_status = {
        "service": "mlb-ml-prediction-api",
        "status": "healthy",
        "timestamp": start_time.isoformat(),
        "version": "2.1.0",
        "checks": {},
        "system_metrics": {},
        "response_time_ms": 0,
    }

    # System resource checks
    try:
        system_metrics = await _check_system_resources()
        health_status["system_metrics"] = system_metrics
        
        # Check if system resources are healthy
        if system_metrics.get("memory_usage_percent", 0) > 90:
            health_status["status"] = "degraded"
        if system_metrics.get("cpu_usage_percent", 0) > 95:
            health_status["status"] = "degraded"
            
    except Exception as e:
        logger.error(f"System metrics check failed: {e}")
        health_status["checks"]["system"] = {
            "status": "unhealthy",
            "message": f"System metrics failed: {str(e)}",
        }

    # Redis connection and feature store health check
    redis_check = await _check_redis_health(redis_client)
    health_status["checks"]["redis"] = redis_check
    if redis_check["status"] != "healthy":
        health_status["status"] = "degraded" if redis_check["status"] == "degraded" else health_status["status"]

    # Database connection health check
    database_check = await _check_database_health()
    health_status["checks"]["database"] = database_check
    if database_check["status"] != "healthy":
        health_status["status"] = "unhealthy" if database_check["status"] == "unhealthy" else health_status["status"]

    # MLflow model registry health check
    mlflow_check = await _check_mlflow_health()
    health_status["checks"]["mlflow"] = mlflow_check
    if mlflow_check["status"] != "healthy":
        health_status["status"] = "degraded" if mlflow_check["status"] == "degraded" else health_status["status"]

    # File system access health check
    filesystem_check = await _check_filesystem_health()
    health_status["checks"]["filesystem"] = filesystem_check
    if filesystem_check["status"] != "healthy":
        health_status["status"] = "degraded"

    # External API dependencies health check (if configured)
    external_apis_check = await _check_external_apis_health()
    health_status["checks"]["external_apis"] = external_apis_check
    if external_apis_check["status"] != "healthy":
        health_status["status"] = "degraded"

    # Monitoring system health check
    monitoring_check = await _check_monitoring_health()
    health_status["checks"]["monitoring"] = monitoring_check

    # Calculate total response time
    end_time = datetime.utcnow()
    health_status["response_time_ms"] = int((end_time - start_time).total_seconds() * 1000)

    # Return appropriate HTTP status
    if health_status["status"] == "unhealthy":
        raise HTTPException(status_code=503, detail=health_status)
    elif health_status["status"] == "degraded":
        # Return 200 but indicate degraded performance
        health_status["warning"] = "Service is running with degraded performance"

    return health_status


# Comprehensive health check helper functions

async def _check_system_resources() -> Dict[str, Any]:
    """Check system resource usage (CPU, Memory, Disk)"""
    try:
        # CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        
        # Memory usage
        memory = psutil.virtual_memory()
        memory_usage_percent = memory.percent
        memory_available_gb = memory.available / (1024**3)
        
        # Disk usage
        disk = psutil.disk_usage('/')
        disk_usage_percent = (disk.used / disk.total) * 100
        disk_free_gb = disk.free / (1024**3)
        
        # Process info
        process = psutil.Process()
        process_memory_mb = process.memory_info().rss / (1024**2)
        process_cpu_percent = process.cpu_percent()
        
        return {
            "cpu_usage_percent": round(cpu_percent, 2),
            "memory_usage_percent": round(memory_usage_percent, 2),
            "memory_available_gb": round(memory_available_gb, 2),
            "disk_usage_percent": round(disk_usage_percent, 2),
            "disk_free_gb": round(disk_free_gb, 2),
            "process_memory_mb": round(process_memory_mb, 2),
            "process_cpu_percent": round(process_cpu_percent, 2),
            "status": "healthy"
        }
    except Exception as e:
        logger.error(f"System resource check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e)
        }


async def _check_redis_health(redis_client: redis.Redis) -> Dict[str, Any]:
    """Comprehensive Redis health check including feature store"""
    check_start = datetime.utcnow()
    
    try:
        # Basic ping test
        await asyncio.wait_for(redis_client.ping(), timeout=5.0)
        
        # Test basic operations with timing
        test_key = "health_check_test"
        test_value = {"timestamp": datetime.utcnow().isoformat(), "test": True}
        
        # Test set operation
        set_start = datetime.utcnow()
        await redis_client.setex(test_key, 60, str(test_value))
        set_time_ms = (datetime.utcnow() - set_start).total_seconds() * 1000
        
        # Test get operation
        get_start = datetime.utcnow()
        retrieved_value = await redis_client.get(test_key)
        get_time_ms = (datetime.utcnow() - get_start).total_seconds() * 1000
        
        # Clean up
        await redis_client.delete(test_key)
        
        # Test feature store if available
        feature_store_status = "not_available"
        if RedisFeatureStore:
            try:
                feature_store = RedisFeatureStore()
                fs_health = await feature_store.health_check()
                feature_store_status = fs_health.get("status", "unknown")
            except Exception as e:
                feature_store_status = f"error: {str(e)}"
        
        total_time_ms = (datetime.utcnow() - check_start).total_seconds() * 1000
        
        # Determine status based on performance
        status = "healthy"
        if total_time_ms > 1000:  # 1 second
            status = "degraded"
        elif not retrieved_value:
            status = "degraded"
            
        return {
            "status": status,
            "response_time_ms": round(total_time_ms, 2),
            "set_time_ms": round(set_time_ms, 2),
            "get_time_ms": round(get_time_ms, 2),
            "feature_store_status": feature_store_status,
            "message": "Redis connection successful with performance metrics"
        }
        
    except asyncio.TimeoutError:
        return {
            "status": "unhealthy",
            "message": "Redis connection timeout (>5s)",
            "response_time_ms": 5000
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "message": f"Redis connection failed: {str(e)}",
            "error_type": type(e).__name__
        }


async def _check_database_health() -> Dict[str, Any]:
    """Comprehensive database connection health check"""
    check_start = datetime.utcnow()
    
    try:
        # Get database configuration
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5432")
        database = os.getenv("POSTGRES_DB", "mlb_betting")
        user = os.getenv("POSTGRES_USER", "samlafell")
        password = os.getenv("POSTGRES_PASSWORD", "")
        
        dsn = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        
        # Test connection with timeout
        conn = await asyncio.wait_for(
            asyncpg.connect(dsn), 
            timeout=10.0
        )
        
        try:
            # Test basic query
            query_start = datetime.utcnow()
            result = await conn.fetchval("SELECT 1")
            query_time_ms = (datetime.utcnow() - query_start).total_seconds() * 1000
            
            # Test table access (if tables exist)
            tables_accessible = False
            try:
                table_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)
                tables_accessible = True
            except Exception:
                table_count = 0
            
            total_time_ms = (datetime.utcnow() - check_start).total_seconds() * 1000
            
            # Determine status
            status = "healthy"
            if total_time_ms > 2000:  # 2 seconds
                status = "degraded"
            elif result != 1:
                status = "degraded"
                
            return {
                "status": status,
                "response_time_ms": round(total_time_ms, 2),
                "query_time_ms": round(query_time_ms, 2),
                "tables_accessible": tables_accessible,
                "table_count": table_count,
                "connection_host": host,
                "connection_port": port,
                "connection_database": database,
                "message": "Database connection successful"
            }
            
        finally:
            await conn.close()
            
    except asyncio.TimeoutError:
        return {
            "status": "unhealthy",
            "message": "Database connection timeout (>10s)",
            "response_time_ms": 10000
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "message": f"Database connection failed: {str(e)}",
            "error_type": type(e).__name__
        }


async def _check_mlflow_health() -> Dict[str, Any]:
    """MLflow model registry health check"""
    check_start = datetime.utcnow()
    
    try:
        # Import MLflow here to avoid startup dependencies
        import mlflow
        
        # Check if MLflow tracking URI is configured
        tracking_uri = mlflow.get_tracking_uri()
        
        # Test basic MLflow operations
        try:
            # Try to list experiments (basic connectivity test)
            experiments = mlflow.search_experiments(max_results=1)
            experiments_accessible = True
            experiment_count = len(experiments) if experiments else 0
        except Exception as e:
            experiments_accessible = False
            experiment_count = 0
            logger.warning(f"MLflow experiments not accessible: {e}")
        
        # Test model registry access
        try:
            # Try to list registered models
            from mlflow import MlflowClient
            client = MlflowClient()
            models = client.search_registered_models(max_results=1)
            models_accessible = True
            model_count = len(models) if models else 0
        except Exception as e:
            models_accessible = False
            model_count = 0
            logger.warning(f"MLflow model registry not accessible: {e}")
        
        total_time_ms = (datetime.utcnow() - check_start).total_seconds() * 1000
        
        # Determine status
        status = "healthy"
        if not experiments_accessible and not models_accessible:
            status = "degraded"
        elif total_time_ms > 3000:  # 3 seconds
            status = "degraded"
            
        return {
            "status": status,
            "response_time_ms": round(total_time_ms, 2),
            "tracking_uri": tracking_uri,
            "experiments_accessible": experiments_accessible,
            "experiment_count": experiment_count,
            "models_accessible": models_accessible,
            "model_count": model_count,
            "message": "MLflow registry connection successful"
        }
        
    except ImportError:
        return {
            "status": "degraded",
            "message": "MLflow not available - not installed",
            "response_time_ms": 0
        }
    except Exception as e:
        return {
            "status": "degraded",
            "message": f"MLflow check failed: {str(e)}",
            "error_type": type(e).__name__,
            "response_time_ms": (datetime.utcnow() - check_start).total_seconds() * 1000
        }


async def _check_filesystem_health() -> Dict[str, Any]:
    """File system access and model artifacts health check"""
    check_start = datetime.utcnow()
    
    try:
        # Common model storage paths
        model_paths = [
            "/tmp/mlflow_models",
            "~/.mlflow",
            "/opt/ml/models",
            "./models",
            "./artifacts"
        ]
        
        accessible_paths = []
        writable_paths = []
        
        for path in model_paths:
            try:
                expanded_path = os.path.expanduser(path)
                if os.path.exists(expanded_path):
                    accessible_paths.append(path)
                    
                    # Test write access
                    test_file = os.path.join(expanded_path, "health_check_test.tmp")
                    try:
                        with open(test_file, 'w') as f:
                            f.write("health check")
                        os.remove(test_file)
                        writable_paths.append(path)
                    except (PermissionError, OSError):
                        pass
            except Exception:
                pass
        
        # Test current directory access
        current_dir_writable = False
        try:
            test_file = "health_check_test.tmp"
            with open(test_file, 'w') as f:
                f.write("health check")
            os.remove(test_file)
            current_dir_writable = True
        except Exception:
            pass
        
        total_time_ms = (datetime.utcnow() - check_start).total_seconds() * 1000
        
        # Determine status
        status = "healthy"
        if not current_dir_writable and not writable_paths:
            status = "degraded"
            
        return {
            "status": status,
            "response_time_ms": round(total_time_ms, 2),
            "accessible_paths": accessible_paths,
            "writable_paths": writable_paths,
            "current_dir_writable": current_dir_writable,
            "message": "Filesystem access check completed"
        }
        
    except Exception as e:
        return {
            "status": "degraded",
            "message": f"Filesystem check failed: {str(e)}",
            "error_type": type(e).__name__
        }


async def _check_external_apis_health() -> Dict[str, Any]:
    """Check external API dependencies (if configured)"""
    check_start = datetime.utcnow()
    
    # Define external APIs that the ML system might depend on
    apis_to_check = []
    
    # Add APIs based on environment configuration
    if os.getenv("ODDS_API_KEY"):
        apis_to_check.append({
            "name": "odds_api",
            "url": "https://api.the-odds-api.com/v4/sports",
            "timeout": 5
        })
    
    api_results = {}
    overall_status = "healthy"
    
    if not apis_to_check:
        return {
            "status": "healthy",
            "message": "No external APIs configured",
            "api_count": 0,
            "response_time_ms": 0
        }
    
    try:
        async with aiohttp.ClientSession() as session:
            for api in apis_to_check:
                api_start = datetime.utcnow()
                try:
                    async with session.get(
                        api["url"], 
                        timeout=aiohttp.ClientTimeout(total=api["timeout"])
                    ) as response:
                        api_time_ms = (datetime.utcnow() - api_start).total_seconds() * 1000
                        
                        api_results[api["name"]] = {
                            "status": "healthy" if response.status < 400 else "degraded",
                            "status_code": response.status,
                            "response_time_ms": round(api_time_ms, 2)
                        }
                        
                        if response.status >= 400:
                            overall_status = "degraded"
                            
                except asyncio.TimeoutError:
                    api_results[api["name"]] = {
                        "status": "degraded",
                        "error": "timeout",
                        "response_time_ms": api["timeout"] * 1000
                    }
                    overall_status = "degraded"
                except Exception as e:
                    api_results[api["name"]] = {
                        "status": "degraded",
                        "error": str(e),
                        "response_time_ms": (datetime.utcnow() - api_start).total_seconds() * 1000
                    }
                    overall_status = "degraded"
        
        total_time_ms = (datetime.utcnow() - check_start).total_seconds() * 1000
        
        return {
            "status": overall_status,
            "response_time_ms": round(total_time_ms, 2),
            "api_count": len(apis_to_check),
            "api_results": api_results,
            "message": "External API health check completed"
        }
        
    except Exception as e:
        return {
            "status": "degraded",
            "message": f"External API check failed: {str(e)}",
            "error_type": type(e).__name__
        }


async def _check_monitoring_health() -> Dict[str, Any]:
    """Check monitoring system health"""
    check_start = datetime.utcnow()
    
    try:
        if get_monitoring_system:
            # Test monitoring system
            monitoring = await get_monitoring_system()
            health_summary = await monitoring.get_health_summary()
            
            total_time_ms = (datetime.utcnow() - check_start).total_seconds() * 1000
            
            return {
                "status": "healthy",
                "response_time_ms": round(total_time_ms, 2),
                "monitoring_available": True,
                "monitoring_status": health_summary.get("status", "unknown"),
                "message": "Monitoring system accessible"
            }
        else:
            return {
                "status": "degraded",
                "message": "Monitoring system not available",
                "monitoring_available": False,
                "response_time_ms": 0
            }
            
    except Exception as e:
        return {
            "status": "degraded",
            "message": f"Monitoring check failed: {str(e)}",
            "error_type": type(e).__name__,
            "monitoring_available": False
        }


@router.get("/health/detailed")
async def detailed_health_check() -> Dict[str, Any]:
    """
    Detailed health check with extended diagnostics
    Includes system metrics, dependency latencies, and configuration status
    """
    start_time = datetime.utcnow()
    
    # Get all individual health checks
    system_resources = await _check_system_resources()
    redis_health = await _check_redis_health(await get_redis_client())
    database_health = await _check_database_health()
    mlflow_health = await _check_mlflow_health()
    filesystem_health = await _check_filesystem_health()
    external_apis_health = await _check_external_apis_health()
    monitoring_health = await _check_monitoring_health()
    
    # Configuration status
    config_status = {
        "redis_configured": bool(os.getenv("REDIS_URL")),
        "database_configured": bool(os.getenv("POSTGRES_HOST")),
        "mlflow_configured": bool(os.getenv("MLFLOW_TRACKING_URI")),
        "monitoring_configured": get_monitoring_system is not None,
        "unified_config_available": get_unified_config is not None
    }
    
    # Calculate overall status
    all_checks = [
        system_resources.get("status", "unknown"),
        redis_health.get("status", "unknown"),
        database_health.get("status", "unknown"),
        mlflow_health.get("status", "unknown"),
        filesystem_health.get("status", "unknown"),
        external_apis_health.get("status", "unknown"),
        monitoring_health.get("status", "unknown")
    ]
    
    if "unhealthy" in all_checks:
        overall_status = "unhealthy"
    elif "degraded" in all_checks:
        overall_status = "degraded"
    else:
        overall_status = "healthy"
    
    total_time_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
    
    return {
        "service": "mlb-ml-prediction-api",
        "status": overall_status,
        "timestamp": start_time.isoformat(),
        "response_time_ms": round(total_time_ms, 2),
        "detailed_checks": {
            "system_resources": system_resources,
            "redis": redis_health,
            "database": database_health,
            "mlflow": mlflow_health,
            "filesystem": filesystem_health,
            "external_apis": external_apis_health,
            "monitoring": monitoring_health
        },
        "configuration": config_status,
        "recommendations": _generate_health_recommendations(all_checks, config_status)
    }


def _generate_health_recommendations(check_statuses: list, config_status: dict) -> list:
    """Generate actionable recommendations based on health check results"""
    recommendations = []
    
    if "unhealthy" in check_statuses:
        recommendations.append("🚨 Critical: Some services are unhealthy and require immediate attention")
    
    if "degraded" in check_statuses:
        recommendations.append("⚠️ Warning: Some services are degraded and may impact performance")
    
    if not config_status.get("redis_configured"):
        recommendations.append("💡 Consider configuring Redis for better feature caching performance")
    
    if not config_status.get("mlflow_configured"):
        recommendations.append("💡 Configure MLflow tracking URI for model versioning and experiments")
    
    if not config_status.get("monitoring_configured"):
        recommendations.append("💡 Enable comprehensive monitoring for production insights")
    
    if not recommendations:
        recommendations.append("✅ All systems are healthy - no recommendations at this time")
    
    return recommendations


@router.get("/health/ready")
async def readiness_check() -> Dict[str, Any]:
    """
    Readiness check - indicates if service is ready to handle requests
    """
    return {
        "service": "mlb-ml-prediction-api",
        "ready": True,
        "timestamp": datetime.utcnow().isoformat(),
    }


@router.get("/health/live")
async def liveness_check() -> Dict[str, Any]:
    """
    Liveness check - indicates if service is alive
    """
    return {
        "service": "mlb-ml-prediction-api",
        "alive": True,
        "timestamp": datetime.utcnow().isoformat(),
    }
