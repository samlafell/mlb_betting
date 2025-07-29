#!/usr/bin/env python3
"""
FastAPI Monitoring Dashboard Service

Real-time monitoring dashboard with WebSocket updates for the MLB betting system.
Provides comprehensive visibility into pipeline executions, system health, and performance metrics.

Features:
- Real-time pipeline execution status via WebSockets
- Comprehensive REST API for monitoring data
- Interactive web dashboard with live updates
- Prometheus metrics integration
- System health and performance monitoring
- Break-glass manual controls
"""

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from ...core.config import get_settings
from ...core.enhanced_logging import get_contextual_logger, LogComponent
from ...core.exceptions import (
    handle_exception, 
    MonitoringError, 
    PipelineExecutionError, 
    WebSocketError,
    DatabaseError
)
from ...core.logging import LogLevel
from ...core.security import require_break_glass_auth, SecurityHeaders
from ...services.monitoring.prometheus_metrics_service import get_metrics_service
from ...services.monitoring.unified_monitoring_service import UnifiedMonitoringService
from ...services.orchestration.pipeline_orchestration_service import (
    pipeline_orchestration_service,
    PipelineStatus,
    SystemHealth
)

# Initialize components
settings = get_settings()
logger = get_contextual_logger(__name__, LogComponent.API_CLIENT)
metrics_service = get_metrics_service()


# Pydantic models for API responses
class PipelineExecutionResponse(BaseModel):
    """Pipeline execution status response."""
    
    pipeline_id: str
    pipeline_type: str
    status: str
    start_time: datetime
    end_time: Optional[datetime] = None
    execution_time_seconds: float = 0.0
    stages_executed: int = 0
    successful_stages: int = 0
    failed_stages: int = 0
    system_state: Dict[str, Any] = {}
    recommendations: List[str] = []


class SystemHealthResponse(BaseModel):
    """System health status response."""
    
    overall_status: str
    uptime_seconds: float
    data_freshness_score: float
    system_load: Dict[str, float]
    active_pipelines: int
    recent_success_rate: float
    slo_compliance: Dict[str, Any]
    alerts: List[Dict[str, Any]] = []


class MetricsResponse(BaseModel):
    """Comprehensive metrics response."""
    
    pipeline_metrics: Dict[str, Any]
    business_metrics: Dict[str, Any]
    system_metrics: Dict[str, Any]
    sli_metrics: Dict[str, Any]
    timestamp: datetime


class WebSocketMessage(BaseModel):
    """WebSocket message structure."""
    
    type: str  # pipeline_update, system_health, metrics_update, alert
    data: Dict[str, Any]
    timestamp: datetime = datetime.now(timezone.utc)


class ConnectionManager:
    """WebSocket connection manager for real-time updates."""
    
    def __init__(self):
        self.active_connections: Set[WebSocket] = set()
        self.connection_metadata: Dict[WebSocket, Dict[str, Any]] = {}
        
    async def connect(self, websocket: WebSocket, client_info: Optional[Dict[str, Any]] = None):
        """Connect a new WebSocket client."""
        await websocket.accept()
        self.active_connections.add(websocket)
        self.connection_metadata[websocket] = client_info or {}
        
        logger.info(
            "WebSocket client connected",
            total_connections=len(self.active_connections),
            client_info=client_info
        )
    
    def disconnect(self, websocket: WebSocket):
        """Disconnect a WebSocket client."""
        self.active_connections.discard(websocket)
        self.connection_metadata.pop(websocket, None)
        
        logger.info(
            "WebSocket client disconnected",
            total_connections=len(self.active_connections)
        )
    
    async def send_personal_message(self, message: WebSocketMessage, websocket: WebSocket):
        """Send message to a specific client."""
        try:
            await websocket.send_text(message.model_dump_json())
        except WebSocketError as e:
            logger.error("WebSocket error sending message", 
                        error=e, correlation_id=e.correlation_id)
            self.disconnect(websocket)
        except Exception as e:
            handled_error = handle_exception(
                e, 
                component="websocket_manager", 
                operation="send_personal_message"
            )
            logger.error("Failed to send WebSocket message", 
                        error=handled_error, correlation_id=handled_error.correlation_id)
            self.disconnect(websocket)
    
    async def broadcast(self, message: WebSocketMessage):
        """Broadcast message to all connected clients."""
        if not self.active_connections:
            return
        
        message_json = message.model_dump_json()
        disconnected_clients = set()
        
        for connection in self.active_connections:
            try:
                await connection.send_text(message_json)
            except WebSocketError as e:
                logger.error("WebSocket error broadcasting message", 
                            error=e, correlation_id=e.correlation_id)
                disconnected_clients.add(connection)
            except Exception as e:
                handled_error = handle_exception(
                    e, 
                    component="websocket_manager", 
                    operation="broadcast_message"
                )
                logger.error("Failed to broadcast WebSocket message", 
                            error=handled_error, correlation_id=handled_error.correlation_id)
                disconnected_clients.add(connection)
        
        # Clean up disconnected clients
        for client in disconnected_clients:
            self.disconnect(client)
        
        if disconnected_clients:
            logger.info(
                "Cleaned up disconnected WebSocket clients",
                disconnected_count=len(disconnected_clients),
                active_connections=len(self.active_connections)
            )


# Initialize FastAPI app
app = FastAPI(
    title="MLB Betting System Monitoring Dashboard",
    description="Real-time monitoring and observability dashboard for the MLB betting pipeline system",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add security headers middleware
@app.middleware("http")
async def add_security_headers(request, call_next):
    """Add security headers to all responses."""
    response = await call_next(request)
    
    # Add standard security headers
    security_headers = SecurityHeaders.get_security_headers()
    for header, value in security_headers.items():
        response.headers[header] = value
    
    return response

# WebSocket connection manager
manager = ConnectionManager()

# Initialize monitoring service
monitoring_service = UnifiedMonitoringService(settings)

# Templates
templates = Jinja2Templates(directory="src/interfaces/api/templates")


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup."""
    await monitoring_service.initialize()
    
    # Start background task for broadcasting updates
    asyncio.create_task(broadcast_system_updates())
    
    logger.info("Monitoring dashboard API started")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown."""
    await monitoring_service.cleanup()
    logger.info("Monitoring dashboard API shutdown")


# REST API Endpoints

@app.get("/", response_class=HTMLResponse)
async def dashboard_home(request: Request):
    """Serve the main dashboard HTML page."""
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc),
        "service": "monitoring-dashboard",
        "version": "1.0.0"
    }


@app.get("/api/system/health", response_model=SystemHealthResponse)
async def get_system_health():
    """Get comprehensive system health status."""
    try:
        # Get monitoring report
        monitoring_report = await monitoring_service.get_system_health()
        
        # Get orchestration metrics
        orchestration_metrics = pipeline_orchestration_service.get_metrics()
        
        # Get Prometheus system overview
        prometheus_overview = metrics_service.get_system_overview()
        
        return SystemHealthResponse(
            overall_status=monitoring_report.overall_status.value,
            uptime_seconds=prometheus_overview.get("uptime_seconds", 0),
            data_freshness_score=monitoring_report.business_metrics.data_freshness_score if monitoring_report.business_metrics else 0,
            system_load={
                "cpu_usage": monitoring_report.system_metrics.cpu_usage if monitoring_report.system_metrics else 0,
                "memory_usage": monitoring_report.system_metrics.memory_usage if monitoring_report.system_metrics else 0,
                "disk_usage": monitoring_report.system_metrics.disk_usage if monitoring_report.system_metrics else 0
            },
            active_pipelines=len(pipeline_orchestration_service.get_active_pipelines()),
            recent_success_rate=orchestration_metrics.get("combined_insights", {}).get("recent_success_rate", 0),
            slo_compliance=prometheus_overview.get("slo_compliance", {}),
            alerts=[
                {
                    "level": alert.level.value,
                    "title": alert.title,
                    "message": alert.message,
                    "timestamp": alert.timestamp.isoformat(),
                    "acknowledged": alert.acknowledged
                }
                for alert in monitoring_report.active_alerts
            ]
        )
        
    except DatabaseError as e:
        logger.error("Database error getting system health", error=e, correlation_id=e.correlation_id)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Database unavailable: {e.user_message}"
        )
    except Exception as e:
        handled_error = handle_exception(
            e, 
            component="monitoring_dashboard", 
            operation="get_system_health"
        )
        logger.error("Failed to get system health", 
                    error=handled_error, 
                    correlation_id=handled_error.correlation_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=handled_error.user_message
        )


@app.get("/api/pipelines/active", response_model=List[PipelineExecutionResponse])
async def get_active_pipelines():
    """Get currently active pipeline executions."""
    try:
        active_pipelines = pipeline_orchestration_service.get_active_pipelines()
        
        return [
            PipelineExecutionResponse(
                pipeline_id=pipeline.pipeline_id,
                pipeline_type=pipeline.pipeline_type,
                status=pipeline.overall_status.value,
                start_time=pipeline.start_time,
                end_time=pipeline.end_time,
                execution_time_seconds=pipeline.total_execution_time,
                stages_executed=len(pipeline.stages),
                successful_stages=sum(
                    1 for stage in pipeline.stages.values() 
                    if stage.status == PipelineStatus.SUCCESS
                ),
                failed_stages=sum(
                    1 for stage in pipeline.stages.values() 
                    if stage.status == PipelineStatus.FAILED
                ),
                system_state=pipeline.system_state,
                recommendations=pipeline.recommendations
            )
            for pipeline in active_pipelines
        ]
        
    except Exception as e:
        logger.error("Failed to get active pipelines", error=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve active pipelines"
        )


@app.get("/api/pipelines/recent", response_model=List[PipelineExecutionResponse])
async def get_recent_pipelines(limit: int = 10):
    """Get recent pipeline executions."""
    try:
        recent_pipelines = pipeline_orchestration_service.get_recent_pipelines(limit)
        
        return [
            PipelineExecutionResponse(
                pipeline_id=pipeline.pipeline_id,
                pipeline_type=pipeline.pipeline_type,
                status=pipeline.overall_status.value,
                start_time=pipeline.start_time,
                end_time=pipeline.end_time,
                execution_time_seconds=pipeline.total_execution_time,
                stages_executed=len(pipeline.stages),
                successful_stages=sum(
                    1 for stage in pipeline.stages.values() 
                    if stage.status == PipelineStatus.SUCCESS
                ),
                failed_stages=sum(
                    1 for stage in pipeline.stages.values() 
                    if stage.status == PipelineStatus.FAILED
                ),
                system_state=pipeline.system_state,
                recommendations=pipeline.recommendations
            )
            for pipeline in recent_pipelines
        ]
        
    except Exception as e:
        logger.error("Failed to get recent pipelines", error=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve recent pipelines"
        )


@app.get("/api/metrics/all", response_model=MetricsResponse)
async def get_all_metrics():
    """Get comprehensive system metrics."""
    try:
        # Get orchestration metrics
        orchestration_metrics = pipeline_orchestration_service.get_metrics()
        
        # Get system overview from Prometheus
        prometheus_overview = metrics_service.get_system_overview()
        
        # Get monitoring report for additional metrics
        monitoring_report = await monitoring_service.get_system_health()
        
        return MetricsResponse(
            pipeline_metrics=orchestration_metrics.get("orchestration", {}),
            business_metrics={
                "opportunities_detected": monitoring_report.business_metrics.opportunities_found if monitoring_report.business_metrics else 0,
                "recommendations_made": monitoring_report.business_metrics.recommendations_made if monitoring_report.business_metrics else 0,
                "total_value_identified": monitoring_report.business_metrics.total_value_identified if monitoring_report.business_metrics else 0,
                "data_freshness_score": monitoring_report.business_metrics.data_freshness_score if monitoring_report.business_metrics else 0
            },
            system_metrics={
                "cpu_usage": monitoring_report.system_metrics.cpu_usage if monitoring_report.system_metrics else 0,
                "memory_usage": monitoring_report.system_metrics.memory_usage if monitoring_report.system_metrics else 0,
                "disk_usage": monitoring_report.system_metrics.disk_usage if monitoring_report.system_metrics else 0,
                "database_connections": monitoring_report.system_metrics.database_connections if monitoring_report.system_metrics else 0
            },
            sli_metrics=prometheus_overview.get("slo_compliance", {}),
            timestamp=datetime.now(timezone.utc)
        )
        
    except Exception as e:
        logger.error("Failed to get metrics", error=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve metrics"
        )


@app.get("/api/metrics/prometheus")
async def get_prometheus_metrics():
    """Get Prometheus metrics in text format."""
    try:
        metrics_text = metrics_service.get_metrics()
        return {
            "metrics": metrics_text,
            "content_type": metrics_service.get_content_type(),
            "timestamp": datetime.now(timezone.utc)
        }
    except Exception as e:
        logger.error("Failed to get Prometheus metrics", error=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve Prometheus metrics"
        )


# Break-glass manual control endpoints

@app.post("/api/control/pipeline/execute")
async def execute_pipeline_manual(
    pipeline_type: str = "full",
    force_execution: bool = True,
    authenticated: bool = Depends(require_break_glass_auth)
):
    """Manually execute a pipeline (break-glass procedure)."""
    try:
        logger.warning(
            "Manual pipeline execution triggered",
            pipeline_type=pipeline_type,
            force_execution=force_execution
        )
        
        # Record break-glass activation
        metrics_service.record_break_glass_activation(
            "manual_pipeline_execution",
            f"Manual execution of {pipeline_type} pipeline"
        )
        
        # Execute pipeline
        result = await pipeline_orchestration_service.execute_smart_pipeline(
            pipeline_type=pipeline_type,
            force_execution=force_execution
        )
        
        # Broadcast update
        await manager.broadcast(WebSocketMessage(
            type="pipeline_manual_start",
            data={
                "pipeline_id": result.pipeline_id,
                "pipeline_type": result.pipeline_type,
                "status": result.overall_status.value,
                "manual_execution": True
            }
        ))
        
        return {
            "success": True,
            "pipeline_id": result.pipeline_id,
            "status": result.overall_status.value,
            "message": f"Manual {pipeline_type} pipeline execution initiated"
        }
        
    except PipelineExecutionError as e:
        logger.error("Pipeline execution failed", 
                    error=e, 
                    correlation_id=e.correlation_id,
                    pipeline_type=pipeline_type)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Pipeline execution failed: {e.user_message}"
        )
    except Exception as e:
        handled_error = handle_exception(
            e, 
            component="monitoring_dashboard", 
            operation="manual_pipeline_execution",
            details={"pipeline_type": pipeline_type, "force_execution": force_execution}
        )
        logger.error("Manual pipeline execution failed", 
                    error=handled_error, 
                    correlation_id=handled_error.correlation_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=handled_error.user_message
        )


@app.post("/api/control/system/override")
async def system_override(
    component: str,
    action: str,
    reason: str,
    authenticated: bool = Depends(require_break_glass_auth)
):
    """Manual system override (break-glass procedure)."""
    try:
        logger.warning(
            "Manual system override triggered",
            component=component,
            action=action,
            reason=reason
        )
        
        # Record manual override
        metrics_service.record_manual_override(component, reason)
        
        # Broadcast alert
        await manager.broadcast(WebSocketMessage(
            type="system_override",
            data={
                "component": component,
                "action": action,
                "reason": reason,
                "severity": "warning"
            }
        ))
        
        return {
            "success": True,
            "message": f"Manual override recorded for {component}: {action}",
            "timestamp": datetime.now(timezone.utc)
        }
        
    except Exception as e:
        logger.error("System override failed", error=e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to record system override: {str(e)}"
        )


# WebSocket endpoint for real-time updates

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time monitoring updates."""
    await manager.connect(websocket, {
        "connected_at": datetime.now(timezone.utc),
        "client_type": "monitoring_dashboard"
    })
    
    try:
        # Send initial system state
        health_response = await get_system_health()
        await manager.send_personal_message(
            WebSocketMessage(
                type="initial_state",
                data=health_response.model_dump()
            ),
            websocket
        )
        
        # Keep connection alive and handle client messages
        while True:
            try:
                data = await websocket.receive_text()
                client_message = json.loads(data)
                
                # Handle client requests
                if client_message.get("type") == "request_update":
                    # Send current metrics
                    metrics_response = await get_all_metrics() 
                    await manager.send_personal_message(
                        WebSocketMessage(
                            type="metrics_update",
                            data=metrics_response.model_dump()
                        ),
                        websocket
                    )
                    
            except json.JSONDecodeError:
                logger.warning("Invalid JSON received from WebSocket client")
            except Exception as e:
                logger.error("Error handling WebSocket message", error=e)
                break
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error("WebSocket connection error", error=e)
        manager.disconnect(websocket)


# Background task for broadcasting system updates

async def broadcast_system_updates():
    """Background task to broadcast system updates to WebSocket clients."""
    while True:
        try:
            if manager.active_connections:
                # Get current system health
                health_response = await get_system_health()
                
                # Broadcast system health update
                await manager.broadcast(WebSocketMessage(
                    type="system_health_update",
                    data=health_response.model_dump()
                ))
                
                # Get active pipelines
                active_pipelines = await get_active_pipelines()
                if active_pipelines:
                    await manager.broadcast(WebSocketMessage(
                        type="pipeline_status_update",
                        data={"active_pipelines": [p.model_dump() for p in active_pipelines]}
                    ))
                
            # Wait before next update
            await asyncio.sleep(settings.dashboard.system_health_update_interval)
            
        except MonitoringError as e:
            logger.error("Monitoring service error during broadcast", 
                        error=e, correlation_id=e.correlation_id)
            await asyncio.sleep(settings.dashboard.error_recovery_delay)
        except WebSocketError as e:
            logger.error("WebSocket error during broadcast", 
                        error=e, correlation_id=e.correlation_id)
            await asyncio.sleep(settings.dashboard.websocket_error_delay)
        except Exception as e:
            handled_error = handle_exception(
                e, 
                component="monitoring_dashboard", 
                operation="broadcast_system_updates"
            )
            logger.error("Error in broadcast system updates", 
                        error=handled_error, correlation_id=handled_error.correlation_id)
            await asyncio.sleep(settings.dashboard.error_recovery_delay)


# Utility function to run the dashboard
def run_dashboard(host: str = "0.0.0.0", port: int = 8001):
    """Run the monitoring dashboard server."""
    uvicorn.run(
        "src.interfaces.api.monitoring_dashboard:app",
        host=host,
        port=port,
        reload=False,
        log_level="info"
    )


if __name__ == "__main__":
    run_dashboard()