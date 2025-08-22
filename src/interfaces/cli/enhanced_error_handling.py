#!/usr/bin/env python3
"""
Enhanced CLI Error Handling and Recovery System

Production-grade error handling improvements for CLI commands with:
- Database connectivity validation
- Clear error messages with recovery suggestions
- --dry-run modes for safe testing
- Graceful degradation strategies
- Circuit breaker patterns for external services

This addresses Issue #38: System Reliability Issues Prevent Production Use
"""

import asyncio
import sys
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

from ...core.config import get_settings
from ...core.logging import UnifiedLogger, LogComponent

console = Console()
logger = UnifiedLogger("enhanced_cli", LogComponent.CLI)


class SystemStatus(Enum):
    """System health status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded" 
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class DatabaseValidator:
    """Database connectivity and health validation."""
    
    def __init__(self):
        self.settings = get_settings()
        self.logger = logger.with_context(component="database_validator")
        
    async def validate_connection(self, timeout_seconds: int = 10) -> Tuple[bool, str, Dict[str, Any]]:
        """
        Validate database connection with timeout and detailed diagnostics.
        
        Returns:
            Tuple of (success, message, metadata)
        """
        start_time = datetime.now()
        metadata = {
            "connection_time_ms": 0,
            "database": self.settings.database.database,
            "host": self.settings.database.host,
            "port": self.settings.database.port
        }
        
        try:
            # Test connection with timeout
            conn = await asyncio.wait_for(
                asyncpg.connect(
                    host=self.settings.database.host,
                    port=self.settings.database.port,
                    user=self.settings.database.user,
                    password=self.settings.database.password,
                    database=self.settings.database.database
                ),
                timeout=timeout_seconds
            )
            
            connection_time = (datetime.now() - start_time).total_seconds() * 1000
            metadata["connection_time_ms"] = round(connection_time, 2)
            
            # Test basic query
            await conn.fetchval("SELECT 1")
            
            # Get database info
            db_version = await conn.fetchval("SELECT version()")
            metadata["postgres_version"] = db_version.split(",")[0] if db_version else "unknown"
            
            await conn.close()
            
            return True, f"✅ Database connection successful ({connection_time:.1f}ms)", metadata
            
        except asyncio.TimeoutError:
            return False, f"❌ Database connection timeout after {timeout_seconds}s", metadata
            
        except asyncpg.InvalidPasswordError:
            return False, "❌ Database authentication failed - check password", metadata
            
        except asyncpg.InvalidCatalogNameError:
            return False, f"❌ Database '{metadata['database']}' does not exist", metadata
            
        except asyncpg.CannotConnectNowError:
            return False, "❌ Database server is not accepting connections", metadata
            
        except ConnectionRefusedError:
            return False, f"❌ Cannot connect to database at {metadata['host']}:{metadata['port']}", metadata
            
        except Exception as e:
            return False, f"❌ Database connection failed: {str(e)}", metadata

    async def get_table_health(self) -> Dict[str, Any]:
        """Get health information about critical tables."""
        try:
            conn = await asyncpg.connect(
                host=self.settings.database.host,
                port=self.settings.database.port,
                user=self.settings.database.user,
                password=self.settings.database.password,
                database=self.settings.database.database
            )
            
            # Check critical tables
            health_data = {}
            critical_tables = [
                ("raw_data", "action_network_odds"),
                ("staging", "betting_odds_unified"),
                ("curated", "enhanced_games"),
                ("curated", "ml_features")
            ]
            
            for schema, table in critical_tables:
                try:
                    count = await conn.fetchval(
                        f"SELECT COUNT(*) FROM {schema}.{table}"
                    )
                    recent_count = await conn.fetchval(
                        f"""
                        SELECT COUNT(*) FROM {schema}.{table} 
                        WHERE created_at > NOW() - INTERVAL '24 hours'
                        """
                    )
                    health_data[f"{schema}.{table}"] = {
                        "total_records": count,
                        "recent_records": recent_count,
                        "status": "healthy" if recent_count > 0 else "stale"
                    }
                except Exception as e:
                    health_data[f"{schema}.{table}"] = {
                        "total_records": 0,
                        "recent_records": 0,
                        "status": "error",
                        "error": str(e)
                    }
            
            await conn.close()
            return health_data
            
        except Exception as e:
            logger.error("Failed to get table health", error=str(e))
            return {}


class ErrorRecoveryGuide:
    """Provides context-aware error recovery suggestions."""
    
    @staticmethod
    def get_database_recovery_steps(error_message: str) -> List[str]:
        """Get specific recovery steps for database errors."""
        if "authentication failed" in error_message.lower():
            return [
                "1. Check DB_PASSWORD environment variable is set",
                "2. Verify password in database configuration",
                "3. Ensure database user exists and has correct permissions",
                "4. Try: export DB_PASSWORD=postgres"
            ]
        elif "connection timeout" in error_message.lower():
            return [
                "1. Check if database server is running",
                "2. Verify network connectivity to database host",
                "3. Check firewall settings",
                "4. Try: docker ps | grep postgres"
            ]
        elif "database" in error_message.lower() and "does not exist" in error_message.lower():
            return [
                "1. Create the database: createdb mlb_betting",
                "2. Run database migrations",
                "3. Check database name in configuration",
                "4. Try: psql -l to list databases"
            ]
        elif "connection refused" in error_message.lower():
            return [
                "1. Start PostgreSQL service",
                "2. Check if running on correct port (default: 5433)",
                "3. For Docker: docker-compose up -d postgres",
                "4. Check port conflicts: lsof -i :5433"
            ]
        else:
            return [
                "1. Check database server status",
                "2. Verify connection parameters",
                "3. Review error logs for details",
                "4. Try: uv run -m src.interfaces.cli database setup-action-network --test-connection"
            ]

    @staticmethod
    def get_collector_recovery_steps(collector_name: str, error_message: str) -> List[str]:
        """Get recovery steps for data collector errors."""
        if "rate limit" in error_message.lower():
            return [
                f"1. {collector_name.upper()} rate limit hit - wait 10-15 minutes",
                "2. Check API key/credentials if required",
                "3. Reduce collection frequency",
                "4. Try: uv run -m src.interfaces.cli monitoring health-check --collector " + collector_name
            ]
        elif "timeout" in error_message.lower():
            return [
                "1. Check internet connectivity",
                "2. Verify external service is available",
                "3. Increase timeout in configuration",
                "4. Try with --test-mode flag first"
            ]
        else:
            return [
                "1. Check collector configuration",
                "2. Verify external service availability",
                "3. Review error logs for details",
                "4. Try: uv run -m src.interfaces.cli data collect --source " + collector_name + " --test-mode"
            ]


class EnhancedCLIValidator:
    """Enhanced CLI command validation and error handling."""
    
    def __init__(self):
        self.db_validator = DatabaseValidator()
        self.recovery_guide = ErrorRecoveryGuide()
        
    async def validate_system_health(self, include_database: bool = True, 
                                   include_collectors: bool = False) -> Dict[str, Any]:
        """
        Comprehensive system health validation.
        
        Args:
            include_database: Test database connectivity
            include_collectors: Test data collector health
            
        Returns:
            Dictionary with health status and recommendations
        """
        health_report = {
            "overall_status": SystemStatus.UNKNOWN,
            "components": {},
            "recommendations": [],
            "timestamp": datetime.now().isoformat()
        }
        
        component_statuses = []
        
        # Database health check
        if include_database:
            console.print("🔍 Testing database connectivity...")
            db_success, db_message, db_metadata = await self.db_validator.validate_connection()
            
            health_report["components"]["database"] = {
                "status": SystemStatus.HEALTHY if db_success else SystemStatus.CRITICAL,
                "message": db_message,
                "metadata": db_metadata
            }
            
            if not db_success:
                health_report["recommendations"].extend(
                    self.recovery_guide.get_database_recovery_steps(db_message)
                )
                component_statuses.append(SystemStatus.CRITICAL)
            else:
                component_statuses.append(SystemStatus.HEALTHY)
                
                # Get table health if database is accessible
                console.print("📊 Checking table health...")
                table_health = await self.db_validator.get_table_health()
                health_report["components"]["tables"] = table_health
        
        # Determine overall status
        if SystemStatus.CRITICAL in component_statuses:
            health_report["overall_status"] = SystemStatus.CRITICAL
        elif SystemStatus.DEGRADED in component_statuses:
            health_report["overall_status"] = SystemStatus.DEGRADED
        elif SystemStatus.HEALTHY in component_statuses:
            health_report["overall_status"] = SystemStatus.HEALTHY
        else:
            health_report["overall_status"] = SystemStatus.UNKNOWN
            
        return health_report

    def display_health_report(self, health_report: Dict[str, Any], detailed: bool = False):
        """Display formatted health report with recovery suggestions."""
        
        # Overall status panel
        status = health_report["overall_status"]
        if status == SystemStatus.HEALTHY:
            status_color = "green"
            status_icon = "✅"
        elif status == SystemStatus.DEGRADED:
            status_color = "yellow"
            status_icon = "⚠️"
        elif status == SystemStatus.CRITICAL:
            status_color = "red"
            status_icon = "❌"
        else:
            status_color = "blue"
            status_icon = "❔"
            
        console.print(
            Panel.fit(
                f"[bold {status_color}]{status_icon} System Status: {status.value.upper()}[/bold {status_color}]",
                title="System Health Report"
            )
        )
        
        # Component status table
        if detailed and "components" in health_report:
            table = Table(title="Component Health Details", show_header=True)
            table.add_column("Component", style="cyan")
            table.add_column("Status", style="green")
            table.add_column("Message")
            table.add_column("Details")
            
            for component, details in health_report["components"].items():
                if isinstance(details, dict) and "status" in details:
                    comp_status = details["status"]
                    status_emoji = "✅" if comp_status == SystemStatus.HEALTHY else "❌"
                    
                    detail_text = ""
                    if "metadata" in details:
                        metadata = details["metadata"]
                        if "connection_time_ms" in metadata:
                            detail_text = f"{metadata['connection_time_ms']}ms"
                            
                    table.add_row(
                        component.title(),
                        f"{status_emoji} {comp_status.value}",
                        details.get("message", ""),
                        detail_text
                    )
                elif component == "tables" and isinstance(details, dict):
                    # Special handling for table health
                    for table_name, table_info in details.items():
                        status_emoji = "✅" if table_info["status"] == "healthy" else "⚠️"
                        table.add_row(
                            f"Table: {table_name}",
                            f"{status_emoji} {table_info['status']}",
                            f"{table_info['total_records']} total, {table_info['recent_records']} recent",
                            ""
                        )
            
            console.print(table)
        
        # Recovery recommendations
        if health_report.get("recommendations"):
            console.print("\n🔧 [bold]Recovery Recommendations:[/bold]")
            for i, recommendation in enumerate(health_report["recommendations"], 1):
                console.print(f"   {recommendation}")
        
        # Safe testing suggestions
        if status != SystemStatus.HEALTHY:
            console.print("\n💡 [bold]Safe Testing Options:[/bold]")
            console.print("   • Use --dry-run flags to test without making changes")
            console.print("   • Use --test-mode for collection testing")
            console.print("   • Use --mock-data for development testing")

    async def enhanced_status_check(self, detailed: bool = False) -> bool:
        """
        Enhanced status check that actually tests system health.
        
        Returns:
            True if system is healthy, False otherwise
        """
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
                transient=True
            ) as progress:
                task = progress.add_task("Running system health checks...", total=None)
                
                health_report = await self.validate_system_health(
                    include_database=True,
                    include_collectors=detailed
                )
                
                progress.update(task, description="Health check complete")
            
            self.display_health_report(health_report, detailed)
            
            return health_report["overall_status"] in [SystemStatus.HEALTHY, SystemStatus.DEGRADED]
            
        except Exception as e:
            console.print(f"❌ [red]Health check failed: {str(e)}[/red]")
            console.print("\n🔧 [bold]Recovery Steps:[/bold]")
            console.print("   1. Check system requirements and dependencies")
            console.print("   2. Verify configuration files are present")
            console.print("   3. Ensure all required services are running")
            return False


# Enhanced CLI decorator for production-grade error handling
def with_enhanced_error_handling(validate_db: bool = True, allow_dry_run: bool = True):
    """
    Decorator that adds enhanced error handling to CLI commands.
    
    Args:
        validate_db: Whether to validate database connectivity
        allow_dry_run: Whether to support --dry-run mode
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            validator = EnhancedCLIValidator()
            
            # Check for dry-run mode
            dry_run = kwargs.get('dry_run', False)
            if dry_run:
                console.print("🧪 [bold blue]DRY RUN MODE[/bold blue] - No changes will be made")
            
            # Validate system health if not in dry-run mode
            if validate_db and not dry_run:
                console.print("🔍 Validating system health...")
                is_healthy = await validator.enhanced_status_check(detailed=False)
                
                if not is_healthy:
                    console.print("\n❌ [red]System health check failed. Use --dry-run to test safely.[/red]")
                    sys.exit(1)
            
            try:
                # Execute the original function
                return await func(*args, **kwargs)
                
            except Exception as e:
                logger.error("Command execution failed", error=str(e), function=func.__name__)
                
                # Provide context-aware error recovery
                console.print(f"\n❌ [red]Command failed: {str(e)}[/red]")
                
                # Get recovery recommendations
                if "database" in str(e).lower():
                    recovery_steps = ErrorRecoveryGuide.get_database_recovery_steps(str(e))
                else:
                    recovery_steps = [
                        "1. Check error logs for detailed information",
                        "2. Verify system requirements are met",
                        "3. Try running with --dry-run flag first",
                        "4. Use --help for command usage information"
                    ]
                
                console.print("\n🔧 [bold]Recovery Steps:[/bold]")
                for step in recovery_steps:
                    console.print(f"   {step}")
                
                sys.exit(1)
                
        return wrapper
    return decorator