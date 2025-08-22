"""
Production Readiness CLI Commands

CLI commands for production deployment validation and health monitoring.
Addresses Issue #38: System Reliability Issues Prevent Production Use
"""

import asyncio
import click
from datetime import datetime
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ....services.monitoring.health_check_service import HealthCheckService, HealthStatus

console = Console()


@click.group()
def production():
    """🚀 Production readiness and deployment validation commands."""
    pass


@production.command()
@click.option('--detailed', is_flag=True, help='Include detailed service diagnostics')
@click.option('--json', 'output_json', is_flag=True, help='Output results in JSON format')
def health(detailed: bool, output_json: bool):
    """Check comprehensive system health status."""
    
    async def run_health_check():
        health_service = HealthCheckService()
        system_health = await health_service.get_system_health(include_detailed=detailed)
        
        if output_json:
            import json
            print(json.dumps(system_health.dict(), indent=2, default=str))
            return
        
        # Display results with Rich formatting
        status_color = "green" if system_health.status == HealthStatus.HEALTHY else "red"
        status_icon = "✅" if system_health.status == HealthStatus.HEALTHY else "❌"
        
        console.print(
            Panel.fit(
                f"[bold {status_color}]{status_icon} System Status: {system_health.status.value.upper()}[/bold {status_color}]",
                title="🏥 System Health Report"
            )
        )
        
        # Service details table
        table = Table(title="📊 Service Health Details", show_header=True)
        table.add_column("Service", style="cyan", width=20)
        table.add_column("Status", width=12)
        table.add_column("Response Time", width=15)
        table.add_column("Message", style="white")
        
        for service in system_health.services:
            if service.status == HealthStatus.HEALTHY:
                status_display = "[green]✅ HEALTHY[/green]"
            elif service.status == HealthStatus.DEGRADED:
                status_display = "[yellow]⚠️ DEGRADED[/yellow]"
            else:
                status_display = "[red]❌ UNHEALTHY[/red]"
            
            table.add_row(
                service.name.replace("_", " ").title(),
                status_display,
                f"{service.response_time_ms:.1f}ms",
                service.message
            )
        
        console.print("\n")
        console.print(table)
        
        # Performance summary
        console.print(f"\n📈 [bold]Performance Summary[/bold]")
        console.print(f"  • Overall Response Time: {system_health.overall_response_time_ms:.1f}ms")
        console.print(f"  • Average Service Response: {system_health.performance_metrics['avg_response_time_ms']:.1f}ms")
        console.print(f"  • Maximum Service Response: {system_health.performance_metrics['max_response_time_ms']:.1f}ms")
        
        if system_health.error_summary:
            console.print(f"  • Services with Errors: {len(system_health.error_summary)}")
        
        if system_health.status == HealthStatus.HEALTHY:
            console.print("\n🎉 [bold green]All systems operational![/bold green]")
        else:
            console.print(f"\n⚠️ [bold yellow]System requires attention[/bold yellow]")
    
    asyncio.run(run_health_check())


@production.command()
@click.option('--service', help='Check health of specific service')
def status(service: str):
    """Quick system status check."""
    
    async def run_status_check():
        health_service = HealthCheckService()
        
        if service:
            service_health = await health_service.get_service_health(service)
            if service_health:
                status_icon = "✅" if service_health.status == HealthStatus.HEALTHY else "❌"
                console.print(f"{status_icon} {service_health.name}: {service_health.status.value} - {service_health.message}")
            else:
                console.print(f"❌ Service '{service}' not found")
        else:
            is_healthy = await health_service.is_system_healthy()
            status_icon = "✅" if is_healthy else "❌"
            status_text = "HEALTHY" if is_healthy else "UNHEALTHY"
            console.print(f"{status_icon} System Status: {status_text}")
    
    asyncio.run(run_status_check())


@production.command()
@click.option('--environment', default='production', help='Target environment')
@click.option('--skip-non-critical', is_flag=True, help='Skip non-critical validation checks')
@click.option('--output-file', help='Save validation results to file')
def validate(environment: str, skip_non_critical: bool, output_file: str):
    """Run comprehensive production deployment validation."""
    
    async def run_validation():
        # Import here to avoid circular dependencies
        import sys
        from pathlib import Path
        
        # Add utilities to path
        utilities_path = Path(__file__).parent.parent.parent.parent.parent / "utilities"
        sys.path.insert(0, str(utilities_path))
        
        from production_deployment_validator import ProductionDeploymentValidator
        
        validator = ProductionDeploymentValidator(environment)
        success = await validator.run_validation(skip_non_critical)
        
        if output_file:
            with open(output_file, 'w') as f:
                f.write(f"Deployment Validation Results - {datetime.now().isoformat()}\n")
                f.write(f"Environment: {environment}\n")
                f.write(f"Overall Success: {success}\n\n")
                
                for check in validator.checks:
                    f.write(f"{check.name}: {check.result.value if check.result else 'UNKNOWN'} - {check.message}\n")
            
            console.print(f"\n📝 Results saved to: {output_file}")
        
        return success
    
    success = asyncio.run(run_validation())
    
    if not success:
        console.print("\n🚨 [bold red]Deployment validation failed. Please address critical issues before deploying.[/bold red]")
        exit(1)


@production.command()
def requirements():
    """Display production deployment requirements checklist."""
    
    console.print(Panel.fit(
        """
🚀 [bold]Production Deployment Requirements[/bold]

[yellow]Infrastructure Requirements:[/yellow]
  ✓ PostgreSQL 15+ database server
  ✓ Python 3.11+ runtime environment
  ✓ UV package manager installed
  ✓ Redis server (for ML feature store)
  ✓ Sufficient disk space (minimum 10GB)
  ✓ Network connectivity to external APIs

[yellow]Environment Variables:[/yellow]
  ✓ DB_PASSWORD - Database authentication
  ✓ DB_HOST - Database host (default: localhost)
  ✓ DB_PORT - Database port (default: 5433)
  ✓ DB_USER - Database username
  ✓ DB_NAME - Database name
  ✓ PYTHONPATH - Python path configuration

[yellow]Security Requirements:[/yellow]
  ✓ Change default database passwords
  ✓ Secure file permissions on config files
  ✓ Network firewall configuration
  ✓ API key management and rotation
  ✓ Audit logging enabled

[yellow]Performance Targets:[/yellow]
  ✓ Database response time <1000ms
  ✓ API response time <2000ms
  ✓ Error rate <1%
  ✓ System uptime >99%

[yellow]Monitoring Requirements:[/yellow]
  ✓ Health check endpoints enabled
  ✓ Prometheus metrics collection
  ✓ Structured logging configured
  ✓ Error alerting system
  ✓ Performance monitoring dashboards
        """,
        title="📋 Production Readiness Checklist"
    ))


if __name__ == "__main__":
    production()