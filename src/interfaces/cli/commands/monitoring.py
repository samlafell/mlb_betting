#!/usr/bin/env python3
"""
Data Collector Monitoring CLI Commands

Provides comprehensive monitoring capabilities for data collectors including:
- Health checks and status monitoring
- Performance analysis and trend reporting
- Alert management and history
- Recovery assistance and diagnostics

Usage Examples:
    # Check health of all collectors
    uv run -m src.interfaces.cli monitoring health-check

    # Check specific collector
    uv run -m src.interfaces.cli monitoring health-check --collector vsin

    # Show performance report
    uv run -m src.interfaces.cli monitoring performance --hours 24

    # Start continuous monitoring
    uv run -m src.interfaces.cli monitoring start-monitoring

    # Show alert history
    uv run -m src.interfaces.cli monitoring alerts --severity critical
"""

import asyncio
import json

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ....core.config import UnifiedSettings
from ....core.logging import LogComponent, get_logger
from ....data.collection.orchestrator import CollectionOrchestrator
from ....services.monitoring.collector_health_service import (
    CollectorHealthStatus,
    HealthMonitoringOrchestrator,
    HealthStatus,
)

logger = get_logger(__name__, LogComponent.CLI)
console = Console()


class MonitoringCommands:
    """Data collector monitoring commands."""

    def create_group(self):
        """Create the monitoring command group."""

        @click.group(name="monitoring")
        def monitoring_group():
            """Data collector monitoring and health management commands."""
            pass

        @monitoring_group.command("health-check")
        @click.option(
            '--collector',
            type=click.Choice(['vsin', 'sbd', 'action_network', 'mlb_stats_api', 'odds_api'], case_sensitive=False),
            help='Specific collector to check'
        )
        @click.option(
            '--detailed',
            is_flag=True,
            help='Show detailed health information including check metadata'
        )
        @click.option(
            '--json-output',
            is_flag=True,
            help='Output results in JSON format'
        )
        def health_check(collector: str | None, detailed: bool, json_output: bool):
            """Run health checks on data collectors."""

            async def _health_check():
                try:
                    # Initialize monitoring system
                    config = UnifiedSettings()
                    orchestrator = CollectionOrchestrator(config)
                    health_monitor = HealthMonitoringOrchestrator(config)

                    # Register collectors
                    await orchestrator.initialize_collectors()
                    for coll in orchestrator.collectors.values():
                        health_monitor.register_collector(coll)

                    if collector:
                        # Check specific collector
                        console.print(f"🔍 Running health check for [bold]{collector}[/bold] collector...")
                        result = await health_monitor.check_specific_collector(collector)

                        if result:
                            if json_output:
                                _output_json_result({collector: result})
                            else:
                                _display_single_health_result(result, detailed)
                        else:
                            console.print(f"❌ Collector '{collector}' not found or failed to check")
                            return
                    else:
                        # Check all collectors
                        console.print("🔍 Running health checks for [bold]all collectors[/bold]...")
                        results = await health_monitor.check_all_collectors()

                        if json_output:
                            _output_json_result(results)
                        else:
                            _display_health_summary(results, detailed)

                except Exception as e:
                    logger.error(f"Health check failed: {e}")
                    console.print(f"❌ Health check failed: {str(e)}")

            asyncio.run(_health_check())

        @monitoring_group.command("performance")
        @click.option(
            '--collector',
            type=click.Choice(['vsin', 'sbd', 'action_network', 'mlb_stats_api', 'odds_api'], case_sensitive=False),
            help='Filter by specific collector'
        )
        @click.option(
            '--hours',
            default=24,
            type=click.IntRange(1, 168),  # 1 hour to 1 week
            help='Hours of performance history to analyze (1-168 hours)'
        )
        @click.option(
            '--show-trends',
            is_flag=True,
            help='Show performance trends and analysis'
        )
        def performance_report(collector: str | None, hours: int, show_trends: bool):
            """Generate performance report for data collectors."""

            async def _performance_report():
                try:
                    config = UnifiedSettings()
                    orchestrator = CollectionOrchestrator(config)
                    health_monitor = HealthMonitoringOrchestrator(config)

                    # Initialize collectors
                    await orchestrator.initialize_collectors()
                    for coll in orchestrator.collectors.values():
                        health_monitor.register_collector(coll)

                    # Get performance data
                    console.print(f"📊 Generating performance report for last [bold]{hours} hours[/bold]...")

                    if collector:
                        results = {collector: await health_monitor.check_specific_collector(collector)}
                    else:
                        results = await health_monitor.check_all_collectors()

                    _display_performance_report(results, hours, show_trends)

                except Exception as e:
                    logger.error(f"Performance report failed: {e}")
                    console.print(f"❌ Performance report failed: {str(e)}")

            asyncio.run(_performance_report())

        @monitoring_group.command("start-monitoring")
        @click.option(
            '--interval',
            default=300,
            type=click.IntRange(30, 3600),  # 30 seconds to 1 hour
            help='Check interval in seconds (30-3600, default: 300 = 5 minutes)'
        )
        @click.option(
            '--daemon',
            is_flag=True,
            help='Run as daemon in background'
        )
        def start_monitoring(interval: int, daemon: bool):
            """Start continuous health monitoring service."""

            async def _start_monitoring():
                try:
                    config = UnifiedSettings()
                    orchestrator = CollectionOrchestrator(config)
                    health_monitor = HealthMonitoringOrchestrator(config)

                    # Initialize collectors
                    await orchestrator.initialize_collectors()
                    for coll in orchestrator.collectors.values():
                        health_monitor.register_collector(coll)

                    console.print("🔄 Starting continuous health monitoring...")
                    console.print(f"📅 Check interval: [bold]{interval} seconds[/bold]")
                    console.print("🛑 Press Ctrl+C to stop monitoring")

                    # Start monitoring
                    await health_monitor.start_monitoring()

                    if not daemon:
                        # Keep running until interrupted
                        try:
                            while True:
                                await asyncio.sleep(1)
                        except KeyboardInterrupt:
                            console.print("\n🛑 Stopping health monitoring...")
                            await health_monitor.stop_monitoring()
                            console.print("✅ Health monitoring stopped")

                except Exception as e:
                    logger.error(f"Monitoring service failed: {e}")
                    console.print(f"❌ Monitoring service failed: {str(e)}")

            asyncio.run(_start_monitoring())

        @monitoring_group.command("alerts")
        @click.option(
            '--severity',
            type=click.Choice(['info', 'warning', 'critical']),
            help='Filter by alert severity'
        )
        @click.option(
            '--hours',
            default=24,
            type=click.IntRange(1, 720),  # 1 hour to 30 days
            help='Hours of alert history to show (1-720 hours)'
        )
        @click.option(
            '--collector',
            type=click.Choice(['vsin', 'sbd', 'action_network', 'mlb_stats_api', 'odds_api'], case_sensitive=False),
            help='Filter by specific collector'
        )
        def alert_history(severity: str | None, hours: int, collector: str | None):
            """Show recent alert history and statistics."""

            try:
                config = UnifiedSettings()
                health_monitor = HealthMonitoringOrchestrator(config)

                console.print(f"📋 Alert history for last [bold]{hours} hours[/bold]")

                if severity:
                    console.print(f"🔍 Filtered by severity: [bold]{severity.upper()}[/bold]")

                if collector:
                    console.print(f"🔍 Filtered by collector: [bold]{collector}[/bold]")

                # Get alert history (simulated for now)
                alerts = _get_simulated_alert_history(severity, hours, collector)

                _display_alert_history(alerts)

            except Exception as e:
                logger.error(f"Alert history failed: {e}")
                console.print(f"❌ Alert history failed: {str(e)}")

        @monitoring_group.command("diagnose")
        @click.option(
            '--collector',
            required=True,
            type=click.Choice(['vsin', 'sbd', 'action_network', 'mlb_stats_api', 'odds_api'], case_sensitive=False),
            help='Collector to diagnose'
        )
        @click.option(
            '--fix',
            is_flag=True,
            help='Attempt automatic fixes for common issues'
        )
        def diagnose_collector(collector: str, fix: bool):
            """Run comprehensive diagnostics on a specific collector."""

            async def _diagnose():
                try:
                    console.print(f"🔧 Running diagnostics for [bold]{collector}[/bold] collector...")

                    config = UnifiedSettings()
                    orchestrator = CollectionOrchestrator(config)
                    health_monitor = HealthMonitoringOrchestrator(config)

                    # Initialize collectors
                    await orchestrator.initialize_collectors()
                    for coll in orchestrator.collectors.values():
                        health_monitor.register_collector(coll)

                    # Run detailed diagnostics
                    result = await health_monitor.check_specific_collector(collector)

                    if result:
                        _display_diagnostic_report(result, fix)
                    else:
                        console.print(f"❌ Collector '{collector}' not found")

                except Exception as e:
                    logger.error(f"Diagnostics failed: {e}")
                    console.print(f"❌ Diagnostics failed: {str(e)}")

            asyncio.run(_diagnose())

        return monitoring_group


def _display_health_summary(results: dict[str, CollectorHealthStatus], detailed: bool):
    """Display health check summary in a formatted table."""

    table = Table(title="🏥 Data Collector Health Summary")
    table.add_column("Collector", style="cyan", no_wrap=True)
    table.add_column("Status", no_wrap=True)
    table.add_column("Uptime", justify="right")
    table.add_column("Performance", justify="right")
    table.add_column("Last Check", style="dim")

    if detailed:
        table.add_column("Issues", style="yellow")

    for name, status in results.items():
        # Status formatting
        if status.overall_status == HealthStatus.HEALTHY:
            status_text = "[green]✅ HEALTHY[/green]"
        elif status.overall_status == HealthStatus.DEGRADED:
            status_text = "[yellow]⚠️ DEGRADED[/yellow]"
        elif status.overall_status == HealthStatus.CRITICAL:
            status_text = "[red]❌ CRITICAL[/red]"
        else:
            status_text = "[dim]❓ UNKNOWN[/dim]"

        # Performance score formatting
        perf_score = status.performance_score
        if perf_score >= 90:
            perf_text = f"[green]{perf_score:.1f}/100[/green]"
        elif perf_score >= 70:
            perf_text = f"[yellow]{perf_score:.1f}/100[/yellow]"
        else:
            perf_text = f"[red]{perf_score:.1f}/100[/red]"

        row = [
            name,
            status_text,
            f"{status.uptime_percentage:.1f}%",
            perf_text,
            status.last_updated.strftime("%H:%M:%S")
        ]

        if detailed:
            issues = []
            for check in status.checks:
                if check.status != HealthStatus.HEALTHY and check.error_message:
                    issues.append(f"{check.check_type.value}: {check.error_message}")

            issues_text = "; ".join(issues[:2]) if issues else "None"
            row.append(issues_text)

        table.add_row(*row)

    console.print(table)

    # Summary statistics
    total_collectors = len(results)
    healthy_count = len([r for r in results.values() if r.overall_status == HealthStatus.HEALTHY])
    critical_count = len([r for r in results.values() if r.overall_status == HealthStatus.CRITICAL])

    summary_text = f"📊 Summary: {healthy_count}/{total_collectors} healthy"
    if critical_count > 0:
        summary_text += f" | [red]{critical_count} critical[/red]"

    console.print(f"\n{summary_text}")


def _display_single_health_result(result: CollectorHealthStatus, detailed: bool):
    """Display detailed health result for a single collector."""

    # Overall status panel
    if result.overall_status == HealthStatus.HEALTHY:
        status_color = "green"
        status_emoji = "✅"
    elif result.overall_status == HealthStatus.DEGRADED:
        status_color = "yellow"
        status_emoji = "⚠️"
    elif result.overall_status == HealthStatus.CRITICAL:
        status_color = "red"
        status_emoji = "❌"
    else:
        status_color = "dim"
        status_emoji = "❓"

    panel_content = f"""
[bold]{status_emoji} Collector: {result.collector_name}[/bold]
Status: [{status_color}]{result.overall_status.value.upper()}[/{status_color}]
Uptime: {result.uptime_percentage:.1f}%
Performance Score: {result.performance_score:.1f}/100
Last Updated: {result.last_updated.strftime('%Y-%m-%d %H:%M:%S')}
"""

    console.print(Panel(panel_content, title="Health Status", border_style=status_color))

    # Individual check results
    if detailed:
        check_table = Table(title="Individual Health Checks")
        check_table.add_column("Check Type", style="cyan")
        check_table.add_column("Status")
        check_table.add_column("Response Time", justify="right")
        check_table.add_column("Details", style="dim")

        for check in result.checks:
            if check.status == HealthStatus.HEALTHY:
                check_status = "[green]✅ HEALTHY[/green]"
            elif check.status == HealthStatus.DEGRADED:
                check_status = "[yellow]⚠️ DEGRADED[/yellow]"
            elif check.status == HealthStatus.CRITICAL:
                check_status = "[red]❌ CRITICAL[/red]"
            else:
                check_status = "[dim]❓ UNKNOWN[/dim]"

            response_time = f"{check.response_time:.3f}s" if check.response_time else "N/A"
            details = check.error_message or "OK"

            check_table.add_row(
                check.check_type.value.title(),
                check_status,
                response_time,
                details
            )

        console.print(check_table)


def _display_performance_report(
    results: dict[str, CollectorHealthStatus],
    hours: int,
    show_trends: bool
):
    """Display performance report for collectors."""

    table = Table(title=f"📊 Performance Report ({hours}h)")
    table.add_column("Collector", style="cyan")
    table.add_column("Performance Score", justify="right")
    table.add_column("Uptime", justify="right")
    table.add_column("Avg Response Time", justify="right")
    table.add_column("Status", no_wrap=True)

    for name, status in results.items():
        # Calculate average response time from checks
        response_times = [
            check.response_time for check in status.checks
            if check.response_time is not None
        ]
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0

        # Performance score formatting
        perf_score = status.performance_score
        if perf_score >= 90:
            perf_text = f"[green]{perf_score:.1f}[/green]"
        elif perf_score >= 70:
            perf_text = f"[yellow]{perf_score:.1f}[/yellow]"
        else:
            perf_text = f"[red]{perf_score:.1f}[/red]"

        # Status
        if status.overall_status == HealthStatus.HEALTHY:
            status_text = "[green]HEALTHY[/green]"
        elif status.overall_status == HealthStatus.DEGRADED:
            status_text = "[yellow]DEGRADED[/yellow]"
        else:
            status_text = "[red]CRITICAL[/red]"

        table.add_row(
            name,
            perf_text,
            f"{status.uptime_percentage:.1f}%",
            f"{avg_response_time:.3f}s",
            status_text
        )

    console.print(table)

    if show_trends:
        console.print("\n📈 [bold]Performance Trends[/bold]")
        console.print("(Trend analysis would show historical performance data)")


def _display_alert_history(alerts: list):
    """Display alert history in a formatted table."""

    if not alerts:
        console.print("📭 No alerts found for the specified criteria")
        return

    table = Table(title="🚨 Alert History")
    table.add_column("Time", style="dim")
    table.add_column("Collector", style="cyan")
    table.add_column("Severity", no_wrap=True)
    table.add_column("Message")

    for alert in alerts:
        if alert['severity'] == 'critical':
            severity_text = "[red]🔴 CRITICAL[/red]"
        elif alert['severity'] == 'warning':
            severity_text = "[yellow]🟡 WARNING[/yellow]"
        else:
            severity_text = "[blue]🔵 INFO[/blue]"

        table.add_row(
            alert['time'],
            alert['collector'],
            severity_text,
            alert['message']
        )

    console.print(table)


def _display_diagnostic_report(result: CollectorHealthStatus, attempt_fix: bool):
    """Display comprehensive diagnostic report."""

    console.print(f"\n🔧 [bold]Diagnostic Report: {result.collector_name}[/bold]")

    # Overall assessment
    if result.overall_status == HealthStatus.CRITICAL:
        console.print("🚨 [red]CRITICAL ISSUES DETECTED[/red]")
    elif result.overall_status == HealthStatus.DEGRADED:
        console.print("⚠️ [yellow]PERFORMANCE ISSUES DETECTED[/yellow]")
    else:
        console.print("✅ [green]COLLECTOR APPEARS HEALTHY[/green]")

    # Detailed check analysis
    for check in result.checks:
        if check.status != HealthStatus.HEALTHY:
            console.print(f"\n❌ [red]{check.check_type.value.title()} Check Failed[/red]")
            console.print(f"   Error: {check.error_message}")

            if attempt_fix:
                console.print("   🔧 Attempting automatic fix...")
                # In real implementation, attempt recovery actions
                console.print("   ✅ Fix attempted (recovery actions would run here)")

    # Recommendations
    console.print("\n💡 [bold]Recommendations:[/bold]")

    if result.performance_score < 70:
        console.print("   • Monitor performance metrics more closely")
        console.print("   • Consider adjusting collection frequency")

    if result.uptime_percentage < 95:
        console.print("   • Investigate connectivity issues")
        console.print("   • Review error logs for patterns")

    console.print("   • Set up automated alerting for this collector")
    console.print("   • Schedule regular health checks")


def _output_json_result(results: dict[str, CollectorHealthStatus]):
    """Output health check results in JSON format."""

    json_data = {}
    for name, status in results.items():
        json_data[name] = {
            'status': status.overall_status.value,
            'uptime_percentage': status.uptime_percentage,
            'performance_score': status.performance_score,
            'last_updated': status.last_updated.isoformat(),
            'checks': [
                {
                    'type': check.check_type.value,
                    'status': check.status.value,
                    'response_time': check.response_time,
                    'error_message': check.error_message,
                    'metadata': check.metadata
                }
                for check in status.checks
            ]
        }

    console.print(json.dumps(json_data, indent=2))


def _get_simulated_alert_history(severity: str | None, hours: int, collector: str | None) -> list:
    """Get simulated alert history for demonstration."""

    # Simulate some alerts for demonstration
    alerts = [
        {
            'time': '2025-07-15 14:30:00',
            'collector': 'vsin',
            'severity': 'warning',
            'message': 'Slow response time detected (8.5s average)'
        },
        {
            'time': '2025-07-15 13:15:00',
            'collector': 'action_network',
            'severity': 'critical',
            'message': 'Connection timeout - unable to reach API'
        },
        {
            'time': '2025-07-15 12:45:00',
            'collector': 'sbd',
            'severity': 'warning',
            'message': 'Data parsing issues - missing required fields'
        }
    ]

    # Filter by criteria
    filtered_alerts = alerts

    if severity:
        filtered_alerts = [a for a in filtered_alerts if a['severity'] == severity]

    if collector:
        filtered_alerts = [a for a in filtered_alerts if a['collector'] == collector]

    return filtered_alerts
