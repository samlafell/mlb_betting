#!/usr/bin/env python3
"""
Collection Health Monitoring CLI Commands

Provides CLI interface for monitoring and managing collection health.
Part of solution for GitHub Issue #36: "Data Collection Fails Silently"
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import click
import asyncpg
from tabulate import tabulate

from ....core.config import get_settings
from ....data.collection.alert_manager import CollectionAlertManager
from ....data.collection.enhanced_orchestrator import EnhancedCollectionOrchestrator
from ....data.collection.circuit_breaker import circuit_breaker_manager
from ....data.collection.health_monitoring import HealthStatus, AlertSeverity


@click.group()
def health():
    """Collection health monitoring and management commands."""
    pass


@health.command()
@click.option('--source', help='Specific source to check (default: all)')
@click.option('--format', 'output_format', type=click.Choice(['table', 'json']), default='table', help='Output format')
@click.option('--detailed', is_flag=True, help='Show detailed health information')
def status(source: Optional[str], output_format: str, detailed: bool):
    """Check current health status of data collection sources."""
    
    async def get_health_status():
        orchestrator = EnhancedCollectionOrchestrator()
        
        try:
            # Get enhanced metrics
            metrics = orchestrator.get_enhanced_metrics()
            
            if source:
                # Filter for specific source
                if source in metrics.get('health_monitoring', {}).get('source_health', {}):
                    source_health = {source: metrics['health_monitoring']['source_health'][source]}
                else:
                    click.echo(f"❌ Source '{source}' not found")
                    return
            else:
                source_health = metrics.get('health_monitoring', {}).get('source_health', {})
            
            if output_format == 'json':
                click.echo(json.dumps(source_health, indent=2, default=str))
            else:
                _display_health_status_table(source_health, detailed)
                
        except Exception as e:
            click.echo(f"❌ Error getting health status: {e}")
        finally:
            await orchestrator.cleanup()
    
    asyncio.run(get_health_status())


@health.command()
@click.option('--hours', default=24, help='Hours to look back for gaps (default: 24)')
@click.option('--threshold', default=4.0, help='Gap threshold in hours (default: 4.0)')
@click.option('--format', 'output_format', type=click.Choice(['table', 'json']), default='table', help='Output format')
def gaps(hours: int, threshold: float, output_format: str):
    """Check for collection gaps that might indicate silent failures."""
    
    async def check_gaps():
        alert_manager = CollectionAlertManager()
        
        try:
            await alert_manager.initialize_db_connection()
            
            # Check gaps for all sources
            sources = ['action_network', 'vsin', 'sbd', 'sports_book_review', 'mlb_stats_api']
            gap_alerts = []
            
            for source in sources:
                gap_alert = await alert_manager.check_collection_gaps(source, threshold)
                if gap_alert:
                    gap_alerts.append(gap_alert)
            
            if output_format == 'json':
                gap_data = []
                for alert in gap_alerts:
                    gap_data.append({
                        'source': alert.source,
                        'gap_hours': alert.metadata.get('gap_hours', 0),
                        'last_collection': alert.metadata.get('last_collection'),
                        'severity': alert.severity.value,
                        'message': alert.message
                    })
                click.echo(json.dumps(gap_data, indent=2, default=str))
            else:
                _display_gaps_table(gap_alerts)
                
        except Exception as e:
            click.echo(f"❌ Error checking gaps: {e}")
    
    asyncio.run(check_gaps())


@health.command()
@click.option('--threshold', default=0.5, help='Dead tuple ratio threshold (default: 0.5)')
@click.option('--format', 'output_format', type=click.Choice(['table', 'json']), default='table', help='Output format')
def dead_tuples(threshold: float, output_format: str):
    """Check for database dead tuple accumulation."""
    
    async def check_dead_tuples():
        alert_manager = CollectionAlertManager()
        
        try:
            await alert_manager.initialize_db_connection()
            
            # Check dead tuple accumulation
            dead_tuple_alerts = await alert_manager.check_dead_tuple_accumulation(threshold)
            
            if output_format == 'json':
                alerts_data = []
                for alert in dead_tuple_alerts:
                    alerts_data.append({
                        'table': alert.source,
                        'dead_tuple_ratio': alert.metadata.get('dead_tuple_ratio', 0),
                        'live_tuples': alert.metadata.get('live_tuples', 0),
                        'dead_tuples': alert.metadata.get('dead_tuples', 0),
                        'severity': alert.severity.value,
                        'message': alert.message
                    })
                click.echo(json.dumps(alerts_data, indent=2, default=str))
            else:
                _display_dead_tuples_table(dead_tuple_alerts)
                
        except Exception as e:
            click.echo(f"❌ Error checking dead tuples: {e}")
    
    asyncio.run(check_dead_tuples())


@health.command()
@click.option('--source', help='Specific source to check (default: all)')
@click.option('--format', 'output_format', type=click.Choice(['table', 'json']), default='table', help='Output format')
def circuit_breakers(source: Optional[str], output_format: str):
    """Check circuit breaker status for data collection sources."""
    
    def get_circuit_breaker_status():
        try:
            status = circuit_breaker_manager.get_all_status()
            
            if source:
                if source in status:
                    status = {source: status[source]}
                else:
                    click.echo(f"❌ Circuit breaker for '{source}' not found")
                    return
            
            if output_format == 'json':
                click.echo(json.dumps(status, indent=2, default=str))
            else:
                _display_circuit_breakers_table(status)
                
        except Exception as e:
            click.echo(f"❌ Error getting circuit breaker status: {e}")
    
    get_circuit_breaker_status()


@health.command()
@click.option('--active-only', is_flag=True, help='Show only active alerts')
@click.option('--severity', type=click.Choice(['info', 'warning', 'critical']), help='Filter by severity')
@click.option('--source', help='Filter by source')
@click.option('--format', 'output_format', type=click.Choice(['table', 'json']), default='table', help='Output format')
def alerts(active_only: bool, severity: Optional[str], source: Optional[str], output_format: str):
    """View current collection alerts."""
    
    async def get_alerts():
        alert_manager = CollectionAlertManager()
        
        try:
            await alert_manager.initialize_db_connection()
            
            # Get active alerts
            active_alerts = alert_manager.get_active_alerts(source)
            
            # Filter by severity if specified
            if severity:
                severity_enum = AlertSeverity(severity)
                active_alerts = [a for a in active_alerts if a.severity == severity_enum]
            
            if output_format == 'json':
                alerts_data = []
                for alert in active_alerts:
                    alerts_data.append({
                        'id': alert.id,
                        'source': alert.source,
                        'alert_type': alert.alert_type,
                        'severity': alert.severity.value,
                        'message': alert.message,
                        'created_at': alert.created_at.isoformat(),
                        'is_active': alert.is_active,
                        'is_auto_recoverable': alert.is_auto_recoverable
                    })
                click.echo(json.dumps(alerts_data, indent=2, default=str))
            else:
                _display_alerts_table(active_alerts)
                
        except Exception as e:
            click.echo(f"❌ Error getting alerts: {e}")
    
    asyncio.run(get_alerts())


@health.command()
@click.argument('alert_id')
@click.option('--notes', help='Resolution notes')
def resolve_alert(alert_id: str, notes: Optional[str]):
    """Resolve a specific alert."""
    
    async def resolve():
        alert_manager = CollectionAlertManager()
        
        try:
            success = await alert_manager.resolve_alert(alert_id, notes or "Resolved via CLI")
            
            if success:
                click.echo(f"✅ Alert {alert_id} resolved successfully")
            else:
                click.echo(f"❌ Alert {alert_id} not found or already resolved")
                
        except Exception as e:
            click.echo(f"❌ Error resolving alert: {e}")
    
    asyncio.run(resolve())


@health.command()
@click.option('--source', required=True, help='Source to test')
def test_connection(source: str):
    """Test connection health for a specific source."""
    
    async def test():
        orchestrator = EnhancedCollectionOrchestrator()
        
        try:
            result = await orchestrator._test_source_health(source)
            
            if result:
                click.echo(f"✅ {source}: Connection healthy")
            else:
                click.echo(f"❌ {source}: Connection failed")
                
        except Exception as e:
            click.echo(f"❌ Error testing {source}: {e}")
        finally:
            await orchestrator.cleanup()
    
    asyncio.run(test())


@health.command()
@click.option('--source', required=True, help='Source to reset circuit breaker for')
def reset_circuit_breaker(source: str):
    """Reset circuit breaker for a specific source."""
    
    async def reset():
        try:
            circuit_breaker = circuit_breaker_manager.get_circuit_breaker(source)
            
            if circuit_breaker:
                await circuit_breaker.reset()
                click.echo(f"✅ Circuit breaker for {source} reset successfully")
            else:
                click.echo(f"❌ Circuit breaker for {source} not found")
                
        except Exception as e:
            click.echo(f"❌ Error resetting circuit breaker: {e}")
    
    asyncio.run(reset())


@health.command()
@click.option('--days', default=7, help='Days of history to show (default: 7)')
@click.option('--source', help='Specific source to show (default: all)')
def history(days: int, source: Optional[str]):
    """Show collection health history."""
    
    async def get_history():
        settings = get_settings()
        
        try:
            dsn = f"postgresql://{settings.database.user}:{settings.database.password}@{settings.database.host}:{settings.database.port}/{settings.database.database}"
            conn = await asyncpg.connect(dsn)
            
            # Query health history
            query = """
            SELECT 
                source,
                DATE(collection_timestamp) as date,
                AVG(success_rate) as avg_success_rate,
                AVG(confidence_score) as avg_confidence_score,
                MAX(consecutive_failures) as max_failures,
                COUNT(*) as total_checks
            FROM operational.collection_health_monitoring
            WHERE collection_timestamp > NOW() - INTERVAL '%s days'
            """ % days
            
            if source:
                query += f" AND source = '{source}'"
            
            query += """
            GROUP BY source, DATE(collection_timestamp)
            ORDER BY source, date DESC
            """
            
            results = await conn.fetch(query)
            
            if results:
                _display_history_table(results)
            else:
                click.echo("No health history found")
                
            await conn.close()
            
        except Exception as e:
            click.echo(f"❌ Error getting history: {e}")
    
    asyncio.run(get_history())


def _display_health_status_table(source_health: Dict[str, Any], detailed: bool = False):
    """Display health status in table format."""
    if not source_health:
        click.echo("No health data available")
        return
    
    headers = ['Source', 'Status', 'Success Rate', 'Confidence', 'Failures', 'Gap (hrs)']
    if detailed:
        headers.extend(['Last Success', 'Response Time'])
    
    rows = []
    for source, health in source_health.items():
        status = health.get('health_status', 'unknown')
        status_icon = {
            'healthy': '✅',
            'degraded': '⚠️',
            'critical': '❌',
            'unknown': '❓'
        }.get(status, '❓')
        
        row = [
            source,
            f"{status_icon} {status}",
            f"{health.get('success_rate', 0):.1f}%",
            f"{health.get('confidence_score', 0):.2f}",
            health.get('consecutive_failures', 0),
            f"{health.get('gap_duration_hours', 0):.1f}"
        ]
        
        if detailed:
            last_success = health.get('last_successful_collection')
            if last_success:
                last_success = datetime.fromisoformat(last_success.replace('Z', '+00:00')).strftime('%H:%M')
            else:
                last_success = 'Never'
            
            row.extend([
                last_success,
                f"{health.get('avg_response_time_ms', 0):.0f}ms"
            ])
        
        rows.append(row)
    
    click.echo(tabulate(rows, headers=headers, tablefmt='grid'))


def _display_gaps_table(gap_alerts: List[Any]):
    """Display gap alerts in table format."""
    if not gap_alerts:
        click.echo("✅ No collection gaps detected")
        return
    
    headers = ['Source', 'Gap Duration', 'Last Collection', 'Severity', 'Message']
    rows = []
    
    for alert in gap_alerts:
        gap_hours = alert.metadata.get('gap_hours', 0)
        last_collection = alert.metadata.get('last_collection', 'Unknown')
        if last_collection != 'Unknown':
            try:
                last_collection = datetime.fromisoformat(last_collection.replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M')
            except:
                pass
        
        severity_icon = {'warning': '⚠️', 'critical': '❌', 'info': 'ℹ️'}.get(alert.severity.value, '❓')
        
        rows.append([
            alert.source,
            f"{gap_hours:.1f} hours",
            last_collection,
            f"{severity_icon} {alert.severity.value}",
            alert.message[:50] + '...' if len(alert.message) > 50 else alert.message
        ])
    
    click.echo(tabulate(rows, headers=headers, tablefmt='grid'))


def _display_dead_tuples_table(dead_tuple_alerts: List[Any]):
    """Display dead tuple alerts in table format."""
    if not dead_tuple_alerts:
        click.echo("✅ No dead tuple issues detected")
        return
    
    headers = ['Table', 'Dead Ratio', 'Live Tuples', 'Dead Tuples', 'Severity']
    rows = []
    
    for alert in dead_tuple_alerts:
        ratio = alert.metadata.get('dead_tuple_ratio', 0)
        live_tuples = alert.metadata.get('live_tuples', 0)
        dead_tuples = alert.metadata.get('dead_tuples', 0)
        
        severity_icon = {'warning': '⚠️', 'critical': '❌'}.get(alert.severity.value, '❓')
        
        rows.append([
            alert.source,
            f"{ratio:.1%}",
            f"{live_tuples:,}",
            f"{dead_tuples:,}",
            f"{severity_icon} {alert.severity.value}"
        ])
    
    click.echo(tabulate(rows, headers=headers, tablefmt='grid'))


def _display_circuit_breakers_table(status: Dict[str, Any]):
    """Display circuit breaker status in table format."""
    if not status:
        click.echo("No circuit breakers found")
        return
    
    headers = ['Source', 'State', 'Success Rate', 'Failures', 'Blocked Calls', 'Last Failure']
    rows = []
    
    for source, cb_status in status.items():
        state = cb_status.get('state', 'unknown')
        state_icon = {
            'closed': '✅',
            'open': '❌',
            'half_open': '⚠️'
        }.get(state, '❓')
        
        metrics = cb_status.get('metrics', {})
        last_failure = metrics.get('last_failure_time')
        if last_failure:
            try:
                last_failure = datetime.fromisoformat(last_failure.replace('Z', '+00:00')).strftime('%H:%M')
            except:
                last_failure = 'Invalid'
        else:
            last_failure = 'None'
        
        rows.append([
            source,
            f"{state_icon} {state}",
            f"{metrics.get('success_rate', 0):.1f}%",
            metrics.get('consecutive_failures', 0),
            metrics.get('blocked_calls', 0),
            last_failure
        ])
    
    click.echo(tabulate(rows, headers=headers, tablefmt='grid'))


def _display_alerts_table(alerts: List[Any]):
    """Display alerts in table format."""
    if not alerts:
        click.echo("✅ No active alerts")
        return
    
    headers = ['Source', 'Type', 'Severity', 'Message', 'Created', 'Recoverable']
    rows = []
    
    for alert in alerts:
        severity_icon = {
            'info': 'ℹ️',
            'warning': '⚠️', 
            'critical': '❌'
        }.get(alert.severity.value, '❓')
        
        created_time = alert.created_at.strftime('%H:%M')
        recoverable = '✅' if alert.is_auto_recoverable else '❌'
        
        rows.append([
            alert.source,
            alert.alert_type,
            f"{severity_icon} {alert.severity.value}",
            alert.message[:40] + '...' if len(alert.message) > 40 else alert.message,
            created_time,
            recoverable
        ])
    
    click.echo(tabulate(rows, headers=headers, tablefmt='grid'))


def _display_history_table(results: List[Any]):
    """Display health history in table format."""
    headers = ['Source', 'Date', 'Avg Success Rate', 'Avg Confidence', 'Max Failures', 'Total Checks']
    rows = []
    
    for result in results:
        rows.append([
            result['source'],
            result['date'].strftime('%Y-%m-%d'),
            f"{result['avg_success_rate']:.1f}%",
            f"{result['avg_confidence_score']:.2f}",
            result['max_failures'],
            result['total_checks']
        ])
    
    click.echo(tabulate(rows, headers=headers, tablefmt='grid'))


if __name__ == '__main__':
    health()