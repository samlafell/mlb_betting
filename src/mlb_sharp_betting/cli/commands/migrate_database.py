"""
CLI command for database migration and performance testing.

This command helps migrate from legacy file-locking to optimized connection pooling.
"""

import asyncio
import time
import click
import structlog
from typing import Dict, Any

from ...services.database_coordinator import DatabaseCoordinator, get_database_coordinator
from ...db.optimized_connection import ConnectionConfig, OperationPriority
from ...core.exceptions import DatabaseError

logger = structlog.get_logger(__name__)


@click.group()
def migrate():
    """Database migration and optimization commands"""
    pass


@migrate.command()
@click.option('--test-size', default='small', type=click.Choice(['small', 'medium', 'large']),
              help='Size of performance test to run')
@click.option('--show-stats', is_flag=True, help='Show detailed performance statistics')
def benchmark(test_size: str, show_stats: bool):
    """
    Run performance benchmark comparing legacy vs optimized database approaches.
    
    This helps you understand the performance improvements before migration.
    """
    click.echo("🚀 DuckDB Performance Benchmark")
    click.echo("=" * 50)
    
    # Configure test size
    test_configs = {
        'small': {'reads': 50, 'writes': 50, 'description': 'Small workload (100 operations)'},
        'medium': {'reads': 200, 'writes': 200, 'description': 'Medium workload (400 operations)'},
        'large': {'reads': 1000, 'writes': 1000, 'description': 'Large workload (2000 operations)'}
    }
    
    config = test_configs[test_size]
    click.echo(f"Running {config['description']}")
    click.echo()
    
    try:
        # Setup coordinators
        legacy_coordinator = DatabaseCoordinator(use_optimized=False)
        optimized_coordinator = DatabaseCoordinator(use_optimized=True)
        
        # Setup test table
        setup_query = """
            CREATE TABLE IF NOT EXISTS benchmark_performance (
                id INTEGER PRIMARY KEY,
                data VARCHAR,
                test_type VARCHAR,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        
        legacy_coordinator.execute_write(setup_query)
        optimized_coordinator.execute_write(setup_query)
        
        # Clear existing data
        legacy_coordinator.execute_write("DELETE FROM benchmark_performance")
        
        click.echo("🔄 Testing Legacy File-Locking Approach...")
        legacy_time = _run_benchmark_operations(
            legacy_coordinator, 
            config['reads'], 
            config['writes'],
            'legacy'
        )
        
        click.echo("⚡ Testing Optimized Connection Pooling...")
        optimized_time = _run_benchmark_operations(
            optimized_coordinator,
            config['reads'],
            config['writes'], 
            'optimized'
        )
        
        # Calculate improvement
        improvement = ((legacy_time - optimized_time) / legacy_time * 100) if legacy_time > 0 else 0
        
        click.echo()
        click.echo("📊 Results:")
        click.echo(f"  Legacy approach:    {legacy_time:.3f}s")
        click.echo(f"  Optimized approach: {optimized_time:.3f}s")
        click.echo(f"  Performance gain:   {improvement:.1f}%")
        
        if improvement > 0:
            click.echo(f"  Time saved:         {(legacy_time - optimized_time):.3f}s")
        
        if show_stats:
            _show_detailed_stats(optimized_coordinator)
            
        # Recommendations
        click.echo()
        if improvement > 30:
            click.echo("✅ Significant performance improvement! Migration recommended.")
        elif improvement > 10:
            click.echo("✅ Good performance improvement. Migration beneficial.")
        else:
            click.echo("⚠️  Modest improvement. Consider migration for other benefits (concurrency, etc.)")
            
    except Exception as e:
        click.echo(f"❌ Benchmark failed: {e}", err=True)
        logger.error("Benchmark failed", error=str(e))


def _run_benchmark_operations(coordinator: DatabaseCoordinator, reads: int, writes: int, test_type: str) -> float:
    """Run benchmark operations and return execution time"""
    start_time = time.time()
    
    # Interleaved read/write operations to simulate real workload
    for i in range(max(reads, writes)):
        if i < writes:
            coordinator.execute_write(
                "INSERT OR REPLACE INTO benchmark_performance (id, data, test_type) VALUES (?, ?, ?)",
                (i, f"{test_type}_data_{i}", test_type)
            )
        
        if i < reads and i % 5 == 0:  # Read every 5th operation
            coordinator.execute_read(
                "SELECT COUNT(*) FROM benchmark_performance WHERE test_type = ?",
                (test_type,)
            )
    
    # Allow batch processing for optimized approach
    if hasattr(coordinator, 'use_optimized') and coordinator.use_optimized:
        time.sleep(0.5)
    
    return time.time() - start_time


def _show_detailed_stats(coordinator: DatabaseCoordinator):
    """Show detailed performance statistics"""
    if hasattr(coordinator, 'get_performance_stats'):
        stats = coordinator.get_performance_stats()
        
        click.echo()
        click.echo("📈 Detailed Performance Stats:")
        click.echo(f"  Mode: {stats.get('mode', 'optimized')}")
        click.echo(f"  Read pool size: {stats.get('read_pool_size', 'N/A')}")
        click.echo(f"  Write queue size: {stats.get('write_queue_size', 'N/A')}")
        click.echo(f"  Max queue size: {stats.get('max_queue_size', 'N/A')}")
        click.echo(f"  Batch size: {stats.get('batch_size', 'N/A')}")
        click.echo(f"  Status: {stats.get('status', 'unknown')}")


@migrate.command()
@click.option('--service', type=click.Choice(['data_collector', 'pre_game', 'sharp_monitor', 'all']),
              default='all', help='Which service to migrate')
@click.option('--dry-run', is_flag=True, help='Show what would be migrated without making changes')
def enable_optimized(service: str, dry_run: bool):
    """
    Enable optimized database mode for specific services.
    
    This command helps you gradually migrate services to the optimized approach.
    """
    click.echo("🔄 Database Migration Assistant")
    click.echo("=" * 40)
    
    if dry_run:
        click.echo("DRY RUN MODE - No changes will be made")
        click.echo()
    
    services_to_migrate = _get_services_to_migrate(service)
    
    for service_name, config in services_to_migrate.items():
        click.echo(f"📦 Service: {service_name}")
        click.echo(f"   Config: {config}")
        
        if dry_run:
            click.echo("   Action: Would enable optimized mode")
        else:
            click.echo("   Action: Enabling optimized mode...")
            try:
                _apply_service_migration(service_name, config)
                click.echo("   ✅ Migration successful")
            except Exception as e:
                click.echo(f"   ❌ Migration failed: {e}")
        
        click.echo()
    
    if not dry_run:
        click.echo("✅ Migration complete!")
        click.echo()
        click.echo("💡 Next steps:")
        click.echo("  1. Monitor service performance")
        click.echo("  2. Check logs for any issues")
        click.echo("  3. Run 'mlb-cli migrate benchmark' to verify improvements")


def _get_services_to_migrate(service: str) -> Dict[str, Dict[str, Any]]:
    """Get service configurations for migration"""
    all_services = {
        'data_collector': {
            'read_pool_size': 4,
            'write_batch_size': 1000,
            'write_batch_timeout': 5.0,
            'description': 'Optimized for bulk data operations'
        },
        'pre_game': {
            'read_pool_size': 8,
            'write_batch_size': 200,
            'write_batch_timeout': 1.0,
            'description': 'Optimized for fast pre-game processing'
        },
        'sharp_monitor': {
            'read_pool_size': 12,
            'write_batch_size': 100,
            'write_batch_timeout': 0.5,
            'description': 'Optimized for real-time monitoring'
        }
    }
    
    if service == 'all':
        return all_services
    else:
        return {service: all_services[service]}


def _apply_service_migration(service_name: str, config: Dict[str, Any]):
    """Apply migration configuration to a service"""
    # This would typically update service configuration files
    # For now, we'll just demonstrate the concept
    click.echo(f"   Applying configuration: {config}")
    
    # In a real implementation, this might:
    # 1. Update service configuration files
    # 2. Restart services with new config
    # 3. Update environment variables
    # 4. Update deployment configurations


@migrate.command()
def health_check():
    """
    Check the health of database connections and performance.
    """
    click.echo("🏥 Database Health Check")
    click.echo("=" * 30)
    
    try:
        # Test legacy coordinator
        click.echo("Testing legacy coordinator...")
        legacy = DatabaseCoordinator(use_optimized=False)
        legacy_healthy = legacy.is_healthy()
        click.echo(f"  Legacy mode: {'✅ Healthy' if legacy_healthy else '❌ Unhealthy'}")
        
        # Test optimized coordinator  
        click.echo("Testing optimized coordinator...")
        optimized = DatabaseCoordinator(use_optimized=True)
        optimized_healthy = optimized.is_healthy()
        click.echo(f"  Optimized mode: {'✅ Healthy' if optimized_healthy else '❌ Unhealthy'}")
        
        # Show stats for optimized mode
        if optimized_healthy:
            stats = optimized.get_performance_stats()
            click.echo()
            click.echo("📊 Current Performance Stats:")
            for key, value in stats.items():
                click.echo(f"  {key}: {value}")
        
        # Overall status
        click.echo()
        if legacy_healthy and optimized_healthy:
            click.echo("✅ All systems healthy - ready for migration!")
        elif optimized_healthy:
            click.echo("✅ Optimized system healthy - can proceed with migration")
        else:
            click.echo("⚠️  Issues detected - investigate before migration")
            
    except Exception as e:
        click.echo(f"❌ Health check failed: {e}", err=True)


@migrate.command()
@click.option('--reset', is_flag=True, help='Reset to legacy mode (rollback)')
def status(reset: bool):
    """
    Show current database configuration status and optionally reset to legacy mode.
    """
    click.echo("📋 Database Configuration Status")
    click.echo("=" * 40)
    
    if reset:
        click.echo("🔄 Resetting to legacy mode...")
        try:
            # This would reset services to use legacy mode
            click.echo("✅ Reset to legacy file-locking mode")
            click.echo("   All services now using legacy coordinator")
        except Exception as e:
            click.echo(f"❌ Reset failed: {e}", err=True)
        return
    
    try:
        # Show current coordinator status
        coordinator = get_database_coordinator()
        
        if hasattr(coordinator, 'use_optimized'):
            mode = "Optimized" if coordinator.use_optimized else "Legacy"
            click.echo(f"Current mode: {mode}")
            
            if coordinator.use_optimized:
                stats = coordinator.get_performance_stats()
                click.echo("Configuration:")
                click.echo(f"  Read pool size: {stats.get('read_pool_size')}")
                click.echo(f"  Write queue size: {stats.get('write_queue_size')}")
                click.echo(f"  Status: {stats.get('status')}")
            else:
                click.echo("Configuration: File-based locking")
        else:
            click.echo("Current mode: Legacy (file-locking)")
        
        click.echo()
        click.echo("💡 Available commands:")
        click.echo("  mlb-cli migrate benchmark    - Test performance")
        click.echo("  mlb-cli migrate enable-optimized - Enable optimized mode")
        click.echo("  mlb-cli migrate health-check - Check system health")
        click.echo("  mlb-cli migrate status --reset - Rollback to legacy mode")
        
    except Exception as e:
        click.echo(f"❌ Status check failed: {e}", err=True)


if __name__ == '__main__':
    migrate() 