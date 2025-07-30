"""
Resource Management CLI Commands
Commands for managing and monitoring the adaptive resource management system
"""

import asyncio
import json
import time
from typing import Optional
import click
from datetime import datetime

# Import resource management components
try:
    from ...ml.monitoring.adaptive_resource_manager import (
        get_adaptive_resource_manager,
        AllocationStrategy,
        ResourcePriority,
        ResourceQuota,
    )
    from ...ml.monitoring.resource_allocation_controller import (
        get_resource_allocation_controller,
    )
    from ...ml.monitoring.resource_monitor import get_resource_monitor
except ImportError as e:
    print(f"Resource management components not available: {e}")
    get_adaptive_resource_manager = None
    get_resource_allocation_controller = None
    get_resource_monitor = None


@click.group()
def resource_management():
    """Resource management and allocation commands"""
    pass


@resource_management.command()
@click.option("--strategy", type=click.Choice(["conservative", "balanced", "aggressive", "adaptive"]), 
              default="adaptive", help="Resource allocation strategy")
@click.option("--duration", default=30, help="Duration to run in seconds")
def start(strategy: str, duration: int):
    """Start adaptive resource management system"""
    if not get_adaptive_resource_manager:
        click.echo("❌ Resource management system not available")
        return
    
    async def run_resource_management():
        try:
            click.echo(f"🚀 Starting adaptive resource management with {strategy} strategy...")
            
            # Initialize components
            strategy_enum = {
                "conservative": AllocationStrategy.CONSERVATIVE,
                "balanced": AllocationStrategy.BALANCED,
                "aggressive": AllocationStrategy.AGGRESSIVE,
                "adaptive": AllocationStrategy.ADAPTIVE,
            }[strategy]
            
            manager = await get_adaptive_resource_manager(strategy_enum)
            controller = await get_resource_allocation_controller()
            
            # Start resource management
            if not manager._running:
                await manager.start_management()
            
            if not controller._running:
                await controller.start_allocation_control()
            
            click.echo("✅ Resource management system started")
            click.echo(f"📊 Running for {duration} seconds...")
            
            # Monitor for specified duration
            start_time = time.time()
            while time.time() - start_time < duration:
                await asyncio.sleep(5)
                
                # Show current status
                stats = manager.get_management_stats()
                status = controller.get_allocation_status()
                
                click.echo(f"⏱️  {int(time.time() - start_time)}s - "
                          f"Adjustments: {stats['total_adjustments']}, "
                          f"Components: {len(status['registered_components'])}")
            
            # Stop systems
            await manager.stop_management()
            await controller.stop_allocation_control()
            
            click.echo("✅ Resource management system stopped")
            
        except Exception as e:
            click.echo(f"❌ Error running resource management: {e}")
    
    asyncio.run(run_resource_management())


@resource_management.command()
def status():
    """Show current resource management status"""
    if not get_adaptive_resource_manager:
        click.echo("❌ Resource management system not available")
        return
    
    async def show_status():
        try:
            # Get system components
            manager = await get_adaptive_resource_manager()
            controller = await get_resource_allocation_controller()
            
            # Get status information
            manager_stats = manager.get_management_stats()
            controller_status = controller.get_allocation_status()
            resource_summary = manager.get_resource_summary()
            
            click.echo("📊 Resource Management System Status")
            click.echo("=" * 50)
            
            # Manager status
            click.echo(f"🎯 Strategy: {manager_stats['allocation_strategy']}")
            click.echo(f"🔄 Running: {manager._running}")
            click.echo(f"📈 Total Adjustments: {manager_stats['total_adjustments']}")
            click.echo(f"✅ Successful: {manager_stats['successful_adjustments']}")
            click.echo(f"❌ Failed: {manager_stats['failed_adjustments']}")
            
            # Controller status
            click.echo(f"\n🎛️  Controller Status:")
            click.echo(f"   Enabled: {controller_status['allocation_enabled']}")
            click.echo(f"   Running: {controller_status['running']}")
            click.echo(f"   Components: {len(controller_status['registered_components'])}")
            
            # Resource quotas
            click.echo(f"\n💾 Resource Quotas:")
            for component, quota_info in resource_summary['quotas'].items():
                click.echo(f"   {component}:")
                click.echo(f"     Priority: {quota_info['priority']}")
                click.echo(f"     CPU: {quota_info['cpu_percent']:.1f}%")
                click.echo(f"     Memory: {quota_info['memory_mb']:.0f}MB")
                click.echo(f"     Last Adjustment: {quota_info['last_adjustment']}")
            
            # Recent predictions
            if resource_summary['predictions']:
                click.echo(f"\n🔮 Resource Predictions:")
                for component, prediction in resource_summary['predictions'].items():
                    click.echo(f"   {component}:")
                    click.echo(f"     Predicted CPU: {prediction['predicted_cpu_percent']:.1f}%")
                    click.echo(f"     Predicted Memory: {prediction['predicted_memory_mb']:.0f}MB")
                    click.echo(f"     Confidence: {prediction['confidence']:.2f}")
            
            # Recent decisions
            if resource_summary['recent_decisions']:
                click.echo(f"\n📋 Recent Allocation Decisions:")
                for decision in resource_summary['recent_decisions'][-5:]:
                    click.echo(f"   {decision['component']} {decision['resource_type']}: "
                              f"{decision['old_allocation']:.1f} → {decision['new_allocation']:.1f}")
                    click.echo(f"     Rationale: {decision['rationale']}")
                    click.echo(f"     Time: {decision['timestamp']}")
            
        except Exception as e:
            click.echo(f"❌ Error getting status: {e}")
    
    asyncio.run(show_status())


@resource_management.command()
@click.option("--component", help="Component name (optional)")
@click.option("--limit", default=10, help="Number of decisions to show")
def history(component: Optional[str], limit: int):
    """Show resource allocation history"""
    if not get_adaptive_resource_manager:
        click.echo("❌ Resource management system not available")
        return
    
    async def show_history():
        try:
            manager = await get_adaptive_resource_manager()
            controller = await get_resource_allocation_controller()
            
            # Get allocation history
            allocation_history = await manager.get_allocation_history(component)
            controller_history = controller.get_allocation_history(component)
            
            click.echo("📜 Resource Allocation History")
            click.echo("=" * 50)
            
            if allocation_history:
                click.echo(f"🎯 Manager Decisions (Last {min(limit, len(allocation_history))}):")
                for decision in allocation_history[-limit:]:
                    click.echo(f"   {decision.timestamp.strftime('%H:%M:%S')} - "
                              f"{decision.component_name} {decision.resource_type}")
                    click.echo(f"     {decision.old_allocation:.1f} → {decision.new_allocation:.1f}")
                    click.echo(f"     Confidence: {decision.confidence:.2f}")
                    click.echo(f"     Rationale: {decision.rationale}")
                    click.echo()
            
            if controller_history:
                click.echo(f"🎛️  Controller History (Last {min(limit, len(controller_history))}):")
                for entry in controller_history[-limit:]:
                    click.echo(f"   {entry['timestamp']} - {entry['component_name']}")
                    click.echo(f"     Changes: {', '.join(entry['changes'])}")
                    click.echo()
            
            if not allocation_history and not controller_history:
                click.echo("📝 No allocation history found")
            
        except Exception as e:
            click.echo(f"❌ Error getting history: {e}")
    
    asyncio.run(show_history())


@resource_management.command()
@click.argument("component_name")
@click.option("--cpu", type=float, help="CPU allocation percentage")
@click.option("--memory", type=float, help="Memory allocation in MB")
@click.option("--priority", type=click.Choice(["critical", "high", "normal", "low"]), 
              help="Component priority")
def set_quota(component_name: str, cpu: Optional[float], memory: Optional[float], 
              priority: Optional[str]):
    """Set resource quota for a component"""
    if not get_adaptive_resource_manager:
        click.echo("❌ Resource management system not available")
        return
    
    async def set_component_quota():
        try:
            manager = await get_adaptive_resource_manager()
            
            # Get existing quota or create new one
            existing_quota = await manager.get_resource_quota(component_name)
            if existing_quota:
                quota = existing_quota
                click.echo(f"📝 Updating existing quota for {component_name}")
            else:
                quota = ResourceQuota(
                    component_name=component_name,
                    priority=ResourcePriority.NORMAL,
                )
                click.echo(f"🆕 Creating new quota for {component_name}")
            
            # Update quota parameters
            if cpu is not None:
                quota.cpu_current_percent = max(5.0, min(80.0, cpu))
                click.echo(f"   CPU: {quota.cpu_current_percent:.1f}%")
            
            if memory is not None:
                quota.memory_current_mb = max(100.0, min(4096.0, memory))
                click.echo(f"   Memory: {quota.memory_current_mb:.0f}MB")
            
            if priority is not None:
                priority_map = {
                    "critical": ResourcePriority.CRITICAL,
                    "high": ResourcePriority.HIGH,
                    "normal": ResourcePriority.NORMAL,
                    "low": ResourcePriority.LOW,
                }
                quota.priority = priority_map[priority]
                click.echo(f"   Priority: {quota.priority.value}")
            
            # Set the quota
            success = await manager.set_resource_quota(quota)
            if success:
                click.echo("✅ Resource quota updated successfully")
            else:
                click.echo("❌ Failed to update resource quota")
            
        except Exception as e:
            click.echo(f"❌ Error setting quota: {e}")
    
    asyncio.run(set_component_quota())


@resource_management.command()
def monitor():
    """Monitor resource usage in real-time"""
    if not get_resource_monitor:
        click.echo("❌ Resource monitor not available")
        return
    
    async def monitor_resources():
        try:
            resource_monitor = await get_resource_monitor()
            if not resource_monitor._running:
                await resource_monitor.start_monitoring()
            
            click.echo("📊 Real-time Resource Monitoring")
            click.echo("Press Ctrl+C to stop...")
            click.echo("=" * 50)
            
            try:
                while True:
                    metrics = resource_monitor.get_current_metrics()
                    alerts = len(resource_monitor.active_alerts)
                    
                    click.echo(f"\r🖥️  CPU: {metrics.cpu_percent:5.1f}% | "
                              f"💾 Memory: {metrics.memory_percent:5.1f}% | "
                              f"💿 Disk: {metrics.disk_usage_percent:5.1f}% | "
                              f"🚨 Alerts: {alerts}", nl=False)
                    
                    await asyncio.sleep(2)
            
            except KeyboardInterrupt:
                click.echo("\n✅ Monitoring stopped")
            
        except Exception as e:
            click.echo(f"❌ Error monitoring resources: {e}")
    
    asyncio.run(monitor_resources())


@resource_management.command()
@click.option("--output", type=click.Choice(["json", "table"]), default="table", 
              help="Output format")
def export(output: str):
    """Export resource management configuration and statistics"""
    if not get_adaptive_resource_manager:
        click.echo("❌ Resource management system not available")
        return
    
    async def export_data():
        try:
            manager = await get_adaptive_resource_manager()
            controller = await get_resource_allocation_controller()
            
            # Collect all data
            export_data = {
                "timestamp": datetime.utcnow().isoformat(),
                "manager_stats": manager.get_management_stats(),
                "controller_status": controller.get_allocation_status(),
                "resource_summary": manager.get_resource_summary(),
                "allocation_history": [
                    {
                        "component": d.component_name,
                        "resource_type": d.resource_type,
                        "old_allocation": d.old_allocation,
                        "new_allocation": d.new_allocation,
                        "rationale": d.rationale,
                        "confidence": d.confidence,
                        "timestamp": d.timestamp.isoformat(),
                    }
                    for d in await manager.get_allocation_history()
                ],
            }
            
            if output == "json":
                click.echo(json.dumps(export_data, indent=2, default=str))
            else:
                # Table output
                click.echo("📋 Resource Management Export")
                click.echo("=" * 50)
                click.echo(f"Export Time: {export_data['timestamp']}")
                click.echo(f"Strategy: {export_data['manager_stats']['allocation_strategy']}")
                click.echo(f"Total Adjustments: {export_data['manager_stats']['total_adjustments']}")
                click.echo(f"Active Components: {len(export_data['controller_status']['registered_components'])}")
                click.echo(f"History Entries: {len(export_data['allocation_history'])}")
            
        except Exception as e:
            click.echo(f"❌ Error exporting data: {e}")
    
    asyncio.run(export_data())


@resource_management.command()
def test():
    """Run resource management system tests"""
    if not get_adaptive_resource_manager:
        click.echo("❌ Resource management system not available")
        return
    
    async def run_tests():
        try:
            click.echo("🧪 Running Resource Management Tests")
            click.echo("=" * 40)
            
            # Test 1: Manager initialization
            click.echo("1️⃣  Testing manager initialization...")
            manager = await get_adaptive_resource_manager()
            click.echo(f"   ✅ Manager initialized with {len(manager.resource_quotas)} quotas")
            
            # Test 2: Controller initialization
            click.echo("2️⃣  Testing controller initialization...")
            controller = await get_resource_allocation_controller()
            click.echo(f"   ✅ Controller initialized")
            
            # Test 3: Resource monitor
            click.echo("3️⃣  Testing resource monitor...")
            try:
                monitor = await get_resource_monitor()
                metrics = monitor.get_current_metrics()
                click.echo(f"   ✅ Resource monitor working - CPU: {metrics.cpu_percent:.1f}%")
            except Exception as e:
                click.echo(f"   ⚠️  Resource monitor issue: {e}")
            
            # Test 4: Basic allocation
            click.echo("4️⃣  Testing basic allocation...")
            quota = ResourceQuota(
                component_name="test_component",
                priority=ResourcePriority.NORMAL,
                cpu_current_percent=30.0,
                memory_current_mb=500.0,
            )
            success = await manager.set_resource_quota(quota)
            if success:
                retrieved = await manager.get_resource_quota("test_component")
                if retrieved and retrieved.cpu_current_percent == 30.0:
                    click.echo("   ✅ Basic allocation working")
                else:
                    click.echo("   ❌ Allocation retrieval failed")
            else:
                click.echo("   ❌ Allocation setting failed")
            
            # Test 5: Statistics
            click.echo("5️⃣  Testing statistics collection...")
            stats = manager.get_management_stats()
            status = controller.get_allocation_status()
            if stats and status:
                click.echo("   ✅ Statistics collection working")
            else:
                click.echo("   ❌ Statistics collection failed")
            
            click.echo("\n🎉 All tests completed!")
            
        except Exception as e:
            click.echo(f"❌ Test error: {e}")
    
    asyncio.run(run_tests())


# Register the command group
if __name__ == "__main__":
    resource_management()