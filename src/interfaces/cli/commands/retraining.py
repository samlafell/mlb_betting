"""
CLI Commands for Automated Retraining Workflows

Provides command-line interface for managing automated retraining workflows
including trigger management, job monitoring, schedule configuration, and
model validation.

Integrates with the complete retraining system:
- RetrainingTriggerService
- AutomatedRetrainingEngine  
- ModelValidationService
- PerformanceMonitoringService
- RetrainingScheduler
"""

import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
import click
from tabulate import tabulate

from src.core.config import get_settings
from src.core.datetime_utils import EST
from src.core.logging import LogComponent, get_logger
from src.data.database import UnifiedRepository
from src.analysis.strategies.orchestrator import StrategyOrchestrator
from src.services.retraining import (
    RetrainingTriggerService,
    AutomatedRetrainingEngine,
    ModelValidationService,
    PerformanceMonitoringService,
    RetrainingScheduler,
    TriggerType,
    TriggerSeverity,
    RetrainingStrategy,
    RetrainingConfiguration,
    ValidationLevel,
    ScheduleType,
    SchedulePriority
)


logger = get_logger(__name__, LogComponent.CLI)


@click.group(name="retraining")
def retraining_cli():
    """Automated retraining workflow management"""
    pass


# Trigger Management Commands

@retraining_cli.group("triggers")
def triggers():
    """Manage retraining triggers"""
    pass


@triggers.command("check")
@click.option("--strategy", help="Check triggers for specific strategy")
@click.option("--watch", is_flag=True, help="Watch triggers in real-time")
@click.option("--refresh-seconds", default=30, help="Refresh interval for watch mode")
def check_triggers(strategy: Optional[str], watch: bool, refresh_seconds: int):
    """Check current retraining triggers"""
    
    async def _check_triggers():
        config = get_settings()
        repository = UnifiedRepository(config.database.connection_string)
        
        trigger_service = RetrainingTriggerService(repository)
        
        if strategy:
            triggers = await trigger_service.check_triggers_for_strategy(strategy)
            _display_triggers(triggers, f"Triggers for {strategy}")
        else:
            all_triggers = trigger_service.get_active_triggers()
            _display_triggers(all_triggers, "All Active Triggers")
    
    async def _watch_triggers():
        while True:
            click.clear()
            await _check_triggers()
            if watch:
                click.echo(f"\n🔄 Refreshing in {refresh_seconds} seconds... (Press Ctrl+C to exit)")
                await asyncio.sleep(refresh_seconds)
            else:
                break
    
    asyncio.run(_watch_triggers())


@triggers.command("create")
@click.option("--strategy", required=True, help="Strategy name")
@click.option("--reason", required=True, help="Trigger reason")
@click.option("--severity", default="high", type=click.Choice(["low", "medium", "high", "critical"]),
              help="Trigger severity")
def create_manual_trigger(strategy: str, reason: str, severity: str):
    """Create a manual retraining trigger"""
    
    async def _create_trigger():
        config = get_settings()
        repository = UnifiedRepository(config.database.connection_string)
        
        trigger_service = RetrainingTriggerService(repository)
        
        trigger = await trigger_service.create_manual_trigger(
            strategy_name=strategy,
            reason=reason,
            severity=TriggerSeverity(severity)
        )
        
        click.echo(f"✅ Created manual trigger: {trigger.trigger_id}")
        click.echo(f"   Strategy: {trigger.strategy_name}")
        click.echo(f"   Severity: {trigger.severity.value}")
        click.echo(f"   Reason: {reason}")
    
    asyncio.run(_create_trigger())


@triggers.command("resolve")
@click.argument("trigger_id")
def resolve_trigger(trigger_id: str):
    """Resolve a trigger condition"""
    
    async def _resolve_trigger():
        config = get_settings()
        repository = UnifiedRepository(config.database.connection_string)
        
        trigger_service = RetrainingTriggerService(repository)
        
        success = trigger_service.resolve_trigger(trigger_id)
        
        if success:
            click.echo(f"✅ Resolved trigger: {trigger_id}")
        else:
            click.echo(f"❌ Trigger not found: {trigger_id}")
    
    asyncio.run(_resolve_trigger())


@triggers.command("stats")
def trigger_statistics():
    """Show trigger statistics"""
    
    async def _show_stats():
        config = get_settings()
        repository = UnifiedRepository(config.database.connection_string)
        
        trigger_service = RetrainingTriggerService(repository)
        stats = trigger_service.get_trigger_statistics()
        
        click.echo("📊 Trigger Statistics:")
        click.echo(f"   Active triggers: {stats['active_triggers']}")
        click.echo(f"   Total triggers detected: {stats['total_triggers_detected']}")
        click.echo(f"   Recent triggers (7 days): {stats['recent_triggers_7_days']}")
        click.echo(f"   Monitoring enabled: {'✅' if stats['monitoring_enabled'] else '❌'}")
        click.echo(f"   Strategies monitored: {stats['strategies_monitored']}")
        
        if stats['triggers_by_type']:
            click.echo("\n   Triggers by type:")
            for trigger_type, count in stats['triggers_by_type'].items():
                click.echo(f"     {trigger_type}: {count}")
        
        if stats['triggers_by_strategy']:
            click.echo("\n   Triggers by strategy:")
            for strategy, count in stats['triggers_by_strategy'].items():
                click.echo(f"     {strategy}: {count}")
    
    asyncio.run(_show_stats())


# Job Management Commands

@retraining_cli.group("jobs")
def jobs():
    """Manage retraining jobs"""
    pass


@jobs.command("start")
@click.option("--strategy", required=True, help="Strategy to retrain")
@click.option("--strategy-type", default="full_retraining", 
              type=click.Choice(["full_retraining", "incremental_update", "targeted_optimization"]),
              help="Retraining strategy")
@click.option("--priority", default="normal",
              type=click.Choice(["critical", "high", "normal", "low"]),
              help="Job priority")
@click.option("--max-evaluations", default=50, help="Maximum hyperparameter evaluations")
@click.option("--timeout-hours", default=12, help="Job timeout in hours")
@click.option("--reason", help="Reason for manual retraining")
def start_retraining_job(
    strategy: str,
    strategy_type: str,
    priority: str,
    max_evaluations: int,
    timeout_hours: int,
    reason: Optional[str]
):
    """Start a retraining job"""
    
    async def _start_job():
        config = get_settings()
        repository = UnifiedRepository(config.database.connection_string)
        
        # Initialize services
        trigger_service = RetrainingTriggerService(repository)
        strategy_orchestrator = StrategyOrchestrator(None, repository, {})
        retraining_engine = AutomatedRetrainingEngine(repository, strategy_orchestrator)
        
        await retraining_engine.start_engine()
        
        try:
            # Create manual trigger if reason provided
            trigger_conditions = []
            if reason:
                trigger = await trigger_service.create_manual_trigger(
                    strategy_name=strategy,
                    reason=reason,
                    severity=TriggerSeverity.HIGH if priority in ["critical", "high"] else TriggerSeverity.MEDIUM
                )
                trigger_conditions.append(trigger)
            
            # Configure retraining
            configuration = RetrainingConfiguration(
                max_evaluations=max_evaluations,
                timeout_hours=timeout_hours,
                high_impact_only=True if max_evaluations <= 30 else False
            )
            
            # Start retraining job
            job = await retraining_engine.trigger_retraining(
                strategy_name=strategy,
                trigger_conditions=trigger_conditions,
                retraining_strategy=RetrainingStrategy(strategy_type.upper()),
                configuration=configuration
            )
            
            click.echo(f"✅ Started retraining job: {job.job_id}")
            click.echo(f"   Strategy: {job.strategy_name}")
            click.echo(f"   Type: {job.retraining_strategy.value}")
            click.echo(f"   Max evaluations: {max_evaluations}")
            click.echo(f"   Timeout: {timeout_hours} hours")
            
        finally:
            await retraining_engine.stop_engine()
    
    asyncio.run(_start_job())


@jobs.command("status")
@click.option("--job-id", help="Specific job ID to check")
@click.option("--strategy", help="Show jobs for specific strategy")
@click.option("--watch", is_flag=True, help="Watch job progress in real-time")
@click.option("--refresh-seconds", default=30, help="Refresh interval for watch mode")
def job_status(job_id: Optional[str], strategy: Optional[str], watch: bool, refresh_seconds: int):
    """Check retraining job status"""
    
    async def _check_status():
        config = get_settings()
        repository = UnifiedRepository(config.database.connection_string)
        
        strategy_orchestrator = StrategyOrchestrator(None, repository, {})
        retraining_engine = AutomatedRetrainingEngine(repository, strategy_orchestrator)
        
        await retraining_engine.start_engine()
        
        try:
            if job_id:
                job = retraining_engine.get_job_status(job_id)
                if job:
                    _display_job_details(job)
                else:
                    click.echo(f"❌ Job not found: {job_id}")
            else:
                active_jobs = retraining_engine.get_active_jobs()
                
                if strategy:
                    active_jobs = [job for job in active_jobs if job.strategy_name == strategy]
                
                if active_jobs:
                    _display_jobs_table(active_jobs, "Active Retraining Jobs")
                else:
                    click.echo("📭 No active retraining jobs")
                
                # Show recent history
                recent_jobs = retraining_engine.get_job_history(strategy, limit=5)
                if recent_jobs:
                    _display_jobs_table(recent_jobs, "Recent Completed Jobs")
        
        finally:
            await retraining_engine.stop_engine()
    
    async def _watch_status():
        while True:
            click.clear()
            await _check_status()
            if watch:
                click.echo(f"\n🔄 Refreshing in {refresh_seconds} seconds... (Press Ctrl+C to exit)")
                await asyncio.sleep(refresh_seconds)
            else:
                break
    
    asyncio.run(_watch_status())


@jobs.command("cancel")
@click.argument("job_id")
@click.option("--force", is_flag=True, help="Force cancellation without confirmation")
def cancel_retraining_job(job_id: str, force: bool):
    """Cancel a running retraining job"""
    
    async def _cancel_job():
        config = get_settings()
        repository = UnifiedRepository(config.database.connection_string)
        
        strategy_orchestrator = StrategyOrchestrator(None, repository, {})
        retraining_engine = AutomatedRetrainingEngine(repository, strategy_orchestrator)
        
        await retraining_engine.start_engine()
        
        try:
            if not force:
                click.confirm(f"Cancel retraining job {job_id}?", abort=True)
            
            success = await retraining_engine.cancel_job(job_id)
            
            if success:
                click.echo(f"✅ Cancelled retraining job: {job_id}")
            else:
                click.echo(f"❌ Failed to cancel job: {job_id} (job not found or already completed)")
        
        finally:
            await retraining_engine.stop_engine()
    
    asyncio.run(_cancel_job())


# Model Management Commands

@retraining_cli.group("models")
def models():
    """Manage model versions and validation"""
    pass


@models.command("validate")
@click.option("--strategy", required=True, help="Strategy name")
@click.option("--candidate-version", required=True, help="Candidate model version ID")
@click.option("--baseline-version", help="Baseline model version ID for comparison")
@click.option("--validation-level", default="standard",
              type=click.Choice(["basic", "standard", "rigorous", "production"]),
              help="Validation rigor level")
@click.option("--output-file", help="Save validation report to file")
def validate_model(
    strategy: str,
    candidate_version: str,
    baseline_version: Optional[str],
    validation_level: str,
    output_file: Optional[str]
):
    """Validate a model version"""
    
    async def _validate_model():
        config = get_settings()
        repository = UnifiedRepository(config.database.connection_string)
        
        validation_service = ModelValidationService(repository)
        
        # This would typically load model versions from the retraining engine
        # For demo purposes, create mock model versions
        from src.services.retraining.automated_engine import ModelVersion
        
        candidate_model = ModelVersion(
            version_id=candidate_version,
            strategy_name=strategy,
            parameters={"min_threshold": 15.0, "confidence_multiplier": 1.8},
            performance_metrics={"roi": 8.5, "win_rate": 0.62, "total_bets": 150},
            created_at=datetime.now(),
            training_data_period="2024-06-01_to_2024-08-31"
        )
        
        baseline_model = None
        if baseline_version:
            baseline_model = ModelVersion(
                version_id=baseline_version,
                strategy_name=strategy,
                parameters={"min_threshold": 10.0, "confidence_multiplier": 1.5},
                performance_metrics={"roi": 7.2, "win_rate": 0.58, "total_bets": 120},
                created_at=datetime.now() - timedelta(days=30),
                training_data_period="2024-05-01_to_2024-07-31"
            )
        
        click.echo(f"🔬 Starting model validation...")
        click.echo(f"   Strategy: {strategy}")
        click.echo(f"   Candidate: {candidate_version}")
        click.echo(f"   Baseline: {baseline_version or 'None'}")
        click.echo(f"   Level: {validation_level}")
        
        # Run validation
        result = await validation_service.validate_model(
            candidate_model=candidate_model,
            baseline_model=baseline_model,
            validation_level=ValidationLevel(validation_level.upper())
        )
        
        # Display results
        _display_validation_result(result)
        
        # Save to file if requested
        if output_file:
            report_data = {
                "validation_id": result.validation_id,
                "strategy": result.model_version.strategy_name,
                "candidate_version": result.model_version.version_id,
                "baseline_version": result.baseline_version.version_id if result.baseline_version else None,
                "validation_level": result.validation_level.value,
                "overall_score": result.overall_score,
                "passes_validation": result.passes_validation,
                "deployment_recommended": result.deployment_recommended,
                "risk_assessment": result.risk_assessment,
                "completed_at": result.completed_at.isoformat() if result.completed_at else None,
                "metrics": result.metrics.__dict__ if result.metrics else None,
                "validation_details": result.validation_details,
                "warnings": result.warnings,
                "errors": result.errors
            }
            
            Path(output_file).parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, 'w') as f:
                json.dump(report_data, f, indent=2, default=str)
            
            click.echo(f"\n💾 Validation report saved to: {output_file}")
    
    asyncio.run(_validate_model())


@models.command("list")
@click.option("--strategy", required=True, help="Strategy name")
@click.option("--limit", default=10, help="Maximum number of versions to show")
def list_model_versions(strategy: str, limit: int):
    """List model versions for a strategy"""
    
    async def _list_versions():
        config = get_settings()
        repository = UnifiedRepository(config.database.connection_string)
        
        strategy_orchestrator = StrategyOrchestrator(None, repository, {})
        retraining_engine = AutomatedRetrainingEngine(repository, strategy_orchestrator)
        
        await retraining_engine.start_engine()
        
        try:
            versions = retraining_engine.get_model_versions(strategy)
            production_model = retraining_engine.get_production_model(strategy)
            
            if versions:
                click.echo(f"📋 Model Versions for {strategy} (showing last {limit}):")
                
                # Sort by creation date, newest first
                sorted_versions = sorted(versions, key=lambda v: v.created_at, reverse=True)[:limit]
                
                table_data = []
                for version in sorted_versions:
                    is_production = "✅ PROD" if (production_model and version.version_id == production_model.version_id) else ""
                    is_baseline = "📊 BASE" if version.is_baseline else ""
                    
                    table_data.append([
                        version.version_id[:12] + "...",
                        version.created_at.strftime("%Y-%m-%d %H:%M"),
                        f"{version.performance_metrics.get('roi', 0):.1f}%",
                        f"{version.performance_metrics.get('win_rate', 0):.1%}",
                        version.performance_metrics.get('total_bets', 0),
                        version.training_data_period,
                        f"{is_production} {is_baseline}".strip()
                    ])
                
                headers = ["Version ID", "Created", "ROI", "Win Rate", "Bets", "Data Period", "Status"]
                click.echo(tabulate(table_data, headers=headers, tablefmt="grid"))
                
                if production_model:
                    click.echo(f"\n🚀 Production Model: {production_model.version_id}")
                    click.echo(f"   ROI: {production_model.performance_metrics.get('roi', 0):.1f}%")
                    click.echo(f"   Win Rate: {production_model.performance_metrics.get('win_rate', 0):.1%}")
            else:
                click.echo(f"📭 No model versions found for {strategy}")
        
        finally:
            await retraining_engine.stop_engine()
    
    asyncio.run(_list_versions())


# Performance Monitoring Commands

@retraining_cli.group("monitoring")
def monitoring():
    """Performance monitoring and alerts"""
    pass


@monitoring.command("status")
@click.option("--strategy", help="Show status for specific strategy")
@click.option("--detailed", is_flag=True, help="Show detailed performance metrics")
def monitoring_status(strategy: Optional[str], detailed: bool):
    """Show performance monitoring status"""
    
    async def _show_status():
        config = get_settings()
        repository = UnifiedRepository(config.database.connection_string)
        
        monitoring_service = PerformanceMonitoringService(repository)
        
        if strategy:
            # Show detailed strategy status
            current_perf = monitoring_service.get_current_performance(strategy)
            trend_analysis = monitoring_service.get_trend_analysis(strategy)
            active_alerts = monitoring_service.get_active_alerts(strategy)
            
            if current_perf:
                click.echo(f"📊 Performance Status for {strategy}:")
                click.echo(f"   Current ROI: {current_perf.avg_roi_per_bet:.2f}%")
                click.echo(f"   Win Rate: {current_perf.win_rate:.1%}")
                click.echo(f"   Total Bets (24h): {current_perf.total_bets}")
                click.echo(f"   Max Drawdown: {current_perf.max_drawdown:.1f}%")
                
                if trend_analysis:
                    click.echo(f"\n📈 Trend Analysis:")
                    click.echo(f"   Direction: {trend_analysis.trend_direction.value}")
                    click.echo(f"   Strength: {trend_analysis.trend_strength:.2f}")
                    click.echo(f"   7-day forecast: {trend_analysis.predicted_7_day_roi:.2f}%")
                
                if active_alerts:
                    click.echo(f"\n🚨 Active Alerts ({len(active_alerts)}):")
                    for alert in active_alerts:
                        click.echo(f"   {alert.alert_level.value.upper()}: {alert.message}")
                else:
                    click.echo(f"\n✅ No active alerts")
            else:
                click.echo(f"❌ No performance data available for {strategy}")
        else:
            # Show overall monitoring status
            status = monitoring_service.get_monitoring_status()
            
            click.echo("📊 Performance Monitoring Status:")
            click.echo(f"   Monitoring enabled: {'✅' if status['monitoring_enabled'] else '❌'}")
            click.echo(f"   Strategies monitored: {status['strategies_monitored']}")
            click.echo(f"   Active alerts: {status['active_alerts']}")
            click.echo(f"   Monitoring interval: {status['monitoring_interval_minutes']} minutes")
            
            if status['alert_breakdown']:
                click.echo(f"\n🚨 Alerts by level:")
                for level, count in status['alert_breakdown'].items():
                    click.echo(f"   {level.upper()}: {count}")
            
            if detailed and status['performance_summary']:
                click.echo(f"\n📈 Performance Summary:")
                for strategy_name, perf in status['performance_summary'].items():
                    click.echo(f"   {strategy_name}:")
                    click.echo(f"     ROI: {perf['roi']:.2f}%")
                    click.echo(f"     Win Rate: {perf['win_rate']:.1%}")
                    click.echo(f"     Bets: {perf['total_bets']}")
    
    asyncio.run(_show_status())


@monitoring.command("alerts")
@click.option("--strategy", help="Show alerts for specific strategy")
@click.option("--level", type=click.Choice(["info", "warning", "critical"]),
              help="Filter by alert level")
@click.option("--history", is_flag=True, help="Show alert history")
def show_alerts(strategy: Optional[str], level: Optional[str], history: bool):
    """Show performance alerts"""
    
    async def _show_alerts():
        config = get_settings()
        repository = UnifiedRepository(config.database.connection_string)
        
        monitoring_service = PerformanceMonitoringService(repository)
        
        if history:
            alerts = monitoring_service.get_alert_history(strategy, limit=20)
            title = "Alert History"
        else:
            alerts = monitoring_service.get_active_alerts(strategy)
            title = "Active Alerts"
        
        if level:
            alerts = [alert for alert in alerts if alert.alert_level.value == level]
        
        if alerts:
            click.echo(f"🚨 {title} ({len(alerts)}):")
            
            table_data = []
            for alert in alerts:
                status = "Resolved" if alert.resolved_at else "Active"
                table_data.append([
                    alert.strategy_name,
                    alert.alert_type,
                    alert.alert_level.value.upper(),
                    alert.message[:50] + "..." if len(alert.message) > 50 else alert.message,
                    alert.triggered_at.strftime("%Y-%m-%d %H:%M"),
                    status
                ])
            
            headers = ["Strategy", "Type", "Level", "Message", "Triggered", "Status"]
            click.echo(tabulate(table_data, headers=headers, tablefmt="grid"))
        else:
            filter_desc = f" ({level.upper()})" if level else ""
            strategy_desc = f" for {strategy}" if strategy else ""
            click.echo(f"✅ No {title.lower()}{filter_desc}{strategy_desc}")
    
    asyncio.run(_show_alerts())


# Schedule Management Commands

@retraining_cli.group("schedules")
def schedules():
    """Manage retraining schedules"""
    pass


@schedules.command("list")
def list_schedules():
    """List all retraining schedules"""
    
    async def _list_schedules():
        config = get_settings()
        repository = UnifiedRepository(config.database.connection_string)
        
        trigger_service = RetrainingTriggerService(repository)
        strategy_orchestrator = StrategyOrchestrator(None, repository, {})
        retraining_engine = AutomatedRetrainingEngine(repository, strategy_orchestrator)
        scheduler = RetrainingScheduler(trigger_service, retraining_engine)
        
        schedules_list = scheduler.get_schedules()
        
        if schedules_list:
            click.echo(f"📅 Retraining Schedules ({len(schedules_list)}):")
            
            table_data = []
            for schedule in schedules_list:
                status = "✅ Enabled" if schedule.enabled else "❌ Disabled"
                next_run = schedule.next_run.strftime("%Y-%m-%d %H:%M") if schedule.next_run else "N/A"
                last_run = schedule.last_run.strftime("%Y-%m-%d %H:%M") if schedule.last_run else "Never"
                
                table_data.append([
                    schedule.schedule_name,
                    schedule.strategy_name,
                    schedule.schedule_type.value,
                    schedule.cron_expression or f"{schedule.interval_hours}h" if schedule.interval_hours else "N/A",
                    next_run,
                    last_run,
                    status
                ])
            
            headers = ["Schedule Name", "Strategy", "Type", "Expression/Interval", "Next Run", "Last Run", "Status"]
            click.echo(tabulate(table_data, headers=headers, tablefmt="grid"))
        else:
            click.echo("📭 No retraining schedules configured")
    
    asyncio.run(_list_schedules())


@schedules.command("create")
@click.option("--name", required=True, help="Schedule name")
@click.option("--strategy", required=True, help="Strategy name")
@click.option("--type", "schedule_type", default="cron",
              type=click.Choice(["cron", "interval"]),
              help="Schedule type")
@click.option("--cron", help="Cron expression (for cron type)")
@click.option("--interval-hours", type=int, help="Interval in hours (for interval type)")
@click.option("--priority", default="normal",
              type=click.Choice(["critical", "high", "normal", "low"]),
              help="Schedule priority")
@click.option("--description", help="Schedule description")
def create_schedule(
    name: str,
    strategy: str,
    schedule_type: str,
    cron: Optional[str],
    interval_hours: Optional[int],
    priority: str,
    description: Optional[str]
):
    """Create a new retraining schedule"""
    
    async def _create_schedule():
        from src.services.retraining.scheduler import RetrainingSchedule
        
        # Validate schedule parameters
        if schedule_type == "cron" and not cron:
            click.echo("❌ Cron expression required for cron schedule type")
            return
        
        if schedule_type == "interval" and not interval_hours:
            click.echo("❌ Interval hours required for interval schedule type")
            return
        
        config = get_settings()
        repository = UnifiedRepository(config.database.connection_string)
        
        trigger_service = RetrainingTriggerService(repository)
        strategy_orchestrator = StrategyOrchestrator(None, repository, {})
        retraining_engine = AutomatedRetrainingEngine(repository, strategy_orchestrator)
        scheduler = RetrainingScheduler(trigger_service, retraining_engine)
        
        schedule = RetrainingSchedule(
            schedule_id=str(len(scheduler.get_schedules()) + 1).zfill(3),  # Simple ID generation
            schedule_name=name,
            strategy_name=strategy,
            schedule_type=ScheduleType(schedule_type.upper()),
            cron_expression=cron,
            interval_hours=interval_hours,
            priority=SchedulePriority[priority.upper()],
            description=description
        )
        
        scheduler.add_schedule(schedule)
        
        click.echo(f"✅ Created retraining schedule: {name}")
        click.echo(f"   Strategy: {strategy}")
        click.echo(f"   Type: {schedule_type}")
        click.echo(f"   Expression: {cron or f'{interval_hours}h'}")
        click.echo(f"   Priority: {priority}")
        click.echo(f"   Next run: {schedule.next_run.strftime('%Y-%m-%d %H:%M') if schedule.next_run else 'N/A'}")
    
    asyncio.run(_create_schedule())


# System Commands

@retraining_cli.command("status")
@click.option("--detailed", is_flag=True, help="Show detailed system status")
def system_status(detailed: bool):
    """Show overall retraining system status"""
    
    async def _show_status():
        config = get_settings()
        repository = UnifiedRepository(config.database.connection_string)
        
        # Initialize services
        trigger_service = RetrainingTriggerService(repository)
        strategy_orchestrator = StrategyOrchestrator(None, repository, {})
        retraining_engine = AutomatedRetrainingEngine(repository, strategy_orchestrator)
        monitoring_service = PerformanceMonitoringService(repository)
        scheduler = RetrainingScheduler(trigger_service, retraining_engine)
        
        await retraining_engine.start_engine()
        
        try:
            click.echo("🤖 Automated Retraining System Status:")
            
            # Trigger service status
            trigger_stats = trigger_service.get_trigger_statistics()
            click.echo(f"\n🎯 Trigger Service:")
            click.echo(f"   Active triggers: {trigger_stats['active_triggers']}")
            click.echo(f"   Monitoring enabled: {'✅' if trigger_stats['monitoring_enabled'] else '❌'}")
            click.echo(f"   Strategies monitored: {trigger_stats['strategies_monitored']}")
            
            # Retraining engine status
            engine_status = retraining_engine.get_engine_status()
            click.echo(f"\n⚙️  Retraining Engine:")
            click.echo(f"   Engine running: {'✅' if engine_status['engine_running'] else '❌'}")
            click.echo(f"   Active jobs: {engine_status['active_jobs_count']}")
            click.echo(f"   Total jobs completed: {engine_status['total_jobs_completed']}")
            click.echo(f"   Production models: {len(engine_status['production_models'])}")
            
            # Performance monitoring status
            monitoring_status = monitoring_service.get_monitoring_status()
            click.echo(f"\n📊 Performance Monitoring:")
            click.echo(f"   Monitoring enabled: {'✅' if monitoring_status['monitoring_enabled'] else '❌'}")
            click.echo(f"   Active alerts: {monitoring_status['active_alerts']}")
            click.echo(f"   Strategies monitored: {monitoring_status['strategies_monitored']}")
            
            # Scheduler status
            scheduler_status = scheduler.get_scheduler_status()
            click.echo(f"\n📅 Scheduler:")
            click.echo(f"   Scheduler running: {'✅' if scheduler_status['scheduler_running'] else '❌'}")
            click.echo(f"   Active schedules: {scheduler_status['schedules']['enabled']}")
            click.echo(f"   Queued jobs: {scheduler_status['jobs']['queued']}")
            click.echo(f"   Running jobs: {scheduler_status['jobs']['running']}")
            
            if detailed:
                click.echo(f"\n📈 Detailed Statistics:")
                
                if trigger_stats['triggers_by_type']:
                    click.echo(f"   Triggers by type:")
                    for trigger_type, count in trigger_stats['triggers_by_type'].items():
                        click.echo(f"     {trigger_type}: {count}")
                
                if engine_status['active_jobs_by_status']:
                    click.echo(f"   Jobs by status:")
                    for status, count in engine_status['active_jobs_by_status'].items():
                        click.echo(f"     {status}: {count}")
                
                if monitoring_status['alert_breakdown']:
                    click.echo(f"   Alerts by level:")
                    for level, count in monitoring_status['alert_breakdown'].items():
                        click.echo(f"     {level.upper()}: {count}")
        
        finally:
            await retraining_engine.stop_engine()
    
    asyncio.run(_show_status())


# Helper functions

def _display_triggers(triggers: List, title: str) -> None:
    """Display triggers in a formatted table."""
    
    if triggers:
        click.echo(f"🎯 {title} ({len(triggers)}):")
        
        table_data = []
        for trigger in triggers:
            table_data.append([
                trigger.strategy_name,
                trigger.trigger_type.value,
                trigger.severity.value,
                trigger.condition_description[:60] + "..." if len(trigger.condition_description) > 60 else trigger.condition_description,
                trigger.detected_at.strftime("%Y-%m-%d %H:%M")
            ])
        
        headers = ["Strategy", "Type", "Severity", "Description", "Detected"]
        click.echo(tabulate(table_data, headers=headers, tablefmt="grid"))
    else:
        click.echo(f"✅ {title}: No active triggers")


def _display_jobs_table(jobs: List, title: str) -> None:
    """Display jobs in a formatted table."""
    
    click.echo(f"\n🔧 {title} ({len(jobs)}):")
    
    table_data = []
    for job in jobs:
        progress = f"{job.progress_percentage:.1f}%" if hasattr(job, 'progress_percentage') else "N/A"
        status = job.status.value if hasattr(job.status, 'value') else str(job.status)
        duration = "N/A"
        
        if hasattr(job, 'started_at') and job.started_at:
            if hasattr(job, 'completed_at') and job.completed_at:
                duration = f"{(job.completed_at - job.started_at).total_seconds() / 60:.1f}m"
            else:
                duration = f"{(datetime.now() - job.started_at).total_seconds() / 60:.1f}m"
        
        table_data.append([
            job.job_id[:12] + "...",
            job.strategy_name,
            job.retraining_strategy.value if hasattr(job.retraining_strategy, 'value') else str(job.retraining_strategy),
            status,
            progress,
            duration
        ])
    
    headers = ["Job ID", "Strategy", "Type", "Status", "Progress", "Duration"]
    click.echo(tabulate(table_data, headers=headers, tablefmt="grid"))


def _display_job_details(job) -> None:
    """Display detailed job information."""
    
    click.echo(f"🔧 Retraining Job Details:")
    click.echo(f"   Job ID: {job.job_id}")
    click.echo(f"   Strategy: {job.strategy_name}")
    click.echo(f"   Type: {job.retraining_strategy.value}")
    click.echo(f"   Status: {job.status.value}")
    click.echo(f"   Progress: {job.progress_percentage:.1f}%")
    
    if hasattr(job, 'current_stage') and job.current_stage:
        click.echo(f"   Current Stage: {job.current_stage}")
    
    if hasattr(job, 'started_at') and job.started_at:
        click.echo(f"   Started: {job.started_at.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if hasattr(job, 'completed_at') and job.completed_at:
        click.echo(f"   Completed: {job.completed_at.strftime('%Y-%m-%d %H:%M:%S')}")
        duration = (job.completed_at - job.started_at).total_seconds() / 60
        click.echo(f"   Duration: {duration:.1f} minutes")
    
    if hasattr(job, 'trigger_conditions') and job.trigger_conditions:
        click.echo(f"\n🎯 Trigger Conditions ({len(job.trigger_conditions)}):")
        for trigger in job.trigger_conditions:
            click.echo(f"   • {trigger.trigger_type.value}: {trigger.condition_description}")
    
    if hasattr(job, 'improvement_percentage') and job.improvement_percentage:
        click.echo(f"\n📈 Results:")
        click.echo(f"   Improvement: {job.improvement_percentage:.1f}%")
        
        if hasattr(job, 'statistical_significance') and job.statistical_significance:
            click.echo(f"   Statistical significance: p={job.statistical_significance:.3f}")
    
    if hasattr(job, 'error_message') and job.error_message:
        click.echo(f"\n❌ Error: {job.error_message}")
    
    if hasattr(job, 'logs') and job.logs:
        click.echo(f"\n📝 Recent Logs:")
        for log_entry in job.logs[-5:]:  # Show last 5 log entries
            click.echo(f"   • {log_entry}")


def _display_validation_result(result) -> None:
    """Display validation result details."""
    
    click.echo(f"\n🔬 Model Validation Results:")
    click.echo(f"   Validation ID: {result.validation_id}")
    click.echo(f"   Strategy: {result.model_version.strategy_name}")
    click.echo(f"   Candidate: {result.model_version.version_id}")
    click.echo(f"   Status: {result.status.value.upper()}")
    click.echo(f"   Overall Score: {result.overall_score:.3f}")
    
    status_icon = "✅" if result.passes_validation else "❌"
    click.echo(f"   Passes Validation: {status_icon} {result.passes_validation}")
    
    deploy_icon = "🚀" if result.deployment_recommended else "⏸️"
    click.echo(f"   Deployment Recommended: {deploy_icon} {result.deployment_recommended}")
    
    click.echo(f"   Risk Assessment: {result.risk_assessment}")
    
    if result.metrics:
        click.echo(f"\n📊 Performance Metrics:")
        click.echo(f"   ROI Improvement: {result.metrics.roi_improvement:.2f}%")
        click.echo(f"   Win Rate Improvement: {result.metrics.win_rate_improvement:.3f}")
        click.echo(f"   Statistical Significance: p={result.metrics.p_value:.3f}")
        click.echo(f"   Confidence Interval: ({result.metrics.confidence_interval[0]:.2f}, {result.metrics.confidence_interval[1]:.2f})")
        click.echo(f"   Consistency Score: {result.metrics.consistency_score:.3f}")
        click.echo(f"   Max Drawdown: {result.metrics.max_drawdown:.2f}%")
        click.echo(f"   Sharpe Ratio: {result.metrics.sharpe_ratio:.3f}")
    
    if result.warnings:
        click.echo(f"\n⚠️  Warnings:")
        for warning in result.warnings:
            click.echo(f"   • {warning}")
    
    if result.errors:
        click.echo(f"\n❌ Errors:")
        for error in result.errors:
            click.echo(f"   • {error}")


# Register the CLI group
__all__ = ["retraining_cli"]