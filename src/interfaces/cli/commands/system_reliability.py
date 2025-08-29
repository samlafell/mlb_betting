"""
CLI Commands for System Reliability Management
Provides comprehensive CLI interface for reliability monitoring, ML retraining, and system health
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Optional
import click

from ....core.self_healing_system import get_self_healing_system
from ....services.retraining.automated_retraining_service import get_automated_retraining_service, RetrainingTrigger
from ....ml.optimization.hyperparameter_optimizer import get_hyperparameter_optimizer
from ....services.monitoring.prometheus_metrics_service import get_metrics_service


@click.group()
def system_reliability():
    """System reliability and automated management commands."""
    pass


# Self-Healing System Commands

@system_reliability.group()
def health():
    """Self-healing system health monitoring commands."""
    pass


@health.command()
def status():
    """Get comprehensive system health status."""
    try:
        healing_system = get_self_healing_system()
        system_status = healing_system.get_system_status()
        
        click.echo("🏥 System Health Status")
        click.echo("=" * 50)
        
        # Overall health
        overall_health = system_status["overall_health"]
        health_emoji = {
            "healthy": "✅",
            "degraded": "⚠️",
            "critical": "🚨",
            "failing": "❌"
        }
        
        click.echo(f"Overall Health: {health_emoji.get(overall_health, '❓')} {overall_health.upper()}")
        click.echo()
        
        # Component status
        click.echo("Component Status:")
        for name, status in system_status["components"].items():
            component_emoji = health_emoji.get(status["status"], "❓")
            click.echo(f"  {component_emoji} {name}: {status['status']} - {status['message']}")
        
        click.echo()
        
        # Recovery statistics
        recovery_stats = system_status["recovery_stats"]
        click.echo("Recovery Statistics:")
        click.echo(f"  Total Recoveries: {recovery_stats['total_recoveries']}")
        click.echo(f"  Successful: {recovery_stats['successful_recoveries']}")
        click.echo(f"  Active Attempts: {recovery_stats['active_recovery_attempts']}")
        
        # Recent recoveries
        recent_recoveries = system_status["recent_recoveries"]
        if recent_recoveries:
            click.echo()
            click.echo("Recent Recoveries:")
            for recovery in recent_recoveries[-5:]:
                success_emoji = "✅" if recovery["success"] else "❌"
                click.echo(f"  {success_emoji} {recovery['component']}: {recovery['action']} at {recovery['started_at']}")
        
    except Exception as e:
        click.echo(f"❌ Error getting system health: {e}", err=True)


@health.command()
@click.option('--start-monitoring', is_flag=True, help='Start the self-healing monitoring system')
def monitor(start_monitoring: bool):
    """Start or check self-healing system monitoring."""
    try:
        healing_system = get_self_healing_system()
        
        if start_monitoring:
            click.echo("🚀 Starting self-healing system monitoring...")
            asyncio.run(healing_system.start_monitoring())
            click.echo("✅ Self-healing monitoring started")
        else:
            click.echo("ℹ️ Use --start-monitoring flag to start monitoring")
            click.echo("Current monitoring status available via 'health status' command")
        
    except Exception as e:
        click.echo(f"❌ Error with monitoring: {e}", err=True)


# ML Retraining Commands

@system_reliability.group()
def retraining():
    """Automated ML model retraining commands."""
    pass


@retraining.command()
@click.argument('model_name')
@click.option('--trigger', 
              type=click.Choice(['manual', 'performance_degradation', 'data_drift', 'scheduled']),
              default='manual',
              help='Retraining trigger type')
@click.option('--reason', default='Manual CLI trigger', help='Reason for retraining')
@click.option('--force', is_flag=True, help='Force retraining even if another job is running')
def trigger(model_name: str, trigger: str, reason: str, force: bool):
    """Trigger retraining for a specific model."""
    try:
        retraining_service = get_automated_retraining_service()
        
        # Convert string to enum
        trigger_enum = RetrainingTrigger(trigger)
        
        click.echo(f"🔄 Triggering retraining for {model_name}...")
        
        async def run_trigger():
            job_id = await retraining_service.trigger_retraining(
                model_name=model_name,
                trigger=trigger_enum,
                trigger_reason=reason,
                force=force
            )
            return job_id
        
        job_id = asyncio.run(run_trigger())
        
        click.echo(f"✅ Retraining job started: {job_id}")
        click.echo(f"Use 'retraining status {job_id}' to monitor progress")
        
    except Exception as e:
        click.echo(f"❌ Error triggering retraining: {e}", err=True)


@retraining.command()
@click.argument('job_id', required=False)
def status(job_id: Optional[str]):
    """Get retraining job status."""
    try:
        retraining_service = get_automated_retraining_service()
        status_info = retraining_service.get_retraining_status(job_id)
        
        if job_id:
            # Single job status
            if "error" in status_info:
                click.echo(f"❌ {status_info['error']}")
                return
            
            click.echo(f"📋 Retraining Job: {job_id}")
            click.echo("=" * 50)
            click.echo(f"Model: {status_info['model_name']}")
            click.echo(f"Status: {status_info['status']}")
            click.echo(f"Trigger: {status_info['trigger']} - {status_info['trigger_reason']}")
            click.echo(f"Created: {status_info['created_at']}")
            
            if status_info.get('started_at'):
                click.echo(f"Started: {status_info['started_at']}")
            
            if status_info.get('completed_at'):
                click.echo(f"Completed: {status_info['completed_at']}")
            
            if status_info.get('data_quality_score'):
                click.echo(f"Data Quality Score: {status_info['data_quality_score']:.3f}")
            
            if status_info.get('performance_metrics'):
                click.echo("Performance Metrics:")
                for metric, value in status_info['performance_metrics'].items():
                    click.echo(f"  {metric}: {value:.3f}")
            
            if status_info.get('error_message'):
                click.echo(f"❌ Error: {status_info['error_message']}")
        
        else:
            # All jobs overview
            active_jobs = status_info["active_jobs"]
            jobs_by_status = status_info["jobs_by_status"]
            
            click.echo("🔄 Retraining Jobs Overview")
            click.echo("=" * 50)
            click.echo(f"Total Jobs: {status_info['total_jobs']}")
            
            click.echo("\nJobs by Status:")
            for status, count in jobs_by_status.items():
                if count > 0:
                    click.echo(f"  {status}: {count}")
            
            if active_jobs:
                click.echo("\nActive Jobs:")
                for job in active_jobs[-10:]:  # Show last 10
                    status_emoji = {
                        "training": "🔄",
                        "validation": "🔍",
                        "ab_testing": "🧪",
                        "deployed": "✅",
                        "failed": "❌",
                        "rolled_back": "↩️"
                    }
                    emoji = status_emoji.get(job["status"], "❓")
                    click.echo(f"  {emoji} {job['job_id']}: {job['model_name']} - {job['status']}")
        
    except Exception as e:
        click.echo(f"❌ Error getting retraining status: {e}", err=True)


@retraining.command()
@click.argument('job_id')
def cancel(job_id: str):
    """Cancel a retraining job."""
    try:
        retraining_service = get_automated_retraining_service()
        
        async def run_cancel():
            return await retraining_service.cancel_retraining_job(job_id)
        
        success = asyncio.run(run_cancel())
        
        if success:
            click.echo(f"✅ Retraining job {job_id} cancelled")
        else:
            click.echo(f"❌ Failed to cancel job {job_id} (job may be completed or not found)")
        
    except Exception as e:
        click.echo(f"❌ Error cancelling retraining job: {e}", err=True)


# Hyperparameter Optimization Commands

@system_reliability.group()
def hyperopt():
    """Hyperparameter optimization commands."""
    pass


@hyperopt.command()
@click.argument('model_name')
@click.option('--days', default=90, help='Training data window in days')
@click.option('--trials', default=100, help='Number of optimization trials')
@click.option('--timeout', default=6, help='Timeout in hours')
def optimize(model_name: str, days: int, trials: int, timeout: int):
    """Optimize hyperparameters for a specific model."""
    try:
        optimizer = get_hyperparameter_optimizer()
        
        click.echo(f"🎯 Starting hyperparameter optimization for {model_name}")
        click.echo(f"Training window: {days} days, Trials: {trials}, Timeout: {timeout}h")
        
        async def run_optimization():
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            config = {
                "n_trials": trials,
                "timeout_hours": timeout,
                "optimization_metric": "roc_auc",
            }
            
            result = await optimizer.optimize_hyperparameters(
                model_name=model_name,
                start_date=start_date,
                end_date=end_date,
                optimization_config=config
            )
            return result
        
        result = asyncio.run(run_optimization())
        
        click.echo("\n✅ Hyperparameter optimization completed!")
        click.echo("=" * 50)
        click.echo(f"Best Score: {result['best_score']:.4f}")
        click.echo(f"Trials Completed: {result['trials_completed']}")
        click.echo(f"Optimization Time: {result['optimization_time_seconds']:.1f}s")
        
        if result.get('improvement_over_default'):
            improvement = result['improvement_over_default']
            click.echo(f"Improvement: {improvement:.2%}")
        
        click.echo("\nBest Parameters:")
        for param, value in result['best_parameters'].items():
            click.echo(f"  {param}: {value}")
        
    except Exception as e:
        click.echo(f"❌ Error optimizing hyperparameters: {e}", err=True)


@hyperopt.command()
@click.option('--days', default=90, help='Training data window in days')
@click.option('--parallel', is_flag=True, help='Run optimizations in parallel')
def optimize_all(days: int, parallel: bool):
    """Optimize hyperparameters for all models."""
    try:
        optimizer = get_hyperparameter_optimizer()
        
        click.echo("🎯 Starting hyperparameter optimization for all models")
        
        async def run_optimization():
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            results = await optimizer.optimize_all_models(
                start_date=start_date,
                end_date=end_date,
                parallel_execution=parallel
            )
            return results
        
        results = asyncio.run(run_optimization())
        
        click.echo("\n✅ Batch hyperparameter optimization completed!")
        click.echo("=" * 50)
        
        for model_name, result in results.items():
            if result.get("status") == "failed":
                click.echo(f"❌ {model_name}: {result.get('error', 'Unknown error')}")
            else:
                score = result.get('best_score', 0)
                trials = result.get('trials_completed', 0)
                click.echo(f"✅ {model_name}: Score={score:.4f}, Trials={trials}")
        
    except Exception as e:
        click.echo(f"❌ Error optimizing all hyperparameters: {e}", err=True)


@hyperopt.command()
@click.argument('model_name', required=False)
def best_params(model_name: Optional[str]):
    """Get best hyperparameters for models."""
    try:
        optimizer = get_hyperparameter_optimizer()
        best_params = optimizer.get_best_parameters(model_name)
        
        if model_name:
            if model_name in best_params:
                params = best_params[model_name]
                click.echo(f"🎯 Best Parameters for {model_name}")
                click.echo("=" * 50)
                click.echo(f"Score: {params['score']:.4f}")
                click.echo(f"Optimization Date: {params['optimization_date']}")
                click.echo(f"Trials: {params['trials_completed']}")
                
                click.echo("\nParameters:")
                for param, value in params['parameters'].items():
                    click.echo(f"  {param}: {value}")
            else:
                click.echo(f"❌ No optimized parameters found for {model_name}")
        else:
            click.echo("🎯 Best Parameters for All Models")
            click.echo("=" * 50)
            
            if not best_params:
                click.echo("No optimized parameters found")
                return
            
            for model, params in best_params.items():
                click.echo(f"\n{model}:")
                click.echo(f"  Score: {params['score']:.4f}")
                click.echo(f"  Date: {params['optimization_date']}")
                click.echo(f"  Trials: {params['trials_completed']}")
        
    except Exception as e:
        click.echo(f"❌ Error getting best parameters: {e}", err=True)


# System Metrics Commands

@system_reliability.group()
def metrics():
    """System metrics and monitoring commands."""
    pass


@metrics.command()
def prometheus():
    """Get Prometheus metrics overview."""
    try:
        metrics_service = get_metrics_service()
        system_overview = metrics_service.get_system_overview()
        
        click.echo("📊 Prometheus Metrics Overview")
        click.echo("=" * 50)
        click.echo(f"Uptime: {system_overview['uptime_seconds']:.0f}s")
        click.echo(f"Active Pipelines: {system_overview['active_pipelines']}")
        click.echo(f"Total SLOs: {system_overview['total_slos']}")
        
        # SLO compliance
        slo_compliance = system_overview.get('slo_compliance', {})
        if slo_compliance:
            click.echo("\nSLO Compliance:")
            for slo_name, slo_data in slo_compliance.items():
                status = slo_data.get('status', 'unknown')
                value = slo_data.get('current_value', 0)
                target = slo_data.get('target', 0)
                
                status_emoji = {
                    "healthy": "✅",
                    "warning": "⚠️",
                    "critical": "🚨"
                }
                emoji = status_emoji.get(status, "❓")
                click.echo(f"  {emoji} {slo_name}: {value:.1f}% (target: {target:.1f}%)")
        
    except Exception as e:
        click.echo(f"❌ Error getting metrics: {e}", err=True)


@metrics.command()
@click.option('--format', type=click.Choice(['text', 'json']), default='text')
def export(format: str):
    """Export Prometheus metrics."""
    try:
        metrics_service = get_metrics_service()
        
        if format == 'json':
            overview = metrics_service.get_system_overview()
            click.echo(json.dumps(overview, indent=2, default=str))
        else:
            metrics_text = metrics_service.get_metrics()
            click.echo(metrics_text)
        
    except Exception as e:
        click.echo(f"❌ Error exporting metrics: {e}", err=True)


# System Overview Command

@system_reliability.command()
def overview():
    """Get comprehensive system reliability overview."""
    try:
        click.echo("🏗️ MLB Betting System - Reliability Overview")
        click.echo("=" * 60)
        
        # Health status
        try:
            healing_system = get_self_healing_system()
            system_status = healing_system.get_system_status()
            overall_health = system_status["overall_health"]
            
            health_emoji = {
                "healthy": "✅",
                "degraded": "⚠️", 
                "critical": "🚨",
                "failing": "❌"
            }
            click.echo(f"System Health: {health_emoji.get(overall_health, '❓')} {overall_health.upper()}")
        except Exception:
            click.echo("System Health: ❓ Unknown")
        
        # Retraining status
        try:
            retraining_service = get_automated_retraining_service()
            retraining_status = retraining_service.get_retraining_status()
            active_jobs = len([j for j in retraining_status["active_jobs"] if j["status"] in ["training", "validation", "ab_testing"]])
            click.echo(f"Active Retraining Jobs: {active_jobs}")
        except Exception:
            click.echo("Active Retraining Jobs: ❓ Unknown")
        
        # Metrics overview
        try:
            metrics_service = get_metrics_service()
            overview = metrics_service.get_system_overview()
            click.echo(f"System Uptime: {overview['uptime_seconds']:.0f}s")
            click.echo(f"Active Pipelines: {overview['active_pipelines']}")
        except Exception:
            click.echo("Metrics: ❓ Unknown")
        
        click.echo("\n🚀 System Capabilities:")
        click.echo("  ✅ Self-Healing Infrastructure")
        click.echo("  ✅ Automated ML Retraining")
        click.echo("  ✅ Hyperparameter Optimization")
        click.echo("  ✅ Circuit Breaker Protection")
        click.echo("  ✅ Comprehensive Monitoring")
        click.echo("  ✅ Predictive Failure Detection")
        
        click.echo("\n📋 Management Commands:")
        click.echo("  system-reliability health status    # Check system health")
        click.echo("  system-reliability retraining status # Check retraining jobs")
        click.echo("  system-reliability hyperopt optimize-all # Optimize all models")
        click.echo("  system-reliability metrics prometheus # View metrics")
        
    except Exception as e:
        click.echo(f"❌ Error getting overview: {e}", err=True)


if __name__ == '__main__':
    system_reliability()