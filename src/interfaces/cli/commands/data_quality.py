#!/usr/bin/env python3
"""
Data Quality CLI Commands

Command-line interface for data quality validation and reporting.
Provides comprehensive quality management commands for operators.
"""

import asyncio
import json
from datetime import datetime
from typing import Optional

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich import box

from ....core.config import get_settings
from ....core.logging import LogComponent, get_logger
from ....services.data_quality import (
    DataQualityValidationService,
    PipelineStage,
    ValidationStatus
)
from ....services.data_quality.monitoring_integration import DataQualityMonitoringIntegration

logger = get_logger(__name__, LogComponent.CLI)
console = Console()


@click.group(name="data-quality", help="Data quality validation and management commands")
def data_quality_cli():
    """Data quality validation and management commands."""
    pass


@data_quality_cli.command(name="validate")
@click.option(
    "--stage",
    type=click.Choice(["raw", "staging", "curated", "all"]),
    default="all",
    help="Pipeline stage to validate"
)
@click.option(
    "--output",
    type=click.Choice(["table", "json", "summary"]),
    default="table",
    help="Output format"
)
@click.option(
    "--save-report",
    type=click.Path(),
    help="Save detailed report to file"
)
@click.option(
    "--threshold",
    type=float,
    help="Custom quality threshold (0.0-1.0)"
)
@click.option(
    "--real",
    is_flag=True,
    help="Run against real database (default: test mode)"
)
def validate_quality(stage: str, output: str, save_report: Optional[str], 
                    threshold: Optional[float], real: bool):
    """Validate data quality for pipeline stages."""
    
    async def run_validation():
        config = get_settings()
        
        # Override threshold if provided
        if threshold is not None:
            if not 0.0 <= threshold <= 1.0:
                console.print("[red]Error: Threshold must be between 0.0 and 1.0[/red]")
                return
        
        service = DataQualityValidationService(config)
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            
            if stage == "all":
                task = progress.add_task("Validating all pipeline stages...", total=None)
                reports = await service.validate_full_pipeline()
                progress.update(task, completed=True)
                
                if output == "json":
                    _print_reports_json(reports)
                elif output == "summary":
                    _print_summary(reports)
                else:
                    _print_reports_table(reports)
                    
            else:
                pipeline_stage = PipelineStage(stage)
                task = progress.add_task(f"Validating {stage.upper()} stage...", total=None)
                report = await service.validate_pipeline_stage(pipeline_stage)
                progress.update(task, completed=True)
                
                if output == "json":
                    _print_report_json(report)
                elif output == "summary":
                    _print_report_summary(report)
                else:
                    _print_report_table(report)
        
        # Save detailed report if requested
        if save_report:
            if stage == "all":
                _save_reports(reports, save_report)
            else:
                _save_report(report, save_report)
            console.print(f"[green]Report saved to {save_report}[/green]")
    
    if not real:
        console.print("[yellow]Running in test mode. Use --real for live data validation.[/yellow]")
    
    try:
        asyncio.run(run_validation())
    except Exception as e:
        console.print(f"[red]Validation failed: {e}[/red]")
        raise click.ClickException(str(e))


@data_quality_cli.command(name="gates")
@click.option(
    "--output",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format"
)
@click.option(
    "--real",
    is_flag=True,
    help="Run against real database"
)
def check_quality_gates(output: str, real: bool):
    """Check quality gates status for pipeline promotion."""
    
    async def run_gates_check():
        config = get_settings()
        service = DataQualityValidationService(config)
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Checking quality gates...", total=None)
            gates = await service.check_quality_gates()
            progress.update(task, completed=True)
        
        if output == "json":
            console.print(json.dumps(gates, indent=2))
        else:
            _print_gates_table(gates)
    
    if not real:
        console.print("[yellow]Running in test mode. Use --real for live data validation.[/yellow]")
    
    try:
        asyncio.run(run_gates_check())
    except Exception as e:
        console.print(f"[red]Quality gates check failed: {e}[/red]")
        raise click.ClickException(str(e))


@data_quality_cli.command(name="metrics")
@click.option(
    "--output",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format"
)
@click.option(
    "--real",
    is_flag=True,
    help="Run against real database"
)
def show_quality_metrics(output: str, real: bool):
    """Show comprehensive quality metrics."""
    
    async def run_metrics():
        config = get_settings()
        service = DataQualityValidationService(config)
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Collecting quality metrics...", total=None)
            metrics = await service.get_quality_metrics()
            progress.update(task, completed=True)
        
        if output == "json":
            console.print(json.dumps({
                "raw_score": metrics.raw_score,
                "staging_score": metrics.staging_score,
                "curated_score": metrics.curated_score,
                "overall_score": metrics.overall_score,
                "total_records_processed": metrics.total_records_processed,
                "quality_gate_pass_rate": metrics.quality_gate_pass_rate,
                "data_freshness_score": metrics.data_freshness_score,
                "anomaly_detection_score": metrics.anomaly_detection_score,
                "timestamp": metrics.timestamp.isoformat()
            }, indent=2))
        else:
            _print_metrics_table(metrics)
    
    if not real:
        console.print("[yellow]Running in test mode. Use --real for live data validation.[/yellow]")
    
    try:
        asyncio.run(run_metrics())
    except Exception as e:
        console.print(f"[red]Metrics collection failed: {e}[/red]")
        raise click.ClickException(str(e))


@data_quality_cli.command(name="monitor")
@click.option(
    "--interval",
    type=int,
    default=60,
    help="Monitoring interval in seconds"
)
@click.option(
    "--alerts",
    is_flag=True,
    help="Enable alert generation"
)
@click.option(
    "--real",
    is_flag=True,
    help="Run against real database"
)
def monitor_quality(interval: int, alerts: bool, real: bool):
    """Continuous quality monitoring with alerts."""
    
    async def run_monitoring():
        config = get_settings()
        integration = DataQualityMonitoringIntegration(config)
        await integration.initialize()
        
        console.print(f"[green]Starting quality monitoring (interval: {interval}s)[/green]")
        if alerts:
            console.print("[yellow]Alert generation enabled[/yellow]")
        
        try:
            iteration = 0
            while True:
                iteration += 1
                console.print(f"\n[blue]--- Monitoring Iteration {iteration} ---[/blue]")
                
                start_time = datetime.now()
                
                try:
                    result = await integration.run_quality_validation_with_monitoring()
                    
                    # Display summary
                    console.print(f"[green]✓[/green] Overall Status: {result['overall_status']}")
                    console.print(f"[blue]ℹ[/blue] Execution Time: {result['execution_time_seconds']:.2f}s")
                    
                    if result['alerts_generated'] > 0:
                        console.print(f"[yellow]⚠[/yellow] Alerts Generated: {result['alerts_generated']}")
                    
                    # Show quality scores
                    metrics = result['quality_metrics']
                    console.print(f"[cyan]📊[/cyan] Quality Scores - Overall: {metrics.overall_score:.1%}, "
                                f"RAW: {metrics.raw_score:.1%}, "
                                f"STAGING: {metrics.staging_score:.1%}, "
                                f"CURATED: {metrics.curated_score:.1%}")
                    
                except Exception as e:
                    console.print(f"[red]✗[/red] Monitoring iteration failed: {e}")
                
                # Wait for next iteration
                elapsed = (datetime.now() - start_time).total_seconds()
                sleep_time = max(0, interval - elapsed)
                
                if sleep_time > 0:
                    console.print(f"[dim]Waiting {sleep_time:.1f}s until next check...[/dim]")
                    await asyncio.sleep(sleep_time)
                
        except KeyboardInterrupt:
            console.print("\n[yellow]Monitoring stopped by user[/yellow]")
        finally:
            await integration.cleanup()
    
    if not real:
        console.print("[yellow]Running in test mode. Use --real for live data monitoring.[/yellow]")
    
    try:
        asyncio.run(run_monitoring())
    except Exception as e:
        console.print(f"[red]Monitoring failed: {e}[/red]")
        raise click.ClickException(str(e))


@data_quality_cli.command(name="dashboard")
@click.option(
    "--output",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format"
)
@click.option(
    "--real",
    is_flag=True,
    help="Run against real database"
)
def show_dashboard_data(output: str, real: bool):
    """Show quality dashboard data."""
    
    async def run_dashboard():
        config = get_settings()
        integration = DataQualityMonitoringIntegration(config)
        await integration.initialize()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Collecting dashboard data...", total=None)
            data = await integration.get_quality_dashboard_data()
            progress.update(task, completed=True)
        
        if output == "json":
            console.print(json.dumps(data, indent=2, default=str))
        else:
            _print_dashboard_summary(data)
        
        await integration.cleanup()
    
    if not real:
        console.print("[yellow]Running in test mode. Use --real for live data validation.[/yellow]")
    
    try:
        asyncio.run(run_dashboard())
    except Exception as e:
        console.print(f"[red]Dashboard data collection failed: {e}[/red]")
        raise click.ClickException(str(e))


@data_quality_cli.command(name="health")
@click.option(
    "--real",
    is_flag=True,
    help="Run against real database"
)
def health_check(real: bool):
    """Quick quality health check."""
    
    async def run_health_check():
        config = get_settings()
        integration = DataQualityMonitoringIntegration(config)
        await integration.initialize()
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            task = progress.add_task("Running health check...", total=None)
            health = await integration.run_quality_health_check()
            progress.update(task, completed=True)
        
        # Display health status
        status = health["status"]
        if status == "healthy":
            console.print(f"[green]✓[/green] Data Quality: HEALTHY ({health['overall_score']:.1%})")
        elif status == "warning":
            console.print(f"[yellow]⚠[/yellow] Data Quality: WARNING ({health['overall_score']:.1%})")
        else:
            console.print(f"[red]✗[/red] Data Quality: CRITICAL ({health.get('overall_score', 0):.1%})")
        
        if 'details' in health:
            details = health['details']
            console.print(f"   Raw: {details['raw_score']:.1%} | "
                         f"Staging: {details['staging_score']:.1%} | "
                         f"Curated: {details['curated_score']:.1%}")
            console.print(f"   Records: {details['total_records']} | "
                         f"Freshness: {details['freshness_score']:.1%}")
        
        console.print(f"   Execution Time: {health['execution_time_seconds']:.3f}s")
        
        await integration.cleanup()
    
    if not real:
        console.print("[yellow]Running in test mode. Use --real for live data validation.[/yellow]")
    
    try:
        asyncio.run(run_health_check())
    except Exception as e:
        console.print(f"[red]Health check failed: {e}[/red]")
        raise click.ClickException(str(e))


# Helper functions for output formatting

def _print_reports_table(reports):
    """Print validation reports in table format."""
    table = Table(title="Data Quality Validation Results", box=box.ROUNDED)
    table.add_column("Stage", style="cyan", no_wrap=True)
    table.add_column("Overall Score", style="magenta")
    table.add_column("Status", style="bold")
    table.add_column("Passed", style="green")
    table.add_column("Warning", style="yellow")
    table.add_column("Failed", style="red")
    table.add_column("Records", style="blue")
    table.add_column("Time (ms)", style="dim")
    
    for stage, report in reports.items():
        status_color = {
            ValidationStatus.PASSED: "green",
            ValidationStatus.WARNING: "yellow",
            ValidationStatus.FAILED: "red",
            ValidationStatus.PENDING: "dim"
        }[report.overall_status]
        
        table.add_row(
            stage.value.upper(),
            f"{report.overall_score:.1%}",
            f"[{status_color}]{report.overall_status.value}[/{status_color}]",
            str(report.passed_validations),
            str(report.warning_validations),
            str(report.failed_validations),
            str(report.total_records),
            f"{report.execution_time_ms:.1f}"
        )
    
    console.print(table)


def _print_report_table(report):
    """Print single validation report in table format."""
    # Main report summary
    panel_content = f"""
[bold]Stage:[/bold] {report.stage.value.upper()}
[bold]Overall Score:[/bold] {report.overall_score:.1%}
[bold]Status:[/bold] {report.overall_status.value}
[bold]Total Records:[/bold] {report.total_records:,}
[bold]Execution Time:[/bold] {report.execution_time_ms:.1f}ms
"""
    
    status_color = {
        ValidationStatus.PASSED: "green",
        ValidationStatus.WARNING: "yellow", 
        ValidationStatus.FAILED: "red",
        ValidationStatus.PENDING: "dim"
    }[report.overall_status]
    
    console.print(Panel(panel_content, title=f"Quality Report - {report.stage.value.upper()}", 
                       border_style=status_color))
    
    # Validation rules details
    if report.validation_results:
        table = Table(title="Validation Rules", box=box.SIMPLE)
        table.add_column("Rule", style="cyan")
        table.add_column("Dimension", style="blue")
        table.add_column("Score", style="magenta")
        table.add_column("Status", style="bold")
        table.add_column("Message", style="dim")
        
        for result in report.validation_results:
            rule_status_color = {
                ValidationStatus.PASSED: "green",
                ValidationStatus.WARNING: "yellow",
                ValidationStatus.FAILED: "red",
                ValidationStatus.PENDING: "dim"
            }[result.status]
            
            dimension = result.metadata.get('rule_dimension', 'unknown')
            
            table.add_row(
                result.rule_name,
                dimension,
                f"{result.score:.1%}",
                f"[{rule_status_color}]{result.status.value}[/{rule_status_color}]",
                result.message[:60] + "..." if len(result.message) > 60 else result.message
            )
        
        console.print(table)


def _print_gates_table(gates):
    """Print quality gates in table format."""
    table = Table(title="Quality Gates Status", box=box.ROUNDED)
    table.add_column("Gate", style="cyan", no_wrap=True)
    table.add_column("Status", style="bold")
    table.add_column("Description", style="dim")
    
    gate_descriptions = {
        "raw_to_staging": "RAW data meets staging promotion criteria",
        "staging_to_curated": "STAGING data meets curated promotion criteria", 
        "curated_ready": "CURATED data ready for production use",
        "no_critical_failures": "No critical quality failures detected",
        "overall_pipeline_ready": "Overall pipeline ready for operation"
    }
    
    for gate_name, passed in gates.items():
        status_text = "[green]✓ PASS[/green]" if passed else "[red]✗ FAIL[/red]"
        description = gate_descriptions.get(gate_name, "Quality gate check")
        
        table.add_row(
            gate_name.replace("_", " ").title(),
            status_text,
            description
        )
    
    console.print(table)


def _print_metrics_table(metrics):
    """Print quality metrics in table format."""
    panel_content = f"""
[bold]Overall Pipeline Score:[/bold] {metrics.overall_score:.1%}

[bold]Stage Scores:[/bold]
  • RAW: {metrics.raw_score:.1%}
  • STAGING: {metrics.staging_score:.1%}  
  • CURATED: {metrics.curated_score:.1%}

[bold]Pipeline Metrics:[/bold]
  • Total Records Processed: {metrics.total_records_processed:,}
  • Quality Gate Pass Rate: {metrics.quality_gate_pass_rate:.1%}
  • Data Freshness Score: {metrics.data_freshness_score:.1%}
  • Anomaly Detection Score: {metrics.anomaly_detection_score:.1%}

[bold]Timestamp:[/bold] {metrics.timestamp.strftime('%Y-%m-%d %H:%M:%S')}
"""
    
    # Determine overall health color
    if metrics.overall_score >= 0.95:
        border_color = "green"
    elif metrics.overall_score >= 0.85:
        border_color = "yellow"
    else:
        border_color = "red"
    
    console.print(Panel(panel_content, title="Quality Metrics Dashboard", 
                       border_style=border_color))


def _print_summary(reports):
    """Print summary of all reports."""
    total_records = sum(report.total_records for report in reports.values())
    overall_score = sum(report.overall_score for report in reports.values()) / len(reports)
    
    console.print(Panel(
        f"[bold]Pipeline Quality Summary[/bold]\n\n"
        f"Overall Score: {overall_score:.1%}\n"
        f"Total Records: {total_records:,}\n"
        f"Stages Validated: {len(reports)}\n"
        f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        title="Quality Summary"
    ))


def _print_dashboard_summary(data):
    """Print dashboard data summary."""
    console.print(Panel(
        f"[bold]Quality Dashboard Summary[/bold]\n\n"
        f"Overall Status: {data.get('overall_status', 'unknown')}\n"
        f"Overall Score: {data.get('overall_score', 0):.1%}\n"
        f"Total Records: {data.get('total_records', 0):,}\n"
        f"Pass Rate: {data.get('pass_rate', 0):.1%}\n"
        f"Active Alerts: {len(data.get('alerts', []))}\n"
        f"Timestamp: {data.get('timestamp', 'unknown')}",
        title="Dashboard Data"
    ))


def _print_reports_json(reports):
    """Print reports in JSON format."""
    json_data = {}
    for stage, report in reports.items():
        json_data[stage.value] = {
            "overall_score": report.overall_score,
            "overall_status": report.overall_status.value,
            "passed_validations": report.passed_validations,
            "warning_validations": report.warning_validations,
            "failed_validations": report.failed_validations,
            "total_records": report.total_records,
            "execution_time_ms": report.execution_time_ms,
            "timestamp": report.timestamp.isoformat()
        }
    console.print(json.dumps(json_data, indent=2))


def _print_report_json(report):
    """Print single report in JSON format."""
    json_data = {
        "stage": report.stage.value,
        "overall_score": report.overall_score,
        "overall_status": report.overall_status.value,
        "passed_validations": report.passed_validations,
        "warning_validations": report.warning_validations,
        "failed_validations": report.failed_validations,
        "total_records": report.total_records,
        "execution_time_ms": report.execution_time_ms,
        "timestamp": report.timestamp.isoformat(),
        "validation_results": [
            {
                "rule_name": result.rule_name,
                "status": result.status.value,
                "score": result.score,
                "message": result.message,
                "execution_time_ms": result.execution_time_ms
            }
            for result in report.validation_results
        ]
    }
    console.print(json.dumps(json_data, indent=2))


def _print_report_summary(report):
    """Print single report summary."""
    console.print(Panel(
        f"[bold]{report.stage.value.upper()} Stage Summary[/bold]\n\n"
        f"Score: {report.overall_score:.1%}\n"
        f"Status: {report.overall_status.value}\n"
        f"Records: {report.total_records:,}\n"
        f"Validations: {report.passed_validations}✓ {report.warning_validations}⚠ {report.failed_validations}✗",
        title=f"{report.stage.value.upper()} Quality"
    ))


def _save_reports(reports, filename):
    """Save multiple reports to file."""
    data = {}
    for stage, report in reports.items():
        data[stage.value] = {
            "overall_score": report.overall_score,
            "overall_status": report.overall_status.value,
            "validation_results": [
                {
                    "rule_name": result.rule_name,
                    "status": result.status.value,
                    "score": result.score,
                    "threshold": result.threshold,
                    "warning_threshold": result.warning_threshold,
                    "message": result.message,
                    "metadata": result.metadata,
                    "execution_time_ms": result.execution_time_ms,
                    "timestamp": result.timestamp.isoformat()
                }
                for result in report.validation_results
            ],
            "total_records": report.total_records,
            "execution_time_ms": report.execution_time_ms,
            "data_sources": report.data_sources,
            "quality_trends": report.quality_trends,
            "timestamp": report.timestamp.isoformat()
        }
    
    with open(filename, 'w') as f:
        json.dump({
            "report_type": "full_pipeline_quality_validation",
            "generated_at": datetime.now().isoformat(),
            "stages": data
        }, f, indent=2)


def _save_report(report, filename):
    """Save single report to file."""
    data = {
        "report_type": "single_stage_quality_validation",
        "generated_at": datetime.now().isoformat(),
        "stage": report.stage.value,
        "overall_score": report.overall_score,
        "overall_status": report.overall_status.value,
        "validation_results": [
            {
                "rule_name": result.rule_name,
                "status": result.status.value,
                "score": result.score,
                "threshold": result.threshold,
                "warning_threshold": result.warning_threshold,
                "message": result.message,
                "metadata": result.metadata,
                "execution_time_ms": result.execution_time_ms,
                "timestamp": result.timestamp.isoformat()
            }
            for result in report.validation_results
        ],
        "total_records": report.total_records,
        "execution_time_ms": report.execution_time_ms,
        "data_sources": report.data_sources,
        "quality_trends": report.quality_trends,
        "timestamp": report.timestamp.isoformat()
    }
    
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)