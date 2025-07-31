"""
ML Commands Module
Unified entry point for all ML-related CLI commands

This module addresses critical issues identified in the code review:
- Import dependency validation with graceful fallbacks
- Centralized configuration integration
- Comprehensive input validation
- Security and error handling improvements
"""

import click
from rich.console import Console

from ....core.config import get_settings

console = Console()

# Import guards for ML training module - addresses critical code review issue
ML_TRAINING_AVAILABLE = True
ML_TRAINING_ERROR = None

try:
    from .ml_training import ml_training_cli
except ImportError as e:
    ML_TRAINING_AVAILABLE = False
    ML_TRAINING_ERROR = str(e)
    ml_training_cli = None


def _validate_ml_setup():
    """Validate ML infrastructure setup before command execution."""
    config = get_settings()
    
    # Check database configuration
    if not config.database.is_configuration_complete():
        issues = config.database.get_connection_issues()
        console.print("[red]❌ Database configuration issues:[/red]")
        for issue in issues:
            console.print(f"  • {issue}")
        console.print("\n[yellow]Fix database configuration and try again.[/yellow]")
        raise click.Abort()
    
    # Check MLflow configuration
    if not config.mlflow.tracking_uri:
        console.print("[red]❌ MLflow tracking URI not configured[/red]")
        console.print("[yellow]Set MLFLOW_TRACKING_URI environment variable or update config.toml[/yellow]")
        raise click.Abort()
    
    return config


@click.group(name="ml")
def ml():
    """
    Machine Learning pipeline management commands
    
    Provides access to training, evaluation, and model management functionality.
    Includes comprehensive dependency validation and error handling.
    """
    pass


@ml.command("setup")
def setup_ml():
    """Set up ML infrastructure and validate dependencies."""
    console.print("[bold blue]ML Infrastructure Setup[/bold blue]")
    
    try:
        config = _validate_ml_setup()
        console.print("[green]✅ Database configuration is valid[/green]")
        console.print(f"[green]✅ MLflow configured: {config.mlflow.tracking_uri}[/green]")
        
        console.print("\n[bold]Next Steps:[/bold]")
        console.print("1. Start MLflow: [bold cyan]docker-compose up -d mlflow[/bold cyan]")
        console.print("2. Start Redis: [bold cyan]docker-compose up -d redis[/bold cyan]")
        console.print("3. Test connection: [bold cyan]uv run -m src.interfaces.cli ml test-connection[/bold cyan]")
        
    except Exception as e:
        console.print(f"[red]❌ Setup validation failed: {e}[/red]")
        raise click.Abort()


@ml.command("test-connection")
def test_connection():
    """Test connection to ML infrastructure (MLflow, Redis, Database)."""
    console.print("[bold blue]Testing ML Infrastructure Connections[/bold blue]")
    
    config = _validate_ml_setup()
    
    results = {}
    
    # Test MLflow connection
    try:
        import mlflow
        mlflow.set_tracking_uri(config.mlflow.tracking_uri)
        client = mlflow.tracking.MlflowClient()
        experiments = client.search_experiments()
        results["mlflow"] = {
            "status": "✅ Connected",
            "details": f"Found {len(experiments)} experiments",
            "url": config.mlflow.tracking_uri
        }
    except Exception as e:
        results["mlflow"] = {
            "status": "❌ Failed",
            "details": str(e),
            "url": config.mlflow.tracking_uri
        }
    
    # Test Redis connection
    try:
        import redis
        r = redis.Redis(
            host=config.ml.redis.host, 
            port=config.ml.redis.port, 
            db=config.ml.redis.database,
            password=config.ml.redis.password if config.ml.redis.password != "${REDIS_PASSWORD}" else None
        )
        r.ping()
        results["redis"] = {
            "status": "✅ Connected",
            "details": "Redis ping successful",
            "url": f"{config.ml.redis.host}:{config.ml.redis.port}"
        }
    except Exception as e:
        results["redis"] = {
            "status": "❌ Failed", 
            "details": str(e),
            "url": f"{config.ml.redis.host}:{config.ml.redis.port}"
        }
    
    # Test Database connection (repository pattern compliant)
    try:
        from ....data.database.connection import get_connection
        import asyncio
        
        async def test_db():
            async with get_connection() as conn:
                result = await conn.fetchval("SELECT COUNT(*) FROM curated.ml_experiments")
                return result
        
        result = asyncio.run(test_db())
        results["database"] = {
            "status": "✅ Connected",
            "details": f"Found {result} ML experiments",
            "url": config.database.masked_connection_string
        }
    except Exception as e:
        results["database"] = {
            "status": "❌ Failed",
            "details": str(e),
            "url": config.database.masked_connection_string
        }
    
    # Display results in a table
    from rich.table import Table
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Service", style="cyan")
    table.add_column("Status", style="green")
    table.add_column("Details", style="yellow")
    table.add_column("URL", style="blue")
    
    for service, info in results.items():
        table.add_row(
            service.title(),
            info["status"],
            info["details"],
            info["url"]
        )
    
    console.print(table)
    
    # Summary
    connected = sum(1 for r in results.values() if "✅" in r["status"])
    total = len(results)
    
    if connected == total:
        console.print(f"\n[green]🎉 All {total} services connected successfully![/green]")
    else:
        console.print(f"\n[yellow]⚠️  {connected}/{total} services connected[/yellow]")
        if connected < total:
            console.print("\n[dim]Troubleshooting:[/dim]")
            console.print("• Check Docker containers: [bold]docker-compose ps[/bold]")
            console.print("• Start services: [bold]docker-compose up -d mlflow redis postgres[/bold]")
            console.print("• Check configuration: [bold]uv run -m src.interfaces.cli ml setup[/bold]")


# Conditionally add ML training commands with proper error handling
if ML_TRAINING_AVAILABLE and ml_training_cli is not None:
    ml.add_command(ml_training_cli, name="training")
else:
    @ml.command("training")
    def training_unavailable():
        """ML training commands (currently unavailable)."""
        console.print(f"[red]❌ ML training module not available: {ML_TRAINING_ERROR}[/red]")
        console.print("\n[yellow]This may be because:[/yellow]")
        console.print("• ML training services are not yet implemented")
        console.print("• Missing Python dependencies")
        console.print("• Import path issues")
        console.print("\n[bold]Try:[/bold]")
        console.print("• [bold cyan]uv run -m src.interfaces.cli ml setup[/bold cyan]")
        console.print("• [bold cyan]uv run -m src.interfaces.cli ml test-connection[/bold cyan]")
        console.print("• Check ML module implementations in src/ml/")
        raise click.Abort()


# Export the main command group
__all__ = ["ml"]
