"""
ML CLI Commands

Command-line interface for ML experiment management, training, and prediction.
Integrates with existing Docker infrastructure (MLflow, Redis, FastAPI).
"""

import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Optional

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text

from ....ml.experiments.experiment_manager import experiment_manager

console = Console()

@click.group()
def ml():
    """ML experiment management and training commands."""
    pass


@ml.command("create-experiment")
@click.argument("name")
@click.option("--description", "-d", help="Experiment description")
@click.option("--model-type", "-t", default="classification", help="Model type")
@click.option("--target", default="sharp_action", help="Target variable")
@click.option("--start-date", help="Training data start date (YYYY-MM-DD)")
@click.option("--end-date", help="Training data end date (YYYY-MM-DD)")
def create_experiment(name, description, model_type, target, start_date, end_date):
    """Create a new ML experiment."""
    console.print(f"[bold blue]Creating ML experiment:[/bold blue] {name}")
    
    async def _create_experiment():
        try:
            result = await experiment_manager.create_experiment(
                name=name,
                description=description,
                model_type=model_type,
                target_variable=target,
                data_start_date=start_date,
                data_end_date=end_date
            )
            
            # Display results
            console.print(Panel(
                f"[green]✅ Experiment created successfully[/green]\\n\\n"
                f"[bold]Experiment ID:[/bold] {result['experiment_id']}\\n"
                f"[bold]MLflow ID:[/bold] {result['mlflow_experiment_id']}\\n"
                f"[bold]Name:[/bold] {result['experiment_name']}\\n"
                f"[bold]Model Type:[/bold] {model_type}\\n"
                f"[bold]Target:[/bold] {target}",
                title="Experiment Created",
                border_style="green"
            ))
            
            # Display next steps
            console.print(f"\\n[dim]Next steps:[/dim]")
            console.print(f"• Start training: [bold]uv run -m src.interfaces.cli ml train {name}[/bold]")
            console.print(f"• View experiment: [bold]uv run -m src.interfaces.cli ml experiment-info {name}[/bold]")
            console.print(f"• MLflow UI: [bold]http://localhost:5001[/bold]")
            
        except Exception as e:
            console.print(f"[red]❌ Failed to create experiment: {e}[/red]")
            raise click.Abort()
    
    asyncio.run(_create_experiment())


@ml.command("list-experiments")
@click.option("--include-archived", "-a", is_flag=True, help="Include archived experiments")
def list_experiments(include_archived):
    """List all ML experiments."""
    console.print("[bold blue]ML Experiments[/bold blue]")
    
    async def _list_experiments():
        try:
            experiments = await experiment_manager.list_experiments(include_archived=include_archived)
            
            if not experiments:
                console.print("[dim]No experiments found.[/dim]")
                return
            
            # Create table
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("ID", style="cyan", no_wrap=True)
            table.add_column("Name", style="green")
            table.add_column("Status", style="yellow")
            table.add_column("Best Accuracy", style="blue")
            table.add_column("Created", style="dim")
            
            for exp in experiments:
                table.add_row(
                    str(exp["experiment_id"]),
                    exp["name"],
                    exp["status"],
                    f"{exp['best_accuracy']:.3f}" if exp["best_accuracy"] else "N/A",
                    exp["created_at"][:10] if exp["created_at"] else "N/A"
                )
            
            console.print(table)
            console.print(f"\\n[dim]Found {len(experiments)} experiments[/dim]")
            
        except Exception as e:
            console.print(f"[red]❌ Failed to list experiments: {e}[/red]")
            raise click.Abort()
    
    asyncio.run(_list_experiments())


@ml.command("experiment-info")
@click.argument("name")
def experiment_info(name):
    """Get detailed experiment information."""
    console.print(f"[bold blue]Experiment Info:[/bold blue] {name}")
    
    async def _get_experiment_info():
        try:
            info = await experiment_manager.get_experiment_summary(name)
            
            # Display experiment details
            console.print(Panel(
                f"[bold]Experiment ID:[/bold] {info['experiment_id']}\\n"
                f"[bold]Name:[/bold] {info['experiment_name']}\\n"
                f"[bold]Status:[/bold] {info['database_record']['status'] if info['database_record'] else 'N/A'}\\n"
                f"[bold]Description:[/bold] {info['database_record']['experiment_description'] if info['database_record'] else 'N/A'}\\n"
                f"[bold]Target:[/bold] {info['database_record']['prediction_target'] if info['database_record'] else 'N/A'}\\n"
                f"[bold]Model Type:[/bold] {info['database_record']['model_type'] if info['database_record'] else 'N/A'}",
                title="Experiment Details",
                border_style="blue"
            ))
            
            # Display run analysis
            run_analysis = info['run_analysis']
            console.print(Panel(
                f"[bold]Total Runs:[/bold] {run_analysis['total_runs']}\\n"
                f"[bold]Completed:[/bold] {run_analysis['completed_runs']}\\n"
                f"[bold]Failed:[/bold] {run_analysis['failed_runs']}\\n"
                f"[bold]Best Accuracy:[/bold] {run_analysis['best_accuracy']:.4f}\\n"
                f"[bold]Best Run ID:[/bold] {run_analysis['best_run_id'] or 'N/A'}",
                title="Run Analysis",
                border_style="green"
            ))
            
            # Display links
            console.print(f"\\n[dim]Links:[/dim]")
            console.print(f"• MLflow UI: [bold]http://localhost:5001/#/experiments/{info['experiment_id']}[/bold]")
            console.print(f"• FastAPI: [bold]http://localhost:8000/api/v1/models[/bold]")
            
        except Exception as e:
            console.print(f"[red]❌ Failed to get experiment info: {e}[/red]")
            raise click.Abort()
    
    asyncio.run(_get_experiment_info())


@ml.command("test-connection")
def test_connection():
    """Test connection to ML infrastructure (MLflow, Redis, Database)."""
    console.print("[bold blue]Testing ML Infrastructure Connections[/bold blue]")
    
    async def _test_connections():
        results = {}
        
        # Test MLflow connection
        try:
            import mlflow
            mlflow.set_tracking_uri("http://localhost:5001")
            client = mlflow.tracking.MlflowClient()
            experiments = client.search_experiments()
            results["mlflow"] = {
                "status": "✅ Connected",
                "details": f"Found {len(experiments)} experiments",
                "url": "http://localhost:5001"
            }
        except Exception as e:
            results["mlflow"] = {
                "status": "❌ Failed",
                "details": str(e),
                "url": "http://localhost:5001"
            }
        
        # Test Redis connection
        try:
            import redis
            r = redis.Redis(host='localhost', port=6379, db=0)
            r.ping()
            results["redis"] = {
                "status": "✅ Connected",
                "details": "Redis ping successful",
                "url": "localhost:6379"
            }
        except Exception as e:
            results["redis"] = {
                "status": "❌ Failed", 
                "details": str(e),
                "url": "localhost:6379"
            }
        
        # Test Database connection
        try:
            from ....data.database.connection import get_connection
            async with get_connection() as conn:
                result = await conn.fetchval("SELECT COUNT(*) FROM curated.ml_experiments")
                results["database"] = {
                    "status": "✅ Connected",
                    "details": f"Found {result} ML experiments",
                    "url": "PostgreSQL"
                }
        except Exception as e:
            results["database"] = {
                "status": "❌ Failed",
                "details": str(e),
                "url": "PostgreSQL"
            }
        
        # Test FastAPI
        try:
            import httpx
            async with httpx.AsyncClient() as client:
                response = await client.get("http://localhost:8000/health")
                if response.status_code == 200:
                    results["fastapi"] = {
                        "status": "✅ Connected",
                        "details": "FastAPI health check passed",
                        "url": "http://localhost:8000"
                    }
                else:
                    results["fastapi"] = {
                        "status": "❌ Failed",
                        "details": f"HTTP {response.status_code}",
                        "url": "http://localhost:8000"
                    }
        except Exception as e:
            results["fastapi"] = {
                "status": "❌ Failed",
                "details": str(e),
                "url": "http://localhost:8000"
            }
        
        # Display results
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
            console.print(f"\\n[green]🎉 All {total} services connected successfully![/green]")
        else:
            console.print(f"\\n[yellow]⚠️  {connected}/{total} services connected[/yellow]")
    
    asyncio.run(_test_connections())


@ml.command("health")
def health():
    """Check ML system health."""
    console.print("[bold blue]ML System Health Check[/bold blue]")
    
    async def _health_check():
        try:
            import httpx
            
            # Check FastAPI health
            async with httpx.AsyncClient() as client:
                response = await client.get("http://localhost:8000/health")
                health_data = response.json()
            
            # Display health status
            status_color = "green" if health_data["status"] == "healthy" else "red"
            console.print(Panel(
                f"[{status_color}]{health_data['status'].upper()}[/{status_color}]\\n\\n"
                f"[bold]Service:[/bold] {health_data['service']}\\n"
                f"[bold]Version:[/bold] {health_data['version']}\\n"
                f"[bold]Timestamp:[/bold] {health_data['timestamp']}",
                title="FastAPI Health",
                border_style=status_color
            ))
            
            # Display individual checks
            checks_table = Table(show_header=True, header_style="bold magenta")
            checks_table.add_column("Check", style="cyan")
            checks_table.add_column("Status", style="green")
            checks_table.add_column("Message", style="yellow")
            
            for check_name, check_info in health_data["checks"].items():
                status_icon = "✅" if check_info["status"] == "healthy" else "❌"
                checks_table.add_row(
                    check_name.title(),
                    f"{status_icon} {check_info['status']}",
                    check_info["message"]
                )
            
            console.print(checks_table)
            
        except Exception as e:
            console.print(f"[red]❌ Health check failed: {e}[/red]")
            raise click.Abort()
    
    asyncio.run(_health_check())


@ml.command("train")
@click.argument("experiment_name")
@click.option("--model-type", "-t", default="lightgbm", help="Model type to train")
@click.option("--target", default="total_over", help="Prediction target")
@click.option("--start-date", help="Training data start date (YYYY-MM-DD)")
@click.option("--end-date", help="Training data end date (YYYY-MM-DD)")
@click.option("--test-size", default=0.2, type=float, help="Test set size (0.0-1.0)")
def train_model(experiment_name, model_type, target, start_date, end_date, test_size):
    """Train a model using existing experiment and training infrastructure."""
    console.print(f"[bold blue]Training ML model:[/bold blue] {experiment_name}")
    
    async def _train_model():
        try:
            # Import training components
            from ....ml.training.lightgbm_trainer import LightGBMTrainer
            from ....ml.features.feature_pipeline import FeaturePipeline
            from datetime import datetime
            
            console.print("🔧 Initializing training components...")
            
            # Initialize trainer with existing MLflow integration
            trainer = LightGBMTrainer(
                experiment_name=experiment_name,
                model_version="v2.1"
            )
            
            # Set date range
            if start_date and end_date:
                start_dt = datetime.fromisoformat(start_date)
                end_dt = datetime.fromisoformat(end_date)
            else:
                # Default to last 90 days of data
                end_dt = datetime.now()
                start_dt = end_dt - timedelta(days=90)
            
            console.print(f"📅 Training data range: {start_dt.date()} to {end_dt.date()}")
            
            # Start training with existing infrastructure
            console.print("🚀 Starting model training...")
            training_results = await trainer.train_models(
                start_date=start_dt,
                end_date=end_dt,
                prediction_targets=[target] if target != "all" else None,
                test_size=test_size
            )
            
            # Display results
            console.print(Panel(
                f"[green]✅ Training completed successfully[/green]\\n\\n"
                f"[bold]Model Performance:[/bold]\\n"
                f"• Accuracy: {training_results.get('accuracy', 'N/A'):.4f}\\n"
                f"• Precision: {training_results.get('precision', 'N/A'):.4f}\\n"
                f"• Recall: {training_results.get('recall', 'N/A'):.4f}\\n"
                f"• F1 Score: {training_results.get('f1_score', 'N/A'):.4f}\\n"
                f"• ROC AUC: {training_results.get('roc_auc', 'N/A'):.4f}\\n\\n"
                f"[bold]Training Details:[/bold]\\n"
                f"• Model Type: {model_type}\\n"
                f"• Target: {target}\\n"
                f"• Training Samples: {training_results.get('n_train', 'N/A')}\\n"
                f"• Test Samples: {training_results.get('n_test', 'N/A')}\\n"
                f"• MLflow Run ID: {training_results.get('run_id', 'N/A')}",
                title="Training Results",
                border_style="green"
            ))
            
            # Display next steps
            console.print(f"\\n[dim]Next steps:[/dim]")
            console.print(f"• View in MLflow: [bold]http://localhost:5001/#/experiments/{training_results.get('experiment_id', '')}[/bold]")
            console.print(f"• Make predictions: [bold]uv run -m src.interfaces.cli ml predict --model {training_results.get('model_name', experiment_name)}[/bold]")
            console.print(f"• Test API: [bold]curl http://localhost:8000/api/v1/predict[/bold]")
            
        except Exception as e:
            console.print(f"[red]❌ Training failed: {e}[/red]")
            raise click.Abort()
    
    asyncio.run(_train_model())


@ml.command("predict")
@click.option("--model", "-m", help="Model name to use for prediction")
@click.option("--game-id", "-g", help="Specific game ID to predict")
@click.option("--date", "-d", help="Date to predict games for (YYYY-MM-DD)")
@click.option("--batch-size", default=10, help="Batch size for multiple predictions")
def make_predictions(model, game_id, date, batch_size):
    """Make predictions using trained models."""
    console.print("[bold blue]Making ML predictions[/bold blue]")
    
    async def _make_predictions():
        try:
            from ....ml.services.prediction_service import PredictionService
            from datetime import datetime
            
            console.print("🔮 Initializing prediction service...")
            
            # Initialize prediction service
            prediction_service = PredictionService()
            
            if game_id:
                # Single game prediction
                console.print(f"📊 Predicting for game: {game_id}")
                
                prediction = await prediction_service.predict_single_game(
                    game_id=game_id,
                    model_name=model
                )
                
                # Display prediction results
                console.print(Panel(
                    f"[bold]Game ID:[/bold] {prediction.game_id}\\n"
                    f"[bold]Model:[/bold] {prediction.model_name} (v{prediction.model_version})\\n\\n"
                    f"[bold]Predictions:[/bold]\\n"
                    f"• Total Over: {prediction.total_over_probability:.3f} ({prediction.total_over_binary})\\n"
                    f"• Home ML: {prediction.home_ml_probability:.3f} ({prediction.home_ml_binary})\\n"
                    f"• Home Spread: {prediction.home_spread_probability:.3f} ({prediction.home_spread_binary})\\n\\n"
                    f"[bold]Confidence:[/bold]\\n"
                    f"• Overall: {prediction.confidence_score:.3f}\\n"
                    f"• Model Certainty: {prediction.model_certainty:.3f}",
                    title="Prediction Results",
                    border_style="blue"
                ))
                
            elif date:
                # Date-based batch predictions
                prediction_date = datetime.fromisoformat(date)
                console.print(f"📅 Predicting for date: {prediction_date.date()}")
                
                predictions = await prediction_service.predict_games_for_date(
                    date=prediction_date,
                    model_name=model,
                    batch_size=batch_size
                )
                
                # Display batch results
                console.print(f"\\n📊 Generated {len(predictions)} predictions:")
                
                table = Table(show_header=True, header_style="bold magenta")
                table.add_column("Game ID", style="cyan")
                table.add_column("Total Over", style="green")
                table.add_column("Home ML", style="yellow")
                table.add_column("Confidence", style="blue")
                
                for pred in predictions[:10]:  # Show first 10
                    table.add_row(
                        pred.game_id,
                        f"{pred.total_over_probability:.3f}",
                        f"{pred.home_ml_probability:.3f}",
                        f"{pred.confidence_score:.3f}"
                    )
                
                console.print(table)
                
                if len(predictions) > 10:
                    console.print(f"[dim]... and {len(predictions) - 10} more predictions[/dim]")
            
            else:
                console.print("[yellow]⚠️  Please specify either --game-id or --date[/yellow]")
                return
            
        except Exception as e:
            console.print(f"[red]❌ Prediction failed: {e}[/red]")
            raise click.Abort()
    
    asyncio.run(_make_predictions())


@ml.command("evaluate")
@click.argument("experiment_name")
@click.option("--start-date", help="Evaluation period start date (YYYY-MM-DD)")
@click.option("--end-date", help="Evaluation period end date (YYYY-MM-DD)")
@click.option("--target", default="total_over", help="Prediction target to evaluate")
def evaluate_model(experiment_name, start_date, end_date, target):
    """Evaluate model performance on historical data."""
    console.print(f"[bold blue]Evaluating model:[/bold blue] {experiment_name}")
    
    async def _evaluate_model():
        try:
            from ....ml.training.lightgbm_trainer import LightGBMTrainer
            from datetime import datetime
            
            console.print("📊 Initializing model evaluation...")
            
            # Initialize trainer for evaluation
            trainer = LightGBMTrainer(experiment_name=experiment_name)
            
            # Set evaluation period
            if start_date and end_date:
                start_dt = datetime.fromisoformat(start_date)
                end_dt = datetime.fromisoformat(end_date)
            else:
                # Default to last 30 days
                end_dt = datetime.now()
                start_dt = end_dt - timedelta(days=30)
            
            console.print(f"📅 Evaluation period: {start_dt.date()} to {end_dt.date()}")
            
            # Run evaluation
            console.print("🔍 Running model evaluation...")
            eval_results = await trainer.evaluate_model_performance(
                model_name=target,
                evaluation_start=start_dt,
                evaluation_end=end_dt
            )
            
            # Display evaluation results
            console.print(Panel(
                f"[bold]Model Performance Evaluation[/bold]\\n\\n"
                f"[bold]Classification Metrics:[/bold]\\n"
                f"• Accuracy: {eval_results['performance_metrics'].get('accuracy', 'N/A'):.4f}\\n"
                f"• Precision: {eval_results['performance_metrics'].get('precision', 'N/A'):.4f}\\n"
                f"• Recall: {eval_results['performance_metrics'].get('recall', 'N/A'):.4f}\\n"
                f"• F1 Score: {eval_results['performance_metrics'].get('f1_score', 'N/A'):.4f}\\n"
                f"• ROC AUC: {eval_results['performance_metrics'].get('roc_auc', 'N/A'):.4f}\\n\\n"
                f"[bold]Evaluation Details:[/bold]\\n"
                f"• Model: {eval_results.get('model_name', 'N/A')}\\n"
                f"• Samples Evaluated: {eval_results.get('samples_evaluated', 'N/A')}\\n"
                f"• Feature Count: {eval_results.get('feature_count', 'N/A')}",
                title="Evaluation Results",
                border_style="green"
            ))
            
        except Exception as e:
            console.print(f"[red]❌ Evaluation failed: {e}[/red]")
            raise click.Abort()
    
    asyncio.run(_evaluate_model())


@ml.command("registry-status")
def registry_status():
    """Show model registry status and statistics."""
    console.print("[bold blue]Model Registry Status[/bold blue]")
    
    async def _registry_status():
        try:
            from ....ml.registry.model_registry import model_registry
            
            console.print("🔍 Checking model registry...")
            
            # Initialize registry
            await model_registry.initialize()
            
            # Get registry stats
            stats = model_registry.get_registry_stats()
            
            if "error" in stats:
                console.print(f"[red]❌ Registry error: {stats['error']}[/red]")
                return
            
            # Display overall stats
            console.print(Panel(
                f"[bold]Registry Overview[/bold]\\n\\n"
                f"[bold]Total Models:[/bold] {stats['total_models']}\\n"
                f"[bold]Total Versions:[/bold] {stats['total_versions']}\\n\\n"
                f"[bold]Models by Stage:[/bold]\\n"
                f"• Production: {stats['models_by_stage']['Production']}\\n"
                f"• Staging: {stats['models_by_stage']['Staging']}\\n"
                f"• None: {stats['models_by_stage']['None']}\\n"
                f"• Archived: {stats['models_by_stage']['Archived']}",
                title="Model Registry",
                border_style="blue"
            ))
            
            # Display individual models
            if stats['models']:
                table = Table(show_header=True, header_style="bold magenta")
                table.add_column("Model Name", style="cyan")
                table.add_column("Total Versions", style="green")
                table.add_column("Production", style="yellow")
                table.add_column("Staging", style="blue")
                table.add_column("Archived", style="dim")
                
                for model in stats['models']:
                    table.add_row(
                        model['name'],
                        str(model['total_versions']),
                        str(model['stages']['Production']),
                        str(model['stages']['Staging']),
                        str(model['stages']['Archived'])
                    )
                
                console.print(table)
            
        except Exception as e:
            console.print(f"[red]❌ Registry status failed: {e}[/red]")
            raise click.Abort()
    
    asyncio.run(_registry_status())


@ml.command("promote")
@click.argument("model_name")
@click.argument("version")
@click.argument("stage", type=click.Choice(["staging", "production"]))
@click.option("--force", is_flag=True, help="Skip validation checks")
def promote_model(model_name, version, stage, force):
    """Promote model version to staging or production."""
    console.print(f"[bold blue]Promoting model:[/bold blue] {model_name} v{version} to {stage}")
    
    async def _promote_model():
        try:
            from ....ml.registry.model_registry import model_registry
            
            console.print("🚀 Initializing model registry...")
            
            # Initialize registry
            await model_registry.initialize()
            
            # Promote model
            if stage == "staging":
                success = await model_registry.promote_to_staging(model_name, version, force)
            else:
                success = await model_registry.promote_to_production(model_name, version, force)
            
            if success:
                console.print(Panel(
                    f"[green]✅ Successfully promoted {model_name} v{version} to {stage}[/green]",
                    title="Promotion Successful",
                    border_style="green"
                ))
            else:
                console.print(f"[red]❌ Failed to promote {model_name} v{version} to {stage}[/red]")
                console.print("Check logs for validation failures or other issues")
            
        except Exception as e:
            console.print(f"[red]❌ Promotion failed: {e}[/red]")
            raise click.Abort()
    
    asyncio.run(_promote_model())


@ml.command("rollback")
@click.argument("model_name")
def rollback_model(model_name):
    """Rollback production model to previous version."""
    console.print(f"[bold blue]Rolling back model:[/bold blue] {model_name}")
    
    async def _rollback_model():
        try:
            from ....ml.registry.model_registry import model_registry
            
            console.print("⏪ Initializing model registry...")
            
            # Initialize registry
            await model_registry.initialize()
            
            # Rollback model
            success = await model_registry.rollback_production(model_name)
            
            if success:
                console.print(Panel(
                    f"[green]✅ Successfully rolled back {model_name} to previous version[/green]",
                    title="Rollback Successful",
                    border_style="green"
                ))
            else:
                console.print(f"[red]❌ Failed to rollback {model_name}[/red]")
                console.print("No previous version found or rollback failed")
            
        except Exception as e:
            console.print(f"[red]❌ Rollback failed: {e}[/red]")
            raise click.Abort()
    
    asyncio.run(_rollback_model())


@ml.command("retrain")
@click.argument("model_name")
@click.option("--days", default=90, help="Sliding window days for retraining")
@click.option("--schedule", help="Schedule automated retraining (cron format)")
def retrain_model(model_name, days, schedule):
    """Trigger model retraining or schedule automated retraining."""
    if schedule:
        console.print(f"[bold blue]Scheduling automated retraining:[/bold blue] {model_name}")
    else:
        console.print(f"[bold blue]Triggering manual retraining:[/bold blue] {model_name}")
    
    async def _retrain_model():
        try:
            from ....ml.workflows.automated_retraining import automated_retraining_service, RetrainingConfig
            
            console.print("🔄 Initializing retraining service...")
            
            # Initialize service
            await automated_retraining_service.initialize()
            
            if schedule:
                # Schedule automated retraining
                config = RetrainingConfig(
                    model_name=model_name,
                    schedule_cron=schedule,
                    sliding_window_days=days,
                    auto_promote_to_staging=True
                )
                
                success = await automated_retraining_service.schedule_model_retraining(config)
                
                if success:
                    console.print(Panel(
                        f"[green]✅ Scheduled automated retraining for {model_name}[/green]\\n\\n"
                        f"[bold]Schedule:[/bold] {schedule}\\n"
                        f"[bold]Sliding Window:[/bold] {days} days\\n"
                        f"[bold]Auto-promote to Staging:[/bold] Yes",
                        title="Retraining Scheduled",
                        border_style="green"
                    ))
                else:
                    console.print(f"[red]❌ Failed to schedule retraining for {model_name}[/red]")
            else:
                # Trigger manual retraining
                job_id = await automated_retraining_service.trigger_manual_retraining(
                    model_name, days
                )
                
                console.print(Panel(
                    f"[green]✅ Triggered manual retraining for {model_name}[/green]\\n\\n"
                    f"[bold]Job ID:[/bold] {job_id}\\n"
                    f"[bold]Sliding Window:[/bold] {days} days\\n"
                    f"[bold]Status:[/bold] Running",
                    title="Retraining Started",
                    border_style="blue"
                ))
                
                console.print(f"\\n[dim]Monitor progress:[/dim]")
                console.print(f"• Check status: [bold]uv run -m src.interfaces.cli ml retrain-status {job_id}[/bold]")
            
        except Exception as e:
            console.print(f"[red]❌ Retraining operation failed: {e}[/red]")
            raise click.Abort()
    
    asyncio.run(_retrain_model())


@ml.command("retrain-status")
@click.argument("job_id")
def retrain_status(job_id):
    """Check status of a retraining job."""
    console.print(f"[bold blue]Retraining Job Status:[/bold blue] {job_id}")
    
    async def _retrain_status():
        try:
            from ....ml.workflows.automated_retraining import automated_retraining_service
            
            # Get job status
            job = await automated_retraining_service.get_retraining_job_status(job_id)
            
            if not job:
                console.print(f"[red]❌ Job not found: {job_id}[/red]")
                return
            
            # Display job status
            status_color = {
                "pending": "yellow",
                "running": "blue", 
                "completed": "green",
                "failed": "red",
                "cancelled": "dim"
            }.get(job.status.value, "white")
            
            console.print(Panel(
                f"[bold]Job ID:[/bold] {job.job_id}\\n"
                f"[bold]Model:[/bold] {job.model_name}\\n"
                f"[bold]Status:[/bold] [{status_color}]{job.status.value.upper()}[/{status_color}]\\n"
                f"[bold]Trigger:[/bold] {job.trigger.value}\\n"
                f"[bold]Scheduled:[/bold] {job.scheduled_time.strftime('%Y-%m-%d %H:%M:%S')}\\n"
                f"[bold]Started:[/bold] {job.started_time.strftime('%Y-%m-%d %H:%M:%S') if job.started_time else 'N/A'}\\n"
                f"[bold]Completed:[/bold] {job.completed_time.strftime('%Y-%m-%d %H:%M:%S') if job.completed_time else 'N/A'}\\n"
                f"[bold]Duration:[/bold] {job.duration_seconds:.1f}s" if job.duration_seconds else "N/A",
                title="Job Status",
                border_style=status_color
            ))
            
            # Display results if completed
            if job.status.value == "completed" and job.performance_metrics:
                console.print(Panel(
                    f"[bold]New Model Version:[/bold] {job.new_model_version or 'N/A'}\\n"
                    f"[bold]Training Samples:[/bold] {job.samples_used or 'N/A'}\\n"
                    f"[bold]Promoted to Staging:[/bold] {'Yes' if job.promoted_to_staging else 'No'}\\n"
                    f"[bold]Promoted to Production:[/bold] {'Yes' if job.promoted_to_production else 'No'}\\n\\n"
                    f"[bold]Performance Metrics:[/bold]\\n" + 
                    "\\n".join([f"• {k}: {v:.4f}" for k, v in job.performance_metrics.items()]),
                    title="Training Results",
                    border_style="green"
                ))
            
            # Display error if failed
            if job.status.value == "failed" and job.error_message:
                console.print(Panel(
                    f"[red]{job.error_message}[/red]",
                    title="Error Details",
                    border_style="red"
                ))
            
        except Exception as e:
            console.print(f"[red]❌ Failed to get job status: {e}[/red]")
            raise click.Abort()
    
    asyncio.run(_retrain_status())


@ml.command("retraining-service")
def retraining_service():
    """Show automated retraining service status."""
    console.print("[bold blue]Automated Retraining Service[/bold blue]")
    
    async def _retraining_service():
        try:
            from ....ml.workflows.automated_retraining import automated_retraining_service
            
            console.print("🔍 Checking retraining service status...")
            
            # Get service stats
            stats = automated_retraining_service.get_service_stats()
            
            if "error" in stats:
                console.print(f"[red]❌ Service error: {stats['error']}[/red]")
                return
            
            # Display service status
            console.print(Panel(
                f"[bold]Service Status[/bold]\\n\\n"
                f"[bold]Scheduler Running:[/bold] {'Yes' if stats['scheduler_running'] else 'No'}\\n"
                f"[bold]Monitoring Enabled:[/bold] {'Yes' if stats['monitoring_enabled'] else 'No'}\\n"
                f"[bold]Configured Models:[/bold] {stats['configured_models']}\\n"
                f"[bold]Scheduled Jobs:[/bold] {stats['scheduled_jobs']}\\n\\n"
                f"[bold]Job Statistics[/bold]\\n"
                f"[bold]Running Jobs:[/bold] {stats['running_jobs']}\\n"
                f"[bold]Completed Jobs:[/bold] {stats['completed_jobs']}\\n"
                f"[bold]Failed Jobs:[/bold] {stats['failed_jobs']}\\n"
                f"[bold]Total Job History:[/bold] {stats['total_jobs_history']}",
                title="Retraining Service",
                border_style="blue"
            ))
            
        except Exception as e:
            console.print(f"[red]❌ Service status failed: {e}[/red]")
            raise click.Abort()
    
    asyncio.run(_retraining_service())