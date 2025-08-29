#!/usr/bin/env python3
"""
Context-Sensitive Help System

Provides intelligent help and guidance based on user context, progress, and current operations.
Integrates with the onboarding system to provide contextual assistance.

Features:
- Context-aware help based on user progress
- Interactive troubleshooting assistance
- Command suggestions based on current state
- Integration with onboarding achievements
- Smart error recovery guidance

Usage Examples:
    # Get contextual help
    uv run -m src.interfaces.cli help context
    
    # Get help for specific command
    uv run -m src.interfaces.cli help command --name predictions
    
    # Troubleshooting assistance
    uv run -m src.interfaces.cli help troubleshoot --issue database-connection
    
    # Show tips for current level
    uv run -m src.interfaces.cli help tips
"""

import json
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Any

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from ....core.logging import get_logger, LogComponent

console = Console()
logger = get_logger(__name__, LogComponent.CLI)


class ContextualHelpSystem:
    """Provides context-sensitive help and guidance."""
    
    def __init__(self):
        self.config_dir = Path.home() / ".mlb_betting_system"
        self.progress_file = self.config_dir / "onboarding_progress.json"
        self.error_patterns_file = Path(__file__).parent / "help_data" / "error_patterns.json"
        self.command_help_file = Path(__file__).parent / "help_data" / "command_help.json"
        
        self.progress = self._load_progress()
        self.error_patterns = self._load_error_patterns()
        self.command_help = self._load_command_help()
    
    def _load_progress(self) -> Dict[str, Any]:
        """Load user progress for contextual help."""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except Exception:
                pass
        return {"current_level": "beginner", "completed_steps": []}
    
    def _load_error_patterns(self) -> Dict[str, Any]:
        """Load error patterns and solutions."""
        # This would load from a JSON file with common error patterns
        return {
            "database_connection": {
                "patterns": ["connection refused", "database does not exist", "role does not exist"],
                "solutions": [
                    "Start database: docker-compose -f docker-compose.quickstart.yml up -d",
                    "Setup database: uv run -m src.interfaces.cli database setup-action-network",
                    "Check connection: uv run -m src.interfaces.cli database status"
                ],
                "level_specific": {
                    "beginner": "Try the onboarding setup: uv run -m src.interfaces.cli onboarding start",
                    "intermediate": "Check your database configuration in config.toml",
                    "advanced": "Verify PostgreSQL service and connection parameters"
                }
            },
            "missing_dependencies": {
                "patterns": ["module not found", "import error", "package not installed"],
                "solutions": [
                    "Install dependencies: uv sync",
                    "Install dev dependencies: uv sync --dev",
                    "Check Python version: python --version (requires 3.10+)"
                ]
            },
            "no_predictions": {
                "patterns": ["no predictions", "no games", "no data available"],
                "solutions": [
                    "Collect data first: uv run -m src.interfaces.cli data collect --source action_network --real",
                    "Run full pipeline: uv run -m src.interfaces.cli pipeline run-full --generate-predictions",
                    "Check data status: uv run -m src.interfaces.cli data status"
                ],
                "level_specific": {
                    "beginner": "Complete onboarding first: uv run -m src.interfaces.cli onboarding start",
                    "intermediate": "Lower confidence threshold: --confidence-threshold 0.3",
                    "advanced": "Check strategy configuration and data quality"
                }
            },
            "mlflow_connection": {
                "patterns": ["mlflow", "tracking uri", "experiment not found"],
                "solutions": [
                    "Setup MLflow: uv run -m src.interfaces.cli ml setup",
                    "Start MLflow server: mlflow server --host 0.0.0.0 --port 5000",
                    "Check MLflow status: uv run -m src.interfaces.cli ml models"
                ]
            }
        }
    
    def _load_command_help(self) -> Dict[str, Any]:
        """Load command-specific help information."""
        return {
            "predictions": {
                "beginner": {
                    "description": "Generate betting predictions using machine learning models",
                    "basic_usage": "uv run -m src.interfaces.cli predictions today",
                    "tips": [
                        "Start with default confidence threshold (0.6)",
                        "Use --format summary for quick overview",
                        "Make sure you've collected data first"
                    ],
                    "prerequisites": ["database_setup", "first_collection"]
                },
                "intermediate": {
                    "description": "Advanced prediction generation with strategy selection",
                    "advanced_usage": [
                        "uv run -m src.interfaces.cli predictions today --confidence-threshold 0.8",
                        "uv run -m src.interfaces.cli predictions today --format json > predictions.json",
                        "uv run -m src.interfaces.cli predictions range --days 7"
                    ],
                    "tips": [
                        "Higher confidence = fewer but more reliable predictions",
                        "JSON format is useful for automation",
                        "Compare predictions across different strategies"
                    ]
                }
            },
            "data": {
                "beginner": {
                    "description": "Collect betting data from various sources",
                    "basic_usage": "uv run -m src.interfaces.cli data collect --source action_network --real",
                    "tips": [
                        "Action Network is the most reliable source",
                        "Always use --real flag for live data",
                        "Check status after collection"
                    ]
                },
                "intermediate": {
                    "description": "Advanced data collection and quality monitoring",
                    "advanced_usage": [
                        "uv run -m src.interfaces.cli data collect --source vsin --real",
                        "uv run -m src.interfaces.cli data status --detailed",
                        "uv run -m src.interfaces.cli data quality deploy"
                    ]
                }
            },
            "monitoring": {
                "beginner": {
                    "description": "Monitor system health and performance",
                    "basic_usage": "uv run -m src.interfaces.cli monitoring dashboard",
                    "tips": [
                        "Dashboard runs on http://localhost:8080",
                        "Keep dashboard open while using the system",
                        "Check health-check if something seems wrong"
                    ]
                }
            }
        }
    
    def get_contextual_help(self, command: Optional[str] = None) -> Dict[str, Any]:
        """Get help based on user's current context and progress."""
        level = self.progress.get("current_level", "beginner")
        completed_steps = self.progress.get("completed_steps", [])
        
        help_info = {
            "user_level": level,
            "completion_status": len(completed_steps),
            "recommended_commands": [],
            "tips": [],
            "next_steps": []
        }
        
        # Determine recommended commands based on progress
        if "environment_validated" not in completed_steps:
            help_info["recommended_commands"].append({
                "command": "uv run -m src.interfaces.cli onboarding start",
                "description": "Start guided onboarding to set up your system",
                "priority": "high"
            })
        elif "database_setup" not in completed_steps:
            help_info["recommended_commands"].append({
                "command": "uv run -m src.interfaces.cli database setup-action-network",
                "description": "Set up database for storing betting data",
                "priority": "high"
            })
        elif "first_collection" not in completed_steps:
            help_info["recommended_commands"].append({
                "command": "uv run -m src.interfaces.cli data collect --source action_network --real",
                "description": "Collect your first betting data",
                "priority": "high"
            })
        else:
            # User has basic setup complete
            help_info["recommended_commands"].extend([
                {
                    "command": "uv run -m src.interfaces.cli predictions today",
                    "description": "Generate betting predictions for today",
                    "priority": "medium"
                },
                {
                    "command": "uv run -m src.interfaces.cli monitoring dashboard",
                    "description": "Open real-time monitoring dashboard",
                    "priority": "medium"
                }
            ])
        
        # Add level-specific tips
        level_tips = {
            "beginner": [
                "Focus on getting your first prediction generated",
                "Use the onboarding system for step-by-step guidance",
                "Don't worry about advanced features yet"
            ],
            "intermediate": [
                "Experiment with different confidence thresholds",
                "Learn to interpret strategy performance metrics",
                "Start using the monitoring dashboard regularly"
            ],
            "advanced": [
                "Explore hyperparameter optimization",
                "Set up custom strategies for your betting style",
                "Configure automated monitoring alerts"
            ],
            "expert": [
                "Implement automated retraining workflows",
                "Set up production deployment monitoring",
                "Configure enterprise-grade alerting"
            ]
        }
        help_info["tips"] = level_tips.get(level, [])
        
        # Command-specific help
        if command and command in self.command_help:
            cmd_help = self.command_help[command].get(level, {})
            help_info["command_specific"] = cmd_help
        
        return help_info
    
    def diagnose_error(self, error_text: str) -> Dict[str, Any]:
        """Analyze error text and provide contextual solutions."""
        diagnosis = {
            "error_type": "unknown",
            "confidence": 0.0,
            "solutions": [],
            "level_specific_help": None
        }
        
        error_lower = error_text.lower()
        level = self.progress.get("current_level", "beginner")
        
        # Match against known error patterns
        best_match = None
        best_confidence = 0.0
        
        for error_type, info in self.error_patterns.items():
            patterns = info["patterns"]
            matches = sum(1 for pattern in patterns if pattern.lower() in error_lower)
            confidence = matches / len(patterns) if patterns else 0.0
            
            if confidence > best_confidence:
                best_confidence = confidence
                best_match = error_type
        
        if best_match and best_confidence > 0.3:
            error_info = self.error_patterns[best_match]
            diagnosis.update({
                "error_type": best_match,
                "confidence": best_confidence,
                "solutions": error_info["solutions"],
                "level_specific_help": error_info.get("level_specific", {}).get(level)
            })
        
        return diagnosis
    
    def get_command_suggestions(self, partial_command: str) -> List[Dict[str, Any]]:
        """Get command suggestions based on partial input."""
        level = self.progress.get("current_level", "beginner")
        
        suggestions = []
        
        # Basic command mappings for different levels
        level_commands = {
            "beginner": [
                ("onboarding start", "Start guided setup"),
                ("onboarding status", "Check your progress"),
                ("predictions today", "Get today's predictions"),
                ("data collect --source action_network --real", "Collect betting data"),
                ("data status", "Check data collection status"),
                ("monitoring dashboard", "Open monitoring dashboard")
            ],
            "intermediate": [
                ("backtesting run --strategies sharp_action", "Run strategy backtest"),
                ("ml models --profitable-only", "Show profitable models"),
                ("predictions today --confidence-threshold 0.8", "High-confidence predictions"),
                ("pipeline run-full --generate-predictions", "Run complete pipeline")
            ],
            "advanced": [
                ("optimization run --strategy sharp_action", "Optimize strategy parameters"),
                ("monitoring performance --hours 24", "24-hour performance analysis"),
                ("retraining setup", "Setup automated retraining"),
                ("data-quality deploy", "Deploy data quality improvements")
            ]
        }
        
        commands = level_commands.get(level, [])
        
        if partial_command:
            # Filter commands that match the partial input
            filtered = [(cmd, desc) for cmd, desc in commands 
                       if partial_command.lower() in cmd.lower()]
            suggestions = [{"command": cmd, "description": desc} for cmd, desc in filtered]
        else:
            # Return all commands for the level
            suggestions = [{"command": cmd, "description": desc} for cmd, desc in commands]
        
        return suggestions[:10]  # Limit to top 10


@click.group(name="help")
def help_group():
    """
    Context-sensitive help and guidance system.
    
    Provides intelligent assistance based on your current progress and context.
    """
    pass


@help_group.command("context")
@click.option(
    "--command",
    help="Get help for specific command"
)
def contextual_help(command: Optional[str]):
    """Get contextual help based on your current progress."""
    
    help_system = ContextualHelpSystem()
    help_info = help_system.get_contextual_help(command)
    
    console.print("🆘 [bold blue]Contextual Help System[/bold blue]")
    console.print("=" * 50)
    
    # User status
    level = help_info["user_level"]
    completion = help_info["completion_status"]
    
    console.print(f"📚 Your Level: [bold]{level.title()}[/bold]")
    console.print(f"📊 Steps Completed: [bold]{completion}[/bold]")
    console.print()
    
    # Recommended commands
    if help_info["recommended_commands"]:
        console.print("🎯 [bold]Recommended Next Steps:[/bold]")
        
        for i, cmd_info in enumerate(help_info["recommended_commands"], 1):
            priority_color = {
                "high": "red",
                "medium": "yellow", 
                "low": "green"
            }.get(cmd_info["priority"], "white")
            
            console.print(f"{i}. [bold {priority_color}]{cmd_info['priority'].upper()}[/bold {priority_color}]: {cmd_info['description']}")
            console.print(f"   [cyan]{cmd_info['command']}[/cyan]")
            console.print()
    
    # Level-specific tips
    if help_info["tips"]:
        console.print("💡 [bold]Tips for Your Level:[/bold]")
        for tip in help_info["tips"]:
            console.print(f"   • {tip}")
        console.print()
    
    # Command-specific help
    if "command_specific" in help_info:
        cmd_help = help_info["command_specific"]
        console.print(f"📖 [bold]Help for '{command}' command:[/bold]")
        console.print(f"   {cmd_help.get('description', 'No description available')}")
        
        if "basic_usage" in cmd_help:
            console.print(f"   Basic usage: [cyan]{cmd_help['basic_usage']}[/cyan]")
        
        if "advanced_usage" in cmd_help:
            console.print(f"   Advanced usage:")
            for usage in cmd_help["advanced_usage"]:
                console.print(f"     [cyan]{usage}[/cyan]")
        
        if cmd_help.get("tips"):
            console.print(f"   Tips:")
            for tip in cmd_help["tips"]:
                console.print(f"     • {tip}")


@help_group.command("troubleshoot")
@click.option(
    "--error",
    help="Error message or description"
)
@click.option(
    "--issue",
    type=click.Choice(["database-connection", "missing-dependencies", "no-predictions", "mlflow-connection"]),
    help="Common issue type"
)
def troubleshooting_help(error: Optional[str], issue: Optional[str]):
    """Get troubleshooting help for errors and issues."""
    
    help_system = ContextualHelpSystem()
    
    console.print("🔧 [bold blue]Troubleshooting Assistant[/bold blue]")
    console.print("=" * 50)
    
    if issue:
        # Handle predefined issue types
        if issue in help_system.error_patterns:
            error_info = help_system.error_patterns[issue]
            level = help_system.progress.get("current_level", "beginner")
            
            console.print(f"🎯 [bold]Issue: {issue.replace('-', ' ').title()}[/bold]")
            console.print()
            
            console.print("🔧 [bold]Solutions:[/bold]")
            for i, solution in enumerate(error_info["solutions"], 1):
                console.print(f"{i}. [cyan]{solution}[/cyan]")
            console.print()
            
            # Level-specific help
            if "level_specific" in error_info and level in error_info["level_specific"]:
                console.print(f"💡 [bold]For {level} users:[/bold]")
                console.print(f"   {error_info['level_specific'][level]}")
        else:
            console.print(f"[yellow]⚠️  Unknown issue type: {issue}[/yellow]")
    
    elif error:
        # Analyze error text
        diagnosis = help_system.diagnose_error(error)
        
        console.print(f"🔍 [bold]Error Analysis[/bold]")
        console.print(f"Error type: {diagnosis['error_type']}")
        console.print(f"Confidence: {diagnosis['confidence']:.1%}")
        console.print()
        
        if diagnosis["solutions"]:
            console.print("🔧 [bold]Suggested Solutions:[/bold]")
            for i, solution in enumerate(diagnosis["solutions"], 1):
                console.print(f"{i}. [cyan]{solution}[/cyan]")
            console.print()
        
        if diagnosis["level_specific_help"]:
            console.print("💡 [bold]Level-Specific Help:[/bold]")
            console.print(f"   {diagnosis['level_specific_help']}")
    
    else:
        # Show general troubleshooting guide
        console.print("📋 [bold]Common Issues and Solutions:[/bold]")
        console.print()
        
        table = Table(show_header=True, header_style="bold blue")
        table.add_column("Issue", style="cyan", width=20)
        table.add_column("Quick Fix", style="white", width=40)
        table.add_column("Command", style="yellow")
        
        table.add_row(
            "Database Connection",
            "Start database and setup schema",
            "uv run -m src.interfaces.cli onboarding start"
        )
        table.add_row(
            "No Predictions",
            "Collect data first",
            "uv run -m src.interfaces.cli data collect --source action_network --real"
        )
        table.add_row(
            "Missing Dependencies",
            "Install packages",
            "uv sync"
        )
        table.add_row(
            "MLflow Issues",
            "Setup ML infrastructure",
            "uv run -m src.interfaces.cli ml setup"
        )
        
        console.print(table)
        console.print()
        console.print("💡 For specific help: [cyan]uv run -m src.interfaces.cli help troubleshoot --issue <issue-type>[/cyan]")


@help_group.command("tips")
def show_tips():
    """Show helpful tips for your current level."""
    
    help_system = ContextualHelpSystem()
    level = help_system.progress.get("current_level", "beginner")
    completed_steps = help_system.progress.get("completed_steps", [])
    
    console.print("💡 [bold blue]Tips and Guidance[/bold blue]")
    console.print("=" * 50)
    console.print(f"Level: [bold]{level.title()}[/bold]")
    console.print()
    
    # Level-specific tips
    level_tips = {
        "beginner": [
            "🎯 Focus on completing the onboarding flow first",
            "📊 Start with Action Network as your primary data source", 
            "🔍 Use 'onboarding status' to track your progress",
            "💡 Don't skip the database setup - it's essential",
            "🎲 Your first prediction might take a few minutes to generate"
        ],
        "intermediate": [
            "📈 Experiment with different confidence thresholds (0.3-0.9)",
            "🧠 Learn to interpret ROI and win rate metrics",
            "📊 Use the monitoring dashboard to track system health",
            "🔄 Run backtests to validate strategy performance",
            "⚡ Try different data sources for more coverage"
        ],
        "advanced": [
            "🎛️ Use hyperparameter optimization to improve strategies",
            "📊 Set up custom monitoring alerts for key metrics",
            "🔧 Create custom strategies for your betting preferences",
            "📈 Analyze performance trends over time",
            "⚡ Configure automated data collection schedules"
        ],
        "expert": [
            "🏭 Set up production deployment with automated monitoring",
            "🔄 Configure automated retraining workflows",
            "🚨 Implement enterprise-grade alerting and recovery",
            "📊 Build custom dashboards for your operation",
            "⚡ Optimize system performance for high-frequency operations"
        ]
    }
    
    tips = level_tips.get(level, [])
    for tip in tips:
        console.print(f"   {tip}")
    
    console.print()
    
    # Progress-specific tips
    if len(completed_steps) < 5:
        console.print("🚀 [bold]Getting Started Tips:[/bold]")
        console.print("   • Run commands step by step, don't rush")
        console.print("   • Check status after each major operation")
        console.print("   • Use 'help context' if you get stuck")
    elif len(completed_steps) < 10:
        console.print("📈 [bold]Intermediate Tips:[/bold]")
        console.print("   • Start experimenting with different strategies")
        console.print("   • Keep the monitoring dashboard open")
        console.print("   • Compare predictions with actual results")
    else:
        console.print("🏆 [bold]Advanced Tips:[/bold]")
        console.print("   • Focus on automation and optimization")
        console.print("   • Set up monitoring alerts for production")
        console.print("   • Share your success with the community!")


@help_group.command("suggest")
@click.option(
    "--partial",
    help="Partial command to get suggestions for"
)
def command_suggestions(partial: Optional[str]):
    """Get command suggestions based on your level and context."""
    
    help_system = ContextualHelpSystem()
    suggestions = help_system.get_command_suggestions(partial or "")
    
    console.print("💬 [bold blue]Command Suggestions[/bold blue]")
    console.print("=" * 50)
    
    if partial:
        console.print(f"Suggestions for: [cyan]{partial}[/cyan]")
    else:
        level = help_system.progress.get("current_level", "beginner")
        console.print(f"Commands for [bold]{level}[/bold] level:")
    
    console.print()
    
    if suggestions:
        for i, suggestion in enumerate(suggestions, 1):
            console.print(f"{i:2}. [cyan]{suggestion['command']}[/cyan]")
            console.print(f"    {suggestion['description']}")
            console.print()
    else:
        console.print("[yellow]No suggestions found for the given input[/yellow]")
        console.print("💡 Try: [cyan]uv run -m src.interfaces.cli help suggest[/cyan] (without --partial)")


# Export the command group
help_system = help_group