#!/usr/bin/env python3
"""
Automated MLB Betting Backtesting System

Updated to use the new BacktestingEngine from Phase 3 consolidation.
This script runs the complete automated backtesting and strategy validation system.

Features:
- Unified backtesting engine with integrated modules
- Enhanced diagnostics with 5-checkpoint system
- Automated scheduling with circuit breakers
- Real-time accuracy monitoring
- Comprehensive reporting

Usage:
    uv run -m mlb_sharp_betting.cli.commands.backtesting [options]

Options:
    --mode            Operation mode: scheduler, single-run, status, test
    --config          Path to configuration file
    --debug           Enable debug logging
    --no-alerts       Disable alert notifications
    --force           Bypass circuit breaker (use with caution)
    --generate-report Generate daily report only
    --backtest-only   Run backtesting without alerts

Examples:
    # Run the full automated scheduler (recommended)
    uv run -m mlb_sharp_betting.cli.commands.backtesting --mode scheduler

    # Run a single backtesting analysis
    uv run -m mlb_sharp_betting.cli.commands.backtesting --mode single-run

    # Check system status
    uv run -m mlb_sharp_betting.cli.commands.backtesting --mode status

    # Test the system with sample data
    uv run -m mlb_sharp_betting.cli.commands.backtesting --mode test
"""

import asyncio
import argparse
import json
import sys
import signal
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import structlog
import click

# 🔄 UPDATED: Use new BacktestingEngine instead of deprecated services
from ...services.backtesting_engine import get_backtesting_engine
from ...services.alert_service import AlertService
from ...core.logging import get_logger


# Configure logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.dev.ConsoleRenderer(colors=True)
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = get_logger(__name__)


class AutomatedBacktestingCLI:
    """Command-line interface for the automated backtesting system using BacktestingEngine."""
    
    def __init__(self, config_path: Optional[Path] = None):
        """Initialize the CLI."""
        self.project_root = Path(__file__).parent.parent.parent.parent.parent
        self.config_path = config_path or self.project_root / "config" / "backtesting_config.json"
        self.logger = logger.bind(component="cli")
        self.config = self._load_config()
        
        # 🔄 UPDATED: Use new BacktestingEngine
        self.backtesting_engine = None
        self.alert_service: Optional[AlertService] = None
    
    def _load_config(self) -> dict:
        """Load configuration from file."""
        if not self.config_path.exists():
            self.logger.warning("Config file not found, using defaults", path=str(self.config_path))
            return {}
        
        try:
            with open(self.config_path) as f:
                return json.load(f)
        except Exception as e:
            self.logger.error("Failed to load config", error=str(e))
            return {}
    
    async def run_scheduler_mode(self, debug: bool = False, no_alerts: bool = False) -> None:
        """Run the automated backtesting scheduler using BacktestingEngine."""
        self.logger.info("Starting automated backtesting scheduler (Phase 3 Engine)")
        
        try:
            # 🔄 UPDATED: Initialize BacktestingEngine
            self.backtesting_engine = get_backtesting_engine()
            await self.backtesting_engine.initialize()
            
            # Set up signal handlers for graceful shutdown
            def signal_handler(signum, frame):
                self.logger.info(f"Received signal {signum}, shutting down gracefully...")
                if self.backtesting_engine:
                    self.backtesting_engine.stop_automated_scheduling()
                sys.exit(0)
            
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            
            # Start automated scheduling
            notifications_enabled = not no_alerts
            self.backtesting_engine.start_automated_scheduling(notifications_enabled=notifications_enabled)
            
            # Print startup information
            self._print_scheduler_info()
            
            # Keep running
            self.logger.info("BacktestingEngine scheduler is running. Press Ctrl+C to stop.")
            while True:
                await asyncio.sleep(60)
                
                # Periodic status update
                if debug:
                    status = self.backtesting_engine.get_comprehensive_status()
                    self.logger.debug("BacktestingEngine status", **status)
                
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt, shutting down...")
        except Exception as e:
            self.logger.error("BacktestingEngine scheduler failed", error=str(e))
            raise
        finally:
            if self.backtesting_engine:
                self.backtesting_engine.stop_automated_scheduling()
                self.logger.info("BacktestingEngine scheduler stopped")
    
    async def run_single_analysis(self, force: bool = False, detailed: bool = False) -> None:
        """Run a single backtesting analysis using BacktestingEngine."""
        self.logger.info("Running single backtesting analysis (Phase 3 Engine)")
        
        try:
            # 🔄 UPDATED: Initialize BacktestingEngine
            self.backtesting_engine = get_backtesting_engine()
            await self.backtesting_engine.initialize()
            
            self.alert_service = AlertService()
            
            # Run daily pipeline
            start_time = datetime.now(timezone.utc)
            results = await self.backtesting_engine.run_daily_pipeline()
            
            # Process alerts if available
            alerts = []
            if self.alert_service and 'backtest_results' in results:
                try:
                    # Note: Alert service may need updating to work with new result format
                    alerts = await self.alert_service.process_backtesting_results(results['backtest_results'])
                except Exception as e:
                    self.logger.warning("Failed to process alerts", error=str(e))
                    alerts = []
            
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            # Print results summary
            self._print_analysis_results(results, alerts, execution_time, detailed)
            
        except Exception as e:
            self.logger.error("Single analysis failed", error=str(e))
            raise
    
    async def show_status(self) -> None:
        """Show system status and health using BacktestingEngine."""
        self.logger.info("Checking system status (Phase 3 Engine)")
        
        try:
            # 🔄 UPDATED: Initialize BacktestingEngine
            self.backtesting_engine = get_backtesting_engine()
            await self.backtesting_engine.initialize()
            
            self.alert_service = AlertService()
            
            # Get comprehensive status from BacktestingEngine
            status_info = self.backtesting_engine.get_comprehensive_status()
            
            # Run diagnostics
            diagnostics_results = await self.backtesting_engine.diagnostics.run_full_diagnostic()
            
            # Get active alerts
            active_alerts = []
            try:
                active_alerts = await self.alert_service.get_active_alerts()
            except Exception as e:
                self.logger.warning("Failed to get active alerts", error=str(e))
            
            # Print status information
            self._print_system_status(status_info, diagnostics_results, active_alerts)
            
        except Exception as e:
            self.logger.error("Status check failed", error=str(e))
            raise
    
    async def run_test_mode(self) -> None:
        """Run system tests using BacktestingEngine."""
        self.logger.info("Running system tests (Phase 3 Engine)")
        
        try:
            # 🔄 UPDATED: Initialize BacktestingEngine
            self.backtesting_engine = get_backtesting_engine()
            await self.backtesting_engine.initialize()
            
            # Run diagnostics as system test
            self.logger.info("Running comprehensive diagnostics...")
            diagnostics_results = await self.backtesting_engine.diagnostics.run_full_diagnostic()
            
            # Run a quick backtest (last 3 days)
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
            
            self.logger.info(f"Running test backtest ({start_date} to {end_date})...")
            backtest_results = await self.backtesting_engine.run_backtest(
                start_date=start_date,
                end_date=end_date,
                include_diagnostics=True,
                include_alignment=True
            )
            
            # Print test results
            self._print_test_results(diagnostics_results, backtest_results)
            
        except Exception as e:
            self.logger.error("System test failed", error=str(e))
            raise
    
    async def generate_report_only(self) -> None:
        """Generate daily report using BacktestingEngine."""
        self.logger.info("Generating daily report (Phase 3 Engine)")
        
        try:
            # 🔄 UPDATED: Initialize BacktestingEngine
            self.backtesting_engine = get_backtesting_engine()
            await self.backtesting_engine.initialize()
            
            # Run daily pipeline for report generation
            results = await self.backtesting_engine.run_daily_pipeline()
            
            # Print report
            self._print_daily_report(results)
            
        except Exception as e:
            self.logger.error("Report generation failed", error=str(e))
            raise
    
    def _print_scheduler_info(self) -> None:
        """Print scheduler startup information."""
        print("🔬 AUTOMATED BACKTESTING SCHEDULER (Phase 3 Engine)")
        print("=" * 70)
        print("🚀 BacktestingEngine Features:")
        print("   ✅ Unified backtesting execution")
        print("   ✅ 5-checkpoint diagnostic system")
        print("   ✅ Automated scheduling with circuit breakers")
        print("   ✅ Real-time accuracy monitoring")
        print("   ✅ Enhanced live alignment validation")
        print()
        print("🔧 Configuration:")
        print(f"   📁 Project Root: {self.project_root}")
        print(f"   ⚙️  Config Path: {self.config_path}")
        print()
        print("📊 The scheduler will:")
        print("   🕐 Run daily backtesting at configured intervals")
        print("   📈 Monitor strategy performance continuously")
        print("   🚨 Generate alerts for significant changes")
        print("   📋 Update strategy configurations automatically")
        print("   🔍 Perform comprehensive diagnostics")
        print()
        print("🛑 To stop: Press Ctrl+C")
        print("=" * 70)
    
    def _print_analysis_results(self, results: dict, alerts: list, execution_time: float, detailed: bool = False) -> None:
        """Print backtesting analysis results."""
        print("\n🔬 BACKTESTING ANALYSIS RESULTS (Phase 3)")
        print("=" * 60)
        print(f"⏱️  Execution Time: {execution_time:.2f} seconds")
        
        # Handle new BacktestingEngine result format
        if 'backtest_results' in results:
            backtest = results['backtest_results']
            
            if isinstance(backtest, dict):
                print(f"📊 Strategies Analyzed: {backtest.get('total_strategies', 0)}")
                print(f"💰 Profitable Strategies: {backtest.get('profitable_strategies', 0)}")
                print(f"📈 Total Bets: {backtest.get('total_bets', 0)}")
                print(f"📊 Average ROI: {backtest.get('average_roi', 0):.1f}%")
                
                if detailed and 'strategy_results' in backtest:
                    self._print_detailed_strategy_results(backtest['strategy_results'])
        
        # Show data collection metrics if available
        if 'data_collection_metrics' in results:
            metrics = results['data_collection_metrics']
            print(f"\n📡 DATA COLLECTION:")
            print(f"   📥 Records Processed: {metrics.get('parsed_records', 0)}")
            print(f"   💾 Records Stored: {metrics.get('stored_records', 0)}")
            print(f"   🎯 Sharp Indicators: {metrics.get('sharp_indicators', 0)}")
        
        # Show diagnostics if available
        if 'diagnostics_results' in results:
            diagnostics = results['diagnostics_results']
            print(f"\n🔍 DIAGNOSTICS:")
            if isinstance(diagnostics, dict):
                for checkpoint, result in diagnostics.items():
                    if checkpoint.startswith('_'):
                        continue
                    status = result.get('status', 'UNKNOWN') if isinstance(result, dict) else str(result)
                    status_emoji = "✅" if status == 'PASS' else "❌" if status == 'FAIL' else "⚠️"
                    print(f"   {status_emoji} {checkpoint.replace('_', ' ').title()}: {status}")
        
        # Show alerts
        if alerts:
            print(f"\n🚨 ALERTS ({len(alerts)}):")
            for alert in alerts[:5]:  # Show top 5
                severity_emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(alert.severity.lower(), "⚪")
                print(f"   {severity_emoji} {alert.message}")
        else:
            print(f"\n✅ No critical alerts generated")
        
        print("=" * 60)
    
    def _print_detailed_strategy_results(self, strategy_results):
        """Print detailed strategy performance results."""
        print(f"\n📈 DETAILED STRATEGY PERFORMANCE:")
        
        if isinstance(strategy_results, list):
            # Sort by ROI
            sorted_results = sorted(
                strategy_results, 
                key=lambda x: x.get('roi_per_100', 0), 
                reverse=True
            )
        else:
            sorted_results = list(strategy_results.values())
        
        for i, result in enumerate(sorted_results[:10], 1):  # Top 10
            if isinstance(result, dict):
                strategy_name = result.get('strategy_name', 'Unknown')
                roi = result.get('roi_per_100', 0)
                win_rate = result.get('win_rate', 0)
                total_bets = result.get('total_bets', 0)
                
                profitability = "💰" if roi > 0 else "📉"
                
                print(f"\n   {i}. {profitability} {strategy_name}")
                print(f"      📊 ROI: {roi:.1f}% | WR: {win_rate:.1f}%")
                print(f"      🎲 Bets: {total_bets}")
    
    def _print_system_status(self, status_info: dict, diagnostics_results: dict, active_alerts: list) -> None:
        """Print comprehensive system status."""
        print("\n🔧 SYSTEM STATUS (Phase 3 BacktestingEngine)")
        print("=" * 60)
        
        # BacktestingEngine status
        print("🔬 BACKTESTING ENGINE STATUS:")
        if status_info:
            modules_loaded = status_info.get('modules_loaded', [])
            print(f"   📦 Modules Loaded: {', '.join(modules_loaded) if modules_loaded else 'None'}")
            print(f"   ⚡ Initialized: {'✅ Yes' if status_info.get('initialized', False) else '❌ No'}")
            
            if 'metrics' in status_info:
                metrics = status_info['metrics']
                print(f"   📊 Cache Status: {metrics.get('cache_status', 'Unknown')}")
        
        # Diagnostics results
        print("\n🔍 DIAGNOSTICS STATUS:")
        if isinstance(diagnostics_results, dict):
            overall_status = "✅ PASS"
            
            for checkpoint, result in diagnostics_results.items():
                if checkpoint.startswith('_'):
                    continue
                
                status = result.get('status', 'UNKNOWN') if isinstance(result, dict) else str(result)
                status_emoji = "✅" if status == 'PASS' else "❌" if status == 'FAIL' else "⚠️"
                
                print(f"   {status_emoji} {checkpoint.replace('_', ' ').title()}: {status}")
                
                if status == 'FAIL':
                    overall_status = "❌ FAIL"
                elif status == 'WARN' and overall_status != "❌ FAIL":
                    overall_status = "⚠️ WARN"
            
            print(f"\n   🎯 Overall Diagnostics: {overall_status}")
        
        # Active alerts
        print(f"\n🚨 ACTIVE ALERTS ({len(active_alerts)}):")
        if active_alerts:
            for alert in active_alerts[:5]:  # Show top 5
                severity_emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(alert.severity.lower(), "⚪")
                print(f"   {severity_emoji} {alert.message}")
        else:
            print("   ✅ No active alerts")
        
        print("=" * 60)
    
    def _print_test_results(self, diagnostics_results: dict, backtest_results: dict) -> None:
        """Print system test results."""
        print("\n🧪 SYSTEM TEST RESULTS (Phase 3 Engine)")
        print("=" * 60)
        
        # Diagnostics test results
        print("🔍 DIAGNOSTICS TEST:")
        if isinstance(diagnostics_results, dict):
            all_passed = True
            for checkpoint, result in diagnostics_results.items():
                if checkpoint.startswith('_'):
                    continue
                
                status = result.get('status', 'UNKNOWN') if isinstance(result, dict) else str(result)
                status_emoji = "✅" if status == 'PASS' else "❌"
                
                print(f"   {status_emoji} {checkpoint.replace('_', ' ').title()}")
                
                if status != 'PASS':
                    all_passed = False
            
            print(f"\n   🎯 Diagnostics: {'✅ ALL PASSED' if all_passed else '❌ SOME FAILED'}")
        
        # Backtest test results
        print("\n🔬 BACKTEST TEST:")
        if isinstance(backtest_results, dict) and 'backtest_results' in backtest_results:
            backtest = backtest_results['backtest_results']
            if isinstance(backtest, dict):
                total_strategies = backtest.get('total_strategies', 0)
                total_bets = backtest.get('total_bets', 0)
                
                print(f"   📊 Strategies Tested: {total_strategies}")
                print(f"   🎲 Total Bets: {total_bets}")
                
                test_passed = total_strategies > 0
                print(f"   🎯 Backtest: {'✅ PASSED' if test_passed else '❌ FAILED'}")
        
        print("=" * 60)
    
    def _print_daily_report(self, results: dict) -> None:
        """Print daily report."""
        print("\n📋 DAILY BACKTESTING REPORT (Phase 3 Engine)")
        print("=" * 60)
        print(f"📅 Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        if 'backtest_results' in results:
            backtest = results['backtest_results']
            if isinstance(backtest, dict):
                print(f"\n📊 SUMMARY:")
                print(f"   🎯 Strategies Analyzed: {backtest.get('total_strategies', 0)}")
                print(f"   💰 Profitable Strategies: {backtest.get('profitable_strategies', 0)}")
                print(f"   📈 Average ROI: {backtest.get('average_roi', 0):.1f}%")
                print(f"   🎲 Total Bets: {backtest.get('total_bets', 0)}")
        
        if 'execution_time_seconds' in results:
            print(f"\n⏱️  Execution Time: {results['execution_time_seconds']:.2f} seconds")
        
        print("=" * 60)


# 🔄 UPDATED: Click command group with BacktestingEngine
@click.group(name="backtesting")
@click.pass_context
def backtesting_group(ctx):
    """🔬 Automated backtesting system using BacktestingEngine (Phase 3)."""
    pass

@backtesting_group.command("run")
@click.option("--mode", 
              type=click.Choice(["scheduler", "single-run", "status", "test", "report"]),
              default="single-run",
              help="Operation mode")
@click.option("--config", 
              type=click.Path(exists=True, path_type=Path),
              help="Path to configuration file")
@click.option("--debug", 
              is_flag=True,
              help="Enable debug logging")
@click.option("--no-alerts", 
              is_flag=True,
              help="Disable alert notifications")
@click.option("--force", 
              is_flag=True,
              help="Bypass circuit breaker (use with caution)")
def run_backtesting(mode: str, config: Optional[Path], debug: bool, no_alerts: bool, force: bool):
    """Run backtesting analysis."""
    
    async def _run():
        # Set log level
        if debug:
            structlog.configure(
                processors=[
                    structlog.stdlib.filter_by_level,
                    structlog.stdlib.add_logger_name,
                    structlog.stdlib.add_log_level,
                    structlog.stdlib.PositionalArgumentsFormatter(),
                    structlog.dev.ConsoleRenderer(colors=True, pad_event=20)
                ],
                context_class=dict,
                logger_factory=structlog.stdlib.LoggerFactory(),
                wrapper_class=structlog.stdlib.BoundLogger,
                cache_logger_on_first_use=True,
            )
        
        # Initialize CLI
        cli = AutomatedBacktestingCLI(config_path=config)
        
        try:
            # Execute based on mode
            if mode == "scheduler":
                await cli.run_scheduler_mode(debug=debug, no_alerts=no_alerts)
            elif mode == "single-run":
                await cli.run_single_analysis(force=force)
            elif mode == "status":
                await cli.show_status()
            elif mode == "test":
                await cli.run_test_mode()
            elif mode == "report":
                await cli.generate_report_only()
                
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.error("Application failed", error=str(e))
            raise click.ClickException(f"Backtesting failed: {e}")
    
    asyncio.run(_run())

@backtesting_group.command("status")
@click.option("--config", 
              type=click.Path(exists=True, path_type=Path),
              help="Path to configuration file")
def show_backtesting_status(config: Optional[Path]):
    """Show current backtesting system status."""
    
    async def _show_status():
        cli = AutomatedBacktestingCLI(config_path=config)
        await cli.show_status()
    
    asyncio.run(_show_status())

@backtesting_group.command("test")
@click.option("--config", 
              type=click.Path(exists=True, path_type=Path),
              help="Path to configuration file")
def test_backtesting_system(config: Optional[Path]):
    """Test backtesting system components."""
    
    async def _test():
        cli = AutomatedBacktestingCLI(config_path=config)
        await cli.run_test_mode()
    
    asyncio.run(_test())


async def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Automated MLB Betting Backtesting System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --mode scheduler          # Run full scheduler (recommended)
  %(prog)s --mode single-run         # Single analysis
  %(prog)s --mode status             # Check system status
  %(prog)s --mode test               # Test system components
        """
    )
    
    parser.add_argument(
        "--mode",
        choices=["scheduler", "single-run", "status", "test", "report"],
        default="scheduler",
        help="Operation mode (default: scheduler)"
    )
    
    parser.add_argument(
        "--config",
        type=Path,
        help="Path to configuration file"
    )
    
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )
    
    parser.add_argument(
        "--no-alerts",
        action="store_true",
        help="Disable alert notifications"
    )
    
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass circuit breaker (use with caution)"
    )
    
    args = parser.parse_args()
    
    # Set log level
    if args.debug:
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.dev.ConsoleRenderer(colors=True, pad_event=20)
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
    
    # Initialize CLI
    cli = AutomatedBacktestingCLI(config_path=args.config)
    
    try:
        # Execute based on mode
        if args.mode == "scheduler":
            await cli.run_scheduler_mode(debug=args.debug, no_alerts=args.no_alerts)
        elif args.mode == "single-run":
            await cli.run_single_analysis(force=args.force)
        elif args.mode == "status":
            await cli.show_status()
        elif args.mode == "test":
            await cli.run_test_mode()
        elif args.mode == "report":
            await cli.generate_report_only()
            
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error("Application failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())