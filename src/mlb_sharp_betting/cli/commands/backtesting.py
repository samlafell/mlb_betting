#!/usr/bin/env python3
"""
Automated MLB Betting Backtesting System

This script runs the complete automated backtesting and strategy validation system.

Features:
- Daily backtesting pipeline with statistical validation
- Performance monitoring and alerting
- Automated threshold recommendations
- Risk management and circuit breakers
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import structlog
import click

from ...services.automated_backtesting_scheduler import AutomatedBacktestingScheduler
from ...services.backtesting_service import BacktestingService
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
    """Command-line interface for the automated backtesting system."""
    
    def __init__(self, config_path: Optional[Path] = None):
        """Initialize the CLI."""
        self.project_root = Path(__file__).parent.parent.parent.parent.parent
        self.config_path = config_path or self.project_root / "config" / "backtesting_config.json"
        self.config = self._load_config()
        self.logger = logger.bind(component="cli")
        
        # Services
        self.scheduler: Optional[AutomatedBacktestingScheduler] = None
        self.backtesting_service: Optional[BacktestingService] = None
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
        """Run the full automated scheduler."""
        self.logger.info("Starting automated backtesting scheduler")
        
        try:
            # Initialize scheduler
            self.scheduler = AutomatedBacktestingScheduler(
                project_root=self.project_root,
                notifications_enabled=not no_alerts,
                backtesting_enabled=self.config.get("backtesting", {}).get("enabled", True)
            )
            
            # Set up signal handlers for graceful shutdown
            def signal_handler(signum, frame):
                self.logger.info(f"Received signal {signum}, shutting down gracefully...")
                if self.scheduler:
                    self.scheduler.stop()
                sys.exit(0)
            
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            
            # Start scheduler
            self.scheduler.start()
            
            # Print startup information
            self._print_scheduler_info()
            
            # Keep running
            self.logger.info("Scheduler is running. Press Ctrl+C to stop.")
            while True:
                await asyncio.sleep(60)
                
                # Periodic status update
                if debug:
                    status = self.scheduler.get_enhanced_status()
                    self.logger.debug("Scheduler status", **status)
                
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt, shutting down...")
        except Exception as e:
            self.logger.error("Scheduler failed", error=str(e))
            raise
        finally:
            if self.scheduler:
                self.scheduler.stop()
                self.logger.info("Scheduler stopped")
    
    async def run_single_analysis(self, force: bool = False, detailed: bool = False) -> None:
        """Run a single backtesting analysis."""
        self.logger.info("Running single backtesting analysis")
        
        try:
            # Initialize services
            self.backtesting_service = BacktestingService()
            self.alert_service = AlertService()
            
            # Run backtesting pipeline
            start_time = datetime.now(timezone.utc)
            results = await self.backtesting_service.run_daily_backtesting_pipeline()
            
            # Process alerts
            alerts = await self.alert_service.process_backtesting_results(results)
            
            execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            # Print results summary
            self._print_analysis_results(results, alerts, execution_time, detailed)
            
        except Exception as e:
            self.logger.error("Single analysis failed", error=str(e))
            raise
    
    async def show_status(self) -> None:
        """Show system status and health."""
        self.logger.info("Checking system status")
        
        try:
            # Initialize services for status check
            self.backtesting_service = BacktestingService()
            self.alert_service = AlertService()
            
            # Validate data quality
            data_quality = await self.backtesting_service._validate_data_quality()
            
            # Get active alerts
            active_alerts = await self.alert_service.get_active_alerts()
            
            # Print status information
            self._print_system_status(data_quality, active_alerts)
            
        except Exception as e:
            self.logger.error("Status check failed", error=str(e))
            raise
    
    async def run_test_mode(self) -> None:
        """Run system tests with sample data."""
        self.logger.info("Running system tests")
        
        try:
            # Initialize services
            self.alert_service = AlertService()
            
            # Import the alert types
            from mlb_sharp_betting.services.alert_service import AlertType, AlertSeverity
            
            # Create test alerts
            test_alerts = [
                self.alert_service._create_alert(
                    alert_type=AlertType.HIGH_PERFORMANCE,
                    severity=AlertSeverity.MEDIUM,
                    title="Test High Performance Alert",
                    message="This is a test alert for high-performing strategy",
                    data={"win_rate": 0.58, "roi": 12.5, "sample_size": 45},
                    strategy_name="test_vsin_strong"
                ),
                self.alert_service._create_alert(
                    alert_type=AlertType.PERFORMANCE_DECLINE,
                    severity=AlertSeverity.HIGH,
                    title="Test Performance Decline Alert",
                    message="This is a test alert for declining strategy performance",
                    data={"current_win_rate": 0.47, "previous_win_rate": 0.54},
                    strategy_name="test_sbd_moderate"
                )
            ]
            
            # Process test alerts
            for alert in test_alerts:
                await self.alert_service._process_alert(alert)
            
            # Show test results
            active_alerts = await self.alert_service.get_active_alerts()
            
            print("\n🧪 TEST MODE RESULTS")
            print("=" * 50)
            print(f"✅ Alert service initialized successfully")
            print(f"✅ Created {len(test_alerts)} test alerts")
            print(f"✅ Active alerts: {len(active_alerts)}")
            print(f"✅ Notifications working: {self.alert_service.metrics['notifications_sent'] > 0}")
            print("\n🎯 Test completed successfully!")
            
        except Exception as e:
            self.logger.error("Test mode failed", error=str(e))
            raise
    
    async def generate_report_only(self) -> None:
        """Generate and display daily report without running full analysis."""
        self.logger.info("Generating daily report")
        
        try:
            # Initialize services
            self.backtesting_service = BacktestingService()
            
            # Execute SQL scripts only (no full pipeline)
            backtest_results = await self.backtesting_service._execute_backtest_scripts()
            
            # Analyze results
            strategy_metrics = await self.backtesting_service._analyze_strategy_performance(backtest_results)
            
            # Create minimal results object for report
            from mlb_sharp_betting.services.backtesting_service import BacktestingResults
            
            results = BacktestingResults(
                backtest_date=datetime.now(timezone.utc),
                total_strategies_analyzed=len(strategy_metrics),
                strategies_with_adequate_data=len([m for m in strategy_metrics if m.sample_size_adequate]),
                profitable_strategies=len([m for m in strategy_metrics if m.win_rate > 0.524]),
                declining_strategies=0,  # Would need historical comparison
                stable_strategies=0,
                threshold_recommendations=[],
                strategy_alerts=[],
                strategy_metrics=strategy_metrics,
                data_completeness_pct=100.0,  # Assume complete for report
                game_outcome_freshness_hours=0.0,
                execution_time_seconds=0.0,
                created_at=datetime.now(timezone.utc)
            )
            
            # Generate and display report
            report = await self.backtesting_service.generate_daily_report(results)
            print("\n" + "=" * 70)
            print("📊 DAILY BACKTESTING REPORT")
            print("=" * 70)
            print(report)
            
        except Exception as e:
            self.logger.error("Report generation failed", error=str(e))
            raise
    
    def _print_scheduler_info(self) -> None:
        """Print scheduler startup information."""
        if not self.scheduler:
            return
        
        status = self.scheduler.get_enhanced_status()
        
        print("\n" + "🚀 AUTOMATED BACKTESTING SCHEDULER STARTED" + "\n")
        print("=" * 70)
        
        print(f"📅 Configuration:")
        print(f"   • Daily backtesting: 2:00 AM EST")
        print(f"   • Mid-day check: 2:00 PM EST") 
        print(f"   • Weekly analysis: Monday 6:00 AM EST")
        print(f"   • Alert summary: Daily 8:00 AM EST")
        
        print(f"\n🎯 Features Enabled:")
        print(f"   • Backtesting: {status['backtesting']['enabled']}")
        print(f"   • Notifications: {self.scheduler.notifications_enabled}")
        print(f"   • Circuit breaker: {self.config.get('circuit_breaker', {}).get('enabled', True)}")
        print(f"   • Risk controls: ✅")
        
        print(f"\n📊 Current Status:")
        print(f"   • Active jobs: {status['jobs_count']}")
        print(f"   • Circuit breaker: {'OPEN' if status['circuit_breaker']['open'] else 'CLOSED'}")
        print(f"   • Active alerts: {status['alerts']['active_count']}")
        
        print(f"\n📋 Scheduled Jobs:")
        for job in self.scheduler.scheduler.get_jobs():
            print(f"   • {job.name}: {job.next_run_time}")
        
        print("\n💡 To check status: uv run run_automated_backtesting.py --mode status")
        print("   To stop: Press Ctrl+C")
        print("=" * 70)
    
    def _format_strategy_name(self, strategy_name: str, source_book: str, split_type: str) -> str:
        """Format strategy name into human-readable description."""
        
        # Strategy name mapping for better readability
        strategy_descriptions = {
            "strategy_comparison_roi": "ROI Strategy",
            "sharp_action_detector": "Sharp Action",
            "timing_based_strategy": "Timing-Based",
            "hybrid_line_sharp_strategy": "Hybrid Line/Sharp",
            "line_movement_strategy": "Line Movement",
            "signal_combinations": "Signal Combo",
            "opposing_markets_strategy": "Opposing Markets",
            "consensus_heavy_strategy": "Consensus Heavy",
            "consensus_moneyline_strategy": "Consensus ML",
            "consensus_signals_current": "Consensus Signals",
            "mixed_consensus_strategy": "Mixed Consensus",
            "executive_summary_report": "Executive Summary",
            # Phase 1 Expert-Recommended Strategies
            "total_line_sweet_spots_strategy": "Total Sweet Spots",
            "underdog_ml_value_strategy": "Underdog ML Value",
            "team_specific_bias_strategy": "Team Bias"
        }
        
        # Extract base strategy name
        base_strategy = strategy_name.split('_')[0] + '_' + strategy_name.split('_')[1] + '_' + strategy_name.split('_')[2] if len(strategy_name.split('_')) >= 3 else strategy_name
        
        # Get clean strategy name
        clean_name = strategy_descriptions.get(base_strategy, base_strategy.replace('_', ' ').title())
        
        # Extract variant from strategy name
        parts = strategy_name.split('_')
        if len(parts) > 3:
            variant = '_'.join(parts[3:])
            
            # Variant descriptions
            variant_descriptions = {
                "FOLLOW_STRONG_SHARP": "Follow Strong Sharp",
                "FOLLOW_STRONG_SH": "Follow Strong Sharp",  # Truncated version
                "STEAM_PLAYS": "Steam Plays",
                "HYBRID_CONFIRMATION": "Hybrid Confirmation", 
                "FOLLOW_BIG_LINE_MOVEMENT": "Follow Big Line Movement",
                "WEAK_SHARP_AWAY_UNDER": "Weak Sharp Away/Under",
                "STRONG_SHARP_AWAY_UNDER": "Strong Sharp Away/Under",
                "STRONG_SHARP_HOME_OVER": "Strong Sharp Home/Over",
                "spread_preference": "Spread Preference",
                "spread_prefere": "Spread Preference",  # Truncated version
                "follow_stronger": "Follow Stronger",
                "follow_stronge": "Follow Stronger",  # Truncated version
                "ml_preference": "ML Preference",
                "contrarian": "Contrarian",
                "cross_book_consensus": "Cross Book Consensus",
                "line_movement_confirmation": "Line Movement Confirmation",
                # Phase 1 Strategy Variants
                "VALUE_OVER_SWEET_SPOT": "Value Over Sweet Spot",
                "VALUE_UNDER_SWEET_SPOT": "Value Under Sweet Spot", 
                "SHARP_SWEET_SPOT_OVER": "Sharp Sweet Spot Over",
                "SHARP_SWEET_SPOT_UNDER": "Sharp Sweet Spot Under",
                "VALUE_AWAY_DOG": "Value Away Dog",
                "VALUE_HOME_DOG": "Value Home Dog",
                "SHARP_AWAY_DOG": "Sharp Away Dog",
                "SHARP_HOME_DOG": "Sharp Home Dog",
                "FADE_BIG_MARKET_HOME": "Fade Big Market Home",
                "FADE_BIG_MARKET_AWAY": "Fade Big Market Away",
                "BACK_SMALL_MARKET_HOME": "Back Small Market Home",
                "BACK_SMALL_MARKET_AWAY": "Back Small Market Away"
            }
            
            clean_variant = variant_descriptions.get(variant, variant.replace('_', ' ').title())
        else:
            clean_variant = ""
        
        # Clean up book names
        book_mapping = {
            "VSIN-draftkings": "VSIN-DK",
            "VSIN-circa": "VSIN-Circa", 
            "SBD-UNKNOWN": "SBD",
            "SBD--": "SBD"
        }
        
        clean_book = book_mapping.get(source_book, source_book)
        
        # Format market type
        market_mapping = {
            "moneyline": "ML",
            "spread": "Spread", 
            "total": "Total",
            "": ""
        }
        
        clean_market = market_mapping.get(split_type, split_type)
        
        # Build final description
        if clean_variant and clean_market:
            return f"{clean_book} {clean_market} • {clean_variant}"
        elif clean_variant:
            return f"{clean_book} • {clean_variant}"
        elif clean_market:
            return f"{clean_book} {clean_market} • {clean_name}"
        else:
            return f"{clean_book} • {clean_name}"
    
    def _format_strategy_name_simple(self, strategy_name: str) -> str:
        """Format strategy name into a simple format."""
        
        # Strategy name mapping for better readability
        strategy_descriptions = {
            "strategy_comparison_roi": "ROI Strategy",
            "sharp_action_detector": "Sharp Action",
            "timing_based_strategy": "Timing-Based",
            "hybrid_line_sharp_strategy": "Hybrid Line/Sharp",
            "line_movement_strategy": "Line Movement",
            "signal_combinations": "Signal Combo",
            "opposing_markets_strategy": "Opposing Markets",
            "consensus_heavy_strategy": "Consensus Heavy",
            "consensus_moneyline_strategy": "Consensus ML",
            "consensus_signals_current": "Consensus Signals",
            "mixed_consensus_strategy": "Mixed Consensus",
            "executive_summary_report": "Executive Summary",
            # Phase 1 Expert-Recommended Strategies
            "total_line_sweet_spots_strategy": "Total Sweet Spots",
            "underdog_ml_value_strategy": "Underdog ML Value",
            "team_specific_bias_strategy": "Team Bias"
        }
        
        # Extract base strategy name
        base_strategy = strategy_name.split('_')[0] + '_' + strategy_name.split('_')[1] + '_' + strategy_name.split('_')[2] if len(strategy_name.split('_')) >= 3 else strategy_name
        
        # Get clean strategy name
        clean_name = strategy_descriptions.get(base_strategy, base_strategy.replace('_', ' ').title())
        
        return clean_name
    
    def _print_analysis_results(self, results, alerts, execution_time: float, detailed: bool = False) -> None:
        """Print single analysis results."""
        print("\n" + "📊 BACKTESTING ANALYSIS RESULTS" + "\n")
        print("=" * 60)
        
        print(f"⏱️  Execution time: {execution_time:.1f} seconds")
        print(f"📈 Strategies analyzed: {results.total_strategies_analyzed}")
        print(f"✅ Adequate sample size: {results.strategies_with_adequate_data}")
        print(f"💰 Profitable strategies: {results.profitable_strategies}")
        print(f"📉 Declining strategies: {results.declining_strategies}")
        print(f"🎯 Recommendations: {len(results.threshold_recommendations)}")
        print(f"🚨 Alerts generated: {len(alerts)}")
        
        print(f"\n📊 Data Quality:")
        print(f"   • Completeness: {results.data_completeness_pct:.1f}%")
        print(f"   • Freshness: {results.game_outcome_freshness_hours:.1f} hours")
        
        if results.threshold_recommendations:
            print(f"\n🎯 Threshold Recommendations:")
            for rec in results.threshold_recommendations[:3]:  # Show first 3
                print(f"   • {self._format_strategy_name_simple(rec.strategy_name)}")
        
        if alerts:
            print(f"\n🚨 Alerts ({len(alerts)}):")
            for alert in alerts[:3]:  # Show first 3
                severity = alert.severity.value if hasattr(alert.severity, 'value') else str(alert.severity)
                severity_emoji = {"CRITICAL": "🔥", "HIGH": "🟠", "MEDIUM": "🟡"}.get(severity, "ℹ️")
                print(f"   {severity_emoji} {alert.message}")
        
        # Strategy performance summary
        if results.strategy_metrics:
            def calculate_weighted_score(strategy):
                return (strategy.win_rate * 100) + (strategy.roi_per_100 * 0.5) + (strategy.total_bets * 0.1)
            
            def deterministic_sort_key(strategy):
                """Create a deterministic sort key with tie-breakers for consistent ordering."""
                score = calculate_weighted_score(strategy)
                # Tie-breakers in order of preference: strategy_name (alphabetical)
                return (-score, strategy.strategy_name, strategy.source_book_type, strategy.split_type)
            
            # Categorize strategies by sample size
            reliable_strategies = [s for s in results.strategy_metrics if s.total_bets >= 50 and s.win_rate > 0.524]
            basic_strategies = [s for s in results.strategy_metrics if s.total_bets >= 17 and s.win_rate > 0.524]
            
            # Show reliable strategies if available
            if reliable_strategies:
                print(f"\n🎯 Reliable Sample Size Strategies (≥50 bets):")
                reliable_sorted = sorted(reliable_strategies, key=deterministic_sort_key)[:10]
                
                for i, strategy in enumerate(reliable_sorted, 1):
                    score = calculate_weighted_score(strategy)
                    profitable_indicator = "🟢" if strategy.win_rate > 0.524 else "🔴"
                    formatted_name = self._format_strategy_name(strategy.strategy_name, strategy.source_book_type, strategy.split_type)
                    print(f"   {i:2d}. {profitable_indicator} {formatted_name:<50} | "
                          f"{strategy.total_bets:3d} bets | "
                          f"{strategy.win_rate*100:5.1f}% WR | "
                          f"{strategy.roi_per_100:+6.1f}% ROI | "
                          f"Score: {score:5.1f}")
            else:
                print(f"\n⚠️  No strategies meet the reliable sample size threshold (≥50 bets)")
                print(f"     This indicates insufficient historical data for meaningful analysis.")
                
                if basic_strategies:
                    print(f"\n📊 Profitable Strategies (≥17 bets) - USE WITH CAUTION:")
                    basic_sorted = sorted(basic_strategies, key=deterministic_sort_key)[:10]
                    
                    for i, strategy in enumerate(basic_sorted, 1):
                        score = calculate_weighted_score(strategy)
                        profitable_indicator = "🟢" if strategy.win_rate > 0.524 else "🔴"
                        formatted_name = self._format_strategy_name(strategy.strategy_name, strategy.source_book_type, strategy.split_type)
                        print(f"   {i:2d}. {profitable_indicator} {formatted_name:<50} | "
                              f"{strategy.total_bets:3d} bets | "
                              f"{strategy.win_rate*100:5.1f}% WR | "
                              f"{strategy.roi_per_100:+6.1f}% ROI | "
                              f"Score: {score:5.1f}")
                    
                    print(f"\n     ⚠️  WARNING: Sample sizes (17-49 bets) have limited statistical reliability")
                    print(f"         Recommended: Collect more data before making significant decisions")
        
        if results.threshold_recommendations:
            print(f"\n🎯 Detailed Recommendations:")
            for rec in results.threshold_recommendations:
                print(f"   • {self._format_strategy_name(rec.strategy_name, rec.source_book, rec.split_type)}")
                print(f"     Current: {rec.current_threshold} → Recommended: {rec.recommended_threshold}")
                print(f"     Confidence: {rec.confidence_level}")
                print(f"     Expected Improvement: +{rec.expected_improvement:.1f}%")
        
        print("=" * 60)
    
    def _print_system_status(self, data_quality: dict, active_alerts: list) -> None:
        """Print system status information."""
        print("\n" + "🔍 SYSTEM STATUS CHECK" + "\n")
        print("=" * 50)
        
        # Data quality status
        completeness = data_quality.get('completeness_pct', 0)
        freshness = data_quality.get('freshness_hours', 999)
        
        print(f"📊 Data Quality:")
        print(f"   • Completeness: {completeness:.1f}% {'✅' if completeness >= 95 else '⚠️'}")
        print(f"   • Freshness: {freshness:.1f}h {'✅' if freshness <= 6 else '⚠️'}")
        
        # Alert status
        critical_alerts = []
        high_alerts = []
        
        for a in active_alerts:
            if hasattr(a, 'severity'):
                severity = a.severity.value if hasattr(a.severity, 'value') else str(a.severity)
            elif isinstance(a, dict):
                severity = a.get('severity', 'UNKNOWN')
            else:
                severity = 'UNKNOWN'
            
            if severity == "CRITICAL":
                critical_alerts.append(a)
            elif severity == "HIGH":
                high_alerts.append(a)
        
        print(f"\n🚨 Active Alerts:")
        print(f"   • Total: {len(active_alerts)}")
        print(f"   • Critical: {len(critical_alerts)} {'🔴' if critical_alerts else '✅'}")
        print(f"   • High: {len(high_alerts)} {'🟠' if high_alerts else '✅'}")
        
        # Overall health
        health_issues = []
        if completeness < 95:
            health_issues.append("Data completeness low")
        if freshness > 6:
            health_issues.append("Data freshness poor") 
        if critical_alerts:
            health_issues.append("Critical alerts active")
        
        print(f"\n🏥 Overall Health:")
        if not health_issues:
            print("   ✅ System healthy - no issues detected")
        else:
            print("   ⚠️  Issues detected:")
            for issue in health_issues:
                print(f"      • {issue}")
        
        print("=" * 50)


@click.group(name="backtesting")
@click.pass_context
def backtesting_group(ctx):
    """Automated backtesting and strategy validation commands."""
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