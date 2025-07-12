"""
Diagnostics CLI Commands

Commands for running comprehensive diagnostics to debug backtesting and live detection disconnects.
"""

import asyncio
import json
from typing import Optional
import click
import structlog

from ...services.backtesting_engine import get_backtesting_engine
from ...core.logging import get_logger

logger = get_logger(__name__)


@click.group()
def diagnostics():
    """Comprehensive diagnostics for debugging system issues"""
    pass


@diagnostics.command()
@click.option('--output', type=str, help='Save results to JSON file')
@click.option('--verbose', is_flag=True, help='Show detailed output')
@click.option('--checkpoint', type=str, help='Run specific checkpoint only (1-5)')
def run_full_diagnostic(output: Optional[str], verbose: bool, checkpoint: Optional[str]):
    """
    Run comprehensive 5-checkpoint diagnostic suite.
    
    This command implements the senior engineer's debugging approach to identify
    disconnects between backtesting and live detection environments.
    
    The 5 checkpoints are:
    1. Data Availability - Are splits populated?
    2. Processor Execution - Are all processors running?
    3. Threshold Validation - Are criteria too strict?
    4. Signal Generation - Raw signals before filtering?
    5. Configuration Sync - Live vs Backtest settings?
    """
    
    async def run_diagnostic():
        try:
            click.echo("🔍 Starting comprehensive backtesting diagnostics...")
            click.echo("This will analyze the disconnect between backtesting and live detection.\n")
            
            # Initialize diagnostics service
            backtesting_engine = get_backtesting_engine()
            await backtesting_engine.initialize()
            diagnostics_service = backtesting_engine.diagnostics
            
            # Run full diagnostic suite
            report = await diagnostics_service.run_full_diagnostic()
            
            # Display summary
            summary = report['diagnostic_summary']
            click.echo(f"📊 DIAGNOSTIC SUMMARY")
            click.echo(f"════════════════════")
            click.echo(f"✅ Passed Checkpoints: {summary['passed_checkpoints']}")
            click.echo(f"⚠️  Warning Checkpoints: {summary['warning_checkpoints']}")
            click.echo(f"❌ Failed Checkpoints: {summary['failed_checkpoints']}")
            click.echo(f"⏱️  Duration: {summary['diagnostic_duration_seconds']:.2f} seconds")
            click.echo()
            
            # Display checkpoint results
            click.echo("🎯 CHECKPOINT RESULTS")
            click.echo("═══════════════════")
            for result in report['checkpoint_results']:
                status_emoji = {
                    'PASS': '✅',
                    'WARN': '⚠️',
                    'FAIL': '❌',
                    'UNKNOWN': '❓'
                }.get(result['status'], '❓')
                
                click.echo(f"{status_emoji} {result['name']}: {result['message']}")
                
                if verbose and result['recommendations']:
                    for rec in result['recommendations']:
                        click.echo(f"   💡 {rec}")
                click.echo()
            
            # Display strategy diagnostics  
            strategy_diag = report['strategy_diagnostics']
            working_strategies = []
            failing_strategies = []
            
            if strategy_diag:
                click.echo("🔧 STRATEGY DIAGNOSTICS")
                click.echo("══════════════════════")
                
                working_strategies = [name for name, diag in strategy_diag.items() 
                                    if diag['processor_status'] == 'PASS']
                failing_strategies = [name for name, diag in strategy_diag.items() 
                                    if diag['processor_status'] != 'PASS']
                
                click.echo(f"✅ Working Strategies ({len(working_strategies)}):")
                for strategy in working_strategies:
                    signals = strategy_diag[strategy]['raw_signals']
                    click.echo(f"   • {strategy}: {signals} signals generated")
                
                if failing_strategies:
                    click.echo(f"\n❌ Failing Strategies ({len(failing_strategies)}):")
                    for strategy in failing_strategies:
                        status = strategy_diag[strategy]['processor_status']
                        recs = strategy_diag[strategy]['recommendations']
                        click.echo(f"   • {strategy} ({status}): {', '.join(recs)}")
                
                click.echo()
            
            # Display actionable recommendations
            recommendations = report['actionable_recommendations']
            if recommendations:
                click.echo("🚀 ACTIONABLE RECOMMENDATIONS")
                click.echo("════════════════════════════")
                
                for i, rec in enumerate(recommendations[:10], 1):
                    priority_emoji = {
                        'CRITICAL': '🔥',
                        'HIGH': '⚠️',
                        'MEDIUM': '📝',
                        'LOW': '💡'
                    }.get(rec['priority'], '📝')
                    
                    click.echo(f"{i}. {priority_emoji} [{rec['priority']}] {rec['action']}")
                    click.echo(f"   Impact: {rec['impact']}")
                    
                    if verbose and rec.get('commands'):
                        click.echo("   Commands:")
                        for cmd in rec['commands'][:3]:
                            click.echo(f"     {cmd}")
                    click.echo()
            
            # Save to file if requested
            if output:
                with open(output, 'w') as f:
                    json.dump(report, f, indent=2, default=str)
                click.echo(f"💾 Full diagnostic report saved to: {output}")
            
            # Final assessment
            click.echo("🎯 NEXT STEPS")
            click.echo("════════════")
            
            if summary['failed_checkpoints'] > 0:
                click.echo("❌ CRITICAL: Address failed checkpoints immediately")
                click.echo("   These are blocking signal generation entirely.")
            elif summary['warning_checkpoints'] > 0:
                click.echo("⚠️  WARNING: Address warning checkpoints for better performance")
                click.echo("   These are reducing signal quality/quantity.")
            else:
                click.echo("✅ All checkpoints passed! System looks healthy.")
            
            if len(working_strategies) < len(strategy_diag) // 2:
                click.echo("🔧 STRATEGY ISSUE: Many strategies are not generating signals")
                click.echo("   Review individual strategy diagnostics above.")
            
            click.echo("\nFor detailed analysis, use --verbose flag or save to JSON with --output")
            
        except Exception as e:
            click.echo(f"❌ Diagnostic failed: {str(e)}")
            logger.error(f"Diagnostic execution failed: {e}")
            return False
        
        return True
    
    # Run the async function
    result = asyncio.run(run_diagnostic())
    if not result:
        exit(1)


@diagnostics.command()
@click.argument('strategy_name', required=True)
@click.option('--test-signal-generation', is_flag=True, help='Test signal generation with permissive settings')
def debug_strategy(strategy_name: str, test_signal_generation: bool):
    """
    Debug a specific strategy that's not generating signals.
    
    This command provides detailed debugging for individual strategies
    that show "NO_SIGNALS" in backtesting logs.
    """
    
    async def debug_single_strategy():
        try:
            click.echo(f"🔍 Debugging strategy: {strategy_name}")
            click.echo("=" * 50)
            
            # Initialize diagnostics service
            backtesting_engine = get_backtesting_engine()
            await backtesting_engine.initialize()
            diagnostics_service = backtesting_engine.diagnostics
            
            # Run full diagnostic to get strategy details
            report = await diagnostics_service.run_full_diagnostic()
            
            # Find strategy in diagnostics
            strategy_diag = report['strategy_diagnostics'].get(strategy_name)
            
            if not strategy_diag:
                click.echo(f"❌ Strategy '{strategy_name}' not found in diagnostics")
                available_strategies = list(report['strategy_diagnostics'].keys())
                click.echo(f"Available strategies: {', '.join(available_strategies[:10])}")
                return False
            
            # Display strategy details
            click.echo(f"📊 Strategy Status: {strategy_diag['processor_status']}")
            click.echo(f"🎯 Raw Signals Found: {strategy_diag['raw_signals']}")
            click.echo(f"🔬 Filtered Signals: {strategy_diag['filtered_signals']}")
            click.echo(f"📈 Data Quality Score: {strategy_diag['data_quality_score']:.2f}")
            click.echo()
            
            # Display data gaps
            if strategy_diag['data_gaps']:
                click.echo("⚠️  DATA GAPS IDENTIFIED:")
                for gap in strategy_diag['data_gaps']:
                    click.echo(f"   • {gap}")
                click.echo()
            
            # Display recommendations
            if strategy_diag['recommendations']:
                click.echo("💡 RECOMMENDATIONS:")
                for rec in strategy_diag['recommendations']:
                    click.echo(f"   • {rec}")
                click.echo()
            
            # Display threshold analysis if available
            if strategy_diag['threshold_analysis']:
                click.echo("🎛️  THRESHOLD ANALYSIS:")
                threshold_data = strategy_diag['threshold_analysis']
                for key, value in threshold_data.items():
                    click.echo(f"   • {key}: {value}")
                click.echo()
            
            # Provide specific guidance based on status
            if strategy_diag['processor_status'] == 'FAIL':
                click.echo("🔥 CRITICAL ISSUE - Processor Failed")
                click.echo("   The processor cannot execute at all.")
                click.echo("   Check logs for initialization errors.")
            elif strategy_diag['processor_status'] == 'WARN':
                click.echo("⚠️  WARNING - No Signals Generated")
                click.echo("   Processor works but finds no qualifying opportunities.")
                click.echo("   This could be due to:")
                click.echo("   • Insufficient data quality")
                click.echo("   • Too restrictive thresholds")
                click.echo("   • Timing issues (data not available when needed)")
            elif strategy_diag['processor_status'] == 'PASS':
                click.echo("✅ SUCCESS - Signals Generated")
                click.echo(f"   Strategy is working and found {strategy_diag['raw_signals']} signals.")
            
            return True
            
        except Exception as e:
            click.echo(f"❌ Strategy debugging failed: {str(e)}")
            logger.error(f"Strategy debugging failed for {strategy_name}: {e}")
            return False
    
    # Run the async function
    result = asyncio.run(debug_single_strategy())
    if not result:
        exit(1)


@diagnostics.command()
def quick_health_check():
    """
    Quick health check focusing on the most common issues.
    
    This is a fast diagnostic that checks:
    - Data availability in last 24 hours
    - Basic processor functionality
    - Recent signal generation
    """
    
    async def quick_check():
        try:
            click.echo("⚡ Quick Health Check")
            click.echo("=" * 30)
            
            # Initialize diagnostics service
            diagnostics_service = await get_diagnostics_service()
            
            # Run just the critical checkpoints
            await diagnostics_service._checkpoint_1_data_availability()
            await diagnostics_service._checkpoint_4_signal_generation()
            
            # Get results
            results = diagnostics_service._diagnostic_results
            strategy_diagnostics = diagnostics_service._strategy_diagnostics
            
            # Quick summary
            data_result = next((r for r in results if r.checkpoint_name == "Data Availability"), None)
            
            if data_result:
                status_emoji = '✅' if data_result.status.value == 'PASS' else '❌'
                click.echo(f"{status_emoji} Data: {data_result.message}")
            
            # Strategy summary
            working_count = len([d for d in strategy_diagnostics.values() 
                               if d.processor_status.value == 'PASS'])
            total_count = len(strategy_diagnostics)
            
            if working_count == 0:
                click.echo("❌ Strategies: No strategies generating signals")
            elif working_count < total_count // 2:
                click.echo(f"⚠️  Strategies: {working_count}/{total_count} working")
            else:
                click.echo(f"✅ Strategies: {working_count}/{total_count} working")
            
            # Quick recommendations
            if data_result and data_result.status.value == 'FAIL':
                click.echo("\n🚀 Quick Fix: Run data collection")
                click.echo("   source .env && uv run src/mlb_sharp_betting/cli.py data collect --force")
            elif working_count == 0:
                click.echo("\n🚀 Quick Fix: Check processor configuration")
                click.echo("   Run full diagnostic for detailed analysis:")
                click.echo("   uv run src/mlb_sharp_betting/cli.py diagnostics run-full-diagnostic --verbose")
            
            return True
            
        except Exception as e:
            click.echo(f"❌ Quick health check failed: {str(e)}")
            return False
    
    # Run the async function
    result = asyncio.run(quick_check())
    if not result:
        exit(1)


@diagnostics.command()
@click.option('--strategy', default='sharp_action', help='Strategy type to debug')
@click.option('--source', default='VSIN', help='Source to test (VSIN, SBD, etc.)')
@click.option('--split-type', default='moneyline', help='Split type to test (moneyline, spread, total)')
@click.option('--show-dynamic', is_flag=True, help='Show dynamic threshold details')
@click.option('--test-signals', is_flag=True, help='Test with sample signal values')
def debug_thresholds(strategy: str, source: str, split_type: str, show_dynamic: bool, test_signals: bool):
    """Debug threshold system to see what values are actually being used."""
    
    async def run_threshold_debug():
        from mlb_sharp_betting.services.dynamic_threshold_manager import get_dynamic_threshold_manager
        from mlb_sharp_betting.analysis.processors.sharpaction_processor import SharpActionProcessor
        from mlb_sharp_betting.services.betting_signal_repository import BettingSignalRepository
        from mlb_sharp_betting.services.strategy_validation import StrategyValidation
        from mlb_sharp_betting.models.betting_analysis import SignalProcessorConfig
        from mlb_sharp_betting.db.connection import get_db_manager
        
        logger = get_logger(__name__)
        click.echo("🔧 THRESHOLD DEBUG MODE")
        click.echo("=" * 60)
        
        try:
            # Initialize components
            db_manager = get_db_manager()
            repository = BettingSignalRepository(db_manager)
            validator = StrategyValidation(db_manager=db_manager)
            config = SignalProcessorConfig()
            
            # Get dynamic threshold manager
            threshold_manager = get_dynamic_threshold_manager()
            
            click.echo(f"📊 Testing Strategy: {strategy}")
            click.echo(f"📊 Testing Source: {source}")
            click.echo(f"📊 Testing Split Type: {split_type}")
            
            if show_dynamic:
                click.echo("\n🎯 DYNAMIC THRESHOLD DETAILS:")
                click.echo("-" * 40)
                
                # Test dynamic threshold system
                threshold_config = await threshold_manager.get_dynamic_threshold(
                    strategy_type=strategy,
                    source=source,
                    split_type=split_type
                )
                
                click.echo(f"Phase: {threshold_config.phase.value}")
                click.echo(f"Sample Size: {threshold_config.sample_size}")
                click.echo(f"Minimum Threshold: {threshold_config.minimum_threshold:.1f}%")
                click.echo(f"Moderate Threshold: {threshold_config.moderate_threshold:.1f}%")
                click.echo(f"High Threshold: {threshold_config.high_threshold:.1f}%")
                click.echo(f"Current ROI: {threshold_config.current_roi:.1f}%")
                click.echo(f"Current Win Rate: {threshold_config.current_win_rate:.1%}")
                click.echo(f"Confidence Score: {threshold_config.confidence_score:.2f}")
                
                # Show bootstrap values for comparison
                bootstrap = threshold_manager.bootstrap_thresholds.get(strategy, {})
                if bootstrap:
                    click.echo(f"\n📋 BOOTSTRAP REFERENCE VALUES:")
                    click.echo(f"Min: {bootstrap.get('min', 'N/A')}%, Mod: {bootstrap.get('mod', 'N/A')}%, High: {bootstrap.get('high', 'N/A')}%")
            
            if test_signals:
                click.echo("\n🧪 TESTING SAMPLE SIGNALS:")
                click.echo("-" * 40)
                
                # Test various signal strengths
                test_differentials = [1.0, 2.5, 3.0, 4.0, 5.0, 8.0, 10.0]
                
                # Create processor to test actual threshold logic
                processor = SharpActionProcessor(repository, validator, config)
                processor.threshold_manager = threshold_manager
                
                for diff in test_differentials:
                    # Test the actual threshold logic
                    from mlb_sharp_betting.models.betting_analysis import ProfitableStrategy
                    test_strategy = ProfitableStrategy(
                        strategy_name="test_strategy",
                        source_book="test",
                        split_type=split_type,
                        win_rate=55.0,  # 55% win rate
                        roi=10.0,
                        total_bets=100,
                        confidence=0.8,
                        ci_lower=50.0,
                        ci_upper=60.0
                    )
                    
                    # Get actual threshold that would be used
                    actual_threshold = await processor._get_actual_threshold_for_debug(
                        test_strategy, diff, source, split_type
                    )
                    
                    # Test if signal would pass
                    would_pass = await processor._meets_strategy_threshold(
                        test_strategy, diff, source, split_type
                    )
                    
                    status = "✅ PASS" if would_pass else "❌ FAIL"
                    click.echo(f"Signal {diff:4.1f}% vs Threshold {actual_threshold:4.1f}% = {status}")
            
            click.echo("\n🎯 DIAGNOSIS COMPLETE")
            click.echo("If you see high threshold values (>8%), the dynamic system may not be working properly.")
            click.echo("If you see low threshold values (3-8%) but signals still fail, check the integration.")
            
        except Exception as e:
            click.echo(f"❌ Threshold debug failed: {e}")
            logger.error(f"❌ Threshold debug failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
        
        return True
    
    # Run the async function
    result = asyncio.run(run_threshold_debug())
    if not result:
        exit(1) 