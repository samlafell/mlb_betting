"""
Orchestrator Demo CLI Command

This command demonstrates the new Strategy Orchestrator that solves
the critical architectural disconnect between Phase 3A and Phase 3B.

Usage:
    uv run src/mlb_sharp_betting/cli.py orchestrator-demo --minutes 300
    uv run src/mlb_sharp_betting/cli.py orchestrator-demo --debug
"""

import asyncio
import click
from datetime import datetime, timedelta

from mlb_sharp_betting.services.adaptive_detector import get_adaptive_detector
from mlb_sharp_betting.services.strategy_orchestrator import get_strategy_orchestrator
from mlb_sharp_betting.core.logging import get_logger


@click.command(name='orchestrator-demo')
@click.option('--minutes', '-m', default=60, help='Minutes ahead to analyze')
@click.option('--debug', '-d', is_flag=True, help='Enable debug mode with detailed output')
@click.option('--force-refresh', '-f', is_flag=True, help='Force refresh of strategy configuration')
def orchestrator_demo(minutes: int, debug: bool, force_refresh: bool):
    """
    Demonstrate the new orchestrator-powered detection system.
    
    This command shows how the Strategy Orchestrator bridges the disconnect
    between Phase 3A (real-time detection) and Phase 3B (backtesting).
    """
    logger = get_logger(__name__)
    
    async def run_demo():
        try:
            print("🚀 ORCHESTRATOR-POWERED BETTING DETECTION DEMO")
            print("=" * 60)
            print("This demo shows the NEW Phase 3A that's informed by Phase 3B results!")
            print()
            
            # Initialize the adaptive detector (which uses the orchestrator)
            print("⚙️  Initializing orchestrator-powered detection system...")
            detector = await get_adaptive_detector()
            
            # Show strategy configuration comparison
            if debug:
                await _show_architecture_comparison()
            
            # Get strategy performance summary
            if debug or force_refresh:
                print("\n📊 CURRENT STRATEGY CONFIGURATION:")
                performance_summary = await detector.get_strategy_performance_summary()
                _display_strategy_summary(performance_summary)
            
            # Run orchestrator-powered analysis
            print(f"\n🎯 ANALYZING OPPORTUNITIES ({minutes} minutes ahead)...")
            result = await detector.analyze_opportunities(minutes_ahead=minutes, debug_mode=debug)
            
            # Show results summary
            print(f"\n📈 ORCHESTRATOR ANALYSIS COMPLETE")
            print("=" * 60)
            
            metadata = result.analysis_metadata
            if 'error' in metadata:
                print(f"❌ Error: {metadata['error']}")
                return
            
            print(f"✅ Analysis Type: {metadata.get('analysis_type', 'unknown')}")
            print(f"⚙️  Configuration: {metadata.get('configuration_version', 'unknown')}")
            print(f"📈 Enabled Strategies: {metadata.get('enabled_strategies', 0)}")
            print(f"🔄 Total Signals Generated: {metadata.get('total_signals', 0)}")
            print(f"✅ Signals After Filtering: {metadata.get('filtered_signals', 0)}")
            print(f"🎯 Final Opportunities: {result.total_opportunities}")
            
            # Show performance comparison if debug
            if debug:
                await _show_performance_comparison(result)
            
            # Show usage instructions
            print(f"\n💡 INTEGRATION INSTRUCTIONS:")
            print("1. Replace MasterBettingDetector calls with AdaptiveBettingDetector")
            print("2. The orchestrator automatically updates every 15 minutes")
            print("3. Strategies are enabled/disabled based on backtesting performance")
            print("4. Thresholds and confidence multipliers adjust dynamically")
            print("5. The same processors run in both backtesting and live detection")
            
            logger.info("Orchestrator demo completed successfully")
            
        except Exception as e:
            logger.error(f"Orchestrator demo failed: {e}")
            print(f"\n❌ Demo failed: {e}")
            raise
    
    # Run the async demo
    try:
        asyncio.run(run_demo())
    except KeyboardInterrupt:
        print("\n⚠️  Demo interrupted by user")
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        raise


async def _show_architecture_comparison():
    """Show the before/after architecture comparison"""
    print("\n🏗️  ARCHITECTURE COMPARISON")
    print("=" * 60)
    
    print("❌ OLD ARCHITECTURE (Disconnected):")
    print("   Phase 3A: MasterBettingDetector")
    print("   ├── Hardcoded SQL queries")
    print("   ├── Fixed thresholds (high=20.0, moderate=15.0)")
    print("   ├── Manual signal analysis logic")
    print("   └── No connection to backtesting results")
    print()
    print("   Phase 3B: BacktestingService")
    print("   ├── Dynamic ProcessorStrategyExecutor")
    print("   ├── Real strategy processors")
    print("   ├── Generates BacktestResult objects")
    print("   └── ❌ Results NEVER used by Phase 3A")
    print()
    
    print("✅ NEW ARCHITECTURE (Connected):")
    print("   Phase 3C: StrategyOrchestrator (NEW!)")
    print("   ├── Loads BacktestResult objects from Phase 3B")
    print("   ├── Generates dynamic configuration for Phase 3A")
    print("   ├── Creates performance-based thresholds")
    print("   └── Enables/disables strategies based on ROI")
    print()
    print("   Phase 3A: AdaptiveBettingDetector (NEW!)")
    print("   ├── Uses orchestrator configuration")
    print("   ├── Executes same processors as backtesting")
    print("   ├── Dynamic confidence multipliers")
    print("   └── ✅ Informed by backtesting performance")
    print()


def _display_strategy_summary(performance_summary):
    """Display strategy performance summary"""
    print(f"  📊 Total Strategies: {performance_summary['total_strategies']}")
    print(f"  ✅ Enabled: {performance_summary['enabled_strategies']}")
    print(f"  ❌ Disabled: {performance_summary['disabled_strategies']}")
    print(f"  🕐 Last Update: {performance_summary['last_updated']}")
    print(f"  📈 Version: {performance_summary['configuration_version']}")
    
    perf = performance_summary['performance_summary']
    print(f"  💰 Avg ROI (Enabled): {perf.get('avg_roi_enabled', 0):.1f}%")
    print(f"  🎯 Avg Win Rate (Enabled): {perf.get('avg_win_rate_enabled', 0):.1f}%")
    
    print("\n============================================================")
    print("📊 Configuration Version:", performance_summary['configuration_version'])
    print("🕐 Last Updated:", performance_summary['last_updated'])
    print()
    
    # Separate enabled and disabled strategies for cleaner display
    enabled_strategies = [s for s in performance_summary['strategy_details'] if s['enabled']]
    disabled_strategies = [s for s in performance_summary['strategy_details'] if not s['enabled']]
    
    print(f"✅ ENABLED STRATEGIES ({len(enabled_strategies)}):")
    for i, strategy in enumerate(enabled_strategies, 1):
        roi_icon = "🔥" if strategy['roi'] > 15 else "⭐" if strategy['roi'] > 10 else "🆗" if strategy['roi'] > 0 else "📉"
        trend_icon = "📈" if strategy['trend'] == "IMPROVING" else "📉" if strategy['trend'] == "DECLINING" else "➡️"
        
        print(f"  {i:2}. ✅ {strategy['name']}")
        print(f"     📊 ROI: {strategy['roi']:+.1f}% | WR: {strategy['win_rate']:.1f}% | Sample: {strategy['sample_size']}")
        print(f"     ⚙️  Confidence: {strategy['confidence_multiplier']:.2f}x | Weight: {strategy['ensemble_weight']:.2f}")
        print(f"     🎚️  Threshold: 25.0% (++0.2)")
        print(f"     {trend_icon} Trend: {strategy['trend']}")
        print()
    
    if disabled_strategies:
        print(f"❌ DISABLED STRATEGIES ({len(disabled_strategies)}):")
        for i, strategy in enumerate(disabled_strategies, 1):
            roi_icon = "🔥" if strategy['roi'] > 15 else "⭐" if strategy['roi'] > 10 else "🆗" if strategy['roi'] > 0 else "📉"
            trend_icon = "📈" if strategy['trend'] == "IMPROVING" else "📉" if strategy['trend'] == "DECLINING" else "➡️"
            
            print(f"  {i:2}. ❌ {strategy['name']}")
            print(f"     📊 ROI: {strategy['roi']:+.1f}% | WR: {strategy['win_rate']:.1f}% | Sample: {strategy['sample_size']}")
            print(f"     ⚙️  Confidence: {strategy['confidence_multiplier']:.2f}x | Weight: {strategy['ensemble_weight']:.2f}")
            print(f"     {trend_icon} Trend: {strategy['trend']} | Status: {strategy['status'].upper()}")
        print()


async def _show_performance_comparison(result):
    """Show performance comparison between old and new approaches"""
    print(f"\n📊 PERFORMANCE ANALYSIS")
    print("=" * 60)
    
    opportunities_by_type = result.opportunities_by_type
    total_opps = sum(opportunities_by_type.values())
    
    print(f"🎯 Signal Distribution:")
    for signal_type, count in opportunities_by_type.items():
        if count > 0:
            percentage = (count / total_opps) * 100 if total_opps > 0 else 0
            print(f"  • {signal_type.value}: {count} ({percentage:.1f}%)")
    
    print(f"\n🔄 Processing Pipeline:")
    metadata = result.analysis_metadata
    raw_signals = metadata.get('total_signals', 0)
    filtered_signals = metadata.get('filtered_signals', 0)
    final_opportunities = result.total_opportunities
    
    filter_rate = ((raw_signals - filtered_signals) / raw_signals * 100) if raw_signals > 0 else 0
    final_rate = (final_opportunities / raw_signals * 100) if raw_signals > 0 else 0
    
    print(f"  📥 Raw Signals: {raw_signals}")
    print(f"  🔄 After Juice Filter: {filtered_signals} ({filter_rate:.1f}% filtered)")
    print(f"  📤 Final Opportunities: {final_opportunities} ({final_rate:.1f}% conversion)")
    
    print(f"\n💡 Key Benefits:")
    print("  ✅ Strategies auto-enable/disable based on performance")
    print("  ✅ Thresholds adjust dynamically (not hardcoded)")
    print("  ✅ Same processors used in backtesting and live detection")
    print("  ✅ Confidence scores based on actual ROI performance")
    print("  ✅ Ensemble weighting prevents poor strategies from dominating")


def add_to_cli(cli_group):
    """Add this command to the CLI group"""
    cli_group.add_command(orchestrator_demo) 