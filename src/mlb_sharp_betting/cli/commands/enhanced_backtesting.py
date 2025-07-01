"""
Enhanced backtesting commands with data integration.
Updated to use the new BacktestingEngine from Phase 3 consolidation.
"""

import click
import asyncio
from datetime import datetime, timedelta
from typing import Optional
import structlog

from ...services.backtesting_engine import get_backtesting_engine
from ...services.pipeline_orchestrator import PipelineOrchestrator
from ...db.connection import get_db_manager

logger = structlog.get_logger(__name__)


@click.group()
def enhanced_backtesting_group():
    """🔬 Enhanced backtesting commands with data integration."""
    pass


@enhanced_backtesting_group.command('run')
@click.option('--lookback-days', type=int, default=30,
              help='Days to look back for backtesting (default: 30)')
@click.option('--ensure-fresh-data/--use-existing', default=True,
              help='Ensure fresh data before backtesting (default: enabled)')
@click.option('--store-results/--no-store', default=True,
              help='Store results in database (default: enabled)')
@click.option('--max-data-age', type=int, default=6,
              help='Maximum data age in hours before forcing collection (default: 6)')
@click.option('--detailed', is_flag=True,
              help='Show detailed backtesting results')
@click.option('--include-diagnostics', is_flag=True,
              help='Include comprehensive diagnostics')
@click.option('--include-alignment', is_flag=True,
              help='Validate live recommendation alignment')
def run_backtesting(lookback_days: int, ensure_fresh_data: bool, store_results: bool, 
                   max_data_age: int, detailed: bool, include_diagnostics: bool, include_alignment: bool):
    """🔬 Run comprehensive strategy backtesting with fresh data using BacktestingEngine"""
    
    async def run_enhanced_backtesting():
        click.echo("🔬 ENHANCED BACKTESTING ENGINE (Phase 3)")
        click.echo("=" * 60)
        click.echo(f"📊 Lookback Period: {lookback_days} days")
        click.echo(f"📡 Fresh Data: {'ENABLED' if ensure_fresh_data else 'DISABLED'}")
        click.echo(f"💾 Store Results: {'ENABLED' if store_results else 'DISABLED'}")
        click.echo(f"🔍 Diagnostics: {'ENABLED' if include_diagnostics else 'DISABLED'}")
        click.echo(f"🎯 Alignment Check: {'ENABLED' if include_alignment else 'DISABLED'}")
        
        try:
            backtesting_engine = get_backtesting_engine()
            await backtesting_engine.initialize()
            
            if ensure_fresh_data:
                click.echo(f"\n📡 Running daily pipeline with fresh data collection...")
                results = await backtesting_engine.run_daily_pipeline()
                
                if 'data_collection_metrics' in results:
                    metrics = results['data_collection_metrics']
                    click.echo(f"   📥 Records Processed: {metrics.get('parsed_records', 0)}")
                    click.echo(f"   💾 Records Stored: {metrics.get('stored_records', 0)}")
                    click.echo(f"   🎯 Sharp Indicators: {metrics.get('sharp_indicators', 0)}")
            else:
                end_date = datetime.now().strftime('%Y-%m-%d')
                start_date = (datetime.now() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
                
                click.echo(f"\n🚀 Running backtest from {start_date} to {end_date}...")
                results = await backtesting_engine.run_backtest(
                    start_date=start_date,
                    end_date=end_date,
                    include_diagnostics=include_diagnostics,
                    include_alignment=include_alignment
                )
            
            click.echo(f"\n✅ BACKTESTING COMPLETED")
            
            if 'execution_time_seconds' in results:
                click.echo(f"   ⏱️  Total Time: {results['execution_time_seconds']:.2f}s")
            
            if 'errors' in results and results['errors']:
                click.echo(f"   ❌ Errors: {len(results['errors'])}")
                for error in results['errors']:
                    click.echo(f"      • {error}")
            
            if 'backtest_results' in results:
                backtest = results['backtest_results']
                click.echo(f"\n🔬 BACKTESTING RESULTS:")
                
                if isinstance(backtest, dict):
                    click.echo(f"   📊 Strategies Analyzed: {backtest.get('total_strategies', 0)}")
                    click.echo(f"   💰 Profitable: {backtest.get('profitable_strategies', 0)}")
                    click.echo(f"   📊 Total Bets: {backtest.get('total_bets', 0)}")
                    click.echo(f"   📈 Average ROI: {backtest.get('average_roi', 0):.1f}%")
                    
                    if detailed and 'strategy_results' in backtest:
                        click.echo(f"\n📈 DETAILED STRATEGY PERFORMANCE:")
                        strategy_results = backtest['strategy_results']
                        
                        if isinstance(strategy_results, list):
                            sorted_results = sorted(
                                strategy_results, 
                                key=lambda x: x.get('roi_per_100', 0), 
                                reverse=True
                            )
                        else:
                            sorted_results = list(strategy_results.values())[:10]
                        
                        for result in sorted_results[:10]:  # Top 10
                            if isinstance(result, dict):
                                strategy_name = result.get('strategy_name', 'Unknown')
                                roi = result.get('roi_per_100', 0)
                                win_rate = result.get('win_rate', 0)
                                total_bets = result.get('total_bets', 0)
                                
                                click.echo(f"\n   🎯 {strategy_name}")
                                click.echo(f"      📊 ROI: {roi:.1f}% | WR: {win_rate:.1f}%")
                                click.echo(f"      🎲 Bets: {total_bets}")
            
            if include_diagnostics and 'diagnostics_results' in results:
                diagnostics = results['diagnostics_results']
                click.echo(f"\n🔍 DIAGNOSTICS RESULTS:")
                
                if isinstance(diagnostics, dict):
                    overall_status = "✅ PASS"
                    
                    for checkpoint, result in diagnostics.items():
                        if checkpoint.startswith('_'):  # Skip private attributes
                            continue
                            
                        status = result.get('status', 'UNKNOWN') if isinstance(result, dict) else str(result)
                        status_emoji = "✅" if status == 'PASS' else "❌" if status == 'FAIL' else "⚠️"
                        
                        click.echo(f"   {status_emoji} {checkpoint.replace('_', ' ').title()}: {status}")
                        
                        if isinstance(result, dict):
                            if 'issues' in result and result['issues']:
                                for issue in result['issues']:
                                    click.echo(f"      ⚠️  {issue}")
                            
                            if 'recommendations' in result and result['recommendations']:
                                for rec in result['recommendations']:
                                    click.echo(f"      💡 {rec}")
                        
                        if status == 'FAIL':
                            overall_status = "❌ FAIL"
                        elif status == 'WARN' and overall_status != "❌ FAIL":
                            overall_status = "⚠️ WARN"
                    
                    click.echo(f"\n🎯 OVERALL STATUS: {overall_status}")
                
                click.echo(f"\n🔍 Diagnostics completed!")
            
            if include_alignment and 'alignment_results' in results:
                alignment = results['alignment_results']
                click.echo(f"\n🎯 LIVE ALIGNMENT VALIDATION:")
                
                if isinstance(alignment, dict):
                    alignment_score = alignment.get('alignment_score', 0)
                    click.echo(f"   📊 Alignment Score: {alignment_score:.1f}%")
                    
                    if alignment_score >= 80:
                        click.echo(f"   ✅ EXCELLENT alignment with live recommendations")
                    elif alignment_score >= 60:
                        click.echo(f"   👍 GOOD alignment with live recommendations")
                    else:
                        click.echo(f"   ⚠️  POOR alignment - review may be needed")
            
            click.echo(f"\n🎉 Enhanced backtesting completed successfully!")
            
        except Exception as e:
            click.echo(f"❌ Enhanced backtesting failed: {e}")
            logger.error("Backtesting failed", error=str(e))
            raise
            
    try:
        asyncio.run(run_enhanced_backtesting())
    except KeyboardInterrupt:
        click.echo("\n⚠️  Backtesting interrupted by user")
    except Exception:
        click.echo("❌ Backtesting failed")
        raise


@enhanced_backtesting_group.command('quick')
@click.option('--strategy-count', type=int, default=5,
              help='Number of top strategies to analyze (default: 5)')
def quick_backtest(strategy_count: int):
    """⚡ Quick backtesting of top strategies using BacktestingEngine"""
    
    async def run_quick_backtest():
        click.echo("⚡ QUICK BACKTESTING ENGINE (Phase 3)")
        click.echo("=" * 50)
        click.echo(f"🎯 Analyzing top {strategy_count} strategies")
        
        try:
            backtesting_engine = get_backtesting_engine()
            await backtesting_engine.initialize()
            
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            
            click.echo(f"\n🚀 Running quick analysis for last 7 days...")
            results = await backtesting_engine.run_backtest(
                start_date=start_date,
                end_date=end_date,
                include_diagnostics=False,
                include_alignment=False
            )
            
            click.echo(f"\n⚡ QUICK RESULTS:")
            
            if 'backtest_results' in results:
                backtest = results['backtest_results']
                if isinstance(backtest, dict):
                    click.echo(f"   📊 Total Strategies: {backtest.get('total_strategies', 0)}")
                    click.echo(f"   💰 Profitable: {backtest.get('profitable_strategies', 0)}")
                    
                    if 'strategy_results' in backtest:
                        strategy_results = backtest['strategy_results']
                        
                        if isinstance(strategy_results, list):
                            sorted_results = sorted(
                                strategy_results, 
                                key=lambda x: x.get('roi_per_100', 0), 
                                reverse=True
                            )
                        else:
                            sorted_results = list(strategy_results.values())
                        
                        click.echo(f"\n🏆 TOP {min(strategy_count, len(sorted_results))} STRATEGIES:")
                        
                        for i, result in enumerate(sorted_results[:strategy_count], 1):
                            if isinstance(result, dict):
                                strategy_name = result.get('strategy_name', 'Unknown')
                                roi = result.get('roi_per_100', 0)
                                win_rate = result.get('win_rate', 0)
                                total_bets = result.get('total_bets', 0)
                                
                                profitability = "💰" if roi > 0 else "📉"
                                
                                click.echo(f"\n   {i}. {profitability} {strategy_name}")
                                click.echo(f"      📊 ROI: {roi:.1f}% | WR: {win_rate:.1f}%")
                                click.echo(f"      🎲 Sample: {total_bets} bets")
            
            click.echo(f"\n⚡ Quick backtesting completed!")
            
        except Exception as e:
            click.echo(f"❌ Quick backtesting failed: {e}")
            logger.error("Quick backtesting failed", error=str(e))
            raise
            
    try:
        asyncio.run(run_quick_backtest())
    except KeyboardInterrupt:
        click.echo("\n⚠️  Quick backtesting interrupted by user")
    except Exception:
        click.echo("❌ Quick backtesting failed")
        raise


@enhanced_backtesting_group.command('diagnostics')
def run_diagnostics():
    """🔍 Run comprehensive backtesting diagnostics using BacktestingEngine"""
    
    async def run_diagnostics_check():
        click.echo("🔍 BACKTESTING DIAGNOSTICS (Phase 3)")
        click.echo("=" * 50)
        
        try:
            backtesting_engine = get_backtesting_engine()
            await backtesting_engine.initialize()
            
            click.echo("🚀 Running 5-checkpoint diagnostic system...")
            diagnostics_results = await backtesting_engine.diagnostics.run_full_diagnostic()
            
            click.echo(f"\n🔍 DIAGNOSTIC RESULTS:")
            
            if isinstance(diagnostics_results, dict):
                overall_status = "✅ PASS"
                
                for checkpoint, result in diagnostics_results.items():
                    if checkpoint.startswith('_'):  # Skip private attributes
                        continue
                        
                    status = result.get('status', 'UNKNOWN') if isinstance(result, dict) else str(result)
                    status_emoji = "✅" if status == 'PASS' else "❌" if status == 'FAIL' else "⚠️"
                    
                    click.echo(f"   {status_emoji} {checkpoint.replace('_', ' ').title()}: {status}")
                    
                    if isinstance(result, dict):
                        if 'issues' in result and result['issues']:
                            for issue in result['issues']:
                                click.echo(f"      ⚠️  {issue}")
                        
                        if 'recommendations' in result and result['recommendations']:
                            for rec in result['recommendations']:
                                click.echo(f"      💡 {rec}")
                    
                    if status == 'FAIL':
                        overall_status = "❌ FAIL"
                    elif status == 'WARN' and overall_status != "❌ FAIL":
                        overall_status = "⚠️ WARN"
                
                click.echo(f"\n🎯 OVERALL STATUS: {overall_status}")
            
            click.echo(f"\n🔍 Diagnostics completed!")
            
        except Exception as e:
            click.echo(f"❌ Diagnostics failed: {e}")
            logger.error("Diagnostics failed", error=str(e))
            raise
            
    try:
        asyncio.run(run_diagnostics_check())
    except KeyboardInterrupt:
        click.echo("\n⚠️  Diagnostics interrupted by user")
    except Exception:
        click.echo("❌ Diagnostics failed")
        raise 