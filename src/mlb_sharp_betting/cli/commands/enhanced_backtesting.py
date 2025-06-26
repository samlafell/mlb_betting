"""
Enhanced backtesting commands with data integration.
"""

import click
import asyncio
from datetime import datetime
from typing import Optional
import structlog

from ...services.enhanced_backtesting_service import EnhancedBacktestingService
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
def run_backtesting(lookback_days: int, ensure_fresh_data: bool, store_results: bool, 
                   max_data_age: int, detailed: bool):
    """🔬 Run comprehensive strategy backtesting with fresh data"""
    
    async def run_enhanced_backtesting():
        click.echo("🔬 ENHANCED BACKTESTING SERVICE")
        click.echo("=" * 60)
        click.echo(f"📊 Lookback Period: {lookback_days} days")
        click.echo(f"📡 Fresh Data: {'ENABLED' if ensure_fresh_data else 'DISABLED'}")
        click.echo(f"💾 Store Results: {'ENABLED' if store_results else 'DISABLED'}")
        
        try:
            enhanced_service = EnhancedBacktestingService(auto_collect_data=ensure_fresh_data)
            
            # Check data freshness if enabled
            if ensure_fresh_data:
                click.echo(f"\n🔍 Checking data freshness...")
                freshness_check = await enhanced_service.check_data_freshness(max_data_age)
                
                data_age = freshness_check.get('data_age_hours', 0)
                click.echo(f"   📅 Data Age: {data_age:.1f} hours")
                
                if freshness_check['needs_collection']:
                    click.echo(f"   📡 Fresh data collection required")
                else:
                    click.echo(f"   ✅ Data is fresh enough")
            
            # Run backtesting pipeline
            click.echo(f"\n🚀 Starting enhanced backtesting pipeline...")
            
            if ensure_fresh_data:
                results = await enhanced_service.run_daily_backtesting_pipeline_with_fresh_data()
            else:
                # Run standard backtesting
                backtest_results = await enhanced_service.run_daily_backtesting_pipeline()
                results = {
                    'backtesting_results': backtest_results,
                    'data_collection_metrics': None,
                    'steps_executed': ['backtesting'],
                    'execution_time_seconds': 0,
                    'errors': []
                }
            
            # Display results
            click.echo(f"\n✅ BACKTESTING COMPLETED")
            click.echo(f"   ⏱️  Total Time: {results['execution_time_seconds']:.2f}s")
            click.echo(f"   🔧 Steps: {', '.join(results['steps_executed'])}")
            
            if results['errors']:
                click.echo(f"   ❌ Errors: {len(results['errors'])}")
                for error in results['errors']:
                    click.echo(f"      • {error}")
            
            # Data collection metrics
            if results['data_collection_metrics']:
                metrics = results['data_collection_metrics']
                click.echo(f"\n📡 DATA COLLECTION METRICS:")
                click.echo(f"   📥 Records Processed: {metrics.get('parsed_records', 0)}")
                click.echo(f"   💾 Records Stored: {metrics.get('stored_records', 0)}")
                click.echo(f"   🎯 Sharp Indicators: {metrics.get('sharp_indicators', 0)}")
            
            # Backtesting results
            if results['backtesting_results']:
                backtest = results['backtesting_results']
                click.echo(f"\n🔬 BACKTESTING RESULTS:")
                click.echo(f"   📊 Strategies Analyzed: {backtest.total_strategies_analyzed}")
                click.echo(f"   ✅ Adequate Data: {backtest.strategies_with_adequate_data}")
                click.echo(f"   💰 Profitable: {backtest.profitable_strategies}")
                click.echo(f"   📉 Declining: {backtest.declining_strategies}")
                click.echo(f"   📈 Stable: {backtest.stable_strategies}")
                click.echo(f"   📋 Recommendations: {len(backtest.threshold_recommendations)}")
                click.echo(f"   🚨 Alerts: {len(backtest.strategy_alerts)}")
                click.echo(f"   📊 Data Quality: {backtest.data_completeness_pct:.1f}%")
                
                if detailed and backtest.strategy_metrics:
                    click.echo(f"\n📈 DETAILED STRATEGY PERFORMANCE:")
                    
                    # Sort by ROI
                    sorted_metrics = sorted(
                        backtest.strategy_metrics, 
                        key=lambda x: x.roi_per_100, 
                        reverse=True
                    )
                    
                    for metric in sorted_metrics[:10]:  # Top 10
                        click.echo(f"\n   🎯 {metric.strategy_name}")
                        click.echo(f"      📊 ROI: {metric.roi_per_100:.1f}% | WR: {metric.win_rate:.1f}%")
                        click.echo(f"      🎲 Bets: {metric.total_bets} | Wins: {metric.wins}")
                        click.echo(f"      📈 Sharpe: {metric.sharpe_ratio:.2f}")
                        click.echo(f"      🎚️  Statistical Significance: {'Yes' if metric.statistical_significance else 'No'}")
                        
                        if metric.trend_direction:
                            trend_emoji = {
                                'improving': '📈',
                                'declining': '📉',
                                'stable': '➡️'
                            }.get(metric.trend_direction, '❓')
                            click.echo(f"      {trend_emoji} Trend: {metric.trend_direction}")
                
                if backtest.threshold_recommendations:
                    click.echo(f"\n🎚️  THRESHOLD RECOMMENDATIONS:")
                    for rec in backtest.threshold_recommendations[:5]:  # Top 5
                        click.echo(f"   📊 {rec.strategy_name}")
                        click.echo(f"      🔄 {rec.current_threshold:.1f}% → {rec.recommended_threshold:.1f}%")
                        click.echo(f"      💡 {rec.justification}")
                        click.echo(f"      📈 Expected Improvement: {rec.expected_improvement:.1f}%")
                        if rec.requires_human_approval:
                            click.echo(f"      ⚠️  Requires manual approval")
                
            click.echo(f"\n🎉 Enhanced backtesting completed successfully!")
            
        except Exception as e:
            click.echo(f"❌ Enhanced backtesting failed: {e}")
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
    """⚡ Quick backtesting of top strategies only"""
    
    async def run_quick_backtest():
        click.echo("⚡ QUICK BACKTESTING SERVICE")
        click.echo("=" * 50)
        click.echo(f"🎯 Analyzing top {strategy_count} strategies")
        
        try:
            enhanced_service = EnhancedBacktestingService(auto_collect_data=False)
            
            # Run standard backtesting (no data collection)
            click.echo(f"\n🚀 Running quick strategy analysis...")
            backtest_results = await enhanced_service.run_daily_backtesting_pipeline()
            
            # Show summary results
            click.echo(f"\n⚡ QUICK RESULTS:")
            click.echo(f"   📊 Total Strategies: {backtest_results.total_strategies_analyzed}")
            click.echo(f"   💰 Profitable: {backtest_results.profitable_strategies}")
            
            if backtest_results.strategy_metrics:
                # Sort by ROI and show top strategies
                sorted_metrics = sorted(
                    backtest_results.strategy_metrics, 
                    key=lambda x: x.roi_per_100, 
                    reverse=True
                )
                
                click.echo(f"\n🏆 TOP {min(strategy_count, len(sorted_metrics))} STRATEGIES:")
                
                for i, metric in enumerate(sorted_metrics[:strategy_count], 1):
                    profitability = "💰" if metric.roi_per_100 > 0 else "📉"
                    significance = "✅" if metric.statistical_significance else "⚠️"
                    
                    click.echo(f"\n   {i}. {profitability} {metric.strategy_name}")
                    click.echo(f"      📊 ROI: {metric.roi_per_100:.1f}% | WR: {metric.win_rate:.1f}%")
                    click.echo(f"      🎲 Sample: {metric.total_bets} bets ({metric.wins} wins)")
                    click.echo(f"      {significance} Statistical Significance: {'Yes' if metric.statistical_significance else 'No'}")
                    
                    if metric.total_bets < 10:
                        click.echo(f"      ⚠️  Small sample size - results may not be reliable")
            
            click.echo(f"\n💡 Run 'mlb-cli backtest run --detailed' for comprehensive analysis")
            
        except Exception as e:
            click.echo(f"❌ Quick backtesting failed: {e}")
            raise
    
    try:
        asyncio.run(run_quick_backtest())
    except KeyboardInterrupt:
        click.echo("\n⚠️  Quick backtesting interrupted")
    except Exception:
        click.echo("❌ Quick backtesting failed")
        raise


@enhanced_backtesting_group.command('validate')
@click.option('--min-sample-size', type=int, default=10,
              help='Minimum sample size for validation (default: 10)')
@click.option('--min-roi', type=float, default=5.0,
              help='Minimum ROI threshold for validation (default: 5.0)')
def validate_strategies(min_sample_size: int, min_roi: float):
    """✅ Validate strategy performance and requirements"""
    
    async def run_validation():
        click.echo("✅ STRATEGY VALIDATION SERVICE")
        click.echo("=" * 50)
        click.echo(f"📊 Minimum Sample Size: {min_sample_size} bets")
        click.echo(f"💰 Minimum ROI: {min_roi}%")
        
        try:
            enhanced_service = EnhancedBacktestingService()
            
            # Validate pipeline requirements
            click.echo(f"\n🔍 Validating pipeline requirements...")
            validations = await enhanced_service.validate_pipeline_requirements()
            
            click.echo(f"\n🔧 SYSTEM VALIDATION:")
            for requirement, is_valid in validations.items():
                status = "✅" if is_valid else "❌"
                req_name = requirement.replace('_', ' ').title()
                click.echo(f"   {status} {req_name}")
            
            if not all(validations.values()):
                click.echo(f"\n⚠️  Some requirements failed - backtesting may not work properly")
                return
            
            # Check data freshness
            click.echo(f"\n📡 Checking data freshness...")
            freshness_check = await enhanced_service.check_data_freshness()
            
            age_status = "✅" if freshness_check['is_fresh'] else "⚠️"
            click.echo(f"   {age_status} Data Age: {freshness_check.get('data_age_hours', 0):.1f} hours")
            click.echo(f"   📊 Total Splits: {freshness_check.get('total_splits', 0):,}")
            click.echo(f"   🎮 Unique Games: {freshness_check.get('unique_games', 0)}")
            
            # Run quick strategy validation
            click.echo(f"\n🔬 Running strategy validation...")
            backtest_results = await enhanced_service.run_daily_backtesting_pipeline()
            
            # Analyze strategies against criteria
            valid_strategies = []
            invalid_strategies = []
            
            for metric in backtest_results.strategy_metrics:
                is_valid = (
                    metric.total_bets >= min_sample_size and
                    metric.roi_per_100 >= min_roi and
                    metric.statistical_significance
                )
                
                if is_valid:
                    valid_strategies.append(metric)
                else:
                    invalid_strategies.append(metric)
            
            # Display validation results
            click.echo(f"\n📊 VALIDATION RESULTS:")
            click.echo(f"   ✅ Valid Strategies: {len(valid_strategies)}")
            click.echo(f"   ❌ Invalid Strategies: {len(invalid_strategies)}")
            click.echo(f"   📈 Success Rate: {len(valid_strategies) / len(backtest_results.strategy_metrics) * 100:.1f}%")
            
            if valid_strategies:
                click.echo(f"\n✅ VALIDATED STRATEGIES:")
                for metric in sorted(valid_strategies, key=lambda x: x.roi_per_100, reverse=True)[:5]:
                    click.echo(f"   🎯 {metric.strategy_name}")
                    click.echo(f"      📊 ROI: {metric.roi_per_100:.1f}% | Sample: {metric.total_bets} bets")
                    click.echo(f"      🎚️  Statistical Significance: Yes")
            
            if invalid_strategies:
                click.echo(f"\n❌ STRATEGIES NEEDING IMPROVEMENT:")
                for metric in invalid_strategies[:3]:  # Show top 3 issues
                    issues = []
                    if metric.total_bets < min_sample_size:
                        issues.append(f"Small sample ({metric.total_bets} bets)")
                    if metric.roi_per_100 < min_roi:
                        issues.append(f"Low ROI ({metric.roi_per_100:.1f}%)")
                    if not metric.statistical_significance:
                        issues.append("Not statistically significant")
                    
                    click.echo(f"   ⚠️  {metric.strategy_name}")
                    click.echo(f"      Issues: {', '.join(issues)}")
            
            click.echo(f"\n💡 Validation completed")
            
        except Exception as e:
            click.echo(f"❌ Validation failed: {e}")
            raise
    
    try:
        asyncio.run(run_validation())
    except KeyboardInterrupt:
        click.echo("\n⚠️  Validation interrupted")
    except Exception:
        click.echo("❌ Validation failed")
        raise


@enhanced_backtesting_group.command('compare')
@click.option('--strategy1', required=True, help='First strategy to compare')
@click.option('--strategy2', required=True, help='Second strategy to compare')
@click.option('--lookback-days', type=int, default=30, help='Days to look back (default: 30)')
def compare_strategies(strategy1: str, strategy2: str, lookback_days: int):
    """📊 Compare performance between two strategies"""
    
    async def run_comparison():
        click.echo("📊 STRATEGY COMPARISON")
        click.echo("=" * 50)
        click.echo(f"🆚 Comparing: {strategy1} vs {strategy2}")
        click.echo(f"📅 Lookback: {lookback_days} days")
        
        try:
            enhanced_service = EnhancedBacktestingService(auto_collect_data=False)
            backtest_results = await enhanced_service.run_daily_backtesting_pipeline()
            
            # Find the two strategies
            strategy1_metrics = None
            strategy2_metrics = None
            
            for metric in backtest_results.strategy_metrics:
                if strategy1.lower() in metric.strategy_name.lower():
                    strategy1_metrics = metric
                if strategy2.lower() in metric.strategy_name.lower():
                    strategy2_metrics = metric
            
            if not strategy1_metrics:
                click.echo(f"❌ Strategy '{strategy1}' not found")
                return
            
            if not strategy2_metrics:
                click.echo(f"❌ Strategy '{strategy2}' not found")
                return
            
            # Display comparison
            click.echo(f"\n📊 PERFORMANCE COMPARISON:")
            
            strategies = [
                ("Strategy 1", strategy1_metrics),
                ("Strategy 2", strategy2_metrics)
            ]
            
            for name, metric in strategies:
                click.echo(f"\n🎯 {name}: {metric.strategy_name}")
                click.echo(f"   📊 ROI: {metric.roi_per_100:.1f}%")
                click.echo(f"   🎯 Win Rate: {metric.win_rate:.1f}%")
                click.echo(f"   🎲 Total Bets: {metric.total_bets}")
                click.echo(f"   📈 Sharpe Ratio: {metric.sharpe_ratio:.2f}")
                click.echo(f"   📊 Max Drawdown: {metric.max_drawdown:.1f}%")
                click.echo(f"   ✅ Statistical Significance: {'Yes' if metric.statistical_significance else 'No'}")
            
            # Determine winner
            click.echo(f"\n🏆 COMPARISON SUMMARY:")
            
            roi_winner = "Strategy 1" if strategy1_metrics.roi_per_100 > strategy2_metrics.roi_per_100 else "Strategy 2"
            wr_winner = "Strategy 1" if strategy1_metrics.win_rate > strategy2_metrics.win_rate else "Strategy 2"
            sharpe_winner = "Strategy 1" if strategy1_metrics.sharpe_ratio > strategy2_metrics.sharpe_ratio else "Strategy 2"
            
            click.echo(f"   💰 Higher ROI: {roi_winner}")
            click.echo(f"   🎯 Higher Win Rate: {wr_winner}")
            click.echo(f"   📈 Better Sharpe Ratio: {sharpe_winner}")
            
            # Overall recommendation
            strategy1_score = sum([
                strategy1_metrics.roi_per_100 > strategy2_metrics.roi_per_100,
                strategy1_metrics.win_rate > strategy2_metrics.win_rate,
                strategy1_metrics.sharpe_ratio > strategy2_metrics.sharpe_ratio,
                strategy1_metrics.statistical_significance
            ])
            
            if strategy1_score > 2:
                click.echo(f"\n🏆 Overall Winner: Strategy 1 ({strategy1_metrics.strategy_name})")
            elif strategy1_score < 2:
                click.echo(f"\n🏆 Overall Winner: Strategy 2 ({strategy2_metrics.strategy_name})")
            else:
                click.echo(f"\n🤝 Result: Very close performance - consider both strategies")
            
        except Exception as e:
            click.echo(f"❌ Comparison failed: {e}")
            raise
    
    try:
        asyncio.run(run_comparison())
    except KeyboardInterrupt:
        click.echo("\n⚠️  Comparison interrupted")
    except Exception:
        click.echo("❌ Comparison failed")
        raise 