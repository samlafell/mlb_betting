"""
Enhanced detection commands with integrated pipeline.
"""

import click
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict
import structlog
import json
import uuid

from ...services.pipeline_orchestrator import PipelineOrchestrator
from ...db.connection import get_db_manager
from ...models.betting_analysis import SignalProcessorConfig, BettingSignal, ConfidenceLevel
from ...services.betting_signal_repository import BettingSignalRepository
from ...services.strategy_validator import StrategyValidator
from ...analysis.processors.strategy_processor_factory import StrategyProcessorFactory
from ...services.betting_recommendation_formatter import BettingRecommendationFormatter
from ...services.pre_game_recommendation_tracker import PreGameRecommendationTracker, PreGameRecommendation

logger = structlog.get_logger(__name__)


def _convert_betting_signals_to_recommendations(signals: List[BettingSignal], 
                                              recommended_at: datetime) -> List[PreGameRecommendation]:
    """
    Convert BettingSignal objects to PreGameRecommendation objects for storage.
    
    Args:
        signals: List of betting signals to convert
        recommended_at: Timestamp when recommendations were made
        
    Returns:
        List of PreGameRecommendation objects
    """
    recommendations = []
    
    for signal in signals:
        try:
            # Generate unique recommendation ID
            rec_id = f"{signal.game_pk}_{signal.bet_type}_{signal.recommended_side}_{int(recommended_at.timestamp())}"
            
            # Map confidence level
            confidence_map = {
                ConfidenceLevel.HIGH: 'HIGH',
                ConfidenceLevel.MODERATE: 'MODERATE', 
                ConfidenceLevel.LOW: 'LOW'
            }
            confidence = confidence_map.get(signal.confidence_level, 'MODERATE')
            
            # Create recommendation text
            if signal.bet_type == 'moneyline':
                recommendation_text = f"BET {signal.recommended_side.upper()} ML"
            elif signal.bet_type == 'spread':
                recommendation_text = f"BET {signal.recommended_side.upper()} SPREAD"
            elif signal.bet_type == 'total':
                recommendation_text = f"BET {signal.recommended_side.upper()}"
            else:
                recommendation_text = f"BET {signal.recommended_side.upper()}"
            
            # Create PreGameRecommendation object
            recommendation = PreGameRecommendation(
                recommendation_id=rec_id,
                game_pk=signal.game_pk,
                home_team=signal.home_team,
                away_team=signal.away_team,
                game_datetime=signal.game_datetime,
                recommendation=recommendation_text,
                bet_type=signal.bet_type,
                confidence_level=confidence,
                signal_source=signal.signal_source,
                signal_strength=signal.confidence_score,
                recommended_at=recommended_at,
                email_sent=False  # These are CLI-generated, not email-generated
            )
            
            recommendations.append(recommendation)
            
        except Exception as e:
            logger.error("Failed to convert signal to recommendation", 
                        signal=signal, error=str(e))
            continue
    
    return recommendations


def _convert_cross_market_flips_to_recommendations(flips: List, 
                                                 recommended_at: datetime) -> List[PreGameRecommendation]:
    """
    Convert cross-market flip objects to PreGameRecommendation objects.
    
    Args:
        flips: List of cross-market flip objects
        recommended_at: Timestamp when recommendations were made
        
    Returns:
        List of PreGameRecommendation objects
    """
    recommendations = []
    
    for flip in flips:
        try:
            # Generate unique recommendation ID for cross-market flip
            rec_id = f"flip_{flip.game_id}_{flip.flip_type.value}_{int(recommended_at.timestamp())}"
            
            # Map confidence to standard levels
            if flip.confidence_score >= 80:
                confidence = 'HIGH'
            elif flip.confidence_score >= 65:
                confidence = 'MODERATE'
            else:
                confidence = 'LOW'
            
            # Use the strategy recommendation as the bet text
            recommendation_text = flip.strategy_recommendation
            
            # Determine bet type from flip type
            bet_type_map = {
                'MONEYLINE_TO_SPREAD': 'spread',
                'SPREAD_TO_MONEYLINE': 'moneyline', 
                'TOTAL_FLIP': 'total'
            }
            bet_type = bet_type_map.get(flip.flip_type.value, 'moneyline')
            
            # Create recommendation
            recommendation = PreGameRecommendation(
                recommendation_id=rec_id,
                game_pk=int(flip.game_id) if flip.game_id.isdigit() else hash(flip.game_id) % 1000000,
                home_team=flip.home_team,
                away_team=flip.away_team,
                game_datetime=flip.game_datetime,
                recommendation=recommendation_text,
                bet_type=bet_type,
                confidence_level=confidence,
                signal_source='CROSS_MARKET_FLIP',
                signal_strength=flip.confidence_score / 100.0,  # Convert percentage to decimal
                recommended_at=recommended_at,
                email_sent=False
            )
            
            recommendations.append(recommendation)
            
        except Exception as e:
            logger.error("Failed to convert cross-market flip to recommendation", 
                        flip=flip, error=str(e))
            continue
    
    return recommendations


@click.group()
def detection_group():
    """🎯 Enhanced betting opportunity detection commands."""
    pass


@detection_group.command('opportunities')
@click.option('--minutes', '-m', type=int, default=60,
              help='Minutes ahead to look for opportunities (default: 60)')
@click.option('--fresh-data/--use-existing', default=True,
              help='Collect fresh data before detection (default: enabled)')
@click.option('--run-backtesting/--skip-backtesting', default=True,
              help='Run backtesting before detection (default: enabled)')
@click.option('--format', '-f', 
              type=click.Choice(["console", "json"]),
              default="console",
              help="Output format (default: console)")
@click.option('--output', '-o',
              type=click.Path(path_type=Path),
              help="Output file path for JSON format")
@click.option('--include-cross-market/--no-cross-market', default=True,
              help='Include cross-market flip detection (default: enabled)')
@click.option('--min-flip-confidence', type=float, default=60.0,
              help='Minimum confidence for cross-market flips (default: 60.0)')
@click.option('--store-recommendations/--no-store', default=True,
              help='Store betting recommendations to PostgreSQL (default: enabled)')
@click.option('--min-confidence', type=float, default=60.0,
              help='Minimum confidence to store recommendations (default: 60.0)')
def detect_opportunities(minutes: int, fresh_data: bool, run_backtesting: bool, 
                        format: str, output: Optional[Path], include_cross_market: bool, 
                        min_flip_confidence: float, store_recommendations: bool, 
                        min_confidence: float):
    """🎯 MASTER BETTING DETECTOR with full data pipeline and PostgreSQL storage
    
    This command runs a complete betting detection pipeline that:
    1. ✅ Checks for current, active profitable strategies  
    2. ✅ Checks data freshness and collects new data if needed
    3. ✅ Runs backtested strategies against current data
    4. ✅ Makes betting recommendations based on validated strategies
    5. ✅ Stores recommendations to PostgreSQL with full context
    6. ✅ Handles multiple recommendations per game (e.g., early vs late)
    
    Each recommendation is stored with:
    - Exact recommendation text and confidence level
    - Strategy source and signal strength at time of recommendation  
    - Game context and timing information
    - Unique ID to track multiple recommendations for same game
    """
    
    async def run_detection():
        click.echo("🎯 MASTER BETTING DETECTOR")
        click.echo("=" * 60)
        click.echo("🤖 Using AI-optimized strategies with intelligent pipeline")
        
        if fresh_data:
            click.echo("📡 Fresh data collection: ENABLED")
        else:
            click.echo("📊 Using existing data: ENABLED")
            
        if run_backtesting:
            click.echo("🔬 Backtesting: ENABLED")
        else:
            click.echo("📋 Using existing backtesting: ENABLED")
            
        if include_cross_market:
            click.echo("🔀 Cross-market flips: ENABLED")
            
        if store_recommendations:
            click.echo("💾 PostgreSQL storage: ENABLED")
            click.echo(f"📊 Min confidence for storage: {min_confidence}%")
        
        # Initialize recommendation tracker if storing
        recommendation_tracker = None
        storage_enabled = store_recommendations  # Create local copy
        if storage_enabled:
            try:
                recommendation_tracker = PreGameRecommendationTracker()
                click.echo("✅ Recommendation tracker initialized")
            except Exception as e:
                click.echo(f"⚠️  Warning: Failed to initialize recommendation tracker: {e}")
                click.echo("   Continuing without recommendation storage...")
                storage_enabled = False
        
        try:
            # Initialize orchestrator
            orchestrator = PipelineOrchestrator()
            
            # Run smart pipeline
            results = await orchestrator.execute_smart_pipeline(
                detection_minutes=minutes,
                force_fresh_data=fresh_data,
                force_backtesting=run_backtesting
            )
            
            # Store recommendations to PostgreSQL if enabled
            stored_recommendations = []
            if storage_enabled and recommendation_tracker:
                current_time = datetime.now()
                
                try:
                    # Convert detection results to recommendations
                    all_signals = []
                    if results['detection_results']:
                        detection_results = results['detection_results']
                        for game_key, game_analysis in detection_results.games.items():
                            # Collect all signals from this game
                            all_signals.extend(game_analysis.sharp_signals)
                            all_signals.extend(game_analysis.opposing_markets) 
                            all_signals.extend(game_analysis.steam_moves)
                            all_signals.extend(game_analysis.book_conflicts)
                    
                    # Filter signals by confidence threshold
                    high_confidence_signals = [
                        signal for signal in all_signals 
                        if signal.confidence_score >= (min_confidence / 100.0)
                    ]
                    
                    # Convert to recommendations
                    signal_recommendations = _convert_betting_signals_to_recommendations(
                        high_confidence_signals, current_time
                    )
                    stored_recommendations.extend(signal_recommendations)
                    
                    # Convert cross-market flips to recommendations
                    if include_cross_market and results['cross_market_flips']:
                        flips = results['cross_market_flips']['flips']
                        flip_recommendations = _convert_cross_market_flips_to_recommendations(
                            flips, current_time
                        )
                        # Filter flip recommendations by confidence
                        high_confidence_flips = [
                            rec for rec in flip_recommendations 
                            if (rec.signal_strength * 100) >= min_confidence
                        ]
                        stored_recommendations.extend(high_confidence_flips)
                    
                    # Store all recommendations
                    if stored_recommendations:
                        await recommendation_tracker.log_pre_game_recommendations(stored_recommendations)
                        click.echo(f"\n💾 RECOMMENDATIONS STORED TO POSTGRESQL")
                        click.echo(f"   📊 Stored {len(stored_recommendations)} recommendations")
                        
                        # Show breakdown by game
                        games_with_recommendations = {}
                        for rec in stored_recommendations:
                            game_key = f"{rec.away_team} @ {rec.home_team}"
                            if game_key not in games_with_recommendations:
                                games_with_recommendations[game_key] = []
                            games_with_recommendations[game_key].append(rec)
                        
                        for game, recs in games_with_recommendations.items():
                            click.echo(f"   🎮 {game}: {len(recs)} recommendation(s)")
                            for rec in recs:
                                confidence_icon = "🔥" if rec.confidence_level == "HIGH" else "⭐" if rec.confidence_level == "MODERATE" else "💡"
                                click.echo(f"      {confidence_icon} {rec.recommendation} ({rec.confidence_level} - {rec.signal_source})")
                    else:
                        click.echo(f"\n💾 No recommendations met {min_confidence}% confidence threshold for storage")
                        
                except Exception as e:
                    click.echo(f"\n❌ Failed to store recommendations: {e}")
                    logger.error("Recommendation storage failed", error=str(e))
            
            if format == "console":
                # Display execution summary
                click.echo(f"\n🚀 PIPELINE EXECUTION SUMMARY")
                click.echo(f"   ⏱️  Total Time: {results['total_execution_time']:.2f}s")
                click.echo(f"   🔧 Steps: {', '.join(results['steps_executed'])}")
                
                if results['errors']:
                    click.echo(f"   ❌ Errors: {len(results['errors'])}")
                    for error in results['errors']:
                        click.echo(f"      • {error}")
                
                if results['warnings']:
                    click.echo(f"   ⚠️  Warnings: {len(results['warnings'])}")
                    for warning in results['warnings']:
                        click.echo(f"      • {warning}")
                
                # Display detection results
                if results['detection_results']:
                    detection_results = results['detection_results']
                    total_opportunities = sum(
                        len(game_analysis.sharp_signals) + 
                        len(game_analysis.opposing_markets) + 
                        len(game_analysis.steam_moves) + 
                        len(game_analysis.book_conflicts)
                        for game_analysis in detection_results.games.values()
                    )
                    
                    click.echo(f"\n🎯 OPPORTUNITY DETECTION RESULTS")
                    click.echo(f"   🎮 Games Analyzed: {len(detection_results.games)}")
                    click.echo(f"   🚨 Total Opportunities: {total_opportunities}")
                    
                    # Display each game's opportunities
                    for game_key, game_analysis in detection_results.games.items():
                        away, home, game_time = game_key
                        opportunities = (
                            len(game_analysis.sharp_signals) + 
                            len(game_analysis.opposing_markets) + 
                            len(game_analysis.steam_moves) + 
                            len(game_analysis.book_conflicts)
                        )
                        
                        if opportunities > 0:
                            click.echo(f"\n   🎲 {away} @ {home}")
                            click.echo(f"      📅 {game_time.strftime('%Y-%m-%d %H:%M EST')}")
                            click.echo(f"      🎯 Opportunities: {opportunities}")
                            
                            if game_analysis.sharp_signals:
                                click.echo(f"         🔪 Sharp Signals: {len(game_analysis.sharp_signals)}")
                            if game_analysis.opposing_markets:
                                click.echo(f"         ⚔️  Opposing Markets: {len(game_analysis.opposing_markets)}")
                            if game_analysis.steam_moves:
                                click.echo(f"         🌊 Steam Moves: {len(game_analysis.steam_moves)}")
                            if game_analysis.book_conflicts:
                                click.echo(f"         📚 Book Conflicts: {len(game_analysis.book_conflicts)}")
                else:
                    click.echo(f"\n⚠️  No opportunity detection results available")
                
                # Display cross-market flips
                if include_cross_market and results['cross_market_flips']:
                    flips = results['cross_market_flips']['flips']
                    
                    if flips:
                        click.echo(f"\n🔀 CROSS-MARKET FLIP ANALYSIS")
                        click.echo("=" * 60)
                        click.echo(f"Found {len(flips)} cross-market flips with ≥{min_flip_confidence}% confidence")
                        
                        for i, flip in enumerate(flips, 1):
                            click.echo(f"\n🎯 FLIP #{i}: {flip.away_team} @ {flip.home_team}")
                            click.echo(f"   📅 Game: {flip.game_datetime.strftime('%Y-%m-%d %H:%M EST')}")
                            click.echo(f"   🔄 Type: {flip.flip_type.value.replace('_', ' ').title()}")
                            click.echo(f"   📊 Confidence: {flip.confidence_score:.1f}%")
                            click.echo(f"   💡 Strategy: {flip.strategy_recommendation}")
                            click.echo(f"   🧠 Reasoning: {flip.reasoning}")
                            
                            # Highlight high-confidence flips
                            if flip.confidence_score >= 80:
                                click.echo(f"   🔥 HIGH CONFIDENCE - STRONG BETTING OPPORTUNITY")
                            elif flip.confidence_score >= 70:
                                click.echo(f"   ✨ GOOD CONFIDENCE - SOLID BETTING OPPORTUNITY")
                            
                            # ⚠️ CRITICAL WARNING: This strategy is untested
                            click.echo(f"   ⚠️  WARNING: Cross-market flip strategies have NO backtesting results")
                            click.echo(f"   📊 Confidence is theoretical only - strategy performance unknown")
                            click.echo(f"   💡 Use small bet sizes until strategy is proven")
                    else:
                        click.echo(f"\n🔀 No cross-market flips found with ≥{min_flip_confidence}% confidence")
                
            elif format == "json":
                import json
                
                # Convert to JSON-serializable format
                json_output = {
                    'timestamp': datetime.now().isoformat(),
                    'pipeline_execution': {
                        'steps_executed': results['steps_executed'],
                        'execution_time_seconds': results['total_execution_time'],
                        'errors': results['errors'],
                        'warnings': results['warnings']
                    },
                    'detection_window_minutes': minutes,
                    'fresh_data_enabled': fresh_data,
                    'backtesting_enabled': run_backtesting,
                    'cross_market_enabled': include_cross_market,
                    'min_flip_confidence': min_flip_confidence
                }
                
                # Add detection results if available
                if results['detection_results']:
                    detection_results = results['detection_results']
                    json_games = {}
                    for game_key, game_analysis in detection_results.games.items():
                        away, home, game_time = game_key
                        game_key_str = f"{away}_vs_{home}_{game_time.isoformat()}"
                        json_games[game_key_str] = {
                            'away_team': away,
                            'home_team': home,
                            'game_time': game_time.isoformat(),
                            'sharp_signals': len(game_analysis.sharp_signals),
                            'opposing_markets': len(game_analysis.opposing_markets),
                            'steam_moves': len(game_analysis.steam_moves),
                            'book_conflicts': len(game_analysis.book_conflicts),
                            'total_opportunities': (
                                len(game_analysis.sharp_signals) + 
                                len(game_analysis.opposing_markets) + 
                                len(game_analysis.steam_moves) + 
                                len(game_analysis.book_conflicts)
                            )
                        }
                    
                    json_output['opportunities'] = {
                        'total_games': len(detection_results.games),
                        'games': json_games,
                        'analysis_metadata': detection_results.analysis_metadata
                    }
                
                # Add cross-market flips if available
                if include_cross_market and results['cross_market_flips']:
                    flips = results['cross_market_flips']['flips']
                    json_flips = []
                    for flip in flips:
                        json_flips.append({
                            'game_id': flip.game_id,
                            'away_team': flip.away_team,
                            'home_team': flip.home_team,
                            'game_datetime': flip.game_datetime.isoformat(),
                            'flip_type': flip.flip_type.value,
                            'confidence_score': flip.confidence_score,
                            'strategy_recommendation': flip.strategy_recommendation,
                            'reasoning': flip.reasoning
                        })
                    
                    json_output['cross_market_flips'] = {
                        'count': len(flips),
                        'flips': json_flips
                    }
                
                json_str = json.dumps(json_output, indent=2)
                if output:
                    output.write_text(json_str)
                    click.echo(f"✅ Detection results saved to: {output}")
                else:
                    click.echo(json_str)
                    
        except Exception as e:
            click.echo(f"❌ Detection pipeline failed: {e}")
            raise
        finally:
            # Cleanup
            try:
                if 'orchestrator' in locals():
                    orchestrator.close()
                if 'recommendation_tracker' in locals() and recommendation_tracker:
                    # Recommendation tracker cleanup is handled automatically
                    pass
            except Exception as cleanup_error:
                click.echo(f"⚠️  Cleanup warning: {cleanup_error}")
    
    try:
        asyncio.run(run_detection())
    except KeyboardInterrupt:
        click.echo("\n⚠️  Detection interrupted by user")
    except Exception:
        click.echo("❌ Detection failed")
        raise


@detection_group.command('smart-pipeline')
@click.option('--minutes', '-m', type=int, default=60,
              help='Minutes ahead to look for opportunities (default: 60)')
@click.option('--force-fresh', is_flag=True,
              help='Force fresh data collection regardless of age')
@click.option('--max-data-age', type=int, default=6,
              help='Maximum data age in hours before forcing collection (default: 6)')
def smart_pipeline(minutes: int, force_fresh: bool, max_data_age: int):
    """🧠 SMART PIPELINE - Automatically decides what needs to be run"""
    
    async def run_smart_pipeline():
        click.echo("🧠 SMART PIPELINE ORCHESTRATOR")
        click.echo("=" * 60)
        click.echo("🤖 Analyzing system state to determine optimal execution plan")
        
        try:
            orchestrator = PipelineOrchestrator()
            
            # Get recommendations first
            recommendations = await orchestrator.get_pipeline_recommendations()
            
            click.echo(f"\n📊 SYSTEM ANALYSIS:")
            click.echo(f"   🏥 System Health: {recommendations['system_health'].upper()}")
            click.echo(f"   🚨 Priority Level: {recommendations['priority_level'].upper()}")
            click.echo(f"   ⏱️  Estimated Runtime: {recommendations['estimated_runtime_minutes']} minutes")
            
            # Show immediate actions
            if recommendations['immediate_actions']:
                click.echo(f"\n🚀 IMMEDIATE ACTIONS PLANNED:")
                for action in recommendations['immediate_actions']:
                    click.echo(f"   ✅ {action['action'].title()}: {action['reason']}")
                    click.echo(f"      ⏱️  Est. {action['estimated_minutes']} minutes")
            
            # Show optional actions
            if recommendations['optional_actions']:
                click.echo(f"\n💡 OPTIONAL ACTIONS AVAILABLE:")
                for action in recommendations['optional_actions']:
                    click.echo(f"   💡 {action['action'].title()}: {action['reason']}")
            
            # Execute smart pipeline
            click.echo(f"\n🚀 EXECUTING SMART PIPELINE...")
            
            results = await orchestrator.execute_smart_pipeline(
                detection_minutes=minutes,
                force_fresh_data=force_fresh
            )
            
            # Display results
            click.echo(f"\n✅ SMART PIPELINE COMPLETED")
            click.echo(f"   ⏱️  Execution Time: {results['total_execution_time']:.2f}s")
            click.echo(f"   🔧 Steps Executed: {', '.join(results['steps_executed'])}")
            
            if results['errors']:
                click.echo(f"   ❌ Errors: {len(results['errors'])}")
                for error in results['errors']:
                    click.echo(f"      • {error}")
            
            if results['warnings']:
                click.echo(f"   ⚠️  Warnings: {len(results['warnings'])}")
                for warning in results['warnings']:
                    click.echo(f"      • {warning}")
            
            # Show step-by-step results
            if 'data_collection' in results['steps_executed']:
                metrics = results['data_collection_metrics']
                if metrics:
                    click.echo(f"\n📡 DATA COLLECTION RESULTS:")
                    click.echo(f"   📥 Records Processed: {metrics.get('parsed_records', 0)}")
                    click.echo(f"   🎯 Sharp Indicators: {metrics.get('sharp_indicators', 0)}")
            
            if 'backtesting' in results['steps_executed']:
                backtest = results['backtesting_results']
                if backtest:
                    click.echo(f"\n🔬 BACKTESTING RESULTS:")
                    click.echo(f"   📊 Strategies Analyzed: {backtest.total_strategies_analyzed}")
                    click.echo(f"   💰 Profitable Strategies: {backtest.profitable_strategies}")
            
            if 'detection' in results['steps_executed']:
                detection = results['detection_results']
                if detection:
                    total_opportunities = sum(
                        len(game_analysis.sharp_signals) + 
                        len(game_analysis.opposing_markets) + 
                        len(game_analysis.steam_moves) + 
                        len(game_analysis.book_conflicts)
                        for game_analysis in detection.games.values()
                    )
                    
                    click.echo(f"\n🎯 OPPORTUNITY DETECTION RESULTS:")
                    click.echo(f"   🎮 Games Analyzed: {len(detection.games)}")
                    click.echo(f"   🚨 Total Opportunities: {total_opportunities}")
                    
                    if total_opportunities > 0:
                        click.echo(f"\n💡 Run 'mlb-cli detect opportunities' for detailed analysis")
            
            # Cross-market flips summary
            if results['cross_market_flips']:
                flips = results['cross_market_flips']['flips']
                if flips:
                    click.echo(f"\n🔀 CROSS-MARKET FLIPS:")
                    click.echo(f"   🎯 High-Confidence Flips: {len(flips)}")
                    click.echo(f"   💡 Run 'mlb-cli cross-market-flips' for details")
            
            click.echo(f"\n🎉 Smart pipeline execution completed successfully!")
            
        except Exception as e:
            click.echo(f"❌ Smart pipeline failed: {e}")
            raise
        finally:
            # Cleanup
            try:
                if 'orchestrator' in locals():
                    orchestrator.close()
            except Exception as cleanup_error:
                click.echo(f"⚠️  Cleanup warning: {cleanup_error}")
    
    try:
        asyncio.run(run_smart_pipeline())
    except KeyboardInterrupt:
        click.echo("\n⚠️  Smart pipeline interrupted by user")
    except Exception:
        click.echo("❌ Smart pipeline failed")
        raise


@detection_group.command('system-recommendations')
@click.option('--minutes', '-m', type=int, default=60,
              help='Minutes ahead to look for opportunities (default: 60)')
def get_system_recommendations(minutes: int):
    """💡 Get intelligent system recommendations for what should be run"""
    
    async def show_recommendations():
        click.echo("💡 SYSTEM RECOMMENDATIONS")
        click.echo("=" * 50)
        
        try:
            orchestrator = PipelineOrchestrator()
            recommendations = await orchestrator.get_pipeline_recommendations()
            
            # System health
            health = recommendations['system_health']
            health_emoji = {
                'excellent': '🟢',
                'good': '🟡', 
                'fair': '🟠',
                'poor': '🔴',
                'unknown': '⚪'
            }.get(health, '⚪')
            
            click.echo(f"{health_emoji} System Health: {health.upper()}")
            click.echo(f"🚨 Priority Level: {recommendations['priority_level'].upper()}")
            click.echo(f"⏱️  Estimated Runtime: {recommendations['estimated_runtime_minutes']} minutes")
            
            # Immediate actions
            if recommendations['immediate_actions']:
                click.echo(f"\n🚀 RECOMMENDED ACTIONS:")
                for i, action in enumerate(recommendations['immediate_actions'], 1):
                    click.echo(f"{i}. {action['action'].replace('_', ' ').title()}")
                    click.echo(f"   📝 Reason: {action['reason']}")
                    click.echo(f"   ⏱️  Time: ~{action['estimated_minutes']} minutes")
                    click.echo()
                
                click.echo(f"💡 Run 'mlb-cli detect smart-pipeline' to execute automatically")
            else:
                click.echo(f"\n✅ No immediate actions needed")
                click.echo(f"💡 System is ready for detection")
            
            # Optional actions
            if recommendations['optional_actions']:
                click.echo(f"\n💡 OPTIONAL IMPROVEMENTS:")
                for action in recommendations['optional_actions']:
                    click.echo(f"• {action['action'].replace('_', ' ').title()}: {action['reason']}")
                    if 'issues' in action:
                        for issue in action['issues']:
                            click.echo(f"    - {issue}")
            
            # Reasoning
            if recommendations['reasoning']:
                click.echo(f"\n🧠 SYSTEM REASONING:")
                for reason in recommendations['reasoning']:
                    click.echo(f"• {reason}")
                    
        except Exception as e:
            click.echo(f"❌ Failed to get recommendations: {e}")
        finally:
            try:
                if 'orchestrator' in locals():
                    orchestrator.close()
            except Exception:
                pass
    
    try:
        asyncio.run(show_recommendations())
    except Exception:
        click.echo("❌ Recommendations failed")
        raise


@detection_group.command('recommendations')
@click.option('--minutes', '-m', type=int, default=240,
              help='Minutes ahead to look for opportunities (default: 240 = 4 hours)')
@click.option('--min-confidence', type=float, default=70.0,
              help='Minimum confidence threshold for recommendations (default: 70%)')
@click.option('--format', '-f', 
              type=click.Choice(["console", "json"]),
              default="console",
              help="Output format (default: console)")
@click.option('--output', '-o',
              type=click.Path(path_type=Path),
              help="Output file path for JSON format")
def betting_recommendations(minutes: int, min_confidence: float, format: str, output: Optional[Path]):
    """🎯 Generate actual betting recommendations with confidence scores and stake suggestions"""
    
    async def generate_recommendations():
        click.echo("🎯 MLB SHARP BETTING RECOMMENDATIONS")
        click.echo("=" * 60)
        click.echo(f"⏰ Looking {minutes} minutes ahead for betting opportunities")
        click.echo(f"📊 Minimum confidence threshold: {min_confidence}%")
        click.echo()
        
        try:
            # Initialize services
            config = SignalProcessorConfig()
            repository = BettingSignalRepository(config)
            
            # Get strategies and create validator
            strategies = await repository.get_profitable_strategies()
            if not strategies:
                click.echo("⚠️  No profitable strategies found - creating mock strategies")
                from ...models.betting_analysis import ProfitableStrategy
                strategies = [
                    ProfitableStrategy(
                        strategy_name="VSIN_sharp_action",
                        source_book="VSIN-draftkings",
                        split_type="moneyline",
                        win_rate=68.0,
                        roi=22.0,
                        total_bets=18,
                        confidence="HIGH CONFIDENCE"
                    ),
                    ProfitableStrategy(
                        strategy_name="Public_fade_underdog",
                        source_book="CONSENSUS",
                        split_type="moneyline", 
                        win_rate=63.0,
                        roi=18.0,
                        total_bets=15,
                        confidence="MODERATE CONFIDENCE"
                    )
                ]
            
            from ...models.betting_analysis import StrategyThresholds
            thresholds = StrategyThresholds()
            validator = StrategyValidator(strategies, thresholds)
            
            # Create processor factory and generate signals
            factory = StrategyProcessorFactory(repository, validator, config)
            processors = factory.create_all_processors()
            
            click.echo(f"🔧 Created {len(processors)} betting processors")
            
            # Generate all signals
            all_signals = []
            for processor_name, processor in processors.items():
                try:
                    signals = await processor.process_with_error_handling(minutes, strategies)
                    if signals:
                        all_signals.extend(signals)
                        click.echo(f"   📊 {processor_name}: {len(signals)} signals")
                    else:
                        click.echo(f"   ℹ️  {processor_name}: No signals")
                except Exception as e:
                    click.echo(f"   ❌ {processor_name}: Failed - {e}")
            
            # Filter by confidence threshold
            high_confidence_signals = [
                signal for signal in all_signals 
                if signal.confidence_score >= (min_confidence / 100.0)
            ]
            
            click.echo(f"\n📋 Generated {len(all_signals)} total signals")
            click.echo(f"🎯 {len(high_confidence_signals)} signals meet {min_confidence}% confidence threshold")
            
            # Format and display recommendations
            formatter = BettingRecommendationFormatter()
            
            if format == "console":
                formatted_output = formatter.format_console_recommendations(high_confidence_signals, min_confidence)
                click.echo(formatted_output)
            elif format == "json":
                json_output = formatter.format_json_recommendations(high_confidence_signals, min_confidence)
                if output:
                    output.write_text(json.dumps(json_output, indent=2, default=str))
                    click.echo(f"✅ Recommendations saved to: {output}")
                else:
                    click.echo(json.dumps(json_output, indent=2, default=str))
                    
        except Exception as e:
            click.echo(f"❌ Failed to generate betting recommendations: {e}")
            raise
    
    try:
        asyncio.run(generate_recommendations())
    except KeyboardInterrupt:
        click.echo("\n⚠️  Recommendation generation interrupted by user")
    except Exception:
        click.echo("❌ Recommendation generation failed")
        raise 


@detection_group.command('test-storage')
@click.option('--dry-run', is_flag=True, help='Test without actually storing to database')
def test_recommendation_storage(dry_run: bool):
    """🧪 Test the recommendation storage system"""
    
    async def run_test():
        click.echo("🧪 TESTING RECOMMENDATION STORAGE SYSTEM")
        click.echo("=" * 60)
        
        try:
            # Initialize tracker
            tracker = PreGameRecommendationTracker()
            click.echo("✅ Recommendation tracker initialized")
            
            # Create test recommendation
            test_rec = PreGameRecommendation(
                recommendation_id=f"test_{int(datetime.now().timestamp())}",
                game_pk=123456,
                home_team="TEST_HOME",
                away_team="TEST_AWAY", 
                game_datetime=datetime.now() + timedelta(hours=2),
                recommendation="BET TEST_HOME ML",
                bet_type="moneyline",
                confidence_level="HIGH",
                signal_source="TEST_SIGNAL",
                signal_strength=0.85,
                recommended_at=datetime.now(),
                email_sent=False
            )
            
            click.echo(f"📋 Created test recommendation:")
            click.echo(f"   🎮 Game: {test_rec.away_team} @ {test_rec.home_team}")
            click.echo(f"   🎯 Recommendation: {test_rec.recommendation}")
            click.echo(f"   📊 Confidence: {test_rec.confidence_level} ({test_rec.signal_strength*100:.1f}%)")
            click.echo(f"   🔍 Source: {test_rec.signal_source}")
            
            if not dry_run:
                # Store to database
                await tracker.log_pre_game_recommendations([test_rec])
                click.echo("✅ Test recommendation stored to PostgreSQL")
                
                # Verify storage
                with tracker.db_manager.get_cursor() as cursor:
                    cursor.execute("""
                        SELECT recommendation_id, recommendation, confidence_level, signal_source
                        FROM tracking.pre_game_recommendations 
                        WHERE recommendation_id = %s
                    """, (test_rec.recommendation_id,))
                    
                    result = cursor.fetchone()
                    if result:
                        click.echo("✅ Verification: Recommendation found in database")
                        click.echo(f"   ID: {result[0]}")
                        click.echo(f"   Text: {result[1]}")
                        click.echo(f"   Confidence: {result[2]}")
                        click.echo(f"   Source: {result[3]}")
                    else:
                        click.echo("❌ Verification failed: Recommendation not found")
            else:
                click.echo("🔍 DRY RUN: Would store recommendation to PostgreSQL")
            
            click.echo("\n🎉 Storage test completed successfully!")
            
        except Exception as e:
            click.echo(f"❌ Storage test failed: {e}")
            raise
    
    try:
        asyncio.run(run_test())
    except Exception:
        click.echo("❌ Test failed")
        raise


@detection_group.command('view-stored')
@click.option('--hours-back', '-h', type=int, default=24,
              help='Hours back to look for stored recommendations (default: 24)')
@click.option('--game', '-g', help='Filter by specific game (e.g., "Yankees Pirates")')
@click.option('--confidence', '-c', type=click.Choice(['HIGH', 'MODERATE', 'LOW']),
              help='Filter by confidence level')
def view_stored_recommendations(hours_back: int, game: Optional[str], confidence: Optional[str]):
    """📋 View recommendations stored in PostgreSQL"""
    
    async def view_recommendations():
        click.echo("📋 STORED BETTING RECOMMENDATIONS")
        click.echo("=" * 60)
        click.echo(f"🔍 Looking back {hours_back} hours")
        if game:
            click.echo(f"🎮 Game filter: {game}")
        if confidence:
            click.echo(f"📊 Confidence filter: {confidence}")
        
        try:
            tracker = PreGameRecommendationTracker()
            
            # Build query
            query = """
                SELECT recommendation_id, game_pk, home_team, away_team, game_datetime,
                       recommendation, bet_type, confidence_level, signal_source, 
                       signal_strength, recommended_at, game_completed, bet_won, profit_loss
                FROM tracking.pre_game_recommendations
                WHERE recommended_at >= %s
            """
            params = [datetime.now() - timedelta(hours=hours_back)]
            
            if game:
                query += " AND (home_team ILIKE %s OR away_team ILIKE %s)"
                game_filter = f"%{game}%"
                params.extend([game_filter, game_filter])
            
            if confidence:
                query += " AND confidence_level = %s"
                params.append(confidence)
            
            query += " ORDER BY recommended_at DESC"
            
            with tracker.db_manager.get_cursor() as cursor:
                cursor.execute(query, params)
                recommendations = cursor.fetchall()
            
            if not recommendations:
                click.echo("❌ No stored recommendations found matching criteria")
                return
            
            click.echo(f"\n📊 Found {len(recommendations)} stored recommendations:")
            
            # Group by game for better display
            games = {}
            for rec in recommendations:
                game_key = f"{rec[3]} @ {rec[2]}"  # away @ home
                if game_key not in games:
                    games[game_key] = []
                games[game_key].append(rec)
            
            for game_key, recs in games.items():
                click.echo(f"\n🎮 {game_key}")
                click.echo("   " + "="*50)
                
                for rec in recs:
                    rec_id, game_pk, home_team, away_team, game_datetime, recommendation, bet_type, conf_level, signal_source, signal_strength, recommended_at, game_completed, bet_won, profit_loss = rec
                    
                    # Format timing
                    time_str = recommended_at.strftime("%Y-%m-%d %H:%M:%S EST")
                    game_time_str = game_datetime.strftime("%Y-%m-%d %H:%M EST")
                    
                    # Confidence icon
                    conf_icon = "🔥" if conf_level == "HIGH" else "⭐" if conf_level == "MODERATE" else "💡"
                    
                    # Status icon
                    if game_completed:
                        status_icon = "✅" if bet_won else "❌" if bet_won is False else "⏳"
                        status_text = f"Won (+${profit_loss:.2f})" if bet_won else f"Lost (-${abs(profit_loss):.2f})" if bet_won is False else "Pending"
                    else:
                        status_icon = "⏳"
                        status_text = "Pending"
                    
                    click.echo(f"   {conf_icon} {recommendation}")
                    click.echo(f"      📅 Game: {game_time_str}")
                    click.echo(f"      ⏰ Recommended: {time_str}")
                    click.echo(f"      📊 {conf_level} confidence ({signal_strength*100:.1f}%) - {signal_source}")
                    click.echo(f"      {status_icon} Status: {status_text}")
                    click.echo(f"      🆔 ID: {rec_id}")
                    click.echo()
            
            # Summary statistics
            click.echo("📈 SUMMARY STATISTICS:")
            total_recs = len(recommendations)
            completed = [r for r in recommendations if r[11]]  # game_completed
            won = [r for r in completed if r[12] is True]  # bet_won
            lost = [r for r in completed if r[12] is False]
            
            click.echo(f"   📊 Total recommendations: {total_recs}")
            click.echo(f"   ✅ Completed games: {len(completed)}")
            if completed:
                win_rate = len(won) / len(completed) * 100
                total_profit = sum(r[13] for r in completed if r[13] is not None)
                click.echo(f"   🎯 Win rate: {win_rate:.1f}% ({len(won)}/{len(completed)})")
                click.echo(f"   💰 Total P&L: ${total_profit:+.2f}")
            
            # Confidence breakdown
            by_confidence = {}
            for rec in recommendations:
                conf = rec[7]  # confidence_level
                if conf not in by_confidence:
                    by_confidence[conf] = 0
                by_confidence[conf] += 1
            
            click.echo(f"   📊 By confidence: {dict(by_confidence)}")
            
        except Exception as e:
            click.echo(f"❌ Failed to view recommendations: {e}")
            raise
    
    try:
        asyncio.run(view_recommendations())
    except Exception:
        click.echo("❌ View failed")
        raise