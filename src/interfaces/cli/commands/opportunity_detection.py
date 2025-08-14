"""
CLI Commands for AI-Powered Opportunity Detection

Command-line interface for the opportunity detection system:
- Discover opportunities across games
- Get detailed opportunity analysis
- Test pattern recognition
- Generate explanations
- Performance monitoring

Part of Issue #59: AI-Powered Betting Opportunity Discovery
"""

import asyncio
import json
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import click
from tabulate import tabulate

from src.ml.opportunity_detection.opportunity_detection_service import (
    OpportunityDetectionService, create_opportunity_detection_service
)
from src.ml.opportunity_detection.explanation_engine import UserProfile, ExplanationStyle, RiskProfile
from src.ml.opportunity_detection.opportunity_scoring_engine import OpportunityTier
from src.analysis.models.unified_models import UnifiedBettingSignal, SignalType
from src.core.config import get_unified_config
from src.core.logging import get_logger

logger = get_logger(__name__)


@click.group()
def opportunity_detection():
    """AI-Powered Opportunity Detection Commands"""
    pass


@opportunity_detection.command()
@click.option('--limit', default=10, help='Maximum number of opportunities to discover')
@click.option('--tier', type=click.Choice(['premium', 'high_value', 'standard', 'low_grade']), 
              help='Filter by opportunity tier')
@click.option('--experience', type=click.Choice(['beginner', 'intermediate', 'advanced', 'professional']),
              default='intermediate', help='User experience level for explanations')
@click.option('--risk-profile', type=click.Choice(['conservative', 'moderate', 'aggressive']),
              default='moderate', help='Risk tolerance profile')
@click.option('--min-score', default=35.0, help='Minimum composite score threshold')
@click.option('--output-format', type=click.Choice(['table', 'json', 'detailed']),
              default='table', help='Output format')
@click.option('--include-explanations/--no-explanations', default=False,
              help='Include natural language explanations')
async def discover(limit: int, tier: Optional[str], experience: str, risk_profile: str, 
                  min_score: float, output_format: str, include_explanations: bool):
    """Discover betting opportunities using AI analysis"""
    try:
        click.echo("🔍 Discovering AI-powered betting opportunities...")
        
        # Create service
        service = await create_opportunity_detection_service()
        
        # Create user profile
        user_profile = UserProfile(
            experience_level=ExplanationStyle(experience),
            risk_tolerance=RiskProfile(risk_profile)
        )
        
        # Get sample signals (in production, this would come from the strategy orchestrator)
        signals_by_game = await _get_sample_signals_for_testing()
        
        if not signals_by_game:
            click.echo("❌ No betting signals found. Run data collection first.")
            return
        
        # Discover opportunities
        if tier:
            opportunities = await service.discover_top_opportunities(
                signals_by_game=signals_by_game,
                limit=limit,
                tier_filter=OpportunityTier(tier),
                user_profile=user_profile
            )
        else:
            all_opportunities = await service.discover_opportunities(
                signals_by_game=signals_by_game,
                user_profile=user_profile
            )
            
            # Filter by min score and limit
            opportunities = [
                opp for opp in all_opportunities 
                if opp.opportunity_score.composite_score >= min_score
            ][:limit]
        
        if not opportunities:
            click.echo("❌ No opportunities found matching criteria.")
            return
        
        # Display results
        click.echo(f"✅ Found {len(opportunities)} opportunities")
        
        if output_format == 'json':
            _output_opportunities_json(opportunities, include_explanations)
        elif output_format == 'detailed':
            _output_opportunities_detailed(opportunities, include_explanations)
        else:
            _output_opportunities_table(opportunities)
            
        # Performance metrics
        metrics = service.get_performance_metrics()
        click.echo(f"\n📊 Performance: {metrics['average_discovery_time_ms']:.1f}ms avg, "
                  f"{metrics['cache_hit_rate']:.1%} cache hit rate")
        
        await service.cleanup()
        
    except Exception as e:
        logger.error(f"Error discovering opportunities: {e}")
        click.echo(f"❌ Error: {e}")
        sys.exit(1)


@opportunity_detection.command()
@click.argument('game_id')
@click.option('--experience', type=click.Choice(['beginner', 'intermediate', 'advanced', 'professional']),
              default='intermediate', help='User experience level for explanations')
@click.option('--risk-profile', type=click.Choice(['conservative', 'moderate', 'aggressive']),
              default='moderate', help='Risk tolerance profile')
@click.option('--include-patterns/--no-patterns', default=True,
              help='Include ML pattern recognition')
@click.option('--include-explanations/--no-explanations', default=True,
              help='Include natural language explanations')
@click.option('--output-format', type=click.Choice(['detailed', 'json']),
              default='detailed', help='Output format')
async def analyze(game_id: str, experience: str, risk_profile: str, 
                 include_patterns: bool, include_explanations: bool, output_format: str):
    """Get detailed AI analysis for a specific game opportunity"""
    try:
        click.echo(f"🔍 Analyzing opportunity for game {game_id}...")
        
        # Create service
        service = await create_opportunity_detection_service()
        
        # Create user profile
        user_profile = UserProfile(
            experience_level=ExplanationStyle(experience),
            risk_tolerance=RiskProfile(risk_profile)
        )
        
        # Get signals for the game (in production, this would query the database)
        signals = await _get_sample_signals_for_game(game_id)
        
        if not signals:
            click.echo(f"❌ No betting signals found for game {game_id}")
            return
        
        # Get detailed analysis
        result = await service.get_opportunity_details(
            game_id=game_id,
            signals=signals,
            user_profile=user_profile,
            include_patterns=include_patterns,
            include_explanation=include_explanations
        )
        
        if not result:
            click.echo(f"❌ Could not generate analysis for game {game_id}")
            return
        
        # Display results
        click.echo(f"✅ Generated detailed analysis for game {game_id}")
        
        if output_format == 'json':
            _output_single_opportunity_json(result)
        else:
            _output_single_opportunity_detailed(result)
        
        await service.cleanup()
        
    except Exception as e:
        logger.error(f"Error analyzing opportunity: {e}")
        click.echo(f"❌ Error: {e}")
        sys.exit(1)


@opportunity_detection.command()
@click.option('--game-count', default=5, help='Number of sample games to test')
@click.option('--pattern-types', default='all', help='Pattern types to test (comma-separated)')
async def test_patterns(game_count: int, pattern_types: str):
    """Test ML pattern recognition capabilities"""
    try:
        click.echo("🧠 Testing ML pattern recognition...")
        
        from src.ml.opportunity_detection.pattern_recognition import MLPatternRecognition, PatternType
        
        # Create pattern recognition engine
        pattern_engine = MLPatternRecognition()
        
        # Get sample signals
        signals_by_game = await _get_sample_signals_for_testing()
        sample_games = list(signals_by_game.keys())[:game_count]
        
        all_patterns = []
        
        for game_id in sample_games:
            signals = signals_by_game[game_id]
            
            # Detect patterns
            patterns = await pattern_engine.detect_patterns(
                signals=signals,
                game_id=game_id,
                market_data={}
            )
            
            all_patterns.extend(patterns)
            
            click.echo(f"Game {game_id}: {len(patterns)} patterns detected")
            for pattern in patterns:
                click.echo(f"  • {pattern.pattern_type.value} (confidence: {pattern.confidence_score:.2f})")
        
        # Summary
        if all_patterns:
            pattern_counts = {}
            for pattern in all_patterns:
                pattern_type = pattern.pattern_type.value
                pattern_counts[pattern_type] = pattern_counts.get(pattern_type, 0) + 1
            
            click.echo(f"\n📊 Pattern Summary ({len(all_patterns)} total):")
            for pattern_type, count in sorted(pattern_counts.items()):
                click.echo(f"  {pattern_type}: {count}")
        else:
            click.echo("❌ No patterns detected in sample data")
        
    except Exception as e:
        logger.error(f"Error testing patterns: {e}")
        click.echo(f"❌ Error: {e}")
        sys.exit(1)


@opportunity_detection.command()
@click.argument('text', required=False)
@click.option('--experience', type=click.Choice(['beginner', 'intermediate', 'advanced', 'professional']),
              default='intermediate', help='User experience level')
@click.option('--format', 'explanation_format', type=click.Choice(['paragraph', 'bullet_points', 'narrative']),
              default='paragraph', help='Explanation format')
async def test_explanations(text: Optional[str], experience: str, explanation_format: str):
    """Test natural language explanation generation"""
    try:
        click.echo("📝 Testing natural language explanations...")
        
        from src.ml.opportunity_detection.explanation_engine import (
            NaturalLanguageExplanationEngine, ExplanationFormat
        )
        from src.ml.opportunity_detection.opportunity_scoring_engine import OpportunityScore, OpportunityTier
        
        # Create explanation engine
        explanation_engine = NaturalLanguageExplanationEngine()
        
        # Create sample opportunity (mock data for testing)
        sample_opportunity = await _create_sample_opportunity_score()
        
        # Create user profile
        user_profile = UserProfile(
            experience_level=ExplanationStyle(experience),
            preferred_format=ExplanationFormat(explanation_format)
        )
        
        # Generate explanation
        explanation = await explanation_engine.generate_opportunity_explanation(
            opportunity=sample_opportunity,
            user_profile=user_profile
        )
        
        if explanation:
            click.echo("✅ Generated explanation:")
            click.echo(f"\nHeadline: {explanation['components'].headline}")
            click.echo(f"\nSummary: {explanation['components'].summary}")
            
            if explanation['formatted_text']:
                click.echo(f"\nFormatted Explanation:\n{explanation['formatted_text']}")
            
            if explanation['components'].key_factors:
                click.echo(f"\nKey Factors:")
                for factor in explanation['components'].key_factors:
                    click.echo(f"  • {factor}")
            
            click.echo(f"\nMetadata: {explanation['metadata']}")
        else:
            click.echo("❌ Failed to generate explanation")
        
    except Exception as e:
        logger.error(f"Error testing explanations: {e}")
        click.echo(f"❌ Error: {e}")
        sys.exit(1)


@opportunity_detection.command()
async def performance():
    """Show opportunity detection service performance metrics"""
    try:
        click.echo("📊 Opportunity Detection Performance Metrics")
        
        # Create service
        service = await create_opportunity_detection_service()
        
        # Get metrics
        metrics = service.get_performance_metrics()
        cache_status = service.get_cache_status()
        
        # Display metrics
        click.echo("\n🎯 Discovery Metrics:")
        click.echo(f"  Opportunities Discovered: {metrics['opportunities_discovered']}")
        click.echo(f"  Patterns Detected: {metrics['patterns_detected']}")
        click.echo(f"  Explanations Generated: {metrics['explanations_generated']}")
        click.echo(f"  Average Discovery Time: {metrics['average_discovery_time_ms']:.1f}ms")
        
        click.echo("\n💾 Cache Metrics:")
        click.echo(f"  Cache Hit Rate: {metrics['cache_hit_rate']:.1%}")
        click.echo(f"  Cached Opportunities: {cache_status['total_cached']}")
        click.echo(f"  Expired Entries: {cache_status['expired_entries']}")
        click.echo(f"  Cache TTL: {cache_status['ttl_hours']} hours")
        
        click.echo("\n🔧 Component Status:")
        for component, status in metrics['component_status'].items():
            status_emoji = "✅" if status == "active" else "⚠️"
            click.echo(f"  {component}: {status_emoji} {status}")
        
        await service.cleanup()
        
    except Exception as e:
        logger.error(f"Error getting performance metrics: {e}")
        click.echo(f"❌ Error: {e}")
        sys.exit(1)


@opportunity_detection.command()
@click.option('--expired-only/--clear-all', default=True,
              help='Clear only expired entries or all cache')
async def clear_cache(expired_only: bool):
    """Clear opportunity detection cache"""
    try:
        click.echo("🧹 Clearing opportunity detection cache...")
        
        # Create service
        service = await create_opportunity_detection_service()
        
        # Clear cache
        cleared_count = await service.clear_cache(expired_only=expired_only)
        
        cache_type = "expired" if expired_only else "all"
        click.echo(f"✅ Cleared {cleared_count} {cache_type} cache entries")
        
        await service.cleanup()
        
    except Exception as e:
        logger.error(f"Error clearing cache: {e}")
        click.echo(f"❌ Error: {e}")
        sys.exit(1)


# Helper functions for output formatting

def _output_opportunities_table(opportunities):
    """Output opportunities in table format"""
    headers = ['Game ID', 'Score', 'Tier', 'Confidence', 'EV', 'Kelly', 'Patterns']
    rows = []
    
    for opp in opportunities:
        score = opp.opportunity_score
        rows.append([
            opp.game_id[:8],  # Truncate for display
            f"{score.composite_score:.1f}",
            score.tier.value,
            score.confidence_level.value,
            f"{score.expected_value:+.2%}",
            f"{score.kelly_fraction:.2%}",
            len(opp.detected_patterns)
        ])
    
    click.echo("\n" + tabulate(rows, headers=headers, tablefmt='grid'))


def _output_opportunities_json(opportunities, include_explanations: bool):
    """Output opportunities in JSON format"""
    data = []
    
    for opp in opportunities:
        opp_data = {
            'opportunity_id': opp.opportunity_id,
            'game_id': opp.game_id,
            'score': {
                'composite_score': opp.opportunity_score.composite_score,
                'tier': opp.opportunity_score.tier.value,
                'confidence_level': opp.opportunity_score.confidence_level.value,
                'expected_value': opp.opportunity_score.expected_value,
                'kelly_fraction': opp.opportunity_score.kelly_fraction
            },
            'patterns_detected': len(opp.detected_patterns),
            'discovery_time_ms': opp.discovery_time_ms,
            'cache_hit': opp.cache_hit
        }
        
        if include_explanations and opp.explanation:
            opp_data['explanation'] = {
                'headline': opp.explanation['components'].headline,
                'summary': opp.explanation['components'].summary,
                'recommendation': opp.explanation['components'].recommendation
            }
        
        data.append(opp_data)
    
    click.echo(json.dumps(data, indent=2, default=str))


def _output_opportunities_detailed(opportunities, include_explanations: bool):
    """Output opportunities in detailed format"""
    for i, opp in enumerate(opportunities, 1):
        score = opp.opportunity_score
        
        click.echo(f"\n{'='*60}")
        click.echo(f"Opportunity #{i}: {opp.game_id}")
        click.echo(f"{'='*60}")
        
        click.echo(f"📊 Score: {score.composite_score:.1f}/100 ({score.tier.value} tier)")
        click.echo(f"🎯 Confidence: {score.confidence_level.value}")
        click.echo(f"💰 Expected Value: {score.expected_value:+.2%}")
        click.echo(f"📈 Kelly Fraction: {score.kelly_fraction:.2%}")
        click.echo(f"⏱️  Discovery Time: {opp.discovery_time_ms:.1f}ms")
        
        if opp.detected_patterns:
            click.echo(f"\n🧠 Detected Patterns ({len(opp.detected_patterns)}):")
            for pattern in opp.detected_patterns[:3]:  # Show top 3
                click.echo(f"  • {pattern.pattern_type.value} (confidence: {pattern.confidence_score:.2f})")
        
        if include_explanations and opp.explanation:
            click.echo(f"\n📝 Explanation:")
            click.echo(f"  {opp.explanation['components'].headline}")
            click.echo(f"  {opp.explanation['components'].summary}")
            if opp.explanation['components'].recommendation:
                click.echo(f"  💡 {opp.explanation['components'].recommendation}")


def _output_single_opportunity_json(result):
    """Output single opportunity in JSON format"""
    data = {
        'opportunity_id': result.opportunity_id,
        'game_id': result.game_id,
        'score': {
            'composite_score': result.opportunity_score.composite_score,
            'tier': result.opportunity_score.tier.value,
            'confidence_level': result.opportunity_score.confidence_level.value,
            'expected_value': result.opportunity_score.expected_value,
            'kelly_fraction': result.opportunity_score.kelly_fraction,
            'scoring_factors': {
                'strategy_performance': result.opportunity_score.scoring_factors.strategy_performance,
                'ml_confidence': result.opportunity_score.scoring_factors.ml_confidence,
                'market_efficiency': result.opportunity_score.scoring_factors.market_efficiency,
                'data_quality': result.opportunity_score.scoring_factors.data_quality,
                'consensus_strength': result.opportunity_score.scoring_factors.consensus_strength,
                'timing_factor': result.opportunity_score.scoring_factors.timing_factor,
                'value_potential': result.opportunity_score.scoring_factors.value_potential
            }
        },
        'patterns': [
            {
                'type': p.pattern_type.value,
                'confidence': p.confidence_score,
                'description': p.description
            } for p in result.detected_patterns
        ],
        'explanation': result.explanation,
        'contributing_signals': len(result.contributing_signals),
        'discovery_time_ms': result.discovery_time_ms
    }
    
    click.echo(json.dumps(data, indent=2, default=str))


def _output_single_opportunity_detailed(result):
    """Output single opportunity in detailed format"""
    score = result.opportunity_score
    
    click.echo(f"\n{'='*80}")
    click.echo(f"AI Opportunity Analysis: {result.game_id}")
    click.echo(f"{'='*80}")
    
    # Core scoring
    click.echo(f"\n📊 Composite Score: {score.composite_score:.1f}/100")
    click.echo(f"🏆 Opportunity Tier: {score.tier.value}")
    click.echo(f"🎯 Confidence Level: {score.confidence_level.value}")
    click.echo(f"💰 Expected Value: {score.expected_value:+.3f}")
    click.echo(f"📈 Kelly Fraction: {score.kelly_fraction:.3f}")
    click.echo(f"⚖️  Risk Profile: {score.risk_profile.value}")
    
    # Scoring factors breakdown
    factors = score.scoring_factors
    click.echo(f"\n🔍 Scoring Factors Breakdown:")
    click.echo(f"  Strategy Performance: {factors.strategy_performance:.1f}/100")
    click.echo(f"  ML Confidence: {factors.ml_confidence:.1f}/100")
    click.echo(f"  Market Efficiency: {factors.market_efficiency:.1f}/100")
    click.echo(f"  Data Quality: {factors.data_quality:.1f}/100")
    click.echo(f"  Consensus Strength: {factors.consensus_strength:.1f}/100")
    click.echo(f"  Timing Factor: {factors.timing_factor:.1f}/100")
    click.echo(f"  Value Potential: {factors.value_potential:.1f}/100")
    
    # Patterns
    if result.detected_patterns:
        click.echo(f"\n🧠 ML-Detected Patterns ({len(result.detected_patterns)}):")
        for pattern in result.detected_patterns:
            click.echo(f"  • {pattern.pattern_type.value}")
            click.echo(f"    Confidence: {pattern.confidence_score:.2f}")
            if pattern.description:
                click.echo(f"    Description: {pattern.description}")
            if pattern.key_indicators:
                click.echo(f"    Indicators: {', '.join(pattern.key_indicators[:3])}")
            click.echo()
    
    # Explanation
    if result.explanation and 'components' in result.explanation:
        components = result.explanation['components']
        
        click.echo(f"📝 AI-Generated Explanation:")
        click.echo(f"  Headline: {components.headline}")
        click.echo(f"  Summary: {components.summary}")
        
        if components.key_factors:
            click.echo(f"  Key Factors:")
            for factor in components.key_factors:
                click.echo(f"    • {factor}")
        
        if components.recommendation:
            click.echo(f"  💡 Recommendation: {components.recommendation}")
        
        if components.risk_assessment:
            click.echo(f"  ⚠️  Risk Assessment: {components.risk_assessment}")
        
        if components.confidence_explanation:
            click.echo(f"  🎯 Confidence: {components.confidence_explanation}")
    
    # Contributing signals
    click.echo(f"\n📈 Contributing Signals: {len(result.contributing_signals)}")
    signal_types = {}
    for signal in result.contributing_signals:
        signal_type = signal.signal_type.value
        signal_types[signal_type] = signal_types.get(signal_type, 0) + 1
    
    for signal_type, count in sorted(signal_types.items()):
        click.echo(f"  {signal_type}: {count}")
    
    # Performance
    click.echo(f"\n⏱️  Performance:")
    click.echo(f"  Discovery Time: {result.discovery_time_ms:.1f}ms")
    click.echo(f"  Cache Hit: {'Yes' if result.cache_hit else 'No'}")


# Sample data functions for testing (replace with real data queries in production)

async def _get_sample_signals_for_testing() -> Dict[str, List[UnifiedBettingSignal]]:
    """Get sample signals for testing (replace with real data in production)"""
    try:
        # This would typically query the database for recent signals
        # For now, return sample data
        
        sample_signals = {}
        
        for i in range(5):  # Create 5 sample games
            game_id = f"game_{i+1}"
            signals = []
            
            # Create sample signals
            for j in range(3):  # 3 signals per game
                signal = UnifiedBettingSignal(
                    signal_id=f"signal_{game_id}_{j}",
                    signal_type=SignalType.SHARP_ACTION if j == 0 else SignalType.LINE_MOVEMENT,
                    strategy_category="sharp_action",
                    game_id=game_id,
                    home_team=f"Team_H_{i}",
                    away_team=f"Team_A_{i}",
                    game_date=datetime.now() + timedelta(hours=2+i),
                    recommended_side="home" if j % 2 == 0 else "away",
                    bet_type="moneyline",
                    confidence_score=0.6 + (j * 0.1),
                    confidence_level="medium",
                    odds=-110.0,
                    signal_strength=0.7 + (j * 0.1),
                    minutes_to_game=120 + (i * 30),
                    timing_category="LATE_AFTERNOON",
                    data_source="test_source",
                    quality_score=0.8 + (j * 0.05),
                    strategy_data={
                        'money_percentage': 55.0 + j,
                        'bet_percentage': 45.0 + j,
                        'line_movement_velocity': 0.5,
                        'consensus_strength': 0.7
                    }
                )
                signals.append(signal)
            
            sample_signals[game_id] = signals
        
        return sample_signals
        
    except Exception as e:
        logger.error(f"Error creating sample signals: {e}")
        return {}


async def _get_sample_signals_for_game(game_id: str) -> List[UnifiedBettingSignal]:
    """Get sample signals for a specific game"""
    all_signals = await _get_sample_signals_for_testing()
    return all_signals.get(game_id, [])


async def _create_sample_opportunity_score():
    """Create sample opportunity score for testing"""
    from src.ml.opportunity_detection.opportunity_scoring_engine import (
        OpportunityScore, ScoringFactors, OpportunityTier
    )
    from src.analysis.models.unified_models import ConfidenceLevel, SignalType
    
    factors = ScoringFactors(
        strategy_performance=75.0,
        ml_confidence=68.0,
        market_efficiency=55.0,
        data_quality=82.0,
        consensus_strength=71.0,
        timing_factor=65.0,
        value_potential=58.0
    )
    
    return OpportunityScore(
        opportunity_id=f"sample_opp_{int(datetime.now().timestamp())}",
        game_id="sample_game",
        signal_type=SignalType.SHARP_ACTION,
        composite_score=67.5,
        tier=OpportunityTier.HIGH_VALUE,
        confidence_level=ConfidenceLevel.HIGH,
        expected_value=0.035,
        kelly_fraction=0.08,
        risk_profile=RiskProfile.MODERATE,
        scoring_factors=factors,
        factor_weights={
            'strategy_performance': 0.25,
            'ml_confidence': 0.20,
            'market_efficiency': 0.15,
            'data_quality': 0.10,
            'consensus_strength': 0.15,
            'timing_factor': 0.08,
            'value_potential': 0.07
        },
        explanation_summary="High-value opportunity with strong strategy performance and good ML confidence."
    )


# Make async commands work with click
def async_command(f):
    """Decorator to make click commands work with async functions"""
    def wrapper(*args, **kwargs):
        return asyncio.run(f(*args, **kwargs))
    return wrapper

# Apply async decorator to all commands
for command in [discover, analyze, test_patterns, test_explanations, performance, clear_cache]:
    command.callback = async_command(command.callback)