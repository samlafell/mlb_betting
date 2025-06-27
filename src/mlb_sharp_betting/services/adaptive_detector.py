"""
Adaptive Betting Detector - Orchestrator-Powered

This replaces the hardcoded MasterBettingDetector with a dynamic system that:
1. Uses the StrategyOrchestrator to get performance-based configurations
2. Executes the same processors used in backtesting
3. Creates the missing feedback loop between Phase 3B and Phase 3A
4. Eliminates the architectural disconnect

This is the NEW Phase 3A that's informed by Phase 3B results.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import pytz

from ..core.logging import get_logger
from ..services.strategy_orchestrator import get_strategy_orchestrator
from ..services.juice_filter_service import get_juice_filter_service
from ..models.betting_analysis import BettingSignal, GameAnalysis, BettingAnalysisResult


class AdaptiveBettingDetector:
    """
    Adaptive betting detector powered by the strategy orchestrator.
    
    This detector bridges the Phase 3A/3B disconnect by:
    - Using dynamic strategy configurations based on backtesting performance
    - Executing the same processors used in backtesting (not hardcoded logic)
    - Providing performance-based recommendations
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.orchestrator = None
        self.juice_filter = get_juice_filter_service()
        self.est = pytz.timezone('US/Eastern')
        
    async def initialize(self):
        """Initialize the detector with orchestrator"""
        try:
            self.orchestrator = await get_strategy_orchestrator()
            self.logger.info("Adaptive betting detector initialized with orchestrator")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize adaptive detector: {e}")
            raise
    
    async def analyze_opportunities(self, minutes_ahead: int = 60, debug_mode: bool = False) -> BettingAnalysisResult:
        """
        Analyze betting opportunities using orchestrator-powered strategy execution.
        
        This method replaces the hardcoded analysis in MasterBettingDetector.
        """
        try:
            if self.orchestrator is None:
                await self.initialize()
            
            # Get dynamic strategy configuration from orchestrator
            strategy_state = await self.orchestrator.get_live_strategy_configuration()
            
            if debug_mode:
                await self._display_strategy_configuration(strategy_state)
            
            # Execute live strategy detection using orchestrator
            signals = await self.orchestrator.execute_live_strategy_detection(minutes_ahead)
            
            # Apply juice filtering
            filtered_signals = await self._apply_juice_filtering(signals)
            
            # Group signals by game
            games = self._group_signals_by_game(filtered_signals)
            
            # Generate analysis result
            result = BettingAnalysisResult(
                games=games,
                strategy_performance=self._extract_strategy_performance(strategy_state),
                analysis_metadata={
                    'analysis_type': 'orchestrator_powered',
                    'minutes_ahead': minutes_ahead,
                    'enabled_strategies': len(strategy_state.enabled_strategies),
                    'total_signals': len(signals),
                    'filtered_signals': len(filtered_signals),
                    'configuration_version': strategy_state.configuration_version,
                    'last_config_update': strategy_state.last_updated.isoformat()
                }
            )
            
            if debug_mode:
                await self._display_debug_analysis(result)
            else:
                await self._display_standard_analysis(result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to analyze opportunities: {e}")
            # Return empty result on failure
            return BettingAnalysisResult(
                games={},
                strategy_performance=[],
                analysis_metadata={'error': str(e)}
            )
    
    async def _display_strategy_configuration(self, strategy_state):
        """Display current strategy configuration in debug mode"""
        print("\n🎯 ORCHESTRATOR STRATEGY CONFIGURATION")
        print("=" * 60)
        
        print(f"📊 Configuration Version: {strategy_state.configuration_version}")
        print(f"🕐 Last Updated: {strategy_state.last_updated.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        
        print(f"\n✅ ENABLED STRATEGIES ({len(strategy_state.enabled_strategies)}):")
        for i, config in enumerate(strategy_state.enabled_strategies, 1):
            roi_color = "🔥" if config.recent_roi > 15 else "⭐" if config.recent_roi > 10 else "✅"
            trend_icon = "📈" if config.performance_trend == "IMPROVING" else "📉" if config.performance_trend == "DECLINING" else "➡️"
            
            print(f"  {i}. {roi_color} {config.strategy_name}")
            print(f"     📊 ROI: {config.recent_roi:+.1f}% | WR: {config.recent_win_rate:.1f}% | Sample: {config.sample_size}")
            print(f"     ⚙️  Confidence: {config.confidence_multiplier:.2f}x | Weight: {config.weight_in_ensemble:.2f}")
            print(f"     🎚️  Threshold: {config.min_differential_threshold:.1f}% (+{config.threshold_adjustment:+.1f})")
            print(f"     {trend_icon} Trend: {config.performance_trend}")
            print()
        
        if strategy_state.disabled_strategies:
            print(f"\n❌ DISABLED STRATEGIES ({len(strategy_state.disabled_strategies)}):")
            for config in strategy_state.disabled_strategies:
                print(f"  • {config.strategy_name}: ROI {config.recent_roi:+.1f}% | WR {config.recent_win_rate:.1f}%")
        
        perf = strategy_state.performance_summary
        print(f"\n📈 PERFORMANCE SUMMARY:")
        print(f"  • Average ROI (Enabled): {perf.get('avg_roi_enabled', 0):.1f}%")
        print(f"  • Average Win Rate (Enabled): {perf.get('avg_win_rate_enabled', 0):.1f}%")
        print(f"  • Total Sample Size: {perf.get('total_sample_size', 0)}")
        
        trends = perf.get('performance_trends', {})
        print(f"  • Trends: {trends.get('IMPROVING', 0)} improving, {trends.get('STABLE', 0)} stable, {trends.get('DECLINING', 0)} declining")
    
    async def _apply_juice_filtering(self, signals: List[BettingSignal]) -> List[BettingSignal]:
        """Apply juice filtering to signals"""
        filtered_signals = []
        
        for signal in signals:
            # Apply juice filter for moneyline bets
            if signal.split_type == 'moneyline':
                # Determine recommended team based on recommendation
                if 'BET' in signal.recommendation:
                    recommended_team = signal.recommendation.replace('BET ', '').strip()
                    
                    # Check if we should filter this bet due to heavy juice
                    if self.juice_filter.should_filter_bet(
                        signal.split_value, 
                        recommended_team, 
                        signal.home_team, 
                        signal.away_team, 
                        'orchestrator_signals'
                    ):
                        self.logger.debug(f"Filtered heavy juice bet: {signal.recommendation} at {signal.split_value}")
                        continue
            
            filtered_signals.append(signal)
        
        return filtered_signals
    
    def _group_signals_by_game(self, signals: List[BettingSignal]) -> Dict[tuple, GameAnalysis]:
        """Group signals by game into GameAnalysis objects"""
        games = {}
        
        for signal in signals:
            game_key = (signal.away_team, signal.home_team, signal.game_time)
            
            if game_key not in games:
                games[game_key] = GameAnalysis(
                    home_team=signal.home_team,
                    away_team=signal.away_team,
                    game_time=signal.game_time,
                    minutes_to_game=signal.minutes_to_game
                )
            
            # Add signal to appropriate category based on signal type
            game_analysis = games[game_key]
            
            if signal.signal_type.value == 'sharp_action':
                game_analysis.sharp_signals.append(signal)
            elif signal.signal_type.value == 'opposing_markets':
                game_analysis.opposing_markets.append(signal)
            elif signal.signal_type.value == 'book_conflicts':
                game_analysis.book_conflicts.append(signal)
            elif signal.signal_type.value == 'steam_moves':
                game_analysis.steam_moves.append(signal)
            else:
                # Default to sharp signals for unknown types
                game_analysis.sharp_signals.append(signal)
        
        return games
    
    async def get_strategy_performance_summary(self) -> Dict[str, Any]:
        """Get strategy performance summary for display"""
        try:
            if self.orchestrator is None:
                await self.initialize()
            
            strategy_state = await self.orchestrator.get_live_strategy_configuration()
            
            # Build strategy details (enabled and disabled combined, sorted by ROI descending)
            all_configs = strategy_state.enabled_strategies + strategy_state.disabled_strategies
            all_configs.sort(key=lambda c: c.recent_roi, reverse=True)
            
            strategy_details = []
            for config in all_configs:
                strategy_details.append({
                    'name': config.strategy_name,
                    'enabled': config.is_enabled,
                    'roi': config.recent_roi,
                    'win_rate': config.recent_win_rate,
                    'sample_size': config.sample_size,
                    'trend': config.performance_trend,
                    'confidence_multiplier': config.confidence_multiplier,
                    'ensemble_weight': config.weight_in_ensemble,
                    'status': config.strategy_status.value
                })
            
            return {
                'total_strategies': len(strategy_details),
                'enabled_strategies': len(strategy_state.enabled_strategies),
                'disabled_strategies': len(strategy_state.disabled_strategies),
                'last_updated': strategy_state.last_updated.isoformat(),
                'configuration_version': strategy_state.configuration_version,
                'performance_summary': strategy_state.performance_summary,
                'strategy_details': strategy_details
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get strategy performance summary: {e}")
            return {
                'total_strategies': 0,
                'enabled_strategies': 0,
                'disabled_strategies': 0,
                'last_updated': 'unknown',
                'configuration_version': 'unknown',
                'performance_summary': {},
                'strategy_details': []
            }
    
    def _extract_strategy_performance(self, strategy_state) -> List[Dict[str, Any]]:
        """Extract strategy performance information"""
        performance_list = []
        
        for config in strategy_state.enabled_strategies + strategy_state.disabled_strategies:
            performance_list.append({
                'strategy_name': config.strategy_name,
                'source_book': 'orchestrator',
                'split_type': config.signal_type.value,
                'win_rate': config.recent_win_rate,
                'roi': config.recent_roi,
                'total_bets': config.sample_size,
                'confidence_level': config.strategy_status.value,
                'ci_lower': max(0, config.recent_win_rate - 10),  # Approximate
                'ci_upper': min(100, config.recent_win_rate + 10)
            })
        
        return performance_list
    
    async def _display_debug_analysis(self, result: BettingAnalysisResult):
        """Display detailed debug analysis"""
        print("\n🔍 DEBUG ANALYSIS RESULTS")
        print("=" * 60)
        
        metadata = result.analysis_metadata
        print(f"📊 Analysis Type: {metadata.get('analysis_type', 'unknown')}")
        print(f"🕐 Generated At: {result.generated_at.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⚙️  Configuration: {metadata.get('configuration_version', 'unknown')}")
        print(f"🎯 Time Window: {metadata.get('minutes_ahead', 0)} minutes ahead")
        print(f"📈 Strategies Used: {metadata.get('enabled_strategies', 0)}")
        print(f"🔄 Raw Signals: {metadata.get('total_signals', 0)}")
        print(f"✅ Filtered Signals: {metadata.get('filtered_signals', 0)}")
        
        if result.total_opportunities == 0:
            print("\n❌ NO BETTING OPPORTUNITIES FOUND")
            print("This could mean:")
            print("  • No games in the specified time window")
            print("  • All signals filtered out due to juice limits")
            print("  • Strategy thresholds are too conservative")
            print("  • Insufficient data for analysis")
            return
        
        print(f"\n🎯 TOTAL OPPORTUNITIES FOUND: {result.total_opportunities}")
        
        # Display opportunities by type
        opportunities_by_type = result.opportunities_by_type
        for signal_type, count in opportunities_by_type.items():
            if count > 0:
                print(f"  • {signal_type.value}: {count} opportunities")
        
        # Display game-by-game analysis
        print("\n🏟️  GAME-BY-GAME ANALYSIS:")
        for (away, home, game_time), game_analysis in result.games.items():
            print(f"\n📅 {away} @ {home}")
            print(f"⏰ Game Time: {game_time.strftime('%Y-%m-%d %H:%M')}")
            print(f"🕐 Minutes to Game: {game_analysis.minutes_to_game}")
            print(f"🎯 Total Opportunities: {game_analysis.total_opportunities}")
            
            # Show each signal
            for signal in game_analysis.all_signals:
                confidence_emoji = self._get_confidence_emoji(signal.confidence_score)
                print(f"  {confidence_emoji} {signal.signal_type.value.upper()}: {signal.recommendation}")
                print(f"    💪 Strength: {signal.signal_strength:.1f} | Confidence: {signal.confidence_score:.2f}")
                print(f"    📊 Strategy: {signal.strategy_name} | ROI: {signal.roi:+.1f}%")
                if hasattr(signal, 'metadata') and signal.metadata:
                    orchestrator_config = signal.metadata.get('orchestrator_config', {})
                    if orchestrator_config:
                        print(f"    ⚙️  Weight: {orchestrator_config.get('strategy_weight', 0):.2f} | Trend: {orchestrator_config.get('performance_trend', 'UNKNOWN')}")
    
    async def _display_standard_analysis(self, result: BettingAnalysisResult):
        """Display standard analysis results"""
        if result.total_opportunities == 0:
            print("\n❌ NO BETTING OPPORTUNITIES FOUND")
            print("🔧 Run with --debug to see detailed analysis")
            return
        
        print(f"\n🎯 {result.total_opportunities} BETTING OPPORTUNITIES FOUND")
        print("=" * 60)
        
        # Show configuration summary
        metadata = result.analysis_metadata
        print(f"⚙️  Using {metadata.get('enabled_strategies', 0)} orchestrator-configured strategies")
        print(f"📊 Configuration: {metadata.get('configuration_version', 'unknown')}")
        
        # Display games with opportunities
        for (away, home, game_time), game_analysis in result.games.items():
            print(f"\n🏟️  {away} @ {home}")
            print(f"⏰ Starts in {game_analysis.minutes_to_game} minutes ({game_time.strftime('%H:%M')})")
            print("-" * 50)
            
            # Show highest confidence signal
            highest_signal = game_analysis.highest_confidence_signal
            if highest_signal:
                confidence_emoji = self._get_confidence_emoji(highest_signal.confidence_score)
                print(f"{confidence_emoji} BEST OPPORTUNITY: {highest_signal.recommendation}")
                print(f"  📊 Strategy: {highest_signal.strategy_name} (Recent ROI: {highest_signal.roi:+.1f}%)")
                print(f"  💪 Signal Strength: {highest_signal.signal_strength:.1f}")
                print(f"  🎯 Confidence: {highest_signal.confidence_score:.2f}")
                
                # Show orchestrator metadata if available
                if hasattr(highest_signal, 'metadata') and highest_signal.metadata:
                    orchestrator_config = highest_signal.metadata.get('orchestrator_config', {})
                    if orchestrator_config:
                        trend = orchestrator_config.get('performance_trend', 'UNKNOWN')
                        trend_icon = "📈" if trend == "IMPROVING" else "📉" if trend == "DECLINING" else "➡️"
                        print(f"  {trend_icon} Performance Trend: {trend}")
            
            # Show additional opportunities if multiple
            other_signals = [s for s in game_analysis.all_signals if s != highest_signal]
            if other_signals:
                print(f"  📋 Additional opportunities: {len(other_signals)}")
    
    def _get_confidence_emoji(self, confidence_score: float) -> str:
        """Get emoji for confidence score"""
        if confidence_score >= 0.90:
            return "🔥"
        elif confidence_score >= 0.75:
            return "⭐"
        elif confidence_score >= 0.60:
            return "✅"
        elif confidence_score >= 0.45:
            return "⚠️"
        else:
            return "❌"


# Global instance
_adaptive_detector = None

async def get_adaptive_detector() -> AdaptiveBettingDetector:
    """Get the global adaptive detector instance"""
    global _adaptive_detector
    
    if _adaptive_detector is None:
        _adaptive_detector = AdaptiveBettingDetector()
        await _adaptive_detector.initialize()
    
    return _adaptive_detector 