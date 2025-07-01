"""
Betting Analysis Formatter - Display Logic Separation

Handles all formatting and display logic for betting analysis results,
keeping presentation concerns separate from business logic.
"""

from datetime import datetime
from typing import Dict, List, Any
import pytz

from ..models.betting_analysis import (
    BettingAnalysisResult, GameAnalysis, BettingSignal, 
    ConfidenceLevel, SignalType, StrategyPerformance
)
from ..core.logging import get_logger


class BettingAnalysisFormatter:
    """Formats betting analysis results for display"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.est = pytz.timezone('US/Eastern')
    
    def format_analysis(self, analysis_result: BettingAnalysisResult, 
                       juice_filter_summary: Dict[str, Any] = None) -> str:
        """Format complete analysis result for display"""
        if not analysis_result.games:
            return self._format_no_opportunities(analysis_result, juice_filter_summary)
        
        output = []
        
        # Header
        output.append(self._format_header(analysis_result, juice_filter_summary))
        
        # Strategy status
        output.append(self._format_strategy_status(analysis_result.strategy_performance))
        
        # Game-by-game analysis
        output.append(self._format_game_analysis(analysis_result.games))
        
        # Summary
        output.append(self._format_summary(analysis_result))
        
        return "\n".join(output)
    
    def _format_header(self, analysis_result: BettingAnalysisResult, 
                      juice_filter_summary: Dict[str, Any] = None) -> str:
        """Format analysis header"""
        lines = [
            "🤖 ADAPTIVE MASTER BETTING DETECTOR",
            "=" * 55,
            "🧠 Using AI-optimized thresholds from backtesting results"
        ]
        
        if juice_filter_summary:
            if juice_filter_summary.get('enabled'):
                lines.append(f"🚫 SMART JUICE FILTER: Won't bet favorites worse than {juice_filter_summary['max_juice_threshold']}")
            else:
                lines.append("⚠️  JUICE FILTER: DISABLED")
        
        now_est = datetime.now(self.est)
        metadata = analysis_result.analysis_metadata
        
        lines.extend([
            f"📅 Current time: {now_est.strftime('%H:%M:%S %Z')}",
            f"🎯 Looking until: {metadata.get('end_time', 'Unknown').strftime('%H:%M:%S %Z') if metadata.get('end_time') else 'Unknown'}"
        ])
        
        return "\n".join(lines)
    
    def _format_strategy_status(self, strategy_performance: List[StrategyPerformance]) -> str:
        """Format strategy performance status"""
        lines = ["📊 STRATEGY STATUS (Reading from Backtesting Database):"]
        
        if not strategy_performance:
            lines.extend([
                "   ⚠️  No profitable strategies found in current backtesting results",
                "   🔧 Run backtesting analysis to populate strategy performance data",
                "   💡 Command: uv run src/mlb_sharp_betting/cli.py backtesting run --mode single-run"
            ])
        else:
            total_bets = sum(s.total_bets for s in strategy_performance)
            weighted_win_rate = (sum(s.win_rate * s.total_bets for s in strategy_performance) / total_bets 
                                if total_bets > 0 else 0)
            weighted_roi = (sum(s.roi * s.total_bets for s in strategy_performance) / total_bets 
                           if total_bets > 0 else 0)
            
            lines.extend([
                f"   ✅ {len(strategy_performance)} profitable strategies active",
                f"   📈 Weighted Win Rate: {weighted_win_rate:.1f}%",
                f"   💰 Weighted ROI: {weighted_roi:+.1f}%"
            ])
            
            # Show top 3 strategies
            top_strategies = sorted(strategy_performance, key=lambda x: x.roi, reverse=True)[:3]
            lines.append("   🏆 Top Strategies:")
            for i, strategy in enumerate(top_strategies, 1):
                name_truncated = strategy.strategy_name[:25]
                lines.append(f"      {i}. {name_truncated:<25} | {strategy.win_rate:5.1f}% WR | {strategy.roi:+6.1f}% ROI | {strategy.total_bets:3d} bets")
        
        return "\n".join(lines)
    
    def _format_game_analysis(self, games: Dict[tuple, GameAnalysis]) -> str:
        """Format game-by-game analysis"""
        total_opportunities = sum(game.total_opportunities for game in games.values())
        
        lines = [
            f"\n🎯 {total_opportunities} BETTING OPPORTUNITIES FOUND",
            "=" * 60
        ]
        
        for game_key, game_analysis in sorted(games.items(), key=lambda x: x[1].game_time):
            lines.append(self._format_single_game(game_analysis))
        
        return "\n".join(lines)
    
    def _format_single_game(self, game_analysis: GameAnalysis) -> str:
        """Format analysis for a single game"""
        now_est = datetime.now(self.est)
        
        lines = [
            f"\n🏟️  {game_analysis.away_team} @ {game_analysis.home_team}",
            f"⏰ Starts in {game_analysis.minutes_to_game} minutes ({game_analysis.game_time.strftime('%H:%M')})",
            "-" * 50
        ]
        
        # Collect and sort all recommendations by confidence
        all_recommendations = []
        
        # Add all signals to recommendations
        for signal in game_analysis.all_signals:
            all_recommendations.append(self._signal_to_recommendation(signal))
        
        # Sort by confidence score (descending), then by priority
        all_recommendations.sort(key=lambda x: (-x['confidence_score'], x['priority']))
        
        for i, rec in enumerate(all_recommendations, 1):
            lines.extend(self._format_recommendation(i, rec))
        
        return "\n".join(lines)
    
    def _signal_to_recommendation(self, signal: BettingSignal) -> Dict[str, Any]:
        """Convert a BettingSignal to a recommendation dictionary"""
        # Determine priority based on signal type
        priority_map = {
            SignalType.STEAM_MOVE: 1,
            SignalType.OPPOSING_MARKETS: 2,
            SignalType.SHARP_ACTION: 3,
            SignalType.TOTAL_SHARP: 3,
            SignalType.BOOK_CONFLICT: 4
        }
        
        # Format signal type display
        type_emoji_map = {
            SignalType.STEAM_MOVE: "⚡",
            SignalType.OPPOSING_MARKETS: "🔄",
            SignalType.SHARP_ACTION: "🔥",
            SignalType.TOTAL_SHARP: "🔥",
            SignalType.BOOK_CONFLICT: "📚"
        }
        
        type_display = f"{type_emoji_map.get(signal.signal_type, '🎯')} {signal.strategy_name}"
        
        # Build reason based on signal type
        if signal.signal_type == SignalType.OPPOSING_MARKETS:
            reason = self._format_opposing_markets_reason(signal)
        else:
            reason = f"{signal.differential:+.1f}% differential"
        
        # Build source details
        book_display = signal.book or 'UNKNOWN'
        source_details = f"{signal.source}-{book_display}"
        
        if signal.signal_type not in [SignalType.OPPOSING_MARKETS, SignalType.BOOK_CONFLICT]:
            source_details += f": {signal.metadata.get('stake_pct', 0):.0f}% money vs {signal.metadata.get('bet_pct', 0):.0f}% bets"
        
        return {
            'type': type_display,
            'bet': signal.recommendation,
            'reason': reason,
            'source_details': source_details,
            'win_rate': signal.win_rate,
            'roi': signal.roi,
            'priority': priority_map.get(signal.signal_type, 5),
            'confidence_score': signal.confidence_score,
            'confidence_level': signal.confidence_level.value,
            'confidence_explanation': signal.confidence_explanation,
            'last_updated': signal.last_updated,
            'strategy_name': signal.strategy_name
        }
    
    def _format_opposing_markets_reason(self, signal: BettingSignal) -> str:
        """Format opposing markets signal reason"""
        metadata = signal.metadata
        
        # Extract metadata for opposing markets
        ml_rec_team = metadata.get('ml_recommendation', 'Unknown')
        spread_rec_team = metadata.get('spread_recommendation', 'Unknown')
        stronger_signal = metadata.get('stronger_signal', 'Unknown')
        bet_type = metadata.get('bet_type', 'Unknown')
        
        return f"ML: {ml_rec_team} vs Spread: {spread_rec_team} → Follow {stronger_signal} ({bet_type})"
    
    def _format_recommendation(self, index: int, rec: Dict[str, Any]) -> List[str]:
        """Format a single recommendation with clear betting instructions"""
        
        # Parse clear betting instructions from the original bet recommendation
        bet_details = self._parse_clear_bet_details(rec)
        
        lines = [
            f"  {index}. {rec['type']}",
            f"     🎰 Bet Type: {bet_details['bet_type']}",
            f"     💰 {bet_details['clear_bet_instruction']}",
            f"     🏪 Book: {bet_details['recommended_book']}",
            f"     📊 {rec['reason']}",
            f"     📈 {rec['win_rate']:.1f}% win rate, {rec['roi']:+.1f}% ROI"
        ]
        
        # Add confidence information
        confidence_emoji = self._get_confidence_emoji(rec['confidence_score'])
        lines.append(f"     {confidence_emoji} Confidence: {rec['confidence_score']:.0f}/100 ({rec['confidence_level']})")
        
        if rec.get('confidence_explanation'):
            lines.append(f"     💡 {rec['confidence_explanation']}")
        
        lines.append(f"     📍 {rec['source_details']}")
        
        # Show strategy name if different from type
        if rec.get('strategy_name') and rec['strategy_name'] != 'Unknown Strategy':
            lines.append(f"     🎯 Strategy: {rec['strategy_name']}")
        
        # Format timestamp
        last_updated = rec['last_updated']
        if hasattr(last_updated, 'astimezone'):
            last_updated_est = last_updated.astimezone(self.est)
        else:
            last_updated_est = last_updated
        
        lines.append(f"     🕐 Updated: {last_updated_est.strftime('%H:%M')} EST")
        lines.append("")  # Empty line between recommendations
        
        return lines
    
    def _parse_clear_bet_details(self, rec: Dict[str, Any]) -> Dict[str, str]:
        """Parse recommendation into clear betting instructions with bet type, odds, and book"""
        
        try:
            # Extract bet type from strategy name or signal type
            strategy_name = rec.get('strategy_name', '').lower()
            bet_recommendation = rec.get('bet', '').upper()
            
            if 'moneyline' in strategy_name or '_ml_' in strategy_name or 'ML' in bet_recommendation:
                bet_type = "MONEYLINE"
            elif 'spread' in strategy_name or '_sprd_' in strategy_name or 'SPREAD' in bet_recommendation:
                bet_type = "SPREAD"
            elif 'total' in strategy_name or '_tot_' in strategy_name or 'TOTAL' in bet_recommendation or 'OVER' in bet_recommendation or 'UNDER' in bet_recommendation:
                bet_type = "TOTAL"
            else:
                bet_type = "UNKNOWN"
            
            # Extract book from strategy name or source details
            book = "UNKNOWN"
            source_details = rec.get('source_details', '').lower()
            if 'draftkings' in strategy_name or 'dk' in strategy_name or 'draftkings' in source_details:
                book = "DraftKings"
            elif 'circa' in strategy_name or 'circa' in source_details:
                book = "Circa"
            elif 'fanduel' in strategy_name or 'fanduel' in source_details:
                book = "FanDuel"
            elif 'betmgm' in strategy_name or 'betmgm' in source_details:
                book = "BetMGM"
            else:
                # Try to extract from source details
                if 'VSIN-' in source_details:
                    if 'draftkings' in source_details or 'dk' in source_details:
                        book = "DraftKings"
                    elif 'circa' in source_details:
                        book = "Circa"
            
            # Clean up the bet recommendation for clear instruction
            original_bet = rec.get('bet', '')
            if original_bet:
                # Clean up the recommendation
                clean_instruction = original_bet.replace("BET ", "").strip()
                
                # Add bet type clarity if not already clear
                if bet_type == "MONEYLINE" and "ML" not in clean_instruction and "MONEYLINE" not in clean_instruction.upper():
                    clear_instruction = f"{clean_instruction} MONEYLINE"
                elif bet_type == "SPREAD" and "SPREAD" not in clean_instruction.upper():
                    clear_instruction = f"{clean_instruction} SPREAD"
                elif bet_type == "TOTAL" and "TOTAL" not in clean_instruction.upper() and "OVER" not in clean_instruction.upper() and "UNDER" not in clean_instruction.upper():
                    clear_instruction = f"{clean_instruction} TOTAL"
                else:
                    clear_instruction = clean_instruction
            else:
                clear_instruction = f"{bet_type} BET"
            
            return {
                'clear_bet_instruction': clear_instruction,
                'bet_type': bet_type,
                'recommended_book': book
            }
            
        except Exception as e:
            # Fallback to original bet recommendation
            return {
                'clear_bet_instruction': rec.get('bet', 'BET RECOMMENDATION'),
                'bet_type': "UNKNOWN",
                'recommended_book': "UNKNOWN"
            }
    
    def _format_summary(self, analysis_result: BettingAnalysisResult) -> str:
        """Format analysis summary"""
        opportunities_by_type = analysis_result.opportunities_by_type
        
        lines = [
            "\n📊 SUMMARY:",
            f"   ⚡ Steam Moves: {opportunities_by_type.get(SignalType.STEAM_MOVE, 0)}",
            f"   🔄 Opposing Markets: {opportunities_by_type.get(SignalType.OPPOSING_MARKETS, 0)}",
            f"   🔥 Sharp Signals: {opportunities_by_type.get(SignalType.SHARP_ACTION, 0) + opportunities_by_type.get(SignalType.TOTAL_SHARP, 0)}",
            f"   📚 Book Conflicts: {opportunities_by_type.get(SignalType.BOOK_CONFLICT, 0)}",
            f"   🎯 Total Games: {analysis_result.total_games}",
            f"   🤖 All recommendations use AI-validated strategies"
        ]
        
        return "\n".join(lines)
    
    def _format_no_opportunities(self, analysis_result: BettingAnalysisResult, 
                                juice_filter_summary: Dict[str, Any] = None) -> str:
        """Format display when no opportunities are found"""
        lines = [
            "=" * 70,
            "🚫 NO BETTING OPPORTUNITIES FOUND",
            "=" * 70,
            f"\n📊 SUMMARY: {analysis_result.total_games} games analyzed",
            f"\n🔍 WHY NO BETS RECOMMENDED:",
            f"   ✅ All games were analyzed using validated strategies",
            f"   ✅ Current betting data does not meet proven thresholds",
            f"   ✅ This protects you from low-probability bets",
            f"\n💡 RECOMMENDATION:",
            f"   🛑 NO BETS RECOMMENDED AT THIS TIME",
            f"   ⏰ Check again closer to other game times",
            f"   📧 You'll receive email alerts when opportunities are found",
            "=" * 70
        ]
        
        return "\n".join(lines)
    
    def _get_confidence_emoji(self, score: float) -> str:
        """Get emoji for confidence score"""
        if score >= 90:
            return "🔥"  # Very high confidence
        elif score >= 75:
            return "⭐"  # High confidence
        elif score >= 60:
            return "✅"  # Moderate confidence
        elif score >= 45:
            return "⚠️"   # Low confidence
        else:
            return "❌"  # Very low confidence
    
    def format_debug_info(self, database_stats: Dict[str, int], 
                         actionable_games: int) -> str:
        """Format debug information"""
        lines = [
            "🔍 DATABASE DEBUG MODE",
            "=" * 60,
            f"📊 Total records in database: {database_stats.get('total_records', 0)}",
            f"📅 Records from last 24 hours: {database_stats.get('recent_records', 0)}",
            f"🎯 Actionable games found: {actionable_games}"
        ]
        
        if database_stats.get('total_records', 0) == 0:
            lines.extend([
                "❌ NO DATA FOUND - This explains why master detector shows no opportunities!",
                "\n💡 TO FIX:",
                "   1. Run: uv run src/mlb_sharp_betting/cli.py run --sport mlb --sportsbook circa",
                "   2. Check if data collection is working",
                "   3. Verify scrapers are collecting live data"
            ])
        
        return "\n".join(lines) 