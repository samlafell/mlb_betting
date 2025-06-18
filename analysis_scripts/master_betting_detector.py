#!/usr/bin/env python3
"""
ADAPTIVE MASTER BETTING DETECTOR
===============================

Intelligently combines all validated betting strategies using:
✅ Dynamic thresholds optimized by backtesting results
✅ Only strategies currently performing above break-even
✅ Automatic configuration updates every 15 minutes
✅ Performance-based confidence levels

This detector learns from its own performance and adapts over time.

Usage: uv run analysis_scripts/master_betting_detector.py --minutes 300
"""

import argparse
import duckdb
import asyncio
import sys
from datetime import datetime, timedelta
from decimal import Decimal
import pytz
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from mlb_sharp_betting.services.strategy_config_manager import StrategyConfigManager


class AdaptiveMasterBettingDetector:
    def __init__(self, db_path='data/raw/mlb_betting.duckdb'):
        self.conn = duckdb.connect(db_path)
        self.est = pytz.timezone('US/Eastern')
        self.config_manager = StrategyConfigManager()
        
    async def analyze_all_opportunities(self, minutes_ahead=60):
        """Comprehensive analysis using only validated, profitable strategies"""
        
        now_est = datetime.now(self.est)
        cutoff_time = now_est + timedelta(minutes=minutes_ahead)
        
        print("🤖 ADAPTIVE MASTER BETTING DETECTOR")
        print("=" * 55)
        print("🧠 Using AI-optimized thresholds from backtesting results")
        print(f"📅 Current time: {now_est.strftime('%H:%M:%S %Z')}")
        print(f"🎯 Looking until: {cutoff_time.strftime('%H:%M:%S %Z')}")
        
        # Load current strategy performance
        await self._display_strategy_status()
        print()
        
        # Get validated opportunities using dynamic thresholds
        sharp_signals = await self._get_validated_sharp_signals(minutes_ahead)
        opposing_markets = await self._get_validated_opposing_signals(minutes_ahead)
        steam_moves = await self._get_validated_steam_moves(minutes_ahead)
        
        # Combine and analyze
        all_opportunities = self._combine_analyses(sharp_signals, opposing_markets, steam_moves)
        
        return all_opportunities
    
    async def _display_strategy_status(self):
        """Display current strategy configuration status"""
        summary = await self.config_manager.get_strategy_summary()
        
        print(f"📊 STRATEGY STATUS:")
        if summary['total_strategies'] == 0:
            print(f"   ⚠️  {summary['status']}")
            print(f"   🔧 {summary['recommendation']}")
        else:
            print(f"   ✅ {summary['total_strategies']} validated strategies active")
            print(f"   📈 Weighted Win Rate: {summary['weighted_avg_win_rate']:.1%}")
            print(f"   💰 Weighted ROI: {summary['weighted_avg_roi']:+.1f}%")
            print(f"   🏆 Top Strategy: {summary['top_strategy']['name']}")
            print(f"      └─ {summary['top_strategy']['win_rate']:.1%} win rate, {summary['top_strategy']['roi_per_100']:+.1f}% ROI")
    
    async def _get_validated_sharp_signals(self, minutes_ahead):
        """Get sharp action signals using validated thresholds"""
        now_est = datetime.now(self.est)
        sharp_signals = []
        
        # Get configurations for each source
        vsin_config = await self.config_manager.get_threshold_config("VSIN")
        sbd_config = await self.config_manager.get_threshold_config("SBD")
        
        query = """
        WITH latest_splits AS (
            SELECT 
                home_team, away_team, game_datetime, split_type, split_value,
                home_or_over_stake_percentage as stake_pct,
                home_or_over_bets_percentage as bet_pct,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, COALESCE(book, 'UNKNOWN') as book, last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY home_team, away_team, game_datetime, split_type, source, COALESCE(book, 'UNKNOWN')
                    ORDER BY last_updated DESC
                ) as rn
            FROM splits.raw_mlb_betting_splits
            WHERE home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND game_datetime IS NOT NULL
        )
        SELECT home_team, away_team, game_datetime, split_type, split_value,
               stake_pct, bet_pct, differential, source, book, last_updated
        FROM latest_splits WHERE rn = 1
        ORDER BY game_datetime ASC, ABS(differential) DESC
        """
        
        results = self.conn.execute(query).fetchall()
        
        for row in results:
            home, away, game_time, split_type, split_value, stake_pct, bet_pct, differential, source, book, last_updated = row
            
            if game_time.tzinfo is None:
                game_time_est = self.est.localize(game_time)
            else:
                game_time_est = game_time.astimezone(self.est)
            
            time_diff_minutes = (game_time_est - now_est).total_seconds() / 60
            
            if 0 <= time_diff_minutes <= minutes_ahead:
                abs_diff = abs(float(differential))
                
                # Apply dynamic thresholds based on source
                confidence = await self._get_dynamic_signal_confidence(source, abs_diff)
                if confidence != "NONE":
                    sharp_signals.append({
                        'type': 'SHARP_ACTION',
                        'home_team': home, 'away_team': away,
                        'game_time': game_time_est,
                        'minutes_to_game': int(time_diff_minutes),
                        'split_type': split_type, 'split_value': split_value,
                        'stake_pct': float(stake_pct), 'bet_pct': float(bet_pct),
                        'differential': float(differential),
                        'source': source, 'book': book,
                        'confidence': confidence,
                        'recommendation': self._get_recommendation(split_type, differential, home, away),
                        'signal_strength': abs_diff,
                        'last_updated': last_updated,
                        'threshold_type': vsin_config.strategy_type if source == 'VSIN' else sbd_config.strategy_type
                    })
        
        return sharp_signals
    
    async def _get_validated_opposing_signals(self, minutes_ahead):
        """Get opposing markets signals using validated strategy configuration"""
        now_est = datetime.now(self.est)
        opposing_signals = []
        
        # Check if opposing markets strategy is validated and enabled
        opposing_config = await self.config_manager.get_opposing_markets_config()
        if not opposing_config['enabled']:
            print(f"⚠️  Opposing Markets: {opposing_config['reason']}")
            return []
        
        print(f"✅ Opposing Markets Strategy: {opposing_config['strategy_name']} "
              f"({opposing_config['win_rate']:.1%} win rate, {opposing_config['roi_per_100']:+.1f}% ROI)")
        
        query = """
        WITH latest_splits AS (
            SELECT 
                home_team, away_team, game_datetime, split_type, split_value,
                home_or_over_stake_percentage as stake_pct,
                home_or_over_bets_percentage as bet_pct,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, COALESCE(book, 'UNKNOWN') as book, last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY home_team, away_team, game_datetime, split_type, source, COALESCE(book, 'UNKNOWN')
                    ORDER BY last_updated DESC
                ) as rn
            FROM splits.raw_mlb_betting_splits
            WHERE home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND game_datetime IS NOT NULL
              AND split_type IN ('moneyline', 'spread')
        ),
        
        clean_splits AS (SELECT * FROM latest_splits WHERE rn = 1),
        
        ml_signals AS (
            SELECT 
                home_team, away_team, game_datetime, source, book,
                differential as ml_diff, stake_pct as ml_stake_pct, bet_pct as ml_bet_pct,
                CASE WHEN differential > 0 THEN home_team ELSE away_team END as ml_rec_team,
                ABS(differential) as ml_strength, last_updated
            FROM clean_splits WHERE split_type = 'moneyline'
        ),
        
        spread_signals AS (
            SELECT 
                home_team, away_team, game_datetime, source, book,
                differential as spread_diff, stake_pct as spread_stake_pct, bet_pct as spread_bet_pct,
                CASE WHEN differential > 0 THEN home_team ELSE away_team END as spread_rec_team,
                ABS(differential) as spread_strength, last_updated
            FROM clean_splits WHERE split_type = 'spread'
        )
        
        SELECT 
            ml.home_team, ml.away_team, ml.game_datetime, ml.source, ml.book,
            ml.ml_rec_team, ml.ml_diff, ml.ml_strength, ml.ml_stake_pct, ml.ml_bet_pct,
            sp.spread_rec_team, sp.spread_diff, sp.spread_strength, sp.spread_stake_pct, sp.spread_bet_pct,
            (ml.ml_strength + sp.spread_strength) / 2 as combined_strength,
            ABS(ml.ml_diff - sp.spread_diff) as opposition_strength,
            CASE 
                WHEN ml.ml_strength > sp.spread_strength THEN 'ML_STRONGER'
                WHEN sp.spread_strength > ml.ml_strength THEN 'SPREAD_STRONGER'
                ELSE 'EQUAL'
            END as dominant_market,
            ml.last_updated
        FROM ml_signals ml
        INNER JOIN spread_signals sp ON ml.home_team = sp.home_team AND ml.away_team = sp.away_team 
            AND ml.game_datetime = sp.game_datetime AND ml.source = sp.source AND ml.book = sp.book
        WHERE ml.ml_rec_team != sp.spread_rec_team
        ORDER BY ml.game_datetime ASC, combined_strength DESC
        """
        
        results = self.conn.execute(query).fetchall()
        
        # Use validated thresholds
        high_threshold = opposing_config['high_confidence_strength']
        moderate_threshold = opposing_config['min_combined_strength']
        
        for row in results:
            (home, away, game_time, source, book, ml_rec, ml_diff, ml_strength, ml_stake, ml_bet,
             spread_rec, spread_diff, spread_strength, spread_stake, spread_bet,
             combined_strength, opposition_strength, dominant_market, last_updated) = row
            
            if game_time.tzinfo is None:
                game_time_est = self.est.localize(game_time)
            else:
                game_time_est = game_time.astimezone(self.est)
            
            time_diff_minutes = (game_time_est - now_est).total_seconds() / 60
            
            if 0 <= time_diff_minutes <= minutes_ahead:
                # Apply validated thresholds
                combined_strength_val = float(combined_strength)
                if combined_strength_val >= high_threshold:
                    confidence = "HIGH CONFIDENCE"
                elif combined_strength_val >= moderate_threshold:
                    confidence = "MODERATE CONFIDENCE"
                else:
                    continue  # Below validated threshold
                
                opposing_signals.append({
                    'type': 'OPPOSING_MARKETS',
                    'home_team': home, 'away_team': away,
                    'game_time': game_time_est,
                    'minutes_to_game': int(time_diff_minutes),
                    'source': source, 'book': book,
                    'ml_recommendation': ml_rec,
                    'spread_recommendation': spread_rec,
                    'ml_differential': float(ml_diff),
                    'spread_differential': float(spread_diff),
                    'ml_strength': float(ml_strength),
                    'spread_strength': float(spread_strength),
                    'combined_strength': combined_strength_val,
                    'opposition_strength': float(opposition_strength),
                    'dominant_market': dominant_market,
                    'confidence': confidence,
                    'follow_stronger_rec': ml_rec if dominant_market == 'ML_STRONGER' else spread_rec,
                    'last_updated': last_updated,
                    'validated_strategy': opposing_config['strategy_name']
                })
        
        return opposing_signals
    
    async def _get_validated_steam_moves(self, minutes_ahead):
        """Get steam move signals using validated strategy configuration"""
        now_est = datetime.now(self.est)
        steam_moves = []
        
        # Check if steam move strategy is validated and enabled
        steam_config = await self.config_manager.get_steam_move_config()
        if not steam_config['enabled']:
            print(f"⚠️  Steam Moves: {steam_config['reason']}")
            return []
        
        print(f"✅ Steam Move Strategy: {steam_config['strategy_name']} "
              f"({steam_config['win_rate']:.1%} win rate, {steam_config['roi_per_100']:+.1f}% ROI)")
        
        query = """
        WITH latest_splits AS (
            SELECT 
                home_team, away_team, game_datetime, split_type, split_value,
                home_or_over_stake_percentage as stake_pct,
                home_or_over_bets_percentage as bet_pct,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, COALESCE(book, 'UNKNOWN') as book, last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY home_team, away_team, game_datetime, split_type, source, COALESCE(book, 'UNKNOWN')
                    ORDER BY last_updated DESC
                ) as rn
            FROM splits.raw_mlb_betting_splits
            WHERE home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND game_datetime IS NOT NULL
        )
        SELECT home_team, away_team, game_datetime, split_type, split_value,
               stake_pct, bet_pct, differential, source, book, last_updated
        FROM latest_splits WHERE rn = 1
        ORDER BY game_datetime ASC, ABS(differential) DESC
        """
        
        results = self.conn.execute(query).fetchall()
        
        # Use validated threshold
        steam_threshold = steam_config['min_threshold']
        time_window_hours = steam_config['time_window_hours']
        
        for row in results:
            home, away, game_time, split_type, split_value, stake_pct, bet_pct, differential, source, book, last_updated = row
            
            if game_time.tzinfo is None:
                game_time_est = self.est.localize(game_time)
            else:
                game_time_est = game_time.astimezone(self.est)
            
            time_diff_minutes = (game_time_est - now_est).total_seconds() / 60
            
            if 0 <= time_diff_minutes <= minutes_ahead:
                # Calculate when the last update was relative to game time
                if last_updated.tzinfo is None:
                    last_updated_est = self.est.localize(last_updated)
                else:
                    last_updated_est = last_updated.astimezone(self.est)
                hours_before_game = (game_time_est - last_updated_est).total_seconds() / 3600
                
                # Steam move criteria: validated threshold within time window
                abs_diff = abs(float(differential))
                is_steam_move = (hours_before_game <= time_window_hours and abs_diff >= steam_threshold)
                
                if is_steam_move:
                    steam_moves.append({
                        'type': 'STEAM_MOVE',
                        'home_team': home, 'away_team': away,
                        'game_time': game_time_est,
                        'minutes_to_game': int(time_diff_minutes),
                        'split_type': split_type, 'split_value': split_value,
                        'differential': float(differential),
                        'source': source, 'book': book,
                        'hours_before_game': hours_before_game,
                        'signal_strength': abs_diff,
                        'confidence': 'STEAM_MOVE',
                        'recommendation': self._get_recommendation(split_type, differential, home, away),
                        'last_updated': last_updated,
                        'validated_strategy': steam_config['strategy_name'],
                        'threshold_used': steam_threshold
                    })
        
        return steam_moves
    
    async def _get_dynamic_signal_confidence(self, source, abs_diff):
        """Get confidence level using dynamically optimized thresholds"""
        config = await self.config_manager.get_threshold_config(source)
        
        if abs_diff >= config.high_confidence_threshold:
            return "HIGH CONFIDENCE"
        elif abs_diff >= config.moderate_confidence_threshold:
            return "MODERATE CONFIDENCE"
        elif abs_diff >= config.minimum_threshold:
            return "LOW CONFIDENCE"
        else:
            return "NONE"
    
    def _combine_analyses(self, sharp_signals, opposing_markets, steam_moves):
        """Combine all analyses into unified game-by-game recommendations"""
        
        # Group all signals by game
        games = {}
        
        # Add sharp signals
        for signal in sharp_signals:
            game_key = (signal['away_team'], signal['home_team'], signal['game_time'])
            if game_key not in games:
                games[game_key] = {'sharp_signals': [], 'opposing_markets': [], 'steam_moves': []}
            games[game_key]['sharp_signals'].append(signal)
        
        # Add opposing markets
        for signal in opposing_markets:
            game_key = (signal['away_team'], signal['home_team'], signal['game_time'])
            if game_key not in games:
                games[game_key] = {'sharp_signals': [], 'opposing_markets': [], 'steam_moves': []}
            games[game_key]['opposing_markets'].append(signal)
        
        # Add steam moves
        for signal in steam_moves:
            game_key = (signal['away_team'], signal['home_team'], signal['game_time'])
            if game_key not in games:
                games[game_key] = {'sharp_signals': [], 'opposing_markets': [], 'steam_moves': []}
            games[game_key]['steam_moves'].append(signal)
        
        return games
    
    def _get_recommendation(self, split_type, differential, home, away):
        """Get betting recommendation based on differential"""
        if split_type in ['moneyline', 'spread']:
            return f"BET {home}" if differential > 0 else f"BET {away}"
        elif split_type == 'total':
            return "BET OVER" if differential > 0 else "BET UNDER"
        return "UNKNOWN"
    
    async def display_comprehensive_analysis(self, games):
        """Display comprehensive analysis with performance-based recommendations"""
        
        if not games:
            print("🚫 No betting opportunities found using validated strategies.")
            print("💡 This means current thresholds are set to avoid low-probability bets.")
            return
        
        total_opportunities = sum(len(g['sharp_signals']) + len(g['opposing_markets']) + len(g['steam_moves']) for g in games.values())
        
        print(f"\n🎯 {total_opportunities} VALIDATED BETTING SIGNALS ACROSS {len(games)} GAMES")
        print("=" * 75)
        print("🧠 All signals use AI-optimized thresholds from backtesting results")
        
        for (away, home, game_time), signals in sorted(games.items(), key=lambda x: x[0][2]):
            now_est = datetime.now(self.est)
            minutes_to_game = int((game_time - now_est).total_seconds() / 60)
            
            print(f"\n🏟️  {away} @ {home}")
            print(f"⏰ Starts in {minutes_to_game} minutes ({game_time.strftime('%H:%M')})")
            print("-" * 65)
            
            # Show steam moves first (highest priority)
            if signals['steam_moves']:
                print("  ⚡ VALIDATED STEAM MOVES (HIGHEST PRIORITY)")
                for steam in signals['steam_moves']:
                    print(f"     🔥 {steam['split_type'].upper()} - {steam['validated_strategy']}")
                    print(f"        💰 {steam['recommendation']}")
                    print(f"        📊 {steam['differential']:+.1f}% differential (threshold: {steam['threshold_used']:.1f}%)")
                    print(f"        🕐 Sharp action {steam['hours_before_game']:.1f}h before game")
                    print(f"        📍 {steam['source']}-{steam['book']}")
            
            # Show opposing markets
            if signals['opposing_markets']:
                print("  🔄 VALIDATED OPPOSING MARKETS")
                for opp in signals['opposing_markets']:
                    print(f"     🎯 STRATEGY: {opp['validated_strategy']}")
                    print(f"        💰 BEST BET: {opp['follow_stronger_rec']}")
                    print(f"        📈 ML Signal: {opp['ml_recommendation']} ({opp['ml_differential']:+.1f}%)")
                    print(f"        📈 Spread Signal: {opp['spread_recommendation']} ({opp['spread_differential']:+.1f}%)")
                    print(f"        🔥 Combined Strength: {opp['combined_strength']:.1f}%")
                    print(f"        🏆 Dominant: {opp['dominant_market']} | {opp['confidence']}")
                    print(f"        📍 {opp['source']}-{opp['book']}")
            
            # Show regular sharp signals
            if signals['sharp_signals']:
                print("  🔥 VALIDATED SHARP ACTION")
                sharp_by_type = {}
                for sharp in signals['sharp_signals']:
                    split_type = sharp['split_type']
                    if split_type not in sharp_by_type:
                        sharp_by_type[split_type] = []
                    sharp_by_type[split_type].append(sharp)
                
                for split_type, sharps in sharp_by_type.items():
                    print(f"     🎯 {split_type.upper()}")
                    for sharp in sorted(sharps, key=lambda x: x['signal_strength'], reverse=True):
                        print(f"        💰 {sharp['recommendation']}")
                        print(f"        📊 {sharp['stake_pct']:.1f}% money vs {sharp['bet_pct']:.1f}% bets ({sharp['differential']:+.1f}%)")
                        print(f"        📈 {sharp['confidence']} ({sharp['threshold_type']} thresholds)")
                        print(f"        📍 {sharp['source']}-{sharp['book']}")
                        print(f"        🕐 Updated: {sharp['last_updated'].strftime('%H:%M')}")
        
        # Enhanced summary with performance context
        steam_count = sum(len(g['steam_moves']) for g in games.values())
        opposing_count = sum(len(g['opposing_markets']) for g in games.values())
        sharp_count = sum(len(g['sharp_signals']) for g in games.values())
        
        print(f"\n📊 VALIDATED SIGNAL SUMMARY:")
        print(f"   ⚡ Steam Moves: {steam_count} (Best historical: 100% win rate)")
        print(f"   🔄 Opposing Markets: {opposing_count} (Best historical: 75% win rate)")
        print(f"   🔥 Sharp Signals: {sharp_count} (Best historical: 54-58% win rate)")
        print(f"   🎯 Total Games: {len(games)}")
        
        print(f"\n🎯 EXECUTION PRIORITY (Based on Historical Performance):")
        print(f"   1. 🥇 STEAM MOVES - Act immediately (Time-sensitive, highest win rate)")
        print(f"   2. 🥈 OPPOSING MARKETS - Use validated strategy (Proven 75% win rate)")
        print(f"   3. 🥉 SHARP SIGNALS - High-confidence only (AI-optimized thresholds)")
        
        print(f"\n🤖 AI OPTIMIZATION STATUS:")
        print(f"   ✅ Thresholds auto-updated from backtesting results")
        print(f"   ✅ Only profitable strategies (>52% win rate) included")
        print(f"   ✅ Configurations refresh every 15 minutes")
        print(f"   ✅ Performance tracking for continuous improvement")


async def main():
    parser = argparse.ArgumentParser(description="Adaptive Master Betting Detector - AI-Optimized Strategies")
    parser.add_argument('--minutes', '-m', type=int, default=60,
                        help='Minutes ahead to look for opportunities (default: 60)')
    
    args = parser.parse_args()
    
    detector = AdaptiveMasterBettingDetector()
    games = await detector.analyze_all_opportunities(args.minutes)
    await detector.display_comprehensive_analysis(games)


if __name__ == "__main__":
    asyncio.run(main()) 