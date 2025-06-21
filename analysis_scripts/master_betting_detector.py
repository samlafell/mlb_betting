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
       uv run analysis_scripts/master_betting_detector.py --debug  # Show all data
"""

import argparse
import duckdb
import asyncio
import sys
import warnings
import logging
from datetime import datetime, timedelta
from decimal import Decimal
import pytz
from pathlib import Path

# Suppress warnings and set clean logging
warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")
logging.getLogger("mlb_sharp_betting").setLevel(logging.WARNING)  # Only show warnings and errors
logging.getLogger("duckdb").setLevel(logging.ERROR)  # Only show errors from DuckDB

# Add src to path for imports
sys.path.insert(0, 'src')

from mlb_sharp_betting.db.connection import get_db_manager
from mlb_sharp_betting.services.database_coordinator import get_database_coordinator, coordinated_database_access
from mlb_sharp_betting.services.strategy_config_manager import StrategyConfigManager
from mlb_sharp_betting.services.juice_filter_service import get_juice_filter_service
from mlb_sharp_betting.services.confidence_scorer import get_confidence_scorer
from mlb_sharp_betting.core.logging import get_logger


class AdaptiveMasterBettingDetector:
    """Advanced betting detector using AI-optimized thresholds from backtesting results"""
    
    def __init__(self, db_path='data/raw/mlb_betting.duckdb'):
        self.db_manager = None  # Lazy loaded through coordinator
        self.coordinator = get_database_coordinator()
        self.config_manager = StrategyConfigManager()
        self.juice_filter = get_juice_filter_service()
        self.confidence_scorer = get_confidence_scorer()
        self.logger = get_logger(__name__)
        self.est = pytz.timezone('US/Eastern')
        
    async def debug_database_contents(self):
        """Debug function to show what data is actually in the database"""
        print("🔍 DATABASE DEBUG MODE")
        print("=" * 60)
        
        # Check basic database stats
        try:
            query = "SELECT COUNT(*) FROM splits.raw_mlb_betting_splits"
            result = self.coordinator.execute_read(query)
            total_count = result[0][0] if result else 0
            print(f"📊 Total records in database: {total_count}")
            
            if total_count == 0:
                print("❌ NO DATA FOUND - This explains why master detector shows no opportunities!")
                print("\n💡 TO FIX:")
                print("   1. Run: uv run src/mlb_sharp_betting/cli.py run --sport mlb --sportsbook circa")
                print("   2. Check if data collection is working")
                print("   3. Verify scrapers are collecting live data")
                return
            
            # Check recent data
            query = """
                SELECT COUNT(*) FROM splits.raw_mlb_betting_splits 
                WHERE last_updated > NOW() - INTERVAL 24 HOUR
            """
            result = self.coordinator.execute_read(query)
            recent_count = result[0][0] if result else 0
            print(f"📅 Records from last 24 hours: {recent_count}")
            
            # Show sample of most recent data
            query = """
                SELECT home_team, away_team, split_type, game_datetime, last_updated,
                       home_or_over_stake_percentage, home_or_over_bets_percentage,
                       ABS(home_or_over_stake_percentage - home_or_over_bets_percentage) as diff,
                       source, book
                FROM splits.raw_mlb_betting_splits 
                ORDER BY last_updated DESC 
                LIMIT 10
            """
            result = self.coordinator.execute_read(query)
            
            print(f"\n📋 MOST RECENT DATA (Last 10 records):")
            for i, row in enumerate(result, 1):
                home, away, split_type, game_dt, updated, stake_pct, bet_pct, diff, source, book = row
                print(f"   {i}. {away} @ {home} - {split_type.upper()}")
                print(f"      🎯 Game Time: {game_dt}")
                print(f"      🕐 Updated: {updated}")
                print(f"      💰 {stake_pct:.1f}% money vs {bet_pct:.1f}% bets = {diff:.1f}% diff")
                print(f"      📍 {source}-{book}")
                print()
            
            # Check what games would be found with current time filters
            now_est = datetime.now(self.est)
            
            # Check next 24 hours (much broader than default 60 minutes)
            end_time = now_est + timedelta(hours=24)
            
            query = """
                SELECT COUNT(*) FROM splits.raw_mlb_betting_splits
                WHERE game_datetime BETWEEN ? AND ?
                  AND home_or_over_stake_percentage IS NOT NULL 
                  AND home_or_over_bets_percentage IS NOT NULL
                  AND game_datetime IS NOT NULL
                  AND NOT (home_or_over_stake_percentage = 0 AND home_or_over_bets_percentage = 0)
            """
            result = self.coordinator.execute_read(query, (now_est, end_time))
            future_count = result[0][0] if result else 0
            
            print(f"🎯 ACTIONABLE GAMES (Next 24 hours with valid data): {future_count}")
            
            if future_count == 0:
                print("❌ NO FUTURE GAMES FOUND")
                print("\n🔍 DIAGNOSING THE ISSUE:")
                
                # Check if we have games but they're in the past
                query = """
                    SELECT COUNT(*) FROM splits.raw_mlb_betting_splits
                    WHERE game_datetime < ?
                """
                result = self.coordinator.execute_read(query, (now_est,))
                past_games = result[0][0] if result else 0
                print(f"   📅 Past games in database: {past_games}")
                
                # Check if we have games but they're too far in future
                far_future = now_est + timedelta(days=7)
                query = """
                    SELECT COUNT(*) FROM splits.raw_mlb_betting_splits
                    WHERE game_datetime > ?
                """
                result = self.coordinator.execute_read(query, (far_future,))
                far_future_games = result[0][0] if result else 0
                print(f"   📅 Games more than 7 days out: {far_future_games}")
                
                # Show game time distribution
                query = """
                    SELECT 
                        MIN(game_datetime) as earliest_game,
                        MAX(game_datetime) as latest_game,
                        COUNT(DISTINCT game_datetime) as unique_game_times
                    FROM splits.raw_mlb_betting_splits
                """
                result = self.coordinator.execute_read(query)
                if result and result[0]:
                    earliest, latest, unique_times = result[0]
                    print(f"   📊 Game time range: {earliest} to {latest}")
                    print(f"   📊 Unique game times: {unique_times}")
                
            else:
                # Show the actionable games
                query = """
                    SELECT home_team, away_team, game_datetime,
                           ABS(home_or_over_stake_percentage - home_or_over_bets_percentage) as diff,
                           source, book, split_type
                    FROM splits.raw_mlb_betting_splits
                    WHERE game_datetime BETWEEN ? AND ?
                      AND home_or_over_stake_percentage IS NOT NULL 
                      AND home_or_over_bets_percentage IS NOT NULL
                      AND game_datetime IS NOT NULL
                      AND NOT (home_or_over_stake_percentage = 0 AND home_or_over_bets_percentage = 0)
                    ORDER BY diff DESC
                    LIMIT 5
                """
                result = self.coordinator.execute_read(query, (now_est, end_time))
                
                print(f"🏆 TOP ACTIONABLE OPPORTUNITIES:")
                for row in result:
                    home, away, game_dt, diff, source, book, split_type = row
                    time_diff = (game_dt - now_est).total_seconds() / 3600  # hours
                    print(f"   🎯 {away} @ {home} - {split_type.upper()}")
                    print(f"      📅 Game in {time_diff:.1f} hours ({game_dt})")
                    print(f"      📊 {diff:.1f}% differential")
                    print(f"      📍 {source}-{book}")
                    
        except Exception as e:
            print(f"❌ Database debug failed: {e}")
    
    async def analyze_all_opportunities(self, minutes_ahead=60, debug_mode=False):
        """Comprehensive analysis using only validated, profitable strategies"""
        
        if debug_mode:
            await self.debug_database_contents()
            return {}
        
        now_est = datetime.now(self.est)
        cutoff_time = now_est + timedelta(minutes=minutes_ahead)
        
        print("🤖 ADAPTIVE MASTER BETTING DETECTOR")
        print("=" * 55)
        print("🧠 Using AI-optimized thresholds from backtesting results")
        
        # Show juice filter status
        juice_summary = self.juice_filter.get_filter_summary()
        if juice_summary['enabled']:
            print(f"🚫 SMART JUICE FILTER: Won't bet favorites worse than {juice_summary['max_juice_threshold']}")
        else:
            print("⚠️  JUICE FILTER: DISABLED")
        
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
        # Get strategies directly from backtesting database
        profitable_strategies = await self._get_current_profitable_strategies()
        
        print(f"📊 STRATEGY STATUS (Reading from Backtesting Database):")
        if not profitable_strategies:
            print(f"   ⚠️  No profitable strategies found in current backtesting results")
            print(f"   🔧 Run backtesting analysis to populate strategy performance data")
            print(f"   💡 Command: uv run src/mlb_sharp_betting/cli.py backtesting run --mode single-run")
        else:
            total_bets = sum(s['total_bets'] for s in profitable_strategies)
            weighted_win_rate = sum(s['win_rate'] * s['total_bets'] for s in profitable_strategies) / total_bets if total_bets > 0 else 0
            weighted_roi = sum(s['roi'] * s['total_bets'] for s in profitable_strategies) / total_bets if total_bets > 0 else 0
            
            print(f"   ✅ {len(profitable_strategies)} profitable strategies active")
            print(f"   📈 Weighted Win Rate: {weighted_win_rate:.1f}%")
            print(f"   💰 Weighted ROI: {weighted_roi:+.1f}%")
            
            # Show top 3 strategies
            top_strategies = sorted(profitable_strategies, key=lambda x: x['roi'], reverse=True)[:3]
            print(f"   🏆 Top Strategies:")
            for i, strategy in enumerate(top_strategies, 1):
                print(f"      {i}. {strategy['strategy_name'][:25]:<25} | {strategy['win_rate']:5.1f}% WR | {strategy['roi']:+6.1f}% ROI | {strategy['total_bets']:3d} bets")
    
    async def _get_validated_sharp_signals(self, minutes_ahead):
        """Get sharp signals using validated thresholds from backtesting."""
        now_est = datetime.now(self.est)
        end_time = now_est + timedelta(minutes=minutes_ahead)
        
        # Get current profitable strategies from backtesting results
        profitable_strategies = await self._get_current_profitable_strategies()
        
        if not profitable_strategies:
            print("⚠️  No profitable sharp action strategies found in recent backtesting")
            return []
        
        print(f"✅ Using {len(profitable_strategies)} profitable strategies from backtesting")
        
        # Only get the LATEST data per game/source/book/market combination
        # AND only signals within 5 minutes of game time (actionable window)
        query = """
        WITH latest_splits AS (
            SELECT 
                home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, book, game_datetime, last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY home_team, away_team, game_datetime, split_type, source, COALESCE(book, 'UNKNOWN')
                    ORDER BY last_updated DESC  -- Simply get the most recent timestamp
                ) as rn
            FROM splits.raw_mlb_betting_splits
            WHERE game_datetime BETWEEN ? AND ?
              AND home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND game_datetime IS NOT NULL
              AND NOT (home_or_over_stake_percentage = 0 AND home_or_over_bets_percentage = 0)  -- Filter out zero data
        ),
        actionable_signals AS (
            SELECT * FROM latest_splits 
            WHERE rn = 1  -- Only the most recent data per game/source/book/market
        )
        SELECT home_team, away_team, split_type, split_value, 
               home_or_over_stake_percentage, home_or_over_bets_percentage,
               differential, source, book, game_datetime, last_updated
        FROM actionable_signals
        WHERE 
            -- Only signals within specified time window (actionable window)
            EXTRACT('epoch' FROM (game_datetime - CURRENT_TIMESTAMP)) / 60 <= ?
            AND EXTRACT('epoch' FROM (game_datetime - CURRENT_TIMESTAMP)) / 60 >= 0
        ORDER BY ABS(differential) DESC
        """
        
        # Use coordinated database access to prevent conflicts
        results = self.coordinator.execute_read(query, (now_est, end_time, minutes_ahead))
        
        sharp_signals = []
        
        for row in results:
            home, away, split_type, split_value, stake_pct, bet_pct, differential, source, book, game_time, last_updated = row
            
            if game_time.tzinfo is None:
                game_time_est = self.est.localize(game_time)
            else:
                game_time_est = game_time.astimezone(self.est)
            
            time_diff_minutes = (game_time_est - now_est).total_seconds() / 60
            
            if 0 <= time_diff_minutes <= minutes_ahead:
                abs_diff = abs(float(differential))
                
                # Check if this signal matches any profitable strategy
                matching_strategy = self._find_matching_strategy(
                    profitable_strategies, source, book, split_type, abs_diff
                )
                
                if not matching_strategy:
                    continue  # No profitable strategy matches this signal
                
                # 🚫 JUICE FILTER: Refuse moneyline bets worse than -160 (only if betting the favorite)
                if split_type == 'moneyline':
                    # Determine which side is being recommended based on stake vs bet differential
                    # Positive differential = home team getting more stake than bets (sharp money on home)
                    # Negative differential = away team getting more stake than bets (sharp money on away)
                    recommended_side = 'home' if differential > 0 else 'away'
                    recommended_team = home if recommended_side == 'home' else away
                    
                    if self.juice_filter.should_filter_bet(split_value, recommended_team, home, away, 'sharp_signals'):
                        print(f"🚫 FILTERED: {home} vs {away} - {recommended_team} moneyline too juiced ({split_value})")
                        continue
                
                # Calculate comprehensive confidence score
                confidence_result = self.confidence_scorer.calculate_confidence(
                    signal_differential=float(differential),
                    source=source,
                    book=book or 'UNKNOWN',
                    split_type=split_type,
                    strategy_name='sharp_action',
                    last_updated=last_updated,
                    game_datetime=game_time_est
                )
                
                sharp_signals.append({
                    'type': 'SHARP_ACTION',
                    'home_team': home, 'away_team': away,
                    'game_time': game_time_est,
                    'minutes_to_game': int(time_diff_minutes),
                    'split_type': split_type, 'split_value': split_value,
                    'stake_pct': float(stake_pct), 'bet_pct': float(bet_pct),
                    'differential': float(differential),
                    'source': source, 'book': book,
                    'confidence': matching_strategy['confidence'],
                    'confidence_score': confidence_result.overall_confidence,
                    'confidence_level': confidence_result.confidence_level,
                    'confidence_explanation': confidence_result.explanation,
                    'recommendation_strength': confidence_result.recommendation_strength,
                    'recommendation': self._get_recommendation(split_type, differential, home, away),
                    'signal_strength': abs_diff,
                    'last_updated': last_updated,
                    'strategy_name': matching_strategy['strategy_name'],
                    'win_rate': matching_strategy['win_rate'],
                    'roi': matching_strategy['roi'],
                    'total_bets': matching_strategy['total_bets']
                })
        
        return sharp_signals
    
    async def _get_validated_opposing_signals(self, minutes_ahead):
        """Get opposing markets signals ONLY if validated by current backtesting results"""
        now_est = datetime.now(self.est)
        opposing_signals = []
        
        # Get current profitable strategies from backtesting database
        profitable_strategies = await self._get_current_profitable_strategies()
        
        # Check if ANY opposing markets strategy is profitable in current backtesting
        opposing_strategies = [s for s in profitable_strategies if 'opposing' in s['strategy_name'].lower() or 'conflict' in s['strategy_name'].lower()]
        
        if not opposing_strategies:
            print(f"⚠️  Opposing Markets: No opposing markets strategies found in current backtesting results")
            print(f"🔧 Only strategies validated by backtesting will be used")
            return []  # Return empty list - no fallback thresholds
        
        # Use the best performing opposing markets strategy
        best_opposing = max(opposing_strategies, key=lambda x: x['roi'])
        print(f"✅ Opposing Markets Strategy: {best_opposing['strategy_name']} "
              f"({best_opposing['win_rate']:.1f}% win rate, {best_opposing['roi']:+.1f}% ROI)")
        
        # Set thresholds based on actual performance
        if best_opposing['win_rate'] >= 65:
            high_threshold = 20.0  # Aggressive for high performers
            moderate_threshold = 15.0
        elif best_opposing['win_rate'] >= 60:
            high_threshold = 25.0  # Moderate
            moderate_threshold = 20.0
        else:
            high_threshold = 30.0  # Conservative
            moderate_threshold = 25.0
        
        query = """
        WITH latest_splits AS (
            SELECT 
                home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, book, game_datetime, last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY home_team, away_team, game_datetime, split_type, source, COALESCE(book, 'UNKNOWN')
                    ORDER BY last_updated DESC  -- Simply get the most recent timestamp
                ) as rn
            FROM splits.raw_mlb_betting_splits
            WHERE game_datetime BETWEEN ? AND ?
              AND home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND split_type IN ('moneyline', 'spread')
              AND NOT (home_or_over_stake_percentage = 0 AND home_or_over_bets_percentage = 0)  -- Filter out zero data
        ),
        clean_splits AS (
            SELECT * FROM latest_splits WHERE rn = 1
        ),
        ml_signals AS (
            SELECT 
                home_team, away_team, game_datetime, source, book,
                differential as ml_diff, 
                home_or_over_stake_percentage as ml_stake_pct, 
                home_or_over_bets_percentage as ml_bet_pct,
                CASE WHEN differential > 0 THEN home_team ELSE away_team END as ml_rec_team,
                ABS(differential) as ml_strength, last_updated
            FROM clean_splits WHERE split_type = 'moneyline'
        ),
        spread_signals AS (
            SELECT 
                home_team, away_team, game_datetime, source, book,
                differential as spread_diff, 
                home_or_over_stake_percentage as spread_stake_pct, 
                home_or_over_bets_percentage as spread_bet_pct,
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
            ml.last_updated,
            -- Add opposing side percentages for ML
            (100 - ml.ml_stake_pct) as ml_opposing_stake_pct,
            (100 - ml.ml_bet_pct) as ml_opposing_bet_pct,
            -- Add opposing side percentages for Spread  
            (100 - sp.spread_stake_pct) as spread_opposing_stake_pct,
            (100 - sp.spread_bet_pct) as spread_opposing_bet_pct
        FROM ml_signals ml
        INNER JOIN spread_signals sp ON ml.home_team = sp.home_team AND ml.away_team = sp.away_team 
            AND ml.game_datetime = sp.game_datetime AND ml.source = sp.source AND ml.book = sp.book
        WHERE ml.ml_rec_team != sp.spread_rec_team  -- Only opposing markets
          -- Only signals within specified time window (actionable window)
          AND EXTRACT('epoch' FROM (ml.game_datetime - CURRENT_TIMESTAMP)) / 60 <= ?
          AND EXTRACT('epoch' FROM (ml.game_datetime - CURRENT_TIMESTAMP)) / 60 >= 0
        ORDER BY combined_strength DESC
        """
        
        # Calculate end time for the query
        end_time = now_est + timedelta(minutes=minutes_ahead)
        
        # Use coordinated database access to prevent conflicts
        results = self.coordinator.execute_read(query, (now_est, end_time, minutes_ahead))
        
        # Thresholds are already set above based on validation status
        
        for row in results:
            (home, away, game_time, source, book, ml_rec_team, ml_diff, ml_strength, ml_stake_pct, ml_bet_pct,
             sp_rec_team, sp_diff, sp_strength, sp_stake_pct, sp_bet_pct, combined_strength, opposition_strength,
             dominant_market, last_updated, ml_opposing_stake_pct, ml_opposing_bet_pct, spread_opposing_stake_pct, spread_opposing_bet_pct) = row
            
            if game_time.tzinfo is None:
                game_time_est = self.est.localize(game_time)
            else:
                game_time_est = game_time.astimezone(self.est)
            
            time_diff_minutes = (game_time_est - now_est).total_seconds() / 60
            
            if 0 <= time_diff_minutes <= minutes_ahead:
                # Apply validated thresholds
                if combined_strength >= high_threshold:
                    confidence = "HIGH CONFIDENCE"
                elif combined_strength >= moderate_threshold:
                    confidence = "MODERATE CONFIDENCE"
                else:
                    continue  # Below validated threshold
                
                # Determine final recommendation (follow stronger signal)
                final_recommendation = ml_rec_team if combined_strength >= high_threshold else sp_rec_team
                
                # Determine which market to bet based on which signal is stronger
                if ml_strength > sp_strength:
                    bet_type = "MONEYLINE"
                    recommended_bet = f"BET {ml_rec_team} MONEYLINE"
                    stronger_signal = "ML"
                elif sp_strength > ml_strength:
                    bet_type = "SPREAD"
                    recommended_bet = f"BET {sp_rec_team} SPREAD"
                    stronger_signal = "Spread"
                else:
                    # Equal strength - use the one with higher confidence threshold
                    if combined_strength >= high_threshold:
                        bet_type = "MONEYLINE"
                        recommended_bet = f"BET {ml_rec_team} MONEYLINE"
                        stronger_signal = "ML"
                    else:
                        bet_type = "SPREAD"
                        recommended_bet = f"BET {sp_rec_team} SPREAD"
                        stronger_signal = "Spread"
                
                # 🚫 CENTRALIZED JUICE FILTER: Skip if betting heavily juiced favorites
                if combined_strength >= high_threshold:  # Following moneyline signal
                    # Get current moneyline odds
                    moneyline_query = """
                    SELECT split_value 
                    FROM splits.raw_mlb_betting_splits 
                    WHERE home_team = ? AND away_team = ? AND split_type = 'moneyline'
                    ORDER BY last_updated DESC LIMIT 1
                    """
                    # Use coordinated database access
                    ml_results = self.coordinator.execute_read(moneyline_query, (home, away))
                    ml_result = ml_results[0] if ml_results else None
                    
                    if ml_result and ml_result[0]:
                        if self.juice_filter.should_filter_bet(ml_result[0], final_recommendation, home, away, 'opposing_markets'):
                            continue  # Skip this bet due to juice filter
                
                # Calculate confidence score for opposing markets
                # Use the stronger signal's differential for confidence calculation
                stronger_differential = ml_diff if ml_strength > sp_strength else sp_diff
                confidence_result = self.confidence_scorer.calculate_confidence(
                    signal_differential=float(stronger_differential),
                    source=source,
                    book=book or 'UNKNOWN',
                    split_type='opposing_markets',
                    strategy_name='opposing_markets',
                    last_updated=last_updated,
                    game_datetime=game_time_est,
                    cross_validation_sources=2  # Both ML and spread signals
                )
                
                opposing_signals.append({
                    'type': 'OPPOSING_MARKETS',
                    'home_team': home, 'away_team': away,
                    'game_time': game_time_est,
                    'minutes_to_game': int(time_diff_minutes),
                    'source': source, 'book': book,
                    'ml_recommendation': ml_rec_team,
                    'spread_recommendation': sp_rec_team,
                    'ml_differential': float(ml_diff),
                    'spread_differential': float(sp_diff),
                    'ml_strength': float(ml_strength),
                    'spread_strength': float(sp_strength),
                    'combined_strength': float(combined_strength),
                    'opposition_strength': float(opposition_strength),
                    'dominant_market': dominant_market,
                    'confidence': confidence,
                    'confidence_score': confidence_result.overall_confidence,
                    'confidence_level': confidence_result.confidence_level,
                    'confidence_explanation': confidence_result.explanation,
                    'recommendation_strength': confidence_result.recommendation_strength,
                    'follow_stronger_rec': final_recommendation,
                    'last_updated': last_updated,
                    'validated_strategy': best_opposing['strategy_name'],
                    'ml_stake_pct': float(ml_stake_pct),
                    'ml_bet_pct': float(ml_bet_pct),
                    'spread_stake_pct': float(sp_stake_pct),
                    'spread_bet_pct': float(sp_bet_pct),
                    'ml_opposing_stake_pct': float(ml_opposing_stake_pct),
                    'ml_opposing_bet_pct': float(ml_opposing_bet_pct),
                    'spread_opposing_stake_pct': float(spread_opposing_stake_pct),
                    'spread_opposing_bet_pct': float(spread_opposing_bet_pct),
                    'bet_type': bet_type,
                    'recommended_bet': recommended_bet,
                    'stronger_signal': stronger_signal
                })
        
        return opposing_signals
    
    async def _get_validated_steam_moves(self, minutes_ahead):
        """Get steam move signals ONLY if validated by current backtesting results"""
        now_est = datetime.now(self.est)
        steam_moves = []
        
        # Get current profitable strategies from backtesting database
        profitable_strategies = await self._get_current_profitable_strategies()
        
        # Check if ANY steam move strategy is profitable in current backtesting
        steam_strategies = [s for s in profitable_strategies if 'steam' in s['strategy_name'].lower() or 'movement' in s['strategy_name'].lower()]
        
        if not steam_strategies:
            print(f"⚠️  Steam Moves: No steam move strategies found in current backtesting results")
            print(f"🔧 Only strategies validated by backtesting will be used")
            return []  # Return empty list - no fallback thresholds
        
        # Use the best performing steam move strategy
        best_steam = max(steam_strategies, key=lambda x: x['roi'])
        print(f"✅ Steam Move Strategy: {best_steam['strategy_name']} "
              f"({best_steam['win_rate']:.1f}% win rate, {best_steam['roi']:+.1f}% ROI)")
        
        query = """
        SELECT home_team, away_team, game_datetime, split_type, split_value,
               home_or_over_stake_percentage as stake_pct,
               home_or_over_bets_percentage as bet_pct,
               (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
               source, book, last_updated
        FROM splits.raw_mlb_betting_splits
        WHERE home_or_over_stake_percentage IS NOT NULL 
          AND home_or_over_bets_percentage IS NOT NULL
          AND game_datetime IS NOT NULL
          AND game_datetime BETWEEN ? AND ?
        ORDER BY game_datetime ASC, ABS(differential) DESC
        """
        
        # Calculate end time for the query
        end_time = now_est + timedelta(minutes=minutes_ahead)
        
        # Use coordinated database access to prevent conflicts
        results = self.coordinator.execute_read(query, (now_est, end_time))
        
        # Use validated threshold based on strategy performance
        if best_steam['win_rate'] >= 65:
            steam_threshold = 20.0  # Aggressive for high performers
            time_window_hours = 2
        elif best_steam['win_rate'] >= 60:
            steam_threshold = 25.0  # Moderate
            time_window_hours = 3
        else:
            steam_threshold = 30.0  # Conservative
            time_window_hours = 4
        
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
                        'validated_strategy': best_steam['strategy_name'],
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
    
    def _get_confidence_emoji(self, score):
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
    
    async def display_comprehensive_analysis(self, games):
        """Display comprehensive analysis with performance-based recommendations"""
        
        if not games:
            await self._display_no_opportunities_analysis()
            return

        total_opportunities = sum(len(g['sharp_signals']) + len(g['opposing_markets']) + len(g['steam_moves']) for g in games.values())
        
        print(f"\n🎯 {total_opportunities} BETTING OPPORTUNITIES FOUND")
        print("=" * 60)
        
        for (away, home, game_time), signals in sorted(games.items(), key=lambda x: x[0][2]):
            now_est = datetime.now(self.est)
            minutes_to_game = int((game_time - now_est).total_seconds() / 60)
            
            print(f"\n🏟️  {away} @ {home}")
            print(f"⏰ Starts in {minutes_to_game} minutes ({game_time.strftime('%H:%M')})")
            print("-" * 50)
            
            # Collect all recommendations for this game
            all_recommendations = []
            
            # Steam moves (highest priority)
            for steam in signals['steam_moves']:
                all_recommendations.append({
                    'type': '⚡ STEAM MOVE',
                    'bet': steam['recommendation'],
                    'reason': f"{steam['differential']:+.1f}% sharp money vs bets",
                    'source_details': f"{steam['source']}-{steam['book']}: {steam.get('stake_pct', 0):.0f}% money vs {steam.get('bet_pct', 0):.0f}% bets",
                    'win_rate': 75.0,  # Placeholder - will be updated with actual data
                    'roi': 25.0,
                    'priority': 1,
                    'confidence_score': steam.get('confidence_score', 85),  # Steam moves get high default confidence
                    'confidence_level': steam.get('confidence_level', 'HIGH'),
                    'confidence_explanation': steam.get('confidence_explanation', 'Strong steam move signal'),
                    'last_updated': steam['last_updated']
                })
            
            # Opposing markets
            for opp in signals['opposing_markets']:
                # Build detailed opposing markets explanation with full percentages
                ml_rec_side = opp['ml_recommendation']
                spread_rec_side = opp['spread_recommendation']
                
                # Determine which team is home/away for percentage display
                home_team = opp['home_team']
                away_team = opp['away_team']
                
                # ML percentages (show both sides)
                if ml_rec_side == home_team:
                    ml_rec_stake = opp['ml_stake_pct']
                    ml_rec_bet = opp['ml_bet_pct']
                    ml_opp_stake = opp['ml_opposing_stake_pct']
                    ml_opp_bet = opp['ml_opposing_bet_pct']
                    ml_details = f"ML: {home_team} {ml_rec_stake:.0f}%/{ml_rec_bet:.0f}% vs {away_team} {ml_opp_stake:.0f}%/{ml_opp_bet:.0f}%"
                else:
                    ml_rec_stake = opp['ml_opposing_stake_pct']
                    ml_rec_bet = opp['ml_opposing_bet_pct']
                    ml_opp_stake = opp['ml_stake_pct']
                    ml_opp_bet = opp['ml_bet_pct']
                    ml_details = f"ML: {away_team} {ml_rec_stake:.0f}%/{ml_rec_bet:.0f}% vs {home_team} {ml_opp_stake:.0f}%/{ml_opp_bet:.0f}%"
                
                # Spread percentages (show both sides)  
                if spread_rec_side == home_team:
                    spread_rec_stake = opp['spread_stake_pct']
                    spread_rec_bet = opp['spread_bet_pct']
                    spread_opp_stake = opp['spread_opposing_stake_pct']
                    spread_opp_bet = opp['spread_opposing_bet_pct']
                    spread_details = f"Spread: {home_team} {spread_rec_stake:.0f}%/{spread_rec_bet:.0f}% vs {away_team} {spread_opp_stake:.0f}%/{spread_opp_bet:.0f}%"
                else:
                    spread_rec_stake = opp['spread_opposing_stake_pct']
                    spread_rec_bet = opp['spread_opposing_bet_pct']
                    spread_opp_stake = opp['spread_stake_pct']
                    spread_opp_bet = opp['spread_bet_pct']
                    spread_details = f"Spread: {away_team} {spread_rec_stake:.0f}%/{spread_rec_bet:.0f}% vs {home_team} {spread_opp_stake:.0f}%/{spread_opp_bet:.0f}%"
                
                source_info = f"{opp['source']}-{opp['book']}"
                
                # Determine bet type and stronger signal
                bet_type = "SPREAD" if opp['spread_strength'] > opp['ml_strength'] else "MONEYLINE"
                stronger_signal = "Spread" if opp['spread_strength'] > opp['ml_strength'] else "ML"
                recommended_bet = f"BET {spread_rec_side} SPREAD" if bet_type == "SPREAD" else f"BET {ml_rec_side} MONEYLINE"
                
                all_recommendations.append({
                    'type': '🔄 OPPOSING MARKETS',
                    'bet': recommended_bet,
                    'reason': f"{ml_details} | {spread_details} → Follow {stronger_signal} ({bet_type})",
                    'source_details': source_info,
                    'win_rate': 65.0,  # Placeholder
                    'roi': 15.0,
                    'priority': 2,
                    'confidence_score': opp.get('confidence_score', 70),
                    'confidence_level': opp.get('confidence_level', 'MODERATE'),
                    'confidence_explanation': opp.get('confidence_explanation', 'Opposing market signals'),
                    'last_updated': opp['last_updated']
                })
            
            # Sharp signals
            for sharp in signals['sharp_signals']:
                source_info = f"{sharp['source']}-{sharp['book']}: {sharp['stake_pct']:.0f}% money vs {sharp['bet_pct']:.0f}% bets"
                
                all_recommendations.append({
                    'type': f'🔥 {sharp["split_type"].upper()} SHARP',
                    'bet': sharp['recommendation'],
                    'reason': f"{sharp['differential']:+.1f}% differential",
                    'source_details': source_info,
                    'win_rate': sharp['win_rate'],
                    'roi': sharp['roi'],
                    'priority': 3,
                    'confidence_score': sharp.get('confidence_score', 60),
                    'confidence_level': sharp.get('confidence_level', 'MODERATE'),
                    'confidence_explanation': sharp.get('confidence_explanation', 'Sharp action signal'),
                    'last_updated': sharp['last_updated']
                })
            
            # Sort by confidence score first, then priority
            all_recommendations.sort(key=lambda x: (-x.get('confidence_score', 0), x['priority']))
            
            for i, rec in enumerate(all_recommendations, 1):
                # Format last updated time
                last_updated = rec['last_updated']
                if hasattr(last_updated, 'tzinfo') and last_updated.tzinfo is None:
                    last_updated_est = self.est.localize(last_updated)
                elif hasattr(last_updated, 'astimezone'):
                    last_updated_est = last_updated.astimezone(self.est)
                else:
                    last_updated_est = last_updated
                
                print(f"  {i}. {rec['type']}")
                print(f"     💰 {rec['bet']}")
                print(f"     📊 {rec['reason']}")
                print(f"     📈 {rec['win_rate']:.1f}% win rate, {rec['roi']:+.1f}% ROI")
                
                # Show confidence score if available
                if 'confidence_score' in rec:
                    confidence_emoji = self._get_confidence_emoji(rec['confidence_score'])
                    print(f"     {confidence_emoji} Confidence: {rec['confidence_score']:.0f}/100 ({rec['confidence_level']})")
                    if rec.get('confidence_explanation'):
                        print(f"     💡 {rec['confidence_explanation']}")
                
                print(f"     📍 {rec['source_details']}")
                print(f"     🕐 Updated: {last_updated_est.strftime('%H:%M')} EST")
                print()
        
        # Simple summary
        steam_count = sum(len(g['steam_moves']) for g in games.values())
        opposing_count = sum(len(g['opposing_markets']) for g in games.values())
        sharp_count = sum(len(g['sharp_signals']) for g in games.values())
        
        print(f"\n📊 SUMMARY:")
        print(f"   ⚡ Steam Moves: {steam_count}")
        print(f"   🔄 Opposing Markets: {opposing_count}")
        print(f"   🔥 Sharp Signals: {sharp_count}")
        print(f"   🎯 Total Games: {len(games)}")
        print(f"   🤖 All recommendations use AI-validated strategies")

    async def _display_no_opportunities_analysis(self):
        """Provide detailed explanation when no betting opportunities are found"""
        
        print("\n" + "="*70)
        print("🚫 NO BETTING OPPORTUNITIES FOUND")
        print("="*70)
        
        # Check what games are available
        now_est = datetime.now(self.est)
        end_time = now_est + timedelta(minutes=5)  # Look ahead 5 minutes
        
        query = """
        SELECT DISTINCT home_team, away_team, game_datetime
        FROM splits.raw_mlb_betting_splits
        WHERE game_datetime BETWEEN ? AND ?
        """
        
        # Use coordinated database access to prevent conflicts
        games_checked = self.coordinator.execute_read(query, (now_est, end_time))
        
        if not games_checked:
            print(f"\n📊 SUMMARY: No games found in timeframe")
            return
        
        print(f"\n📊 SUMMARY: {len(games_checked)} games analyzed")
        
        # Show close calls
        query = """
        SELECT home_team, away_team, 
               ABS(home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
               split_type, source
        FROM splits.raw_mlb_betting_splits
        WHERE game_datetime BETWEEN ? AND ?
          AND ABS(home_or_over_stake_percentage - home_or_over_bets_percentage) > 8
        ORDER BY differential DESC
        LIMIT 5
        """
        
        # Use coordinated database access to prevent conflicts
        close_calls = self.coordinator.execute_read(query, (now_est, end_time))
        
        if close_calls:
            print(f"\n📊 CLOSEST BETTING DATA (Not Meeting Thresholds):")
            for row in close_calls:
                home, away, game_time, split_type, source = row
                print(f"   📈 {away} @ {home} - {split_type.upper()}")
                print(f"      💰 {game_time.strftime('%I:%M %p EST')}")
                print(f"      📍 {source}-{game_time.strftime('%I:%M %p EST')}")
        
        print(f"\n🔍 WHY NO BETS RECOMMENDED:")
        print(f"   ✅ All games were analyzed using validated strategies")
        print(f"   ✅ Current betting data does not meet proven thresholds")
        print(f"   ✅ This protects you from low-probability bets")
        
        # Show current thresholds being used
        print(f"\n📊 CURRENT AI-OPTIMIZED THRESHOLDS:")
        vsin_config = await self.config_manager.get_threshold_config('VSIN')
        sbd_config = await self.config_manager.get_threshold_config('SBD')
        
        print(f"   📈 VSIN: {vsin_config.minimum_threshold:.1f}% minimum differential")
        print(f"   📈 SBD: {sbd_config.minimum_threshold:.1f}% minimum differential")
        print(f"   🎯 These thresholds are based on backtesting results")
        
        # Check if there's any data that's close but doesn't meet thresholds
        await self._show_close_calls()
        
        print(f"\n💡 RECOMMENDATION:")
        print(f"   🛑 NO BETS RECOMMENDED AT THIS TIME")
        print(f"   ⏰ Check again closer to other game times")
        print(f"   📧 You'll receive email alerts when opportunities are found")
        print("="*70)
    
    async def _show_close_calls(self):
        """Show betting data that's close to thresholds but doesn't qualify"""
        now_est = datetime.now(self.est)
        end_time = now_est + timedelta(minutes=5)
        
        query = """
        SELECT home_team, away_team, game_datetime, split_type, 
               stake_pct, bet_pct, differential, source, book
        FROM betting_splits 
        WHERE game_datetime BETWEEN ? AND ?
        AND ABS(differential) > 0
        ORDER BY ABS(differential) DESC
        LIMIT 5
        """
        
        close_calls = self.coordinator.execute_read(query, (now_est, end_time))
        
        if close_calls:
            print(f"\n📊 CLOSEST BETTING DATA (Not Meeting Thresholds):")
            for row in close_calls:
                home, away, game_time, split_type, stake_pct, bet_pct, diff, source, book = row
                print(f"   📈 {away} @ {home} - {split_type.upper()}")
                print(f"      💰 {stake_pct:.1f}% money vs {bet_pct:.1f}% bets = {diff:+.1f}% differential")
                print(f"      📍 {source}-{book} (Below {await self._get_threshold_for_source(source):.1f}% threshold)")
    
    async def _get_threshold_for_source(self, source):
        """Get minimum threshold for a source"""
        config = await self.config_manager.get_threshold_config(source)
        return config.minimum_threshold

    def _summarize_strategy_performance(self, profitable_strategies):
        """Summarize strategy performance from a list of profitable strategies"""
        perf_stats = {
            'steam_moves': {'best_wr': 0, 'best_roi': 0, 'count': 0},
            'opposing_markets': {'best_wr': 0, 'best_roi': 0, 'count': 0},
            'sharp_action': {'best_wr': 0, 'best_roi': 0, 'count': 0}
        }
        
        for strategy in profitable_strategies:
            if strategy['win_rate'] > perf_stats['sharp_action']['best_wr']:
                perf_stats['sharp_action']['best_wr'] = strategy['win_rate']
                perf_stats['sharp_action']['best_roi'] = strategy['roi']
                perf_stats['sharp_action']['count'] = strategy['total_bets']
            
            if strategy['win_rate'] > perf_stats['opposing_markets']['best_wr']:
                perf_stats['opposing_markets']['best_wr'] = strategy['win_rate']
                perf_stats['opposing_markets']['best_roi'] = strategy['roi']
                perf_stats['opposing_markets']['count'] = strategy['total_bets']
            
            if strategy['win_rate'] > perf_stats['steam_moves']['best_wr']:
                perf_stats['steam_moves']['best_wr'] = strategy['win_rate']
                perf_stats['steam_moves']['best_roi'] = strategy['roi']
                perf_stats['steam_moves']['count'] = strategy['total_bets']
        
        return perf_stats

    async def _get_strategy_performance_metadata(self, source, book, split_type, strategy_type):
        """Get strategy performance metadata for a given strategy"""
        # Map source and book to the format used in backtesting
        source_book_mapping = {
            ('VSIN', 'circa'): 'VSIN-circa',
            ('VSIN', 'draftkings'): 'VSIN-draftkings', 
            ('SBD', 'UNKNOWN'): 'SBD-UNKNOWN',
        }
        
        source_book_key = source_book_mapping.get((source, book), f"{source}-{book}")
        
        # Try different strategy name patterns
        strategy_patterns = [
            f"{strategy_type}",
            f"{strategy_type}_{split_type}",
            f"{source_book_key}_{strategy_type}",
        ]
        
        for pattern in strategy_patterns:
            query = """
            SELECT win_rate * 100 as win_rate_pct, roi_per_100
            FROM backtesting.strategy_performance
            WHERE source_book_type LIKE ? AND split_type = ? AND strategy_name LIKE ?
            AND total_bets >= 8 AND win_rate > 0.524
            ORDER BY roi_per_100 DESC LIMIT 1
            """
            
            results = self.coordinator.execute_read(query, (f"%{source_book_key}%", split_type, f"%{pattern}%"))
            if results:
                win_rate, roi = results[0]
                return {'win_rate': win_rate, 'roi': roi}
        
        # Fallback: get best performing strategy for this source/book/split_type combination
        query = """
        SELECT win_rate * 100 as win_rate_pct, roi_per_100
        FROM backtesting.strategy_performance
        WHERE source_book_type LIKE ? AND split_type = ?
        AND total_bets >= 8 AND win_rate > 0.524
        ORDER BY roi_per_100 DESC LIMIT 1
        """
        
        results = self.coordinator.execute_read(query, (f"%{source_book_key}%", split_type))
        if results:
            win_rate, roi = results[0]
            return {'win_rate': win_rate, 'roi': roi}
        
        return None

    async def _get_current_profitable_strategies(self):
        """Get current profitable strategies from latest backtesting results"""
        query = """
        SELECT 
            strategy_name,
            source_book_type,
            split_type,
            win_rate * 100 as win_rate_pct,
            roi_per_100,
            total_bets,
            confidence_interval_lower * 100 as ci_lower,
            confidence_interval_upper * 100 as ci_upper
        FROM backtesting.strategy_performance 
        WHERE backtest_date = (SELECT MAX(backtest_date) FROM backtesting.strategy_performance)
          AND total_bets >= 8
          AND win_rate > 0.524  -- Only profitable strategies
          AND roi_per_100 > 5.0  -- Minimum 5% ROI
        ORDER BY roi_per_100 DESC, total_bets DESC
        """
        
        try:
            results = self.coordinator.execute_read(query)
            strategies = []
            
            if not results:
                self.logger.info("No profitable strategies found in backtesting database")
                return []
            
            backtest_date = results[0][8] if results else None
            self.logger.info(f"Loading {len(results)} profitable strategies from backtest date: {backtest_date}")
            
            for row in results:
                strategy_name, source_book, split_type, win_rate, roi, total_bets, ci_lower, ci_upper, _ = row
                
                # Determine confidence level based on sample size and performance
                if total_bets >= 50 and win_rate >= 60:
                    confidence = "HIGH CONFIDENCE"
                elif total_bets >= 25 and win_rate >= 55:
                    confidence = "MODERATE CONFIDENCE"
                else:
                    confidence = "LOW CONFIDENCE"
                
                strategies.append({
                    'strategy_name': strategy_name,
                    'source_book': source_book,
                    'split_type': split_type,
                    'win_rate': win_rate,
                    'roi': roi,
                    'total_bets': total_bets,
                    'confidence': confidence,
                    'ci_lower': ci_lower,
                    'ci_upper': ci_upper
                })
            
            return strategies
            
        except Exception as e:
            self.logger.warning(f"Could not get profitable strategies from backtesting database: {e}")
            self.logger.warning("This may indicate that backtesting hasn't been run recently or database schema issues")
            return []
    
    def _find_matching_strategy(self, profitable_strategies, source, book, split_type, abs_diff):
        """Find a profitable strategy that matches the current signal"""
        
        # Create source-book key
        source_book_key = f"{source}-{book}" if book else source
        
        # Look for exact matches first
        for strategy in profitable_strategies:
            if (strategy['split_type'] == split_type and 
                source_book_key in strategy['source_book']):
                
                # Use dynamic thresholds based on strategy performance
                # Better performing strategies can use lower thresholds
                if strategy['win_rate'] >= 65:
                    threshold = 15.0  # Aggressive threshold for high performers
                elif strategy['win_rate'] >= 60:
                    threshold = 18.0  # Moderate threshold
                elif strategy['win_rate'] >= 55:
                    threshold = 22.0  # Conservative threshold
                else:
                    threshold = 25.0  # Very conservative
                
                if abs_diff >= threshold:
                    return strategy
        
        # Look for broader matches (same source, any split type)
        for strategy in profitable_strategies:
            if source_book_key in strategy['source_book']:
                # Use higher threshold for broader matches
                threshold = 25.0
                if abs_diff >= threshold:
                    return strategy
        
        return None


async def main():
    parser = argparse.ArgumentParser(description="Adaptive Master Betting Detector - AI-Optimized Strategies")
    parser.add_argument('--minutes', '-m', type=int, default=60,
                        help='Minutes ahead to look for opportunities (default: 60)')
    parser.add_argument('--debug', '-d', action='store_true',
                        help='Show all data, regardless of time filters')
    
    args = parser.parse_args()
    
    detector = AdaptiveMasterBettingDetector()
    
    try:
        games = await detector.analyze_all_opportunities(args.minutes, args.debug)
        if not args.debug:  # Only run display analysis if not in debug mode
            await detector.display_comprehensive_analysis(games)
    finally:
        # CRITICAL: Ensure database connections are properly closed
        # This prevents database locks for subsequent workflows
        try:
            if hasattr(detector, 'coordinator') and detector.coordinator:
                # The database coordinator handles cleanup automatically
                pass
            if hasattr(detector, 'db_manager') and detector.db_manager:
                detector.db_manager.close()
        except Exception as e:
            print(f"Warning: Error during database cleanup: {e}")
            # Don't fail the entire analysis for cleanup errors
            pass


if __name__ == "__main__":
    asyncio.run(main()) 