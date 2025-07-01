"""
Betting Signal Repository - Centralized Data Access Layer

Extracts all database query logic from the main detector class
for better separation of concerns and testability.

🚀 ENHANCED: Added intelligent caching and batch data retrieval to eliminate
redundant database calls when multiple processors need similar data.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Set
import pytz
from dataclasses import asdict
import asyncio
from functools import lru_cache
import hashlib
import json

from ..models.betting_analysis import SignalProcessorConfig, ProfitableStrategy
from ..services.database_coordinator import get_database_coordinator
from ..core.logging import get_logger


class BettingSignalRepository:
    """
    Centralized repository for all betting signal database queries
    
    🚀 ENHANCED: Added intelligent caching and batch data retrieval
    """
    
    def __init__(self, config: SignalProcessorConfig):
        self.coordinator = get_database_coordinator()
        self.config = config
        self.logger = get_logger(__name__)
        self.est = pytz.timezone('US/Eastern')
        
        # 🚀 PERFORMANCE: Add intelligent caching to reduce database calls
        self._data_cache = {}
        self._cache_timestamps = {}
        self._cache_ttl = 300  # 5 minutes cache TTL
        self._batch_requests = set()  # Track what's been requested in current batch
        
        # Track repository usage for optimization
        self._call_stats = {
            'cache_hits': 0,
            'cache_misses': 0,
            'database_calls': 0,
            'batch_optimizations': 0
        }
    
    def _get_cache_key(self, method_name: str, *args) -> str:
        """Generate cache key for method call with parameters"""
        # Create deterministic cache key from method name and arguments
        key_data = {
            'method': method_name,
            'args': [str(arg) for arg in args],
            'config_hash': hash(str(self.config))
        }
        return hashlib.md5(json.dumps(key_data, sort_keys=True).encode()).hexdigest()
    
    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached data is still valid"""
        if cache_key not in self._cache_timestamps:
            return False
        
        cache_time = self._cache_timestamps[cache_key]
        return (datetime.now() - cache_time).total_seconds() < self._cache_ttl
    
    def _get_from_cache(self, cache_key: str) -> Optional[List[Dict]]:
        """Get data from cache if valid"""
        if self._is_cache_valid(cache_key) and cache_key in self._data_cache:
            self._call_stats['cache_hits'] += 1
            self.logger.debug(f"📋 Cache HIT for key: {cache_key[:8]}...")
            return self._data_cache[cache_key]
        
        self._call_stats['cache_misses'] += 1
        return None
    
    def _store_in_cache(self, cache_key: str, data: List[Dict]) -> None:
        """Store data in cache"""
        self._data_cache[cache_key] = data
        self._cache_timestamps[cache_key] = datetime.now()
        self.logger.debug(f"💾 Cached {len(data)} records for key: {cache_key[:8]}...")
    
    async def get_sharp_signal_data(self, start_time: datetime, end_time: datetime) -> List[Dict]:
        """
        Get raw sharp signal data for analysis
        
        🚀 ENHANCED: Added intelligent caching to reduce database calls
        """
        # Check cache first
        cache_key = self._get_cache_key('get_sharp_signal_data', start_time, end_time)
        cached_data = self._get_from_cache(cache_key)
        if cached_data is not None:
            return cached_data
        
        # 🚀 PERFORMANCE LOG: Track database calls
        self._call_stats['database_calls'] += 1
        self.logger.info(f"🗄️  Fetching sharp signal data from database (time window: {start_time.strftime('%H:%M')} to {end_time.strftime('%H:%M')})")
        
        query = """
        WITH valid_splits AS (
            SELECT 
                home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, book, game_datetime, last_updated
            FROM splits.raw_mlb_betting_splits
            WHERE game_datetime BETWEEN %s AND %s
              AND home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND game_datetime IS NOT NULL
              AND NOT (home_or_over_stake_percentage = 0 AND home_or_over_bets_percentage = 0)
              AND ABS(home_or_over_stake_percentage - home_or_over_bets_percentage) >= %s
              AND last_updated >= NOW() - INTERVAL '24 hours'
        ),
        latest_splits AS (
            SELECT 
                home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                differential, source, book, game_datetime, last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY home_team, away_team, game_datetime, split_type, source, COALESCE(book, 'UNKNOWN')
                    ORDER BY last_updated DESC
                ) as rn
            FROM valid_splits
        )
        SELECT home_team, away_team, split_type, split_value, 
               home_or_over_stake_percentage, home_or_over_bets_percentage,
               differential, source, book, game_datetime, last_updated
        FROM latest_splits
        WHERE rn = 1
        ORDER BY ABS(differential) DESC
        """
        
        data = self.coordinator.execute_read(query, (
            start_time, end_time, 
            self.config.minimum_differential
        ))
        
        # Store in cache and return
        self._store_in_cache(cache_key, data)
        self.logger.info(f"📊 Retrieved {len(data)} sharp signal records from database")
        return data
    
    async def get_batch_signal_data(self, start_time: datetime, end_time: datetime, 
                                  signal_types: Set[str] = None) -> Dict[str, List[Dict]]:
        """
        🚀 NEW: Batch data retrieval to reduce database round trips
        
        Retrieves multiple signal types in a single optimized query when possible.
        This eliminates redundant database calls when multiple processors need data.
        """
        if signal_types is None:
            signal_types = {'sharp_action', 'opposing_markets', 'book_conflicts', 'steam_moves'}
        
        # Check if we can serve any requests from cache
        batch_cache_key = self._get_cache_key('get_batch_signal_data', start_time, end_time, sorted(signal_types))
        cached_batch = self._get_from_cache(batch_cache_key)
        if cached_batch is not None:
            self._call_stats['batch_optimizations'] += 1
            self.logger.info(f"🚀 Serving {len(signal_types)} signal types from batch cache")
            return cached_batch
        
        self.logger.info(f"🔄 Fetching batch data for {len(signal_types)} signal types: {signal_types}")
        self._call_stats['database_calls'] += 1
        self._call_stats['batch_optimizations'] += 1
        
        # Single optimized query for all signal types
        query = """
        WITH valid_splits AS (
            SELECT 
                home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, book, game_datetime, last_updated
            FROM splits.raw_mlb_betting_splits
            WHERE game_datetime BETWEEN %s AND %s
              AND home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND game_datetime IS NOT NULL
              AND NOT (home_or_over_stake_percentage = 0 AND home_or_over_bets_percentage = 0)
              AND last_updated >= NOW() - INTERVAL '24 hours'
        ),
        latest_splits AS (
            SELECT 
                home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                differential, source, book, game_datetime, last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY home_team, away_team, game_datetime, split_type, source, COALESCE(book, 'UNKNOWN')
                    ORDER BY last_updated DESC
                ) as rn
            FROM valid_splits
        ),
        clean_data AS (
            SELECT * FROM latest_splits WHERE rn = 1
        )
        SELECT 
            home_team, away_team, split_type, split_value,
            home_or_over_stake_percentage, home_or_over_bets_percentage,
            differential, source, book, game_datetime, last_updated,
            ABS(differential) as abs_differential,
            CASE WHEN differential > 0 THEN home_team ELSE away_team END as recommended_team
        FROM clean_data
        WHERE ABS(differential) >= %s
        ORDER BY split_type, ABS(differential) DESC
        """
        
        all_data = self.coordinator.execute_read(query, (
            start_time, end_time, self.config.minimum_differential
        ))
        
        # Organize data by signal type
        batch_result = {}
        
        # Sharp action data (all signals with sufficient differential)
        if 'sharp_action' in signal_types:
            batch_result['sharp_action'] = [
                row for row in all_data 
                if abs(row['differential']) >= self.config.minimum_differential
            ]
        
        # Book conflicts (where we have multiple books for same game/market)
        if 'book_conflicts' in signal_types:
            book_groups = {}
            for row in all_data:
                if row['book']:  # Only process rows with book information
                    key = f"{row['home_team']}-{row['away_team']}-{row['split_type']}-{row['game_datetime']}"
                    if key not in book_groups:
                        book_groups[key] = []
                    book_groups[key].append(row)
            
            # Find conflicts (opposing signals from different books)
            conflicts = []
            for key, group in book_groups.items():
                if len(group) >= 2:
                    # Check for opposing recommendations
                    rec_teams = set(row['recommended_team'] for row in group)
                    if len(rec_teams) > 1:  # Conflicting recommendations
                        conflicts.extend(group)
            
            batch_result['book_conflicts'] = conflicts
        
        # Opposing markets (ML vs Spread conflicts)
        if 'opposing_markets' in signal_types:
            game_groups = {}
            for row in all_data:
                if row['split_type'] in ['moneyline', 'spread']:
                    key = f"{row['home_team']}-{row['away_team']}-{row['game_datetime']}-{row['source']}-{row['book']}"
                    if key not in game_groups:
                        game_groups[key] = {}
                    game_groups[key][row['split_type']] = row
            
            opposing_markets = []
            for key, markets in game_groups.items():
                if 'moneyline' in markets and 'spread' in markets:
                    ml_rec = markets['moneyline']['recommended_team']
                    sp_rec = markets['spread']['recommended_team'] 
                    if ml_rec != sp_rec:  # Opposing recommendations
                        opposing_markets.extend([markets['moneyline'], markets['spread']])
            
            batch_result['opposing_markets'] = opposing_markets
        
        # Steam moves (rapid line movement indicators)
        if 'steam_moves' in signal_types:
            batch_result['steam_moves'] = [
                row for row in all_data 
                if abs(row['differential']) >= 15  # Higher threshold for steam moves
            ]
        
        # Cache the batch result
        self._store_in_cache(batch_cache_key, batch_result)
        
        total_records = sum(len(data) for data in batch_result.values())
        self.logger.info(f"📊 Batch retrieval complete: {total_records} total records across {len(signal_types)} signal types")
        
        return batch_result
    
    def get_repository_stats(self) -> Dict[str, any]:
        """
        🚀 NEW: Get repository performance statistics
        """
        cache_hit_rate = (
            self._call_stats['cache_hits'] / 
            (self._call_stats['cache_hits'] + self._call_stats['cache_misses'])
        ) if (self._call_stats['cache_hits'] + self._call_stats['cache_misses']) > 0 else 0
        
        return {
            **self._call_stats,
            'cache_hit_rate_pct': round(cache_hit_rate * 100, 1),
            'cached_items': len(self._data_cache),
            'cache_ttl_seconds': self._cache_ttl,
            'efficiency_rating': 'HIGH' if cache_hit_rate > 0.7 else 'MEDIUM' if cache_hit_rate > 0.4 else 'LOW'
        }
    
    def clear_cache(self) -> None:
        """Clear repository cache (useful for testing or forced refresh)"""
        cleared_items = len(self._data_cache)
        self._data_cache.clear()
        self._cache_timestamps.clear()
        self.logger.info(f"🧹 Cleared repository cache ({cleared_items} items)")
    
    def reset_stats(self) -> None:
        """Reset repository statistics"""
        self._call_stats = {
            'cache_hits': 0,
            'cache_misses': 0,
            'database_calls': 0,
            'batch_optimizations': 0
        }
    
    async def get_opposing_markets_data(self, start_time: datetime, end_time: datetime) -> List[Dict]:
        """Get opposing markets data where ML and spread signals conflict"""
        query = """
        WITH latest_splits AS (
            SELECT 
                home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, book, game_datetime, last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY home_team, away_team, game_datetime, split_type, source, COALESCE(book, 'UNKNOWN')
                    ORDER BY last_updated DESC
                ) as rn
            FROM splits.raw_mlb_betting_splits
            WHERE game_datetime BETWEEN %s AND %s
              AND home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND split_type IN ('moneyline', 'spread')
              AND NOT (home_or_over_stake_percentage = 0 AND home_or_over_bets_percentage = 0)
              AND last_updated >= NOW() - INTERVAL '24 hours'
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
            FROM clean_splits 
            WHERE split_type = 'moneyline'
              AND ABS(differential) >= 5
              AND ABS(differential) <= %s
        ),
        spread_signals AS (
            SELECT 
                home_team, away_team, game_datetime, source, book,
                differential as spread_diff, 
                home_or_over_stake_percentage as spread_stake_pct, 
                home_or_over_bets_percentage as spread_bet_pct,
                CASE WHEN differential > 0 THEN home_team ELSE away_team END as spread_rec_team,
                ABS(differential) as spread_strength, last_updated
            FROM clean_splits 
            WHERE split_type = 'spread'
              AND ABS(differential) >= 8
              AND ABS(differential) <= %s
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
            (100 - ml.ml_stake_pct) as ml_opposing_stake_pct,
            (100 - ml.ml_bet_pct) as ml_opposing_bet_pct,
            (100 - sp.spread_stake_pct) as spread_opposing_stake_pct,
            (100 - sp.spread_bet_pct) as spread_opposing_bet_pct
        FROM ml_signals ml
        INNER JOIN spread_signals sp ON ml.home_team = sp.home_team AND ml.away_team = sp.away_team 
            AND ml.game_datetime = sp.game_datetime AND ml.source = sp.source AND ml.book = sp.book
        WHERE ml.ml_rec_team != sp.spread_rec_team
        ORDER BY combined_strength DESC
        """
        
        return self.coordinator.execute_read(query, (
            start_time, end_time,
            self.config.maximum_differential,
            self.config.maximum_differential
        ))
    
    async def get_steam_move_data(self, start_time: datetime, end_time: datetime) -> List[Dict]:
        """Get potential steam move data"""
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
          AND game_datetime BETWEEN %s AND %s
          AND last_updated >= NOW() - INTERVAL '24 hours'
        ORDER BY game_datetime ASC, ABS(home_or_over_stake_percentage - home_or_over_bets_percentage) DESC
        """
        
        return self.coordinator.execute_read(query, (
            start_time, end_time
        ))
    
    async def get_book_conflict_data(self, start_time: datetime, end_time: datetime) -> List[Dict]:
        """Get book conflict data where different books show opposing signals"""
        query = """
        WITH valid_splits AS (
            SELECT 
                home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, book, game_datetime, last_updated
            FROM splits.raw_mlb_betting_splits
            WHERE game_datetime BETWEEN %s AND %s
              AND home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND game_datetime IS NOT NULL
              AND book IS NOT NULL
              AND NOT (home_or_over_stake_percentage = 0 AND home_or_over_bets_percentage = 0)
              AND ABS(home_or_over_stake_percentage - home_or_over_bets_percentage) >= %s
              AND last_updated >= NOW() - INTERVAL '24 hours'
        ),
        latest_splits AS (
            SELECT 
                home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                differential, source, book, game_datetime, last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY home_team, away_team, game_datetime, split_type, source, book
                    ORDER BY last_updated DESC
                ) as rn
            FROM valid_splits
        ),
        book_comparisons AS (
            SELECT 
                a.home_team, a.away_team, a.split_type, a.game_datetime,
                a.source, a.book as book_a, a.differential as diff_a,
                a.home_or_over_stake_percentage as stake_a, a.home_or_over_bets_percentage as bet_a,
                b.book as book_b, b.differential as diff_b,
                b.home_or_over_stake_percentage as stake_b, b.home_or_over_bets_percentage as bet_b,
                ABS(a.differential - b.differential) as conflict_strength,
                SIGN(a.differential) != SIGN(b.differential) as opposing_directions,
                a.last_updated
            FROM latest_splits a
            JOIN latest_splits b ON a.home_team = b.home_team 
                AND a.away_team = b.away_team
                AND a.game_datetime = b.game_datetime
                AND a.split_type = b.split_type
                AND a.source = b.source
                AND a.book != b.book
            WHERE a.rn = 1 AND b.rn = 1
        )
        SELECT *
        FROM book_comparisons
        WHERE opposing_directions = true
        ORDER BY conflict_strength DESC
        """
        
        return self.coordinator.execute_read(query, (
            start_time, end_time,
            self.config.book_conflict_minimum_strength
        ))
    
    async def get_profitable_strategies(self) -> List[ProfitableStrategy]:
        """Get all profitable strategies from backtesting results"""
        query = """
        SELECT 
            strategy_name,
            source_book_type,
            split_type,
            win_rate * 100 as win_rate_pct,
            roi_per_100,
            total_bets,
            confidence_level,
            backtest_date
        FROM backtesting.strategy_performance 
        WHERE backtest_date = (SELECT MAX(backtest_date) FROM backtesting.strategy_performance)
          AND total_bets >= 5
          AND (
            (roi_per_100 >= 20.0) OR
            (roi_per_100 >= 15.0 AND total_bets >= 8) OR
            (roi_per_100 >= 10.0 AND win_rate >= 0.45) OR
            (roi_per_100 >= 5.0 AND win_rate >= 0.55 AND total_bets >= 10) OR
            (total_bets >= 20 AND roi_per_100 > 0.0)
          )
        ORDER BY roi_per_100 DESC, total_bets DESC
        """
        
        try:
            results = self.coordinator.execute_read(query)
            strategies = []
            
            for row in results:
                import math
                win_rate = float(row['win_rate_pct'])
                total_bets = row['total_bets']
                
                # Calculate confidence intervals
                if total_bets > 0:
                    p = win_rate / 100.0
                    n = total_bets
                    margin_of_error = 1.96 * math.sqrt((p * (1 - p)) / n) * 100
                    ci_lower = max(0, win_rate - margin_of_error)
                    ci_upper = min(100, win_rate + margin_of_error)
                else:
                    ci_lower = ci_upper = win_rate
                
                # Determine confidence level
                confidence_level = row['confidence_level']
                if confidence_level:
                    confidence = confidence_level.upper() + " CONFIDENCE"
                elif total_bets >= 50 and win_rate >= 60:
                    confidence = "HIGH CONFIDENCE"
                elif total_bets >= 25 and win_rate >= 55:
                    confidence = "MODERATE CONFIDENCE"
                else:
                    confidence = "LOW CONFIDENCE"
                
                strategies.append(ProfitableStrategy(
                    strategy_name=row['strategy_name'],
                    source_book=row['source_book_type'],
                    split_type=row['split_type'],
                    win_rate=win_rate,
                    roi=float(row['roi_per_100']),
                    total_bets=total_bets,
                    confidence=confidence,
                    ci_lower=ci_lower,
                    ci_upper=ci_upper
                ))
            
            return strategies
            
        except Exception as e:
            self.logger.error(f"Could not get profitable strategies: {e}")
            return []
    
    async def get_moneyline_odds(self, home_team: str, away_team: str) -> Optional[str]:
        """Get current moneyline odds for juice filtering"""
        query = """
        SELECT split_value 
        FROM splits.raw_mlb_betting_splits 
        WHERE home_team = %s AND away_team = %s AND split_type = 'moneyline'
        ORDER BY last_updated DESC LIMIT 1
        """
        
        results = self.coordinator.execute_read(query, (home_team, away_team))
        return results[0]['split_value'] if results else None
    
    async def get_database_stats(self) -> Dict[str, int]:
        """Get database statistics for debugging"""
        try:
            # Total records
            total_query = "SELECT COUNT(*) FROM splits.raw_mlb_betting_splits"
            total_result = self.coordinator.execute_read(total_query)
            total_count = total_result[0]['count'] if total_result else 0
            
            # Recent records
            recent_query = """
                SELECT COUNT(*) FROM splits.raw_mlb_betting_splits 
                WHERE last_updated > NOW() - INTERVAL '24 hours'
            """
            recent_result = self.coordinator.execute_read(recent_query)
            recent_count = recent_result[0]['count'] if recent_result else 0
            
            return {
                'total_records': total_count,
                'recent_records': recent_count
            }
        except Exception as e:
            self.logger.error(f"Error getting database stats: {e}")
            return {'total_records': 0, 'recent_records': 0}
    
    async def get_actionable_games_count(self, start_time: datetime, end_time: datetime) -> int:
        """Get count of games with actionable signals in the time window"""
        query = """
        SELECT COUNT(DISTINCT CONCAT(home_team, '|', away_team, '|', game_datetime))
        FROM splits.raw_mlb_betting_splits
        WHERE game_datetime BETWEEN %s AND %s
          AND home_or_over_stake_percentage IS NOT NULL 
          AND home_or_over_bets_percentage IS NOT NULL
          AND ABS(home_or_over_stake_percentage - home_or_over_bets_percentage) >= %s
        """
        
        result = self.coordinator.execute_read(query, (start_time, end_time, self.config.minimum_differential))
        return result[0]['count'] if result else 0

    # New methods for strategy processors
    
    async def get_moneyline_splits(self, start_time: datetime, end_time: datetime) -> List[Dict]:
        """Get moneyline splits data with enhanced filtering"""
        query = """
        WITH latest_ml_splits AS (
            SELECT 
                game_id, home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, COALESCE(book, 'UNKNOWN') as book, game_datetime, last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY game_id, source, COALESCE(book, 'UNKNOWN')
                    ORDER BY last_updated DESC
                ) as rn
            FROM splits.raw_mlb_betting_splits
            WHERE game_datetime BETWEEN %s AND %s
              AND split_type = 'moneyline'
              AND home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND last_updated >= NOW() - INTERVAL '24 hours'
        )
        SELECT game_id, home_team, away_team, split_type, split_value,
               home_or_over_stake_percentage, home_or_over_bets_percentage,
               differential, source, book, game_datetime, last_updated
        FROM latest_ml_splits
        WHERE rn = 1
        ORDER BY ABS(differential) DESC
        """
        
        return self.coordinator.execute_read(query, (
            start_time, end_time
        ))
    
    async def get_spread_splits(self, start_time: datetime, end_time: datetime) -> List[Dict]:
        """Get spread splits data with enhanced filtering"""
        query = """
        WITH latest_spread_splits AS (
            SELECT 
                game_id, home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, COALESCE(book, 'UNKNOWN') as book, game_datetime, last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY game_id, source, COALESCE(book, 'UNKNOWN')
                    ORDER BY last_updated DESC
                ) as rn
            FROM splits.raw_mlb_betting_splits
            WHERE game_datetime BETWEEN %s AND %s
              AND split_type = 'spread'
              AND home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND split_value IS NOT NULL  -- Ensure we have spread values
              AND last_updated >= NOW() - INTERVAL '24 hours'
        )
        SELECT game_id, home_team, away_team, split_type, split_value,
               home_or_over_stake_percentage, home_or_over_bets_percentage,
               differential, source, book, game_datetime, last_updated
        FROM latest_spread_splits
        WHERE rn = 1
          AND ABS(differential) >= %s
        ORDER BY ABS(differential) DESC
        """
        
        return self.coordinator.execute_read(query, (
            start_time, end_time, 
            self.config.minimum_differential
        ))
    
    async def get_multi_book_data(self, start_time: datetime, end_time: datetime) -> List[Dict]:
        """Get multi-book data for conflict detection"""
        query = """
        WITH latest_splits AS (
            SELECT 
                game_id, home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, book, game_datetime, last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY game_id, split_type, source, book
                    ORDER BY last_updated DESC
                ) as rn
            FROM splits.raw_mlb_betting_splits
            WHERE game_datetime BETWEEN %s AND %s
              AND home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND book IS NOT NULL
              AND last_updated >= NOW() - INTERVAL '24 hours'
        )
        SELECT game_id, home_team, away_team, split_type, split_value,
               home_or_over_stake_percentage, home_or_over_bets_percentage,
               differential, source, book, game_datetime, last_updated
        FROM latest_splits
        WHERE rn = 1
        ORDER BY game_datetime, split_type, ABS(differential) DESC
        """
        
        return self.coordinator.execute_read(query, (
            start_time, end_time
        ))
    
    async def get_public_betting_data(self, start_time: datetime, end_time: datetime) -> List[Dict]:
        """Get public betting data for fade opportunities"""
        query = """
        WITH latest_splits AS (
            SELECT 
                game_id, home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage, home_or_over_bets_percentage,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, COALESCE(book, 'UNKNOWN') as book, game_datetime, last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY game_id, split_type, source, COALESCE(book, 'UNKNOWN')
                    ORDER BY last_updated DESC
                ) as rn
            FROM splits.raw_mlb_betting_splits
            WHERE game_datetime BETWEEN %s AND %s
              AND home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND EXTRACT('epoch' FROM (game_datetime - last_updated)) / 60 BETWEEN -5 AND 45
        )
        SELECT game_id, home_team, away_team, split_type, split_value,
               home_or_over_stake_percentage, home_or_over_bets_percentage,
               differential, source, book, game_datetime, last_updated,
               -- Public betting indicators (calculated but not filtered)
               CASE 
                   WHEN home_or_over_bets_percentage > 70 THEN 'HEAVY_PUBLIC_HOME_OVER'
                   WHEN home_or_over_bets_percentage < 30 THEN 'HEAVY_PUBLIC_AWAY_UNDER'
                   ELSE 'BALANCED'
               END as public_tendency,
               -- Fade opportunity strength (calculated but not filtered)
               CASE 
                   WHEN home_or_over_bets_percentage > 75 OR home_or_over_bets_percentage < 25 THEN 'HIGH_FADE'
                   WHEN home_or_over_bets_percentage > 70 OR home_or_over_bets_percentage < 30 THEN 'MODERATE_FADE'
                   ELSE 'LOW_FADE'
               END as fade_strength
        FROM latest_splits
        WHERE rn = 1
        ORDER BY ABS(differential) DESC
        """
        
        return self.coordinator.execute_read(query, (
            start_time, end_time
        ))

    async def get_consensus_signal_data(self, start_time: datetime, end_time: datetime) -> List[Dict]:
        """Get consensus signal data for consensus processor"""  
        query = """
        WITH latest_moneyline AS (
            SELECT 
                home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage as money_pct,
                home_or_over_bets_percentage as bet_pct,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, book, game_datetime, last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY home_team, away_team, game_datetime, source, COALESCE(book, 'UNKNOWN')
                    ORDER BY last_updated DESC
                ) as rn
            FROM splits.raw_mlb_betting_splits
            WHERE game_datetime BETWEEN %s AND %s
              AND split_type = 'moneyline'
              AND home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND game_datetime IS NOT NULL
              AND NOT (home_or_over_stake_percentage = 0 AND home_or_over_bets_percentage = 0)
              AND last_updated >= NOW() - INTERVAL '24 hours'
        )
        SELECT home_team, away_team, split_type, split_value,
               money_pct, bet_pct, differential, source, book, 
               game_datetime, last_updated
        FROM latest_moneyline
        WHERE rn = 1
        ORDER BY ABS(differential) DESC
        """
        
        return self.coordinator.execute_read(query, (start_time, end_time))

    async def get_underdog_value_data(self, start_time: datetime, end_time: datetime) -> List[Dict]:
        """Get underdog value data for underdog value processor"""
        query = """
        WITH latest_moneyline AS (
            SELECT 
                home_team, away_team, split_type, split_value,
                home_or_over_stake_percentage as home_stake_pct,
                home_or_over_bets_percentage as home_bet_pct,
                (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
                source, book, game_datetime, last_updated,
                ROW_NUMBER() OVER (
                    PARTITION BY home_team, away_team, game_datetime, source, COALESCE(book, 'UNKNOWN')
                    ORDER BY last_updated DESC
                ) as rn
            FROM splits.raw_mlb_betting_splits
            WHERE game_datetime BETWEEN %s AND %s
              AND split_type = 'moneyline' 
              AND home_or_over_stake_percentage IS NOT NULL 
              AND home_or_over_bets_percentage IS NOT NULL
              AND game_datetime IS NOT NULL
              AND split_value IS NOT NULL
              AND split_value != '{}'
              AND NOT (home_or_over_stake_percentage = 0 AND home_or_over_bets_percentage = 0)
              AND last_updated >= NOW() - INTERVAL '24 hours'
        )
        SELECT home_team, away_team, split_type, split_value,
               home_stake_pct, home_bet_pct, differential, source, book,
               game_datetime, last_updated
        FROM latest_moneyline
        WHERE rn = 1
        ORDER BY ABS(differential) DESC
        """
        
        return self.coordinator.execute_read(query, (start_time, end_time))

    async def get_line_movement_data(self, start_time: datetime, end_time: datetime) -> List[Dict]:
        """Get line movement data for movement analysis"""
        query = """
        SELECT 
            home_team, away_team, split_type, split_value,
            home_or_over_stake_percentage as stake_pct,
            home_or_over_bets_percentage as bet_pct,
            (home_or_over_stake_percentage - home_or_over_bets_percentage) as differential,
            source, book, game_datetime, last_updated,
            -- Add movement indicators
            CASE 
                WHEN split_type = 'spread' AND split_value IS NOT NULL THEN split_value
                ELSE NULL
            END as line_value
        FROM splits.raw_mlb_betting_splits
        WHERE game_datetime BETWEEN %s AND %s
          AND home_or_over_stake_percentage IS NOT NULL 
          AND home_or_over_bets_percentage IS NOT NULL
          AND game_datetime IS NOT NULL
          AND last_updated >= NOW() - INTERVAL '48 hours'  -- Longer window for movement tracking
        ORDER BY home_team, away_team, game_datetime, split_type, source, book, last_updated ASC
        """
        
        return self.coordinator.execute_read(query, (start_time, end_time))

    async def get_hybrid_sharp_data(self, start_time: datetime, end_time: datetime) -> List[Dict]:
        """Get hybrid sharp data with line movement calculations"""
        # Ensure minimum_differential has a default value
        min_diff = getattr(self.config, 'minimum_differential', 10.0)
        
        query = """
        WITH comprehensive_data AS (
            SELECT 
                rmbs.game_id,
                rmbs.source,
                COALESCE(rmbs.book, 'UNKNOWN') as book,
                rmbs.split_type,
                rmbs.home_team,
                rmbs.away_team,
                rmbs.game_datetime,
                rmbs.last_updated,
                rmbs.split_value,
                rmbs.home_or_over_stake_percentage as stake_pct,
                rmbs.home_or_over_bets_percentage as bet_pct,
                rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage as differential,
                
                -- Extract line values from JSON using PostgreSQL JSONB operators with safe casting
                CASE 
                    WHEN rmbs.split_type = 'moneyline' AND rmbs.split_value LIKE '{%}' THEN
                        CASE WHEN (rmbs.split_value::JSONB->>'home') ~ '^-?[0-9]+\.?[0-9]*$' 
                             THEN (rmbs.split_value::JSONB->>'home')::DOUBLE PRECISION 
                             ELSE NULL END
                    WHEN rmbs.split_type IN ('spread', 'total') AND rmbs.split_value ~ '^-?[0-9]+\.?[0-9]*$' THEN
                        rmbs.split_value::DOUBLE PRECISION
                    ELSE NULL
                END as line_value,
                
                -- Calculate hours before game
                EXTRACT('epoch' FROM (rmbs.game_datetime - rmbs.last_updated)) / 3600 AS hours_before_game,
                
                -- Sharp action indicators
                CASE 
                    WHEN ABS(rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage) >= 20 THEN 'PREMIUM_SHARP'
                    WHEN ABS(rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage) >= 15 THEN 'STRONG_SHARP'
                    WHEN ABS(rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage) >= 10 THEN 'MODERATE_SHARP'
                    WHEN ABS(rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage) >= 5 THEN 'WEAK_SHARP'
                    ELSE 'NO_SHARP_ACTION'
                END || 
                CASE 
                    WHEN rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage > 5 THEN '_HOME_OVER'
                    WHEN rmbs.home_or_over_stake_percentage - rmbs.home_or_over_bets_percentage < -5 THEN '_AWAY_UNDER'
                    ELSE ''
                END as sharp_indicator
                
            FROM splits.raw_mlb_betting_splits rmbs
            JOIN public.game_outcomes go ON rmbs.game_id = go.game_id
            WHERE rmbs.game_datetime BETWEEN %s AND %s
              AND rmbs.last_updated < rmbs.game_datetime
              AND rmbs.split_value IS NOT NULL
              AND rmbs.game_datetime IS NOT NULL
              AND rmbs.home_or_over_stake_percentage IS NOT NULL
              AND rmbs.home_or_over_bets_percentage IS NOT NULL
              AND go.home_score IS NOT NULL
              AND go.away_score IS NOT NULL
        ),
        
        line_movement_data AS (
            SELECT 
                game_id, source, book, split_type, home_team, away_team, game_datetime,
                differential, stake_pct, bet_pct, sharp_indicator,
                line_value,
                FIRST_VALUE(line_value) OVER (
                    PARTITION BY game_id, source, book, split_type 
                    ORDER BY last_updated ASC
                ) as opening_line,
                LAST_VALUE(line_value) OVER (
                    PARTITION BY game_id, source, book, split_type 
                    ORDER BY last_updated ASC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) as closing_line,
                last_updated,
                ROW_NUMBER() OVER (PARTITION BY game_id, source, book, split_type ORDER BY last_updated DESC) as rn
            FROM comprehensive_data
            WHERE line_value IS NOT NULL
        )
        
        SELECT 
            game_id, source, book, split_type, home_team, away_team, game_datetime,
            opening_line, closing_line, 
            COALESCE(closing_line - opening_line, 0) as line_movement,
            differential, stake_pct, bet_pct, sharp_indicator,
            
            -- Movement classification
            CASE 
                WHEN split_type = 'moneyline' THEN
                    CASE WHEN ABS(COALESCE(closing_line - opening_line, 0)) >= 20 THEN 'SIGNIFICANT'
                         WHEN ABS(COALESCE(closing_line - opening_line, 0)) >= 10 THEN 'MODERATE'
                         WHEN ABS(COALESCE(closing_line - opening_line, 0)) > 0 THEN 'MINOR'
                         ELSE 'NONE' END
                ELSE
                    CASE WHEN ABS(COALESCE(closing_line - opening_line, 0)) >= 1.0 THEN 'SIGNIFICANT'
                         WHEN ABS(COALESCE(closing_line - opening_line, 0)) >= 0.5 THEN 'MODERATE'
                         WHEN ABS(COALESCE(closing_line - opening_line, 0)) > 0 THEN 'MINOR'
                         ELSE 'NONE' END
            END as movement_significance,
            
            -- Simplified hybrid strategy classification
            CASE 
                WHEN ABS(differential) >= 20 AND sharp_indicator LIKE 'PREMIUM_SHARP_%' THEN 'PREMIUM_SHARP_ACTION'
                WHEN ABS(differential) >= 15 AND sharp_indicator LIKE 'STRONG_SHARP_%' THEN 'STRONG_SHARP_ACTION'
                WHEN ABS(differential) >= 10 THEN 'MODERATE_SHARP_ACTION'
                ELSE 'WEAK_OR_NO_SIGNAL'
            END as hybrid_strategy_type
            
        FROM line_movement_data
        WHERE rn = 1 
          AND opening_line IS NOT NULL 
          AND closing_line IS NOT NULL
          AND ABS(differential) >= %s
        ORDER BY ABS(differential) DESC, ABS(COALESCE(closing_line - opening_line, 0)) DESC
        LIMIT 1000
        """
        
        try:
            return self.coordinator.execute_read(query, (start_time, end_time, min_diff))
        except Exception as e:
            # Log the error but return empty list to prevent processor failure
            self.logger.error(f"Failed to get hybrid sharp data: {e}")
            return [] 