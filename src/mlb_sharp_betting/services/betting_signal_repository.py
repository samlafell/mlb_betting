"""
Betting Signal Repository - Centralized Data Access Layer

Extracts all database query logic from the main detector class
for better separation of concerns and testability.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import pytz
from dataclasses import asdict

from ..models.betting_analysis import SignalProcessorConfig, ProfitableStrategy
from ..services.database_coordinator import get_database_coordinator
from ..core.logging import get_logger


class BettingSignalRepository:
    """Centralized repository for all betting signal database queries"""
    
    def __init__(self, config: SignalProcessorConfig):
        self.coordinator = get_database_coordinator()
        self.config = config
        self.logger = get_logger(__name__)
        self.est = pytz.timezone('US/Eastern')
    
    async def get_sharp_signal_data(self, start_time: datetime, end_time: datetime) -> List[Dict]:
        """Get raw sharp signal data for analysis"""
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
        
        return self.coordinator.execute_read(query, (
            start_time, end_time, 
            self.config.minimum_differential
        ))
    
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
              AND last_updated >= NOW() - INTERVAL '24 hours'
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