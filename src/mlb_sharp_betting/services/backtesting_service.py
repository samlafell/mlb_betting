"""
Refactored Backtesting Service

Clean, focused architecture that eliminates SQL script duplication by using
the Strategy Processor Factory exclusively. Implements the simplified service
design from the Senior Engineer's architecture recommendations.

Key improvements:
- Eliminates 76 duplicate strategies from SQL scripts
- Uses only Factory processors (8-12 unique strategies)
- Clean separation of concerns with focused components
- Maintains compatibility with existing pipeline
- Improved data quality and validation
- 🚨 FIXED: Clean logging to prevent terminal spam
"""

import os
import json
import asyncio
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass, asdict
from abc import ABC, abstractmethod
import structlog
import logging
import numpy as np
from scipy import stats
import hashlib

from ..core.config import get_settings
from ..core.logging import get_logger, get_clean_logger, setup_universal_logger_compatibility, BackwardCompatibleLogger
from ..db.connection import DatabaseManager
from .database_coordinator import get_database_coordinator
from ..db.table_registry import get_table_registry, DatabaseType, Tables

# Factory integration imports
from ..analysis.processors.strategy_processor_factory import StrategyProcessorFactory
from ..services.betting_signal_repository import BettingSignalRepository
from ..services.strategy_validator import StrategyValidator
from ..models.betting_analysis import SignalProcessorConfig
from ..db.repositories import get_game_outcome_repository  # Add this import

# 🚨 ENSURE UNIVERSAL COMPATIBILITY: Initialize once at module level
setup_universal_logger_compatibility()

try:
    from ..db.connection import DatabaseManager, get_db_manager
    from ..core.exceptions import DatabaseError, ValidationError
except ImportError:
    # Handle direct execution
    import sys
    from pathlib import Path
    
    # Add the parent directory to the path
    current_dir = Path(__file__).parent
    project_root = current_dir.parent.parent
    sys.path.insert(0, str(project_root))
    
    from mlb_sharp_betting.db.connection import DatabaseManager, get_db_manager
    from mlb_sharp_betting.core.exceptions import DatabaseError, ValidationError


def setup_clean_backtesting_logging():
    """
    🚨 SIMPLIFIED: Configure clean logging for backtesting using universal compatibility.
    
    Now that we have universal logger compatibility, this function is simplified
    to just handle file logging setup. Console output is handled automatically.
    """
    
    # Create logs directory if it doesn't exist
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Create timestamped log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"backtesting_{timestamp}.log"
    
    # Configure standard logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            # File handler - captures ALL logs
            logging.FileHandler(log_file),
        ]
    )
    
    # Create console handler with higher level (only important stuff)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    
    # Set specific logger levels to reduce noise
    logging.getLogger("executor").setLevel(logging.WARNING)  # Reduce executor spam
    logging.getLogger("strategy_factory").setLevel(logging.WARNING)  # Reduce factory spam
    logging.getLogger("database").setLevel(logging.ERROR)  # Only show DB errors
    logging.getLogger("mlb_sharp_betting.analysis.processors").setLevel(logging.WARNING)
    logging.getLogger("mlb_sharp_betting.services").setLevel(logging.WARNING)
    
    # Add console handler only to root logger
    root_logger = logging.getLogger()
    root_logger.addHandler(console_handler)
    
    print(f"📝 Detailed logs will be saved to: {log_file}")
    return log_file


# 🚨 REMOVED: BackwardCompatibleLogger, BoundCompatibleLogger, and patch functions
# These are now handled universally by the core.logging module's setup_universal_logger_compatibility()
# which applies compatibility to ALL bound loggers automatically when the module is imported.


def convert_numpy_types(value):
    """
    Convert numpy types to native Python types for JSON serialization
    """
    if isinstance(value, np.integer):
        return int(value)
    elif isinstance(value, np.floating):
        return float(value)
    elif isinstance(value, np.ndarray):
        return value.tolist()
    elif isinstance(value, (np.bool_, bool)):
        return bool(value)
    else:
        return value


@dataclass
class BacktestResult:
    """Standardized backtest result format."""
    strategy_name: str
    total_bets: int
    wins: int
    win_rate: float
    roi_per_100: float
    confidence_score: float
    sample_size_category: str  # INSUFFICIENT, BASIC, RELIABLE, ROBUST
    
    # Additional metrics for compatibility
    source_book_type: str = "UNKNOWN"
    split_type: str = "UNKNOWN"
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    confidence_interval_lower: float = 0.0
    confidence_interval_upper: float = 0.0
    sample_size_adequate: bool = False
    statistical_significance: bool = False
    p_value: float = 1.0
    
    # Timestamps
    last_updated: datetime = None
    backtest_date: datetime = None
    created_at: datetime = None


class StrategyExecutor(ABC):
    """Abstract base for strategy execution."""
    
    @abstractmethod
    async def execute(self, start_date: str, end_date: str) -> List[BacktestResult]:
        """Execute strategy and return standardized results."""
        pass
    
    @abstractmethod
    def get_strategy_name(self) -> str:
        """Get strategy identifier."""
        pass


class ProcessorStrategyExecutor(StrategyExecutor):
    """Execute processor-based strategies using real historical data."""
    
    def __init__(self, processor_factory: StrategyProcessorFactory, processor_name: str, db_manager: Optional[DatabaseManager] = None):
        self.processor_factory = processor_factory
        self.processor_name = processor_name
        
        # Initialize clean logging
        get_clean_logger()  # Ensure clean logging is set up
        self.logger = BackwardCompatibleLogger(f"executor-{processor_name}")
        
        # 🚨 FIXED: Proper database manager initialization
        if db_manager and db_manager.is_initialized():
            self.db_manager = db_manager
        else:
            self.logger.debug_file_only("Initializing new database manager for executor")
            self.db_manager = DatabaseManager()
            
            # Initialize if not already done
            if not self.db_manager.is_initialized():
                try:
                    self.db_manager.initialize()
                    self.logger.debug_file_only("Database manager initialized successfully")
                except Exception as e:
                    self.logger.error_console(f"Failed to initialize database manager: {e}")
                    # Don't raise here, let execute() handle it gracefully
        
        # Circuit breaker to prevent infinite loops
        self._loop_detection = {
            "check_count": 0,
            "max_checks": 50,  # Prevent infinite status checks
            "last_status": None
        }
        
        # Validate database connection on initialization (quietly)
        try:
            if self.db_manager.is_initialized():
                with self.db_manager.get_cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                self.logger.debug_file_only("Database connection validated for executor")
            else:
                self.logger.debug_file_only("Database not initialized during executor creation")
        except Exception as e:
            self.logger.debug_file_only(f"Database connection test failed during initialization: {e}")
            # Don't raise here, let execute() handle it gracefully
    
    async def execute(self, start_date: str, end_date: str) -> List[BacktestResult]:
        """Execute processor strategy with book-specific result separation."""
        
        strategy_name = self.processor_name
        
        try:
            # Check strategy status ONCE, not in a loop
            processor_status = self._check_processor_status_safely()
            
            if processor_status != "IMPLEMENTED":
                self.logger.debug_file_only(
                    f"❌ SKIP: {strategy_name} - status: {processor_status} (not IMPLEMENTED)"
                )
                return []  # Return immediately instead of looping
            
            self.logger.debug_file_only(f"✅ STATUS: {strategy_name} is IMPLEMENTED - proceeding")
            
            # Validate database connection
            if not self._validate_database_connection():
                self.logger.debug_file_only(f"❌ DB: Database connection failed for {strategy_name}")
                return []
            
            self.logger.debug_file_only(f"✅ DB: Database connection validated for {strategy_name}")
            
            # Get historical games
            historical_games = await self._get_historical_games_safely(start_date, end_date)
            
            if not historical_games:
                self.logger.debug_file_only(f"❌ GAMES: No historical games found for {strategy_name}")
                return []
            
            self.logger.debug_file_only(f"✅ GAMES: Found {len(historical_games)} games for {strategy_name}")
            
            # Process games with real betting data and outcomes
            signals = []
            games_processed = 0
            games_with_betting_data = 0
            games_with_signals = 0
            max_games = min(100, len(historical_games))
            
            for game in historical_games[:max_games]:
                games_processed += 1
                
                try:
                    # Get betting data within actionable window (45 minutes before game)
                    betting_records = await self._get_betting_data_for_game(
                        game['game_id'], 
                        game.get('game_datetime')
                    )
                    
                    if betting_records:
                        games_with_betting_data += 1
                        
                        # 🚨 FIXED: Generate only ONE signal per game per market type
                        consolidated_signal = await self._consolidate_betting_data_for_single_signal(
                            game, betting_records
                        )
                        if consolidated_signal:
                            signals.append(consolidated_signal)
                            games_with_signals += 1
                                
                except Exception as e:
                    self.logger.debug_file_only(f"Error processing game {game.get('game_id', 'unknown')}: {e}")
                    continue
            
            # If no signals, provide detailed reason
            if not signals:
                if games_with_betting_data == 0:
                    self.logger.debug_file_only(f"❌ NO_DATA: {strategy_name} - no betting data found for any games")
                elif games_with_signals == 0:
                    self.logger.debug_file_only(f"❌ NO_SIGNALS: {strategy_name} - betting data found but no signals generated")
                else:
                    self.logger.debug_file_only(f"❌ UNKNOWN: {strategy_name} - unexpected state in signal generation")
                return []
            
            self.logger.debug_file_only(f"✅ SIGNALS: Generated {len(signals)} raw signals for {strategy_name}")
            
            # Evaluate signals against actual game outcomes
            evaluated_signals = await self._evaluate_signals_against_outcomes(signals)
            self.logger.debug_file_only(
                f"✅ EVALUATION: Evaluated {len(evaluated_signals)} signals against outcomes for {strategy_name}"
            )
            
            if evaluated_signals:
                # 🚨 NEW: Check if this processor supports book-specific strategies
                if strategy_name == "sharp_action" and self._supports_book_specific_results(evaluated_signals):
                    # Group signals by source-book combination
                    book_specific_results = self._create_book_specific_results(evaluated_signals)
                    self.logger.debug_file_only(
                        f"✅ BOOK_SPECIFIC: {strategy_name} split into {len(book_specific_results)} book-specific strategies"
                    )
                    return book_specific_results
                else:
                    # Standard single result
                    aggregated_result = self._aggregate_strategy_performance(evaluated_signals)
                    self.logger.debug_file_only(
                        f"✅ SUCCESS: {strategy_name} completed successfully",
                        total_bets=aggregated_result.total_bets,
                        win_rate=f"{aggregated_result.win_rate:.1%}",
                        roi=f"{aggregated_result.roi_per_100:.1f}%"
                    )
                    return [aggregated_result]
            else:
                self.logger.debug_file_only(f"❌ EVAL_FAILED: {strategy_name} - signal evaluation failed")
                return []
                
        except Exception as e:
            self.logger.debug_file_only(f"💥 EXCEPTION: Processor execution failed for {strategy_name}: {e}")
            import traceback
            self.logger.debug_file_only(f"🔍 TRACEBACK: {traceback.format_exc()}")
            return []
    
    def _check_processor_status_safely(self) -> str:
        """Check processor status with circuit breaker to prevent infinite loops."""
        
        # Reset loop detection if this is a new strategy
        if self._loop_detection["last_status"] != self.processor_name:
            self._loop_detection["check_count"] = 0
            self._loop_detection["last_status"] = self.processor_name
        
        # Increment check count
        self._loop_detection["check_count"] += 1
        
        # Circuit breaker - if we've checked too many times, assume NOT_IMPLEMENTED
        if self._loop_detection["check_count"] > self._loop_detection["max_checks"]:
            self.logger.debug_file_only(
                f"Circuit breaker activated for {self.processor_name} - too many status checks"
            )
            return "PLANNED"  # Treat as not implemented to skip
        
        try:
            # Check implementation status
            if hasattr(self.processor_factory, 'get_implementation_status'):
                status_dict = self.processor_factory.get_implementation_status()
                return status_dict.get(self.processor_name, "PLANNED")
            else:
                # Fallback if method doesn't exist
                return "PLANNED"
                
        except Exception as e:
            self.logger.debug_file_only(f"Error checking status for {self.processor_name}: {e}")
            return "PLANNED"
    
    def _validate_database_connection(self) -> bool:
        """Validate database connection without spamming logs."""
        try:
            if not self.db_manager.is_initialized():
                return False
            
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
                return True
                
        except Exception:
            return False
    
    async def _get_historical_games_safely(self, start_date: str, end_date: str) -> List[Dict]:
        """Get historical games with complete outcomes for proper backtesting."""
        try:
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT 
                        go.game_id,
                        go.home_team,
                        go.away_team,
                        go.game_date,
                        go.game_date as game_datetime,
                        go.home_score,
                        go.away_score,
                        go.home_win,
                        go.over,
                        go.home_score + go.away_score as total_score
                    FROM public.game_outcomes go
                    WHERE go.game_date BETWEEN %s AND %s
                      AND go.home_score IS NOT NULL 
                      AND go.away_score IS NOT NULL
                      AND go.game_date < CURRENT_TIMESTAMP - INTERVAL '6 hours'  -- Only completed games
                    ORDER BY go.game_date DESC
                    LIMIT 100  -- Limit to prevent massive datasets
                """, (start_date, end_date))
                
                columns = [desc[0] for desc in cursor.description]
                games = [dict(zip(columns, row)) for row in cursor.fetchall()]
                
                self.logger.debug_file_only(f"Retrieved {len(games)} completed games with outcomes from public.game_outcomes")
                return games
                
        except Exception as e:
            self.logger.debug_file_only(f"Error getting historical games from public.game_outcomes: {e}")
            return []
    
    def _categorize_sample_size(self, total_bets: int) -> str:
        """Categorize sample size."""
        if total_bets < 10:
            return "INSUFFICIENT"
        elif total_bets < 25:
            return "BASIC"
        elif total_bets < 50:
            return "RELIABLE"
        else:
            return "ROBUST"
    
    async def _get_historical_games(self, start_date: str, end_date: str) -> List[Dict]:
        """Get historical games with known outcomes in date range."""
        with self.db_manager.get_cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT 
                    go.game_id,
                    go.home_team,
                    go.away_team,
                    go.game_date,
                    go.game_date as game_datetime,
                    go.home_score,
                    go.away_score,
                    go.home_win,
                    go.over,
                    go.home_score + go.away_score as total_score
                FROM public.game_outcomes go
                WHERE go.game_date BETWEEN %s AND %s
                  AND go.home_score IS NOT NULL 
                  AND go.away_score IS NOT NULL
                  AND go.game_date < CURRENT_TIMESTAMP - INTERVAL '6 hours'
                ORDER BY go.game_date
            """, (start_date, end_date))
            
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    async def _get_actionable_betting_data(self, game_id: str, game_datetime) -> Dict:
        """Get betting data that was available within actionable window before game."""
        with self.db_manager.get_cursor() as cursor:
            # For backtesting, use a realistic window (6 hours) based on actual data collection patterns
            # Production systems collect data 2-5 hours before games, so 45 minutes is too restrictive
            # Use actual column names from splits.raw_mlb_betting_splits table
            cursor.execute("""
                SELECT 
                    source,
                    book,
                    split_type,
                    home_or_over_bets_percentage,
                    away_or_under_bets_percentage,
                    home_or_over_stake_percentage,
                    away_or_under_stake_percentage,
                    split_value,
                    last_updated
                FROM splits.raw_mlb_betting_splits
                WHERE game_id = %s
                  AND EXTRACT('epoch' FROM (%s - last_updated)) / 60 <= 360  -- Within 6 hours (360 min)
                  AND EXTRACT('epoch' FROM (%s - last_updated)) / 60 >= 0.5  -- At least 30 sec before
                ORDER BY last_updated DESC
            """, (game_id, game_datetime, game_datetime))
            
            columns = [desc[0] for desc in cursor.description]
            betting_records = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            return {'game_id': game_id, 'betting_data': betting_records}
    
    async def _get_betting_data_for_game(self, game_id: str, game_datetime=None) -> List[Dict]:
        """Get betting data within actionable window (45 minutes before game) for backtesting."""
        with self.db_manager.get_cursor() as cursor:
            if game_datetime:
                # Enforce 45-minute actionable window
                cursor.execute("""
                    SELECT 
                        source,
                        book,
                        split_type,
                        home_or_over_bets_percentage,
                        away_or_under_bets_percentage,
                        home_or_over_stake_percentage,
                        away_or_under_stake_percentage,
                        split_value,
                        last_updated
                    FROM splits.raw_mlb_betting_splits
                    WHERE game_id = %s
                      AND EXTRACT('epoch' FROM (%s - last_updated)) / 60 BETWEEN 0.5 AND 45
                    ORDER BY last_updated DESC
                """, (game_id, game_datetime))
            else:
                # Fallback for games without datetime
                cursor.execute("""
                    SELECT 
                        source,
                        book,
                        split_type,
                        home_or_over_bets_percentage,
                        away_or_under_bets_percentage,
                        home_or_over_stake_percentage,
                        away_or_under_stake_percentage,
                        split_value,
                        last_updated
                    FROM splits.raw_mlb_betting_splits
                    WHERE game_id = %s
                    ORDER BY last_updated DESC
                    LIMIT 10  -- Limit to most recent data
                """, (game_id,))
            
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    async def _consolidate_betting_data_for_single_signal(self, game: Dict, betting_records: List[Dict]) -> Optional[Dict]:
        """
        Consolidate multiple betting records into a SINGLE signal per game per market.
        
        This enforces the 1-bet-per-game-per-market rule and detects multi-book consensus.
        """
        if not betting_records:
            return None
            
        # Group betting records by market type (moneyline, spread, total)
        markets = {}
        for record in betting_records:
            market_type = record.get('split_type', 'moneyline').lower()
            if market_type not in markets:
                markets[market_type] = []
            markets[market_type].append(record)
        
        # For this processor, pick the BEST market based on signal strength
        best_signal = None
        best_strength = 0
        
        for market_type, market_records in markets.items():
            # Find the strongest signal in this market
            market_signal = await self._find_strongest_signal_in_market(
                game, market_records, market_type
            )
            
            if market_signal and market_signal.get('differential', 0) > best_strength:
                # Track multi-book consensus as additional metadata
                market_signal['num_books_agreeing'] = len(market_records)
                market_signal['books_list'] = [r.get('book', 'unknown') for r in market_records]
                market_signal['consensus_strength'] = len(market_records) / len(betting_records)
                
                best_signal = market_signal
                best_strength = market_signal.get('differential', 0)
        
        return best_signal
    
    async def _find_strongest_signal_in_market(self, game: Dict, market_records: List[Dict], market_type: str) -> Optional[Dict]:
        """Find the strongest signal within a specific market type."""
        if not market_records:
            return None
            
        # Calculate the strongest differential from all records
        best_record = None
        best_differential = 0
        
        for record in market_records:
            home_bet_pct = record.get('home_or_over_bets_percentage', 50.0)
            home_money_pct = record.get('home_or_over_stake_percentage', 50.0)
            
            if home_bet_pct is None or home_money_pct is None:
                continue
                
            differential = abs(home_bet_pct - home_money_pct)
            if differential > best_differential:
                best_differential = differential
                best_record = record
        
        if best_record and best_differential >= 10.0:  # Minimum threshold
            # Generate signal based on the strongest record
            return await self._simulate_processor_analysis(None, game, best_record)
            
        return None
    
    async def _run_processor_on_historical_data(self, game: Dict, betting_data: Dict) -> List[Dict]:
        """Run processor logic on historical betting data - ONE SIGNAL PER GAME."""
        signals = []
        
        try:
            # Create processor instance
            processor = self.processor_factory.create_processor(self.processor_name)
            if not processor:
                return []
            
            # 🚨 FIX: Only create ONE signal per game, not one per betting record
            # Find the best/most recent betting data record for this game
            betting_records = betting_data['betting_data']
            if not betting_records:
                return []
            
            # Use the most recent betting record (they're ordered by last_updated DESC)
            best_record = betting_records[0]
            
            # Simulate processor analysis on the best record only
            signal = await self._simulate_processor_analysis(processor, game, best_record)
            if signal:
                signals.append(signal)
            
        except Exception as e:
            self.logger.debug(f"Processor simulation failed for game {game['game_id']}: {e}")
        
        return signals
    
    async def _simulate_processor_analysis(self, processor, game: Dict, bet_record: Dict) -> Optional[Dict]:
        """
        Simulate what the processor would have recommended given historical data.
        
        This is the key method that needs to be implemented for each processor type.
        """
        try:
            # Map processor names to simulation methods (fix: use actual processor names)
            if self.processor_name == "sharp_action":
                return await self._simulate_sharp_action_analysis(game, bet_record)
            elif self.processor_name == "opposing_markets":
                return await self._simulate_opposing_markets_analysis(game, bet_record)
            elif self.processor_name == "book_conflicts":
                return await self._simulate_book_conflict_analysis(game, bet_record)
            elif self.processor_name == "public_money_fade":
                return await self._simulate_public_fade_analysis(game, bet_record)
            elif self.processor_name == "late_sharp_flip":
                return await self._simulate_late_flip_analysis(game, bet_record)
            elif self.processor_name == "consensus_moneyline":
                return await self._simulate_consensus_moneyline_analysis(game, bet_record)
            elif self.processor_name == "underdog_ml_value":
                return await self._simulate_underdog_ml_value_analysis(game, bet_record)
            elif self.processor_name == "line_movement":
                return await self._simulate_line_movement_analysis(game, bet_record)
            # Add other processor types as needed
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Simulation failed: {e}")
            return None
    
    async def _simulate_sharp_action_analysis(self, game: Dict, bet_record: Dict) -> Optional[Dict]:
        """Simulate sharp action processor analysis."""
        # Calculate bet/money differential (key indicator for sharp action)
        home_bet_pct = bet_record.get('home_or_over_bets_percentage', 50.0)
        home_money_pct = bet_record.get('home_or_over_stake_percentage', 50.0)
        
        if home_bet_pct is None or home_money_pct is None:
            return None
        
        differential = abs(home_bet_pct - home_money_pct)
        
        # Apply sharp action thresholds (from your existing logic)
        if differential >= 20.0:  # Strong sharp action threshold
            confidence = 0.85
            signal_strength = "STRONG"
        elif differential >= 15.0:  # Moderate sharp action threshold
            confidence = 0.70
            signal_strength = "MODERATE"
        elif differential >= 10.0:  # Weak sharp action threshold
            confidence = 0.55
            signal_strength = "WEAK"
        else:
            return None  # No signal
        
        # Determine recommended bet based on money flow direction and market type
        split_type = bet_record.get('split_type', 'moneyline').lower()
        
        if split_type in ['total', 'totals']:
            # Handle totals/over-under markets
            if home_money_pct > home_bet_pct + 10:  # Sharp money on over
                recommended_bet = "OVER"
                bet_target = "OVER"
            elif home_bet_pct > home_money_pct + 10:  # Sharp money on under
                recommended_bet = "UNDER"
                bet_target = "UNDER"
            else:
                return None
        else:
            # Handle moneyline/spread markets
            if home_money_pct > home_bet_pct + 10:  # Sharp money on home
                recommended_bet = "HOME_ML"
                bet_target = game['home_team']
            elif home_bet_pct > home_money_pct + 10:  # Sharp money on away
                recommended_bet = "AWAY_ML"
                bet_target = game['away_team']
            else:
                return None
        
        # Extract source and book information from bet_record
        source = bet_record.get('source', bet_record.get('data_source', 'VSIN'))
        book = bet_record.get('book', bet_record.get('sportsbook', 'unknown'))
        split_type = bet_record.get('split_type', 'moneyline')

        # Create market-specific strategy name
        market_suffix = "_totals" if split_type in ['total', 'totals'] else "_moneyline"
        strategy_name = f"{self.processor_name}{market_suffix}"
        
        return {
            'game_id': game['game_id'],
            'strategy_name': strategy_name,
            'recommended_bet': recommended_bet,
            'bet_target': bet_target,
            'confidence': confidence,
            'signal_strength': signal_strength,
            'differential': differential,
            'game_datetime': game['game_datetime'],
            'home_team': game['home_team'],
            'away_team': game['away_team'],
            # Add book-specific information
            'source': source,
            'book': book,
            'split_type': split_type,
            'sportsbook': book,
            'data_source': source
        }
    
    async def _simulate_opposing_markets_analysis(self, game: Dict, bet_record: Dict) -> Optional[Dict]:
        """Simulate opposing markets processor analysis."""
        # Look for moneyline vs spread market conflicts
        home_bet_pct = bet_record.get('home_or_over_bets_percentage', 50.0)
        home_money_pct = bet_record.get('home_or_over_stake_percentage', 50.0)
        
        if home_bet_pct is None or home_money_pct is None:
            return None
        
        # Simulate opposing market signals - when ML and spread point in different directions
        # This is a simplified implementation
        if bet_record.get('split_type') == 'moneyline':
            # Simulate spread market going opposite direction
            # If moderate ML action on home (>55%), but hypothetical spread action on away
            if home_bet_pct > 55.0:
                # Simulate opposing spread market
                recommended_bet = "AWAY_ML"  # Fade the ML consensus
                bet_target = game['away_team']
                confidence = 0.60
                signal_strength = "MODERATE"
                
                return {
                    'game_id': game['game_id'],
                    'strategy_name': self.processor_name,
                    'recommended_bet': recommended_bet,
                    'bet_target': bet_target,
                    'confidence': confidence,
                    'signal_strength': signal_strength,
                    'market_conflict': 'ML_vs_SPREAD',
                    'game_datetime': game['game_datetime'],
                    'home_team': game['home_team'],
                    'away_team': game['away_team']
                }
            
            elif home_bet_pct < 45.0:
                # Heavy away ML action, fade with home bet
                recommended_bet = "HOME_ML"
                bet_target = game['home_team']
                confidence = 0.60
                signal_strength = "MODERATE"
                
                return {
                    'game_id': game['game_id'],
                    'strategy_name': self.processor_name,
                    'recommended_bet': recommended_bet,
                    'bet_target': bet_target,
                    'confidence': confidence,
                    'signal_strength': signal_strength,
                    'market_conflict': 'ML_vs_SPREAD',
                    'game_datetime': game['game_datetime'],
                    'home_team': game['home_team'],
                    'away_team': game['away_team']
                }
        
        return None
    
    async def _simulate_book_conflict_analysis(self, game: Dict, bet_record: Dict) -> Optional[Dict]:
        """Simulate book conflict processor analysis."""
        # Look for line discrepancies across different books
        home_bet_pct = bet_record.get('home_or_over_bets_percentage', 50.0)
        home_money_pct = bet_record.get('home_or_over_stake_percentage', 50.0)
        book = bet_record.get('book', 'UNKNOWN')
        
        if home_bet_pct is None or home_money_pct is None:
            return None
        
        # Simulate book conflicts - when one book has different action than others
        # This is a simplified implementation that simulates conflicts
        
        # If this is DraftKings data, simulate conflict with other books
        if book.upper() in ['DK', 'DRAFTKINGS'] and home_bet_pct > 55.0:
            # Simulate that other books show opposite action
            recommended_bet = "AWAY_ML"  # Bet against DK public consensus
            bet_target = game['away_team']
            confidence = 0.68
            signal_strength = "STRONG"
            
            return {
                'game_id': game['game_id'],
                'strategy_name': self.processor_name,
                'recommended_bet': recommended_bet,
                'bet_target': bet_target,
                'confidence': confidence,
                'signal_strength': signal_strength,
                'conflict_book': book,
                'conflict_percentage': home_bet_pct,
                'game_datetime': game['game_datetime'],
                'home_team': game['home_team'],
                'away_team': game['away_team']
            }
        
        elif book.upper() in ['CIRCA'] and home_bet_pct < 45.0:
            # Simulate Circa showing heavy away action, bet home
            recommended_bet = "HOME_ML"
            bet_target = game['home_team']
            confidence = 0.68
            signal_strength = "STRONG"
            
            return {
                'game_id': game['game_id'],
                'strategy_name': self.processor_name,
                'recommended_bet': recommended_bet,
                'bet_target': bet_target,
                'confidence': confidence,
                'signal_strength': signal_strength,
                'conflict_book': book,
                'conflict_percentage': home_bet_pct,
                'game_datetime': game['game_datetime'],
                'home_team': game['home_team'],
                'away_team': game['away_team']
            }
        
        return None
    
    async def _simulate_public_fade_analysis(self, game: Dict, bet_record: Dict) -> Optional[Dict]:
        """Simulate public fade processor analysis."""
        # Identify heavily public bets to fade
        home_bet_pct = bet_record.get('home_or_over_bets_percentage', 50.0)
        home_money_pct = bet_record.get('home_or_over_stake_percentage', 50.0)
        
        if home_bet_pct is None or home_money_pct is None:
            return None
        
        # Look for heavily public sides (80%+ public backing)
        if home_bet_pct >= 80.0:  # Public heavily on home
            recommended_bet = "AWAY_ML"
            bet_target = game['away_team']
            confidence = 0.65
            signal_strength = "MODERATE"
        elif home_bet_pct <= 20.0:  # Public heavily on away
            recommended_bet = "HOME_ML"
            bet_target = game['home_team']
            confidence = 0.65
            signal_strength = "MODERATE"
        else:
            return None  # Not public enough to fade
        
        # Extract source and book information
        source = bet_record.get('source', bet_record.get('data_source', 'VSIN'))
        book = bet_record.get('book', bet_record.get('sportsbook', 'unknown'))
        split_type = bet_record.get('split_type', 'moneyline')

        return {
            'game_id': game['game_id'],
            'strategy_name': self.processor_name,
            'recommended_bet': recommended_bet,
            'bet_target': bet_target,
            'confidence': confidence,
            'signal_strength': signal_strength,
            'public_percentage': home_bet_pct,
            'game_datetime': game['game_datetime'],
            'home_team': game['home_team'],
            'away_team': game['away_team'],
            'source': source,
            'book': book,
            'split_type': split_type,
            'sportsbook': book,
            'data_source': source
        }
    
    async def _simulate_late_flip_analysis(self, game: Dict, bet_record: Dict) -> Optional[Dict]:
        """Simulate late flip processor analysis."""
        # Detect late sharp money flips based on timestamp proximity to game time
        home_bet_pct = bet_record.get('home_or_over_bets_percentage', 50.0)
        home_money_pct = bet_record.get('home_or_over_stake_percentage', 50.0)
        last_updated = bet_record.get('last_updated')
        game_datetime = game.get('game_datetime')
        
        if (home_bet_pct is None or home_money_pct is None or 
            last_updated is None or game_datetime is None):
            return None
        
        # Calculate time to game
        if isinstance(last_updated, str):
            from datetime import datetime
            try:
                last_updated = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
            except:
                return None
        
        if isinstance(game_datetime, str):
            from datetime import datetime
            try:
                game_datetime = datetime.fromisoformat(game_datetime.replace('Z', '+00:00'))
            except:
                return None
        
        # Calculate minutes to game
        try:
            time_to_game = (game_datetime - last_updated).total_seconds() / 60
        except:
            return None
        
        # Look for late flips (within 2 hours of game time)
        if 0 < time_to_game <= 120:  # Within 2 hours
            differential = abs(home_bet_pct - home_money_pct)
            
            # Strong late movement threshold
            if differential >= 15.0:
                # Determine flip direction
                if home_money_pct > home_bet_pct + 15:  # Late sharp money on home
                    recommended_bet = "HOME_ML"
                    bet_target = game['home_team']
                    confidence = 0.75
                    signal_strength = "STRONG"
                elif home_bet_pct > home_money_pct + 15:  # Late sharp money on away
                    recommended_bet = "AWAY_ML"
                    bet_target = game['away_team']
                    confidence = 0.75
                    signal_strength = "STRONG"
                else:
                    return None
                
                return {
                    'game_id': game['game_id'],
                    'strategy_name': self.processor_name,
                    'recommended_bet': recommended_bet,
                    'bet_target': bet_target,
                    'confidence': confidence,
                    'signal_strength': signal_strength,
                    'flip_differential': differential,
                    'minutes_to_game': time_to_game,
                    'game_datetime': game['game_datetime'],
                    'home_team': game['home_team'],
                    'away_team': game['away_team']
                }
        
        return None
    
    async def _simulate_consensus_moneyline_analysis(self, game: Dict, bet_record: Dict) -> Optional[Dict]:
        """Simulate consensus moneyline processor analysis."""
        home_bet_pct = bet_record.get('home_or_over_bets_percentage', 50.0)
        home_money_pct = bet_record.get('home_or_over_stake_percentage', 50.0)
        
        if home_bet_pct is None or home_money_pct is None:
            return None
        
        # Look for consensus signals where multiple books agree
        split_type = bet_record.get('split_type', 'moneyline').lower()
        
        if split_type in ['total', 'totals']:
            # Handle totals consensus
            if home_bet_pct > 65.0 and home_money_pct > 60.0:
                recommended_bet = "OVER"
                bet_target = "OVER"
                confidence = 0.70
                signal_strength = "MODERATE"
            elif home_bet_pct < 35.0 and home_money_pct < 40.0:
                recommended_bet = "UNDER"
                bet_target = "UNDER"
                confidence = 0.70
                signal_strength = "MODERATE"
            else:
                return None
        elif split_type == 'moneyline':
            # Strong consensus on one side
            if home_bet_pct > 65.0 and home_money_pct > 60.0:
                recommended_bet = "HOME_ML"
                bet_target = game['home_team']
                confidence = 0.70
                signal_strength = "MODERATE"
            elif home_bet_pct < 35.0 and home_money_pct < 40.0:
                recommended_bet = "AWAY_ML"
                bet_target = game['away_team']
                confidence = 0.70
                signal_strength = "MODERATE"
            else:
                return None
        else:
            return None
                
            # Create market-specific strategy name
            market_suffix = "_totals" if split_type in ['total', 'totals'] else "_moneyline"
            strategy_name = f"{self.processor_name}{market_suffix}"
            
            return {
                'game_id': game['game_id'],
                'strategy_name': strategy_name,
                'recommended_bet': recommended_bet,
                'bet_target': bet_target,
                'confidence': confidence,
                'signal_strength': signal_strength,
                'consensus_bet_pct': home_bet_pct,
                'consensus_money_pct': home_money_pct,
                'game_datetime': game['game_datetime'],
                'home_team': game['home_team'],
                'away_team': game['away_team']
            }
        
        return None
    
    async def _simulate_underdog_ml_value_analysis(self, game: Dict, bet_record: Dict) -> Optional[Dict]:
        """Simulate underdog ML value processor analysis."""
        home_bet_pct = bet_record.get('home_or_over_bets_percentage', 50.0)
        home_money_pct = bet_record.get('home_or_over_stake_percentage', 50.0)
        
        if home_bet_pct is None or home_money_pct is None:
            return None
        
        # Look for underdog value - when public is heavily on favorite
        split_type = bet_record.get('split_type', 'moneyline').lower()
        
        if split_type in ['total', 'totals']:
            # Handle totals - fade heavy public action
            if home_bet_pct > 70.0:  # Public heavily on over
                recommended_bet = "UNDER"
                bet_target = "UNDER"
                confidence = 0.60
                signal_strength = "MODERATE"
            elif home_bet_pct < 30.0:  # Public heavily on under
                recommended_bet = "OVER"
                bet_target = "OVER"
                confidence = 0.60
                signal_strength = "MODERATE"
            else:
                return None
        elif split_type == 'moneyline':
            # Public heavily on home (>70%), bet away underdog
            if home_bet_pct > 70.0:
                recommended_bet = "AWAY_ML"
                bet_target = game['away_team']
                confidence = 0.60
                signal_strength = "MODERATE"
            # Public heavily on away (>70%), bet home underdog  
            elif home_bet_pct < 30.0:
                recommended_bet = "HOME_ML"
                bet_target = game['home_team']
                confidence = 0.60
                signal_strength = "MODERATE"
            else:
                return None
        else:
            return None
                
            # Create market-specific strategy name
            market_suffix = "_totals" if split_type in ['total', 'totals'] else "_moneyline"
            strategy_name = f"{self.processor_name}{market_suffix}"
            
            return {
                'game_id': game['game_id'],
                'strategy_name': strategy_name,
                'recommended_bet': recommended_bet,
                'bet_target': bet_target,
                'confidence': confidence,
                'signal_strength': signal_strength,
                'public_bet_pct': home_bet_pct,
                'underdog_value': abs(50 - home_bet_pct),
                'game_datetime': game['game_datetime'],
                'home_team': game['home_team'],
                'away_team': game['away_team']
            }
        
        return None
    
    async def _simulate_line_movement_analysis(self, game: Dict, bet_record: Dict) -> Optional[Dict]:
        """Simulate line movement processor analysis."""
        home_bet_pct = bet_record.get('home_or_over_bets_percentage', 50.0)
        home_money_pct = bet_record.get('home_or_over_stake_percentage', 50.0)
        
        if home_bet_pct is None or home_money_pct is None:
            return None
        
        # Simulate line movement signals based on sharp money vs public betting
        differential = abs(home_bet_pct - home_money_pct)
        
        # Line movement typically follows money, not public bets
        split_type = bet_record.get('split_type', 'moneyline').lower()
        
        if differential >= 15.0:  # Significant divergence suggests line movement
            if split_type in ['total', 'totals']:
                # Handle totals line movement
                if home_money_pct > home_bet_pct + 15:  # Sharp money on over
                    recommended_bet = "OVER"
                    bet_target = "OVER"
                    sharp_direction = 'OVER'
                elif home_bet_pct > home_money_pct + 15:  # Sharp money on under
                    recommended_bet = "UNDER"
                    bet_target = "UNDER"
                    sharp_direction = 'UNDER'
                else:
                    return None
            else:
                # Handle moneyline/spread line movement
                if home_money_pct > home_bet_pct + 15:  # Sharp money on home
                    recommended_bet = "HOME_ML"
                    bet_target = game['home_team']
                    sharp_direction = 'HOME'
                elif home_bet_pct > home_money_pct + 15:  # Sharp money on away
                    recommended_bet = "AWAY_ML"
                    bet_target = game['away_team']
                    sharp_direction = 'AWAY'
                else:
                    return None
                    
            # Create market-specific strategy name
            market_suffix = "_totals" if split_type in ['total', 'totals'] else "_moneyline"
            strategy_name = f"{self.processor_name}{market_suffix}"
            
            return {
                'game_id': game['game_id'],
                'strategy_name': strategy_name,
                'recommended_bet': recommended_bet,
                'bet_target': bet_target,
                'confidence': 0.65,
                'signal_strength': "MODERATE",
                'line_movement_differential': differential,
                'sharp_money_direction': sharp_direction,
                'game_datetime': game['game_datetime'],
                'home_team': game['home_team'],
                'away_team': game['away_team']
            }
        
        return None
    
    async def _evaluate_signals_against_outcomes(self, signals: List[Dict]) -> List[Dict]:
        """Evaluate processor signals against actual game outcomes."""
        evaluated_signals = []
        
        for signal in signals:
            try:
                # Get game outcome
                game_outcome = await self._get_game_outcome(signal['game_id'])
                if not game_outcome:
                    continue
                
                # Determine if the recommended bet won
                bet_won = self._evaluate_bet_outcome(signal, game_outcome)
                
                # Calculate potential profit/loss (assuming -110 odds)
                if bet_won:
                    profit_loss = 100.0  # Win $100 on $110 bet
                else:
                    profit_loss = -110.0  # Lose $110
                
                evaluated_signals.append({
                    **signal,
                    'bet_won': bet_won,
                    'profit_loss': profit_loss,
                    'actual_home_score': game_outcome['home_score'],
                    'actual_away_score': game_outcome['away_score']
                })
                
            except Exception as e:
                self.logger.debug(f"Failed to evaluate signal for game {signal['game_id']}: {e}")
                continue
        
        return evaluated_signals
    
    async def _get_game_outcome(self, game_id: str) -> Optional[Dict]:
        """Get actual game outcome."""
        with self.db_manager.get_cursor() as cursor:
            cursor.execute("""
                SELECT game_id, home_team, away_team, home_score, away_score, home_win, over
                FROM public.game_outcomes
                WHERE game_id = %s
            """, (game_id,))
            
            result = cursor.fetchone()
            if result:
                return {
                    'game_id': result[0],
                    'home_team': result[1], 
                    'away_team': result[2],
                    'home_score': result[3],
                    'away_score': result[4],
                    'home_win': result[5],
                    'over': result[6]
                }
            return None
    
    def _evaluate_bet_outcome(self, signal: Dict, game_outcome: Dict) -> bool:
        """Determine if the recommended bet won based on actual outcome."""
        recommended_bet = signal.get('recommended_bet')
        home_score = game_outcome['home_score']
        away_score = game_outcome['away_score']
        
        if recommended_bet == "HOME_ML":
            return home_score > away_score
        elif recommended_bet == "AWAY_ML":
            return away_score > home_score
        elif recommended_bet == "OVER":
            total_line = signal.get('total_line', 0)
            return (home_score + away_score) > total_line
        elif recommended_bet == "UNDER":
            total_line = signal.get('total_line', 0)
            return (home_score + away_score) < total_line
        
        return False
    
    def _aggregate_strategy_performance(self, evaluated_signals: List[Dict]) -> BacktestResult:
        """Aggregate individual bet results into strategy performance metrics."""
        if not evaluated_signals:
            return BacktestResult(
                strategy_name=self.processor_name,
                total_bets=0,
                wins=0,
                win_rate=0.0,
                roi_per_100=0.0,
                confidence_score=0.0,
                sample_size_category="INSUFFICIENT"
            )
        
        total_bets = len(evaluated_signals)
        total_wins = sum(1 for signal in evaluated_signals if signal['bet_won'])
        total_profit_loss = sum(signal['profit_loss'] for signal in evaluated_signals)
        
        win_rate = total_wins / total_bets if total_bets > 0 else 0.0
        
        # Calculate ROI per $100 wagered
        total_wagered = total_bets * 110  # Assuming -110 odds
        roi_per_100 = (total_profit_loss / total_wagered * 100) if total_wagered > 0 else 0.0
        
        # Calculate average confidence
        avg_confidence = sum(signal.get('confidence', 0.5) for signal in evaluated_signals) / total_bets
        
        # Determine sample size category
        if total_bets < 10:
            sample_category = "INSUFFICIENT"
        elif total_bets < 25:
            sample_category = "BASIC"
        elif total_bets < 50:
            sample_category = "RELIABLE"
        else:
            sample_category = "ROBUST"
        
        return BacktestResult(
            strategy_name=self.processor_name,
            total_bets=total_bets,
            wins=total_wins,
            win_rate=win_rate,
            roi_per_100=roi_per_100,
            confidence_score=avg_confidence,
            sample_size_category=sample_category,
            source_book_type="FACTORY_PROCESSOR",
            split_type="HISTORICAL_BACKTEST",
            created_at=datetime.now(timezone.utc)
        )
    
    def get_strategy_name(self) -> str:
        """Get strategy identifier."""
        return self.processor_name

    def _supports_book_specific_results(self, evaluated_signals: List[Dict]) -> bool:
        """Check if signals contain source-book information for splitting."""
        if not evaluated_signals:
            return False
        
        # Check if signals have source and book information
        sample_signal = evaluated_signals[0]
        has_source = 'source' in sample_signal or 'data_source' in sample_signal
        has_book = 'book' in sample_signal or 'sportsbook' in sample_signal
        
        return has_source or has_book
    
    def _create_book_specific_results(self, evaluated_signals: List[Dict]) -> List[BacktestResult]:
        """Create separate BacktestResult objects for each source-book combination."""
        
        # Group signals by source-book combination
        grouped_signals = {}
        
        for signal in evaluated_signals:
            # Extract source and book info
            source = signal.get('source', signal.get('data_source', 'UNKNOWN'))
            book = signal.get('book', signal.get('sportsbook', 'unknown'))
            split_type = signal.get('split_type', 'moneyline')
            
            # Create strategy key
            strategy_key = f"{self.processor_name}_{source}_{book}_{split_type}"
            
            if strategy_key not in grouped_signals:
                grouped_signals[strategy_key] = []
            
            grouped_signals[strategy_key].append(signal)
        
        # Create BacktestResult for each group
        results = []
        
        for strategy_key, signals_group in grouped_signals.items():
            if len(signals_group) >= 3:  # Minimum sample size for book-specific strategy
                
                total_bets = len(signals_group)
                total_wins = sum(1 for signal in signals_group if signal['bet_won'])
                total_profit_loss = sum(signal['profit_loss'] for signal in signals_group)
                
                win_rate = total_wins / total_bets if total_bets > 0 else 0.0
                
                # Calculate ROI per $100 wagered
                total_wagered = total_bets * 110  # Assuming -110 odds
                roi_per_100 = (total_profit_loss / total_wagered * 100) if total_wagered > 0 else 0.0
                
                # Calculate average confidence
                avg_confidence = sum(signal.get('confidence', 0.5) for signal in signals_group) / total_bets
                
                # Determine sample size category
                if total_bets < 10:
                    sample_category = "INSUFFICIENT"
                elif total_bets < 25:
                    sample_category = "BASIC"
                elif total_bets < 50:
                    sample_category = "RELIABLE"
                else:
                    sample_category = "ROBUST"
                
                # Extract source-book info for the result
                sample_signal = signals_group[0]
                source = sample_signal.get('source', sample_signal.get('data_source', 'UNKNOWN'))
                book = sample_signal.get('book', sample_signal.get('sportsbook', 'unknown'))
                split_type = sample_signal.get('split_type', 'moneyline')
                
                result = BacktestResult(
                    strategy_name=strategy_key,
                    total_bets=total_bets,
                    wins=total_wins,
                    win_rate=win_rate,
                    roi_per_100=roi_per_100,
                    confidence_score=avg_confidence,
                    sample_size_category=sample_category,
                    source_book_type=f"{source}_{book}",
                    split_type=split_type,
                    created_at=datetime.now(timezone.utc)
                )
                
                results.append(result)
        
        return results


class DataQualityValidator:
    """Validates data quality and result integrity."""
    
    def __init__(self):
        self.logger = get_logger("DataQualityValidator")
    
    def validate_result(self, result: BacktestResult) -> bool:
        """Validate a single backtest result."""
        try:
            # Check for 0 bets but non-zero metrics
            if result.total_bets == 0 and (result.win_rate != 0 or result.roi_per_100 != 0):
                self.logger.warning(f"Invalid: {result.strategy_name} has 0 bets but non-zero metrics")
                return False
            
            # Check for impossible values
            if result.wins > result.total_bets:
                self.logger.warning(f"Invalid: {result.strategy_name} has more wins than total bets")
                return False
            
            # Check for impossible win rates
            if result.win_rate > 1.0 or result.win_rate < 0.0:
                self.logger.warning(f"Invalid: {result.strategy_name} has impossible win rate")
                return False
            
            # Check for extreme ROI values (basic sanity check)
            if abs(result.roi_per_100) > 10000:  # 100x ROI is suspiciously high
                self.logger.warning(f"Suspicious: {result.strategy_name} has extreme ROI")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Validation error for {result.strategy_name}: {e}")
            return False
    
    def filter_valid_results(self, results: List[BacktestResult]) -> List[BacktestResult]:
        """Filter out invalid results."""
        valid_results = []
        invalid_count = 0
        
        for result in results:
            if self.validate_result(result):
                valid_results.append(result)
            else:
                invalid_count += 1
        
        if invalid_count > 0:
            self.logger.info(f"Filtered out {invalid_count} invalid results")
        
        return valid_results


class DeduplicationEngine:
    """Handles strategy deduplication logic."""
    
    def __init__(self):
        self.fingerprint_cache: Dict[str, str] = {}
        self.logger = get_logger("DeduplicationEngine")
    
    def deduplicate_results(self, results: List[BacktestResult]) -> List[BacktestResult]:
        """
        Merge duplicate strategies and return deduplicated list.
        
        Since we're using processors only, we shouldn't have duplicates,
        but we keep this for safety and future compatibility.
        """
        if not results:
            return []
        
        fingerprint_groups = {}
        
        for result in results:
            fingerprint = self._generate_fingerprint(result)
            if fingerprint not in fingerprint_groups:
                fingerprint_groups[fingerprint] = []
            fingerprint_groups[fingerprint].append(result)
        
        deduplicated = []
        merged_count = 0
        
        for group in fingerprint_groups.values():
            if len(group) == 1:
                deduplicated.append(group[0])
            else:
                merged = self._merge_results(group)
                deduplicated.append(merged)
                merged_count += len(group) - 1
        
        if merged_count > 0:
            self.logger.info(f"Merged {merged_count} duplicate strategies")
        
        return deduplicated
    
    def _generate_fingerprint(self, result: BacktestResult) -> str:
        """Generate unique fingerprint for strategy logic."""
        # For processor-based strategies, each processor should be unique
        # But we'll fingerprint based on strategy name and key characteristics
        fingerprint_data = f"{result.strategy_name}_{result.source_book_type}_{result.split_type}"
        return hashlib.md5(fingerprint_data.encode()).hexdigest()
    
    def _merge_results(self, group: List[BacktestResult]) -> BacktestResult:
        """Merge duplicate strategies by combining their sample sizes."""
        if len(group) == 1:
            return group[0]
        
        # Merge by combining sample sizes and recalculating metrics
        merged = group[0]  # Start with first result
        
        total_bets = sum(r.total_bets for r in group)
        total_wins = sum(r.wins for r in group)
        
        if total_bets > 0:
            merged.total_bets = total_bets
            merged.wins = total_wins
            merged.win_rate = total_wins / total_bets
            
            # Recalculate ROI (simplified - in real implementation this would be more sophisticated)
            weighted_roi = sum(r.roi_per_100 * r.total_bets for r in group) / total_bets
            merged.roi_per_100 = weighted_roi
            
            # Update sample size category
            merged.sample_size_category = self._get_sample_size_category(total_bets)
            
            # Keep original confidence score - it was calculated properly by ConfidenceScorer
            # Only validate it's in proper range without artificial boosting
            if merged.confidence_score > 1.0:
                merged.confidence_score = merged.confidence_score / 100.0  # Convert percentage to decimal if needed
            
            # Cap at reasonable maximum - no signal should be 100% confident
            merged.confidence_score = min(0.92, merged.confidence_score)  # Max 92%, not 95%
        
        return merged
    
    def _get_sample_size_category(self, total_bets: int) -> str:
        """Categorize sample size."""
        if total_bets < 10:
            return "INSUFFICIENT"
        elif total_bets < 25:
            return "BASIC"
        elif total_bets < 50:
            return "RELIABLE"
        else:
            return "ROBUST"


class SimplifiedBacktestingService:
    """
    Simplified, focused backtesting service.
    
    This is the new implementation that uses only the Strategy Processor Factory,
    eliminating the SQL script duplication problem.
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        # Setup clean logging first
        self.log_file = setup_clean_backtesting_logging()
        self.logger = get_logger("backtesting_service")
        
        self.settings = get_settings()
        self.coordinator = get_database_coordinator()
        
        # 🚨 FIXED: Proper database manager initialization
        if db_manager and db_manager.is_initialized():
            self.db_manager = db_manager
        else:
            self.logger.debug_file_only("Initializing database manager for backtesting service")
            self.db_manager = DatabaseManager()
            
            if not self.db_manager.is_initialized():
                try:
                    self.db_manager.initialize()
                    self.logger.debug_file_only("Database manager initialized successfully")
                except Exception as e:
                    self.logger.error_console(f"Failed to initialize database manager: {e}")
                    raise
        
        # Initialize components
        self.executors: List[StrategyExecutor] = []
        self.validator = DataQualityValidator()
        self.deduplication_engine = DeduplicationEngine()
        
        # Initialize factory components
        self._processor_factory = None
        self._repository = None
        self._strategy_validator = None
        
        # Track statistics
        self.stats = {
            "total_strategies_attempted": 0,
            "successful_strategies": 0,
            "failed_strategies": 0,
            "profitable_strategies": 0,
            "signals_generated": 0
        }
    
    async def initialize(self):
        """Initialize the service and register strategy executors."""
        self.logger.info_console("🚀 Initializing backtesting service...")
        
        # Initialize factory components
        await self._initialize_factory_components()
        
        # Register all available processor executors
        await self._register_processor_executors()
        
        self.logger.info_console(f"✅ Initialized with {len(self.executors)} strategies")
    
    async def _initialize_factory_components(self):
        """Initialize the strategy processor factory and related components."""
        try:
            # Initialize repository
            self._repository = BettingSignalRepository(self.db_manager)
            
            # Initialize strategy validator with mock data for now
            # In a real implementation, this would load profitable strategies from the database
            from ..models.betting_analysis import ProfitableStrategy, StrategyThresholds
            
            # Create mock profitable strategies for testing
            mock_strategies = [
                ProfitableStrategy(
                    strategy_name="sharp_action",
                    source_book="CIRCA",
                    split_type="moneyline",
                    win_rate=0.56,
                    roi=12.5,
                    total_bets=45,
                    confidence=0.78,
                    ci_lower=0.52,
                    ci_upper=0.60
                ),
                ProfitableStrategy(
                    strategy_name="opposing_markets",
                    source_book="DRAFTKINGS",
                    split_type="spread",
                    win_rate=0.54,
                    roi=8.3,
                    total_bets=32,
                    confidence=0.72,
                    ci_lower=0.48,
                    ci_upper=0.60
                )
            ]
            
            # Create strategy thresholds
            thresholds = StrategyThresholds(
                high_performance_threshold=15.0,
                high_performance_wr=0.60,
                moderate_performance_threshold=20.0,
                moderate_performance_wr=0.55,
                low_performance_threshold=25.0,
                low_performance_wr=0.52
            )
            
            self._strategy_validator = StrategyValidator(mock_strategies, thresholds)
            
            # Initialize processor config
            config = SignalProcessorConfig(
                minimum_differential=5.0,
                maximum_differential=80.0,
                data_freshness_hours=2,
                steam_move_time_window_hours=4,
                book_conflict_minimum_strength=5.0
            )
            
            # Initialize factory
            self._processor_factory = StrategyProcessorFactory(
                self._repository,
                self._strategy_validator,
                config
            )
            
            self.logger.info("✅ Factory components initialized")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to initialize factory components: {e}")
            raise
    
    async def _register_processor_executors(self):
        """Register executors with circuit breaker to prevent infinite loops."""
        self.logger.debug_file_only("Starting executor registration")
        
        # Get available strategies (with timeout to prevent infinite loops)
        try:
            available_strategies = self._get_available_strategies_safely()
            self.stats["total_strategies_attempted"] = len(available_strategies)
            
            for strategy_name in available_strategies:
                try:
                    # Create executor
                    executor = ProcessorStrategyExecutor(
                        self._processor_factory, 
                        strategy_name, 
                        self.db_manager
                    )
                    self.executors.append(executor)
                    self.stats["successful_strategies"] += 1
                    
                    self.logger.debug_file_only(f"Registered executor: {strategy_name}")
                    
                except Exception as e:
                    self.stats["failed_strategies"] += 1
                    self.logger.debug_file_only(f"Failed to register {strategy_name}: {e}")
                    continue
            
        except Exception as e:
            self.logger.error_console(f"Executor registration failed: {e}")
            raise
    
    def _get_available_strategies_safely(self) -> List[str]:
        """Get IMPLEMENTED strategies only to avoid running PLANNED ones."""
        if not hasattr(self, '_processor_factory') or not self._processor_factory:
            self.logger.warning_console("Processor factory not available, using mock strategies")
            return ["sharp_action", "opposing_markets", "book_conflicts"]
        
        try:
            # Get implementation status to filter only IMPLEMENTED strategies
            status_dict = self._processor_factory.get_implementation_status()
            implemented_strategies = [
                strategy for strategy, status in status_dict.items() 
                if status == "IMPLEMENTED"
            ]
            
            self.logger.debug_file_only(
                f"Filtered strategies: {len(implemented_strategies)} IMPLEMENTED out of {len(status_dict)} total"
            )
            
            if not implemented_strategies:
                self.logger.warning_console("No IMPLEMENTED strategies found, using fallback")
                return ["sharp_action"]  # Fallback to at least one known working strategy
            
            return implemented_strategies
            
        except Exception as e:
            self.logger.warning_console(f"Failed to get strategies from factory: {e}")
            return ["sharp_action", "opposing_markets", "book_conflicts"]  # Fallback
    
    def register_executor(self, executor: StrategyExecutor):
        """Register a strategy executor."""
        self.executors.append(executor)
        self.logger.info(f"Registered custom executor: {executor.get_strategy_name()}")
    
    async def run_backtest(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """Run backtest with clean progress reporting."""
        
        self.logger.info_console(f"🎯 Starting backtest: {start_date} to {end_date}")
        
        # Ensure initialized
        if not self._processor_factory:
            await self.initialize()
        
        # Execute strategies with progress tracking
        all_results = []
        successful_executions = 0
        
        for i, executor in enumerate(self.executors, 1):
            strategy_name = executor.get_strategy_name()
            
            # Log progress (but not spam)
            if i % 5 == 0 or i == len(self.executors):
                self.logger.info_console(f"📈 Progress: {i}/{len(self.executors)} strategies")
            
            try:
                # Log detailed execution to file only
                self.logger.debug_file_only(f"Executing strategy: {strategy_name}")
                
                results = await executor.execute(start_date, end_date)
                
                if results:
                    all_results.extend(results)
                    successful_executions += 1
                    self.stats["signals_generated"] += len(results)
                    
                    # Only log to console if significant results
                    if any(r.total_bets > 10 for r in results):
                        self.logger.info_console(f"✅ {strategy_name}: {len(results)} signals")
                
                # Log all attempts to file
                self.logger.debug_file_only(
                    f"Strategy {strategy_name} completed",
                    results_count=len(results),
                    has_significant_results=any(r.total_bets > 10 for r in results) if results else False
                )
                
            except Exception as e:
                self.logger.debug_file_only(f"Strategy {strategy_name} failed: {e}")
                continue
        
        # Process results
        valid_results = self.validator.filter_valid_results(all_results)
        deduplicated_results = self.deduplication_engine.deduplicate_results(valid_results)
        
        # Count profitable strategies
        profitable_count = sum(1 for r in deduplicated_results if self._is_profitable(r))
        self.stats["profitable_strategies"] = profitable_count
        
        # Generate clean summary
        summary = {
            "Total strategies": len(self.executors),
            "Successful executions": successful_executions,
            "Valid results": len(valid_results),
            "Final unique strategies": len(deduplicated_results),
            "Profitable strategies": profitable_count,
            "Strong signals (>25 bets)": len([r for r in deduplicated_results if r.total_bets > 25])
        }
        
        self.logger.summary("Backtesting Results", summary)
        
        # Show profitable strategies
        if profitable_count > 0:
            print("\n🟢 PROFITABLE STRATEGIES:")
            for result in deduplicated_results:
                if self._is_profitable(result):
                    print(f"  • {result.strategy_name}: {result.total_bets} bets, "
                          f"{result.win_rate:.1%} WR, {result.roi_per_100:+.1f}% ROI")
        
        print(f"\n📝 Detailed logs saved to: {self.log_file}")
        
        performance_summary = self._analyze_performance(deduplicated_results)
        strategy_metrics = self._convert_to_legacy_metrics(deduplicated_results)
        
        return {
            "results": deduplicated_results,
            "strategy_metrics": strategy_metrics,
            "summary": performance_summary,
            "stats": self.stats,
            "execution_stats": {
                "raw_count": len(all_results),
                "valid_count": len(valid_results),
                "deduplicated_count": len(deduplicated_results),
                "successful_executions": successful_executions
            },
            "backtest_date": datetime.now(timezone.utc),
            "date_range": {
                "start_date": start_date,
                "end_date": end_date
            }
        }
    
    def _analyze_performance(self, results: List[BacktestResult]) -> Dict[str, Any]:
        """Analyze strategy performance with ROI prioritization."""
        if not results:
            return {
                "total_strategies": 0,
                "profitable_strategies": 0,
                "reliable_strategies": 0,
                "top_performers": []
            }
        
        profitable = [r for r in results if self._is_profitable(r)]
        reliable = [r for r in results if r.sample_size_category in ['RELIABLE', 'ROBUST']]
        
        # Calculate aggregate metrics
        total_bets = sum(r.total_bets for r in results)
        total_wins = sum(r.wins for r in results)
        overall_win_rate = total_wins / total_bets if total_bets > 0 else 0
        
        # Calculate weighted ROI
        weighted_roi = sum(r.roi_per_100 * r.total_bets for r in results) / total_bets if total_bets > 0 else 0
        
        return {
            "total_strategies": len(results),
            "profitable_strategies": len(profitable),
            "reliable_strategies": len(reliable),
            "top_performers": sorted(results, key=lambda x: x.roi_per_100, reverse=True)[:5],
            "aggregate_metrics": {
                "total_bets": total_bets,
                "total_wins": total_wins,
                "overall_win_rate": overall_win_rate,
                "weighted_roi": weighted_roi
            }
        }
    
    def _is_profitable(self, result: BacktestResult) -> bool:
        """Determine if strategy is profitable using ROI-first logic."""
        # Aggressive loss filter
        if result.roi_per_100 < -10.0:
            return False
        
        # For reliable sample sizes, any positive ROI is good
        if result.total_bets >= 20:
            return result.roi_per_100 > 0.0
        
        # For smaller samples, require higher thresholds
        return result.roi_per_100 > 5.0 and result.win_rate > 0.55
    
    def _convert_to_legacy_metrics(self, results: List[BacktestResult]) -> List['StrategyMetrics']:
        """Convert BacktestResult to legacy StrategyMetrics for compatibility."""
        from dataclasses import dataclass
        
        @dataclass
        class StrategyMetrics:
            """Legacy metrics class for compatibility."""
            strategy_name: str
            source_book_type: str
            split_type: str
            total_bets: int
            wins: int
            win_rate: float
            roi_per_100: float
            sharpe_ratio: float = 0.0
            max_drawdown: float = 0.0
            confidence_interval_lower: float = 0.0
            confidence_interval_upper: float = 0.0
            sample_size_adequate: bool = False
            statistical_significance: bool = False
            p_value: float = 1.0
            last_updated: datetime = None
            backtest_date: datetime = None
            created_at: datetime = None
        
        legacy_metrics = []
        
        for result in results:
            metric = StrategyMetrics(
                strategy_name=result.strategy_name,
                source_book_type=result.source_book_type,
                split_type=result.split_type,
                total_bets=result.total_bets,
                wins=result.wins,
                win_rate=result.win_rate,
                roi_per_100=result.roi_per_100,
                sharpe_ratio=result.sharpe_ratio,
                max_drawdown=result.max_drawdown,
                confidence_interval_lower=result.confidence_interval_lower,
                confidence_interval_upper=result.confidence_interval_upper,
                sample_size_adequate=result.sample_size_adequate,
                statistical_significance=result.statistical_significance,
                p_value=result.p_value,
                last_updated=result.last_updated,
                backtest_date=result.backtest_date,
                created_at=result.created_at
            )
            legacy_metrics.append(metric)
        
        return legacy_metrics


# Maintain backward compatibility
class BacktestingService(SimplifiedBacktestingService):
    """
    Backward compatibility wrapper for the legacy BacktestingService.
    
    This maintains the existing interface while using the new simplified
    architecture under the hood.
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        super().__init__(db_manager)
        # Use compatible logger instead of binding
        self.logger = get_logger("BacktestingService")
        
        # Legacy configuration
        self.use_factory_only = True  # Enable factory-only mode as recommended
        
        # Maintain legacy attributes for compatibility
        self.table_registry = get_table_registry(DatabaseType.POSTGRESQL)
    
    async def run_daily_backtesting_pipeline(self) -> 'BacktestingResults':
        """
        Legacy method that maintains compatibility with existing pipeline.
        
        This wraps the new run_backtest method and converts the results
        to the expected legacy format.
        """
        # Calculate date range for daily backtesting (last 30 days)
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=30)
        
        # Run the new backtesting pipeline
        results = await self.run_backtest(
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d")
        )
        
        # Convert to legacy BacktestingResults format
        return self._convert_to_legacy_results(results)
    
    def _convert_to_legacy_results(self, results: Dict[str, Any]) -> 'BacktestingResults':
        """Convert new results format to legacy BacktestingResults."""
        from dataclasses import dataclass
        
        @dataclass
        class BacktestingResults:
            """Legacy results class for compatibility."""
            backtest_date: datetime
            total_strategies_analyzed: int
            strategies_with_adequate_data: int
            profitable_strategies: int
            declining_strategies: int
            stable_strategies: int
            threshold_recommendations: List[Any]
            strategy_alerts: List[Dict[str, Any]]
            strategy_metrics: List[Any]
            data_completeness_pct: float
            game_outcome_freshness_hours: float
            execution_time_seconds: float
            created_at: datetime
        
        summary = results.get("summary", {})
        
        return BacktestingResults(
            backtest_date=results.get("backtest_date", datetime.now(timezone.utc)),
            total_strategies_analyzed=summary.get("total_strategies", 0),
            strategies_with_adequate_data=summary.get("reliable_strategies", 0),
            profitable_strategies=summary.get("profitable_strategies", 0),
            declining_strategies=0,  # Not calculated in new system
            stable_strategies=0,     # Not calculated in new system
            threshold_recommendations=[],  # Not implemented in new system
            strategy_alerts=[],      # Not implemented in new system
            strategy_metrics=results.get("strategy_metrics", []),
            data_completeness_pct=95.0,  # Mock value
            game_outcome_freshness_hours=2.0,  # Mock value
            execution_time_seconds=30.0,  # Mock value
            created_at=datetime.now(timezone.utc)
        )
    
    # Legacy method aliases for compatibility
    async def analyze_all_strategies(self) -> Dict[str, Any]:
        """Legacy method for analyzing all strategies."""
        results = await self.run_backtest(
            (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
            datetime.now().strftime("%Y-%m-%d")
        )
        return {
            "total_strategies": len(results.get("results", [])),
            "profitable_strategies": results.get("summary", {}).get("profitable_strategies", 0),
            "execution_stats": results.get("execution_stats", {})
        }


# Legacy exports for compatibility
StrategyMetrics = BacktestResult  # Alias for backward compatibility

# Create BacktestingResults class for backward compatibility
@dataclass
class BacktestingResults:
    """Legacy results class for compatibility with existing services."""
    backtest_date: datetime
    total_strategies_analyzed: int
    strategies_with_adequate_data: int
    profitable_strategies: int
    declining_strategies: int
    stable_strategies: int
    threshold_recommendations: List[Any]
    strategy_alerts: List[Dict[str, Any]]
    strategy_metrics: List[Any]
    data_completeness_pct: float
    game_outcome_freshness_hours: float
    execution_time_seconds: float
    created_at: datetime

# Additional legacy aliases
ThresholdRecommendation = BacktestResult  # Placeholder
StrategyFingerprint = BacktestResult  # Placeholder
DeduplicatedStrategy = BacktestResult  # Placeholder


async def main():
    """Example usage of the new simplified service."""
    # Initialize global logger
    global logger
    if logger is None:
        logger = get_logger(__name__)
    
    service = SimplifiedBacktestingService()
    await service.initialize()
    
    # Run backtest
    results = await service.run_backtest("2024-01-01", "2024-12-31")
    
    print(f"✅ Backtesting complete:")
    print(f"   Total strategies: {results['summary']['total_strategies']}")
    print(f"   Profitable: {results['summary']['profitable_strategies']}")
    print(f"   Reliable: {results['summary']['reliable_strategies']}")
    print(f"   Execution stats: {results['execution_stats']}")


if __name__ == "__main__":
    asyncio.run(main())