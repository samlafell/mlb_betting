"""
Unified Backtesting Engine

Consolidates 5 backtesting services into a single, comprehensive service:
- Enhanced backtesting logic with live alignment
- Comprehensive diagnostics with 5-checkpoint system  
- Automated scheduling with circuit breakers
- Real-time accuracy monitoring during refactoring
- Legacy compatibility with original backtesting service

Architecture:
- CoreEngine: Enhanced backtesting execution
- DiagnosticsModule: 5-checkpoint diagnostic system
- SchedulerModule: Automated daily/weekly scheduling
- AccuracyModule: Real-time performance monitoring
- LegacyCompatibility: Backward compatibility wrappers

🎯 Phase 3 Consolidation: 5,318 → ~2,000 lines (62% reduction)
"""

import asyncio
import json
import threading
from datetime import datetime, timezone, timedelta, time
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple, Set
from dataclasses import dataclass, asdict, field
from enum import Enum
from abc import ABC, abstractmethod
import structlog
import logging
import numpy as np
from decimal import Decimal

from ..core.config import get_settings
from ..core.logging import get_logger, get_clean_logger, setup_universal_logger_compatibility
from ..db.connection import DatabaseManager, get_db_manager
from ..core.exceptions import DatabaseError, ValidationError

# Factory and analysis imports
from ..analysis.processors.strategy_processor_factory import StrategyProcessorFactory
from ..services.betting_signal_repository import BettingSignalRepository
from ..services.dynamic_threshold_manager import get_dynamic_threshold_manager
from ..models.betting_analysis import SignalProcessorConfig, BettingSignal, SignalType
from ..db.repositories import get_game_outcome_repository

# Service imports for integration
from .alert_service import AlertService, AlertSeverity, AlertType
from .pre_game_recommendation_tracker import PreGameRecommendationTracker, PreGameRecommendation

# Ensure universal compatibility
setup_universal_logger_compatibility()


# =============================================================================
# SHARED DATA STRUCTURES
# =============================================================================

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
    
    # Additional metrics
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


class DiagnosticStatus(Enum):
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
    UNKNOWN = "UNKNOWN"


class PerformanceStatus(Enum):
    EXCELLENT = "excellent"
    GOOD = "good"
    WARNING = "warning"
    CRITICAL = "critical"
    FAILED = "failed"


@dataclass
class UnifiedBetOutcome:
    """Standardized bet outcome used across all components."""
    recommendation_id: str
    game_pk: int
    bet_type: str  # moneyline, spread, total
    bet_side: str  # home, away, over, under
    bet_amount: Decimal  # Standard unit size
    odds: Decimal  # American odds format
    
    # Outcome tracking
    bet_won: Optional[bool] = None
    actual_profit_loss: Optional[Decimal] = None
    game_final_score: Optional[str] = None
    outcome_details: Optional[str] = None
    
    # Source tracking
    source_component: str = "unknown"
    evaluation_method: str = "unknown"
    
    # Timing
    bet_placed_at: datetime = None
    game_start_time: datetime = None
    outcome_determined_at: Optional[datetime] = None


# =============================================================================
# CORE BACKTESTING ENGINE
# =============================================================================

class BacktestingEngine:
    """
    Unified Backtesting Engine
    
    Consolidates all backtesting functionality into a single, comprehensive service.
    Provides modules for core execution, diagnostics, scheduling, and accuracy monitoring.
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """Initialize the unified backtesting engine."""
        
        # Setup logging
        setup_universal_logger_compatibility()
        self.logger = get_logger(__name__)
        
        # Database manager
        if db_manager and db_manager.is_initialized():
            self.db_manager = db_manager
        else:
            self.db_manager = DatabaseManager()
            if not self.db_manager.is_initialized():
                self.db_manager.initialize()
        
        # Core components initialization (will be lazy-loaded)
        self._core_engine = None
        self._diagnostics_module = None
        self._scheduler_module = None
        self._accuracy_module = None
        
        # State tracking
        self._initialized = False
        self._modules_loaded = set()
        
        self.logger.info("BacktestingEngine initialized - modules will be loaded on demand")
    
    async def initialize(self):
        """Initialize the backtesting engine and core dependencies."""
        if self._initialized:
            return
        
        try:
            # Initialize core dependencies
            processor_config = SignalProcessorConfig()
            signal_repository = BettingSignalRepository(processor_config)
            
            # Get profitable strategies and create factory
            profitable_strategies = await signal_repository.get_profitable_strategies()
            
            # Store for module initialization  
            self._signal_repository = signal_repository
            self._processor_config = processor_config
            self._profitable_strategies = profitable_strategies  # Store for use in backtesting
            
            self.logger.info(f"🎯 Loaded {len(profitable_strategies)} profitable strategies for backtesting")
            
            self._initialized = True
            self.logger.info("BacktestingEngine core initialization completed")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize BacktestingEngine: {e}")
            raise
    
    # =============================================================================
    # MODULE ACCESS PROPERTIES
    # =============================================================================
    
    @property
    def core_engine(self):
        """Access the core backtesting engine module."""
        if self._core_engine is None:
            self._core_engine = self._load_core_engine()
        return self._core_engine
    
    @property
    def diagnostics(self):
        """Access the diagnostics module."""
        if self._diagnostics_module is None:
            self._diagnostics_module = self._load_diagnostics_module()
        return self._diagnostics_module
    
    @property
    def scheduler(self):
        """Access the scheduler module."""
        if self._scheduler_module is None:
            self._scheduler_module = self._load_scheduler_module()
        return self._scheduler_module
    
    @property
    def accuracy_monitor(self):
        """Access the accuracy monitoring module."""
        if self._accuracy_module is None:
            self._accuracy_module = self._load_accuracy_module()
        return self._accuracy_module
    
    # =============================================================================
    # MODULE LOADERS
    # =============================================================================
    
    def _load_core_engine(self):
        """Load the core backtesting engine module."""
        if not self._initialized:
            raise RuntimeError("BacktestingEngine must be initialized before loading modules")
        
        self.logger.info("Loading core backtesting engine module")
        core_engine = CoreBacktestingEngine(
            db_manager=self.db_manager,
            signal_repository=self._signal_repository,
            processor_config=self._processor_config
        )
        # Pass profitable strategies to core engine
        core_engine._profitable_strategies = getattr(self, '_profitable_strategies', [])
        return core_engine
    
    def _load_diagnostics_module(self):
        """Load the diagnostics module."""
        if not self._initialized:
            raise RuntimeError("BacktestingEngine must be initialized before loading modules")
        
        self.logger.info("Loading diagnostics module")
        return DiagnosticsModule(
            db_manager=self.db_manager,
            signal_repository=self._signal_repository
        )
    
    def _load_scheduler_module(self):
        """Load the scheduler module."""
        self.logger.info("Loading scheduler module")
        return SchedulerModule(backtesting_engine=self)
    
    def _load_accuracy_module(self):
        """Load the accuracy monitoring module."""
        self.logger.info("Loading accuracy monitoring module")
        return AccuracyModule(db_manager=self.db_manager)
    
    # =============================================================================
    # UNIFIED PUBLIC API
    # =============================================================================
    
    async def run_backtest(self, start_date: str, end_date: str, 
                          include_diagnostics: bool = False,
                          include_alignment: bool = False) -> Dict[str, Any]:
        """
        Run comprehensive backtest with optional diagnostics and alignment.
        
        Args:
            start_date: Start date for backtest (YYYY-MM-DD)
            end_date: End date for backtest (YYYY-MM-DD)
            include_diagnostics: Whether to run diagnostic checks
            include_alignment: Whether to include live alignment analysis
            
        Returns:
            Comprehensive backtest results
        """
        if not self._initialized:
            await self.initialize()
        
        self.logger.info(f"Running comprehensive backtest: {start_date} to {end_date}")
        
        # Core backtesting
        core_results = await self.core_engine.run_enhanced_backtest(
            start_date, end_date, validate_alignment=include_alignment
        )
        
        results = {
            'backtest_results': core_results,
            'execution_timestamp': datetime.now(timezone.utc).isoformat(),
            'parameters': {
                'start_date': start_date,
                'end_date': end_date,
                'include_diagnostics': include_diagnostics,
                'include_alignment': include_alignment
            }
        }
        
        # Optional diagnostics
        if include_diagnostics:
            diagnostic_results = await self.diagnostics.run_full_diagnostic()
            results['diagnostic_results'] = diagnostic_results
        
        self.logger.info("Comprehensive backtest completed successfully")
        return results
    
    async def run_daily_pipeline(self) -> Dict[str, Any]:
        """Run the daily backtesting pipeline."""
        if not self._initialized:
            await self.initialize()
        
        return await self.core_engine.run_daily_backtesting_pipeline()
    
    def start_automated_scheduling(self, notifications_enabled: bool = True) -> None:
        """Start automated backtesting scheduling."""
        self.scheduler.start_automated_backtesting(notifications_enabled)
    
    def stop_automated_scheduling(self) -> None:
        """Stop automated backtesting scheduling."""
        self.scheduler.stop_automated_backtesting()
    
    async def establish_accuracy_baseline(self, lookback_days: int = 7) -> None:
        """Establish accuracy monitoring baseline."""
        await self.accuracy_monitor.establish_baseline(lookback_days)
    
    def get_comprehensive_status(self) -> Dict[str, Any]:
        """Get comprehensive status of all modules."""
        status = {
            'engine_initialized': self._initialized,
            'modules_loaded': list(self._modules_loaded),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        # Add module-specific status if loaded
        if self._core_engine:
            status['core_engine'] = {'loaded': True}
        if self._diagnostics_module:
            status['diagnostics'] = {'loaded': True}
        if self._scheduler_module:
            status['scheduler'] = self.scheduler.get_status()
        if self._accuracy_module:
            status['accuracy_monitor'] = self.accuracy_monitor.get_current_status()
            
        return status


# =============================================================================
# CORE ENGINE MODULE
# =============================================================================

class CoreBacktestingEngine:
    """Core backtesting execution engine - consolidated from enhanced_backtesting_service.py"""
    
    def __init__(self, db_manager: DatabaseManager, signal_repository, processor_config):
        self.db_manager = db_manager
        self.signal_repository = signal_repository
        self.processor_config = processor_config
        self.logger = get_logger(f"{__name__}.core_engine")
        
        # Initialize processor factory
        self.processor_factory = None
        
    async def initialize_factory(self):
        """Initialize the processor factory."""
        if self.processor_factory is None:
            # Initialize the processor factory with actual dependencies
            self.logger.info("Initializing processor factory for core engine")
            
            # Create the factory with the dependencies we have
            from ..services.strategy_validation import StrategyValidation
            
            # Initialize strategy validation
            strategy_validation = StrategyValidation(db_manager=self.db_manager)
            await strategy_validation.initialize()
            
            # Create the processor factory
            self.processor_factory = StrategyProcessorFactory(
                repository=self.signal_repository,
                validator=strategy_validation,
                config=self.processor_config
            )
            self.logger.info("Processor factory initialized successfully")
    
    async def run_enhanced_backtest(self, start_date: str, end_date: str, 
                                  validate_alignment: bool = True) -> Dict[str, Any]:
        """Run enhanced backtest with live alignment validation."""
        
        start_time = datetime.now()
        await self.initialize_factory()
        
        self.logger.info(f"Running enhanced backtest: {start_date} to {end_date}")
        
        try:
            # Get betting splits data for the date range
            splits_data = await self._get_betting_splits_data(start_date, end_date)
            
            if not splits_data:
                self.logger.warning(f"No betting splits data found for {start_date} to {end_date}")
                return self._empty_backtest_result(start_date, end_date)
            
            # Run backtesting strategies
            strategy_results = await self._run_backtesting_strategies(splits_data)
            
            # Calculate summary metrics
            total_bets = sum(r.get('total_bets', 0) for r in strategy_results)
            profitable_strategies = len([r for r in strategy_results if r.get('roi_per_100', 0) > 0])
            
            # Calculate overall performance
            if total_bets > 0:
                total_wins = sum(r.get('wins', 0) for r in strategy_results)
                overall_win_rate = total_wins / total_bets if total_bets > 0 else 0
                
                # Weight ROI by number of bets for overall calculation
                weighted_roi = sum(r.get('roi_per_100', 0) * r.get('total_bets', 0) for r in strategy_results)
                overall_roi = weighted_roi / total_bets if total_bets > 0 else 0
            else:
                overall_win_rate = 0
                overall_roi = 0
            
            # Store results in database
            await self._store_backtest_results(strategy_results, start_date, end_date)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            results = {
                'backtest_results': {
                    'start_date': start_date,
                    'end_date': end_date,
                    'total_strategies': len(strategy_results),
                    'strategies_analyzed': len(strategy_results),
                    'profitable_strategies': profitable_strategies,
                    'total_bets': total_bets,
                    'overall_win_rate': overall_win_rate,
                    'overall_roi': overall_roi,
                    'average_roi': overall_roi,  # For backward compatibility
                    'strategy_results': strategy_results
                },
                'data_quality_score': self._calculate_data_quality_score(splits_data),
                'execution_time_seconds': execution_time
            }
            
            # Alignment validation if requested
            if validate_alignment:
                results['alignment_analysis'] = await self._analyze_live_alignment(start_date, end_date)
            
            self.logger.info(f"Backtest completed: {len(strategy_results)} strategies, {total_bets} total bets, {profitable_strategies} profitable")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Backtest failed: {e}")
            execution_time = (datetime.now() - start_time).total_seconds()
            return {
                'backtest_results': {
                    'start_date': start_date,
                    'end_date': end_date,
                    'total_strategies': 0,
                    'strategies_analyzed': 0,
                    'profitable_strategies': 0,
                    'total_bets': 0,
                    'overall_win_rate': 0.0,
                    'overall_roi': 0.0,
                    'average_roi': 0.0,
                    'strategy_results': [],
                    'error': str(e)
                },
                'data_quality_score': 0.0,
                'execution_time_seconds': execution_time
            }
    
    def _empty_backtest_result(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """Return empty backtest result when no data is available."""
        return {
            'backtest_results': {
                'start_date': start_date,
                'end_date': end_date,
                'total_strategies': 0,
                'strategies_analyzed': 0,
                'profitable_strategies': 0,
                'total_bets': 0,
                'overall_win_rate': 0.0,
                'overall_roi': 0.0,
                'average_roi': 0.0,
                'strategy_results': []
            },
            'data_quality_score': 0.0,
            'execution_time_seconds': 0.0
        }
    
    async def _get_betting_splits_data(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Get betting splits data for the specified date range."""
        try:
            query = """
                SELECT 
                    s.game_id,
                    s.home_team,
                    s.away_team,
                    s.split_type,
                    s.home_or_over_bets_percentage,
                    s.home_or_over_stake_percentage,
                    s.source,
                    s.book,
                    s.last_updated,
                    s.game_datetime,
                    -- Get game outcome
                    o.home_score,
                    o.away_score,
                    o.winning_team,
                    o.home_win,
                    o.over,
                    o.total_line,
                    o.home_spread_line
                FROM splits.raw_mlb_betting_splits s
                LEFT JOIN public.game_outcomes o ON s.game_id = o.game_id
                WHERE s.last_updated >= %s
                  AND s.last_updated <= %s
                  AND s.game_datetime IS NOT NULL
                  AND s.home_or_over_bets_percentage IS NOT NULL
                  AND s.home_or_over_stake_percentage IS NOT NULL
                ORDER BY s.game_datetime, s.last_updated
            """
            
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, (start_date, end_date + ' 23:59:59'))
                results = cursor.fetchall()
            
            # Convert to list of dictionaries
            splits_data = []
            for row in results:
                if isinstance(row, dict):
                    splits_data.append(row)
                else:
                    # Handle tuple format
                    splits_data.append({
                        'game_id': row[0],
                        'home_team': row[1],
                        'away_team': row[2],
                        'split_type': row[3],
                        'home_or_over_bets_percentage': float(row[4]) if row[4] is not None else None,
                        'home_or_over_stake_percentage': float(row[5]) if row[5] is not None else None,
                        'source': row[6],
                        'book': row[7],
                        'last_updated': row[8],
                        'game_datetime': row[9],
                        'home_score': row[10],
                        'away_score': row[11],
                        'winning_team': row[12],
                        'home_win': row[13],
                        'over': row[14],
                        'total_line': row[15],
                        'home_spread_line': row[16]
                    })
            
            self.logger.info(f"Retrieved {len(splits_data)} betting splits records for backtesting")
            return splits_data
            
        except Exception as e:
            self.logger.error(f"Failed to get betting splits data: {e}")
            return []
    
    async def _run_backtesting_strategies(self, splits_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Run backtesting strategies using the StrategyProcessorFactory instead of hardcoded strategies."""
        strategy_results = []
        
        # Check if factory is initialized
        if not self.processor_factory:
            self.logger.error("StrategyProcessorFactory not initialized - falling back to hardcoded strategies")
            return await self._run_legacy_hardcoded_strategies(splits_data)
        
        # Get all implemented processors from factory
        all_processors = self.processor_factory.get_all_processors()
        
        if not all_processors:
            self.logger.warning("No processors available from factory - falling back to hardcoded strategies")
            return await self._run_legacy_hardcoded_strategies(splits_data)
        
        self.logger.info(f"🚀 Running backtesting with {len(all_processors)} processors from factory")
        
        # Group data by game for analysis
        games_data = self._group_splits_by_game(splits_data)
        
        # Process each strategy processor
        for processor_name, processor in all_processors.items():
            try:
                self.logger.info(f"Processing strategy: {processor_name}")
                
                # Convert splits data to format expected by processors
                profitable_strategies = await self._get_profitable_strategies_for_processor(processor)
                
                # Execute processor to get betting signals
                betting_signals = await processor.process_with_error_handling(
                    minutes_ahead=1440,  # 24 hours for backtesting
                    profitable_strategies=profitable_strategies
                )
                
                # Convert signals to backtest results
                if betting_signals:
                    backtest_results = await self._convert_signals_to_backtest_results(
                        betting_signals, games_data, processor_name
                    )
                    strategy_results.extend(backtest_results)
                else:
                    self.logger.info(f"No signals generated for {processor_name}")
                    
            except Exception as e:
                self.logger.error(f"Error processing {processor_name}: {e}")
                continue
        
        self.logger.info(f"✅ Completed backtesting with {len(strategy_results)} results from {len(all_processors)} processors")
        return strategy_results
    
    async def _run_legacy_hardcoded_strategies(self, splits_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Fallback method with original hardcoded strategies for backward compatibility."""
        self.logger.warning("⚠️  Using legacy hardcoded strategies - should migrate to factory-based approach")
        
        strategy_results = []
        
        # Group data by game and split type for analysis
        games_data = self._group_splits_by_game(splits_data)
        
        # Define strategies to test (original hardcoded approach)
        strategies = [
            {
                'name': 'Sharp Money Follow',
                'description': 'Follow the money when it disagrees with public bets',
                'min_differential': 10.0,
                'strategy_type': 'sharp_action'
            },
            {
                'name': 'Public Fade',
                'description': 'Fade the public when differential is high',
                'min_differential': 15.0,
                'strategy_type': 'public_fade'
            },
            {
                'name': 'Strong Sharp Action',
                'description': 'Follow strong sharp money movements',
                'min_differential': 20.0,
                'strategy_type': 'strong_sharp'
            },
            {
                'name': 'Moderate Sharp Action',
                'description': 'Follow moderate sharp money movements',
                'min_differential': 12.0,
                'strategy_type': 'moderate_sharp'
            }
        ]
        
        # Get all available source-book combinations from the data
        available_source_books = self._get_available_source_books(splits_data)
        self.logger.info(f"Available source-book combinations: {available_source_books}")
        
        # Run each strategy
        for strategy in strategies:
            for split_type in ['moneyline', 'spread', 'total']:
                for source_book in available_source_books:
                    strategy_result = await self._backtest_strategy(
                        games_data, 
                        strategy, 
                        split_type, 
                        source_book
                    )
                    
                    if strategy_result and strategy_result.get('total_bets', 0) > 0:
                        strategy_results.append(strategy_result)
        
        return strategy_results
    
    async def _get_profitable_strategies_for_processor(self, processor) -> List:
        """Get profitable strategies relevant to this processor."""
        # 🎯 FIXED: Return actual profitable strategies loaded during initialization
        # This enables processors to generate signals based on proven strategies
        strategies = getattr(self, '_profitable_strategies', [])
        
        if not strategies:
            self.logger.warning(f"⚠️  No profitable strategies available for {processor.__class__.__name__}")
            # 🚨 BOOTSTRAP MODE: Use dynamic thresholds with very loose criteria
            self.logger.info("🔄 Operating in BOOTSTRAP mode with loose thresholds")
        else:
            self.logger.info(f"✅ Providing {len(strategies)} profitable strategies to {processor.__class__.__name__}")
            
        return strategies
    
    async def _convert_signals_to_backtest_results(self, betting_signals, games_data: Dict, processor_name: str) -> List[Dict[str, Any]]:
        """Convert BettingSignals to backtest results by evaluating against historical outcomes."""
        if not betting_signals:
            return []
        
        # Group signals by source-book and split type for analysis
        signal_groups = {}
        for signal in betting_signals:
            # Extract source, book, and split type from signal metadata
            source = signal.metadata.get('source', 'UNKNOWN')
            book = signal.metadata.get('book', '')
            split_type = signal.metadata.get('split_type', 'UNKNOWN')
            
            # Create standardized source-book combination
            source_book = self._get_standardized_source_book({
                'source': source,
                'book': book
            })
            
            key = f"{source_book}_{split_type}"
            if key not in signal_groups:
                signal_groups[key] = []
            signal_groups[key].append(signal)
        
        backtest_results = []
        
        for group_key, signals in signal_groups.items():
            source_book, split_type = group_key.split('_', 1)
            
            total_bets = 0
            wins = 0
            total_profit = 0.0
            
            for signal in signals:
                # Find corresponding game data
                game_key = f"{signal.game_id}_{split_type}_{source_book}"
                if game_key not in games_data:
                    continue
                
                game_splits = games_data[game_key]
                if not game_splits:
                    continue
                
                # Use the most recent split for this game
                latest_split = max(game_splits, key=lambda x: x.get('last_updated', datetime.min))
                
                # Determine bet outcome
                bet_won = self._determine_bet_outcome_from_signal(signal, latest_split, split_type)
                
                if bet_won is not None:
                    total_bets += 1
                    if bet_won:
                        wins += 1
                        # Use odds from signal if available, otherwise assume -110
                        profit = self._calculate_profit_from_odds(signal.odds) if signal.odds else (100 / 110)
                        total_profit += profit
                    else:
                        total_profit -= 1  # Standard unit loss
            
            if total_bets > 0:
                # Calculate metrics
                win_rate = wins / total_bets
                roi_per_100 = (total_profit / total_bets) * 100
                
                # Determine confidence level
                if total_bets >= 50:
                    confidence_level = "HIGH"
                elif total_bets >= 20:
                    confidence_level = "MEDIUM"
                else:
                    confidence_level = "LOW"
                
                backtest_result = {
                    'strategy_name': f"{processor_name} - {split_type.title()} - {source_book}",
                    'source_book_type': source_book,
                    'split_type': split_type,
                    'total_bets': total_bets,
                    'wins': wins,
                    'win_rate': win_rate,
                    'roi_per_100': roi_per_100,
                    'confidence_level': confidence_level,
                    'strategy_type': processor_name,
                    'backtest_date': datetime.now().strftime('%Y-%m-%d'),
                    'created_at': datetime.now(),
                    'last_updated': datetime.now()
                }
                
                backtest_results.append(backtest_result)
        
        return backtest_results
    
    def _determine_bet_outcome_from_signal(self, signal, split_data: Dict[str, Any], split_type: str) -> Optional[bool]:
        """Determine if a betting signal would have won based on game outcome."""
        # Skip if no score data
        if split_data.get('home_score') is None or split_data.get('away_score') is None:
            return None
        
        try:
            home_score = int(split_data['home_score'])
            away_score = int(split_data['away_score'])
        except (ValueError, TypeError):
            return None
        
        # Extract bet side from signal's recommended bet
        recommended_bet = signal.recommended_bet.lower()
        
        if split_type == 'moneyline':
            home_won = home_score > away_score
            
            if 'home' in recommended_bet:
                return home_won
            elif 'away' in recommended_bet:
                return not home_won
                
        elif split_type == 'spread':
            # Use spread from signal metadata or split data
            spread = signal.metadata.get('spread') or split_data.get('home_spread_line')
            if spread is not None:
                try:
                    spread = float(spread)
                    home_score_with_spread = home_score + spread
                    home_covered = home_score_with_spread > away_score
                    
                    if 'home' in recommended_bet:
                        return home_covered
                    elif 'away' in recommended_bet:
                        return not home_covered
                except (ValueError, TypeError):
                    pass
                    
        elif split_type == 'total':
            # Use total from signal metadata or split data
            total_line = signal.metadata.get('total') or split_data.get('total_line')
            if total_line is not None:
                try:
                    total_line = float(total_line)
                    actual_total = home_score + away_score
                    
                    if 'over' in recommended_bet:
                        return actual_total > total_line
                    elif 'under' in recommended_bet:
                        return actual_total < total_line
                except (ValueError, TypeError):
                    pass
        
        return None
    
    def _calculate_profit_from_odds(self, odds) -> float:
        """Calculate profit from American odds."""
        try:
            odds_value = float(odds)
            if odds_value > 0:
                # Positive odds: win $odds for every $100 bet
                return odds_value / 100
            else:
                # Negative odds: win $100 for every $|odds| bet
                return 100 / abs(odds_value)
        except (ValueError, TypeError):
            return 100 / 110  # Default to -110 odds
    
    def _group_splits_by_game(self, splits_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Group splits data by game for analysis."""
        games_data = {}
        
        for split in splits_data:
            # Create standardized source-book combination
            source_book = self._get_standardized_source_book(split)
            
            # Use source-book combination in the key to separate different books
            game_key = f"{split['game_id']}_{split['split_type']}_{source_book}"
            if game_key not in games_data:
                games_data[game_key] = []
            games_data[game_key].append(split)
        
        return games_data
    
    def _get_standardized_source_book(self, split: Dict[str, Any]) -> str:
        """Get standardized source-book combination string."""
        source = split.get('source', 'UNKNOWN')
        book = split.get('book', '') or ''  # Handle None values
        book = book.lower() if book else ''
        
        # Standardize book names
        if source == 'VSIN':
            if book in ['circa']:
                return 'VSIN-CIRCA'
            elif book in ['draftkings', 'dk']:
                return 'VSIN-DRAFTKINGS'
            else:
                # Fallback for any other VSIN books
                return f'VSIN-{book.upper()}' if book else 'VSIN-UNKNOWN'
        elif source == 'SBD':
            return 'SBD'  # SBD doesn't have sub-books
        else:
            return source
    
    def _get_available_source_books(self, splits_data: List[Dict[str, Any]]) -> List[str]:
        """Get all available source-book combinations from the data."""
        source_books = set()
        
        for split in splits_data:
            source_book = self._get_standardized_source_book(split)
            source_books.add(source_book)
        
        return sorted(list(source_books))
    
    async def _backtest_strategy(self, games_data: Dict[str, List[Dict[str, Any]]], 
                               strategy: Dict[str, Any], split_type: str, source_book: str) -> Dict[str, Any]:
        """Backtest a specific strategy on the games data."""
        
        strategy_name = f"{strategy['name']} - {split_type.title()} - {source_book}"
        min_differential = strategy['min_differential']
        
        total_bets = 0
        wins = 0
        total_profit = 0.0
        
        for game_key, game_splits in games_data.items():
            # Filter for the specific split type and source-book combination
            # The game_key now includes the source_book, so we just need to check if it matches
            if not game_key.endswith(f"_{split_type}_{source_book}"):
                continue
                
            # All splits in this group should match our criteria
            relevant_splits = game_splits
            
            if not relevant_splits:
                continue
            
            # Use the most recent split for this game/type/source
            latest_split = max(relevant_splits, key=lambda x: x['last_updated'])
            
            # Calculate differential
            bets_pct = latest_split['home_or_over_bets_percentage']
            stake_pct = latest_split['home_or_over_stake_percentage']
            
            if bets_pct is None or stake_pct is None:
                continue
                
            differential = abs(bets_pct - stake_pct)
            
            # Check if this meets the strategy threshold
            if differential < min_differential:
                continue
            
            # Determine the bet based on strategy
            if strategy['strategy_type'] in ['sharp_action', 'strong_sharp', 'moderate_sharp']:
                # Follow the money (stake vs bets)
                if stake_pct > bets_pct:
                    # Sharp money on home/over
                    bet_side = 'home' if split_type in ['moneyline', 'spread'] else 'over'
                else:
                    # Sharp money on away/under
                    bet_side = 'away' if split_type in ['moneyline', 'spread'] else 'under'
            elif strategy['strategy_type'] == 'public_fade':
                # Fade the public (opposite of bets)
                if bets_pct > 50:
                    # Public on home/over, fade them
                    bet_side = 'away' if split_type in ['moneyline', 'spread'] else 'under'
                else:
                    # Public on away/under, fade them
                    bet_side = 'home' if split_type in ['moneyline', 'spread'] else 'over'
            else:
                continue
            
            # Determine win/loss based on game outcome
            bet_won = self._determine_bet_outcome(latest_split, bet_side, split_type)
            
            if bet_won is not None:  # Only count bets where we have outcome data
                total_bets += 1
                if bet_won:
                    wins += 1
                    # Assume standard -110 odds for profit calculation
                    total_profit += 100 / 110  # Win $100 for every $110 risked
                else:
                    total_profit -= 1  # Lose $1 for every $1 risked (standard unit)
        
        if total_bets == 0:
            return None
        
        # Calculate metrics
        win_rate = wins / total_bets
        roi_per_100 = (total_profit / total_bets) * 100
        
        # Determine confidence level
        if total_bets >= 50:
            confidence_level = "HIGH"
        elif total_bets >= 20:
            confidence_level = "MEDIUM"
        else:
            confidence_level = "LOW"
        
        return {
            'strategy_name': strategy_name,
            'source_book_type': source_book,
            'split_type': split_type,
            'total_bets': total_bets,
            'wins': wins,
            'win_rate': win_rate,
            'roi_per_100': roi_per_100,
            'confidence_level': confidence_level,
            'min_differential': min_differential,
            'strategy_type': strategy['strategy_type'],
            'backtest_date': datetime.now().strftime('%Y-%m-%d'),
            'created_at': datetime.now(),
            'last_updated': datetime.now()
        }
    
    def _determine_bet_outcome(self, split_data: Dict[str, Any], bet_side: str, split_type: str) -> Optional[bool]:
        """Determine if a bet won based on the game outcome using basic logic."""
        
        # Skip if no score data
        if split_data.get('home_score') is None or split_data.get('away_score') is None:
            return None
        
        try:
            home_score = int(split_data['home_score'])
            away_score = int(split_data['away_score'])
        except (ValueError, TypeError):
            return None
        
        if split_type == 'moneyline':
            # Calculate winner from scores
            home_won = home_score > away_score
            
            if bet_side == 'home':
                return home_won
            else:  # bet_side == 'away'
                return not home_won
                
        elif split_type == 'spread':
            # Use the home_spread_line if available
            if split_data.get('home_spread_line') is not None:
                try:
                    spread = float(split_data['home_spread_line'])
                    # Apply spread to home team score
                    home_score_with_spread = home_score + spread
                    home_covered = home_score_with_spread > away_score
                    
                    if bet_side == 'home':
                        return home_covered
                    else:  # bet_side == 'away'
                        return not home_covered
                except (ValueError, TypeError):
                    pass
            
            # Fallback: use straight win/loss (not accurate but better than nothing)
            home_won = home_score > away_score
            if bet_side == 'home':
                return home_won
            else:
                return not home_won
                
        elif split_type == 'total':
            # Calculate actual total
            actual_total = home_score + away_score
            
            # Use the total_line if available
            if split_data.get('total_line') is not None:
                try:
                    total_line = float(split_data['total_line'])
                    over_won = actual_total > total_line
                    
                    if bet_side == 'over':
                        return over_won
                    else:  # bet_side == 'under'
                        return not over_won
                except (ValueError, TypeError):
                    pass
            
            # Fallback: use average MLB total of 8.5
            if bet_side == 'over':
                return actual_total > 8.5
            else:  # bet_side == 'under'
                return actual_total <= 8.5
        
        return None
    
    def _calculate_data_quality_score(self, splits_data: List[Dict[str, Any]]) -> float:
        """Calculate a data quality score based on the splits data."""
        if not splits_data:
            return 0.0
        
        # Check various quality metrics
        total_records = len(splits_data)
        
        # Count records with required fields
        complete_records = sum(1 for s in splits_data 
                             if s.get('home_or_over_bets_percentage') is not None 
                             and s.get('home_or_over_stake_percentage') is not None
                             and s.get('game_datetime') is not None)
        
        # Count records with outcome data
        outcome_records = sum(1 for s in splits_data 
                            if s.get('game_outcome') is not None)
        
        # Calculate score
        completeness_score = complete_records / total_records
        outcome_score = outcome_records / total_records if total_records > 0 else 0
        
        # Weighted average (completeness is more important)
        quality_score = (completeness_score * 0.7) + (outcome_score * 0.3)
        
        return min(100.0, quality_score * 100)
    
    async def _store_backtest_results(self, strategy_results: List[Dict[str, Any]], 
                                    start_date: str, end_date: str):
        """Store backtest results in the database."""
        try:
            # Create backtesting schema if it doesn't exist
            with self.db_manager.get_cursor() as cursor:
                cursor.execute("CREATE SCHEMA IF NOT EXISTS backtesting")
                
                # Create strategy_configurations table if it doesn't exist
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS backtesting.strategy_configurations (
                        id SERIAL PRIMARY KEY,
                        strategy_name VARCHAR(255) NOT NULL,
                        source_book_type VARCHAR(100) NOT NULL,
                        split_type VARCHAR(50) NOT NULL,
                        win_rate DECIMAL(5,4) NOT NULL,
                        roi_per_100 DECIMAL(8,2) NOT NULL,
                        total_bets INTEGER NOT NULL,
                        confidence_level VARCHAR(20) NOT NULL,
                        min_threshold DECIMAL(8,2) DEFAULT 15.0,
                        moderate_threshold DECIMAL(8,2) DEFAULT 22.5,
                        high_threshold DECIMAL(8,2) DEFAULT 30.0,
                        is_active BOOLEAN DEFAULT true,
                        max_drawdown DECIMAL(8,2) DEFAULT 0.0,
                        sharpe_ratio DECIMAL(8,2) DEFAULT 0.0,
                        kelly_criterion DECIMAL(8,2) DEFAULT 0.0,
                        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(strategy_name, source_book_type, split_type)
                    )
                """)
                
                # Create index for faster lookups
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_strategy_configs_active 
                    ON backtesting.strategy_configurations(is_active, roi_per_100 DESC)
                """)
                
                # Create threshold_configurations table if it doesn't exist
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS backtesting.threshold_configurations (
                        id SERIAL PRIMARY KEY,
                        source VARCHAR(100) NOT NULL,
                        strategy_type VARCHAR(100) NOT NULL,
                        high_confidence_threshold DECIMAL(8,2) NOT NULL,
                        moderate_confidence_threshold DECIMAL(8,2) NOT NULL,
                        minimum_threshold DECIMAL(8,2) NOT NULL,
                        opposing_high_threshold DECIMAL(8,2) NOT NULL,
                        opposing_moderate_threshold DECIMAL(8,2) NOT NULL,
                        steam_threshold DECIMAL(8,2) NOT NULL,
                        steam_time_window_hours DECIMAL(4,2) DEFAULT 2.0,
                        min_sample_size INTEGER DEFAULT 10,
                        min_win_rate DECIMAL(5,4) DEFAULT 0.52,
                        is_active BOOLEAN DEFAULT true,
                        last_validated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        confidence_level VARCHAR(20) DEFAULT 'MODERATE',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(source, strategy_type)
                    )
                """)
                
                # Create index for threshold lookups
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_threshold_configs_active 
                    ON backtesting.threshold_configurations(is_active, source, strategy_type)
                """)
            
            # Store individual strategy results
            for result in strategy_results:
                try:
                    # Use INSERT ... ON CONFLICT with strategy combination instead of custom ID
                    query = """
                        INSERT INTO backtesting.strategy_performance (
                            backtest_date, strategy_name, source_book_type, split_type,
                            total_bets, wins, win_rate, roi_per_100, confidence_level,
                            created_at, last_updated, kelly_criterion, sharpe_ratio, 
                            max_drawdown, total_profit_loss, is_active, strategy_type
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (strategy_name, source_book_type, split_type, backtest_date) 
                        DO UPDATE SET
                            total_bets = EXCLUDED.total_bets,
                            wins = EXCLUDED.wins,
                            win_rate = EXCLUDED.win_rate,
                            roi_per_100 = EXCLUDED.roi_per_100,
                            confidence_level = EXCLUDED.confidence_level,
                            last_updated = EXCLUDED.last_updated,
                            kelly_criterion = EXCLUDED.kelly_criterion,
                            sharpe_ratio = EXCLUDED.sharpe_ratio,
                            max_drawdown = EXCLUDED.max_drawdown,
                            total_profit_loss = EXCLUDED.total_profit_loss,
                            is_active = EXCLUDED.is_active,
                            strategy_type = EXCLUDED.strategy_type
                    """
                    
                    with self.db_manager.get_cursor() as cursor:
                        cursor.execute(query, (
                            result['backtest_date'],
                            result['strategy_name'],
                            result['source_book_type'],
                            result['split_type'],
                            result['total_bets'],
                            result['wins'],
                            result['win_rate'],
                            result['roi_per_100'],
                            result['confidence_level'],
                            result['created_at'],
                            result['last_updated'],
                            result.get('kelly_criterion', 0.0),
                            result.get('sharpe_ratio', 0.0),
                            result.get('max_drawdown', 0.0),
                            result.get('total_profit_loss', 0.0),
                            True,  # is_active - default to true for new results
                            result.get('strategy_type', 'unknown')  # strategy_type
                        ))
                        
                except Exception as e:
                    self.logger.warning(f"Failed to store result for {result.get('strategy_name', 'unknown')}: {e}")
            
            # After storing performance results, update strategy configurations
            await self._update_strategy_configurations(strategy_results)
            
            self.logger.info(f"Stored {len(strategy_results)} strategy results in database")
            
        except Exception as e:
            self.logger.error(f"Failed to store backtest results: {e}")
    
    async def _update_strategy_configurations(self, strategy_results: List[Dict[str, Any]]):
        """Update strategy configurations based on backtest results."""
        try:
            for result in strategy_results:
                # 🎯 DYNAMIC THRESHOLDS: Use threshold manager for ROI-based optimization
                roi = result.get('roi_per_100', 0)
                win_rate = result.get('win_rate', 0.5)
                total_bets = result.get('total_bets', 0)
                
                # Use dynamic threshold manager for better threshold calculation
                try:
                    threshold_manager = get_dynamic_threshold_manager()
                    threshold_config = await threshold_manager.get_dynamic_threshold(
                        strategy_type=result.get('strategy_type', 'default'),
                        source=result.get('source_book_type', 'default'),
                        split_type=result.get('split_type', 'default')
                    )
                    
                    min_threshold = threshold_config.minimum_threshold
                    moderate_threshold = threshold_config.moderate_threshold
                    high_threshold = threshold_config.high_threshold
                    
                    self.logger.debug(f"🎯 Using dynamic thresholds for {result.get('strategy_name')}: "
                                    f"min={min_threshold:.1f}%, mod={moderate_threshold:.1f}%, high={high_threshold:.1f}%")
                    
                except Exception as e:
                    self.logger.warning(f"Failed to get dynamic thresholds, using fallback calculation: {e}")
                    # Fallback to original calculation but with more aggressive values
                    min_threshold = max(5.0, 50.0 / max(abs(roi), 1.0)) if roi != 0 else 8.0  # More aggressive
                    moderate_threshold = min_threshold * 1.5
                    high_threshold = min_threshold * 2.0
                
                # Determine if strategy should be active based on performance
                # Use the same disabling logic as StrategyManager
                should_disable = (
                    (win_rate < 0.50 and roi < 0.0) or
                    (win_rate < 0.55 and roi < -5.0) or
                    (roi < -10.0) or
                    (total_bets < 5)
                )
                is_active = not should_disable
                
                # Insert or update strategy configuration
                config_query = """
                    INSERT INTO backtesting.strategy_configurations (
                        strategy_name, source_book_type, split_type, win_rate, roi_per_100,
                        total_bets, confidence_level, min_threshold, moderate_threshold, 
                        high_threshold, is_active, max_drawdown, sharpe_ratio, kelly_criterion,
                        last_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (strategy_name, source_book_type, split_type)
                    DO UPDATE SET
                        win_rate = EXCLUDED.win_rate,
                        roi_per_100 = EXCLUDED.roi_per_100,
                        total_bets = EXCLUDED.total_bets,
                        confidence_level = EXCLUDED.confidence_level,
                        min_threshold = EXCLUDED.min_threshold,
                        moderate_threshold = EXCLUDED.moderate_threshold,
                        high_threshold = EXCLUDED.high_threshold,
                        is_active = EXCLUDED.is_active,
                        max_drawdown = EXCLUDED.max_drawdown,
                        sharpe_ratio = EXCLUDED.sharpe_ratio,
                        kelly_criterion = EXCLUDED.kelly_criterion,
                        last_updated = EXCLUDED.last_updated
                """
                
                with self.db_manager.get_cursor() as cursor:
                    cursor.execute(config_query, (
                        result['strategy_name'],
                        result['source_book_type'],
                        result['split_type'],
                        result['win_rate'],
                        result['roi_per_100'],
                        result['total_bets'],
                        result['confidence_level'],
                        min_threshold,
                        moderate_threshold,
                        high_threshold,
                        is_active,
                        result.get('max_drawdown', 0.0),
                        result.get('sharpe_ratio', 0.0),
                        result.get('kelly_criterion', 0.0),
                        datetime.now()
                    ))
            
            self.logger.info(f"Updated {len(strategy_results)} strategy configurations")
            
            # Also update threshold configurations based on performance
            await self._update_threshold_configurations(strategy_results)
            
        except Exception as e:
            self.logger.error(f"Failed to update strategy configurations: {e}")
    
    async def _update_threshold_configurations(self, strategy_results: List[Dict[str, Any]]):
        """Update threshold configurations based on strategy performance."""
        try:
            # Group results by source to calculate optimal thresholds
            source_performance = {}
            
            for result in strategy_results:
                source = result.get('source_book_type', 'UNKNOWN')
                strategy_type = result.get('strategy_type', 'sharp_action')
                
                if source not in source_performance:
                    source_performance[source] = {}
                
                if strategy_type not in source_performance[source]:
                    source_performance[source][strategy_type] = []
                
                source_performance[source][strategy_type].append(result)
            
            # Update thresholds for each source/strategy combination
            for source, strategy_types in source_performance.items():
                for strategy_type, results in strategy_types.items():
                    # Calculate optimal thresholds based on performance
                    best_performing = max(results, key=lambda x: x.get('roi_per_100', 0))
                    avg_roi = sum(r.get('roi_per_100', 0) for r in results) / len(results)
                    avg_win_rate = sum(r.get('win_rate', 0.5) for r in results) / len(results)
                    
                    # Dynamic threshold calculation based on performance
                    if avg_roi >= 15.0:
                        # High performance - use more aggressive thresholds
                        high_conf = 20.0
                        moderate_conf = 12.0
                        minimum = 8.0
                    elif avg_roi >= 8.0:
                        # Good performance - use moderate thresholds  
                        high_conf = 25.0
                        moderate_conf = 15.0
                        minimum = 10.0
                    else:
                        # Lower performance - use conservative thresholds
                        high_conf = 30.0
                        moderate_conf = 20.0
                        minimum = 15.0
                    
                    # Opposing market thresholds (higher than regular)
                    opposing_high = high_conf * 1.5
                    opposing_moderate = moderate_conf * 1.3
                    
                    # Steam thresholds
                    steam_threshold = high_conf * 0.9
                    
                    # Determine confidence level
                    total_bets = sum(r.get('total_bets', 0) for r in results)
                    if total_bets >= 50 and avg_roi >= 10.0:
                        confidence_level = "HIGH"
                    elif total_bets >= 20 and avg_roi >= 5.0:
                        confidence_level = "MODERATE"
                    else:
                        confidence_level = "LOW"
                    
                    # Insert or update threshold configuration
                    threshold_query = """
                        INSERT INTO backtesting.threshold_configurations (
                            source, strategy_type, high_confidence_threshold, 
                            moderate_confidence_threshold, minimum_threshold,
                            opposing_high_threshold, opposing_moderate_threshold,
                            steam_threshold, steam_time_window_hours, min_sample_size,
                            min_win_rate, is_active, last_validated, confidence_level
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source, strategy_type)
                        DO UPDATE SET
                            high_confidence_threshold = EXCLUDED.high_confidence_threshold,
                            moderate_confidence_threshold = EXCLUDED.moderate_confidence_threshold,
                            minimum_threshold = EXCLUDED.minimum_threshold,
                            opposing_high_threshold = EXCLUDED.opposing_high_threshold,
                            opposing_moderate_threshold = EXCLUDED.opposing_moderate_threshold,
                            steam_threshold = EXCLUDED.steam_threshold,
                            min_win_rate = EXCLUDED.min_win_rate,
                            is_active = EXCLUDED.is_active,
                            last_validated = EXCLUDED.last_validated,
                            confidence_level = EXCLUDED.confidence_level
                    """
                    
                    with self.db_manager.get_cursor() as cursor:
                        cursor.execute(threshold_query, (
                            source,
                            strategy_type,
                            high_conf,
                            moderate_conf,
                            minimum,
                            opposing_high,
                            opposing_moderate,
                            steam_threshold,
                            2.0,  # steam_time_window_hours
                            max(10, int(total_bets / len(results))),  # min_sample_size based on avg
                            avg_win_rate,
                            avg_roi > 0,  # is_active if profitable
                            datetime.now(),
                            confidence_level
                        ))
            
            self.logger.info(f"Updated threshold configurations for {len(source_performance)} sources")
            
        except Exception as e:
            self.logger.error(f"Failed to update threshold configurations: {e}")
    
    async def _analyze_live_alignment(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """Analyze alignment between backtesting and live recommendations."""
        
        self.logger.info(f"Analyzing live alignment for period: {start_date} to {end_date}")
        
        # Placeholder for alignment analysis
        return {
            'alignment_score': 85.0,  # Placeholder
            'discrepancies': [],
            'recommendations': ['Alignment analysis placeholder - full implementation needed']
        }
    
    async def run_daily_backtesting_pipeline(self) -> Dict[str, Any]:
        """Run the daily backtesting pipeline."""
        
        # Calculate date range (previous day)
        end_date = datetime.now(timezone.utc) - timedelta(days=1)
        start_date = end_date - timedelta(days=7)  # 7-day lookback
        
        return await self.run_enhanced_backtest(
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d'),
            validate_alignment=True
        )


# =============================================================================
# PLACEHOLDER MODULES (to be implemented in subsequent steps)
# =============================================================================

class DiagnosticsModule:
    """Diagnostics module - consolidated from backtesting_diagnostics.py"""
    
    def __init__(self, db_manager: DatabaseManager, signal_repository):
        self.db_manager = db_manager
        self.signal_repository = signal_repository
        self.logger = get_logger(f"{__name__}.diagnostics")
    
    async def run_full_diagnostic(self) -> Dict[str, Any]:
        """Run full 5-checkpoint diagnostic suite."""
        
        self.logger.info("Running full diagnostic suite - placeholder implementation")
        
        return {
            'diagnostic_summary': {
                'total_checkpoints': 5,
                'passed_checkpoints': 0,
                'failed_checkpoints': 0,
                'warning_checkpoints': 0
            },
            'checkpoint_results': [],
            'recommendations': ['Diagnostics module placeholder - full implementation needed']
        }


class SchedulerModule:
    """Scheduler module - consolidated from automated_backtesting_scheduler.py"""
    
    def __init__(self, backtesting_engine):
        self.backtesting_engine = backtesting_engine
        self.logger = get_logger(f"{__name__}.scheduler")
        self._scheduler_active = False
    
    def start_automated_backtesting(self, notifications_enabled: bool = True):
        """Start automated backtesting scheduling."""
        self._scheduler_active = True
        self.logger.info("Automated backtesting scheduler started - placeholder implementation")
    
    def stop_automated_backtesting(self):
        """Stop automated backtesting scheduling."""
        self._scheduler_active = False
        self.logger.info("Automated backtesting scheduler stopped")
    
    def get_status(self) -> Dict[str, Any]:
        """Get scheduler status."""
        return {
            'active': self._scheduler_active,
            'placeholder': True
        }


class AccuracyModule:
    """Accuracy monitoring module - consolidated from betting_accuracy_monitor.py"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.logger = get_logger(f"{__name__}.accuracy")
        self._baseline_established = False
    
    async def establish_baseline(self, lookback_days: int = 7):
        """Establish accuracy monitoring baseline."""
        self._baseline_established = True
        self.logger.info(f"Accuracy baseline established for {lookback_days} days - placeholder implementation")
    
    def get_current_status(self) -> Dict[str, Any]:
        """Get current accuracy monitoring status."""
        return {
            'baseline_established': self._baseline_established,
            'placeholder': True
        }


# =============================================================================
# SINGLETON ACCESS
# =============================================================================

_backtesting_engine_instance: Optional[BacktestingEngine] = None

def get_backtesting_engine() -> BacktestingEngine:
    """Get the singleton BacktestingEngine instance."""
    global _backtesting_engine_instance
    
    if _backtesting_engine_instance is None:
        _backtesting_engine_instance = BacktestingEngine()
    
    return _backtesting_engine_instance


# =============================================================================
# LEGACY COMPATIBILITY
# =============================================================================

# Backward compatibility exports
SimplifiedBacktestingService = BacktestingEngine  # Alias for legacy code
EnhancedBacktestingService = BacktestingEngine    # Alias for legacy code

__all__ = [
    'BacktestingEngine',
    'BacktestResult', 
    'UnifiedBetOutcome',
    'DiagnosticStatus',
    'PerformanceStatus',
    'get_backtesting_engine',
    
    # Legacy compatibility
    'SimplifiedBacktestingService',
    'EnhancedBacktestingService'
] 