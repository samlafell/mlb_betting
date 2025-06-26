#!/usr/bin/env python3
"""
REFACTORED ADAPTIVE MASTER BETTING DETECTOR
==========================================

Clean, modular implementation using proper separation of concerns:
✅ Repository pattern for database access
✅ Strategy pattern for signal processors  
✅ Unified strategy validation
✅ Separated business logic from presentation
✅ Centralized configuration

This demonstrates the improved architecture suggested by the senior engineer.

Usage: uv run analysis_scripts/refactored_master_betting_detector.py --minutes 300
       uv run analysis_scripts/refactored_master_betting_detector.py --debug
"""

import argparse
import asyncio
import sys
import warnings
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Suppress warnings and set clean logging
warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")
logging.getLogger("mlb_sharp_betting").setLevel(logging.WARNING)

# Add src to path for imports
sys.path.insert(0, 'src')

from mlb_sharp_betting.models.betting_analysis import (
    BettingAnalysisResult, GameAnalysis, StrategyThresholds, 
    SignalProcessorConfig, StrategyPerformance
)
from mlb_sharp_betting.services.betting_signal_repository import BettingSignalRepository
from mlb_sharp_betting.services.strategy_validator import StrategyValidator
from mlb_sharp_betting.services.betting_analysis_formatter import BettingAnalysisFormatter
from mlb_sharp_betting.analysis.processors.real_time_processor import RealTimeProcessor
from mlb_sharp_betting.services.juice_filter_service import get_juice_filter_service
from mlb_sharp_betting.core.logging import get_logger


class RefactoredAdaptiveMasterBettingDetector:
    """
    Clean, modular betting detector using proper separation of concerns
    
    Key improvements over original:
    - Repository pattern for data access
    - Strategy pattern for signal processing
    - Unified strategy validation
    - Separated presentation from business logic
    - Centralized configuration
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
        
        # Configuration - centralized instead of hardcoded throughout
        self.strategy_thresholds = StrategyThresholds()
        self.processor_config = SignalProcessorConfig()
        
        # Core services
        self.repository = BettingSignalRepository(self.processor_config)
        self.juice_filter = get_juice_filter_service()
        self.formatter = BettingAnalysisFormatter()
        
        # Will be initialized with profitable strategies
        self.validator: StrategyValidator = None
        self.signal_processors = {}
    
    async def analyze_opportunities(self, minutes_ahead: int = 60) -> BettingAnalysisResult:
        """
        Main analysis method - returns pure data structures
        
        This method focuses purely on business logic and returns structured data.
        Display formatting is handled separately by the formatter.
        """
        start_time = datetime.now()
        
        # Load profitable strategies from backtesting results
        profitable_strategies = await self.repository.get_profitable_strategies()
        
        if not profitable_strategies:
            self.logger.warning("No profitable strategies found in backtesting results")
            return self._create_empty_result(start_time, minutes_ahead)
        
        # Initialize validator and processors with strategies
        self.validator = StrategyValidator(profitable_strategies, self.strategy_thresholds)
        self._initialize_signal_processors()
        
        # Process all signal types in parallel for efficiency
        signals_by_type = await self._process_all_signal_types(minutes_ahead, profitable_strategies)
        
        # Combine signals into game-by-game analysis
        games = self._combine_signals_by_game(signals_by_type)
        
        # Create structured result
        return BettingAnalysisResult(
            games=games,
            strategy_performance=self._create_strategy_performance_list(profitable_strategies),
            analysis_metadata={
                'start_time': start_time,
                'end_time': start_time + timedelta(minutes=minutes_ahead),
                'minutes_ahead': minutes_ahead,
                'strategies_count': len(profitable_strategies),
                'processor_config': self.processor_config.__dict__
            }
        )
    
    async def display_analysis(self, analysis_result: BettingAnalysisResult):
        """Separate display method using the formatter"""
        juice_summary = self.juice_filter.get_filter_summary()
        formatted_output = self.formatter.format_analysis(analysis_result, juice_summary)
        print(formatted_output)
    
    async def debug_database_contents(self):
        """Debug method using repository for data access"""
        database_stats = await self.repository.get_database_stats()
        
        if database_stats['total_records'] == 0:
            debug_output = self.formatter.format_debug_info(database_stats, 0)
            print(debug_output)
            return
        
        # Get actionable games count
        start_time = datetime.now()
        end_time = start_time + timedelta(hours=24)
        actionable_games = await self.repository.get_actionable_games_count(start_time, end_time)
        
        debug_output = self.formatter.format_debug_info(database_stats, actionable_games)
        print(debug_output)
        
        # Show sample data if available
        if database_stats['total_records'] > 0:
            await self._show_sample_data()
    
    def _initialize_signal_processors(self):
        """Initialize all strategy processors using factory pattern"""
        # Import the factory here to avoid circular imports
        from mlb_sharp_betting.analysis.processors.strategy_processor_factory import StrategyProcessorFactory
        
        # Create factory and initialize all processors
        factory = StrategyProcessorFactory(self.repository, self.validator, self.processor_config)
        self.signal_processors = factory.create_all_processors()
        
        # Log initialization summary
        total_processors = len(self.signal_processors)
        processor_names = list(self.signal_processors.keys())
        
        self.logger.info(f"🏭 Initialized {total_processors} strategy processors")
        self.logger.info(f"📋 Active processors: {', '.join(processor_names)}")
        
        # Store factory for additional operations
        self._processor_factory = factory
    
    async def _process_all_signal_types(self, minutes_ahead: int, profitable_strategies):
        """Process all signal types in parallel for maximum efficiency"""
        import asyncio
        
        # Create tasks for parallel execution of all processors
        tasks = []
        for strategy_name, processor in self.signal_processors.items():
            task = asyncio.create_task(
                processor.process_with_error_handling(minutes_ahead, profitable_strategies),
                name=f"process_{strategy_name}"
            )
            tasks.append((strategy_name, task))
        
        # Execute all processors in parallel
        signals_by_type = {}
        if tasks:
            self.logger.info(f"🚀 Starting parallel processing of {len(tasks)} strategy processors...")
            results = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)
            
            # Process results
            for (strategy_name, _), result in zip(tasks, results):
                if isinstance(result, Exception):
                    self.logger.error(f"❌ Strategy {strategy_name} failed: {result}")
                    signals_by_type[strategy_name] = []
                else:
                    signals_by_type[strategy_name] = result
                    if result:
                        self.logger.info(f"✅ {strategy_name}: {len(result)} signals processed")
                    else:
                        self.logger.info(f"ℹ️  {strategy_name}: No signals found")
        else:
            self.logger.warning("No strategy processors available")
        
        # Log summary
        total_signals = sum(len(signals) for signals in signals_by_type.values())
        active_strategies = sum(1 for signals in signals_by_type.values() if signals)
        
        self.logger.info(f"📊 Processing Complete: {total_signals} total signals from {active_strategies} active strategies")
        
        return signals_by_type
    
    def _combine_signals_by_game(self, signals_by_type) -> dict:
        """Combine all signals into game-by-game analysis with enhanced routing"""
        games = {}
        
        for strategy_name, signals in signals_by_type.items():
            for signal in signals:
                # Create game key
                game_key = (signal.away_team, signal.home_team, signal.game_time)
                
                # Initialize game analysis if not exists
                if game_key not in games:
                    games[game_key] = GameAnalysis(
                        home_team=signal.home_team,
                        away_team=signal.away_team,
                        game_time=signal.game_time,
                        minutes_to_game=signal.minutes_to_game
                    )
                
                # Route signals to appropriate categories based on strategy type and signal type
                self._route_signal_to_category(games[game_key], signal, strategy_name)
        
        return games
    
    def _route_signal_to_category(self, game_analysis, signal, strategy_name: str):
        """Route signals to appropriate categories based on strategy type"""
        
        # Create a comprehensive routing map for all strategy types
        routing_map = {
            # Sharp action strategies
            'sharp_action': lambda: game_analysis.sharp_signals.append(signal),
            
            # Market conflict strategies
            'opposing_markets': lambda: game_analysis.opposing_markets.append(signal),
            'book_conflicts': lambda: game_analysis.book_conflicts.append(signal),
            
            # Public betting strategies
            'public_money_fade': lambda: game_analysis.sharp_signals.append(signal),
            
            # Timing-based strategies
            'late_sharp_flip': lambda: game_analysis.sharp_signals.append(signal),
            'timing_based': lambda: game_analysis.sharp_signals.append(signal),
            'enhanced_late_sharp_flip': lambda: game_analysis.sharp_signals.append(signal),
            
            # Line movement strategies
            'line_movement': lambda: game_analysis.steam_moves.append(signal),
            'hybrid_line_sharp': lambda: game_analysis.steam_moves.append(signal),
            
            # Value strategies
            'underdog_ml_value': lambda: game_analysis.sharp_signals.append(signal),
            'total_line_sweet_spots': lambda: game_analysis.sharp_signals.append(signal),
            
            # Consensus strategies
            'consensus_moneyline': lambda: game_analysis.sharp_signals.append(signal),
            'signal_combinations': lambda: game_analysis.sharp_signals.append(signal),
            
            # Analysis strategies
            'team_specific_bias': lambda: game_analysis.sharp_signals.append(signal),
            'strategy_comparison_roi': lambda: game_analysis.sharp_signals.append(signal),
        }
        
        # Route the signal
        if strategy_name in routing_map:
            routing_map[strategy_name]()
        else:
            # Default routing based on signal type
            if signal.signal_type.value == 'OPPOSING_MARKETS':
                game_analysis.opposing_markets.append(signal)
            elif signal.signal_type.value == 'BOOK_CONFLICT':
                game_analysis.book_conflicts.append(signal)
            elif signal.signal_type.value == 'STEAM_MOVE':
                game_analysis.steam_moves.append(signal)
            else:
                # Default to sharp signals
                game_analysis.sharp_signals.append(signal)
    
    def _create_strategy_performance_list(self, profitable_strategies) -> list:
        """Convert profitable strategies to strategy performance list"""
        return [
            StrategyPerformance(
                strategy_name=strategy.strategy_name,
                source_book=strategy.source_book,
                split_type=strategy.split_type,
                win_rate=strategy.win_rate,
                roi=strategy.roi,
                total_bets=strategy.total_bets,
                confidence_level=strategy.confidence,
                ci_lower=strategy.ci_lower,
                ci_upper=strategy.ci_upper
            )
            for strategy in profitable_strategies
        ]
    
    def _create_empty_result(self, start_time, minutes_ahead) -> BettingAnalysisResult:
        """Create empty result when no strategies are available"""
        return BettingAnalysisResult(
            games={},
            strategy_performance=[],
            analysis_metadata={
                'start_time': start_time,
                'end_time': start_time + timedelta(minutes=minutes_ahead),
                'minutes_ahead': minutes_ahead,
                'strategies_count': 0,
                'error': 'No profitable strategies found'
            }
        )
    
    async def _show_sample_data(self):
        """Show sample data for debugging"""
        start_time = datetime.now()
        end_time = start_time + timedelta(hours=24)
        
        # Get sample sharp signal data
        sample_data = await self.repository.get_sharp_signal_data(start_time, end_time)
        
        if sample_data:
            print(f"\n📋 SAMPLE DATA (First 5 records):")
            for i, row in enumerate(sample_data[:5], 1):
                home = row['home_team']
                away = row['away_team']
                split_type = row['split_type']
                differential = row['differential']
                source = row['source']
                book = row.get('book', 'UNKNOWN')
                
                print(f"   {i}. {away} @ {home} - {split_type.upper()}")
                print(f"      💰 {differential:+.1f}% differential")
                print(f"      📍 {source}-{book}")
                print()


async def main():
    """Main entry point with clean argument handling"""
    parser = argparse.ArgumentParser(
        description="Refactored Adaptive Master Betting Detector - Clean Architecture"
    )
    parser.add_argument('--minutes', '-m', type=int, default=60,
                        help='Minutes ahead to look for opportunities (default: 60)')
    parser.add_argument('--debug', '-d', action='store_true',
                        help='Show database debug information')
    
    args = parser.parse_args()
    
    detector = RefactoredAdaptiveMasterBettingDetector()
    
    try:
        if args.debug:
            await detector.debug_database_contents()
        else:
            # Clean separation: analyze first, then display
            analysis_result = await detector.analyze_opportunities(args.minutes)
            await detector.display_analysis(analysis_result)
            
    except Exception as e:
        logging.error(f"Analysis failed: {e}")
        raise
    finally:
        # Clean shutdown (database connections handled automatically by coordinator)
        pass


if __name__ == "__main__":
    asyncio.run(main()) 