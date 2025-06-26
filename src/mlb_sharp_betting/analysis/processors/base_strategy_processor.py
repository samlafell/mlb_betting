"""
Enhanced Base Strategy Processor

Extends the existing BaseSignalProcessor with strategy-specific functionality,
error handling, and validation methods for all strategy processors.

Part of Phase 1 foundation architecture for comprehensive strategy processing.
"""

from abc import abstractmethod
from typing import List, Dict, Any
import asyncio

from ...analyzers.signal_processor_base import BaseSignalProcessor
from ...models.betting_analysis import BettingSignal, ProfitableStrategy


class BaseStrategyProcessor(BaseSignalProcessor):
    """
    Enhanced base class for all strategy processors
    
    Provides strategy-specific functionality on top of the existing
    BaseSignalProcessor foundation, including error handling,
    validation, and strategy categorization.
    """
    
    @abstractmethod
    def get_strategy_category(self) -> str:
        """Return strategy category for proper routing and organization"""
        pass
    
    @abstractmethod
    def get_required_tables(self) -> List[str]:
        """Return database tables required for this strategy"""
        pass
    
    @abstractmethod
    def get_strategy_description(self) -> str:
        """Return human-readable description of the strategy"""
        pass
    
    def validate_strategy_data(self, raw_data: List[Dict]) -> bool:
        """
        Validate strategy-specific data requirements
        Override in subclasses for custom validation logic
        """
        return len(raw_data) > 0
    
    async def process_with_error_handling(self, minutes_ahead: int, 
                                        profitable_strategies: List[ProfitableStrategy]) -> List[BettingSignal]:
        """
        Wrapper with comprehensive error handling for strategy processing
        
        Ensures that failures in individual strategy processors don't crash
        the entire analysis system.
        """
        try:
            self.logger.info(f"Starting {self.__class__.__name__} processing")
            signals = await self.process(minutes_ahead, profitable_strategies)
            
            if signals:
                self.logger.info(f"✅ {self.__class__.__name__}: {len(signals)} signals generated")
            else:
                self.logger.info(f"ℹ️  {self.__class__.__name__}: No signals found")
                
            return signals
            
        except Exception as e:
            self.logger.error(f"❌ {self.__class__.__name__} processing failed: {e}", exc_info=True)
            return []
    
    def _should_apply_juice_filter(self, raw_data: Dict[str, Any]) -> bool:
        """
        Determine if juice filter should be applied to this signal
        Uses user preference from memory: refuses to bet on odds more negative than -160
        """
        split_type = raw_data.get('split_type', '')
        split_value = raw_data.get('split_value')
        
        if split_type == 'moneyline' and split_value:
            recommended_team = self._get_recommended_team(raw_data)
            return self._should_filter_juice(
                split_type, split_value, recommended_team, 
                raw_data['home_team'], raw_data['away_team']
            )
        return False
    
    def _get_recommended_team(self, raw_data: Dict[str, Any]) -> str:
        """Get the recommended team based on the differential"""
        differential = raw_data.get('differential', 0)
        return raw_data['home_team'] if differential > 0 else raw_data['away_team']
    
    def _is_valid_signal_data(self, row: Dict[str, Any], now_est, minutes_ahead: int) -> bool:
        """
        Enhanced validation for signal data with strategy-specific checks
        """
        # Basic validation from parent class
        if not all(key in row for key in ['home_team', 'away_team', 'game_datetime', 
                                        'differential', 'source', 'split_type']):
            return False
        
        # Time window validation
        game_time = self._normalize_game_time(row['game_datetime'])
        time_diff = self._calculate_minutes_to_game(game_time, now_est)
        
        if time_diff < 0 or time_diff > minutes_ahead:
            return False
        
        # Differential validation
        if abs(float(row['differential'])) < self.config.minimum_differential:
            return False
        
        # Strategy-specific validation (can be overridden)
        return self.validate_strategy_data([row])
    
    def _find_matching_strategies(self, profitable_strategies: List[ProfitableStrategy], 
                                signal_data: Dict[str, Any]) -> List[ProfitableStrategy]:
        """Find profitable strategies that match this signal type and data"""
        strategy_category = self.get_strategy_category()
        signal_type = self.get_signal_type().value
        
        # Use validator to find matching strategies
        matching_strategy = self.validator.find_matching_strategy(
            signal_type, 
            signal_data['source'], 
            signal_data.get('book'), 
            signal_data['split_type'], 
            abs(float(signal_data['differential']))
        )
        
        return [matching_strategy] if matching_strategy else []
    
    def get_processor_info(self) -> Dict[str, Any]:
        """Get comprehensive information about this processor"""
        return {
            'processor_name': self.__class__.__name__,
            'signal_type': self.get_signal_type().value,
            'strategy_category': self.get_strategy_category(),
            'description': self.get_strategy_description(),
            'required_tables': self.get_required_tables(),
            'supports_parallel_processing': True
        } 