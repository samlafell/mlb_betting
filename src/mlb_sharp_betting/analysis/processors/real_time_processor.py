"""
Real-Time Sharp Action Signal Processor

Real-time signal processing with strategy validation for sharp action detection.
Handles live signal processing where the money percentage differs significantly 
from bet percentage, with juice filtering and timing coordination.

Migrated from analyzers/sharp_action_processor.py as part of Phase 2 refactoring.
"""

from typing import List, Dict, Any
from datetime import datetime

from ...models.betting_analysis import BettingSignal, SignalType, ProfitableStrategy
from .base_strategy_processor import BaseStrategyProcessor


class RealTimeProcessor(BaseStrategyProcessor):
    """Processor for sharp action signals (money vs bet percentage differentials)"""
    
    def get_signal_type(self) -> SignalType:
        """Return the signal type this processor handles"""
        return SignalType.SHARP_ACTION
    
    def get_strategy_category(self) -> str:
        """Return strategy category for proper routing"""
        return "SHARP_ACTION"
    
    def get_required_tables(self) -> List[str]:
        """Return database tables required for this strategy"""
        return ["splits.raw_mlb_betting_splits"]
    
    def get_strategy_description(self) -> str:
        """Return human-readable description of the strategy"""
        return "Real-time sharp action detection based on money vs bet percentage differentials"
    
    async def process(self, minutes_ahead: int, 
                     profitable_strategies: List[ProfitableStrategy]) -> List[BettingSignal]:
        """Process sharp action signals using profitable strategies"""
        start_time, end_time = self._create_time_window(minutes_ahead)
        
        # Get raw signal data from repository
        raw_signals = await self.repository.get_sharp_signal_data(start_time, end_time)
        
        # Filter strategies for sharp action signals
        sharp_strategies = self.validator.get_strategies_by_type('SHARP_ACTION')
        total_strategies = self.validator.get_strategies_by_type('TOTAL_SHARP')
        
        if not sharp_strategies and not total_strategies:
            self.logger.warning("No profitable sharp action strategies found")
            return []
        
        signals = []
        now_est = datetime.now(self.est)
        
        for row in raw_signals:
            # Basic validation
            if not self._is_valid_signal_data(row, now_est, minutes_ahead):
                continue
            
            abs_diff = abs(float(row['differential']))
            split_type = row['split_type']
            
            # Match to appropriate strategy type
            if split_type == 'total':
                matching_strategy = self.validator.find_matching_strategy(
                    'TOTAL_SHARP', row['source'], row['book'], split_type, abs_diff
                )
                signal_type_override = SignalType.TOTAL_SHARP
            else:
                matching_strategy = self.validator.find_matching_strategy(
                    'SHARP_ACTION', row['source'], row['book'], split_type, abs_diff
                )
                signal_type_override = SignalType.SHARP_ACTION
            
            if not matching_strategy:
                continue
            
            # Apply juice filter
            if self._should_apply_juice_filter(row):
                continue
            
            # Calculate confidence score
            confidence_data = self._calculate_confidence(
                row['differential'], row['source'], row['book'],
                row['split_type'], matching_strategy.strategy_name,
                row['last_updated'], self._normalize_game_time(row['game_datetime'])
            )
            
            # Create the signal
            signal = self._create_betting_signal(row, matching_strategy, confidence_data)
            
            # Override signal type if needed (for totals)
            if signal_type_override != self.get_signal_type():
                signal.signal_type = signal_type_override
            
            signals.append(signal)
        
        self._log_processing_summary(len(signals), len(sharp_strategies) + len(total_strategies), len(raw_signals))
        return signals
    
    def _is_valid_signal_data(self, row: Dict[str, Any], current_time: datetime, 
                             minutes_ahead: int) -> bool:
        """Validate signal data quality and timing"""
        try:
            game_time = self._normalize_game_time(row['game_datetime'])
            time_diff_minutes = self._calculate_minutes_to_game(game_time, current_time)
            
            # Check time window
            if not (0 <= time_diff_minutes <= minutes_ahead):
                return False
            
            # Check data completeness
            required_fields = ['home_team', 'away_team', 'split_type', 'differential', 'source']
            if not all(row.get(field) is not None for field in required_fields):
                return False
            
            # Check differential strength
            abs_diff = abs(float(row['differential']))
            if abs_diff < self.config.minimum_differential:
                return False
            
            return True
            
        except (ValueError, TypeError, KeyError) as e:
            self.logger.warning(f"Invalid signal data: {e}")
            return False
    
    def _should_apply_juice_filter(self, row: Dict[str, Any]) -> bool:
        """Check if juice filter should be applied to this signal"""
        if row['split_type'] != 'moneyline':
            return False
        
        # Determine recommended side
        differential = row['differential']
        recommended_team = row['home_team'] if differential > 0 else row['away_team']
        
        return self._should_filter_juice(
            row['split_type'], row.get('split_value'),
            recommended_team, row['home_team'], row['away_team']
        ) 