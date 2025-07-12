"""
Enhanced Base Strategy Processor

Base class for all strategy processors with strategy-specific functionality,
error handling, and validation methods.

Part of Phase 1 foundation architecture for comprehensive strategy processing.
Incorporates selected improvements for confidence calculation and strategy matching.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple
import asyncio
from enum import Enum
from datetime import datetime
import pytz
from datetime import timedelta

from ...models.betting_analysis import BettingSignal, ProfitableStrategy, SignalType, SignalProcessorConfig
from ...services.betting_signal_repository import BettingSignalRepository
from ...services.strategy_validation import StrategyValidation
from ...core.logging import get_logger


class ConfidenceModifierType(Enum):
    """Types of confidence modifiers that can be applied"""
    BOOK_RELIABILITY = "book_reliability"
    STRATEGY_PERFORMANCE = "strategy_performance"
    TIME_DECAY = "time_decay"
    SIGNAL_STRENGTH = "signal_strength"


class BaseStrategyProcessor(ABC):
    """
    Enhanced base class for all strategy processors
    
    Provides strategy-specific functionality including error handling,
    validation, and strategy categorization.
    """
    
    def __init__(self, repository: BettingSignalRepository, validator: StrategyValidation, 
                 config: SignalProcessorConfig):
        """Initialize the processor with required dependencies"""
        self.repository = repository
        self.validator = validator
        self.config = config
        self.logger = get_logger(self.__class__.__name__)
        self.est = pytz.timezone('US/Eastern')
    
    @abstractmethod
    def get_signal_type(self) -> SignalType:
        """Return the signal type this processor handles"""
        pass
    
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
    
    @abstractmethod
    async def process(self, minutes_ahead: int, 
                     profitable_strategies: List[ProfitableStrategy]) -> List[BettingSignal]:
        """Process signals for this strategy"""
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
    
    # ===== ENHANCED CONFIDENCE CALCULATION (from coworker's version) =====
    
    def calculate_confidence_with_modifiers(self, row: Dict[str, Any], 
                                          matching_strategy: ProfitableStrategy,
                                          modifiers: Dict[ConfidenceModifierType, float] = None) -> Dict[str, Any]:
        """
        Calculate confidence with flexible modifier system.
        Simplified version of coworker's approach for immediate value.
        """
        base_confidence = self._calculate_confidence(
            row['differential'], row['source'], row['book'],
            row['split_type'], matching_strategy.strategy_name,
            row['last_updated'], self._normalize_game_time(row['game_datetime'])
        )
        
        total_modifier = 1.0
        applied_modifiers = {}
        
        # Apply modifiers if provided
        if modifiers:
            for modifier_type, modifier_value in modifiers.items():
                if modifier_type == ConfidenceModifierType.BOOK_RELIABILITY:
                    book_mod = self._calculate_book_modifier(row.get('source'), row.get('book'), matching_strategy)
                    total_modifier *= book_mod
                    applied_modifiers['book_reliability'] = book_mod
                    
                elif modifier_type == ConfidenceModifierType.STRATEGY_PERFORMANCE:
                    perf_mod = self._calculate_performance_modifier(matching_strategy)
                    total_modifier *= perf_mod
                    applied_modifiers['strategy_performance'] = perf_mod
                    
                elif modifier_type == ConfidenceModifierType.SIGNAL_STRENGTH:
                    strength_mod = self._calculate_signal_strength_modifier(row)
                    total_modifier *= strength_mod
                    applied_modifiers['signal_strength'] = strength_mod
        
        adjusted_confidence = base_confidence['confidence_score'] * total_modifier
        adjusted_confidence = max(0.1, min(0.95, adjusted_confidence))  # Keep in valid range
        
        return {
            **base_confidence,
            'confidence_score': adjusted_confidence,
            'total_modifier': total_modifier,
            'applied_modifiers': applied_modifiers
        }
    
    def _calculate_book_modifier(self, source: str, book: str, strategy: ProfitableStrategy = None) -> float:
        """
        Calculate book modifier based on historical strategy performance on specific books.
        
        If strategy is provided, looks up historical ROI for this strategy-book combination.
        Otherwise returns neutral modifier of 1.0 (no arbitrary assumptions).
        """
        if not source or not book or not strategy:
            return 1.0
        
        # TODO: Implement lookup of historical ROI for this strategy-book combination
        # This would query the database for past performance of this specific strategy
        # on this specific book and return a modifier based on that data
        # 
        # Example logic:
        # strategy_book_roi = self._get_strategy_book_historical_roi(strategy.strategy_name, source, book)
        # if strategy_book_roi > 10:  # Strong historical performance
        #     return 1.1
        # elif strategy_book_roi < -5:  # Poor historical performance  
        #     return 0.9
        # else:
        #     return 1.0
        
        # For now, return neutral until we implement historical data lookup
        return 1.0
    
    def _calculate_performance_modifier(self, strategy: ProfitableStrategy) -> float:
        """Calculate strategy performance modifier"""
        modifier = 1.0
        
        # Win rate impact
        if strategy.win_rate >= 70:
            modifier *= 1.2
        elif strategy.win_rate >= 60:
            modifier *= 1.1
        elif strategy.win_rate < 55:
            modifier *= 0.9
        
        # Sample size impact
        if strategy.total_bets >= 100:
            modifier *= 1.05
        elif strategy.total_bets < 20:
            modifier *= 0.95
        
        return modifier
    
    def _calculate_signal_strength_modifier(self, row: Dict[str, Any]) -> float:
        """Calculate signal strength modifier based on data quality"""
        modifier = 1.0
        
        # Differential strength
        abs_diff = abs(float(row.get('differential', 0)))
        if abs_diff >= 25:
            modifier *= 1.15
        elif abs_diff >= 20:
            modifier *= 1.1
        elif abs_diff >= 15:
            modifier *= 1.05
        
        return modifier
    
    # ===== ENHANCED STRATEGY MATCHING (from coworker's version) =====
    
    def extract_strategy_components(self, strategy_name: str) -> Tuple[str, str]:
        """
        Extract source_book and split_type from strategy name.
        Simplified version for immediate use.
        """
        strategy_name_lower = strategy_name.lower()
        
        # Handle direct format: "SOURCE-BOOK-SPLITTYPE"
        if strategy_name.count('-') >= 2:
            parts = strategy_name.split('-')
            if len(parts) >= 3:
                source_book = f"{parts[0]}-{parts[1]}"
                split_type = parts[2]
                return source_book, split_type
        
        # Default extraction logic
        source_book = self._extract_source_book(strategy_name_lower)
        split_type = self._extract_split_type(strategy_name_lower)
        
        return source_book, split_type
    
    def _extract_source_book(self, strategy_name_lower: str) -> str:
        """Extract source-book combination from strategy name"""
        if 'vsin-dra' in strategy_name_lower or 'vsin-draftkings' in strategy_name_lower:
            return "VSIN-draftkings"
        elif 'vsin-cir' in strategy_name_lower or 'vsin-circa' in strategy_name_lower:
            return "VSIN-circa"
        elif 'sbd' in strategy_name_lower:
            return "SBD-unknown"
        elif 'vsin' in strategy_name_lower:
            return "VSIN-unknown"
        else:
            return "unknown-unknown"
    
    def _extract_split_type(self, strategy_name_lower: str) -> str:
        """Extract split type from strategy name"""
        if 'moneyline' in strategy_name_lower or '_ml_' in strategy_name_lower:
            return "moneyline"
        elif 'spread' in strategy_name_lower or '_sprd_' in strategy_name_lower:
            return "spread"
        elif 'total' in strategy_name_lower or '_tot_' in strategy_name_lower:
            return "total"
        else:
            return "unknown"
    
    def get_processor_info(self) -> Dict[str, Any]:
        """Get comprehensive information about this processor"""
        return {
            'processor_name': self.__class__.__name__,
            'signal_type': self.get_signal_type().value,
            'strategy_category': self.get_strategy_category(),
            'description': self.get_strategy_description(),
            'required_tables': self.get_required_tables(),
            'supports_parallel_processing': True,
            'version': '1.5_enhanced',
            'enhancements': [
                'modular_confidence_calculation',
                'enhanced_strategy_matching',
                'book_reliability_modifiers'
            ]
        }
        
    # ===== UTILITY METHODS =====
    
    def _normalize_game_time(self, game_datetime) -> datetime:
        """Normalize game datetime to EST timezone"""
        if isinstance(game_datetime, str):
            from dateutil import parser
            game_datetime = parser.parse(game_datetime)
        
        if game_datetime.tzinfo is None:
            # 🚨 TIMEZONE BUG FIX: Database stores game times in EST, not UTC
            # Previous logic incorrectly assumed timezone-naive datetimes were UTC
            # This caused games to appear 4-5 hours in the past during analysis
            game_datetime = self.est.localize(game_datetime)
        
        return game_datetime.astimezone(self.est)
    
    def _calculate_minutes_to_game(self, game_time: datetime, current_time: datetime) -> int:
        """Calculate minutes from current time to game time"""
        time_diff = game_time - current_time
        return int(time_diff.total_seconds() / 60)
    
    def _create_time_window(self, minutes_ahead: int) -> Tuple[datetime, datetime]:
        """Create time window for signal processing"""
        now_est = datetime.now(self.est)
        end_time = now_est + timedelta(minutes=minutes_ahead)
        return now_est, end_time
    
    def _should_filter_juice(self, split_type: str, split_value: Any, 
                           recommended_team: str, home_team: str, away_team: str) -> bool:
        """Apply juice filter based on user preferences (-160 limit)"""
        if split_type != 'moneyline':
            return False
            
        try:
            # Handle JSON format split_value
            if isinstance(split_value, str) and split_value.startswith('{'):
                import json
                odds_data = json.loads(split_value)
                
                if recommended_team == home_team:
                    odds = odds_data.get('home')
                else:
                    odds = odds_data.get('away')
                    
                if odds and odds < -160:
                    return True
                    
        except (json.JSONDecodeError, KeyError, TypeError):
            pass
            
        return False
    
    def _calculate_confidence(self, differential: float, source: str, book: str,
                            split_type: str, strategy_name: str, 
                            last_updated: datetime, game_time: datetime) -> Dict[str, Any]:
        """Calculate base confidence score for a signal"""
        
        # Base confidence from differential strength
        # Ensure robust type conversion from Decimal to float to avoid arithmetic errors
        try:
            if hasattr(differential, '__float__'):
                # Handle Decimal types explicitly
                abs_diff = float(abs(differential))
            else:
                abs_diff = abs(float(differential))
        except (ValueError, TypeError, AttributeError) as e:
            # Fallback to a safe default if conversion fails
            self.logger.warning(f"Failed to convert differential to float: {differential} ({type(differential)}), using 0.0")
            abs_diff = 0.0
            
        base_score = min(0.95, abs_diff / 30.0)  # Scale to max 95%
        
        # Time-based modifier (closer to game = more confident)
        now_est = datetime.now(self.est)
        minutes_to_game = self._calculate_minutes_to_game(game_time, now_est)
        
        if minutes_to_game <= 60:  # Within 1 hour
            time_modifier = 1.1
        elif minutes_to_game <= 180:  # Within 3 hours
            time_modifier = 1.05
        else:
            time_modifier = 1.0
            
        # Data freshness modifier
        # ✅ FIX: Ensure timezone compatibility for datetime operations
        if last_updated.tzinfo is None:
            # If last_updated is timezone-naive, assume it's in EST
            last_updated = pytz.timezone('US/Eastern').localize(last_updated)
        elif last_updated.tzinfo != self.est:
            # Convert to EST if in different timezone
            last_updated = last_updated.astimezone(self.est)
            
        data_age_hours = (now_est - last_updated).total_seconds() / 3600
        if data_age_hours <= 1:
            freshness_modifier = 1.05
        elif data_age_hours <= 4:
            freshness_modifier = 1.0
        else:
            freshness_modifier = 0.95
            
        final_confidence = base_score * time_modifier * freshness_modifier
        final_confidence = max(0.1, min(0.95, final_confidence))  # Keep in valid range
        
        # ✅ FIX: Add missing confidence_level, confidence_explanation, and recommendation_strength
        from ...models.betting_analysis import ConfidenceLevel
        
        # Determine confidence level based on final confidence score
        if final_confidence >= 0.85:
            confidence_level = ConfidenceLevel.VERY_HIGH
        elif final_confidence >= 0.7:
            confidence_level = ConfidenceLevel.HIGH
        elif final_confidence >= 0.55:
            confidence_level = ConfidenceLevel.MODERATE
        elif final_confidence >= 0.4:
            confidence_level = ConfidenceLevel.LOW
        else:
            confidence_level = ConfidenceLevel.VERY_LOW
        
        # Create confidence explanation
        confidence_explanation = (
            f"{abs_diff:.1f}% differential • {source} {book} {split_type} • "
            f"{minutes_to_game}min to game • {data_age_hours:.1f}h old data • "
            f"{final_confidence:.2f} confidence ({confidence_level.value})"
        )
        
        # Determine recommendation strength based on confidence
        if final_confidence >= 0.8:
            recommendation_strength = "STRONG"
        elif final_confidence >= 0.6:
            recommendation_strength = "MODERATE"
        else:
            recommendation_strength = "WEAK"
        
        return {
            'confidence_score': final_confidence,
            'confidence_level': confidence_level,  # ✅ ADDED: Required by _create_betting_signal
            'confidence_explanation': confidence_explanation,  # ✅ ADDED: Required by _create_betting_signal  
            'recommendation_strength': recommendation_strength,  # ✅ ADDED: Required by _create_betting_signal
            'base_score': base_score,
            'time_modifier': time_modifier,
            'freshness_modifier': freshness_modifier,
            'minutes_to_game': minutes_to_game,
            'data_age_hours': data_age_hours
        }
    
    def _log_processing_summary(self, total_signals: int, total_strategies: int, total_opportunities: int):
        """
        Log processing summary for strategy processors
        
        This method provides standardized logging for processor results
        to track performance and debugging information.
        """
        processor_name = self.__class__.__name__
        
        self.logger.info(
            f"📊 {processor_name} Summary",
            total_signals=total_signals,
            profitable_strategies=total_strategies,
            opportunities_found=total_opportunities
        )
        
        # Log performance indicator
        if total_opportunities > 0:
            self.logger.info(f"✅ {processor_name}: Found {total_opportunities} betting opportunities")
        else:
            self.logger.info(f"ℹ️  {processor_name}: No opportunities found in current data")
        
        # Log efficiency metric
        if total_signals > 0:
            efficiency = (total_opportunities / total_signals) * 100
            self.logger.debug(f"📈 {processor_name} Efficiency: {efficiency:.1f}% ({total_opportunities}/{total_signals})")
        
        # Log strategy utilization
        if total_strategies > 0:
            self.logger.debug(f"🎯 {processor_name} Strategy Utilization: {total_strategies} profitable strategies found")

    def _create_betting_signal(self, raw_data: Dict[str, Any], strategy: ProfitableStrategy,
                              confidence_data: Dict[str, Any], metadata: Dict[str, Any] = None) -> BettingSignal:
        """
        Create a betting signal from raw data, strategy, and confidence information.
        
        This is a generic implementation that can be used by most processors.
        Processors with special needs can override this method.
        
        Args:
            raw_data: Raw signal data from the database
            strategy: Matching profitable strategy
            confidence_data: Confidence calculation results
            metadata: Optional additional metadata
            
        Returns:
            BettingSignal: Complete betting signal ready for analysis
        """
        from ...models.betting_analysis import BettingSignal
        
        # Determine recommended team based on differential
        differential = float(raw_data['differential'])
        if differential > 0:
            recommended_team = raw_data['home_team']
            recommendation = f"BET {recommended_team} (HOME)"
        else:
            recommended_team = raw_data['away_team']
            recommendation = f"BET {recommended_team} (AWAY)"
        
        # Normalize game time
        game_time = self._normalize_game_time(raw_data['game_datetime'])
        now_est = datetime.now(self.est)
        
        # Format book attribution to make data source clear
        source = raw_data.get('source', 'Unknown')
        raw_book = raw_data.get('book', 'unknown')
        
        # Create clear book attribution
        if source == 'VSIN' and raw_book and raw_book != 'unknown':
            # For VSIN data, show source with specific book
            book_attribution = f"VSIN ({raw_book.title()})"
        elif source == 'SBD':
            # For SBD data, just show SBD since it's their own data
            book_attribution = "SBD"
        else:
            # For other cases, show both if available
            if raw_book and raw_book != 'unknown':
                book_attribution = f"{source} ({raw_book.title()})"
            else:
                book_attribution = source
        
        # Prepare metadata
        signal_metadata = {
            'processor': self.__class__.__name__,
            'strategy_category': self.get_strategy_category(),
            'raw_differential': differential,
            'original_source': source,  # Keep original source for reference
            'original_book': raw_book,  # Keep original book for reference
            'split_type': raw_data.get('split_type')
        }
        
        if metadata:
            signal_metadata.update(metadata)
        
        # Create the betting signal
        return BettingSignal(
            signal_type=self.get_signal_type(),
            home_team=raw_data['home_team'],
            away_team=raw_data['away_team'],
            game_time=game_time,
            minutes_to_game=self._calculate_minutes_to_game(game_time, now_est),
            split_type=raw_data['split_type'],
            split_value=raw_data.get('split_value'),
            source=source,  # Keep original source for compatibility
            book=book_attribution,  # Use formatted book attribution
            differential=differential,
            signal_strength=abs(differential),  # Use absolute differential as signal strength
            confidence_score=confidence_data['confidence_score'],
            confidence_level=confidence_data['confidence_level'],
            confidence_explanation=confidence_data['confidence_explanation'],
            recommendation=recommendation,
            recommendation_strength=confidence_data['recommendation_strength'],
            last_updated=raw_data['last_updated'],
            strategy_name=strategy.strategy_name,
            win_rate=strategy.win_rate,
            roi=strategy.roi,
            total_bets=strategy.total_bets,
            metadata=signal_metadata
        ) 