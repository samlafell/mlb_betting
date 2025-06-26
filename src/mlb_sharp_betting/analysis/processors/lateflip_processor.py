"""
Late Flip Processor

Detects late sharp money direction changes and implements fade-the-late-flip strategy.
When sharp money flips direction in the final 2-3 hours before game time,
fade the late sharp action and follow the early sharp action.

Converts late_sharp_flip_strategy_postgres.sql logic to Python processor.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import json

from .base_strategy_processor import BaseStrategyProcessor
from ...models.betting_analysis import BettingSignal, SignalType, ProfitableStrategy


class LateFlipProcessor(BaseStrategyProcessor):
    """
    Processor for detecting late sharp flip signals
    
    Identifies games where sharp money flips direction late (within 3 hours of game time)
    and recommends fading the late flip while following the original early sharp action.
    Theory: Late flips are often "dumb money" disguised as sharp action.
    """
    
    def get_signal_type(self) -> SignalType:
        """Return the signal type this processor handles"""
        return SignalType.LATE_FLIP
    
    def get_strategy_category(self) -> str:
        """Return strategy category for proper routing"""
        return "TIMING_BASED"
    
    def get_required_tables(self) -> List[str]:
        """Return database tables required for this strategy"""
        return ["splits.raw_mlb_betting_splits"]
    
    def get_strategy_description(self) -> str:
        """Return human-readable description of the strategy"""
        return "Detects late sharp money direction changes and fades the late flip while following early sharp action"
    
    def validate_strategy_data(self, raw_data: List[Dict]) -> bool:
        """Validate we have time-series betting data for flip detection"""
        if not raw_data:
            return False
            
        # Check we have data with timestamps and differentials
        has_timeline_data = any(
            row.get('last_updated') is not None and 
            row.get('differential') is not None and
            abs(row.get('differential', 0)) >= 8  # Minimum sharp threshold
            for row in raw_data
        )
        
        return has_timeline_data
    
    async def process(self, minutes_ahead: int, 
                     profitable_strategies: List[ProfitableStrategy]) -> List[BettingSignal]:
        """Process late flip signals using profitable strategies"""
        start_time, end_time = self._create_time_window(minutes_ahead)
        
        # Get time-series betting data for flip analysis
        steam_data = await self.repository.get_steam_move_data(start_time, end_time)
        
        if not steam_data:
            self.logger.info("Insufficient time-series data for late flip analysis")
            return []
        
        # Find late flip opportunities
        flip_opportunities = self._find_late_flip_opportunities(steam_data)
        
        if not flip_opportunities:
            self.logger.info("No late flip opportunities found")
            return []
        
        # Convert to signals
        signals = []
        now_est = datetime.now(self.est)
        
        for flip_data in flip_opportunities:
            # Apply basic filters
            if not self._is_valid_flip_data(flip_data, now_est, minutes_ahead):
                continue
                
            # Check if flip strength is significant enough
            flip_strength = flip_data.get('flip_strength', 0)
            if flip_strength < 20.0:  # Minimum combined flip strength
                continue
            
            # Apply juice filter if needed
            if self._should_apply_juice_filter(flip_data):
                continue
            
            # Find matching profitable strategies
            matching_strategies = self._find_matching_strategies(profitable_strategies, flip_data)
            if not matching_strategies:
                continue
            
            matching_strategy = matching_strategies[0]  # Use best matching strategy
            
            # Calculate confidence
            confidence_data = self._calculate_confidence_for_late_flip(flip_data, matching_strategy)
            
            # Create the signal
            signal = self._create_betting_signal(flip_data, matching_strategy, confidence_data)
            signals.append(signal)
        
        self._log_processing_summary(len(signals), len(profitable_strategies), len(flip_opportunities))
        return signals
    
    def _find_late_flip_opportunities(self, steam_data: List[Dict]) -> List[Dict]:
        """
        Find games with late sharp money flips
        Implements the core logic from late_sharp_flip_strategy_postgres.sql
        """
        flip_opportunities = []
        
        # Group data by game/source/book for timeline analysis
        grouped_data = self._group_by_game_source_book(steam_data)
        
        for game_key, timeline_data in grouped_data.items():
            if len(timeline_data) < 3:  # Need multiple updates to detect flip
                continue
            
            # Sort by timestamp to create timeline
            timeline_data.sort(key=lambda x: x['last_updated'])
            
            flip_analysis = self._analyze_timeline_for_flip(timeline_data)
            
            if flip_analysis and self._is_significant_flip(flip_analysis):
                flip_analysis['game_key'] = game_key
                flip_opportunities.append(flip_analysis)
        
        return flip_opportunities
    
    def _group_by_game_source_book(self, data: List[Dict]) -> Dict[tuple, List[Dict]]:
        """Group steam data by game/source/book for timeline analysis"""
        grouped = {}
        
        for record in data:
            # Only focus on moneyline for clear win/loss tracking
            if record.get('split_type') != 'moneyline':
                continue
                
            key = (
                record.get('home_team'),
                record.get('away_team'), 
                record.get('game_datetime'),
                record.get('source'),
                record.get('book', 'UNKNOWN')
            )
            
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(record)
        
        return grouped
    
    def _analyze_timeline_for_flip(self, timeline_data: List[Dict]) -> Optional[Dict[str, Any]]:
        """
        Analyze timeline data to detect late sharp flips
        Returns flip analysis or None if no significant flip detected
        """
        if len(timeline_data) < 3:
            return None
        
        # Calculate time periods for each update
        enhanced_timeline = []
        for record in timeline_data:
            game_datetime = self._normalize_game_time(record['game_datetime'])
            last_updated = self._normalize_game_time(record['last_updated'])
            
            hours_before_game = (game_datetime - last_updated).total_seconds() / 3600
            
            # Classify time period
            if hours_before_game >= 4:
                time_period = 'EARLY'
            elif hours_before_game >= 1:
                time_period = 'LATE'
            else:
                time_period = 'VERY_LATE'
            
            enhanced_record = record.copy()
            enhanced_record['hours_before_game'] = hours_before_game
            enhanced_record['time_period'] = time_period
            enhanced_timeline.append(enhanced_record)
        
        # Find early and late sharp actions
        early_readings = [r for r in enhanced_timeline if r['time_period'] == 'EARLY']
        late_readings = [r for r in enhanced_timeline if r['time_period'] in ['LATE', 'VERY_LATE']]
        
        if not early_readings or not late_readings:
            return None
        
        # Get first significant early reading and final late reading
        early_reading = None
        for reading in early_readings:
            if abs(reading.get('differential', 0)) >= 8:  # Minimum sharp threshold
                early_reading = reading
                break
        
        if not early_reading:
            return None
        
        # Get final late reading
        late_reading = late_readings[-1]  # Most recent reading
        
        if abs(late_reading.get('differential', 0)) < 8:  # Must be significant
            return None
        
        # Check for flip (opposite signs and both significant)
        early_diff = early_reading['differential']
        late_diff = late_reading['differential']
        
        flip_type = None
        if (early_diff > 0 and late_diff < 0) and (abs(early_diff) >= 10 and abs(late_diff) >= 10):
            flip_type = 'HOME_TO_AWAY'
        elif (early_diff < 0 and late_diff > 0) and (abs(early_diff) >= 10 and abs(late_diff) >= 10):
            flip_type = 'AWAY_TO_HOME'
        
        if not flip_type:
            return None
        
        # Calculate flip metrics
        early_sharp_side = 'HOME' if early_diff > 0 else 'AWAY'
        late_sharp_side = 'HOME' if late_diff > 0 else 'AWAY'
        
        # Fade late flip recommendation = follow early sharp
        fade_late_flip_pick = early_sharp_side
        
        hours_between_readings = abs(early_reading['hours_before_game'] - late_reading['hours_before_game'])
        
        # Categorize flip strength and timing
        flip_strength = (abs(early_diff) + abs(late_diff)) / 2
        
        if abs(early_diff) >= 20 and abs(late_diff) >= 20:
            strength_category = 'STRONG_FLIP'
        elif abs(early_diff) >= 15 or abs(late_diff) >= 15:
            strength_category = 'MEDIUM_FLIP'
        else:
            strength_category = 'WEAK_FLIP'
        
        if hours_between_readings >= 6:
            timing_category = 'LONG_DEVELOPMENT'
        elif hours_between_readings >= 3:
            timing_category = 'MEDIUM_DEVELOPMENT'
        else:
            timing_category = 'QUICK_FLIP'
        
        # Build flip analysis
        flip_analysis = {
            'home_team': timeline_data[0]['home_team'],
            'away_team': timeline_data[0]['away_team'],
            'game_datetime': timeline_data[0]['game_datetime'],
            'split_type': 'moneyline',
            'source': timeline_data[0]['source'],
            'book': timeline_data[0].get('book', 'UNKNOWN'),
            
            # Flip detection
            'flip_type': flip_type,
            'early_sharp_side': early_sharp_side,
            'late_sharp_side': late_sharp_side,
            'fade_late_flip_pick': fade_late_flip_pick,
            
            # Timing metrics
            'early_differential': early_diff,
            'late_differential': late_diff,
            'early_timestamp': early_reading['last_updated'],
            'late_timestamp': late_reading['last_updated'],
            'hours_between_readings': hours_between_readings,
            
            # Strength metrics
            'flip_strength': flip_strength,
            'strength_category': strength_category,
            'timing_category': timing_category,
            
            # Total updates and timeline
            'total_updates': len(timeline_data),
            'timeline_data': enhanced_timeline,
            
            # For signal creation
            'differential': flip_strength,  # Use flip strength as differential measure
            'split_value': f"FLIP_{flip_type}",
            'last_updated': late_reading['last_updated']
        }
        
        return flip_analysis
    
    def _is_significant_flip(self, flip_analysis: Dict[str, Any]) -> bool:
        """
        Determine if the flip is significant enough to act on
        Based on thresholds from SQL logic
        """
        # Must have adequate update history
        if flip_analysis['total_updates'] < 3:
            return False
        
        # Both early and late readings must be significant
        if abs(flip_analysis['early_differential']) < 10.0:
            return False
        if abs(flip_analysis['late_differential']) < 10.0:
            return False
        
        # Flip strength must be adequate
        if flip_analysis['flip_strength'] < 15.0:
            return False
        
        # Should have reasonable time development (not too quick, not too long)
        hours_between = flip_analysis['hours_between_readings']
        if hours_between < 1.0 or hours_between > 48.0:
            return False
        
        return True
    
    def _is_valid_flip_data(self, flip_data: Dict[str, Any], now_est, minutes_ahead: int) -> bool:
        """Enhanced validation for late flip data"""
        # Basic validation
        required_fields = ['home_team', 'away_team', 'game_datetime', 'flip_type', 'fade_late_flip_pick', 'flip_strength']
        if not all(field in flip_data for field in required_fields):
            return False
        
        # Time window validation
        game_time = self._normalize_game_time(flip_data['game_datetime'])
        time_diff = self._calculate_minutes_to_game(game_time, now_est)
        
        if time_diff < 0 or time_diff > minutes_ahead:
            return False
        
        # Must have a flip recommendation
        if not flip_data.get('fade_late_flip_pick'):
            return False
        
        # Minimum flip strength
        if flip_data.get('flip_strength', 0) < 15.0:
            return False
        
        return True
    
    def _calculate_confidence_for_late_flip(self, flip_data: Dict[str, Any], 
                                          strategy: ProfitableStrategy) -> Dict[str, Any]:
        """
        Calculate confidence score for late flip signals
        """
        base_confidence = strategy.confidence_score
        
        # Adjust based on flip strength
        flip_strength = flip_data.get('flip_strength', 15)
        strength_multiplier = min(1.4, 1.0 + (flip_strength - 15.0) / 40.0)
        
        # Adjust based on strength category
        strength_category = flip_data.get('strength_category', 'WEAK_FLIP')
        strength_category_multiplier = {
            'STRONG_FLIP': 1.3,
            'MEDIUM_FLIP': 1.1,
            'WEAK_FLIP': 0.9
        }.get(strength_category, 1.0)
        
        # Adjust based on timing development
        timing_category = flip_data.get('timing_category', 'QUICK_FLIP')
        timing_multiplier = {
            'LONG_DEVELOPMENT': 1.2,    # Gradual development more reliable
            'MEDIUM_DEVELOPMENT': 1.1,  # Good development time
            'QUICK_FLIP': 0.9          # Quick flips less reliable
        }.get(timing_category, 1.0)
        
        # Adjust based on number of updates (more data = more confidence)
        total_updates = flip_data.get('total_updates', 3)
        updates_multiplier = min(1.2, 1.0 + (total_updates - 3) / 10.0)
        
        # Adjust based on hours between readings
        hours_between = flip_data.get('hours_between_readings', 3)
        time_development_multiplier = min(1.1, max(0.9, 1.0 + (hours_between - 6) / 20.0))
        
        final_confidence = (base_confidence * strength_multiplier * strength_category_multiplier * 
                          timing_multiplier * updates_multiplier * time_development_multiplier)
        final_confidence = max(0.1, min(1.0, final_confidence))
        
        return {
            'base_confidence': base_confidence,
            'strength_multiplier': strength_multiplier,
            'strength_category_multiplier': strength_category_multiplier,
            'timing_multiplier': timing_multiplier,
            'updates_multiplier': updates_multiplier,
            'time_development_multiplier': time_development_multiplier,
            'final_confidence': final_confidence,
            'flip_strength': flip_strength,
            'strength_category': strength_category,
            'timing_category': timing_category,
            'total_updates': total_updates,
            'hours_between': hours_between
        } 