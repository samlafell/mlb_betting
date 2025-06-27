"""
Hybrid Sharp Action + Line Movement Processor

Combines line movement analysis with sharp action detection to create high-confidence
betting opportunities. Looks for confirmation between line movement and professional 
money flow patterns.

This processor implements the core logic from hybrid_line_sharp_strategy_postgres.sql (11KB, 271 lines),
identifying situations where:
1. Line movement confirms sharp action direction
2. Steam plays (sharp action without line movement)
3. Reverse line movement (public betting opposite to line movement)
4. Strong confirmation signals (both indicators align)

Key Strategy Classifications:
- STRONG_CONFIRMATION: Line movement + strong sharp action in same direction
- MODERATE_CONFIRMATION: Moderate alignment between line and sharp signals
- STEAM_PLAY: Strong sharp action without significant line movement
- REVERSE_LINE_MOVEMENT: Line moves opposite to public betting patterns
"""

from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import json

from ...models.betting_analysis import BettingSignal, SignalType, ProfitableStrategy
from .base_strategy_processor import BaseStrategyProcessor


class HybridSharpProcessor(BaseStrategyProcessor):
    """
    Hybrid processor combining line movement analysis with sharp action detection.
    
    Creates comprehensive betting signals by analyzing the correlation between:
    - Line movement patterns
    - Professional money indicators (sharp action)
    - Public betting behavior
    - Opening vs closing line dynamics
    
    This is the modern implementation of hybrid_line_sharp_strategy_postgres.sql.
    """
    
    def get_signal_type(self) -> SignalType:
        """Return the signal type this processor handles"""
        return SignalType.HYBRID_SHARP
    
    def get_strategy_category(self) -> str:
        """Return strategy category for proper routing"""
        return "HYBRID_SHARP_LINE_MOVEMENT"
    
    def get_required_tables(self) -> List[str]:
        """Return database tables required for this strategy"""
        return ["splits.raw_mlb_betting_splits", "public.game_outcomes"]
    
    def get_strategy_description(self) -> str:
        """Return human-readable description of the strategy"""
        return "Hybrid strategy combining line movement with sharp action for enhanced signal confidence"
    
    async def process(self, minutes_ahead: int, 
                     profitable_strategies: List[ProfitableStrategy]) -> List[BettingSignal]:
        """Process hybrid sharp + line movement signals"""
        # Placeholder implementation
        self.logger.info("Processing hybrid sharp signals...")
        return []
    
    def _calculate_hybrid_metrics(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate comprehensive hybrid metrics from the data.
        
        Returns dictionary with line movement and sharp action metrics.
        """
        try:
            # Extract core values
            differential = float(row.get('differential', 0))
            stake_pct = float(row.get('stake_pct', 50))
            bet_pct = float(row.get('bet_pct', 50))
            
            # Calculate line movement from split_value if available
            line_movement = self._calculate_line_movement(row)
            
            # Classify sharp action strength
            sharp_strength = self._classify_sharp_strength(differential)
            
            # Determine sharp direction
            sharp_direction = self._determine_sharp_direction(differential, row['split_type'])
            
            # Calculate line movement significance
            line_significance = self._classify_line_movement(line_movement, row['split_type'])
            
            return {
                'differential': differential,
                'stake_pct': stake_pct,
                'bet_pct': bet_pct,
                'line_movement': line_movement,
                'sharp_strength': sharp_strength,
                'sharp_direction': sharp_direction,
                'line_significance': line_significance,
                'abs_differential': abs(differential),
                'abs_line_movement': abs(line_movement) if line_movement else 0
            }
            
        except (ValueError, TypeError, KeyError) as e:
            self.logger.warning(f"Failed to calculate hybrid metrics: {e}")
            return {
                'differential': 0,
                'line_movement': 0,
                'sharp_strength': 'NO_SHARP',
                'sharp_direction': 'NEUTRAL',
                'line_significance': 'MINIMAL',
                'abs_differential': 0,
                'abs_line_movement': 0
            }
    
    def _calculate_line_movement(self, row: Dict[str, Any]) -> Optional[float]:
        """Calculate line movement from available data."""
        try:
            # Try to extract from metadata if available
            if row.get('line_movement') is not None:
                return float(row['line_movement'])
            
            # Calculate from split_value changes if historical data available
            split_value = row.get('split_value')
            if split_value and isinstance(split_value, str):
                try:
                    if split_value.startswith('{'):
                        # JSON format for moneyline
                        value_data = json.loads(split_value)
                        # This would need historical comparison - simplified for now
                        return 0.0
                    else:
                        # Numeric format for spread/total
                        return 0.0  # Would need historical comparison
                except json.JSONDecodeError:
                    return 0.0
            
            return 0.0
            
        except (ValueError, TypeError):
            return 0.0
    
    def _classify_sharp_strength(self, differential: float) -> str:
        """Classify the strength of sharp action based on differential."""
        abs_diff = abs(differential)
        
        if abs_diff >= 15:
            return 'STRONG_SHARP'
        elif abs_diff >= 10:
            return 'MODERATE_SHARP'
        elif abs_diff >= 5:
            return 'WEAK_SHARP'
        else:
            return 'NO_SHARP'
    
    def _determine_sharp_direction(self, differential: float, split_type: str) -> str:
        """Determine the direction of sharp action."""
        if differential >= 5:
            return 'HOME_OVER' if split_type != 'total' else 'OVER'
        elif differential <= -5:
            return 'AWAY_UNDER' if split_type != 'total' else 'UNDER'
        else:
            return 'NEUTRAL'
    
    def _classify_line_movement(self, line_movement: Optional[float], split_type: str) -> str:
        """Classify the significance of line movement."""
        if line_movement is None:
            return 'UNKNOWN'
        
        abs_movement = abs(line_movement)
        
        if split_type == 'moneyline':
            if abs_movement >= 20:
                return 'SIGNIFICANT'
            elif abs_movement >= 10:
                return 'MODERATE'
            elif abs_movement >= 5:
                return 'MINOR'
            else:
                return 'MINIMAL'
        else:  # spread or total
            if abs_movement >= 2.0:
                return 'SIGNIFICANT'
            elif abs_movement >= 1.0:
                return 'MODERATE'
            elif abs_movement >= 0.5:
                return 'MINOR'
            else:
                return 'MINIMAL'
    
    def _classify_hybrid_strategy(self, metrics: Dict[str, Any]) -> str:
        """
        Classify the type of hybrid strategy based on metrics.
        
        Returns strategy classification matching legacy SQL logic.
        """
        sharp_strength = metrics['sharp_strength']
        line_significance = metrics['line_significance']
        line_movement = metrics.get('line_movement', 0)
        differential = metrics['differential']
        
        # Strong confirmation: line movement + strong sharp action in same direction
        if line_significance in ['SIGNIFICANT', 'MODERATE'] and sharp_strength == 'STRONG_SHARP':
            movement_direction = 1 if line_movement > 0 else -1
            sharp_direction = 1 if differential > 0 else -1
            
            if movement_direction == sharp_direction:
                return 'STRONG_CONFIRMATION'
            else:
                return 'STRONG_CONFLICT'
        
        # Moderate confirmation
        elif line_significance in ['MODERATE', 'MINOR'] and sharp_strength == 'MODERATE_SHARP':
            movement_direction = 1 if line_movement > 0 else -1
            sharp_direction = 1 if differential > 0 else -1
            
            if movement_direction == sharp_direction:
                return 'MODERATE_CONFIRMATION'
            else:
                return 'MODERATE_CONFLICT'
        
        # Steam play: strong sharp action without significant line movement
        elif line_significance == 'MINIMAL' and sharp_strength == 'STRONG_SHARP':
            return 'STEAM_PLAY'
        
        # Public move: line movement without sharp confirmation
        elif line_significance == 'SIGNIFICANT' and sharp_strength == 'NO_SHARP':
            return 'PUBLIC_MOVE'
        
        # Reverse line movement: line moves opposite to public betting
        elif line_significance in ['SIGNIFICANT', 'MODERATE']:
            stake_pct = metrics.get('stake_pct', 50)
            if ((line_movement > 0 and stake_pct < 40) or 
                (line_movement < 0 and stake_pct > 60)):
                return 'REVERSE_LINE_MOVEMENT'
        
        return 'NO_CLEAR_SIGNAL'
    
    def _get_hybrid_strategies(self, profitable_strategies: List[ProfitableStrategy]) -> List[ProfitableStrategy]:
        """Extract hybrid-specific strategies from profitable strategies list."""
        hybrid_strategies = []
        
        for strategy in profitable_strategies:
            strategy_name = strategy.strategy_name.lower()
            if any(keyword in strategy_name for keyword in [
                'hybrid', 'line_movement', 'sharp', 'confirmation', 'steam'
            ]):
                hybrid_strategies.append(strategy)
        
        self.logger.info(f"Found {len(hybrid_strategies)} hybrid sharp strategies")
        return hybrid_strategies
    
    def _find_hybrid_strategy(self, strategy_classification: str, row: Dict[str, Any],
                            hybrid_strategies: List[ProfitableStrategy]) -> Optional[ProfitableStrategy]:
        """Find matching hybrid strategy based on classification."""
        
        # Look for exact classification match first
        for strategy in hybrid_strategies:
            if strategy_classification.lower() in strategy.strategy_name.lower():
                return strategy
        
        # Look for general hybrid strategies
        for strategy in hybrid_strategies:
            if 'hybrid' in strategy.strategy_name.lower():
                # Check if signal strength meets strategy requirements
                abs_diff = abs(float(row.get('differential', 0)))
                if strategy.win_rate >= 60 and abs_diff >= 15:
                    return strategy
                elif strategy.win_rate >= 55 and abs_diff >= 10:
                    return strategy
                elif strategy.win_rate >= 50 and abs_diff >= 5:
                    return strategy
        
        return None
    
    def _calculate_hybrid_confidence(self, row: Dict[str, Any], 
                                   hybrid_metrics: Dict[str, Any],
                                   strategy_classification: str,
                                   matching_strategy: ProfitableStrategy) -> Dict[str, Any]:
        """Calculate confidence with hybrid-specific adjustments."""
        
        base_confidence = self._calculate_confidence(
            row['differential'], row['source'], row['book'],
            row['split_type'], matching_strategy.strategy_name,
            row['last_updated'], self._normalize_game_time(row['game_datetime'])
        )
        
        # Apply hybrid-specific confidence modifiers
        confidence_modifier = self._get_hybrid_confidence_modifier(
            strategy_classification, hybrid_metrics
        )
        
        adjusted_confidence = base_confidence['confidence_score'] * confidence_modifier
        adjusted_confidence = max(0.1, min(0.95, adjusted_confidence))
        
        return {
            **base_confidence,
            'confidence_score': adjusted_confidence,
            'strategy_classification': strategy_classification,
            'hybrid_modifier': confidence_modifier,
            'sharp_strength': hybrid_metrics['sharp_strength'],
            'line_significance': hybrid_metrics['line_significance']
        }
    
    def _get_hybrid_confidence_modifier(self, strategy_classification: str, 
                                      metrics: Dict[str, Any]) -> float:
        """Calculate confidence modifier based on hybrid strategy type."""
        
        base_modifier = 1.0
        
        # Strategy-specific modifiers
        if strategy_classification == 'STRONG_CONFIRMATION':
            base_modifier = 1.3  # High confidence when line and sharp align strongly
        elif strategy_classification == 'MODERATE_CONFIRMATION':
            base_modifier = 1.15  # Good confidence with moderate alignment
        elif strategy_classification == 'STEAM_PLAY':
            base_modifier = 1.2  # Steam plays are often reliable
        elif strategy_classification == 'REVERSE_LINE_MOVEMENT':
            base_modifier = 1.1  # Reverse line movement can be powerful
        elif strategy_classification in ['STRONG_CONFLICT', 'MODERATE_CONFLICT']:
            base_modifier = 0.8  # Lower confidence when signals conflict
        elif strategy_classification == 'PUBLIC_MOVE':
            base_modifier = 0.7  # Public moves are often less reliable
        
        # Additional modifiers based on metrics strength
        if metrics['sharp_strength'] == 'STRONG_SHARP':
            base_modifier *= 1.1
        elif metrics['sharp_strength'] == 'NO_SHARP':
            base_modifier *= 0.9
        
        if metrics['line_significance'] == 'SIGNIFICANT':
            base_modifier *= 1.05
        
        return base_modifier
    
    def _create_hybrid_signal(self, row: Dict[str, Any], 
                            matching_strategy: ProfitableStrategy,
                            confidence_data: Dict[str, Any],
                            hybrid_metrics: Dict[str, Any],
                            strategy_classification: str) -> BettingSignal:
        """Create a hybrid betting signal with enhanced metadata."""
        
        signal = self._create_betting_signal(row, matching_strategy, confidence_data)
        
        # Enhance with hybrid-specific metadata
        signal.metadata = signal.metadata or {}
        signal.metadata.update({
            'hybrid_strategy': True,
            'strategy_classification': strategy_classification,
            'sharp_strength': hybrid_metrics['sharp_strength'],
            'sharp_direction': hybrid_metrics['sharp_direction'],
            'line_significance': hybrid_metrics['line_significance'],
            'line_movement': hybrid_metrics.get('line_movement'),
            'differential': hybrid_metrics['differential'],
            'processor_type': 'hybrid_sharp'
        })
        
        # Update strategy name to reflect hybrid nature
        signal.strategy_name = f"hybrid_sharp_{strategy_classification.lower()}_{signal.strategy_name}"
        
        return signal
    
    def _is_valid_hybrid_data(self, row: Dict[str, Any], current_time: datetime, 
                            minutes_ahead: int) -> bool:
        """Validate hybrid signal data quality and completeness."""
        try:
            # Basic validation
            if not self._is_valid_signal_data(row, current_time, minutes_ahead):
                return False
            
            # Hybrid-specific validation
            required_fields = ['differential', 'stake_pct', 'bet_pct']
            if not all(row.get(field) is not None for field in required_fields):
                return False
            
            # Check for minimum signal strength
            abs_diff = abs(float(row['differential']))
            if abs_diff < 5.0:  # Minimum for any hybrid consideration
                return False
            
            return True
            
        except (ValueError, TypeError, KeyError) as e:
            self.logger.warning(f"Invalid hybrid data: {e}")
            return False
    
    def _log_hybrid_summary(self, signals: List[BettingSignal], 
                          hybrid_strategies: List[ProfitableStrategy], 
                          raw_data_count: int):
        """Log summary of hybrid processing."""
        
        classification_counts = {}
        for signal in signals:
            classification = signal.metadata.get('strategy_classification', 'unknown') if signal.metadata else 'unknown'
            classification_counts[classification] = classification_counts.get(classification, 0) + 1
        
        self.logger.info(
            f"Hybrid sharp processing complete: {len(signals)} signals from {raw_data_count} raw records",
            extra={
                'total_signals': len(signals),
                'raw_data_count': raw_data_count,
                'hybrid_strategies': len(hybrid_strategies),
                'classifications': classification_counts
            }
        ) 