#!/usr/bin/env python3
"""
Smart Line Movement Filter

Filters line movement data to eliminate noise and keep only significant movements.
Based on the filtering logic from the previous conversation.
"""

from datetime import datetime
from typing import Dict, List, Any
import structlog

logger = structlog.get_logger(__name__)


class SmartLineMovementFilter:
    """
    Smart filter for Action Network line movement data.
    
    Reduces noise by filtering out insignificant movements and keeping
    only movements that represent real market activity.
    """
    
    def __init__(self):
        self.filtered_count = 0
        self.total_count = 0
    
    def filter_movements(self, movements: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Filter line movements to keep only significant ones.
        
        Args:
            movements: List of movement dictionaries
            
        Returns:
            Filtered list of significant movements
        """
        if not movements:
            return []
        
        self.total_count += len(movements)
        
        # Sort movements by timestamp
        sorted_movements = sorted(movements, key=lambda x: x.get('updated_at', ''))
        
        # Keep first and last movements
        if len(sorted_movements) <= 2:
            return sorted_movements
        
        filtered = [sorted_movements[0]]  # Always keep first
        
        # Filter middle movements
        for i in range(1, len(sorted_movements) - 1):
            current = sorted_movements[i]
            previous = filtered[-1]
            
            # Keep if significant change
            if self._is_significant_movement(current, previous):
                filtered.append(current)
        
        # Always keep last movement
        filtered.append(sorted_movements[-1])
        
        self.filtered_count += len(filtered)
        
        logger.debug("Filtered movements", 
                    original=len(movements), 
                    filtered=len(filtered),
                    reduction_pct=((len(movements) - len(filtered)) / len(movements) * 100))
        
        return filtered
    
    def _is_significant_movement(self, current: Dict[str, Any], previous: Dict[str, Any]) -> bool:
        """
        Determine if a movement is significant enough to keep.
        Uses improved American odds calculation and line value change detection.
        
        Args:
            current: Current movement data
            previous: Previous movement data
            
        Returns:
            True if movement is significant
        """
        # Check for odds changes using corrected American odds calculation
        current_odds = current.get('odds')
        previous_odds = previous.get('odds')
        
        if current_odds is not None and previous_odds is not None:
            # Calculate corrected American odds movement
            corrected_odds_change = self._calculate_american_odds_change(previous_odds, current_odds)
            
            # Keep if corrected odds changed by more than 5 points
            if abs(corrected_odds_change) >= 5:
                return True
        
        # Check for line value changes (spread/totals)
        current_value = current.get('value')
        previous_value = previous.get('value')
        
        if current_value is not None and previous_value is not None:
            # Keep if line changed by more than 0.5
            if abs(current_value - previous_value) >= 0.5:
                return True
        
        # Check for time-based significance (keep movements more than 1 hour apart)
        try:
            current_time = datetime.fromisoformat(current.get('updated_at', '').replace('Z', '+00:00'))
            previous_time = datetime.fromisoformat(previous.get('updated_at', '').replace('Z', '+00:00'))
            
            time_diff = (current_time - previous_time).total_seconds()
            if time_diff > 3600:  # 1 hour
                return True
        except:
            pass
        
        return False
    
    def _calculate_american_odds_change(self, previous_odds: int, current_odds: int) -> int:
        """
        Calculate the correct American odds movement.
        
        American odds work as follows:
        - Positive odds (+150): You win $150 on a $100 bet
        - Negative odds (-150): You bet $150 to win $100
        - Movement from -101 to +101 is crossing zero (2 point movement)
        - Movement from -150 to -140 is 10 point movement toward even
        
        Args:
            previous_odds: Previous odds value
            current_odds: Current odds value
            
        Returns:
            Corrected odds change
        """
        # Both odds same sign: simple difference
        if (previous_odds > 0 and current_odds > 0) or (previous_odds < 0 and current_odds < 0):
            return current_odds - previous_odds
        
        # Crossing zero: special handling for American odds
        elif previous_odds < 0 and current_odds > 0:
            # From negative to positive: -101 to +101 = 2 points
            return (current_odds - 100) + (100 - abs(previous_odds))
        
        elif previous_odds > 0 and current_odds < 0:
            # From positive to negative: +101 to -101 = -2 points  
            return -((abs(current_odds) - 100) + (100 - previous_odds))
        
        else:
            # Fallback to simple difference
            return current_odds - previous_odds
    
    def get_stats(self) -> Dict[str, Any]:
        """Get filtering statistics."""
        return {
            'total_movements': self.total_count,
            'filtered_movements': self.filtered_count,
            'reduction_rate': ((self.total_count - self.filtered_count) / max(self.total_count, 1) * 100),
            'efficiency': f"{self.filtered_count}/{self.total_count}"
        }