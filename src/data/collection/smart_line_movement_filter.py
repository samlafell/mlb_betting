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
        
        Args:
            current: Current movement data
            previous: Previous movement data
            
        Returns:
            True if movement is significant
        """
        # Check for odds changes
        current_odds = current.get('odds')
        previous_odds = previous.get('odds')
        
        if current_odds is not None and previous_odds is not None:
            # Keep if odds changed by more than 5 points
            if abs(current_odds - previous_odds) > 5:
                return True
        
        # Check for line value changes (spread/totals)
        current_value = current.get('value')
        previous_value = previous.get('value')
        
        if current_value is not None and previous_value is not None:
            # Keep if line changed by more than 0.5
            if abs(current_value - previous_value) > 0.5:
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
    
    def get_stats(self) -> Dict[str, Any]:
        """Get filtering statistics."""
        return {
            'total_movements': self.total_count,
            'filtered_movements': self.filtered_count,
            'reduction_rate': ((self.total_count - self.filtered_count) / max(self.total_count, 1) * 100),
            'efficiency': f"{self.filtered_count}/{self.total_count}"
        }