"""
Sharp Action Strategy Implementation

Contains business rules, thresholds, and strategic logic for sharp action detection.
This module centralizes the strategic decision-making for sharp betting opportunities.

Part of Phase 2 refactoring to consolidate sharp action analysis logic.
"""

from typing import Dict, List, Optional, Tuple
from datetime import datetime
from enum import Enum
import logging

from ...models.betting_analysis import BettingSignal, SignalType


class SharpActionThreshold(Enum):
    """Thresholds for different levels of sharp action strength"""
    MINIMAL = 5.0      # 5% differential
    WEAK = 10.0        # 10% differential  
    MODERATE = 15.0    # 15% differential
    STRONG = 20.0      # 20% differential
    VERY_STRONG = 25.0 # 25% differential
    EXTREME = 30.0     # 30%+ differential


class BetType(Enum):
    """Types of bets for sharp action analysis"""
    MONEYLINE = "moneyline"
    SPREAD = "spread" 
    TOTAL = "total"


class SharpActionStrategy:
    """
    Sharp Action Strategy Implementation
    
    Centralizes business rules and strategic logic for identifying and acting
    on sharp betting opportunities. Handles thresholds, filtering criteria,
    and strategic decision-making.
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """Initialize strategy with configuration"""
        self.config = config or {}
        self.logger = logging.getLogger(__name__)
        
        # Strategy thresholds
        self.min_differential = self.config.get('min_differential', SharpActionThreshold.WEAK.value)
        self.min_confidence = self.config.get('min_confidence', 0.6)
        self.max_juice = self.config.get('max_juice', -160)  # From memory: user refuses to bet more negative than -160
        
        # Timing preferences
        self.min_time_to_game = self.config.get('min_time_to_game_minutes', 30)  # 30 minutes minimum
        self.max_time_to_game = self.config.get('max_time_to_game_hours', 24)    # 24 hours maximum
        
        # Volume thresholds
        self.min_sample_size = self.config.get('min_sample_size', 100)  # Minimum number of bets
        
        # Sharp strength preferences
        self.preferred_strength = self.config.get('preferred_strength', ['STRONG', 'VERY_STRONG', 'EXTREME'])
    
    def evaluate_sharp_opportunity(self, signal_data: Dict) -> Dict[str, any]:
        """
        Evaluate a sharp action opportunity and provide strategic assessment.
        
        Args:
            signal_data: Dictionary containing betting signal information
            
        Returns:
            Dict containing evaluation results and recommendations
        """
        evaluation = {
            "signal_id": signal_data.get("signal_id"),
            "game_id": signal_data.get("game_id"),
            "is_opportunity": False,
            "strength": "NONE",
            "confidence": 0.0,
            "recommended_action": "PASS",
            "reasoning": [],
            "risk_factors": [],
            "expected_value": 0.0
        }
        
        try:
            # Extract key metrics
            differential = abs(signal_data.get("sharp_differential", 0))
            bet_type = signal_data.get("split_type", "").lower()
            line_movement = abs(signal_data.get("line_movement", 0))
            reverse_movement = signal_data.get("reverse_movement", False)
            juice = signal_data.get("juice", 0)
            
            # Evaluate differential strength
            strength = self._assess_differential_strength(differential)
            evaluation["strength"] = strength.name
            
            # Check minimum thresholds
            if differential < self.min_differential:
                evaluation["reasoning"].append(f"Differential {differential:.1f}% below minimum {self.min_differential}%")
                return evaluation
            
            # Check juice filter (from memory: user refuses heavily juiced lines)
            if bet_type == "moneyline" and juice < self.max_juice:
                evaluation["risk_factors"].append(f"Heavy juice {juice} (limit: {self.max_juice})")
                evaluation["recommended_action"] = "AVOID"
                return evaluation
            
            # Check timing window
            time_check = self._check_timing_window(signal_data.get("game_time"))
            if not time_check["valid"]:
                evaluation["reasoning"].append(time_check["reason"])
                return evaluation
            
            # Calculate base confidence
            confidence = self._calculate_confidence(differential, line_movement, reverse_movement, bet_type)
            evaluation["confidence"] = confidence
            
            # Check if opportunity meets our standards
            if confidence >= self.min_confidence and strength.name in self.preferred_strength:
                evaluation["is_opportunity"] = True
                evaluation["recommended_action"] = "BET"
                evaluation["reasoning"].append(f"Strong signal: {differential:.1f}% differential")
                
                if reverse_movement:
                    evaluation["reasoning"].append("Reverse line movement detected")
                    confidence += 0.1  # Bonus for reverse movement
                
                if line_movement > 0.5:
                    evaluation["reasoning"].append(f"Significant line movement: {line_movement:.1f}")
                
            elif confidence >= (self.min_confidence - 0.1):
                evaluation["recommended_action"] = "MONITOR"
                evaluation["reasoning"].append("Near threshold - monitor for changes")
            
            # Calculate expected value (simplified)
            evaluation["expected_value"] = self._estimate_expected_value(
                confidence, differential, bet_type, juice
            )
            
        except Exception as e:
            self.logger.error(f"Error evaluating sharp opportunity: {e}")
            evaluation["reasoning"].append(f"Evaluation error: {str(e)}")
        
        return evaluation
    
    def filter_signals_by_strategy(self, signals: List[BettingSignal]) -> List[BettingSignal]:
        """
        Filter betting signals based on strategic criteria.
        
        Args:
            signals: List of betting signals to filter
            
        Returns:
            Filtered list of signals meeting strategy criteria
        """
        filtered_signals = []
        
        for signal in signals:
            # Convert signal to dict for evaluation
            signal_data = {
                "signal_id": getattr(signal, 'signal_id', None),
                "game_id": getattr(signal, 'game_id', None),
                "sharp_differential": abs(getattr(signal, 'confidence', 0) * 100),  # Approximate from confidence
                "split_type": getattr(signal, 'signal_type', SignalType.SHARP_ACTION).value,
                "game_time": getattr(signal, 'game_datetime', None),
                "juice": getattr(signal, 'odds', -110)  # Default juice
            }
            
            evaluation = self.evaluate_sharp_opportunity(signal_data)
            
            if evaluation["is_opportunity"] or evaluation["recommended_action"] == "MONITOR":
                # Update signal with strategy evaluation
                if hasattr(signal, 'strategy_evaluation'):
                    signal.strategy_evaluation = evaluation
                    
                filtered_signals.append(signal)
        
        return filtered_signals
    
    def get_bet_sizing_recommendation(self, evaluation: Dict, bankroll: float) -> Dict[str, float]:
        """
        Provide bet sizing recommendations based on evaluation.
        
        Args:
            evaluation: Strategy evaluation results
            bankroll: Current bankroll size
            
        Returns:
            Dict with bet sizing recommendations
        """
        if not evaluation.get("is_opportunity", False):
            return {"recommended_bet": 0.0, "max_bet": 0.0, "risk_percentage": 0.0}
        
        confidence = evaluation.get("confidence", 0.0)
        strength = evaluation.get("strength", "WEAK")
        expected_value = evaluation.get("expected_value", 0.0)
        
        # Base risk percentage (from memory: risk management rules)
        base_risk = 0.02  # 2% maximum per bet
        
        # Adjust based on confidence and strength
        if strength in ["EXTREME", "VERY_STRONG"]:
            risk_multiplier = min(confidence * 1.5, 1.0)
        elif strength == "STRONG":
            risk_multiplier = min(confidence * 1.2, 0.8)
        else:
            risk_multiplier = min(confidence, 0.6)
        
        # Apply expected value consideration
        if expected_value > 0.1:  # 10%+ EV
            risk_multiplier *= 1.2
        elif expected_value < 0.05:  # Less than 5% EV
            risk_multiplier *= 0.8
        
        risk_percentage = base_risk * risk_multiplier
        recommended_bet = bankroll * risk_percentage
        max_bet = bankroll * base_risk  # Never exceed base risk
        
        return {
            "recommended_bet": round(recommended_bet, 2),
            "max_bet": round(max_bet, 2),
            "risk_percentage": round(risk_percentage * 100, 2)
        }
    
    def _assess_differential_strength(self, differential: float) -> SharpActionThreshold:
        """Assess the strength category of a sharp differential"""
        if differential >= SharpActionThreshold.EXTREME.value:
            return SharpActionThreshold.EXTREME
        elif differential >= SharpActionThreshold.VERY_STRONG.value:
            return SharpActionThreshold.VERY_STRONG
        elif differential >= SharpActionThreshold.STRONG.value:
            return SharpActionThreshold.STRONG
        elif differential >= SharpActionThreshold.MODERATE.value:
            return SharpActionThreshold.MODERATE
        elif differential >= SharpActionThreshold.WEAK.value:
            return SharpActionThreshold.WEAK
        else:
            return SharpActionThreshold.MINIMAL
    
    def _check_timing_window(self, game_time: Optional[datetime]) -> Dict[str, any]:
        """Check if game timing is within acceptable window"""
        if not game_time:
            return {"valid": False, "reason": "No game time provided"}
        
        now = datetime.now()
        time_diff = (game_time - now).total_seconds() / 60  # Minutes to game
        
        if time_diff < self.min_time_to_game:
            return {"valid": False, "reason": f"Too close to game time ({time_diff:.0f} min)"}
        
        max_minutes = self.max_time_to_game * 60
        if time_diff > max_minutes:
            return {"valid": False, "reason": f"Too far from game time ({time_diff/60:.1f} hours)"}
        
        return {"valid": True, "reason": f"Good timing window ({time_diff/60:.1f} hours to game)"}
    
    def _calculate_confidence(self, differential: float, line_movement: float, 
                            reverse_movement: bool, bet_type: str) -> float:
        """Calculate confidence score for the opportunity"""
        # Base confidence from differential
        confidence = min(differential / 30.0, 1.0)  # Scale to 0-1
        
        # Line movement bonus
        if line_movement > 0.5:
            confidence += min(line_movement * 0.1, 0.2)
        
        # Reverse movement bonus
        if reverse_movement:
            confidence += 0.15
        
        # Bet type adjustments
        if bet_type == "moneyline":
            confidence *= 1.1  # ML slightly more reliable
        elif bet_type == "total":
            confidence *= 0.95  # Totals slightly less reliable
        
        return min(confidence, 1.0)
    
    def _estimate_expected_value(self, confidence: float, differential: float, 
                               bet_type: str, juice: int) -> float:
        """Estimate expected value of the opportunity"""
        # Simple EV calculation
        # This is a simplified model - in practice would be more sophisticated
        
        # Convert juice to probability
        if juice < 0:
            implied_prob = abs(juice) / (abs(juice) + 100)
        else:
            implied_prob = 100 / (juice + 100)
        
        # Adjust win probability based on sharp signal strength
        adjustment = (differential / 100) * 0.5  # Conservative adjustment
        adjusted_prob = implied_prob + adjustment
        
        # Calculate EV
        if juice < 0:
            payout_ratio = 100 / abs(juice)
        else:
            payout_ratio = juice / 100
        
        expected_value = (adjusted_prob * payout_ratio) - ((1 - adjusted_prob) * 1)
        
        return expected_value
    
    def get_strategy_summary(self) -> Dict[str, any]:
        """Get summary of current strategy configuration"""
        return {
            "thresholds": {
                "min_differential": self.min_differential,
                "min_confidence": self.min_confidence,
                "max_juice": self.max_juice
            },
            "timing": {
                "min_time_to_game_minutes": self.min_time_to_game,
                "max_time_to_game_hours": self.max_time_to_game
            },
            "preferences": {
                "preferred_strength": self.preferred_strength,
                "min_sample_size": self.min_sample_size
            },
            "risk_management": {
                "max_bet_percentage": 2.0,
                "juice_limit": self.max_juice
            }
        }


__all__ = ["SharpActionStrategy", "SharpActionThreshold", "BetType"] 