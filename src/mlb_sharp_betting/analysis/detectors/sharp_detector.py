"""
Unified Sharp Action Detector

Provides a unified interface for sharp action detection that coordinates
between analytical processing (ML-based historical analysis) and 
real-time processing (live signal validation).

This replaces the stub implementations in analyzers/sharp_detector.py
and services/sharp_monitor.py with a proper unified interface.
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

from ...models.betting_analysis import BettingSignal, SignalType, ProfitableStrategy
from ...models.base import BaseModel
from ..processors.analytical_processor import AnalyticalProcessor
from ..processors.real_time_processor import RealTimeProcessor


class SharpDetector:
    """
    Unified Sharp Action Detection Interface
    
    Coordinates between analytical and real-time processing to provide
    comprehensive sharp action detection capabilities.
    
    Key Features:
    - Historical pattern analysis via AnalyticalProcessor
    - Real-time signal processing via RealTimeProcessor  
    - Unified API for both analytical and live detection
    - Strategy integration and confidence scoring
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None) -> None:
        """Initialize sharp detector with configuration."""
        self.config = config or {}
        
        # Initialize processors
        self.analytical_processor = AnalyticalProcessor()
        self.real_time_processor = None  # Will be initialized when needed
        
        # Detection settings
        self.min_confidence = self.config.get('min_confidence', 0.6)
        self.enable_analytical = self.config.get('enable_analytical', True)
        self.enable_real_time = self.config.get('enable_real_time', True)
    
    def analyze_historical_patterns(self) -> Dict[str, Any]:
        """
        Run comprehensive historical analysis of sharp action patterns.
        
        Returns:
            Dict containing analysis results, insights, and strategy recommendations
        """
        if not self.enable_analytical:
            return {"error": "Analytical processing disabled"}
        
        return self.analytical_processor.run_full_analysis()
    
    async def detect_real_time_signals(self, minutes_ahead: int = 360,
                                     profitable_strategies: Optional[List[ProfitableStrategy]] = None) -> List[BettingSignal]:
        """
        Detect real-time sharp action signals for upcoming games.
        
        Args:
            minutes_ahead: Look ahead window in minutes (default 6 hours)
            profitable_strategies: List of profitable strategies to apply
            
        Returns:
            List of detected betting signals
        """
        if not self.enable_real_time:
            return []
        
        # Initialize real-time processor if needed
        if self.real_time_processor is None:
            self.real_time_processor = RealTimeProcessor()
        
        # Process signals
        strategies = profitable_strategies or []
        signals = await self.real_time_processor.process(minutes_ahead, strategies)
        
        # Filter by confidence
        filtered_signals = [
            signal for signal in signals 
            if signal.confidence >= self.min_confidence
        ]
        
        return filtered_signals
    
    def analyze_patterns_for_game(self, game_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze sharp action patterns for a specific game.
        
        Args:
            game_data: Game information and betting data
            
        Returns:
            Dict containing sharp action analysis for the game
        """
        # This method provides game-specific analysis
        # Implementation would depend on specific requirements
        
        analysis = {
            "game_id": game_data.get("game_id"),
            "sharp_indicators": [],
            "confidence": 0.0,
            "recommendations": []
        }
        
        # TODO: Implement game-specific sharp analysis logic
        # This would involve analyzing betting splits, line movements, etc.
        
        return analysis
    
    def get_sharp_strength_assessment(self, sharp_differential: float, 
                                    line_movement: float = 0.0,
                                    reverse_movement: bool = False) -> Dict[str, Any]:
        """
        Assess the strength of sharp action based on key indicators.
        
        Args:
            sharp_differential: Difference between money % and bet %
            line_movement: Magnitude of line movement  
            reverse_movement: Whether line moved against public betting
            
        Returns:
            Dict containing strength assessment
        """
        # Calculate base strength from differential
        abs_diff = abs(sharp_differential)
        
        if abs_diff >= 25:
            strength = "Extreme"
            score = 90 + min(abs_diff - 25, 10)
        elif abs_diff >= 20:
            strength = "Very Strong" 
            score = 75 + (abs_diff - 20)
        elif abs_diff >= 15:
            strength = "Strong"
            score = 60 + (abs_diff - 15)
        elif abs_diff >= 10:
            strength = "Moderate"
            score = 40 + (abs_diff - 10) * 2
        elif abs_diff >= 5:
            strength = "Weak"
            score = 20 + (abs_diff - 5) * 4
        else:
            strength = "Minimal"
            score = abs_diff * 4
        
        # Adjust for line movement
        if line_movement > 0.5:
            score += min(line_movement * 5, 15)
        
        # Bonus for reverse line movement (strong indicator)
        if reverse_movement:
            score += 20
            
        score = min(score, 100)
        
        return {
            "strength": strength,
            "score": score,
            "differential": sharp_differential,
            "line_movement": line_movement,
            "reverse_movement": reverse_movement,
            "recommended_action": "Bet" if score >= 70 else "Monitor" if score >= 50 else "Pass"
        }
    
    def validate_input(self, data: List[BaseModel]) -> bool:
        """Validate input data for sharp detection."""
        if not data:
            return False
        
        # Check that we have necessary data fields
        required_fields = ['home_team', 'away_team', 'game_datetime']
        
        for item in data:
            if hasattr(item, '__dict__'):
                item_dict = item.__dict__
            else:
                item_dict = item
                
            for field in required_fields:
                if field not in item_dict:
                    return False
        
        return True
    
    def get_detection_summary(self) -> Dict[str, Any]:
        """Get summary of detection capabilities and configuration."""
        return {
            "analytical_enabled": self.enable_analytical,
            "real_time_enabled": self.enable_real_time,
            "min_confidence": self.min_confidence,
            "processors": {
                "analytical": "AnalyticalProcessor" if self.enable_analytical else None,
                "real_time": "RealTimeProcessor" if self.enable_real_time else None
            },
            "capabilities": [
                "Historical pattern analysis",
                "Real-time signal detection", 
                "Game-specific analysis",
                "Sharp strength assessment",
                "Strategy integration"
            ]
        }


__all__ = ["SharpDetector"] 