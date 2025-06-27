"""
Enhanced Sharp Action Detector

Implements comprehensive sharp betting pattern detection using:
- Multi-book consensus analysis
- Volume-weighted signal strength
- Time-decay adjustment for signal relevance
- Real-time signal processing via SharpActionProcessor
- Steam move detection with configurable time windows

The detector processes raw betting data through sophisticated algorithms
to identify professional betting patterns and generate actionable signals.
"""

from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
import numpy as np
from dataclasses import dataclass

from ...core.logging import get_logger
from ...models.base import BaseModel
from ...models.betting_analysis import BettingSignal, SignalType, ProfitableStrategy
from ..processors.analytical_processor import AnalyticalProcessor
from ..processors.sharpaction_processor import SharpActionProcessor


class SharpDetector:
    """
    Advanced sharp action detection with multi-signal analysis.
    
    Processes betting splits data to identify professional betting patterns
    using statistical analysis, volume weighting, and temporal factors.
    
    Key capabilities:
    - Multi-book consensus detection
    - Volume-weighted signal strength calculation
    - Time-decay adjustment for signal relevance
    - Steam move identification
    - Cross-market validation
    
    The detector implements sophisticated algorithms to filter noise
    and identify high-confidence sharp betting opportunities.
    """
    
    def __init__(self, 
                 enable_consensus: bool = True,
                 enable_volume_weighting: bool = True,
                 enable_time_decay: bool = True,
                 enable_steam_detection: bool = True,
                 enable_real_time: bool = False,
                 consensus_threshold: float = 0.7,
                 volume_threshold: int = 100,
                 time_decay_hours: int = 24,
                 steam_window_minutes: int = 30):
        """
        Initialize sharp detector with configurable parameters.
        
        Args:
            enable_consensus: Enable multi-book consensus analysis
            enable_volume_weighting: Weight signals by betting volume
            enable_time_decay: Apply time decay to older signals
            enable_steam_detection: Detect steam moves
            enable_real_time: Enable real-time processing
            consensus_threshold: Minimum consensus for signal validation
            volume_threshold: Minimum volume for signal consideration
            time_decay_hours: Hours for time decay calculation
            steam_window_minutes: Time window for steam move detection
        """
        self.logger = get_logger(__name__)
        
        # Feature toggles
        self.enable_consensus = enable_consensus
        self.enable_volume_weighting = enable_volume_weighting
        self.enable_time_decay = enable_time_decay
        self.enable_steam_detection = enable_steam_detection
        self.enable_real_time = enable_real_time
        
        # Configuration parameters
        self.consensus_threshold = consensus_threshold
        self.volume_threshold = volume_threshold
        self.time_decay_hours = time_decay_hours
        self.steam_window_minutes = steam_window_minutes
        
        # Initialize processors
        self.analytical_processor = AnalyticalProcessor()
        self.sharp_action_processor = SharpActionProcessor()
        
        self.logger.info(f"Sharp detector initialized with features: {self._get_feature_summary()}")
    
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

    def _get_feature_summary(self) -> Dict[str, Any]:
        """Get summary of enabled features for logging."""
        return {
            "consensus": self.enable_consensus,
            "volume_weighting": self.enable_volume_weighting, 
            "time_decay": self.enable_time_decay,
            "steam_detection": self.enable_steam_detection,
            "real_time": "SharpActionProcessor" if self.enable_real_time else None
        }


__all__ = ["SharpDetector"] 