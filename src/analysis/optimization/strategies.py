"""
Strategy Parameter Registry

Defines parameter spaces for all supported betting strategies.
Contains optimized parameter definitions based on strategy analysis.
"""

from typing import Dict, List
import numpy as np

from .parameter_space import ParameterSpace, ParameterConfig, ParameterType
from src.core.logging import LogComponent, get_logger


class StrategyParameterRegistry:
    """
    Registry of parameter spaces for all supported betting strategies.
    
    Provides pre-configured parameter spaces for strategy optimization
    based on analysis of existing processors and their parameter impact.
    """
    
    def __init__(self):
        self.logger = get_logger(__name__, LogComponent.OPTIMIZATION)
        self._parameter_spaces: Dict[str, ParameterSpace] = {}
        self._initialize_parameter_spaces()
    
    def _initialize_parameter_spaces(self) -> None:
        """Initialize parameter spaces for all supported strategies"""
        
        # Sharp Action Processor Parameters
        sharp_action_params = [
            ParameterConfig(
                name="min_differential_threshold",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(5.0, 30.0),
                default_value=10.0,
                description="Minimum money/bet percentage differential to consider sharp action"
            ),
            ParameterConfig(
                name="high_confidence_threshold", 
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(15.0, 35.0),
                default_value=20.0,
                description="High confidence differential threshold for premium signals"
            ),
            ParameterConfig(
                name="volume_weight_factor",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(1.0, 3.0),
                default_value=1.5,
                description="Weight factor for volume in confidence calculation"
            ),
            ParameterConfig(
                name="min_volume_threshold",
                parameter_type=ParameterType.DISCRETE,
                choices=[50, 75, 100, 150, 200, 250, 300],
                default_value=100,
                description="Minimum volume required for reliable sharp action detection"
            ),
            # Book weight multipliers for premium sharp books
            ParameterConfig(
                name="pinnacle_weight",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(1.5, 3.0),
                default_value=2.0,
                description="Weight multiplier for Pinnacle (sharp book)"
            ),
            ParameterConfig(
                name="circa_weight",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(1.5, 2.5),
                default_value=1.8,
                description="Weight multiplier for Circa (sharp book)"
            ),
            ParameterConfig(
                name="draftkings_weight",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(1.0, 1.5),
                default_value=1.2,
                description="Weight multiplier for DraftKings"
            ),
            # Timing multipliers for different game phases
            ParameterConfig(
                name="ultra_late_multiplier",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(1.2, 2.0),
                default_value=1.5,
                description="Timing multiplier for ultra late (≤30 min) sharp action"
            ),
            ParameterConfig(
                name="closing_hour_multiplier",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(1.1, 1.6),
                default_value=1.3,
                description="Timing multiplier for closing hour (≤60 min) sharp action"
            )
        ]
        
        self._parameter_spaces["sharp_action"] = ParameterSpace("SharpActionProcessor", sharp_action_params)
        
        # Line Movement Processor Parameters
        line_movement_params = [
            ParameterConfig(
                name="min_movement_threshold",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(0.25, 1.5),
                default_value=0.5,
                description="Minimum line movement to consider significant"
            ),
            ParameterConfig(
                name="steam_move_threshold",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(0.75, 2.0),
                default_value=1.0,
                description="Threshold for steam move detection"
            ),
            ParameterConfig(
                name="late_movement_hours",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(1.0, 6.0),
                default_value=3.0,
                description="Hours before game to consider late movement"
            ),
            ParameterConfig(
                name="min_book_consensus",
                parameter_type=ParameterType.DISCRETE,
                choices=[2, 3, 4, 5],
                default_value=3,
                description="Minimum books required for movement consensus"
            ),
            # Movement type modifiers
            ParameterConfig(
                name="reverse_line_movement_modifier",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(1.1, 1.8),
                default_value=1.4,
                description="Confidence modifier for reverse line movement"
            ),
            ParameterConfig(
                name="steam_move_modifier",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(1.1, 1.6),
                default_value=1.3,
                description="Confidence modifier for steam moves"
            ),
            ParameterConfig(
                name="late_movement_modifier",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(1.1, 1.5),
                default_value=1.2,
                description="Confidence modifier for late movements"
            ),
            ParameterConfig(
                name="multi_book_consensus_modifier",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(1.1, 1.5),
                default_value=1.2,
                description="Confidence modifier for multi-book consensus"
            ),
            # Movement thresholds by bet type
            ParameterConfig(
                name="moneyline_movement_threshold",
                parameter_type=ParameterType.DISCRETE,
                choices=[3, 4, 5, 6, 7, 8, 10],
                default_value=5,
                description="Minimum cents movement for moneyline significance"
            ),
            ParameterConfig(
                name="spread_movement_threshold",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(0.25, 1.0),
                default_value=0.5,
                description="Minimum point movement for spread significance"
            )
        ]
        
        self._parameter_spaces["line_movement"] = ParameterSpace("LineMovementProcessor", line_movement_params)
        
        # Consensus Processor Parameters
        consensus_params = [
            ParameterConfig(
                name="heavy_consensus_threshold",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(80.0, 95.0),
                default_value=90.0,
                description="Threshold for heavy consensus pattern detection"
            ),
            ParameterConfig(
                name="mixed_consensus_money_threshold",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(70.0, 85.0),
                default_value=80.0,
                description="Money percentage threshold for mixed consensus"
            ),
            ParameterConfig(
                name="mixed_consensus_bet_threshold",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(55.0, 70.0),
                default_value=60.0,
                description="Bet percentage threshold for mixed consensus"
            ),
            ParameterConfig(
                name="min_consensus_strength",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(60.0, 80.0),
                default_value=70.0,
                description="Minimum consensus strength to generate signal"
            ),
            ParameterConfig(
                name="max_alignment_difference",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(20.0, 40.0),
                default_value=30.0,
                description="Maximum difference between money and bet percentages"
            ),
            # Consensus modifiers
            ParameterConfig(
                name="heavy_consensus_modifier",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(1.1, 1.6),
                default_value=1.3,
                description="Confidence modifier for heavy consensus patterns"
            ),
            ParameterConfig(
                name="mixed_consensus_modifier",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(1.0, 1.3),
                default_value=1.1,
                description="Confidence modifier for mixed consensus patterns"
            ),
            ParameterConfig(
                name="perfect_alignment_modifier",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(1.1, 1.4),
                default_value=1.2,
                description="Bonus modifier for perfect alignment (≤5% difference)"
            )
        ]
        
        self._parameter_spaces["consensus"] = ParameterSpace("ConsensusProcessor", consensus_params)
        
        # Late Flip Processor Parameters (if it exists)
        late_flip_params = [
            ParameterConfig(
                name="flip_detection_threshold",
                parameter_type=ParameterType.CONTINUOUS,
                bounds=(15.0, 40.0),
                default_value=25.0,
                description="Percentage change threshold for flip detection"
            ),
            ParameterConfig(
                name="late_window_minutes",
                parameter_type=ParameterType.DISCRETE,
                choices=[30, 45, 60, 90, 120, 180],
                default_value=60,
                description="Time window in minutes to consider 'late' flip"
            ),
            ParameterConfig(
                name="min_flip_volume",
                parameter_type=ParameterType.DISCRETE,
                choices=[200, 300, 500, 750, 1000],
                default_value=500,
                description="Minimum volume required for reliable flip detection"
            )
        ]
        
        self._parameter_spaces["late_flip"] = ParameterSpace("LateFlipProcessor", late_flip_params)
        
        self.logger.info(f"Initialized {len(self._parameter_spaces)} strategy parameter spaces")
    
    def get_parameter_space(self, strategy_name: str) -> ParameterSpace:
        """
        Get parameter space for a strategy.
        
        Args:
            strategy_name: Name of the strategy (e.g., 'sharp_action', 'line_movement')
            
        Returns:
            ParameterSpace for the strategy
            
        Raises:
            KeyError: If strategy not found in registry
        """
        strategy_key = strategy_name.lower().replace("processor", "").replace("_processor", "")
        
        if strategy_key not in self._parameter_spaces:
            available = list(self._parameter_spaces.keys())
            raise KeyError(f"Strategy '{strategy_name}' not found. Available strategies: {available}")
        
        return self._parameter_spaces[strategy_key]
    
    def list_strategies(self) -> List[str]:
        """Get list of all available strategies"""
        return list(self._parameter_spaces.keys())
    
    def get_all_parameter_spaces(self) -> Dict[str, ParameterSpace]:
        """Get all parameter spaces"""
        return self._parameter_spaces.copy()
    
    def create_combined_parameter_space(self, strategy_names: List[str]) -> ParameterSpace:
        """
        Create a combined parameter space for multiple strategies.
        
        Args:
            strategy_names: List of strategy names to combine
            
        Returns:
            Combined ParameterSpace with prefixed parameter names
        """
        combined_params = []
        
        for strategy_name in strategy_names:
            strategy_space = self.get_parameter_space(strategy_name)
            
            # Add strategy prefix to parameter names
            for param_name in strategy_space.get_parameter_names():
                param_config = strategy_space.get_parameter_config(param_name)
                prefixed_config = ParameterConfig(
                    name=f"{strategy_name}_{param_config.name}",
                    parameter_type=param_config.parameter_type,
                    bounds=param_config.bounds,
                    choices=param_config.choices,
                    default_value=param_config.default_value,
                    description=f"[{strategy_name}] {param_config.description}"
                )
                combined_params.append(prefixed_config)
        
        combined_name = "_".join(strategy_names)
        return ParameterSpace(f"Combined_{combined_name}", combined_params)
    
    def get_high_impact_parameters(self, strategy_name: str) -> List[str]:
        """
        Get list of high-impact parameters for a strategy.
        
        These are parameters that typically have the most effect on ROI
        and should be prioritized in optimization.
        
        Args:
            strategy_name: Name of the strategy
            
        Returns:
            List of high-impact parameter names
        """
        high_impact_params = {
            "sharp_action": [
                "min_differential_threshold",
                "high_confidence_threshold", 
                "ultra_late_multiplier",
                "pinnacle_weight"
            ],
            "line_movement": [
                "min_movement_threshold",
                "steam_move_threshold",
                "reverse_line_movement_modifier",
                "min_book_consensus"
            ],
            "consensus": [
                "heavy_consensus_threshold",
                "mixed_consensus_money_threshold",
                "heavy_consensus_modifier"
            ],
            "late_flip": [
                "flip_detection_threshold",
                "late_window_minutes"
            ]
        }
        
        strategy_key = strategy_name.lower().replace("processor", "").replace("_processor", "")
        return high_impact_params.get(strategy_key, [])
    
    def create_focused_parameter_space(self, strategy_name: str, focus_high_impact: bool = True) -> ParameterSpace:
        """
        Create a focused parameter space with only high-impact parameters.
        
        Useful for initial optimization when you want to focus on parameters
        with the highest ROI impact.
        
        Args:
            strategy_name: Name of the strategy
            focus_high_impact: If True, only include high-impact parameters
            
        Returns:
            Focused ParameterSpace
        """
        full_space = self.get_parameter_space(strategy_name)
        
        if not focus_high_impact:
            return full_space
        
        high_impact_names = self.get_high_impact_parameters(strategy_name)
        focused_params = []
        
        for param_name in high_impact_names:
            try:
                param_config = full_space.get_parameter_config(param_name)
                focused_params.append(param_config)
            except KeyError:
                self.logger.warning(f"High-impact parameter {param_name} not found in {strategy_name}")
        
        return ParameterSpace(f"Focused_{strategy_name}", focused_params)