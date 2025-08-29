"""
Parameter Space Management for Hyperparameter Optimization

Defines parameter spaces for different strategy processors with proper constraints
and validation. Supports continuous, discrete, and categorical parameters.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union
import json
import numpy as np
from abc import ABC, abstractmethod

from src.core.logging import LogComponent, get_logger


class ParameterType(str, Enum):
    """Types of parameters for optimization"""
    CONTINUOUS = "continuous"
    DISCRETE = "discrete"
    CATEGORICAL = "categorical"
    BOOLEAN = "boolean"


@dataclass
class ParameterConfig:
    """Configuration for a single parameter to optimize"""
    name: str
    parameter_type: ParameterType
    bounds: Optional[Tuple[float, float]] = None  # For continuous parameters
    choices: Optional[List[Any]] = None  # For discrete/categorical parameters
    default_value: Any = None
    description: str = ""
    
    def __post_init__(self):
        """Validate parameter configuration"""
        if self.parameter_type == ParameterType.CONTINUOUS:
            if self.bounds is None:
                raise ValueError(f"Continuous parameter {self.name} requires bounds")
            if len(self.bounds) != 2 or self.bounds[0] >= self.bounds[1]:
                raise ValueError(f"Invalid bounds for parameter {self.name}: {self.bounds}")
                
        elif self.parameter_type in [ParameterType.DISCRETE, ParameterType.CATEGORICAL]:
            if self.choices is None or len(self.choices) == 0:
                raise ValueError(f"Discrete/categorical parameter {self.name} requires choices")
    
    def sample_value(self) -> Any:
        """Sample a random value within the parameter space"""
        if self.parameter_type == ParameterType.CONTINUOUS:
            return np.random.uniform(self.bounds[0], self.bounds[1])
        elif self.parameter_type == ParameterType.DISCRETE:
            return np.random.choice(self.choices)
        elif self.parameter_type == ParameterType.CATEGORICAL:
            return np.random.choice(self.choices)
        elif self.parameter_type == ParameterType.BOOLEAN:
            return np.random.choice([True, False])
        else:
            return self.default_value
    
    def validate_value(self, value: Any) -> bool:
        """Validate if a value is within the parameter space"""
        try:
            if self.parameter_type == ParameterType.CONTINUOUS:
                return self.bounds[0] <= float(value) <= self.bounds[1]
            elif self.parameter_type == ParameterType.DISCRETE:
                return value in self.choices
            elif self.parameter_type == ParameterType.CATEGORICAL:
                return value in self.choices
            elif self.parameter_type == ParameterType.BOOLEAN:
                return isinstance(value, bool)
            return True
        except (ValueError, TypeError):
            return False


class ParameterSpace:
    """
    Defines the parameter space for strategy optimization.
    
    Manages parameter configurations, validation, and sampling for
    hyperparameter optimization of betting strategies.
    """
    
    def __init__(self, strategy_name: str, parameters: List[ParameterConfig]):
        """
        Initialize parameter space for a strategy.
        
        Args:
            strategy_name: Name of the strategy processor
            parameters: List of parameter configurations
        """
        self.strategy_name = strategy_name
        self.parameters = {param.name: param for param in parameters}
        self.logger = get_logger(__name__, LogComponent.OPTIMIZATION)
        
        self.logger.info(f"Initialized parameter space for {strategy_name} with {len(parameters)} parameters")
    
    def get_parameter_names(self) -> List[str]:
        """Get list of all parameter names"""
        return list(self.parameters.keys())
    
    def get_parameter_config(self, name: str) -> ParameterConfig:
        """Get configuration for a specific parameter"""
        if name not in self.parameters:
            raise KeyError(f"Parameter {name} not found in space")
        return self.parameters[name]
    
    def sample_parameters(self, n_samples: int = 1) -> List[Dict[str, Any]]:
        """
        Sample random parameter combinations.
        
        Args:
            n_samples: Number of parameter combinations to sample
            
        Returns:
            List of parameter dictionaries
        """
        samples = []
        for _ in range(n_samples):
            sample = {}
            for param_name, param_config in self.parameters.items():
                sample[param_name] = param_config.sample_value()
            samples.append(sample)
        
        return samples
    
    def validate_parameters(self, parameters: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate a parameter configuration.
        
        Args:
            parameters: Dictionary of parameter values
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Check for missing parameters
        for param_name in self.parameters:
            if param_name not in parameters:
                errors.append(f"Missing parameter: {param_name}")
        
        # Check for invalid parameters
        for param_name, value in parameters.items():
            if param_name not in self.parameters:
                errors.append(f"Unknown parameter: {param_name}")
                continue
            
            param_config = self.parameters[param_name]
            if not param_config.validate_value(value):
                errors.append(f"Invalid value for {param_name}: {value}")
        
        return len(errors) == 0, errors
    
    def get_default_parameters(self) -> Dict[str, Any]:
        """Get default parameter values"""
        return {
            name: config.default_value 
            for name, config in self.parameters.items()
            if config.default_value is not None
        }
    
    def create_grid_space(self, grid_points: int = 5) -> List[Dict[str, Any]]:
        """
        Create a grid search space.
        
        Args:
            grid_points: Number of points per continuous parameter
            
        Returns:
            List of all parameter combinations in grid
        """
        parameter_grids = {}
        
        for param_name, param_config in self.parameters.items():
            if param_config.parameter_type == ParameterType.CONTINUOUS:
                parameter_grids[param_name] = np.linspace(
                    param_config.bounds[0], 
                    param_config.bounds[1], 
                    grid_points
                ).tolist()
            elif param_config.parameter_type in [ParameterType.DISCRETE, ParameterType.CATEGORICAL]:
                parameter_grids[param_name] = param_config.choices
            elif param_config.parameter_type == ParameterType.BOOLEAN:
                parameter_grids[param_name] = [True, False]
        
        # Generate all combinations
        import itertools
        param_names = list(parameter_grids.keys())
        param_values = [parameter_grids[name] for name in param_names]
        
        grid_combinations = []
        for combination in itertools.product(*param_values):
            grid_combination = dict(zip(param_names, combination))
            grid_combinations.append(grid_combination)
        
        self.logger.info(f"Created grid space with {len(grid_combinations)} combinations")
        return grid_combinations
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert parameter space to dictionary format"""
        return {
            "strategy_name": self.strategy_name,
            "parameters": {
                name: {
                    "name": config.name,
                    "parameter_type": config.parameter_type.value,
                    "bounds": config.bounds,
                    "choices": config.choices,
                    "default_value": config.default_value,
                    "description": config.description
                }
                for name, config in self.parameters.items()
            }
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ParameterSpace":
        """Create parameter space from dictionary format"""
        parameters = []
        for param_data in data["parameters"].values():
            param_config = ParameterConfig(
                name=param_data["name"],
                parameter_type=ParameterType(param_data["parameter_type"]),
                bounds=tuple(param_data["bounds"]) if param_data["bounds"] else None,
                choices=param_data["choices"],
                default_value=param_data["default_value"],
                description=param_data["description"]
            )
            parameters.append(param_config)
        
        return cls(data["strategy_name"], parameters)
    
    def save_to_file(self, filepath: str) -> None:
        """Save parameter space to JSON file"""
        with open(filepath, 'w') as f:
            json.dump(self.to_dict(), f, indent=2, default=str)
        self.logger.info(f"Saved parameter space to {filepath}")
    
    @classmethod
    def load_from_file(cls, filepath: str) -> "ParameterSpace":
        """Load parameter space from JSON file"""
        with open(filepath, 'r') as f:
            data = json.load(f)
        return cls.from_dict(data)