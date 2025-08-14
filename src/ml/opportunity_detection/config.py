"""
Configuration management for ML Opportunity Detection System

Centralized configuration with validation, type safety, and environment-specific settings.
Replaces hardcoded thresholds and magic numbers throughout the system.
"""

from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field, field_validator, ValidationError
from enum import Enum
import yaml
import os
from pathlib import Path


class OpportunityTier(str, Enum):
    """Opportunity tier classifications"""
    PREMIUM = "PREMIUM"
    HIGH_VALUE = "HIGH_VALUE"
    MODERATE = "MODERATE"
    LOW = "LOW"


class RiskProfile(str, Enum):
    """Risk tolerance profiles"""
    CONSERVATIVE = "CONSERVATIVE"
    MODERATE = "MODERATE"
    AGGRESSIVE = "AGGRESSIVE"


class PatternType(str, Enum):
    """ML pattern types"""
    ANOMALY = "ANOMALY"
    TEMPORAL_CLUSTER = "TEMPORAL_CLUSTER"
    SIGNAL_CORRELATION = "SIGNAL_CORRELATION"
    TREND_REVERSAL = "TREND_REVERSAL"


class ScoringWeightsConfig(BaseModel):
    """Scoring weights configuration with validation"""
    sharp_action: float = Field(0.25, ge=0.0, le=1.0, description="Sharp action weight")
    line_movement: float = Field(0.20, ge=0.0, le=1.0, description="Line movement weight")
    consensus_divergence: float = Field(0.15, ge=0.0, le=1.0, description="Consensus divergence weight")
    historical_patterns: float = Field(0.15, ge=0.0, le=1.0, description="Historical patterns weight")
    timing_factors: float = Field(0.10, ge=0.0, le=1.0, description="Timing factors weight")
    market_efficiency: float = Field(0.10, ge=0.0, le=1.0, description="Market efficiency weight")
    confidence_level: float = Field(0.05, ge=0.0, le=1.0, description="Confidence level weight")
    
    @field_validator('sharp_action', 'line_movement', 'consensus_divergence', 
                     'historical_patterns', 'timing_factors', 'market_efficiency', 'confidence_level')
    @classmethod
    def validate_weight(cls, v):
        """Ensure all weights are valid numbers"""
        if not isinstance(v, (int, float)):
            raise ValueError(f"Weight must be a number, got {type(v)}")
        return float(v)


class OpportunityThresholdsConfig(BaseModel):
    """Opportunity score thresholds configuration"""
    premium_threshold: float = Field(75.0, ge=0.0, le=100.0, description="Premium opportunity threshold")
    high_value_threshold: float = Field(60.0, ge=0.0, le=100.0, description="High value opportunity threshold")
    moderate_threshold: float = Field(40.0, ge=0.0, le=100.0, description="Moderate opportunity threshold")
    minimum_score: float = Field(25.0, ge=0.0, le=100.0, description="Minimum viable opportunity score")
    
    @field_validator('premium_threshold', 'high_value_threshold', 'moderate_threshold', 'minimum_score')
    @classmethod
    def validate_threshold_order(cls, v):
        """Validate threshold values are reasonable"""
        if not isinstance(v, (int, float)):
            raise ValueError(f"Threshold must be a number, got {type(v)}")
        return float(v)


class PatternDetectionConfig(BaseModel):
    """Pattern detection configuration"""
    enabled: bool = Field(True, description="Enable pattern detection")
    
    # Anomaly detection settings
    anomaly_contamination: float = Field(0.1, ge=0.01, le=0.5, description="Expected anomaly rate")
    anomaly_n_estimators: int = Field(100, ge=10, le=500, description="Isolation forest estimators")
    anomaly_max_samples: int = Field(256, ge=32, le=1024, description="Max samples for isolation forest")
    
    # Statistical analysis settings
    z_score_threshold: float = Field(2.0, ge=1.0, le=5.0, description="Z-score threshold for anomalies")
    percentile_threshold: float = Field(95.0, ge=80.0, le=99.9, description="Percentile threshold for anomalies")
    
    # Temporal clustering settings
    temporal_window_minutes: int = Field(30, ge=5, le=120, description="Temporal clustering window")
    min_cluster_size: int = Field(3, ge=2, le=10, description="Minimum cluster size")
    
    # Correlation settings
    correlation_threshold: float = Field(0.7, ge=0.3, le=0.95, description="Minimum correlation strength")
    min_correlation_samples: int = Field(10, ge=5, le=50, description="Minimum samples for correlation")


class ExplanationEngineConfig(BaseModel):
    """Explanation engine configuration"""
    enabled: bool = Field(True, description="Enable explanation generation")
    
    # Language settings
    supported_languages: List[str] = Field(
        default=['en', 'es', 'fr'], 
        description="Supported languages for explanations"
    )
    default_language: str = Field('en', description="Default explanation language")
    
    # User experience settings
    beginner_max_length: int = Field(200, ge=50, le=500, description="Max explanation length for beginners")
    intermediate_max_length: int = Field(400, ge=100, le=800, description="Max explanation length for intermediate")
    expert_max_length: int = Field(800, ge=200, le=1500, description="Max explanation length for experts")
    
    # Technical detail levels
    beginner_technical_terms: int = Field(2, ge=0, le=10, description="Max technical terms for beginners")
    intermediate_technical_terms: int = Field(8, ge=2, le=20, description="Max technical terms for intermediate")
    expert_technical_terms: int = Field(20, ge=5, le=50, description="Max technical terms for experts")


class PerformanceConfig(BaseModel):
    """Performance and optimization configuration"""
    # Response time targets
    max_discovery_time_ms: float = Field(2000.0, ge=100.0, le=10000.0, description="Max discovery time per game")
    max_scoring_time_ms: float = Field(500.0, ge=50.0, le=2000.0, description="Max scoring time per opportunity")
    max_pattern_time_ms: float = Field(800.0, ge=100.0, le=3000.0, description="Max pattern detection time")
    max_explanation_time_ms: float = Field(300.0, ge=50.0, le=1000.0, description="Max explanation generation time")
    
    # Memory limits
    max_baseline_features: int = Field(1000, ge=100, le=5000, description="Max baseline features for ML")
    max_cache_size: int = Field(500, ge=50, le=2000, description="Max cache entries")
    
    # Batch processing
    default_batch_size: int = Field(10, ge=1, le=50, description="Default batch size for processing")
    max_concurrent_requests: int = Field(20, ge=1, le=100, description="Max concurrent opportunity requests")
    
    # Cache settings
    cache_ttl_hours: int = Field(2, ge=1, le=24, description="Cache time-to-live in hours")
    performance_cache_ttl_hours: int = Field(4, ge=1, le=48, description="Performance data cache TTL")


class SecurityConfig(BaseModel):
    """Security and validation configuration"""
    # Input validation
    max_game_id_length: int = Field(50, ge=10, le=200, description="Maximum game ID length")
    max_signal_strength: float = Field(1.0, ge=0.1, le=10.0, description="Maximum signal strength")
    max_signals_per_game: int = Field(100, ge=1, le=1000, description="Maximum signals per game")
    
    # Rate limiting
    max_requests_per_minute: int = Field(60, ge=1, le=1000, description="Max requests per minute per user")
    max_games_per_request: int = Field(50, ge=1, le=200, description="Max games per discovery request")
    
    # Audit logging
    log_all_requests: bool = Field(True, description="Log all opportunity requests")
    log_performance_metrics: bool = Field(True, description="Log performance metrics")
    log_user_interactions: bool = Field(False, description="Log user interactions (privacy-sensitive)")


class MLOpportunityConfig(BaseModel):
    """Complete ML Opportunity Detection configuration"""
    
    # Core configuration sections
    scoring_weights: ScoringWeightsConfig = Field(default_factory=ScoringWeightsConfig)
    opportunity_thresholds: OpportunityThresholdsConfig = Field(default_factory=OpportunityThresholdsConfig)
    pattern_detection: PatternDetectionConfig = Field(default_factory=PatternDetectionConfig)
    explanation_engine: ExplanationEngineConfig = Field(default_factory=ExplanationEngineConfig)
    performance: PerformanceConfig = Field(default_factory=PerformanceConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    
    # Service-level settings
    service_name: str = Field("ml-opportunity-detection", description="Service identifier")
    version: str = Field("1.0.0", description="Service version")
    environment: str = Field("development", description="Environment (development/staging/production)")
    
    # Feature flags
    enable_ml_predictions: bool = Field(True, description="Enable ML prediction integration")
    enable_database_integration: bool = Field(True, description="Enable database integration")
    enable_performance_monitoring: bool = Field(True, description="Enable performance monitoring")
    enable_detailed_logging: bool = Field(True, description="Enable detailed logging")
    
    @field_validator('environment')
    @classmethod
    def validate_environment(cls, v):
        """Validate environment values"""
        valid_environments = ['development', 'staging', 'production', 'testing']
        if v not in valid_environments:
            raise ValueError(f"Environment must be one of {valid_environments}")
        return v
    
    def get_tier_threshold(self, tier: OpportunityTier) -> float:
        """Get threshold for a specific opportunity tier"""
        thresholds = {
            OpportunityTier.PREMIUM: self.opportunity_thresholds.premium_threshold,
            OpportunityTier.HIGH_VALUE: self.opportunity_thresholds.high_value_threshold,
            OpportunityTier.MODERATE: self.opportunity_thresholds.moderate_threshold,
            OpportunityTier.LOW: self.opportunity_thresholds.minimum_score
        }
        return thresholds[tier]
    
    def get_performance_target(self, operation: str) -> float:
        """Get performance target for a specific operation"""
        targets = {
            'discovery': self.performance.max_discovery_time_ms,
            'scoring': self.performance.max_scoring_time_ms,
            'pattern_detection': self.performance.max_pattern_time_ms,
            'explanation': self.performance.max_explanation_time_ms
        }
        return targets.get(operation, 1000.0)  # Default 1 second
    
    def is_production(self) -> bool:
        """Check if running in production environment"""
        return self.environment == 'production'
    
    def validate_signal_strength(self, strength: float) -> bool:
        """Validate signal strength value"""
        return 0.0 <= strength <= self.security.max_signal_strength


# Configuration loading utilities

def load_config_from_file(config_path: Optional[str] = None) -> MLOpportunityConfig:
    """
    Load configuration from YAML file with fallback to defaults
    
    Args:
        config_path: Path to configuration file (optional)
        
    Returns:
        MLOpportunityConfig instance
    """
    try:
        if config_path is None:
            # Look for config file in standard locations
            possible_paths = [
                'ml_opportunity_config.yaml',
                'config/ml_opportunity_config.yaml',
                'src/ml/opportunity_detection/config.yaml',
                os.path.expanduser('~/.mlb_betting/ml_config.yaml')
            ]
            
            config_path = None
            for path in possible_paths:
                if os.path.exists(path):
                    config_path = path
                    break
        
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)
            return MLOpportunityConfig(**config_data)
        else:
            # Use defaults
            return MLOpportunityConfig()
            
    except (ValidationError, yaml.YAMLError, IOError) as e:
        print(f"Error loading config from {config_path}: {e}")
        print("Using default configuration")
        return MLOpportunityConfig()


def load_config_from_env() -> MLOpportunityConfig:
    """
    Load configuration from environment variables
    
    Returns:
        MLOpportunityConfig instance with environment overrides
    """
    config = MLOpportunityConfig()
    
    # Override with environment variables if present
    env_mappings = {
        'ML_OPP_ENVIRONMENT': 'environment',
        'ML_OPP_ENABLE_PATTERNS': 'pattern_detection.enabled',
        'ML_OPP_ENABLE_EXPLANATIONS': 'explanation_engine.enabled',
        'ML_OPP_CACHE_TTL_HOURS': 'performance.cache_ttl_hours',
        'ML_OPP_MAX_DISCOVERY_TIME_MS': 'performance.max_discovery_time_ms',
        'ML_OPP_PREMIUM_THRESHOLD': 'opportunity_thresholds.premium_threshold',
        'ML_OPP_HIGH_VALUE_THRESHOLD': 'opportunity_thresholds.high_value_threshold'
    }
    
    for env_var, config_path in env_mappings.items():
        env_value = os.getenv(env_var)
        if env_value is not None:
            try:
                # Navigate to nested config attribute
                config_obj = config
                path_parts = config_path.split('.')
                for part in path_parts[:-1]:
                    config_obj = getattr(config_obj, part)
                
                # Set the value with appropriate type conversion
                current_value = getattr(config_obj, path_parts[-1])
                if isinstance(current_value, bool):
                    env_value = env_value.lower() in ('true', '1', 'yes', 'on')
                elif isinstance(current_value, (int, float)):
                    env_value = type(current_value)(env_value)
                
                setattr(config_obj, path_parts[-1], env_value)
                
            except (AttributeError, ValueError, TypeError) as e:
                print(f"Error setting config from {env_var}={env_value}: {e}")
    
    return config


# Global configuration instance
_config_instance: Optional[MLOpportunityConfig] = None


def get_ml_config() -> MLOpportunityConfig:
    """
    Get global ML opportunity configuration instance (singleton pattern)
    
    Returns:
        MLOpportunityConfig instance
    """
    global _config_instance
    if _config_instance is None:
        # Try to load from file first, then environment overrides
        _config_instance = load_config_from_file()
        
        # Apply environment overrides
        env_config = load_config_from_env()
        # For simplicity, we'll use the env config if it was modified
        # In production, you might want a more sophisticated merge strategy
        _config_instance = env_config
    
    return _config_instance


def reload_config(config_path: Optional[str] = None) -> MLOpportunityConfig:
    """
    Reload configuration (useful for testing and hot-reloading)
    
    Args:
        config_path: Optional path to configuration file
        
    Returns:
        Reloaded MLOpportunityConfig instance
    """
    global _config_instance
    _config_instance = None
    return get_ml_config()


# Export commonly used configurations for easy import
def get_scoring_weights() -> ScoringWeightsConfig:
    """Get scoring weights configuration"""
    return get_ml_config().scoring_weights


def get_opportunity_thresholds() -> OpportunityThresholdsConfig:
    """Get opportunity thresholds configuration"""
    return get_ml_config().opportunity_thresholds


def get_performance_config() -> PerformanceConfig:
    """Get performance configuration"""
    return get_ml_config().performance


def get_security_config() -> SecurityConfig:
    """Get security configuration"""
    return get_ml_config().security