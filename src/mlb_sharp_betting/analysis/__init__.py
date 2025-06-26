"""
MLB Sharp Betting Analysis Module

This module provides comprehensive sharp action analysis with layered architecture:
- Analytical processing: ML-based historical analysis and feature extraction
- Real-time processing: Live signal processing with strategy validation
- Detection interface: Unified API for sharp action detection
- Strategy management: Business rules and thresholds
"""

from .detectors.sharp_detector import SharpDetector
from .processors.analytical_processor import AnalyticalProcessor
from .processors.real_time_processor import RealTimeProcessor
from .strategies.sharp_action_strategy import SharpActionStrategy

__all__ = [
    "SharpDetector",
    "AnalyticalProcessor", 
    "RealTimeProcessor",
    "SharpActionStrategy",
] 