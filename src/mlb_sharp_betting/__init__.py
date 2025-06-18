"""
MLB Sharp Betting Data Analysis System

A modular system for collecting, analyzing, and monitoring sharp betting action
in Major League Baseball.
"""

__version__ = "0.2.0"
__author__ = "MLB Sharp Betting Analysis Team"

# Core imports for easy access
from mlb_sharp_betting.core.config import Settings
from mlb_sharp_betting.core.exceptions import MLBSharpBettingError
from mlb_sharp_betting.core.logging import get_logger

__all__ = [
    "Settings",
    "MLBSharpBettingError", 
    "get_logger",
] 