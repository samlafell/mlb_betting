"""
Unified Analysis System

Consolidated analysis capabilities from the legacy mlb_sharp_betting system
into a unified, enterprise-grade strategy processing framework.

This package provides:
- Unified strategy processors with async-first architecture
- Strategy orchestration and factory patterns
- Performance monitoring and validation
- A/B testing framework for strategy comparison
- Backtesting engine integration

Part of Phase 3: Strategy Integration - Unified Architecture Migration
"""

# Backtesting imports commented out - modules don't exist yet
# from .backtesting import (
#     UnifiedBacktestingEngine,
#     BacktestingOrchestrator,
#     PerformanceAnalyzer,
#     StrategyComparator
# )
from .models import (
    CrossStrategyComparison,
    UnifiedBettingSignal,
    UnifiedPerformanceMetrics,
    UnifiedStrategyData,
)
from .strategies import BaseStrategyProcessor, StrategyFactory, StrategyOrchestrator

__all__ = [
    # Strategy Processing
    "BaseStrategyProcessor",
    "StrategyOrchestrator",
    "StrategyFactory",
    # Models
    "UnifiedBettingSignal",
    "UnifiedStrategyData",
    "UnifiedPerformanceMetrics",
    "CrossStrategyComparison",
]

__version__ = "3.0.0"
__phase__ = "Phase 3: Strategy Integration"
