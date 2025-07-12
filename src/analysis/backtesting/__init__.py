"""
Unified Backtesting System

Consolidated backtesting capabilities with enhanced features:
- Unified backtesting engine for all strategies
- Advanced performance analysis and metrics
- Strategy comparison and A/B testing
- Portfolio optimization and risk management
- Real-time backtesting with live data integration

This package provides:
- UnifiedBacktestingEngine: Modern async backtesting engine
- PerformanceAnalyzer: Comprehensive performance analysis
- StrategyComparator: A/B testing and strategy comparison
- BacktestingOrchestrator: Coordinated backtesting execution

Part of Phase 3: Strategy Integration - Unified Architecture Migration
"""

from .engine import UnifiedBacktestingEngine
from .analyzer import PerformanceAnalyzer
from .comparator import StrategyComparator
from .orchestrator import BacktestingOrchestrator

__all__ = [
    "UnifiedBacktestingEngine",
    "PerformanceAnalyzer",
    "StrategyComparator",
    "BacktestingOrchestrator"
] 