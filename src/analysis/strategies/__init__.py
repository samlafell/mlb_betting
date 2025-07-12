"""
Unified Strategy Processing System

Consolidated strategy processing capabilities with async-first architecture,
factory patterns, and comprehensive monitoring.

This package provides:
- BaseStrategyProcessor: Modern async base class for all strategies
- StrategyFactory: Dynamic strategy creation and management
- StrategyOrchestrator: Coordinated strategy execution
- StrategyValidator: Comprehensive strategy validation
- StrategyPerformanceMonitor: Real-time performance tracking

Part of Phase 3: Strategy Integration - Unified Architecture Migration
"""

from .base import BaseStrategyProcessor
from .factory import StrategyFactory
from .orchestrator import StrategyOrchestrator

__all__ = [
    "BaseStrategyProcessor",
    "StrategyFactory",
    "StrategyOrchestrator"
] 