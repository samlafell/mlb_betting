"""
Unified Interfaces Package

This package consolidates all interfaces (CLI, API, Web) from the legacy modules
into a single, cohesive interface system.

Phase 4 Migration: Interface & Service Consolidation
- Consolidates CLI commands from mlb_sharp_betting, sportsbookreview, and action modules
- Provides unified API layer for external access
- Implements consistent interface patterns across all access methods
"""

from .cli import cli

__all__ = [
    'cli'
] 