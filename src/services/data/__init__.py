"""
Unified Data Services Package

Consolidates all data-related services from legacy modules.

This package provides the UnifiedDataService as the main entry point
for all data collection operations across different sources.

Phase 5A Migration: Simplified Implementation
- UnifiedDataService: Main data collection orchestrator
- Additional services to be added as they are implemented
"""

from .unified_data_service import UnifiedDataService

__all__ = [
    'UnifiedDataService'
] 