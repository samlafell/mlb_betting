"""
Curated Zone Services

Services for processing data from STAGING → CURATED zones.
Implements ML-ready data transformation and feature generation.

This module provides the missing pipeline services identified in the gap analysis:
- Enhanced Games Service: Process staging games → curated enhanced_games
- ML Temporal Features Service: Generate ML features from staging odds  
- Betting Splits Aggregator: Multi-source betting splits processing
- STAGING → CURATED Orchestrator: Pipeline coordination

Reference: 
- docs/STAGING_CURATED_GAP_ANALYSIS.md
- docs/DATA_GAP_ANALYSIS.md
"""

try:
    from .enhanced_games_service import EnhancedGamesService
    from .staging_curated_orchestrator import StagingCuratedOrchestrator
    
    __all__ = [
        "EnhancedGamesService", 
        "StagingCuratedOrchestrator"
    ]
except ImportError as e:
    # Handle import errors gracefully during development
    import warnings
    warnings.warn(f"Could not import curated zone services: {e}")
    __all__ = []