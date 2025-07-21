"""
Data Pipeline Package

Implements the three-tier data pipeline architecture:
- RAW Zone: Unprocessed data from external sources
- STAGING Zone: Cleaned and validated data
- CURATED Zone: Feature-enriched, analysis-ready data

Reference: docs/SYSTEM_DESIGN_ANALYSIS.md
"""

from .zone_interface import DataZone, ZoneType
from .base_processor import BaseZoneProcessor
from .pipeline_orchestrator import DataPipelineOrchestrator

# Import zone processors to register them with the factory
from . import raw_zone
from . import staging_zone
from . import curated_zone

__all__ = [
    'DataZone',
    'ZoneType',
    'BaseZoneProcessor',
    'DataPipelineOrchestrator'
]