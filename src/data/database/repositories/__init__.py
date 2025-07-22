"""Database repositories package."""

from .analysis_reports_repository import AnalysisReportsRepository

# Re-export legacy repositories for backward compatibility
from ..repositories_legacy import (
    BettingAnalysisCreateSchema,
    BettingAnalysisRepository,
    BettingAnalysisUpdateSchema,
    GameCreateSchema,
    GameRepository,
    GameUpdateSchema,
    OddsCreateSchema,
    OddsRepository,
    OddsUpdateSchema,
    SharpDataCreateSchema,
    SharpDataRepository,
    SharpDataUpdateSchema,
    UnifiedRepository,
    get_unified_repository,
)

__all__ = [
    "AnalysisReportsRepository",
    # Legacy exports
    "BettingAnalysisCreateSchema",
    "BettingAnalysisRepository",
    "BettingAnalysisUpdateSchema",
    "GameCreateSchema",
    "GameRepository",
    "GameUpdateSchema",
    "OddsCreateSchema",
    "OddsRepository",
    "OddsUpdateSchema",
    "SharpDataCreateSchema",
    "SharpDataRepository",
    "SharpDataUpdateSchema",
    "UnifiedRepository",
    "get_unified_repository",
]